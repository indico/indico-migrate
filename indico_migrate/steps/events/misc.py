# This file is part of Indico.
# Copyright (C) 2002 - 2017 European Organization for Nuclear Research (CERN).
#
# Indico is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License as
# published by the Free Software Foundation; either version 3 of the
# License, or (at your option) any later version.
#
# Indico is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Indico; if not, see <http://www.gnu.org/licenses/>.

from __future__ import unicode_literals

import re
from datetime import timedelta
from operator import itemgetter

from indico.core.db import db
from indico.modules.events.models.events import EventType
from indico.modules.events.reminders.models.reminders import EventReminder
from indico.modules.events.settings import event_core_settings, event_contact_settings
from indico.util.console import cformat, verbose_iterator
from indico.util.date_time import now_utc
from indico_migrate import convert_to_unicode
from indico_migrate.steps.events import EventMigrationStep


WEBFACTORY_NAME_RE = re.compile(r'^MaKaC\.webinterface\.(\w+)(?:\.WebFactory)?$')
SPLIT_EMAILS_RE = re.compile(r'[\s;,]+')
SPLIT_PHONES_RE = re.compile(r'[/;,]+')
ALARM_SENT_THRESHOLD = now_utc() - timedelta(days=1)


class EventTypeImporter(EventMigrationStep):
    def initialize_global_maps(self, g):
        g.wf_registry = {}

    def setup(self):
        self.print_info("Fetching data from WF registry")
        self.global_maps.wf_registry = {}
        for event_id, wf in self._iter_wfs():
            if wf is None:
                # conferences that have been lectures/meetings in the past
                continue

            wf_id = WEBFACTORY_NAME_RE.match(wf.__module__).group(1)
            if wf_id in ('simple_event', 'meeting'):
                self.global_maps.wf_registry[event_id] = wf_id
            else:
                self.print_error('Unexpected WF ID: {}'.format(wf_id))

    def migrate(self, conf, event):
        wf_entry = self.global_maps.wf_registry.get(conf.id)
        if wf_entry is None:
            event._type = EventType.conference
        else:
            event._type = EventType.lecture if wf_entry == 'simple_event' else EventType.meeting

    def _iter_wfs(self):
        it = self.zodb_root['webfactoryregistry'].iteritems()
        total = len(self.zodb_root['webfactoryregistry'])
        if not self.quiet:
            it = verbose_iterator(it, total, itemgetter(0), lambda x: '')
        for conf_id, wf in it:
            if conf_id.isdigit():
                yield conf_id, wf


class EventSettingsImporter(EventMigrationStep):
    def migrate(self, conf, event):
        if getattr(conf, '_screenStartDate', None):
            event_core_settings.set(event, 'start_dt_override', conf._screenStartDate)
        if getattr(conf, '_screenEndDate', None):
            event_core_settings.set(event, 'end_dt_override', conf._screenEndDate)
        organizer_info = convert_to_unicode(getattr(conf, '._orgText', ''))
        if organizer_info:
            event_core_settings.set(event, 'organizer_info', organizer_info)
        additional_info = convert_to_unicode(getattr(conf, 'contactInfo', ''))
        if additional_info:
            event_core_settings.set(event, 'additional_info', additional_info)
        si = conf._supportInfo
        contact_title = convert_to_unicode(si._caption)
        contact_email = convert_to_unicode(si._email)
        contact_phone = convert_to_unicode(si._telephone)
        contact_emails = map(unicode.strip, SPLIT_EMAILS_RE.split(contact_email)) if contact_email else []
        contact_phones = map(unicode.strip, SPLIT_PHONES_RE.split(contact_phone)) if contact_phone else []
        if contact_title:
            event_contact_settings.set(event, 'title', contact_title)
        if contact_emails:
            event_contact_settings.set(event, 'emails', contact_emails)
        if contact_phones:
            event_contact_settings.set(event, 'phones', contact_phones)


class EventAlarmImporter(EventMigrationStep):
    def migrate(self, conf, event):

        for alarm in conf.alarmList.itervalues():
            if not alarm.startDateTime:
                self.print_error('Alarm has no start time')
                continue
            start_dt = self._naive_to_aware(event, alarm.startDateTime).replace(second=0, microsecond=0)
            if not hasattr(alarm, 'status'):
                # Those ancient alarms can be safely assumed to be sent
                is_sent = True
            else:
                is_sent = alarm.status not in {1, 2}  # not spooled/queued
            is_overdue = False
            if not is_sent and start_dt < ALARM_SENT_THRESHOLD:
                is_sent = True
                is_overdue = True
            recipients = filter(None, {convert_to_unicode(x).strip().lower() for x in alarm.toAddr})
            reminder = EventReminder(event_new=event, creator=self.janitor,
                                     created_dt=alarm.createdOn, scheduled_dt=start_dt, is_sent=is_sent,
                                     event_start_delta=getattr(alarm, '_relative', None), recipients=recipients,
                                     send_to_participants=alarm.toAllParticipants,
                                     include_summary=alarm.confSumary,
                                     reply_to_address=convert_to_unicode(alarm.fromAddr).strip().lower(),
                                     message=convert_to_unicode(alarm.note).strip())
            db.session.add(reminder)
            status = (cformat('%{red!}OVERDUE%{reset}') if is_overdue else
                      cformat('%{green!}SENT%{reset}') if is_sent else
                      cformat('%{yellow}PENDING%{reset}'))
            self.print_success(cformat('%{cyan}{}%{reset} {}').format(reminder.scheduled_dt, status))


class EventShortUrlsImporter(EventMigrationStep):
    def initialize_global_maps(self, g):
        g.used_short_urls = {}

    def _validate_shorturl(self, shorturl):
        if shorturl.isdigit():
            return 'only-digits'
        if 'http://' in shorturl or 'https://' in shorturl:
            return 'url'
        # XXX: we allow spaces and similar harmless garbage here. it's awful but no need in breaking existing urls
        if not re.match(r'^[-a-zA-Z0-9/._ &@]+$', shorturl) or '//' in shorturl:
            return 'invalid-chars'
        if shorturl[0] == '/':
            return 'leading-slash'
        if shorturl[-1] == '/':
            return 'trailing-slash'
        return None

    def migrate(self, conf, event):
        if not getattr(conf, '_sortUrlTag', None):
            return
        shorturl = convert_to_unicode(conf._sortUrlTag)
        error = self._validate_shorturl(shorturl)
        if error == 'url':
            # show obvious garbage in a less prominent way
            self.print_warning(cformat('%{yellow}Shorturl %{yellow!}{}%{reset}%{yellow} is invalid: %{yellow!}{}')
                               .format(shorturl, error))
            return
        elif error:
            self.print_warning(cformat('%{red}Shorturl %{yellow!}{}%{reset}%{red} is invalid: %{red!}{}')
                               .format(shorturl, error))
            return
        conflict = self.global_maps.used_short_urls.get(shorturl.lower())
        if conflict:
            # if there's a conflict caused by the previously case-sensitive url shortcuts,
            # discard them in both events - it's better to get a 404 error than a wrong event
            self.print_error(cformat('%{red!}Shorturl %{reset}%{red}{}%{red!} collides with '
                                     'that of event %{reset}%{red}{}%{red!}; discarding both')
                             .format(shorturl, conflict))
            conflict.url_shortcut = None
            return
        self.global_maps.used_short_urls[shorturl.lower()] = event
        event.url_shortcut = shorturl
        self.print_success('{} -> {}'.format(shorturl, event.title))

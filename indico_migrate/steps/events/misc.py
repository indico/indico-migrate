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
from indico.modules.events.features.util import set_feature_enabled
from indico.modules.events.models.events import EventType
from indico.modules.events.models.legacy_mapping import LegacyEventMapping
from indico.modules.events.payment import payment_event_settings, payment_settings
from indico.modules.events.reminders.models.reminders import EventReminder
from indico.modules.events.settings import event_contact_settings, event_core_settings
from indico.util.date_time import now_utc
from indico.util.string import fix_broken_string

from indico_migrate.attachments import AttachmentMixin
from indico_migrate.badges_posters import BadgeMigration, PosterMigration
from indico_migrate.steps.events import EventMigrationStep
from indico_migrate.util import LocalFileImporterMixin, convert_to_unicode


WEBFACTORY_NAME_RE = re.compile(r'^MaKaC\.webinterface\.(\w+)(?:\.WebFactory)?$')
SPLIT_EMAILS_RE = re.compile(r'[\s;,]+')
SPLIT_PHONES_RE = re.compile(r'[/;,]+')
ALARM_SENT_THRESHOLD = now_utc() - timedelta(days=1)


class EventTypeImporter(EventMigrationStep):
    step_id = 'type'

    def setup(self):
        self.print_info("Fetching data from WF registry")
        for event_id, wf in self._iter_wfs():
            if wf is None:
                # conferences that have been lectures/meetings in the past
                continue

            wf_id = WEBFACTORY_NAME_RE.match(wf.__module__).group(1)
            if wf_id in ('simple_event', 'meeting'):
                self.global_ns.wf_registry[event_id] = wf_id
            else:
                self.print_error('Unexpected WF ID: {}'.format(wf_id))

    def migrate(self):
        wf_entry = self.global_ns.wf_registry.get(self.conf.id)
        if wf_entry is None:
            self.event._type = EventType.conference
        else:
            self.event._type = EventType.lecture if wf_entry == 'simple_event' else EventType.meeting

    def _iter_wfs(self):
        it = self.zodb_root['webfactoryregistry'].iteritems()
        total = len(self.zodb_root['webfactoryregistry'])
        if not self.quiet:
            it = self.logger.progress_iterator('Loading data', it, total, itemgetter(0), lambda x: '')
        return it


class EventSettingsImporter(EventMigrationStep):
    step_id = 'settings'

    def migrate(self):
        if getattr(self.conf, '_screenStartDate', None):
            event_core_settings.set(self.event, 'start_dt_override', self.conf._screenStartDate)
        if getattr(self.conf, '_screenEndDate', None):
            event_core_settings.set(self.event, 'end_dt_override', self.conf._screenEndDate)
        organizer_info = convert_to_unicode(getattr(self.conf, '._orgText', ''))
        if organizer_info:
            event_core_settings.set(self.event, 'organizer_info', organizer_info)
        additional_info = convert_to_unicode(getattr(self.conf, 'contactInfo', ''))
        if additional_info:
            event_core_settings.set(self.event, 'additional_info', additional_info)
        si = self.conf._supportInfo
        contact_title = convert_to_unicode(si._caption)
        contact_email = convert_to_unicode(si._email)
        contact_phone = convert_to_unicode(si._telephone)
        contact_emails = map(unicode.strip, SPLIT_EMAILS_RE.split(contact_email)) if contact_email else []
        contact_phones = map(unicode.strip, SPLIT_PHONES_RE.split(contact_phone)) if contact_phone else []
        if contact_title:
            event_contact_settings.set(self.event, 'title', contact_title)
        if contact_emails:
            event_contact_settings.set(self.event, 'emails', contact_emails)
        if contact_phones:
            event_contact_settings.set(self.event, 'phones', contact_phones)


class EventAlarmImporter(EventMigrationStep):
    step_id = 'alarm'

    def migrate(self):
        for alarm in self.conf.alarmList.itervalues():
            if not getattr(alarm, 'startDateTime', None):
                self.print_error('Alarm has no start time')
                continue
            start_dt = self._naive_to_aware(alarm.startDateTime).replace(second=0, microsecond=0)
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
            reminder = EventReminder(event=self.event, creator=self.system_user,
                                     created_dt=alarm.createdOn, scheduled_dt=start_dt, is_sent=is_sent,
                                     event_start_delta=getattr(alarm, '_relative', None), recipients=recipients,
                                     send_to_participants=alarm.toAllParticipants,
                                     include_summary=alarm.confSumary,
                                     reply_to_address=convert_to_unicode(alarm.fromAddr).strip().lower(),
                                     message=convert_to_unicode(alarm.note).strip())
            db.session.add(reminder)
            status = ('%[red!]OVERDUE%[reset]' if is_overdue else
                      '%[green!]SENT%[reset]' if is_sent else
                      '%[yellow]PENDING%[reset]')
            self.print_success('%[cyan]{}%[reset] {}'.format(reminder.scheduled_dt, status))


class EventShortUrlsImporter(EventMigrationStep):
    step_id = 'shorturl'

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

    def migrate(self):
        if not getattr(self.conf, '_sortUrlTag', None):
            return
        shorturl = convert_to_unicode(self.conf._sortUrlTag)
        error = self._validate_shorturl(shorturl)
        if error == 'url':
            # show obvious garbage in a less prominent way
            self.print_warning('%[yellow]Shorturl %[yellow!]{}%[reset]%[yellow] is invalid: %[yellow!]{}'
                               .format(shorturl, error))
            return
        elif error:
            self.print_warning('%[red]Shorturl %[yellow!]{}%[reset]%[red] is invalid: %[red!]{}'
                               .format(shorturl, error))
            return
        conflict = self.global_ns.used_short_urls.get(shorturl.lower())
        if conflict:
            # if there's a conflict caused by the previously case-sensitive url shortcuts,
            # discard them in both events - it's better to get a 404 error than a wrong event
            self.print_error('%[red!]Shorturl %[reset]%[red]{}%[red!] collides with '
                                     'that of event %[reset]%[red]{}%[red!]; discarding both'
                             .format(shorturl, conflict))
            conflict.url_shortcut = None
            return
        self.global_ns.used_short_urls[shorturl.lower()] = self.event
        self.event.url_shortcut = shorturl
        self.print_success('{} -> {}'.format(shorturl, self.event.title))


class EventMiscImporter(EventMigrationStep):
    step_id = 'misc'

    def migrate(self):
        self.global_ns.legacy_event_ids[self.conf.id] = self.event
        self._migrate_location()
        self._migrate_keywords_visibility()

    def _migrate_keywords_visibility(self):
        self.event.created_dt = self._naive_to_aware(self.conf._creationDS)
        self.event.visibility = self._convert_visibility(self.conf._visibility)
        old_keywords = getattr(self.conf, '_keywords', None)
        if old_keywords is None:
            self.print_info("Conference object has no '_keywords' attribute")
            return
        keywords = self._convert_keywords(old_keywords)
        if keywords:
            self.event.keywords = keywords
            if not self.quiet:
                self.print_success('Keywords: {}'.format(repr(keywords)))

    def _migrate_location(self):
        custom_location = self.conf.places[0] if getattr(self.conf, 'places', None) else None
        custom_room = self.conf.rooms[0] if getattr(self.conf, 'rooms', None) else None
        location_name = None
        room_name = None
        has_room = False
        if custom_location:
            location_name = convert_to_unicode(fix_broken_string(custom_location.name, True))
            if custom_location.address:
                self.event.own_address = convert_to_unicode(fix_broken_string(custom_location.address, True))
        if custom_room:
            room_name = convert_to_unicode(fix_broken_string(custom_room.name, True))
        if location_name and room_name:
            mapping = self.global_ns.room_mapping.get((location_name, room_name))
            if mapping:
                has_room = True
                self.event.own_venue_id = mapping[0]
                self.event.own_room_id = mapping[1]
        # if we don't have a RB room set, use whatever location/room name we have
        if not has_room:
            venue_id = self.global_ns.venue_mapping.get(location_name)
            if venue_id is not None:
                self.event.own_venue_id = venue_id
                self.event.own_venue_name = ''
            else:
                self.event.own_venue_name = location_name or ''
            self.event.own_room_name = room_name or ''

    def _convert_visibility(self, old_visibility):
        return None if old_visibility > 900 else old_visibility

    def _convert_keywords(self, old_keywords):
        return filter(None, map(unicode.strip, map(convert_to_unicode, old_keywords.splitlines())))


class EventLegacyIdImporter(EventMigrationStep):
    step_id = 'legacyid'

    def migrate(self):
        if self.is_legacy_event:
            db.session.add(LegacyEventMapping(legacy_event_id=self.conf.id, event_id=self.event.id))
            if not self.quiet:
                self.print_success('-> %[cyan]{}'.format(self.event.id))


class EventPaymentSettingsImporter(EventMigrationStep):
    step_id = 'payment'

    def migrate(self):
        if not hasattr(self.conf, '_registrationForm') or not hasattr(self.conf, '_modPay'):
            self.event_ns.misc_data['payment_currency'] = payment_settings.get('currency')
            self.event_ns.payment_messages.update({
                'register': '',
                'success': ''
            })
            self.print_info('Event has no legacy payment/registration data')
            return

        old_payment = self.conf._modPay
        default_conditions = payment_settings.get('conditions')
        conditions = (getattr(old_payment, 'paymentConditions', default_conditions)
                      if (getattr(old_payment, 'paymentConditionsEnabled', False) and
                          convert_to_unicode(getattr(old_payment, 'specificPaymentConditions', '')).strip() == '')
                      else getattr(old_payment, 'specificPaymentConditions', ''))
        # Get rid of the most terrible part of the old default conditions
        conditions = convert_to_unicode(conditions).replace('CANCELLATION :', 'CANCELLATION:')
        payment_enabled = getattr(old_payment, 'activated', False)
        if payment_enabled:
            set_feature_enabled(self.event, 'payment', True)

        payment_event_settings.set(self.event, 'conditions', conditions)

        register_email = getattr(old_payment, 'receiptMsg', '')
        success_email = getattr(old_payment, 'successMsg', '')

        # The new messages are shown in an "additional info" section, so the old defaults can always go away
        if convert_to_unicode(register_email) == 'Please, see the summary of your order:':
            register_email = ''
        if convert_to_unicode(success_email) == 'Congratulations, your payment was successful.':
            success_email = ''

        # save these messages for later, since the settings
        # are now part of the reg. form
        currency = getattr(self.conf._registrationForm, '_currency', '')
        if not re.match(r'^[A-Z]{3}$', currency):
            currency = ''
        self.event_ns.misc_data['payment_currency'] = currency
        self.event_ns.payment_messages['register'] = register_email
        self.event_ns.payment_messages['success'] = success_email

        self.print_success("Payment enabled={0}, currency={1}".format(payment_enabled, currency))


class EventAttachmentsImporter(AttachmentMixin, EventMigrationStep):
    step_id = 'attachments'

    def __init__(self, *args, **kwargs):
        self._set_config_options(**kwargs)
        super(EventAttachmentsImporter, self).__init__(*args, **kwargs)

    def migrate(self):
        self.migrate_event_attachments()


class EventBadgesPostersImporter(LocalFileImporterMixin, EventMigrationStep):
    step_id = 'designer'

    def __init__(self, *args, **kwargs):
        super(EventBadgesPostersImporter, self).__init__(*args, **kwargs)
        self._set_config_options(**kwargs)

    def migrate(self):
        BadgeMigration(self, self.conf, self.event, self.system_user).run()
        PosterMigration(self, self.conf, self.event, self.system_user).run()

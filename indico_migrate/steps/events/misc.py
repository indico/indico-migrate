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
from operator import itemgetter

from indico.modules.events.models.events import EventType
from indico.modules.events.settings import event_core_settings, event_contact_settings
from indico.util.console import verbose_iterator
from indico_migrate import convert_to_unicode
from indico_migrate.steps.events import EventMigrationStep


WEBFACTORY_NAME_RE = re.compile(r'^MaKaC\.webinterface\.(\w+)(?:\.WebFactory)?$')
SPLIT_EMAILS_RE = re.compile(r'[\s;,]+')
SPLIT_PHONES_RE = re.compile(r'[/;,]+')


class EventTypeImporter(EventMigrationStep):
    def setup(self):
        self.print_info("Fetching data from WF registry")
        self.wf_registry = {}
        for event_id, wf in self._iter_wfs():
            if wf is None:
                # conferences that have been lectures/meetings in the past
                continue

            wf_id = WEBFACTORY_NAME_RE.match(wf.__module__).group(1)
            if wf_id in ('simple_event', 'meeting'):
                self.wf_registry[event_id] = wf_id
            else:
                self.print_error('Unexpected WF ID: {}'.format(wf_id), event_id=event_id)

    def migrate(self, conf, event):
        wf_entry = self.wf_registry.get(conf.id)
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

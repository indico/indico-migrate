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

from datetime import datetime, timedelta

from indico.core.db import db
from indico.modules.events.logs import EventLogEntry, EventLogKind, EventLogRealm
from indico.util.date_time import format_datetime, format_human_timedelta
from indico.util.string import seems_html

from indico_migrate.steps.events import EventMigrationStep
from indico_migrate.util import convert_to_unicode


def _convert_data(conf, value):
    if isinstance(value, timedelta):
        value = format_human_timedelta(value)
    elif isinstance(value, datetime):
        tz = getattr(conf, 'timezone', 'UTC')
        value = format_datetime(value, locale='en_GB', timezone=tz)
    elif value.__class__.__name__ == 'ContributionType':
        value = value._name
    elif value.__class__.__name__ == 'AbstractFieldContent':
        value = '{}: "{}"'.format(convert_to_unicode(value.field._caption), convert_to_unicode(value.value))
    return convert_to_unicode(value).strip()


class EventLogImporter(EventMigrationStep):
    step_id = 'logs'

    def migrate(self):
        if not hasattr(self.conf, '_logHandler'):
            self.print_error('Event has no log handler!')
            return
        for item in self.conf._logHandler._logLists['emailLog']:
            entry = self._migrate_email_log(item)
            db.session.add(entry)
            if not self.quiet:
                self.print_success(str(entry))
        for item in self.conf._logHandler._logLists['actionLog']:
            entry = self._migrate_action_log(item)
            db.session.add(entry)
            if not self.quiet:
                self.print_success(str(entry))

    def _migrate_log(self, item):
        user = None
        if (item._responsibleUser and item._responsibleUser.__class__.__name__ == 'Avatar' and
                unicode(item._responsibleUser.id).isdigit()):
            user = self.global_ns.avatar_merged_user.get(item._responsibleUser.id)
        module = item._module or 'Unknown'
        if module.startswith('MaKaC/plugins/Collaboration'):
            module = 'Collaboration'
        elif module == 'chat' or module.startswith('MaKaC/plugins/InstantMessaging/XMPP'):
            module = 'Chat'
        elif module == 'vc_vidyo':
            module = 'Vidyo'
        elif module == 'Timetable/SubContribution':
            module = 'Timetable/Subcontribution'
        elif module.islower():
            module = module.title()
        entry = EventLogEntry(event=self.event, logged_dt=self._naive_to_aware(item._logDate),
                              module=module, user=user, kind=EventLogKind.other)
        return entry

    def _migrate_email_log(self, item):
        info = item._logInfo
        entry = self._migrate_log(item)
        entry.realm = EventLogRealm.emails
        entry.type = 'email'
        entry.summary = 'Sent email: {}'.format(convert_to_unicode(info['subject']).strip())
        content_type = convert_to_unicode(info.get('contentType')) or (
            'text/html' if seems_html(info['body']) else 'text/plain')
        entry.data = {
            'from': convert_to_unicode(info['fromAddr']),
            'to': map(convert_to_unicode, set(info['toList'])),
            'cc': map(convert_to_unicode, set(info['ccList'])),
            'bcc': map(convert_to_unicode, set(info.get('bccList', []))),
            'subject': convert_to_unicode(info['subject']),
            'body': convert_to_unicode(info['body']),
            'content_type': content_type,
        }
        return entry

    def _migrate_action_log(self, item):
        info = item._logInfo
        entry = self._migrate_log(item)
        entry.realm = EventLogRealm.event
        entry.type = 'simple'
        entry.summary = convert_to_unicode(info['subject']).strip()
        entry.data = {convert_to_unicode(k): _convert_data(self.conf, v) for k, v in info.iteritems() if k != 'subject'}
        return entry

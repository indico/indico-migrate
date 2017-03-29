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

from HTMLParser import HTMLParser
from indico.modules.categories import upcoming_events_settings
from indico.web.flask.templating import strip_tags

from indico_migrate import Importer, convert_to_unicode


def _sanitize_title(title, _ws_re=re.compile(r'\s+')):
    title = convert_to_unicode(title)
    title = HTMLParser().unescape(strip_tags(title))
    return _ws_re.sub(' ', title).strip()


class GlobalPostEventsImporter(Importer):
    def migrate(self):
        self.migrate_global_ip_acl()
        self.migrate_api_settings()
        self.migrate_global_settings()
        self.migrate_upcoming_event_settings()

    def migrate_upcoming_event_settings(self):
        self.print_step('Upcoming event settings')
        mod = self.zodb_root['modules']['upcoming_events']
        upcoming_events_settings.set('max_entries', int(mod._maxEvents))
        print [o.obj for o in mod._objects]
        entries = [{'weight': float(entry.weight),
                    'days': entry.advertisingDelta.days,
                    'type': 'category' if type(entry.obj).__name__ == 'Category' else 'event',
                    'id': int(entry.obj.id)}
                   for entry in mod._objects]
        upcoming_events_settings.set('entries', entries)

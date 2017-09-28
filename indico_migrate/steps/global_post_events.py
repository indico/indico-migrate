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

import itertools
import re
from datetime import date
from HTMLParser import HTMLParser

from indico.modules.categories import upcoming_events_settings
from indico.util.string import strip_tags

from indico_migrate.importer import TopLevelMigrationStep
from indico_migrate.util import convert_to_unicode, step_description


def _sanitize_title(title, _ws_re=re.compile(r'\s+')):
    title = convert_to_unicode(title)
    title = HTMLParser().unescape(strip_tags(title))
    return _ws_re.sub(' ', title).strip()


class GlobalPostEventsImporter(TopLevelMigrationStep):
    step_name = 'global_post'

    @step_description('Upcoming event settings')
    def migrate(self):
        self.migrate_upcoming_event_settings()
        self.migrate_survey_tasks()

    def migrate_upcoming_event_settings(self):
        mod = self.zodb_root['modules']['upcoming_events']
        upcoming_events_settings.set('max_entries', int(mod._maxEvents))
        entries = []
        for entry in mod._objects:
            is_category = type(entry.obj).__name__ == 'Category'
            try:
                obj_id = (self.global_ns.legacy_category_ids[entry.obj.id].id if is_category
                          else self.global_ns.event_ids[entry.obj.id].id)
            except KeyError:
                self.print_warning('invalid id for upcoming events: {} (category: {})'.format(entry.obj.id,
                                                                                              is_category))
                continue
            entries.append({
                'weight': float(entry.weight),
                'days': entry.advertisingDelta.days,
                'type': 'category' if is_category else 'event',
                'id': obj_id
            })
        upcoming_events_settings.set('entries', entries)

    def migrate_survey_tasks(self):
        scheduler_root = self.zodb_root['modules']['scheduler']
        it = (t for t in itertools.chain.from_iterable(scheduler_root._waitingQueue._container.itervalues())
              if t.__class__.__name__ == 'EvalutationAlarm')

        today = date.today()
        for task in it:
            survey = self.global_ns.legacy_survey_mapping[task.conf]
            start_date = task.conf._evaluations[0].startDate.date()
            if start_date < today:
                self.print_warning('evaluation starts in the past ({})'.format(start_date))
                survey.start_notification_sent = True
            elif not task.conf._evaluations[0].visible:
                self.print_warning('evaluation is disabled')
            else:
                self.print_success('survey notification task [{}]'.format(start_date))

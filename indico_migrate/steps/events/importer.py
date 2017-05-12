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

from operator import attrgetter

import pytz
from indico.core.db import db
from indico.modules.events.models.events import Event
from indico.modules.events.models.legacy_mapping import LegacyEventMapping
from indico.modules.events.models.settings import EventSetting
from indico.modules.users import User
from indico.util.console import cformat, verbose_iterator
from indico.util.string import is_legacy_id, fix_broken_string
from indico.util.struct.iterables import committing_iterator

from indico_migrate import TopLevelMigrationStep, convert_to_unicode


# this function is here only to avoid import loops
def _get_all_steps():
    from indico_migrate.steps.events.acls import EventACLImporter
    from indico_migrate.steps.events.layout import EventLayoutImporter, EventImageImporter
    from indico_migrate.steps.events.logs import EventLogImporter
    from indico_migrate.steps.events.menus import EventMenuImporter
    from indico_migrate.steps.events.misc import (EventTypeImporter, EventSettingsImporter, EventAlarmImporter,
                                                  EventShortUrlsImporter)
    from indico_migrate.steps.events.surveys import EventSurveyImporter
    return (EventTypeImporter, EventACLImporter, EventLogImporter, EventSettingsImporter, EventAlarmImporter,
            EventImageImporter, EventLayoutImporter, EventShortUrlsImporter, EventMenuImporter, EventSurveyImporter)


class EventImporter(TopLevelMigrationStep):

    def __init__(self, *args, **kwargs):
        self.janitor_user_id = kwargs.pop('janitor_user_id')
        self.janitor = User.get_one(self.janitor_user_id)
        super(EventImporter, self).__init__(*args, **kwargs)
        self.event_id_counter = self.zodb_root['counters']['CONFERENCE']._Counter__count
        self.kwargs = kwargs
        self.kwargs['janitor'] = self.janitor

    def has_data(self):
        return (EventSetting.query.filter(EventSetting.module.in_(['core', 'contact'])).has_rows() or
                Event.query.filter_by(is_locked=True).has_rows())

    def initialize_global_maps(self, g):
        g.legacy_event_ids = {}

    def migrate(self):
        self.migrate_event_data()
        db.session.commit()

    def migrate_event_data(self):
        all_event_steps = _get_all_steps()

        importers = [importer(self.app, self.sqlalchemy_uri, self.zodb_root, not self.quiet, self.dblog,
                              self.default_group_provider, self.tz, **self.kwargs) for importer in all_event_steps]
        for importer in importers:
            importer.setup()

        self.print_step("Event data")
        for conf in committing_iterator(self._iter_events()):
            is_legacy = False

            if is_legacy_id(conf.id):
                event_id = int(self.gen_event_id())
                is_legacy = True
            else:
                event_id = int(conf.id)

            if 'title' not in conf.__dict__:
                self.print_error('Event has no title in ZODB', conf.id)
                continue

            try:
                parent_category = self.global_maps.legacy_category_ids[conf._Conference__owners[0].id]
            except (IndexError, KeyError):
                self.print_error(cformat('%{red!}Event has no category!'), event_id=conf.id)
                continue

            title = convert_to_unicode(conf.__dict__['title']) or '(no title)'
            if not self.quiet:
                self.print_success(title)

            tz = conf.__dict__.get('timezone', 'UTC')
            event = Event(id=event_id,
                          title=title,
                          description=convert_to_unicode(conf.__dict__['description']) or '',
                          timezone=tz,
                          start_dt=self._fix_naive(conf.__dict__['startDate'], tz),
                          end_dt=self._fix_naive(conf.__dict__['endDate'], tz),
                          is_locked=conf._closed,
                          category=parent_category,
                          is_deleted=False)

            self.global_maps.legacy_event_ids[conf.id] = event
            self._migrate_location(conf, event)
            self._migrate_keywords_visibility(conf, event)

            for importer in importers:
                with db.session.no_autoflush:
                    importer.run(conf, event)

            if is_legacy:
                db.session.add(LegacyEventMapping(legacy_event_id=conf.id, event_id=event_id))
                if not self.quiet:
                    self.print_success(cformat('-> %{cyan}{}').format(event_id), event_id=conf.id)

    def _convert_keywords(self, old_keywords):
        return filter(None, map(unicode.strip, map(convert_to_unicode, old_keywords.splitlines())))

    def _convert_visibility(self, old_visibility):
        return None if old_visibility > 900 else old_visibility

    def _migrate_keywords_visibility(self, conf, event):
        event.created_dt = self._fix_naive(conf._creationDS, event.timezone)
        event.visibility = self._convert_visibility(conf._visibility)
        old_keywords = getattr(conf, '_keywords', None)
        if old_keywords is None:
            self.print_warning("Conference object has no '_keywords' attribute")
            return
        keywords = self._convert_keywords(old_keywords)
        if keywords:
            event.keywords = keywords
            if not self.quiet:
                self.print_success('Keywords: {}'.format(repr(keywords)))

    def _migrate_location(self, old_event, new_event):
        custom_location = old_event.places[0] if getattr(old_event, 'places', None) else None
        custom_room = old_event.rooms[0] if getattr(old_event, 'rooms', None) else None
        location_name = None
        room_name = None
        has_room = False
        if custom_location:
            location_name = convert_to_unicode(fix_broken_string(custom_location.name, True))
            if custom_location.address:
                new_event.own_address = convert_to_unicode(fix_broken_string(custom_location.address, True))
        if custom_room:
            room_name = convert_to_unicode(fix_broken_string(custom_room.name, True))
        if location_name and room_name:
            mapping = self.global_maps.room_mapping.get((location_name, room_name))
            if mapping:
                has_room = True
                new_event.own_venue_id = mapping[0]
                new_event.own_room_id = mapping[1]
        # if we don't have a RB room set, use whatever location/room name we have
        if not has_room:
            venue_id = self.global_maps.venue_mapping.get(location_name)
            if venue_id is not None:
                new_event.own_venue_id = venue_id
                new_event.own_venue_name = ''
            else:
                new_event.own_venue_name = location_name or ''
            new_event.own_room_name = room_name or ''

    def _iter_events(self):
        def _it():
            for conf in self.zodb_root['conferences'].itervalues():
                dir(conf)  # make zodb load attrs
                yield conf
        it = _it()
        total = len(self.zodb_root['conferences'])
        if self.quiet:
            it = verbose_iterator(it, total, attrgetter('id'), lambda x: x.__dict__.get('title', ''))
        for old_event in self.flushing_iterator(it):
            yield old_event

    def _fix_naive(self, dt, tz):
        if dt.tzinfo is None:
            self.print_warning('Naive datetime converted ({})'.format(dt))
            return pytz.timezone(tz).localize(dt)
        else:
            return dt

    def gen_event_id(self):
        self.event_id_counter += 1
        return self.event_id_counter

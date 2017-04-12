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
from operator import attrgetter

import pytz
from indico.core.db import db
from indico.modules.events.models.events import Event
from indico.modules.events.models.legacy_mapping import LegacyEventMapping
from indico.modules.events.models.settings import EventSetting
from indico.modules.events.settings import event_core_settings, event_contact_settings
from indico.modules.users import User
from indico.util.console import cformat, verbose_iterator
from indico.util.string import is_legacy_id, fix_broken_string
from indico.util.struct.iterables import committing_iterator

from indico_migrate import TopLevelMigrationStep, convert_to_unicode


SPLIT_EMAILS_RE = re.compile(r'[\s;,]+')
SPLIT_PHONES_RE = re.compile(r'[/;,]+')


# this function is here only to avoid import loops
def _get_all_steps():
    from indico_migrate.steps.events.managers import EventManagerImporter, EventTypeImporter
    return (EventManagerImporter, EventTypeImporter)


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
                          start_dt=self._fix_naive(conf, conf.__dict__['startDate'], tz),
                          end_dt=self._fix_naive(conf, conf.__dict__['endDate'], tz),
                          is_locked=conf._closed,
                          category=parent_category,
                          is_deleted=False)

            self._migrate_location(conf, event)

            for importer in importers:
                with db.session.no_autoflush:
                    importer.run(conf, event)

            self._migrate_settings(conf, event_id)

            if is_legacy:
                db.session.add(LegacyEventMapping(legacy_event_id=conf.id, event_id=event_id))
                if not self.quiet:
                    self.print_success(cformat('-> %{cyan}{}').format(event_id), event_id=conf.id)

    def _migrate_settings(self, conf, event_id):
        if getattr(conf, '_screenStartDate', None):
            event_core_settings.set(event_id, 'start_dt_override', conf._screenStartDate)
        if getattr(conf, '_screenEndDate', None):
            event_core_settings.set(event_id, 'end_dt_override', conf._screenEndDate)
        organizer_info = convert_to_unicode(getattr(conf, '._orgText', ''))
        if organizer_info:
            event_core_settings.set(event_id, 'organizer_info', organizer_info)
        additional_info = convert_to_unicode(getattr(conf, 'contactInfo', ''))
        if additional_info:
            event_core_settings.set(event_id, 'additional_info', additional_info)
        si = conf._supportInfo
        contact_title = convert_to_unicode(si._caption)
        contact_email = convert_to_unicode(si._email)
        contact_phone = convert_to_unicode(si._telephone)
        contact_emails = map(unicode.strip, SPLIT_EMAILS_RE.split(contact_email)) if contact_email else []
        contact_phones = map(unicode.strip, SPLIT_PHONES_RE.split(contact_phone)) if contact_phone else []
        if contact_title:
            event_contact_settings.set(event_id, 'title', contact_title)
        if contact_emails:
            event_contact_settings.set(event_id, 'emails', contact_emails)
        if contact_phones:
            event_contact_settings.set(event_id, 'phones', contact_phones)

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

    def _fix_naive(self, old_event, dt, tz):
        if dt.tzinfo is None:
            self.print_warning('Naive datetime converted ({})'.format(dt), old_event.id)
            return pytz.timezone(tz).localize(dt)
        else:
            return dt

    def gen_event_id(self):
        self.event_id_counter += 1
        return self.event_id_counter

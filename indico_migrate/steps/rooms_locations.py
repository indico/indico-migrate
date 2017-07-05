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

import os
from collections import defaultdict
from itertools import ifilter

from indico.core.db import db
from indico.modules.groups import GroupProxy
from indico.modules.rb import rb_settings
from indico.modules.rb.models.aspects import Aspect
from indico.modules.rb.models.blocked_rooms import BlockedRoom
from indico.modules.rb.models.blockings import Blocking
from indico.modules.rb.models.equipment import EquipmentType
from indico.modules.rb.models.locations import Location
from indico.modules.rb.models.photos import Photo
from indico.modules.rb.models.room_attributes import RoomAttribute, RoomAttributeAssociation
from indico.modules.rb.models.room_bookable_hours import BookableHours
from indico.modules.rb.models.room_nonbookable_periods import NonBookablePeriod
from indico.modules.rb.models.rooms import Room
from indico.util.date_time import as_utc
from indico.util.string import is_valid_mail

from indico_migrate.importer import TopLevelMigrationStep
from indico_migrate.util import convert_to_unicode, step_description


attribute_map = {
    'Simba List': 'Manager Group',
    'Booking Simba List': 'Allowed Booking Group'
}


def generate_name(old_room):
    return '{}-{}-{}'.format(old_room.building, old_room.floor, old_room.roomNr)


def get_canonical_name_of(old_room):
    return '{}-{}'.format(old_room._locationName, generate_name(old_room))


def get_room_id(guid):
    return int(guid.split('|')[1].strip())


class RoomsLocationsImporter(TopLevelMigrationStep):
    step_name = 'rooms_locations'

    def __init__(self, *args, **kwargs):
        self.photo_path = kwargs.pop('photo_path')
        self.rb_root = kwargs.get('rb_root')
        super(RoomsLocationsImporter, self).__init__(*args, **kwargs)

    def migrate(self):
        self.migrate_settings()
        self.migrate_locations()
        self.migrate_rooms()
        self.migrate_blockings()
        db.session.commit()
        self.fix_sequences('roombooking')

    def migrate_settings(self):
        rb_settings.delete_all()
        opts = self.zodb_root['plugins']['RoomBooking']._PluginBase__options

        # Admins & authorized users/groups
        rb_settings.acls.set('authorized_principals', self.convert_principal_list(opts['AuthorisedUsersGroups']))
        rb_settings.acls.set('admin_principals', self.convert_principal_list(opts['Managers']))
        # Assistance emails
        emails = [email for email in opts['assistanceNotificationEmails']._PluginOption__value
                  if is_valid_mail(email, False)]
        rb_settings.set('assistance_emails', emails)
        # Simple settings
        rb_settings.set('notification_hour', opts['notificationHour']._PluginOption__value)
        rb_settings.set('notification_before_days', opts['notificationBefore']._PluginOption__value)
        db.session.flush()

    @step_description('Room locations')
    def migrate_locations(self):
        default_location_name = self.zodb_root['DefaultRoomBookingLocation']
        custom_attributes_dict = self.rb_root['CustomAttributesList']

        for old_location in self.zodb_root['RoomBookingLocationList']:
            # create location
            location = Location(
                name=convert_to_unicode(old_location.friendlyName),
                is_default=(old_location.friendlyName == default_location_name)
            )

            self.print_info('- %[cyan]{}'.format(location.name))

            # add aspects
            for old_aspect in old_location._aspects.values():
                a = Aspect(
                    name=convert_to_unicode(old_aspect.name),
                    center_latitude=old_aspect.centerLatitude,
                    center_longitude=old_aspect.centerLongitude,
                    zoom_level=old_aspect.zoomLevel,
                    top_left_latitude=old_aspect.topLeftLatitude,
                    top_left_longitude=old_aspect.topLeftLongitude,
                    bottom_right_latitude=old_aspect.bottomRightLatitude,
                    bottom_right_longitude=old_aspect.bottomRightLongitude
                )

                self.print_info('  %[blue!]Aspect:%[reset] {}'.format(a.name))

                location.aspects.append(a)
                if old_aspect.defaultOnStartup:
                    location.default_aspect = a

            # add custom attributes
            for ca in custom_attributes_dict.get(location.name, []):
                if ca['type'] != 'str':
                    raise RuntimeError('Non-str custom attributes are unsupported: {}'.format(ca))
                attr_name = attribute_map.get(ca['name'], ca['name'])
                attr = RoomAttribute(name=attr_name.replace(' ', '-').lower(), title=attr_name, type=ca['type'],
                                     is_required=ca['required'], is_hidden=ca['hidden'])
                location.attributes.append(attr)
                self.print_info('  %[blue!]Attribute:%[reset] {}'.format(attr.title))

            self.global_ns.venue_mapping[location.name] = location.id
            # add new created location
            db.session.add(location)
        db.session.flush()

    @step_description('Rooms')
    def migrate_rooms(self):
        eq = defaultdict(set)
        vc = defaultdict(set)
        for old_room_id, old_room in self.rb_root['Rooms'].iteritems():
            eq[old_room._locationName].update(e for e in old_room._equipment.split('`') if e)
            vc[old_room._locationName].update(e for e in getattr(old_room, 'avaibleVC', []) if e)

        for name, eqs in eq.iteritems():
            location = Location.find_first(name=name)

            if location is None:
                self.print_warning("Location '{}' does not exist. Skipped equipment: {}".format(name, eqs))
                continue

            location.equipment_types.extend(EquipmentType(name=x) for x in eqs)
            self.print_info('- [%[cyan]{}%[reset]] {}'.format(name, eqs))
            db.session.add(location)
        db.session.flush()

        for name, vcs in vc.iteritems():
            location = Location.find_first(name=name)

            if location is None:
                self.print_warning("Location '{}' does not exist. Skipped VC equipment: {}".format(name, vcs))
                continue

            pvc = location.get_equipment_by_name('Video conference')
            for vc_name in vcs:
                req = EquipmentType(name=vc_name)
                req.parent = pvc
                location.equipment_types.append(req)
                self.print_info('- [%[cyan]{}%[reset]] {}'.format(name, req.name))
            db.session.add(location)
        db.session.flush()

        for old_room_id, old_room in self.rb_root['Rooms'].iteritems():
            location = Location.find_first(name=old_room._locationName)

            if location is None:
                self.print_warning("Location '{}' does not exist. Skipped room '{}'".format(old_room._locationName,
                                                                                            old_room.id))
                continue

            r = Room(
                id=old_room_id,
                location=location,
                name=convert_to_unicode((old_room._name or '').strip() or generate_name(old_room)),
                site=convert_to_unicode(old_room.site),
                division=convert_to_unicode(old_room.division),
                building=convert_to_unicode(old_room.building),
                floor=convert_to_unicode(old_room.floor),
                number=convert_to_unicode(old_room.roomNr),

                notification_before_days=((old_room.resvStartNotificationBefore or None)
                                          if getattr(old_room, 'resvStartNotification', False)
                                          else None),
                notification_for_responsible=getattr(old_room, 'resvNotificationToResponsible', False),
                notification_for_assistance=getattr(old_room, 'resvNotificationAssistance', False),

                reservations_need_confirmation=old_room.resvsNeedConfirmation,

                telephone=convert_to_unicode(getattr(old_room, 'telephone', None)),
                key_location=convert_to_unicode(getattr(old_room, 'whereIsKey', None)),

                capacity=getattr(old_room, 'capacity', None),
                surface_area=getattr(old_room, 'surfaceArea', None),
                latitude=getattr(old_room, 'latitude', None),
                longitude=getattr(old_room, 'longitude', None),

                comments=convert_to_unicode(getattr(old_room, 'comments', None)),

                owner=self.global_ns.avatar_merged_user[old_room.responsibleId],

                is_active=old_room.isActive,
                is_reservable=old_room.isReservable,
                max_advance_days=int(old_room.maxAdvanceDays) if getattr(old_room, 'maxAdvanceDays', None) else None
            )

            self.print_info('- [%[cyan]{}%[reset]] %[grey!]{:4}%[reset]  %[green!]{}%[reset]'.format(
                location.name, r.id, r.name))

            for old_bookable_time in getattr(old_room, '_dailyBookablePeriods', []):
                r.bookable_hours.append(
                    BookableHours(
                        start_time=old_bookable_time._startTime,
                        end_time=old_bookable_time._endTime
                    )
                )
                self.print_info('  %[blue!]Bookable:%[reset] {}'.format(r.bookable_hours[-1]))

            for old_nonbookable_date in getattr(old_room, '_nonBookableDates', []):
                r.nonbookable_periods.append(
                    NonBookablePeriod(
                        start_dt=old_nonbookable_date._startDate,
                        end_dt=old_nonbookable_date._endDate
                    )
                )
                self.print_info('  %[blue!]Nonbookable:%[reset] {}'.format(r.nonbookable_periods[-1]))

            if self.photo_path:
                try:
                    with open(os.path.join(self.photo_path, 'large_photos',
                                           get_canonical_name_of(old_room) + '.jpg'), 'rb') as f:
                        large_photo = f.read()
                except Exception:
                    large_photo = None

                try:
                    with open(os.path.join(self.photo_path, 'small_photos',
                                           get_canonical_name_of(old_room) + '.jpg'), 'rb') as f:
                        small_photo = f.read()
                except Exception:
                    small_photo = None

                if large_photo and small_photo:
                    r.photo = Photo(data=large_photo, thumbnail=small_photo)
                    self.print_info('  %[blue!]Photos')

            new_eq = []
            for old_equipment in ifilter(None, old_room._equipment.split('`') + old_room.avaibleVC):
                room_eq = location.get_equipment_by_name(old_equipment)
                new_eq.append(room_eq)
                r.available_equipment.append(room_eq)
            if new_eq:
                self.print_info('  %[blue!]Equipment:%[reset] {}'
                                .format(', '.join(sorted(x.name for x in new_eq))))

            for attr_name, value in getattr(old_room, 'customAtts', {}).iteritems():
                value = convert_to_unicode(value)
                if not value or ('Simba' in attr_name and value == u'Error: unknown mailing list'):
                    continue
                attr_name = attribute_map.get(attr_name, attr_name).replace(' ', '-').lower()
                ca = location.get_attribute_by_name(attr_name)
                if not ca:
                    self.print_info('  %[blue!]Attribute:%[reset] {} %[red!]not found'.format(attr_name))
                    continue
                attr = RoomAttributeAssociation()
                attr.value = value
                attr.attribute = ca
                r.attributes.append(attr)
                self.print_info('  %[blue!]Attribute:%[reset] {} = {}'
                                .format(attr.attribute.title, attr.value))

            self.global_ns.room_mapping[(location.name, r.name)] = (location.id, r.id)
            db.session.add(location)
        db.session.flush()

    @step_description('Room blockings')
    def migrate_blockings(self):
        state_map = {
            None: BlockedRoom.State.pending,
            False: BlockedRoom.State.rejected,
            True: BlockedRoom.State.accepted
        }

        for old_blocking_id, old_blocking in self.rb_root['RoomBlocking']['Blockings'].iteritems():
            b = Blocking(
                id=old_blocking.id,
                created_by_user=self.global_ns.avatar_merged_user[old_blocking._createdBy],
                created_dt=as_utc(old_blocking._utcCreatedDT),
                start_date=old_blocking.startDate,
                end_date=old_blocking.endDate,
                reason=convert_to_unicode(old_blocking.message)
            )

            self.print_info(u'- %[cyan]{}'.format(b.reason))
            for old_blocked_room in old_blocking.blockedRooms:
                br = BlockedRoom(
                    state=state_map[old_blocked_room.active],
                    rejected_by=old_blocked_room.rejectedBy,
                    rejection_reason=convert_to_unicode(old_blocked_room.rejectionReason),
                )
                room = Room.get(get_room_id(old_blocked_room.roomGUID))
                room.blocked_rooms.append(br)
                b.blocked_rooms.append(br)
                self.print_info(u'  %[blue!]Room:%[reset] {} ({})'.format(room.full_name,
                                                                          BlockedRoom.State(br.state).title))

            for old_principal in old_blocking.allowed:
                if old_principal._type == 'Avatar':
                    principal = self.global_ns.avatar_merged_user[old_principal._id]
                elif old_principal._type == 'Group':
                    assert int(old_principal.id) in self.global_ns.all_groups
                    principal = GroupProxy(int(old_principal._id))
                elif old_principal._type in {'CERNGroup', 'LDAPGroup', 'NiceGroup'}:
                    principal = GroupProxy(old_principal._id, self.default_group_provider)

                b.allowed.add(principal)
                self.print_info(u'  %[blue!]Allowed:%[reset] {}'.format(principal))
            db.session.add(b)
        db.session.flush()

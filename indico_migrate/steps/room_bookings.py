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
import time
from datetime import datetime, timedelta

from babel import dates

from indico.core.db import db
from indico.modules.rb.models.reservation_edit_logs import ReservationEditLog
from indico.modules.rb.models.reservation_occurrences import ReservationOccurrence
from indico.modules.rb.models.reservations import RepeatMapping, Reservation
from indico.modules.rb.models.rooms import Room
from indico.util.date_time import as_utc

from indico_migrate.importer import TopLevelMigrationStep
from indico_migrate.util import convert_to_unicode, step_description


FRENCH_MONTH_NAMES = [(str(i), name[:3].encode('utf-8').lower())
                      for i, name in dates.get_month_names(locale='fr_FR').iteritems()]


def utc_to_local(dt):
    assert dt.tzinfo is None
    return dt - timedelta(seconds=time.altzone)


def parse_dt_string(value):
    try:
        return datetime.strptime(value, '%d %b %Y %H:%M')
    except ValueError:
        # French month name
        for num, name in FRENCH_MONTH_NAMES:
            if name in value:
                value = value.lower().replace(name, num)
                break
        return datetime.strptime(value, '%d %m %Y %H:%M')


class RoomBookingsImporter(TopLevelMigrationStep):
    step_name = 'room_bookings'

    def __init__(self, *args, **kwargs):
        self.rb_root = kwargs.get('rb_root')
        super(RoomBookingsImporter, self).__init__(*args, **kwargs)

    @step_description('Room Bookings')
    def migrate(self):
        i = 1
        for rid, v in self.rb_root['Reservations'].iteritems():
            room = Room.get(v.room.id)
            if room is None:
                self.print_error('skipping resv for dead room {0.room.id}: {0.id} ({0._utcCreatedDT})'.format(v))
                continue

            repeat_frequency, repeat_interval = RepeatMapping.convert_legacy_repeatability(v.repeatability)
            booked_for_id = getattr(v, 'bookedForId', None)

            r = Reservation(
                id=v.id,
                room=room,
                created_dt=as_utc(v._utcCreatedDT),
                start_dt=utc_to_local(v._utcStartDT),
                end_dt=utc_to_local(v._utcEndDT),
                booked_for_user=self.global_ns.avatar_merged_user.get(booked_for_id),
                booked_for_name=convert_to_unicode(v.bookedForName),
                created_by_user=self.global_ns.avatar_merged_user.get(v.createdBy),
                is_cancelled=v.isCancelled,
                is_accepted=v.isConfirmed,
                is_rejected=v.isRejected,
                booking_reason=convert_to_unicode(v.reason),
                rejection_reason=convert_to_unicode(getattr(v, 'rejectionReason', None)),
                repeat_frequency=repeat_frequency,
                repeat_interval=repeat_interval,
                uses_vc=getattr(v, 'usesAVC', False),
                needs_vc_assistance=getattr(v, 'needsAVCSupport', False),
                needs_assistance=getattr(v, 'needsAssistance', False)
            )

            for eq_name in getattr(v, 'useVC', []):
                eq = room.location.get_equipment_by_name(eq_name)
                if eq:
                    r.used_equipment.append(eq)

            occurrence_rejection_reasons = {}
            if getattr(v, 'resvHistory', None):
                for h in reversed(v.resvHistory._entries):
                    ts = as_utc(parse_dt_string(h._timestamp))

                    if len(h._info) == 2:
                        possible_rejection_date, possible_rejection_reason = h._info
                        m = re.match(r'Booking occurrence of the (\d{1,2} \w{3} \d{4}) rejected',
                                     possible_rejection_reason)
                        if m:
                            d = datetime.strptime(m.group(1), '%d %b %Y')
                            occurrence_rejection_reasons[d] = possible_rejection_reason[9:].strip('\'')

                    el = ReservationEditLog(
                        timestamp=ts,
                        user_name=h._responsibleUser,
                        info=map(convert_to_unicode, h._info)
                    )
                    r.edit_logs.append(el)

            notifications = getattr(v, 'startEndNotification', []) or []
            excluded_days = getattr(v, '_excludedDays', []) or []
            ReservationOccurrence.create_series_for_reservation(r)
            for occ in r.occurrences:
                occ.notification_sent = occ.date in notifications
                occ.is_rejected = r.is_rejected
                occ.is_cancelled = r.is_cancelled or occ.date in excluded_days
                occ.rejection_reason = (convert_to_unicode(occurrence_rejection_reasons[occ.date])
                                        if occ.date in occurrence_rejection_reasons else None)

            event_id = getattr(v, '_ReservationBase__owner', None)
            if hasattr(event_id, '_Impersistant__obj'):  # Impersistant object
                event_id = event_id._Impersistant__obj
            if event_id is not None:
                event = self.zodb_root['conferences'].get(event_id)
                if event:
                    # For some stupid reason there are bookings in the database which have a completely unrelated parent
                    guids = getattr(event, '_Conference__roomBookingGuids', [])
                    if any(int(x.id) == v.id for x in guids if x.id is not None):
                        r.event_id = int(event_id)
                    else:
                        self.print_error('event {} does not contain booking {}'.format(event_id, v.id))

            self.print_info('- [%[cyan]{}%[reset]/%[green!]{}%[reset]]  %[grey!]{}%[reset]  {}'.format(
                room.location_name,
                room.name,
                r.id,
                r.created_dt.date()))

            i = (i + 1) % 1000
            if not i:
                db.session.commit()
        db.session.commit()
        self.fix_sequences('roombooking')

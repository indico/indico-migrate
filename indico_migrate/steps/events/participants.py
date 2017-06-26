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

from itertools import chain
from uuid import uuid4

from werkzeug.utils import cached_property

from indico.core.db import db
from indico.modules.events.features.util import set_feature_enabled
from indico.modules.events.payment import payment_settings
from indico.modules.events.registration.models.form_fields import RegistrationFormField
from indico.modules.events.registration.models.forms import ModificationMode, RegistrationForm
from indico.modules.events.registration.models.items import PersonalDataType, RegistrationFormSection
from indico.modules.events.registration.models.registrations import Registration, RegistrationData, RegistrationState
from indico.modules.events.registration.util import create_personal_data_fields
from indico.util.date_time import now_utc
from indico.util.string import normalize_phone_number

from indico_migrate.steps.events import EventMigrationStep
from indico_migrate.util import convert_to_unicode


PARTICIPATION_FORM_TITLE = 'Participants'

PARTICIPANT_ATTR_MAP = {
    PersonalDataType.affiliation: '_affiliation',
    PersonalDataType.address: '_address',
    PersonalDataType.phone: '_telephone'
}

PARTICIPANT_STATUS_MAP = {
    'declined': RegistrationState.rejected,
    'refused': RegistrationState.withdrawn,
    'rejected': RegistrationState.withdrawn,
    'pending': RegistrationState.pending
}


class EventParticipantsImporter(EventMigrationStep):
    step_id = 'part'

    def migrate(self):
        self.regform = None
        self.emails = set()
        self.users = set()
        self.pd_field_map = {}
        self.status_field = None
        self.status_map = {}
        self.title_map = {}
        self.past_event = self.event.end_dt < now_utc()

        self.migrate_regforms()

    def migrate_regforms(self):
        try:
            self.old_participation = self.conf._participation
        except AttributeError:
            self.print_info('Event has no participation')
            return
        if not self.old_participation._participantList and not self.old_participation._pendingParticipantList:
            self.print_info('Participant lists are empty')
            return

        set_feature_enabled(self.event, 'registration', True)

        with db.session.no_autoflush:
            self.regform = RegistrationForm(event_id=self.event.id, title=PARTICIPATION_FORM_TITLE,
                                            is_participation=True, currency=payment_settings.get('currency'))
            if not self.quiet:
                self.print_success('%[cyan]{}'.format(self.regform.title))
            self._migrate_settings()
            self._create_form()
            self._migrate_participants()

        db.session.add(self.regform)
        db.session.flush()

    def teardown(self):
        # add all managers as registration notification recipients
        db.session.execute(db.text("""
            UPDATE event_registration.forms rf SET manager_notification_recipients = (
                SELECT array_agg(ue.email)
                FROM events.principals p
                JOIN users.emails ue ON (ue.user_id = p.user_id AND NOT ue.is_user_deleted AND ue.is_primary)
                WHERE p.event_id = rf.event_id AND p.full_access AND p.type = 1
            )
            WHERE manager_notification_recipients = '{}' AND manager_notifications_enabled AND title = :title;
        """).bindparams(title=PARTICIPATION_FORM_TITLE))
        db.session.commit()

    def iter_participants(self):
        return chain(self.old_participation._participantList.itervalues(),
                     self.old_participation._pendingParticipantList.itervalues(),
                     getattr(self.old_participation, '_declinedParticipantList', {}).itervalues())

    @cached_property
    def status_used(self):
        default_statuses = {'added', 'pending'}
        return any(p._status not in default_statuses for p in self.iter_participants())

    def _migrate_settings(self):
        old_part = self.old_participation
        if old_part._allowedForApplying:
            self.regform.start_dt = self.event.created_dt
            self.regform.end_dt = self.event.end_dt
        self.regform.moderation_enabled = not getattr(old_part, '_autoAccept', False)
        self.regform.publish_registrations_enabled = old_part._displayParticipantList
        self.regform.registration_limit = max(0, int(getattr(old_part, '_numMaxParticipants', 0))) or None
        self.regform.manager_notifications_enabled = getattr(old_part, '_notifyMgrNewParticipant', False)
        self.regform.modification_mode = ModificationMode.not_allowed
        # manager emails are migrated afterwards

    def _create_form(self):
        create_personal_data_fields(self.regform)
        for item in self.regform.form_items:
            if not item.is_field:
                item.position = 1  # pd section
                continue
            # we have nothing but personal data fields right now. no need for extra checks!
            if item.personal_data_type != PersonalDataType.country:
                self.pd_field_map[item.personal_data_type] = item
            if item.personal_data_type == PersonalDataType.title:
                self.title_map = {v: k for k, v in item.data['captions'].iteritems()}

        # create administrative section for statuses
        if self.status_used:
            section = RegistrationFormSection(registration_form=self.regform, is_manager_only=True, title='Status',
                                              position=2)
            if self.status_used:
                choices = []
                for status in ('refused', 'excused', 'invited', 'accepted', 'rejected', 'declined'):
                    uuid = unicode(uuid4())
                    caption = status.title()
                    choices.append({'price': 0, 'is_billable': False, 'places_limit': 0, 'is_enabled': True,
                                    'caption': caption, 'id': uuid})
                    self.status_map[status] = {'uuid': uuid, 'caption': caption}
                field_data = {
                    'item_type': 'dropdown',
                    'with_extra_slots': False,
                    'default_item': None,
                    'choices': choices
                }
                self.status_field = field = RegistrationFormField(registration_form=self.regform, parent=section,
                                                                  input_type='single_choice', title='Status')
                field.data, field.versioned_data = field.field_impl.process_field_data(field_data)

    def _migrate_participants(self):
        offset = self.event_ns.misc_data.get('last_registrant_friendly_id', 0)
        for n, old_part in enumerate(self.iter_participants(), offset + 1):
            registration = self._migrate_participant(old_part)
            registration.friendly_id = n
            self.regform.registrations.append(registration)
        db.session.flush()

    def _migrate_participant(self, old_part):
        state = PARTICIPANT_STATUS_MAP.get(old_part._status, RegistrationState.complete)
        registration = Registration(first_name=convert_to_unicode(old_part._firstName),
                                    last_name=convert_to_unicode(old_part._familyName),
                                    email=self._fix_email(old_part._email),
                                    submitted_dt=self.event.created_dt,
                                    base_price=0, price_adjustment=0,
                                    checked_in=old_part._present, state=state,
                                    currency=payment_settings.get('currency'))
        self.print_info('%[yellow]Registration%[reset] - %[cyan]{}%[reset] [{}]'
                        .format(registration.full_name, state.title))
        self._migrate_participant_user(old_part, registration)
        self._migrate_participant_data(old_part, registration)
        self._migrate_participant_status(old_part, registration)
        return registration

    def _fix_email(self, email):
        email = convert_to_unicode(email).lower() or 'no-email@example.com'
        no_email = email == 'no-email@example.com'
        try:
            user, host = email.split('@', 1)
        except ValueError:
            self.print_warning('Garbage email %[red]{0}%[reset]; using %[green]{0}@example.com%[reset] instead'
                               .format(email))
            user = email
            host = 'example.com'
            email += '@example.com'
        n = 1
        while email in self.emails:
            email = '{}+{}@{}'.format(user, n, host)
            n += 1
        if n != 1 and not no_email:
            self.print_warning('Duplicate email %[yellow]{}@{}%[reset]; using %[green]{}%[reset] instead'
                               .format(user, host, email))
        self.emails.add(email)
        return email

    def _migrate_participant_user(self, old_part, registration):
        user = self.global_ns.users_by_email.get(registration.email)
        if user is not None:
            if user in self.users:
                self.print_warning('User {} is already associated with a registration; not associating them with {}'
                                   .format(user, registration))
                return
            self.users.add(user)
            registration.user = user
        if not self.past_event and old_part._avatar and old_part._avatar.id in self.global_ns.avatar_merged_user:
            user = self.global_ns.avatar_merged_user[old_part._avatar.id]
            if not registration.user:
                self.print_warning('No email match; discarding association between {} and {}'
                                   .format(user, registration))
            elif registration.user != user:
                self.print_warning('Email matches other user; associating {} with {} instead of {}'
                                   .format(registration, registration.user, user))

    def _migrate_participant_data(self, old_part, registration):
        for pd_type, field in self.pd_field_map.iteritems():
            if pd_type.column:
                friendly_value = value = getattr(registration, pd_type.column)
            elif pd_type == PersonalDataType.title:
                try:
                    value = {self.title_map[old_part._title]: 1}
                except KeyError:
                    value = None
                friendly_value = convert_to_unicode(old_part._title)
            elif pd_type == PersonalDataType.position:
                continue
            else:
                value = convert_to_unicode(getattr(old_part, PARTICIPANT_ATTR_MAP[pd_type]))
                if pd_type == PersonalDataType.phone and value:
                    value = normalize_phone_number(value)
                friendly_value = value
            if value:
                field.is_enabled = True
            if not self.quiet:
                self.print_info('%[yellow!]{}%[reset] %[cyan!]{}%[reset]'.format(pd_type.name, friendly_value))
            registration.data.append(RegistrationData(field_data=field.current_data, data=value))

    def _migrate_participant_status(self, old_part, registration):
        if not self.status_used:
            return
        if old_part._status not in {'added', 'pending'}:
            status_info = self.status_map[old_part._status]
            data = {status_info['uuid']: 1}
            caption = status_info['caption']
        else:
            data = None
            caption = ''
        if not self.quiet and data:
            self.print_info('%[red]STATUS%[reset] %[cyan]{}'.format(caption))
        registration.data.append(RegistrationData(field_data=self.status_field.current_data, data=data))

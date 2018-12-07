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

import mimetypes
import re
from copy import deepcopy
from datetime import datetime, timedelta
from decimal import Decimal
from operator import attrgetter
from uuid import uuid4

from sqlalchemy.orm.attributes import flag_modified

from indico.core.db import db
from indico.modules.events import Event
from indico.modules.events.features.util import set_feature_enabled
from indico.modules.events.payment.models.transactions import PaymentTransaction, TransactionStatus
from indico.modules.events.registration.models.form_fields import (RegistrationFormField, RegistrationFormFieldData,
                                                                   RegistrationFormPersonalDataField)
from indico.modules.events.registration.models.forms import ModificationMode, RegistrationForm
from indico.modules.events.registration.models.items import (PersonalDataType, RegistrationFormPersonalDataSection,
                                                             RegistrationFormSection, RegistrationFormText)
from indico.modules.events.registration.models.legacy_mapping import LegacyRegistrationMapping
from indico.modules.events.registration.models.registrations import Registration, RegistrationData, RegistrationState
from indico.util.date_time import as_utc, now_utc
from indico.util.fs import secure_filename
from indico.util.string import normalize_phone_number

from indico_migrate.steps.events import EventMigrationStep
from indico_migrate.util import LocalFileImporterMixin, convert_to_unicode, sanitize_user_input


def ensure_tzinfo(dt):
    return as_utc(dt) if dt.tzinfo is None else dt


def get_input_type_id(input):
    return {
        'LabelInput': 'label',
        'CheckboxInput': 'checkbox',
        'YesNoInput': 'yes/no',
        'FileInput': 'file',
        'RadioGroupInput': 'radio',
        'CountryInput': 'country',
        'DateInput': 'date',
        'TextInput': 'text',
        'TelephoneInput': 'telephone',
        'TextareaInput': 'textarea',
        'NumberInput': 'number'
    }[input.__class__.__name__]


def _get_pay_later_data(ti_data):
    return {
        'amount': ti_data['OrderTotal'],
        'currency': ti_data['Currency'],
        'provider': '_manual',
        'data': {'_migrated': True}
    }


def _get_cern_yellow_pay_data(ti_data):
    return {
        'amount': float(ti_data['OrderTotal']),
        'currency': ti_data['Currency'],
        'provider': 'cern',
        'timestamp': ensure_tzinfo(ti_data['payment_date']),
        'data': {
            'BRAND': ti_data.get('PaymentMethod', ''),
            'PAYID': ti_data['TransactionID'],
            '_migrated': True
        }
    }


def _get_paypal_data(ti_data):
    return {
        'amount': float(ti_data['mc_gross']),
        'currency': ti_data['mc_currency'],
        'provider': 'paypal',
        'timestamp': ensure_tzinfo(ti_data['payment_date']),
        'data': {
            'verify_sign': ti_data['verify_sign'],
            'payer_id': ti_data['payer_id'],
            'mc_gross': ti_data['mc_gross'],
            'mc_currency': ti_data['mc_currency'],
            '_migrated': True
        }
    }


def _get_worldpay_data(ti_data):
    return {
        'amount': float(ti_data['amount']),
        'currency': ti_data['currency'],
        'provider': '_manual',
        'timestamp': ensure_tzinfo(ti_data['payment_date']),
        'data': {
            'email': ti_data['email'],
            'transId': ti_data['transId'],
            '_migrated': True,
            '_old_provider': 'worldpay'
        }
    }


def _get_saferpay_data(ti_data):
    del ti_data['ModPay']
    amount = float(ti_data.pop('OrderTotal'))
    currency = ti_data.pop('Currency')
    timestamp = ensure_tzinfo(ti_data.pop('payment_date'))
    return {
        'amount': amount,
        'currency': currency,
        'provider': 'sixpay',
        'timestamp': timestamp,
        'data': dict(ti_data, _migrated=True),
    }


def _extract_int(val):
    if isinstance(val, str):
        val = re.sub(r'[^0-9]+', '', val)
    return int(val)


class EventRegFormImporter(LocalFileImporterMixin, EventMigrationStep):
    step_id = 'regform'

    def __init__(self, *args, **kwargs):
        super(EventRegFormImporter, self).__init__(*args, **kwargs)
        self._set_config_options(**kwargs)

    def teardown(self):
        # sync friendly ids
        value = db.func.coalesce(db.session.query(db.func.max(Registration.friendly_id)).
                                 filter(Registration.event_id == Event.id)
                                 .as_scalar(), 0)
        Event.query.update({Event._last_friendly_registration_id: value}, synchronize_session=False)

    def migrate(self):
        self.section_map = {}
        self.field_map = {}
        self.status_map = {}
        self.emails = set()
        self.price_adjusted_versions = {}
        self.accommodation_field = None
        self.accommodation_choice_map = {}
        self.social_events_field = None
        self.social_events_choice_map = {}
        self.social_events_info_map = {}
        self.social_events_versions = {}
        self.reason_field = None
        self.multi_session_field = None
        self.specific_session_fields = None
        self.session_choices = None
        self.session_choice_map = {}
        self.session_extra_choice_versions = None
        self.session_extra_choice_map = None
        self.users = set()
        self.past_event = self.event.end_dt < now_utc()

        try:
            regform = self.conf._registrationForm
        except AttributeError:
            self.print_warning('Event has no regform')
            return

        self.old_regform = regform

        if (not self.conf._registrants and
            (not regform.activated or
             regform.startRegistrationDate.date() == regform.endRegistrationDate.date())):
            return

        self.migrate_regform()
        set_feature_enabled(self.event, 'registration', True)
        db.session.add(self.regform)
        db.session.flush()

    def migrate_regform(self):
        self.regform = RegistrationForm(event_id=int(self.event.id), base_price=0,
                                        currency=self.event_ns.misc_data['payment_currency'])
        self._migrate_settings()
        self.print_success('%[blue!]{}%[reset] - %[cyan]{}'.format(self.regform.start_dt.date(), self.regform.title))
        self._migrate_form()
        self._migrate_custom_statuses()
        self._migrate_registrations()

    def _migrate_settings(self):
        old_rf = self.old_regform
        self.regform.title = sanitize_user_input(old_rf.title)
        self.regform.introduction = sanitize_user_input(old_rf.announcement)
        self.regform.contact_info = sanitize_user_input(old_rf.contactInfo)
        self.regform.start_dt = self._naive_to_aware(old_rf.startRegistrationDate)
        self.regform.end_dt = self._naive_to_aware(old_rf.endRegistrationDate)
        self.regform.modification_mode = ModificationMode.allowed_always
        self.regform.require_login = getattr(old_rf, '_mandatoryAccount', False)
        self.regform.registration_limit = old_rf.usersLimit if old_rf.usersLimit > 0 else None
        self.regform.notification_sender_address = convert_to_unicode(getattr(old_rf, '_notificationSender', None))
        self.regform.manager_notification_recipients = sorted(set(old_rf.notification._ccList) |
                                                              set(old_rf.notification._toList))
        self.regform.manager_notifications_enabled = bool(self.regform.manager_notification_recipients)
        self.regform.publish_registrations_enabled = not self.event_ns.misc_data['participant_list_disabled']

        self.regform.message_unpaid = self.event_ns.payment_messages['register']
        self.regform.message_complete = self.event_ns.payment_messages['success']

        old_eticket = getattr(old_rf, '_eTicket', None)
        if old_eticket:
            self.regform.tickets_enabled = old_eticket._enabled
            self.regform.ticket_on_email = old_eticket._attachedToEmail
            self.regform.ticket_on_summary_page = old_eticket._showAfterRegistration
            self.regform.ticket_on_event_page = old_eticket._showInConferenceMenu
        if hasattr(old_rf, 'modificationEndDate'):
            modification_end_dt = (self._naive_to_aware(old_rf.modificationEndDate)
                                   if old_rf.modificationEndDate else None)
            if modification_end_dt and modification_end_dt > self.regform.end_dt:
                self.regform.modification_end_dt = modification_end_dt

    def _migrate_form(self):
        for form in self.old_regform._sortedForms:
            type_ = form.__class__.__name__
            if type_ == 'PersonalDataForm':
                self._migrate_personal_data_section(form)
            elif type_ == 'GeneralSectionForm':
                self._migrate_general_section(form)
            elif type_ == 'FurtherInformationForm':
                self._migrate_further_info_section(form)
            elif type_ == 'ReasonParticipationForm':
                self._migrate_reason_section(form)
            elif type_ == 'AccommodationForm':
                self._migrate_accommodation_section(form)
            elif type_ == 'SocialEventForm':
                self._migrate_social_event_section(form)
            elif type_ == 'SessionsForm':
                self._migrate_sessions_section(form)
            else:
                raise TypeError('Unhandled section: ' + type_)

    def _migrate_sessions_section(self, form):
        if not form._enabled and not any(x._sessions for x in self.conf._registrants.itervalues()):
            return
        section = RegistrationFormSection(registration_form=self.regform, title=sanitize_user_input(form._title),
                                          description=sanitize_user_input(form._description, html=True))
        self.print_info('%[green!]Section/Sessions%[reset] - %[cyan]{}'.format(section.title))
        field_data = {
            'with_extra_slots': False,
            'choices': []
        }
        for sess in form._sessions.itervalues():
            # we intentionally use a static uuid even if we have two fields.
            # this way we don't have to bother with per-field choice mappings
            uuid = unicode(uuid4())
            data = {'price': 0, 'is_billable': False, 'is_enabled': True,
                    'caption': sanitize_user_input(sess._session.title), 'id': uuid}
            if form._type != '2priorities':
                data['is_billable'], data['price'] = self._convert_billable(sess)
            field_data['choices'].append(data)
            self.session_choice_map[sess] = uuid
        self.session_choices = field_data['choices']
        if form._type == '2priorities':
            field_data['item_type'] = 'dropdown'
            field_data['default_item'] = None
            # primary choice
            field = RegistrationFormField(registration_form=self.regform, input_type='single_choice',
                                          title='Preferred choice', is_required=True)
            field.data, field.versioned_data = field.field_impl.process_field_data(field_data)
            section.children.append(field)
            # secondary choice
            field2 = RegistrationFormField(registration_form=self.regform, input_type='single_choice',
                                           title='Secondary choice')
            field2.data, field2.versioned_data = field2.field_impl.process_field_data(field_data)
            section.children.append(field2)
            self.specific_session_fields = (field, field2)
        else:
            # multi-choice field
            field = self.multi_session_field = RegistrationFormField(registration_form=self.regform, title='Sessions',
                                                                     input_type='multi_choice')
            field.data, field.versioned_data = field.field_impl.process_field_data(field_data)
            section.children.append(field)

    def _migrate_social_event_section(self, form):
        if not form._enabled and not any(x._socialEvents for x in self.conf._registrants.itervalues()):
            return
        section = RegistrationFormSection(registration_form=self.regform, title=sanitize_user_input(form._title),
                                          description=sanitize_user_input(form._description, html=True))
        self.print_info('%[green!]Section/Social%[reset] - %[cyan]{}'.format(section.title))
        input_type = 'multi_choice' if getattr(form, '_selectionType', 'multiple') == 'multiple' else 'single_choice'
        field_data = {'with_extra_slots': True, 'choices': []}
        if input_type == 'single_choice':
            field_data['item_type'] = 'radiogroup'
            field_data['default_item'] = None
        for item in form._socialEvents.itervalues():
            uuid = unicode(uuid4())
            billable, price = self._convert_billable(item)
            extra_slots_pay = bool(getattr(item, '_pricePerPlace', False))
            field_data['choices'].append({
                'price': price,
                'is_billable': billable,
                'places_limit': int(getattr(item, '_placesLimit', 0)),
                'is_enabled': not bool(getattr(item, '_cancelled', True)),
                'max_extra_slots': int(item._maxPlacePerRegistrant),
                'extra_slots_pay': extra_slots_pay,
                'caption': sanitize_user_input(item._caption),
                'id': uuid
            })
            self.social_events_choice_map[item] = uuid
            self.social_events_info_map[item] = (billable, price, extra_slots_pay)

        field = self.social_events_field = RegistrationFormField(registration_form=self.regform, input_type=input_type,
                                                                 title=section.title,
                                                                 description=sanitize_user_input(form._introSentence),
                                                                 is_required=bool(getattr(form, '_mandatory', False)))
        field.data, field.versioned_data = field.field_impl.process_field_data(field_data)
        section.children.append(field)

    def _migrate_accommodation_section(self, form):
        if not form._enabled and all(x._accommodation._accommodationType is None
                                     for x in self.conf._registrants.itervalues()):
            return
        arrival_offset = getattr(form, '_arrivalOffsetDates', [-2, 0])
        departure_offset = getattr(form, '_departureOffsetDates', [1, 3])
        no_acc_choice_id = unicode(uuid4())
        data = {
            'arrival_date_from': (self.event.start_dt + timedelta(days=arrival_offset[0])).strftime('%Y-%m-%d'),
            'arrival_date_to': (self.event.start_dt + timedelta(days=arrival_offset[1])).strftime('%Y-%m-%d'),
            'departure_date_from': (self.event.end_dt + timedelta(days=departure_offset[0])).strftime('%Y-%m-%d'),
            'departure_date_to': (self.event.end_dt + timedelta(days=departure_offset[1])).strftime('%Y-%m-%d'),
            'captions': {
                no_acc_choice_id: 'No accommodation'
            }
        }
        versioned_data = {'choices': []}
        for item in form._accommodationTypes.itervalues():
            uuid = unicode(uuid4())
            billable, price = self._convert_billable(item)
            data['captions'][uuid] = sanitize_user_input(item._caption)
            versioned_data['choices'].append({
                'price': price,
                'is_billable': billable,
                'places_limit': int(getattr(item, '_placesLimit', 0)),
                'is_enabled': not getattr(item, '_cancelled', False),
                'id': uuid
            })
            self.accommodation_choice_map[item] = uuid

        # Add a 'No accommodation' option
        versioned_data['choices'].append({
            'is_no_accommodation': True,
            'is_enabled': form._enabled,
            'price': 0,
            'is_billable': False,
            'places_limit': 0,
            'placeholder': 'Title of the "None" option',
            'id': no_acc_choice_id
        })

        section = RegistrationFormSection(registration_form=self.regform, title=sanitize_user_input(form._title),
                                          description=sanitize_user_input(form._description, html=True))
        self.print_info('%[green!]Section/Accommodation%[reset] - %[cyan]{}'.format(section.title))
        field = self.accommodation_field = RegistrationFormField(registration_form=self.regform, title=section.title,
                                                                 input_type='accommodation')
        field.data = data
        field.versioned_data = versioned_data
        section.children.append(field)

    def _migrate_reason_section(self, form):
        if not form._enabled and not any(x._reasonParticipation for x in self.conf._registrants.itervalues()):
            return
        section = RegistrationFormSection(registration_form=self.regform, title=sanitize_user_input(form._title),
                                          description=sanitize_user_input(form._description, html=True))
        self.print_info('%[green!]Section/Reason%[reset] - %[cyan]{}'.format(section.title))
        field = self.reason_field = RegistrationFormField(registration_form=self.regform, title='Reason',
                                                          input_type='textarea')
        field.data, field.versioned_data = field.field_impl.process_field_data({'number_of_rows': 4})
        section.children.append(field)

    def _migrate_further_info_section(self, form):
        if not form._content or not form._enabled:
            return
        section = RegistrationFormSection(registration_form=self.regform, title=sanitize_user_input(form._title))
        self.print_info('%[green!]Section/Info%[reset] - %[cyan]{}'.format(section.title))
        text = RegistrationFormText(registration_form=self.regform, title='Information',
                                    description=sanitize_user_input(form._content, html=True))
        section.children.append(text)

    def _migrate_personal_data_section(self, form):
        pd_type_map = {
            'email': PersonalDataType.email,
            'firstName': PersonalDataType.first_name,
            'surname': PersonalDataType.last_name,
            'institution': PersonalDataType.affiliation,
            'title': PersonalDataType.title,
            'address': PersonalDataType.address,
            'phone': PersonalDataType.phone,
            'country': PersonalDataType.country,
            'position': PersonalDataType.position
        }
        section = RegistrationFormPersonalDataSection(registration_form=self.regform,
                                                      title=sanitize_user_input(form._title),
                                                      description=sanitize_user_input(form._description, html=True))
        self.print_info('%[green!]Section/Personal%[reset] - %[cyan]{}'.format(section.title))
        self.section_map[form] = section
        for f in getattr(form, '_sortedFields', []) or getattr(form, '_fields', []):
            old_pd_type = getattr(f, '_pdField', None)
            pd_type = pd_type_map.get(old_pd_type)
            field = self._migrate_field(f, pd_type)
            section.children.append(field)

    def _migrate_general_section(self, form):
        section = RegistrationFormSection(registration_form=self.regform, title=sanitize_user_input(form._title),
                                          description=sanitize_user_input(form._description, html=True),
                                          is_enabled=getattr(form, '_enabled', True))
        self.print_info('%[green!]Section%[reset] - %[cyan]{}'.format(section.title))
        self.section_map[form] = section
        for f in getattr(form, '_sortedFields', []) or getattr(form, '_fields', []):
            section.children.append(self._migrate_field(f))
        return section

    def _migrate_deleted_field(self, old_field):
        try:
            section = self.section_map[old_field._parent]
        except KeyError:
            section = self._migrate_general_section(old_field._parent)
            section.is_deleted = True
        field = self._migrate_field(old_field)
        field.is_deleted = True
        section.children.append(field)
        return field

    def _migrate_field(self, old_field, pd_type=None):
        if get_input_type_id(old_field._input) == 'label':
            text = RegistrationFormText(registration_form=self.regform, title=sanitize_user_input(old_field._caption),
                                        description=sanitize_user_input(getattr(old_field, '_description', '')))
            billable, price = self._convert_billable(old_field)
            if billable and price:
                self.regform.base_price += Decimal(price)
            self.print_info('%[green]Text%[reset] - %[cyan]{}'.format(text.title))
            return text
        field_cls = RegistrationFormPersonalDataField if pd_type is not None else RegistrationFormField
        pd_required = pd_type is not None and pd_type.is_required
        is_required = bool(old_field._mandatory or pd_required)
        is_enabled = bool(not getattr(old_field, '_disabled', False) or pd_required)
        field = field_cls(registration_form=self.regform, personal_data_type=pd_type, is_required=is_required,
                          is_enabled=is_enabled, title=sanitize_user_input(old_field._caption),
                          description=sanitize_user_input(getattr(old_field, '_description', '')))
        self._migrate_field_input(field, old_field, pd_type)
        self.print_info('%[green]Field/{}%[reset] - %[cyan]{}'.format(field.input_type, field.title))
        self.field_map[old_field] = field
        return field

    def _migrate_field_input(self, field, old_field, pd_type):
        field_data = {}
        field_billable = False
        field_places_limit = False
        inp = old_field._input
        old_type = get_input_type_id(inp)
        if pd_type == PersonalDataType.email:
            input_type = 'email'
        elif old_type in {'text', 'country', 'file'}:
            input_type = old_type
        elif old_type == 'textarea':
            input_type = 'textarea'
            field_data['number_of_rows'] = _extract_int(getattr(inp, '_numberOfRows', None) or 3)
            field_data['number_of_columns'] = _extract_int(getattr(inp, '_numberOfColumns', None) or 60)
        elif old_type == 'number':
            input_type = 'number'
            field_billable = True
            field_data['min_value'] = int(getattr(inp, '_minValue', 0))
        elif old_type == 'radio':
            input_type = 'single_choice'
            field_data['item_type'] = getattr(inp, '_inputType', 'dropdown')
            field_data['with_extra_slots'] = False
            field_data['default_item'] = None
            field_data['choices'] = []
            items = inp._items.itervalues() if hasattr(inp._items, 'itervalues') else inp._items
            for item in items:
                uuid = unicode(uuid4())
                billable, price = self._convert_billable(item)
                field_data['choices'].append({
                    'price': price,
                    'is_billable': billable,
                    'places_limit': int(getattr(item, '_placesLimit', 0)),
                    'is_enabled': bool(getattr(item, '_enabled', True)),
                    'caption': sanitize_user_input(item._caption),
                    'id': uuid
                })
                if item._caption == getattr(inp, '_defaultItem', None):
                    field_data['default_item'] = uuid
        elif old_type == 'checkbox':
            input_type = 'checkbox'
            field_billable = True
            field_places_limit = True
        elif old_type == 'yes/no':
            input_type = 'bool'
            field_billable = True
            field_places_limit = True
        elif old_type == 'date':
            input_type = 'date'
            field_data['date_format'] = inp.dateFormat
        elif old_type == 'telephone':
            input_type = 'phone'
        else:
            raise ValueError('Unexpected field type: ' + old_type)
        field.input_type = input_type
        if field_billable:
            field_data['is_billable'], field_data['price'] = self._convert_billable(old_field)
        if field_places_limit:
            field_data['places_limit'] = int(getattr(old_field, '_placesLimit', 0))
        field.data, field.versioned_data = field.field_impl.process_field_data(field_data)

    def _migrate_custom_statuses(self):
        statuses = getattr(self.old_regform, '_statuses', None)
        if not statuses:
            return
        section = RegistrationFormSection(registration_form=self.regform, is_manager_only=True, title='Custom Statuses',
                                          description='Custom registration statuses (only visible to managers)')
        for status in statuses.itervalues():
            self.status_map[status] = {'field': None, 'choices': {}}
            default = None
            choices = []
            for v in status._statusValues.itervalues():
                uuid = unicode(uuid4())
                if v is status._defaultValue:
                    default = uuid
                caption = sanitize_user_input(v._caption)
                self.status_map[status]['choices'][v] = {'uuid': uuid, 'caption': caption}
                choices.append({'price': 0, 'is_billable': False, 'places_limit': 0, 'is_enabled': True,
                                'caption': caption, 'id': uuid})
            data = {
                'item_type': 'dropdown',
                'with_extra_slots': False,
                'default_item': default,
                'choices': choices
            }
            field = RegistrationFormField(registration_form=self.regform, parent=section, input_type='single_choice',
                                          title=sanitize_user_input(status._caption))
            field.data, field.versioned_data = field.field_impl.process_field_data(data)
            self.status_map[status]['field'] = field

    def _convert_billable(self, item):
        try:
            price = float(str(item._price).replace(',', '.')) if getattr(item, '_price', 0) else 0
        except ValueError:
            self.print_warning('Setting invalid price %[red]{!r}%[reset] to %[green]0%[reset]'
                               .format(item._price))
            return False, 0
        return bool(getattr(item, '_billable', False)), price

    def _migrate_registrations(self):
        for old_reg in sorted(self.conf._registrants.itervalues(), key=attrgetter('_id')):
            registration = self._migrate_registration(old_reg)
            self.event_ns.misc_data['last_registrant_friendly_id'] = max(
                int(registration.friendly_id), self.event_ns.misc_data.get('last_registrant_friendly_id', 0))
            self.regform.registrations.append(registration)

    def _migrate_registration(self, old_reg):
        registration = Registration(first_name=convert_to_unicode(old_reg._firstName),
                                    last_name=convert_to_unicode(old_reg._surname),
                                    email=self._fix_email(old_reg._email),
                                    submitted_dt=getattr(old_reg, '_registrationDate', self.regform.start_dt),
                                    base_price=0, price_adjustment=0,
                                    checked_in=getattr(old_reg, '_checkedIn', False))
        # set `checked_in_dt` after initialization since `checked_in` sets it to current dt automatically
        registration.checked_in_dt = getattr(old_reg, '_checkInDate', None)
        # the next two columns break when testing things locally with an existing
        # db, but both can be safely commented out without causing any issues
        registration.friendly_id = int(old_reg._id)
        registration.ticket_uuid = getattr(old_reg, '_checkInUUID', None)
        self.print_info('%[yellow]Registration%[reset] - %[cyan]{}%[reset] [{}]'.format(
            registration.full_name, old_reg._id))
        self._migrate_registration_user(old_reg, registration)
        self._migrate_registration_fields(old_reg, registration)
        self._migrate_registration_accommodation(old_reg, registration)
        self._migrate_registration_social_events(old_reg, registration)
        self._migrate_registration_reason(old_reg, registration)
        self._migrate_registration_sessions(old_reg, registration)
        self._migrate_registration_statuses(old_reg, registration)
        # adjust price if necessary
        old_price = Decimal(str(getattr(old_reg, '_total', 0))).max(0)  # negative prices are garbage
        calc_price = registration.price
        registration.price_adjustment = old_price - calc_price
        if registration.price_adjustment:
            self.print_warning('Price mismatch: {} (calculated) != {} (saved). Setting adjustment of {}'
                               .format(calc_price, old_price, registration.price_adjustment))
            assert registration.price == old_price
        # payment transaction
        currency = self._migrate_payment_transaction(old_reg, registration)
        # if no currency (or transaction) was found, use the default
        registration.currency = currency or self.event_ns.misc_data['payment_currency']
        # set the registration state
        if (not registration.price or
                (registration.transaction and registration.transaction.status == TransactionStatus.successful)):
            registration.state = RegistrationState.complete
        else:
            registration.state = RegistrationState.unpaid
        # create the legacy mapping
        if hasattr(old_reg, '_randomId'):
            registration.legacy_mapping = LegacyRegistrationMapping(
                event_id=self.event.id,
                legacy_registrant_id=int(old_reg._id),
                legacy_registrant_key=convert_to_unicode(old_reg._randomId)
            )
        return registration

    def _fix_email(self, email):
        email = convert_to_unicode(email).lower()
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
        if n != 1:
            self.print_warning('Duplicate email %[yellow]{}@{}%[reset]; using %[green]{}%[reset] instead'
                               .format(user, host, email))
        self.emails.add(email)
        return email

    def _migrate_registration_user(self, old_reg, registration):
        user = self.global_ns.users_by_email.get(registration.email)
        if user is not None:
            if user in self.users:
                self.print_warning('User {} is already associated with a registration; not associating them '
                                   'with {}'.format(user, registration))
                return
            self.users.add(user)
            registration.user = user
        if not self.past_event and old_reg._avatar:
            user = self.global_ns.avatar_merged_user.get(old_reg._avatar.id)
            if not user:
                return
            if not registration.user:
                self.print_warning('No email match; discarding association between {} and {}'
                                   .format(user, registration))
            elif registration.user != user:
                self.print_warning('Email matches another user; associating {} with {} instead of {}'
                                   .format(registration, registration.user, old_reg._avatar))

    def _migrate_registration_statuses(self, old_reg, registration):
        for old_status in getattr(old_reg, '_statuses', {}).itervalues():
            try:
                info = self.status_map[old_status._status]
            except KeyError:
                if old_status._value:
                    val = convert_to_unicode(old_status._value._caption) if old_status._value else None
                    self.print_warning('Skipping deleted status %[red!]{}%[reset] ({})'
                                       .format(convert_to_unicode(old_status._status._caption), val))
                continue
            field = info['field']
            status_info = info['choices'][old_status._value] if old_status._value else None
            data = {status_info['uuid']: 1} if status_info is not None else None
            registration.data.append(RegistrationData(field_data=field.current_data, data=data))
            if not self.quiet and status_info:
                self.print_info('%[red]STATUS%[reset] %[yellow!]{}%[reset] %[cyan]{}'
                                .format(field.title, status_info['caption']))

    def _migrate_registration_sessions(self, old_reg, registration):
        if not old_reg._sessions:
            return
        elif self.multi_session_field:
            self._migrate_registration_sessions_multi(old_reg, registration)
        elif self.specific_session_fields:
            self._migrate_registration_sessions_specific(old_reg, registration)
        else:
            raise RuntimeError('{} has sessions but the new form has no session fields'.format(old_reg))

    def _migrate_registration_sessions_multi(self, old_reg, registration):
        old_sessions = old_reg._sessions
        choice_map, data_version = self._get_session_objects(old_sessions)
        choices = {choice_map[old_sess._regSession]: 1 for old_sess in old_sessions}
        registration.data.append(RegistrationData(field_data=data_version, data=choices))
        if not self.quiet:
            self.print_info('%[blue!]SESSIONS%[reset] %[cyan!]{}'
                            .format(', '.join(sanitize_user_input(old_sess._regSession._session.title)
                                              for old_sess in old_sessions)))

    def _migrate_registration_sessions_specific(self, old_reg, registration):
        old_sessions = old_reg._sessions
        choice_map, data_versions = self._get_session_objects(old_sessions[:2])
        for i, old_sess in enumerate(old_sessions[:2]):
            uuid = choice_map[old_sess._regSession]
            registration.data.append(RegistrationData(field_data=data_versions[i], data={uuid: 1}))
            if not self.quiet:
                self.print_info('%[blue!]SESSION/{}%[reset] %[cyan!]{}'
                                .format(i + 1, sanitize_user_input(old_sess._regSession._session.title)))

    def _get_session_objects(self, old_sessions):
        # everything exists in the current version
        if all(old_sess._regSession in self.session_choice_map for old_sess in old_sessions):
            if self.specific_session_fields:
                return self.session_choice_map, (self.specific_session_fields[0].current_data,
                                                 self.specific_session_fields[1].current_data)
            else:
                return self.session_choice_map, self.multi_session_field.current_data

        if self.session_extra_choice_versions is None:
            # create one version that covers all choices not available in the current version
            self.print_info('%[magenta!]{}'.format('Creating version for missing sessions'))
            self.session_extra_choice_map = dict(self.session_choice_map)
            choices = list(self.session_choices)
            done = set(self.session_choice_map.viewkeys())
            captions = {}
            for old_reg in self.conf._registrants.itervalues():
                for old_sess in old_reg._sessions:
                    old_reg_sess = old_sess._regSession
                    if old_reg_sess in done:
                        continue
                    uuid = unicode(uuid4())
                    data = {'id': uuid, 'price': 0, 'is_billable': False, 'is_enabled': True,
                            'caption': sanitize_user_input(old_reg_sess._session.title)}
                    if self.multi_session_field:
                        # we don't create separate versions based on the prices since luckily there are
                        # no billable sessions!  and in any case, those would be handled fine by the
                        # registration-level `price_adjustment`
                        data['is_billable'], data['price'] = self._convert_billable(old_reg_sess)
                    choices.append(data)
                    captions[uuid] = data['caption']
                    self.session_extra_choice_map[old_reg_sess] = uuid
                    done.add(old_reg_sess)
            if self.specific_session_fields:
                versioned_data = [None] * 2
                for i in xrange(2):
                    self.specific_session_fields[i].data['captions'].update(captions)
                    versioned_data[i] = deepcopy(self.specific_session_fields[i].current_data.versioned_data)
                    versioned_data[i]['choices'] = choices
                    flag_modified(self.specific_session_fields[i], 'data')
                self.session_extra_choice_versions = (
                    RegistrationFormFieldData(field=self.specific_session_fields[0], versioned_data=versioned_data[0]),
                    RegistrationFormFieldData(field=self.specific_session_fields[1], versioned_data=versioned_data[1])
                )
            else:
                self.multi_session_field.data['captions'].update(captions)
                flag_modified(self.multi_session_field, 'data')
                versioned_data = deepcopy(self.multi_session_field.current_data.versioned_data)
                versioned_data['choices'] = choices
                self.session_extra_choice_versions = (
                    RegistrationFormFieldData(field=self.multi_session_field, versioned_data=versioned_data),
                )

        if self.specific_session_fields:
            return self.session_extra_choice_map, self.session_extra_choice_versions
        else:
            return self.session_extra_choice_map, self.session_extra_choice_versions[0]

    def _migrate_registration_reason(self, old_reg, registration):
        if not old_reg._reasonParticipation:
            return
        reason = convert_to_unicode(old_reg._reasonParticipation).strip()
        if not reason:
            return
        if not self.quiet:
            self.print_info('%[blue!]REASON%[reset] %[yellow!]{}%[reset] %[cyan!]{}'
                            .format(self.reason_field.title, reason))
        registration.data.append(RegistrationData(field_data=self.reason_field.current_data,
                                                  data=reason))

    def _migrate_registration_social_events(self, old_reg, registration):
        if not old_reg._socialEvents:
            return
        field = self.social_events_field
        old_events = old_reg._socialEvents
        simple = True
        key = set()
        for se in old_events:
            billable, price = self._convert_billable(se)
            price_per_place = getattr(se, '_pricePerPlace', False)
            if self.social_events_info_map.get(se._socialEventItem) != (billable, price, price_per_place):
                simple = False
            key.add((se._socialEventItem, billable, price, price_per_place))
        key = frozenset(key)
        if simple:
            # we can use the current data version
            data = {self.social_events_choice_map[se._socialEventItem]: int(se._noPlaces)
                    for se in old_events}
            registration.data.append(RegistrationData(field_data=field.current_data, data=data))
        elif key in self.social_events_versions:
            # we can reuse a custom version
            info = self.social_events_versions[key]
            data = {info['mapping'][se._socialEventItem]: int(se._noPlaces) for se in old_events}
            registration.data.append(RegistrationData(field_data=info['data_version'], data=data))
        else:
            # we have to use a custom version
            data = {}
            mapping = {}
            data_version = RegistrationFormFieldData(field=field)
            data_version.versioned_data = deepcopy(field.current_data.versioned_data)
            for se in old_events:
                uuid = unicode(uuid4())
                assert uuid not in field.data['captions']
                field.data['captions'][uuid] = sanitize_user_input(se._socialEventItem._caption)
                billable, price = self._convert_billable(se)
                data_version.versioned_data['choices'].append({
                    'id': uuid,
                    'extra_slots_pay': bool(getattr(se, '_pricePerPlace', False)),
                    'max_extra_slots': int(getattr(se._socialEventItem, '_maxPlacePerRegistrant', 0)),
                    'price': price,
                    'is_billable': billable,
                    'is_enabled': not getattr(se._socialEventItem, '_cancelled', False),
                    'places_limit': int(getattr(se._socialEventItem, '_placesLimit', 0))
                })
                mapping[se._socialEventItem] = uuid
                data[uuid] = int(se._noPlaces)
            self.social_events_versions[key] = {'data_version': data_version, 'mapping': mapping}
            registration.data.append(RegistrationData(field_data=data_version, data=data))

    def _migrate_registration_accommodation(self, old_reg, registration):
        old_ac = old_reg._accommodation
        ac_type = old_ac._accommodationType
        if ac_type is None:
            return
        field = self.accommodation_field
        billable, price = self._convert_billable(old_ac)
        data = {'arrival_date': old_ac._arrivalDate.date().strftime('%Y-%m-%d'),
                'departure_date': old_ac._departureDate.date().strftime('%Y-%m-%d')}
        if not self.quiet:
            self.print_info('%[blue!]ACCOMODATION%[reset] %[cyan!]{} [{} - {}]%[reset] %[red!]{}'
                            .format(sanitize_user_input(ac_type._caption), data['arrival_date'], data['departure_date'],
                                    '{:.02f}'.format(price) if billable and price else ''))
        uuid = self.accommodation_choice_map.get(ac_type)
        if uuid is not None:
            data['choice'] = uuid
            version_with_item = None
            for version in [field.current_data] + field.data_versions:
                choice = next((x for x in version.versioned_data['choices'] if x['id'] == uuid), None)
                if choice is None:
                    continue
                version_with_item = version
                if choice['is_billable'] == billable and choice['price'] == price:
                    data_version = version
                    break
            else:
                assert version_with_item is not None
                data_version = RegistrationFormFieldData(field=field)
                data_version.versioned_data = deepcopy(version_with_item.versioned_data)
                choice = next((x for x in data_version.versioned_data['choices'] if x['id'] == uuid), None)
                choice['is_billable'] = billable
                choice['price'] = price
        else:
            uuid = unicode(uuid4())
            data['choice'] = uuid
            data_version = RegistrationFormFieldData(field=field)
            data_version.versioned_data = deepcopy(field.current_data.versioned_data)
            field.data['captions'][uuid] = sanitize_user_input(ac_type._caption)
            data_version.versioned_data['choices'].append({
                'price': price,
                'is_billable': billable,
                'places_limit': int(getattr(ac_type, '_placesLimit', 0)),
                'is_enabled': not getattr(ac_type, '_cancelled', False),
                'caption': sanitize_user_input(ac_type._caption),
                'id': uuid
            })
        registration.data.append(RegistrationData(field_data=data_version, data=data))

    def _migrate_registration_fields(self, old_reg, registration):
        for mig in old_reg._miscellaneous.itervalues():
            for item_id, item in mig._responseItems.iteritems():
                if get_input_type_id(item._generalField._input) == 'label':
                    billable, price = self._convert_billable(item)
                    if billable and price:
                        registration.base_price += Decimal(price)
                        if not self.quiet:
                            self.print_info('%[blue!]STATIC%[reset] %[cyan!]{}%[reset] %[red!]{}'.format(
                                sanitize_user_input(item._generalField._caption),
                                '{:.02f}'.format(price) if billable and price else ''))
                elif item._generalField._id != item_id:
                    self.print_warning('Skipping invalid data (field id mismatch) for obsolete version of "{}" '
                                       '(registrant {})'
                                       .format(sanitize_user_input(item._generalField._caption), old_reg._id))
                else:
                    self._migrate_registration_field(item, registration)

    def _migrate_registration_field(self, old_item, registration):
        try:
            field = self.field_map[old_item._generalField]
        except KeyError:
            field = self._migrate_deleted_field(old_item._generalField)
        data_version = field.current_data
        billable, price = self._convert_billable(old_item)
        if not self.quiet:
            self.print_info('%[yellow!]{}%[reset] %[cyan!]{}%[reset] %[red!]{}'
                            .format(sanitize_user_input(old_item._generalField._caption),
                                    sanitize_user_input(str(old_item._value)),
                                    '{:.02f}'.format(price) if billable and price else ''))
        attrs = {}
        if field.input_type in {'text', 'textarea', 'email'}:
            if isinstance(old_item._value, basestring):
                attrs['data'] = convert_to_unicode(old_item._value)
            else:
                self.print_warning("Non-string '%[red]{!r}%[reset]' in {} field"
                                   .format(old_item._value, field.input_type))
                attrs['data'] = unicode(old_item._value)
        elif field.input_type == 'number':
            if not isinstance(old_item._value, (int, float)) and not old_item._value:
                return
            try:
                attrs['data'] = float(old_item._value)
            except ValueError:
                self.print_warning("Garbage number '%[red]{0}%[reset]' in number field"
                                   .format(convert_to_unicode(old_item._value)))
            else:
                if attrs['data'] == int(attrs['data']):
                    # if we store a float we keep an ugly '.0'
                    attrs['data'] = int(attrs['data'])
            data_version = self._ensure_version_price(field, billable, price) or data_version
        elif field.input_type == 'phone':
            attrs['data'] = normalize_phone_number(convert_to_unicode(old_item._value))
        elif field.input_type == 'date':
            if old_item._value:
                dt = (datetime.strptime(old_item._value, field.data['date_format'])
                      if isinstance(old_item._value, basestring)
                      else old_item._value)
                attrs['data'] = dt.isoformat()
        elif field.input_type in {'bool', 'checkbox'}:
            attrs['data'] = old_item._value == 'yes'
            data_version = self._ensure_version_price(field, billable, price) or data_version
        elif field.input_type == 'country':
            attrs['data'] = old_item._value
        elif field.input_type == 'file':
            if not old_item._value:
                return
            local_file = old_item._value
            content_type = mimetypes.guess_type(local_file.fileName)[0] or 'application/octet-stream'
            storage_backend, storage_path, size, md5 = self._get_local_file_info(local_file)
            filename = secure_filename(local_file.fileName, 'attachment')
            if storage_path is None:
                self.print_error('%[red!]File not found on disk; skipping it [{}]'
                                 .format(local_file.id))
                return
            attrs['filename'] = filename
            attrs['content_type'] = content_type
            attrs['storage_backend'] = storage_backend
            attrs['storage_file_id'] = storage_path
            attrs['size'] = size
            attrs['md5'] = md5

        elif field.input_type == 'single_choice':
            try:
                value = sanitize_user_input(old_item._value)
            except RuntimeError:
                self.print_warning("Garbage caption '%[red]{!r}%[reset]' in choice field"
                                   .format(old_item._value))
                return
            rv = self._migrate_registration_choice_field(field, value, price, billable)
            if rv is None:
                return
            attrs['data'] = rv['data']
            data_version = rv.get('data_version', data_version)
        else:
            raise ValueError('Unexpected field type: ' + field.input_type)
        registration.data.append(RegistrationData(field_data=data_version, **attrs))

    def _ensure_version_price(self, field, billable, price):
        if field.versioned_data['is_billable'] == billable and field.versioned_data['price'] == price:
            return None
        try:
            return self.price_adjusted_versions[(field, billable, price)]
        except KeyError:
            data_version = RegistrationFormFieldData(field=field)
            data_version.versioned_data = deepcopy(field.current_data.versioned_data)
            data_version.versioned_data['is_billable'] = billable
            data_version.versioned_data['price'] = price
            self.price_adjusted_versions[(field, billable, price)] = data_version
            return data_version

    def _migrate_registration_choice_field(self, field, selected, price, billable):
        rv = {}
        uuid = next((id_ for id_, caption in field.data['captions'].iteritems() if caption == selected), None)
        if uuid is not None:
            rv['data'] = {uuid: 1}
            version_with_item = None
            for data_version in [field.current_data] + field.data_versions:
                choice = next((x for x in data_version.versioned_data['choices'] if x['id'] == uuid), None)
                if choice is None:
                    continue
                version_with_item = data_version
                if choice['is_billable'] == billable and choice['price'] == price:
                    rv['data_version'] = data_version
                    break
            else:
                assert version_with_item is not None
                rv['data_version'] = data_version = RegistrationFormFieldData(field=field)
                data_version.versioned_data = deepcopy(version_with_item.versioned_data)
                choice = next((x for x in data_version.versioned_data['choices'] if x['id'] == uuid), None)
                choice['is_billable'] = billable
                choice['price'] = price
        elif not selected:
            return
        else:
            uuid = unicode(uuid4())
            rv['data'] = {uuid: 1}
            rv['data_version'] = data_version = RegistrationFormFieldData(field=field)
            data_version.versioned_data = deepcopy(field.current_data.versioned_data)
            field.data['captions'][uuid] = selected
            data_version.versioned_data['choices'].append({
                'price': price,
                'is_billable': billable,
                'places_limit': 0,
                'is_enabled': True,
                'caption': selected,
                'id': uuid
            })
        return rv

    def _migrate_payment_transaction(self, registrant, registration):
        transaction = getattr(registrant, '_transactionInfo', None)
        if not transaction:
            return
        try:
            data = self._get_transaction_data(transaction, self.event)
        except ValueError, e:
            self.print_error("Error processing transaction data of '{}': {}".format(registrant._id, e))
            return

        if data['provider'] == '_manual' and data['amount'] == 0.0:
            self.print_warning("Skipping {0[provider]} transaction with zero amount (reg. {1})".format(
                data, registrant._id))
            return

        elif data['amount'] < 0.0:
            self.print_warning("Skipping {0[provider]} transaction with negative amount (reg. {1}): "
                               "'{0[amount]} {0[currency]}".format(data, registrant._id))
            return

        registration.transaction = PaymentTransaction(status=TransactionStatus.successful, **data)
        self.print_success(unicode(registration.transaction))
        return data['currency']

    def _get_transaction_data(self, ti, event):
        mapping = {
            'TransactionPayLaterMod': _get_pay_later_data,
            'TransactionCERNYellowPay': _get_cern_yellow_pay_data,
            'TransactionPayPal': _get_paypal_data,
            'TransactionWorldPay': _get_worldpay_data,
            'TransactionSaferPay': _get_saferpay_data,
        }

        try:
            method = mapping[ti.__class__.__name__]
        except KeyError:
            raise ValueError('Unknown transaction type: {}'.format(ti.__class__.__name__))

        return method(ti._Data)

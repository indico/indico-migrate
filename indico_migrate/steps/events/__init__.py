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

from pytz import utc as utc_tz

from indico.modules.events.models.persons import EventPerson
from indico.modules.users.models.users import UserTitle

from indico_migrate.importer import Importer
from indico_migrate.steps.events.importer import EventImporter
from indico_migrate.util import convert_to_unicode, strict_sanitize_email


__all__ = ('EventImporter', 'EventMigrationStep')


USER_TITLE_MAP = {unicode(x.title): x for x in UserTitle}

PERSON_INFO_MAP = {
    '_address': 'address',
    '_affiliation': 'affiliation',
    '_firstName': 'first_name',
    '_surName': 'last_name',
    '_phone': 'phone'
}

AVATAR_PERSON_INFO_MAP = {
    'address': lambda x: x.address[0],
    'affiliation': lambda x: x.organisation[0],
    'first_name': lambda x: x.name,
    'last_name': lambda x: x.surName,
    'phone': lambda x: x.telephone[0]
}


class EventMigrationStep(Importer):
    USER_TITLE_MAP = {unicode(x.title): x for x in UserTitle}
    step_id = '?'

    def __init__(self, *args, **kwargs):
        super(EventMigrationStep, self).__init__(*args, **kwargs)
        self.system_user = kwargs.pop('system_user')
        self.context = None

    def bind(self, context):
        """Bind importer to a given EventContext"""
        self.context = context

    @property
    def conf(self):
        """Get legacy Conference object"""
        return self.context.conf if self.context else None

    @property
    def event(self):
        """Get new SQLAlchemy Event object"""
        return self.context.event if self.context else None

    @property
    def is_legacy_event(self):
        return self.context.is_legacy

    @property
    def event_ns(self):
        return self.context.event_ns

    def run(self):
        self.migrate()

    @property
    def log_prefix(self):
        if self.conf:
            return '%[reset]%[cyan]{:>6}%[reset]  %[grey!]{:11}%[reset]'.format(self.conf.id, self.step_id)
        else:
            return ''

    def migrate(self):
        raise NotImplementedError

    def setup(self):
        pass

    def teardown(self):
        pass

    def user_from_legacy(self, legacy_user, system_user=False):
        user = self.convert_principal(legacy_user)
        if user:
            return user
        self.print_error('%[red!]Invalid legacy user: {}'.format(legacy_user))
        return self.system_user if system_user else None

    def _naive_to_aware(self, dt, utc=True):
        """Convert a naive date to a TZ-aware one, using the event's TZ."""
        dt_aware = self.event.tzinfo.localize(dt) if dt.tzinfo is None else dt
        return dt_aware.astimezone(utc_tz) if utc else dt_aware

    def _get_person_data(self, old_person):
        if old_person.__class__.__name__ == 'Avatar':
            data = {new_attr: convert_to_unicode(func(old_person))
                    for new_attr, func in AVATAR_PERSON_INFO_MAP.viewitems()}
        else:
            data = {new_attr: convert_to_unicode(getattr(old_person, old_attr, ''))
                    for old_attr, new_attr in PERSON_INFO_MAP.iteritems()}
        data['_title'] = USER_TITLE_MAP.get(getattr(old_person, '_title', ''), UserTitle.none)
        return data

    def event_person_from_legacy(self, old_person, skip_empty_email=False, skip_empty_names=False):
        """Translate an old participation-like (or avatar) object to an EventPerson."""
        data = self._get_person_data(old_person)
        if skip_empty_names and not data['first_name'] and not data['last_name']:
            self.print_warning('%[yellow!]Skipping nameless event person', always=False)
            return None
        # retrieve e-mail in both Avatar and Participation objects
        email = strict_sanitize_email(getattr(old_person, '_email', None) or getattr(old_person, 'email', None))
        if email:
            person = (self.event_ns.event_persons_by_email.get(email) or
                      self.event_ns.event_persons_by_user.get(self.global_ns.users_by_email.get(email)))
        elif skip_empty_email:
            return None
        else:
            person = self.event_ns.event_persons_by_data.get((data['first_name'], data['last_name'],
                                                              data['affiliation']))
        if not person:
            user = self.global_ns.users_by_email.get(email)
            person = EventPerson(event=self.event, user=user, email=email, **data)
            self.add_event_person(person)
        return person

    def add_event_person(self, person):
        if person.email:
            self.event_ns.event_persons_by_email[person.email] = person
        if person.user:
            self.event_ns.event_persons_by_user[person.user] = person
        if not person.email and not person.user:
            self.event_ns.event_persons_by_data[person.first_name, person.last_name, person.affiliation] = person

    def get_event_person_by_email(self, email):
        if not email:
            return None
        return (self.event_ns.event_persons_by_email.get(email) or
                self.event_ns.event_persons_by_user.get(self.global_ns.users_by_email.get(email)))

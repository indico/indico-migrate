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
from indico.util.console import cformat

from indico_migrate.cli import Importer
from indico_migrate.steps.events.importer import EventImporter
from indico_migrate.util import convert_to_unicode, strict_sanitize_email


__all__ = ('EventImporter', 'EventMigrationStep')


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
    def prefix(self):
        if self.conf:
            return cformat('%{cyan}{:<12}%{reset} %{grey!}{:<10}%{reset}').format(
                '[{}]'.format(self.conf.id), '[{}]'.format(self.step_id))
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
        self.print_error(cformat('%{red!}Invalid legacy user: {}').format(legacy_user))
        return self.system_user if system_user else None

    def _naive_to_aware(self, dt, utc=True):
        """Convert a naive date to a TZ-aware one, using the event's TZ."""
        dt_aware = self.event.tzinfo.localize(dt) if dt.tzinfo is None else dt
        return dt_aware.astimezone(utc_tz) if utc else dt_aware

    def event_person_from_legacy(self, old_person, skip_empty_email=False, skip_empty_names=False):
        """Translate an old participation-like object to an EventPerson."""
        data = dict(first_name=convert_to_unicode(old_person._firstName),
                    last_name=convert_to_unicode(old_person._surName),
                    _title=self.USER_TITLE_MAP.get(getattr(old_person, '_title', ''), UserTitle.none),
                    affiliation=convert_to_unicode(getattr(old_person, '_affiliation', None) or
                                                   getattr(old_person, '_affilliation', None)),
                    address=convert_to_unicode(old_person._address),
                    phone=convert_to_unicode(getattr(old_person, '_telephone', None) or
                                             getattr(old_person, '_phone', None)))
        if skip_empty_names and not data['first_name'] and not data['last_name']:
            self.print_warning(cformat('%{yellow!}Skipping nameless event person'))
            return None
        email = strict_sanitize_email(old_person._email)
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
            person = EventPerson(event_new=self.event, user=user, email=email, **data)
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

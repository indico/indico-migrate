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

from indico.core.db.sqlalchemy.protection import ProtectionMode
from indico.modules.events.models.persons import EventPerson
from indico.modules.users.models.users import UserTitle
from indico.util.console import cformat
from indico_migrate.cli import Importer
from indico_migrate.steps.events.importer import EventImporter
from indico_migrate.util import strict_sanitize_email, convert_to_unicode

__all__ = ('EventImporter', 'EventMigrationStep')


class EventMigrationStep(Importer):

    def __init__(self, *args, **kwargs):
        super(EventMigrationStep, self).__init__(*args, **kwargs)
        self.janitor = kwargs.pop('janitor')
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
            return cformat('%{grey!}{:<12}%{reset}').format('[' + self.conf.id + ']')
        else:
            return ''

    def migrate(self):
        raise NotImplementedError

    def setup(self):
        pass

    def teardown(self):
        pass

    def _protection_from_ac(self, target, ac, acl_attr='acl', ac_attr='allowed', allow_public=False):
        """Convert AccessController data to ProtectionMixin style.

        This needs to run inside the context of `patch_default_group_provider`.

        :param target: The new object that uses ProtectionMixin
        :param ac: The old AccessController
        :param acl_attr: The attribute name for the acl of `target`
        :param ac_attr: The attribute name for the acl in `ac`
        :param allow_public: If the object allows `ProtectionMode.public`.
                             Otherwise, public is converted to inheriting.
        """
        if ac._accessProtection == -1:
            target.protection_mode = ProtectionMode.public if allow_public else ProtectionMode.inheriting
        elif ac._accessProtection == 0:
            target.protection_mode = ProtectionMode.inheriting
        elif ac._accessProtection == 1:
            target.protection_mode = ProtectionMode.protected
            acl = getattr(target, acl_attr)
            for principal in getattr(ac, ac_attr):
                principal = self.convert_principal(principal)
                assert principal is not None
                acl.add(principal)
        else:
            raise ValueError('Unexpected protection: {}'.format(ac._accessProtection))

    def _naive_to_aware(self, dt, utc=True):
        """Convert a naive date to a TZ-aware one, using the event's TZ."""
        dt_aware = self.event.tzinfo.localize(dt) if dt.tzinfo is None else dt
        return dt_aware.astimezone(utc_tz) if utc else dt_aware

    def event_person_from_legacy(self, old_person):
        """Translate an old participation-like object to an EventPerson."""
        data = dict(first_name=convert_to_unicode(old_person._firstName),
                    last_name=convert_to_unicode(old_person._surName),
                    _title=self.USER_TITLE_MAP.get(getattr(old_person, '_title', ''), UserTitle.none),
                    affiliation=convert_to_unicode(old_person._affilliation),
                    address=convert_to_unicode(old_person._address),
                    phone=convert_to_unicode(old_person._telephone))
        email = strict_sanitize_email(old_person._email)
        if email:
            person = (self.event_ns.event_persons_by_email.get(email) or
                      self.event_ns.event_persons_by_user.get(self.global_ns.users_by_email.get(email)))
        else:
            person = self.event_ns.event_persons_by_data.get((data['first_name'], data['last_name'],
                                                              data['affiliation']))
        if not person:
            user = self.global_ns.users_by_email.get(email)
            person = EventPerson(event_new=self.event, user=user, email=email, **data)
            if email:
                self.event_ns.event_persons_by_email[email] = person
            if user:
                self.event_ns.event_persons_by_user[user] = person
            if not email and not user:
                self.event_ns.event_persons_by_data[person.first_name, person.last_name, person.affiliation] = person
        return person

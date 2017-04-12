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

from indico.core.db.sqlalchemy.protection import ProtectionMode
from indico.util.console import cformat
from indico_migrate.cli import Importer
from indico_migrate.steps.events.importer import EventImporter

__all__ = ('EventImporter', 'EventMigrationStep')


class EventMigrationStep(Importer):

    def __init__(self, *args, **kwargs):
        super(EventMigrationStep, self).__init__(*args, **kwargs)
        self.conf = None
        self.janitor = kwargs.pop('janitor')

    def run(self, conf, event):
        self.conf = conf
        self.migrate(conf, event)

    @property
    def prefix(self):
        if self.conf:
            return cformat('%{grey!}{:<12}%{reset}').format('[' + self.conf.id + ']')
        else:
            return ''

    def migrate(self, conf, event):
        raise NotImplementedError

    def setup(self):
        pass

    def protection_from_ac(self, target, ac, acl_attr='acl', ac_attr='allowed', allow_public=False):
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

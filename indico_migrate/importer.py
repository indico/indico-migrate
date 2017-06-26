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

import time
from operator import itemgetter

from sqlalchemy.sql import func, select

from indico.core.db.sqlalchemy import db
from indico.core.db.sqlalchemy.protection import ProtectionMode
from indico.modules.groups import GroupProxy

from indico_migrate.logger import logger_proxy
from indico_migrate.util import convert_to_unicode


class Importer(object):
    step_name = ''

    #: Specify plugins that need to be loaded for the import (e.g. to access its .settings property)
    plugins = frozenset()

    print_info = logger_proxy('info')
    print_success = logger_proxy('success')
    print_warning = logger_proxy('warning')
    print_error = logger_proxy('error')
    print_log = logger_proxy('log')

    def __init__(self, logger, app, sqlalchemy_uri, zodb_root, verbose, dblog, default_group_provider, tz, **kwargs):
        self.sqlalchemy_uri = sqlalchemy_uri
        self.quiet = not verbose
        self.dblog = dblog
        self.zodb_root = zodb_root
        self.app = app
        self.tz = tz
        self.default_group_provider = default_group_provider
        self.logger = logger

        self.initialize_global_ns(Importer._global_ns)

    def initialize_global_ns(self, g):
        pass

    @property
    def log_prefix(self):
        return '%[cyan]{:<14}%[reset]'.format('[%[grey!]{}%[cyan]]'.format(self.step_name))

    @property
    def makac_info(self):
        return self.zodb_root['MaKaCInfo']['main']

    @property
    def global_ns(self):
        return Importer._global_ns

    def __repr__(self):
        return '<{}({})>'.format(type(self).__name__, self.sqlalchemy_uri)

    def flushing_iterator(self, iterable, n=5000):
        """Iterates over `iterable` and flushes the ZODB cache every `n` items.

        :param iterable: an iterable object
        :param n: number of items to flush after
        """
        conn = self.zodb_root._p_jar
        for i, item in enumerate(iterable, 1):
            yield item
            if i % n == 0:
                conn.sync()

    def convert_principal(self, old_principal):
        """Converts a legacy principal to PrincipalMixin style"""
        if old_principal.__class__.__name__ == 'Avatar':
            principal = self.global_ns.avatar_merged_user.get(old_principal.id)
            if not principal and 'email' in old_principal.__dict__:
                email = convert_to_unicode(old_principal.__dict__['email']).lower()
                principal = self.global_ns.users_by_primary_email.get(
                    email, self.global_ns.users_by_secondary_email.get(email))
                if principal is not None:
                    self.print_warning('Using {} for {} (matched via {})'.format(principal, old_principal, email))
            if not principal:
                self.print_error("User {} doesn't exist".format(old_principal.id))
            return principal
        elif old_principal.__class__.__name__ == 'Group':
            assert int(old_principal.id) in self.global_ns.all_groups
            return GroupProxy(int(old_principal.id))
        elif old_principal.__class__.__name__ in {'CERNGroup', 'LDAPGroup', 'NiceGroup'}:
            return GroupProxy(old_principal.id, self.default_group_provider)

    def convert_principal_list(self, opt):
        """Convert ACL principals to new objects"""
        return set(filter(None, (self.convert_principal(principal) for principal in opt._PluginOption__value)))

    def fix_sequences(self, schema=None, tables=None):
        for name, cls in sorted(db.Model._decl_class_registry.iteritems(), key=itemgetter(0)):
            table = getattr(cls, '__table__', None)
            if table is None:
                continue
            elif schema is not None and table.schema != schema:
                continue
            elif tables is not None and cls.__tablename__ not in tables:
                continue
            # Check if we have a single autoincrementing primary key
            candidates = [col for col in table.c if col.autoincrement and col.primary_key]
            if len(candidates) != 1 or not isinstance(candidates[0].type, db.Integer):
                continue
            serial_col = candidates[0]
            sequence_name = '{}.{}_{}_seq'.format(table.schema, cls.__tablename__, serial_col.name)

            query = select([func.setval(sequence_name, func.max(serial_col) + 1)], table)
            db.session.execute(query)
        db.session.commit()

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


class TopLevelMigrationStep(Importer):
    def run(self):
        start = time.time()
        self.pre_migrate()
        try:
            self.migrate()
        finally:
            self.post_migrate()
        self.print_log('%[cyan]{:.06f} seconds%[reset]\a'.format((time.time() - start)))

    def pre_migrate(self):
        pass

    def migrate(self):
        raise NotImplementedError

    def post_migrate(self):
        pass

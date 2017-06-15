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

from __future__ import print_function, unicode_literals

import sys
import time
from operator import itemgetter

import click
from IPython.core import ultratb
from sqlalchemy.sql import func, select

# inject_unicode_debug happens to access the Config object
from indico.util import string as indico_util_string
indico_util_string.inject_unicode_debug = lambda s, level=1: s


from indico.core.db.sqlalchemy import db
from indico.core.db.sqlalchemy.protection import ProtectionMode
from indico.modules.groups import GroupProxy

from indico_migrate import gui
from indico_migrate.logger import logger_proxy, StdoutLogger
from indico_migrate.migrate import migrate
from indico_migrate.namespaces import SharedNamespace
from indico_migrate.util import MigrationStateManager, UnbreakingDB, convert_to_unicode, get_storage

click.disable_unicode_literals_warning = True


def except_hook(exc_class, exception, tb):
    ftb = ultratb.FormattedTB(mode='Verbose', color_scheme='Linux', call_pdb=1, include_vars=False)
    return ftb(exc_class, exception, tb)


@click.command()
@click.argument('sqlalchemy-uri')
@click.argument('zodb-uri')
@click.option('--verbose', '-v', is_flag=True, default=False, help="Use verbose output")
@click.option('--dblog', '-L', is_flag=True, default=False, help="Enable db query logging")
@click.option('--ldap-provider-name', default='ldap', help="Provider name to use for existing LDAP identities")
@click.option('--default-group-provider', default='ldap', help="Name of the default group provider")
@click.option('--ignore-local-accounts', is_flag=True, default=False, help="Do not migrate existing local accounts")
@click.option('--system-user-id', type=int, help="The ID of the system user")
@click.option('--default-email', required=True, help="Fallback email in case of garbage")
@click.option('--archive-dir', required=True, multiple=True,
              help="The base path where resources are stored (ArchiveDir in indico.conf). When used multiple times, "
                   "the dirs are checked in order until a file is found.")
@click.option('--storage-backend', required=True,
              help="The name of the storage backend used for attachments.")
@click.option('--avoid-storage-check', is_flag=True,
              help="Avoid checking files in storage unless absolutely necessary due to encoding issues. This will "
                   "migrate all files with size=0.  When this option is specified, --archive-dir must be used exactly "
                   "once.")
@click.option('--symlink-backend', help="The name of the storage backend used for symlinks.")
@click.option('--symlink-target', help="If set, any files with a non-UTF8 path will be symlinked in this location and "
                                       "store the path to the symlink instead (relative to the archive dir). "
                                       "When this option is specified, --archive-dir must be used exactly once.")
@click.option('--rb-zodb-uri', required=False, help="ZODB URI for the room booking database")
@click.option('--photo-path', type=click.Path(exists=True, file_okay=False),
              help="path to the folder containing room photos")
@click.option('--reference-type', 'reference_types', multiple=True,
              help="Reference types ('report numbers'). Can be used multiple times to specify multiple reference types")
@click.option('--default-currency', required=True, help="currency unit to use by default")
@click.option('--debug', is_flag=True, default=False, help="Open debug shell if there is an error")
@click.option('--no-gui', is_flag=True, default=False, help="Don't run the GUI")
@click.option('--save-restore', type=click.File('w'), help="Save a restore point to the given file in case of failure")
@click.option('--restore-file', type=click.File('r'), help="Restore migration from a file (enables debug)")
def cli(sqlalchemy_uri, zodb_uri, rb_zodb_uri, verbose, dblog, debug, restore_file, no_gui, **kwargs):
    """
    This script migrates your database from ZODB/Indico 1.2 to PostgreSQL (2.0).

    You always need to specify both the SQLAlchemy connection URI and
    ZODB URI (both zeo:// and file:// work).
    """

    if restore_file:
        debug = True

    if debug:
        sys.excepthook = except_hook

    zodb_root = UnbreakingDB(get_storage(zodb_uri)).open().root()

    Importer._global_ns = SharedNamespace('global_ns', zodb_root, {
        'user_favorite_categories': 'setdict',
        'room_mapping': dict,
        'venue_mapping': dict,
        'legacy_event_ids': dict,
        'legacy_category_ids': dict,
        'wf_registry': dict,
        'used_short_urls': dict,
        'legacy_survey_mapping': dict,
        'ip_domains': dict,
        'avatar_merged_user': dict,
        'all_groups': dict,
        'users_by_primary_email': dict,
        'users_by_secondary_email': dict,
        'users_by_email': dict,
        'reference_types': dict,
    })

    # register the global namespace, so that it gets dumped to disk
    # in the event of a failure
    MigrationStateManager.register_ns(Importer._global_ns)

    if not no_gui:
        logger = gui.setup(not verbose)
    else:
        logger = StdoutLogger(not verbose)

    migrate(logger, zodb_root, rb_zodb_uri, sqlalchemy_uri, verbose=verbose, dblog=dblog, restore_file=restore_file,
            debug=debug, **kwargs)


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
        from indico_migrate.util import cformat2
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


def main():
    return cli()

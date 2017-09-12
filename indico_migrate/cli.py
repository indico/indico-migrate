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

import click
from IPython.core import ultratb

from indico_migrate import gui
from indico_migrate.logger import StdoutLogger
from indico_migrate.migrate import migrate
from indico_migrate.namespaces import SharedNamespace
from indico_migrate.util import MigrationStateManager, UnbreakingDB, get_storage

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
@click.option('--migrate-broken-events', is_flag=True, default=False,
              help="Migrate broken events that have no category and would usually be skipped. "
                   "They will be added to a new 'Lost & Found' top-level category which needs to be checked "
                   "(and possibly deleted) manually.")
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

    from indico_migrate.importer import Importer

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
        'lostandfound_category': lambda: None,
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


def main():
    return cli()

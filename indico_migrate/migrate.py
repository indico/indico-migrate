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

import os
import sys
import yaml

import pytz
from sqlalchemy.orm import configure_mappers

from indico.core.db.sqlalchemy import db
from indico.core.db.sqlalchemy.logging import apply_db_loggers
from indico.core.db.sqlalchemy.migration import migrate as alembic_migrate
from indico.core.db.sqlalchemy.util.models import import_all_models
from indico.core.plugins import plugin_engine
from indico.util.console import cformat
from indico.web.flask.wrappers import IndicoFlask
from indico_migrate.util import UnbreakingDB, get_storage, MigrationStateManager

# TODO: handle plugins


def _monkeypatch_config():
    """Make sure we're not accesing the indico.conf"""
    def _raise_method():
        raise RuntimeError("Config file shouldn't be accessed during migration!")

    from indico.core.config import Config
    Config.getInstance = staticmethod(_raise_method)


def migrate(zodb_root, zodb_rb_uri, sqlalchemy_uri, verbose=False, dblog=False, restore_file=None, **kwargs):
    from indico_migrate.steps.events import EventImporter
    from indico_migrate.steps.categories import CategoryImporter
    from indico_migrate.steps.global_post_events import GlobalPostEventsImporter
    from indico_migrate.steps.global_pre_events import GlobalPreEventsImporter
    from indico_migrate.steps.rooms_locations import RoomsLocationsImporter
    from indico_migrate.steps.room_bookings import RoomBookingsImporter
    from indico_migrate.steps.users_groups import UserImporter
    steps = (GlobalPreEventsImporter, UserImporter, RoomsLocationsImporter, CategoryImporter, EventImporter,
             RoomBookingsImporter, GlobalPostEventsImporter)

    app, tz = setup(zodb_root, sqlalchemy_uri)

    default_group_provider = kwargs.pop('default_group_provider')

    with app.app_context():
        if restore_file:
            data = yaml.load(restore_file)
            MigrationStateManager.load_restore_point(data)

        try:
            for step in steps:
                if MigrationStateManager.has_already_run(step):
                    continue
                if step in (RoomsLocationsImporter, RoomBookingsImporter):
                    if zodb_rb_uri:
                        zodb_rb_root = UnbreakingDB(get_storage(zodb_rb_uri)).open().root()
                        step(app, sqlalchemy_uri, zodb_root, verbose, dblog, default_group_provider, tz,
                             rb_root=zodb_rb_root, **kwargs).run()
                else:
                    step(app, sqlalchemy_uri, zodb_root, verbose, dblog, default_group_provider, tz, **kwargs).run()
                MigrationStateManager.register_step(step)
        except:
            if kwargs.get('debug'):
                print cformat('%{yellow}Saving restore point...'),
                MigrationStateManager.save_restore_point()
                print cformat('%{green!}DONE')
            raise


def db_has_data():
    """Check if there is already data in the DB"""
    models = ('Category', 'User', 'LocalGroup', 'NewsItem', 'IPNetworkGroup', 'LegacyCategoryMapping',
              'LegacyEventMapping', 'Event')
    for model_name in models:
        if getattr(db.m, model_name).query.has_rows():
            return True
    return False


def setup(zodb_root, sqlalchemy_uri, dblog=False):
    app = IndicoFlask('indico_migrate')
    app.config['PLUGINENGINE_NAMESPACE'] = 'indico.plugins'
    app.config['SQLALCHEMY_DATABASE_URI'] = sqlalchemy_uri
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = True
    _monkeypatch_config()

    plugin_engine.init_app(app)
    if not plugin_engine.load_plugins(app):
        print(cformat('%{red!}Could not load some plugins: {}%{reset}').format(
            ', '.join(plugin_engine.get_failed_plugins(app))))
        sys.exit(1)
    db.init_app(app)
    if dblog:
        app.debug = True
        apply_db_loggers(app)
    import_all_models()
    configure_mappers()
    alembic_migrate.init_app(app, db, os.path.join(app.root_path, 'migrations'))

    try:
        tz = pytz.timezone(getattr(zodb_root['MaKaCInfo']['main'], '_timezone', 'UTC'))
    except KeyError:
        tz = pytz.utc

    with app.app_context():
        if db_has_data():
            # Usually there's no good reason to migrate with data in the DB. However, during development one might
            # comment out some migration tasks and run the migration anyway.
            print(cformat('%{yellow!}*** WARNING'))
            print(cformat('%{yellow!}***%{reset} Your database is not empty, migration may fail or add duplicate '
                          'data!'))
            if raw_input(cformat('%{yellow!}***%{reset} To confirm this, enter %{yellow!}YES%{reset}: ')) != 'YES':
                print('Aborting')
                sys.exit(1)
    return app, tz

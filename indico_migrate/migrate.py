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

import logging.config
import os
import sys
from logging.handlers import SocketHandler

import pytz
import yaml
from flask.helpers import get_root_path
from sqlalchemy.orm import configure_mappers

from indico.core.db.sqlalchemy import db
from indico.core.db.sqlalchemy.logging import apply_db_loggers
from indico.core.db.sqlalchemy.migration import migrate as alembic_migrate
from indico.core.db.sqlalchemy.migration import prepare_db
from indico.core.db.sqlalchemy.util.management import get_all_tables
from indico.core.db.sqlalchemy.util.models import import_all_models
from indico.core.logger import Logger
from indico.core.plugins import plugin_engine
from indico.modules.oauth.models.applications import SystemAppType, OAuthApplication
from indico.util.console import cformat
from indico.web.flask.wrappers import IndicoFlask

from indico_migrate.paste import ask_to_paste, get_full_stack
from indico_migrate.util import MigrationStateManager, UnbreakingDB, get_storage


def _monkeypatch_config():
    """Make sure we're not accesing the indico.conf"""
    def _raise_method(self):
        raise RuntimeError("Config file shouldn't be accessed during migration!")

    from indico.core.config import IndicoConfig
    IndicoConfig.data = property(_raise_method)


def _zodb_powered_loader(_zodb_root):
    class _ZODBLoader(yaml.Loader):
        zodb_root = _zodb_root

    return _ZODBLoader


def migrate(logger, zodb_root, zodb_rb_uri, sqlalchemy_uri, verbose=False, dblog=False, restore_file=None, **kwargs):
    from indico_migrate.steps.badges_posters import GlobalBadgePosterImporter
    from indico_migrate.steps.event_series import EventSeriesImporter
    from indico_migrate.steps.events import EventImporter
    from indico_migrate.steps.categories import CategoryImporter
    from indico_migrate.steps.global_post_events import GlobalPostEventsImporter
    from indico_migrate.steps.global_pre_events import GlobalPreEventsImporter
    from indico_migrate.steps.rooms_locations import RoomsLocationsImporter
    from indico_migrate.steps.room_bookings import RoomBookingsImporter
    from indico_migrate.steps.users_groups import UserImporter
    steps = (GlobalPreEventsImporter, UserImporter, RoomsLocationsImporter, CategoryImporter, EventImporter,
             RoomBookingsImporter, GlobalPostEventsImporter, EventSeriesImporter, GlobalBadgePosterImporter)

    app, tz = setup(logger, zodb_root, sqlalchemy_uri, dblog=dblog, restore=(restore_file is not None))

    default_group_provider = kwargs.pop('default_group_provider')
    save_restore = kwargs.pop('save_restore')
    debug = kwargs.get('debug', False)

    with app.app_context():
        try:
            if restore_file:
                logger.print_info('loading restore file %[cyan!]{}'.format(restore_file.name), always=True)
                import time
                time.sleep(1)
                # preload some data, so that we don't have to
                # retrieve it from the DB later
                all_users = db.m.User.query.all()
                all_categories = db.m.Category.query.all()
                logger.print_info('{} users, {} categories preloaded'.format(len(all_users), len(all_categories)),
                                  always=True)
                data = yaml.load(restore_file, Loader=_zodb_powered_loader(zodb_root))
                MigrationStateManager.load_restore_point(data)

            for step in steps:
                if MigrationStateManager.has_already_run(step):
                    logger.print_info('Skipping previously-run step {}...'.format(step.__name__), always=True)
                    continue
                if step in (RoomsLocationsImporter, RoomBookingsImporter):
                    if zodb_rb_uri:
                        zodb_rb_root = UnbreakingDB(get_storage(zodb_rb_uri)).open().root()
                        step(logger, app, sqlalchemy_uri, zodb_root, verbose, dblog, default_group_provider, tz,
                             rb_root=zodb_rb_root, **kwargs).run()
                else:
                    step(logger, app, sqlalchemy_uri, zodb_root, verbose, dblog, default_group_provider, tz,
                         **kwargs).run()
                MigrationStateManager.register_step(step)
            logger.set_success()
            logger.shutdown()
        except Exception as exc:
            stack = get_full_stack() + '\n' + unicode(exc)
            logger.save_exception(stack)
            if save_restore:
                db.session.rollback()
                logger.print_warning('%[yellow]Saving restore point...'),
                MigrationStateManager.save_restore_point(save_restore)
                logger.print_warning('%[green!]Restore point saved.')
                logger.wait_for_input()

            logger.shutdown()

            if debug:
                raise

            print stack

            if not ask_to_paste(logger.buffer):
                raise
        finally:
            logger.save_to_disk()


def db_has_data():
    """Check if there is already data in the DB"""
    models = ('Category', 'User', 'LocalGroup', 'NewsItem', 'IPNetworkGroup', 'LegacyCategoryMapping',
              'LegacyEventMapping', 'Event', 'Contribution', 'TimetableEntry', 'Room', 'Reservation',
              'Session', 'Abstract')
    for model_name in models:
        if getattr(db.m, model_name).query.has_rows():
            return True
    return False


def setup(logger, zodb_root, sqlalchemy_uri, dblog=False, restore=False):
    app = IndicoFlask('indico_migrate')
    app.config['PLUGINENGINE_NAMESPACE'] = 'indico.plugins'
    app.config['SQLALCHEMY_DATABASE_URI'] = sqlalchemy_uri
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = True
    _monkeypatch_config()

    plugin_engine.init_app(app)
    if not plugin_engine.load_plugins(app):
        print(cformat('%[red!]Could not load some plugins: {}%[reset]').format(
            ', '.join(plugin_engine.get_failed_plugins(app))))
        sys.exit(1)
    db.init_app(app)
    if dblog:
        app.debug = True
        apply_db_loggers(app, force=True)
        db_logger = Logger.get('_db')
        db_logger.level = logging.DEBUG
        db_logger.propagate = False
        db_logger.addHandler(SocketHandler('127.0.0.1', 9020))

    # avoid "no handlers registered" warnings
    logging.root.addHandler(logging.NullHandler())

    import_all_models()
    configure_mappers()
    alembic_migrate.init_app(app, db, os.path.join(app.root_path, 'migrations'))

    try:
        tz = pytz.timezone(getattr(zodb_root['MaKaCInfo']['main'], '_timezone', 'UTC'))
    except KeyError:
        tz = pytz.utc

    with app.app_context():
        if not restore:
            all_tables = sum(get_all_tables(db).values(), [])
            if all_tables:
                if db_has_data():
                    logger.fatal_error('Your database is not empty!\n'
                                       'If you want to reset it, please drop and recreate it first.')
            else:
                # the DB is empty, prepare DB tables
                # prevent alembic from messing with the logging config
                tmp = logging.config.fileConfig
                logging.config.fileConfig = lambda fn: None
                prepare_db(empty=True, root_path=get_root_path('indico'), verbose=False)
                logging.config.fileConfig = tmp
                _create_oauth_apps()
    return app, tz


def _create_oauth_apps():
    for sat in SystemAppType:
        if sat != SystemAppType.none:
            db.session.add(OAuthApplication(system_app_type=sat, **sat.default_data))
    db.session.commit()

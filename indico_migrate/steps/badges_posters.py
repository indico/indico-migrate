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

from indico.core.db import db
from indico.core.db.sqlalchemy.util.management import DEFAULT_TEMPLATE_DATA
from indico.modules.categories import Category
from indico.modules.designer import TemplateType
from indico.modules.designer.models.templates import DesignerTemplate
from indico.modules.users import User

from indico_migrate.badges_posters import BadgeMigration, PosterMigration
from indico_migrate.importer import TopLevelMigrationStep
from indico_migrate.util import LocalFileImporterMixin


class GlobalBadgePosterImporter(LocalFileImporterMixin, TopLevelMigrationStep):
    step_name = 'badges'

    def __init__(self, *args, **kwargs):
        self._set_config_options(**kwargs)
        super(GlobalBadgePosterImporter, self).__init__(*args, **kwargs)

    def migrate(self):
        default_conference = getattr(self.zodb_root['MaKaCInfo']['main'], '_defaultConference', None)
        if not default_conference:
            self.print_warning('%[yellow!]Server has no default conference')
            return
        system_user = User.get_system_user()
        BadgeMigration(self, default_conference, None, system_user).run()
        PosterMigration(self, default_conference, None, system_user).run()
        dt = DesignerTemplate(category_id=0, title='Default ticket', type=TemplateType.badge,
                              data=DEFAULT_TEMPLATE_DATA, is_system_template=True)
        Category.get_root().default_ticket_template = dt
        db.session.commit()

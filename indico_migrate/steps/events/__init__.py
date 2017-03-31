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

from indico.util.console import cformat
from indico_migrate.cli import Importer
from indico_migrate.steps.events.importer import EventImporter

__all__ = ('EventImporter', 'EventMigrationStep')


class EventMigrationStep(Importer):

    def __init__(self, *args, **kwargs):
        super(EventMigrationStep, self).__init__(*args, **kwargs)
        self.conf = None

    def run(self, conf, event):
        self.conf = conf
        self.migrate(conf, event)

    @property
    def prefix(self):
        if self.conf:
            return cformat('%{cyan!}{:<12}%{reset}').format('[' + self.conf.id + ']')
        else:
            return ''

    def migrate(self, conf, event):
        raise NotImplementedError

    def setup(self):
        pass

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

from indico.core.db.sqlalchemy.descriptions import RenderMode
from indico.modules.events.notes.models.notes import EventNote

from indico_migrate.steps.events import EventMigrationStep
from indico_migrate.util import convert_to_unicode, get_archived_file


class EventNotesImporter(EventMigrationStep):
    step_id = 'notes'

    def __init__(self, *args, **kwargs):
        super(EventNotesImporter, self).__init__(*args, **kwargs)
        self.archive_dirs = kwargs.pop('archive_dir')

    def migrate(self):
        for obj, minutes, special_prot in self._iter_minutes():
            if special_prot:
                self.print_warning('%[yellow!]{} minutes have special permissions; skipping them'.format(obj))
                continue
            path = get_archived_file(minutes, self.archive_dirs)[1]
            if path is None:
                self.print_error('%[red!]{} minutes not found on disk; skipping them'.format(obj))
                continue
            with open(path, 'r') as f:
                data = convert_to_unicode(f.read()).strip()
            if not data:
                self.print_warning('%[yellow]{} minutes are empty; skipping them'.format(obj), always=False)
                continue
            note = EventNote(object=obj)
            note.create_revision(RenderMode.html, data, self.system_user)
            if not self.quiet:
                self.print_success('%[cyan]{}'.format(obj))

    def _has_special_protection(self, material, resource):
        material_ac = material._Material__ac
        resource_ac = resource._Resource__ac
        # both inherit
        if resource_ac._accessProtection == 0 and material_ac._accessProtection == 0:
            return False
        # resource is protected
        if resource_ac._accessProtection > 0:
            return True
        # material is protected and resource inherits
        if resource_ac._accessProtection == 0 and material_ac._accessProtection > 0:
            return True
        return False

    def _get_minutes(self, obj):
        material = obj.minutes
        if material is None:
            return None, None
        if material.file is None:
            return None, None
        return material.file, self._has_special_protection(material, material.file)

    def _iter_minutes(self):
        minutes, special = self._get_minutes(self.conf)
        if minutes:
            yield self.event, minutes, special
        for old_session, session in self.event_ns.legacy_session_map.iteritems():
            minutes, special = self._get_minutes(old_session)
            if minutes:
                yield session, minutes, special
        for old_contrib, contrib in self.event_ns.legacy_contribution_map.iteritems():
            minutes, special = self._get_minutes(old_contrib)
            if minutes:
                yield contrib, minutes, special
            for old_subcontrib in old_contrib._subConts:
                subcontrib = self.event_ns.legacy_subcontribution_map[old_subcontrib]
                minutes, special = self._get_minutes(old_subcontrib)
                if minutes:
                    yield subcontrib, minutes, special

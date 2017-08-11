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

import mimetypes
from itertools import chain

import pytz

from indico.modules.attachments import Attachment, AttachmentFolder
from indico.modules.attachments.models.attachments import AttachmentFile, AttachmentType
from indico.modules.attachments.models.legacy_mapping import LegacyAttachmentFolderMapping, LegacyAttachmentMapping
from indico.util.date_time import now_utc
from indico.util.fs import secure_filename

from indico_migrate.util import LocalFileImporterMixin, convert_to_unicode


class AttachmentMixin(LocalFileImporterMixin):
    def pre_migrate(self):
        super(AttachmentMixin, self).pre_migrate()
        self.setup()

    def post_migrate(self):
        super(AttachmentMixin, self).post_migrate()
        self.teardown()

    def setup(self):
        self._old_onupdate = Attachment.__table__.columns.modified_dt.onupdate
        Attachment.__table__.columns.modified_dt.onupdate = None

    def teardown(self):
        Attachment.__table__.columns.modified_dt.onupdate = self._old_onupdate

    def migrate_category_attachments(self, category, old_category):
        for material, resources in self._iter_attachments(old_category):
            folder = self._folder_from_material(material, category)
            if not self.quiet:
                self.print_success('%[cyan][{}]'.format(folder.title))
            for resource in resources:
                attachment = self._attachment_from_resource(folder, material, resource, old_category)
                if attachment is None:
                    continue
                if not self.quiet:
                    if attachment.type == AttachmentType.link:
                        self.print_success('- %[cyan]{}'.format(attachment.title))
                    else:
                        self.print_success('- %[cyan!]{}'.format(attachment.title))

    def migrate_event_attachments(self):
        for obj, material, resources, legacy_link_data in self._iter_event_materials():
            folder = self._folder_from_material(material, obj)
            LegacyAttachmentFolderMapping(material_id=material.id, folder=folder, **legacy_link_data)
            if not self.quiet:
                self.print_success('%[cyan][{}]%[reset] %[blue!]({})'.format(folder.title, folder.link_repr))
            for resource in resources:
                attachment = self._attachment_from_resource(folder, material, resource, self.conf)
                if attachment is None:
                    continue
                LegacyAttachmentMapping(material_id=material.id, resource_id=resource.id, attachment=attachment,
                                        **legacy_link_data)
                if not self.quiet:
                    if attachment.type == AttachmentType.link:
                        self.print_success('- %[cyan]{}'.format(attachment.title))
                    else:
                        self.print_success('- %[cyan!]{}'.format(attachment.title))

    def _iter_event_materials(self):
        for material, resources in self._iter_attachments(self.conf):
            yield self.event, material, resources, {'event': self.event}
        for old_session, session in self.event_ns.legacy_session_map.iteritems():
            for material, resources in self._iter_attachments(old_session):
                yield session, material, resources, {'event': self.event, 'session_id': old_session.id}
        for old_contrib, contrib in self.event_ns.legacy_contribution_map.iteritems():
            for material, resources in self._iter_attachments(old_contrib):
                yield contrib, material, resources, {'event': self.event, 'contribution_id': old_contrib.id}
            for old_subcontrib in old_contrib._subConts:
                subcontrib = self.event_ns.legacy_subcontribution_map[old_subcontrib]
                for material, resources in self._iter_attachments(old_subcontrib):
                    yield subcontrib, material, resources, {'event': self.event,
                                                            'contribution_id': old_contrib.id,
                                                            'subcontribution_id': old_subcontrib.id}

    def _folder_from_material(self, material, linked_object):
        folder = AttachmentFolder(title=convert_to_unicode(material.title).strip() or 'Material',
                                  description=convert_to_unicode(material.description),
                                  object=linked_object,
                                  is_always_visible=not getattr(material._Material__ac, '_hideFromUnauthorizedUsers',
                                                                False))
        self.protection_from_ac(folder, material._Material__ac)
        return folder

    def _attachment_from_resource(self, folder, material, resource, base_object=None):
        modified_dt = (getattr(material, '_modificationDS', None) or getattr(base_object, 'startDate', None) or
                       getattr(base_object, '_modificationDS', None) or now_utc())
        if modified_dt.tzinfo is None:
            if hasattr(self, 'event'):
                modified_dt = self._naive_to_aware(modified_dt)
            else:  # category
                modified_dt = pytz.utc.localize(modified_dt)
        data = {'folder': folder,
                'user': self.system_user,
                'title': convert_to_unicode(resource.name).strip() or folder.title,
                'description': convert_to_unicode(resource.description),
                'modified_dt': modified_dt}
        if resource.__class__.__name__ == 'Link':
            data['type'] = AttachmentType.link
            data['link_url'] = convert_to_unicode(resource.url).strip()
            if not data['link_url']:
                self.print_error('%[red!][{}] Skipping link, missing URL'.format(data['title']))
                return None
        else:
            data['type'] = AttachmentType.file
            storage_backend, storage_path, size, md5 = self._get_local_file_info(resource)
            if storage_path is None:
                self.print_error('%[red!]File {} not found on disk'.format(resource._LocalFile__archivedId))
                return None
            filename = secure_filename(convert_to_unicode(resource.fileName), 'attachment')
            data['file'] = AttachmentFile(user=self.system_user, created_dt=modified_dt, filename=filename,
                                          content_type=mimetypes.guess_type(filename)[0] or 'application/octet-stream',
                                          size=size, storage_backend=storage_backend, storage_file_id=storage_path,
                                          md5=md5)
        attachment = Attachment(**data)
        self.protection_from_ac(attachment, resource._Resource__ac)
        return attachment

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

    def _iter_attachments(self, obj):
        all_materials = chain(obj.materials.itervalues(), [getattr(obj, 'minutes', None)],
                              [getattr(obj, 'slides', None)], [getattr(obj, 'paper', None)],
                              [getattr(obj, 'poster', None)], [getattr(obj, 'video', None)])
        all_materials = (m for m in all_materials if m is not None)
        for material in all_materials:
            # skip minutes with no special protection - they are migrated in the event_notes migration
            resources = [resource for _, resource in material._Material__resources.iteritems() if
                         not (material.id == 'minutes' and resource.id == 'minutes' and
                              not self._has_special_protection(material, resource))]
            if resources:
                yield material, resources

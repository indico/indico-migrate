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
import os
from io import BytesIO

from PIL import Image

from indico.core.db import db
from indico.modules.events.layout import layout_settings
from indico.modules.events.layout.models.images import ImageFile
from indico.modules.events.layout.models.legacy_mapping import LegacyImageMapping
from indico.modules.events.models.events import EventType
from indico.util.date_time import now_utc
from indico.util.fs import secure_filename
from indico.util.string import crc32

from indico_migrate.steps.events import EventMigrationStep
from indico_migrate.util import LocalFileImporterMixin, convert_to_unicode, get_archived_file


ALLOWED_THEMES = {'orange.css', 'brown.css', 'right_menu.css'}


class EventImageImporter(LocalFileImporterMixin, EventMigrationStep):
    step_id = 'image'

    def __init__(self, *args, **kwargs):
        super(EventImageImporter, self).__init__(*args, **kwargs)
        self._set_config_options(**kwargs)

    def migrate(self):
        for picture in self._iter_pictures(self.conf):
            local_file = picture._localFile
            content_type = mimetypes.guess_type(local_file.fileName)[0] or 'application/octet-stream'
            storage_backend, storage_path, size, md5 = self._get_local_file_info(local_file)

            if storage_path is None:
                self.print_warning('%[yellow][{}]%[reset] -> %[red!]Not found in filesystem'.format(
                    local_file.id))
                continue

            filename = secure_filename(convert_to_unicode(local_file.fileName), 'image')
            image = ImageFile(event_id=self.event.id,
                              filename=filename,
                              content_type=content_type,
                              created_dt=now_utc(),
                              size=size,
                              md5=md5,
                              storage_backend=storage_backend,
                              storage_file_id=storage_path)

            map_entry = LegacyImageMapping(event_id=self.event.id, legacy_image_id=local_file.id, image=image)
            db.session.add(image)
            db.session.add(map_entry)

            if not self.quiet:
                self.print_success('%[cyan][{}]%[reset] -> %[blue!]{}'.format(local_file.id, image))

    def _iter_pictures(self, conf):
        try:
            dmgr = self.zodb_root['displayRegistery'][conf.id]
        except KeyError:
            self.print_error('Skipping event with no displaymgr')
            return

        imgr = getattr(dmgr, '_imagesMngr', None)
        if imgr:
            for _, picture in imgr._picList.iteritems():
                yield picture
        else:
            self.print_info('No _imagesMngr attribute!')


class EventLayoutImporter(EventMigrationStep):
    step_id = 'layout'

    def __init__(self, *args, **kwargs):
        super(EventLayoutImporter, self).__init__(*args, **kwargs)
        self.default_styles = self.zodb_root['MaKaCInfo']['main']._styleMgr._defaultEventStylesheet
        self.archive_dirs = kwargs.pop('archive_dir')

    def _process_logo(self, logo):
        path = get_archived_file(logo, self.archive_dirs)[1]
        if path is None:
            self.print_error('%[red!]Logo not found on disk; skipping it')
            return

        try:
            logo_image = Image.open(path)
        except IOError as e:
            self.print_warning("Cannot open {}: {}".format(convert_to_unicode(path), e))
            return

        if logo_image.mode == 'CMYK':
            self.print_warning("Logo is a CMYK {}; converting to RGB".format(logo_image.format))
            # this may result in wrong colors, but there's not much we can do...
            logo_image = logo_image.convert('RGB')

        logo_bytes = BytesIO()
        try:
            logo_image.save(logo_bytes, 'PNG')
        except Exception as e:
            self.print_warning("Cannot write PNG logo: {}".format(path, e))
            return
        logo_bytes.seek(0)
        logo_content = logo_bytes.read()
        logo_filename = secure_filename(convert_to_unicode(logo.fileName), 'logo')
        logo_filename = os.path.splitext(logo_filename)[0] + '.png'
        self.event.logo_metadata = {
            'size': len(logo_content),
            'hash': crc32(logo_content),
            'filename': logo_filename,
            'content_type': 'image/png'
        }
        self.event.logo = logo_content
        if not self.quiet:
            self.print_success('- %[cyan][Logo] {}'.format(logo.fileName))

    def _process_css(self, css):
        stylesheet = css._localFile
        path = get_archived_file(stylesheet, self.archive_dirs)[1]
        if path is None:
            self.print_error('%[red!]CSS file not found on disk; skipping it')
            return
        with open(path, 'rb') as f:
            stylesheet_content = convert_to_unicode(f.read())
        self.event.stylesheet_metadata = {
            'size': len(stylesheet_content),
            'hash': crc32(stylesheet_content),
            'filename': secure_filename(convert_to_unicode(stylesheet.fileName), 'stylesheet.css'),
            'content_type': 'text/css'
        }
        self.event.stylesheet = stylesheet_content
        if not self.quiet:
            self.print_success('- %[cyan][CSS] {}'.format(stylesheet.fileName))

    def migrate(self):
        dmgr = self.zodb_root['displayRegistery'][self.conf.id]

        style_mgr = getattr(dmgr, '_styleMngr', None) if self.event._type == EventType.conference else None
        custom_css = getattr(style_mgr, '_css', None) if self.event._type == EventType.conference else None

        if self.event._type == EventType.conference:
            logo = self.conf._logo
            settings = self._get_event_settings(dmgr)
            layout_settings.set_multi(self.event, settings)
            if not self.quiet:
                self.print_success('- %[cyan]Layout settings')
            if logo:
                self._process_logo(logo)
            if custom_css:
                self._process_css(custom_css)
        else:
            theme = dmgr._defaultstyle
            if not theme or theme == self.default_styles[self.event._type.legacy_name]:
                return
            layout_settings.set(self.event, 'timetable_theme', theme)
            if not self.quiet:
                self.print_success('- %[cyan]Default timetable theme: {}'.format(theme))

    def _get_event_settings(self, dmgr):
        format_opts = getattr(dmgr, '_format', None)
        tt = getattr(dmgr, '_tickerTape', None)
        style_mgr = getattr(dmgr, '_styleMngr', None)
        menu = getattr(dmgr, '_menu', None)

        settings = {
            'is_searchable': getattr(dmgr, '_searchEnabled', None),
            'show_nav_bar': getattr(dmgr, '_displayNavigationBar', None),
            'show_social_badges': getattr(dmgr, '_showSocialApps', None),
        }
        if format_opts:
            settings['header_text_color'] = format_opts._data.get('titleTextColor')
            settings['header_background_color'] = format_opts._data.get('titleBgColor')
        else:
            self.print_error('%[red!]Skipping some settings, missing _format attribute')
        if tt:
            settings['show_banner'] = getattr(tt, '_enabledNowPlaying', None)
            settings['announcement'] = getattr(tt, '_text', None)
            settings['show_announcement'] = getattr(tt, '_enabledSimpleText', None)
        else:
            self.print_error('%[red!]Skipping some settings, missing _tickerTape attribute')
        if style_mgr:
            template = getattr(style_mgr, '_usingTemplate', None)
            theme = getattr(template, 'templateId', None)
            settings['theme'] = theme if theme in ALLOWED_THEMES else None
            settings['use_custom_css'] = getattr(style_mgr, '_css', None) is not None
        elif not self.is_legacy_event:
            self.print_error('%[red!]Skipping some settings, missing _styleMngr attribute')
        if menu:
            settings['timetable_by_room'] = getattr(menu, '_timetable_layout', None) == 'room'
            settings['timetable_detailed'] = getattr(menu, '_timetable_detailed_view', False)
        else:
            self.print_error('%[red!]Skipping some settings, missing _menu attribute')

        return {k: v for k, v in settings.iteritems() if v is not None}

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
from indico.modules.events.layout import layout_settings
from indico.modules.events.layout.models.legacy_mapping import LegacyPageMapping
from indico.modules.events.layout.models.menu import EventPage, MenuEntry, MenuEntryType

from indico_migrate.steps.events import EventMigrationStep
from indico_migrate.util import sanitize_user_input


class _AltString(object):
    def __init__(self, *strings):
        self.strings = set(strings)

    def __eq__(self, other):
        return other in self.strings

    def __ne__(self, other):
        return not (self == other)


MENU_ENTRY_NAME_MAP = {
    'overview': 'overview',
    'programme': 'program',
    'manageTrack': 'program_my_tracks',
    'CFA': 'call_for_abstracts',
    'ViewAbstracts': 'user_abstracts',
    'SubmitAbstract': 'abstract_submission',
    'timetable': 'timetable',
    'contributionList': 'contributions',
    'authorIndex': 'author_index',
    'speakerIndex': 'speaker_index',
    'mystuff': 'my_conference',
    'mytracks': 'my_tracks',
    'mysessions': 'my_sessions',
    'mycontribs': 'my_contributions',
    'paperreviewing': 'paper_reviewing',
    'managepaperreviewing': 'paper_setup',
    'assigncontributions': 'paper_assign',
    'judgelist': 'contributions_to_judge',
    'judgelistreviewer': 'contributions_as_reviewer',
    'judgelisteditor': 'contributions_as_editor',
    'uploadpaper': 'paper_upload',
    'downloadtemplate': 'download_template',
    'abstractsBook': 'abstracts_book',
    'registrationForm': 'registration',
    'registrants': 'registrants',
    'evaluation': 'surveys',
    'chat-event-page': 'chat:chatrooms',
    'vc-event-page': 'videoconference_rooms',
    # XXX: these should only occur in dev databases
    'surveys': 'surveys',
    'chat:chatrooms': 'chat:chatrooms',
    'videoconference_rooms': 'videoconference_rooms'
}

DEFAULT_MENU_TITLES = {
    'abstract_submission': {'submit a new abstract', 'submit abstract'},
    'abstracts_book': {'book of abstracts'},
    'author_index': {'author list', 'author undex'},
    'call_for_abstracts': {'call for abstracts'},
    'chat:chatrooms': {'chat rooms'},
    'contributions': {'contribution list'},
    'contributions_as_editor': {'layout reviewer area'},
    'contributions_as_reviewer': {'content reviewer area'},
    'contributions_to_judge': {'referee area'},
    'download_template': {'download template'},
    'my_conference': {'my conference'},
    'my_contributions': {'my contributions'},
    'my_sessions': {'my session', 'my sessions'},
    'my_tracks': {'my tracks'},
    'overview': {'overview'},
    'paper_assign': {'assign papers'},
    'paper_reviewing': {'paper reviewing'},
    'paper_setup': {'manage paper reviewing'},
    'paper_upload': {'upload paper'},
    'program': {'scientific programme'},
    'program_my_tracks': {'manage my track', 'manage my tracks'},
    'registrants': {'list of participants', 'partecipants', 'participant list'},
    'registration': {'registration'},
    'speaker_index': {'speaker index', 'speaker list'},
    'surveys': {'evaluation', 'surveys'},
    'timetable': {'timetable'},
    'user_abstracts': {'view my abstracts'},
    'videoconference_rooms': {'video conference rooms', 'videoconference rooms'}
}


DEFAULT_MENU_STRUCTURE = [
    ['SystemLink', 'overview', 'overview', True, []],
    ['SystemLink', 'programme', 'scientific programme', True, [
        ['SystemLink', 'manageTrack', 'manage my tracks', True]
    ]],
    ['SystemLink', 'CFA', 'call for abstracts', True, [
        ['SystemLink', 'ViewAbstracts', 'view my abstracts', True],
        ['SystemLink', 'SubmitAbstract', _AltString('submit abstract', 'submit a new abstract'), True]
    ]],
    ['SystemLink', 'timetable', 'timetable', True, []],
    ['SystemLink', 'contributionList', 'contribution list', True, []],
    ['SystemLink', 'authorIndex', _AltString('author list', 'author index'), True, []],
    ['SystemLink', 'speakerIndex', _AltString('speaker list', 'author index'), False, []],
    ['SystemLink', 'mystuff', 'my conference', True, [
        ['SystemLink', 'mytracks', 'my tracks', True],
        ['SystemLink', 'mysessions', 'my sessions', True],
        ['SystemLink', 'mycontribs', 'my contributions', True]
    ]],
    ['SystemLink', 'paperreviewing', 'paper reviewing', True, [
        ['SystemLink', 'managepaperreviewing', 'manage paper reviewing', True],
        ['SystemLink', 'assigncontributions', 'assign papers', True],
        ['SystemLink', 'judgelist', 'referee area', True],
        ['SystemLink', 'judgelistreviewer', 'content reviewer area', True],
        ['SystemLink', 'judgelisteditor', 'layout reviewer area', True],
        ['SystemLink', 'uploadpaper', 'upload paper', True],
        ['SystemLink', 'downloadtemplate', 'download template', True]
    ]],
    ['SystemLink', 'abstractsBook', 'book of abstracts', True, []],
    ['SystemLink', 'registrationForm', 'registration', True, []],
    ['SystemLink', 'registrants', 'participant list', False, []],
    ['SystemLink', 'evaluation', 'evaluation', True, [
        ['SystemLink', 'newEvaluation', 'evaluation form', True],
        ['SystemLink', 'viewMyEvaluation', 'modify my evaluation', True]
    ]]
]

DEFAULT_MENU_STRUCTURE_2 = DEFAULT_MENU_STRUCTURE + [
    ['SystemLink', 'vc-event-page', 'videoconference rooms', True, []],
    ['SystemLink', 'chat-event-page', 'chat rooms', True, []]
]
DEFAULT_MENU_STRUCTURES = [DEFAULT_MENU_STRUCTURE, DEFAULT_MENU_STRUCTURE_2]
REMOVED_MENU_NAMES = {'ViewMyRegistration', 'NewRegistration', 'downloadETicket', 'newEvaluation', 'viewMyEvaluation',
                      'collaboration', 'instantMessaging'}
NOT_TOP_LEVEL_NAMES = {'program_my_tracks', 'evaluation_form', 'evaluation_edit'}


def _menu_item_data(entry, children=True):
    if entry._name in REMOVED_MENU_NAMES:
        return None
    data = [entry.__class__.__name__, entry._name, getattr(entry, '_caption', '').lower(), entry._active]
    if children:
        data.append(filter(None, (_menu_item_data(child, False) for child in getattr(entry, '_listLink', []))))
    return data


def _get_menu_structure(display_mgr):
    return filter(None, map(_menu_item_data, display_mgr._menu._listLink))


class EventMenuImporter(EventMigrationStep):
    step_id = 'menus'

    def migrate(self):
        dmgr = self.zodb_root['displayRegistery'][self.conf.id]
        if _get_menu_structure(dmgr) not in DEFAULT_MENU_STRUCTURES:
            self.print_success('Custom menu')
            new_menu_data = list(self._migrate_menu(self.event, dmgr._menu))
            db.session.add_all(new_menu_data)
            layout_settings.set(self.event, 'use_custom_menu', True)

        # set on event context whether "participants" item is enabled
        # this is used later in the registration form migration
        entries = [entry for entry in new_menu_data if entry.name == 'participants']
        assert(len(entries) < 2)
        self.event_ns.misc_data['participant_list_disabled'] = False
        if entries:
            self.event_ns.misc_data['participant_list_disabled'] = entries[0].is_disabled

    def _migrate_menu(self, event, container, parent=None, used=None):
        if used is None:
            used = set()
        for pos, item in enumerate(container._listLink, 1):
            data = {'parent': parent, 'is_enabled': item._active, 'position': pos}
            item_type = item.__class__.__name__
            if item_type == 'SystemLink':
                if item._name in REMOVED_MENU_NAMES:
                    continue
                name = MENU_ENTRY_NAME_MAP.get(item._name)
                if name is None:
                    self.print_warning('Skipping unknown menu entry: {}'.format(name))
                    continue
                data['name'] = name
                if not parent and data['name'] in NOT_TOP_LEVEL_NAMES:
                    self.print_warning('%[yellow]Skipping top-level menu entry {}'.format(data['name']))
                    continue
                elif data['name'] in used:
                    self.print_error('%[red!]duplicate menu entry name {}; skipping'.format(data['name']))
                    continue
                used.add(data['name'])
                data['title'] = sanitize_user_input(item._caption)
                if not data['title']:
                    data['title'] = None
                    self.print_warning('%[yellow!]Menu entry {} has no title; using default'.format(
                        data['name']))
                elif data['title'].lower() in DEFAULT_MENU_TITLES[data['name']]:
                    data['title'] = None
                if item._name == 'chatrooms':
                    data['plugin'] = 'chat'
                    data['type'] = MenuEntryType.plugin_link
                else:
                    data['type'] = MenuEntryType.internal_link
            elif item_type == 'Spacer':
                data['type'] = MenuEntryType.separator
            elif item_type == 'ExternLink':
                data['type'] = MenuEntryType.user_link
                data['title'] = sanitize_user_input(item._caption)
                data['link_url'] = item._URL.strip()
                if not data['link_url']:
                    if getattr(item, '_listLink', None):
                        self.print_warning('%[yellow!]Link "{}" has no URL but children'.format(data['title']))
                    else:
                        self.print_warning('%[yellow]Skipping link "{}" with no URL'.format(data['title']))
                        continue
                if not data['title']:
                    if getattr(item, '_listLink', None):
                        data['title'] = '(no title)'
                        self.print_warning('%[yellow!]Link has no title but children; using dummy title')
                    else:
                        self.print_warning('%[yellow]Skipping link with no title')
                        continue
            elif item_type == 'PageLink':
                data['type'] = MenuEntryType.page
                data['title'] = sanitize_user_input(item._caption)
                if not data['title']:
                    data['title'] = '(no title)'
                    self.print_warning('%[yellow!]Page has no title; using dummy')
                data['page'] = EventPage(event=event, html=item._page._content)
                data['page'].legacy_mapping = LegacyPageMapping(event_id=event.id, legacy_page_id=item._page._id)
                if item._page._isHome:
                    event.default_page = data['page']
            else:
                self.print_error('Unexpected menu item type: {}'.format(item_type))
                continue
            entry = MenuEntry(**data)
            entry.event = event  # XXX: the kwarg does not populate the relationship!
            yield entry
            if getattr(item, '_listLink', None):
                # child entries
                if not parent:
                    for sub_entry in self._migrate_menu(event, item, entry, used):
                        yield sub_entry
                else:
                    self.print_warning('Skipping children inside nested entry')

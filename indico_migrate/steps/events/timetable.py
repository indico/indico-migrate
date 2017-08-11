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

import itertools
from collections import defaultdict
from operator import attrgetter, itemgetter

from sqlalchemy.orm import joinedload, lazyload

from indico.core.db.sqlalchemy.colors import ColorTuple
from indico.core.db.sqlalchemy.descriptions import RenderMode
from indico.core.db.sqlalchemy.principals import EmailPrincipal
from indico.core.db.sqlalchemy.protection import ProtectionMode
from indico.modules.events.contributions.models.contributions import Contribution
from indico.modules.events.contributions.models.fields import ContributionFieldValue
from indico.modules.events.contributions.models.legacy_mapping import (LegacyContributionMapping,
                                                                       LegacySubContributionMapping)
from indico.modules.events.contributions.models.persons import (AuthorType, ContributionPersonLink,
                                                                SubContributionPersonLink)
from indico.modules.events.contributions.models.principals import ContributionPrincipal
from indico.modules.events.contributions.models.references import ContributionReference, SubContributionReference
from indico.modules.events.contributions.models.subcontributions import SubContribution
from indico.modules.events.models.persons import EventPerson, EventPersonLink
from indico.modules.events.models.references import EventReference
from indico.modules.events.sessions.models.blocks import SessionBlock
from indico.modules.events.sessions.models.legacy_mapping import LegacySessionBlockMapping, LegacySessionMapping
from indico.modules.events.sessions.models.persons import SessionBlockPersonLink
from indico.modules.events.sessions.models.principals import SessionPrincipal
from indico.modules.events.sessions.models.sessions import Session
from indico.modules.events.timetable.models.breaks import Break
from indico.modules.events.timetable.models.entries import TimetableEntry
from indico.modules.events.tracks import Track
from indico.modules.events.tracks.settings import track_settings
from indico.modules.rb import Location, Room
from indico.util.string import fix_broken_string, is_valid_mail, sanitize_email

from indico_migrate.steps.events import PERSON_INFO_MAP, EventMigrationStep
from indico_migrate.util import convert_to_unicode, strict_sanitize_email


PROTECTION_MODE_MAP = {
    -1: ProtectionMode.public,
    0: ProtectionMode.inheriting,
    1: ProtectionMode.protected,
}


def most_common(iterable, key=None):
    """Return the most common element of an iterable."""
    groups = itertools.groupby(sorted(iterable), key=key)
    return max(groups, key=lambda x: sum(1 for _ in x[1]))[0]


class EventTracksImporter(EventMigrationStep):
    step_id = 'tracks'

    def migrate(self):
        program = convert_to_unicode(getattr(self.conf, 'programDescription', ''))
        if program:
            track_settings.set_multi(self.event, {'program_render_mode': RenderMode.html, 'program': program})
        for pos, old_track in enumerate(self.conf.program, 1):
            track = Track(title=convert_to_unicode(old_track.title),
                          description=convert_to_unicode(old_track.description),
                          code=convert_to_unicode(old_track._code),
                          position=pos,
                          abstract_reviewers=set())
            self.print_info('%[white!]Track:%[reset] {}'.format(track.title))
            for coordinator in old_track._coordinators:
                user = self.user_from_legacy(coordinator)
                if user is None:
                    continue
                self.print_info('%[blue!]  Coordinator:%[reset] {}'.format(user))
                track.conveners.add(user)
                track.abstract_reviewers.add(user)
                self.event.update_principal(user, add_roles={'abstract_reviewer', 'track_convener'}, quiet=True)
            self.event_ns.track_map[old_track] = track
            self.event_ns.track_map_by_id[int(old_track.id)] = track
            self.event.tracks.append(track)


class EventTimetableImporter(EventMigrationStep):
    step_id = 'timetable'

    def setup(self):
        self.room_mapping = {(r.location.name, r.name): r for r in Room.query.options(lazyload(Room.owner),
                                                                                      joinedload(Room.location))}
        self.venue_mapping = {location.name: location for location in Location.query}

    def migrate(self):
        self.legacy_session_ids_used = set()
        self._migrate_references()
        self._migrate_event_persons()
        self._migrate_event_persons_links()
        self._migrate_sessions()
        self._migrate_contributions()
        self._migrate_timetable()

    def _migrate_references(self):
        self.event.references = list(self._process_references(EventReference, self.conf))

    def _migrate_sessions(self):
        sessions = []
        friendly_id_map = {}
        friendly_ids_used = set()
        skipped = []
        for id_, session in sorted(self.conf.sessions.items(),
                                   key=lambda x: (x[0].isdigit(), int(x[0]) if x[0].isdigit() else x[0])):
            id_ = int(id_.lstrip('s'))  # legacy: s123
            if id_ in friendly_ids_used:
                skipped.append(session)
                continue
            friendly_id_map[session] = id_
            friendly_ids_used.add(id_)
        for i, session in enumerate(skipped, (max(friendly_ids_used) if friendly_ids_used else 0) + 1):
            assert i not in friendly_ids_used
            friendly_id_map[session] = i
            friendly_ids_used.add(i)
        for old_session in self.conf.sessions.itervalues():
            sessions.append(self._migrate_session(old_session, friendly_id_map[old_session]))
        if sessions:
            self.event._last_friendly_session_id = max(s.friendly_id for s in sessions)

    def _migrate_session(self, old_session, friendly_id=None):
        ac = old_session._Session__ac
        code = convert_to_unicode(old_session._code)
        if code == 'no code':
            code = ''
        session = Session(event=self.event, title=convert_to_unicode(old_session.title),
                          description=convert_to_unicode(old_session.description),
                          is_poster=(old_session._ttType == 'poster'), code=code,
                          default_contribution_duration=old_session._contributionDuration,
                          protection_mode=PROTECTION_MODE_MAP[ac._accessProtection])
        if friendly_id is not None:
            session.friendly_id = friendly_id
        else:
            # migrating a zombie session; we simply give it a new friendly id
            self.event._last_friendly_session_id += 1
            session.friendly_id = self.event._last_friendly_session_id
        if not self.quiet:
            self.print_info('%[blue!]Session%[reset] {}'.format(session.title))
        self.event_ns.legacy_session_map[old_session] = session
        if old_session.id not in self.legacy_session_ids_used:
            session.legacy_mapping = LegacySessionMapping(event=self.event, legacy_session_id=old_session.id)
            self.legacy_session_ids_used.add(old_session.id)
        else:
            self.print_warning('%[yellow!]Duplicate session id; not adding legacy mapping for {}'
                               .format(old_session.id))
        # colors
        try:
            session.colors = ColorTuple(old_session._textColor, old_session._color)
        except (AttributeError, ValueError) as e:
            self.print_warning('%[yellow]Session has no colors: "{}" [{}]'.format(session.title, e))
        principals = {}
        # managers / read access
        self._process_ac(SessionPrincipal, principals, ac, allow_emails=True)
        # coordinators
        for submitter in old_session._coordinators.itervalues():
            self._process_principal(SessionPrincipal, principals, submitter, 'Coordinator', roles={'coordinate'})
        self._process_principal_emails(SessionPrincipal, principals, getattr(old_session, '_coordinatorsEmail', []),
                                       'Coordinator', roles={'coordinate'}, allow_emails=True)
        session.acl_entries = set(principals.itervalues())
        return session

    def _migrate_contributions(self):
        contribs = []
        friendly_id_map = {}
        friendly_ids_used = set()
        skipped = []
        for id_, contrib in sorted(self.conf.contributions.items(),
                                   key=lambda x: (not x[0].isdigit(), int(x[0]) if x[0].isdigit() else x[0])):
            try:
                id_ = int(id_)  # legacy: s1t2
            except ValueError:
                skipped.append(contrib)
                continue
            if id_ in friendly_ids_used:
                skipped.append(contrib)
                continue
            friendly_id_map[contrib] = id_
            friendly_ids_used.add(id_)
        for i, contrib in enumerate(skipped, (max(friendly_ids_used) if friendly_ids_used else 0) + 1):
            assert i not in friendly_ids_used
            friendly_id_map[contrib] = i
            friendly_ids_used.add(i)
        for old_contrib in self.conf.contributions.itervalues():
            contribs.append(self._migrate_contribution(old_contrib, friendly_id_map[old_contrib]))
        if contribs:
            # there may be a higher last_friendly_contribution_id from abstracts
            self.event._last_friendly_contribution_id = max(self.event._last_friendly_contribution_id,
                                                            max(c.friendly_id for c in contribs))

    def _migrate_contribution(self, old_contrib, friendly_id):
        ac = old_contrib._Contribution__ac
        try:
            description = old_contrib._fields.get('content', '')
        except AttributeError:
            if not self.is_legacy_event:
                self.print_warning('Contribution {} has no fields'.format(old_contrib))
            description = ''
        description = convert_to_unicode(getattr(description, 'value', description))  # str or AbstractFieldContent
        status = getattr(old_contrib, '_status', None)
        status_class = status.__class__.__name__ if status else None

        contrib = Contribution(event=self.event, friendly_id=friendly_id,
                               title=convert_to_unicode(old_contrib.title),
                               render_mode=RenderMode.html,
                               description=description, duration=old_contrib.duration,
                               protection_mode=PROTECTION_MODE_MAP[ac._accessProtection],
                               board_number=convert_to_unicode(getattr(old_contrib, '_boardNumber', '')),
                               keywords=self._process_keywords(getattr(old_contrib, '_keywords', '')),
                               is_deleted=(status_class == 'ContribStatusWithdrawn'))
        if old_contrib._track is not None:
            track = self.event_ns.track_map.get(old_contrib._track)
            if not track:
                self.print_warning('Track not found: {}. Setting to None.'.format(old_contrib._track))
            else:
                contrib.track = track
        if not self.quiet:
            self.print_info('%[cyan]Contribution%[reset] {}'.format(contrib.title))
        self.event_ns.legacy_contribution_map[old_contrib] = contrib
        contrib.legacy_mapping = LegacyContributionMapping(event=self.event, legacy_contribution_id=old_contrib.id)
        # contribution type
        if old_contrib._type is not None:
            try:
                contrib.type = self.event_ns.legacy_contribution_type_map[old_contrib._type]
            except AttributeError:
                self.print_warning('%[yellow!]Invalid contrib type {}'
                                   .format(convert_to_unicode(old_contrib._type._name)))
        # abstract
        if old_contrib in self.event_ns.legacy_contribution_abstracts:
            contrib.abstract = self.event_ns.legacy_contribution_abstracts[old_contrib]
        # ACLs (managers, read access, submitters)
        principals = {}
        self._process_ac(ContributionPrincipal, principals, ac)
        for submitter in old_contrib._submitters:
            self._process_principal(ContributionPrincipal, principals, submitter, 'Submitter', roles={'submit'})
        self._process_principal_emails(ContributionPrincipal, principals, getattr(old_contrib, '_submittersEmail', []),
                                       'Submitter', roles={'submit'})
        contrib.acl_entries = set(principals.itervalues())
        # speakers, authors and co-authors
        contrib.person_links = list(self._migrate_contribution_person_links(old_contrib))
        # references ("report numbers")
        contrib.references = list(self._process_references(ContributionReference, old_contrib))
        # contribution/abstract fields
        if hasattr(old_contrib, '_fields'):
            contrib.field_values = list(self._migrate_contribution_field_values(old_contrib))
        contrib.subcontributions = [self._migrate_subcontribution(old_contrib, old_subcontrib, pos)
                                    for pos, old_subcontrib in enumerate(old_contrib._subConts, 1)]
        contrib._last_friendly_subcontribution_id = len(contrib.subcontributions)
        return contrib

    def _migrate_subcontribution(self, old_contrib, old_subcontrib, position):
        subcontrib = SubContribution(position=position, friendly_id=position, duration=old_subcontrib.duration,
                                     title=convert_to_unicode(old_subcontrib.title),
                                     description=convert_to_unicode(old_subcontrib.description),
                                     render_mode=RenderMode.html)
        if not self.quiet:
            self.print_info('  %[cyan!]SubContribution%[reset] {}'.format(subcontrib.title))
        self.event_ns.legacy_subcontribution_map[old_subcontrib] = subcontrib
        subcontrib.legacy_mapping = LegacySubContributionMapping(event=self.event,
                                                                 legacy_contribution_id=old_contrib.id,
                                                                 legacy_subcontribution_id=old_subcontrib.id)
        subcontrib.references = list(self._process_references(SubContributionReference, old_subcontrib))
        subcontrib.person_links = list(self._migrate_subcontribution_person_links(old_subcontrib))
        return subcontrib

    def _migrate_contribution_field_values(self, old_contrib):
        fields = dict(old_contrib._fields)
        fields.pop('content', None)
        for field_id, field_content in fields.iteritems():
            value = getattr(field_content, 'value', field_content)
            if isinstance(value, list):
                # legacy data, apparently there was a 'keywords' abstract field type once
                value = ', '.join(value)
            value = convert_to_unicode(value)
            if not value:
                continue
            try:
                new_field = self.event_ns.legacy_contribution_field_map[field_id]
            except KeyError:
                self.print_warning('%[yellow!]Contribution field "{}" does not exist'.format(field_id))
                continue
            new_value = self._process_contribution_field_value(field_id, value, new_field, ContributionFieldValue)
            if new_value:
                if not self.quiet:
                    self.print_info('%[green] - [field]%[reset] {}: {}'.format(new_field.title, new_value.data))
                yield new_value

    def _process_contribution_field_value(self, old_field_id, old_value, new_field, field_class):
        if new_field.field_type == 'text':
            data = convert_to_unicode(old_value)
            return field_class(contribution_field=new_field, data=data)
        elif new_field.field_type == 'single_choice':
            data = self.event_ns.legacy_field_option_id_map[old_field_id, int(old_value)]
            return field_class(contribution_field=new_field, data=data)
        else:
            raise ValueError('Unexpected field type: {}'.format(new_field.field_type))

    def _migrate_timetable(self):
        schedule = self.conf._Conference__schedule
        if schedule is None:
            self.print_error('%[red!]Event has no schedule')
            return
        if not self.quiet:
            self.print_info('%[green]Timetable...')
        self._migrate_timetable_entries(schedule._entries)

    def _migrate_timetable_entries(self, old_entries, session_block=None):
        for old_entry in old_entries:
            item_type = old_entry.__class__.__name__
            if item_type == 'ContribSchEntry':
                entry = self._migrate_contribution_timetable_entry(old_entry, session_block)
            elif item_type == 'BreakTimeSchEntry':
                entry = self._migrate_break_timetable_entry(old_entry, session_block)
            elif item_type == 'LinkedTimeSchEntry':
                parent = old_entry._LinkedTimeSchEntry__owner
                parent_type = parent.__class__.__name__
                if parent_type == 'Contribution':
                    self.print_warning('%[yellow!]Found LinkedTimeSchEntry for contribution')
                    entry = self._migrate_contribution_timetable_entry(old_entry, session_block)
                elif parent_type != 'SessionSlot':
                    self.print_error('%[red!]Found LinkedTimeSchEntry for {}'.format(parent_type))
                    continue
                else:
                    assert session_block is None
                    entry = self._migrate_block_timetable_entry(old_entry)
            else:
                raise ValueError('Unexpected item type: ' + item_type)
            if session_block:
                if entry.start_dt < session_block.timetable_entry.start_dt:
                    self.print_warning('%[yellow!]Block boundary (start violated; extending block from {} to {})'
                                       .format(session_block.timetable_entry.start_dt, entry.start_dt))
                    session_block.timetable_entry.start_dt = entry.start_dt
                if entry.end_dt > session_block.timetable_entry.end_dt:
                    self.print_warning('%[yellow!]Block boundary (end violated; extending block from {} to {})'
                                       .format(session_block.timetable_entry.end_dt, entry.end_dt))
                    session_block.duration += entry.end_dt - session_block.timetable_entry.end_dt

    def _migrate_contribution_timetable_entry(self, old_entry, session_block=None):
        old_contrib = old_entry._LinkedTimeSchEntry__owner
        contrib = self.event_ns.legacy_contribution_map[old_contrib]
        contrib.timetable_entry = TimetableEntry(event=self.event,
                                                 start_dt=self.context._fix_naive(old_contrib.startDate))
        self._migrate_location(old_contrib, contrib)
        if session_block:
            contrib.session = session_block.session
            contrib.session_block = session_block
            contrib.timetable_entry.parent = session_block.timetable_entry
        return contrib.timetable_entry

    def _migrate_break_timetable_entry(self, old_entry, session_block=None):
        break_ = Break(title=convert_to_unicode(old_entry.title), description=convert_to_unicode(old_entry.description),
                       duration=old_entry.duration)
        try:
            break_.colors = ColorTuple(old_entry._textColor, old_entry._color)
        except (AttributeError, ValueError) as e:
            self.print_warning('%[yellow]Break has no colors: "{}" [{}]'.format(break_.title, e))
        break_.timetable_entry = TimetableEntry(event=self.event,
                                                start_dt=self.context._fix_naive(old_entry.startDate))
        self._migrate_location(old_entry, break_)
        if session_block:
            break_.timetable_entry.parent = session_block.timetable_entry
        return break_.timetable_entry

    def _migrate_block_timetable_entry(self, old_entry):
        old_block = old_entry._LinkedTimeSchEntry__owner
        try:
            session = self.event_ns.legacy_session_map[old_block.session]
        except KeyError:
            self.print_warning('%[yellow!]Found zombie session {}'.format(old_block.session))
            session = self._migrate_session(old_block.session)
        session_block = SessionBlock(session=session, title=convert_to_unicode(old_block.title),
                                     duration=old_block.duration)
        session_block.timetable_entry = TimetableEntry(event=self.event,
                                                       start_dt=self.context._fix_naive(old_block.startDate))
        if session.legacy_mapping is not None:
            session_block.legacy_mapping = LegacySessionBlockMapping(event=self.event,
                                                                     legacy_session_id=old_block.session.id,
                                                                     legacy_session_block_id=old_block.id)
        self._migrate_location(old_block, session_block)
        session_block.person_links = list(self._migrate_session_block_person_links(old_block))
        self._migrate_timetable_entries(old_block._schedule._entries, session_block)
        return session_block.timetable_entry

    def _migrate_location(self, old_entry, new_entry):
        custom_location = (old_entry.places[0] if getattr(old_entry, 'places', None)
                           else getattr(old_entry, 'place', None))
        custom_room = (old_entry.rooms[0] if getattr(old_entry, 'rooms', None)
                       else getattr(old_entry, 'room', None))
        new_entry.inherit_location = not custom_location and not custom_room
        if new_entry.inherit_location:
            return
        # we don't inherit, so let's migrate the data we have
        # address is always allowed
        if not custom_location:
            custom_location = self._get_parent_location(old_entry, attr='places')
        if not custom_room:
            custom_room = self._get_parent_location(old_entry, attr='rooms')
        new_entry.address = (convert_to_unicode(fix_broken_string(custom_location.address, True))
                             if custom_location and custom_location.address else '')
        location_name = (convert_to_unicode(fix_broken_string(custom_location.name, True))
                         if custom_location and custom_location.name else '')
        if custom_room:
            room_name = convert_to_unicode(fix_broken_string(custom_room.name, True))
            rb_room = self.room_mapping.get((location_name, room_name))
            # if we have a room from the rb module, we only link this, otherwise we use the (custom) names
            if rb_room:
                new_entry.room = rb_room
            else:
                new_entry.venue_name = location_name
                new_entry.room_name = room_name
        venue = self.venue_mapping.get(new_entry.venue_name)
        if venue is not None:
            # store proper reference to the venue if it's a predefined one
            new_entry.venue = venue
            new_entry.venue_name = ''

    def _get_parent_location(self, obj, attr):
        type_ = obj.__class__.__name__
        if type_ == 'SessionSlot':
            return getattr(self.conf, attr)[0] if getattr(self.conf, attr, None) else None
        elif type_ in ('BreakTimeSchEntry', 'Contribution', 'AcceptedContribution'):
            if type_ == 'AcceptedContribution':
                contrib_parent = obj._session
                if getattr(contrib_parent, attr, None):
                    return getattr(contrib_parent, attr)[0]
                else:
                    owner = self.conf
            elif type_ == 'Contribution':
                contrib_parent = obj.parent
                if attr == 'places' and contrib_parent:
                    places = getattr(contrib_parent, attr, None)
                    return getattr(contrib_parent, 'place', None) if not places else places[0]
                if attr == 'rooms' and contrib_parent:
                    rooms = getattr(contrib_parent, attr, None)
                    return getattr(contrib_parent, 'room', None) if not rooms else rooms[0]
            elif type_ == 'BreakTimeSchEntry':
                owner = obj._sch._owner
            return self._get_parent_location(owner, attr)
        elif type_ == 'Conference':
            return getattr(obj, attr)[0] if getattr(obj, attr, None) else None
        elif type_ == 'Session':
            return self._get_parent_location(self.conf, attr)

    def _process_references(self, reference_cls, old_object):
        try:
            rnh = old_object._reportNumberHolder
        except AttributeError:
            return
        for name, values in rnh._reports.iteritems():
            try:
                reference_type = self.global_ns.reference_types[name]
            except KeyError:
                self.print_warning('%[yellow!]Unknown reference type: {}'.format(name))
                continue
            if isinstance(values, basestring):
                values = [values]
            for value in map(convert_to_unicode, values):
                if value == 'None':
                    self.print_warning("%[yellow!]Skipping 'None' value")
                    continue
                if not self.quiet:
                    self.print_info(' - %[magenta]{}: %[green!]{}'.format(name, value))
                yield reference_cls(reference_type=reference_type, value=value)

    def _convert_principal(self, old_principal):
        principal = self.convert_principal(old_principal)
        if (principal is None and old_principal.__class__.__name__ in ('Avatar', 'AvatarUserWrapper') and
                'email' in old_principal.__dict__):
            email = old_principal.__dict__['email'].lower()
            principal = self.global_ns.users_by_email.get(email)
            if principal is not None:
                self.print_warning('Using {} for {} (matched via {})'.format(principal, old_principal, email),
                                   always=False)
        return principal

    def _process_principal(self, principal_cls, principals, legacy_principal, name, read_access=None, full_access=None,
                           roles=None, allow_emails=True):
        if legacy_principal is None:
            return
        elif isinstance(legacy_principal, basestring):
            user = self.global_ns.users_by_email.get(legacy_principal)
            principal = user or EmailPrincipal(legacy_principal)
        else:
            principal = self._convert_principal(legacy_principal)
        if principal is None:
            self.print_warning('%[yellow]{} does not exist:%[reset] {}' .format(name, legacy_principal),
                               always=False)
            return
        elif not allow_emails and isinstance(principal, EmailPrincipal):
            self.print_warning('%[yellow]{} cannot be an email principal:%[reset] {}'
                               .format(name, legacy_principal), always=False)
            return
        try:
            entry = principals[principal]
        except KeyError:
            entry = principal_cls(principal=principal, full_access=False, roles=[])
            principals[principal] = entry
        if read_access:
            entry.read_access = True
        if full_access:
            entry.full_access = True
        if roles:
            entry.roles = sorted(set(entry.roles) | set(roles))
        if not self.quiet:
            self.print_info(' - [{}] {}'.format(name.lower(), principal))

    def _process_principal_emails(self, principal_cls, principals, emails, name, read_access=None, full_access=None,
                                  roles=None, allow_emails=True):
        emails = {sanitize_email(convert_to_unicode(email).lower()) for email in emails}
        emails = {email for email in emails if is_valid_mail(email, False)}
        for email in emails:
            self._process_principal(principal_cls, principals, email, name, read_access, full_access, roles,
                                    allow_emails=allow_emails)

    def _process_ac(self, principal_cls, principals, ac, allow_emails=True):
        # read access
        for principal in ac.allowed:
            self._process_principal(principal_cls, principals, principal, 'Access', read_access=True,
                                    allow_emails=allow_emails)
        # email-based read access
        emails = getattr(ac, 'allowedEmail', [])
        self._process_principal_emails(principal_cls, principals, emails, 'Access', read_access=True,
                                       allow_emails=allow_emails)
        # managers
        for manager in ac.managers:
            self._process_principal(principal_cls, principals, manager, 'Manager', full_access=True,
                                    allow_emails=allow_emails)
        # email-based managers
        emails = getattr(ac, 'managersEmail', [])
        self._process_principal_emails(principal_cls, principals, emails, 'Manager', full_access=True,
                                       allow_emails=allow_emails)

    def _process_keywords(self, keywords):
        return map(convert_to_unicode, keywords.splitlines())

    def _migrate_event_persons(self):
        all_persons = defaultdict(list)
        old_people = []
        for chairperson in getattr(self.conf, '_chairs', []):
            old_people.append(chairperson)
        for old_contrib in self.conf.contributions.itervalues():
            for speaker in getattr(old_contrib, '_speakers', []):
                old_people.append(speaker)
            for author in getattr(old_contrib, '_primaryAuthors', []):
                old_people.append(author)
            for coauthor in getattr(old_contrib, '_coAuthors', []):
                old_people.append(coauthor)
            for old_subcontrib in old_contrib._subConts:
                for speaker in getattr(old_subcontrib, 'speakers', []):
                    old_people.append(speaker)
        schedule = self.conf._Conference__schedule
        if schedule:
            for old_entry in schedule._entries:
                entry_type = old_entry.__class__.__name__
                if entry_type == 'LinkedTimeSchEntry':
                    old_block = old_entry._LinkedTimeSchEntry__owner
                    for convener in getattr(old_block, '_conveners', []):
                        old_people.append(convener)
        for old_person in old_people:
            person = self.event_person_from_legacy(old_person, skip_empty_email=True, skip_empty_names=True)
            if person:
                user = self.global_ns.users_by_email.get(person.email)
                email = user.email if user else person.email
                all_persons[email].append(person)
        for email, persons in all_persons.iteritems():
            person = self.get_event_person_by_email(email)
            if not person:
                person = EventPerson(email=email,
                                     event=self.event,
                                     user=self.global_ns.users_by_email.get(email),
                                     first_name=most_common(persons, key=attrgetter('first_name')),
                                     last_name=most_common(persons, key=attrgetter('last_name')),
                                     _title=most_common(persons, key=attrgetter('_title')),
                                     affiliation=most_common(persons, key=attrgetter('affiliation')),
                                     address=most_common(persons, key=attrgetter('address')),
                                     phone=most_common(persons, key=attrgetter('phone')))
                self.add_event_person(person)
            if not self.quiet:
                self.print_info('%[magenta!]Event Person%[reset] {}({})'.format(person.full_name, person.email))

    def _get_person(self, old_person):
        email = getattr(old_person, '_email', None) or getattr(old_person, 'email', None)
        email = strict_sanitize_email(convert_to_unicode(email).lower()) if email else email
        if not email:
            return self.event_person_from_legacy(old_person, skip_empty_names=True)
        return self.get_event_person_by_email(email)

    def _migrate_event_persons_links(self):
        person_link_map = {}
        for chair in getattr(self.conf, '_chairs', []):
            person = self._get_person(chair)
            if not person:
                continue
            link = person_link_map.get(person)
            if link:
                self.print_warning('%[yellow!]Duplicated chair "{}" for event'.format(person.full_name))
            else:
                link = EventPersonLink(person=person, **self._get_person_data(chair))
                person_link_map[person] = link
                self.event.person_links.append(link)

    def _update_link_data(self, link, data_list):
        for attr in PERSON_INFO_MAP.itervalues():
            value = most_common(data_list, key=itemgetter(attr))
            if value and value != getattr(link, attr):
                setattr(link, attr, value)

    def _migrate_contribution_person_links(self, old_entry):
        person_link_map = {}
        person_link_data_map = defaultdict(list)
        for speaker in getattr(old_entry, '_speakers', []):
            person = self._get_person(speaker)
            if not person:
                continue
            person_link_data = self._get_person_data(speaker)
            person_link_data_map[person].append(person_link_data)
            link = person_link_map.get(person)
            if link:
                self._update_link_data(link, person_link_data_map[person])
                link.is_speaker = True
            else:
                link = ContributionPersonLink(person=person, is_speaker=True, **person_link_data)
                person_link_map[person] = link
                yield link
        for author in getattr(old_entry, '_primaryAuthors', []):
            person = self._get_person(author)
            if not person:
                continue
            person_link_data = self._get_person_data(author)
            person_link_data_map[person].append(person_link_data)
            link = person_link_map.get(person)
            if link:
                self._update_link_data(link, person_link_data_map[person])
                link.author_type = AuthorType.primary
            else:
                link = ContributionPersonLink(person=person, author_type=AuthorType.primary, **person_link_data)
                person_link_map[person] = link
                yield link
        for coauthor in getattr(old_entry, '_coAuthors', []):
            person = self._get_person(coauthor)
            if not person:
                continue
            person_link_data = self._get_person_data(coauthor)
            person_link_data_map[person].append(person_link_data)
            link = person_link_map.get(person)
            if link:
                self._update_link_data(link, person_link_data_map[person])
                if link.author_type == AuthorType.primary:
                    self.print_warning('%[yellow!]Primary author "{}" is also co-author'
                                       .format(person.full_name))
                else:
                    link.author_type = AuthorType.secondary
            else:
                link = ContributionPersonLink(person=person, author_type=AuthorType.secondary, **person_link_data)
                person_link_map[person] = link
                yield link

    def _migrate_subcontribution_person_links(self, old_entry):
        person_link_map = {}
        person_link_data_map = defaultdict(list)
        for speaker in getattr(old_entry, 'speakers', []):
            person = self._get_person(speaker)
            if not person:
                continue
            person_link_data = self._get_person_data(speaker)
            person_link_data_map[person].append(person_link_data)
            link = person_link_map.get(person)
            if link:
                self._update_link_data(link, person_link_data_map[person])
                self.print_warning('%[yellow!]Duplicated speaker "{}" for sub-contribution'
                                   .format(person.full_name))
            else:
                link = SubContributionPersonLink(person=person, **person_link_data)
                person_link_map[person] = link
                yield link

    def _migrate_session_block_person_links(self, old_entry):
        person_link_map = {}
        person_link_data_map = defaultdict(list)
        for convener in getattr(old_entry, '_conveners', []):
            person = self._get_person(convener)
            if not person:
                continue
            person_link_data = self._get_person_data(convener)
            person_link_data_map[person].append(person_link_data)
            link = person_link_map.get(person)
            if link:
                self._update_link_data(link, person_link_data_map[person])
                self.print_warning('%[yellow!]Duplicated session block convener "{}"'.format(person.full_name))
            else:
                link = SessionBlockPersonLink(person=person, **person_link_data)
                person_link_map[person] = link
                yield link

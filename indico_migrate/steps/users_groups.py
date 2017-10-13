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

from collections import defaultdict
from datetime import timedelta
from operator import attrgetter
from uuid import uuid4

import pytz
from babel.dates import get_timezone
from pytz import all_timezones_set

from indico.core.db import db
from indico.modules.api import APIKey
from indico.modules.auth import Identity
from indico.modules.groups.models.groups import LocalGroup
from indico.modules.users import User, user_settings
from indico.modules.users.models.users import UserTitle
from indico.util.caching import memoize
from indico.util.i18n import get_all_locales
from indico.util.string import is_valid_mail, sanitize_email
from indico.util.struct.iterables import committing_iterator

from indico_migrate.importer import TopLevelMigrationStep
from indico_migrate.util import convert_to_unicode, step_description


USER_TITLE_MAP = {x.title: x for x in UserTitle}
SYNCED_FIELD_MAP = {
    'firstName': 'first_name',
    'surName': 'last_name',
    'affiliation': 'affiliation',
    'address': 'address',
    'phone': 'phone'
}


@memoize
def _get_all_locales():
    return set(get_all_locales())


class UserImporter(TopLevelMigrationStep):
    step_name = 'users'

    def __init__(self, *args, **kwargs):
        self.ldap_provider_name = kwargs.pop('ldap_provider_name')
        self.ignore_local_accounts = kwargs.pop('ignore_local_accounts')
        self.system_user_id = kwargs.pop('system_user_id')
        super(UserImporter, self).__init__(*args, **kwargs)

    def migrate(self):
        self.unresolved_merge_targets = defaultdict(set)
        self.favorite_avatars = {}
        self.migrate_users()
        self.fix_sequences('users', {'users'})
        self.migrate_favorite_users()
        self.migrate_admins()
        if 'groups' in self.zodb_root:
            self.migrate_groups()
        self.fix_sequences('users', {'groups'})
        self.migrate_system_user()
        self.global_ns.users_by_email = dict(self.global_ns.users_by_primary_email)
        self.global_ns.users_by_email.update(self.global_ns.users_by_secondary_email)
        # delete identities of deleted users. they should not have any since otherwise
        # a login using a remote provider fails instead of creating a new user for them
        Identity.query.filter(Identity.user.has(is_deleted=True)).delete(synchronize_session=False)
        db.session.commit()

    def migrate_system_user(self):
        if self.system_user_id is not None:
            user = User.get(self.system_user_id, is_deleted=False)
            if not user:
                raise Exception('Invalid system_user user id')
            user.is_system = True
            self.print_success('Using existing system user: {}'.format(user), always=True)
            return
        user_id = 0 if not User.get(0) else None
        db.session.add(User(id=user_id, is_system=True, first_name='Indico', last_name='System'))
        db.session.flush()
        self.print_success('Added new system user: {}'.format(User.get_system_user()), always=True)

    @step_description('Users')
    def migrate_users(self):
        seen_identities = set()

        for avatar in committing_iterator(self._iter_avatars(), 5000):
            if getattr(avatar, '_mergeTo', None):
                self.print_warning('Skipping {} - merged into {}'.format(avatar.id, avatar._mergeTo.id))
                merged_user = self.global_ns.avatar_merged_user.get(avatar._mergeTo.id)
                if merged_user:
                    self.global_ns.avatar_merged_user[avatar.id] = merged_user
                else:
                    # if the merge target hasn't yet been migrated, keep track of it
                    self.unresolved_merge_targets[avatar._mergeTo.id].add(avatar.id)
                continue
            elif avatar.status == 'Not confirmed':
                self.print_warning('Skipping {} - not activated'.format(avatar.id))
                continue
            elif not avatar.name.strip() and not avatar.surName.strip():
                links = {(obj, role): list(objs)
                         for obj, x in avatar.linkedTo.iteritems()
                         for role, objs in x.iteritems()
                         if objs}
                if not avatar.identities and not links:
                    self.print_warning('Skipping {} - no names and no identities/links'.format(avatar.id))
                    continue

            user = self._user_from_avatar(avatar)
            self._fix_collisions(user, avatar)
            db.session.add(user)
            settings = self._settings_from_avatar(avatar)
            user_settings.set_multi(user, settings)
            # favorite users cannot be migrated here since the target user might not have been migrated yet
            for old_categ in avatar.linkedTo['category']['favorite']:
                if old_categ:
                    self.global_ns.user_favorite_categories[old_categ.id].add(user)
            db.session.flush()
            self.print_success('%[white!]{:6d}%[reset] %[cyan]{}%[reset] [%[blue!]{}%[reset]] '
                               '{{%[cyan!]{}%[reset]}}'.format(user.id, user.full_name, user.email,
                                                               ', '.join(user.secondary_emails)))
            # migrate API keys
            self._migrate_api_keys(avatar, user)
            # migrate identities of avatars
            for old_identity in avatar.identities:
                identity = None
                username = convert_to_unicode(old_identity.login).strip().lower()

                if not username:
                    self.print_warning("Empty username: {}. Skipping identity.".format(old_identity))
                    continue

                provider = {
                    'LocalIdentity': 'indico',
                    'LDAPIdentity': self.ldap_provider_name
                }.get(old_identity.__class__.__name__)

                if provider is None:
                    self.print_error("Unsupported provider: {}. Skipping identity.".format(
                        old_identity.__class__.__name__))
                    continue

                if (provider, username) in seen_identities:
                    self.print_error("Duplicate identity: {}, {}. Skipping.".format(provider, username))
                    continue

                if provider == 'indico' and not self.ignore_local_accounts:
                    identity = Identity(provider=provider, identifier=username)

                    if not hasattr(old_identity, 'algorithm'):  # plaintext password
                        if not old_identity.password:
                            # password is empty, skip identity
                            self.print_error("Identity '{}' has empty password. Skipping identity.".format(
                                              old_identity.login))
                            continue
                        identity.password = old_identity.password
                    else:
                        assert old_identity.algorithm == 'bcrypt'
                        identity.password_hash = old_identity.password

                elif provider == self.ldap_provider_name:
                    identity = Identity(provider=provider, identifier=username)

                if identity:
                    self.print_info('%[blue!]<->%[reset]  %[yellow]{}%[reset]'.format(identity))
                    user.identities.add(identity)
                    seen_identities.add((provider, username))

            if hasattr(avatar, 'personalInfo') and avatar.personalInfo._basket._users:
                self.favorite_avatars[user.id] = avatar.personalInfo._basket._users

            # Map old merged identities (no longer in AvatarHolder)
            # to newly created user
            for merged_avatar in getattr(avatar, '_mergeFrom', ()):
                if merged_avatar.id != avatar.id:
                    self.global_ns.avatar_merged_user[merged_avatar.id] = user

            self.global_ns.avatar_merged_user[avatar.id] = user
            if avatar.id in self.unresolved_merge_targets:
                del self.unresolved_merge_targets[avatar.id]
                self._resolve_merge_targets(avatar.id, user)
        db.session.flush()

    def _resolve_merge_targets(self, avatar_id, user):
        for source_av, target_av in self.unresolved_merge_targets.items():
            if target_av == avatar_id:
                self.global_ns.avatar_merged_user[source_av] = user
                del self.unresolved_merge_targets[source_av]
                self._resolve_merge_targets(source_av, user)

    def _migrate_api_keys(self, avatar, user):
        ak = getattr(avatar, 'apiKey', None)
        if not ak:
            return
        last_used_uri = None
        if ak._lastPath and ak._lastQuery:
            last_used_uri = '{}?{}'.format(convert_to_unicode(ak._lastPath), convert_to_unicode(ak._lastQuery))
        elif ak._lastPath:
            last_used_uri = convert_to_unicode(ak._lastPath)

        api_key = APIKey(token=ak._key, secret=ak._signKey, is_blocked=ak._isBlocked,
                         is_persistent_allowed=getattr(ak, '_persistentAllowed', False),
                         created_dt=self._to_utc(ak._createdDT), last_used_dt=self._to_utc(ak._lastUsedDT),
                         last_used_ip=ak._lastUsedIP, last_used_uri=last_used_uri,
                         last_used_auth=ak._lastUseAuthenticated, use_count=ak._useCount)
        user.api_key = api_key
        self.print_info('%[blue!]<->%[reset]  %[yellow]{}%[reset]'.format(api_key))

        for old_key in ak._oldKeys:
            # We have no creation time so we use *something* older..
            fake_created_dt = self._to_utc(ak._createdDT) - timedelta(hours=1)
            # We don't have anything besides the api key for old keys, so we use a random secret
            user.old_api_keys.append(APIKey(token=old_key, secret=unicode(uuid4()), created_dt=fake_created_dt,
                                            is_active=False))

    @step_description('Favorite users')
    def migrate_favorite_users(self):
        users = {u.id: u for u in User.find(User.id.in_(set(self.favorite_avatars)))}
        for user_id, avatars in self.favorite_avatars.viewitems():
            user = users[user_id]
            self.print_success('%[white!]{:6d}%[reset] %[cyan]{}%[reset]'.format(user_id, user.full_name))
            for avatar_id in avatars:
                fav_user = self.global_ns.avatar_merged_user.get(avatar_id)
                if not fav_user:
                    self.print_warning('User not found: {} (in {})'.format(avatar_id, user_id))
                    continue
                user.favorite_users.add(fav_user)
                self.print_info(u'%[blue!]F%[reset] %[white!]{:6d}%[reset] %[cyan]{}%[reset]'
                                .format(fav_user.id, fav_user.full_name))
            # add the user to his/her own favorites
            user.favorite_users.add(user)
        db.session.flush()

    @step_description('Admins')
    def migrate_admins(self):
        for avatar in committing_iterator(self.zodb_root['adminlist']._AdminList__list):
            try:
                user = self.global_ns.avatar_merged_user[avatar.id]
            except ValueError:
                continue
            if user is None or user.is_deleted:
                continue
            user.is_admin = True
            self.print_success('%[cyan]{}'.format(user))
        db.session.flush()

    @step_description('Groups')
    def migrate_groups(self):
        it = committing_iterator(self.zodb_root['groups'].itervalues())
        used_names = set()
        for old_group in self.logger.progress_iterator('Migrating groups', it, len(self.zodb_root['groups']),
                                                       attrgetter('id'), lambda x: ''):
            if old_group.__class__.__name__ != 'Group':
                continue
            group_name = orig_group_name = convert_to_unicode(old_group.name).strip()
            n = 0
            while group_name.lower() in used_names:
                group_name = '{}-{}'.format(orig_group_name, n)
                n += 1
                self.print_warning('Duplicate group name: {}, using {} instead'.format(orig_group_name, group_name))
            used_names.add(group_name.lower())
            group = LocalGroup(id=int(old_group.id), name=group_name)
            self.print_success('%[white!]{:6d}%[reset] %[cyan]{}%[reset]'.format(group.id, group.name))
            members = set()
            for old_member in old_group.members:
                if old_member.__class__.__name__ != 'Avatar':
                    self.print_warning('Unsupported group member type: {}'.format(old_member.__class__.__name__))
                    continue
                user = self.global_ns.avatar_merged_user.get(old_member.id)
                if user is None:
                    self.print_warning('User not found: {}'.format(old_member.id))
                    continue
                members.add(user)
            for member in sorted(members, key=attrgetter('full_name')):
                self.print_info('%[blue!]<->%[reset]        %[white!]{:6d} %[yellow]{} ({})'.format(
                    member.id, member.full_name, member.email))
            group.members = members
            self.global_ns.all_groups[group.id] = group
            db.session.add(group)
        db.session.flush()

    def _user_from_avatar(self, avatar, **kwargs):
        email = sanitize_email(convert_to_unicode(avatar.email).lower().strip())
        secondary_emails = {sanitize_email(convert_to_unicode(x).lower().strip()) for x in avatar.secondaryEmails}
        secondary_emails = {x for x in secondary_emails if x and is_valid_mail(x, False) and x != email}
        # we handle deletion later. otherwise it might be set before secondary_emails which would
        # result in those emails not being marked as deleted
        user = User(id=int(avatar.id),
                    email=email,
                    first_name=convert_to_unicode(avatar.name).strip() or 'UNKNOWN',
                    last_name=convert_to_unicode(avatar.surName).strip() or 'UNKNOWN',
                    title=USER_TITLE_MAP.get(avatar.title, UserTitle.none),
                    phone=convert_to_unicode(avatar.telephone[0]).strip(),
                    affiliation=convert_to_unicode(avatar.organisation[0]).strip(),
                    address=convert_to_unicode(avatar.address[0]).strip(),
                    secondary_emails=secondary_emails,
                    is_blocked=avatar.status == 'disabled',
                    is_deleted=False,
                    **kwargs)
        if not is_valid_mail(user.email):
            user.is_deleted = True
        return user

    def _settings_from_avatar(self, avatar):
        timezone = avatar.timezone
        if not timezone or timezone not in all_timezones_set:
            timezone = getattr(self.zodb_root['MaKaCInfo']['main'], '_timezone', 'UTC')
        language = avatar._lang

        if language not in _get_all_locales():
            language = getattr(self.zodb_root['MaKaCInfo']['main'], '_lang', 'en_GB')
        show_past_events = False

        if hasattr(avatar, 'personalInfo'):
            show_past_events = bool(getattr(avatar.personalInfo, '_showPastEvents', False))

        settings = {
            'lang': language,
            'timezone': timezone,
            'force_timezone': avatar.displayTZMode == 'MyTimezone',
            'show_past_events': show_past_events,
        }

        unlocked_fields = {SYNCED_FIELD_MAP.get(field) for field in getattr(avatar, 'unlockedFields', [])} - {None}
        if unlocked_fields:
            settings['synced_fields'] = list(set(SYNCED_FIELD_MAP.viewvalues()) - unlocked_fields)

        return settings

    def _fix_collisions(self, user, avatar):
        is_deleted = user.is_deleted
        # Mark both users as deleted if there's a primary email collision
        coll = self.global_ns.users_by_primary_email.get(user.email)
        if coll and not is_deleted:
            if bool(avatar.identities) ^ bool(coll.identities):
                # exactly one of them has identities - keep the one that does
                to_delete = {coll if avatar.identities else user}
            else:
                to_delete = {user, coll}
            for u in to_delete:
                self.print_log('%[magenta!]---%[reset] %[yellow!]Deleting {} - primary email collision%[reset] '
                               '[%[blue!]{}%[reset]]'.format(u.id, u.email))
                u.is_deleted = True
                db.session.flush()
        # if the user was already deleted we don't care about primary email collisions
        if not is_deleted:
            self.global_ns.users_by_primary_email[user.email] = user

        # Remove primary email from another user's secondary email list
        coll = self.global_ns.users_by_secondary_email.get(user.email)
        if coll and user.merged_into_id != coll.id:
            self.print_log('%[magenta!]---%[reset] %[yellow!]1 Removing colliding secondary email (P/S from {}%[reset] '
                           '[%[blue!]{}%[reset]])'.format(coll, user.email))
            coll.secondary_emails.remove(user.email)
            del self.global_ns.users_by_secondary_email[user.email]
            db.session.flush()

        # Remove email from both users if there's a collision
        for email in list(user.secondary_emails):
            # colliding with primary email
            coll = self.global_ns.users_by_primary_email.get(email)
            if coll:
                self.print_log('%[magenta!]---%[reset] %[yellow!]Removing colliding secondary email '
                               '(S/P from {}%[reset] [%[blue!]{}%[reset]])'.format(user, email))
                user.secondary_emails.remove(email)
                db.session.flush()
            # colliding with a secondary email
            coll = self.global_ns.users_by_secondary_email.get(email)
            if coll:
                self.print_log('%[magenta!]---%[reset] %[yellow!]Removing colliding secondary email '
                               '(S/S from {}%[reset] [%[blue!]{}%[reset]])'.format(user, email))
                user.secondary_emails.remove(email)
                db.session.flush()
                self.global_ns.users_by_secondary_email[email] = coll
            # if the user was already deleted we don't care about secondary email collisions
            if not is_deleted and email in user.secondary_emails:
                self.global_ns.users_by_secondary_email[email] = user

    def _to_utc(self, dt):
        if dt is None:
            return None
        server_tz = get_timezone(getattr(self.zodb_root['MaKaCInfo']['main'], '_timezone', 'UTC'))
        return server_tz.localize(dt).astimezone(pytz.utc)

    def _iter_avatars(self):
        it = self.zodb_root['avatars'].itervalues()
        if self.quiet:
            it = self.logger.progress_iterator('Migrating users', it, len(self.zodb_root['avatars']), attrgetter('id'),
                                               lambda x: '')
        return it

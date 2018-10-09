
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
from indico.core.db.sqlalchemy.principals import EmailPrincipal
from indico.core.db.sqlalchemy.protection import ProtectionMode
from indico.modules.events.models.principals import EventPrincipal
from indico.util.string import is_valid_mail, sanitize_email

from indico_migrate.steps.events import EventMigrationStep
from indico_migrate.util import patch_default_group_provider, convert_to_unicode


PROTECTION_MODE_MAP = {-1: ProtectionMode.public, 0: ProtectionMode.inheriting, 1: ProtectionMode.protected}


class EventACLImporter(EventMigrationStep):
    step_id = 'acl'

    def process_principal(self, principals, legacy_principal, name, color, full_access=None, roles=None,
                          read_access=None):
        if isinstance(legacy_principal, basestring):
            user = self.global_ns.users_by_email.get(legacy_principal)
            principal = user or EmailPrincipal(legacy_principal)
        else:
            principal = self.convert_principal(legacy_principal)
        if principal is None:
            self.print_warning(('%%[%s]{}%%[reset]%%[yellow] does not exist:%%[reset] {} ({})' % color)
                               .format(name, legacy_principal, getattr(legacy_principal, 'id', '-')))
            return
        try:
            entry = principals[principal]
        except KeyError:
            entry = EventPrincipal(event_id=self.event.id, principal=principal, full_access=False, roles=[])
            principals[principal] = entry
        if full_access:
            entry.full_access = True
        if read_access:
            entry.read_access = True
        if roles:
            entry.roles = sorted(set(entry.roles) | set(roles))
        if not self.quiet:
            self.print_log(('%%[%s][{}]%%[reset] {}' % color).format(name.lower(), principal))
        return principal

    def process_emails(self, principals, emails, name, color, full_access=None, roles=None):
        emails = {sanitize_email(convert_to_unicode(email).lower()) for email in emails}
        emails = {email for email in emails if is_valid_mail(email, False)}
        for email in emails:
            self.process_principal(principals, email, name, color, full_access, roles)

    def migrate(self):
        ac = self.conf._Conference__ac
        old_protection_mode = PROTECTION_MODE_MAP[ac._accessProtection]
        entries = {}
        # add creator as a manager
        try:
            creator = self.conf._Conference__creator
        except AttributeError:
            # events created after the removal of the `self.__creator` assignment
            # should happen only on dev machines
            self.print_error('Event has no creator attribute')
        else:
            user = self.process_principal(entries, creator, 'Creator', 'green!', full_access=True)
            if user:
                self.event.creator = user
            else:
                self.event.creator = self.system_user
                self.print_warning('Event {} has no creator'.format(self.event.id))

        if old_protection_mode == ProtectionMode.public and ac.requiredDomains:
            self.event.protection_mode = ProtectionMode.protected
            self._migrate_domains(ac.requiredDomains)
        else:
            self.event.protection_mode = old_protection_mode
        if not self.quiet:
            self.print_success('Protection mode set to {}'.format(self.event.protection_mode.name))

        no_access_contact = convert_to_unicode(getattr(ac, 'contactInfo', ''))
        if no_access_contact != 'no contact info defined':
            self.event.own_no_access_contact = no_access_contact
        self.event.access_key = convert_to_unicode(getattr(self.conf, '_accessKey', ''))

        with patch_default_group_provider(self.default_group_provider):

            for allowed in ac.allowed:
                self.process_principal(entries, allowed, 'Access', 'blue!', read_access=True)

            # add managers
            for manager in ac.managers:
                self.process_principal(entries, manager, 'Manager', 'blue!', full_access=True)
            # add email-based managers
            emails = getattr(ac, 'managersEmail', [])
            self.process_emails(entries, emails, 'Manager', 'green', full_access=True)
            # add registrars
            for registrar in getattr(self.conf, '_Conference__registrars', []):
                self.process_principal(entries, registrar, 'Registrar', 'cyan', roles={'registration'})
            # add submitters
            for submitter in getattr(ac, 'submitters', []):
                self.process_principal(entries, submitter, 'Submitter', 'magenta!', roles={'submit'})
            # email-based (pending) submitters
            pqm = getattr(self.conf, '_pendingQueuesMgr', None)
            if pqm is not None:
                emails = set(getattr(pqm, '_pendingConfSubmitters', []))
                self.process_emails(entries, emails, 'Submitter', 'magenta', roles={'submit'})
            db.session.add_all(entries.itervalues())

    def _migrate_domains(self, old_domains):
        for old_domain in old_domains:
            domain_name = convert_to_unicode(old_domain.name).lower()
            network = self.global_ns.ip_domains.get(domain_name)
            if not network:
                self.print_warning('Skipping unknown protection domain: {}'.format(domain_name))
                continue
            self.event.update_principal(network, read_access=True, quiet=True)
            if not self.quiet:
                self.print_success('Adding {} IPNetworkGroup to the ACLs'.format(network))

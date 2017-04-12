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

import re
from operator import itemgetter

from indico.core.db import db
from indico.core.db.sqlalchemy.principals import EmailPrincipal
from indico.modules.events.models.events import EventType
from indico.modules.events.models.principals import EventPrincipal
from indico.util.console import cformat, verbose_iterator
from indico.util.string import is_valid_mail, sanitize_email
from indico_migrate import convert_to_unicode
from indico_migrate.steps.events import EventMigrationStep
from indico_migrate.util import patch_default_group_provider

WEBFACTORY_NAME_RE = re.compile(r'^MaKaC\.webinterface\.(\w+)(?:\.WebFactory)?$')


class EventManagerImporter(EventMigrationStep):
    def setup(self):
        # keep all users in memory to avoid extra queries.
        self.all_users_by_email = dict(self.global_maps.users_by_primary_email)
        self.all_users_by_email.update(self.global_maps.users_by_secondary_email)

    def process_principal(self, event, principals, legacy_principal, name, color, full_access=None, roles=None):
        if isinstance(legacy_principal, basestring):
            user = self.all_users_by_email.get(legacy_principal)
            principal = user or EmailPrincipal(legacy_principal)
        else:
            principal = self.convert_principal(legacy_principal)
        if principal is None:
            self.print_warning(cformat('%%{%s}{}%%{reset}%%{yellow} does not exist:%%{reset} {} ({})' % color)
                               .format(name, legacy_principal, legacy_principal.id), event_id=event.id)
            return
        try:
            entry = principals[principal]
        except KeyError:
            entry = EventPrincipal(event_id=event.id, principal=principal, full_access=False, roles=[])
            principals[principal] = entry
        if full_access:
            entry.full_access = True
        if roles:
            entry.roles = sorted(set(entry.roles) | set(roles))
        if not self.quiet:
            self.print_msg(cformat('      %%{%s}[{}]%%{reset} {}' % color).format(name.lower(), principal))
        return principal

    def process_emails(self, event, principals, emails, name, color, full_access=None, roles=None):
        emails = {sanitize_email(convert_to_unicode(email).lower()) for email in emails}
        emails = {email for email in emails if is_valid_mail(email, False)}
        for email in emails:
            self.process_principal(event, principals, email, name, color, full_access, roles)

    def migrate(self, conf, event):
        ac = conf._Conference__ac
        entries = {}
        # add creator as a manager
        try:
            creator = conf._Conference__creator
        except AttributeError:
            # events created after the removal of the `self.__creator` assignment
            # should happen only on dev machines
            self.print_error(cformat('%{red!}Event has no creator attribute'), event_id=conf.id)
        else:
            user = self.process_principal(event, entries, creator, 'Creator', 'green!', full_access=True)
            if user:
                event.creator = user
            else:
                event.creator = self.janitor
                self.print_warning('Event {} has no creator'.format(event.id))

        with patch_default_group_provider(self.default_group_provider):
            # add managers
            for manager in ac.managers:
                self.process_principal(event, entries, manager, 'Manager', 'blue!', full_access=True)
            # add email-based managers
            emails = getattr(ac, 'managersEmail', [])
            self.process_emails(event, entries, emails, 'Manager', 'green', full_access=True)
            # add registrars
            for registrar in getattr(conf, '_Conference__registrars', []):
                self.process_principal(event, entries, registrar, 'Registrar', 'cyan', roles={'registration'})
            # add submitters
            for submitter in getattr(ac, 'submitters', []):
                self.process_principal(event, entries, submitter, 'Submitter', 'magenta!', roles={'submit'})
            # email-based (pending) submitters
            pqm = getattr(conf, '_pendingQueuesMgr', None)
            if pqm is not None:
                emails = set(getattr(pqm, '_pendingConfSubmitters', []))
                self.process_emails(event, entries, emails, 'Submitter', 'magenta', roles={'submit'})
            db.session.add_all(entries.itervalues())


class EventTypeImporter(EventMigrationStep):
    def setup(self):
        self.print_info("Fetching data from WF registry")
        self.wf_registry = {}
        for event_id, wf in self._iter_wfs():
            if wf is None:
                # conferences that have been lectures/meetings in the past
                continue

            wf_id = WEBFACTORY_NAME_RE.match(wf.__module__).group(1)
            if wf_id in ('simple_event', 'meeting'):
                self.wf_registry[event_id] = wf_id
            else:
                self.print_error('Unexpected WF ID: {}'.format(wf_id), event_id=event_id)

    def migrate(self, conf, event):
        wf_entry = self.wf_registry.get(conf.id)
        if wf_entry is None:
            event._type = EventType.conference
        else:
            event._type = EventType.lecture if wf_entry == 'simple_event' else EventType.meeting

    def _iter_wfs(self):
        it = self.zodb_root['webfactoryregistry'].iteritems()
        total = len(self.zodb_root['webfactoryregistry'])
        if not self.quiet:
            it = verbose_iterator(it, total, itemgetter(0), lambda x: '')
        for conf_id, wf in it:
            if conf_id.isdigit():
                yield conf_id, wf

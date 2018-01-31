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
from HTMLParser import HTMLParser
from ipaddress import ip_network
from operator import attrgetter

from indico.core.db import db
from indico.modules.api import api_settings
from indico.modules.cephalopod import cephalopod_settings
from indico.modules.core.settings import core_settings, social_settings
from indico.modules.events.models.references import ReferenceType
from indico.modules.events.payment import payment_settings
from indico.modules.legal import legal_settings
from indico.modules.networks.models.networks import IPNetworkGroup
from indico.modules.news import news_settings
from indico.modules.news.models.news import NewsItem
from indico.modules.users import user_management_settings
from indico.util.string import strip_tags

from indico_migrate.importer import TopLevelMigrationStep
from indico_migrate.util import convert_to_unicode, step_description


def _sanitize_title(title, _ws_re=re.compile(r'\s+')):
    title = convert_to_unicode(title)
    title = HTMLParser().unescape(strip_tags(title))
    return _ws_re.sub(' ', title).strip()


class GlobalPreEventsImporter(TopLevelMigrationStep):
    step_name = 'global_pre'

    def __init__(self, *args, **kwargs):
        self.reference_types = kwargs.pop('reference_types')
        self.default_currency = kwargs.pop('default_currency')
        super(GlobalPreEventsImporter, self).__init__(*args, **kwargs)

    def migrate(self):
        self.migrate_global_ip_acl()
        self.migrate_api_settings()
        self.migrate_global_settings()
        self.migrate_user_management_settings()
        self.migrate_legal_settings()
        if 'EPayment' in self.zodb_root['plugins']:
            self.migrate_payment_settings()
        if 'news' in self.zodb_root['modules']:
            self.migrate_news_settings()
            self.migrate_news()
        if 'domains' in self.zodb_root:
            self.migrate_networks()
        self.migrate_reference_types()
        cephalopod_settings.set('show_migration_message', True)
        db.session.commit()

    @step_description('API settings')
    def migrate_api_settings(self):
        settings_map = {
            '_apiPersistentAllowed': 'allow_persistent',
            '_apiMode': 'security_mode',
            '_apiCacheTTL': 'cache_ttl',
            '_apiSignatureTTL': 'signature_ttl'
        }
        for old, new in settings_map.iteritems():
            api_settings.set(new, getattr(self.makac_info, old, None))

    @step_description('Global settings')
    def migrate_global_settings(self):
        core_settings.set_multi({
            'site_title': convert_to_unicode(self.makac_info._title),
            'site_organization': convert_to_unicode(self.makac_info._organisation)
        })
        social_settings.set_multi({
            'enabled': bool(self.makac_info._socialAppConfig['active']),
            'facebook_app_id': convert_to_unicode(self.makac_info._socialAppConfig['facebook'].get('appId'))
        })

    @step_description('User management settings')
    def migrate_user_management_settings(self):
        settings_dict = {
            '_notifyAccountCreation': 'notify_account_creation'
        }

        for old_setting_name, new_setting_name in settings_dict.iteritems():
            user_management_settings.set(new_setting_name, getattr(self.makac_info, old_setting_name))

    @step_description('Legal settings')
    def migrate_legal_settings(self):
        settings_map = {
            '_protectionDisclaimerProtected': 'network_protected_disclaimer',
            '_protectionDisclaimerRestricted': 'restricted_disclaimer'
        }
        for old, new in settings_map.iteritems():
            legal_settings.set(new, convert_to_unicode(getattr(self.makac_info, old, '')))

    @step_description('Payment settings')
    def migrate_payment_settings(self):
        currency_opt = self.zodb_root['plugins']['EPayment']._PluginBase__options['customCurrency']
        currencies = [{'code': oc['abbreviation'], 'name': oc['name']} for oc in currency_opt._PluginOption__value]

        payment_settings.set('currencies', currencies)
        for currency in currencies:
            self.print_info(("saving currency: name='{name}', code={code}").format(**currency))
        payment_settings.set('currency', self.default_currency)
        self.print_info(("default currency: {}").format(self.default_currency))

        db.session.commit()

    @step_description('Global IP acl')
    def migrate_global_ip_acl(self):
        ip_networks = filter(None, map(self._to_network, self.makac_info._ip_based_acl_mgr._full_access_acl))
        if not ip_networks:
            self.print_error('%[red]No valid IPs found')
            return
        network = IPNetworkGroup(name='Full Attachment Access', hidden=True, attachment_access_override=True,
                                 description='IPs that can access all attachments without authentication',
                                 networks=ip_networks)
        db.session.add(network)
        db.session.flush()
        self.print_success(repr(network))

    @step_description('IP Networks')
    def migrate_networks(self):
        for domain in self._iter_domains():
            ip_networks = filter(None, map(self._to_network, set(domain.filterList)))
            if not ip_networks:
                self.print_warning('%[yellow]Domain has no valid IPs: {}'
                                   .format(convert_to_unicode(domain.name)))
            network = IPNetworkGroup(name=convert_to_unicode(domain.name),
                                     description=convert_to_unicode(domain.description), networks=ip_networks)
            db.session.add(network)
            self.global_ns.ip_domains[convert_to_unicode(domain.name).lower()] = network
            self.print_success(repr(network))
        db.session.flush()

    @step_description('News')
    def migrate_news(self):
        old_items = sorted(self.zodb_root['modules']['news']._newsItems, key=attrgetter('_creationDate'))
        for old_item in old_items:
            n = NewsItem(title=_sanitize_title(old_item._title), content=convert_to_unicode(old_item._content),
                         created_dt=old_item._creationDate)
            db.session.add(n)
            db.session.flush()
            self.print_success(n.title)

    @step_description('News settings')
    def migrate_news_settings(self):
        mod = self.zodb_root['modules']['news']
        news_settings.set('show_recent', bool(self.makac_info._newsActive))
        news_settings.set('new_days', int(mod._recentDays))

    @step_description('Reference types')
    def migrate_reference_types(self):
        for name in self.reference_types:
            self.global_ns.reference_types[name] = reftype = ReferenceType(name=name)
            db.session.add(reftype)
            self.print_success(name)
        db.session.commit()

    def _iter_domains(self):
        return self.zodb_root['domains'].itervalues()

    def _to_network(self, mask):
        mask = convert_to_unicode(mask).strip()
        net = None
        if re.match(r'^[0-9.]+$', mask):
            # ipv4 mask
            mask = mask.rstrip('.')
            segments = mask.split('.')
            if len(segments) <= 4:
                addr = '.'.join(segments + ['0'] * (4 - len(segments)))
                net = ip_network('{}/{}'.format(addr, 8 * len(segments)))
        elif re.match(r'^[0-9a-f:]+', mask):
            # ipv6 mask
            mask = mask.rstrip(':')  # there shouldn't be a `::` in the IP as it was a startswith-like check before
            segments = mask.split(':')
            if len(segments) <= 8:
                addr = ':'.join(segments + ['0'] * (8 - len(segments)))
                net = ip_network('{}/{}'.format(addr, 16 * len(segments)))
        if net is None:
            self.print_warning('%[yellow!]Skipped invalid mask: {}'.format(mask))
        return net

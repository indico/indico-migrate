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

from persistent import Persistent
from yaml import Dumper, Loader

from indico.core.db import db


def sqlalchemy_representer(dumper, obj):
    return dumper.represent_sequence('!sqlalchemy', [obj.__class__.__name__,
                                                     obj.id])


def zodb_representer(dumper, obj):
    return dumper.represent_sequence('!zodb', [obj._p_oid])


def sqlalchemy_constructor(loader, node):
    data = loader.construct_sequence(node)
    if data[1] is None:
        # object was never persisted
        return None
    return getattr(db.m, data[0]).get(data[1])


def zodb_constructor(loader, node):
    oid = loader.construct_sequence(node)[0]
    return loader.zodb_root._p_jar[oid]


Dumper.add_multi_representer(db.Model, sqlalchemy_representer)
Dumper.add_multi_representer(Persistent, zodb_representer)
Loader.add_constructor('!sqlalchemy', sqlalchemy_constructor)
Loader.add_constructor('!zodb', zodb_constructor)


STORE_MAP = {
    'setdict': lambda: defaultdict(set)
}


class SharedNamespace(object):
    def __init__(self, name, zodb_root, store_types):
        self.name = name
        self._store_types = store_types
        self._stores = {k: STORE_MAP.get(ktype, ktype)() for k, ktype in store_types.viewitems()}

    def __getattr__(self, key):
        return self._stores[key]

    def serialize(self):
        return {k: store for k, store in self._stores.viewitems()}

    def load(self, data):
        self._stores.update(data)

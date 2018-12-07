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

import errno
import hashlib
import os
import re
import sys
from contextlib import contextmanager
from datetime import timedelta
from functools import wraps
from HTMLParser import HTMLParser
from urlparse import urlparse
from uuid import uuid4

import click
import yaml
from colorclass import Color
from termcolor import colored
from ZEO.ClientStorage import ClientStorage
from ZODB import DB, FileStorage
from ZODB.broken import Broken, find_global

from indico.core.auth import IndicoMultipass
from indico.util.caching import memoize
from indico.util.date_time import now_utc
from indico.util.string import sanitize_email, strip_tags


WHITESPACE_RE = re.compile(r'\s+')

_last_dt = None


def _cformat_sub(m):
    bg = u'on_{}'.format(m.group('bg')) if m.group('bg') else None
    attrs = ['bold'] if m.group('fg_bold') else None
    return colored(u'', m.group('fg'), bg, attrs=attrs)[:-4]


def cformat2(string):
    """Replaces %{color} and %{color,bgcolor} with ansi colors.

    Bold foreground can be achieved by suffixing the color with a '!'
    """
    reset = colored(u'')
    string = string.replace(u'%[reset]', reset)
    string = re.sub(ur'%\[(?P<fg>[a-z]+)(?P<fg_bold>!?)(?:,(?P<bg>[a-z]+))?\]', _cformat_sub, string)
    if not string.endswith(reset):
        string += reset
    return Color(string)


class NotBroken(Broken):
    """Like Broken, but it makes the attributes available"""

    def __setstate__(self, state):
        self.__dict__.update(state)


class UnbreakingDB(DB):
    def classFactory(self, connection, modulename, globalname):
        modulename = re.sub(r'^IndexedCatalog\.BTrees\.', 'BTrees.', modulename)
        if globalname == 'PersistentMapping':
            modulename = 'persistent.mapping'
        elif globalname == 'PersistentList':
            modulename = 'persistent.list'
        elif globalname == 'LDAPGroupWrapper':
            modulename = 'indico_migrate.zodb_objects'
            globalname = 'LDAPGroupWrapper'
        return find_global(modulename, globalname, Broken=NotBroken)


def get_storage(zodb_uri):
    uri_parts = urlparse(str(zodb_uri))

    print cformat2("%[green]Trying to open {}...").format(zodb_uri)

    if uri_parts.scheme == 'zeo':
        if uri_parts.port is None:
            print cformat2("%[yellow]No ZEO port specified. Assuming 9675")

        storage = ClientStorage((uri_parts.hostname, uri_parts.port or 9675),
                                username=uri_parts.username,
                                password=uri_parts.password,
                                realm=uri_parts.path[1:])

    elif uri_parts.scheme in ('file', None):
        storage = FileStorage.FileStorage(uri_parts.path)
    else:
        raise Exception("URI scheme not known: {}".format(uri_parts.scheme))
    print cformat2("%[green]Done!")
    return storage


def convert_to_unicode(val, strip=True, _control_char_re=re.compile(ur'[\x00-\x08\x0b-\x0c\x0e-\x1f]')):
    if isinstance(val, str):
        try:
            rv = unicode(val, 'utf-8')
        except UnicodeError:
            rv = unicode(val, 'latin1')
    elif isinstance(val, unicode):
        rv = val
    elif isinstance(val, int):
        rv = unicode(val)
    elif val is None:
        rv = u''
    else:
        raise RuntimeError('Unexpected type {} is found for unicode conversion: {!r}'.format(type(val), val))
    # get rid of hard tabs and control chars
    rv = rv.replace(u'\t', u' ' * 4)
    rv = _control_char_re.sub(u'', rv)
    if strip:
        rv = rv.strip()
    return rv


def option_value(opt):
    """Gets a plugin option value"""
    value = opt._PluginOption__value
    if isinstance(value, basestring):
        value = convert_to_unicode(value)
    return value


def get_archived_file(f, archive_paths):
    """Returns the name and path of an archived file

    :param f: A `LocalFile` object
    :param archive_paths: The path that was used in the ``ArchiveDir``
                          config option ot a list of multiple paths.
    """
    # this is based pretty much on MaterialLocalRepository.__getFilePath, but we don't
    # call any legacy methods in ZODB migrations to avoid breakage in the future.
    if f is None:
        return None, None
    if isinstance(archive_paths, basestring):
        archive_paths = [archive_paths]
    archive_id = f._LocalFile__archivedId
    repo = f._LocalFile__repository
    for archive_path in archive_paths:
        path = os.path.join(archive_path.encode('ascii'), repo._MaterialLocalRepository__files[archive_id])
        if os.path.exists(path):
            return f.fileName, path
        for mode, enc in (('strict', 'iso-8859-1'), ('replace', sys.getfilesystemencoding()), ('replace', 'ascii')):
            enc_path = path.decode('utf-8', mode).encode(enc, 'replace')
            if os.path.exists(enc_path):
                return f.fileName, enc_path
    return f.fileName, None


@contextmanager
def patch_default_group_provider(provider_name):
    """Monkeypatches Multipass to use a certain default group provider"""
    class FakeProvider(object):
        name = provider_name
    provider = FakeProvider()
    prop = IndicoMultipass.default_group_provider
    IndicoMultipass.default_group_provider = property(lambda m: provider)
    try:
        yield
    finally:
        IndicoMultipass.default_group_provider = prop


def get_file_md5(path, chunk_size=1024*1024):
    checksum = hashlib.md5()
    with open(path, 'rb') as fileobj:
        while True:
            chunk = fileobj.read(chunk_size)
            if not chunk:
                break
            checksum.update(chunk)
    return unicode(checksum.hexdigest())


class LocalFileImporterMixin(object):
    """This mixin takes care of interpreting arcane LocalFile information,
       handling incorrectly encoded paths and other artifacts.
       Several usage options are added to the CLI (see below).
    """

    def _set_config_options(self, **kwargs):
        self.archive_dirs = kwargs.pop('archive_dir')
        self.avoid_storage_check = kwargs.pop('avoid_storage_check')
        self.symlink_backend = kwargs.pop('symlink_backend')
        self.symlink_target = kwargs.pop('symlink_target', None)
        self.storage_backend = kwargs.pop('storage_backend')

        if (self.avoid_storage_check or self.symlink_target) and len(self.archive_dirs) != 1:
            raise click.exceptions.UsageError('Invalid number of archive-dirs for --no-storage-access or '
                                              '--symlink-target')
        if bool(self.symlink_target) != bool(self.symlink_backend):
            raise click.exceptions.UsageError('Both or none of --symlink-target and --symlink-backend must be used.')
        return kwargs

    def _get_local_file_info(self, resource, force_access=False):
        archive_id = resource._LocalFile__archivedId
        repo_path = resource._LocalFile__repository._MaterialLocalRepository__files[archive_id]
        for archive_path in map(bytes, self.archive_dirs):
            path = os.path.join(archive_path, repo_path)
            if any(ord(c) > 127 for c in repo_path):
                foobar = (('strict', 'iso-8859-1'), ('replace', sys.getfilesystemencoding()), ('replace', 'ascii'))
                for mode, enc in foobar:
                    try:
                        dec_path = path.decode('utf-8', mode)
                    except UnicodeDecodeError:
                        dec_path = path.decode('iso-8859-1', mode)
                    enc_path = dec_path.encode(enc, 'replace')
                    if os.path.exists(enc_path):
                        path = enc_path
                        break
                else:
                    parent_path = os.path.dirname(path)
                    try:
                        candidates = os.listdir(parent_path)
                    except OSError as e:
                        if e.errno != errno.ENOENT:
                            raise
                        return None, None, 0, ''
                    if len(candidates) != 1:
                        return None, None, 0, ''
                    path = os.path.join(parent_path, candidates[0])
                    if not os.path.exists(path):
                        return None, None, 0, ''

            assert path
            try:
                size = 0 if (self.avoid_storage_check and not force_access) else os.path.getsize(path)
                md5 = '' if (self.avoid_storage_check and not force_access) else get_file_md5(path)
            except OSError as e:
                if e.errno != errno.ENOENT:
                    raise
                return None, None, 0, ''
            rel_path = os.path.relpath(path, archive_path)
            try:
                rel_path = rel_path.decode('utf-8')
            except UnicodeDecodeError:
                if not self.symlink_target:
                    return None, None, 0, ''
                symlink_name = uuid4()
                symlink = os.path.join(self.symlink_target, bytes(symlink_name))
                os.symlink(path, symlink)
                return self.symlink_backend, symlink_name, size, md5
            else:
                return self.storage_backend, rel_path, size, md5


def strict_sanitize_email(email, fallback=None):
    return sanitize_email(convert_to_unicode(email).lower(), require_valid=True) or fallback


@memoize
def sanitize_user_input(string, html=False):
    string = convert_to_unicode(string)
    if not html:
        string = HTMLParser().unescape(strip_tags(string))
    return WHITESPACE_RE.sub(' ', string).strip()


def strict_now_utc():
    """Return strictly increasing now_utc() values"""
    global _last_dt
    dt = now_utc()
    if _last_dt and (dt - _last_dt) < timedelta(seconds=1):
        dt += timedelta(seconds=1)
    _last_dt = dt
    return dt


class MigrationStateManager(object):
    _namespaces = {}
    _steps = []

    @classmethod
    def register_step(cls, step):
        cls._steps.append(step.__name__)

    @classmethod
    def has_already_run(cls, step):
        return step.__name__ in cls._steps

    @classmethod
    def register_ns(cls, ns):
        cls._namespaces[ns.name] = ns

    @classmethod
    def save_restore_point(cls, fd):
        ns_data = {ns.name: ns.serialize() for ns in cls._namespaces.viewvalues()}
        yaml.dump({
            'namespaces': ns_data,
            'steps': cls._steps
        }, fd)

    @classmethod
    def load_restore_point(cls, data):
        cls._steps = data['steps']
        for name, ns in cls._namespaces.viewitems():
            ns.load(data['namespaces'][name])


def step_description(description):
    def _step_description(f):

        @wraps(f)
        def _f(self, *args, **kwargs):
            self.logger.print_step(description)
            f(self, *args, **kwargs)
        return _f
    return _step_description

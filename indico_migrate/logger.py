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
import shutil
import sys
from io import BytesIO

from indico.util.console import clear_line, verbose_iterator

from indico_migrate.util import cformat2


CFORMAT_TAGS = re.compile(r'%\[[a-z]+!?(?:,[a-z]+)?\]')


def strip_cformat(text):
    return CFORMAT_TAGS.sub('', text)


def logger_proxy(msg_type):
    def _log_message(importer, *args, **kwargs):
        return getattr(importer.logger, 'print_' + msg_type)(*args, prefix=importer.log_prefix, **kwargs)
    return _log_message


class BaseLogger(object):
    def __init__(self, quiet):
        self.quiet = quiet
        self.buffer = BytesIO()

    def shutdown(self):
        pass

    def save_exception(self, stack):
        self.buffer.write(b'\n\n' + stack.encode('utf-8') + b'\n')

    def save_to_disk(self):
        with open('migration.log', 'w') as f:
            self.buffer.seek(0)
            shutil.copyfileobj(self.buffer, f)

    def wait_for_input(self):
        pass

    def fatal_error(self, message):
        for line in message.split('\n'):
            print(cformat2('%[red!]***%[reset] ') + line)
        sys.exit(-1)

    def print_success(self, msg, always=False, prefix='', event_id=''):
        self.print_msg('%[green]\u2713%[reset]', msg, always=always, prefix=prefix, event_id=event_id)

    def print_error(self, msg, always=True, prefix='', event_id=''):
        self.print_msg('%[red]\u00d7%[reset]', msg, always=always, prefix=prefix, event_id=event_id)

    def print_warning(self, msg, always=True, prefix='', event_id=''):
        self.print_msg('%[yellow!]!%[reset]', msg, always=always, prefix=prefix, event_id=event_id)

    def print_info(self, msg, always=False, prefix='', event_id=''):
        self.print_msg('%[blue!]i%[reset]', msg, always=always, prefix=prefix, event_id=event_id)

    def print_log(self, msg, always=False, prefix='', event_id=''):
        self.print_msg('%[magenta!]-%[reset]', msg, always=always, prefix=prefix, event_id=event_id)

    def print_msg(self, icon, msg, always=False, prefix='', event_id=''):
        """Write the message to both the screen and the internal buffer."""
        if always or not self.quiet:
            self._print_to_buffer(icon, msg, always, prefix, event_id)
        self._print_msg(icon, msg, always=always, prefix=prefix, event_id=event_id)

    def _print_msg(self, icon, msg, always=False, prefix='', event_id=''):
        raise NotImplemented

    def _print_to_buffer(self, icon, msg, always, prefix, event_id):
        suffix = ''
        if event_id:
            suffix = ' [{}]'.format(event_id)
        if prefix:
            prefix += ' '
        self.buffer.write(strip_cformat(icon + ' ' + prefix + msg + suffix + '\n').encode('utf-8'))


class StdoutLogger(BaseLogger):
    def _print_msg(self, icon, msg, always=False, prefix='', event_id=''):
        """Prints a message to the console.

        By default, messages are not shown in quiet mode, but this
        can be changed using the `always` parameter.
        """
        if self.quiet:
            if not always:
                return
            clear_line()

        suffix = ''
        if event_id:
            suffix = ' %[cyan][%[cyan!]{}%[cyan]]%[reset]'.format(event_id)
        if prefix:
            prefix += ' '
        print cformat2(icon + ' ' + prefix + msg + suffix).encode('utf-8')

    def print_step(self, msg):
        self.print_msg('%[cyan,blue] > %[cyan!,blue]', '{:<30}'.format(msg), always=True)

    def progress_iterator(self, description, iterable, total, get_id, get_title, print_every=10):
        return verbose_iterator(iterable, total, get_id, get_title, print_every=print_every)

    def set_success(self):
        self.print_success('%[green!]Migration finished!', always=True)

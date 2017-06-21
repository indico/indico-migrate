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

import inspect

import click
from github3 import GitHub

import indico

from indico_migrate.util import cformat2
from indico_migrate.version import __version__


def get_full_stack():
    result = 'Traceback (most recent call last):'
    for item in reversed(inspect.stack()[2:]):
        result += ' |{1}:{2} [{3}]\n'.format(*item)
    for line in item[4]:
        result += ' ' + line.lstrip()
    for item in inspect.trace():
        result += ' |{1}:{2} [{3}]\n'.format(*item)
    for line in item[4]:
        result += ' ' + line.lstrip()
    return result


def ask_to_paste(buffer):
    print
    print cformat2('%[yellow]*** %[red]ERROR')
    print cformat2('%[yellow]*** %[white]There has been an unexpected error during the migration.')
    print cformat2("%[yellow]*** %[white]You may choose to post an error report on %[cyan]gist.github.com%[reset] "
                   "which you can then send to the Indico Team for bug debugging purposes.")
    print cformat2("%[yellow]*** %[white]The URL won't be publicly advertised and %[yellow]only data "
                   "that was shown on the screen will be sent%[white].\n")
    if click.confirm('Do you wish to submit the error report?'):
        buffer.seek(0)
        return post_gist(buffer.read())
    else:
        return False


def post_gist(text):
    """Post error information to Gist"""
    # requests.post(STIKKED_URL + '/api/create')

    files = {
        'debug.txt': {
            'content': text
            }
        }

    gh = GitHub()
    gist = gh.create_gist('indico-migrate {} / indico {}'.format(__version__, indico.__version__), files, public=False)

    print
    print '\nThe URL of the error report is:\n'
    print cformat2("%[cyan]" + gist.html_url + '\n')
    print 'Please let us know about it on IRC (#indico @ Freenode) or via e-mail (indico-team@cern.ch).\n'

    return True

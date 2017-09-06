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
import time
import warnings

from urwid import (Text, Pile, LineBox, ListBox, SimpleFocusListWalker, ProgressBar, Columns, AttrMap,
                   Divider, Filler, GridFlow, SolidFill, BoxAdapter)
from urwid.raw_display import Screen

from indico_migrate.logger import BaseLogger


COLOR_SEGMENT_RE = re.compile(r'(%\[[a-z]+!?(?:,[a-z]+)?\])')
COLOR_SEGMENT_FORMAT_RE = re.compile(r'%\[(?P<fg>[a-z]+)(?P<fg_bold>!?)(?:,(?P<bg>[a-z]+))?\]')


PALETTE = {
    ('cyan', 'blue', False): 'cyan_blue',
    ('cyan', 'blue', True): 'cyan_blue_bold',
    ('red', None, False): 'red',
    ('red', None, True): 'red_highlight',
    ('yellow', None, False): 'yellow',
    ('yellow', None, True): 'yellow_highlight',
    ('cyan', None, False): 'cyan',
    ('cyan', None, True): 'cyan_highlight',
    ('blue', None, False): 'blue',
    ('blue', None, True): 'blue_highlight',
    ('green', None, False): 'green',
    ('green', None, True): 'green_highlight',
    ('white', None, False): 'white',
    ('white', None, True): 'white_highlight',
    ('white', 'blue', False): 'white_on_blue',
    ('white', 'blue', True): 'white_on_blue_highlight',
    ('white', 'yellow', False): 'white_on_yellow',
    ('white', 'yellow', True): 'white_on_yellow_highlight',
    ('white', 'green', False): 'white_on_green',
    ('white', 'green', True): 'white_on_green_highlight',
    ('white', 'red', False): 'white_on_red',
    ('white', 'red', True): 'white_on_red_highlight',
    ('grey', None, False): 'grey',
    ('grey', None, True): 'grey_highlight',
    ('magenta', None, False): 'magenta',
    ('magenta', None, True): 'magenta_highlight',
    (None, None, False): 'default'
}

PALETTE_COLOR_TO_URWID = {
    'cyan': 'light cyan',
    'red': 'light red',
    'green': 'light green',
    'yellow': 'yellow',
    'blue': 'light blue',
    'white': 'white',
    'magenta': 'light magenta',
    'grey': 'dark gray'
}


def generate_urwid_palette(palette):
    result = []
    for (fg, bg, fmt), name in palette.viewitems():
        fmt = ',bold' if fmt else ''
        result.append((name,
                       PALETTE_COLOR_TO_URWID.get(fg, 'default') + fmt,
                       PALETTE_COLOR_TO_URWID.get(bg, 'default')))
    return result


def color_segments(string):
    segments = filter(lambda x: x != '', COLOR_SEGMENT_RE.split(string))
    current_format = (None, None, False)
    text = ''
    result = []
    for elem in segments:
        m = COLOR_SEGMENT_FORMAT_RE.match(elem)
        if m:
            if text:
                result.append((PALETTE[current_format], text))
                text = ''
            current_format = (None if m.group('fg') == 'reset' else m.group('fg'),
                              m.group('bg'),
                              bool(m.group('fg_bold')))
        else:
            text = elem
    if text:
        result.append((PALETTE[current_format], text))
    return result or ''


class GUILogger(BaseLogger):
    def __init__(self, gui, quiet):
        super(GUILogger, self).__init__(quiet)
        self.gui = gui

    def fatal_error(self, message):
        self.gui.stop()
        super(GUILogger, self).fatal_error(message)

    def progress_iterator(self, description, iterable, total, get_id, get_title, print_every=10):
        start_time = time.time()
        progress_bar = self.gui.create_progress_bar(description)
        for n, elem in enumerate(iterable, 1):
            if n % print_every == 0:
                elapsed = time.time() - start_time  # seconds
                eta = int((total - n) * elapsed / n)
                progress_bar.set_state(n * 100 / total, get_id(elem)[:12], eta)
            yield elem
        progress_bar.remove()

    def print_step(self, msg):
        contents = self.gui.steps.contents
        if contents:
            prev_msg = contents[-1][0].get_text()[0]
            del contents[-1]
            contents.append((Text(('step_done', '\u2713 ' + prev_msg[1:])), ('given', 20)))
        contents.append((Text(('step_working', '> ' + msg)), ('given', 20)))
        self.gui.steps.focus_position = len(contents) - 1

        self.gui.set_step_banner(msg)
        self.gui.redraw()
        # this is cheating, but makes the interface so much nicer!
        time.sleep(0.25)

    def _print_msg(self, icon, msg, always=False, prefix='', event_id=''):
        if always or not self.quiet:
            self.gui.print_log(icon, msg, prefix, event_id)

    def set_success(self):
        self.gui.set_success()

    def shutdown(self):
        self.gui.stop()

    def wait_for_input(self):
        self.gui.wait_for_input()


class StepProgressBar(object):
    def __init__(self, gui, description):
        self.progress_bar = ProgressBar('progress_empty', 'progress_done', satt='progress_progress')
        self.id_text = Text('')
        self.eta_text = Text('', align='right')
        title = Text(' {} '.format(description), align='center')
        self.progress_widget = AttrMap(LineBox(Columns([title, ('weight', 2, self.progress_bar), self.id_text,
                                                        self.eta_text])), 'box')
        gui.progress.append(self.progress_widget)
        self.gui = gui
        gui.redraw()

    def set_state(self, progress, elem_id, eta):
        m, s = divmod(eta, 60)
        h, m = divmod(m, 60)

        self.progress_bar.set_completion(progress)
        self.id_text.set_text([' ', '{:8}'.format(elem_id)])
        self.eta_text.set_text([('eta', '{:2d}:{:02d}:{:02d}'.format(h, m, s)), ('box', ' left... ')])
        self.gui.redraw()

    def remove(self):
        self.gui.progress.remove(self.progress_widget)
        self.gui.redraw()


class GUI(object):
    def __init__(self):
        self.screen = Screen()
        self.screen.set_input_timeouts(max_wait=0)
        self.steps = GridFlow([], 20, 2, 1, 'left')
        self.progress = SimpleFocusListWalker([])
        self.log = SimpleFocusListWalker([])

        self.widget = AttrMap(LineBox(Pile([
            ('fixed', 6, AttrMap(Filler(self.steps), 'default')),
            ('fixed', 1, Filler(Divider('\u2500'))),
            ('fixed', 3, ListBox(self.progress)),
            AttrMap(LineBox(ListBox(self.log), title='Message log'), 'default')
        ]), title='Indico 1.2 -> 2.0 migration'), 'global_frame')

        self.screen.register_palette([
            ('green', 'light green', ''),
            ('white', 'white', ''),
            ('red', 'dark red', ''),
            ('yellow', 'yellow', ''),
            ('progress_empty', 'black', 'light gray'),
            ('progress_progress', 'light cyan', 'light gray'),
            ('progress_done', 'black', 'light cyan'),
            ('box', 'white', 'dark gray'),
            ('step_done', 'light green', ''),
            ('step_working', 'dark gray', ''),
            ('global_frame', 'light cyan', ''),
            ('fill', 'light cyan', 'dark cyan'),
            ('done', 'white', 'dark green'),
            ('eta', 'yellow', 'dark gray')
        ] + generate_urwid_palette(PALETTE))

    def print_log(self, icon, message, prefix='', event_id=''):
        self.log.append(Text([
            color_segments(icon),
            ' ',
            color_segments(prefix),
            ' ' if prefix else '',
            color_segments('%[cyan][%[cyan!]{}%[cyan]]%[reset]'.format(event_id)) if event_id else '',
            ' ' if event_id else '',
            color_segments(message)
        ]))
        self.log.set_focus(len(self.log) - 1)
        self.redraw()

    def start(self):
        # don't let Python warnings ruin the GUI
        warnings.filterwarnings('ignore')
        self.screen.start()
        self.redraw()

    def stop(self):
        self.screen.stop()
        warnings.filterwarnings('default')

    def create_progress_bar(self, description):
        if self.progress:
            del self.progress[:]
        return StepProgressBar(self, description)

    def set_success(self):
        if self.progress:
            del self.progress[:]
        self.progress.append(AttrMap(Text('Migration finished!', align='center'), 'done'))
        self.progress.append(AttrMap(Text('Please press any key...', align='center'), 'done'))
        self.redraw()
        self.wait_for_input()

    def wait_for_input(self):
        self.screen._getch(None)

    def set_step_banner(self, msg):
        if self.progress:
            del self.progress[:]
        self.progress.append(BoxAdapter(AttrMap(SolidFill('#'), 'fill'), 3))

    def redraw(self):
        screen_size = self.screen.get_cols_rows()
        canvas = self.widget.render(screen_size, focus=True)
        self.screen.get_input()
        self.screen.draw_screen(screen_size, canvas)


def setup(quiet):
    gui = GUI()
    gui.start()
    return GUILogger(gui, quiet)

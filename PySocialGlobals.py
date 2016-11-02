from __future__ import absolute_import

#EMOJI GLOBALS
"""
Definitions in emoji globals
EMOJI_CLASS = PyMoji.PyMoji()
EMOJI_LIST = [ e[0] for e in EMOJI_CLASS.emoji_list ]
EMOJI_SET = set(EMOJI_LIST)
EMOJI_RE = EMOJI_CLASS.emoji_regex
EMOJI_COMPILED_RE = re.compile(EMOJI_RE)
SKINTONES_LIST = [u'\U0001f3fb',
                  u'\U0001f3fc',
                  u'\U0001f3fd',
                  u'\U0001f3fe',
                  u'\U0001f3ff']
SKINTONES_SET = set(SKINTONES_LIST)
SKINTONES_RE = r'[\U0001f3fb-\U0001f3ff]'
SKINTONES_COMPILED_RE = re.compile(SKINTONES_RE)
"""
from .PyMoji import *

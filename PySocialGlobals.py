from __future__ import absolute_import

__all__ = [
           'EMOJI_CLASS',
           'EMOJI_RE',
           'EMOJI_LIST',
           'EMOJI_SET',
           'SKINTONES_LIST',
           'SKINTONES_RE',
           'SKINTONES_SET'
          ]

#EMOJI GLOBALS
from .PyMoji import PyMoji, SKINTONES_LIST, SKINTONES_RE
EMOJI_CLASS = PyMoji( verbose = False )
EMOJI_RE = EMOJI_CLASS.emoji_regex
EMOJI_LIST = [ e[0] for e in EMOJI_CLASS.emoji_list ]
EMOJI_SET = set(EMOJI_LIST)
SKINTONES_SET = set(SKINTONES_LIST)

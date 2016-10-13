#from __future__ import print_function, absolute_import, unicode_literals
import sys, os
sys.path.append(os.path.dirname(__file__))
from .PyMoji import PyMoji

EMOJI_CLASS = PyMoji( verbose = False )
EMOJI_RE = EMOJI_CLASS.emoji_regex
EMOJIS = [ e[0] for e in EMOJI_CLASS.emoji_list ]

from .TwitterPy.TweetTokenizer2 import TweetTokenizer
from .TwitterPy.TwitterFrame import TwitterFrame
#import TwitterPy
#from .TwitterPy import Twiterator, TwitterTools, TweetTokenizer2

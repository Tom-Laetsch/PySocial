# coding: utf-8
#
# Natural Language Toolkit: Twitter Tokenizer
#
# Copyright (C) 2001-2016 NLTK Project
# Author: Christopher Potts <cgpotts@stanford.edu>
#         Ewan Klein <ewan@inf.ed.ac.uk> (modifications)
#         Pierpaolo Pantone <> (modifications)
# URL: <http://nltk.org/>
# For license information, see LICENSE.TXT
#


"""
Twitter-aware tokenizer, designed to be flexible and easy to adapt to new
domains and tasks. The basic logic is this:

1. The tuple regex_strings defines a list of regular expression
   strings.

2. The regex_strings strings are put, in order, into a compiled
   regular expression object called word_re.

3. The tokenization is done by word_re.findall(s), where s is the
   user-supplied string, inside the tokenize() method of the class
   Tokenizer.

4. When instantiating Tokenizer objects, there is a single option:
   preserve_case.  By default, it is set to True. If it is set to
   False, then the tokenizer will downcase everything except for
   emoticons.

"""



######################################################################

from __future__ import unicode_literals
import re
from nltk.compat import htmlentitydefs, int2byte, unichr

######## TOM ADDED CODE
from PyMoji import PyMoji
EMOJI_CLASS = PyMoji()
EMOJI_RE = re.compile(EMOJI_CLASS.emoji_regex, re.UNICODE)
#####

######################################################################
# The following strings are components in the regular expression
# that is used for tokenizing. It's important that phone_number
# appears first in the final regex (since it can contain whitespace).
# It also could matter that tags comes after emoticons, due to the
# possibility of having text like
#
#     <:| and some text >:)
#
# Most importantly, the final element should always be last, since it
# does a last ditch whitespace-based tokenization of whatever is left.

# ToDo: Update with http://en.wikipedia.org/wiki/List_of_emoticons ?

# This particular element is used in a couple ways, so we define it
# with a name:
EMOTICONS = r"""
    (?:
      [<>]?
      [:;=8]                     # eyes
      [\-o\*\']?                 # optional nose
      [\)\]\(\[dDpP/\:\}\{@\|\\] # mouth
      |
      [\)\]\(\[dDpP/\:\}\{@\|\\] # mouth
      [\-o\*\']?                 # optional nose
      [:;=8]                     # eyes
      [<>]?
      |
      <3                         # heart
    )"""

# URL pattern due to John Gruber, modified by Tom Winzig. See
# https://gist.github.com/winzig/8894715

URLS = r"""			# Capture 1: entire matched URL
  (?:
  https?:				# URL protocol and colon
    (?:
      /{1,3}				# 1-3 slashes
      |					#   or
      [a-z0-9%]				# Single letter or digit or '%'
                                       # (Trying not to match e.g. "URI::Escape")
    )
    |					#   or
                                       # looks like domain name followed by a slash:
    [a-z0-9.\-]+[.]
    (?:[a-z]{2,13})
    /
  )
  (?:					# One or more:
    [^\s()<>{}\[\]]+			# Run of non-space, non-()<>{}[]
    |					#   or
    \([^\s()]*?\([^\s()]+\)[^\s()]*?\) # balanced parens, one level deep: (...(...)...)
    |
    \([^\s]+?\)				# balanced parens, non-recursive: (...)
  )+
  (?:					# End with:
    \([^\s()]*?\([^\s()]+\)[^\s()]*?\) # balanced parens, one level deep: (...(...)...)
    |
    \([^\s]+?\)				# balanced parens, non-recursive: (...)
    |					#   or
    [^\s`!()\[\]{};:'".,<>?«»“”‘’]	# not a space or one of these punct chars
  )
  |					# OR, the following to match naked domains:
  (?:
  	(?<!@)			        # not preceded by a @, avoid matching foo@_gmail.com_
    [a-z0-9]+
    (?:[.\-][a-z0-9]+)*
    [.]
    (?:[a-z]{2,13})
    \b
    /?
    (?!@)			        # not succeeded by a @,
                            # avoid matching "foo.na" in "foo.na@example.com"
  )
"""

#### TOM ADDED CODE
URL_RE = re.compile(URLS, re.VERBOSE | re.I | re.UNICODE)
####

# The components of the tokenizer:
REGEXPS = (
    URLS,
    # Phone numbers:
    r"""
    (?:
      (?:            # (international)
        \+?[01]
        [\-\s.]*
      )?
      (?:            # (area code)
        [\(]?
        \d{3}
        [\-\s.\)]*
      )?
      \d{3}          # exchange
      [\-\s.]*
      \d{4}          # base
    )"""
    ,
    # ASCII Emoticons
    EMOTICONS
    ,
    # HTML tags:
    r"""<[^>\s]+>"""
    ,
    # ASCII Arrows
    r"""[\-]+>|<[\-]+"""
    ,
    # Twitter username:
    r"""(?:@[\w_]+)"""
    ,
    # Twitter hashtags:
    r"""(?:\#+[\w_]+[\w\'_\-]*[\w_]+)"""
    ,
    # email addresses
    r"""[\w.+-]+@[\w-]+\.(?:[\w-]\.?)+[\w-]"""
    ,
    # TOM ADDED CODE
    ## combine lengths of punctuation
    r"""[.,!?][\s.,!?]+(?<=[.,!?])"""
    ,
    # Remaining word types:
    r"""
    (?:[^\W\d_](?:[^\W\d_]|['\-_])+[^\W\d_]) # Words with apostrophes or dashes.
    |
    (?:[+\-]?\d+[,/.:-]\d+[+\-]?)  # Numbers, including fractions, decimals.
    |
    (?:[\w_]+)                     # Words without apostrophes or dashes.
    |
    (?:\.(?:\s*\.){1,})            # Ellipsis dots.
    |
    (?:\S)                         # Everything else that isn't whitespace.
    """
    )

######################################################################
# This is the core tokenizing regex:

###### TOM ADDED CDOE: EMOJI_CLASS.emoji_regex + "|" +
WORD_RE = re.compile(EMOJI_CLASS.emoji_regex + "|" + r"""(%s)""" % "|".join(REGEXPS), re.VERBOSE | re.I
                     | re.UNICODE)
######

# WORD_RE performs poorly on these patterns:
HANG_RE = re.compile(r'([^a-zA-Z0-9])\1{3,}')

# The emoticon string gets its own regex so that we can preserve case for
# them as needed:
EMOTICON_RE = re.compile(EMOTICONS, re.VERBOSE | re.I | re.UNICODE)

# These are for regularizing HTML entities to Unicode:
ENT_RE = re.compile(r'&(#?(x?))([^&;\s]+);')


######################################################################
# Functions for converting html entities
######################################################################

def _str_to_unicode(text, encoding=None, errors='strict'):
    if encoding is None:
        encoding = 'utf-8'
    if isinstance(text, bytes):
        return text.decode(encoding, errors)
    return text

def _replace_html_entities(text, keep=(), remove_illegal=True, encoding='utf-8'):
    """
    Remove entities from text by converting them to their
    corresponding unicode character.

    :param text: a unicode string or a byte string encoded in the given
    `encoding` (which defaults to 'utf-8').

    :param list keep:  list of entity names which should not be replaced.\
    This supports both numeric entities (``&#nnnn;`` and ``&#hhhh;``)
    and named entities (such as ``&nbsp;`` or ``&gt;``).

    :param bool remove_illegal: If `True`, entities that can't be converted are\
    removed. Otherwise, entities that can't be converted are kept "as
    is".

    :returns: A unicode string with the entities removed.

    See https://github.com/scrapy/w3lib/blob/master/w3lib/html.py

        >>> from nltk.tokenize.casual import _replace_html_entities
        >>> _replace_html_entities(b'Price: &pound;100')
        'Price: \\xa3100'
        >>> print(_replace_html_entities(b'Price: &pound;100'))
        Price: £100
        >>>
    """

    def _convert_entity(match):
        entity_body = match.group(3)
        if match.group(1):
            try:
                if match.group(2):
                    number = int(entity_body, 16)
                else:
                    number = int(entity_body, 10)
                # Numeric character references in the 80-9F range are typically
                # interpreted by browsers as representing the characters mapped
                # to bytes 80-9F in the Windows-1252 encoding. For more info
                # see: http://en.wikipedia.org/wiki/Character_encodings_in_HTML
                if 0x80 <= number <= 0x9f:
                    return int2byte(number).decode('cp1252')
            except ValueError:
                number = None
        else:
            if entity_body in keep:
                return match.group(0)
            else:
                number = htmlentitydefs.name2codepoint.get(entity_body)
        if number is not None:
            try:
                return unichr(number)
            except ValueError:
                pass

        return "" if remove_illegal else match.group(0)

    return ENT_RE.sub(_convert_entity, _str_to_unicode(text, encoding))


######################################################################

class TweetTokenizer:
    r"""
    Tokenizer for tweets.

        >>> from nltk.tokenize import TweetTokenizer
        >>> tknzr = TweetTokenizer()
        >>> s0 = "This is a cooool #dummysmiley: :-) :-P <3 and some arrows < > -> <--"
        >>> tknzr.tokenize(s0)
        ['This', 'is', 'a', 'cooool', '#dummysmiley', ':', ':-)', ':-P', '<3', 'and', 'some', 'arrows', '<', '>', '->', '<--']

    Examples using `strip_handles` and `reduce_len parameters`:

        >>> tknzr = TweetTokenizer(strip_handles=True, reduce_len=True)
        >>> s1 = '@remy: This is waaaaayyyy too much for you!!!!!!'
        >>> tknzr.tokenize(s1)
        [':', 'This', 'is', 'waaayyy', 'too', 'much', 'for', 'you', '!', '!', '!']
    """

    def __init__(self, preserve_case=True, reduce_len=False, strip_handles=False,
                 banish_emoji = False, banish_url = True ## TOM ADDED CODE
                 ):
        self.preserve_case = preserve_case
        self.reduce_len = reduce_len
        self.strip_handles = strip_handles
        self.banish_emoji = banish_emoji ## TOM ADDED CODE
        self.banish_url = banish_url ## TOM ADDED CODE

    def tokenize(self, text):
        """
        :param text: str
        :rtype: list(str)
        :return: a tokenized list of strings; concatenating this list returns\
        the original string if `preserve_case=False`
        """
        # Fix HTML character entities:
        text = _replace_html_entities(text)
        # Remove URLS
        if self.banish_url:
            text = URL_RE.sub(' ', text)
        # Remove username handles
        if self.strip_handles:
            text = remove_handles(text)
        # Normalize word lengthening
        if self.reduce_len:
            text = reduce_lengthening(text)
        # Shorten problematic sequences of characters
        safe_text = HANG_RE.sub(r'\1\1\1', text)


        # Tokenize:
        #words = WORD_RE.findall(safe_text) #TOM ADDED CODE: commented this on_timeout
        # TOM ADDED CODE
        if self.banish_emoji:
            s_temp = EMOJI_RE.sub(" ", safe_text)
            word = WORD_RE.findall(safe_text)
        else:
            s_temp = safe_text
            words = []
            search = EMOJI_RE.search(s_temp)
            while search != None:
                a = search.span(0)[0]
                b = search.span(0)[1]
                words += WORD_RE.findall(s_temp[:a])
                words.append(s_temp[a:b])
                s_temp = s_temp[b:]
                search = EMOJI_RE.search(s_temp)
            if len(s_temp) > 0:
                words += WORD_RE.findall(s_temp)


        # Possibly alter the case, but avoid changing emoticons like :D into :d:
        if not self.preserve_case:
            words = list(map((lambda x : x if EMOTICON_RE.search(x) else
                              x.lower()), words))
        return words

######################################################################
# Normalization Functions
######################################################################

def reduce_lengthening(text):
    """
    Replace repeated character sequences of length 3 or greater with sequences
    of length 3.
    """
    pattern = re.compile(r"(.)\1{2,}")
    return pattern.sub(r"\1\1\1", text)

def remove_handles(text):
    """
    Remove Twitter username handles from text.
    """
    pattern = re.compile(r"(^|(?<=[^\w.-]))@[A-Za-z_]+\w+")
    return pattern.sub('', text)

######################################################################
# Tokenization Function
######################################################################

def casual_tokenize(text, preserve_case=True, reduce_len=False, strip_handles=False):
    """
    Convenience function for wrapping the tokenizer.
    """
    return TweetTokenizer(preserve_case=preserve_case, reduce_len=reduce_len,
                          strip_handles=strip_handles).tokenize(text)

###############################################################################

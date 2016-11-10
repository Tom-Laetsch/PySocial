from __future__ import print_function, unicode_literals
from bs4 import BeautifulSoup
import requests
try:
    import cPickle as pickle
except:
    import pickle
from datetime import datetime
from os.path import isfile, join, dirname
import re

def emoji_regex_maker( list_of_emojis ):
    emojis = sorted( list_of_emojis, key = lambda x: len(x))
    emoji_re = ''
    for e in list_of_emojis:
        emoji_re += '(?:' + e + ')|'
    #asterisks cause trouble
    emoji_re = re.sub(r'\*', r'\*', emoji_re)
    #remove trailing or |
    return emoji_re[:-1]


class PyMoji(object):
    def __init__( self,
                  try_load_first = True,
                  save_on_update = True,
                  version = -1,
                  verbose = True,
                  save_dir = join(dirname(__file__), '.pymoji_saved/'),
                  saved_version_basename = '.emoji_list_fps' ):

        self.save_dir = save_dir
        self.saved_version_basename = saved_version_basename
        self._attr_setter( try_load_first, save_on_update, version, verbose )

    def _update_emoji_list( self, save_on_update = True ):
        emoji_url = 'http://unicode.org/emoji/charts/full-emoji-list.html'
        idx_class = 'rchars'
        emoji_class = 'chars'
        keywords_target = 'annotate'
        r = requests.get(emoji_url).text
        soup = BeautifulSoup(r, "lxml")
        tr = soup.find_all('tr')
        emoji_list = []
        for chunk in tr:
            try:
                idx_html = chunk.find('td', class_ = idx_class)
                idx = int(idx_html.get_text())
                emoji_html = chunk.find('td', class_ = emoji_class)
                emoji = emoji_html.get_text()
                keywords_html = chunk.find_all('a', target = keywords_target)
                keywords = [keyword.get_text() for keyword in keywords_html]
                emoji_list.append( (emoji, {'keywords': keywords, 'idx': idx}) )
            except AttributeError:
                pass
        emoji_list = sorted(emoji_list, key = lambda x: len(x[0]), reverse = True)
        version_date = datetime.now().date().isoformat()
        if save_on_update:
            filename = '%s_emoji_list.pickle' % version_date
            filepath = join(self.save_dir, filename)
            try:
                with open(filepath, 'wb') as fout:
                    pickle.dump(obj = emoji_list, file = fout, protocol = 2)
                if isfile(filepath):
                    fsaved_names = join(self.save_dir, self.saved_version_basename)
                    if isfile(fsaved_names):
                        #if this file already exists, add \n
                        with open(fsaved_names, 'a') as fsaved:
                            fsaved.write('\n' + filename)
                    else:
                        #if file does not exist, do not add \n
                        with open(fsaved_names, 'w') as fsaved:
                            fsaved.write(filename)
            except IOError:
                print("IOError: Could not save emoji list pickle file.")
        return emoji_list, version_date

    def _load_emoji_list( self, version = -1, verbose = False ):
        # version is a negative int with -1 most recent, and -n the nth previously saved version.
        filenames = []
        fsaved_names = join(self.save_dir, self.saved_version_basename)
        try:
            with open(fsaved_names, 'r') as fsaved:
                for name in fsaved:
                    filenames.append(name)
        except IOError:
            if verbose: print("No saved emoji list files found.")
            return None, None
        if abs(version) > len(filenames): version = 0
        filename = filenames[version]
        try:
            filepath = join(self.save_dir,filename)
            with open(filepath, 'rb') as fin:
                try:
                    emoji_list = pickle.load(fin, encoding = 'latin1')
                except TypeError:
                    emoji_list = pickle.load(fin)
            version_date = filename[:10]
            if verbose: print("Imported emoji list from date: %s" % version_date)
        except IOError:
            if verbose: print("No saved emoji list files found. To create one, run _update_emoji_list().")
            return None, None
        return emoji_list, version_date


    def _emoji_regex_maker( self ):
        return emoji_regex_maker( [e[0] for e in self._emoji_list] )

    def _attr_setter( self,
                     try_load_first = True,
                     save_on_update = True,
                     version = -1,
                     verbose = True ):
        emoji_list = None
        if try_load_first:
            emoji_list, version_date = self._load_emoji_list( version = version, verbose = verbose )
        if emoji_list == None:
            if verbose: print("Attempting to update emoji_list...")
            emoji_list, version_date = self._update_emoji_list( save_on_update = save_on_update )
            if verbose: print("emoji_list now updated.")
        setattr(self, '_emoji_list', emoji_list)
        setattr(self, '_version_date', version_date)

        emoji_regex = self._emoji_regex_maker()
        setattr(self, '_emoji_regex', emoji_regex)

    def update_emoji_list( self, save_on_update = True ):
        self._attr_setter( try_load_first = False, save_on_update = save_on_update )

    @property
    def emoji_list( self ):
        return self._emoji_list

    @property
    def emoji_regex( self ):
        return self._emoji_regex

    @property
    def version_date( self ):
        return self._version_date

    @property
    def save_dir( self ):
        return self._save_dir

    @save_dir.setter
    def save_dir( self, save_dir ):
        self._save_dir = save_dir

    @property
    def saved_version_basename( self ):
        return self._emoji_list_version_fp

    @saved_version_basename.setter
    def saved_version_basename( self, saved_version_basename ):
        self._emoji_list_version_fp = saved_version_basename

EMOJI_CLASS = PyMoji( verbose = False )
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
SKINTONED_EMOJI_LIST = [ e for e in EMOJI_LIST if any(st in e for st in SKINTONES_LIST) ]
SKINTONED_EMOJI_SET = set( SKINTONED_EMOJI_LIST )
SKINTONED_EMOJI_RE = emoji_regex_maker( SKINTONED_EMOJI_LIST )
SKINTONED_EMOJI_COMPILED_RE = re.compile( SKINTONED_EMOJI_RE )

__all__ = [
            "emoji_regex_maker",
            "EMOJI_CLASS",
            "EMOJI_LIST",
            "EMOJI_SET",
            "EMOJI_RE",
            "EMOJI_COMPILED_RE",
            "SKINTONES_LIST",
            "SKINTONES_SET",
            "SKINTONES_RE",
            "SKINTONES_COMPILED_RE",
            "SKINTONED_EMOJI_LIST",
            "SKINTONED_EMOJI_RE",
            "SKINTONED_EMOJI_COMPILED_RE"
          ]

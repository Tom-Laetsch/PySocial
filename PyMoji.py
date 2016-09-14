from __future__ import print_function
from bs4 import BeautifulSoup
import requests
try:
    import cPickle as pickle
except:
    import pickle
from datetime import datetime
from os.path import isfile, join, dirname
import re

save_dir = join(dirname(__file__), '.pymoji_saved/')
emoji_list_version_fp = '.emoji_list_fps'

class PyMoji(object):
    def __init__( self,
                  try_load_first = True,
                  save_on_update = True,
                  version = -1 ):
        self.version = -1
        self.emoji_list = None
        if try_load_first:
            self.emoji_list = self.load_emoji_list( version = -1, verbose = False )
        if self.emoji_list == None:
            self.emoji_list = self.update_emoji_list( save_pickle = save_on_update )
        self.emoji_regex = self.get_emoji_regex()

    def update_emoji_list( self, save_pickle = True ):
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
        if save_pickle:
            filename = '%s_emoji_list.pickle' % datetime.isoformat(datetime.now())
            filepath = join(save_dir, filename)
            try:
                with open(filepath, 'wb') as fout:
                    pickle.dump(obj = emoji_list, file = fout, protocol = 2)
                if isfile(filepath):
                    fsaved_names = join(save_dir, emoji_list_version_fp)
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
        return emoji_list

    def load_emoji_list( self, version = -1, verbose = True ):
        # version is a negative int with -1 most recent, and -n the nth previously saved version.
        filenames = []
        fsaved_names = join(save_dir, emoji_list_version_fp)
        try:
            with open(fsaved_names, 'r') as fsaved:
                for name in fsaved:
                    filenames.append(name)
        except IOError:
            if verbose: print("No saved emoji list files found. To create one, run update_emoji_list().")
            return None
        if abs(version) > len(filenames): version = 0
        filename = filenames[version]
        try:
            filepath = join(save_dir,filename)
            with open(filepath, 'rb') as fin:
                try:
                    emoji_list = pickle.load(fin, encoding = 'latin1')
                except TypeError:
                    emoji_list = pickle.load(fin)
            print("Imported emoji list from date: %s" % filename[:-18])
        except IOError:
            if verbose: print("No saved emoji list files found. To create one, run update_emoji_list().")
            return None
        return emoji_list

    def get_emoji_regex( self ):
        emoji_re = ''
        for e in self.emoji_list:
            emoji_re += '(?:' + e[0] + ')|'
        #asterisks cause trouble
        emoji_re = re.sub(r'\*', r'\*', emoji_re)
        #remove trailing or |
        return emoji_re[:-1]

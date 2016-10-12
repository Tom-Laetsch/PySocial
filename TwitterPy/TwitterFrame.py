from __future__ import print_function, division
import numpy as np
import pandas as pd
import re
from .TweetTokenizer2 import TweetTokenizer
from ..PyMoji import PyMoji

emoji_regex = PyMoji(verbose = False).emoji_regex

class TwitterFrame( pd.DataFrame ):
    def __init__( self,  data=None, index=None, columns=None, dtype=None, copy=False, standardize = True ):
        super(self.__class__,self).__init__(data=data, index=index, columns=columns, dtype=dtype, copy=copy)
        #tokenize text and add column
        if standardize:
            if 'text' in self.columns.values:
                if 'tokenized_text' not in self.columns.values:
                    self['tokenized_text'] = self.tokenize_text( verbose = False )
                #extract emoji present
                if 'emoji' not in self.columns.values:
                    self['emoji'] = self.extract_emoji( verbose = False )
            #create screen_name column
            if 'user' in self.columns.values:
                if 'screen_name' not in self.columns.values:
                    self['screen_name'] = self.extract_screen_name( verbose = False )

            drops = ['geo','id','quoted_status_id','quoted_status_id_str']
            drops = [key for key in drops if key in self.columns.values]

            if len(drops) > 0:
                self.__init__(self.drop(drops, axis = 1))

    def from_pickle( self, pickle_file ):
        #self._twitter_frame = pd.read_pickle( pickle_file )
        self.__init__( pd.read_pickle(pickle_file) )
        #self.standardize()

    def from_json( self, json_file ):
        with open(json_file, 'r') as fin:
            data = fin.readlines()
        self.__init__( pd.read_json( '[' + ','.join(x.strip() for x in data) + ']' ) )
        #self._twitter_frame = pd.read_json( '[' + ','.join(x.strip() for x in data) + ']' )
        #self.standardize()

    #def to_pickle( self, output_file ):
    #    self._twitter_frame.to_pickle( output_file )

    def tokenize_text( self, verbose = True, **kwargs ):
        tknzr = TweetTokenizer(**kwargs).tokenize
        #return self._twitter_frame.text.apply( lambda x: tknzr(x) )
        try:
            return self.text.apply( lambda x: tknzr(x) )
        except Exception as e:
            if verbose: print( "Error %s encountered." % e )
            return None

    def extract_emoji( self, verbose = True ):
        try:
            return self.text.apply(lambda x: re.findall(emoji_regex, x))
        except Exception as e:
            if verbose: print( "Error %s encountered." % e )
            return None

    def extract_screen_name( self, verbose = True ):
        try:
            return self.user.apply(lambda x: x['screen_name'])
        except Exception as e:
            if verbose: print( "Error %s encountered." % e )
            return None

    def order_by_screen_name( self, verbose = True ):
        if not 'screen_name' in self.columns.values:
            self['screen_name'] = self.extract_screen_name()
        try:
            grp = self.groupby( "screen_name", as_index = False )
            self.__init__( self.merge(grp['user'].agg(len),
                                on='screen_name',
                                how='outer',
                                suffixes=('', '_n')).sort_values(by = ['user_n', 'screen_name'],
                                                                 ascending=[0,1]).reset_index() )
        except Exception as e:
            if verbose: print( "Error %s encountered." % e )
            return None

    def restrict_by_screen_name_and_time( self, num_allowed = 1, time_delta = 'day', verbose = True ):
        if not 'screen_name' in self.columns.values:
            self['screen_name'] = self.extract_screen_name()
        try:
            self.order_by_screen_name()
            grp = self.groupby( 'screen_name', as_index = False )
            #restrict screen_name allowances
            keep = dict(screen_name = [], keep = [])
            for name, g in grp:
                glen = len(g)
                i_star = np.random.choice(range(glen), num_allowed, replace=False)
                for i in range(glen):
                    keep['screen_name'].append(name)
                    keep['keep'].append(i in i_star)
            #first merge (temporary step: part 1 of 2 merges)
            self.__init__(self.merge( pd.DataFrame(keep), on='screen_name', how='outer', suffixes = ('','') ) )
        except Exception as e:
            print("Exception: %s" % e)
            return False

    def standardize_self( self ):
        #drop unwanted columns
        if 'screen_name' not in self.columns.values:
            return None

        #group and order the data by screen name, adds column user_n with number of posts by user
        self.__init__( self.order_by_screen_name() )

'''
    def extract_emoji( self ):
        return self._twitter_frame.text.apply(lambda x: re.findall(emoji_regex, x))

    def extract_screen_name( self ):
        return self._twitter_frame.user.apply(lambda x: x['screen_name'])

    def order_by_screen_name( self ):
        if not 'screen_name' in self._twitter_frame.columns.values:
            self._twitter_frame['screen_name'] = self.extract_screen_name()
        grp = self._twitter_frame.groupby( "screen_name", as_index = False )
        return pd.merge(self._twitter_frame, grp['user'].agg(len),
                        on='screen_name',
                        how='outer',
                        suffixes=('', '_n')).sort_values(by = ['user_n', 'screen_name'],
                                                         ascending=[0,1]).reset_index()

    def restrict_by_screen_name_and_time( self, num_allowed = 1, time_delta = 'day' ):
        if not 'screen_name' in self._twitter_frame.columns.values:
            self._twitter_frame['screen_name'] = self.extract_screen_name()
        grp = self._twitter_frame.groupby( 'screen_name', as_index = False )
        #restrict screen_name allowances
        keep = dict(screen_name = [], keep = [])
        for name, g in grp:
            glen = len(g)
            i_star = np.random.choice(range(glen), num_allowed, replace=False)
            for i in range(glen):
                keep['screen_name'].append(name)
                keep['keep'].append(i in i_star)
        #first merge (temporary step: part 1 of 2 merges)
        return pd.merge( pd.DataFrame(keep), self._twitter_frame,
                         on='screen_name',
                         how='outer',
                         suffixes = ('','') )

    @property
    def twitter_frame( self ):
        return self._twitter_frame

    def standardize( self ):
        #drop unwanted columns
        try:
            self._twitter_frame = self._twitter_frame.drop( [
                                                                'geo', #deprecated
                                                                'id', #only keep id_str
                                                                'quoted_status_id', #contained in quoted_status
                                                                'quoted_status_id_str' #contained in quoted_status
                                                             ],
                                                             axis = 1)
        except ValueError:
            #columns are already missing
            pass
        #tokenize text and add column
        self._twitter_frame['tokenized_text'] = self.tokenize_text()
        #extract emoji present
        self._twitter_frame['emoji'] = self.extract_emoji()
        #create screen_name column
        self._twitter_frame['screen_name'] = self.extract_screen_name()
        #group and order the data by screen name, adds column user_n with number of posts by user
        self._twitter_frame = self.order_by_screen_name()
'''

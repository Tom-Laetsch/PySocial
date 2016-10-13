from __future__ import print_function, division, absolute_import
import numpy as np
import pandas as pd
import re
from .TweetTokenizer2 import TweetTokenizer
from PySocial import EMOJI_RE

#short list of columns that might be useful
keeps = [
         'screen_name',
         'tokenized_text',
         'hashtags',
         'mentions',
         'emojis',
         'user_n',
         'created_at',
         'text',
         'user',
         'coordinates'
        ]
#drop unwanted columns
drops = [
         'geo', #deprecated
         'id', #use id_str
         'quoted_status_id', #use quoted_status
         'quoted_status_id_str' #use quoted_status
        ]

class TwitterFrame( pd.DataFrame ):
    def __init__( self,
                  data=None, index=None, columns=None, dtype=None, copy=False, #for DataFrame
                  keep_columns = keeps,
                  remove_columns = [],
                  standardize = False ):
        super(self.__class__,self).__init__(data=data,
                                            index=index,
                                            columns=columns,
                                            dtype=dtype,
                                            copy=copy)
        if standardize:
            self.standardize_self( keep_columns, remove_columns )

    def standardize_self( self,
                          keep_columns = keeps,
                          remove_columns = [],
                          verbose = True
                        ):
        #create tokenized text and emoji list columns
        if 'text' in self.columns.values:
            if 'tokenized_text' not in self.columns.values:
                if verbose: print("Creating tokenized_text...")
                self['tokenized_text'] = self.tokenize_text( verbose = False )
            #extract emoji present
            if 'emojis' not in self.columns.values:
                if verbose: print("Extracting emojis...")
                self['emojis'] = self.extract_emojis( verbose = False )
        #create hashtags and mentions columns
        if 'entities' in self.columns.values:
            if 'hashtags' not in self.columns.values:
                if verbose: print("Extracting hashtags...")
                self['hashtags'] = self.extract_hashtags()
            if 'mentions' not in self.columns.values:
                if verbose: print("Extracting user_mentions, saving as 'mentions'...")
                self['mentions'] = self.extract_mentions()
        #create screen_name column
        if 'user' in self.columns.values:
            if 'screen_name' not in self.columns.values:
                if verbose: print("Extracting screen_name...")
                self['screen_name'] = self.extract_screen_name( verbose = False )

        if len(keep_columns) > 0:
            to_drop = [ key for key in self.columns.values if key not in keep_columns ]
            if len(to_drop) == len(self.columns.values):
                to_drop = []
        else:
            to_drop = [key for key in remove_columns if key in self.columns.values]

        if len(to_drop) > 0:
            if verbose: print("Dropping unwanted columns...")
            self.__init__( self.drop(to_drop, axis = 1) )

        if verbose: print("Standardization complete.")

    def from_pickle( self, pickle_file, standardize = True ):
        #self._twitter_frame = pd.read_pickle( pickle_file )
        self.__init__( pd.read_pickle(pickle_file), standardize = standardize )
        #self.standardize()

    def from_json( self, json_file, standardize = True ):
        with open(json_file, 'r') as fin:
            data = fin.readlines()
        self.__init__( pd.read_json( '[' + ','.join(x.strip() for x in data) + ']' ) )

    def tokenize_text( self, verbose = True, **kwargs ):
        tknzr = TweetTokenizer(**kwargs).tokenize
        #return self._twitter_frame.text.apply( lambda x: tknzr(x) )
        try:
            return self.text.apply( lambda x: tuple(tknzr(x)) )
        except Exception as e:
            if verbose: print( "Error %s encountered." % e )
            return None

    def extract_screen_name( self, verbose = True ):
        try:
            return self.user.apply(lambda x: x['screen_name'])
        except Exception as e:
            if verbose: print( "Error %s encountered." % e )
            return None

    def extract_emojis( self, verbose = True ):
        try:
            return self.text.apply(lambda x: tuple(re.findall(EMOJI_RE, x)))
        except Exception as e:
            if verbose: print( "Error %s encountered." % e )
            return None

    def extract_hashtags( self, verbose = True ):
        def hashtag_fn( entities ):
            return tuple( [x['text'] for x in entities['hashtags']] )
        return self.entities.apply( lambda x: hashtag_fn(x))

    def extract_mentions( self, verbose = True ):
        def mentions_fn( entities ):
            return tuple([ x['screen_name'] for x in entities['user_mentions'] ])
        return self.entities.apply( lambda x: mentions_fn(x) )

    def order_by_screen_name( self, verbose = True ):
        if not 'screen_name' in self.columns.values:
            self['screen_name'] = self.extract_screen_name()
        try:
            grp = self.groupby( "screen_name", as_index = False )
            self.__init__( self.merge(grp['user'].agg(len),
                                on='screen_name',
                                how='outer',
                                suffixes=('', '_n')).sort_values(by = ['user_n', 'screen_name', 'created_at'],
                                                                 ascending=[0,1,1]).reset_index() )
        except Exception as e:
            if verbose: print( "Error %s encountered." % e )
            return None

    def restrict_within_time( self,
                              by = ['screen_name'],
                              num_allowed = 1,
                              time_delta = 'day',
                              seed = None,
                              verbose = True ):
        if 'user' in self.columns.values:
            if 'screen_name' in by and 'screen_name' not in self.columns.values:
                if verbose: print("Extracting screen_name...")
                self['screen_name'] = self.extract_screen_name()
        if 'text' in self.columns.values:
            if 'emojis' in by and 'emojis' not in self.columns.values:
                if verbose: print("Extracting emojis...")
                self['emojis'] = self.extract_emojis()
        if 'entities' in self.columns.values:
            if 'hashtags' in by and 'hashtags' not in self.columns.values:
                if verbose: print("Extracting hashtags...")
                self['hashtags'] = self.extract_hashtags()
            if 'mentions' in by and 'mentions' not in self.columns.values:
                if verbose: print("Extracting mentions...")
                self['mentions'] = self.extract_mentions()
        grpby = [ key for key in by if key in self.columns.values ]

        try:
            def time_fn( dt_object ):
                if time_delta == 'day':
                    return dt_object.day
                elif time_delta == 'hour':
                    return dt_object.hour
                elif time_delta == 'minute':
                    return dt_object.minute
                elif time_delta == 'month':
                    return dt_object.month
                elif time_delta == 'year':
                    return dt_object.year
                elif time_delta == 'second':
                    return dt_object.second


            if verbose: print("Grouping...")
            if time_delta in ['day', 'hour', 'minute', 'month', 'year', 'second']:
                grp = self.groupby( [*grpby, self.created_at.apply(lambda x: time_fn(x))],
                                    as_index = False )
            else:
                grp = self.groupby( [*grpby], as_index = False )

            def sel_fn( g ):
                np.random.seed( seed )
                return g.iloc[ np.random.choice( range(0,len(g)),
                                                 num_allowed, replace=False ), : ]

            if verbose: print("Selecting %d random choice(s) from groups..." % num_allowed)
            return TwitterFrame( data = grp.apply( lambda x: sel_fn(x) ) )
            #restrict screen_name allowances
            #keep = dict(screen_name = [], keep = [])
            #for name, g in grp:
            #    glen = len(g)
            #    i_star = np.random.choice(range(glen), num_allowed, replace=False)
            #    for i in range(glen):
            #        keep['screen_name'].append(name)
            #        keep['keep'].append(i in i_star)
            #first merge (temporary step: part 1 of 2 merges)
            #self.__init__(self.merge( pd.DataFrame(keep), on='screen_name', how='outer', suffixes = ('','') ) )
        except Exception as e:
            print("Exception: %s" % e)
            return False


'''
    def extract_emojis( self ):
        return self._twitter_frame.text.apply(lambda x: re.findall(EMOJI_RE, x))

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

    def restrict_within_time( self, num_allowed = 1, time_delta = 'day' ):
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
        self._twitter_frame['emoji'] = self.extract_emojis()
        #create screen_name column
        self._twitter_frame['screen_name'] = self.extract_screen_name()
        #group and order the data by screen name, adds column user_n with number of posts by user
        self._twitter_frame = self.order_by_screen_name()
'''

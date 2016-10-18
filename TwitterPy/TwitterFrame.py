from __future__ import print_function, division, absolute_import
import numpy as np
import pandas as pd
import re
from .TweetTokenizer2 import TweetTokenizer
from PySocial import EMOJI_RE

EMOJI_COMPILED_RE = re.compile(EMOJI_RE, re.UNICODE)
#short list of frequently used columns
keeps = [
         'screen_name',
         'tokenized_text',
         'hashtags',
         'mentions',
         'emojis',
         'user_n',
         'location_id',
         'id_str',
         'coordinates',
         'created_at',
         'text',
         'entities',
         'user',
         'quoted_status',
         'in_reply_to_id_str',
         'favorite_count',
         'retweet_count',
         'place'
        ]
#lesser used columns
drops = [
         '_id', #use id_str
         'contributors',
         'display_text_range',
         'extended_entities',
         'extended_tweet',
         'favorited',
         'filter_level',
         'geo', #deprecated
         'id', #use id_str
         'in_reply_to_status_id', #use *_id_str
         'in_reply_to_user_id',  #use *_id_str
         'is_quote_status',
         'lang',
         'possibly_sensitive',
         'quoted_status_id',
         'quoted_status_id_str',
         'retweeted',
         'scopes',
         'source',
         'timestamp_ms',
         'truncated',
         'withheld_in_countries'
        ]

class TwitterFrame( pd.DataFrame ):
    def __init__( self,
                  data=None, index=None, columns=None, dtype=None, copy=False, #for DataFrame
                  keep_columns = [],
                  remove_columns = drops,
                  standardize = False ):
        super(self.__class__,self).__init__(data=data,
                                            index=index,
                                            columns=columns,
                                            dtype=dtype,
                                            copy=copy)
        if standardize:
            self.standardize_self( keep_columns, remove_columns )

    def __getitem__(self, key):
        ret = super(self.__class__, self).__getitem__( key )
        if isinstance(ret, pd.core.frame.DataFrame):
            return TwitterFrame( ret )
        else:
            return ret

    def standardize_self( self,
                          keep_columns = keeps,
                          remove_columns = [],
                          verbose = True
                        ):

        #deal with un/wanted columns (SAFER TO DROP THAN KEEP)
        if len(keep_columns) > 0:
            to_drop = [ key for key in self.columns.values if key not in keep_columns ]
            if len(to_drop) == len(self.columns.values):
                to_drop = []
        else:
            to_drop = [key for key in remove_columns if key in self.columns.values]
        if len(to_drop) > 0:
            if verbose: print("Dropping unwanted columns...")
            self.__init__( self.drop(to_drop, axis = 1) )

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
        if verbose: print("Standardization complete.")

    def from_pickle( self, pickle_file, standardize = True ):
        #self._twitter_frame = pd.read_pickle( pickle_file )
        self.__init__( pd.read_pickle(pickle_file), standardize = standardize )
        #self.standardize()

    def read_pickle( self, pickle_file, standardize = True ):
        #self._twitter_frame = pd.read_pickle( pickle_file )
        return TwitterFrame( pd.read_pickle(pickle_file), standardize = standardize )
        #self.standardize()

    def from_twitter_json( self, json_file, standardize = True ):
        with open(json_file, 'r') as fin:
            data = fin.readlines()
        self.__init__(
                       pd.read_json( '[' + ','.join(x.strip() for x in data) + ']' ),
                       standardize = standardize
                     )

    def read_twitter_json( self, json_file, standardize = True ):
        with open(json_file, 'r') as fin:
            data = fin.readlines()
        return TwitterFrame(
                               pd.read_json( '[' + ','.join(x.strip() for x in data) + ']' ),
                               standardize = standardize
                           )

    def to_twitter_json( self, fpath, start_new = True ):
        try:
            if start_new:
                with open( fpath, 'w' ) as fout: pass
            with open( fpath, 'a' ) as fout:
                for i in range( len(self) ):
                    self.loc[i].to_json( fout )
                    fout.write("\n")
        except Exception as e:
            print("Exception encountered: %s" % e)

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
            return self.text.apply(lambda x: tuple(EMOJI_COMPILED_RE.findall(x)))
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

    def create_location_id( self, location_function, verbose = True, *args, **kwargs ):
        if 'coordinates' in self.columns.values:
            def loc_fn( coords ):
                lon, lat = coords['coordinates']
                return location_function( lon, lat, *args, *kwargs )
            self['location_id'] = self.coordinates.apply( lambda x: loc_fn(x) )
        else:
            if verbose: print("No 'coordinates' column. Location ID not created.")
            return False

    def order_self_by_screen_name( self, verbose = True ):
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
                    return '%s-%s-%s' % (dt_object.month,
                                         dt_object.day,
                                         dt_object.year)
                elif time_delta == 'hour':
                    return '%s-%s-%s %s:00:00' % (dt_object.month,
                                                  dt_object.day,
                                                  dt_object.year,
                                                  dt_object.hour)
                elif time_delta == 'minute':
                    return '%s-%s-%s %s:%s:00' % (dt_object.month,
                                                  dt_object.day,
                                                  dt_object.year,
                                                  dt_object.hour,
                                                  dt_object.minute)
                elif time_delta == 'month':
                    return '%s-%s' % (dt_object.month,
                                      dt_object.year)
                elif time_delta == 'year':
                    return str(dt_object.year)
                elif time_delta == 'second':
                    return '%s-%s-%s %s:%s:%s' % (dt_object.month,
                                                  dt_object.day,
                                                  dt_object.year,
                                                  dt_object.hour,
                                                  dt_object.minute,
                                                  dt_object.second)


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
        except Exception as e:
            print("Exception: %s" % e)
            return False

    def concat_self( self, tf, tf_file_type = None, **kwargs ):
        if tf_file_type is None:
            self.__init__( pd.concat([self, tf], **kwargs) )
        elif tf_file_type == 'pickle':
            self.__init__( pd.concat([self, self.read_pickle(tf)], **kwargs) )
        elif tf_file_type == 'json':
            self.__init__( pd.concat([self, self.read_json(tf)], **kwargs) )

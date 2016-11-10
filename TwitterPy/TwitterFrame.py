from __future__ import print_function, division, absolute_import
import numpy as np
import pandas as pd
import re

from .TweetTokenizer2 import *
from ..PyMoji import *

_RE_TYPE = type(EMOJI_COMPILED_RE)
_DEFAULT_TKNZR = TweetTokenizer().tokenize
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

def from_twitter_json( json_file, standardize = True ):
    with open(json_file, 'r') as fin:
        if standardize:
            return standardize_twitframe( pd.read_json('[' + ','.join(x.strip() for x in fin.readlines()) + ']') )
        else:
            return pd.read_json('[' + ','.join(x.strip() for x in fin.readlines()) + ']')

def to_twitter_json( twitframe, output_path, start_new = True ):
    try:
        if start_new:
            with open( output_path, 'w' ) as fout: pass
        with open( output_path, 'a' ) as fout:
            for i in range( len(twitframe) ):
                try:
                    twitframe.loc[i].to_json( fout )
                    fout.write("\n")
                except Exception as e:
                    print("Exception %s encountered on line %d." % (e,i))
                    continue
    except Exception as e:
        print("Exception encountered: %s" % e)

def tokenize_tweet_text( twitframe,
                         tknzr = _DEFAULT_TKNZR,
                         verbose = True,
                         **kwargs ):
    #return twitframe._twitter_frame.text.apply( lambda x: tknzr(x) )
    try:
        return twitframe.text.apply( lambda x: tuple(tknzr(x, **kwargs)) )
    except Exception as e:
        if verbose: print( "Error %s encountered." % e )
        return None

def extract_screen_name( twitframe, verbose = True ):
    try:
        return twitframe.user.apply(lambda x: x['screen_name'])
    except Exception as e:
        if verbose: print( "Error %s encountered." % e )
        return None

def extract_from_text( twitframe, regex, verbose = True ):
    """
    Extract regex from tweet text by passing either compiled regular expression
    or string pattern to be matched. Defaults to emoji extraction.
    """
    try:
        if isinstance(regex, _RE_TYPE):
            return twitframe.text.apply(lambda x: tuple(regex.findall(x)))
        else:
            return twitframe.text.apply(lambda x: tuple(re.findall(regex, x)))
    except Exception as e:
        if verbose: print( "Error %s encountered." % e )
        return None

def extract_emojis( twitframe, verbose = True ):
    return extract_from_text( twitframe, regex = EMOJI_COMPILED_RE, verbose = verbose )

def extract_hashtags( twitframe, verbose = True ):
    def hashtag_fn( entities ):
        return tuple( [x['text'] for x in entities['hashtags']] )
    return twitframe.entities.apply( lambda x: hashtag_fn(x))

def extract_mentions( twitframe, verbose = True ):
    def mentions_fn( entities ):
        return tuple([ x['screen_name'] for x in entities['user_mentions'] ])
    return twitframe.entities.apply( lambda x: mentions_fn(x) )

def order_by_screen_name( twitframe, verbose = True ):
    if not 'screen_name' in twitframe.columns.values:
        twitframe['screen_name'] = extract_screen_name( twitframe, verbose = verbose )
    try:
        grp = twitframe.groupby( "screen_name", as_index = False )
        return twitframe.merge(grp['user'].agg(len),
                               on='screen_name',
                               how='outer',
                               suffixes=('', '_n')).sort_values(by = ['user_n', 'screen_name', 'created_at'],
                                                                ascending=[0,1,1])
    except Exception as e:
        if verbose: print( "Error %s encountered." % e )
        return twitframe

def standardize_twitframe( twitframe,
                           keep_columns = [],
                           remove_columns = drops,
                           reset_index = True,
                           verbose = True
                         ):

    #deal with un/wanted columns (SAFER TO DROP THAN KEEP)
    if len(keep_columns) > 0:
        to_drop = [ key for key in twitframe.columns.values if key not in keep_columns ]
        if len(to_drop) == len(twitframe.columns.values):
            to_drop = []
    else:
        to_drop = [key for key in remove_columns if key in twitframe.columns.values]
    if len(to_drop) > 0:
        if verbose: print("Dropping unwanted columns...")
        twitframe = twitframe.drop(to_drop, axis = 1)
    #remove repeated rows (uid in id_str)
    twitframe = twitframe.drop_duplicates('id_str')
    #create screen_name column
    if 'user' in twitframe.columns.values:
        if 'screen_name' not in twitframe.columns.values:
            if verbose: print("Extracting screen_name...")
            twitframe['screen_name'] = extract_screen_name( twitframe, verbose = False )
    """
    #order by screen_name users
    twitframe = order_by_screen_name( twitframe, verbose = verbose )
    """
    #return the adjusted TwitterFrame resetting index
    if verbose: print("Standardization complete.")
    return twitframe.reset_index() if reset_index else twitframe

def restrict_within_time( twitframe,
                          by = ['screen_name'],
                          num_allowed = 1,
                          time_delta = 'day',
                          seed = None,
                          verbose = True ):
    if 'user' in twitframe.columns.values:
        if 'screen_name' in by and 'screen_name' not in twitframe.columns.values:
            if verbose: print("Extracting screen_name...")
            twitframe['screen_name'] = extract_screen_name( twitframe, verbose = verbose )
    if 'text' in twitframe.columns.values:
        if 'emojis' in by and 'emojis' not in twitframe.columns.values:
            if verbose: print("Extracting emojis...")
            twitframe['emojis'] = extract_emojis( twitframe, verbose = verbose )
    if 'entities' in twitframe.columns.values:
        if 'hashtags' in by and 'hashtags' not in twitframe.columns.values:
            if verbose: print("Extracting hashtags...")
            twitframe['hashtags'] = extract_hashtags( twitframe, verbose = verbose )
        if 'mentions' in by and 'mentions' not in twitframe.columns.values:
            if verbose: print("Extracting mentions...")
            twitframe['mentions'] = extract_mentions( twitframe, verbose = verbose )
    grpby = [ key for key in by if key in twitframe.columns.values ]

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
            grp = twitframe.groupby( [*grpby, twitframe.created_at.apply(lambda x: time_fn(x))],
                                     as_index = False )
        else:
            grp = twitframe.groupby( [*grpby], as_index = False )

        def sel_fn( g ):
            np.random.seed( seed )
            return g.iloc[ np.random.choice( range(0,len(g)),
                                             num_allowed, replace=False ), : ]

        if verbose: print("Selecting %d random choice(s) from groups..." % num_allowed)
        twitframe = grp.apply( lambda x: sel_fn(x) )
    except Exception as e:
        print("Exception: %s" % e)
        pass
    finally:
        return twitframe

__all__ = [
            "from_twitter_json",
            "to_twitter_json",
            "tokenize_tweet_text",
            "extract_screen_name",
            "extract_emojis",
            "extract_mentions",
            "extract_hashtags",
            "extract_from_text",
            "extract_emojis",
            "order_by_screen_name",
            "standardize_twitframe",
            "restrict_within_time"
          ]

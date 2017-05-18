from __future__ import absolute_import, print_function, division
from os import listdir
from os.path import isfile, isdir, join, basename, dirname
import json

from .TwitterExtras import adjTweetCoords
from .TweetTokenizer2 import TweetTokenizer
from ..PyMoji import EMOJI_COMPILED_RE

#helper function: makes a list of filesnames from passed argument
def json_files_from_list(files_path_dir):
    files = None
    if type(files_path_dir) == list:
        files = [f for f in files_path_dir if isfile(f) and 'json' in f]
    elif type(files_path_dir) == str:
        if isdir(files_path_dir):
            files = [join(files_path_dir,f) for f in listdir(files_path_dir) if isfile(join(files_path_dir,f))]
        elif isfile(files_path_dir):
            files = [files_path_dir]
    return files

drops =  [
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

class dictTwiterator( object ):
    def __init__(self, json_files, drops = drops, verbose = True):
        self.json_files = json_files_from_list(json_files)
        self.verbose = verbose
        self.drops = drops

    def standardize_tweet_dict( self, tweet, verbose = True ):
        try:
            tweet_dict = tweet
            tweet_dict['screen_name'] = tweet['user']['screen_name']
            tweet_dict['emojis'] = tuple( EMOJI_COMPILED_RE.findall( tweet['text'] ) )
            tweet_dict['hashtags'] = tuple( tweet['entities']['hashtags'] )
            tweet_dict['mentions'] = tuple( tweet['entities']['user_mentions'] )
            tweet_dict['adjusted_coordinates'] = adjTweetCoords( tweet )
            for key in self.drops:
                tweet_dict.pop( key, None )
            return tweet_dict
        except Exception as e:
            if verbose:
                print("Exception: %s" % e)
            return None

    def __iter__( self ):
        for f in self.json_files:
            try:
                with open(f, 'r') as fin:
                    for line in fin:
                        try:
                            tweet = json.loads( line )
                            tweet_dict = self.standardize_tweet_dict( tweet, verbose = self.verbose )
                            if not tweet_dict is None:
                                yield tweet_dict
                        except Exception as e:
                            if self.verbose:
                                print("Exception: %s" % e)
                            continue
            except IOError as ioe:
                if self.verbose:
                    print("IO Error %s" % ioe)
                    print("--Filenmame: %s" % basename(f))
                    print("--Directory: %s." % dirname(f))
                yield None
            except Exception as e:
                if self.verbose:
                    print("Exception encountered: %s" % e)
                raise( e )

class textTwiterator( object ):
    def __init__(self, json_files, processor = TweetTokenizer().tokenize, verbose = True):
        self.json_files = json_files_from_list(json_files)
        self.processor = processor
        self.verbose = verbose

    def __iter__( self ):
        for f in self.json_files:
            try:
                with open(f, 'r') as fin:
                    for line in fin:
                        try:
                            tweet = json.loads( line )
                            yield self.processor( tweet['text'] )
                        except Exception as e:
                            if self.verbose:
                                print("Exception: %s" % e)
                            continue
            except IOError as ioe:
                if self.verbose:
                    print("IO Error %s" % ioe)
                    print("--Filenmame: %s" % basename(f))
                    print("--Directory: %s." % dirname(f))
                yield None
            except Exception as e:
                if self.verbose:
                    print("Exception encountered: %s" % e)
                raise( e )

__all__ = [
            "dictTwiterator",
            "textTwiterator"
          ]

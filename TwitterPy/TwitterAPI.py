from __future__ import print_function, absolute_import, division
from datetime import datetime as dt
from dateutil import parser
import random
import time
import json

#IO stuff
import sys
from os.path import isfile, dirname, basename
try:
    import cPickle as pickle
except:
    import pickle

#Necessary Imports for streaming API
import sys, time
from tweepy import StreamListener, OAuthHandler, API, Cursor
from tweepy.streaming import Stream

#Twitter API Funs
def twitAuth(consumer_key, consumer_secret, access_token, access_secret):
    auth = OAuthHandler(consumer_key,consumer_secret)
    auth.set_access_token(access_token,access_secret)
    return auth

def twitStream(consumer_key,
               consumer_secret,
               access_token,
               access_secret,
               ondata_handler, #function direction where to send data
               track = None, #None default value
               locations = None, #None default value
               verbose = False,
               **kwargs):

    auth = twitAuth(consumer_key,consumer_secret,access_token,access_secret)

    class CustomStreamListener(StreamListener):
        def on_data(self, data):
            return ondata_handler(data, **kwargs)

        def on_exception(self,status_code):
            print('Exception %s sent during stream.' % status_code)
            return True

        def on_error(self, status_code):
            print('Exception %s sent during stream.' % status_code)
            if 400 <= status_code < 500:
                print('Allocation issues. Pausing...', file=sys.stderr)
                time.sleep(900)
                return True #Try again after 15 minutes
            ## print error to sys.stderr file (using >> formating, for python 2.x)
            print('Encountered error with status code: %s' % status_code, file = sys.stderr)
            return True # Don't kill the stream

        def on_timeout(self):
            ## print error to sys.stderr file (using >> formating, for python 2.x)
            print('Timeout...', file = sys.stderr)
            return True # Don't kill the stream


    while True:
        try:
            sapi = Stream(auth, CustomStreamListener())
            sapi.filter(track = track, locations = locations)

        except:
            time.sleep(60)
            pass

def twitRestAPI( consumer_key,
                 consumer_secret,
                 access_token,
                 access_secret,
                 **kwargs ):
    auth = twitAuth( consumer_key, consumer_secret, access_token, access_secret )
    kwargs['retry_count'] = kwargs.get('retry_count', 1)
    kwargs['wait_on_rate_limit'] = kwargs.get('wait_on_rate_limit',True)
    kwargs['wait_on_rate_limit_notify'] = kwargs.get('wait_on_rate_limit_notify',True)
    return API( auth, **kwargs )

def all_follower_ids( api, usr_name_or_id, sleep_btwn = 60 ):
    ids = []
    for page in Cursor(api.followers_ids, usr_name_or_id).pages():
        ids.extend(page)
        #go slow to avoid rate limits: 15 requests in 15 min
        time.sleep( sleep_btwn )
    return ids

def all_timeline( api, usr_name_or_id, include_rts = False, sleep_btwn = 1 ):
    '''
    - Twitter restricts us to 3200 of the most recent tweets, which is a reasonable
    quantity for non-heavy users. However, with count = 200 (per page), we are allowed
    16 pages of data (16*200=3200).
    - include_rts is a logical where if T will include retweets in our collected data.
    - sleep_btwn tells us how long (in sec) to sleep between requests to
    avoid hitting rate limits: 1 for user, 0.6 for app
    '''
    statuses = []
    for page in Cursor( api.user_timeline,
                        id = usr_name_or_id,
                        count = 200,
                        include_rts = include_rts ).pages():
        statuses.extend( page )
        #go slow to avoid rate limits: 900 (user) or 1500 (app) in 15 minutes
        time.sleep( sleep_btwn )
    return( statuses )

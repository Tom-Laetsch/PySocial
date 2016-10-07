from __future__ import print_function, division
import numpy as np
import pandas as pd
from .IO_Helpers import files_from_list

def tweet_lon_lat(tweet, verbose = False):
    try:
        lat, lon = tweet['geo']['coordinates']
        return lon, lat
    except Exception as e:
        if verbose:
            print("Exception %s retrieving geotag.")
        return None, None

def in_bounding_box(tweet,
                    box = [-74.265,40.49,-73.655,40.93], #default NYC
                    verbose = False):
    lonmin = box[0]
    lonmax = box[2]
    latmin = box[1]
    latmax = box[3]

    lon, lat = tweet_lon_lat(tweet, verbose)
    if lat != None and lon != None:
        if lonmin <= lon <= lonmax and latmin <= lat <= latmax:
            return True
    return False

    #try:
    #    if tweet['geo'] != None:
    #        try:
    #            lon = tweet['geo']['coordinates'][1]
    #            lat = tweet['geo']['coordinates'][0]
    #            if lonmin <= lon <= lonmax and latmin <= lat <= latmax:
    #                return True
    #        except KeyError:
                #'coordinates' not in tweet['geo'].keys()
    #            return False
    #except KeyError:
        #'geo' not in tweet.keys()
    #    return False
    #return False

def JSON_to_DF(jsonfiles):
    files = files_from_list(jsonfiles)
    data_str = ''
    for f in files:
        with open(f, 'r') as fin:
            data_str += ','.join(line.strip() for line in fin.readlines())
    data_str = '[' + data_str + ']'
    return pd.read_json(data_str)

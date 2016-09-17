from __future__ import print_function

def tweet_lat_lon(tweet, verbose = False):
    try:
        return tweet['geo']['coordinates']
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

    lat, lon = tweet_lat_lon(tweet, verbose)
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

from __future__ import absolute_import, division, print_function

def adjTweetCoords( tweet_dict, verbose = True ):
    coords = tweet_dict['coordinates']
    if not coords is None:
        try:
            return tuple( coords['coordinates'] )
        except Exception as e:
            if verbose:
                print("Exception: %s" % e)
            pass
    place = tweet_dict['place']
    if not place is None:
        try:
            lons,lats = list( zip(*place['bounding_box']['coordinates'][0]) )
            return tuple( [sum(lons)/len(lons), sum(lats)/len(lats)] )
        except Exception as e:
            if verbose:
                print("Exception: %s" % e)
            pass
    return tuple( [None, None] )


__all__ = [
            "adjTweetCoords"
          ]

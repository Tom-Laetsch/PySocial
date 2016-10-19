from __future__ import absolute_import, print_function, division
from os import listdir
from os.path import isfile, isdir, join, basename, dirname
import json, re, pprint
from PySocial import TweetTokenizer, EMOJI_RE, EMOJI_LIST

pp = pprint.PrettyPrinter(indent = 4)

#helper function: makes a list of filesnames from passed argument
def files_from_list(files_path_dir):
    files = None
    if type(files_path_dir) == list:
        files = [f for f in files_path_dir if isfile(f)]
    elif type(files_path_dir) == str:
        if isdir(files_path_dir):
            files = [join(files_path_dir,f) for f in listdir(files_path_dir) if isfile(join(files_path_dir,f))]
        elif isfile(files_path_dir):
            files = [files_path_dir]
    return files

#helper function: Standard IOError message
def IOError_message(fpath, err = ''):
    print("IO Error %s" % err)
    print("--Filenmame: %s" % basename(fpath))
    print("--Directory: %s." % dirname(fpath))


#creates an iterator loading lines of json text
class JSON_Line_Iterator(object):
    #an iterator for tweets from json file
    def __init__(self, jsonfiles, verbose = False):
        self.jsonfiles = files_from_list(jsonfiles)
        if self.jsonfiles is None:
            print("No JSON files found.")
        self.verbose = verbose
    def __iter__(self):
        for f in self.jsonfiles:
            try:
                with open(f, 'r') as jf:
                    for i,line in enumerate(jf):
                        try:
                            yield json.loads(line)
                        except Exception as e:
                            if self.verbose:
                                print("Exception on line %d of file %s: \n-%s" % (i,basename(f),e))
                            pass
            except IOError:
                if self.verbose: IOError_message(f)
                yield None

def emojiInText( tweet, verbose = False ):
    try:
        return len( re.findall(EMOJI_RE, tweet['text']) ) > 0
    except Exception as e:
        if verbose:
            print("Exception: %s" % e)
            print("----------------")
            print("In tweet: %s" % tweet)
            print("----------------")
            print("")
        return False

def placeInCountry( tweet, country_code = 'US', verbose = False):
    try:
        place = tweet['place']
        if isinstance(place,dict):
            cc = place.get('country_code')
            return cc == country_code
        return False
    except Exception as e:
        if verbose:
            print("Exception: %s" % e)
            print("----------------")
            print("In tweet: %s" % tweet)
            print("----------------")
            print("")
        return False

class JSONTwiterator( object ):
    def __init__( self, tjsonfiles, criteria = ['placeInCountry'], verbose = False ):
        self._json_line_iterator = JSON_Line_Iterator( tjsonfiles, verbose )
        self.criteria_dict = dict()
        self.criteria = criteria
        self.set_criteria( criteria )


    def set_criteria( self, criteria ):
        if criteria is None:
            return False
        criteria = list(criteria)
        available = {
                        'emojiInText' : {'function': emojiInText,
                                         'kwargs': {
                                                     'verbose': False
                                                   }
                                        },
                        'placeInCountry': {'function': placeInCountry,
                                           'kwargs': {
                                                      'country_code': 'US',
                                                      'verbose': False
                                                     }
                                          }
                     }
        unavailable = [c for c in criteria if c not in available.keys()]
        if len(unavailable) > 0:
            print('Not all criteria give are available.')
            print('Available criteria are:')
            for key in available.keys():
                print('- %s' % key)
                for kwval, val in available[key]['kwargs'].items():
                    print('\t-kwargs: %s | default = %s' % (kwval,val))
            print("Please use set_criteria( criteria = list_of_criteria_names ). No criteria set.")
            return False
        else:
            self.criteria_dict.update({key: available[key] for key in criteria})
            return True

    def _criteria_fun( self, tweet ):
        for _, criteria in self.criteria_dict.items():
            if isinstance(criteria, dict):
                function = criteria.get('function', lambda x: True)
                kwargs = criteria.get('kwargs', dict())
                if not function( tweet, **kwargs ): return False
        return True

    def __iter__( self ):
        for tweet in self._json_line_iterator:
            if self._criteria_fun( tweet ):
                yield tweet

from __future__ import print_function
from os import listdir
from os.path import isfile, isdir, join, basename, dirname
from gensim.models.doc2vec import TaggedDocument
from .TweetTokenizer2 import TweetTokenizer
from .TwitterTools import tweet_lat_lon
import json

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

def IOError_message(fpath):
    print("IO Error")
    print("--Filenmame: %s" % basename(fpath))
    print("--Directory: %s." % dirname(fpath))

class JSON_Line_Iterator(object):
    #an iterator for tweets from json file
    def __init__(self, jsonfiles, verbose = False):
        self.jsonfiles = files_from_list(jsonfiles)
        if self.jsonfiles == None:
            print("No JSON files found.")
        self.verbose = verbose
    def __iter__(self):
        for f in self.jsonfiles:
            try:
                with open(f, 'r') as jf:
                    for i,line in enumerate(jf):
                        try:
                            yield json.loads(line)
                        except ValueError:
                            if self.verbose:
                                print("JSON Decode Error with line %d in file %s." % (i, basename(f)))
                            pass
            except IOError:
                IOError_message(f)
                yield None


class Token_Twiterator(object):
        def __init__(self,
                     jsonfiles,
                     text_processor = 'TweetTokenizer',
                     token_condition = lambda x: True,
                     stop_words = set(),
                     alt_ret = [], #alternate keys to return
                     verbose = False,
                     **text_processor_kwargs):
            self._JSON_Line_Iterator = JSON_Line_Iterator(jsonfiles, verbose)
            if text_processor == 'TweetTokenizer':
                self.text_processor = TweetTokenizer(**text_processor_kwargs).tokenize
            else:
                self.text_processor = text_processor
            self.token_condition = token_condition
            self.stop_words = stop_words
            self.alt_ret = alt_ret

        def __iter__(self):
            for tweet in self._JSON_Line_Iterator:
                try:
                    proc_text = [word for word in self.text_processor(tweet['text']) if word not in self.stop_words]
                    if self.token_condition(proc_text):
                        if len(self.alt_ret) > 0:
                            ret_dict = dict()
                            for key in self.alt_ret:
                                try:
                                    ret_dict[key] = tweet[key]
                                except KeyError:
                                    pass
                            yield proc_text, ret_dict
                        else:
                            yield proc_text
                    else:
                        pass
                except KeyError:
                    pass

class D2V_Twiterator(object):
    def __init__(self,
                 jsonfiles,
                 text_processor = 'TweetTokenizer',
                 token_condition = lambda x: True,
                 stop_words = set(),
                 d2v_label = "DOC_VEC",
                 verbose = False,
                 **text_processor_kwargs):
            self._twiterator = Token_Twiterator(jsonfiles,
                                                text_processor,
                                                token_condition,
                                                stop_words,
                                                verbose,
                                                **text_processor_kwargs)
            self.d2v_label = d2v_label

    def __iter__(self):
        for i, text_tokens in enumerate(self._twiterator):
            doc_label = "%s_" % i + self.d2v_label
            yield TaggedDocument(words = text_tokens, tags = [doc_label])

class Geo_Twiterator(object):
    def __init__(self,
                 jsonfiles,
                 ret_wo_geo = False, #bool, return even if not geotagged?
                 alt_ret = [], #list of tweet keys to return other than geo
                 verbose = False):
        self._JSON_Line_Iterator = JSON_Line_Iterator(jsonfiles, verbose)
        self.ret_wo_geo = ret_wo_geo
        self.alt_ret = alt_ret
        self.verbose = verbose

    def __iter__(self):
        for tweet in self._JSON_Line_Iterator:
            lon, lat = tweet_lat_lon(tweet, self.verbose)
            if lon == None or lat == None:
                if not self.ret_wo_geo:
                    continue #skip to the next iteration
            if len(self.alt_ret) > 0:
                ret_dict = dict()
                for key in self.alt_ret:
                    try:
                        ret_dict[key] = tweet[key]
                    except KeyError:
                        ret_dict[key] = None
                yield lon, lat, ret_dict
            else:
                yield lon, lat

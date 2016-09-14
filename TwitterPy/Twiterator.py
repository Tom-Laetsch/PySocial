from __future__ import print_function
from os import listdir
from os.path import isfile, isdir, join, basename, dirname
from gensim.models.doc2vec import TaggedDocument
from .TweetTokenizer2 import TweetTokenizer
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
                     stop_words = set(),
                     condition = lambda x: True,
                     verbose = False,
                     **text_processor_kwargs):
            self._JSON_Line_Iterator = JSON_Line_Iterator(jsonfiles, verbose)
            if text_processor == 'TweetTokenizer':
                self.text_processor = TweetTokenizer(**text_processor_kwargs).tokenize
            else:
                self.text_processor = text_processor
            self.stop_words = stop_words
            self.condition = condition

        def __iter__(self):
            for tweet in self._JSON_Line_Iterator:
                try:
                    proc_text = [word for word in self.text_processor(tweet['text']) if word not in self.stop_words]
                    if self.condition(proc_text):
                        yield proc_text
                    else:
                        pass
                except KeyError:
                    pass

class D2V_Twiterator(object):
    def __init__(self,
                 jsonfiles,
                 text_processor = 'TweetTokenizer',
                 stop_words = set(),
                 condition = lambda x: True,
                 d2v_label = "DOC_VEC",
                 verbose = False,
                 **text_processor_kwargs):
            self._twiterator = Tweet_Text_Twiterator(jsonfiles,
                                                     text_processor,
                                                     stop_words,
                                                     condition,
                                                     verbose,
                                                     **text_processor_kwargs)
            self.d2v_label = d2v_label

    def __iter__(self):
        for i, text_tokens in enumerate(self._twiterator):
            doc_label = "%s_" % i + self.d2v_label
            yield TaggedDocument(words = text_tokens, tags = [doc_label])

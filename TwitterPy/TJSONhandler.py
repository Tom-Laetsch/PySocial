from __future__ import absolute_import, print_function, division
from os import listdir
from os.path import isfile, isdir, join, basename, dirname
import json

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

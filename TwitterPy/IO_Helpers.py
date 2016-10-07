from __future__ import print_function, division
from os import listdir
from os.path import isfile, isdir, join, basename, dirname

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


def IOError_message(fpath, err = ''):
    print("IO Error %s" % err)
    print("--Filenmame: %s" % basename(fpath))
    print("--Directory: %s." % dirname(fpath))

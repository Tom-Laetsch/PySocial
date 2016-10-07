from __future__ import print_function, division
import numpy as np
import pandas as pd
import os, re

def Twitter_JSON_to_DF(filepath):
    try:
        with open(filepath, 'r') as fin:
            #Strip off '\n' from each line:
            ## data = map(lambda x: x.strip() for x in fin.readlines())
            #Convert to single long json object:
            ## data = '[' + ','.join(data) + ']'
            #Read into dataframe
            ## twit_df = pd.read_json(data)
            #All together:
            twit_df = pd.read_json('[' + ','.join(x.strip() for x in fin.readlines()) + ']')
    except Exception as e:
        print("Error: %s encountered while reading and converting JSON to dataframe." % e)
        return e
    return twit_df

def ScreenName_Reduce(twit_df,
                      time_delta = '1Day'):
    if time_delta == '1Hour':
        time_lambda = lambda x: (x.year, x.month, x.day, x.hour)
    else:
        time_lambda = lambda x: (x.year, x.month, x.day)
    grp = twit_df.groupby([twit_df.user.map(lambda x: x['screen_name']),
                           twit_df.created_at.map(time_lambda)])
    return grp.apply(lambda obj: ...)

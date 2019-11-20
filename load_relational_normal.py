# -*- coding: utf-8 -*-
"""
Created on Fri Oct 18 21:54:47 2019

@author: Nathan

Reads in all the normal crawl data into a single pandas dataframe
clean_data.py needs to be run on the data first so that it will read in correctly to pandas
"""

import pandas as pd
import os
import re
import time

start = time.time()
os.chdir('.\Data') # This is the folder I have all the data in
master = pd.DataFrame()

'''
'NormalCrawl' is the subfolder with all the data for the normal crawl,
so .\Data\NormalCrawl, is where it's stored.
If you want to read different data, obviously change this.
'''
for root, dirs, files, in os.walk('NormalCrawl', topdown=True):
    for fname in files:
        if re.match('\d.txt', fname): # looks for all files that are "a number".txt
            print(os.path.join(root,fname))
            #this is only using the first 9 columns. If you want all, then get rid of this parameter
            temp_db = pd.read_csv(os.path.join(root,fname), usecols=[*range(9)], header=None, sep='\t', error_bad_lines=False)
            master = pd.concat([master, temp_db]) # this is the dataframe object you can reference

end = time.time()
print(round(end-start, 2))

'''
Once this is done you export master to csv, using the command: master.to_csv().
You may have to specify where you want to export it, and what name you want to give the file
'''
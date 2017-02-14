# -*- coding: utf-8 -*-
"""
Created on Mon Feb 13 20:57:22 2017

Post analysis on option data quality and a possible change to update frequency

@author: Jingmin Zhang
"""

PROJECT_PATH = (r'C:\Users\GlowingToilet\Google Drive\Projects'
                + r'\yahoo-option-scraper')

from configparser import ConfigParser
import pandas as pd
import sqlite3
import numpy as np

# Read config
config_path = PROJECT_PATH + '/config_nasdaq100.ini'
config = ConfigParser()
config.read(config_path)

# Get the most recent symbol list from database
sql = ('SELECT Symbol FROM '
       + config['CURRENT']['SymbolTableName']
       + ' WHERE Date in ('
       + ' SELECT max(Date) FROM '
       + config['CURRENT']['SymbolTableName']
       + ')')

conn = sqlite3.connect(config['CURRENT']['DatabasePath'])
symbols = pd.read_sql(sql, conn)
conn.close()

# Add bin number to the symbols
batch_size = int(config['CURRENT']['BatchSize'])
num_bins = np.ceil(symbols.shape[0] / batch_size)
bins = pd.cut(symbols.index, num_bins, labels=False)

symbols.merge(pd.DataFrame(bins, columns=['Bin']), 
              left_index=True, right_index=True)


# Loop through each symbol to get following statistics:
#   1. Total option volume by timestamp
#   2. Total volume by EOD
#   3. Number of unique timestamps (# of pulls from Yahoo)
#   4. Min and max intervals between nearest positive volumes

# 1. Total optiona volume by timestamp
sql = ('SELECT Symbol, sum(Volume) as Volume, "Download Time" FROM '
       + config['CURRENT']['DataTableName']
       + ' WHERE "Download Date" = "2017-02-13"'
       + ' GROUP BY Symbol, "Download Time"')

conn = sqlite3.connect(config['CURRENT']['DatabasePath'])
vol_by_time = pd.read_sql(sql, conn)
conn.close()


# 2. Total volume by EOD
vol_by_eod = vol_by_time.pivot_table(values='Volume', 
                                     columns=['Symbol'], 
                                     aggfunc=np.sum)

# 3. Number of unique timestamps (# of pulls from Yahoo)
unique_pulls = vol_by_time.pivot_table(values='Volume',
                                       columns=['Symbol'],
                                       aggfunc=len)

# 4. Min and max intervals between nearest positive volumes
# The following code is criminally slow...Need to use groupby
vol_by_time['tag'] = np.nan
for i in range(vol_by_time.shape[0]):
    if (not i or 
        vol_by_time['Symbol'][i - 1] != vol_by_time['Symbol'][i]):
        tag = 0
        
    if vol_by_time['Volume'][i]:
        tag += 1
        
    vol_by_time['tag'][i] = tag



    
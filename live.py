# -*- coding: utf-8 -*-
"""
Created on Sun Feb  5 23:09:46 2017

@author: Jingmin Zhang
"""

from yahoo_scraper import YahooScraper
from configparser import ConfigParser
import pandas as pd
import numpy as np
import sqlite3
import time
from datetime import datetime

def dynamic_sleep_interval(start_time):
    """Provides dynamic sleep interval"""
    
    eod_time = datetime.now().replace(hour=23, minute=59, second=59)
    time_to_start = start_time - datetime.now()
    
    if time_to_start.total_seconds() <= 0:
        interval = (eod_time - datetime.now()).total_seconds()
    else:
        interval = time_to_start.total_seconds() - 1
                                              
    return np.max([interval, 1]) # At least 1 second to avoid special cases
                                              
        
# Read config
config_path = (r'C:\Users\GlowingToilet\Google Drive\Projects'
               + r'\Yahoo_Option_Scraper\config.ini')
config = ConfigParser()
config.read(config_path)

# Update batch number and close
batch_num = int(config['CURRENT']['BatchNumber'])
config['CURRENT']['BatchNumber'] = str(batch_num + 1)
with open(config_path, 'w') as configfile:
    config.write(configfile)

# Assign symbols to bins
batch_size = int(config['CURRENT']['BatchSize'])
symbols = pd.read_csv(config['CURRENT']['SymbolListPath'])
num_bins = np.ceil(symbols.shape[0] / batch_size)
bins = pd.cut(symbols.index, num_bins, labels=False)
symbols = list(symbols.iloc[bins == batch_num, 0])



while True:
    # Get start and end times
    start_time = eval(config['CURRENT']['StartTimeLocal'])
    start_time = datetime.now().replace(hour=start_time[0],
                                        minute=start_time[1],
                                        second=start_time[2],
                                        microsecond=0)
    
    end_time = eval(config['CURRENT']['EndTimeLocal'])
    end_time = datetime.now().replace(hour=end_time[0],
                                      minute=end_time[1],
                                      second=end_time[2],
                                      microsecond=0)
    
    if datetime.now() > start_time and datetime.now() < end_time:
        ys = YahooScraper(symbols, ext_path=config['CURRENT']['ExtensionPath'])
        ys.scrape_all()
        conn = sqlite3.connect(config['CURRENT']['DatabasePath'])
        ys.save_to_sqlite(config['CURRENT']['TableName'], conn)
        conn.close()
        time.sleep(5) # In session sleep
    else:
        interval = dynamic_sleep_interval(start_time)
        time.sleep(interval)


# Save to DB

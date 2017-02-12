# -*- coding: utf-8 -*-
"""
Created on Sun Feb  5 23:09:46 2017

TODO: calculate delta of volumes each iteration

@author: Jingmin Zhang
"""

PROJECT_PATH = (r'C:\Users\GlowingToilet\Google Drive\Projects'
                + r'\yahoo_option_scraper')

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
config_path = PROJECT_PATH + '/config_nasdaq100.ini'
config = ConfigParser()
config.read(config_path)

# Update batch number and close
batch_num = int(config['CURRENT']['BatchNumber'])
config['CURRENT']['BatchNumber'] = str(batch_num + 1)
with open(config_path, 'w') as configfile:
    config.write(configfile)
    
print('Current Batch Number: {}'.format(batch_num))
read_symbols = True

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
        if read_symbols: # Download NASDAQ100 symbol list every day once
            # Avoid duplicate downloads on multiple instances
            if not batch_num: # Batch 0
                try:
                    df = pd.read_html('http://www.cnbc.com/nasdaq-100/')
                    df = pd.DataFrame(df[0].append(df[1], 
                                      ignore_index=True)['Symbol'])
                    df['Date'] = str(datetime.date(datetime.now()))
                    conn = sqlite3.connect(config['CURRENT']['DatabasePath'])
                    df.to_sql(config['CURRENT']['SymbolTableName'], conn, 
                              if_exists = 'append', index=False)
                    conn.close()
                except Exception as inst:
                    print(repr(inst))
           
            time.sleep(5)  # Wait for list update from Batch 0
            
            # Read symbol list from database
            conn = sqlite3.connect(config['CURRENT']['DatabasePath'])
            sql = ('SELECT DISTINCT Symbol FROM '
                   + config['CURRENT']['SymbolTableName']
                   + ' WHERE Date IN '
                   + '(SELECT Date FROM '
                   + config['CURRENT']['SymbolTableName']
                   + ' ORDER BY date DESC LIMIT 1)')
            symbols = pd.read_sql_query(sql, conn)
            conn.close() 
                
            read_symbols = False

            # Assign symbols to bins
            batch_size = int(config['CURRENT']['BatchSize'])
            num_bins = np.ceil(symbols.shape[0] / batch_size)
            bins = pd.cut(symbols.index, num_bins, labels=False)
            symbols = list(symbols.iloc[bins == batch_num, 0])
            
            # If no symbols
            if not len(symbols):
                break

        ys = YahooScraper(symbols, ext_path=config['CURRENT']['ExtensionPath'])
        ys.scrape_all()
        conn = sqlite3.connect(config['CURRENT']['DatabasePath'])
        ys.save_to_sqlite(config['CURRENT']['DataTableName'], conn)
        conn.close()
        time.sleep(2) # Prevent too frequent looping
    else:
        print('Out of trading session...Sleeping...')
        interval = dynamic_sleep_interval(start_time)
        time.sleep(interval)
        read_symbols = True

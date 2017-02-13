# -*- coding: utf-8 -*-
"""
Created on Sun Feb  5 23:09:46 2017

@author: Jingmin Zhang
"""

PROJECT_PATH = (r'C:\Users\GlowingToilet\Google Drive\Projects'
                + r'\yahoo-option-scraper')

from yahoo_scraper import YahooScraper
from configparser import ConfigParser
import pandas as pd
import numpy as np
import sqlite3
import time
from datetime import datetime
from pandas.io.sql import DatabaseError

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
        if read_symbols: # Only run once per trading session
            # Check if symbol list is updated today
            try:
                sql = ('SELECT DISTINCT max(Date) as Date FROM '
                       + config['CURRENT']['SymbolTableName'])
                conn = sqlite3.connect(config['CURRENT']['DatabasePath'])
                last_update_date = pd.read_sql_query(sql, conn)
                last_update_date = last_update_date.values[0][0]
                is_upto_date = last_update_date == str(datetime.now().date())
            except:
                print('Failed to request symbols from database, '
                      'downloading symbol list now.')
                is_upto_date = False

            if not is_upto_date:
                if not batch_num: # Batch 0 will update the list
                    try:
                        df = pd.read_html('http://www.cnbc.com/nasdaq-100/')
                        df = pd.DataFrame(df[0].append(df[1], 
                                          ignore_index=True)['Symbol'])
                        df['Date'] = str(datetime.date(datetime.now()))
                        conn = sqlite3.connect(
                            config['CURRENT']['DatabasePath'])
                        df.to_sql(config['CURRENT']['SymbolTableName'], conn, 
                                  if_exists='append', index=False)
                        conn.close()
                    except Exception as inst:
                        print(repr(inst))
                else:
                    is_updated = False
                    while not is_updated: # Wait for list to be updated
                        print('Waiting for symbol list to be updated...')
                        try:
                            conn = sqlite3.connect(
                                config['CURRENT']['DatabasePath'])
                            last_update_date = pd.read_sql_query(sql, conn)
                            last_update_date = last_update_date.values[0][0]
                            is_updated = (
                                last_update_date == str(datetime.now().date()))
                        except:
                            is_updated = False
                            
                        time.sleep(1)
                                    
            # Read symbol list from database
            sql = ('SELECT DISTINCT Symbol FROM '
                   + config['CURRENT']['SymbolTableName']
                   + ' WHERE Date IN '
                   + '(SELECT Date FROM '
                   + config['CURRENT']['SymbolTableName']
                   + ' ORDER BY date DESC LIMIT 1)')
            conn = sqlite3.connect(config['CURRENT']['DatabasePath'])
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

        try:
            # Pull cumulative sum of option volumes till now for today
            sql_symbols = ','.join(['"' + s + '"' for s in ys.symbols])
            
            sql = ('SELECT "Contract Name", sum(Volume) as Volume_sum FROM '
                   + config['CURRENT']['DataTableName']
                   + ' WHERE "Download Date" = "'
                   + str(datetime.now().date())
                   + '" AND Volume IS NOT NULL'
                   + ' AND Symbol in ('
                   + sql_symbols
                   + ')'
                   + ' GROUP BY "Contract Name" ')
            conn = sqlite3.connect(config['CURRENT']['DatabasePath'])
            vol_sum = pd.read_sql_query(sql, conn)
            conn.close()
            
            # Calculate the incremental change
            df = ys.data.merge(vol_sum, how='left', on='Contract Name')
            df['Volume_sum'] = df['Volume_sum'].apply(
                    lambda x: 0 if np.isnan(x) else x)
            
            df['Volume'] = df['Volume'] - df['Volume_sum']
            df['Volume'] = df['Volume'].apply(
                    lambda x: 0 if x < 0 or np.isnan(x) else x)
            
            ys.data = df.drop('Volume_sum', axis=1)
        except DatabaseError:
            print('No historical data of today. Writing to the database '
                  'directly...')
        
        # Export the incremental volume to database
        conn = sqlite3.connect(config['CURRENT']['DatabasePath'])
        ys.save_to_sqlite(config['CURRENT']['DataTableName'], conn)
        conn.close()
        time.sleep(1) # Prevent too frequent looping
    else:
        print('Out of trading session...Sleeping...')
        interval = dynamic_sleep_interval(start_time)
        time.sleep(interval)
        read_symbols = True

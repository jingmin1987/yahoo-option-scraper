# -*- coding: utf-8 -*-
"""
Created on Sun Feb  5 23:24:09 2017

Configuration file generator.

@author: Jingmin Zhang
"""

PROJECT_PATH = (r'C:\Users\GlowingToilet\Google Drive\Projects'
                + r'\yahoo-option-scraper')

from configparser import ConfigParser

save_path = PROJECT_PATH
config_path = PROJECT_PATH + '/config_nasdaq100.ini'
ext_path = r'C:\Users\GlowingToilet\Downloads\extension_1_10_4.crx'
database_path = (r'C:\Users\GlowingToilet\Google Drive\Databases'
                 + '\SQLite3\yahoo_options.db')
nas100_url = 'http://www.cnbc.com/nasdaq-100/'
data_tb = 'data_nasdaq100'
symbol_tb = 'symbol_nasdaq100'

config = ConfigParser()
config['DEFAULT'] = {
    'BatchSize': '15',
    'SavePath': save_path,
    'ExtensionPath': ext_path,
    'DatabasePath': database_path,
    'DataTableName': data_tb,
    'SymbolTableName': symbol_tb,
    'StartTimeLocal': '(9, 45, 30)',
    'EndTimeLocal': '(16, 15, 0)',
    'NASDAQ100': nas100_url,
    'RunWeekdays': '[0, 1, 2, 3, 4]'
}
 
config['CURRENT'] = {
    'BatchNumber': '0'        
}

with open(config_path, 'w') as configfile:
    config.write(configfile)
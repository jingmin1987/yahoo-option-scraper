# -*- coding: utf-8 -*-
"""
Created on Thu Feb  9 18:49:44 2017

@author: Jingmin Zhang
"""

import os
import time

# System parameter
NUM_OF_PROCESS = 15

# Reset config
cmd = (r'start cmd /c python "C:\Users\GlowingToilet\Google Drive'
       + r'\Projects\Yahoo_Option_Scraper\config_nasdaq100.py"')
os.system(cmd)

# Start scraping
cmd = (r'start cmd /c python "C:\Users\GlowingToilet\Google Drive'
       + r'\Projects\Yahoo_Option_Scraper\live_nasdaq100.py"')
for i in range(NUM_OF_PROCESS):
    os.system(cmd)
    time.sleep(2)
    
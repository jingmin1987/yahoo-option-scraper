# -*- coding: utf-8 -*-
"""
Created on Thu Feb  9 18:49:44 2017

@author: Jingmin Zhang
"""
PROJECT_PATH = (r'C:\Users\GlowingToilet\Google Drive\Projects'
                + r'\yahoo-option-scraper')
import os
import time

# System parameter
NUM_OF_PROCESS = 15

# Reset config
cmd = (r'start cmd /c python "' 
       + PROJECT_PATH 
       + '/config_nasdaq100.py"')
os.system(cmd)

# Start scraping
cmd = (r'start cmd /c python "' 
       + PROJECT_PATH 
       + '/live_nasdaq100.py"')
for i in range(NUM_OF_PROCESS):
    os.system(cmd)
    time.sleep(5) # To avoid prior instance fail to write the batch number
    
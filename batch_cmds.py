# -*- coding: utf-8 -*-
"""
Created on Thu Feb  9 18:49:44 2017

Batch commands to run multiple instances of crawler simultaneously.

@author: Jingmin Zhang
"""
PROJECT_PATH = (r'C:\Users\GlowingToilet\Google Drive\Projects'
                + r'\yahoo-option-scraper')
import os

# System parameter
NUM_OF_PROCESS = 10

# Reset config
cmd = (r'start cmd /c python "' 
       + PROJECT_PATH 
       + '/config_nasdaq100.py"')
os.system(cmd)

# Start scraping
cmd = (r'start cmd /c python "' 
       + PROJECT_PATH 
       + '/live_nasdaq100.py" {}'.format(NUM_OF_PROCESS))
       
os.system(cmd)

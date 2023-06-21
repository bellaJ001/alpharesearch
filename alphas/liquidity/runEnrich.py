# -*- coding: utf-8 -*-
"""
Created on Tue Jun 13 00:20:37 2023

@author: bella
"""


import pandas as pd
import numpy as np
import datetime
import os
os.chdir('C:\\Users\\bella\\OneDrive\\Desktop\\projects\\QiShi\\code')
import logging
import functools as ft
from dataProcess.dataLoader import loadData
from backtester.utils import  get_multi_process_by_month, get_multi_process_by_split
import alphas.liquidity.multiprocess_liq_1min as multiLiq  
import alphas.liquidity.liquidity_1min as liq
#import alphas.liquidity.process_1min_liquidity as liq
import time
import functools as ft
    

for year in [ 2019, 2020]:
    
     multi_times = get_multi_process_by_month(year) 
     if year == 2019:
         multi_times = multi_times[5:]
    # startDates = [i[0] for i in multi_times]
    # endDates = [i[1] for i in multi_times]
    
     #time_ts = []
     errs = []
     tic = time.time()
     
     for month_start_end in multi_times:  
         startDate = month_start_end[0]
         endDate = month_start_end[1]
         print("start one month! {}".format(startDate.strftime('%Y%m%d')))
         err = multiLiq.enrichEodwith1minbyMonth_multi_process(startDate, endDate, processes = 6)
         #err = liq.enrichEodwith1minbyMonth(startDate, endDate, time_ts = time_ts, intervals= liq._intervals)
         errs.append(err)
         print("finish one month!")
    
     toc = time.time()
     print("need {time} for year {y}".format(time = toc - tic, y = year))
     
for year in [ 2019, 2020]:
    
     multi_times = get_multi_process_by_month(year) 
     if year == 2019:
         multi_times = multi_times[8:]
    # startDates = [i[0] for i in multi_times]
    # endDates = [i[1] for i in multi_times]
    
     time_ts = []
     errs = []
     tic = time.time()
     
     for month_start_end in multi_times:  
         startDate = month_start_end[0]
         endDate = month_start_end[1]
         print("start one month! {}".format(startDate.strftime('%Y%m%d')))
         err = liq.enrichEodwith1minbyMonth(startDate, endDate, time_ts = time_ts, intervals= liq._intervals)
         errs.append(err)
         print("finish one month!")
    
     toc = time.time()
     print("need {time} for year {y}".format(time = toc - tic, y = year))
   
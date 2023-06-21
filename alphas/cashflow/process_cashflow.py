# -*- coding: utf-8 -*-
"""
Created on Fri Jun 16 02:28:01 2023

multi process cashflow 1min data into daily

@author: bella
"""

import pandas as pd
import numpy as np
import datetime
import logging
import alphas.cashflow.cashflow_1min as cf
from constants.path import _eod_pv_data_dir

_eod_price_enriched_cf_dir = _eod_pv_data_dir + "price_volume_enriched_cashflow\\" 

def processDailyCashFlow(asofDate, turover_threshold =  40000, active_time = False ):
    data = cf.get_all_1min_data(asofDate)
    cashflow = cf.process_cash_flow(data, turover_threshold , active_time)
    
    try:
        if not os.path.exists(_eod_price_enriched_cf_dir):
            os.mkdir(_eod_price_enriched_cf_dir)   
        
        dir_to_save = _eod_price_enriched_cf_dir + str(asofDate.year) 

        if not os.path.exists(dir_to_save):
            os.mkdir(dir_to_save)     
        path_to_save = dir_to_save + '\\' + asofDate.strftime('%Y%m%d')
        cashflow.to_parquet(path_to_save +  '.parquet', engine='fastparquet')
        #logging.debug('save enriched eod pv data: %s', asofDate.strftime('%Y%m%d'))
        print('save enriched eod pv data: {}'.format( asofDate.strftime('%Y%m%d')))
        return
    except:
        
        #logging.error('exception: %s', asofDate.strftime('%Y%m%d'))
        print('exception: {}'.format( asofDate.strftime('%Y%m%d')))
        return(asofDate.strftime('%Y%m%d'))
    
from multiprocessing import Process, Queue
import  time
from backtester.utils import  get_all_trading_days

def processData_loop_by_day( dates, q):
    for asofDate in dates:
        errs = processDailyCashFlow(asofDate)
        q.put(errs)

def processData_multi_process(startDate, endDate, processes = 10):

    q = Queue()      
   
    dateList = get_all_trading_days(startDate, endDate, holdingPeriod = 1) 
    if not dateList: # empty if no trading dates
        err = [(startDate, endDate)]
        return err

    # split work into 5 or 10 processes
    def splitlist(inlist, chunksize):
        chunksize = int(chunksize)
        return [inlist[x:x+chunksize] for x in range(0, len(inlist), chunksize)]
       
    dateListSplitted = splitlist(dateList, len(dateList)/processes)
    
    print("sub list number: {}".format(len(dateListSplitted)))   
    print("splitted list ready")

    t = time.time()
    for subDatelist in dateListSplitted:
        print("sub list length: {}".format(len(subDatelist)))
        p = Process(target= processData_loop_by_day, args=(subDatelist,q))
        p.Daemon = True
        p.start()
    for subDatelist in dateListSplitted:
        p.join()
    print("total process time for 1m: {}".format(time.time()-t))
    # while True:
    #     print(q.get())
    return q
 
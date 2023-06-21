#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Apr  7 17:13:32 2023

Load 1min or eod data with specidied type

@author: bella
"""

import pandas as pd
import datetime as dt
# os.getcwd()
import os
# os.chdir('//Users/bella/Desktop/projects/QiShi/code') 
os.chdir('C:\\Users\\bella\\OneDrive\\Desktop\\projects\\QiShi\\code')
from constants.path import getEodPVDataPath, get1minPVDataPath, getRefDataPath, _processed_data_root_dir
from dataProcessor.utils import getData


def getRefData(asOfDate, dataType):
    """['lmt', 'mkt_val', 'industry', 'idx_trading', 'idx_univ']
    """
    path = getRefDataPath(asOfDate, dataType)
    if not os.path.exists(path):
        return pd.DataFrame()
    if dataType in ['idx_univ', 'idx_trading']:
        data = getData('parquet', path)
    else: 
        data = getData('csv', path)

    return data
        
def getPVData(asofDate, freq, dataType):
    """ 

    Parameters
    ----------
    freq : str
        '1min', 'eod' 
    dataType : TYPE
        1min : "close", "price", "turnover", "volume", "matchItem"
        eod:  "priceAdj", "volume", "all", "matchItem"

    Returns
    -------
    dataframe
    pv data
    
    """
    if freq == "1min":
        path = get1minPVDataPath(asofDate, dataType )
    elif freq == "eod":
        path = getEodPVDataPath(asofDate,dataType )
    else:
        raise ValueError('freq error')
        return
    if not os.path.exists(path):
        return pd.DataFrame()
    
    data = getData('parquet', path)
    return data

def loadDataDaily(asofDate, dataType):
    """
    dataType : str
    
    reference data type:  'lmt', 'mkt_val', 'industry', 'idx_trading', 'idx_univ', 'adj_fct'
    pv data type : '1min_close', '1min_price', '1min_turnover', '1min_volume', '1min_matchItem', 'eod_priceAdj', 'eod_volume', eod_all', 'eod_matchItem'
    """
    if dataType in ['adj_fct', 'lmt', 'mkt_val', 'industry', 'idx_trading', 'idx_univ']:
        return getRefData(asofDate, dataType)
    elif dataType.startswith('1min'):      
        return getPVData(asofDate, '1min', dataType.split('_')[1])
    elif dataType.startswith('eod'):      
        return getPVData(asofDate, 'eod', dataType.split('_')[1])  # eod_price: raw price
    else:
        raise ValueError('data type error')
        return pd.DataFrame()   
    
    
def loadData(startDate, endDate, dataType, holdingPeriod = 1):
    """  
    reference data type: 'adj_fct', 'lmt', 'mkt_val', 'industry', 'idx_trading', 'idx_univ'
    pv data type : '1min_close', '1min_price', '1min_turnover', '1min_volume', '1min_matchItem', 'eod_priceAdj', 'eod_volume', 'eod_all', 'eod_matchItem'

    """
    dataList= []
    errorList= []
    #dateInterval = str(holdingPeriod) + 'D'
    allDatesDf = pd.read_parquet(_processed_data_root_dir + "dates.parquet")
    allDates = list(allDatesDf['dates'])
    
    dates = [i.date() for i in allDates if i.date() >= startDate and i.date() <= endDate]   
    dates = [i for i in dates[::holdingPeriod]]

    if dataType in ['adj_fct', 'mkt_val', 'industry']:
        path = getRefDataPath(endDate, dataType)
        dataDf = getData('parquet', path)
        dataDf1 = dataDf[ dataDf['date'].isin(dates)]
        return dataDf1
    
    for date_i in dates:
       # date_i = i.date()
        dailyData = loadDataDaily(date_i, dataType )
        
        if not dailyData.empty:  
            dailyData['date'] = date_i
            dailyData['date'] = pd.to_datetime(dailyData['date'])
            dataList.append(dailyData)
        else:
            errorList.append(date_i.strftime('%Y%m%d'))  
    dataDf = pd.concat(dataList, axis = 0)
    return dataDf   
    
    
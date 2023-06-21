# -*- coding: utf-8 -*-
"""
Created on Tue Jun 20 15:53:46 2023

@author: bella
"""

import pandas as pd
import numpy as np
import re

def addRollingStats(df, featureCol, window, min_period = 1):
    """
    e.g. 
    window =5: rolling 5 mins stats
    
    """
    meanCol = featureCol + '_' + str(window) +'mean'
    stdCol = featureCol + '_' + str(window) +'std'  
    zCol = featureCol + '_' + str(window) +'zscore'
    
    df[meanCol] =  df.groupby([ 'ticker'])[featureCol].transform(lambda x: x.rolling(window, min_period).mean())   
    df[stdCol] =  df.groupby(['ticker'])[featureCol].transform(lambda x: x.rolling(window, min_period).std())   
    df[zCol] =  df[meanCol]/df[stdCol]
    
    return df

def winsorize(df, groupbyCol, col, percentile):
    """
    groupbyCol: ['date', 'ticker'] intraday 1min data
                ['date']: cross sectional --> backtest
    percentile: 1,2,5

    """
    lower = 0.01*percentile
    upper = 1-lower
    
    df[col] = df.groupby(groupbyCol)[col].transform(lambda x: np.maximum(x.quantile(lower), np.minimum(x, x.quantile(upper))))

    return df

def getAggStats(df, groupbyCol, aggColList, stats, interval_name ):
    """
    groupbyCol: list,   ['date', 'ticker'] intraday 1min data
                        ['date']: cross sectional --> backtest
    
    aggColList: list, ['volume','turover','accvolume'] --> column wants to calculate stats
    
    stats: str, 'mean', 'std'
    
    interval_name: str, suffix on columns: '_daily', '_morning_open_5'
    
    Returns
    -------
    dataFrame with column like['date','ticker','volume_mean','turover_mean','accvolume_mean']

    """
    if stats == 'mean':       
        df1 = df.groupby(groupbyCol)[aggColList].mean().reset_index()        
        for i in aggColList:
            df1 = df1.rename({i:i+'_mean' + interval_name}, axis = 1)
    
    elif stats == 'std':       
        df1 = df.groupby(groupbyCol)[aggColList].std().reset_index()
        for i in aggColList:
            df1 = df1.rename({i:i+'_std' + interval_name}, axis = 1)
    elif stats == 'sum':       
        df1 = df.groupby(groupbyCol)[aggColList].sum().reset_index()
        for i in aggColList:
            df1 = df1.rename({i:i+'_sum' + interval_name}, axis = 1)
    elif stats == 'count':
        df1 = df.groupby(groupbyCol)[aggColList].count().reset_index()
        for i in aggColList:
            df1 = df1.rename({i:i+'_count' + interval_name}, axis = 1)
                   
    return df1
 
def Filter(string, substr):
    return [str for str in string if
             any(sub in str for sub in substr)]

#e.g. string = data.columns.to_list()
#substrList =  ['mean', 'std', 'zscore']
#print(Filter(string, substr))
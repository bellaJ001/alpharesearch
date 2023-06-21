#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar 30 15:30:38 2023

process index data, tag "1" for ticker if it's a component of an index

@author: bella
"""

import pandas as pd
import datetime
import functools as ft
# os.getcwd()
# os.chdir('//Users/bella/Desktop/projects/QiShi/code') 

from contants.path import getRawRefDataPath,  _index_list, _raw_1min_dir
from dataProcessor.utils import getData
import os


def processIndexUniv(asofDate, indexList = _index_list ):
    """

    Parameters
    ----------
    asofDate : datetime.date
        
    indexList : list
       ['hs300', 'zz500', 'zz800', 'zz1000', 'zz9999']

    Returns
    -------
    indexUnivDf : pd.DataFrame
        cols:[ticker, is_zz800...]

    """
    
    indexUniv = []
    
    for idx in _index_list:
        path  = getRawRefDataPath(asofDate, 'idx_univ', idx)
        data = getData('csv', path)
        if data.empty:
            return pd.DataFrame()
        data['is_'+idx] = 1
        data.drop(['name', 'code'], axis = 1, inplace = True)
        indexUniv.append(data)
        
    indexUnivDf = ft.reduce(lambda left, right: pd.merge(left, right, on='ticker', how = 'outer'), indexUniv)
    for idx in _index_list:       
        indexUnivDf['is_'+idx].fillna(0, inplace = True)
        
    return indexUnivDf


def processIdxData(df):
    """
    rename column name

    """
    
    df.columns = [(i + '_idx') for i in df.columns]
    return df
        


### not using###############
    

#def processRawData(df):
#    """
#    process daily data for each ticker
#    df:  ticker level 1 min trading data

#    """
#    #df['date'] = pd.to_datetime(df['date'])
#    dfClose = df.iloc[-1: ]
    #fdClose= dfClose.drop( columns = [ 'volume', 'turover', 'match_items'])
#    if 'low' in df.columns:
#        dfClose['low'] = df['low'].min()

#    if 'high' in df.columns:
#        dfClose['high'] =  df['high'].max()

#    return dfClose    
#def getDayTickerUniv(asofDate, raw_1min_dir = _raw_1min_dir):
#    """ 
#    get all tickers in trade date
#    """
#    dateStr = asofDate.strftime('%Y%m%d')
#    path = raw_1min_dir + dateStr + '.zip'
#    if os.path.exists(path):
#        zf = zipfile.ZipFile(path, 'r')
#        tickers = [i.split('.')[0] for i in zf.namelist()]
#        zf.close()
#    else: 
#        tickers = []
    
#    return tickers 

    

    
    
    
    
    
    
    


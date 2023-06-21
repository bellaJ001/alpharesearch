#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Apr  7 18:29:00 2023

@author: bella
"""

import pandas as pd
import numpy as np
import datetime as dt
from datetime import timedelta
from constants.path import _processed_data_root_dir
from dataLoader.data_loader import loadData
import statsmodels.api as sm


def neutralize(rawScore, col, startDate, endDate, method):
    """
    neutralize data with different methods, regress and take residuals as adjusted score
    
    Parameters
    ----------
    rawScore : DataFrame
             raw score data (panel) with column: ['date', 'ticker', 'score']
    col: str
         column name to be neutralized
    method: str
            None, 'industry', 'mcap', 'industry_and_mcap'
    
    Returns
    -------
    DataFrame 
    adjusted score data (panel) with column: ['date', 'ticker', 'score_adj']
    
    """
    if method == None:
        rawScore[col] = rawScore.groupby('date')[col].transform(lambda x: (x - x.mean())/x.std())
        return rawScore[['date', 'ticker', col]]
    elif method == 'industry':
        return neutralize_by_industry(rawScore, col, startDate, endDate)
    elif method == 'mcap':
        return neutralize_by_mcap(rawScore, col, startDate, endDate)
    elif  method == 'industry_and_mcap':
        return neutralize_by_industry_and_mcap(rawScore, col, startDate, endDate)

def neutralize_by_industry(rawScore, col, startDate, endDate):
    """
    neutralize score by industry only
    """    
    ind = loadData(startDate, endDate, 'industry', holdingPeriod = 1 )
    ind = ind[['date', 'ticker', 'sw1']]
    ind.rename(columns = {'sw1':'industry'}, inplace = True)
    
    rawScore = pd.merge(rawScore, ind, on = ['date','ticker'], how = 'left')    
    rawScore[col] = rawScore.groupby(['date','industry'])[col].transform(lambda x: (x - x.mean())/x.std()) 
    
    return rawScore[['date', 'ticker', col]]
       
def neutralize_by_mcap(rawScore, col, startDate, endDate):
    """
    neutralize score by mcap only
    """    
    mcapDf = loadData(startDate, endDate, 'mkt_val', holdingPeriod = 1)
    mcapDf = mcapDf[['date', 'ticker', 'neg_mkt_val']]
    mcapDf['ln_neg_mkt_val'] = np.log(mcapDf['neg_mkt_val'])
    
    rawScore = pd.merge(rawScore, mcapDf, on = ['date','ticker'], how = 'left')    
    adjScore = rawScore.groupby('date').apply(lambda x:sm.OLS( x[col],  sm.add_constant( x['ln_neg_mkt_val'])).fit().resid).to_frame().reset_index()
    
    adjScore.columns = ['date', 'index', col ]
    adjScore['date'] = adjScore['date'].dt.date
    adjScore['date'] = pd.to_datetime(adjScore['date'])
    adjScore['ticker'] = adjScore['index'].map(rawScore['ticker'])
    adjScore.drop(columns = 'index', inplace = True)
    
    adjScore = adjScore[['date', 'ticker', col]]
    return adjScore

def neutralize_by_industry_and_mcap(rawScore, col, startDate, endDate):
    """
    neutralize score by mcap and industry
    """    
    mcapDf = loadData(startDate, endDate, 'mkt_val', holdingPeriod = 1)
    mcapDf = mcapDf[['date', 'ticker', 'neg_mkt_val']]
    mcapDf['ln_neg_mkt_val'] = np.log(mcapDf['neg_mkt_val'])

    ind = loadData(startDate, endDate, 'industry', holdingPeriod = 1 )
    ind = ind[['date', 'ticker', 'sw1']]
    ind.rename(columns = {'sw1':'industry'}, inplace = True)
     
    rawScore = pd.merge(rawScore, mcapDf, on = ['date','ticker'], how = 'left') 
    rawScore = pd.merge(rawScore, ind, on = ['date','ticker'], how = 'left')
    
    adjScore = rawScore.groupby('date').apply(lambda x:sm.OLS( x[col],  sm.add_constant( pd.concat([x['ln_neg_mkt_val'], pd.get_dummies(x['industry'])], axis=1))).fit().resid).to_frame().reset_index()
    
    adjScore.columns = ['date', 'index', col ]
    adjScore['date'] = adjScore['date'].dt.date
    adjScore['date'] = pd.to_datetime(adjScore['date'])
    adjScore['ticker'] = adjScore['index'].map(rawScore['ticker'])
    adjScore.drop(columns = 'index', inplace = True)
    
    adjScore = adjScore[['date', 'ticker', col]]
    return adjScore


def get_all_trading_days(startDate, endDate, holdingPeriod):
    """    
    get all trading dates between start end with holding period

    Returns
    -------
    dates : list
        [datetime.date]

    """
    
    allDatesDf = pd.read_parquet(_processed_data_root_dir + "dates.parquet")
    allDates = list(allDatesDf['dates'])
     
    dates = [i.date() for i in allDates if i.date() >= startDate and i.date() <= endDate]   
    dates = [i for i in dates[::holdingPeriod]]
    return dates

def filter_small_trading_days(data):
    """
    filter out tickers with trading dates <= 100 diring backtest period
    """
    countDays = pd.read_parquet(_processed_data_root_dir + 'count_trading_days.parquet')
    remove_tickers = countDays[countDays['return_count']<=100]['ticker'].to_list()
    data = data[~data['ticker'].isin(remove_tickers)]
    return data
    
    
#df.columns = ['dates']
#df.to_parquet("/Applications/dates.csv")
#df.to_parquet("/Volumes/GoogleDrive/My Drive/QiShi/Data/dates.parquet")
#df = pd.read_csv("/Applications/dates.csv")

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed May 31 20:10:21 2023

@author: bella
"""

from dataLoader.data_loader import loadData
import pandas as pd
import datetime as dt
import numpy as np
import statsmodels.api as sm

# volume: qty
# turnover: value
def getEnrichEODdata(startDate, endDate):
    volumeDf = loadData(startDate, endDate, 'eod_volume')
    mcapDf = loadData(startDate, endDate, 'mkt_val')
    volume_enriched = pd.merge(volumeDf, mcapDf, on = ['date', 'ticker'], how = 'left')
    volume_enriched['ln_neg_mkt_val'] = np.log(volume_enriched['neg_mkt_val'])
       
    eodPrice = loadData(startDate, endDate, 'eod_priceAdj')
    eodPrice['intraDay_return'] = (eodPrice['close'] - eodPrice['open'])/eodPrice['open']
    
    eodPrice['vol_range'] = np.log(eodPrice['high'] / eodPrice['low'])
    
    dataAll = pd.merge(volume_enriched, eodPrice, on = ['date', 'ticker'], how = 'left')
        
    ind = loadData(startDate, endDate, 'industry')
    ind = ind[['date', 'ticker', 'sw1']]
    ind.rename(columns = {'sw1':'industry'}, inplace = True)
    
    dataAll = pd.merge(dataAll, ind, on = ['date','ticker'], how = 'left')

    return dataAll

def enrichLiquidity(data):
    data['turnover_ratio_qty'] = data['accvolume']/data['neg_shares']
    data['turnover_ratio_val'] = data['accturover']/data['neg_mkt_val']
    
    data['Amihud_illiq_otc'] = data['intraDay_return'].abs()/data['accturover']
    data['Amihud_illiq_ctc'] = data['daily_return_adj'].abs()/data['accturover']
    
    data['Amihud_illiq_otc_modify'] = data['intraDay_return'].abs()/data['turnover_ratio_val']
    
    return data

def adjLiquidityFactor(data, col, ind_neutral = False, mcap_neutral = True, vol_neutral = False):
    adj_col = col + '_adj'
    data[adj_col] = data[col].copy()
    data[adj_col] = data[adj_col].clip(upper = data[col].quantile(0.99), lower = data[col].quantile(0.01))
    
    if mcap_neutral and ind_neutral and vol_neutral:
        adjScore = data.groupby('date').apply(lambda x:sm.OLS( x[adj_col],  sm.add_constant( pd.concat([x[['vol_range','ln_neg_mkt_val']], pd.get_dummies(x['industry'])], axis=1))).fit().resid).to_frame().reset_index()
   
    elif mcap_neutral and not ind_neutral and  vol_neutral:
        adjScore = data.groupby('date').apply(lambda x:sm.OLS( x[adj_col],  sm.add_constant( x[['ln_neg_mkt_val', 'vol_range']])).fit().resid).to_frame().reset_index()
             
    elif mcap_neutral and ind_neutral and not vol_neutral:
        adjScore = data.groupby('date').apply(lambda x:sm.OLS( x[adj_col],  sm.add_constant( pd.concat([x['ln_neg_mkt_val'], pd.get_dummies(x['industry'])], axis=1))).fit().resid).to_frame().reset_index()
    
    elif mcap_neutral and not ind_neutral and not vol_neutral: 
        adjScore = data.groupby('date').apply(lambda x:sm.OLS( x[adj_col],  sm.add_constant( x['ln_neg_mkt_val'])).fit().resid).to_frame().reset_index()
    
    adjScore.columns = ['date', 'index', adj_col ]
    adjScore['date'] = adjScore['date'].dt.date
    adjScore['date'] = pd.to_datetime(adjScore['date'])
    adjScore['ticker'] = adjScore['index'].map(data['ticker'])
    adjScore.drop(columns = 'index', inplace = True)
    
    adjScore = adjScore[['date', 'ticker', adj_col]]
    
    return adjScore
    
# adjScore['score'] = adjScore['score']*(-1)
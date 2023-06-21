#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Apr 14 01:53:22 2023

calculate stock return and index return with start, end , holding period

@author: bella
"""


from dataLoader.data_loader import loadData
import datetime as dt
import pandas as pd
from constants.path import _stk_daily_return_dir, _idx_daily_return_dir
import logging
import os


class returnCalculator:
    def __init__(self, startDate, endDate):
        self.startDate = startDate
        self.endDate = endDate
    
    def calcStkRet(self, holdingPeriod):
        rawData = loadData(self.startDate, self.endDate, 'eod_priceAdj', holdingPeriod)
        rawData = rawData.sort_values(by = 'date', ascending = True)
        rawData['prevClose'] = rawData.groupby('ticker')['close'].shift(1)
        rawData['return'] = (rawData['close'] - rawData['prevClose']) / rawData['prevClose']
        return rawData 

    def rollingStockReturn(self, window):
        rawData = loadData(self.startDate, self.endDate, 'eod_priceAdj', holdingPeriod =1)
        rawData = rawData.sort_values(by = 'date', ascending = True)
        rawData['prevClose'] = rawData.groupby('ticker')['close'].shift(window)
        rawData = rawData[~rawData['prevClose'].isna()]
        returnCol = 'return_rolling_{}d'.format(window)
        rawData[returnCol] = (rawData['close'] - rawData['prevClose']) / rawData['prevClose']
        return rawData

    def calcStkDailyRet(self):
        dataPath = _stk_daily_return_dir + 'daily_return_adj.parquet'
        if not os.path.exists(dataPath):
            df = self.calcStkRet(1)
            df.to_parquet(dataPath, engine='fastparquet')
            logging.debug("calc and save stock daily returns from {} to {}".format(self.startDate, self.endDate ))
            return df
        df = pd.read_parquet(dataPath)
        df = df[(df['date'].dt.date >= self.startDate) & (df['date'].dt.date <= self.endDate)]
        return df

   
    def calcIndexRet(self, holdingPeriod):
        rawIndexData = loadData(self.startDate, self.endDate, 'idx_trading', holdingPeriod)
        #calc return with close data
        rawIndexData = rawIndexData[['ticker_idx', 'close_idx', 'date']]
        rawIndexData = rawIndexData.sort_values(by = 'date', ascending = True)
        rawIndexData['prevClose_idx'] = rawIndexData.groupby('ticker_idx')['close_idx'].shift(1)
        rawIndexData['return_idx'] = (rawIndexData['close_idx'] - rawIndexData['prevClose_idx']) / rawIndexData['prevClose_idx']
        rawIndexData= rawIndexData[['date', 'ticker_idx', 'return_idx']]
        return rawIndexData
    
    def rollingIndexReturn(self, window):
        rawData = loadData(self.startDate, self.endDate, 'idx_trading', holdingPeriod =1)
        rawData = rawData.sort_values(by = 'date', ascending = True)
        rawData['prevClose_idx'] = rawData.groupby('ticker_idx')['close_idx'].shift(window)
        rawData = rawData[~rawData['prevClose_idx'].isna()]
        returnCol = 'return_idx_rolling_{}d'.format(window)
        rawData[returnCol] = (rawData['close_idx'] - rawData['prevClose_idx']) / rawData['prevClose_idx']
        return rawData


    def calcIndexDailyRet(self):
        dataPath = _idx_daily_return_dir + 'daily_return.parquet'
        if not os.path.exists(dataPath):
            df = self.calcIndexRet( 1)
            df.to_parquet(dataPath, engine='fastparquet')
            logging.debug("calc and save index daily returns from {} to {}".format(self.startDate, self.endDate ))
            return df
        df = pd.read_parquet(dataPath)
        return df
        
    @staticmethod
    def calcForwardReturn(returnData, tickerCol, returnCol):
        """    
    
        Parameters
        ----------
        returnData : stock return/ index return
      
        tickerCol : ticker/ ticker_idx
            
        returnCol : return/ return_idx
    
        Returns
        -------
        returnData : forward 1,3,5,10, 20d return
    
        """
        returnData = returnData.sort_values(by = 'date', ascending = True)
        
        returnData['forward_return_1'] =  returnData.groupby(tickerCol)[returnCol].shift(-1)
        returnData['forward_return_3'] =  returnData.groupby(tickerCol)[returnCol].shift(-3)
        returnData['forward_return_5'] =  returnData.groupby(tickerCol)[returnCol].shift(-5)
        returnData['forward_return_10'] =  returnData.groupby(tickerCol)[returnCol].shift(-10)
        returnData['forward_return_20'] =  returnData.groupby(tickerCol)[returnCol].shift(-20)
        return returnData
    
    @staticmethod
    def beta(data, period):
         returns = data[['ticker', 'date', 'return', 'bmk_return']].dropna()
         returns = returns.sort_values(by = 'date', ascending  = True)
         cov = returns.iloc[0:,2].rolling(period).cov(returns.iloc[0:,3])
         market_var = returns.iloc[0:,3].rolling(period).var()
        
         individual_beta = cov / market_var
         betaCol = 'beta_rolling_{}d'.format(period)
    
         individual_beta_df = pd.DataFrame({betaCol:list(individual_beta)}).reset_index(drop = True)
         individual_beta_df['date'] = returns['date'].to_list()
         return individual_beta_df

    def calcRollingBeta(self,  window, bmk= '000905'):
        #rc =returnCalculator(startDate, endDate)
        data = self.calcStkDailyRet() 
        indexRet = self.calcIndexDailyRet()
        indexRet = indexRet[indexRet['ticker_idx'] == bmk]
       
        bmk_ret = indexRet.set_index('date')['return_idx']
        data['bmk_return'] = data['date'].map(bmk_ret)
        data = data[~data['return'].isna()]
        
        beta = data.groupby('ticker').apply(self.beta, period = window).reset_index().drop(columns = 'level_1', axis = 1)
        betaCol = 'beta_rolling_{}d'.format(window)
        beta = beta[['date', 'ticker', betaCol]]
           
        return beta
    

def calcAdjReturn(startDate, endDate, bmk, window):
    """
    calculate benchmark adjusted return with window

    """
    rc =returnCalculator(startDate, endDate)
    data = rc.calcStkDailyRet() # TODO: cache
    indexRet = rc.calcIndexDailyRet()
    indexRet = indexRet[indexRet['ticker_idx'] == '000905']
   
    bmk_ret = indexRet.set_index('date')['return_idx']
    data['bmk_return'] = data['date'].map(bmk_ret)
    
    beta = data.groupby('ticker').apply(rc.beta, period = window).reset_index().drop(columns = 'level_1', axis = 1)
       
    data = data[['ticker', 'date', 'return', 'bmk_return']]
    data = data.merge(beta, on = ['ticker', 'date'], how = 'left')
    
    data = data[~data['return'].isna()]
    betaCol = 'beta_rolling_{}d'.format(window)
    data['adj_return'] = data['return'] - data[betaCol]* data['bmk_return']
    
    data = data[~data['adj_return'].isna()]
    return data
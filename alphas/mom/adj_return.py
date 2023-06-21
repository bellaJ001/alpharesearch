#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Apr 28 14:33:59 2023

@author: bella
"""
import datetime as dt
import numpy as np
import pandas as pd
from dataProcessor.return_calculator import returnCalculator


def calcAdjReturn(startDate, endDate, window):
    rc = returnCalculator(startDate, endDate)
    data = rc.rollingStockReturn(window)
    indexRet = rc.rollingIndexReturn(window)
    indexRet = indexRet[indexRet['ticker_idx'] == '000905' ]
    
    betaCol = 'beta_rolling_{}d'.format(window)
    retCol = 'return_rolling_{}d'.format(window)
    indexRetCol = 'return_idx_rolling_{}d'.format(window)
    adjRetCol = 'adj_return_rolling_{}d'.format(window)
    
    bmk_ret = indexRet.set_index('date')[indexRetCol]
    
    data[indexRetCol] = data['date'].map(bmk_ret)
    
    beta = rc.calcRollingBeta(window, bmk = '000905')
    
    allData = data.merge(beta, on = ['date','ticker'], how = 'left')
    
    allData[adjRetCol] = allData[retCol] - allData[betaCol]* allData[indexRetCol]
    rawScore = allData[[ 'date', 'ticker', adjRetCol]]
    return rawScore
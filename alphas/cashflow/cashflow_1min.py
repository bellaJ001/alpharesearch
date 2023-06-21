# -*- coding: utf-8 -*-
"""
Created on Fri Jun 16 02:25:26 2023

process 1min data to get cashflow info and processed into daily data

@author: bella
"""

import pandas as pd
import numpy as np
import datetime
import os
os.chdir('C:\\Users\\bella\\OneDrive\\Desktop\\projects\\QiShi\\code')
import functools as ft
from dataLoader.data_loader import loadData

def get_all_1min_data(asofDate):
    priceDf = loadData(asofDate, asofDate, '1min_price')
    volumeDf = loadData(asofDate, asofDate, '1min_volume')
    turnoverDf = loadData(asofDate, asofDate, '1min_turnover')
    matchItem = loadData(asofDate, asofDate, '1min_matchItem')
    dfs = [priceDf, volumeDf, turnoverDf, matchItem]
    data = ft.reduce(lambda left, right: pd.merge(left, right, on=['date', 'ticker', 'time']), dfs)
    data = data.sort_values(by = ['date', 'ticker', 'time'])
    data['last_close'] = data.groupby( ['date', 'ticker'])['close'].shift(1)
    data['1min_return'] = (data['close'] - data['last_close'])/data['last_close']
    data['1min_trend'] = np.nan
    data.loc[data['1min_return']>0, '1min_trend'] = 1
    data.loc[data['1min_return']==0, '1min_trend'] = 0
    data.loc[data['1min_return']<0, '1min_trend'] = -1
    
    return data

def get_effTime(df, active_time = False):
    effTime_open = (df['time']<=959)
    if active_time:
        effTime_morning = (df['time']<=1029) | ((df['time']>=1100) & (df['time']<=1115))
        effTime_afternoon =  ((df['time']>=1300) & (df['time']<=1330)) | (df['time']>=1400)
    else:
        effTime_morning = df['time'] < 1130
        effTime_afternoon =  df['time']>=1300
    
    effTime_close =  df['time']>=1400
    return effTime_open, effTime_close, effTime_morning, effTime_afternoon

def process_cash_flow(data, turover_threshold =  40000, active_time = False):
    data['is_effTime'] = 0
    data['is_open30'] = 0
    data['is_close30'] = 0
    data['is_morning'] = 0
    data['is_afternoon'] = 0
    
    effTime_open, effTime_close, effTime_morning, effTime_afternoon = get_effTime(data, active_time)
    
    data.loc[(effTime_morning|effTime_afternoon) , 'is_effTime'] =1
    data.loc[(effTime_open) , 'is_open30'] =1
    data.loc[(effTime_close) , 'is_close30'] =1
    data.loc[(effTime_morning) , 'is_morning'] =1
    data.loc[(effTime_afternoon) , 'is_afternoon'] =1
    
    data['is_large'] = np.nan
    data.loc[(effTime_morning|effTime_afternoon) & (data['turover']>=turover_threshold), 'is_large'] =1
    data.loc[(effTime_morning|effTime_afternoon) & (data['turover']<turover_threshold), 'is_large'] =0
    # typical price
    data['tp'] = (data['high'] +  data['low'] + data['close'])/3
    data['money_flow'] = data['tp']* data['volume']
    
    data['money_flow_buy'] = 0
    data['money_flow_sell'] = 0
    data.loc[data['1min_trend'].isin([0,1]), 'money_flow_buy'] = data['money_flow']
    data.loc[data['1min_trend'].isin([0,-1]), 'money_flow_sell'] = data['money_flow']
    
    large_money_flow_buy = data[(data['is_large']==1)].groupby(['date','ticker'])['money_flow_buy'].sum().reset_index().rename(columns = {'money_flow_buy':'large_money_flow_day_buy'})
    large_money_flow_sell = data[(data['is_large']==1) ].groupby(['date','ticker'])['money_flow_sell'].sum().reset_index().rename(columns = {'money_flow_sell':'large_money_flow_day_sell'})
    small_money_flow_buy = data[(data['is_large']==0)].groupby(['date','ticker'])['money_flow_buy'].sum().reset_index().rename(columns = {'money_flow_buy':'small_money_flow_day_buy'})
    small_money_flow_sell = data[(data['is_large']==0)].groupby(['date','ticker'])['money_flow_sell'].sum().reset_index().rename(columns = {'money_flow_sell':'small_money_flow_day_sell'})

    dfs = [large_money_flow_buy, large_money_flow_sell, small_money_flow_buy, small_money_flow_sell]

    for _type in [ 'open', 'morning', 'close', 'afternoon']:
        if _type == 'open':
            condition = (data['is_open30']==1)
        elif _type == 'morning':
            condition = (data['is_morning']==1)
        elif _type == 'close':
            condition = (data['is_close30']==1)
        elif _type == 'afternoon':
            condition = (data['is_afternoon']==1)
        large_money_flow_buy = data[(data['is_large']==1)& condition].groupby(['date','ticker'])['money_flow_buy'].sum().reset_index().rename(columns = {'money_flow_buy':'large_money_flow_'+ _type+'_buy'})
        large_money_flow_sell = data[(data['is_large']==1) & condition].groupby(['date','ticker'])['money_flow_sell'].sum().reset_index().rename(columns = {'money_flow_sell':'large_money_flow_'+ _type+'_sell'})
        small_money_flow_buy = data[(data['is_large']==0)& condition].groupby(['date','ticker'])['money_flow_buy'].sum().reset_index().rename(columns = {'money_flow_buy':'small_money_flow_'+ _type+'_buy'})
        small_money_flow_sell = data[(data['is_large']==0) & condition].groupby(['date','ticker'])['money_flow_sell'].sum().reset_index().rename(columns = {'money_flow_sell':'small_money_flow_'+ _type+'_sell'})
        dfs+= [large_money_flow_buy, large_money_flow_sell, small_money_flow_buy, small_money_flow_sell]
    
    money_flow = ft.reduce(lambda left, right: pd.merge(left, right, on=['ticker', 'date'], how ="outer"), dfs)
    
    close_959 = data[data['time'] == 959][['date', 'ticker', 'close']].rename(columns = {'close':'959_close'})
    close_1129 = data[data['time'] == 1129][['date','ticker', 'close']].rename(columns = {'close':'1129_close'})
    open_1300 = data[data['time'] == 1300][['date','ticker', 'open']].rename(columns = {'open':'1300_open'})
    
    open_close = ft.reduce(lambda left, right: pd.merge(left, right, on=['ticker', 'date'], how ="outer"), [close_959, close_1129, open_1300 ])
    
    money_flow= money_flow.merge(open_close, on=['ticker', 'date'], how ="outer")
    return money_flow



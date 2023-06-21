# -*- coding: utf-8 -*-
"""
Created on Fri Jun  9 18:50:56 2023

@author: bella
"""

import pandas as pd
import numpy as np
import datetime
import os
os.chdir('C:\\Users\\bella\\OneDrive\\Desktop\\projects\\QiShi\\code')
import logging
from path.path import _eod_pv_data_dir
import functools as ft
from dataProcess.dataLoader import loadData
from backtester.utils import winsorize, getAggStats, get_all_trading_days, get_multi_process_by_month, get_multi_process_by_split
from functools import partial
from itertools import repeat
import multiprocessing
from multiprocessing import Pool
import time
import functools as ft
    

_eod_adj_price_enriched_liq_dir = _eod_pv_data_dir + "price_adj_enriched_liq\\"

_intervals = {'morning_open_5':[930,934], 
             'morning_open_10':[930,939],
             'morning_open_30':[930,959], 
             'morning_close_5':[1125,1129], 
             'morning_close_10':[1120,1129], 
             'afternoon_open_5':[1300, 1304], 
             'afternoon_open_10':[1300, 1309],
             'afternoon_open_30':[1300, 1329],
             'afternoon_close_5':[1455, 1459],
             'afternoon_close_10':[1450, 1459]}


def enrich1minDatawithLiq(allDf):
    allDf = allDf.replace(0, np.nan)   # change original data
    
    allDf['priceShortCut']=  2*(allDf['high']-allDf['low'])-np.abs(allDf['close'] - allDf['open'])
    allDf['trend'] = np.where(allDf['close']- allDf['open']>0, 1,0)
    allDf['trend'] = np.where(allDf['close'] == allDf['open'], 2, allDf['trend']) # open = close: 2, close>open:1 3. close<open: 0 
    
    allDf['sell_shortCut'] = np.where(allDf['trend'] == 0, allDf['priceShortCut'], np.nan)
    allDf['buy_shortCut'] = np.where(allDf['trend'] == 1, allDf['priceShortCut'], np.nan)
    
    allDf['sell_turnover'] = np.where(allDf['trend'] == 0, allDf['turover'], np.nan)
    allDf['buy_turnover'] = np.where(allDf['trend'] == 1, allDf['turover'], np.nan) #上涨成交量
    
    allDf['illiq_high_low'] =  np.where(allDf['turover']!=0, (allDf['high']-allDf['low'])/allDf['turover'], np.nan) # high_low: not return
    allDf['1min_return'] =  (allDf['close']-allDf['open'])/allDf['open']
    allDf['illiq_open_close'] =  np.where(allDf['turover']!=0, np.abs(allDf['1min_return'])/allDf['turover'], np.nan) # open_close: return
    
    allDf['illiq_high_low_buy'] = np.where(allDf['trend'] == 1, allDf['illiq_high_low'], np.nan) 
    allDf['illiq_high_low_sell'] = np.where(allDf['trend'] == 0, allDf['illiq_high_low'], np.nan) 
    
    allDf['illiq_open_close_buy'] = np.where(allDf['trend'] == 1, allDf['illiq_open_close'], np.nan) 
    allDf['illiq_open_close_sell'] = np.where(allDf['trend'] ==0, allDf['illiq_open_close'], np.nan) 
    
    allDf['illiq_path'] = np.where(allDf['turover']!=0, allDf['priceShortCut']/allDf['turover'], np.nan) # 1min illiq ratio
    allDf['illiq_path_buy'] = np.where(allDf['trend'] == 1, allDf['illiq_path'], np.nan) # 1min illiq factor
    allDf['illiq_path_sell'] = np.where(allDf['trend'] == 0, allDf['illiq_path'], np.nan)
    
    return allDf


def enrichEODwithLiq(eodDf, oneminDf):
    from backtester.utils import winsorize, getAggStats
    
    # winsorize and take mean, std of ratio columns
    
    cols1 =  ['illiq_path_sell', 'illiq_path_buy', 'illiq_high_low_buy', 'illiq_high_low_sell', 'illiq_open_close_buy', 'illiq_open_close_sell' ]
    cols2 = ['illiq_high_low', 'illiq_path', 'illiq_open_close' ]
    cols = cols1+cols2
    
    for col in cols1:
        winsorize(oneminDf, groupbyCol = ['date'], col = col, percentile =1)
        
    for col in cols2 :
        winsorize(oneminDf, groupbyCol = ['date'], col = col, percentile =2)
    
    day_mean = getAggStats(oneminDf, groupbyCol= ['date', 'ticker'], aggColList = cols, stats = 'mean', interval_name = '_daily' )
    day_std = getAggStats(oneminDf, groupbyCol= ['date', 'ticker'], aggColList = cols, stats = 'std', interval_name = '_daily' )
    # columns: date	ticker volume_mean	turover_mean	
    day_zcore = pd.merge(day_mean, day_std, on = ['date', 'ticker'], how = "outer")
    
    for col in cols:
        mean_col = col + '_mean' + '_daily'
        std_col = col + '_std' + '_daily'
        z_col = col + '_zscore' + '_daily'
        day_zcore[z_col] = day_zcore[mean_col] / day_zcore[std_col]
         
    # sum shortcut and turnover
    
    cols_to_sum = ['priceShortCut', 'buy_shortCut', 'sell_shortCut', 'buy_turnover', 'sell_turnover' ]
    day_sum = getAggStats(oneminDf, groupbyCol= ['date', 'ticker'], aggColList = cols_to_sum, stats = 'sum', interval_name = '_daily' )
    
    cols_to_count = ['turover', 'buy_turnover', 'sell_turnover']   # count non nan values and exclue later
    volumn_count = getAggStats(oneminDf, groupbyCol= ['date', 'ticker'], aggColList = cols_to_count, stats = 'count', interval_name = '_daily' )
    
    dfs = [eodDf, day_zcore, day_sum, volumn_count ]
    
    enrichedEod = ft.reduce(lambda left, right: pd.merge(left, right, on=['ticker', 'date'], how = 'outer'), dfs)
    
    return enrichedEod
   

def calcIntervalLiqChangeStats(df, interval, diff_suffix):
    """
    calc interval turnover, liquidity change and summarize stats into mean/sum --> drop 'time' and summarize into eod data
    
    df: 1min data
    time1, time2: int, e.g. 1120,1129
    """
    
    time1, time2 = interval[0], interval[1]
    
    df_two_point = df[df['time'].isin([time1,time2])] [['date','time','ticker','accvolume', 'accturover', 'close', 'open', 'high', 'low']]
    df_two_point.sort_values(by = ['date', 'ticker', 'time'])
    df_two_point.columns = [str(time2)+ '_'+ i if i not in ['date', 'time', 'ticker'] else i for i in df_two_point.columns]
    
    diff_suffix = '_' + diff_suffix
    
    for col in ['open', 'close', 'high', 'low', 'accvolume', 'accturover']:
        df_two_point[str(time1)+'_' + col] = df_two_point.groupby(['date', 'ticker'])[str(time2)+ '_'+ col].shift(1)    
    
    df_two_point['turnover_diff'+ diff_suffix] = df_two_point[str(time2) +'_accturover']- df_two_point[str(time1)+'_accturover']# accturover: value
    df_two_point['volume_diff'+ diff_suffix] =  df_two_point[str(time2) +'_accvolume']- df_two_point[str(time1)+'_accvolume']
    
    df_two_point['return'+ diff_suffix] = (df_two_point[str(time2) +'_close'] -  df_two_point[str(time1)+'_open'])/df_two_point[str(time1)+'_open']
    
    df_two_point = df_two_point[df_two_point['time'] == time2].reset_index(drop = True)
    
    df_interval = df[(df['time']>= time1) & (df['time']<= time2)]#[['date','time','ticker','accvolume', 'accturover', 'close', 'open', 'high', 'low']] 
    
    cols1 =  ['illiq_path_sell', 'illiq_path_buy', 'illiq_high_low_buy', 'illiq_high_low_sell', 'illiq_open_close_buy', 'illiq_open_close_sell' ]
    cols2 = ['illiq_high_low', 'illiq_path', 'illiq_open_close' ]
    cols = cols1+cols2
    
    for col in cols1:
        winsorize(df_interval, groupbyCol = ['date'], col = col, percentile =1)
        
    for col in cols2 :
        winsorize(df_interval, groupbyCol = ['date'], col = col, percentile =2)
        
    # only 30 mins, no need to calc std, z score
    interval_mean = getAggStats(df_interval, groupbyCol= ['date', 'ticker'], aggColList = cols, stats = 'mean', interval_name = diff_suffix)         
    
    # sum shortcut and turnover   
    cols_to_sum = ['priceShortCut', 'buy_shortCut', 'sell_shortCut', 'buy_turnover', 'sell_turnover' ]
    interval_sum = getAggStats(df_interval, groupbyCol= ['date', 'ticker'], aggColList = cols_to_sum, stats = 'sum', interval_name = diff_suffix )
 
    cols_to_count = ['turover', 'buy_turnover', 'sell_turnover']   # count non nan values and exclue later
    interval_volumn_count = getAggStats(df_interval, groupbyCol= ['date', 'ticker'], aggColList = cols_to_count, stats = 'count', interval_name = diff_suffix )
    
    dfs = [df_two_point.drop(columns=['time']), interval_mean, interval_sum, interval_volumn_count ]

    final = ft.reduce(lambda left, right: pd.merge(left, right, on=[ 'date', 'ticker'], how = "outer"), dfs)
         
    return final


# def enrichEodwithLiqIntervalStats(allDf, eodDf, intervals):
#     """
#     allDf: 1min data
#     eodDf: eodData
#     """
#     tic = time.time()
#     for interval_name, interval in intervals.items():
#         enrichedData = calcIntervalLiqChangeStats(allDf, interval, interval_name)
#         eodDf = pd.merge(eodDf, enrichedData, on = ['date', 'ticker'], how = 'left', suffixes=('', '_DROP')).filter(regex='^(?!.*_DROP)')
        
#         if (interval[0] == 930) and ('return_'+ interval_name) in eodDf.columns:
#               eodDf['return_'+ interval_name] = (eodDf[str(interval[1]) +'_close'] - eodDf['open']) / eodDf['open']
    
#     toc = time.time()
#     print('interval stats calculation done in %.2f seconds'%(toc - tic))
#     return eodDf

#=============================================================================

def enrichEodwithLiqIntervalStats(allDf, eodDf, intervals): # multiprocess version
    """
    allDf: 1min data
    eodDf: eodData
    """
  
    interval_names= []
    interval_startEnd = []
    
    for name, interval in intervals.items():
        interval_names.append(name)
        interval_startEnd.append(interval)
    
    tic = time.time()
    
    pool = multiprocessing.Pool( processes = len(intervals) )  
    enrichedData = pool.starmap(calcIntervalLiqChangeStats, zip( repeat(allDf), interval_startEnd, interval_names ))
    #enrichedData: tuple with different interval liq stats, need to left merge on eod data    
    pool.close()
    
    toc = time.time()
    logging.debug('done in %s seconds', toc - tic)
    
    dfs = [eodDf] + [res for res in enrichedData]
    eodEnriched = ft.reduce(lambda left, right: pd.merge(left, right, on=[ 'date', 'ticker'], suffixes=('', '_DROP'), how = "outer"), dfs)
    eodEnriched = eodEnriched.filter(regex='^(?!.*_DROP)')
        
    #if interval[0] == 930:
    for interval_name, end_time in zip(['morning_open_5', 'morning_open_10', 'morning_open_30' ], [934,939,959]):
        if ('return_'+ interval_name) in eodEnriched.columns:
            eodEnriched['return_'+ interval_name] = (eodEnriched[str(end_time) +'_close'] - eodEnriched['open']) / eodEnriched['open']
    return eodEnriched

#=============================================================================
def enrichAndProcessEodwithIntervalStats_liq(startDate, endDate, intervals= _intervals):
    """
    enrich and save eod with eod, interval liquidity change stats data
    
    use as one of the process of multiprocessing
    """
    
    errors = []
    
    try:
        priceDf = loadData(startDate, endDate, '1min_price')
        volumeDf = loadData(startDate,endDate, '1min_volume')
        turnoverDf = loadData(startDate,endDate, '1min_turnover')
        dfs = [priceDf, volumeDf, turnoverDf]
        eodPriceDf = loadData(startDate,endDate, 'eod_priceAdj')
        
        allDf = ft.reduce(lambda left, right: pd.merge(left, right, on=['date','ticker', 'time'], how = "outer"), dfs)
        allDf_raw = allDf.copy()
        allDf.date = pd.to_datetime(allDf.date)
        allDf = allDf.sort_values(by = ['date', 'ticker', 'time'])
        allDf = enrich1minDatawithLiq(allDf)
        
    except:
        # in case meet vacation all data empty
        errors.append((startDate, endDate))
        return errors
    
    #enrich with eod, interval liq changes data
    eodDf = enrichEODwithLiq(eodPriceDf, allDf)    
    eodDf= enrichEodwithLiqIntervalStats(allDf, eodDf, intervals)

    dates = eodDf["date"].drop_duplicates()
    
    for asofDate in dates:    
        try:
            daily_df =  eodDf[eodDf["date"] == asofDate]
            
            dir_to_save = _eod_adj_price_enriched_liq_dir + str(asofDate.year) 
            
            if not os.path.exists(dir_to_save):
                os.mkdir(dir_to_save)     
            path_to_save = dir_to_save + '\\' + asofDate.strftime('%Y%m%d')
            daily_df.to_parquet(path_to_save +  '.parquet', engine='fastparquet')
            #logging.debug('save enriched eod pv data: %s', asofDate.strftime('%Y%m%d'))
            print('save enriched eod pv data: {}'.format( asofDate.strftime('%Y%m%d')))
        except:
            errors.append(asofDate.strftime('%Y%m%d'))
            #logging.error('exception: %s', asofDate.strftime('%Y%m%d'))
            print('exception: {}'.format( asofDate.strftime('%Y%m%d')))
    return errors



def enrichEodwith1minbyMonth(startDate, endDate, time_ts = None, intervals= _intervals):
    # year:2018, 2019, 2020

    dates = get_all_trading_days(startDate, endDate, holdingPeriod = 1) 
    if not dates: # empty if no trading dates
        err = [(startDate, endDate)]
        return err
    
    multi_times = dates.copy()
    #startDates, endDates = dates.copy(), dates.copy()

    tic = time.time()

    errs = []
    for asofDate in multi_times:
        err = enrichAndProcessEodwithIntervalStats_liq(asofDate,  asofDate, intervals )
        errs.append(err)
    #pool.map(partial(enrichAndProcessEodwithIntervalStats_liq, intervals = intervals), multi_times)

    toc = time.time()
    
    if not time_ts:
        time_ts.append(toc - tic)
   
    print('finish processing in %.2f seconds'%(toc - tic))
    return err


def enrichEodwith1minbyMonth_multi_process(startDate, endDate, time_ts = None, intervals= _intervals):
    # year:2018, 2019, 2020

    dates = get_all_trading_days(startDate, endDate, holdingPeriod = 1) 
    if not dates: # empty if no trading dates
        err = [(startDate, endDate)]
        return err
    
    startDates, endDates = dates.copy(), dates.copy()
    multi_times = dates.copy()

# =============================================================================
#     multi_times = get_multi_process_by_split(startDate, endDate, splits = 8)
#     startDates = [i[0] for i in multi_times]
#     endDates = [i[1] for i in multi_times]
#     
# =============================================================================
    tic = time.time()

    pool = Pool(processes = len(multi_times))
    err = pool.starmap(enrichAndProcessEodwithIntervalStats_liq, zip( startDates, endDates, repeat(intervals) ))

# =============================================================================
#     errs = []
#     for startDate, endDate in multi_times:
#         err = enrichAndProcessEodwithIntervalStats_liq(startDate,  endDate, intervals )
#         errs.append(err)
#    #pool.map(partial(enrichAndProcessEodwithIntervalStats_liq, intervals = intervals), multi_times)
# =============================================================================
    pool.close()
    toc = time.time()
    
    if not time_ts:
        time_ts.append(toc - tic)
   
    print('finish processing in %.2f seconds'%(toc - tic))
    return err

def enrichEodwith1min(year, intervals= _intervals):
    # year:2018, 2019, 2020

    multi_times = get_multi_process_by_month(year) 
    startDates = [i[0] for i in multi_times]
    endDates = [i[1] for i in multi_times]
    
    time_ts = []
    errs = []
    tic = time.time()
    
    for month_start_end in multi_times:  
        print("start one month!")
        startDate = month_start_end[0]
        endDate = month_start_end[1]
        err = enrichEodwith1minbyMonth(startDate, endDate, time_ts = time_ts, intervals= _intervals)
        errs.append(err)
        print("finish one month!")

    toc = time.time()
# =============================================================================
#     pool = Pool(processes = len(multi_times))
#     err = pool.starmap(enrichAndProcessEodwithIntervalStats_liq, zip( startDates, endDates, repeat(intervals) ))
#     #pool.map(partial(enrichAndProcessEodwithIntervalStats_liq, intervals = intervals), multi_times)
#     pool.close()
#     toc = time.time()
# =============================================================================
   
    logging.debug('finish processing for year %s in %s seconds', year,  toc - tic)
    return errs, time_ts
    
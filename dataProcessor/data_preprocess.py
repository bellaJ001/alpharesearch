#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Apr 18 11:34:49 2023

process raw 1min csv PV data into 1min and eod parquet data with format:   dir\\year\\datetime.parquet 
process raw  csv reference data into parquet data with format:   dir\\year\\datetime.parquet 

@author: bella
"""
import pandas as pd
import numpy as np
from contants.path import _raw_1min_dir, _raw_adj_fct_dir, _stk_daily_return_dir
from path.path import _1min_pv_data_dir, _eod_pv_data_dir, _idx_univ_dir, _idx_price_dir, _eod_adj_factor_dir
import os
import datetime
from zipfile import ZipFile
import logging
import fastparquet
from dataProcessor.utils import readDataFromDir

# =============================================================================
# _1min_close_dir = "/Volumes/GoogleDrive/My Drive/QiShi/Data/1min_pv/close"
# _1min_price_dir = "/Volumes/GoogleDrive/My Drive/QiShi/Data/1min_pv/price"
# _1min_turnover_dir = "/Volumes/GoogleDrive/My Drive/QiShi/Data/1min_pv/turnover"
# _1min_volume_dir = "/Volumes/GoogleDrive/My Drive/QiShi/Data/1min_pv/volume"
# 
# =============================================================================

_1min_close_dir = _1min_pv_data_dir + "close\\"
_1min_price_dir = _1min_pv_data_dir + "price\\"
_1min_turnover_dir = _1min_pv_data_dir + "turnover\\"
_1min_volume_dir = _1min_pv_data_dir + "volume\\"
_1min_matchItem_dir = _1min_pv_data_dir + "match_item\\"


_eod_price_dir = _eod_pv_data_dir + "price\\"
_eod_volume_dir = _eod_pv_data_dir + "volume\\"
_eod_dir = _eod_pv_data_dir + "all\\"
_eod_price_enriched_dir = _eod_pv_data_dir + "price_enriched\\"
_eod_adj_price_enriched_dir = _eod_pv_data_dir + "priceAdj\\"
_eod_matchItem_dir = _eod_pv_data_dir + "match_item\\"

################################################################
## process index and index universe data from csv to parquet
################################################################

from dataProcess.data_processor import processIndexUniv

def processIdxUnivdata(startDate, endDate, raw_idx_univ_dir = _idx_univ_dir):
    for asofDate in pd.bdate_range(startDate, endDate):
        raw_idx_univ_path = raw_idx_univ_dir  + str(asofDate.year) + '\\' + asofDate.strftime('%Y%m%d') 
        try:          
            df = processIndexUniv(asofDate )
            if not df.empty:
                df.to_parquet(raw_idx_univ_path + '.parquet', engine='fastparquet')
        except:
            logging.warning("Exception on %s", asofDate.strftime('%Y%m%d'))
    return

from dataLoader.data_loader import loadDataDaily,loadData

def processIdxdata(startDate, endDate, raw_idx_dir = _idx_price_dir):
    for asofDate in pd.bdate_range(startDate, endDate):     
        raw_idx_price_path = raw_idx_dir + str(asofDate.year) + '\\' + asofDate.strftime('%Y%m%d') 
        try:          
            df = loadDataDaily(asofDate, 'idx_trading')
            if not df.empty:
                df.to_parquet(raw_idx_price_path + '.parquet', engine='fastparquet')
        except:
            logging.warning("Exception on %s", asofDate.strftime('%Y%m%d'))
    return

################################################################
## enrich 1 min price to get eod data and adjust eod price with adjust factor
################################################################
  
def process1minCloseData(startDate, endDate):
    """ process 1min csv data to eod, get open, close, daily high, low, and save as parquet

    """
    rawData = loadData(startDate, endDate, '1min_price', holdingPeriod = 1)
    rawData = rawData.reset_index(drop = True)
    rawData = rawData.replace(0, np.nan)   
    rawData['date'] = pd.to_datetime(rawData['date']) # 930 can be 0 for some ticker
      
    rawData['high_day'] = rawData.groupby(['ticker','date'])['high'].transform(max)
    rawData['low_day'] = rawData.groupby(['ticker','date'])['low'].transform(min)
    
    # get highest/ lowest price time 
   
    #df = rawData.groupby(['ticker','date']).apply(lambda x: x.loc[x['low'] == x['low_day'],'time']).reset_index() 
    #rawData.drop_duplicates(subset=['ticker','date'], keep="first")

    rawData = rawData.sort_values(['ticker', 'date', 'time'])
    rawData['open_day'] = rawData.groupby(['ticker','date'])['open'].transform('first')
    rawData['close_day'] = rawData.groupby(['ticker','date'])['close'].transform('last')
    closeDf_all = rawData.groupby(['ticker','date']).tail(1)
    try:
        closeDf_all.to_parquet(_eod_price_enriched_dir + 'eod_price_enriched' + '_'+ startDate.strftime('%Y%m%d') + '_'+ endDate.strftime('%Y%m%d')+ '.parquet', engine='fastparquet')
    except:
        logging.warning("Exception on %s", startDate.strftime('%Y%m%d'))
    return


def processAdjFactor(year, raw_adj_fct_dir = _raw_adj_fct_dir):
    """
    process adj factor from csv by year

    """
    import glob
    import ntpath

    adj_factor_list = []
    adj_factor_dir = raw_adj_fct_dir + str(year) +'\\'
    for f in glob.glob(adj_factor_dir+'*.csv'):
        asofDate = ntpath.basename(f).split('.')[0]
        df = pd.read_csv(f)
        df['date'] = pd.to_datetime(asofDate)        
        df['ticker'] = df['code'].apply(lambda x: str(x).zfill(6))
        df.drop(columns = ['code'], inplace = True)
        adj_factor_list.append(df)
    
    adj_factor_dfs = pd.concat(adj_factor_list, axis = 0)
    adj_factor_dfs = adj_factor_dfs.sort_values(by = 'date', ascending = True)
    
    directory = _eod_adj_factor_dir + str(year)
    if not os.path.exists(directory):
        os.mkdir(directory)            

    adj_factor_dfs.to_parquet(directory + '\\'+ str(year) + '.parquet', engine='fastparquet')
    return

def adjPricebyTicker(df):
    """
    helper function to calculate adjust price
    """
    df = df.sort_values(by = ['date'], ascending = True)
    df['adj_factor'] = df['cum_adjf']/df['cum_adjf'].iloc[-1]
    for col in ['open', 'close', 'low', 'high', 'high_day','low_day', 'open_day', 'close_day']:
        df[col + '_adj'] = df[col]*df['adj_factor']
    df['prev_close_adj'] = df['close_day_adj'].shift(1)
    df['daily_return_adj'] = (df['close_day_adj'] - df['prev_close_adj']) / df['prev_close_adj']
    return df

    
def processAdjPrice(enriched_eod_price_dir = _eod_price_enriched_dir, adj_fct_dir = _eod_adj_factor_dir, eod_adj_price_enriched_dir = _eod_adj_price_enriched_dir):
    """
    process adj factor and adj open, high, low, close price
    """
    
    price_enriched = readDataFromDir(enriched_eod_price_dir, dataType = 'parquet')
    
    adj_factor_list = []
    
    for year in [2018, 2019, 2020]:
       
        adj_factor_df = readDataFromDir(adj_fct_dir + str(year), dataType = 'parquet')
        adj_factor_list.append(adj_factor_df)
    
    adj_factor_dfs = pd.concat(adj_factor_list, axis = 0)
    
    price_all = pd.merge(price_enriched, adj_factor_dfs, on = ['date', 'ticker'], how = 'left')
    price_all = price_all.sort_values(by = ['date', 'ticker'], ascending = True)
    
    price_all_adj = price_all.groupby('ticker').apply(adjPricebyTicker)
       
    # save adj factor and daily return
    
    #price_all_adj[['date', 'ticker', 'adj_factor']].to_parquet(adj_fct_dir +  '/all.parquet', engine='fastparquet')
    #price_all_adj[['date', 'ticker', 'daily_return_adj']].to_parquet(_stk_daily_return_dir +  '/daily_return_adj.parquet', engine='fastparquet')
    
    errors = []
    
    dates = price_all_adj["date"].drop_duplicates()
    for asofDate in dates:
        try:
            
            daily_df = price_all_adj[price_all_adj["date"] == asofDate]
            
            daily_df = daily_df[['date', 'ticker', 'high_day_adj', 'low_day_adj', 'open_day_adj', 'close_day_adj', 'daily_return_adj', 'adj_factor' ]]
            daily_df.rename(columns = {'high_day_adj': 'high', 'low_day_adj' : 'low', 'close_day_adj': 'close', 'open_day_adj' : 'open'}, inplace = True )
            dir_to_save = eod_adj_price_enriched_dir + str(asofDate.year) 
            if not os.path.exists(dir_to_save):
                os.mkdir(dir_to_save)            
        
            path_to_save = dir_to_save + '\\' + asofDate.strftime('%Y%m%d')
            daily_df.to_parquet(path_to_save +  '.parquet', engine='fastparquet')
        except:
            errors.append(asofDate.strftime('%Y%m%d'))
    return errors

################################################################
## process match_item (trade numbers during each minute) data from raw 1min csv PV data into parquet 
################################################################


def process1minMatchItembyDate(asofDate, raw_1min_dir = _raw_1min_dir):    
    path =_raw_1min_dir + asofDate.strftime('%Y%m%d') + '.zip'
    if not os.path.exists(path):
        logging.warning("no data on %s", asofDate.strftime('%Y%m%d'))
        return 
    _1min_matchItem_path = _1min_matchItem_dir + str(asofDate.year) + '\\' + asofDate.strftime('%Y%m%d')    
    
    if not os.path.exists(_1min_matchItem_dir):
        os.mkdir(_1min_matchItem_dir)        

    if not os.path.exists(_1min_matchItem_dir + str(asofDate.year) + '\\'):
        os.mkdir(_1min_matchItem_dir + str(asofDate.year) + '\\')        
    
    _eod_matchItem_path = _eod_matchItem_dir + str(asofDate.year) + '\\' + asofDate.strftime('%Y%m%d') 

    if not os.path.exists(_eod_matchItem_dir):
        os.mkdir(_eod_matchItem_dir)    

    if not os.path.exists(_eod_matchItem_dir + str(asofDate.year) + '\\'):
        os.mkdir(_eod_matchItem_dir + str(asofDate.year) + '\\')    
   
    try:
        with ZipFile(path, "r") as zip_ref:
           # Get list of files names in zip
            list_of_files = zip_ref.namelist()
            
            matchItem_1min_list = []
            matchItem_eod_list = []
            
            for file in list_of_files:
                df = pd.read_csv(zip_ref.open(file), usecols = ['code', 'time', 'dif_mi', 'match_items']) # fields = ['code', 'close']
                df['ticker'] = df['code'].apply(lambda x: str(x).zfill(6))
                df.drop(columns = ['code'], inplace = True)
                
                matchItem_1min = df.copy()
                matchItem_eod = df.iloc[-1: ][['ticker', 'time', 'dif_mi', 'match_items']]
                
                matchItem_eod_list.append(matchItem_eod)            
                matchItem_1min_list.append(matchItem_1min)
                            
            matchItem_1min_all = pd.concat(matchItem_1min_list, axis = 0)     
            matchItem_eod_all =  pd.concat(matchItem_eod_list, axis = 0) 
       
            
            matchItem_1min_all.to_parquet(_1min_matchItem_path + '.parquet', engine='fastparquet')
            matchItem_eod_all.to_parquet(_eod_matchItem_path + '.parquet', engine='fastparquet')
            
        #logging.info('all data save successfully on %s', asofDate.strftime('%Y%m%d'))
        print('match item data save successfully on {}'.format(asofDate.strftime('%Y%m%d')))       
        return 
    
    except:
        return asofDate.strftime('%Y%m%d')

################################################################
## process 1 min price and volumn raw csv data into eod parquet data
################################################################
    
def process1minPVbyDate(asofDate, raw_1min_dir = _raw_1min_dir):
    
    path =_raw_1min_dir + asofDate.strftime('%Y%m%d') + '.zip'
    if not os.path.exists(path):
        logging.warning("no data on %s", asofDate.strftime('%Y%m%d'))
        return 
    _1min_price_path = _1min_price_dir + str(asofDate.year) + '\\' + asofDate.strftime('%Y%m%d')
    _1min_close_path = _1min_close_dir + str(asofDate.year) + '\\' + asofDate.strftime('%Y%m%d')
    _1min_turnover_path = _1min_turnover_dir + str(asofDate.year) + '\\' + asofDate.strftime('%Y%m%d')    
    _1min_volume_path = _1min_volume_dir + str(asofDate.year) + '\\' + asofDate.strftime('%Y%m%d')
    
    _eod_price_path = _eod_price_dir + str(asofDate.year) + '\\' + asofDate.strftime('%Y%m%d') 
    _eod_volume_path = _eod_volume_dir + str(asofDate.year) + '\\' + asofDate.strftime('%Y%m%d')
    #_eod_path = _eod_dir  + str(asofDate.year) + '/' + asofDate.strftime('%Y%m%d')
    
    with ZipFile(path, "r") as zip_ref:
       # Get list of files names in zip
        list_of_files = zip_ref.namelist()
        
        priceDf_1min_list = []
        closeDf_1min_list = []
        turnoverDf_1min_list = []
        volumeDf_1min_list = []
              
        priceDf_eod_list = []
        volumeDf_eod_list = []
        #allDf_eod_list = []
        
        for file in list_of_files:
            df = pd.read_csv(zip_ref.open(file), usecols = ['code', 'time', 'open', 'close', 'volume', 'turover', 'high', 'low', 'accvolume', 'accturover']) # fields = ['code', 'close']
            df['ticker'] = df['code'].apply(lambda x: str(x).zfill(6))
            df.drop(columns = ['code'], inplace = True)
            
            priceDf_1min = df[['ticker', 'time',  'open', 'close', 'low', 'high']]
            closeDf_1min = df[['ticker', 'time', 'close']]
            volumeDf_1min = df[['ticker', 'time', 'volume', 'accvolume']]
            turnoverDf_1min = df[['ticker', 'time', 'turover', 'accturover']]
            
            #allDf_eod = df.iloc[-1: ]
            priceDf_eod = df.iloc[-1: ][['ticker', 'time', 'open', 'close', 'low', 'high']]
            volumeDf_eod = df.iloc[-1: ][['ticker', 'time', 'volume', 'accvolume', 'turover', 'accturover']]
            
            priceDf_1min_list.append(priceDf_1min)
            closeDf_1min_list.append(closeDf_1min)
            turnoverDf_1min_list.append(turnoverDf_1min)
            volumeDf_1min_list.append(volumeDf_1min)                       
            
            priceDf_eod_list.append(priceDf_eod)
            volumeDf_eod_list.append(volumeDf_eod)
            #allDf_eod_list.append(allDf_eod)
                    
        priceDf_1min_all = pd.concat(priceDf_1min_list, axis = 0)     
        closeDf_1min_all = pd.concat(closeDf_1min_list, axis = 0)   
        volumeDf_1min_all = pd.concat(volumeDf_1min_list, axis = 0)   
        turnoverDf_1min_all = pd.concat(turnoverDf_1min_list, axis = 0)   
        
        priceDf_eod_all =  pd.concat(priceDf_eod_list, axis = 0) 
        volumeDf_eod_all =  pd.concat(volumeDf_eod_list, axis = 0) 
        #allDf_eod_all =  pd.concat(allDf_eod_list, axis = 0) 
      
        
        priceDf_1min_all.to_parquet(_1min_price_path + '.parquet', engine='fastparquet')
        closeDf_1min_all.to_parquet(_1min_close_path + '.parquet', engine='fastparquet')
        volumeDf_1min_all.to_parquet(_1min_volume_path + '.parquet', engine='fastparquet')
        turnoverDf_1min_all.to_parquet(_1min_turnover_path + '.parquet', engine='fastparquet')
               
        
        priceDf_eod_all.to_parquet(_eod_price_path + '.parquet', engine='fastparquet')
        volumeDf_eod_all.to_parquet(_eod_volume_path + '.parquet', engine='fastparquet')
        #allDf_eod_all.to_parquet(_eod_path + '.parquet', engine='fastparquet')
        
    logging.info('all data save successfully on %s', asofDate.strftime('%Y%m%d'))
    
    return

# pd.read_parquet(_1min_close_path + '.parquet')
# eod_price_path = _eod_price_dir + '/' + str(checkDate.year) + '/' + checkDate.strftime('%Y%m%d') 
    
def process1minPV(startDate, endDate, raw_1min_dir = _raw_1min_dir):
    errorList= []
    #dateInterval = str(holdingPeriod) + 'B'
    for asofDate in pd.bdate_range(startDate, endDate):
        try:          
            process1minPVbyDate(asofDate,  _raw_1min_dir)
        except:           
            errorList.append(asofDate.strftime('%Y%m%d'))
            logging.warning("Exception on %s", asofDate.strftime('%Y%m%d'))

    return errorList



################################
## process ref  data from csv into parquet
################################

from constants.path import ref_path_mapping_dict_raw, ref_path_mapping_dict

mcap_dir_raw =ref_path_mapping_dict_raw['mkt_val']
mcap_dir_processed =ref_path_mapping_dict['mkt_val']


ind_dir_raw =_ref_path_mapping_dict_raw['industry']
ind_dir_processed =_ref_path_mapping_dict['industry']

def processRefDatafromCSV(year, ref_data_dir = mcap_dir_raw, data_save_dir = mcap_dir_processed ):
    """
     /Volumes/GoogleDrive/.shortcut-targets-by-id/1PsYMkyEXLSqagFPvuRxIIqxePA7zUMp-/Data/other/mkt_val/2018/20180109.csv
    """
    import glob
    import ntpath

    ref_data_list = []
    ref_data_dir = ref_data_dir + str(year) +'\\'
    
    for f in glob.glob(ref_data_dir+'*.csv'):
        asofDate = ntpath.basename(f).split('.')[0]
        df = pd.read_csv(f)
        df['date'] = pd.to_datetime(asofDate)        
        df['ticker'] = df['code'].apply(lambda x: str(x).zfill(6))
        df.drop(columns = ['code'], inplace = True)
        ref_data_list.append(df)
    
    ref_data_dfs = pd.concat(ref_data_list, axis = 0)
    ref_data_dfs = ref_data_dfs.sort_values(by = 'date', ascending = True)
    
    directory = data_save_dir  + str(year)
    if not os.path.exists(directory):
        os.mkdir(directory)            

    ref_data_dfs.to_parquet(directory + '\\'+ str(year) + '.parquet', engine='fastparquet')
    return ref_data_dfs

def processRefData_all(ref_data_dir = mcap_dir_raw, data_save_dir = mcap_dir_processed):
    
    ref_data_list = []
    for year in [2018, 2019, 2020]:
        
        df = processRefDatafromCSV(year, ref_data_dir, data_save_dir )
       
        ref_data_list.append(df)
    
    ref_datas = pd.concat(ref_data_list, axis = 0)
    
    ref_datas = ref_datas.sort_values(by = ['date', 'ticker'], ascending = True)
    
    # save all ref data
    
    ref_datas.to_parquet(data_save_dir +  'all.parquet', engine='fastparquet')
    
    return

 #processRefData_all(mcap_dir_raw,mcap_dir_processed)   


    
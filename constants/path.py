#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
data path for raw data, processed data

@author: bella
"""

import pandas as pd
import zipfile
import datetime

_processed_data_root_dir = 'G:\\My Drive\\QiShi\\Data\\'

_trading_days_path = _processed_data_root_dir +'count_trading_days.parquet'

# processed price and volume data: process 1min csv data into eod parquet data,  (date, ticker) level

_1min_pv_data_dir = _processed_data_root_dir + '1min_pv\\'

_eod_pv_data_dir = _processed_data_root_dir + 'EOD_pv\\'

_stk_daily_return_dir = _processed_data_root_dir + 'stock_return_daily\\'

_idx_daily_return_dir = _processed_data_root_dir + 'index_return_daily\\'

# processed ref data path: processed into parquet format, (date, ticker) lvel

_eod_adj_factor_dir = _processed_data_root_dir + 'adj_factor\\'

_ref_mkt_val_dir = _processed_data_root_dir + 'mkt_val\\'

_ref_industry_dir = _processed_data_root_dir + 'industry\\'

_idx_univ_dir = _processed_data_root_dir + 'index_univ\\'

_idx_price_dir = _processed_data_root_dir + 'index\\'


# result dir: alpha backtest result
_result_root_dir = 'G:\\My Drive\\QiShi\\test_factors\\'

_result_plot_dir  = _result_root_dir + 'plot\\'

_portfolio_stats_dir  = _result_root_dir + 'result\\'


# raw data dir: 1min csv data by ticker: date/ticker/1min csv data

_raw_data_dir = "G:\\.shortcut-targets-by-id\\1PsYMkyEXLSqagFPvuRxIIqxePA7zUMp-\\Data\\"

# zip path for raw 1 min data
_raw_1min_dir = _raw_data_dir + 'qishi_1min_zip\\'

# sum adjusted factor for stock price
_raw_adj_fct_dir = _raw_data_dir + 'other\\adj_fct\\'

# index price data by date
_raw_idx_dir = _raw_data_dir + 'other\\idx\\'

# limit data
_raw_lmt_dir = _raw_data_dir + 'other\\lmt\\'

# market cap, market value data by date
_raw_mkt_val_dir = _raw_data_dir + 'other\\mkt_val\\'

# industry classification by date
_raw_industry_dir = _raw_data_dir + 'other\\sw\\'

# components for each index by dates: hs300, zz500, zz800, zz1000, zz9999
_raw_idx_univ_dir = _raw_data_dir + 'other\\univ\\'

_info_type = ['adj_fct', 'lmt', 'mkt_val', 'industry', 'idx_trading' ] #idx_univ

_index_list = ['hs300', 'zz500', 'zz800', 'zz1000', 'zz9999']

# data mapping dictionary

ref_path_mapping_dict_raw = {'adj_fct': _eod_adj_factor_dir, 'lmt': _raw_lmt_dir, 'mkt_val': _raw_mkt_val_dir, 'industry': _raw_industry_dir, 'idx_trading': _raw_idx_dir, 'idx_univ': _raw_idx_univ_dir }

ref_path_mapping_dict = {'adj_fct': _eod_adj_factor_dir, 'lmt': _raw_lmt_dir, 'mkt_val': _ref_mkt_val_dir, 'industry': _ref_industry_dir, 'idx_trading': _idx_price_dir, 'idx_univ': _idx_univ_dir }

#'/Volumes/GoogleDrive/.shortcut-targets-by-id/1PsYMkyEXLSqagFPvuRxIIqxePA7zUMp-/Data/other/idx/2018'

def get1minPVDataPath(asofDate, info_type):
    """  
    Parameters
    ----------
    asofDate: datetime.date
    
    info_type : str
        "close", "price", "turnover", "volume", "matchItem"

    Returns
    -------
    path:  end with '.parquet'

    """  
    yearStr = asofDate.strftime('%Y')
    dateStr = asofDate.strftime('%Y%m%d')    
    
    if info_type ==  "matchItem":
        info_type = "match_item"
    
    path = _1min_pv_data_dir+ info_type + '\\'+ yearStr + '\\' + dateStr + '.parquet'
    return path 

def getEodPVDataPath(asofDate, info_type):
    """ 

    Parameters
    ----------
    asofDate: datetime.date
    
    info_type : str
        "adj_price", "volume", "all", "matchItem"

    Returns
    -------
    path:  end with '.parquet'

    """
    
    yearStr = asofDate.strftime('%Y')
    dateStr = asofDate.strftime('%Y%m%d')    

    if info_type ==  "matchItem":
        info_type = "match_item"
     
    path = _eod_pv_data_dir+ info_type + '\\'+ yearStr + '\\' + dateStr + '.parquet'
    return path   
    
def getRawRefDataPath(asofDate, info_type, indexType = None):
    """
    

    Parameters
    ----------
    asofDate : datetime.date
        
    info_type : str
       element in ['adj_fct', 'lmt', 'mkt_val', 'industry', 'idx_univ', 'idx_trading']
    
    indexType : str
        element in ['hs300', 'zz500', 'zz800', 'zz1000', 'zz9999']

    Returns
    -------
    path
        

    """
    
    yearStr = asofDate.strftime('%Y')
    dateStr = asofDate.strftime('%Y%m%d')    
    if info_type == 'idx_univ':
        path = ref_path_mapping_dict_raw[info_type]+ indexType + '\\'+ yearStr + '\\' + dateStr + '.csv'
    else:
        path = ref_path_mapping_dict_raw[info_type]+ yearStr + '\\' + dateStr + '.csv'
    return path
    

#data after processed with parquet

def getRefDataPath(asofDate, info_type):
    """
    

    Parameters
    ----------
    asofDate : datetime.date
        
    info_type : str
       element in ['adj_fct', 'lmt', 'mkt_val', 'industry', 'idx_univ', 'idx_trading']
    
    indexType : str
        element in ['hs300', 'zz500', 'zz800', 'zz1000', 'zz9999']

    Returns
    -------
    path
        
    """
    
    yearStr = asofDate.strftime('%Y')
    dateStr = asofDate.strftime('%Y%m%d')    
    if info_type in ['idx_univ', 'idx_trading'] :
        path = ref_path_mapping_dict[info_type]+ yearStr + '\\' + dateStr + '.parquet'
    elif info_type in ['adj_fct', 'mkt_val', 'industry']:
        path = ref_path_mapping_dict[info_type]+ 'all' + '.parquet'
    else:
        path = ref_path_mapping_dict[info_type]+ yearStr + '\\' + dateStr + '.csv'
    return path

    
    
    
    

    








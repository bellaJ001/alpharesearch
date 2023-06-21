# -*- coding: utf-8 -*-
"""
Created on Tue Jun 20 14:17:42 2023

data processor utils functions

@author: bella
"""
import pandas as pd
import numpy as np
import os
import datetime as dt
from datetime import timedelta


def get_multi_process_by_month(year):
    year, nMonths = str(year), 12
    monthStart = pd.date_range(year, periods=nMonths, freq='MS').to_pydatetime()
    monthStart = [i.date() for i in monthStart]
    monthEnd = pd.date_range(year, periods=nMonths, freq='M').to_pydatetime()
    monthEnd = [i.date() for i in monthEnd]
    
    multi_times = [(s,e) for s,e in zip(monthStart, monthEnd)]
    return multi_times

def get_multi_process_by_split(startDate, endDate, splits = 5):
    
    ts = pd.date_range(startDate, endDate, periods= splits).to_pydatetime()
    ts = [i.date() for i in ts]
    next_start = [ i+timedelta(1) for i in ts]
    
    starts =[ ts[0]] + next_start[1:-1]
    ends = ts[1:]
    
    multi_times = [(s,e) for s,e in zip(starts, ends)]
    
    return multi_times

def getData(dataType, path):
    """
    get data with path

    Returns
    -------
    dataframe
    col = [code,....]

    """
   
    try:
        if dataType == 'csv':
            data = pd.read_csv(path)
            data['ticker'] = data['code'].apply(lambda x: str(x).zfill(6))
        elif dataType == 'parquet':
            data = pd.read_parquet(path)
    except: 
        data = pd.DataFrame()
    
    return data

def readDataFromDir(dir_path, dataType):
    """
    read all the data and concat into a dataframe from a dir

    Parameters
    ----------
    dir_path : string
       path for parquet files

    Returns
    -------
    dataframe: concat all files in dir.

    """
    file_list = os.listdir(dir_path)
    file_list = [f for f in file_list if f.split('.')[1]!= 'ini']

    files = []
    for f in file_list:
        if dataType == 'parquet':
                df= pd.read_parquet( dir_path + '\\' + f )
        elif dataType == 'csv':
            df= pd.read_csv( dir_path + '\\' + f )
        else:
            print(f) 
        files.append(df)
    if len(files) ==0:
        return pd.DataFrame()
    allData = pd.concat(files, axis = 0)
    return allData
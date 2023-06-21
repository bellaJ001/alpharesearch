# -*- coding: utf-8 -*-
"""
Created on Fri Jun 16 16:50:40 2023

@author: bella
"""

from multiprocessing import Process, Queue
import  time
from backtester.backtest import  runBacktest
# alphas.cashflow.enrich_cashflow import adjCashFlowFactor

def backtest_single_loop( data, factorlist, backtester, params, q):
    for factorName in factorlist:
        try:
            #rawScore, factorName = adjCashFlowFactor(data, factorName)
            rawScore= data[['date', 'ticker',factorName]]
            portfolioOutput, portfolioStats, portfolioOutput_long, portfolioStats_long, decileOutput, decileStats =  runBacktest(backtester, params, rawScore, factorName )
            stats = {'portfolioOutput':portfolioOutput, 'portfolioStats': portfolioStats, 'portfolioOutput_long': portfolioOutput_long, 'portfolioStats_long': portfolioStats_long, 
                   'decileOutput': decileOutput, 'decileStats':decileStats}
            res = {factorName: stats}
        except:
            res = {factorName: 'err'}
        q.put(res)

def backtest_multi_process(data, factorList, backtester, params, processes = 10):

    q = Queue()      
   
    # dateList = get_all_trading_days(startDate, endDate, holdingPeriod = 1) 
    # if not dateList: # empty if no trading dates
    #     err = [(startDate, endDate)]
    #     return err

    # split work into 5 or 10 processes

    def splitlist(inlist, chunksize):
        chunksize = int(chunksize)
        return [inlist[x:x+chunksize] for x in range(0, len(inlist), chunksize)]
       
    factorListSplitted = splitlist(factorList, len(factorList)/processes)
    
    print("sub list number: {}".format(len(factorListSplitted)))
    
    print("splitted list ready")

    t = time.time()
    for subFactorlist in factorListSplitted:
        print("sub list length: {}".format(len(subFactorlist)))

        p = Process(target= backtest_single_loop, args=(data, subFactorlist,backtester,params, q))
        p.Daemon = True
        p.start()
    for subFactorlist in factorListSplitted:
        p.join()
    print("total process time for all factors: {}".format(time.time()-t))
    # while True:
    #     print(q.get())
    return q
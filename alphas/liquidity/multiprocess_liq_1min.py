# -*- coding: utf-8 -*-
"""
Created on Tue Jun 13 13:28:27 2023

@author: bella
"""


from multiprocessing import Process, Queue
import  time
import alphas.liquidity.liquidity_1min as liq
from backtester.utils import  get_all_trading_days

# =============================================================================
# def f2(wordlist, mainwordlist, q):
#     for mainword in mainwordlist:
#         matches = liq.enrichAndProcessEodwithIntervalStats_liq(mainword,wordlist,len(wordlist),0.7)
#         q.put(matches)
# 
# =============================================================================
def enrichEodwithLiqStats_loop_by_day( dates, q):
    for asofDate in dates:
        errs = liq.enrichAndProcessEodwithIntervalStats_liq(asofDate, asofDate, liq._intervals)
        q.put(errs)

def enrichEodwith1minbyMonth_multi_process(startDate, endDate, processes = 10):

    # constants (for 50 input words, find closest match in list of 100 000 comparison words)
    q = Queue()
# =============================================================================
#     wordlist = ["".join([random.choice([letter for letter in "abcdefghijklmnopqersty"]) for lengthofword in xrange(5)]) for nrofwords in xrange(100000)]
#     mainword = "hello"
#     mainwordlist = [mainword for each in xrange(50)]
# =============================================================================
    
    
    dateList = get_all_trading_days(startDate, endDate, holdingPeriod = 1) 
    if not dateList: # empty if no trading dates
        err = [(startDate, endDate)]
        return err
    
# =============================================================================
#     # normal approach
#     
#     t = time.time()
#     for mainword in mainwordlist: # for date in dateli
#         matches = difflib.get_close_matches(mainword,wordlist,len(wordlist),0.7)
#         q.put(matches)
#     print(time.time()-t)
# 
# =============================================================================
    # split work into 5 or 10 processes

    def splitlist(inlist, chunksize):
        chunksize = int(chunksize)
        return [inlist[x:x+chunksize] for x in range(0, len(inlist), chunksize)]
       
    dateListSplitted = splitlist(dateList, len(dateList)/processes)
    
    print("sub list number: {}".format(len(dateListSplitted)))
    #date range list: mainwordlistsplitted
    print("splitted list ready")

    t = time.time()
    for subDatelist in dateListSplitted:
        print("sub list length: {}".format(len(subDatelist)))
        p = Process(target= enrichEodwithLiqStats_loop_by_day, args=(subDatelist,q))
        p.Daemon = True
        p.start()
    for subDatelist in dateListSplitted:
        p.join()
    print("total process time for 1m: {}".format(time.time()-t))
    # while True:
    #     print(q.get())
    return q
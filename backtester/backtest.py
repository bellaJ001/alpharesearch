#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Apr 12 17:37:36 2023

backtester for single alpha

@author: bella
"""

import datetime as dt
from datetime import timedelta
from dataLoader.data_loader import loadData
import statsmodels.api as sm
import numpy as np
import pandas as pd
from dataProcessor.return_calculator import returnCalculator
from backtester.utils import neutralize, filter_small_trading_days
from utils.helper import winsorize
from constants.path import _result_plot_dir, _portfolio_stats_dir
import matplotlib.pyplot as plt
plt.style.use('ggplot')

#'000905', 'is_zz500 '
class BackTest:
    def __init__(self, startDate, endDate, holdingPeriod):
        self.startDate = startDate
        self.endDate = endDate
        self.holdingPeriod = holdingPeriod   
        self.rc = returnCalculator(self.startDate - timedelta(holdingPeriod), self.endDate + timedelta(holdingPeriod))
        self.stockReturn = self.rc.calcStkRet( self.holdingPeriod )
        self.stockForwardReturn = self.rc.calcForwardReturn(self.stockReturn, 'ticker', 'return')
    
    def calcAdjScore(self, rawData, signal_univ ='zz9999', neutralize_method = None ):
        """
        calculate adjusted score with neutralized method: 'industry', 'mcap', 'industry_and_mcap'
        
        Parameters
        ----------
        rawScore : raw score data (panel)
                 ['date', 'ticker', 'score']
           
        ind_neutral : run industry neutral  industry data 
                  df = loadDataDaily(datetime.date(2020,1, 20), 'industry')
        adj_method : score, rank
        
        weight_method: 'equal', 'rank', 'score'
        
        Returns
        -------
        DataFrame
        adjusted score data (panel) with column: ['date', 'ticker', 'score_adj']
    
        """
        rawScore = rawData.copy()
        rawScore.columns = ['date', 'ticker', 'score']
        rawScore = rawScore[~rawScore['score'].isna()]
        rawScore['date'] = pd.to_datetime(rawScore['date'])
        # filter tickers in universe
        rawScore = self.enrichUniverse(rawScore)        
        univ_col = 'is_'+ signal_univ               
        rawScore =rawScore[rawScore[univ_col] == 1]        
        # remove tickers with trading days < 150
        rawScore = filter_small_trading_days(rawScore)

        rawScore['score_adj'] = rawScore['score'].copy()
                
        if neutralize_method in ['industry', 'industry_and_mcap']:
            winsorize(rawScore, groupbyCol = ['date', 'industry'], col ='score_adj', percentile = 1)
        elif neutralize_method == 'mcap' or not neutralize_method:
            winsorize(rawScore, groupbyCol = ['date'], col ='score_adj', percentile = 1)
        
        adjScore = neutralize(rawScore, 'score_adj', self.startDate, self.endDate, neutralize_method)
        
        return adjScore

    def calcWeight(self, adjScore, weight_method = 'zscore', decile_number = 10,  top_decile = 0, bottom_decile = 9):
        """       
        construct a dollar neutral portfolio by calculating asset weight for each cross sectional portfolio with different method
        split portfolio into 10 groups by ranking signals from small to large, equal weight in each decile to calculate decile performance later
        
        "zscore": weight each asset by signal z score
        "equal": construct l/s portfolio by long bottom decile, short top decile, assets in each decile are equal weight
        "square_score": weight each asset by signal square abs(score)
               
        Parameters
        ----------
        adjScore : DataFrame
            adjusted score data (panel) with column: ['date', 'ticker', 'score_adj']
        weight_method : str
            asset weight method: 'zscore', 'equal', 'square_score'. The default is 'zscore'.
        decile_number : int
            total decile for each cross sectional portfolio. The default is 10.
        top_decile : int
            short decile number. The default is 0.
        bottom_decile : int
            long decile number. The default is 9.

        Returns
        -------
        DataFrame
            adjScore with columns: ['date', 'ticker', 'weight', 'score_adj', 'decile_rank', 'decile_weight']

        """        
        adjScore['decile_rank'] = adjScore.groupby('date')['score_adj'].transform(lambda x: pd.qcut(x.rank(method='first'), decile_number, labels = False))
        
        countDf = adjScore.groupby(['date', 'decile_rank']).size().rename('count').to_frame().reset_index()
        adjScore = adjScore.merge(countDf, on = ['date', 'decile_rank'])
        adjScore['decile_weight'] = 1/adjScore['count']

        if weight_method == 'zscore':   
            # use standardlized signal value as weight
            adjScore['score_sum'] = np.nan    
            pos_sum = adjScore[adjScore['score_adj']>0].groupby('date').agg({'score_adj':sum})
            neg_sum = adjScore[adjScore['score_adj']<0].groupby('date').agg({'score_adj':sum})
           
            adjScore.loc[adjScore['score_adj']>0, 'score_sum'] = adjScore[adjScore['score_adj']>0]['date'].map(pos_sum.reset_index().set_index('date')['score_adj'] )
            adjScore.loc[adjScore['score_adj']<0, 'score_sum'] = adjScore[adjScore['score_adj']<0]['date'].map(neg_sum.reset_index().set_index('date')['score_adj'].abs() )
     
            adjScore['weight'] = adjScore['score_adj']/adjScore['score_sum'] # weight for each ticker in portfolio

            return adjScore[['date', 'ticker', 'weight', 'score_adj', 'decile_rank', 'decile_weight']]
        
        elif weight_method == 'equal':
            # long top decile and short the bottom, equal weight in each decile
            adjScore['weight'] = 0
                                    
            adjScore.loc[adjScore['decile_rank'] == top_decile, 'weight'] = -1/adjScore['count'] # rank by rankscore value from small to large
            adjScore.loc[adjScore['decile_rank'] == bottom_decile, 'weight'] = 1/adjScore['count']
                
            return rawScore[['date', 'ticker', 'weight', 'score_adj', 'decile_rank', 'decile_weight']]
       
        elif weight_method == 'square_score':
            # calculate weight = sqrt(score_adj) / sum(sqrt(score_adj))
            adjScore['square_score_sum'] = np.nan    
            pos_sum = adjScore[adjScore['score_adj']>0].groupby('date')['score_adj'].apply(lambda x: np.sqrt(x.abs()).sum())
            neg_sum = adjScore[adjScore['score_adj']<0].groupby('date')['score_adj'].apply(lambda x: np.sqrt(x.abs()).sum())
           
            adjScore.loc[adjScore['score_adj']>0, 'square_score_sum'] = adjScore[adjScore['score_adj']>0]['date'].map(pos_sum.reset_index().set_index('date')['score_adj'] )
            adjScore.loc[adjScore['score_adj']<0, 'square_score_sum'] = adjScore[adjScore['score_adj']<0]['date'].map(neg_sum.reset_index().set_index('date')['score_adj'] )
            
            adjScore['weight'] = adjScore.groupby('date')['score_adj'].transform(lambda x: np.sign(x) * (np.sqrt(x.abs())))      
            adjScore['weight'] = adjScore['weight']/adjScore['square_score_sum']
           
            return adjScore[['date', 'ticker', 'weight', 'score_adj', 'decile_rank', 'decile_weight']]
            
        else:
            raise ValueError('missing column for calculation')
            return pd.DataFrame()
    
    def calcAlphaDecay(self, data):
        """
        data enriched with signal and stock forward return
        """
        alpha_decay1 =  data.groupby('date').apply(lambda df: df['score_adj'].corr(df['forward_return_1'], method = 'spearman'))
        alpha_decay3 =  data.groupby('date').apply(lambda df: df['score_adj'].corr(df['forward_return_3'], method = 'spearman'))
        alpha_decay5 =  data.groupby('date').apply(lambda df: df['score_adj'].corr(df['forward_return_5'], method = 'spearman'))
        alpha_decay10 =  data.groupby('date').apply(lambda df: df['score_adj'].corr(df['forward_return_10'], method = 'spearman'))
        alpha_decay20 =  data.groupby('date').apply(lambda df: df['score_adj'].corr(df['forward_return_20'], method = 'spearman'))
        decays = pd.concat( [alpha_decay1, alpha_decay3,alpha_decay5, alpha_decay10, alpha_decay20 ], axis = 1).reset_index()
                          
        decays.columns = ['date', 'decay1', 'decay3', 'decay5', 'decay10', 'decay20']
        return decays
     
    def loadIdxForwardRet(self, idx):
        """
        idx : benchmark, '000001', '000300', '000852', '000905', '000906', '399102', '399106'
        """
        indexRet = self.rc.calcIndexRet( self.holdingPeriod)
        indexForwardRet = self.rc.calcForwardReturn(indexRet, 'ticker_idx', 'return_idx')
        return indexForwardRet[indexForwardRet['ticker_idx'] == idx]
    
    def enrichUniverse(self, data):
        univ = loadData(self.startDate, self.endDate, 'idx_univ')
        data = pd.merge(data, univ,  on = ['date', 'ticker'], how = 'left')
        return data
    
    def calcTurnover(self, data, weightCol):
        weightDf = data.pivot(index = 'ticker', columns = 'date', values = weightCol)
        weightDf = weightDf.unstack().reset_index()
        weightDf = weightDf.rename( {0:'weight'}, axis =1)
        weightDf['weight'].fillna(0,inplace = True)
        weightDf['turnover'] = weightDf.groupby('ticker')['weight'].diff().abs()
        turnoverDf = weightDf.groupby('date')['turnover'].sum().reset_index()
        return turnoverDf
  
    def enrichStockandIdxReturn(self, signal, bmk = '000905', signal_univ = 'zz500'):
        """
        signal: rawData[['date', 'ticker', 'weight', 'score_adj', 'decile_rank']]
        
        bmk: '000001', '000300', '000852', '000905', '000906', '399102', '399106'
 
        """
         #stockForwardReturn has holding period
        data = pd.merge( self.stockForwardReturn, signal, on = ['date', 'ticker'], how = 'left') # todo: check nan, halt?
        data = data[~data['weight'].isna()] # not in universe
        idx_ret = self.loadIdxForwardRet(bmk)
        bmk_forward_ret = idx_ret.set_index('date')['forward_return_1']
        data['forward_return_bmk'] = data['date'].map(bmk_forward_ret)

        decilePortfolio = data.copy()   
        decilePortfolio = decilePortfolio.drop('weight', axis=1)
        
        return data, decilePortfolio, bmk_forward_ret
    
    def calcPortfolioStats(self, enrichedData, bmk_forward_ret):#signal, bmk = '000905', signal_univ = 'zz9999'):
        """
        calculate portfolio return for scores
        
        signal: rawData[['date', 'ticker', 'weight', 'score_adj', 'decile_rank']]
        
        bmk: '000001', '000300', '000852', '000905', '000906', '399102', '399106'
    
        Returns
        -------
        portfolio return, turnover and alpha decays in daily level
    
        """
        
        #stockForwardReturn has holding period
        data = enrichedData.copy()
        data = data[data['weight'] != 0 ]
        
        data = data.sort_values(by = 'date', ascending  = True)
        
        turnover = self.calcTurnover(data, 'weight')
        
        data['signal_return'] = data['weight'] * data['forward_return_1']
     
        portfolio_ret = data.groupby('date')['signal_return'].sum()
           
        rankIC = data.groupby('date').apply(lambda df: df['score_adj'].corr(df['forward_return_1'], method = 'spearman'))
        
        portfolioDf = pd.concat([portfolio_ret, bmk_forward_ret, rankIC], axis = 1)
        portfolioDf.columns = ['signal_return', 'bmk_return', 'rankIC'] # forward 1 return
        portfolioDf = portfolioDf[~portfolioDf['signal_return'].isna()]
        portfolioDf = portfolioDf[~portfolioDf['bmk_return'].isna()]
        
        portfolioDf['portfolio_cumRet'] = (portfolioDf['signal_return']+1).cumprod() -1
        portfolioDf['benchmark_cumRet'] = (portfolioDf['bmk_return']+1).cumprod() -1
                
        portfolioDf['portfolio_ret_mktadj'] = portfolioDf['signal_return'] - portfolioDf['bmk_return'] # day level
        portfolioDf['portfolio_cumRet_mktadj'] = (portfolioDf['portfolio_ret_mktadj']+1).cumprod() -1
              
        alpha_decay =  self.calcAlphaDecay(data) # 'rankIC' =  'decay1'
        portfolioDf = portfolioDf.merge(turnover, on = 'date')
        portfolioDf = portfolioDf.merge(alpha_decay, on = 'date')
         
        return portfolioDf

    def calcSignalStats(self, portfolioDf):
        """
        
        Parameters
        ----------
        portfolioDf : portfolio returns time series data
        
        Returns
        -------
        outPutStats : signal mean stats over back test period

        """
        portfolioDf['portfoliocumRet'] = (portfolioDf['portfolio_ret_mktadj']+1).cumprod() -1
        portfolioDf['portfolioValue'] = (portfolioDf['portfolio_ret_mktadj']+1).cumprod()
        portfolioDf['drawdown'] = (portfolioDf['portfolioValue'])/ (portfolioDf['portfolioValue'].cummax()) 
        maxDrawDown = 1- min(portfolioDf['drawdown'])
        # performance
        meanRet = portfolioDf['portfolio_ret_mktadj'].mean()/ self.holdingPeriod * 250
        sdRet = portfolioDf['portfolio_ret_mktadj'].std()/ np.sqrt(self.holdingPeriod) * np.sqrt(250)
        portfolioIR = meanRet/sdRet
        portfolioIC = portfolioDf['rankIC'].mean()  
        portfolioIC_sig_ratio = portfolioDf[portfolioDf['rankIC'].abs()>0.02]['rankIC'].count()/portfolioDf['rankIC'].count()
        
        turnover = portfolioDf['turnover'][2:-2].mean()
        corrBmk = portfolioDf['signal_return'].corr(portfolioDf['bmk_return'])
        outputStats = pd.DataFrame({ 'portfolioIR':[portfolioIR], 'portfolioIC': [portfolioIC], 'portfolioIC_sig_ratio': [portfolioIC_sig_ratio], 'turnover': [turnover], 'maxDrawDown': [maxDrawDown],  'corrBmk': [corrBmk] }) 
        
        # decays:
        for decay in ['decay1', 'decay3','decay5', 'decay10', 'decay20']:
            outputStats[decay] = portfolioDf[decay].mean()
        
        return outputStats

    def calcDecilePerformance(self, decilePortfolio, bmk_forward_ret):
        decilePortfolio.rename( {'decile_weight': 'weight'},axis = 1, inplace = True)
        decileOutput = decilePortfolio.groupby('decile_rank').apply(self.calcPortfolioStats, bmk_forward_ret = bmk_forward_ret)
        decileOutput = decileOutput.droplevel(1, axis=0).reset_index()
        decileStats = decileOutput.groupby('decile_rank').apply(self.calcSignalStats)
        decileStats =  decileStats.droplevel(1, axis=0).reset_index()
        return decileOutput, decileStats
    
    def plot(self, factorName, portfolioOutput, portfolioOutput_long, decilePortfolio):
        
        fig, axes = plt.subplots(nrows=5, ncols=1, figsize=(10,15), constrained_layout=True)
        # cumulative returns
        ax = axes[0]
        returns = pd.merge(portfolioOutput[['date', 'portfolio_cumRet', 'benchmark_cumRet', 'portfolio_cumRet_mktadj']], portfolioOutput_long[['date', 'portfolio_cumRet', 'portfolio_cumRet_mktadj']], on = 'date', how = 'left', suffixes=['','_long_only'] )
        returns.set_index('date').plot( grid=True, ax = axes[0])
        ax.set_title(f'{factorName} Cum. Return: holding period = {self.holdingPeriod} days')

        # returns
        returns = pd.merge(portfolioOutput[['date', 'portfolio_ret_mktadj']], portfolioOutput_long[['date', 'portfolio_ret_mktadj']], on = 'date', how = 'left', suffixes=['','_long_only'] )
        #rankICs.set_index('date').plot(kind='scatter',figsize=(15,5), grid=True)
        meanRet = portfolioOutput['portfolio_ret_mktadj'].mean()/ self.holdingPeriod * 250
        meanRet_long = portfolioOutput_long['portfolio_ret_mktadj'].mean()/ self.holdingPeriod * 250
        
        #ax = axes[1]
        ax = returns.set_index('date').plot(kind='bar', grid =True, ax = axes[1])
        new_ticks = ax.get_xticks()[::20]
        curr_label = [label.get_text() for label in ax.get_xticklabels()]
        new_label = [i.split(' ')[0] for i in curr_label[::20]]
        ax.set_xticks(new_ticks)
        ax.set_xticklabels(new_label,rotation=45)        
        ax.set_title(f'{factorName} mkt adjusted return: holding period = {self.holdingPeriod} days, long/short avg by year= { round(meanRet,2) }, long only avg by year = { round(meanRet_long,2) }')
 
        # turnover
        ax = axes[2]
        turnovers = pd.merge(portfolioOutput[['date', 'turnover']], portfolioOutput_long[['date', 'turnover']], on = 'date', how = 'left', suffixes=['','_long_only'] )
        turnovers.set_index('date').plot( grid=True, ax = axes[2])
        ax.set_title(f'{factorName} turnover: holding period = {self.holdingPeriod} days')
        
        # decile performance: return on avg      
        ax = axes[3]
        decilePortfolio['portfolio_ret_mktadj_year'] = decilePortfolio['portfolio_ret_mktadj']/self.holdingPeriod * 250
        portfolio_ret_mktadj = decilePortfolio.groupby('decile_rank')['portfolio_ret_mktadj_year'].mean().reset_index().set_index('decile_rank')
        portfolio_ret_mktadj.plot(kind='bar', ax = axes[3])
        ax.set_title('return by decile')
        
        # decile performance: cumulative return       
        ax = axes[4]
        decilePortfolio.set_index('date', inplace=True)
        colors = ['#1f77b4','#ff7f0e','#2ca02c','#d62728','#9467bd','#8c564b','#e377c2','#7f7f7f','#bcbd22','#17becf']
    
        i = 0
        for n, g in decilePortfolio.groupby('decile_rank')['portfolio_cumRet_mktadj']:
            ax.plot(g, color=colors[i], label=n )
            i += 1
        ax.set_title(f'{factorName} Decile Cum. Adj Return: holding period = {self.holdingPeriod} days')
        ax.legend()
        ax.grid(alpha=0.3)
        
        savepath = _result_plot_dir + factorName + '_hold_' + str(self.holdingPeriod) +'d' + '.png'
        plt.savefig(savepath)      
        fig.tight_layout(pad=2.0)
        

def runBacktest(bt, params, rawScore, factorName ):
    """ 
    bt: BackTest(startDate, endDate, holdingPeriod)
    params: dict
    
    rawScore: dataframe with column in order
        ['date', 'ticker', 'score']"""

    holdingPeriod = params['holdingPeriod']
    signal_univ, bmk =  params['signal_univ'], params['bmk']#'zz500']
    neutralize_method, weight_method = params['neutralize_method'], params['weight_method']
    decile_number, top_decile, bottom_decile = params['decile_number'], params['top_decile'], params['bottom_decile']
    
    ## bmk： 000905 zz500/ 000852 zz1000
    ## signal_univ: zz9999
    rawScore = rawScore.sort_values(by = ['date', 'ticker'])
    adjScore = bt.calcAdjScore( rawScore, signal_univ , neutralize_method )
    adjScore = bt.calcWeight(adjScore, weight_method, decile_number, top_decile, bottom_decile )
    
    adjScore, decilePortfolio, bmk_forward_ret = bt.enrichStockandIdxReturn(adjScore, bmk, signal_univ)
    
    portfolioOutput = bt.calcPortfolioStats( adjScore, bmk_forward_ret ) 
    portfolioStats = bt.calcSignalStats(portfolioOutput)
    
    longScore = adjScore[adjScore['weight'] > 0]
    portfolioOutput_long = bt.calcPortfolioStats( longScore, bmk_forward_ret )
    portfolioStats_long = bt.calcSignalStats(portfolioOutput_long)
    
    decileOutput, decileStats = bt.calcDecilePerformance( decilePortfolio, bmk_forward_ret)
    
    result_saving_path = _portfolio_stats_dir + factorName + '_holding_' + str(holdingPeriod) +'d' + '.xlsx'
    
    with pd.ExcelWriter(result_saving_path) as writer:
        portfolioOutput.to_excel(writer, sheet_name='portfolio_output')
        portfolioStats.to_excel(writer, sheet_name='portfolio_stats')
        portfolioOutput_long.to_excel(writer, sheet_name='portfolio_output_long')
        portfolioStats_long.to_excel(writer, sheet_name='portfolio_stats_long')
        decileOutput.to_excel(writer, sheet_name='decile_output')
        decileStats.to_excel(writer, sheet_name='decile_stats')
        
    bt.plot(factorName, portfolioOutput, portfolioOutput_long, decileOutput)
    
    return portfolioOutput, portfolioStats, portfolioOutput_long, portfolioStats_long, decileOutput, decileStats
    



    
    
    
    
    
    
    
    
    
    
    
    
    

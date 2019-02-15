#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Fri Apr  6 08:23:27 2018

@author: miguelcaballero
"""

###  Begin Test:
from library import summary
import pandas as pd
import numpy as np

# =============================================================================
# INGA-16-90
# =============================================================================

def missing_SCADA(logger,df,scada,name,flag):
    
    # Data preparation
    df['ts'] = pd.to_datetime(df['timestamp'])
    df.index = df['ts']
    
    # Complete time-series
    index = pd.date_range(df.index[0], df.index[-1], freq='600S')
    
    # Fill time-series gaps with NaN
    
    test = False
    df[flag] = 0
    if len(index) > len(df):
        missingValsIndex = index.difference(df.index)
        df = df.reindex(index, fill_value=np.nan)
        df[flag].loc[missingValsIndex] = 1
        test=True
    total = df.count()
    total = total.iloc[0]
    #logger.info(summary(flag,total,scada))
    if test:
        tmp=len(missingValsIndex)
        logger.error(summary(flag,tmp,scada))
# =============================================================================
# INGA-84   
# =============================================================================
        
def beyond_physical_limits(logger,df,scada,name,flag,threshold):
   
    # Data preparation
    df['ts'] = pd.to_datetime(df['timestamp'])
    total = df.count()
    df[flag] = 0
    
    # find values different than threshold
    columns = df.columns.values
    index = df.index[ (df[columns[4]] < threshold[0]) | (df[columns[4]] > threshold[1]) ]
    
    test = False
    # Fill time-series gaps with NaN
    if len(index) > 0:
        df[flag].loc[index] = 1
        test = True
    
    # Create scalars for reporting
    total = total.iloc[0]
    # Create Test Results
    #logger.info(summary(flag,total,scada))
    if test:
        tmp=len(index)
        logger.error(summary(flag,tmp,scada))
    df.drop(['ts'],axis=1)
        
##################################################################
def test_run_all(logger,df,scada):
    # Test's
    missing_SCADA(logger,df,scada,"missing_SCADA","missVals")
    beyond_physical_limits(logger,df,scada,"beyond_physical_limits","bpl",[1, 25])
    
    
    
    
    
    
    
    
    

from testing import get_logger, get_scada_dataframe, write_scada_dataframe,test_report,summary,get_scada_columns
import sys
import logging
import pandas as pd
import numpy as np

def threshold_limits(logger,df,scada,windspd_mean_flag,windspd_min_max_flag,windspd_stdev_flag,ambtemp_mean_flag,ambtemp_other_flag):
    if any(df.tag == 'windspd'):
        df['windspd_mean_flag'] = np.where(((df['mean'] <0) | (df['mean'] > 90)),1,0)
        df['windspd_min_max_flag'] = np.where(((df['min'] <0) | (df['min'] > 90) | (df['max'] <0) | (df['max'] > 90)),1,0)
        df['windspd_stdev_flag'] = np.where(((df['stdev'] <0) | (df['stdev'] > 4)),1,0)

        r10 = df[windspd_mean_flag].sum()
        r11 = df[windspd_min_max_flag].sum()
        r12 = df[windspd_stdev_flag].sum()
        logger.info(summary(windspd_mean_flag,r10,scada))
        logger.info(summary(windspd_min_max_flag,r11,scada))
        logger.info(summary(windspd_stdev_flag,r12,scada))

    elif any(df.tag == 'ambtemp'):
        df['ambtemp_mean_flag'] = np.where(((df['mean'] <-50) | (df['mean'] > 60)),1,0)
        df['ambtemp_other_flag'] = np.where(((df['min'] <-50) | (df['min'] > 60) | (df['max']<-50) | (df['max']>60) | (df['stdev']<-50) |
        (df['stdev'] > 60)),1,0)

        r13 = df['ambtemp_mean_flag'].sum()
        r14 = df['ambtemp_other_flag'].sum()
        logger.info(summary(ambtemp_mean_flag,r13,scada))
        logger.info(summary(ambtemp_other_flag,r14,scada))

    else:
        print("move to next test")
		
def timestamp_start_end(logger,df,scada,timestamp_flag):
    df['timestamp_flag'] = np.where((pd.to_datetime(df['TimestampStart'],format='%Y-%m-%d %H:%M:%S')>pd.to_datetime(df['TimestampEnd'],format='%Y-%m-%d %H:%M:%S')),1,0)
																				 
																				 
def test_run_all(logger,df,scada):
    threshold_limits(logger,df,scada,'windspd_mean_flag','windspd_min_max_flag','windspd_other_flag','ambtemp_mean_flag','ambtemp_other_flag')
    timestamp_start_end(logger,df,scada,timestamp_flag)
	#timestamp_start_end(logger,df,scada,timestamp_flag)




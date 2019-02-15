from testing import get_logger, get_scada_dataframe, test_report,summary,get_alarm_columns
import pandas as pd
import numpy as np

##################################################################
def test_missing_data(logger,df,key,flag):
   fields=get_alarm_columns()
   sum=0
   for field in fields:
       flg = "F." + field 
       df[flg] = np.where((pd.isnull(df[field])), 1, 0);
       sum = sum + df[flg].sum()
   logger.info(summary(flag,sum,key))

##################################################################
def test_timestamp_end_before_start(logger,df,key,flag):
   fields=get_alarm_columns()
   sum=0
   logger.info(summary(flag,sum,key))

##################################################################
def test_unknown_alarm_code(logger,df,key,flag,alarm_codes):
   sum=0
   logger.info(summary(flag,sum,key))

##################################################################
def test_unknown_device_type(logger,df,key,flag,device_types):
   sum=0
   logger.info(summary(flag,sum,key))

##################################################################
def test_run_all(logger,df,key,alarm_codes,device_types):
    test_missing_data(logger,df,key,"null_field")
    test_timestamp_end_before_start(logger,df,key,"timestamp_illogical")
    test_unknown_device_type(logger,df,key,"device_type_unknown",device_types)
    test_unknown_alarm_code(logger,df,key,"alarm_code_unknown",alarm_codes)
    test_timestamp_end_before_start(logger,df,key,"timestamp_illogical")

##################################################################
#  End of Test
##################################################################

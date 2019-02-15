from library import get_logger, get_scada_dataframe, write_scada_dataframe,test_report,summary,get_scada_columns
import sys
import logging
import pandas as pd
import numpy as np
##################################################################
def test_linear_interp(logger,df,scada,flag):
   interval = 3
   input_col = df['avg']
   pd_series= pd.Series(input_col)
   temp_list= []
   for i in range(len(pd_series)-1):
       k= pd_series.iloc[i+1]-pd_series.iloc[i]
       temp_list.append(k)
   temp_series= pd.Series(temp_list).values
   se= pd.Series(temp_series)
   output=[]
   temp_int= interval - 1
   output= se.groupby([se, se.diff().ne(0).cumsum()]).transform('size').ge(temp_int).astype(int)
   clean= []
   clean.append(output[0])
   for i in range(len(output)-2):
       i += 1
       if output[i] == 1:
           output[i-1] = 1
       else:
           pass
   clean= clean + list(output)
   df[flag] = clean
   total = df.count();
   total = total[flag]
   logger.info(summary(flag,total,scada))

##################################################################
def test_flat_line(logger,df,scada,flag):
   input_col = df['avg']
   interval=3
   pd_series=pd.Series(input_col)
   output= []
   output= pd_series.groupby([pd_series, pd_series.diff().ne(0).cumsum()]).transform('size').ge(interval).astype(float)
   df[flag] = output
   total = df.count();
   total = total[flag]
   logger.info(summary(flag,total,scada))

##################################################################
def test_time_interval(logger,df,scada,flag):
   df['ts'] = pd.to_datetime(df['ts'], format='%Y/%m/%dT%H:%M:%S')
   df['ts_min'] = df['ts'].apply(lambda x : x.minute)
   df['ts_sec'] = df['ts'].apply(lambda x : x.second)
   df[flag] = np.where(((df['ts_min'] % 10 != 0) | (df['ts_sec'] != 0)), 1, 0);
   total = df.count();
   total = total[flag]
   logger.info(summary(flag,total,scada))

##################################################################
def test_sdev_mean(logger,df,scada,flag):   
   df[flag] = np.where((df['stdev'] > abs(df['avg'])), 1, 0);
   total = df.count();
   total=total[flag]
   logger.info(summary(flag,total,scada))

##################################################################
def test_missing_values(logger,df,scada,f1,f2):
   df[f1] = np.where((pd.isnull(df['avg'])), 1, 0);
   df[f2] = np.where((pd.isnull(df['avg']) | pd.isnull(df['min']) | pd.isnull(df['max']) | pd.isnull(df['stdev'])), 1, 0);
   total = df.count();
   r1 = total[f1]
   r2 = total[f2]
   logger.info(summary(f1,r1,scada))
   logger.info(summary(f2,r2,scada))

##################################################################
def test_run_all(logger,df,scada):
    test_linear_interp(logger,df,scada,"li1")
    test_flat_line(logger,df,scada,"sd1")
    test_time_interval(logger,df,scada,"ti1")
    test_sdev_mean(logger,df,scada,"sm1")   
    test_missing_values(logger,df,scada,"miss1","miss2")
    cols=get_scada_columns()
    added=[ 'li1', 'sd1', 'ti1', 'sm1', 'sm1', 'miss1', 'miss2']
    desired=cols+added

##################################################################
#  End of Test
##################################################################


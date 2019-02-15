from library import get_logger, get_scada_dataframe, write_scada_dataframe,test_report,summary,get_scada_columns
import sys
import logging
import pandas as pd
import numpy as np
##################################################################
def test_linear_interp(logger,df,scada,flag):
   interval = 3
   input_col = df['mean']
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
   output[output ==1]= se[se==0]
   output= output.fillna(1)
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

   total = df[flag].sum();
   #total = total[flag]
   logger.info(summary(flag,total,scada))

##################################################################
def test_flat_line(logger,df,scada,flag):
   input_col = df['mean']
   interval=3
   pd_series=pd.Series(input_col)
   output= []
   output= pd_series.groupby([pd_series, pd_series.diff().ne(0).cumsum()]).transform('size').ge(interval).astype(float)
   df[flag] = output
   total = df[flag].sum()#count();
   #total = total[flag]
   logger.info(summary(flag,total,scada))

##################################################################
def test_time_interval(logger,df,scada,flag):
   dof= df.copy()
   dof['ts_temp'] = pd.to_datetime(dof['timestamp'], format='%Y/%m/%dT%H:%M:%S')
   dof['ts_min'] = dof['ts_temp'].apply(lambda x : x.minute)
   dof['ts_sec'] = dof['ts_temp'].apply(lambda x : x.second)
   df[flag] = np.where(((dof['ts_min'] % 10 != 0) | (dof['ts_sec'] != 0)), 1, 0);
   total = df[flag].sum()
   
   logger.info(summary(flag,total,scada))

##################################################################
def test_sdev_mean(logger,df,scada,flag):   
   df[flag] = np.where((df['stdev'] > abs(df['mean'])), 1, 0);
   total = df[flag].sum()
   logger.info(summary(flag,total,scada))

##################################################################
def test_missing_values(logger,df,scada,f1,f2):
   df[f1] = np.where((pd.isnull(df['mean'])), 1, 0);
   df[f2] = np.where((pd.isnull(df['mean']) | pd.isnull(df['min']) | pd.isnull(df['max']) | pd.isnull(df['stdev'])), 1, 0);
   r1 = df[f1].sum()
   r2 = df[f2].sum()
   logger.info(summary(f1,r1,scada))
   logger.info(summary(f2,r2,scada))
##################################################################
def erroneous(logger,df,scada,erroneous_1, erroneous_2):
    Jessica=df[['mean','min','max','stdev']]

    Jones=df[['mean']]
    b= len(Jessica['mean'])
    flag_other=[]
    flag_mean=[]
    for i in range(b):
        if any(str(x).isalpha() for x in Jessica.iloc[i]):
            flag_other.append(1)
        else:
            flag_other.append(0)
        if any(str(x).isalpha() for x in Jones.iloc[i]):
            flag_mean.append(1)
        else:
            flag_mean.append(0)     
    df[erroneous_1]= flag_mean
    df[erroneous_2]=flag_other
    r1= df[erroneous_1].sum()
    r2= df[erroneous_2].sum()
    logger.info(summary(erroneous_1, r1, scada))
    logger.info(summary(erroneous_2, r2, scada))


##################################################################
def test_run_all(logger,df,scada):
    #test_linear_interp(logger,df,scada,"linear_interp")
    #test_flat_line(logger,df,scada,"flat_line")
    #test_time_interval(logger,df,scada,"bad_interval")
    test_sdev_mean(logger,df,scada,"stdev>mean")   
    #test_missing_values(logger,df,scada,"mean_missing","other_missing")
    #erroneous(logger,df,scada, 'mean_erroneous','other_erroneous')

##################################################################
#  End of Test
##################################################################


from pyathena import connect
from pyathena.util import as_pandas
from testing import get_logger, get_scada_dataframe, write_scada_dataframe,test_report,read_json_file
import sys
import json
import logging
import pandas as pd
import numpy as np
from test_suite1 import test_run_all as suite1
from test_suite2 import test_run_all as suite2
from test_suite3 import test_run_all as suite3


def athena_get_metric(bucket,schema,plant,turbine,metric):
    cursor = connect(
                 s3_staging_dir='s3://{}/scada/temp'.format(bucket),
                 schema_name=schema,
                 region_name='us-east-1').cursor()
    query ="SELECT *  FROM test_scada_mapped_{} where partition_device='{}' and partition_metric='{}'".format(plant,turbine,metric)
    query ="SELECT timestamp,mean,min,max,count  FROM test_scada_mapped_{} where partition_device='{}' and partition_metric='{}'".format(plant,turbine,metric)
    print query
    cursor.execute(query)
    df = as_pandas(cursor)
    return df

##################################################################
def main (bucket,schema,windfarm,turbine,metric): 
   logger = logging.getLogger()
   df = athena_get_metric(bucket,schema,windfarm,turbine,metric)
   print df
   pd.to_numeric(df['mean'], errors='coerce')
   scada="schema:{} {}.{}.{}".format(schema,windfarm,turbine,metric)
   print(scada)
   print(df)
   pd.to_numeric(df['mean'], errors='coerce')

   # Add Test Suites to Invoke Here
   suite1(logger,df,scada)
   #suite2(logger,df,scada)
   # Upload the test results to the log
   #test_report(logger,bucket,scada,tr_file)
   # Write the dataframe out with the flags
   #write_scada_dataframe(logger,df, bucket, scada, df_file,output_folder)
##################################################################
#  End of Test
##################################################################
# This allows test to run from the command line
if __name__ == "__main__":
   main('sentient-science-customer-test', 'customer-test', 'kcw', 'T02', 'baropress')

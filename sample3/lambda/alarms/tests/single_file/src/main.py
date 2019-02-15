from urllib import unquote
from testing import get_logger, get_alarm_dataframe, get_alarm_dataframe_csv, write_dataframe,test_report,read_json_file
import sys
import json
import logging
import pandas as pd
import numpy as np
import traceback
from test_suite1 import test_run_all as suite1
##################################################################
def main (logger,tester, bucket, key, df_file,tr_file,output_folder): 

   #  Load alarm data  into a data frame
   df = get_alarm_dataframe(logger,bucket,key) # from S3
   logger.debug("loaded:" + key)

   #alarm_codes = read_s3_csv(bucket,"reference/alarm_codes.csv")
   #TODO: read alarm_codes from S3
   alarm_codes = ["87", "146", "166", "181", "182", "183", "184", "221", "314", "363"]
   logger.debug("loaded alarm codes")

   #device_types = read_s3_csv(bucket,"reference/device_types.csv")
   #TODO: read device types from S3
   device_types=["GE 1.6 ESS-XLE"]
   logger.debug("loaded device types")

   # Add Test Suites to Invoke Here
   suite1(tester,df,key,alarm_codes,device_types)
   logger.debug("ran test suites")

   # Upload the test results to the log
   test_report(logger,bucket,key,tr_file)
   logger.debug("wrote log")

   # Write the dataframe out with the flags
   write_dataframe(logger,df, bucket, key, df_file,output_folder)
   logger.debug("wrote dataframe")
##################################################################
#  End of Test
##################################################################
# This allows the test to run as a lambda function
def lambda_handler(event, context, dir="/tmp"):
    logger = logging.getLogger()
    try:
        key = unquote(event["Records"][0]["s3"]["object"]["key"])
        bucket = event["Records"][0]["s3"]["bucket"]["name"]
        df_file=dir+"/test_rows.csv"
        tr_file=dir+"/test_summary.csv"
        tester = get_logger(tr_file,key)
        results=key.replace("organized","tested")
        results=key.replace("mapped","tested")
        main(logger,tester,bucket,key, df_file, tr_file,results)
        return 'Success'
    except Exception as e:
        logger.error("Error during the processing input={}: {}".format(input, traceback.format_exc(e)))
        return 'Error'

# This allows test to run from the command line
if __name__ == "__main__":
   event = read_json_file(sys.argv[2])
   context={}
   lambda_handler(event,context,"tmp")

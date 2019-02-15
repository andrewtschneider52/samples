from library import get_logger, get_scada_dataframe, write_scada_dataframe,test_report
import sys
import logging
import pandas as pd
import numpy as np
from test_suite1 import test_run_all as suite1
##################################################################
def main (logger,tester, bucket, scada, df_file,tr_file,output_folder): 
   #  Load scada data  into a data frame
   df = get_scada_dataframe(logger,bucket,scada)

   # Add Test Suites to Invoke Here
   suite1(tester,df,scada)
   #suite2(tester,df,scada)
   #suite3(tester,df,scada)
   #  etc.

   # Upload the test results to the log
   test_report(logger,bucket,scada,tr_file)
   # Write the dataframe out with the flags
   write_scada_dataframe(logger,df, bucket, scada, df_file,output_folder)
##################################################################
#  End of Test
##################################################################

# This allows the test to run as a lambda function
def lambda_handler(event, context):
    from urllib import unquote
    logger = logging.getLogger()
    try:
        scada = unquote(event["Records"][0]["s3"]["object"]["key"])
        bucket = event["Records"][0]["s3"]["bucket"]["name"]
        df_file="/tmp/test_rows.csv"
        tr_file="/tmp/test_summary.csv"
        tester = get_logger(tr_file,scada)
        results=scada.replace("mapped","tested")
        main(logger,tester,bucket,scada, df_file, tr_file,results)
        return 'Ok'
    except Exception as e:
        #logger.error("Error during the processing input={}: {}".format(input, traceback.format_exc(e)))
        logger.error("Error during the processing input={}:".format(input))
        logger.error("Error during the processing input={}:".format(e))
        return 'Error'

# This allows test to run from the command line
if __name__ == "__main__":
   bucket=sys.argv[2]
   scada=sys.argv[3]
   results=scada.replace("mapped","tested")
   df_file="tmp/test_rows.csv"
   tr_file="tmp/test_summary.csv"
   logger = get_logger("tmp/test.log",scada)
   tester = get_logger(tr_file,scada)
   main(logger,tester,bucket,scada, df_file, tr_file,results)

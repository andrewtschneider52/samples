#from library import get_logger, get_scada_dataframe, write_scada_dataframe,test_report,read_json_file
from testing  import get_logger, get_scada_dataframe, write_scada_dataframe,test_report,read_json_file
import sys
import json
import logging
import pandas as pd
import numpy as np
from test_suite1 import test_run_all as suite1
from test_suite2 import test_run_all as suite2
from test_suite3 import test_run_all as suite3
##################################################################
def main (logger,tester, bucket, scada, df_file,tr_file,output_folder): 
   #  Load scada data  into a data frame
   df = get_scada_dataframe(logger,bucket,scada)
   print (df)

   # Add Test Suites to Invoke Here
   suite1(tester,df,scada)
   suite2(tester,df,scada)
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
def lambda_handler(event, context, dir="/tmp"):
    from urllib import unquote
    logger = logging.getLogger()
    try:
        scada = unquote(event["Records"][0]["s3"]["object"]["key"])
        bucket = event["Records"][0]["s3"]["bucket"]["name"]
        df_file=dir+"/test_rows.csv"
        tr_file=dir+"/test_summary.csv"
        tester = get_logger(tr_file,scada)
        results=scada.replace("mapped","tested")
        main(logger,tester,bucket,scada, df_file, tr_file,results)
        temp = { u"Records": [{"s3": { 
           u"bucket": {u"name": bucket}, 
           u"object": {u"key": results } }}]}
        logger.debug(json.dumps(temp))
        return temp
    except Exception as e:
        #logger.error("Error during the processing input={}: {}".format(input, traceback.format_exc(e)))
        logger.error("Error during the processing input={}:".format(input))
        logger.error("Error during the processing input={}:".format(e))
        return 'Error'

# This allows test to run from the command line
if __name__ == "__main__":
   event = read_json_file(sys.argv[2])
   context={}
   lambda_handler(event,context,"tmp")

import io
import os
import pandas as pd
import sys
import boto3
import io
import bz2
import logging
from datetime import datetime

def get_logger(log_path,file_name): 
    logger = logging.getLogger(log_path)
    logger.setLevel(logging.DEBUG)
    format_str = u"%(asctime)s,%(levelname)s,%(funcName)s,%(message)s"
    date_str='%d-%m-%Y %H:%M:%S'
    logging.basicConfig(format=format_str,datefmt=date_str)
    handler = logging.FileHandler(log_path)
    formatter = logging.Formatter(format_str)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger

def get_base( filename) :
    return filename[0:-8]

def get_last( filename) :
    dirs = filename.split("/")
    size = len(dirs)
    p1=dirs[size-4]
    p2=dirs[size-3]
    p3=dirs[size-2]
    p4=dirs[size-1]
    sub_path="{}/{}/{}".format(p1,p2,p3)
    return sub_path


def get_output_filename( filename) :
    base=get_base(filename)
    head = base.replace("translated","tested")
    csv = "{}.csv".format(head)
    return csv

def get_results_filename( filename) :
    last=get_last(get_base(filename))
    csv = "logs/{}.csv".format(last)
    return csv

def get_test_name(filename):
   test_name=os.path.basename(filename)[0:-3]
   return test_name

def get_file_type(filename):
   test_name=os.path.basename(filename)[-3:-1]
   return test_name

def get_scada_columns():
    names=["farm","turbine","tag","ts","avg","min","max","stdev","count"]
    return names

def read_s3_csv_bz2(logger, bucket,filename):
    foo= bz2.BZ2Decompressor()
    result=foo.decompress(read_s3_csv(logger, bucket, filename))
    return result

def read_s3_csv(logger, bucket,filename):
    s3 = boto3.resource('s3')
    result=s3.Object(bucket, filename).get()["Body"].read()
    return result

def get_scada_dataframe(logger, bucket,filename): 
   file_type=get_file_type(filename)
   if (file_type == "bz"):
      result=read_s3_csv_bz2(logger, bucket,filename)
   if (file_type == "cs"):
      result=read_s3_csv(logger, bucket,filename)
   df = pd.read_csv(io.BytesIO(result), names=get_scada_columns(), header=1,delimiter=",")
   return df

def test_report(logger,bucket,scada,tr_file):
   s3 = boto3.resource('s3')
   s3_tr_file = get_results_filename(scada)
   s3.Bucket(bucket).upload_file(tr_file, s3_tr_file)

def write_scada_dataframe(logger, df, bucket,scada,temp,output_folder):
   s3 = boto3.resource('s3')
   df.to_csv(temp)
   test_results = get_output_filename(output_folder)
   s3.Bucket(bucket).upload_file(temp, test_results) 

def summary(flag,value,scada):
    return "{},{},{}".format(flag,value,scada)

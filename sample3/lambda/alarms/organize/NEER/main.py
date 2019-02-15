#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Mon Apr 23 16:46:00 2018
NextEra's Interval Format to Sentient Science's standard format

@author: miguelcaballero
"""
# Third-party libraries
import boto3
import pandas as pd

# Built-in libraries
import os
import gzip
import math
import time
import datetime

# decorator to print the executing time of a function/method
def my_timer(orig_func):
    def wrapper(*args, **kwargs):
        t1 = time.time()
        result = orig_func(*args, **kwargs)
        t2 = time.time() - t1
        print('{} ran in: {} sec'.format(orig_func.__name__, t2))
        return result
    return wrapper

# Function that return a list of files unprocessed
def list_files_bucket(bucket):
    s3 = boto3.client('s3')
    keys = []
    resp = s3.list_objects_v2(Bucket=bucket)
    for obj in resp['Contents']:
        if obj['Key'].startswith('alarms/Fault'):
            keys.append(obj['Key'])
    return keys

def download_s3_gz(bucket,filename):
    s3 = boto3.client('s3')
    s3.download_file(bucket, filename, '/tmp/file.gz')

def unzip_csv_gz(bucket,filename):
    download_s3_gz(bucket,filename)
    with gzip.open('/tmp/file.gz','rb') as f:
        file_content=f.read()
    csvfile = open('/tmp/file.csv','wb')
    csvfile.write(file_content)
    csvfile.close() 

def get_scada_columns():
    names=['PLANTNAME','TURBINETYPENAME','TURBINENAME','PLANTSK','TURBINESK','EVENTSK','STARTUTCDATETIME','ENDUTCDATETIME','LOADUTCDATETIME','SOURCEFAULTCODE']
    return names

def get_alarms_dataframe(bucket,filename): 
    unzip_csv_gz(bucket,filename)
    df = pd.read_csv('/tmp/file.csv', low_memory=False)
    df = df[get_scada_columns()]
    os.remove('/tmp/file.csv')
    os.remove('/tmp/file.gz')
    return df

# Function that split files from customer in CSV files per wind farm
def split_windFarm(bucket, scadaL):
    for counter, scada in enumerate(scadaL):
        print(scada)

        # Get date of file
        date = [int(x) for x in scada.split('.')[0].split('-')[1:]]
        date.reverse()
        date = datetime.datetime(*date).isoformat()

        file_in = get_alarms_dataframe(bucket,scada)
        WindFarmsList = file_in.PLANTNAME.unique()
        header = True if counter == 0 else False
            
        for WF_temp in WindFarmsList:
                outWindFarmTemp = file_in[file_in["PLANTNAME"] == WF_temp]
                fname=WF_temp+"."+date+".data.csv"
                fname=fname.replace(" ", "_")
                fname = os.path.join('/tmp/',fname)
                
                if os.path.isfile(fname):
                    header = True
                
                with open(fname, 'a') as f:
                    outWindFarmTemp.to_csv(f, header=header)
        del file_in

# Function split logs for a single wind-farm into logs for each individual turbine       
def wind_turbines_files(bucket, path):
    csv_files = os.listdir('/tmp')
    csv_files = [k for k in csv_files if '.csv' in k]
    
    for File in csv_files:
        alarms = pd.read_csv('/tmp/' + File, low_memory=False)
        unq_alarms = alarms.SOURCEFAULTCODE.unique()
        
        # Quality control to resolve input data inconsistencies
        if unq_alarms.astype('str').dtype.kind == 'U':
            print("\nAlarms have embedded strings instead of just numbers - resolving")
            a22 = list(map(float, unq_alarms.astype('str')))
            a22 = [-9999.0 if math.isnan(x) else x for x in a22]
            results2 = list(map(int,a22))
            unq_alarms = results2
            del results2
        
        df = pd.DataFrame(sorted(unq_alarms), columns=["alarms"])
    
        wndTurbL = alarms['TURBINENAME'].unique()
        # with open('text.txt', 'a') as f:

        CurrProj = File.replace('.data.csv','')
        CurrProj, date = CurrProj.split('.')
        # fname = CurrProj + '_' + 'AlarmList.csv'
        # fout = 'alarm_list/' + fname
        # df.to_csv(fout, header=False) 
        
        # Get rid of some variables
        del df
        # del fname
        # del fout
        # with open('text.txt', 'a') as f:
        #     f.write(CurrProj)
        # continue
        
        # Assign new paths for I/O	
        new_path = path + CurrProj + "/"
        # os.mkdir(new_path)
        for wndTurb in wndTurbL:
            alarm_tmp = alarms[alarms["TURBINENAME"] == wndTurb]
            out_alarm = alarm_tmp[['PLANTNAME','TURBINENAME','STARTUTCDATETIME','ENDUTCDATETIME','LOADUTCDATETIME','SOURCEFAULTCODE']]
            out_alarm.columns = ['plant', 'device', 'start_time', 'close_time', 'load_time', 'code']
            type_wndTurb = alarm_tmp['TURBINETYPENAME'].unique()
            type_wndTurb = type_wndTurb.astype('str')[0]

            # file_name=str(wndTurb)+"_#_"+type_wndTurb#+"_data.csv"
            file_name = '.'.join([CurrProj, str(wndTurb), date])
    		# apply some standardizing conversions to filenames to allow easy identification/parsing
            # file_name=file_name.replace(" ", "_")
            # file_name=file_name.replace(".", "_")
            file_name = file_name+".csv"
            fout_local = '/tmp/'+file_name
            fout_s3 = new_path+str(wndTurb)+'/'+file_name

            # print fout_s3

            #Write out file locally
            with open(fout_local, 'a') as f:
                out_alarm.to_csv(f, header=True, index=False)

            # Write output file to S3
            s3 = boto3.resource('s3')
            s3.Bucket(bucket).upload_file(fout_local, fout_s3, Config=boto3.s3.transfer.TransferConfig())
            # s3.Bucket('sentient-science-customer-nextera').upload_file(fout_local, fout_s3, Config=boto3.s3.transfer.TransferConfig())

            # Remove Local file from tmp space
            os.remove(fout_local)
        os.remove('/tmp/' + File)

# Steps to transform customer logs format into sentient format  
@ my_timer
def main(bucket, file, path='csv_files/'):
    # List of files to process
    # scadaL = list_files_bucket(bucket)
    # scadaL = [k for k in scadaL if '.gz' in k]
    # Split data in wind farms
    split_windFarm(bucket, [file])
    # Split data in wind-turbines
    # path = 'csv_files/'
    wind_turbines_files(bucket, path)
    # move files processed to a folder

def lambda_handler(event, context):
    from  urllib import unquote
    _key = unquote(event["Records"][0]["s3"]["object"]["key"])
    _bucket = event["Records"][0]["s3"]["bucket"]["name"]
    main(_bucket, _key, 'alarms/organized/')

# if __name__ == "__main__":
#     bucket = 'sentient-science-customer-neer'
#     main(bucket, file='alarms/Fault-01-01-2018.csv.gz', path='alarms/organized/')
    
    
    
    
    
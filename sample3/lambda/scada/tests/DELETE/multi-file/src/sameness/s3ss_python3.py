# -*- coding: utf-8 -*-

"""Tools for interacting with Sentient S3.
This module contains methods for basic interaction with S3 buckets.
Example:
    Import the function to add a new file or folder to an existing S3 bucket
    
        from s3ss import create_object
        
Attributes:
    This module contains no module level attributes
Todo:
    * ...
"""

import os
import sys

import pandas as pd
import requests
import boto3
import botocore

import boto
import boto.s3
from boto.s3.key import Key

def percent_cb(complete, total):
    sys.stdout.write('.')
    sys.stdout.flush()
    
def upload_file(bucket, key, file=''):
    
    '''
    use: upload_file('bucket-name', 'desired/file/path/name', 
                       'your .aws/config profile', 'path/to/file/to/upload')
    '''
    
    s3_connection = boto.connect_s3(os.getenv('AWS_ACCESS_KEY_ID'),
                                    os.getenv('AWS_SECRET_ACCESS_KEY'))
    
    s3_bucket = s3_connection.get_bucket(bucket, validate=False)
    
    s3_key = s3_bucket.get_key(key, validate=False)

    if s3_key == None:
    
        s3_key = s3_bucket.new_key(key)
        
    with open(file, 'rb') as f:
        
        s3_key.set_contents_from_file(f)
    
    print('Added new object', 's3://' + bucket + ':' + key)

def get_save_object(bucket, key, outfile, aws_cred):

    newdir = '/'.join(outfile.split('/')[:-1])
    
    if not os.path.exists(newdir): os.makedirs(newdir)

    session = boto3.Session(profile_name=aws_cred)

    s3res = session.resource('s3')

    bucket = s3res.Bucket(bucket)

    bucket.download_file(Key=key, Filename=outfile)

def delete_bucket(bucket_name, aws_cred):
    
    session = boto3.Session(profile_name=aws_cred)
    
    s3res = session.resource('s3')
    
    bucket = s3res.Bucket(bucket_name)
    
    bucket.objects.all().delete()

    bucket.delete()
    
    print('Deleted bucket', 's3://' + bucket_name)
    
def find_or_create_new_customer(customer_string, aws_cred, test_mode=False):
    
    new_bucket_name = 'sentient-science-customer-' + customer_string
    
    series = get_series()
    
    segments = get_segment_sites()
    
    if customer_string not in segments:
        
        print('Fatal Error: $s not in reference data here: &s' % \
        (customer_string, 'https://raw.githubusercontent.com/sentientscience/' \
         + 'analytics/master/resources/1_datamgt/names/site_segments.csv'))
        
        return
        
    new_customer_id = segments[customer_string]
    
    relevant_series = series[series.segment_site == new_customer_id][['scada_plant', 'scada_device']]
    
    plants = relevant_series.groupby(['scada_plant'])
    
    if test_mode == True:
        
        delete_bucket(new_bucket_name)
    
    session = boto3.Session(profile_name=aws_cred)
    
    s3client = session.client('s3')
    
    s3res = session.resource('s3')
    
    existing_buckets = s3client.list_buckets()
    
    relevant_buckets = [obj for obj in existing_buckets['Buckets'] if obj['Name'] == new_bucket_name]
    
    if len(relevant_buckets) == 0:
        # add new bucket
        print('Added new bucket', new_bucket_name)
        
        s3res.create_bucket(Bucket=new_bucket_name)
        
        create_object(new_bucket_name, 'scada', aws_cred)
        
        create_object(new_bucket_name, 'scada/raw', aws_cred)
        
        create_object(new_bucket_name, 'scada/encoded', aws_cred)
        
        create_object(new_bucket_name, 'scada/csvinated', aws_cred)
        
        create_object(new_bucket_name, 'scada/organized', aws_cred)
        
        create_object(new_bucket_name, 'scada/translated', aws_cred)
        
        create_object(new_bucket_name, 'scada/recordtested', aws_cred)
        
        create_object(new_bucket_name, 'scada/settested', aws_cred)
        
        create_object(new_bucket_name, 'scada/final', aws_cred)
        
        create_object(new_bucket_name, 'reference', aws_cred)
        
        create_object(new_bucket_name, 'reference/metric_map', aws_cred)
        
        create_object(new_bucket_name, 'windfarms', aws_cred)
        
        for plant, df in plants:
            
            create_object(new_bucket_name, 'scada/number-facts/' + plant, aws_cred)
            
            create_object(new_bucket_name, 'scada/csv/' + plant, aws_cred)
            
            for idx, row in df.iterrows():
                
                create_object(new_bucket_name, 'scada/number-facts/' +  plant + '/' + row['scada_device'], aws_cred)
    else:
        
        print('Found existing bucket, using it', new_bucket_name)

def get_matching_s3_objects(bucket, aws_cred, prefix='', suffix=''):
    """
    List objects in an S3 bucket.
    :param bucket: Name of the S3 bucket.
    :param prefix: Only fetch objects whose key starts with
        this prefix (optional).
    :param suffix: Only fetch objects whose keys end with
        this suffix (optional).
    """
    session = boto3.Session(profile_name=aws_cred)
    
    s3client = session.client('s3')
    
    kwargs = {'Bucket': bucket}

    # If the prefix is a single string (not a tuple of strings), we can
    # do the filtering directly in the S3 API.
    if isinstance(prefix, str):
        
        kwargs['Prefix'] = prefix

    while True:

        # The S3 API response is a large blob of metadata.
        # 'Contents' contains information about the listed objects.
        resp = s3client.list_objects_v2(**kwargs)

        try:
            contents = resp['Contents']
        except KeyError:
            return

        for obj in contents:
            
            key = obj['Key']
            
            if key.startswith(prefix) and key.endswith(suffix):
                
                yield obj

        # The S3 API is paginated, returning up to 1000 keys at a time.
        # Pass the continuation token into the next response, until we
        # reach the final page (when this field is missing).
        try:
            kwargs['ContinuationToken'] = resp['NextContinuationToken']
        
        except KeyError:
            
            break

def _rawcsv_to_dataframe(raw, sep='\r\n', header=True):
    
    '''
    Takes in a string that is a comma separated blob
    Returns a pandas dataframe
    '''

    raw = [row.split(',') for row in raw.split(sep)]
    
    if header: 
        header = raw[0]
        data = raw[1:]

    else: 
        data = raw

    return pd.DataFrame(data, columns=header)

    
def get_series():

    url = "https://raw.githubusercontent.com/sentientscience/analytics/master/resources/1_datamgt/names/series.csv"

    querystring = {"token":"AAcBnbRmyCjrJ5G7W1pKhA0UaShNvJxOks5aqVzqwA%3D%3D"}

    headers = {
        'Authorization': "Basic c29lbGxpbmdlcmFqOjg2N2JjNzA2ZGNhNjM5OGE5NGU3MmYyMTc1ZWQxMmRlMTYzNTFjY2E=",
        'Cache-Control': "no-cache",
        'Postman-Token': "b1ccfb4d-a5f7-406f-8286-558d0a98ee66"
        }

    response = requests.request("GET", url, headers=headers, params=querystring)

    series = _rawcsv_to_dataframe(response.content)
    
    return series

def get_segment_sites():

    url = "https://raw.githubusercontent.com/sentientscience/analytics/master/resources/1_datamgt/names/site_segments.csv"

    querystring = {"token":"AAcBnZ9bA-v3JBWNVE3rzL1SdMdEzjOwks5aqWH5wA%3D%3D"}

    headers = {
        'Authorization': "Basic c29lbGxpbmdlcmFqOjg2N2JjNzA2ZGNhNjM5OGE5NGU3MmYyMTc1ZWQxMmRlMTYzNTFjY2E=",
        'Cache-Control': "no-cache",
        'Postman-Token': "b47be88f-9cc7-462b-940e-53d37016712b"
        }

    response = requests.request("GET", url, headers=headers, params=querystring)
    
    segment_sites = _rawcsv_to_dataframe(response.content.replace('\xef\xbb\xbf', ''))
    
    segment_sites = {obj['filename']: obj['segment_site'] for obj in segment_sites.to_dict('records')}
    
    return segment_sites
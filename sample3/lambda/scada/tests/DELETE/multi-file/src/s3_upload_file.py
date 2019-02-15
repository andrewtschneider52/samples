# -*- coding: utf-8 -*-

"""A script that calls to s3ss.upload_file from the CLI

This module is meant to access s3ss.upload_file

Example:
    
    sudo python ~/code/data-engineering-etl/lib/s3_upload_file.py bucket key file
        
Attributes:


Todo:

"""
import os
import sys
import argparse
sys.path.append(os.path.expanduser('~') + '/code/data-engineering-etl/lib/')
from s3ss_python3 import upload_file, get_save_object

def main(args):

    # Get CLI params
    bucket = args.b
    out_key = args.k
    localfile_out = args.f
    out_fn = localfile_out.split('/')[-1]
    
    upload_file(bucket=bucket, key=out_key + '/' + out_fn,
                file=localfile_out)

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Python argparser for S3')
    parser.add_argument('f', type=str, help='file path to local file')
    parser.add_argument('b', type=str, help='s3 bucket')
    parser.add_argument('k', type=str, help='s3 base directory for output file')
    
    main(parser.parse_args())

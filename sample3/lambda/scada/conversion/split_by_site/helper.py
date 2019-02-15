import boto3
from datetime import datetime
s3 = boto3.resource("s3")
def open_s3_file(bucket, key):
    tmp_filename = "/tmp/{}".format(key.split('/')[:-1])
    with open(tmp_filename, 'wb') as data:
        s3.Object(bucket, key).download_fileobj(data)
    return open(tmp_filename)

def write_s3_file(bucket, key, content):
    tmp_filename = "/tmp/{}".format(key.split('/')[:-1])
    with open(tmp_filename, 'wb') as data:
        data.write(content)
    s3.Object(bucket, key).upload_file(tmp_filename)

def get_wf_asset(string, sep_list=['.','_',',']):
    if len(string) != 0:
        for i in sep_list:
            if i in string:
                string = string.split(i)
                string = ".".join(string)
    else:
        string = string
    return string


def get_metric(string, suffix_list=['mean', 'max', 'min', 'sdev', 'stdev' 'count']):
    result = []
    if len(string) != 0:
        for stat in suffix_list:
            if string.endswith(stat):
                result = list(string.rpartition(stat)[:2])
                # string= string.rsplit(stat)
                # string = string[0]
    return result

def get_iso_timestamp(customer_timestamp, customer_date_format="%m/%d/%Y %H:%M"):
    d= datetime.strptime(customer_timestamp, customer_date_format)
    d= d.isoformat()
    return d
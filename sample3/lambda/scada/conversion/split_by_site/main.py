import boto3
import helper
import json
import time

def split_by_site(bucket, key):
    site = ""
    start_date = ""
    end_date = ""
    buffer = ""
    with helper.open_s3_file(bucket, key) as s3_file:
        for line in s3_file:
            if "Site Name: " in line:
                if buffer != "":
                    # Send S3 file
                    # new_filename = "{}_{}_{}.csv".format(site, start_date, end_date)
                    new_filename = "tmp/organized/{plant}/{plant}.{start_date}.{end_date}.csv".format(plant=site, start_date=start_date, end_date=end_date)
                    helper.write_s3_file(bucket, new_filename, buffer)
                    # with open(new_filename, "wb") as new_file:
                    #     new_file.write(buffer)
                    # Invoke next step
                    boto3.client("lambda").invoke(
                        FunctionName = "ingestigator-tf-split_by_turbine",
                        InvocationType = "Event", # For testing purposes
                        Payload=json.dumps({
                            "bucket": bucket,
                            "key": new_filename
                        })
                    )
                    # Clean out buffer
                    buffer = ""
                line_array = [e.strip() for e in line.split(",")]
                site = line_array[0].replace("Site Name: ", "")
                start_date = helper.get_iso_timestamp(line_array[1].replace("Start Date: ", ""), "%m/%d/%Y")
                end_date = helper.get_iso_timestamp(line_array[2].replace("End Date: ", ""), "%m/%d/%Y")
            else:
                buffer = "\n".join([buffer, line.strip()]).strip()
    # Send S3 file in buffer

def main(bucket, key):
    start = time.time()
    split_by_site(bucket, key)
    end = time.time()
    print  "Took {}s".format(end - start)

def lambda_handler(event, context):
    _bucket = event["Records"][0]["s3"]["bucket"]["name"]
    _key = event["Records"][0]["s3"]["object"]["key"]
    split_by_site(_bucket, _key)

# if __name__ == "__main__":
#     bucket= 'sentient-science-customer-duke'
#     key= 'scada/raw/ScadaData_daily_20180312_233348.csv'
#     main(bucket, key)
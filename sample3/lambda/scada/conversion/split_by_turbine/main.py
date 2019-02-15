import helper
import boto3
import json

def split_by_turbine(bucket, key):
    site, start_date, end_date = key.split('/')[-1].split(".")[:3]
    # start_date = helper.get_iso_timestamp(start_date, "%m.%d.%Y")
    # end_date = helper.get_iso_timestamp(end_date, "%m.%d.%Y"))
    buffer = ""
    turbine_data = {}
    turbine_columns = []
    # with open(key) as s3_file: # Currently is a local file.
    with helper.open_s3_file(bucket, key) as s3_file: # Currently is a local file.
        for i, line in enumerate(s3_file):
            row = line.split(",")
            if i == 0: # First line
                # Set up data structures for organizing data by turbine
                turbine_columns = line.split(",")
                for i, col in enumerate(turbine_columns):
                    if col.strip():
                        turbine_columns[i] = helper.get_wf_asset(col).split(".")[1]
                        if not turbine_columns[i].startswith("Met"):
                            # Initialize Turbine's data
                            # Met columns are not seperate turbines
                            turbine_data[turbine_columns[i]] = ""
            for key in turbine_data:
                indices = [i for i, x in enumerate(turbine_columns) if x == key]
                met_indice = [i for i, x in enumerate(turbine_columns) if x.startswith("Met")][0]
                indices.append(met_indice)
                buffer = ",".join([row[0]] + [row[i] for i in indices])
                turbine_data[key] = "\n".join([turbine_data[key], buffer.strip()]).strip()
    for turbine in turbine_data:
        # filename = "turbines/{plant}.{device}.{start_date}.{end_date}.csv".format(plant=site, device=turbine, start_date=start_date, end_date=end_date)
        filename = "tmp/organized/{plant}/{device}/{plant}.{device}.{start_date}.{end_date}.csv".format(plant=site, device=turbine, start_date=start_date, end_date=end_date)
        # Load file to S3
        helper.write_s3_file(bucket, filename, turbine_data[turbine])
        # with open(filename, "wb") as new_file:
        #     new_file.write(turbine_data[turbine])
        # Invoke next Lambda function
        boto3.client("lambda").invoke(
            FunctionName = "ingestigator-tf-split_by_metric",
            InvocationType = "Event",
            Payload = json.dumps({
                "bucket": bucket,
                "key": filename
            })
        )

def lambda_handler(event, context):
    bucket = event["bucket"]
    # bucket = event['Records'][0]['s3']['bucket']['name']
    key = event["key"]
    # key = event["Records"][0]["s3"]["object"]["key"]
    split_by_turbine(bucket, key)
    helper.s3.Object(bucket, key).delete()

def main(bucket, key):
    split_by_turbine(bucket, key)

# if __name__ == "__main__":
#     bucket = "sentient-science-customer-duke"
#     key = "../test_data/sites/CampbellHill.2018-03-11T00:00:00.2018-03-12T00:00:00.csv"
#     main(bucket, key)
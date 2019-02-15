import helper

def split_by_metric(bucket, key):
    # site, turbine, start_date, end_date = key.replace(".csv", "").split("_")
    site, turbine, start_date, end_date = key.split('/')[-1].split(".")[:4]
    # site = site.lstrip("turbines/")
    # start_date = helper.get_iso_timestamp(start_date, "%m.%d.%Y")
    # end_date = helper.get_iso_timestamp(end_date, "%m.%d.%Y")
    buffer = ""
    metric_data = {}
    metric_columns = []
    # with open(key) as s3_file: # Currently is a local file.
    with helper.open_s3_file(bucket,key) as s3_file: # S3 File
        for i, line in enumerate(s3_file):
            row = line.strip().split(",")
            if i == 0:
                continue
            elif i == 1: # Second line
                # Set up data structures for organizing data by metric
                """
                Example Structure:
                    metric_columns = [
                        [<metric>, <stat>]
                        [yawpos, mean],
                        [yawpos, max],
                        [yawpos, min],
                        [realpower, mean],
                        [realpower, max],
                        etc.
                    ]
                    metric_data = {
                        "yawpos": [
                            [plant, device, metric, timestamp, <stats>],
                            [CampbellHill, T01, yawpos, 3/11/2018 0:00:00, <value of stat>]
                        ],
                        "realpower": ...
                    }
                """
                metric_columns = line.split(",")
                for i, col in enumerate(metric_columns):
                    col = col.strip()
                    if i != 0 and col != "":
                        # Start from 2nd column since first column is Timestamp
                        metric_columns[i] = helper.get_metric(col) # Returns a list: [metric, stat]
                        metric, stat = metric_columns[i]
                        if metric not in metric_data.keys():
                            # Initialize metric
                            metric_data[metric] = [["plant","device","metric","timestamp","mean", "min", "max", "stdev", "count"]]

            else:
                timestamp = helper.get_iso_timestamp(row[0])
                # key is a metric
                for key in metric_data:
                    headers = metric_data[key][0][4:]
                    default_obj = {x: "" for i, x in enumerate(headers)}
                    value_obj = {x[1]: row[i] for i, x in enumerate(metric_columns) if x[0] == key}
                    default_obj.update(value_obj)
                    
                    values = [default_obj[stat] for i, stat in enumerate(headers)]
                    buffer = [site,turbine,key,timestamp] + values
                    metric_data[key].append(buffer)
                    # print metric_data[key]
            # break
    for key in metric_data:
        # filename = "metrics/{}_{}_{}_{}_{}.csv".format(site, turbine, key, start_date, end_date)
        filename = "scada/organized/{plant}/{device}/{metric}/{plant}.{device}.{metric}.{start_date}.{end_date}.csv".format(plant=site, device=turbine, metric=key, start_date=start_date, end_date=end_date)
        filecontent = "\n".join([ ",".join(x) for x in metric_data[key]])
        # Load file to S3
        helper.write_s3_file(bucket, filename, filecontent)
        # with open(filename, "wb") as new_file:
        #     new_file.write(filecontent)

def lambda_handler(event, context):
    # bucket = event['Records'][0]['s3']['bucket']['name']
    # key = event["Records"][0]["s3"]["object"]["key"]
    bucket = event["bucket"]
    # bucket = event['Records'][0]['s3']['bucket']['name']
    key = event["key"]
    # key = event["Records"][0]["s3"]["object"]["key"]
    split_by_metric(bucket, key)
    helper.s3.Object(bucket, key).delete()    

def main(bucket, key):
    split_by_metric(bucket, key)

# if __name__ == "__main__":
#     bucket = "sentient-science-customer-duke"
#     key = "../test_data/turbines/CampbellHill.T01.2018-03-11T00:00:00.2018-03-12T00:00:00.csv"
#     main(bucket, key)
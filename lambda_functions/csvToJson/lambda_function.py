import csv
import json
from datetime import datetime
from io import TextIOWrapper

import boto3

now = datetime.now()
date_time = now.strftime("%Y-%m-%d %H:%M:%S.%f")
TZS = 'GMT+11'

s3_client = boto3.client('s3')

UPLOAD_PREFIX = "processedJSON/"
UPLOAD_FILENAME = "environmental_risk"


def lambda_handler(event, context):
    try:
        print("Starting Conversion of CSV to JSON and store in",
              UPLOAD_PREFIX)
        print("Event received:", json.dumps(event))

        # Retrieves the bucketname and the file
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']

        print(f"Checking if files exist in {UPLOAD_PREFIX}")

        existing_files = s3_client.list_objects_v2(
            Bucket=bucket,
            Prefix=UPLOAD_PREFIX)

        if 'Contents' in existing_files:
            for obj in existing_files['Contents']:
                print(f"Deleting existing file: {obj['Key']}")
                s3_client.delete_object(Bucket=bucket, Key=obj['Key'])
        else:
            print("No files found in bucket")

        response = s3_client.get_object(Bucket=bucket, Key=key)
        data_to_string = TextIOWrapper(response['Body'], encoding='utf-8')

        # Convert to standarised JSON format
        json_header = {
                "data_source": "ClarityAI_Dataset",
                "dataset_type": "Environmental_Risk",
                "dataset_id": "https://ap-southeast-2.console.aws.amazon.com" +
                "/s3/buckets/sierrabucket2025?region=ap-southeast-2&bucketTy" +
                "pe=general&tab=objects",
                "time_object": {"timestamp": date_time, "timezone": TZS},
                "events": []
            }

        reader = csv.DictReader(data_to_string, delimiter=',')
        for row in reader:
            timestamp_row = row['reported_date']
            company_name = row['company_name']
            perm_id = row['perm_id']
            data_type = row['data_type']
            disclosure = row['disclosure']
            metric_description = row['metric_description']
            metric_name = row['metric_name']
            metric_unit = row['metric_unit']
            metric_value = row['metric_value']
            metric_year = row['metric_year']
            nb_points_of_observations = row['nb_points_of_observations']
            metric_period = row['metric_period']
            provider_name = row['provider_name']
            pillar = row['pillar']
            headquarter_country = row['headquarter_country']

            attributes = {
                "company_name": company_name,
                "perm_id": perm_id,
                "data_type": data_type,
                "disclosure": disclosure,
                "metric_description": metric_description,
                "metric_name": metric_name,
                "metric_unit": metric_unit,
                "metric_value": metric_value,
                "metric_year": metric_year,
                "nb_points_of_observations": nb_points_of_observations,
                "metric_period": metric_period,
                "provider_name": provider_name,
                "pillar": pillar,
                "headquarter_country": headquarter_country
                }

            event = {
                "time_object": {
                    "timestamp": timestamp_row,
                    "duration": 1,
                    "duration_unit": "day",
                    "timezone": TZS
                },
                "event_type": "ESG data",
                "attribute": attributes
            }
            json_header["events"].append(event)

        # uploads the converted json file to ProcessedJSONFolder
        s3_client.put_object(
            Bucket=bucket,
            Key=f"{UPLOAD_PREFIX}{UPLOAD_FILENAME}.json",
            Body=json.dumps(json_header, indent=4)
        )

        # Acknowledge completion of conversion
        print("Conversion of CSV to JSON completed.")

    except Exception as e:
        # Log error details
        print(f"Error: {str(e)}")
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}

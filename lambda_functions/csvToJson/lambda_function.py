import csv
import json
from datetime import datetime
from io import TextIOWrapper

import boto3

now = datetime.now()
date_time = now.strftime("%Y-%m-%d %H:%M:%S.%f")
TZS = 'GMT+11'

s3Client = boto3.client('s3')


def lambda_handler(event, context):
    # Retrieves the bucketname and the file
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']

    response = s3Client.get_object(Bucket=bucket, Key=key)
    data_to_string = TextIOWrapper(response['Body'], encoding='utf-8')

    # Code to convert to standarised JSON format
    json_header = {
            "data_source": "ClarityAI_Dataset",
            "dataset_type": "Environmental_Risk",
            "dataset_id": "https://ap-southeast-2.console.aws.amazon.com" +
            "/s3/buckets/sierrabucket2025?region=ap-southeast-2&bucketTy" +
            "pe=general&tab=objects",
            "time_object": {"timestamp": date_time, "timezone": TZS},
            "events": []
        }

    reader = csv.DictReader(data_to_string, delimiter='|')
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
    s3Client.put_object(
        Bucket=bucket,
        Key='ProcessedJSONFolder/EnvironmentalRisk.json',
        Body=json.dumps(json_header, indent=4)
    )

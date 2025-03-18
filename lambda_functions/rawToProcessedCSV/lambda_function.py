from datetime import datetime
from io import StringIO, TextIOWrapper

import json
import boto3
import pandas as pd

UPLOAD_PREFIX = "ProcessedCSV/"
UPLOAD_FILENAME = "environmental_risk"

now = datetime.now()
date_time = now.strftime("%Y-%m-%d %H:%M:%S.%f")
TZS = 'GMT+11'

s3_client = boto3.client('s3')


def lambda_handler(event, context):
    try:
        print("Starting Conversion of Raw CSV to Processed CSV and store in",
              UPLOAD_PREFIX)
        print("Event received:", json.dumps(event))

        # Retrieves the bucketname and the file
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']

        response = s3_client.get_object(
            Bucket=bucket,
            Key=key
        )

        data_to_string = TextIOWrapper(
            response['Body'],
            encoding='utf-8'
        )

        #  Condition when user chooses different filtering
        csv_data_frame = pd.read_csv(
            data_to_string,
            sep="|"
        )

        #   Filter for Environmental Risk Metrics
        csv_data_frame_filter = ["CO2DIRECTSCOPE1",
                                 "CO2INDIRECTSCOPE2",
                                 "CO2INDIRECTSCOPE3",
                                 "CO2_NO_EQUIVALENTS",
                                 "NOXEMISSIONS",
                                 "SOXEMISSIONS",
                                 "VOCEMISSIONS",
                                 "WASTETOTAL",
                                 "HAZARDOUSWASTE",
                                 "PARTICULATE_MATTER_EMISSIONS",
                                 "AIRPOLLUTANTS_DIRECT",
                                 "AIRPOLLUTANTS_INDIRECT",
                                 "NATURAL_RESOURCE_USE_DIRECT",
                                 "WATERWITHDRAWALTOTAL",
                                 "WATER_USE_PAI_M10",
                                 "TOXIC_CHEMICALS_REDUCTION",
                                 "VOC_EMISSIONS_REDUCTION",
                                 "N_OXS_OX_EMISSIONS_REDUCTION"]

        filtered_data_frame = csv_data_frame[csv_data_frame["metric_name"]
                                             .isin(csv_data_frame_filter)]

        # DataFrame to CSV file and then saved to s3Bucket
        csv_path_tmp = StringIO()
        filtered_data_frame.to_csv(
            csv_path_tmp,
            sep="|",
            index=False
        )
        s3_client.put_object(
            Bucket=bucket,
            Key=f"{UPLOAD_PREFIX}/{UPLOAD_FILENAME}.csv",
            Body=csv_path_tmp.getvalue()
        )

        # Acknowledge completion of conversion
        print("Conversion of Raw CSV to Processed CSV completed.")

        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"status": "Success"})
        }

    except Exception as e:
        # Log error details
        print(f"Error: {str(e)}")
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}

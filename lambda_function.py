import json
import base64
import csv
import io
import boto3


def lambda_handler(event, context):
    s3 = boto3.client("s3")
    # get bucketname and filename
    bucketname = event["pathParameters"]["bucket"]
    filename = event["queryStringParameters"]["file"]

    URL = s3.generate_presigned_url("put_object", Params ={"Bucket": bucketname, "Key": filename}, ExpiresIn=3600)



    return {
        "statusCode": 200,
         "body": json.dumps({"message": "URL", "data": URL})
     }


    # # Check if body exists
    # if "body" not in event or event["body"] is None:
    #     return {
    #         "statusCode": 400,
    #         "body": json.dumps({"error": "No CSV data received"})
    #     }

    # try:
    #     # Decode Base64 CSV if necessary
    #     if event.get("isBase64Encoded"):
    #         csv_bytes = base64.b64decode(event["body"])
    #     else:
    #         csv_bytes = event["body"].encode("utf-8")
    #     csv_string = csv_bytes.decode("utf-8")

    #     # Parse CSV
    #     csv_reader = csv.reader(io.StringIO(csv_string))

    #     # Get only first 5 rows
    #     csv_data = [row for _, row in zip(range(5), csv_reader)]

    #     return {
    #         "statusCode": 200,
    #         "body": json.dumps({"message": "CSV processed", "data": csv_data})
    #     }

    # except Exception as e:
    #     return {
    #         "statusCode": 500,
    #         "body": json.dumps({"error": str(e)})
    #     }

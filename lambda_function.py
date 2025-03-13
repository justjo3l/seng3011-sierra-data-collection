import json
import boto3
from botocore.config import Config

ALLOWED_BUCKETS = {"sierra-e-bucket"}


def lambda_handler(event, context):
    try:
        # Log the full event
        print("Event received:", json.dumps(event))

        s3 = boto3.client(
            's3',
            region_name='ap-southeast-2',
            config=Config(signature_version='s3v4')
        )

        params = event.get("queryStringParameters", {})
        # Log query params
        print("Query Parameters:", params)

        if not params:
            return {
                "statusCode": 400,
                "body": json.dumps({
                    "error": "No query parameters found"
                    })
                }

        bucket_name = params.get("bucket")
        file_name = params.get("file")

        if not bucket_name or not file_name:
            return {
                "statusCode": 400,
                "body": json.dumps({
                    "error": "Missing 'bucket' or 'file' parameter"
                    })
                }

        if bucket_name not in ALLOWED_BUCKETS:
            return {
                "statusCode": 403,
                "body": json.dumps({
                    "error": "Bucket not allowed"
                    })
                }

        # Log bucket and file name
        print(f"Bucket: {bucket_name}, File: {file_name}")

        # Generate pre-signed URL
        presigned_url = s3.generate_presigned_url(
            'put_object',
            Params={
                'Bucket': bucket_name,
                'Key': file_name,
                'ContentType': 'text/csv'
                },
            ExpiresIn=3600
        )

        # Log the URL
        print("Generated Presigned URL:", presigned_url)

        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"URL": presigned_url})
        }

    except Exception as e:
        # Log error details
        print(f"Error: {str(e)}")
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}

import json
import base64
import csv
import io


def lambda_handler(event, context):

    # Check if body exists
    if event.get("body") is None:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": event})
        }

    try:
        # Decode body if Base64-encoded
        if event.get("isBase64Encoded", False):
            csv_bytes = base64.b64decode(event["body"])
        else:
            csv_bytes = event["body"].encode("utf-8")  # Assume plain text CSV

        # Convert bytes to string and parse CSV
        csv_string = csv_bytes.decode("utf-8")
        csv_reader = csv.reader(io.StringIO(csv_string))

        # Read CSV rows into a list
        csv_data = [row for row in csv_reader]

        return {
            "statusCode": 200,
            "body": json.dumps({"message": "CSV received", "data": csv_data})
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }

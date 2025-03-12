import json
import base64
import csv
import io


def lambda_handler(event, context):
    print("Received event:", json.dumps(event, indent=2))  # Debugging
    
    # Log headers to check Content-Type
    headers = event.get("headers", {})
    print("Headers:", headers)  # Logs all headers

    # Handle case sensitivity
    content_type = headers.get("Content-Type", headers.get("content-type"))

    # Logs the Content-Type specifically
    print("Content-Type:", content_type)

    # Check if body exists
    if "body" not in event or event["body"] is None:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "No CSV data received"})
        }

    try:
        # Decode Base64 CSV if necessary
        if event.get("isBase64Encoded"):
            csv_bytes = base64.b64decode(event["body"])
        else:
            csv_bytes = event["body"].encode("utf-8")
        csv_string = csv_bytes.decode("utf-8")

        # Parse CSV
        csv_reader = csv.reader(io.StringIO(csv_string))

        # Get only first 5 rows
        csv_data = [row for _, row in zip(range(5), csv_reader)]

        return {
            "statusCode": 200,
            "body": json.dumps({"message": "CSV processed", "data": csv_data})
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }

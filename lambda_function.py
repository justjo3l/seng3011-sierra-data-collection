import json
import base64
import csv
from io import StringIO


def lambda_handler(event, context):
    # Check if the request body is Base64-encoded
    is_base64 = event.get("isBase64Encoded", False)
    body = base64.b64decode(event["body"]) if is_base64 else event["body"]

    # Decode CSV data
    try:
        csv_data = body.decode("utf-8")
        reader = csv.reader(StringIO(csv_data))
        rows = [row for row in reader]  # Convert CSV into a list of rows

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "CSV received",
                "rows": rows[:5]
            }),
        }
    except Exception as e:
        return {
            "statusCode": 400,
            "body": json.dumps({
                "error": str(e)
            }),
        }
    
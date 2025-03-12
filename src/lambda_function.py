import json
import base64
import csv
from io import StringIO
from email import message_from_bytes
from email.policy import default


def lambda_handler(event, context):
    # Decode body if base64 encoded
    is_base64 = event.get("isBase64Encoded", False)

    if is_base64:
        body = base64.b64decode(event["body"])
    else:
        body = event["body"].encode()

    # Extract `Content-Type` from headers
    content_type = event["headers"].get("content-type", "")
    if not content_type.startswith("multipart/form-data"):
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "Invalid content type"})
        }

    # Parse multipart form data using email parser
    message = message_from_bytes(
        b"Content-Type: " +
        content_type.encode() +
        b"\r\n\r\n" +
        body,
        policy=default)

    # Find the CSV file in the form-data
    for part in message.iter_parts():
        if part.get_content_type() == "text/csv":
            csv_data = part.get_payload(decode=True).decode("utf-8")
            reader = csv.reader(StringIO(csv_data))
            rows = [row for row in reader]  # Convert CSV into a list of rows

            return {
                "statusCode": 200,
                "body": json.dumps({
                    "message": "CSV received",
                    "rows": rows[:5]
                    }),
            }

    return {
        "statusCode": 400,
        "body": json.dumps({
            "error": "No CSV file found"
            })
        }

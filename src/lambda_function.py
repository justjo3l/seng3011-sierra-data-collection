import json
import base64
import cgi
from io import StringIO
import csv

def lambda_handler(event, context):
    # Decode body if base64 encoded
    is_base64 = event.get("isBase64Encoded", False)
    body = base64.b64decode(event["body"]) if is_base64 else event["body"]

    # Parse multipart form data
    environ = {"CONTENT_TYPE": event["headers"].get("content-type", "")}
    form_data = cgi.FieldStorage(fp=StringIO(body.decode()), environ=environ, keep_blank_values=True)

    # Extract CSV file
    file_item = form_data["file"]
    if not file_item.filename:
        return {"statusCode": 400, "body": json.dumps({"error": "No file uploaded"})}

    # Read CSV content
    csv_content = file_item.file.read().decode("utf-8")
    reader = csv.reader(StringIO(csv_content))
    csv_data = [row for row in reader]

    return {
        "statusCode": 200,
        "body": json.dumps({"message": "CSV received", "rows": csv_data[:5]}),
    }

import boto3
import os
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

bucket = "ragbareacttestings3vector"
cutoff = datetime(2026, 1, 23, tzinfo=timezone.utc)

s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_REGION')
)
count = 0

paginator = s3.get_paginator("list_objects_v2")

for page in paginator.paginate(Bucket=bucket):
    for obj in page.get("Contents", []):
        if obj["LastModified"] >= cutoff:
            count += 1

print("Files uploaded on or after 23 Jan 2026:", count)

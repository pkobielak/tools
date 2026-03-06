#!/usr/bin/env python3
# Usage: ./print_s3_json.py [bucket region endpoint access_key secret_key] <key>
#        S3 connection params can be omitted if set in .env (see .env.example)
import sys, json

try:
    import boto3
    from botocore.exceptions import ClientError
except Exception:
    print("Missing deps. Install: pip install boto3")
    raise

from s3_config import get_s3_config, s3_config_available

args = sys.argv[1:]
if len(args) == 6:
    bucket, region, endpoint, access_key, secret_key, key = args
elif len(args) == 1 and s3_config_available():
    bucket, region, endpoint, access_key, secret_key = get_s3_config()
    key = args[0]
else:
    print("Usage: [bucket region endpoint access_key secret_key] key")
    print("       S3 params can be set in .env instead of passing them on the command line")
    sys.exit(1)

key = key.lstrip('/')  # accept leading slash

try:
    s3 = boto3.client(
        "s3",
        region_name=region,
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )
except Exception as e:
    print(f"Failed to initialize S3 client: {e}")
    sys.exit(1)

try:
    resp = s3.get_object(Bucket=bucket, Key=key)
except ClientError as e:
    err = e.response.get("Error", {})
    print(f"Error fetching object '{key}': {err.get('Code')}: {err.get('Message') or str(e)}")
    sys.exit(2)
except Exception as e:
    print(f"Error fetching object '{key}': {e}")
    sys.exit(2)

data = resp["Body"].read()
text = data.decode("utf-8", errors="replace")

try:
    obj = json.loads(text)
except json.JSONDecodeError as e:
    print(f"Invalid JSON in '{key}': {e}")
    sys.exit(3)

print(json.dumps(obj, ensure_ascii=False, indent=2))


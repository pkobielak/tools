#!/usr/bin/env python3
# Usage: ./test_s3_access.py [bucket region endpoint access_key secret_key] <prefix>
#        S3 connection params can be omitted if set in .env (see .env.example)
import sys
from collections import defaultdict

try:
    import boto3
    from botocore.exceptions import ClientError
except Exception:
    print("Missing deps. Install: pip install boto3")
    raise

from s3_config import get_s3_config, s3_config_available

args = sys.argv[1:]
if len(args) == 6:
    bucket, region, endpoint, key, secret, prefix = args
elif len(args) == 1 and s3_config_available():
    bucket, region, endpoint, key, secret = get_s3_config()
    prefix = args[0]
else:
    print("Usage: [bucket region endpoint access_key secret_key] prefix")
    print("       S3 params can be set in .env instead of passing them on the command line")
    sys.exit(1)

try:
    s3 = boto3.client(
        "s3",
        region_name=region,
        endpoint_url=endpoint,
        aws_access_key_id=key,
        aws_secret_access_key=secret,
    )
    print(f"✓ Connected to S3 endpoint: {endpoint}")
    print(f"✓ Testing access to bucket: {bucket} (prefix: {prefix})\n")
except Exception as e:
    print(f"✗ Failed to initialize S3 client: {e}")
    sys.exit(1)

try:
    s3.head_bucket(Bucket=bucket)
    print(f"✓ Bucket '{bucket}' is accessible\n")
except ClientError as e:
    error_code = e.response.get("Error", {}).get("Code", "Unknown")
    print(f"✗ Cannot access bucket '{bucket}': {error_code}")
    sys.exit(1)

try:
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
    
    def nested_dict():
        return defaultdict(nested_dict)
    
    tree = nested_dict()
    total_objects = 0
    
    for page in pages:
        if 'Contents' not in page:
            continue
            
        for obj in page['Contents']:
            key = obj['Key']
            total_objects += 1
            display_key = key[len(prefix):].lstrip('/') if prefix else key
            parts = display_key.split('/')
            
            current = tree
            for part in parts[:-1]:
                current = current[part]
            
            if '__files__' not in current:
                current['__files__'] = []
            current['__files__'].append(parts[-1])
    
    print(f"Bucket tree (showing max 8 items per level, complete depth):")
    print(f"Total objects: {total_objects}\n")
    print(f"{bucket}/{prefix}")
    
    def count_items(node):
        if not isinstance(node, dict):
            return 0
        files = len(node.get('__files__', []))
        dirs = len([k for k in node.keys() if k != '__files__'])
        return files + dirs
    
    def print_tree(node, indent="", is_last=True):
        files = sorted(node.get('__files__', []))
        dirs = sorted([k for k in node.keys() if k != '__files__'])
        
        items = []
        for d in dirs[:8]:
            items.append(('dir', d, node[d]))
        
        remaining = 8 - len(items)
        for f in files[:remaining]:
            items.append(('file', f))
        
        total_items = len(files) + len(dirs)
        
        for i, item in enumerate(items):
            is_last_item = (i == len(items) - 1 and total_items <= 8)
            
            item_prefix = indent + ("└── " if is_last_item else "├── ")
            
            if is_last_item:
                next_indent = indent + "    "
            else:
                next_indent = indent + "│   "
            
            if item[0] == 'file':
                print(f"{item_prefix}{item[1]}")
            else:
                dir_name, dir_node = item[1], item[2]
                item_count = count_items(dir_node)
                print(f"{item_prefix}{dir_name}/ ({item_count} items)")
                
                print_tree(dir_node, next_indent, is_last_item)
        
        if total_items > 8:
            if is_last:
                print(f"{indent}└── ...")
            else:
                print(f"{indent}└── ...")
    
    print_tree(tree, "", True)
    
    print(f"\n✓ Successfully listed bucket contents")
    
except Exception as e:
    print(f"✗ Failed to list bucket contents: {e}")
    sys.exit(1)

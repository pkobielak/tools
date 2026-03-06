#!/usr/bin/env python3
# Usage: ./download_from_s3.py <bucket> <region> <endpoint> <access_key> <secret_key> <s3_prefix> <local_dir> [workers]
import sys
import time
import threading
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    import boto3
    from botocore.exceptions import ClientError
    from boto3.s3.transfer import TransferConfig
except Exception:
    print("Missing deps. Install: pip install boto3")
    raise

args = sys.argv[1:]
if not (7 <= len(args) <= 8):
    print("Usage: bucket region endpoint access_key secret_key s3_prefix local_dir [workers]")
    sys.exit(1)

bucket, region, endpoint, key, secret, s3_prefix, local_dir = args[:7]
workers = int(args[7]) if len(args) == 8 else 8

s3 = boto3.client(
    "s3",
    region_name=region,
    endpoint_url=endpoint,
    aws_access_key_id=key,
    aws_secret_access_key=secret,
)

# Ensure local directory exists
root = Path(local_dir)
root.mkdir(parents=True, exist_ok=True)

# List all objects with the given prefix
print(f"Listing objects in s3://{bucket}/{s3_prefix}...")
objects = []
paginator = s3.get_paginator('list_objects_v2')
for page in paginator.paginate(Bucket=bucket, Prefix=s3_prefix):
    for obj in page.get('Contents', []):
        # Skip if it's a directory marker
        if not obj['Key'].endswith('/'):
            objects.append(obj)

if not objects:
    print("No files to download")
    sys.exit(0)

print(f"Found {len(objects)} files")

cfg = TransferConfig(multipart_threshold=8*1024*1024, max_concurrency=workers, use_threads=True)

lock = threading.Lock()
stats = {
    "total_files": len(objects),
    "total_bytes": sum(obj['Size'] for obj in objects),
    "downloaded": 0,
    "skipped": 0,
    "failed": 0,
    "processed": 0,
    "bytes_done": 0,
}
start_ts = time.time()
stop_event = threading.Event()

def exists_same_size(local_path: Path, size: int) -> bool:
    try:
        return local_path.exists() and local_path.stat().st_size == size
    except Exception:
        return False

def download(obj: dict):
    obj_key = obj['Key']
    size = obj['Size']
    
    # Calculate relative path by removing the prefix
    if obj_key.startswith(s3_prefix):
        rel_path = obj_key[len(s3_prefix):].lstrip('/')
    else:
        rel_path = obj_key
    
    local_path = root / rel_path
    
    try:
        if exists_same_size(local_path, size):
            with lock:
                stats["skipped"] += 1
                stats["processed"] += 1
                stats["bytes_done"] += size
            return f"SKIP  {obj_key}"
        
        # Ensure parent directory exists
        local_path.parent.mkdir(parents=True, exist_ok=True)
        
        s3.download_file(bucket, obj_key, str(local_path), Config=cfg)
        with lock:
            stats["downloaded"] += 1
            stats["processed"] += 1
            stats["bytes_done"] += size
        return f"OK    {obj_key} -> {local_path}"
    except Exception as e:
        with lock:
            stats["failed"] += 1
            stats["processed"] += 1
            stats["bytes_done"] += size
        return f"FAIL  {obj_key} -> {e.__class__.__name__}: {e}"

def fmt_eta(seconds: float) -> str:
    if seconds != seconds or seconds == float("inf") or seconds < 0:
        return "unknown"
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"

def reporter():
    while not stop_event.wait(5):
        with lock:
            processed = stats["processed"]
            left = stats["total_files"] - processed
            downloaded = stats["downloaded"]
            skipped = stats["skipped"]
            failed = stats["failed"]
            bytes_done = stats["bytes_done"]
            total_bytes = stats["total_bytes"]
        elapsed = time.time() - start_ts
        rate_f = processed / elapsed if elapsed > 0 else 0.0
        rate_b = bytes_done / elapsed if elapsed > 0 else 0.0
        eta = (left / rate_f) if rate_f > 0 else float("inf")
        mbps = rate_b / (1024*1024)
        pct = (bytes_done / total_bytes * 100.0) if total_bytes else 100.0
        print(f"[{elapsed:6.1f}s] downloaded={downloaded} skipped={skipped} failed={failed} left={left} "
              f"done={pct:5.1f}% rate={mbps:6.2f} MB/s ETA={fmt_eta(eta)}")

rep_thr = threading.Thread(target=reporter, daemon=True)
rep_thr.start()

with ThreadPoolExecutor(max_workers=workers) as ex:
    futs = [ex.submit(download, obj) for obj in objects]
    for fut in as_completed(futs):
        print(fut.result())

stop_event.set()
rep_thr.join()

elapsed = time.time() - start_ts
with lock:
    downloaded = stats["downloaded"]
    skipped = stats["skipped"]
    failed = stats["failed"]
    total = stats["total_files"]
    bytes_done = stats["bytes_done"]
total_mb = bytes_done / (1024*1024)
print("\nSummary:")
print(f"Total files : {total}")
print(f"Downloaded  : {downloaded}")
print(f"Skipped     : {skipped}")
print(f"Failed      : {failed}")
print(f"Bytes done  : {total_mb:.2f} MB")
print(f"Elapsed     : {elapsed:.2f} s")

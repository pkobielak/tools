#!/usr/bin/env python3
# Usage: ./upload_to_s3.py [bucket region endpoint access_key secret_key] <source_dir> <dest_dir> [workers]
#        S3 connection params can be omitted if set in .env (see .env.example)
import sys
import time
import mimetypes
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

from s3_config import get_s3_config, s3_config_available

args = sys.argv[1:]
if 7 <= len(args) <= 8:
    bucket, region, endpoint, key, secret, source_dir, dest_dir = args[:7]
    workers = int(args[7]) if len(args) == 8 else 8
elif 2 <= len(args) <= 3 and s3_config_available():
    bucket, region, endpoint, key, secret = get_s3_config()
    source_dir, dest_dir = args[:2]
    workers = int(args[2]) if len(args) == 3 else 8
else:
    print("Usage: [bucket region endpoint access_key secret_key] source_dir dest_dir [workers]")
    print("       S3 params can be set in .env instead of passing them on the command line")
    sys.exit(1)

s3 = boto3.client(
    "s3",
    region_name=region,
    endpoint_url=endpoint,
    aws_access_key_id=key,
    aws_secret_access_key=secret,
)

root = Path(source_dir)
if not root.exists():
    print(f"Missing '{source_dir}' directory")
    sys.exit(1)

files = [p for p in root.rglob("*") if p.is_file()]
if not files:
    print("No files to upload")
    sys.exit(0)

cfg = TransferConfig(multipart_threshold=8*1024*1024, max_concurrency=workers, use_threads=True)

lock = threading.Lock()
stats = {
    "total_files": len(files),
    "total_bytes": sum(p.stat().st_size for p in files),
    "uploaded": 0,
    "skipped": 0,
    "failed": 0,
    "processed": 0,
    "bytes_done": 0,
}
start_ts = time.time()
stop_event = threading.Event()

def exists_same_size(obj_key: str, size: int) -> bool:
    try:
        head = s3.head_object(Bucket=bucket, Key=obj_key)
        return head.get("ContentLength") == size
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") in ("404", "NoSuchKey", "NotFound"):
            return False
        raise

def content_type(path: Path):
    ct, _ = mimetypes.guess_type(path.as_posix())
    return {"ContentType": ct} if ct else {}

def upload(path: Path):
    obj_key = f"{dest_dir}/{path.relative_to(root)}"
    size = path.stat().st_size
    try:
        if exists_same_size(obj_key, size):
            with lock:
                stats["skipped"] += 1
                stats["processed"] += 1
                stats["bytes_done"] += size
            return f"SKIP  {obj_key}"
        s3.upload_file(str(path), bucket, obj_key, ExtraArgs=content_type(path), Config=cfg)
        with lock:
            stats["uploaded"] += 1
            stats["processed"] += 1
            stats["bytes_done"] += size
        return f"OK    {obj_key}"
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
            uploaded = stats["uploaded"]
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
        print(f"[{elapsed:6.1f}s] uploaded={uploaded} skipped={skipped} failed={failed} left={left} "
              f"done={pct:5.1f}% rate={mbps:6.2f} MB/s ETA={fmt_eta(eta)}")

rep_thr = threading.Thread(target=reporter, daemon=True)
rep_thr.start()

with ThreadPoolExecutor(max_workers=workers) as ex:
    futs = [ex.submit(upload, f) for f in files]
    for fut in as_completed(futs):
        print(fut.result())

stop_event.set()
rep_thr.join()

elapsed = time.time() - start_ts
with lock:
    uploaded = stats["uploaded"]
    skipped = stats["skipped"]
    failed = stats["failed"]
    total = stats["total_files"]
    bytes_done = stats["bytes_done"]
total_mb = bytes_done / (1024*1024)
print("\nSummary:")
print(f"Total files: {total}")
print(f"Uploaded   : {uploaded}")
print(f"Skipped    : {skipped}")
print(f"Failed     : {failed}")
print(f"Bytes done : {total_mb:.2f} MB")
print(f"Elapsed    : {elapsed:.2f} s")

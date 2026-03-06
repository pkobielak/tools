#!/usr/bin/env python3
# Usage: ./copy_prefix_s3.py [bucket region endpoint access_key secret_key] <src_prefix> <dest_prefix> [workers]
#        S3 connection params can be omitted if set in .env (see .env.example)
import sys, time, threading
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    import boto3
    from botocore.exceptions import ClientError
except Exception:
    print("Missing deps. Install: pip install boto3")
    raise

from s3_config import get_s3_config, s3_config_available

args = sys.argv[1:]
if 7 <= len(args) <= 8:
    bucket, region, endpoint, key, secret, src_prefix, dest_prefix = args[:7]
    workers = int(args[7]) if len(args) == 8 else 16
elif 2 <= len(args) <= 3 and s3_config_available():
    bucket, region, endpoint, key, secret = get_s3_config()
    src_prefix, dest_prefix = args[:2]
    workers = int(args[2]) if len(args) == 3 else 16
else:
    print("Usage: [bucket region endpoint access_key secret_key] src_prefix dest_prefix [workers]")
    print("       S3 params can be set in .env instead of passing them on the command line")
    sys.exit(1)

s3 = boto3.client("s3", region_name=region, endpoint_url=endpoint, aws_access_key_id=key, aws_secret_access_key=secret)

src = src_prefix.strip("/"); dst = dest_prefix.strip("/")
src_root = (src + "/") if src else ""; dst_root = (dst + "/") if dst else ""

def exists_same_size(key: str, size: int) -> bool:
    for i in range(5):
        try:
            return s3.head_object(Bucket=bucket, Key=key).get("ContentLength") == size
        except ClientError as e:
            err = e.response.get("Error", {})
            code = err.get("Code"); status = e.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if code in ("404", "NoSuchKey", "NotFound"): return False
            if (code in ("503", "ServiceUnavailable") or status == 503) and i < 4:
                time.sleep(1*(2**i)); continue
            raise

def dst_key(key: str) -> str:
    return dst_root + (key[len(src_root):] if key.startswith(src_root) else key)

lock = threading.Lock()
stats = {"total_files": 0, "total_bytes": 0, "copied": 0, "skipped": 0, "failed": 0, "processed": 0, "bytes_done": 0}
failures = []
start_ts = time.time(); stop_event = threading.Event()

def fmt_eta(sec: float) -> str:
    if sec != sec or sec == float("inf") or sec < 0: return "unknown"
    m, s = divmod(int(sec), 60); h, m = divmod(m, 60); return f"{h:02d}:{m:02d}:{s:02d}"

def reporter():
    while not stop_event.wait(1):
        with lock:
            p=stats["processed"]; l=stats["total_files"]-p; c=stats["copied"]; s=stats["skipped"]; f=stats["failed"]; bd=stats["bytes_done"]; tb=stats["total_bytes"]
        el = time.time()-start_ts; rf=p/el if el>0 else 0.0; rb=bd/el if el>0 else 0.0
        eta = (l/rf) if rf>0 else float("inf"); pct=(bd/tb*100.0) if tb else 100.0
        print(f"[{el:6.1f}s] copied={c} skipped={s} failed={f} left={l} done={pct:5.1f}% rate={rb/1048576:6.2f} MB/s ETA={fmt_eta(eta)}")

def copy_one(key: str, size: int):
    to_key = dst_key(key)
    try:
        if exists_same_size(to_key, size):
            with lock:
                stats["skipped"]+=1; stats["processed"]+=1; stats["bytes_done"]+=size
            return
        last_exc = None
        for i in range(5):
            try:
                s3.copy({"Bucket": bucket, "Key": key}, bucket, to_key)
                last_exc = None
                break
            except ClientError as e:
                err = e.response.get("Error", {})
                code = err.get("Code"); status = e.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
                if (code in ("503", "ServiceUnavailable") or status == 503) and i < 4:
                    time.sleep(1*(2**i)); last_exc = e; continue
                last_exc = e; break
            except Exception as e:
                last_exc = e; break
        if last_exc is None:
            with lock:
                stats["copied"]+=1; stats["processed"]+=1; stats["bytes_done"]+=size
            return
        raise last_exc
    except Exception as e:
        code = msg = None
        if isinstance(e, ClientError):
            err = e.response.get("Error", {})
            code = err.get("Code")
            msg = err.get("Message") or str(e)
        else:
            code = e.__class__.__name__
            msg = str(e)
        with lock:
            stats["failed"]+=1; stats["processed"]+=1; stats["bytes_done"]+=size
            failures.append((key, to_key, code, msg))

# List all objects under src_prefix
keys = []
for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=src_root):
    for obj in page.get("Contents", []):
        k=obj["Key"]; sz=obj.get("Size",0)
        if not k.endswith("/"):
            keys.append((k, sz))

if not keys:
    print("No objects to copy"); sys.exit(0)

with lock:
    stats["total_files"] = len(keys); stats["total_bytes"] = sum(sz for _, sz in keys)

thr = threading.Thread(target=reporter, daemon=True); thr.start()
with ThreadPoolExecutor(max_workers=workers) as ex:
    for _ in as_completed([ex.submit(copy_one, k, sz) for k, sz in keys]):
        pass
stop_event.set(); thr.join()

elapsed = time.time()-start_ts
with lock:
    c=stats["copied"]; s=stats["skipped"]; f=stats["failed"]; t=stats["total_files"]; bd=stats["bytes_done"]
print("\nSummary:")
print(f"Total objects: {t}")
print(f"Copied       : {c}")
print(f"Skipped      : {s}")
print(f"Failed       : {f}")
print(f"Bytes done   : {bd/1048576:.2f} MB")
print(f"Elapsed      : {elapsed:.2f} s")
if failures:
    print("\nFailures:")
    for src_k, dst_k, code, msg in failures:
        print(f"- from='{src_k}' to='{dst_k}' error={code}: {msg}")
    sys.exit(2)

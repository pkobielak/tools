"""Load S3 connection config from .env file (optional fallback for CLI args)."""
import os
from pathlib import Path

def load_env():
    """Load .env file from the script's directory if it exists."""
    env_path = Path(__file__).parent / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip().strip("\"'")
        if key and key not in os.environ:
            os.environ[key] = value

load_env()

def get_s3_config():
    """Return (bucket, region, endpoint, access_key, secret_key) from env vars."""
    bucket = os.environ.get("S3_BUCKET")
    region = os.environ.get("S3_REGION")
    endpoint = os.environ.get("S3_ENDPOINT")
    access_key = os.environ.get("S3_ACCESS_KEY")
    secret_key = os.environ.get("S3_SECRET_KEY")
    return bucket, region, endpoint, access_key, secret_key

def s3_config_available():
    """Check if all S3 config vars are set."""
    return all(get_s3_config())

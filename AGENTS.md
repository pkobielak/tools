# Tools

Collection of CLI utilities for S3 operations and YouTube data extraction.

## Guidance for AI Assistants

Before running any script on the user's behalf:

1. **Ask clarifying questions** to understand what the user actually wants to achieve. Don't assume intent from a vague request. Examples:
   - "Which S3 prefix do you want to copy from and to?"
   - "Do you want to download everything under that prefix or just specific files?"
   - "How many hours of video content are you targeting?"

2. **Show the expected result before executing.** Present the exact command you plan to run and describe what it will do, including:
   - Source and destination paths
   - Approximate number of files or data size if known
   - Whether it will overwrite existing files (scripts skip files with matching size)
   - The worker count being used

3. **Wait for user confirmation** before running any command. These scripts can move large amounts of data or make many API calls.

## Setup

Uses [uv](https://docs.astral.sh/uv/) as the package manager. All scripts should be run with `uv run`.

```bash
uv sync
```

## S3 Configuration

All S3 scripts accept connection params either via CLI args or a `.env` file.

To use `.env`, copy the example and fill in your values:

```bash
cp .env.example .env
```

Required env vars: `S3_BUCKET`, `S3_REGION`, `S3_ENDPOINT`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`.

CLI args always take precedence over `.env` values.

## Performance

Before running any script, increase the open file descriptor limit to avoid issues with high concurrency:

```bash
ulimit -n 65000
```

The `[workers]` parameter on S3 scripts defaults to 8 or 16 depending on the script but can safely be set up to 200.

## S3 Scripts

### upload_to_s3.py

Upload a local directory to S3 with parallel workers and skip-if-same-size logic.

```bash
uv run upload_to_s3.py <source_dir> <dest_dir> [workers]
uv run upload_to_s3.py <bucket> <region> <endpoint> <access_key> <secret_key> <source_dir> <dest_dir> [workers]
```

### download_from_s3.py

Download all files under an S3 prefix to a local directory.

```bash
uv run download_from_s3.py <s3_prefix> <local_dir> [workers]
uv run download_from_s3.py <bucket> <region> <endpoint> <access_key> <secret_key> <s3_prefix> <local_dir> [workers]
```

### copy_prefix_s3.py

Copy all objects from one S3 prefix to another within the same bucket.

```bash
uv run copy_prefix_s3.py <src_prefix> <dest_prefix> [workers]
uv run copy_prefix_s3.py <bucket> <region> <endpoint> <access_key> <secret_key> <src_prefix> <dest_prefix> [workers]
```

### copy_list_s3.py

Copy specific S3 objects listed in a file to a destination prefix.

```bash
uv run copy_list_s3.py <file_list> <dest_prefix> [workers]
uv run copy_list_s3.py <bucket> <region> <endpoint> <access_key> <secret_key> <file_list> <dest_prefix> [workers]
```

The file list should contain one S3 key or `s3://bucket/key` URI per line.

### s3_browser.py

Interactive TUI browser for S3 buckets (curses-based).

```bash
uv run s3_browser.py [prefix]
uv run s3_browser.py <bucket> <region> <endpoint> <access_key> <secret_key> [prefix]
```

Controls: arrow keys to navigate, Space to preview text files, `q` to quit.

### print_s3_json.py

Fetch and pretty-print a JSON file from S3.

```bash
uv run print_s3_json.py <key>
uv run print_s3_json.py <bucket> <region> <endpoint> <access_key> <secret_key> <key>
```

### test_s3_access.py

Test S3 connectivity and display a tree view of bucket contents under a prefix.

```bash
uv run test_s3_access.py <prefix>
uv run test_s3_access.py <bucket> <region> <endpoint> <access_key> <secret_key> <prefix>
```

## YouTube Scripts

### yt_channel_urls.py

List video URLs from a YouTube channel up to a target number of hours.

```bash
uv run yt_channel_urls.py <channel_url> <hours> [--output <file.txt>]
```

### yt_total_hours.py

Calculate total video hours from a text file of YouTube links.

```bash
uv run yt_total_hours.py [links_file]
```

Defaults to `yt-links.txt` if no file is provided.

#!/usr/bin/env python3
"""
Calculate total video hours from a list of YouTube links.

Usage:
    uv run yt_total_hours.py [links_file]
    
If no file is provided, defaults to yt-links.txt in the same directory.
"""

import sys
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import yt_dlp


def get_video_duration(url: str) -> tuple[str, float | None, str | None]:
    """Fetch video duration using yt-dlp."""
    ydl_opts = {
        "quiet": True,
        "no_warnings": True,
        "extract_flat": False,
    }
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=False)
            duration = info.get("duration")
            if duration is not None:
                return (url, float(duration), None)
            return (url, None, "No duration found")
    except Exception as e:
        return (url, None, str(e)[:50])


def format_duration(total_seconds: float) -> str:
    """Format seconds into human-readable duration."""
    hours = int(total_seconds // 3600)
    minutes = int((total_seconds % 3600) // 60)
    seconds = int(total_seconds % 60)
    return f"{hours}h {minutes}m {seconds}s"


def main():
    # Determine input file
    script_dir = Path(__file__).parent
    if len(sys.argv) > 1:
        links_file = Path(sys.argv[1])
    else:
        links_file = script_dir / "yt-links.txt"

    if not links_file.exists():
        print(f"Error: File not found: {links_file}")
        sys.exit(1)

    # Read links
    with open(links_file) as f:
        links = [line.strip() for line in f if line.strip()]

    print(f"Found {len(links)} links in {links_file.name}")
    print("Fetching video durations (this may take a while)...\n")

    # Fetch durations in parallel
    total_seconds = 0.0
    successful = 0
    failed = 0
    failed_urls = []

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(get_video_duration, url): url for url in links}
        
        for i, future in enumerate(as_completed(futures), 1):
            url, duration, error = future.result()
            
            if duration is not None:
                total_seconds += duration
                successful += 1
            else:
                failed += 1
                failed_urls.append((url, error))
            
            # Progress indicator
            print(f"\r[{i}/{len(links)}] Processed... ", end="", flush=True)

    print("\n")

    # Print results
    print("=" * 50)
    print(f"Total videos processed: {successful}/{len(links)}")
    if failed > 0:
        print(f"Failed to fetch: {failed}")
    print(f"\nTotal duration: {format_duration(total_seconds)}")
    print(f"Total hours: {total_seconds / 3600:.2f}")
    print("=" * 50)

    # Show failed URLs if any
    if failed_urls and len(failed_urls) <= 10:
        print("\nFailed URLs:")
        for url, error in failed_urls:
            print(f"  - {url}: {error}")
    elif failed_urls:
        print(f"\n(Showing first 10 of {len(failed_urls)} failed URLs)")
        for url, error in failed_urls[:10]:
            print(f"  - {url}: {error}")


if __name__ == "__main__":
    main()














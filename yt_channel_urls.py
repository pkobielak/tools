#!/usr/bin/env python3
"""
List video URLs from a YouTube channel up to a specified number of hours.

Usage:
    uv run yt_channel_urls.py <channel_url> <hours> [--output <file.txt>]
    
Examples:
    uv run yt_channel_urls.py https://www.youtube.com/@channelname 10
    uv run yt_channel_urls.py https://www.youtube.com/channel/UC... 5 --output urls.txt
    uv run yt_channel_urls.py "https://www.youtube.com/c/ChannelName" 2.5 -o my_urls.txt
    
Arguments:
    channel_url  URL of the YouTube channel (supports @handle, /channel/, /c/ formats)
    hours        Target number of hours of video content to collect
    
Options:
    -o, --output  Save URLs to a text file instead of just printing
"""

import argparse
import sys
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import yt_dlp


def get_channel_videos(channel_url: str) -> list[dict]:
    """Fetch all video entries from a channel."""
    ydl_opts = {
        "quiet": True,
        "no_warnings": True,
        "extract_flat": True,
        "playlist_items": "1-10000",  # Get up to 10k videos
    }
    
    # Ensure we're targeting the videos tab
    if not channel_url.endswith("/videos"):
        channel_url = channel_url.rstrip("/") + "/videos"
    
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(channel_url, download=False)
            if info is None:
                return []
            
            entries = info.get("entries", [])
            return [e for e in entries if e is not None]
    except Exception as e:
        print(f"Error fetching channel: {e}", file=sys.stderr)
        return []


def get_video_duration(video_id: str) -> tuple[str, float | None, str | None]:
    """Fetch video duration for a single video."""
    url = f"https://www.youtube.com/watch?v={video_id}"
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
                return (video_id, float(duration), None)
            return (video_id, None, "No duration found")
    except Exception as e:
        return (video_id, None, str(e)[:50])


def format_duration(total_seconds: float) -> str:
    """Format seconds into human-readable duration."""
    hours = int(total_seconds // 3600)
    minutes = int((total_seconds % 3600) // 60)
    seconds = int(total_seconds % 60)
    return f"{hours}h {minutes}m {seconds}s"


def main():
    parser = argparse.ArgumentParser(
        description="List video URLs from a YouTube channel up to a specified number of hours."
    )
    parser.add_argument("channel_url", help="URL of the YouTube channel")
    parser.add_argument("hours", type=float, help="Target number of hours of video content")
    parser.add_argument(
        "-o", "--output",
        type=str,
        default=None,
        help="Save URLs to a text file"
    )
    
    args = parser.parse_args()
    
    target_seconds = args.hours * 3600
    
    print(f"Fetching videos from channel: {args.channel_url}")
    print(f"Target duration: {args.hours} hours ({format_duration(target_seconds)})")
    print()
    
    # Get all video entries from channel
    print("Fetching video list from channel...")
    entries = get_channel_videos(args.channel_url)
    
    if not entries:
        print("No videos found or failed to fetch channel.", file=sys.stderr)
        sys.exit(1)
    
    print(f"Found {len(entries)} videos on channel")
    print("Fetching video durations to find target hours...\n")
    
    # Collect videos until we reach target hours
    collected_urls = []
    total_seconds = 0.0
    processed = 0
    
    # First, try to use duration from flat extraction if available
    videos_needing_duration = []
    for entry in entries:
        video_id = entry.get("id")
        duration = entry.get("duration")
        
        if video_id is None:
            continue
            
        if duration is not None:
            # Duration available from flat extraction
            url = f"https://www.youtube.com/watch?v={video_id}"
            collected_urls.append(url)
            total_seconds += float(duration)
            processed += 1
            
            print(f"\r[{processed}/{len(entries)}] Collected {len(collected_urls)} videos, "
                  f"total: {format_duration(total_seconds)}", end="", flush=True)
            
            if total_seconds >= target_seconds:
                break
        else:
            videos_needing_duration.append(video_id)
    
    # If we still need more videos and have some without duration info
    if total_seconds < target_seconds and videos_needing_duration:
        print(f"\nFetching detailed duration for {len(videos_needing_duration)} videos...")
        
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {
                executor.submit(get_video_duration, vid): vid 
                for vid in videos_needing_duration
            }
            
            for future in as_completed(futures):
                video_id, duration, error = future.result()
                processed += 1
                
                if duration is not None:
                    url = f"https://www.youtube.com/watch?v={video_id}"
                    collected_urls.append(url)
                    total_seconds += duration
                    
                    print(f"\r[{processed}/{len(entries)}] Collected {len(collected_urls)} videos, "
                          f"total: {format_duration(total_seconds)}", end="", flush=True)
                    
                    if total_seconds >= target_seconds:
                        # Cancel remaining futures
                        for f in futures:
                            f.cancel()
                        break
    
    print("\n")
    
    # Results
    print("=" * 60)
    print(f"Collected {len(collected_urls)} videos")
    print(f"Total duration: {format_duration(total_seconds)} ({total_seconds / 3600:.2f} hours)")
    print("=" * 60)
    print()
    
    # Output URLs
    if args.output:
        output_path = Path(args.output)
        with open(output_path, "w") as f:
            for url in collected_urls:
                f.write(url + "\n")
        print(f"URLs saved to: {output_path}")
        print(f"Total URLs: {len(collected_urls)}")
    else:
        print("Video URLs:")
        print("-" * 60)
        for url in collected_urls:
            print(url)


if __name__ == "__main__":
    main()

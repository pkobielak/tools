#!/usr/bin/env python3
# Usage: ./s3_browser.py <bucket> <region> <endpoint> <access_key> <secret_key> [prefix]
import sys
import curses
from typing import List, Dict, Optional, Tuple

try:
    import boto3
    from botocore.exceptions import ClientError
except Exception:
    print("Missing deps. Install: pip install boto3")
    raise

# Parse arguments
args = sys.argv[1:]
if len(args) < 5 or len(args) > 6:
    print("Usage: bucket region endpoint access_key secret_key [prefix]")
    sys.exit(1)

bucket, region, endpoint, key, secret = args[:5]
prefix = args[5] if len(args) == 6 else ""

# Ensure prefix ends with / if it's not empty (S3 directory convention)
if prefix and not prefix.endswith('/'):
    prefix += '/'

# Global S3 client
s3_client = None
current_prefix = prefix
items_per_page = 20
current_page = 0
selected_index = 0
status_message = ""


class S3Item:
    def __init__(self, name: str, is_dir: bool, full_path: str, size: int = 0):
        self.name = name
        self.is_dir = is_dir
        self.full_path = full_path
        self.size = size


def init_s3():
    global s3_client, status_message
    try:
        s3_client = boto3.client(
            "s3",
            region_name=region,
            endpoint_url=endpoint,
            aws_access_key_id=key,
            aws_secret_access_key=secret,
        )
        s3_client.head_bucket(Bucket=bucket)
        status_message = f"Connected to {bucket}"
        return True
    except Exception as e:
        status_message = f"Error: {str(e)}"
        return False


def list_current_directory() -> List[S3Item]:
    """List items in the current directory with directories first."""
    global status_message

    try:
        # Only fetch first batch for speed (no pagination)
        # MaxKeys limited to 300 for faster response
        response = s3_client.list_objects_v2(
            Bucket=bucket,
            Prefix=current_prefix,
            Delimiter='/',
            MaxKeys=300
        )

        items = []

        # Add subdirectories (CommonPrefixes)
        for prefix_obj in response.get('CommonPrefixes', []):
            full_path = prefix_obj['Prefix']
            # Extract just the directory name (remove current prefix and trailing /)
            name = full_path[len(current_prefix):].rstrip('/')
            if name:  # Skip empty names
                items.append(S3Item(name, True, full_path, 0))

        # Add files (Contents)
        for obj in response.get('Contents', []):
            key = obj['Key']
            # Skip the prefix itself if it's returned as an object
            if key == current_prefix or key.endswith('/'):
                continue

            # Extract just the file name
            name = key[len(current_prefix):]
            # Only add if it's a direct child (no slashes in remaining name)
            if '/' not in name and name:
                items.append(S3Item(name, False, key, obj.get('Size', 0)))

        # Sort: directories first, then files, both alphabetically
        items.sort(key=lambda x: (not x.is_dir, x.name.lower()))

        # Update status with truncation info
        if response.get('IsTruncated'):
            status_message = f"Showing first {len(items)} items (more available) in {current_prefix or '/'}"
        else:
            status_message = f"Found {len(items)} items in {current_prefix or '/'}"

        return items

    except Exception as e:
        status_message = f"Error listing: {str(e)}"
        return []


def format_size(size: int) -> str:
    """Format file size in human-readable format."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size < 1024.0:
            return f"{size:3.1f}{unit}"
        size /= 1024.0
    return f"{size:.1f}PB"


def is_previewable(filename: str) -> bool:
    """Check if file can be previewed as text."""
    text_extensions = ['.txt', '.csv', '.json', '.log', '.md', '.yml', '.yaml',
                       '.xml', '.html', '.css', '.js', '.py', '.sh', '.conf',
                       '.cfg', '.ini', '.sql', '.tsv']
    return any(filename.lower().endswith(ext) for ext in text_extensions)


def preview_file(stdscr, item: S3Item, max_lines: int = 20) -> None:
    """Preview a text file from S3."""
    global status_message

    height, width = stdscr.getmaxyx()

    # Show loading message
    stdscr.clear()
    stdscr.addstr(0, 0, f"Loading preview: {item.name}"[:width-1], curses.A_BOLD | curses.A_REVERSE)
    stdscr.addstr(2, 0, "Please wait...")
    stdscr.refresh()

    try:
        # Download the file (first part only for large files)
        # Get first 100KB to preview
        max_bytes = 100 * 1024

        response = s3_client.get_object(
            Bucket=bucket,
            Key=item.full_path,
            Range=f'bytes=0-{max_bytes-1}'
        )

        # Read and decode content
        content = response['Body'].read()

        # Try to decode as text
        try:
            text = content.decode('utf-8')
        except UnicodeDecodeError:
            try:
                text = content.decode('latin-1')
            except:
                text = "Unable to decode file as text"

        # Split into lines
        lines = text.split('\n')[:max_lines]

        # Draw preview screen
        while True:
            stdscr.clear()

            # Header
            header = f"Preview: {item.name} ({format_size(item.size)})"
            stdscr.addstr(0, 0, header[:width-1], curses.A_BOLD | curses.A_REVERSE)

            # Show lines
            for i, line in enumerate(lines):
                y_pos = i + 2
                if y_pos >= height - 2:
                    break

                # Truncate long lines
                display_line = line[:width-1] if len(line) < width else line[:width-4] + "..."

                try:
                    stdscr.addstr(y_pos, 0, display_line)
                except curses.error:
                    pass

            # Footer
            footer_y = height - 1
            if len(text.split('\n')) > max_lines:
                footer = f"Showing first {max_lines} lines | q/ESC:Close"
            else:
                footer = "q/ESC:Close"

            try:
                stdscr.addstr(footer_y, 0, footer[:width-1], curses.A_BOLD)
            except curses.error:
                pass

            stdscr.refresh()

            # Wait for key to close
            key = stdscr.getch()
            if key == ord('q') or key == ord('Q') or key == 27:  # ESC
                break

        status_message = f"Previewed {item.name}"

    except Exception as e:
        # Show error
        stdscr.clear()
        stdscr.addstr(0, 0, "Error loading preview"[:width-1], curses.A_BOLD | curses.A_REVERSE)
        stdscr.addstr(2, 0, str(e)[:width-1])
        stdscr.addstr(4, 0, "Press any key to continue...")
        stdscr.refresh()
        stdscr.getch()
        status_message = f"Error previewing: {str(e)}"


def draw_ui(stdscr, items: List[S3Item]):
    global selected_index, current_page, status_message

    stdscr.clear()
    height, width = stdscr.getmaxyx()

    # Header
    header = f"S3 Browser - {bucket}/{current_prefix}"
    stdscr.addstr(0, 0, header[:width-1], curses.A_BOLD | curses.A_REVERSE)

    # Calculate pagination
    total_pages = (len(items) - 1) // items_per_page + 1 if items else 1
    start_idx = current_page * items_per_page
    end_idx = min(start_idx + items_per_page, len(items))
    visible_items = items[start_idx:end_idx]

    # Draw items
    for i, item in enumerate(visible_items):
        y_pos = i + 2
        if y_pos >= height - 3:  # Leave room for footer
            break

        # Determine if this item is selected
        is_selected = (start_idx + i) == selected_index
        attr = curses.A_REVERSE if is_selected else curses.A_NORMAL

        # Format the line
        if item.is_dir:
            icon = "📁"
            line = f"{icon} {item.name}/"
        else:
            # Use different icon for previewable files
            if is_previewable(item.name):
                icon = "📝"
            else:
                icon = "📄"
            size_str = format_size(item.size)
            line = f"{icon} {item.name} ({size_str})"

        # Truncate if needed
        line = line[:width-1]

        try:
            stdscr.addstr(y_pos, 0, line, attr)
        except curses.error:
            pass

    # Navigation info
    nav_y = height - 2
    nav_info = f"Page {current_page + 1}/{total_pages} | Items {start_idx + 1}-{end_idx}/{len(items)}"
    try:
        stdscr.addstr(nav_y, 0, nav_info[:width-1])
    except curses.error:
        pass

    # Footer with controls
    footer_y = height - 1
    footer = "↑↓:Nav | ←:Back | →:Open | Space:Preview | PgUp/Dn:Page | r:Refresh | q:Quit"
    try:
        stdscr.addstr(footer_y, 0, footer[:width-1], curses.A_BOLD)
    except curses.error:
        pass

    # Status message
    if status_message:
        status_y = 1
        try:
            stdscr.addstr(status_y, 0, status_message[:width-1], curses.A_DIM)
        except curses.error:
            pass

    stdscr.refresh()


def main(stdscr):
    global selected_index, current_page, current_prefix, status_message

    # Initialize curses
    curses.curs_set(0)  # Hide cursor
    stdscr.keypad(True)
    stdscr.timeout(100)  # Non-blocking getch with 100ms timeout

    # Initialize colors if available
    if curses.has_colors():
        curses.init_pair(1, curses.COLOR_CYAN, curses.COLOR_BLACK)

    # Initialize S3 connection
    if not init_s3():
        stdscr.addstr(0, 0, f"Failed to connect: {status_message}")
        stdscr.addstr(1, 0, "Press any key to exit...")
        stdscr.refresh()
        stdscr.getch()
        return

    # Navigation stack for going back
    prefix_stack = []

    # Cache items to avoid re-fetching on every keypress
    items = []
    needs_refresh = True

    while True:
        # Only fetch items when needed
        if needs_refresh:
            status_message = "Loading..."
            draw_ui(stdscr, items)
            items = list_current_directory()
            needs_refresh = False

            # Ensure selected_index is valid
            if items:
                selected_index = max(0, min(selected_index, len(items) - 1))
                current_page = selected_index // items_per_page
            else:
                selected_index = 0
                current_page = 0

        draw_ui(stdscr, items)

        key = stdscr.getch()

        if key == -1:  # No key pressed (timeout)
            continue

        if key == ord('q') or key == ord('Q'):
            break
        elif key == ord('r') or key == ord('R'):  # Refresh
            needs_refresh = True
        elif key == ord(' ') and items:  # Space - Preview file
            if selected_index < len(items):
                item = items[selected_index]
                if not item.is_dir and is_previewable(item.name):
                    preview_file(stdscr, item)
                    needs_refresh = False  # Don't refresh after preview
                elif not item.is_dir:
                    status_message = f"Cannot preview {item.name} (not a text file)"
                else:
                    status_message = "Cannot preview directories"
        elif key == curses.KEY_UP and items:
            if selected_index > 0:
                selected_index -= 1
                current_page = selected_index // items_per_page
        elif key == curses.KEY_DOWN and items:
            if selected_index < len(items) - 1:
                selected_index += 1
                current_page = selected_index // items_per_page
        elif key == curses.KEY_NPAGE and items:  # Page Down
            new_index = min(selected_index + items_per_page, len(items) - 1)
            selected_index = new_index
            current_page = selected_index // items_per_page
        elif key == curses.KEY_PPAGE and items:  # Page Up
            new_index = max(selected_index - items_per_page, 0)
            selected_index = new_index
            current_page = selected_index // items_per_page
        elif key == curses.KEY_LEFT:  # Go back
            if prefix_stack:
                current_prefix = prefix_stack.pop()
                selected_index = 0
                current_page = 0
                needs_refresh = True
        elif (key == curses.KEY_RIGHT or key == ord('\n')) and items:  # Enter directory
            if selected_index < len(items):
                item = items[selected_index]
                if item.is_dir:
                    prefix_stack.append(current_prefix)
                    # Ensure the new prefix ends with /
                    current_prefix = item.full_path
                    if not current_prefix.endswith('/'):
                        current_prefix += '/'
                    selected_index = 0
                    current_page = 0
                    needs_refresh = True
                else:
                    status_message = f"File: {item.name} ({format_size(item.size)})"


if __name__ == "__main__":
    try:
        curses.wrapper(main)
    except KeyboardInterrupt:
        print("\nExiting...")
        sys.exit(0)

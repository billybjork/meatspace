#!/usr/bin/env python3

import argparse
import os
import subprocess
import sys
import shutil # For dependency checking (shutil.which)

# --- Configuration ---
# Suffix to add to the final QuickTime-compatible file
FINAL_SUFFIX = "_qt"

# ffmpeg settings for re-encoding
# Use lists for arguments to avoid shell injection issues
FFMPEG_ARGS = [
    "-map", "0", # Map all streams from input
    "-c:v:0", "libx264", "-preset", "fast", "-crf", "20", # Target 1st video stream
    "-c:a:0", "aac", "-b:a", "192k",                    # Target 1st audio stream
    "-c:v:1", "copy",                                   # Copy 2nd video stream (thumbnail)
    "-movflags", "+faststart",                           # Optimize for streaming
]

# yt-dlp output template (base name without extension)
# --restrict-filenames is used later to ensure safe names
OUTPUT_TEMPLATE_BASE = '%(title)s [%(id)s]'

# yt-dlp download arguments
YTDLP_ARGS = [
    "-f", "bv*+ba/b",              # Best video + best audio / best merged
    "--merge-output-format", "mp4", # Merge into mp4 container
    "--embed-metadata",            # Embed available metadata
    "--embed-thumbnail",           # Embed thumbnail
    "--convert-thumbnails", "jpg",  # Ensure thumbnail is JPG before embedding
    "--restrict-filenames",        # Ensure safe filenames
    # Output template will be dynamically set later
    "--ignore-config",             # Avoid interference from user config
    "--no-cache-dir",              # Avoid potential cache issues
]
# ---------------------

def run_command(cmd_list, step_name="Command", check_returncode=True, capture_output=False):
    """Runs an external command using subprocess."""
    print(f"--- Running: {step_name} ---")
    # print(f"Executing: {' '.join(cmd_list)}") # Uncomment for debugging the exact command
    try:
        process = subprocess.run(
            cmd_list,
            check=check_returncode, # Raise CalledProcessError if return code is non-zero
            capture_output=capture_output,
            text=True, # Work with strings not bytes
            encoding='utf-8'
        )
        if capture_output:
            # print(f"STDOUT:\n{process.stdout}") # Uncomment for debugging output
            # print(f"STDERR:\n{process.stderr}") # Uncomment for debugging errors
            return process.stdout.strip() # Return stripped stdout
        else:
            # For commands where we don't capture, stderr might still print useful info
            # (e.g., ffmpeg progress) directly to the console. Stdout might too.
            pass
        return True # Indicate success
    except FileNotFoundError:
        print(f"Error: Command not found: '{cmd_list[0]}'. Is it installed and in your PATH?", file=sys.stderr)
        sys.exit(1)
    except subprocess.CalledProcessError as e:
        print(f"Error during '{step_name}': Command '{' '.join(e.cmd)}' returned non-zero exit status {e.returncode}.", file=sys.stderr)
        if e.stderr:
            print(f"STDERR:\n{e.stderr}", file=sys.stderr)
        if e.stdout:
             print(f"STDOUT:\n{e.stdout}", file=sys.stderr) # Sometimes error details are on stdout
        return False # Indicate failure
    except Exception as e:
        print(f"An unexpected error occurred during '{step_name}': {e}", file=sys.stderr)
        return False

def main():
    """Main script execution."""
    parser = argparse.ArgumentParser(
        description="Downloads and converts a YouTube video to QuickTime-compatible H.264/AAC MP4, embedding metadata and thumbnail.",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("video_url", help="The URL of the YouTube video to process.")
    args = parser.parse_args()
    video_url = args.video_url

    print(f"Processing URL: {video_url}")

    # --- Dependency Check ---
    print("--- Checking Dependencies ---")
    if not shutil.which("yt-dlp"):
        print("Error: 'yt-dlp' command not found. Please install it.", file=sys.stderr)
        sys.exit(1)
    if not shutil.which("ffmpeg"):
        print("Error: 'ffmpeg' command not found. Please install it.", file=sys.stderr)
        sys.exit(1)
    print("Dependencies (yt-dlp, ffmpeg) found.")
    print("---")

    # --- Determine Filenames ---
    print("[Step 0/3] Determining output filenames...")
    cmd_get_filename = [
        "yt-dlp",
        "--get-filename",
        "--restrict-filenames",
        "-o", OUTPUT_TEMPLATE_BASE,
        video_url
    ]
    intermediate_file_base = run_command(cmd_get_filename, "Get Filename", capture_output=True)

    if not intermediate_file_base:
        print("Error: Could not determine filename base for URL. yt-dlp --get-filename failed.", file=sys.stderr)
        sys.exit(1)

    # Add extensions
    intermediate_file = f"{intermediate_file_base}.mp4"
    final_file = f"{intermediate_file_base}{FINAL_SUFFIX}.mp4"

    print(f"Intermediate file will be: {intermediate_file}")
    print(f"Final file will be:        {final_file}")
    print("---")

    # --- Step 1: Download with yt-dlp ---
    print("[Step 1/3] Downloading video, embedding metadata & thumbnail...")
    # Construct the full command list for download
    cmd_download = YTDLP_ARGS + ["-o", f"{intermediate_file_base}.%(ext)s", video_url]

    if not run_command(cmd_download, "yt-dlp Download"):
        print("Error: yt-dlp download (Step 1) failed.", file=sys.stderr)
        sys.exit(1)

    # --- Sanity Check: Ensure Intermediate File Exists ---
    if not os.path.exists(intermediate_file):
        print(f"Error: Intermediate file '{intermediate_file}' was not found after yt-dlp supposedly finished.", file=sys.stderr)
        # This case might indicate an issue with yt-dlp's output naming or merging
        sys.exit(1)
    print(f"[Step 1/3] Download complete. Intermediate file: {intermediate_file}")
    print("---")

    # --- Step 2: Re-encode with ffmpeg ---
    print("[Step 2/3] Re-encoding for QuickTime compatibility (H.264/AAC)...")
    cmd_ffmpeg = [
        "ffmpeg",
        "-i", intermediate_file,
    ] + FFMPEG_ARGS + [ # Add the list of ffmpeg args
        final_file
    ]

    if not run_command(cmd_ffmpeg, "ffmpeg Re-encoding"):
        print("Error: ffmpeg re-encoding (Step 2) failed.", file=sys.stderr)
        print(f"Intermediate file '{intermediate_file}' has been kept for inspection.")
        sys.exit(1)
    print(f"[Step 2/3] Re-encoding complete. Final file: {final_file}")
    print("---")

    # --- Step 3: Cleanup ---
    print("[Step 3/3] Cleaning up intermediate file...")
    try:
        os.remove(intermediate_file)
        print(f"[Step 3/3] Cleanup complete. Deleted '{intermediate_file}'.")
    except OSError as e:
        # Log warning but don't treat as fatal error for the overall process
        print(f"Warning: Failed to delete intermediate file '{intermediate_file}': {e}", file=sys.stderr)
    print("---")

    print(f"Process finished successfully for {video_url}")
    print(f"Output file: {final_file}")
    sys.exit(0)

if __name__ == "__main__":
    main()
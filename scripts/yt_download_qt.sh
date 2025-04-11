#!/bin/bash

# === Configuration ===
# Suffix to add to the final QuickTime-compatible file
FINAL_SUFFIX="_qt"

# ffmpeg settings for re-encoding (adjust CRF, preset, audio bitrate if needed)
FFMPEG_VCODEC_ARGS="-c:v:0 libx264 -preset fast -crf 20" # Target first video stream
FFMPEG_ACODEC_ARGS="-c:a:0 aac -b:a 192k"              # Target first audio stream
FFMPEG_THUMB_ARGS="-c:v:1 copy"                         # Copy second video stream (thumbnail)

# === Input Validation ===
if [ -z "$1" ]; then
  echo "Usage: $0 <youtube_video_url>"
  echo "       Downloads and converts a YouTube video to QuickTime-compatible H.264/AAC MP4,"
  echo "       embedding metadata and thumbnail."
  exit 1
fi
VIDEO_URL="$1"
echo "Processing URL: $VIDEO_URL"

# === Determine Filenames ===
# Define the base template yt-dlp will use (without extension initially)
# Using --restrict-filenames ensures safer names for shell processing
OUTPUT_TEMPLATE_BASE='%(title)s [%(id)s]'
echo "Determining intermediate filename..."
INTERMEDIATE_FILE_BASE=$(yt-dlp --get-filename --restrict-filenames -o "$OUTPUT_TEMPLATE_BASE" "$VIDEO_URL" 2>/dev/null)

# Check if filename generation succeeded
if [ -z "$INTERMEDIATE_FILE_BASE" ]; then
    echo "Error: Could not determine filename base for URL."
    # Optional: Rerun with verbose output to debug filename issue
    # yt-dlp --get-filename --restrict-filenames -o "$OUTPUT_TEMPLATE_BASE" "$VIDEO_URL" -v
    exit 1
fi

# Add the .mp4 extension (yt-dlp's --merge-output-format ensures this)
INTERMEDIATE_FILE="${INTERMEDIATE_FILE_BASE}.mp4"
# Define the final filename using the suffix
FINAL_FILE="${INTERMEDIATE_FILE_BASE}${FINAL_SUFFIX}.mp4"

echo "Intermediate file will be: $INTERMEDIATE_FILE"
echo "Final file will be:        $FINAL_FILE"
echo "---"

# === Step 1: Download with yt-dlp (Embed Metadata & Converted Thumbnail) ===
echo "[Step 1/3] Downloading video, embedding metadata & thumbnail..."
# Use the full template with extension for the download command
# Ensure --restrict-filenames is used here too for the actual download
if ! yt-dlp \
    -f 'bv*+ba/b' \
    --merge-output-format mp4 \
    --embed-metadata \
    --embed-thumbnail \
    --convert-thumbnails jpg \
    --restrict-filenames \
    -o "${OUTPUT_TEMPLATE_BASE}.%(ext)s" \
    --ignore-config \
    --no-cache-dir \
    "$VIDEO_URL"; then
    echo "Error: yt-dlp download (Step 1) failed."
    exit 1
fi

# === Sanity Check: Ensure Intermediate File Exists ===
if [ ! -f "$INTERMEDIATE_FILE" ]; then
    echo "Error: Intermediate file '$INTERMEDIATE_FILE' was not found after yt-dlp supposedly finished."
    exit 1
fi
echo "[Step 1/3] Download complete. Intermediate file: $INTERMEDIATE_FILE"
echo "---"

# === Step 2: Re-encode with ffmpeg (Keep Metadata/Thumbnail) ===
echo "[Step 2/3] Re-encoding for QuickTime compatibility (H.264/AAC)..."
# Use explicit stream specifiers (-c:v:0, -c:a:0, -c:v:1)
if ! ffmpeg \
    -i "$INTERMEDIATE_FILE" \
    -map 0 \
    $FFMPEG_VCODEC_ARGS \
    $FFMPEG_ACODEC_ARGS \
    $FFMPEG_THUMB_ARGS \
    -movflags +faststart \
    "$FINAL_FILE"; then
    echo "Error: ffmpeg re-encoding (Step 2) failed."
    echo "Intermediate file '$INTERMEDIATE_FILE' has been kept for inspection."
    exit 1
fi
echo "[Step 2/3] Re-encoding complete. Final file: $FINAL_FILE"
echo "---"

# === Step 3: Cleanup ===
echo "[Step 3/3] Cleaning up intermediate file..."
rm -f "$INTERMEDIATE_FILE"
if [ $? -ne 0 ]; then
    echo "Warning: Failed to delete intermediate file '$INTERMEDIATE_FILE'."
else
    echo "[Step 3/3] Cleanup complete."
fi
echo "---"

echo "Process finished successfully for $VIDEO_URL"
echo "Output file: $FINAL_FILE"
exit 0
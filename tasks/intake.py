import os
import shutil
import subprocess
import tempfile
import json
from pathlib import Path

from prefect import task, get_run_logger
import psycopg2
from psycopg2 import sql
import yt_dlp  # Use the library

# Import utils from the 'utils' directory at the project root
try:
    from utils.db_utils import get_db_connection, get_media_base_dir
except ImportError:
    raise ImportError("Could not import from utils.db_utils. Ensure utils/__init__.py exists and PYTHONPATH is set correctly.")

# --- Task Configuration ---
# Suffix to add to the final QuickTime-compatible file if re-encoded
FINAL_SUFFIX = "_qt"

# ffmpeg settings for re-encoding (QuickTime compatibility)
FFMPEG_ARGS = [
    "-map", "0",  # Map all streams from input
    "-c:v:0", "libx264", "-preset", "fast", "-crf", "20", "-pix_fmt", "yuv420p",  # Target 1st video stream (H.264)
    "-c:a:0", "aac", "-b:a", "192k",  # Target 1st audio stream (AAC)
    "-c:s", "mov_text",  # Convert subtitles to mov_text if present
    "-c:d", "copy",  # Copy data streams
    "-c:v:1", "copy",  # Attempt to copy 2nd video stream (often thumbnail)
    # Add other streams if needed (-c:v:2 copy etc.)
    "-movflags", "+faststart",  # Optimize for streaming
]


def run_external_command(cmd_list, step_name="Command", cwd=None):
    """
    Runs an external command using subprocess, logs output, and raises errors.
    Uses Prefect logger.
    """
    logger = get_run_logger()
    cmd_str = ' '.join(cmd_list)
    logger.info(f"Running {step_name}: {cmd_str}")
    try:
        # Using PIPE for stdout/stderr allows capturing output for logging
        process = subprocess.Popen(
            cmd_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, encoding='utf-8', cwd=cwd
        )
        stdout, stderr = process.communicate()  # Wait for process to finish

        if process.returncode != 0:
            logger.error(f"Error during '{step_name}': Command failed with exit code {process.returncode}.")
            if stderr:
                logger.error(f"STDERR:\n{stderr}")
            if stdout:
                logger.error(f"STDOUT:\n{stdout}")  # Sometimes errors are on stdout
            raise subprocess.CalledProcessError(process.returncode, cmd_list, output=stdout, stderr=stderr)
        return True  # Indicate success

    except FileNotFoundError:
        logger.error(f"Error during '{step_name}': Command not found: '{cmd_list[0]}'. Is it installed and in PATH?")
        raise
    except Exception as e:
        logger.error(f"An unexpected error occurred during '{step_name}': {e}", exc_info=True)
        raise


@task(name="Intake Source Video", retries=1, retry_delay_seconds=30)
def intake_task(source_video_id: int,
                input_source: str,  # URL or local file path
                re_encode_for_qt: bool = True,  # Whether to run ffmpeg re-encoding step
                overwrite_existing: bool = False  # Allow overwriting existing files/DB entries
                ):
    """
    Intakes a source video: downloads (if URL) or processes a local file,
    optionally re-encodes for QuickTime compatibility (H.264/AAC), moves
    it to the final media location, and updates the source_videos DB record.
    """
    logger = get_run_logger()
    logger.info(f"TASK [Intake]: Starting for source_video_id: {source_video_id}, Input: '{input_source}'")
    conn = None
    media_base_dir = Path(get_media_base_dir())  # Use pathlib
    source_videos_dir = media_base_dir / "source_videos"
    source_videos_dir.mkdir(parents=True, exist_ok=True)  # Ensure base dir exists

    initial_temp_filepath = None  # Path within temp_dir to the file before potential re-encoding
    final_rel_path = None  # Relative path for DB storage
    metadata = {}  # To store extracted metadata (duration, width, etc.)
    temp_dir_obj = None  # To hold temporary directory object

    # --- Dependency Check ---
    if re_encode_for_qt or not input_source.lower().startswith(('http://', 'https://')):
        # Need ffmpeg/ffprobe if re-encoding OR if processing local file (for metadata)
        if not shutil.which("ffmpeg"):
            logger.error("Dependency 'ffmpeg' not found in PATH.")
            raise FileNotFoundError("ffmpeg is required but not found.")
        if not shutil.which("ffprobe"):
            logger.error("Dependency 'ffprobe' not found in PATH.")
            raise FileNotFoundError("ffprobe is required but not found.")
        logger.info("Checked dependencies (ffmpeg, ffprobe).")

    try:
        # --- Database Connection and Transaction Setup ---
        conn = get_db_connection()
        conn.autocommit = False  # Start transaction explicitly

        with conn.cursor() as cur:
            # === Transaction Start ===

            # 1. Acquire Advisory Lock (Domain 1 for source_videos)
            try:
                cur.execute("SELECT pg_advisory_xact_lock(1, %s)", (source_video_id,))
                logger.info(f"Acquired DB lock for source_video_id: {source_video_id}")
            except Exception as lock_err:
                logger.error(f"Failed to acquire DB lock for source_video_id {source_video_id}: {lock_err}")
                raise RuntimeError("DB Lock acquisition failed") from lock_err

            # 2. Check current state and update to processing state
            cur.execute("SELECT ingest_state, filepath FROM source_videos WHERE id = %s", (source_video_id,))
            result = cur.fetchone()
            if not result:
                raise ValueError(f"Source video with ID {source_video_id} not found in database.")
            current_state, existing_filepath = result

            # Allow processing only if 'new' or if overwrite is forced
            if current_state != 'new' and not overwrite_existing:
                logger.warning(
                    f"Source video {source_video_id} is not in 'new' state ('{current_state}') and overwrite=False. Skipping intake."
                )
                conn.rollback()  # Release lock and rollback any potential implicit changes
                return {
                    "status": "skipped",
                    "reason": f"Already processed (state: {current_state})",
                    "filepath": existing_filepath
                }

            # --- Determine Input Type and Set Initial State ---
            is_url = input_source.lower().startswith(('http://', 'https://'))
            new_state_during_processing = 'downloading' if is_url else 'processing_local'

            cur.execute(
                "UPDATE source_videos SET ingest_state = %s, retry_count = 0, last_error = NULL, updated_at = NOW() WHERE id = %s",
                (new_state_during_processing, source_video_id)
            )
            logger.info(f"Set state to '{new_state_during_processing}' for source_video_id: {source_video_id}")

            # === Core Processing in Temporary Directory ===
            temp_dir_obj = tempfile.TemporaryDirectory(prefix=f"meatspace_intake_{source_video_id}_")
            temp_dir = Path(temp_dir_obj.name)
            logger.info(f"Using temporary directory: {temp_dir}")

            base_filename_no_ext = "unknown_video"  # Default base name

            if is_url:
                # --- Handle URL Download ---
                logger.info("Input is URL. Processing with yt-dlp...")
                output_template = str(temp_dir / '%(title)s [%(id)s].%(ext)s')

                # Custom logger for yt-dlp to integrate with Prefect
                class YtdlpLogger:
                    def debug(self, msg):
                        # Skip verbose debug messages unless needed
                        if msg.startswith('[debug] '):
                            pass
                        else:
                            logger.debug(msg)

                    def warning(self, msg):
                        logger.warning(msg)

                    def error(self, msg):
                        logger.error(msg)

                    def info(self, msg):
                        logger.info(msg)  # Map yt-dlp info to Prefect logger

                ydl_opts = {
                    'format': 'bv*[ext=mp4]+ba[ext=m4a]/b[ext=mp4] / bv*+ba/b',  # Prefer mp4 if available, fallback
                    'outtmpl': output_template,
                    'merge_output_format': 'mp4',
                    'embedmetadata': True,
                    'embedthumbnail': True,
                    'postprocessors': [{'key': 'FFmpegThumbnailsConvertor', 'format': 'jpg'}],
                    'restrictfilenames': True,
                    'ignoreerrors': False,  # Fail task on download errors
                    'logger': YtdlpLogger(),
                    'progress_hooks': [
                        lambda d: logger.info(
                            f"yt-dlp status: {d.get('status')} {d.get('_percent_str', '')} "
                            f"{d.get('_speed_str', '')} ETA {d.get('_eta_str', '')}"
                        ) if d.get('status') == 'downloading' else None
                    ],
                    'noprogress': False,  # Show progress
                    # Consider adding user agent if needed: 'http_headers': {'User-Agent': 'your-app/1.0'}
                }

                try:
                    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                        logger.info(f"Extracting info for {input_source}...")
                        info_dict = ydl.extract_info(input_source, download=False)
                        video_title = info_dict.get('title', 'Unknown Title')
                        logger.info(f"Downloading video: {video_title}...")
                        ydl.download([input_source])  # Perform download

                    downloaded_files = list(temp_dir.glob('*.mp4'))  # Find downloaded file(s)
                    if not downloaded_files:
                        raise FileNotFoundError("yt-dlp finished but no MP4 output file found in temp directory.")
                    initial_temp_filepath = downloaded_files[0]  # Assume first mp4 is the main one
                    logger.info(f"Download complete. Initial temp file: {initial_temp_filepath}")

                    # Extract metadata needed for DB
                    metadata['title'] = video_title
                    metadata['duration'] = info_dict.get('duration')
                    metadata['width'] = info_dict.get('width')
                    metadata['height'] = info_dict.get('height')
                    metadata['fps'] = info_dict.get('fps')
                    metadata['original_url'] = info_dict.get('webpage_url', input_source)
                    metadata['extractor'] = info_dict.get('extractor_key')
                    metadata['upload_date'] = info_dict.get('upload_date')  # YYYYMMDD format

                    # Determine a safe base filename (without extension)
                    base_filename_no_ext = Path(ydl.prepare_filename(info_dict, outtmpl='%(title)s [%(id)s]')).stem

                except yt_dlp.utils.DownloadError as e:
                    logger.error(f"yt-dlp DownloadError: {e}")
                    raise
                except Exception as e:
                    logger.error(f"Error during yt-dlp processing: {e}", exc_info=True)
                    raise

            else:
                # --- Handle Local File Path ---
                logger.info(f"Input is a local path: {input_source}")
                input_path = Path(input_source).resolve()  # Get absolute path
                if not input_path.is_file():
                    raise FileNotFoundError(f"Provided local file path does not exist: {input_path}")

                # Copy the file to the temp directory to work on it
                initial_temp_filepath = temp_dir / input_path.name
                shutil.copy2(input_path, initial_temp_filepath)  # copy2 preserves metadata
                logger.info(f"Copied local file to temp location: {initial_temp_filepath}")

                base_filename_no_ext = input_path.stem  # Base name from file
                metadata['title'] = base_filename_no_ext  # Default title

                # Attempt metadata extraction using ffprobe
                try:
                    ffprobe_cmd = [
                        "ffprobe", "-v", "error", "-show_format", "-show_streams",
                        "-of", "json", str(initial_temp_filepath)
                    ]
                    process = subprocess.run(
                        ffprobe_cmd, check=True, capture_output=True,
                        text=True, encoding='utf-8'
                    )
                    probe_data = json.loads(process.stdout)

                    video_stream = next(
                        (s for s in probe_data.get('streams', []) if s.get('codec_type') == 'video'),
                        None
                    )
                    if video_stream:
                        metadata['width'] = video_stream.get('width')
                        metadata['height'] = video_stream.get('height')
                        fr = video_stream.get('avg_frame_rate', '0/1')
                        if '/' in fr:
                            num, den = map(int, fr.split('/'))
                            if den > 0:
                                metadata['fps'] = num / den
                    if 'format' in probe_data:
                        metadata['duration'] = float(probe_data['format'].get('duration', 0))
                        # Get creation time if available (example tag)
                        creation_time = probe_data['format'].get('tags', {}).get('creation_time')
                        if creation_time:
                            metadata['creation_time'] = creation_time  # Store as string for now

                    logger.info(f"Attempted metadata extraction from local file: {metadata}")
                except Exception as probe_err:
                    logger.warning(
                        f"Could not extract metadata using ffprobe for {initial_temp_filepath}: {probe_err}. "
                        "Metadata may be incomplete."
                    )

            # --- Determine Final File Path & Check Existence ---
            final_ext = ".mp4"  # Assume mp4 output
            final_filename_no_suffix = f"{base_filename_no_ext}{final_ext}"
            final_filename_with_suffix = f"{base_filename_no_ext}{FINAL_SUFFIX}{final_ext}"

            # Decide if re-encoding is needed/requested
            perform_re_encode = re_encode_for_qt
            final_filename = final_filename_with_suffix if perform_re_encode else final_filename_no_suffix

            # Special case: if input was local and already had the target suffix, don't re-encode
            if not is_url and input_source.endswith(final_filename_with_suffix) and re_encode_for_qt:
                logger.info(
                    f"Local input file '{input_source}' already has the target suffix '{FINAL_SUFFIX}'. Skipping re-encode."
                )
                perform_re_encode = False
                final_filename = final_filename_with_suffix  # Use the name it already has

            final_abs_path = source_videos_dir / final_filename
            final_rel_path = Path(os.path.relpath(final_abs_path, media_base_dir))  # Convert to relative path

            logger.info(f"Target final path: {final_abs_path}")
            logger.info(f"Target relative path for DB: {final_rel_path}")

            # Check if final file exists before potentially expensive re-encode
            if final_abs_path.exists() and not overwrite_existing:
                logger.warning(
                    f"Final file {final_abs_path} already exists and overwrite=False. "
                    "Updating DB record to 'downloaded' and skipping processing."
                )
                # Update DB state assuming the existing file is correct, keeping existing timestamps if possible
                cur.execute(
                    """
                    UPDATE source_videos
                    SET ingest_state = 'downloaded', filepath = %s, filename = %s,
                        title = COALESCE(title, %s), duration_seconds = COALESCE(duration_seconds, %s),
                        width = COALESCE(width, %s), height = COALESCE(height, %s), fps = COALESCE(fps, %s),
                        downloaded_at = COALESCE(downloaded_at, NOW()),
                        updated_at = NOW(), last_error = NULL
                    WHERE id = %s
                    """,
                    (str(final_rel_path), final_filename, metadata.get('title'), metadata.get('duration'),
                     metadata.get('width'), metadata.get('height'), metadata.get('fps'), source_video_id)
                )
                conn.commit()
                if temp_dir_obj:
                    temp_dir_obj.cleanup()
                return {"status": "skipped", "reason": "Final file already exists", "filepath": str(final_rel_path)}

            # --- Optional Re-encoding Step ---
            file_to_move = initial_temp_filepath  # Start with the initial temp file

            if perform_re_encode:
                logger.info(f"Starting ffmpeg re-encoding ({' -> '.join(FFMPEG_ARGS[:5])}...).")
                encoded_temp_path = temp_dir / f"{base_filename_no_ext}_encoded_temp.mp4"

                ffmpeg_cmd = [
                    "ffmpeg", "-y",  # Overwrite temp output
                    "-i", str(initial_temp_filepath)  # Input is temp file
                ] + FFMPEG_ARGS + [
                    str(encoded_temp_path)  # Output to temp file
                ]

                try:
                    run_external_command(ffmpeg_cmd, "ffmpeg Re-encoding")
                    logger.info(f"Re-encoding successful. Output: {encoded_temp_path}")
                    file_to_move = encoded_temp_path
                except Exception as encode_err:
                    logger.error(f"ffmpeg re-encoding failed: {encode_err}")
                    raise RuntimeError("FFmpeg re-encoding failed") from encode_err
            else:
                logger.info("Skipping re-encoding step.")

            # --- Move Processed File to Final Location ---
            logger.info(f"Moving '{file_to_move.name}' to final destination '{final_abs_path}'")
            try:
                final_abs_path.parent.mkdir(parents=True, exist_ok=True)  # Ensure target dir exists
                shutil.move(str(file_to_move), str(final_abs_path))
                logger.info("Move successful.")
            except Exception as move_err:
                logger.error(f"Failed to move file to final destination '{final_abs_path}': {move_err}")
                if final_abs_path.exists():
                    logger.warning(f"Attempting to remove potentially incomplete file at final destination: {final_abs_path}")
                    try:
                        final_abs_path.unlink()
                    except OSError:
                        logger.error("Failed to remove incomplete file.")
                raise RuntimeError("Failed to move processed file") from move_err

            # --- Update DB Record on Success ---
            logger.info("Updating database record with final details...")
            db_title = metadata.get('title')
            db_duration = metadata.get('duration')
            db_width = metadata.get('width')
            db_height = metadata.get('height')
            db_fps = metadata.get('fps')
            db_original_url = metadata.get('original_url')
            db_upload_date = metadata.get('upload_date')

            cur.execute(
                """
                UPDATE source_videos
                SET ingest_state = 'downloaded',
                    filepath = %s,
                    filename = %s,
                    title = COALESCE(title, %s),
                    duration_seconds = %s,
                    width = %s,
                    height = %s,
                    fps = %s,
                    original_url = COALESCE(original_url, %s),
                    upload_date = %s,
                    downloaded_at = NOW(),
                    updated_at = NOW(),
                    last_error = NULL
                WHERE id = %s
                """,
                (str(final_rel_path), final_filename, db_title, db_duration,
                 db_width, db_height, db_fps, db_original_url, db_upload_date, source_video_id)
            )

            conn.commit()
            logger.info(f"TASK [Intake]: Successfully processed source_video_id: {source_video_id}. Final file: {final_rel_path}")

            return {"status": "success", "filepath": str(final_rel_path), "metadata": metadata}

    except Exception as e:
        task_name = "Intake"
        logger.error(f"TASK FAILED ({task_name}): source_video_id {source_video_id} - {e}", exc_info=True)
        if conn:
            try:
                conn.rollback()
                logger.info("Database transaction rolled back.")
                conn.autocommit = True
                with conn.cursor() as err_cur:
                    err_cur.execute(
                        """
                        UPDATE source_videos
                        SET ingest_state = 'download_failed',
                            last_error = %s,
                            retry_count = retry_count + 1,
                            updated_at = NOW()
                        WHERE id = %s
                        """,
                        (f"{type(e).__name__}: {str(e)[:500]}", source_video_id)
                    )
                logger.info(f"Updated DB state to 'download_failed' for source_video_id: {source_video_id}")
            except Exception as db_err:
                logger.error(f"Failed to update error state in DB after rollback: {db_err}")
        raise

    finally:
        if conn:
            conn.autocommit = True
            conn.close()
            logger.debug(f"DB connection closed for source_video_id: {source_video_id}")
        if temp_dir_obj:
            temp_dir_obj.cleanup()
            logger.info("Cleaned up temporary directory.")
import json
import os
import shutil
import subprocess
import sys
import tempfile
import threading
from datetime import datetime
from pathlib import Path
import logging

import boto3
from botocore.exceptions import ClientError, NoCredentialsError

from prefect import get_run_logger, task
import yt_dlp

# --- Local Imports ---
try:
    from db.sync_db import get_db_connection, release_db_connection
except ImportError:
    # Standard fallback logic
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
    try:
        from db.sync_db import get_db_connection, release_db_connection
    except ImportError as e:
        print(f"ERROR importing db_utils in intake.py: {e}")
        def get_db_connection():
            raise NotImplementedError("Dummy get_db_connection")
        def release_db_connection(conn):
            raise NotImplementedError("Dummy release_db_connection")


# --- Task Configuration ---
FINAL_SUFFIX = "_qt"
FFMPEG_ARGS = [
    "-map", "0",
    "-c:v:0", "libx264", "-preset", "fast", "-crf", "20", "-pix_fmt", "yuv420p",
    "-c:a:0", "aac", "-b:a", "192k",
    "-c:s", "mov_text",
    "-c:d", "copy",
    "-c:v:1", "copy",  # Attempt to copy 2nd video stream (e.g., thumbnail)
    "-movflags", "+faststart",
]

# --- S3 Configuration ---
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
AWS_REGION = os.getenv("AWS_REGION", "us-west-1")

# Validate required env vars
if not S3_BUCKET_NAME:
    raise ValueError("Missing required environment variable: S3_BUCKET_NAME")

# --- Initialize S3 Client ---
s3_client = None # Initialize to None
try:
    s3_client = boto3.client("s3", region_name=AWS_REGION)
    print(f"Initialized S3 client for bucket: {S3_BUCKET_NAME} in region: {AWS_REGION}")
except NoCredentialsError:
    print("AWS credentials not found. Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY or use IAM roles.")
    # Error will be caught later in the task if s3_client is None
except Exception as e:
    print(f"Failed to initialize S3 client: {e}")
    # Error will be caught later in the task if s3_client is None


# --- Helper Function for External Commands ---
def run_external_command(cmd_list, step_name="Command", cwd=None):
    """Runs an external command using subprocess, logs output, and raises errors."""
    logger = get_run_logger()
    cmd_str = ' '.join(cmd_list)
    logger.info(f"Running {step_name}: {cmd_str}")
    try:
        process = subprocess.Popen(
            cmd_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, encoding='utf-8', errors='replace', cwd=cwd
        )
        stdout, stderr = process.communicate()

        if process.returncode != 0:
            logger.error(f"Error during '{step_name}': Command failed with exit code {process.returncode}.")
            if stderr: logger.error(f"STDERR (last 1000 chars):\n{stderr[-1000:]}")
            if stdout: logger.error(f"STDOUT (last 1000 chars):\n{stdout[-1000:]}")
            raise subprocess.CalledProcessError(process.returncode, cmd_list, output=stdout, stderr=stderr)
        else:
            pass # Success
        return True

    except FileNotFoundError:
        logger.error(f"Error during '{step_name}': Command not found: '{cmd_list[0]}'. Is it installed and in PATH?")
        raise
    except Exception as e:
        logger.error(f"An unexpected error occurred during '{step_name}': {e}", exc_info=True)
        raise


# --- S3 Upload Progress Callback Class ---
class S3TransferProgress:
    """ Callback for boto3 reporting progress via Prefect logger """
    def __init__(self, total_size, filename, logger, throttle_percentage=5):
        self._filename = filename
        self._total_size = total_size
        self._seen_so_far = 0
        self._lock = threading.Lock()
        self._logger = logger or get_run_logger()
        try:
            self._throttle_percentage = max(1, min(int(throttle_percentage), 100))
        except (ValueError, TypeError):
            self._logger.warning(f"Invalid throttle_percentage '{throttle_percentage}', defaulting to 5.")
            self._throttle_percentage = 5
        self._last_logged_percentage = -1

    def __call__(self, bytes_amount):
        try:
            with self._lock:
                self._seen_so_far += bytes_amount
                current_percentage = 0
                if isinstance(self._total_size, (int, float)) and self._total_size > 0:
                    try:
                        current_percentage = int((self._seen_so_far / self._total_size) * 100)
                    except Exception as calc_err:
                         self._logger.warning(f"S3 Progress ({self._filename}): Error calculating percentage: {calc_err}")
                         current_percentage = 0
                elif self._total_size == 0:
                    current_percentage = 100

                should_log = False
                try:
                    last_logged_int = int(self._last_logged_percentage)
                    throttle_int = int(self._throttle_percentage)
                    should_log = (
                        (current_percentage >= last_logged_int + throttle_int and current_percentage < 100) or
                        (current_percentage == 100 and last_logged_int != 100)
                    )
                except Exception as comp_err:
                    self._logger.warning(f"S3 Progress ({self._filename}): Error during logging comparison: {comp_err}")
                    should_log = False

                if should_log:
                    try:
                        size_mb = float(self._seen_so_far) / (1024 * 1024)
                        total_size_mb = float(self._total_size) / (1024 * 1024) if isinstance(self._total_size, (int, float)) and self._total_size > 0 else 0.0
                        self._logger.info(
                            f"S3 Upload: {self._filename} - {current_percentage}% complete "
                            f"({size_mb:.2f}/{total_size_mb:.2f} MiB)"
                        )
                        self._last_logged_percentage = current_percentage
                    except Exception as log_fmt_err:
                        self._logger.warning(f"S3 Progress ({self._filename}): Error formatting log message: {log_fmt_err}")

                elif self._total_size == 0 and self._seen_so_far == 0 and self._last_logged_percentage == -1:
                    self._logger.info(f"S3 Upload: {self._filename} - 100% complete (0.00/0.00 MiB)")
                    self._last_logged_percentage = 100

        except Exception as callback_err:
            self._logger.error(f"S3 Progress ({self._filename}): Unexpected error in progress callback: {callback_err}", exc_info=True)


# --- Prefect Task ---
@task(name="Intake Source Video", retries=1, retry_delay_seconds=30)
def intake_task(source_video_id: int,
                input_source: str,
                re_encode_for_qt: bool = True,
                overwrite_existing: bool = False
                ):
    """
    Intakes a source video: downloads/copies, re-encodes, uploads to S3, updates DB.
    """
    logger = get_run_logger()
    logger.info(f"TASK [Intake]: Starting for source_video_id: {source_video_id}, Input: '{input_source}'")
    conn = None
    initial_temp_filepath = None
    s3_object_key = None
    final_filename_for_db = None
    metadata = {}
    temp_dir_obj = None
    task_outcome = "failed" # Default outcome
    error_message = None # Store error message for DB update if needed

    # --- Dependency Check ---
    if not s3_client:
        logger.error("S3 client is not available (failed during initialization). Cannot proceed.")
        error_message = "S3 client failed to initialize. Check credentials/config."
        # Raise the error here to fail the task immediately
        raise RuntimeError(error_message)

    is_url = input_source.lower().startswith(('http://', 'https://'))
    if re_encode_for_qt or not is_url: # Check ffmpeg only if needed
        if not shutil.which("ffmpeg"):
            error_message = "Dependency 'ffmpeg' not found in PATH."
            logger.error(error_message)
            raise FileNotFoundError(error_message)
    if not is_url: # Check ffprobe only for local files
        if not shutil.which("ffprobe"):
            logger.warning("Dependency 'ffprobe' not found in PATH. Metadata from local files will be limited.")
    logger.info("Checked dependencies (S3 client OK, ffmpeg/ffprobe if needed).")

    try:
        # --- Database Connection and Lock Setup ---
        conn = get_db_connection()
        if conn is None:
             # Raise specific error if connection fails initially
             error_message = "Failed to get database connection from pool."
             logger.error(error_message)
             raise ConnectionError(error_message)
        conn.autocommit = False # Manual transaction control

        # --- Acquire Lock and Check State ---
        with conn.cursor() as cur:
            try:
                cur.execute("SELECT pg_try_advisory_xact_lock(1, %s)", (source_video_id,))
                lock_acquired = cur.fetchone()[0]
                if not lock_acquired:
                    logger.warning(f"Could not acquire DB lock for source_video_id: {source_video_id}. Another process may be working on it. Skipping.")
                    conn.rollback()
                    return {"status": "skipped_lock", "reason": "Could not acquire lock", "source_video_id": source_video_id}
                logger.info(f"Acquired DB lock for source_video_id: {source_video_id}")
            except Exception as lock_err:
                logger.error(f"Error checking/acquiring DB lock for source_video_id {source_video_id}: {lock_err}", exc_info=True)
                raise RuntimeError("DB Lock acquisition check failed") from lock_err

            cur.execute("SELECT ingest_state, filepath FROM source_videos WHERE id = %s FOR UPDATE", (source_video_id,))
            result = cur.fetchone()
            if not result:
                raise ValueError(f"Source video with ID {source_video_id} not found in database.")
            current_state, existing_filepath = result
            logger.info(f"Current DB state for {source_video_id}: '{current_state}'")

            expected_processing_state = 'downloading' if is_url else 'processing_local'

            allow_processing = current_state == expected_processing_state or \
                               current_state == 'download_failed' or \
                               (current_state in ('new', None) and overwrite_existing)

            if current_state == 'downloaded' and not overwrite_existing:
                logger.info(f"Source video {source_video_id} is already 'downloaded' and overwrite=False. Skipping.") # Changed to INFO
                allow_processing = False
                task_outcome = "skipped_already_done"

            if not allow_processing:
                reason = "Already downloaded" if task_outcome == "skipped_already_done" else f"State '{current_state}' not runnable (expected '{expected_processing_state}' or 'download_failed' with Overwrite={overwrite_existing})"
                logger.warning(f"Skipping intake task for source video {source_video_id}. Reason: {reason}.")
                conn.rollback() # Release lock/transaction

                return {"status": "skipped_state", "reason": reason, "s3_key": existing_filepath, "source_video_id": source_video_id}

            # If processing allowed, commit to release row lock before long I/O
            conn.commit()
            logger.info(f"Verified state '{current_state}'. Proceeding with intake processing for ID {source_video_id}...")
            # Advisory lock is also released by commit.

        # === Core Processing in Temporary Directory ===
        temp_dir_obj = tempfile.TemporaryDirectory(prefix=f"meatspace_intake_{source_video_id}_")
        temp_dir = Path(temp_dir_obj.name)
        logger.info(f"Using temporary directory: {temp_dir}")

        base_filename_no_ext = f"source_{source_video_id}_video" # Safer default

        if is_url:
            # --- Handle URL Download ---
            logger.info("Input is URL. Processing with yt-dlp...")
            output_template = str(temp_dir.joinpath('%(title).100B [%(id)s].%(ext)s'))
            class YtdlpLogger:
                def debug(self, msg): logger.debug(f"yt-dlp debug: {msg}") if not msg.startswith('[debug] ') else None
                def warning(self, msg): logger.warning(f"yt-dlp warning: {msg}")
                def error(self, msg): logger.error(f"yt-dlp error: {msg}")
                def info(self, msg): logger.info(f"yt-dlp: {msg}")

            def ytdlp_progress_hook(d):
                if d['status'] == 'downloading':
                    progress_info = f"{d.get('_percent_str', 'N/A%')} ({d.get('_downloaded_bytes_str', '?')}/{d.get('_total_bytes_str', '?')}) Speed:{d.get('_speed_str', '?')} ETA:{d.get('_eta_str', '?')}"
                    logger.debug(f"yt-dlp: Downloading... {progress_info}")
                elif d['status'] == 'finished':
                    logger.info(f"yt-dlp: Finished downloading {d.get('filename')} Total size: {d.get('total_bytes_str') or d.get('downloaded_bytes_str')}")
                elif d['status'] == 'error':
                    logger.error(f"yt-dlp: Error during download for {d.get('filename')}")

            ydl_opts = {
                'format': 'bv*[ext=mp4]+ba[ext=m4a]/b[ext=mp4]/bv*+ba/b', # Best MP4 video + best M4A audio, merge; fallback to best MP4 overall
                'outtmpl': output_template,
                'merge_output_format': 'mp4',
                'embedmetadata': True, 'embedthumbnail': True, 'restrictfilenames': True,
                'ignoreerrors': False, 'logger': YtdlpLogger(), 'progress_hooks': [ytdlp_progress_hook],
                'noprogress': False, 'socket_timeout': 120,
            }
            try:
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    logger.info(f"Extracting info for {input_source}...")
                    info_dict = ydl.extract_info(input_source, download=False)
                    video_title = info_dict.get('title', f'Untitled Video {source_video_id}')
                    logger.info(f"Downloading video: '{video_title}'...")
                    ydl.download([input_source])

                downloaded_files = list(temp_dir.glob('*.mp4'))
                if not downloaded_files:
                    raise FileNotFoundError("yt-dlp finished but no MP4 output file found.")
                if len(downloaded_files) > 1:
                    logger.warning(f"Multiple MP4 files found after yt-dlp, using first: {downloaded_files[0].name}")
                initial_temp_filepath = downloaded_files[0]
                logger.info(f"Download complete. Initial temp file: {initial_temp_filepath}")

                # Extract metadata
                metadata['title'] = video_title
                metadata['duration'] = info_dict.get('duration')
                metadata['width'] = info_dict.get('width')
                metadata['height'] = info_dict.get('height')
                metadata['fps'] = info_dict.get('fps')
                metadata['original_url'] = info_dict.get('webpage_url', input_source)
                metadata['extractor'] = info_dict.get('extractor_key')
                metadata['upload_date'] = info_dict.get('upload_date') # Format YYYYMMDD

                # Prepare base filename from title/id
                safe_stem = Path(ydl.prepare_filename(info_dict, outtmpl='%(title).100B [%(id)s]')).stem
                base_filename_no_ext = safe_stem

            except yt_dlp.utils.DownloadError as e:
                logger.error(f"yt-dlp DownloadError: {e}", exc_info=True)
                error_message = f"yt-dlp download failed: {e}"
                raise
            except Exception as e:
                logger.error(f"Error during yt-dlp processing: {e}", exc_info=True)
                error_message = f"yt-dlp processing error: {e}"
                raise

        else:
            # --- Handle Local File Path ---
            logger.info(f"Input is a local path: {input_source}")
            input_path = Path(input_source).resolve()
            if not input_path.is_file():
                raise FileNotFoundError(f"Local file path does not exist: {input_path}")

            initial_temp_filepath = temp_dir / input_path.name
            logger.info(f"Copying local file '{input_path}' to '{initial_temp_filepath}'")
            shutil.copy2(input_path, initial_temp_filepath) # copy2 preserves metadata
            logger.info(f"Copied local file to temp location.")

            base_filename_no_ext = input_path.stem
            metadata['title'] = base_filename_no_ext # Default title
            metadata['original_url'] = f"localfile://{input_path}"

            ffprobe_path = shutil.which("ffprobe")
            if ffprobe_path:
                try:
                    logger.info(f"Attempting ffprobe metadata extraction for {initial_temp_filepath}")
                    ffprobe_cmd = [ffprobe_path, "-v", "error", "-show_format", "-show_streams", "-of", "json", str(initial_temp_filepath)]
                    process = subprocess.run(ffprobe_cmd, check=True, capture_output=True, text=True, encoding='utf-8', errors='replace')
                    probe_data = json.loads(process.stdout)
                    video_stream = next((s for s in probe_data.get('streams', []) if s.get('codec_type') == 'video'), None)
                    if video_stream:
                        metadata['width'] = video_stream.get('width')
                        metadata['height'] = video_stream.get('height')
                        fr = video_stream.get('avg_frame_rate', '0/1')
                        if '/' in fr:
                            try:
                                num, den = map(int, fr.split('/'))
                                if den > 0: metadata['fps'] = num / den
                            except ValueError: logger.warning(f"Could not parse frame rate: {fr}")
                        if not metadata.get('duration'):
                             metadata['duration'] = float(video_stream.get('duration', 0))
                    if 'format' in probe_data:
                        format_duration = probe_data['format'].get('duration')
                        if format_duration: metadata['duration'] = float(format_duration)
                        creation_time_str = probe_data['format'].get('tags', {}).get('creation_time')
                        if creation_time_str:
                             metadata['creation_time_iso'] = creation_time_str # Store ISO time if available
                    logger.info(f"Metadata extracted via ffprobe: {metadata}")
                except Exception as probe_err:
                    logger.warning(f"Could not extract metadata using ffprobe: {probe_err}", exc_info=False)
            else:
                logger.warning("ffprobe not found, skipping local file metadata extraction.")

        # --- Determine Final Filename and S3 Object Key ---
        final_ext = ".mp4"
        safe_base_filename_no_ext = "".join(c if c.isalnum() or c in ['-', '_'] else '_' for c in base_filename_no_ext)
        safe_base_filename_no_ext = safe_base_filename_no_ext[:150] # Limit length
        final_filename_no_suffix = f"{safe_base_filename_no_ext}{final_ext}"
        final_filename_with_suffix = f"{safe_base_filename_no_ext}{FINAL_SUFFIX}{final_ext}"
        perform_re_encode = re_encode_for_qt
        final_filename_for_db = final_filename_with_suffix if perform_re_encode else final_filename_no_suffix
        s3_object_key = f"source_videos/{final_filename_for_db}"
        logger.info(f"Target S3 object key: s3://{S3_BUCKET_NAME}/{s3_object_key}")

        # --- Re-encode into consistent format ---
        file_to_upload = initial_temp_filepath
        if perform_re_encode:
            logger.info(f"Starting ffmpeg re-encoding...")
            encoded_temp_path = temp_dir / final_filename_for_db
            ffmpeg_cmd = ["ffmpeg", "-y", "-i", str(initial_temp_filepath)] + FFMPEG_ARGS + [str(encoded_temp_path)]
            try:
                run_external_command(ffmpeg_cmd, "ffmpeg Re-encoding")
                logger.info(f"Re-encoding successful: {encoded_temp_path.name}")
                file_to_upload = encoded_temp_path
            except Exception as encode_err:
                logger.error(f"ffmpeg re-encoding failed: {encode_err}")
                error_message = f"FFmpeg re-encoding failed: {encode_err}"
                raise RuntimeError("FFmpeg re-encoding failed") from encode_err
        else:
            target_temp_path = temp_dir / final_filename_for_db
            if initial_temp_filepath != target_temp_path:
                logger.info(f"Skipping re-encoding. Renaming '{initial_temp_filepath.name}' to '{target_temp_path.name}'.")
                shutil.move(str(initial_temp_filepath), str(target_temp_path))
                file_to_upload = target_temp_path
            else:
                 logger.info(f"Skipping re-encoding. File already has target name: {file_to_upload.name}")


        # --- Upload Processed File to S3 ---
        logger.info(f"Preparing to upload '{file_to_upload.name}' to S3: {s3_object_key}")
        try:
            file_size = os.path.getsize(file_to_upload)
            logger.info(f"File size: {file_size / (1024*1024):.2f} MiB")
            # Adjust throttle based on size
            throttle = 10 if file_size < 100 * 1024 * 1024 else 5
            progress_callback = S3TransferProgress(file_size, file_to_upload.name, logger, throttle_percentage=throttle)

            logger.info(f"Starting S3 upload to s3://{S3_BUCKET_NAME}/{s3_object_key}...")
            with open(file_to_upload, "rb") as f:
                s3_client.upload_fileobj(f, S3_BUCKET_NAME, s3_object_key, Callback=progress_callback)
            logger.info(f"✅ S3 Upload completed successfully for '{file_to_upload.name}'.") # Added confirmation

        except ClientError as s3_err:
            logger.error(f"Failed to upload to S3: {s3_err}", exc_info=True)
            error_message = f"S3 upload failed: {s3_err}"
            raise RuntimeError("S3 upload failed") from s3_err
        except Exception as upload_err:
            logger.error(f"Unexpected error during S3 upload: {upload_err}", exc_info=True)
            error_message = f"S3 upload failed: {upload_err}"
            raise RuntimeError("S3 upload failed") from upload_err

        # --- Final Database Update ---
        conn_final = None
        try:
            logger.info("Attempting final database update...")
            conn_final = get_db_connection()
            if conn_final is None: raise ConnectionError("Failed to get DB connection for final update.")
            conn_final.autocommit = False
            with conn_final.cursor() as cur_final:
                # Re-acquire lock for the final update transaction
                cur_final.execute("SELECT pg_try_advisory_xact_lock(1, %s)", (source_video_id,))
                lock_acquired = cur_final.fetchone()[0]
                if not lock_acquired:
                    # This is unlikely if the first lock worked, but possible
                    raise RuntimeError("Failed to re-acquire lock for final DB update.")
                logger.info(f"Re-acquired lock for final update.")

                logger.info("Parsing metadata and preparing final DB update...")
                # Metadata parsing logic
                db_title = metadata.get('title'); db_duration = metadata.get('duration'); db_width = metadata.get('width')
                db_height = metadata.get('height'); db_fps = metadata.get('fps'); db_original_url = metadata.get('original_url')
                db_upload_date_str = metadata.get('upload_date')
                db_published_date = None
                if db_upload_date_str and isinstance(db_upload_date_str, str) and len(db_upload_date_str) == 8:
                    try: db_published_date = datetime.strptime(db_upload_date_str, '%Y%m%d').date()
                    except ValueError: logger.warning(f"Could not parse upload_date '{db_upload_date_str}'.")
                elif db_upload_date_str: logger.warning(f"Non-standard upload_date format '{db_upload_date_str}'.")
                db_duration = float(db_duration) if db_duration is not None else None
                db_width = int(db_width) if db_width is not None else None
                db_height = int(db_height) if db_height is not None else None
                db_fps = float(db_fps) if db_fps is not None else None

                logger.info(f"Executing final UPDATE statement for ID {source_video_id}...")
                cur_final.execute(
                    """
                    UPDATE source_videos
                    SET ingest_state = 'downloaded', filepath = %s, title = COALESCE(%s, title),
                        duration_seconds = %s, width = %s, height = %s, fps = %s,
                        original_url = COALESCE(%s, original_url), published_date = %s,
                        downloaded_at = NOW(), updated_at = NOW(), last_error = NULL
                    WHERE id = %s AND ingest_state = %s -- Concurrency check using the state we started with
                    RETURNING id;
                    """,
                    (s3_object_key, db_title, db_duration, db_width, db_height, db_fps,
                     db_original_url, db_published_date, source_video_id, expected_processing_state) # Check against the state we expected at the start
                )
                updated_id_result = cur_final.fetchone() # Fetch the result
                if updated_id_result is None:
                     cur_final.execute("SELECT ingest_state FROM source_videos WHERE id = %s", (source_video_id,))
                     current_state_on_fail = cur_final.fetchone()
                     state_str = current_state_on_fail[0] if current_state_on_fail else "NOT FOUND"
                     fail_msg = (f"Final DB update failed for ID {source_video_id}. "
                                 f"Concurrency check failed: Expected state '{expected_processing_state}', but found '{state_str}'. "
                                 f"Upload to S3 ({s3_object_key}) likely succeeded, but DB state is inconsistent.")
                     logger.error(fail_msg)
                     conn_final.rollback() # Rollback the failed update attempt
                     error_message = f"DB final update state mismatch: expected {expected_processing_state}, got {state_str}"
                     raise RuntimeError(fail_msg) # Raise a specific error
                else:
                    conn_final.commit()
                    logger.info(f"✅ Final DB update committed successfully. State set to 'downloaded'.")
                    task_outcome = "success"

        except Exception as final_db_err:
             logger.error(f"Error during final database update: {final_db_err}", exc_info=True)
             if conn_final: conn_final.rollback()
             # Ensure error_message is set if not already
             if not error_message: error_message = f"Final DB update failed: {final_db_err}"
             raise # Re-raise the caught exception
        finally:
             if conn_final:
                 release_db_connection(conn_final)
                 logger.debug("Released final DB connection.")

        # === Successful Completion ===
        logger.info(f"TASK [Intake]: Successfully processed source_video_id: {source_video_id}. Final S3 key: {s3_object_key}")
        final_return_value = {"status": task_outcome, "s3_key": s3_object_key, "metadata": metadata}
        logger.info(f"--- Returning value for ID {source_video_id}: {final_return_value} ---") # Log before return
        return final_return_value

    # === Main Exception Handling ===
    except Exception as e:
        # Attempt to update DB to failed state
        task_name = "Intake"
        # Check if it's an error we already caught and formatted
        if not error_message:
             error_message = f"{type(e).__name__}: {str(e)[:450]}" # Format current exception if needed
        logger.error(f"TASK FAILED ({task_name}): source_video_id {source_video_id} - {error_message}", exc_info=True)
        task_outcome = "failed"

        # Rollback initial transaction if still active
        if conn and not conn.closed and not conn.autocommit:
            try: conn.rollback(); logger.info("Initial DB transaction rolled back on error.")
            except Exception as rb_err: logger.error(f"Error during initial rollback: {rb_err}")

        # Attempt to update DB state to failed using a NEW connection
        error_conn = None
        try:
            logger.info(f"Attempting to update DB state to 'download_failed' for {source_video_id}...")
            error_conn = get_db_connection()
            if error_conn is None: raise ConnectionError("Failed to get DB connection for error update.")
            error_conn.autocommit = True # Use autocommit for simple error update
            with error_conn.cursor() as err_cur:
                err_cur.execute(
                    """
                    UPDATE source_videos
                    SET ingest_state = 'download_failed', last_error = %s,
                        retry_count = COALESCE(retry_count, 0) + 1, updated_at = NOW()
                    WHERE id = %s AND ingest_state != 'downloaded'; -- Avoid overwriting success
                    """,
                    (error_message, source_video_id)
                )
            logger.info(f"Updated DB state to 'download_failed' for source_video_id: {source_video_id}")
        except Exception as db_err_update:
            logger.error(f"CRITICAL: Failed to update error state in DB for {source_video_id}: {db_err_update}")
        finally:
            if error_conn:
                release_db_connection(error_conn)

        # Re-raise the original exception for Prefect
        raise e

    finally:
        # --- Final Logging and Cleanup ---
        # Log final outcome with appropriate level
        log_level = logging.INFO if task_outcome == "success" else logging.ERROR
        logger.log(log_level, f"TASK [Intake] finished for ID {source_video_id}. Outcome: {task_outcome}.")

        # Release initial connection if acquired
        if conn:
             release_db_connection(conn)
             logger.debug(f"Released initial DB connection for source_video_id: {source_video_id}")

        # Cleanup temporary directory
        if temp_dir_obj:
            try:
                temp_dir_obj.cleanup()
                logger.info(f"Cleaned up temporary directory: {temp_dir}")
            except Exception as cleanup_err:
                 logger.warning(f"Error cleaning up temporary directory {temp_dir}: {cleanup_err}")

        # Optional: Explicitly return dict on failure for inspection (Prefect mainly uses exception)
        # if task_outcome != "success":
        #     return {"status": task_outcome, "s3_key": s3_object_key, "error": error_message, "source_video_id": source_video_id}
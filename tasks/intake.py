import os
import shutil
import subprocess
import tempfile
import json
from pathlib import Path
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
from datetime import datetime # Import datetime for date parsing

from prefect import task, get_run_logger
import psycopg2
from psycopg2 import sql # Although not used here, good practice if building dynamic SQL
import yt_dlp

# Import utils from the 'utils' directory at the project root
try:
    # Assuming db_utils is one level up from 'tasks' directory
    from utils.db_utils import get_db_connection
except ImportError:
     # Handle potential path issues during development/deployment
    import sys
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
    try:
        from utils.db_utils import get_db_connection
    except ImportError as e:
        print(f"Failed to import db_utils even after path adjustment: {e}")
        def get_db_connection(): raise NotImplementedError("Dummy get_db_connection")


# --- Task Configuration ---
FINAL_SUFFIX = "_qt"
FFMPEG_ARGS = [
    "-map", "0",
    "-c:v:0", "libx264", "-preset", "fast", "-crf", "20", "-pix_fmt", "yuv420p",
    "-c:a:0", "aac", "-b:a", "192k",
    "-c:s", "mov_text",
    "-c:d", "copy",
    "-c:v:1", "copy", # Attempt to copy 2nd video stream (often thumbnail)
    "-movflags", "+faststart",
]

# --- S3 Configuration ---
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
AWS_REGION = os.getenv("AWS_REGION")

if not S3_BUCKET_NAME:
    raise ValueError("S3_BUCKET_NAME environment variable not set.")

s3_client = None
try:
    s3_client = boto3.client('s3', region_name=AWS_REGION)
    print(f"Successfully initialized S3 client for bucket: {S3_BUCKET_NAME} in region: {AWS_REGION or 'default'}")
except NoCredentialsError:
    print("ERROR: AWS credentials not found. Configure AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, or use IAM roles/profiles.")
except Exception as e:
    print(f"ERROR initializing S3 client: {e}")


def run_external_command(cmd_list, step_name="Command", cwd=None):
    """Runs an external command using subprocess, logs output, and raises errors."""
    logger = get_run_logger()
    cmd_str = ' '.join(cmd_list)
    logger.info(f"Running {step_name}: {cmd_str}")
    try:
        process = subprocess.Popen(
            cmd_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, encoding='utf-8', cwd=cwd
        )
        stdout, stderr = process.communicate()

        if process.returncode != 0:
            logger.error(f"Error during '{step_name}': Command failed with exit code {process.returncode}.")
            if stderr: logger.error(f"STDERR:\n{stderr}")
            if stdout: logger.error(f"STDOUT:\n{stdout}") # Sometimes errors are on stdout
            raise subprocess.CalledProcessError(process.returncode, cmd_list, output=stdout, stderr=stderr)
        return True

    except FileNotFoundError:
        logger.error(f"Error during '{step_name}': Command not found: '{cmd_list[0]}'. Is it installed and in PATH?")
        raise
    except Exception as e:
        logger.error(f"An unexpected error occurred during '{step_name}': {e}", exc_info=True)
        raise


@task(name="Intake Source Video", retries=1, retry_delay_seconds=30)
def intake_task(source_video_id: int,
                input_source: str,
                re_encode_for_qt: bool = True,
                overwrite_existing: bool = False
                ):
    """
    Intakes a source video: downloads (if URL) or processes local file,
    optionally re-encodes, uploads to S3, and updates the DB record.
    """
    logger = get_run_logger()
    logger.info(f"TASK [Intake]: Starting for source_video_id: {source_video_id}, Input: '{input_source}'")
    conn = None
    initial_temp_filepath = None
    s3_object_key = None # Will store the key like 'source_videos/filename.mp4'
    # final_filename_for_db IS needed for logic, just not for DB insert itself
    final_filename_for_db = None # Base filename (e.g., video_qt.mp4)
    metadata = {}
    temp_dir_obj = None

    # --- Dependency Check ---
    if not s3_client:
        logger.error("S3 client is not available. Cannot proceed with intake.")
        raise RuntimeError("S3 client failed to initialize. Check credentials and configuration.")

    if re_encode_for_qt or not input_source.lower().startswith(('http://', 'https://')):
        if not shutil.which("ffmpeg"):
            logger.error("Dependency 'ffmpeg' not found in PATH.")
            raise FileNotFoundError("ffmpeg is required but not found.")
        if not shutil.which("ffprobe"):
            logger.error("Dependency 'ffprobe' not found in PATH.")
            raise FileNotFoundError("ffprobe is required but not found.")
        logger.info("Checked dependencies (ffmpeg, ffprobe, S3 client).")

    try:
        # --- Database Connection and Transaction Setup ---
        conn = get_db_connection()
        conn.autocommit = False

        with conn.cursor() as cur:
            # === Transaction Start ===

            # 1. Acquire Advisory Lock
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

            # State checking logic (allow 'new', 'download_failed' or force with overwrite)
            allow_processing = current_state in ('new', None, 'download_failed') or overwrite_existing
            if current_state == 'downloaded' and not overwrite_existing:
                logger.warning(f"Source video {source_video_id} is already 'downloaded' and overwrite=False. Skipping.")
                allow_processing = False # Explicitly block if already downloaded without overwrite

            if not allow_processing:
                logger.warning(
                    f"Skipping intake for source video {source_video_id}. "
                    f"Current state: '{current_state}', Overwrite: {overwrite_existing}."
                )
                conn.rollback() # Release lock
                return {
                    "status": "skipped",
                    "reason": f"Already processed or not in runnable state (state: {current_state})",
                    "s3_key": existing_filepath
                }

            # --- Determine Input Type and Set Initial State ---
            is_url = input_source.lower().startswith(('http://', 'https://'))
            new_state_during_processing = 'downloading' if is_url else 'processing_local'

            cur.execute(
                """
                UPDATE source_videos
                SET ingest_state = %s,
                    retry_count = CASE WHEN ingest_state = 'download_failed' THEN retry_count ELSE 0 END, -- Reset retries only if starting fresh
                    last_error = NULL,
                    updated_at = NOW()
                WHERE id = %s
                """,
                (new_state_during_processing, source_video_id)
            )
            logger.info(f"Set state to '{new_state_during_processing}' for source_video_id: {source_video_id}")

            # === Core Processing in Temporary Directory ===
            temp_dir_obj = tempfile.TemporaryDirectory(prefix=f"meatspace_intake_{source_video_id}_")
            temp_dir = Path(temp_dir_obj.name)
            logger.info(f"Using temporary directory: {temp_dir}")

            base_filename_no_ext = f"source_{source_video_id}_video" # Safer default

            if is_url:
                # --- Handle URL Download ---
                logger.info("Input is URL. Processing with yt-dlp...")
                output_template = str(temp_dir / '%(title).100s [%(id)s].%(ext)s')

                class YtdlpLogger:
                    def debug(self, msg): logger.debug(msg) if not msg.startswith('[debug] ') else None
                    def warning(self, msg): logger.warning(msg)
                    def error(self, msg): logger.error(msg)
                    def info(self, msg): logger.info(msg)

                ydl_opts = {
                    'format': 'bv*[ext=mp4]+ba[ext=m4a]/b[ext=mp4]/bv*+ba/b',
                    'outtmpl': output_template, 'merge_output_format': 'mp4',
                    'embedmetadata': True, 'embedthumbnail': True,
                    'postprocessors': [{'key': 'FFmpegThumbnailsConvertor', 'format': 'jpg'}],
                    'restrictfilenames': True, 'ignoreerrors': False,
                    'logger': YtdlpLogger(),
                    'progress_hooks': [lambda d: logger.info(f"yt-dlp: {d.get('status')} {d.get('_percent_str', '')} {d.get('_speed_str', '')} ETA {d.get('_eta_str', '')}") if d.get('status') == 'downloading' else None],
                    'noprogress': False,
                }

                try:
                    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                        logger.info(f"Extracting info for {input_source}...")
                        info_dict = ydl.extract_info(input_source, download=False)
                        video_title = info_dict.get('title', f'Untitled Video {source_video_id}')
                        logger.info(f"Downloading video: {video_title}...")
                        ydl.download([input_source])

                    downloaded_files = list(temp_dir.glob('*.mp4'))
                    if not downloaded_files:
                        raise FileNotFoundError("yt-dlp finished but no MP4 output file found.")
                    initial_temp_filepath = downloaded_files[0]
                    logger.info(f"Download complete. Initial temp file: {initial_temp_filepath}")

                    # Populate metadata dictionary
                    metadata['title'] = video_title
                    metadata['duration'] = info_dict.get('duration')
                    metadata['width'] = info_dict.get('width')
                    metadata['height'] = info_dict.get('height')
                    metadata['fps'] = info_dict.get('fps')
                    metadata['original_url'] = info_dict.get('webpage_url', input_source)
                    metadata['extractor'] = info_dict.get('extractor_key')
                    metadata['upload_date'] = info_dict.get('upload_date') # YYYYMMDD format

                    safe_stem = Path(ydl.prepare_filename(info_dict, outtmpl='%(title).100s [%(id)s]')).stem
                    base_filename_no_ext = safe_stem # Use sanitized name from yt-dlp

                except yt_dlp.utils.DownloadError as e:
                    logger.error(f"yt-dlp DownloadError: {e}")
                    raise
                except Exception as e:
                    logger.error(f"Error during yt-dlp processing: {e}", exc_info=True)
                    raise

            else:
                # --- Handle Local File Path ---
                logger.info(f"Input is a local path: {input_source}")
                input_path = Path(input_source).resolve()
                if not input_path.is_file():
                    raise FileNotFoundError(f"Provided local file path does not exist: {input_path}")

                initial_temp_filepath = temp_dir / input_path.name
                shutil.copy2(input_path, initial_temp_filepath)
                logger.info(f"Copied local file to temp location: {initial_temp_filepath}")

                base_filename_no_ext = input_path.stem
                metadata['title'] = base_filename_no_ext

                # Attempt metadata extraction using ffprobe
                try:
                    ffprobe_cmd = ["ffprobe", "-v", "error", "-show_format", "-show_streams", "-of", "json", str(initial_temp_filepath)]
                    process = subprocess.run(ffprobe_cmd, check=True, capture_output=True, text=True, encoding='utf-8')
                    probe_data = json.loads(process.stdout)

                    video_stream = next((s for s in probe_data.get('streams', []) if s.get('codec_type') == 'video'), None)
                    if video_stream:
                        metadata['width'] = video_stream.get('width')
                        metadata['height'] = video_stream.get('height')
                        fr = video_stream.get('avg_frame_rate', '0/1')
                        if '/' in fr:
                            num, den = map(int, fr.split('/'))
                            if den > 0: metadata['fps'] = num / den
                    if 'format' in probe_data:
                        metadata['duration'] = float(probe_data['format'].get('duration', 0))
                        metadata['creation_time'] = probe_data['format'].get('tags', {}).get('creation_time') # Example tag
                    logger.info(f"Attempted metadata extraction from local file: {metadata}")
                except Exception as probe_err:
                    logger.warning(f"Could not extract metadata using ffprobe for {initial_temp_filepath}: {probe_err}. Metadata may be incomplete.")

            # --- Determine Final Filename and S3 Object Key ---
            final_ext = ".mp4"
            final_filename_no_suffix = f"{base_filename_no_ext}{final_ext}"
            final_filename_with_suffix = f"{base_filename_no_ext}{FINAL_SUFFIX}{final_ext}"

            perform_re_encode = re_encode_for_qt
            # ** This variable holds the base filename used for S3 key construction **
            final_filename_for_db = final_filename_with_suffix if perform_re_encode else final_filename_no_suffix

            # Special case for local input already having suffix
            if not is_url and input_source.endswith(final_filename_with_suffix) and re_encode_for_qt:
                logger.info(f"Local input file '{input_source}' already has target suffix. Skipping re-encode.")
                perform_re_encode = False
                # Ensure final_filename_for_db reflects the actual name used
                final_filename_for_db = final_filename_with_suffix

            # Define the S3 object key using the calculated filename
            s3_object_key = f"source_videos/{final_filename_for_db}"
            logger.info(f"Target S3 object key: s3://{S3_BUCKET_NAME}/{s3_object_key}")

            # --- Optional Re-encoding Step ---
            file_to_upload = initial_temp_filepath

            if perform_re_encode:
                logger.info(f"Starting ffmpeg re-encoding...")
                # Use the final filename base for the temp encoded file for consistency
                encoded_temp_path = temp_dir / f"{Path(final_filename_for_db).stem}_encoded_temp.mp4"
                ffmpeg_cmd = ["ffmpeg", "-y", "-i", str(initial_temp_filepath)] + FFMPEG_ARGS + [str(encoded_temp_path)]
                try:
                    run_external_command(ffmpeg_cmd, "ffmpeg Re-encoding")
                    logger.info(f"Re-encoding successful. Temp output: {encoded_temp_path}")
                    file_to_upload = encoded_temp_path
                except Exception as encode_err:
                    logger.error(f"ffmpeg re-encoding failed: {encode_err}")
                    raise RuntimeError("FFmpeg re-encoding failed") from encode_err
            else:
                logger.info("Skipping re-encoding step.")

            # --- Upload Processed File to S3 ---
            logger.info(f"Uploading '{file_to_upload.name}' to S3 object: {s3_object_key}")
            try:
                with open(file_to_upload, "rb") as f:
                    s3_client.upload_fileobj(f, S3_BUCKET_NAME, s3_object_key)
                logger.info("S3 Upload successful.")
            except ClientError as s3_err:
                logger.error(f"Failed to upload to S3 bucket '{S3_BUCKET_NAME}', key '{s3_object_key}': {s3_err}")
                raise RuntimeError("S3 upload failed") from s3_err
            except Exception as upload_err:
                logger.error(f"An unexpected error occurred during S3 upload: {upload_err}")
                raise RuntimeError("S3 upload failed") from upload_err


            # --- Update DB Record on Success ---
            logger.info("Updating database record with final details (including S3 key)...")
            db_title = metadata.get('title')
            db_duration = metadata.get('duration')
            db_width = metadata.get('width')
            db_height = metadata.get('height')
            db_fps = metadata.get('fps')
            db_original_url = metadata.get('original_url')
            db_upload_date_str = metadata.get('upload_date') # Get the string YYYYMMDD

            # Convert upload_date string to date object for 'published_date' column
            db_published_date = None # Default to None
            if db_upload_date_str and isinstance(db_upload_date_str, str) and len(db_upload_date_str) == 8:
                try:
                    db_published_date = datetime.strptime(db_upload_date_str, '%Y%m%d').date()
                    logger.info(f"Parsed upload_date '{db_upload_date_str}' to {db_published_date} for published_date column.")
                except ValueError:
                    logger.warning(f"Could not parse upload_date '{db_upload_date_str}' to YYYYMMDD format. Storing published_date as NULL.")
            elif db_upload_date_str:
                 logger.warning(f"Unexpected upload_date format '{db_upload_date_str}'. Expected YYYYMMDD. Storing published_date as NULL.")


            # *** CORRECTED UPDATE STATEMENT AND PARAMETERS ***
            cur.execute(
                """
                UPDATE source_videos
                SET ingest_state = 'downloaded',        -- Final success state for intake
                    filepath = %s,                      -- Store S3 object key (e.g., source_videos/name_qt.mp4)
                    title = COALESCE(%s, title),        -- Use new title if not NULL, else keep old
                    duration_seconds = %s,
                    width = %s,
                    height = %s,
                    fps = %s,
                    original_url = COALESCE(%s, original_url), -- Use new URL if not NULL, else keep old
                    published_date = %s,                -- Use parsed date object
                    downloaded_at = NOW(),              -- Timestamp of successful completion
                    updated_at = NOW(),
                    last_error = NULL
                WHERE id = %s
                """,
                (s3_object_key,          # Corresponds to filepath = %s
                 db_title,             # Corresponds to title = COALESCE(%s, title)
                 db_duration,          # Corresponds to duration_seconds = %s
                 db_width,             # Corresponds to width = %s
                 db_height,            # Corresponds to height = %s
                 db_fps,               # Corresponds to fps = %s
                 db_original_url,      # Corresponds to original_url = COALESCE(%s, original_url)
                 db_published_date,    # Corresponds to published_date = %s (should be date obj or None)
                 source_video_id       # Corresponds to WHERE id = %s
                 )
            )

            conn.commit() # Commit transaction (releases lock implicitly)
            logger.info(f"TASK [Intake]: Successfully processed source_video_id: {source_video_id}. Final S3 key: {s3_object_key}")

            return {"status": "success", "s3_key": s3_object_key, "metadata": metadata}

    except Exception as e:
        task_name = "Intake"
        logger.error(f"TASK FAILED ({task_name}): source_video_id {source_video_id} - {e}", exc_info=True)
        if conn:
            try:
                conn.rollback() # Rollback DB changes
                logger.info("Database transaction rolled back.")
                conn.autocommit = True # Reset autocommit
                with conn.cursor() as err_cur:
                    # Update state to failed, increment retry, log error
                    err_cur.execute(
                        """
                        UPDATE source_videos
                        SET ingest_state = 'download_failed',
                            last_error = %s,
                            retry_count = COALESCE(retry_count, 0) + 1,
                            updated_at = NOW()
                        WHERE id = %s AND ingest_state != 'downloaded'
                        """,
                        (f"{type(e).__name__}: {str(e)[:500]}", source_video_id)
                    )
                logger.info(f"Updated DB state to 'download_failed' for source_video_id: {source_video_id}")
            except Exception as db_err:
                logger.error(f"Failed to update error state in DB after rollback: {db_err}")
        # Re-raise the original exception to mark the Prefect task as failed
        raise

    finally:
        if conn:
            conn.autocommit = True # Ensure autocommit is back to default
            conn.close()
            logger.debug(f"DB connection closed for source_video_id: {source_video_id}")
        if temp_dir_obj:
            try:
                temp_dir_obj.cleanup()
                logger.info(f"Cleaned up temporary directory: {temp_dir}")
            except Exception as cleanup_err:
                 logger.warning(f"Error cleaning up temporary directory {temp_dir}: {cleanup_err}")
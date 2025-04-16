import os
import shutil
import subprocess
import tempfile
from pathlib import Path
import re # For sanitizing filenames

import boto3
from botocore.exceptions import ClientError, NoCredentialsError

from prefect import task, get_run_logger
import psycopg2
from psycopg2 import sql # Keep for potential future dynamic SQL

# Import utils
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

# --- S3 Configuration ---
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
AWS_REGION = os.getenv("AWS_REGION")

if not S3_BUCKET_NAME:
    raise ValueError("S3_BUCKET_NAME environment variable not set.")

s3_client = None
try:
    s3_client = boto3.client('s3', region_name=AWS_REGION)
    print(f"Splice Task: Successfully initialized S3 client for bucket: {S3_BUCKET_NAME} in region: {AWS_REGION or 'default'}")
except NoCredentialsError:
    print("Splice Task: ERROR initializing S3 client - AWS credentials not found.")
    # Task will fail later if s3_client is None
except Exception as e:
    print(f"Splice Task: ERROR initializing S3 client: {e}")
    # Task will fail later if s3_client is None

# --- Task Configuration ---
CLIP_S3_PREFIX = "clips/" # S3 prefix for clip files
CLIP_DURATION_SECONDS = 5 # Example: Target duration for each clip using segment muxer
MIN_CLIP_DURATION = 1.0 # Example: Minimum duration (more relevant for scene detection)
FFMPEG_PATH = "ffmpeg" # Path to ffmpeg executable

def sanitize_filename(name):
    """Removes potentially problematic characters for filenames and S3 keys."""
    if not name: # Handle case where title might be None
        return "untitled"
    name = str(name) # Ensure it's a string
    name = re.sub(r'[^\w\s\-.]', '', name) # Keep word chars, whitespace, hyphen, period
    name = re.sub(r'\s+', '_', name).strip('_') # Replace whitespace with underscores, remove leading/trailing
    return name[:150] if name else "sanitized_untitled" # Limit length and provide fallback


def run_ffmpeg_command(cmd_list, step_name="ffmpeg command", cwd=None):
    """Runs an ffmpeg command, logs output, raises error on failure."""
    logger = get_run_logger()
    cmd_str = ' '.join(cmd_list)
    logger.info(f"Running {step_name}: {cmd_str}")
    try:
        process = subprocess.Popen(
            cmd_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, encoding='utf-8', cwd=cwd
        )
        # Log stderr in real-time (optional, can be verbose)
        # stderr_lines = []
        # if process.stderr:
        #     for line in iter(process.stderr.readline, ''):
        #         line = line.strip()
        #         if line:
        #             logger.debug(f"ffmpeg stderr: {line}")
        #             stderr_lines.append(line)
        # stdout, _ = process.communicate() # Get remaining stdout
        # stderr = "\n".join(stderr_lines)

        stdout, stderr = process.communicate() # Simpler: wait and get all output

        if process.returncode != 0:
            logger.error(f"Error during '{step_name}': Failed with exit code {process.returncode}.")
            # Log full stderr on error
            if stderr: logger.error(f"FFmpeg STDERR:\n{stderr}")
            if stdout: logger.error(f"FFmpeg STDOUT:\n{stdout}")
            raise subprocess.CalledProcessError(process.returncode, cmd_list, output=stdout, stderr=stderr)

        # Log snippets of output on success for confirmation (optional)
        logger.info(f"{step_name} completed successfully.")
        # logger.debug(f"FFmpeg STDOUT (snippet):\n{stdout[:500]}")
        # logger.debug(f"FFmpeg STDERR (snippet):\n{stderr[:500]}")
        return True
    except FileNotFoundError:
        logger.error(f"Error during '{step_name}': Command '{FFMPEG_PATH}' not found. Is it installed and in PATH?")
        raise
    except Exception as e:
        logger.error(f"An unexpected error occurred during '{step_name}': {e}", exc_info=True)
        raise


@task(name="Splice Source Video into Clips", retries=1, retry_delay_seconds=60)
def splice_video_task(source_video_id: int):
    """
    Downloads source video from S3, splices using ffmpeg segment muxer,
    uploads clips to S3, and creates 'clips' table records.
    """
    logger = get_run_logger()
    logger.info(f"TASK [Splice]: Starting for source_video_id: {source_video_id}")
    conn = None
    temp_dir_obj = None
    source_s3_key = None
    source_title = f"Source_{source_video_id}" # Default title
    created_clip_ids = []
    processed_clip_count = 0
    failed_clip_count = 0

    if not s3_client:
        logger.error("S3 client not initialized. Cannot proceed.")
        raise RuntimeError("S3 client failed to initialize.")
    if not shutil.which(FFMPEG_PATH):
        logger.error(f"Dependency '{FFMPEG_PATH}' not found in PATH.")
        raise FileNotFoundError("ffmpeg is required but not found.")

    try:
        # --- Database Connection and Initial Check ---
        conn = get_db_connection()
        conn.autocommit = False

        with conn.cursor() as cur:
            # === Transaction Start (covers initial check/update) ===
            # 1. Acquire Row Lock (FOR UPDATE) & Check State
            try:
                cur.execute(
                    # Select FOR UPDATE locks the row until commit/rollback
                    "SELECT filepath, ingest_state, title FROM source_videos WHERE id = %s FOR UPDATE",
                    (source_video_id,)
                )
                result = cur.fetchone()
                if not result:
                    # No row found, release implicit lock by returning
                    raise ValueError(f"Source video with ID {source_video_id} not found.")

                source_s3_key, current_state, db_title = result
                # Use title from DB if available, otherwise keep default
                if db_title: source_title = db_title

                logger.info(f"Acquired DB lock for source_video_id: {source_video_id}. Title: '{source_title}'. Current state: '{current_state}'. S3 Key: '{source_s3_key}'")

                if not source_s3_key:
                    raise ValueError(f"Source video {source_video_id} has no S3 filepath. Cannot download.")

                # Allow running if 'downloaded'
                if current_state != 'downloaded':
                    logger.warning(f"Source video {source_video_id} is not in 'downloaded' state ('{current_state}'). Skipping splicing.")
                    conn.rollback() # Release lock
                    return {"status": "skipped", "reason": f"Incorrect state: {current_state}"}

                # 2. Update Source State to 'splicing'
                cur.execute(
                    "UPDATE source_videos SET ingest_state = 'splicing', updated_at = NOW(), last_error = NULL WHERE id = %s",
                    (source_video_id,)
                )
                logger.info(f"Set source_video_id {source_video_id} state to 'splicing'")
                # Commit this *short* transaction to release the lock quickly
                conn.commit()

            except (ValueError, psycopg2.DatabaseError) as initial_db_err:
                logger.error(f"DB Error during initial check/update for source_video_id {source_video_id}: {initial_db_err}")
                if conn: conn.rollback() # Ensure rollback if commit didn't happen or error occurred
                raise # Re-raise to fail the task early

            # === Main Processing (outside initial transaction) ===
            temp_dir_obj = tempfile.TemporaryDirectory(prefix=f"meatspace_splice_{source_video_id}_")
            temp_dir = Path(temp_dir_obj.name)
            logger.info(f"Using temporary directory: {temp_dir}")

            # 3. Download Source Video from S3
            local_source_path = temp_dir / Path(source_s3_key).name # Use filename from S3 key
            logger.info(f"Downloading s3://{S3_BUCKET_NAME}/{source_s3_key} to {local_source_path}...")
            try:
                s3_client.download_file(S3_BUCKET_NAME, source_s3_key, str(local_source_path))
                logger.info(f"Source video download complete: {local_source_path}")
            except ClientError as s3_err:
                logger.error(f"Failed to download source video from S3: {s3_err}")
                # Try to update DB state to failed (requires separate transaction)
                raise RuntimeError("S3 download failed") from s3_err # Fail the task

            # 4. Splicing Logic (using ffmpeg segment muxer)
            clips_output_dir = temp_dir / "clips_out"
            clips_output_dir.mkdir()
            # Use sanitized source title in the pattern for clarity
            sanitized_title_prefix = sanitize_filename(source_title)
            output_pattern = clips_output_dir / f"{sanitized_title_prefix}_seg_%05d.mp4" # e.g., My_Video_seg_00001.mp4

            ffmpeg_splice_cmd = [
                FFMPEG_PATH,
                "-i", str(local_source_path),      # Input file
                "-c", "copy",                      # Copy codecs (faster, assumes compatible format)
                "-map", "0",                       # Map all streams (video, audio, subtitles etc.)
                "-segment_time", str(CLIP_DURATION_SECONDS), # Create segments approx this long
                "-f", "segment",                   # Use segment muxer
                "-reset_timestamps", "1",          # Reset timestamps for each segment (important!)
                "-segment_format_options", "movflags=+faststart", # Apply faststart to segments
                str(output_pattern)                # Output pattern
            ]

            logger.info(f"Starting ffmpeg splicing process for source {source_video_id}...")
            try:
                run_ffmpeg_command(ffmpeg_splice_cmd, "ffmpeg Splicing", cwd=str(temp_dir)) # Run in temp dir
                logger.info("ffmpeg splicing process finished.")
            except subprocess.CalledProcessError as ffmpeg_err:
                 logger.error(f"ffmpeg splicing failed: {ffmpeg_err}")
                 raise RuntimeError("FFmpeg splicing command failed") from ffmpeg_err
            except Exception as e:
                 logger.error(f"Unexpected error during splicing command execution: {e}")
                 raise

            # 5. Process, Upload, and Record Each Clip
            generated_clip_files = sorted(list(clips_output_dir.glob("*.mp4")))
            logger.info(f"Found {len(generated_clip_files)} potential clip files after splicing.")

            if not generated_clip_files:
                 logger.warning(f"No clip files were generated by ffmpeg for source {source_video_id}.")
                 # Update source state in the final transaction

            # --- Start *second* transaction for clips and final source update ---
            conn.autocommit = False # Ensure explicit transaction control
            with conn.cursor() as cur:
                # === Transaction Start (covers clip inserts and final source update) ===
                for clip_index, clip_path in enumerate(generated_clip_files):
                    try:
                        # Segment muxer filenames are like prefix_seg_00000.mp4
                        # Extract sequence number if needed (though clip_index works too)
                        # match = re.search(r'_seg_(\d+)\.mp4$', clip_path.name)
                        # seq_num = int(match.group(1)) if match else clip_index

                        # Derive identifier from source title and index/sequence
                        # Use clip_index which starts at 0
                        clip_identifier = f"{sanitized_title_prefix}_clip_{clip_index:05d}"
                        clip_s3_key = f"{CLIP_S3_PREFIX}{clip_identifier}.mp4"

                        logger.info(f"Processing clip {clip_index}: {clip_path.name} -> ID: {clip_identifier}, S3 Key: {clip_s3_key}")

                        # Optional: Check clip duration using ffprobe (adds overhead)
                        # ffprobe_cmd = [...] get duration
                        # if duration < MIN_CLIP_DURATION: continue

                        # Upload clip to S3
                        logger.debug(f"Uploading {clip_path.name} to s3://{S3_BUCKET_NAME}/{clip_s3_key}")
                        with open(clip_path, "rb") as f:
                            s3_client.upload_fileobj(f, S3_BUCKET_NAME, clip_s3_key)
                        logger.debug(f"S3 upload successful for {clip_s3_key}")

                        # Insert clip record into DB
                        # Calculate approximate start/end times based on segment index and duration
                        start_time_seconds = clip_index * CLIP_DURATION_SECONDS
                        # End time is approximate; ffprobe would give exact duration
                        end_time_seconds = start_time_seconds + CLIP_DURATION_SECONDS
                        # Segment muxer might produce slightly different durations
                        # Store approximates for now, can be refined later if needed

                        cur.execute(
                            """
                            INSERT INTO clips (source_video_id, clip_filepath, clip_identifier,
                                               start_time_seconds, end_time_seconds, ingest_state,
                                               created_at, updated_at)
                            VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW())
                            RETURNING id;
                            """,
                            (source_video_id, clip_s3_key, clip_identifier,
                             start_time_seconds, end_time_seconds, 'pending_review') # Set state
                        )
                        new_clip_id = cur.fetchone()[0]
                        created_clip_ids.append(new_clip_id)
                        processed_clip_count += 1
                        logger.info(f"Successfully processed and recorded clip_id: {new_clip_id}")

                    except (ClientError, psycopg2.DatabaseError, Exception) as clip_err:
                        failed_clip_count += 1
                        logger.error(f"Failed to process/upload/record clip {clip_path.name}: {clip_err}", exc_info=True)
                        # Continue processing other clips

                # 6. Final Source Video Update (within the same transaction as clip inserts)
                final_source_state = 'spliced' # Default assumption
                final_error_message = None

                if processed_clip_count == 0 and failed_clip_count == 0 and not generated_clip_files:
                     final_source_state = 'splicing_failed'
                     final_error_message = "No clips generated by ffmpeg."
                     logger.warning(f"Source {source_video_id}: No clips generated.")
                elif failed_clip_count > 0 and processed_clip_count == 0:
                     final_source_state = 'splicing_failed'
                     final_error_message = f"All {failed_clip_count} generated clips failed processing."
                     logger.error(f"Source {source_video_id}: All {failed_clip_count} clips failed.")
                elif failed_clip_count > 0:
                     final_source_state = 'splicing_partial_failure'
                     final_error_message = f"{failed_clip_count} of {processed_clip_count + failed_clip_count} clips failed during processing."
                     logger.warning(f"Source {source_video_id}: {final_error_message}")
                elif processed_clip_count > 0:
                      final_source_state = 'spliced' # Success
                      logger.info(f"Source {source_video_id}: Splicing successful, {processed_clip_count} clips created.")


                logger.info(f"Updating source video {source_video_id} final state to '{final_source_state}'")
                cur.execute(
                    """
                    UPDATE source_videos
                    SET ingest_state = %s,
                        spliced_at = CASE WHEN %s IN ('spliced', 'splicing_partial_failure') THEN NOW() ELSE spliced_at END,
                        last_error = %s,
                        updated_at = NOW()
                    WHERE id = %s
                    """,
                    (final_source_state, final_source_state, final_error_message, source_video_id)
                )

                # Commit clip inserts and final source update together
                conn.commit()
                logger.info(f"TASK [Splice]: Finished for source_video_id: {source_video_id}. Processed: {processed_clip_count}, Failed: {failed_clip_count}. Final State: {final_source_state}")


        return {
            "status": "success" if final_source_state == 'spliced' else final_source_state,
            "processed_clips": processed_clip_count,
            "failed_clips": failed_clip_count,
            "created_clip_ids": created_clip_ids
            }

    except Exception as e:
        task_name = "Splice"
        logger.error(f"TASK FAILED ({task_name}): source_video_id {source_video_id} - {e}", exc_info=True)
        if conn:
            try:
                conn.rollback() # Rollback any pending transaction
                logger.info("Database transaction rolled back due to task failure.")
                # Try to update source state to failure if it was 'splicing'
                conn.autocommit = True # Use autocommit for this final error update attempt
                with conn.cursor() as err_cur:
                    err_cur.execute(
                        """
                        UPDATE source_videos
                        SET ingest_state = 'splicing_failed',
                            last_error = %s,
                            updated_at = NOW()
                        WHERE id = %s AND ingest_state = 'splicing'
                        """,
                        (f"{type(e).__name__}: {str(e)[:500]}", source_video_id)
                    )
                logger.info(f"Attempted to set source_video_id {source_video_id} state to 'splicing_failed' due to error.")
            except Exception as db_err:
                logger.error(f"Failed to update error state in DB after rollback: {db_err}")
        raise # Re-raise the original exception to fail the Prefect task

    finally:
        if conn:
            conn.autocommit = True # Reset autocommit before closing
            conn.close()
            logger.debug(f"DB connection closed for source_video_id: {source_video_id}")
        if temp_dir_obj:
            try:
                temp_dir_obj.cleanup()
                logger.info(f"Cleaned up temporary directory: {temp_dir}")
            except Exception as cleanup_err:
                 logger.warning(f"Error cleaning up temporary directory {temp_dir}: {cleanup_err}")
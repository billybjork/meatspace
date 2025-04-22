import os
import subprocess
import tempfile
from pathlib import Path
import shutil
import json
import re
import time
import sys # Import sys for path manipulation if needed

from prefect import task, get_run_logger
import psycopg2
from psycopg2 import sql, extras # Import extras for DictCursor
from botocore.exceptions import ClientError

# --- Project Root Setup (if needed, e.g., for utils) ---
# Assuming db_utils is in ../utils relative to this file's dir
try:
    from utils.db_utils import get_db_connection, release_db_connection
except ImportError:
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    if project_root not in sys.path: sys.path.insert(0, project_root)
    try:
        from utils.db_utils import get_db_connection, release_db_connection
    except ImportError as e:
        print(f"ERROR importing db_utils in merge.py: {e}")
        def get_db_connection(cursor_factory=None): raise NotImplementedError("Dummy DB connection")
        def release_db_connection(conn): pass

# --- Import Shared Components ---
s3_client = None
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
FFMPEG_PATH = os.getenv("FFMPEG_PATH", "ffmpeg")
CLIP_S3_PREFIX = os.getenv("CLIP_S3_PREFIX", "clips/") # Ensure this is set

try:
    # Attempt to import from splice first
    from .splice import s3_client as splice_s3_client, run_ffmpeg_command, sanitize_filename
    if splice_s3_client: s3_client = splice_s3_client
    print("Imported shared components (S3 client?, ffmpeg runner, sanitizer) from tasks.splice")
except ImportError as e:
     print(f"INFO: Could not import components from .splice, using defaults/direct init. Error: {e}")
     # Define fallbacks (same as before)
     def run_ffmpeg_command(cmd, step, cwd=None):
         logger = get_run_logger(); logger.info(f"Executing Fallback FFMPEG Step: {step}"); logger.debug(f"Command: {' '.join(cmd)}")
         try: result = subprocess.run(cmd, capture_output=True, text=True, check=True, cwd=cwd, encoding='utf-8', errors='replace'); logger.debug(f"FFMPEG Output:\n{result.stdout[:500]}..."); return result
         except FileNotFoundError: logger.error(f"ERROR: {cmd[0]} command not found at path '{FFMPEG_PATH}'."); raise
         except subprocess.CalledProcessError as e: logger.error(f"ERROR: {step} failed. Code: {e.returncode}\nStderr:\n{e.stderr}"); raise
     def sanitize_filename(name):
         name = re.sub(r'[^\w\.\-]+', '_', name); name = re.sub(r'_+', '_', name).strip('_'); return name if name else "default_filename"

# Initialize S3 client if not imported
if not s3_client and S3_BUCKET_NAME:
     try: import boto3; s3_client = boto3.client('s3', region_name=os.getenv("AWS_REGION")); print("Initialized default Boto3 S3 client in merge.")
     except ImportError: print("ERROR: Boto3 required but not installed."); s3_client = None
     except Exception as boto_err: print(f"ERROR: Failed to initialize fallback Boto3 client: {boto_err}"); s3_client = None

# --- Task Definition ---
@task(name="Merge Clips Backward", retries=1, retry_delay_seconds=30)
def merge_clips_task(clip_id_target: int, clip_id_source: int):
    """
    Merges the source clip (N) into the target clip (N-1) using ffmpeg concat.
    The target clip (N-1) is updated with the combined content, its artifacts
    are deleted, and it's sent for sprite regeneration.
    The source clip (N) is archived, and its artifacts are deleted.
    """
    logger = get_run_logger()
    logger.info(f"TASK [Merge Backward]: Starting merge of Source Clip {clip_id_source} into Target Clip {clip_id_target}")

    # --- Resource & State Variables ---
    conn = None
    temp_dir_obj = None
    temp_dir = None
    source_video_id = None # Store common source video ID
    clip_target_data = None
    clip_source_data = None
    expected_target_state = 'pending_merge_target'
    expected_source_state = 'marked_for_merge_into_previous'

    # --- Pre-checks ---
    if clip_id_target == clip_id_source: raise ValueError("Cannot merge a clip with itself.")
    if not all([s3_client, S3_BUCKET_NAME]): raise RuntimeError("S3 client or bucket name is not configured.")
    if not FFMPEG_PATH or not shutil.which(FFMPEG_PATH): raise FileNotFoundError(f"ffmpeg command not found at '{FFMPEG_PATH}'.")

    try:
        conn = get_db_connection() # Use connection pool
        if conn is None: raise ConnectionError("Failed to get database connection.")
        conn.autocommit = False # Manual transaction control

        # === Database Operations: Initial Fetch and Validation ===
        try:
            with conn.cursor(cursor_factory=extras.DictCursor) as cur:
                # Lock both clips involved using advisory locks
                cur.execute("SELECT pg_advisory_xact_lock(2, %s)", (clip_id_target,))
                cur.execute("SELECT pg_advisory_xact_lock(2, %s)", (clip_id_source,))
                logger.debug(f"Acquired locks for target {clip_id_target} and source {clip_id_source}")

                # Fetch data for both clips FOR UPDATE (row lock within transaction)
                cur.execute(
                    """
                    SELECT id, clip_filepath, clip_identifier, start_frame, end_frame,
                           start_time_seconds, end_time_seconds, source_video_id,
                           ingest_state, processing_metadata
                    FROM clips
                    WHERE id = ANY(%s::int[]) FOR UPDATE;
                    """, ([clip_id_target, clip_id_source],)
                )
                results = cur.fetchall()
                if len(results) != 2: raise ValueError(f"Could not find/lock both clips {clip_id_target}, {clip_id_source}.")

                clip_target_data = next((r for r in results if r['id'] == clip_id_target), None)
                clip_source_data = next((r for r in results if r['id'] == clip_id_source), None)
                if not clip_target_data or not clip_source_data: raise ValueError("Mismatch fetching clip data.")

                # --- Validation ---
                if clip_target_data['source_video_id'] != clip_source_data['source_video_id']:
                    raise ValueError("Clips do not belong to the same source video.")
                source_video_id = clip_target_data['source_video_id']

                # Validate states (allow proceeding with warning for robustness, but log it)
                if clip_target_data['ingest_state'] != expected_target_state:
                     logger.warning(f"Target clip {clip_id_target} state is '{clip_target_data['ingest_state']}' (expected '{expected_target_state}'). Proceeding cautiously.")
                if clip_source_data['ingest_state'] != expected_source_state:
                     logger.warning(f"Source clip {clip_id_source} state is '{clip_source_data['ingest_state']}' (expected '{expected_source_state}'). Proceeding cautiously.")

                # Basic metadata check (optional stricter check)
                target_meta = clip_target_data['processing_metadata'] or {}
                source_meta = clip_source_data['processing_metadata'] or {}
                if target_meta.get('merge_source_clip_id') != clip_id_source: logger.warning(f"Target {clip_id_target} metadata missing/incorrect source link.")
                if source_meta.get('merge_target_clip_id') != clip_id_target: logger.warning(f"Source {clip_id_source} metadata missing/incorrect target link.")

                if not clip_target_data['clip_filepath'] or not clip_source_data['clip_filepath']:
                    raise ValueError("One or both clips are missing filepaths.")

            # Keep transaction open
            logger.info(f"Clips validated. Target: {clip_id_target}, Source: {clip_id_source}")

        except (ValueError, psycopg2.DatabaseError) as db_err:
             logger.error(f"DB Error during initial checks/locking for merge: {db_err}", exc_info=True)
             if conn: conn.rollback()
             raise # Re-raise to fail the task

        # === Main Processing: Download, Merge, Upload ===
        temp_dir_obj = tempfile.TemporaryDirectory(prefix=f"meatspace_merge_")
        temp_dir = Path(temp_dir_obj.name)
        logger.info(f"Using temporary directory: {temp_dir}")

        local_clip_target_path = temp_dir / Path(clip_target_data['clip_filepath']).name
        local_clip_source_path = temp_dir / Path(clip_source_data['clip_filepath']).name
        concat_list_path = temp_dir / "concat_list.txt"
        logger.info(f"Downloading clips to {temp_dir}...")
        try:
            s3_client.download_file(S3_BUCKET_NAME, clip_target_data['clip_filepath'], str(local_clip_target_path))
            s3_client.download_file(S3_BUCKET_NAME, clip_source_data['clip_filepath'], str(local_clip_source_path))
        except ClientError as s3_err:
            logger.error(f"Failed to download clips from S3: {s3_err}", exc_info=True)
            if conn: conn.rollback()
            raise RuntimeError(f"S3 download failed") from s3_err

        with open(concat_list_path, "w") as f:
            f.write(f"file '{local_clip_target_path.resolve().as_posix()}'\n")
            f.write(f"file '{local_clip_source_path.resolve().as_posix()}'\n")

        new_end_frame = clip_source_data['end_frame']
        new_end_time = clip_source_data['end_time_seconds']
        # Generate new identifier and filename for the merged clip
        source_prefix = f"{clip_target_data['source_video_id']}" # Use source video ID as prefix
        updated_clip_identifier = f"{source_prefix}_{clip_target_data['start_frame']}_{new_end_frame}"
        updated_clip_filename = f"{sanitize_filename(updated_clip_identifier)}.mp4"
        updated_local_clip_path = temp_dir / updated_clip_filename
        # Ensure clip prefix ends with /
        s3_prefix = CLIP_S3_PREFIX.strip('/') + '/'
        updated_clip_s3_key = f"{s3_prefix}{updated_clip_filename}"

        ffmpeg_merge_cmd_copy = [ FFMPEG_PATH, '-y', '-f', 'concat', '-safe', '0', '-i', str(concat_list_path), '-c', 'copy', str(updated_local_clip_path) ]
        try:
            run_ffmpeg_command(ffmpeg_merge_cmd_copy, "ffmpeg Merge (copy)", cwd=str(temp_dir))
        except Exception as e:
            logger.warning(f"FFmpeg copy merge failed ({e}), attempting re-encode...")
            ffmpeg_merge_cmd_reencode = [ FFMPEG_PATH, '-y', '-f', 'concat', '-safe', '0', '-i', str(concat_list_path), '-c:v', 'libx264', '-preset', 'medium', '-crf', '23', '-pix_fmt', 'yuv420p', '-c:a', 'aac', '-b:a', '128k', '-movflags', '+faststart', str(updated_local_clip_path) ]
            try: run_ffmpeg_command(ffmpeg_merge_cmd_reencode, "ffmpeg Merge (re-encode)", cwd=str(temp_dir))
            except Exception as reencode_err:
                logger.error(f"FFmpeg re-encode merge also failed: {reencode_err}", exc_info=True)
                if conn: conn.rollback()
                raise RuntimeError("FFmpeg merge failed") from reencode_err

        if not updated_local_clip_path.is_file() or updated_local_clip_path.stat().st_size == 0:
             raise RuntimeError(f"Merged output file missing or empty: {updated_local_clip_path}")

        logger.info(f"Uploading merged clip to s3://{S3_BUCKET_NAME}/{updated_clip_s3_key}")
        try:
            with open(updated_local_clip_path, "rb") as f:
                s3_client.upload_fileobj(f, S3_BUCKET_NAME, updated_clip_s3_key)
        except ClientError as s3_err:
            logger.error(f"Failed to upload merged clip to S3: {s3_err}", exc_info=True)
            if conn: conn.rollback()
            raise RuntimeError(f"S3 upload failed") from s3_err

        # === Final Database Updates (within the existing transaction) ===
        try:
            with conn.cursor() as cur:
                # 1. Delete ALL existing artifacts for BOTH clips
                # It's safer to delete artifacts before updating the clip states
                delete_artifacts_sql = sql.SQL("""
                    DELETE FROM clip_artifacts WHERE clip_id = ANY(%s);
                """)
                affected_clips_list = [clip_id_target, clip_id_source]
                cur.execute(delete_artifacts_sql, (affected_clips_list,))
                logger.info(f"Deleted {cur.rowcount} existing artifact(s) for clips {clip_id_target} and {clip_id_source}.")

                # 2. Update the Target Clip (N-1)
                # REMOVED nullification of sprite/keyframe/embedding columns
                update_target_sql = sql.SQL("""
                    UPDATE clips
                    SET
                        clip_filepath = %s,       -- New S3 path
                        clip_identifier = %s,   -- New identifier
                        end_frame = %s,           -- Source clip's end frame
                        end_time_seconds = %s,    -- Source clip's end time
                        ingest_state = %s,        -- Send back for sprite gen
                        processing_metadata = NULL, -- Clear merge metadata
                        last_error = 'Merged with clip ' || %s::text,
                        updated_at = NOW(),
                        keyframed_at = NULL,      -- Reset timestamps as content changed
                        embedded_at = NULL
                    WHERE id = %s;
                """)
                cur.execute(update_target_sql, (
                    updated_clip_s3_key, updated_clip_identifier, new_end_frame,
                    new_end_time, 'pending_sprite_generation', # Back into pipeline
                    clip_id_source, clip_id_target
                ))
                logger.info(f"Updated target clip {clip_id_target}. State -> 'pending_sprite_generation'.")

                # 3. Archive the Source Clip (N)
                # REMOVED nullification of non-existent columns
                archive_source_sql = sql.SQL("""
                    UPDATE clips
                    SET
                        ingest_state = 'merged',      -- Final state
                        clip_filepath = NULL,         -- Nullify its video path
                        processing_metadata = jsonb_set(COALESCE(processing_metadata, '{}'::jsonb), '{merged_into_clip_id}', %s::jsonb),
                        last_error = 'Merged into clip ' || %s::text,
                        updated_at = NOW(),
                        keyframed_at = NULL,          -- Clear timestamps
                        embedded_at = NULL
                    WHERE id = %s;
                """)
                cur.execute(archive_source_sql, (
                    json.dumps(clip_id_target), # Store link in metadata
                    clip_id_target, clip_id_source
                ))
                logger.info(f"Archived source clip {clip_id_source} with state 'merged'.")

                # Log S3 keys for potential later cleanup
                logger.info(f"S3 Cleanup eventually needed for ORIGINAL clips: s3://{S3_BUCKET_NAME}/{clip_target_data['clip_filepath']} and s3://{S3_BUCKET_NAME}/{clip_source_data['clip_filepath']}")

            conn.commit() # Commit artifact deletion and clip updates
            logger.info(f"Merge DB changes committed. Target clip {clip_id_target} updated, source clip {clip_id_source} archived.")
            return {"status": "success", "updated_clip_id": clip_id_target, "archived_clip_id": clip_id_source}

        except (ValueError, psycopg2.DatabaseError) as db_err:
            logger.error(f"DB Error during final update for merge: {db_err}", exc_info=True)
            if conn: conn.rollback()
            raise # Re-raise to trigger main error handling

    # === Main Error Handling Block ===
    except Exception as e:
        logger.error(f"TASK FAILED [Merge Backward]: Target {clip_id_target}, Source {clip_id_source} - {e}", exc_info=True)
        if conn:
            try:
                conn.rollback()
                conn.autocommit = True
                with conn.cursor() as err_cur:
                    error_message = f"Merge failed: {type(e).__name__}: {str(e)[:450]}"
                    # Revert both clips to 'merge_failed' state
                    err_cur.execute(
                        """
                        UPDATE clips SET ingest_state = 'merge_failed', last_error = %s,
                                        processing_metadata = NULL, updated_at = NOW()
                        WHERE id = ANY(%s::int[]) AND ingest_state IN (%s, %s);
                        """,
                        (error_message, [clip_id_target, clip_id_source], expected_target_state, expected_source_state)
                    )
                logger.info(f"Attempted to revert clips {clip_id_target}, {clip_id_source} states to 'merge_failed'.")
            except Exception as db_err_on_fail:
                logger.error(f"CRITICAL: Failed to update error state in DB after merge failure: {db_err_on_fail}")
        raise e
    finally:
        # Cleanup connection and temp dir
        if conn:
            release_db_connection(conn) # Use pool release function
            logger.debug("DB connection released.")
        if temp_dir_obj:
            try: shutil.rmtree(temp_dir); logger.debug(f"Cleaned up temp dir: {temp_dir}")
            except Exception as cleanup_err: logger.warning(f"Error cleaning up temp dir {temp_dir}: {cleanup_err}")
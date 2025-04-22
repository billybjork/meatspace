import os
import subprocess
import tempfile
from pathlib import Path
import shutil
import json
import math
import time
import re
import numpy as np # Keep if needed
import sys

from prefect import task, get_run_logger
import psycopg2
from psycopg2 import sql, extras
from botocore.exceptions import ClientError

# --- Project Root Setup ---
try:
    from utils.db_utils import get_db_connection, release_db_connection
except ImportError:
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    if project_root not in sys.path: sys.path.insert(0, project_root)
    try:
        from utils.db_utils import get_db_connection, release_db_connection
    except ImportError as e:
        print(f"ERROR importing db_utils in split.py: {e}")
        def get_db_connection(cursor_factory=None): raise NotImplementedError("Dummy DB connection")
        def release_db_connection(conn): pass

# --- Import Shared Components ---
s3_client = None
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
FFMPEG_PATH = os.getenv("FFMPEG_PATH", "ffmpeg")
CLIP_S3_PREFIX = os.getenv("CLIP_S3_PREFIX", "clips/")

try:
    from .splice import s3_client as splice_s3_client, run_ffmpeg_command, sanitize_filename, MIN_CLIP_DURATION_SECONDS
    if splice_s3_client: s3_client = splice_s3_client
    if not MIN_CLIP_DURATION_SECONDS: MIN_CLIP_DURATION_SECONDS = 1.0 # Fallback if not imported
    print("Imported shared components from tasks.splice")
except ImportError as e:
     print(f"INFO: Could not import components from .splice, using defaults/direct init. Error: {e}")
     # Define fallbacks (same as merge.py)
     def run_ffmpeg_command(cmd, step, cwd=None):
         logger=get_run_logger(); logger.info(f"Fallback FFMPEG: {step}"); logger.debug(f"Cmd: {' '.join(cmd)}")
         try: result=subprocess.run(cmd,capture_output=True,text=True,check=True,cwd=cwd,encoding='utf-8',errors='replace'); logger.debug(f"Output:\n{result.stdout[:500]}..."); return result
         except FileNotFoundError: logger.error(f"ERROR: {cmd[0]} not found ('{FFMPEG_PATH}')"); raise
         except subprocess.CalledProcessError as e: logger.error(f"ERROR: {step} failed. Code: {e.returncode}\nStderr:\n{e.stderr}"); raise
     def sanitize_filename(name):
         name=re.sub(r'[^\w\.\-]+','_',name); name=re.sub(r'_+','_',name).strip('_'); return name if name else "default_filename"
     MIN_CLIP_DURATION_SECONDS = 1.0 # Fallback minimum duration

# Initialize S3 client if not imported
if not s3_client and S3_BUCKET_NAME:
     try: import boto3; s3_client = boto3.client('s3', region_name=os.getenv("AWS_REGION")); print("Initialized default Boto3 S3 client in split.")
     except ImportError: print("ERROR: Boto3 required."); s3_client = None
     except Exception as boto_err: print(f"ERROR: Failed fallback Boto3 init: {boto_err}"); s3_client = None

# Constants
ARTIFACT_TYPE_SPRITE_SHEET = "sprite_sheet"

@task(name="Split Clip at Frame", retries=1, retry_delay_seconds=45)
def split_clip_task(clip_id: int):
    """
    Splits a single clip into two based on a FRAME NUMBER specified in its metadata.
    Downloads the original source video, uses ffmpeg to extract two new clips,
    uploads them, creates new DB records, and archives the original clip.
    Also deletes artifacts associated with the original clip.
    """
    logger = get_run_logger()
    logger.info(f"TASK [Split]: Starting for original clip_id: {clip_id}")

    # --- Resource & State Variables ---
    conn = None
    temp_dir_obj = None
    temp_dir = None
    new_clip_ids = []
    original_clip_data = {}
    source_video_id = None
    # Track final state for logging/return, default failure until success proven
    final_original_state = "split_failed"
    task_exception = None # Store exception for potential re-raise

    # --- Pre-checks ---
    if not all([s3_client, S3_BUCKET_NAME]): raise RuntimeError("S3 client or bucket name not configured.")
    if not FFMPEG_PATH or not shutil.which(FFMPEG_PATH): raise FileNotFoundError(f"ffmpeg not found at '{FFMPEG_PATH}'.")

    try:
        conn = get_db_connection() # Use pool
        conn.autocommit = False
        temp_dir_obj = tempfile.TemporaryDirectory(prefix=f"meatspace_split_{clip_id}_")
        temp_dir = Path(temp_dir_obj.name)
        logger.info(f"Using temporary directory: {temp_dir}")

        # === Phase 1: Initial DB Check and State Update ===
        split_request_frame = None # Define before try block
        try:
            with conn.cursor(cursor_factory=extras.DictCursor) as cur:
                # 1. Lock original clip
                cur.execute("SELECT pg_advisory_xact_lock(2, %s)", (clip_id,))
                logger.debug(f"Acquired lock for original clip {clip_id}")

                # 2. Fetch original clip data, JOINING to get sprite artifact metadata
                cur.execute(
                    """
                    SELECT
                        c.clip_filepath, c.clip_identifier, c.start_time_seconds, c.end_time_seconds,
                        c.source_video_id, c.ingest_state, c.processing_metadata,
                        c.start_frame, c.end_frame,
                        COALESCE((ss.metadata->>'clip_fps_source')::double precision, sv.fps) AS effective_fps,
                        ss.metadata AS sprite_artifact_metadata,
                        sv.filepath AS source_video_filepath, sv.title AS source_title
                    FROM clips c
                    JOIN source_videos sv  ON c.source_video_id = sv.id
                    LEFT JOIN clip_artifacts ss
                        ON c.id = ss.clip_id
                        AND ss.artifact_type = %s
                    WHERE c.id = %s
                    FOR UPDATE OF c, sv
                    """,
                    (ARTIFACT_TYPE_SPRITE_SHEET, clip_id)
                )
                original_clip_data = cur.fetchone()

                if not original_clip_data: raise ValueError(f"Original clip {clip_id} not found.")
                if original_clip_data['ingest_state'] != 'pending_split': raise ValueError(f"Clip {clip_id} not in 'pending_split' state (state: {original_clip_data['ingest_state']}).")
                if not original_clip_data['source_video_filepath']: raise ValueError("Source video filepath missing for source.")
                if not original_clip_data['effective_fps'] or original_clip_data['effective_fps'] <= 0:
                     # Log the source of FPS info if available
                     fps_source = "sprite metadata" if (original_clip_data['sprite_artifact_metadata'] and original_clip_data['sprite_artifact_metadata'].get('clip_fps_source')) else "source video table"
                     logger.error(f"Invalid effective_fps ({original_clip_data['effective_fps']}) derived from {fps_source} for clip {clip_id}.")
                     raise ValueError(f"Cannot determine valid FPS for clip {clip_id} for splitting.")

                # 3. Get split frame from processing_metadata
                metadata = original_clip_data['processing_metadata'] # Should be a dict
                if not isinstance(metadata, dict) or 'split_request_at_frame' not in metadata:
                    raise ValueError(f"Invalid or missing 'split_request_at_frame' in processing_metadata for clip {clip_id}.")
                try:
                    split_request_frame = int(metadata['split_request_at_frame'])
                except (ValueError, TypeError):
                    raise ValueError(f"Invalid non-integer 'split_request_at_frame': {metadata.get('split_request_at_frame')}")

                # 4. Lock source video
                source_video_id = original_clip_data['source_video_id']
                cur.execute("SELECT pg_advisory_xact_lock(1, %s)", (source_video_id,))
                logger.debug(f"Acquired lock for source video {source_video_id}")

                # 5. Update original clip state
                cur.execute(
                    "UPDATE clips SET ingest_state = 'splitting', updated_at = NOW(), last_error = NULL WHERE id = %s",
                    (clip_id,)
                )
                logger.info(f"Set original clip {clip_id} state to 'splitting'")

            conn.commit() # Commit state update
            logger.debug("Phase 1 DB check/update committed.")

        except (ValueError, psycopg2.DatabaseError, TypeError) as err:
            logger.error(f"Error during initial check/update for split (Clip {clip_id}): {err}", exc_info=True)
            if conn: conn.rollback()
            task_exception = err # Store exception
            raise # Re-raise to main handler

        # === Phase 2: Main Processing (Calculate Time, Download Source, Extract Clips) ===
        try:
            relative_split_frame = split_request_frame # Already validated as int
            original_start_time = original_clip_data['start_time_seconds']
            original_end_time = original_clip_data['end_time_seconds']
            clip_fps = original_clip_data['effective_fps']

            if not isinstance(original_start_time, (int, float)) or not isinstance(original_end_time, (int, float)):
                raise ValueError("Invalid start/end times.") # Should be caught earlier

            # --- Calculate Absolute Split Time ---
            time_offset = relative_split_frame / clip_fps
            final_absolute_split_time = original_start_time + time_offset
            logger.info(f"Requested split at relative frame {relative_split_frame} (FPS: {clip_fps:.2f}). Calculated absolute split time: {final_absolute_split_time:.4f}s")

            # --- Validate Split Time ---
            time_tolerance = 1e-3 # Small tolerance
            min_duration_check = max(MIN_CLIP_DURATION_SECONDS, time_tolerance * 2) # Min duration for validation

            # Check if split point allows for potentially valid clips on both sides
            if not (original_start_time + min_duration_check / 2 < final_absolute_split_time < original_end_time - min_duration_check / 2):
                 duration = original_end_time - original_start_time
                 logger.error(f"Calculated split time {final_absolute_split_time:.4f}s is too close to clip boundaries ({original_start_time:.4f}s - {original_end_time:.4f}s, duration {duration:.3f}s) or requested frame ({relative_split_frame}) invalid.")
                 raise ValueError(f"Calculated split time invalid or too close to boundaries.")

            # --- Download Source Video ---
            source_s3_key = original_clip_data['source_video_filepath']
            local_source_path = temp_dir / Path(source_s3_key).name
            logger.info(f"Downloading full source video s3://{S3_BUCKET_NAME}/{source_s3_key}...")
            s3_client.download_file(S3_BUCKET_NAME, source_s3_key, str(local_source_path))

            # --- Define New Clip Properties & Extract ---
            clips_to_create = []
            base_identifier = original_clip_data['clip_identifier']
            source_prefix = f"{source_video_id}" # Use source ID for filename prefix

            ffmpeg_encode_options = [ '-map', '0:v:0?', '-map', '0:a:0?', '-c:v', 'libx264', '-preset', 'medium', '-crf', '23', '-pix_fmt', 'yuv420p', '-c:a', 'aac', '-b:a', '128k', '-movflags', '+faststart' ]

            # Clip A (Start to Split Point)
            clip_a_start = original_start_time; clip_a_end = final_absolute_split_time
            clip_a_duration = clip_a_end - clip_a_start
            if clip_a_duration >= MIN_CLIP_DURATION_SECONDS:
                new_start_frame_a = original_clip_data['start_frame'] # Clip A starts at original's start frame
                # Calculate end frame relative to source video start frame
                new_end_frame_a = original_clip_data['start_frame'] + relative_split_frame - 1 # Split frame becomes the end of A (exclusive)
                clip_a_identifier = f"{source_prefix}_{new_start_frame_a}_{new_end_frame_a}_splA"
                clip_a_filename = f"{sanitize_filename(clip_a_identifier)}.mp4"
                local_clip_a_path = temp_dir / clip_a_filename
                s3_prefix = CLIP_S3_PREFIX.strip('/') + '/'
                clip_a_s3_key = f"{s3_prefix}{clip_a_filename}"
                logger.info(f"Extracting Clip A: {clip_a_identifier} (Time {clip_a_start:.4f}s - {clip_a_end:.4f}s)")
                cmd = [ FFMPEG_PATH, '-y', '-i', str(local_source_path), '-ss', str(clip_a_start), '-to', str(clip_a_end), *ffmpeg_encode_options, str(local_clip_a_path) ]
                run_ffmpeg_command(cmd, f"ffmpeg Extract Clip A")
                clips_to_create.append((clip_a_identifier, local_clip_a_path, clip_a_s3_key, clip_a_start, clip_a_end, new_start_frame_a, new_end_frame_a))
            else: logger.warning(f"Skipping Clip A: Duration {clip_a_duration:.3f}s < {MIN_CLIP_DURATION_SECONDS}s.")

            # Clip B (Split Point to End)
            clip_b_start = final_absolute_split_time; clip_b_end = original_end_time
            clip_b_duration = clip_b_end - clip_b_start
            if clip_b_duration >= MIN_CLIP_DURATION_SECONDS:
                new_start_frame_b = original_clip_data['start_frame'] + relative_split_frame # Clip B starts at the split frame
                new_end_frame_b = original_clip_data['end_frame'] # Clip B ends at original's end frame
                clip_b_identifier = f"{source_prefix}_{new_start_frame_b}_{new_end_frame_b}_splB"
                clip_b_filename = f"{sanitize_filename(clip_b_identifier)}.mp4"
                local_clip_b_path = temp_dir / clip_b_filename
                s3_prefix = CLIP_S3_PREFIX.strip('/') + '/'
                clip_b_s3_key = f"{s3_prefix}{clip_b_filename}"
                logger.info(f"Extracting Clip B: {clip_b_identifier} (Time {clip_b_start:.4f}s - {clip_b_end:.4f}s)")
                cmd = [ FFMPEG_PATH, '-y', '-i', str(local_source_path), '-ss', str(clip_b_start), '-to', str(clip_b_end), *ffmpeg_encode_options, str(local_clip_b_path) ]
                run_ffmpeg_command(cmd, f"ffmpeg Extract Clip B")
                clips_to_create.append((clip_b_identifier, local_clip_b_path, clip_b_s3_key, clip_b_start, clip_b_end, new_start_frame_b, new_end_frame_b))
            else: logger.warning(f"Skipping Clip B: Duration {clip_b_duration:.3f}s < {MIN_CLIP_DURATION_SECONDS}s.")

            if not clips_to_create:
                 raise ValueError(f"Neither split segment met minimum duration of {MIN_CLIP_DURATION_SECONDS}s.")

        except (ValueError, ClientError, FileNotFoundError, RuntimeError, subprocess.CalledProcessError) as phase2_err:
            logger.error(f"Error during Phase 2 (Processing/Extraction) for clip {clip_id}: {phase2_err}", exc_info=True)
            if conn: conn.rollback() # Rollback if processing fails before final DB update
            task_exception = phase2_err
            final_original_state = 'split_failed' # Ensure state is marked for error handling
            raise # Re-raise to main handler

        # === Phase 3: Final Database Updates (Upload New, Archive Old, Delete Artifacts) ===
        # (Uses the same transaction)
        final_error_message = ""
        try:
            with conn.cursor() as cur:
                 # Lock again just before final updates
                 cur.execute("SELECT pg_advisory_xact_lock(2, %s)", (clip_id,))
                 cur.execute("SELECT pg_advisory_xact_lock(1, %s)", (source_video_id,))

                 logger.info(f"Uploading and recording {len(clips_to_create)} new clip(s)...")
                 for identifier, local_path, s3_key, start_time, end_time, start_frame, end_frame in clips_to_create:
                     # Upload to S3
                     if not local_path.is_file() or local_path.stat().st_size == 0:
                          raise RuntimeError(f"Output file missing or empty before upload: {local_path.name}")
                     logger.debug(f"Uploading {local_path.name} to s3://{S3_BUCKET_NAME}/{s3_key}")
                     with open(local_path, "rb") as f: s3_client.upload_fileobj(f, S3_BUCKET_NAME, s3_key)

                     # Insert new clip record
                     cur.execute(
                         """
                         INSERT INTO clips (source_video_id, clip_filepath, clip_identifier,
                                            start_frame, end_frame, start_time_seconds, end_time_seconds,
                                            ingest_state, created_at, updated_at)
                         VALUES (%s, %s, %s, %s, %s, %s, %s, 'pending_sprite_generation', NOW(), NOW()) RETURNING id;
                         """, # New clips need sprite generation
                         (source_video_id, s3_key, identifier, start_frame, end_frame, start_time, end_time)
                     )
                     created_clip_id = cur.fetchone()[0]
                     new_clip_ids.append(created_clip_id)
                     logger.info(f"Created new split clip record ID: {created_clip_id} ({identifier}) -> pending_sprite_generation")

                 # Archive the original clip
                 final_original_state = 'archived' # Archive original since new clips created/uploaded
                 final_error_message = f'Split at frame {relative_split_frame} into {len(new_clip_ids)} clip(s): {",".join(map(str, new_clip_ids))}'
                 logger.info(f"Archiving original clip {clip_id}...")
                 cur.execute(
                    """
                    UPDATE clips
                    SET ingest_state = %s,
                        last_error   = %s,
                        processing_metadata = NULL,
                        -- clip_filepath = NULL,  -- remove this line
                        updated_at = NOW(),
                        keyframed_at = NULL,
                        embedded_at  = NULL
                    WHERE id = %s AND ingest_state = 'splitting';
                    """,
                    (final_original_state, final_error_message, clip_id)
                )

                 # Delete ALL artifacts associated with the original clip
                 delete_artifacts_sql = sql.SQL("DELETE FROM clip_artifacts WHERE clip_id = %s;")
                 cur.execute(delete_artifacts_sql, (clip_id,))
                 logger.info(f"Deleted {cur.rowcount} artifact(s) associated with original archived clip {clip_id}.")


            conn.commit() # Commit creation of new clips and archiving of old one
            logger.info(f"TASK [Split]: Finished successfully for original clip {clip_id}. Final State: {final_original_state}. New Clips: {new_clip_ids}")

            # --- Post-Commit S3 Cleanup (Optional - consider moving to separate flow) ---
            # Delete original clip video file AFTER successful commit
            original_clip_s3_key = original_clip_data.get('clip_filepath')
            if original_clip_s3_key:
                logger.info(f"Attempting post-commit S3 delete for original clip video: s3://{S3_BUCKET_NAME}/{original_clip_s3_key}")
                try:
                    s3_client.delete_object(Bucket=S3_BUCKET_NAME, Key=original_clip_s3_key)
                    logger.info(f"Successfully deleted original clip video file from S3.")
                except ClientError as del_err:
                    # Log warning, but don't fail the task as main work is done
                    logger.warning(f"Failed to delete original clip video file {original_clip_s3_key} from S3 after split: {del_err}")

            # Return success
            return {"status": "success", "created_clip_ids": new_clip_ids, "original_clip_archived": True}

        except (ClientError, ValueError, psycopg2.DatabaseError, RuntimeError) as final_db_err:
            logger.error(f"Error during final DB/Upload phase for split (Clip {clip_id}): {final_db_err}", exc_info=True)
            if conn: conn.rollback()
            final_original_state = 'split_failed' # Ensure failure state
            task_exception = final_db_err
            raise # Re-raise to main handler

    # === Main Error Handling Block ===
    except Exception as e:
        logger.error(f"TASK FAILED [Split]: original clip_id {clip_id} - {e}", exc_info=True)
        final_original_state = 'split_failed' # Ensure correct state on failure
        task_exception = task_exception or e # Store the first exception encountered

        if conn:
            try:
                conn.rollback()
                conn.autocommit = True
                with conn.cursor() as err_cur:
                    err_cur.execute(
                        """
                        UPDATE clips SET ingest_state = 'split_failed', last_error = %s, updated_at = NOW()
                        WHERE id = %s AND ingest_state IN ('splitting', 'pending_split');
                        """, (f"Split failed: {type(task_exception).__name__}: {str(task_exception)[:450]}", clip_id)
                    )
                logger.info(f"Attempted to set original clip {clip_id} state to 'split_failed'.")
            except Exception as db_err:
                 logger.error(f"CRITICAL: Failed to update error state in DB after split failure for clip {clip_id}: {db_err}")

        # Re-raise the original exception if captured
        if task_exception: raise task_exception
        else: raise # Re-raise generic exception if specific one wasn't stored


    finally:
        # --- Cleanup ---
        if conn:
            release_db_connection(conn) # Use pool release
            logger.debug(f"DB connection released for split task, original clip_id: {clip_id}")
        if temp_dir_obj:
            try: shutil.rmtree(temp_dir); logger.info(f"Cleaned up temporary directory: {temp_dir}")
            except Exception as cleanup_err: logger.warning(f"Error cleaning up temp dir {temp_dir}: {cleanup_err}")

        logger.info(f"Split task final state for original clip {clip_id}: {final_original_state}")
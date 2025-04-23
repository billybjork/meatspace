import os
import subprocess
import tempfile
from pathlib import Path
import shutil
import time
import re
import sys
import json # Needed for metadata parsing

from prefect import task, get_run_logger
import psycopg2
from psycopg2 import sql, extras
from botocore.exceptions import ClientError

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
MIN_CLIP_DURATION_SECONDS = 1.0 # Default fallback

try:
    from .splice import s3_client as splice_s3_client, run_ffmpeg_command, sanitize_filename, MIN_CLIP_DURATION_SECONDS as imported_min_duration
    if splice_s3_client: s3_client = splice_s3_client
    if imported_min_duration: MIN_CLIP_DURATION_SECONDS = imported_min_duration
    print("Imported shared components from tasks.splice")
except ImportError as e:
     print(f"INFO: Could not import components from .splice, using defaults/direct init. Error: {e}")
     # Define fallbacks
     def run_ffmpeg_command(cmd_list, step_name="ffmpeg command", cwd=None):
         logger=get_run_logger()
         logger.info(f"Executing Fallback FFMPEG Step: {step_name}")
         logger.debug(f"Command: {' '.join(cmd_list)}")
         try:
             result=subprocess.run(cmd_list,capture_output=True,text=True,check=True,cwd=cwd,encoding='utf-8',errors='replace')
             logger.debug(f"FFMPEG Output:\n{result.stdout[:500]}...")
             return result
         except FileNotFoundError:
             logger.error(f"ERROR: {cmd_list[0]} command not found at path '{FFMPEG_PATH}'.")
             raise
         except subprocess.CalledProcessError as e:
             logger.error(f"ERROR: {step_name} failed. Exit code: {e.returncode}")
             logger.error(f"Stderr:\n{e.stderr}")
             raise
     def sanitize_filename(name):
         name=re.sub(r'[^\w\.\-]+','_', str(name)) # Ensure input is string
         name=re.sub(r'_+','_',name).strip('_')
         return name if name else "default_filename"
     # MIN_CLIP_DURATION_SECONDS already set to fallback

# Initialize S3 client if not imported
if not s3_client and S3_BUCKET_NAME:
     try:
         import boto3
         s3_client = boto3.client('s3', region_name=os.getenv("AWS_REGION"))
         print("Initialized default Boto3 S3 client in split.")
     except ImportError:
         print("ERROR: Boto3 required.")
         s3_client = None
     except Exception as boto_err:
         print(f"ERROR: Failed fallback Boto3 init: {boto_err}")
         s3_client = None

# Constants
ARTIFACT_TYPE_SPRITE_SHEET = "sprite_sheet"


@task(name="Split Clip at Frame", retries=1, retry_delay_seconds=45)
def split_clip_task(clip_id: int):
    """
    Splits a single clip into two based on a FRAME NUMBER specified in its metadata.
    Downloads the original source video, uses ffmpeg to extract two new clips,
    uploads them to S3 under a source-video-specific directory, creates new DB
    records, and archives the original clip.
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
    source_title = None # To store source title
    sanitized_source_title = None # To store sanitized title
    # Track final state for logging/return, default failure until success proven
    final_original_state = "split_failed"
    task_exception = None # Store exception for potential re-raise

    # --- Pre-checks ---
    if not all([s3_client, S3_BUCKET_NAME]):
        raise RuntimeError("S3 client or bucket name not configured.")
    if not FFMPEG_PATH or not shutil.which(FFMPEG_PATH):
        raise FileNotFoundError(f"ffmpeg not found at '{FFMPEG_PATH}'.")
    if not shutil.which("ffprobe"):
         logger.warning("ffprobe command not found in PATH. FPS calculation might rely on potentially less accurate sources.")

    try:
        conn = get_db_connection() # Use pool
        if conn is None:
            raise ConnectionError("Failed to get database connection.")
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

                # 2. Fetch original clip data, JOINING source video for title, filepath, and fallback FPS
                cur.execute(
                    """
                    SELECT
                        c.clip_filepath, c.clip_identifier, c.start_time_seconds, c.end_time_seconds,
                        c.source_video_id, c.ingest_state, c.processing_metadata,
                        c.start_frame, c.end_frame,
                        COALESCE(sv.fps, 0) AS source_video_fps, -- Use source video FPS as fallback
                        sv.filepath AS source_video_filepath,
                        sv.title AS source_title -- Fetch source title
                    FROM clips c
                    JOIN source_videos sv ON c.source_video_id = sv.id
                    WHERE c.id = %s
                    FOR UPDATE OF c, sv; -- Lock both rows
                    """,
                    (clip_id,)
                )
                original_clip_data = cur.fetchone()

                if not original_clip_data:
                    raise ValueError(f"Original clip {clip_id} not found.")
                current_state = original_clip_data['ingest_state']
                if current_state != 'pending_split':
                    raise ValueError(f"Clip {clip_id} not in 'pending_split' state (state: '{current_state}').")
                if not original_clip_data['source_video_filepath']:
                    raise ValueError(f"Source video filepath missing for source associated with clip {clip_id}.")

                # Assign source_video_id and source_title
                source_video_id = original_clip_data['source_video_id']
                source_title = original_clip_data.get('source_title', f'source_{source_video_id}') # Fallback title
                sanitized_source_title = sanitize_filename(source_title) # Sanitize immediately

                # 3. Get split frame from processing_metadata
                metadata = original_clip_data['processing_metadata']
                if not isinstance(metadata, dict) or 'split_request_at_frame' not in metadata:
                    raise ValueError(f"Invalid or missing 'split_request_at_frame' in processing_metadata for clip {clip_id}.")
                try:
                    split_request_frame = int(metadata['split_request_at_frame'])
                except (ValueError, TypeError) as e:
                    raise ValueError(f"Invalid non-integer 'split_request_at_frame': {metadata.get('split_request_at_frame')} - {e}")

                # 4. Lock source video (already locked by FOR UPDATE)
                logger.debug(f"Source video {source_video_id} row locked by FOR UPDATE clause.")

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
            # --- Determine FPS ---
            clip_fps = 0.0
            # Try ffprobe first on the original *clip* file (more accurate for the specific clip's encoding)
            ffprobe_path = shutil.which("ffprobe")
            original_clip_s3_path = original_clip_data['clip_filepath']
            local_original_clip_path_for_probe = None

            if ffprobe_path and original_clip_s3_path:
                try:
                    probe_temp_dir = tempfile.TemporaryDirectory(prefix=f"meatspace_split_probe_{clip_id}_")
                    local_original_clip_path_for_probe = Path(probe_temp_dir.name) / Path(original_clip_s3_path).name
                    logger.info(f"Downloading original clip s3://{S3_BUCKET_NAME}/{original_clip_s3_path} for FPS probe...")
                    s3_client.download_file(S3_BUCKET_NAME, original_clip_s3_path, str(local_original_clip_path_for_probe))

                    probe_cmd = [ffprobe_path, "-v", "error", "-select_streams", "v:0", "-show_entries", "stream=r_frame_rate", "-of", "default=noprint_wrappers=1:nokey=1", str(local_original_clip_path_for_probe)]
                    result = subprocess.run(probe_cmd, capture_output=True, text=True, check=True, encoding='utf-8', errors='replace')
                    fps_str = result.stdout.strip()
                    if '/' in fps_str:
                        num, den = map(int, fps_str.split('/'))
                        if den > 0: clip_fps = num / den
                    probe_temp_dir.cleanup() # Clean up probe temp dir
                except Exception as probe_err:
                    logger.warning(f"Failed to probe original clip FPS ({original_clip_s3_path}): {probe_err}. Falling back to source FPS.")
                    if local_original_clip_path_for_probe and Path(probe_temp_dir.name).exists():
                        probe_temp_dir.cleanup()

            # Fallback to source video FPS if probe failed or ffprobe not found
            if clip_fps <= 0:
                clip_fps = original_clip_data.get('source_video_fps', 0)
                if clip_fps > 0:
                    logger.warning(f"Using source video FPS ({clip_fps:.3f}) as fallback.")
                else:
                    raise ValueError(f"Cannot determine valid FPS for clip {clip_id} from probe or source video ({clip_fps}).")

            # --- Calculate Absolute Split Time ---
            relative_split_frame = split_request_frame # Already validated as int
            original_start_time = original_clip_data['start_time_seconds']
            original_end_time = original_clip_data['end_time_seconds']

            if not isinstance(original_start_time, (int, float)) or not isinstance(original_end_time, (int, float)):
                raise TypeError("Invalid start/end times obtained from database.") # Should be float or numeric

            # Frame numbers are 0-based, time calculation needs careful handling
            # Time offset represents the duration *up to the beginning* of the split frame
            time_offset = relative_split_frame / clip_fps
            final_absolute_split_time = original_start_time + time_offset
            logger.info(f"Using FPS: {clip_fps:.3f}")
            logger.info(f"Requested split at relative frame {relative_split_frame}. Calculated absolute split time: {final_absolute_split_time:.4f}s")

            # --- Validate Split Time ---
            time_tolerance = 1 / clip_fps # Use one frame duration as tolerance/minimum segment length check basis
            min_duration_check = max(MIN_CLIP_DURATION_SECONDS, time_tolerance * 2) # Ensure segments are reasonably long

            if not (original_start_time + min_duration_check / 2 <= final_absolute_split_time <= original_end_time - min_duration_check / 2):
                 duration = original_end_time - original_start_time
                 logger.error(f"Calculated split time {final_absolute_split_time:.4f}s is invalid or too close to clip boundaries ({original_start_time:.4f}s - {original_end_time:.4f}s, duration {duration:.3f}s) for requested frame {relative_split_frame}.")
                 raise ValueError(f"Calculated split time invalid or too close to boundaries based on minimum duration.")

            # --- Download Source Video ---
            source_s3_key = original_clip_data['source_video_filepath']
            local_source_path = temp_dir / Path(source_s3_key).name
            logger.info(f"Downloading full source video s3://{S3_BUCKET_NAME}/{source_s3_key}...")
            s3_client.download_file(S3_BUCKET_NAME, source_s3_key, str(local_source_path))

            # --- Define New Clip Properties & Extract ---
            clips_to_create = []
            base_identifier = original_clip_data['clip_identifier'] # Keep for reference if needed
            source_id_prefix = f"{source_video_id}" # Use source ID for filename prefix
            s3_base_prefix = CLIP_S3_PREFIX.strip('/') + '/' # Base S3 prefix for clips

            # Use consistent encoding options
            ffmpeg_encode_options = [ '-map', '0:v:0?', '-map', '0:a:0?', '-c:v', 'libx264', '-preset', 'medium', '-crf', '23', '-pix_fmt', 'yuv420p', '-c:a', 'aac', '-b:a', '128k', '-movflags', '+faststart' ]

            # --- Clip A (Start to Split Point) ---
            clip_a_start = original_start_time
            clip_a_end = final_absolute_split_time # End time is exclusive for ffmpeg -to
            clip_a_duration = clip_a_end - clip_a_start

            if clip_a_duration >= MIN_CLIP_DURATION_SECONDS:
                # Frame calculation needs care: start frame is inclusive, end frame is *last included frame*
                new_start_frame_a = original_clip_data['start_frame'] # Clip A starts at original's start frame
                # End frame is the frame *before* the split frame number
                new_end_frame_a = original_clip_data['start_frame'] + relative_split_frame - 1

                # Generate identifier and filename
                clip_a_identifier = f"{source_id_prefix}_{new_start_frame_a}_{new_end_frame_a}_splA"
                clip_a_filename = f"{sanitize_filename(clip_a_identifier)}.mp4"
                local_clip_a_path = temp_dir / clip_a_filename
                # Generate S3 Key using sanitized source title
                clip_a_s3_key = f"{s3_base_prefix}{sanitized_source_title}/{clip_a_filename}"

                logger.info(f"Extracting Clip A: {clip_a_identifier} (Time {clip_a_start:.4f}s - {clip_a_end:.4f}s) -> s3://{S3_BUCKET_NAME}/{clip_a_s3_key}")
                # Use -to for end time
                cmd = [ FFMPEG_PATH, '-y', '-i', str(local_source_path), '-ss', str(clip_a_start), '-to', str(clip_a_end), *ffmpeg_encode_options, str(local_clip_a_path) ]
                run_ffmpeg_command(cmd, f"ffmpeg Extract Clip A")
                clips_to_create.append((clip_a_identifier, local_clip_a_path, clip_a_s3_key, clip_a_start, clip_a_end, new_start_frame_a, new_end_frame_a))
            else:
                logger.warning(f"Skipping Clip A: Calculated duration {clip_a_duration:.3f}s < minimum {MIN_CLIP_DURATION_SECONDS}s.")

            # --- Clip B (Split Point to End) ---
            clip_b_start = final_absolute_split_time # Start time is inclusive for ffmpeg -ss
            clip_b_end = original_end_time
            clip_b_duration = clip_b_end - clip_b_start

            if clip_b_duration >= MIN_CLIP_DURATION_SECONDS:
                # Frame calculation: start frame is the split frame number
                new_start_frame_b = original_clip_data['start_frame'] + relative_split_frame
                new_end_frame_b = original_clip_data['end_frame'] # Clip B ends at original's end frame

                # Generate identifier and filename
                clip_b_identifier = f"{source_id_prefix}_{new_start_frame_b}_{new_end_frame_b}_splB"
                clip_b_filename = f"{sanitize_filename(clip_b_identifier)}.mp4"
                local_clip_b_path = temp_dir / clip_b_filename
                # Generate S3 Key using sanitized source title
                clip_b_s3_key = f"{s3_base_prefix}{sanitized_source_title}/{clip_b_filename}"

                logger.info(f"Extracting Clip B: {clip_b_identifier} (Time {clip_b_start:.4f}s - {clip_b_end:.4f}s) -> s3://{S3_BUCKET_NAME}/{clip_b_s3_key}")
                 # Use -to for end time if available, otherwise rely on -ss and end of source
                cmd = [ FFMPEG_PATH, '-y', '-ss', str(clip_b_start), '-i', str(local_source_path), '-to', str(clip_b_end - clip_b_start), *ffmpeg_encode_options, str(local_clip_b_path) ] # Use -to relative to start time for segment B

                # Alternative using duration (-t)
                # cmd = [ FFMPEG_PATH, '-y', '-ss', str(clip_b_start), '-i', str(local_source_path), '-t', str(clip_b_duration), *ffmpeg_encode_options, str(local_clip_b_path) ]

                run_ffmpeg_command(cmd, f"ffmpeg Extract Clip B")
                clips_to_create.append((clip_b_identifier, local_clip_b_path, clip_b_s3_key, clip_b_start, clip_b_end, new_start_frame_b, new_end_frame_b))
            else:
                 logger.warning(f"Skipping Clip B: Calculated duration {clip_b_duration:.3f}s < minimum {MIN_CLIP_DURATION_SECONDS}s.")

            if not clips_to_create:
                 raise ValueError(f"Neither split segment met minimum duration requirement of {MIN_CLIP_DURATION_SECONDS}s.")

        except (ValueError, ClientError, FileNotFoundError, RuntimeError, subprocess.CalledProcessError, TypeError) as phase2_err:
            logger.error(f"Error during Phase 2 (Processing/Extraction) for clip {clip_id}: {phase2_err}", exc_info=True)
            # No explicit rollback here, as Phase 1 was committed. Error state handled in main block.
            task_exception = phase2_err
            final_original_state = 'split_failed' # Ensure state is marked for error handling
            raise # Re-raise to main handler

        # === Phase 3: Final Database Updates (Upload New, Archive Old, Delete Artifacts) ===
        final_error_message = ""
        try:
            # Re-acquire connection for the final transaction (or reuse if architecture allows)
            # For safety, let's assume we need a fresh transaction context
            # conn = get_db_connection() # Get potentially new connection
            # conn.autocommit = False # Start new transaction
            # If reusing 'conn', ensure it's valid and set autocommit=False

            with conn.cursor() as cur:
                 # Re-lock relevant rows within this transaction
                 cur.execute("SELECT pg_advisory_xact_lock(2, %s)", (clip_id,))
                 # Source video row should also be locked if modifying related clips atomically
                 cur.execute("SELECT pg_advisory_xact_lock(1, %s)", (source_video_id,))
                 logger.debug(f"Re-acquired locks for final DB update (Clip {clip_id}, Source {source_video_id})")

                 logger.info(f"Uploading and recording {len(clips_to_create)} new clip(s)...")
                 for identifier, local_path, s3_key, start_time, end_time, start_frame, end_frame in clips_to_create:
                     # Validate file before upload
                     if not local_path.is_file() or local_path.stat().st_size == 0:
                          raise RuntimeError(f"Output file missing or empty before upload: {local_path.name}")

                     # Upload to S3
                     logger.debug(f"Uploading {local_path.name} to s3://{S3_BUCKET_NAME}/{s3_key}")
                     try:
                         with open(local_path, "rb") as f:
                             s3_client.upload_fileobj(f, S3_BUCKET_NAME, s3_key)
                     except ClientError as upload_err:
                         raise RuntimeError(f"S3 upload failed for {s3_key}") from upload_err

                     # Insert new clip record
                     cur.execute(
                         """
                         INSERT INTO clips (source_video_id, clip_filepath, clip_identifier,
                                            start_frame, end_frame, start_time_seconds, end_time_seconds,
                                            ingest_state, created_at, updated_at)
                         VALUES (%s, %s, %s, %s, %s, %s, %s, 'pending_sprite_generation', NOW(), NOW()) RETURNING id;
                         """,
                         (source_video_id, s3_key, identifier, start_frame, end_frame, start_time, end_time)
                     )
                     created_clip_id = cur.fetchone()[0]
                     new_clip_ids.append(created_clip_id)
                     logger.info(f"Created new split clip record ID: {created_clip_id} ('{identifier}') -> pending_sprite_generation")

                 # Archive the original clip
                 final_original_state = 'archived' # Archive original since new clips created/uploaded
                 final_error_message = f'Split at frame {relative_split_frame} into {len(new_clip_ids)} clip(s): {",".join(map(str, new_clip_ids))}'
                 logger.info(f"Archiving original clip {clip_id}...")
                 # Add a WHERE clause condition to prevent accidental updates if state changed
                 update_orig_sql = sql.SQL("""
                    UPDATE clips
                    SET ingest_state = %s,
                        last_error = %s,
                        processing_metadata = NULL, -- Clear split request
                        -- clip_filepath = NULL, -- KEEP ORIGINAL FILEPATH FOR NOW for potential manual recovery
                        updated_at = NOW(),
                        keyframed_at = NULL, -- Reset derived data timestamps
                        embedded_at = NULL
                    WHERE id = %s AND ingest_state = 'splitting'; -- Ensure state is correct
                    """)
                 cur.execute(update_orig_sql,(final_original_state, final_error_message, clip_id))
                 if cur.rowcount == 0:
                     logger.error(f"Failed to archive original clip {clip_id}. State might have changed unexpectedly. Rolling back.")
                     raise RuntimeError(f"Failed to update original clip {clip_id} state to archive (expected 'splitting').")

                 # Delete ALL artifacts associated with the original clip
                 delete_artifacts_sql = sql.SQL("DELETE FROM clip_artifacts WHERE clip_id = %s;")
                 cur.execute(delete_artifacts_sql, (clip_id,))
                 logger.info(f"Deleted {cur.rowcount} artifact(s) associated with original archived clip {clip_id}.")

            conn.commit() # Commit creation of new clips and archiving of old one
            logger.info(f"TASK [Split]: Finished successfully for original clip {clip_id}. Final State: {final_original_state}. New Clips: {new_clip_ids}")

            # --- Post-Commit S3 Cleanup ---
            # Consider moving to a separate, potentially delayed, cleanup flow/task
            # If deleting immediately, do it AFTER successful commit
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

        # Attempt to update original clip state to 'split_failed' using a new connection
        error_conn = None
        try:
            logger.info(f"Attempting to update original clip {clip_id} state to 'split_failed'...")
            error_conn = get_db_connection()
            if error_conn is None:
                raise ConnectionError("Failed to get DB connection for error update.")
            error_conn.autocommit = True # Use autocommit for simple update
            with error_conn.cursor() as err_cur:
                error_message = f"Split failed: {type(task_exception).__name__}: {str(task_exception)[:450]}"
                err_cur.execute(
                    """
                    UPDATE clips SET
                        ingest_state = 'split_failed',
                        last_error = %s,
                        processing_metadata = processing_metadata - 'split_request_at_frame', -- Attempt to remove just the split key
                        updated_at = NOW()
                    WHERE id = %s AND ingest_state IN ('splitting', 'pending_split'); -- Update only if in expected states
                    """, (error_message, clip_id)
                )
            logger.info(f"Attempted to set original clip {clip_id} state to 'split_failed'.")
        except Exception as db_err_update:
             logger.error(f"CRITICAL: Failed to update error state in DB after split failure for clip {clip_id}: {db_err_update}")
        finally:
            if error_conn:
                release_db_connection(error_conn)

        # Re-raise the original exception if captured
        if task_exception: raise task_exception from e # Chain exceptions if possible
        else: raise # Re-raise generic exception if specific one wasn't stored

    finally:
        # --- Cleanup ---
        if conn:
            # Ensure the primary connection is closed/released if it wasn't already handled
            try:
                # Reset state before releasing, just in case
                conn.autocommit = True
                conn.rollback() # Rollback any potential lingering transaction state
            except Exception as conn_clean_err:
                 logger.warning(f"Ignoring error during final cleanup of primary connection: {conn_clean_err}")
            finally:
                 release_db_connection(conn) # Use pool release
                 logger.debug(f"DB connection released for split task, original clip_id: {clip_id}")

        if temp_dir_obj:
            try:
                shutil.rmtree(temp_dir)
                logger.info(f"Cleaned up temporary directory: {temp_dir}")
            except Exception as cleanup_err:
                logger.warning(f"Error cleaning up temp dir {temp_dir}: {cleanup_err}")

        logger.info(f"Split task final outcome for original clip {clip_id}: {final_original_state}")
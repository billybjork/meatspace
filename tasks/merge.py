import os
import subprocess
import tempfile
from pathlib import Path
import shutil
import json
import time
from prefect import task, get_run_logger
import psycopg2
from psycopg2 import sql, extras
from botocore.exceptions import ClientError

try:
    from utils.db_utils import get_db_connection
except ImportError:
    print("ERROR importing db_utils in merge.py")
    # Provide a dummy implementation for basic loading, but it won't work
    def get_db_connection(): raise NotImplementedError("Dummy DB connection - db_utils not found")

# Assuming these are correctly configured and importable from splice
try:
    from .splice import (
        s3_client, S3_BUCKET_NAME, FFMPEG_PATH, CLIP_S3_PREFIX,
        run_ffmpeg_command, sanitize_filename
    )
    if not all([s3_client, S3_BUCKET_NAME, FFMPEG_PATH, CLIP_S3_PREFIX]):
        raise ImportError("Missing required config from splice module")
except ImportError as e:
     print(f"ERROR importing from splice module in merge.py: {e}")
     # Define dummies to allow loading, but task will fail
     s3_client = None
     S3_BUCKET_NAME = None
     FFMPEG_PATH = "ffmpeg"
     CLIP_S3_PREFIX = "clips/"
     def run_ffmpeg_command(cmd, desc, cwd=None): raise NotImplementedError("Dummy ffmpeg runner")
     def sanitize_filename(name): return name


@task(name="Merge Clips Backward", retries=1, retry_delay_seconds=30)
def merge_clips_task(clip_id_target: int, clip_id_source: int):
    """
    Merges the source clip (N) into the target clip (N-1) using ffmpeg concat.
    The target clip (N-1) is updated with the combined content and sent for
    sprite regeneration. The source clip (N) is archived.
    Uses manual psycopg2 transaction handling.

    Args:
        clip_id_target: The ID of the clip to merge *into* (the earlier clip, N-1).
        clip_id_source: The ID of the clip being merged (the later clip, N).
    """
    logger = get_run_logger()
    logger.info(f"TASK [Merge Backward]: Starting merge of Source Clip {clip_id_source} into Target Clip {clip_id_target}")
    conn = None
    temp_dir_obj = None
    source_video_id = None # Store the common source video ID

    if clip_id_target == clip_id_source:
        raise ValueError("Cannot merge a clip with itself.")
    if not all([s3_client, S3_BUCKET_NAME]):
         raise RuntimeError("S3 client or bucket name is not configured. Cannot perform merge.")

    try:
        conn = get_db_connection() # Use psycopg2 connection from db_utils
        if conn is None:
            raise ConnectionError("Failed to get database connection.")
        conn.autocommit = False # Start manual transaction

        # === Database Operations: Initial Fetch and Validation ===
        try:
            with conn.cursor(cursor_factory=extras.DictCursor) as cur:
                # Lock both clips involved
                cur.execute("SELECT pg_advisory_xact_lock(2, %s)", (clip_id_target,))
                cur.execute("SELECT pg_advisory_xact_lock(2, %s)", (clip_id_source,))
                logger.debug(f"Acquired locks for target clip {clip_id_target} and source clip {clip_id_source}")

                # Fetch data for both clips
                cur.execute(
                    """
                    SELECT
                        id, clip_filepath, clip_identifier,
                        start_frame, end_frame, start_time_seconds, end_time_seconds,
                        source_video_id, ingest_state, processing_metadata
                    FROM clips
                    WHERE id = ANY(%s::int[]);
                    """, ([clip_id_target, clip_id_source],)
                )
                results = cur.fetchall()
                if len(results) != 2:
                    raise ValueError(f"Could not find both clips {clip_id_target}, {clip_id_source} for merging.")

                # Identify target and source data from results
                clip_target_data = next((r for r in results if r['id'] == clip_id_target), None)
                clip_source_data = next((r for r in results if r['id'] == clip_id_source), None)

                if not clip_target_data or not clip_source_data:
                    # This case should be caught by len(results) != 2, but double-check
                    raise ValueError("Mismatch in fetched clip data.")

                # --- Validation ---
                if clip_target_data['source_video_id'] != clip_source_data['source_video_id']:
                    raise ValueError("Clips do not belong to the same source video.")
                source_video_id = clip_target_data['source_video_id'] # Store for potential future use/locking

                # Check expected states (adjust if your states differ slightly)
                expected_target_state = 'pending_merge_target'
                expected_source_state = 'marked_for_merge_into_previous'

                if clip_target_data['ingest_state'] != expected_target_state:
                     logger.warning(f"Target clip {clip_id_target} has unexpected state: '{clip_target_data['ingest_state']}' (expected '{expected_target_state}')")
                     # Decide if this is a hard failure or if merge should proceed cautiously
                     # raise ValueError("Target clip state not ready for merge.")
                if clip_source_data['ingest_state'] != expected_source_state:
                     logger.warning(f"Source clip {clip_id_source} has unexpected state: '{clip_source_data['ingest_state']}' (expected '{expected_source_state}')")
                     # raise ValueError("Source clip state not ready for merge.")

                # Verify metadata link (optional but recommended)
                target_meta = clip_target_data['processing_metadata'] or {}
                source_meta = clip_source_data['processing_metadata'] or {}
                if target_meta.get('merge_source_clip_id') != clip_id_source or \
                   source_meta.get('merge_target_clip_id') != clip_id_target:
                    logger.warning(f"Metadata link mismatch between clips {clip_id_target} and {clip_id_source}.")
                    # Consider if this should be a failure or just a warning

                # Ensure filepaths exist
                if not clip_target_data['clip_filepath'] or not clip_source_data['clip_filepath']:
                    raise ValueError("One or both clips are missing filepaths, cannot merge.")

            # Keep transaction open for FFmpeg processing before final DB updates
            logger.info(f"Clips validated. Target: {clip_id_target} (State: {clip_target_data['ingest_state']}), Source: {clip_id_source} (State: {clip_source_data['ingest_state']})")

        except (ValueError, psycopg2.DatabaseError) as db_err:
             logger.error(f"DB Error during initial checks/locking for merge: {db_err}", exc_info=True)
             if conn: conn.rollback() # Rollback on initial DB error
             raise # Re-raise to fail the task

        # === Main Processing: Download, Merge, Upload ===
        temp_dir_obj = tempfile.TemporaryDirectory(prefix=f"meatspace_merge_")
        temp_dir = Path(temp_dir_obj.name)
        logger.info(f"Using temporary directory: {temp_dir}")

        # Define local paths
        local_clip_target_path = temp_dir / Path(clip_target_data['clip_filepath']).name
        local_clip_source_path = temp_dir / Path(clip_source_data['clip_filepath']).name
        concat_list_path = temp_dir / "concat_list.txt"

        logger.info(f"Downloading clips to {temp_dir}...")
        try:
            s3_client.download_file(S3_BUCKET_NAME, clip_target_data['clip_filepath'], str(local_clip_target_path))
            s3_client.download_file(S3_BUCKET_NAME, clip_source_data['clip_filepath'], str(local_clip_source_path))
        except ClientError as s3_err:
            logger.error(f"Failed to download clips from S3: {s3_err}", exc_info=True)
            if conn: conn.rollback() # Rollback if download fails
            raise RuntimeError(f"S3 download failed: {s3_err}") from s3_err

        # Create the concatenation list file for ffmpeg
        # The order matters: target first, then source.
        with open(concat_list_path, "w") as f:
            f.write(f"file '{local_clip_target_path.resolve().as_posix()}'\n") # Use resolved absolute paths
            f.write(f"file '{local_clip_source_path.resolve().as_posix()}'\n")
        logger.debug(f"Created concat list: {concat_list_path}")

        # Define the *new* properties for the updated target clip
        # Use target's start time/frame, source's end time/frame
        new_end_frame = clip_source_data['end_frame']
        new_end_time = clip_source_data['end_time_seconds']
        # The identifier could be based on the target, possibly adding suffix
        base_identifier = clip_target_data['clip_identifier']
        # Avoid adding _merged suffix repeatedly if target was already a merge result
        if not base_identifier.endswith(("_merged", "_split")): # Avoid appending if already modified
             updated_clip_identifier = f"{base_identifier}_merged"
        else:
             # Maybe use target start frame and new end frame?
             updated_clip_identifier = f"{sanitize_filename(clip_target_data['source_video_id'])}_{clip_target_data['start_frame']}_{new_end_frame}"

        updated_clip_filename = f"{sanitize_filename(updated_clip_identifier)}.mp4"
        updated_local_clip_path = temp_dir / updated_clip_filename
        updated_clip_s3_key = f"{CLIP_S3_PREFIX}{updated_clip_filename}" # New S3 key for the merged content

        # FFmpeg command (using concat demuxer)
        # Try simple copy first, fallback to re-encode if needed (common for concat)
        ffmpeg_merge_cmd = [
            FFMPEG_PATH, '-y',           # Overwrite output without asking
            '-f', 'concat',        # Use the concat demuxer
            '-safe', '0',          # Allow relative paths in list (though we use absolute)
            '-i', str(concat_list_path), # Input concat list file
            '-c', 'copy',          # Copy streams without re-encoding (fast, lossless if compatible)
            str(updated_local_clip_path) # Output merged file
        ]
        try:
            run_ffmpeg_command(ffmpeg_merge_cmd, "ffmpeg Merge (copy)", cwd=str(temp_dir))
            logger.info("FFmpeg merge with stream copy successful.")
        except Exception as e:
            logger.warning(f"FFmpeg copy merge failed ({e}), attempting re-encode...")
            # Re-encode if copy fails (safer, but slower and potentially lossy)
            ffmpeg_merge_cmd_reencode = [
                FFMPEG_PATH, '-y',
                '-f', 'concat', '-safe', '0', '-i', str(concat_list_path),
                '-c:v', 'libx264', '-preset', 'medium', '-crf', '23', '-pix_fmt', 'yuv420p', # Standard video encode settings
                '-c:a', 'aac', '-b:a', '128k', # Standard audio encode settings
                '-movflags', '+faststart',      # Optimize for web playback
                str(updated_local_clip_path)
            ]
            try:
                run_ffmpeg_command(ffmpeg_merge_cmd_reencode, "ffmpeg Merge (re-encode)", cwd=str(temp_dir))
                logger.info("FFmpeg merge with re-encode successful.")
            except Exception as reencode_err:
                logger.error(f"FFmpeg re-encode merge also failed: {reencode_err}", exc_info=True)
                if conn: conn.rollback() # Rollback if merge fails
                raise RuntimeError("FFmpeg merge failed") from reencode_err

        # Upload the newly merged file to S3
        logger.info(f"Uploading merged clip content to s3://{S3_BUCKET_NAME}/{updated_clip_s3_key}")
        try:
            with open(updated_local_clip_path, "rb") as f:
                s3_client.upload_fileobj(f, S3_BUCKET_NAME, updated_clip_s3_key)
        except ClientError as s3_err:
            logger.error(f"Failed to upload merged clip to S3: {s3_err}", exc_info=True)
            if conn: conn.rollback() # Rollback if upload fails
            # Attempt to delete the potentially partially uploaded S3 object? Maybe not necessary.
            raise RuntimeError(f"S3 upload failed: {s3_err}") from s3_err

        # === Final Database Updates ===
        try:
            with conn.cursor() as cur:
                # 1. Update the Target Clip (N-1)
                update_target_sql = """
                    UPDATE clips
                    SET
                        clip_filepath = %s,       -- New S3 path for merged content
                        clip_identifier = %s,   -- New identifier
                        end_frame = %s,           -- Source clip's end frame
                        end_time_seconds = %s,    -- Source clip's end time
                        ingest_state = %s,        -- Send back for sprite generation
                        processing_metadata = NULL, -- Clear merge metadata
                        sprite_sheet_filepath = NULL, -- Clear old sprite info
                        sprite_metadata = NULL,
                        keyframe_count = NULL,    -- Clear old keyframe info
                        keyframe_timestamps = NULL,
                        keyframe_filepaths = NULL,
                        embedding_model = NULL,   -- Clear old embedding info
                        embedding_vector = NULL,
                        embedding_strategy = NULL,
                        last_error = 'Merged with clip ' || %s::text, -- Add note
                        updated_at = NOW()
                    WHERE id = %s;
                """
                cur.execute(update_target_sql, (
                    updated_clip_s3_key,
                    updated_clip_identifier,
                    new_end_frame,
                    new_end_time,
                    'pending_sprite_generation', # Back into the pipeline
                    clip_id_source, # Clip ID it was merged with
                    clip_id_target
                ))
                logger.info(f"Updated target clip {clip_id_target} with merged data. State set to 'pending_sprite_generation'.")

                # 2. Archive the Source Clip (N)
                archive_source_sql = """
                    UPDATE clips
                    SET
                        ingest_state = 'merged', -- Final state indicating it was merged
                        clip_filepath = NULL,        -- Nullify file path
                        sprite_sheet_filepath = NULL,
                        sprite_metadata = NULL,
                        keyframe_count = NULL,
                        keyframe_timestamps = NULL,
                        keyframe_filepaths = NULL,
                        embedding_model = NULL,
                        embedding_vector = NULL,
                        embedding_strategy = NULL,
                        processing_metadata = jsonb_set(COALESCE(processing_metadata, '{}'::jsonb), '{merged_into_clip_id}', %s::jsonb), -- Store link
                        last_error = 'Merged into clip ' || %s::text,
                        updated_at = NOW()
                    WHERE id = %s;
                """
                cur.execute(archive_source_sql, (
                    json.dumps(clip_id_target), # Store target ID in source's metadata
                    clip_id_target,
                    clip_id_source
                ))
                logger.info(f"Archived source clip {clip_id_source} with state 'merged'.")

                # 3. (Optional but Recommended) Delete old S3 files for the merged clips
                # It's safer to do this after the DB commit is successful, maybe in a separate cleanup task/flow
                # For now, just log the files to be deleted later.
                logger.info(f"S3 Cleanup needed for: s3://{S3_BUCKET_NAME}/{clip_target_data['clip_filepath']}")
                logger.info(f"S3 Cleanup needed for: s3://{S3_BUCKET_NAME}/{clip_source_data['clip_filepath']}")

            # If all DB updates succeed, commit the transaction
            conn.commit()
            logger.info(f"Merge successful. Target clip {clip_id_target} updated, source clip {clip_id_source} archived.")
            return {"status": "success", "updated_clip_id": clip_id_target, "archived_clip_id": clip_id_source}

        except (ValueError, psycopg2.DatabaseError) as db_err:
            logger.error(f"DB Error during final update for merge: {db_err}", exc_info=True)
            if conn: conn.rollback() # Rollback on final DB error
            # No need to manually set error states here, the main exception handler below will do it.
            raise # Re-raise to trigger main error handling

    # === Main Error Handling Block ===
    except Exception as e:
        logger.error(f"TASK FAILED [Merge Backward]: Target {clip_id_target}, Source {clip_id_source} - {e}", exc_info=True)
        if conn:
            try:
                conn.rollback() # Ensure rollback happens
                # Now try to update states to reflect failure
                conn.autocommit = True # Switch to autocommit for error state update
                with conn.cursor() as err_cur:
                    error_message = f"Merge failed: {type(e).__name__}: {str(e)[:450]}" # Truncate error
                    # Revert both clips to 'pending_review' or a specific 'merge_failed' state
                    # Clearing metadata is important here.
                    err_cur.execute(
                        """
                        UPDATE clips
                        SET ingest_state = 'merge_failed', -- Or 'pending_review' if you want user to retry manually
                            last_error = %s,
                            processing_metadata = NULL, -- Clear merge request metadata
                            updated_at = NOW()
                        WHERE id = ANY(%s::int[])
                        AND ingest_state IN (%s, %s); -- Only update if they were in expected pre-merge states
                        """,
                        (error_message, [clip_id_target, clip_id_source], expected_target_state, expected_source_state)
                    )
                logger.info(f"Attempted to revert clips {clip_id_target}, {clip_id_source} states to 'merge_failed' after failure.")
            except Exception as db_err_on_fail:
                logger.error(f"CRITICAL: Failed to update error state in DB after merge failure: {db_err_on_fail}")
        # Re-raise the original exception to mark the Prefect task run as failed
        raise e
    finally:
        # Ensure connection is closed and temp dir is cleaned up
        if conn:
            try:
                if not conn.autocommit: # If we didn't switch to autocommit during error handling
                    conn.autocommit = True
                conn.close()
                logger.debug("DB connection closed.")
            except Exception as conn_close_err:
                 logger.warning(f"Error closing DB connection: {conn_close_err}")
        if temp_dir_obj:
            try:
                shutil.rmtree(temp_dir)
                logger.debug(f"Cleaned up temp dir: {temp_dir}")
            except Exception as cleanup_err:
                logger.warning(f"Error cleaning up temp dir {temp_dir}: {cleanup_err}")
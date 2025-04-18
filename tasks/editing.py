import os
import subprocess
import tempfile
from pathlib import Path
import shutil
import json # <-- ADD THIS IMPORT
from prefect import task, get_run_logger
import cv2
import psycopg2
from psycopg2 import sql, extras # <-- ADD extras IMPORT
import time
import numpy as np
from botocore.exceptions import ClientError

try:
    from utils.db_utils import get_db_connection
except ImportError:
    print("ERROR importing db_utils in editing.py")
    def get_db_connection(): raise NotImplementedError("Dummy DB connection")

# Ensure all necessary items are imported from splice
from .splice import (
    s3_client, S3_BUCKET_NAME, FFMPEG_PATH, CLIP_S3_PREFIX,
    run_ffmpeg_command, sanitize_filename, detect_scenes,
    MIN_CLIP_DURATION_SECONDS
)

@task(name="Merge Clips", retries=1, retry_delay_seconds=30)
def merge_clips_task(clip_id_1: int, clip_id_2: int):
    """
    Merges two adjacent clips into a new clip using ffmpeg concat demuxer.
    Archives the original clips and creates a new clip record in 'pending_review'.
    Uses manual psycopg2 transaction handling.
    """
    logger = get_run_logger()
    logger.info(f"TASK [Merge]: Starting for clips {clip_id_1} and {clip_id_2}")
    conn = None
    temp_dir_obj = None
    new_clip_id = None

    if clip_id_1 == clip_id_2: raise ValueError("Cannot merge a clip with itself.")

    try:
        conn = get_db_connection()
        conn.autocommit = False # Start manual transaction

        temp_dir_obj = tempfile.TemporaryDirectory(prefix=f"meatspace_merge_")
        temp_dir = Path(temp_dir_obj.name)
        logger.info(f"Using temporary directory: {temp_dir}")

        # === Database Operations Block ===
        try:
            # Use DictCursor for easier data access by column name
            with conn.cursor(cursor_factory=extras.DictCursor) as cur:
                cur.execute("SELECT pg_advisory_xact_lock(2, %s)", (clip_id_1,))
                cur.execute("SELECT pg_advisory_xact_lock(2, %s)", (clip_id_2,))
                logger.debug(f"Acquired locks for clips {clip_id_1}, {clip_id_2}")

                cur.execute(
                    """
                    SELECT id, clip_filepath, clip_identifier, start_frame, end_frame, start_time_seconds, end_time_seconds, source_video_id, ingest_state
                    FROM clips WHERE id = ANY(%s::int[]) ORDER BY start_frame;
                    """, ([clip_id_1, clip_id_2],)
                )
                results = cur.fetchall()
                if len(results) != 2: raise ValueError(f"Could not find both clips {clip_id_1}, {clip_id_2} for merging.")

                # Access data using dictionary keys
                clip1_data = results[0]
                clip2_data = results[1]

                if clip1_data['source_video_id'] != clip2_data['source_video_id']: raise ValueError("Clips do not belong to the same source video.")
                if not (clip1_data['ingest_state'] == 'pending_merge_with_next' and clip2_data['ingest_state'] in ['pending_review', 'review_skipped']):
                     logger.warning(f"Unexpected states for merge: Clip1 ({clip1_data['id']}) state: {clip1_data['ingest_state']}, Clip2 ({clip2_data['id']}) state: {clip2_data['ingest_state']}")

                source_video_id = clip1_data['source_video_id']
                cur.execute("SELECT pg_advisory_xact_lock(1, %s)", (source_video_id,))
                logger.debug(f"Acquired lock for source video {source_video_id}")

            # Keep transaction open

        except (ValueError, psycopg2.DatabaseError) as db_err:
             logger.error(f"DB Error during initial checks/locking for merge: {db_err}", exc_info=True)
             if conn: conn.rollback()
             raise


        # === Main Processing ===
        local_clip1_path = temp_dir / Path(clip1_data['clip_filepath']).name
        local_clip2_path = temp_dir / Path(clip2_data['clip_filepath']).name
        concat_list_path = temp_dir / "concat_list.txt"

        logger.info(f"Downloading clips to {temp_dir}...")
        s3_client.download_file(S3_BUCKET_NAME, clip1_data['clip_filepath'], str(local_clip1_path))
        s3_client.download_file(S3_BUCKET_NAME, clip2_data['clip_filepath'], str(local_clip2_path))

        with open(concat_list_path, "w") as f:
            f.write(f"file '{local_clip1_path.name}'\n")
            f.write(f"file '{local_clip2_path.name}'\n")
        logger.debug(f"Created concat list: {concat_list_path}")

        base_identifier = clip1_data['clip_identifier']
        # Ensure the merged identifier doesn't get excessively long if already merged
        if base_identifier.endswith("_merged"):
            new_clip_identifier = base_identifier
        else:
            new_clip_identifier = f"{base_identifier}_merged"

        new_clip_filename = f"{sanitize_filename(new_clip_identifier)}.mp4"
        new_local_clip_path = temp_dir / new_clip_filename
        new_clip_s3_key = f"{CLIP_S3_PREFIX}{new_clip_filename}"
        new_start_frame = clip1_data['start_frame']; new_end_frame = clip2_data['end_frame']
        new_start_time = clip1_data['start_time_seconds']; new_end_time = clip2_data['end_time_seconds']

        ffmpeg_merge_cmd = [ FFMPEG_PATH, '-y', '-f', 'concat', '-safe', '0', '-i', str(concat_list_path), '-c', 'copy', str(new_local_clip_path) ]
        try: run_ffmpeg_command(ffmpeg_merge_cmd, "ffmpeg Merge (copy)", cwd=str(temp_dir))
        except Exception as e:
            logger.warning(f"FFmpeg copy merge failed ({e}), attempting re-encode...")
            ffmpeg_merge_cmd_reencode = [ FFMPEG_PATH, '-y', '-f', 'concat', '-safe', '0', '-i', str(concat_list_path), '-c:v', 'libx264', '-preset', 'medium', '-crf', '23', '-pix_fmt', 'yuv420p', '-c:a', 'aac', '-b:a', '128k', '-movflags', '+faststart', str(new_local_clip_path) ]
            run_ffmpeg_command(ffmpeg_merge_cmd_reencode, "ffmpeg Merge (re-encode)", cwd=str(temp_dir))

        logger.info(f"Uploading merged clip to s3://{S3_BUCKET_NAME}/{new_clip_s3_key}")
        with open(new_local_clip_path, "rb") as f: s3_client.upload_fileobj(f, S3_BUCKET_NAME, new_clip_s3_key)

        # === Final Database Updates ===
        try:
            with conn.cursor() as cur:
                # Insert new clip record
                cur.execute(
                    """
                    INSERT INTO clips (source_video_id, clip_filepath, clip_identifier, start_frame, end_frame, start_time_seconds, end_time_seconds, ingest_state, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, 'pending_review', NOW(), NOW()) RETURNING id;
                    """, (source_video_id, new_clip_s3_key, new_clip_identifier, new_start_frame, new_end_frame, new_start_time, new_end_time)
                )
                new_clip_id = cur.fetchone()[0]
                logger.info(f"Created new merged clip record with ID: {new_clip_id}")

                # Archive original clips
                cur.execute(
                    """
                    UPDATE clips SET ingest_state = 'archived',
                           last_error = 'Merged into clip ' || %s::text,
                           processing_metadata = NULL, -- Clear merge request metadata
                           updated_at = NOW()
                    WHERE id = ANY(%s::int[]);
                    """, (new_clip_id, [clip_id_1, clip_id_2])
                )
                logger.info(f"Archived original clips {clip_id_1}, {clip_id_2}")
            # --- End Cursor Context ---
            conn.commit() # Commit final updates
            logger.info(f"Merge successful. New clip ID: {new_clip_id}")
            return {"status": "success", "new_clip_id": new_clip_id}

        except (ValueError, psycopg2.DatabaseError) as db_err:
            logger.error(f"DB Error during final update for merge: {db_err}", exc_info=True)
            if conn: conn.rollback()
            raise

    # === Main Error Handling ===
    except Exception as e:
        logger.error(f"TASK FAILED [Merge]: clips {clip_id_1}, {clip_id_2} - {e}", exc_info=True)
        if conn:
            try:
                conn.rollback()
                conn.autocommit = True
                # Set state back to review or pending_review if merge failed
                # This allows user to retry merge or choose another action
                with conn.cursor() as err_cur:
                    err_cur.execute(
                        """
                        UPDATE clips SET ingest_state =
                            CASE
                                WHEN id = %s AND ingest_state = 'pending_merge_with_next' THEN 'pending_review' -- Revert clip1 to pending_review
                                WHEN id = %s AND ingest_state IN ('pending_review', 'review_skipped') THEN ingest_state -- Keep clip2 state
                                ELSE 'merge_failed' -- Fallback if states were weird
                            END,
                            last_error = %s,
                            updated_at = NOW()
                        WHERE id = ANY(%s::int[]);
                        """,
                        (clip_id_1, clip_id_2, f"Merge failed: {type(e).__name__}: {str(e)[:450]}", [clip_id_1, clip_id_2])
                    )
                logger.info(f"Attempted to revert original clips {clip_id_1}, {clip_id_2} states after merge failure.")
            except Exception as db_err: logger.error(f"Failed to update error state in DB after merge failure: {db_err}")
        raise e
    finally:
        if conn: conn.autocommit = True; conn.close(); logger.debug("DB connection closed.")
        if temp_dir_obj:
            try: shutil.rmtree(temp_dir); logger.debug(f"Cleaned up temp dir: {temp_dir}")
            except Exception as cleanup_err: logger.warning(f"Error cleaning up temp dir {temp_dir}: {cleanup_err}")

@task(name="Split Clip at Time", retries=1, retry_delay_seconds=45)
def split_clip_task(clip_id: int):
    """
    Splits a single clip into two based on a time specified in its metadata.
    Downloads the original source video, uses ffmpeg to extract two new clips,
    uploads them, creates new DB records, and archives the original clip.
    """
    logger = get_run_logger()
    logger.info(f"TASK [Split]: Starting for original clip_id: {clip_id}")
    conn = None
    temp_dir_obj = None
    new_clip_ids = []
    original_clip_data = {}
    source_video_id = None
    final_original_state = "split_failed" # Default to failure

    try:
        conn = get_db_connection()
        conn.autocommit = False # Manual transaction control
        temp_dir_obj = tempfile.TemporaryDirectory(prefix=f"meatspace_split_{clip_id}_")
        temp_dir = Path(temp_dir_obj.name)
        logger.info(f"Using temporary directory: {temp_dir}")

        # === Initial DB Check and State Update ===
        try:
            with conn.cursor(cursor_factory=extras.DictCursor) as cur: # Use DictCursor
                cur.execute("SELECT pg_advisory_xact_lock(2, %s)", (clip_id,))
                logger.debug(f"Acquired lock for clip {clip_id}")

                # Fetch original clip details and source video path
                cur.execute(
                    """
                    SELECT
                        c.clip_filepath, c.clip_identifier, c.start_time_seconds, c.end_time_seconds,
                        c.source_video_id, c.ingest_state, c.processing_metadata,
                        sv.filepath as source_video_filepath, sv.title as source_title
                    FROM clips c
                    JOIN source_videos sv ON c.source_video_id = sv.id
                    WHERE c.id = %s FOR UPDATE;
                    """, (clip_id,)
                )
                original_clip_data = cur.fetchone()

                if not original_clip_data: raise ValueError(f"Original clip {clip_id} not found.")
                if original_clip_data['ingest_state'] != 'pending_split': raise ValueError(f"Clip {clip_id} not in 'pending_split' state.")
                if not original_clip_data['source_video_filepath']: raise ValueError("Source video filepath missing.")
                if not original_clip_data['processing_metadata'] or 'split_request_at' not in original_clip_data['processing_metadata']:
                    # Attempt to load JSON string if direct access fails (robustness for different psycopg2/postgres versions)
                    metadata_val = original_clip_data['processing_metadata']
                    try:
                         # Try loading if it's a string representation
                         if isinstance(metadata_val, str):
                             metadata_dict = json.loads(metadata_val)
                             if 'split_request_at' not in metadata_dict:
                                 raise ValueError("Missing 'split_request_at' key after JSON load.")
                             original_clip_data['processing_metadata'] = metadata_dict # Replace string with dict
                         elif isinstance(metadata_val, dict) and 'split_request_at' in metadata_val:
                             pass # Already a dict with the key
                         else:
                              raise ValueError("Metadata is not a dict or valid JSON string, or missing key.")
                    except (json.JSONDecodeError, ValueError) as json_err:
                         logger.error(f"Failed to parse processing_metadata for clip {clip_id}: {json_err}", exc_info=True)
                         logger.error(f"Metadata content: {metadata_val}")
                         raise ValueError(f"Invalid or missing 'split_request_at' in metadata for clip {clip_id}.") from json_err


                source_video_id = original_clip_data['source_video_id']
                cur.execute("SELECT pg_advisory_xact_lock(1, %s)", (source_video_id,))
                logger.debug(f"Acquired lock for source video {source_video_id}")

                # Update state to 'splitting'
                cur.execute("UPDATE clips SET ingest_state = 'splitting', updated_at = NOW() WHERE id = %s", (clip_id,))
                logger.info(f"Set original clip {clip_id} state to 'splitting'")
            # --- End Cursor Context ---
            conn.commit() # Commit initial check/state update
            logger.debug("Initial check/state update committed.")
        except (ValueError, psycopg2.DatabaseError) as db_err:
            logger.error(f"DB Error during initial check/update for split: {db_err}", exc_info=True)
            if conn: conn.rollback()
            raise

        # === Main Processing (Outside DB Transaction) ===
        relative_split_time = original_clip_data['processing_metadata']['split_request_at']
        original_start_time = original_clip_data['start_time_seconds']
        original_end_time = original_clip_data['end_time_seconds']

        # Ensure times are valid floats
        if not isinstance(original_start_time, (int, float)) or not isinstance(original_end_time, (int, float)):
            raise ValueError(f"Invalid start/end times for clip {clip_id}: start={original_start_time}, end={original_end_time}")

        absolute_split_time = original_start_time + relative_split_time

        # Validate calculated absolute time
        # Add tolerance for floating point comparison
        time_tolerance = 1e-6
        if absolute_split_time <= (original_start_time - time_tolerance) or absolute_split_time >= (original_end_time + time_tolerance):
             raise ValueError(f"Calculated absolute split time {absolute_split_time:.3f}s is outside original clip bounds ({original_start_time:.3f}s - {original_end_time:.3f}s). Relative time: {relative_split_time:.3f}s")

        # Download FULL Source Video
        source_s3_key = original_clip_data['source_video_filepath']
        local_source_path = temp_dir / Path(source_s3_key).name
        logger.info(f"Downloading full source video s3://{S3_BUCKET_NAME}/{source_s3_key} to {local_source_path}...")
        s3_client.download_file(S3_BUCKET_NAME, source_s3_key, str(local_source_path))

        # --- Extract New Clips ---
        base_identifier = original_clip_data['clip_identifier']
        clips_to_create = [] # Store details for DB insertion [(identifier, path, s3_key, start, end)]

        # Define common ffmpeg options for re-encoding
        ffmpeg_encode_options = [
            '-map', '0:v:0?', '-map', '0:a:0?', # Map first video/audio streams if present
            '-c:v', 'libx264', '-preset', 'medium', '-crf', '23', '-pix_fmt', 'yuv420p', # Video encoding
            '-c:a', 'aac', '-b:a', '128k', # Audio encoding
            '-movflags', '+faststart' # Optimize for web streaming
        ]

        # -- Clip A --
        clip_a_identifier = f"{base_identifier}_split_a"
        clip_a_filename = f"{sanitize_filename(clip_a_identifier)}.mp4"
        local_clip_a_path = temp_dir / clip_a_filename
        clip_a_s3_key = f"{CLIP_S3_PREFIX}{clip_a_filename}"
        clip_a_start = original_start_time
        clip_a_end = absolute_split_time # End time is exclusive for 'to'
        clip_a_duration = clip_a_end - clip_a_start

        if clip_a_duration >= MIN_CLIP_DURATION_SECONDS:
            logger.info(f"Extracting Clip A: {clip_a_identifier} (Time {clip_a_start:.3f}s - {clip_a_end:.3f}s)")
            ffmpeg_extract_cmd_a = [
                FFMPEG_PATH, '-y',
                '-i', str(local_source_path),
                '-ss', str(clip_a_start),
                '-to', str(clip_a_end), # Use 'to' for precise end time
                 *ffmpeg_encode_options, # Unpack encoding options
                str(local_clip_a_path)
            ]
            run_ffmpeg_command(ffmpeg_extract_cmd_a, f"ffmpeg Extract Clip A")
            clips_to_create.append((clip_a_identifier, local_clip_a_path, clip_a_s3_key, clip_a_start, clip_a_end))
        else:
             logger.warning(f"Skipping Clip A: Duration {clip_a_duration:.3f}s < minimum {MIN_CLIP_DURATION_SECONDS}s.")

        # -- Clip B --
        clip_b_identifier = f"{base_identifier}_split_b"
        clip_b_filename = f"{sanitize_filename(clip_b_identifier)}.mp4"
        local_clip_b_path = temp_dir / clip_b_filename
        clip_b_s3_key = f"{CLIP_S3_PREFIX}{clip_b_filename}"
        clip_b_start = absolute_split_time # Start time is inclusive for 'ss'
        clip_b_end = original_end_time
        clip_b_duration = clip_b_end - clip_b_start

        if clip_b_duration >= MIN_CLIP_DURATION_SECONDS:
            logger.info(f"Extracting Clip B: {clip_b_identifier} (Time {clip_b_start:.3f}s - {clip_b_end:.3f}s)")
            ffmpeg_extract_cmd_b = [
                FFMPEG_PATH, '-y',
                '-i', str(local_source_path),
                '-ss', str(clip_b_start), # Use 'ss' for start
                '-to', str(clip_b_end),   # Use 'to' for end
                *ffmpeg_encode_options, # Unpack encoding options
                str(local_clip_b_path)
            ]
            run_ffmpeg_command(ffmpeg_extract_cmd_b, f"ffmpeg Extract Clip B")
            clips_to_create.append((clip_b_identifier, local_clip_b_path, clip_b_s3_key, clip_b_start, clip_b_end))
        else:
            logger.warning(f"Skipping Clip B: Duration {clip_b_duration:.3f}s < minimum {MIN_CLIP_DURATION_SECONDS}s.")


        # === Final Database Updates (New Clips & Archive Original) ===
        try:
            with conn.cursor() as cur:
                 cur.execute("SELECT pg_advisory_xact_lock(2, %s)", (clip_id,))
                 cur.execute("SELECT pg_advisory_xact_lock(1, %s)", (source_video_id,))

                 if not clips_to_create:
                     logger.warning(f"Neither split segment met minimum duration. Archiving original clip {clip_id} without creating new clips.")
                     final_original_state = 'archived'
                     final_error_message = f"Split resulted in 0 valid clips (requested time: {relative_split_time:.3f}s). Both segments below min duration."
                 else:
                    logger.info(f"Uploading and recording {len(clips_to_create)} new clip(s)...")
                    for identifier, local_path, s3_key, start_time, end_time in clips_to_create:
                        try:
                            logger.debug(f"Uploading {local_path.name} to s3://{S3_BUCKET_NAME}/{s3_key}")
                            with open(local_path, "rb") as f: s3_client.upload_fileobj(f, S3_BUCKET_NAME, s3_key)

                            # Insert new clip record
                            cur.execute(
                                """
                                INSERT INTO clips (source_video_id, clip_filepath, clip_identifier, start_frame, end_frame, start_time_seconds, end_time_seconds, ingest_state, created_at, updated_at)
                                VALUES (%s, %s, %s, NULL, NULL, %s, %s, 'pending_review', NOW(), NOW()) RETURNING id;
                                """, # Setting start/end frames NULL for now
                                (source_video_id, s3_key, identifier, start_time, end_time)
                            )
                            created_clip_id = cur.fetchone()[0]
                            new_clip_ids.append(created_clip_id)
                            logger.info(f"Created new split clip record with ID: {created_clip_id} ({identifier})")
                        except (ClientError, psycopg2.DatabaseError) as upload_db_err:
                             # If one clip fails, rollback all new clip creations for this split
                             logger.error(f"Failed to upload/record new clip {identifier}: {upload_db_err}", exc_info=True)
                             raise RuntimeError(f"Failed to process new clip {identifier}") from upload_db_err

                    # If loop completed successfully
                    final_original_state = 'archived'
                    final_error_message = f'Split into {len(new_clip_ids)} clip(s): {",".join(map(str, new_clip_ids))}'
                    logger.info(f"Successfully created {len(new_clip_ids)} new clips.")

                 # Update Original Clip
                 logger.info(f"Updating original clip {clip_id} final state to '{final_original_state}'")
                 cur.execute(
                     """
                     UPDATE clips SET ingest_state = %s,
                            last_error = %s,
                            processing_metadata = NULL, -- Clear split request metadata
                            updated_at = NOW()
                     WHERE id = %s AND ingest_state = 'splitting';
                     """, (final_original_state, final_error_message, clip_id)
                 )
            # --- End Cursor Context ---
            conn.commit() # Commit final updates
            logger.info(f"TASK [Split]: Finished for original clip {clip_id}. Final State: {final_original_state}. New Clips Created: {len(new_clip_ids)}")
            return {"status": "success", "created_clip_ids": new_clip_ids, "original_clip_archived": (final_original_state == 'archived')}

        except (ValueError, psycopg2.DatabaseError, RuntimeError) as db_err:
            logger.error(f"DB or Upload Error during final update/creation for split: {db_err}", exc_info=True)
            if conn: conn.rollback()
            final_original_state = 'split_failed' # Ensure state reflects failure on partial success/rollback
            raise # Re-raise to trigger main error handling

    # === Main Error Handling ===
    except Exception as e:
        logger.error(f"TASK FAILED [Split]: original clip_id {clip_id} - {e}", exc_info=True)
        final_original_state = 'split_failed' # Ensure state reflects failure
        if conn:
            try:
                conn.rollback()
                conn.autocommit = True
                with conn.cursor() as err_cur:
                    # Always try to set to split_failed on any exception during the task
                    err_cur.execute(
                        """
                        UPDATE clips SET ingest_state = 'split_failed', last_error = %s, updated_at = NOW()
                        WHERE id = %s AND ingest_state IN ('splitting', 'pending_split');
                        """, (f"Split failed: {type(e).__name__}: {str(e)[:450]}", clip_id)
                    )
                logger.info(f"Attempted to set original clip {clip_id} state to 'split_failed'.")
            except Exception as db_err: logger.error(f"Failed to update error state in DB after split failure: {db_err}")
        raise e # Re-raise original exception
    finally:
        # --- Cleanup ---
        if conn: conn.autocommit = True; conn.close(); logger.debug(f"DB connection closed for split task, original clip_id: {clip_id}")
        if temp_dir_obj:
            try: shutil.rmtree(temp_dir, ignore_errors=True); logger.info(f"Cleaned up temporary directory: {temp_dir}")
            except Exception as cleanup_err: logger.warning(f"Error cleaning up temp dir {temp_dir}: {cleanup_err}")
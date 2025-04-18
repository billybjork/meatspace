import os
import subprocess
import tempfile
from pathlib import Path
import shutil
import json
import math
import time
import cv2
import numpy as np
from prefect import task, get_run_logger
import psycopg2
from psycopg2 import sql, extras
from botocore.exceptions import ClientError

try:
    from utils.db_utils import get_db_connection
except ImportError:
    print("ERROR importing db_utils in split.py")
    def get_db_connection(): raise NotImplementedError("Dummy DB connection")

from .splice import (
    s3_client, S3_BUCKET_NAME, FFMPEG_PATH, CLIP_S3_PREFIX,
    run_ffmpeg_command, sanitize_filename, detect_scenes,
    MIN_CLIP_DURATION_SECONDS
)


@task(name="Split Clip at Time", retries=1, retry_delay_seconds=45)
def split_clip_task(clip_id: int):
    """
    Splits a single clip into two based on a time specified in its metadata.
    Optionally refines the split time using localized scene detection.
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
            with conn.cursor(cursor_factory=extras.DictCursor) as cur:
                cur.execute("SELECT pg_advisory_xact_lock(2, %s)", (clip_id,))
                logger.debug(f"Acquired lock for clip {clip_id}")
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

                # Robust metadata check
                metadata_val = original_clip_data['processing_metadata']
                split_request_at = None
                if isinstance(metadata_val, dict):
                    split_request_at = metadata_val.get('split_request_at')
                elif isinstance(metadata_val, str):
                    try:
                        metadata_dict = json.loads(metadata_val)
                        split_request_at = metadata_dict.get('split_request_at')
                        original_clip_data['processing_metadata'] = metadata_dict # Update if parsed
                    except json.JSONDecodeError:
                        logger.warning(f"Metadata for clip {clip_id} is invalid JSON string: {metadata_val}")
                if split_request_at is None:
                    raise ValueError(f"Invalid or missing 'split_request_at' in metadata for clip {clip_id}. Content: {metadata_val}")
                original_clip_data['processing_metadata']['split_request_at'] = float(split_request_at) # Ensure it's float

                source_video_id = original_clip_data['source_video_id']
                cur.execute("SELECT pg_advisory_xact_lock(1, %s)", (source_video_id,))
                logger.debug(f"Acquired lock for source video {source_video_id}")

                cur.execute("UPDATE clips SET ingest_state = 'splitting', updated_at = NOW() WHERE id = %s", (clip_id,))
                logger.info(f"Set original clip {clip_id} state to 'splitting'")
            conn.commit()
            logger.debug("Initial check/state update committed.")
        except (ValueError, psycopg2.DatabaseError, TypeError) as db_err: # Added TypeError
            logger.error(f"DB Error during initial check/update for split: {db_err}", exc_info=True)
            if conn: conn.rollback()
            raise

        # === Main Processing (Refinement & Splitting) ===
        relative_split_time = original_clip_data['processing_metadata']['split_request_at']
        original_start_time = original_clip_data['start_time_seconds']
        original_end_time = original_clip_data['end_time_seconds']

        if not isinstance(original_start_time, (int, float)) or not isinstance(original_end_time, (int, float)):
            raise ValueError(f"Invalid start/end times for clip {clip_id}: start={original_start_time}, end={original_end_time}")

        absolute_hint_time = original_start_time + relative_split_time
        final_absolute_split_time = absolute_hint_time # Default if refinement fails

        # --- Download Source Video ---
        source_s3_key = original_clip_data['source_video_filepath']
        local_source_path = temp_dir / Path(source_s3_key).name
        logger.info(f"Downloading full source video s3://{S3_BUCKET_NAME}/{source_s3_key} to {local_source_path}...")
        s3_client.download_file(S3_BUCKET_NAME, source_s3_key, str(local_source_path))

        # --- Refinement Logic ---
        refinement_window_seconds = 0.5 # +/- seconds around the hint
        refinement_threshold = 0.2     # More sensitive threshold for Bhattacharyya
        refinement_method = cv2.HISTCMP_BHATTACHARYYA # Good method for sensitivity

        logger.info(f"Attempting split refinement around user hint: {absolute_hint_time:.3f}s")
        try:
            window_start_abs = max(original_start_time, absolute_hint_time - refinement_window_seconds)
            window_end_abs = min(original_end_time, absolute_hint_time + refinement_window_seconds)
            window_duration = window_end_abs - window_start_abs

            if window_duration > 0.1: # Only refine if window is reasonably sized
                window_segment_path = temp_dir / "window_segment.mp4"
                logger.info(f"Extracting refinement window: {window_start_abs:.3f}s to {window_end_abs:.3f}s")
                # Extract window (re-encode, faster preset, maybe no audio)
                ffmpeg_extract_window_cmd = [
                    FFMPEG_PATH, '-y', '-i', str(local_source_path),
                    '-ss', str(window_start_abs), '-to', str(window_end_abs),
                    '-map', '0:v:0?', # Map video only
                    '-c:v', 'libx264', '-preset', 'ultrafast', '-crf', '18', '-pix_fmt', 'yuv420p',
                    '-an', # No audio needed for detection
                    str(window_segment_path)
                ]
                run_ffmpeg_command(ffmpeg_extract_window_cmd, "ffmpeg Extract Refinement Window")

                logger.info(f"Detecting scenes within window (Thresh={refinement_threshold}, Method={refinement_method})...")
                segment_scenes, segment_fps, _, _ = detect_scenes(
                    str(window_segment_path), # Pass path as string
                    threshold=refinement_threshold,
                    hist_method=refinement_method
                )

                if segment_scenes and len(segment_scenes) == 1 and segment_fps and segment_fps > 0:
                    # Exactly one cut found! Refine the time.
                    # Cut is the start frame of the *second* detected scene within the window.
                    cut_frame_in_window = segment_scenes[0][0] # start_frame of the first (and only) detected scene boundary
                    cut_time_in_window = cut_frame_in_window / segment_fps
                    refined_absolute_split_time = window_start_abs + cut_time_in_window

                    # Sanity check: Ensure refined time is strictly within original bounds
                    time_tolerance = 1e-6
                    if (original_start_time + time_tolerance) < refined_absolute_split_time < (original_end_time - time_tolerance):
                        logger.info(f"Refinement successful: Using CV-detected split time: {refined_absolute_split_time:.3f}s")
                        final_absolute_split_time = refined_absolute_split_time
                    else:
                        logger.warning(f"Refined time {refined_absolute_split_time:.3f}s out of original clip bounds ({original_start_time:.3f}s - {original_end_time:.3f}s). Reverting to user hint.")
                        # final_absolute_split_time remains absolute_hint_time
                elif segment_scenes:
                    logger.warning(f"Refinement ambiguous: Found {len(segment_scenes)} cuts in window. Using user hint.")
                elif segment_scenes is None: # detect_scenes failed
                     logger.warning("Refinement failed: Scene detection error on window segment. Using user hint.")
                else: # No cuts found
                    logger.info("Refinement: No clear cuts found near user hint. Using original hint time.")

            else:
                logger.warning("Refinement window too small or invalid, skipping refinement.")

        except Exception as refine_err:
            logger.error(f"Error during split refinement: {refine_err}. Falling back to user hint.", exc_info=True)
            # final_absolute_split_time remains absolute_hint_time

        # --- Final Validation & Clip Extraction ---
        time_tolerance = 1e-6
        if final_absolute_split_time <= (original_start_time - time_tolerance) or final_absolute_split_time >= (original_end_time + time_tolerance):
             # This check should ideally not fail if refinement logic is correct, but good safeguard
             raise ValueError(f"Final calculated split time {final_absolute_split_time:.3f}s is outside original clip bounds ({original_start_time:.3f}s - {original_end_time:.3f}s).")

        logger.info(f"Proceeding with split at final time: {final_absolute_split_time:.3f}s")
        base_identifier = original_clip_data['clip_identifier']
        clips_to_create = []

        ffmpeg_encode_options = [
            '-map', '0:v:0?', '-map', '0:a:0?',
            '-c:v', 'libx264', '-preset', 'medium', '-crf', '23', '-pix_fmt', 'yuv420p',
            '-c:a', 'aac', '-b:a', '128k',
            '-movflags', '+faststart'
        ]

        # -- Clip A --
        clip_a_start = original_start_time
        clip_a_end = final_absolute_split_time
        clip_a_duration = clip_a_end - clip_a_start
        if clip_a_duration >= MIN_CLIP_DURATION_SECONDS:
            clip_a_identifier = f"{base_identifier}_split_a"
            clip_a_filename = f"{sanitize_filename(clip_a_identifier)}.mp4"
            local_clip_a_path = temp_dir / clip_a_filename
            clip_a_s3_key = f"{CLIP_S3_PREFIX}{clip_a_filename}"
            logger.info(f"Extracting Clip A: {clip_a_identifier} (Time {clip_a_start:.3f}s - {clip_a_end:.3f}s)")
            ffmpeg_extract_cmd_a = [ FFMPEG_PATH, '-y', '-i', str(local_source_path), '-ss', str(clip_a_start), '-to', str(clip_a_end), *ffmpeg_encode_options, str(local_clip_a_path) ]
            run_ffmpeg_command(ffmpeg_extract_cmd_a, f"ffmpeg Extract Clip A")
            clips_to_create.append((clip_a_identifier, local_clip_a_path, clip_a_s3_key, clip_a_start, clip_a_end))
        else: logger.warning(f"Skipping Clip A: Duration {clip_a_duration:.3f}s < minimum.")

        # -- Clip B --
        clip_b_start = final_absolute_split_time
        clip_b_end = original_end_time
        clip_b_duration = clip_b_end - clip_b_start
        if clip_b_duration >= MIN_CLIP_DURATION_SECONDS:
            clip_b_identifier = f"{base_identifier}_split_b"
            clip_b_filename = f"{sanitize_filename(clip_b_identifier)}.mp4"
            local_clip_b_path = temp_dir / clip_b_filename
            clip_b_s3_key = f"{CLIP_S3_PREFIX}{clip_b_filename}"
            logger.info(f"Extracting Clip B: {clip_b_identifier} (Time {clip_b_start:.3f}s - {clip_b_end:.3f}s)")
            ffmpeg_extract_cmd_b = [ FFMPEG_PATH, '-y', '-i', str(local_source_path), '-ss', str(clip_b_start), '-to', str(clip_b_end), *ffmpeg_encode_options, str(local_clip_b_path) ]
            run_ffmpeg_command(ffmpeg_extract_cmd_b, f"ffmpeg Extract Clip B")
            clips_to_create.append((clip_b_identifier, local_clip_b_path, clip_b_s3_key, clip_b_start, clip_b_end))
        else: logger.warning(f"Skipping Clip B: Duration {clip_b_duration:.3f}s < minimum.")

        # === Final Database Updates ===
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
                            cur.execute(
                                """
                                INSERT INTO clips (source_video_id, clip_filepath, clip_identifier, start_frame, end_frame, start_time_seconds, end_time_seconds, ingest_state, created_at, updated_at)
                                VALUES (%s, %s, %s, NULL, NULL, %s, %s, 'pending_review', NOW(), NOW()) RETURNING id;
                                """, (source_video_id, s3_key, identifier, start_time, end_time)
                            )
                            created_clip_id = cur.fetchone()[0]
                            new_clip_ids.append(created_clip_id)
                            logger.info(f"Created new split clip record with ID: {created_clip_id} ({identifier})")
                        except (ClientError, psycopg2.DatabaseError) as upload_db_err:
                             logger.error(f"Failed to upload/record new clip {identifier}: {upload_db_err}", exc_info=True)
                             raise RuntimeError(f"Failed to process new clip {identifier}") from upload_db_err

                    final_original_state = 'archived'
                    final_error_message = f'Split (refined: {final_absolute_split_time != absolute_hint_time}) into {len(new_clip_ids)} clip(s): {",".join(map(str, new_clip_ids))}' # Add refinement info
                    logger.info(f"Successfully created {len(new_clip_ids)} new clips.")

                 logger.info(f"Updating original clip {clip_id} final state to '{final_original_state}'")
                 cur.execute(
                     """
                     UPDATE clips SET ingest_state = %s, last_error = %s, processing_metadata = NULL, updated_at = NOW()
                     WHERE id = %s AND ingest_state = 'splitting';
                     """, (final_original_state, final_error_message, clip_id)
                 )
            conn.commit()
            logger.info(f"TASK [Split]: Finished for original clip {clip_id}. Final State: {final_original_state}. New Clips Created: {len(new_clip_ids)}")
            return {"status": "success", "created_clip_ids": new_clip_ids, "original_clip_archived": (final_original_state == 'archived')}

        except (ValueError, psycopg2.DatabaseError, RuntimeError) as db_err:
            logger.error(f"DB or Upload Error during final update/creation for split: {db_err}", exc_info=True)
            if conn: conn.rollback()
            final_original_state = 'split_failed'
            raise

    # === Main Error Handling ===
    except Exception as e:
        logger.error(f"TASK FAILED [Split]: original clip_id {clip_id} - {e}", exc_info=True)
        final_original_state = 'split_failed'
        if conn:
            try:
                conn.rollback()
                conn.autocommit = True
                with conn.cursor() as err_cur:
                    err_cur.execute(
                        """
                        UPDATE clips SET ingest_state = 'split_failed', last_error = %s, updated_at = NOW()
                        WHERE id = %s AND ingest_state IN ('splitting', 'pending_split');
                        """, (f"Split failed: {type(e).__name__}: {str(e)[:450]}", clip_id)
                    )
                logger.info(f"Attempted to set original clip {clip_id} state to 'split_failed'.")
            except Exception as db_err: logger.error(f"Failed to update error state in DB after split failure: {db_err}")
        raise e
    finally:
        if conn: conn.autocommit = True; conn.close(); logger.debug(f"DB connection closed for split task, original clip_id: {clip_id}")
        if temp_dir_obj:
            try: shutil.rmtree(temp_dir, ignore_errors=True); logger.info(f"Cleaned up temporary directory: {temp_dir}")
            except Exception as cleanup_err: logger.warning(f"Error cleaning up temp dir {temp_dir}: {cleanup_err}")
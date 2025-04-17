# tasks/editing.py
import os
import subprocess
import tempfile
from pathlib import Path
import shutil
from prefect import task, get_run_logger
import cv2
import psycopg2
from psycopg2 import sql
import time
import numpy as np
from botocore.exceptions import ClientError

try:
    from utils.db_utils import get_db_connection
except ImportError:
    print("ERROR importing db_utils in editing.py")
    def get_db_connection(): raise NotImplementedError("Dummy DB connection")

# Import necessary components from splice and shared config
from .splice import (
    s3_client, S3_BUCKET_NAME, FFMPEG_PATH, CLIP_S3_PREFIX,
    run_ffmpeg_command, sanitize_filename, detect_scenes, # Need detect_scenes here
    MIN_CLIP_DURATION_SECONDS # Use same minimum duration
)

# Import necessary config like S3 client, bucket, ffmpeg path etc.
# (You might centralize config/clients later)
from .splice import s3_client, S3_BUCKET_NAME, FFMPEG_PATH, CLIP_S3_PREFIX, run_ffmpeg_command, sanitize_filename

@task(name="Merge Clips", retries=1, retry_delay_seconds=30)
def merge_clips_task(clip_id_1: int, clip_id_2: int):
    """
    Merges two adjacent clips into a new clip using ffmpeg concat demuxer.
    Archives the original clips and creates a new clip record in 'pending_review'.
    """
    logger = get_run_logger()
    logger.info(f"TASK [Merge]: Starting for clips {clip_id_1} and {clip_id_2}")
    conn = None
    temp_dir_obj = None
    new_clip_id = None

    # Basic validation
    if clip_id_1 == clip_id_2:
        raise ValueError("Cannot merge a clip with itself.")

    try:
        conn = get_db_connection()
        conn.autocommit = False

        temp_dir_obj = tempfile.TemporaryDirectory(prefix=f"meatspace_merge_")
        temp_dir = Path(temp_dir_obj.name)

        with conn.transaction():
            # === Transaction Start ===
            # Lock both clips AND the source video row for safety
            cur = conn.cursor()
            cur.execute("SELECT pg_advisory_xact_lock(2, %s)", (clip_id_1,))
            cur.execute("SELECT pg_advisory_xact_lock(2, %s)", (clip_id_2,))
            logger.debug(f"Acquired locks for clips {clip_id_1}, {clip_id_2}")

            # Fetch details for both clips, ensuring they belong to the same source
            cur.execute(
                """
                SELECT id, clip_filepath, clip_identifier, start_frame, end_frame, start_time_seconds, end_time_seconds, source_video_id, ingest_state
                FROM clips
                WHERE id = ANY(%s::int[]) ORDER BY start_frame; -- Order ensures clip1 is the earlier one
                """,
                ([clip_id_1, clip_id_2],)
            )
            results = cur.fetchall()

            if len(results) != 2:
                raise ValueError(f"Could not find both clips {clip_id_1}, {clip_id_2} for merging.")

            clip1_data = dict(zip([desc[0] for desc in cur.description], results[0]))
            clip2_data = dict(zip([desc[0] for desc in cur.description], results[1]))

            # --- Validations ---
            if clip1_data['source_video_id'] != clip2_data['source_video_id']:
                raise ValueError("Clips do not belong to the same source video.")
            # Ensure they are adjacent (or very close) based on frames/time if possible
            # Optional: Check if clip1.end_frame == clip2.start_frame
            # Check states allow merging (e.g., one is 'pending_merge_with_next', the other 'pending_review')
            if not (clip1_data['ingest_state'] == 'pending_merge_with_next' and clip2_data['ingest_state'] in ['pending_review', 'review_skipped']):
                 logger.warning(f"Unexpected states for merge: Clip1 ({clip1_data['id']}) state: {clip1_data['ingest_state']}, Clip2 ({clip2_data['id']}) state: {clip2_data['ingest_state']}")
                 # Decide whether to proceed or fail. Let's proceed cautiously.
                 # raise ValueError("Clips are not in the expected states for merging.")

            source_video_id = clip1_data['source_video_id']
            cur.execute("SELECT pg_advisory_xact_lock(1, %s)", (source_video_id,)) # Lock source
            logger.debug(f"Acquired lock for source video {source_video_id}")


            # --- Prepare for ffmpeg concat ---
            # 1. Download both clips from S3
            local_clip1_path = temp_dir / Path(clip1_data['clip_filepath']).name
            local_clip2_path = temp_dir / Path(clip2_data['clip_filepath']).name
            concat_list_path = temp_dir / "concat_list.txt"

            logger.info(f"Downloading clips to {temp_dir}...")
            s3_client.download_file(S3_BUCKET_NAME, clip1_data['clip_filepath'], str(local_clip1_path))
            s3_client.download_file(S3_BUCKET_NAME, clip2_data['clip_filepath'], str(local_clip2_path))

            # 2. Create concat list file (ensure safe filenames)
            with open(concat_list_path, "w") as f:
                f.write(f"file '{local_clip1_path.name}'\n")
                f.write(f"file '{local_clip2_path.name}'\n")
            logger.debug(f"Created concat list: {concat_list_path}")

            # --- Define new clip properties ---
            # Use identifier from the *first* clip as the base? Or create a new one?
            base_identifier = clip1_data['clip_identifier'] # Or maybe strip _clip_XXXX part and add new suffix?
            new_clip_identifier = f"{base_identifier}_merged" # Simple approach
            new_clip_filename = f"{sanitize_filename(new_clip_identifier)}.mp4"
            new_local_clip_path = temp_dir / new_clip_filename
            new_clip_s3_key = f"{CLIP_S3_PREFIX}{new_clip_filename}"

            new_start_frame = clip1_data['start_frame']
            new_end_frame = clip2_data['end_frame'] # End frame of the second clip
            new_start_time = clip1_data['start_time_seconds']
            new_end_time = clip2_data['end_time_seconds'] # End time of the second clip

            # --- Run ffmpeg concat ---
            ffmpeg_merge_cmd = [
                FFMPEG_PATH, '-y',
                '-f', 'concat', '-safe', '0', # Use concat demuxer
                '-i', str(concat_list_path),
                '-c', 'copy', # Try to copy streams without re-encoding if possible! Much faster.
                str(new_local_clip_path)
            ]
            try:
                run_ffmpeg_command(ffmpeg_merge_cmd, "ffmpeg Merge (copy)", cwd=str(temp_dir))
            except Exception as e:
                logger.warning(f"FFmpeg copy merge failed ({e}), attempting re-encode...")
                # Fallback to re-encoding if copy fails (e.g., different codecs/timebases)
                ffmpeg_merge_cmd_reencode = [
                    FFMPEG_PATH, '-y',
                    '-f', 'concat', '-safe', '0',
                    '-i', str(concat_list_path),
                    '-c:v', 'libx264', '-preset', 'medium', '-crf', '23', '-pix_fmt', 'yuv420p',
                    '-c:a', 'aac', '-b:a', '128k', # Or copy audio if compatible: '-c:a', 'copy',
                    '-movflags', '+faststart',
                    str(new_local_clip_path)
                ]
                run_ffmpeg_command(ffmpeg_merge_cmd_reencode, "ffmpeg Merge (re-encode)", cwd=str(temp_dir))


            # --- Upload new clip to S3 ---
            logger.info(f"Uploading merged clip to s3://{S3_BUCKET_NAME}/{new_clip_s3_key}")
            with open(new_local_clip_path, "rb") as f:
                s3_client.upload_fileobj(f, S3_BUCKET_NAME, new_clip_s3_key)

            # --- Update Database ---
            # 1. Create new clip record
            cur.execute(
                """
                INSERT INTO clips (source_video_id, clip_filepath, clip_identifier,
                                   start_frame, end_frame, start_time_seconds, end_time_seconds,
                                   ingest_state, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, 'pending_review', NOW(), NOW())
                RETURNING id;
                """,
                (source_video_id, new_clip_s3_key, new_clip_identifier,
                 new_start_frame, new_end_frame, new_start_time, new_end_time)
            )
            new_clip_id = cur.fetchone()[0]
            logger.info(f"Created new merged clip record with ID: {new_clip_id}")

            # 2. Archive original clips
            cur.execute(
                """
                UPDATE clips
                SET ingest_state = 'archived',
                    last_error = 'Merged into clip ' || %s::text,
                    updated_at = NOW()
                WHERE id = ANY(%s::int[]);
                """,
                (new_clip_id, [clip_id_1, clip_id_2])
            )
            logger.info(f"Archived original clips {clip_id_1}, {clip_id_2}")

        # === Transaction End (Commit) ===
        logger.info(f"Merge successful. New clip ID: {new_clip_id}")
        return {"status": "success", "new_clip_id": new_clip_id}

    except Exception as e:
        logger.error(f"TASK FAILED [Merge]: clips {clip_id_1}, {clip_id_2} - {e}", exc_info=True)
        if conn:
            try:
                conn.rollback()
                conn.autocommit = True
                with conn.cursor() as err_cur:
                    # Set original clips to merge_failed
                    err_cur.execute(
                        """
                        UPDATE clips SET ingest_state = 'merge_failed', last_error = %s, updated_at = NOW()
                        WHERE id = ANY(%s::int[]) AND ingest_state IN ('pending_merge_with_next', 'pending_review', 'review_skipped');
                        """,
                        (f"{type(e).__name__}: {str(e)[:500]}", [clip_id_1, clip_id_2])
                    )
                logger.info(f"Set original clips {clip_id_1}, {clip_id_2} state to 'merge_failed'.")
            except Exception as db_err:
                logger.error(f"Failed to update error state in DB after merge failure: {db_err}")
        raise e
    finally:
         if conn:
             conn.autocommit = True
             conn.close()
         if temp_dir_obj:
             try:
                shutil.rmtree(temp_dir)
                logger.debug(f"Cleaned up temp dir: {temp_dir}")
             except Exception as cleanup_err:
                 logger.warning(f"Error cleaning up temp dir {temp_dir}: {cleanup_err}")


# Define the specific retry parameters (as requested)
RETRY_SCENE_DETECT_METHOD = cv2.HISTCMP_BHATTACHARYYA
RETRY_SCENE_DETECT_THRESHOLD = 0.3

@task(name="Re-Splice Clip Segment", retries=1, retry_delay_seconds=60)
def resplice_clip_task(clip_id: int):
    """
    Takes a clip marked for re-splicing, extracts its corresponding segment
    from the source video, runs scene detection with retry parameters on that
    segment, creates new clips from the detected sub-scenes, and archives the original.
    """
    logger = get_run_logger()
    logger.info(f"TASK [Re-Splice]: Starting for original clip_id: {clip_id}")
    conn = None
    temp_dir_obj = None
    new_clip_ids = []
    processed_new_clip_count = 0
    failed_new_clip_count = 0

    try:
        conn = get_db_connection()
        conn.autocommit = False
        temp_dir_obj = tempfile.TemporaryDirectory(prefix=f"meatspace_resplice_{clip_id}_")
        temp_dir = Path(temp_dir_obj.name)
        logger.info(f"Using temporary directory: {temp_dir}")

        with conn.transaction():
            # === Transaction Start ===
            cur = conn.cursor()
            # Lock original clip and source video
            cur.execute("SELECT pg_advisory_xact_lock(2, %s)", (clip_id,))
            logger.debug(f"Acquired lock for clip {clip_id}")

            # Fetch original clip details and source video path
            cur.execute(
                """
                SELECT
                    c.clip_filepath, c.clip_identifier, c.start_frame, c.end_frame,
                    c.start_time_seconds, c.end_time_seconds, c.source_video_id, c.ingest_state,
                    sv.filepath as source_video_filepath,
                    sv.title as source_title
                FROM clips c
                JOIN source_videos sv ON c.source_video_id = sv.id
                WHERE c.id = %s FOR UPDATE; -- Lock the row
                """, (clip_id,)
            )
            result = cur.fetchone()
            if not result: raise ValueError(f"Original clip {clip_id} not found.")
            clip_data = dict(zip([desc[0] for desc in cur.description], result))

            # --- Validations ---
            if clip_data['ingest_state'] != 'pending_resplice':
                 raise ValueError(f"Clip {clip_id} is not in 'pending_resplice' state (state: {clip_data['ingest_state']}). Task should not have been triggered.")
            if not clip_data['source_video_filepath']:
                 raise ValueError(f"Source video filepath not found for clip {clip_id}.")
            if clip_data['start_time_seconds'] is None or clip_data['end_time_seconds'] is None:
                 raise ValueError(f"Missing start/end time for original clip {clip_id}, cannot extract segment.")

            source_video_id = clip_data['source_video_id']
            cur.execute("SELECT pg_advisory_xact_lock(1, %s)", (source_video_id,)) # Lock source
            logger.debug(f"Acquired lock for source video {source_video_id}")

            # --- Set Original Clip State to 'resplicing' ---
            cur.execute("UPDATE clips SET ingest_state = 'resplicing', updated_at = NOW() WHERE id = %s", (clip_id,))
            logger.info(f"Set original clip {clip_id} state to 'resplicing'")
            # --- End Initial DB Update ---

        # === Main Processing (Outside initial transaction scope, but before final commit) ===
        # 1. Download FULL Source Video
        source_s3_key = clip_data['source_video_filepath']
        local_source_path = temp_dir / Path(source_s3_key).name
        logger.info(f"Downloading full source video s3://{S3_BUCKET_NAME}/{source_s3_key} to {local_source_path}...")
        s3_client.download_file(S3_BUCKET_NAME, source_s3_key, str(local_source_path))
        logger.info("Source video download complete.")

        # 2. Extract Original Clip's Segment using ffmpeg
        original_start_time = clip_data['start_time_seconds']
        original_end_time = clip_data['end_time_seconds']
        original_duration = original_end_time - original_start_time
        if original_duration <= 0:
             raise ValueError(f"Invalid duration calculated for original clip {clip_id}.")

        segment_filename = f"segment_{clip_id}.mp4"
        local_segment_path = temp_dir / segment_filename
        logger.info(f"Extracting segment (Time {original_start_time:.2f}s to {original_end_time:.2f}s) to {local_segment_path}...")
        ffmpeg_extract_segment_cmd = [
            FFMPEG_PATH, '-y',
            '-i', str(local_source_path),
            '-ss', str(original_start_time),
            '-to', str(original_end_time),
            '-c', 'copy', # Try copying streams first for speed
            str(local_segment_path)
        ]
        try:
            run_ffmpeg_command(ffmpeg_extract_segment_cmd, f"ffmpeg Extract Segment {clip_id} (copy)")
        except Exception as copy_err:
             logger.warning(f"FFmpeg segment copy failed ({copy_err}), attempting re-encode...")
             ffmpeg_extract_segment_cmd_reencode = [
                FFMPEG_PATH, '-y',
                '-i', str(local_source_path),
                '-ss', str(original_start_time),
                '-to', str(original_end_time),
                 '-map', '0:v:0?', '-map', '0:a:0?',
                 '-c:v', 'libx264', '-preset', 'medium', '-crf', '23', '-pix_fmt', 'yuv420p',
                 '-c:a', 'aac', '-b:a', '128k',
                 '-movflags', '+faststart',
                 str(local_segment_path)
             ]
             run_ffmpeg_command(ffmpeg_extract_segment_cmd_reencode, f"ffmpeg Extract Segment {clip_id} (re-encode)")
        logger.info("Segment extraction complete.")

        # 3. Detect Scenes within the Extracted Segment
        logger.info(f"Detecting scenes within the segment using Method={RETRY_SCENE_DETECT_METHOD}, Threshold={RETRY_SCENE_DETECT_THRESHOLD}...")
        segment_scenes, segment_fps, _, _ = detect_scenes(
            str(local_segment_path),
            threshold=RETRY_SCENE_DETECT_THRESHOLD,
            hist_method=RETRY_SCENE_DETECT_METHOD
        )

        if segment_scenes is None:
            raise RuntimeError("Scene detection failed on the extracted segment.")
        if not segment_scenes:
            logger.warning("No sub-scenes detected within the segment using retry parameters. No new clips will be created.")
        else:
            logger.info(f"Detected {len(segment_scenes)} potential sub-scenes within the segment.")

        # --- Start *Final* Transaction for new clips and archiving original ---
        with conn.transaction():
             # === Transaction Start (Final DB Updates) ===
             cur = conn.cursor() # Re-acquire cursor for this transaction

             if not segment_scenes:
                 logger.info("Skipping new clip creation loop as no sub-scenes were detected.")
             else:
                 base_identifier = clip_data['clip_identifier']
                 sanitized_source_title = sanitize_filename(clip_data['source_title'])

                 for idx, (start_frame, end_frame_exclusive) in enumerate(segment_scenes):
                    try:
                        if segment_fps <= 0:
                             logger.warning(f"Invalid segment FPS ({segment_fps}), cannot calculate times for sub-scene {idx}. Skipping.")
                             failed_new_clip_count += 1
                             continue

                        # Calculate times RELATIVE TO THE SEGMENT start
                        segment_start_time = start_frame / segment_fps
                        segment_end_time = end_frame_exclusive / segment_fps
                        segment_duration = segment_end_time - segment_start_time

                        # Calculate ABSOLUTE times/frames relative to the ORIGINAL SOURCE
                        new_clip_start_time = original_start_time + segment_start_time
                        new_clip_end_time = original_start_time + segment_end_time
                        # Frame calculation is harder without original FPS, estimate or set NULL
                        # If original clip_data['start_frame'] is not None and original_fps is known:
                        # new_clip_start_frame = clip_data['start_frame'] + round(segment_start_time * original_fps)
                        # new_clip_end_frame = clip_data['start_frame'] + round(segment_end_time * original_fps)
                        # For simplicity now, let's set frames to NULL
                        new_clip_start_frame = None
                        new_clip_end_frame = None

                        # Check Minimum Duration
                        if segment_duration < MIN_CLIP_DURATION_SECONDS:
                            logger.info(f"Skipping sub-scene {idx}: Duration {segment_duration:.2f}s < minimum {MIN_CLIP_DURATION_SECONDS}s.")
                            continue

                        # Generate identifiers and paths for the NEW clip
                        new_clip_sub_id = chr(ord('a') + idx) # a, b, c...
                        new_clip_identifier = f"{base_identifier}_resplice_{new_clip_sub_id}"
                        new_clip_filename = f"{sanitize_filename(new_clip_identifier)}.mp4"
                        new_local_clip_path = temp_dir / new_clip_filename
                        new_clip_s3_key = f"{CLIP_S3_PREFIX}{new_clip_filename}"

                        logger.info(f"Extracting new clip {idx}: {new_clip_identifier} (Abs Time {new_clip_start_time:.2f}s-{new_clip_end_time:.2f}s)")

                        # Extract NEW clip using ffmpeg from FULL SOURCE using ABSOLUTE times
                        ffmpeg_extract_new_clip_cmd = [
                            FFMPEG_PATH, '-y',
                            '-i', str(local_source_path), # Input is the full source
                            '-ss', str(new_clip_start_time), # Absolute start time
                            '-to', str(new_clip_end_time),   # Absolute end time
                            '-map', '0:v:0?', '-map', '0:a:0?',
                            '-c:v', 'libx264', '-preset', 'medium', '-crf', '23', '-pix_fmt', 'yuv420p',
                            '-c:a', 'aac', '-b:a', '128k',
                            '-movflags', '+faststart',
                            str(new_local_clip_path)
                        ]
                        run_ffmpeg_command(ffmpeg_extract_new_clip_cmd, f"ffmpeg Extract New Clip {idx}")

                        # Upload new clip to S3
                        logger.debug(f"Uploading {new_local_clip_path.name} to s3://{S3_BUCKET_NAME}/{new_clip_s3_key}")
                        with open(new_local_clip_path, "rb") as f:
                            s3_client.upload_fileobj(f, S3_BUCKET_NAME, new_clip_s3_key)
                        logger.debug("S3 upload successful.")

                        # Insert new clip record into DB
                        cur.execute(
                            """
                            INSERT INTO clips (source_video_id, clip_filepath, clip_identifier,
                                               start_frame, end_frame, start_time_seconds, end_time_seconds,
                                               ingest_state, created_at, updated_at)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, 'pending_review', NOW(), NOW())
                            RETURNING id;
                            """,
                            (source_video_id, new_clip_s3_key, new_clip_identifier,
                             new_clip_start_frame, new_clip_end_frame,
                             new_clip_start_time, new_clip_end_time)
                        )
                        created_clip_id = cur.fetchone()[0]
                        new_clip_ids.append(created_clip_id)
                        processed_new_clip_count += 1
                        logger.info(f"Successfully processed and recorded new clip_id: {created_clip_id}")

                    except (ClientError, psycopg2.DatabaseError, subprocess.CalledProcessError, Exception) as clip_err:
                         failed_new_clip_count += 1
                         logger.error(f"Failed to process/extract/upload/record new clip {idx} derived from original {clip_id}: {clip_err}", exc_info=True)
                         # Continue processing other potential sub-clips

             # 4. Archive Original Clip (within the final transaction)
             final_original_state = 'archived' # Default to archived
             final_error_message = f'Re-spliced into {len(new_clip_ids)} clips: {",".join(map(str, new_clip_ids))}'
             if processed_new_clip_count == 0 and failed_new_clip_count > 0:
                  final_original_state = 'resplice_failed'
                  final_error_message = f"Re-splicing failed: All {failed_new_clip_count} detected sub-scenes failed processing."
             elif processed_new_clip_count == 0 and failed_new_clip_count == 0:
                  # This means either 0 scenes detected, or all were too short
                  final_error_message = "Re-splicing resulted in 0 new clips (no scenes detected or all below min duration)."
                  # Keep state as archived

             logger.info(f"Updating original clip {clip_id} final state to '{final_original_state}'")
             cur.execute(
                 """
                 UPDATE clips
                 SET ingest_state = %s,
                     last_error = %s,
                     updated_at = NOW()
                 WHERE id = %s AND ingest_state = 'resplicing'; -- Ensure it wasn't changed concurrently
                 """,
                 (final_original_state, final_error_message, clip_id)
             )

             # Commit new clip inserts and original clip update
             # === Transaction End (Commit) ===
             logger.info(f"TASK [Re-Splice]: Finished for original clip {clip_id}. Final State: {final_original_state}. New Clips Created: {processed_new_clip_count}, New Clips Failed: {failed_new_clip_count}")

        return {
            "status": "success" if final_original_state == 'archived' else final_original_state,
            "processed_new_clips": processed_new_clip_count,
            "failed_new_clips": failed_new_clip_count,
            "detected_sub_scenes": len(segment_scenes) if segment_scenes else 0,
            "created_clip_ids": new_clip_ids
        }

    except Exception as e:
        task_name = "Re-Splice"
        logger.error(f"TASK FAILED ({task_name}): original clip_id {clip_id} - {e}", exc_info=True)
        if conn:
            try:
                conn.rollback() # Rollback any partial transaction
                conn.autocommit = True
                with conn.cursor() as err_cur:
                    # Try to set the original clip state to failed if it was in 'resplicing' or 'pending_resplice'
                    err_cur.execute(
                        """
                        UPDATE clips
                        SET ingest_state = 'resplice_failed',
                            last_error = %s,
                            updated_at = NOW()
                        WHERE id = %s AND ingest_state IN ('resplicing', 'pending_resplice')
                        """,
                        (f"{type(e).__name__}: {str(e)[:500]}", clip_id)
                    )
                logger.info(f"Attempted to set original clip {clip_id} state to 'resplice_failed'.")
            except Exception as db_err:
                logger.error(f"Failed to update error state in DB after rollback: {db_err}")
        raise # Re-raise the exception for Prefect to capture

    finally:
        # Cleanup remains the same
        if conn:
            conn.autocommit = True
            conn.close()
            logger.debug(f"DB connection closed for original clip_id: {clip_id}")
        if temp_dir_obj:
            try:
                shutil.rmtree(temp_dir) # Use shutil.rmtree for directory
                logger.info(f"Cleaned up temporary directory: {temp_dir}")
            except Exception as cleanup_err:
                 logger.warning(f"Error cleaning up temporary directory {temp_dir}: {cleanup_err}")
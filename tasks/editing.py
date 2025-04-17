# tasks/editing.py
import os
import subprocess
import tempfile
from pathlib import Path
import shutil
from prefect import task, get_run_logger
import psycopg2
from psycopg2 import sql

# Assumes db_utils and S3 setup are similar to splice.py/keyframe.py
try:
    from utils.db_utils import get_db_connection #, get_media_base_dir # May not need media base dir if operating purely on S3 keys
except ImportError:
    # Handle import errors as in other task files
    print("ERROR importing db_utils in editing.py")
    def get_db_connection(): raise NotImplementedError("Dummy DB connection")

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


@task(name="Split Clip", retries=1, retry_delay_seconds=30)
def split_clip_task(clip_id: int, split_at_seconds: float):
    """
    Splits a clip into two at the specified time using ffmpeg.
    Archives the original clip and creates two new clip records in 'pending_review'.
    """
    logger = get_run_logger()
    logger.info(f"TASK [Split]: Starting for clip {clip_id} at {split_at_seconds}s")
    conn = None
    temp_dir_obj = None
    new_clip_ids = []

    try:
        conn = get_db_connection()
        conn.autocommit = False
        temp_dir_obj = tempfile.TemporaryDirectory(prefix=f"meatspace_split_")
        temp_dir = Path(temp_dir_obj.name)

        with conn.transaction():
            # Lock clip and source
            cur = conn.cursor()
            cur.execute("SELECT pg_advisory_xact_lock(2, %s)", (clip_id,))

            # Fetch clip details
            cur.execute(
                """
                SELECT clip_filepath, clip_identifier, start_frame, end_frame, start_time_seconds, end_time_seconds,
                       source_video_id, ingest_state, COALESCE(end_time_seconds - start_time_seconds, 0) as duration
                FROM clips WHERE id = %s;
                """, (clip_id,)
            )
            result = cur.fetchone()
            if not result: raise ValueError(f"Clip {clip_id} not found.")
            clip_data = dict(zip([desc[0] for desc in cur.description], result))

            # --- Validations ---
            if clip_data['ingest_state'] != 'pending_split':
                # Maybe allow splitting from other states if needed?
                raise ValueError(f"Clip {clip_id} is not in 'pending_split' state (state: {clip_data['ingest_state']}).")
            if split_at_seconds <= 0 or split_at_seconds >= clip_data['duration']:
                 raise ValueError(f"Split time {split_at_seconds}s is outside the clip duration (0 - {clip_data['duration']:.2f}s).")

            source_video_id = clip_data['source_video_id']
            cur.execute("SELECT pg_advisory_xact_lock(1, %s)", (source_video_id,))
            logger.debug(f"Acquired locks for clip {clip_id} and source {source_video_id}")

            # --- Prepare paths and download ---
            local_clip_path = temp_dir / Path(clip_data['clip_filepath']).name
            logger.info(f"Downloading clip to {temp_dir}...")
            s3_client.download_file(S3_BUCKET_NAME, clip_data['clip_filepath'], str(local_clip_path))

            # --- Define properties for new clips ---
            base_identifier = clip_data['clip_identifier']
            # Clip A (Part 1)
            clip_a_identifier = f"{base_identifier}_split_a"
            clip_a_filename = f"{sanitize_filename(clip_a_identifier)}.mp4"
            clip_a_local_path = temp_dir / clip_a_filename
            clip_a_s3_key = f"{CLIP_S3_PREFIX}{clip_a_filename}"
            clip_a_start_time = clip_data['start_time_seconds']
            clip_a_end_time = clip_data['start_time_seconds'] + split_at_seconds # Relative end time for ffmpeg
            clip_a_duration = split_at_seconds

            # Clip B (Part 2)
            clip_b_identifier = f"{base_identifier}_split_b"
            clip_b_filename = f"{sanitize_filename(clip_b_identifier)}.mp4"
            clip_b_local_path = temp_dir / clip_b_filename
            clip_b_s3_key = f"{CLIP_S3_PREFIX}{clip_b_filename}"
            clip_b_start_time_abs = clip_data['start_time_seconds'] + split_at_seconds # Absolute start time for DB
            clip_b_end_time = clip_data['end_time_seconds'] # Absolute end time for DB
            clip_b_duration = clip_data['duration'] - split_at_seconds

            # --- Run ffmpeg commands ---
            # Clip A (from start to split point)
            ffmpeg_split_a_cmd = [
                FFMPEG_PATH, '-y',
                '-i', str(local_clip_path),
                '-ss', '0', # Start from beginning of input clip
                '-t', str(clip_a_duration), # Duration of part A
                '-map', '0:v:0?', '-map', '0:a:0?',
                '-c:v', 'libx264', '-preset', 'medium', '-crf', '23', '-pix_fmt', 'yuv420p', # Re-encode
                '-c:a', 'aac', '-b:a', '128k', # Re-encode audio
                '-movflags', '+faststart',
                str(clip_a_local_path)
            ]
            run_ffmpeg_command(ffmpeg_split_a_cmd, "ffmpeg Split Part A")

            # Clip B (from split point to end)
            ffmpeg_split_b_cmd = [
                FFMPEG_PATH, '-y',
                '-i', str(local_clip_path),
                '-ss', str(split_at_seconds), # Start from the split point within the input clip
                '-t', str(clip_b_duration), # Duration of part B (optional, ffmpeg usually goes to end)
                '-map', '0:v:0?', '-map', '0:a:0?',
                 '-c:v', 'libx264', '-preset', 'medium', '-crf', '23', '-pix_fmt', 'yuv420p',
                '-c:a', 'aac', '-b:a', '128k',
                 '-movflags', '+faststart',
                str(clip_b_local_path)
            ]
            run_ffmpeg_command(ffmpeg_split_b_cmd, "ffmpeg Split Part B")

            # --- Upload new clips to S3 ---
            logger.info(f"Uploading split clips to S3...")
            with open(clip_a_local_path, "rb") as f: s3_client.upload_fileobj(f, S3_BUCKET_NAME, clip_a_s3_key)
            with open(clip_b_local_path, "rb") as f: s3_client.upload_fileobj(f, S3_BUCKET_NAME, clip_b_s3_key)

            # --- Update Database ---
             # Frame numbers are harder to calculate accurately without video info, maybe set NULL?
            # Or estimate based on time and original FPS if available. For now, set NULL.
            clip_a_start_frame = clip_data['start_frame']
            clip_a_end_frame = None # Unknown without FPS
            clip_b_start_frame = None # Unknown without FPS
            clip_b_end_frame = clip_data['end_frame']

            # Insert Clip A
            cur.execute(
                """INSERT INTO clips (source_video_id, clip_filepath, clip_identifier, start_frame, end_frame, start_time_seconds, end_time_seconds, ingest_state, created_at, updated_at)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, 'pending_review', NOW(), NOW()) RETURNING id;""",
                (source_video_id, clip_a_s3_key, clip_a_identifier, clip_a_start_frame, clip_a_end_frame, clip_a_start_time, clip_a_end_time)
            )
            new_clip_ids.append(cur.fetchone()[0])

            # Insert Clip B
            cur.execute(
                 """INSERT INTO clips (source_video_id, clip_filepath, clip_identifier, start_frame, end_frame, start_time_seconds, end_time_seconds, ingest_state, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, 'pending_review', NOW(), NOW()) RETURNING id;""",
                 (source_video_id, clip_b_s3_key, clip_b_identifier, clip_b_start_frame, clip_b_end_frame, clip_b_start_time_abs, clip_b_end_time)
             )
            new_clip_ids.append(cur.fetchone()[0])
            logger.info(f"Created new split clip records: {new_clip_ids}")

            # Archive original clip
            cur.execute(
                """UPDATE clips SET ingest_state = 'archived', last_error = 'Split into clips ' || %s::text, updated_at = NOW() WHERE id = %s;""",
                (','.join(map(str, new_clip_ids)), clip_id)
            )
            logger.info(f"Archived original clip {clip_id}")

        # === Transaction End (Commit) ===
        logger.info(f"Split successful. New clip IDs: {new_clip_ids}")
        return {"status": "success", "new_clip_ids": new_clip_ids}

    except Exception as e:
        logger.error(f"TASK FAILED [Split]: clip {clip_id} - {e}", exc_info=True)
        if conn:
             try:
                 conn.rollback()
                 conn.autocommit = True
                 with conn.cursor() as err_cur:
                     err_cur.execute(
                         """UPDATE clips SET ingest_state = 'split_failed', last_error = %s, updated_at = NOW()
                            WHERE id = %s AND ingest_state = 'pending_split';""",
                         (f"{type(e).__name__}: {str(e)[:500]}", clip_id)
                     )
                 logger.info(f"Set original clip {clip_id} state to 'split_failed'.")
             except Exception as db_err:
                 logger.error(f"Failed to update error state in DB after split failure: {db_err}")
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
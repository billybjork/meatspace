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
    def get_db_connection(): raise NotImplementedError("Dummy DB connection")

from .splice import (
    s3_client, S3_BUCKET_NAME, FFMPEG_PATH, CLIP_S3_PREFIX,
    run_ffmpeg_command, sanitize_filename
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
        if base_identifier.endswith("_merged"): new_clip_identifier = base_identifier
        else: new_clip_identifier = f"{base_identifier}_merged"

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
                cur.execute(
                    """
                    INSERT INTO clips (source_video_id, clip_filepath, clip_identifier, start_frame, end_frame, start_time_seconds, end_time_seconds, ingest_state, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, 'pending_review', NOW(), NOW()) RETURNING id;
                    """, (source_video_id, new_clip_s3_key, new_clip_identifier, new_start_frame, new_end_frame, new_start_time, new_end_time)
                )
                new_clip_id = cur.fetchone()[0]
                logger.info(f"Created new merged clip record with ID: {new_clip_id}")

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
            conn.commit()
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
                with conn.cursor() as err_cur:
                    err_cur.execute(
                        """
                        UPDATE clips SET ingest_state =
                            CASE
                                WHEN id = %s AND ingest_state = 'pending_merge_with_next' THEN 'pending_review'
                                WHEN id = %s AND ingest_state IN ('pending_review', 'review_skipped') THEN ingest_state
                                ELSE 'merge_failed'
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
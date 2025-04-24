import cv2
import os
import re
import shutil
import tempfile
from pathlib import Path
from datetime import datetime

from prefect import task, get_run_logger
import psycopg2
from psycopg2 import sql
from psycopg2.extras import Json, execute_values
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
import sys

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

if project_root not in sys.path:
    sys.path.insert(0, project_root)
try:
    # Ensure db_utils provides connection pool management for psycopg2
    from utils.db_utils import get_db_connection, release_db_connection, initialize_db_pool
except ImportError as e:
    print(f"Error importing DB utils in keyframe.py: {e}")
    def get_db_connection(): raise NotImplementedError("DB Utils not loaded")
    def release_db_connection(conn): pass
    def initialize_db_pool(): pass # Add dummy if needed

# --- S3 Configuration ---
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
AWS_REGION = os.getenv("AWS_REGION")

# --- Configuration ---
KEYFRAMES_S3_PREFIX = os.getenv("KEYFRAMES_S3_PREFIX", "clip_artifacts/keyframes/")
ARTIFACT_TYPE_KEYFRAME = "keyframe" # Constant for artifact type

# --- Initialize S3 Client ---
s3_client = None
if S3_BUCKET_NAME:
    try:
        s3_client = boto3.client('s3', region_name=AWS_REGION)
        print(f"Keyframe Task: Successfully initialized S3 client for bucket: {S3_BUCKET_NAME}")
    except NoCredentialsError:
         print("Keyframe Task: ERROR initializing S3 client - AWS credentials not found.")
         s3_client = None
    except Exception as e:
        print(f"Keyframe Task: ERROR initializing S3 client: {e}")
        s3_client = None
else:
     print("Keyframe Task: WARNING - S3_BUCKET_NAME not set. S3 operations will fail.")


# --- Core Frame Extraction Logic ---
def _extract_and_save_frames_internal(
    local_temp_video_path: str,
    clip_identifier: str,
    title: str,
    local_temp_output_dir: str,
    strategy: str = 'midpoint'
    ) -> list[dict]:
    """
    Extracts frames based on strategy, saves them locally, and returns metadata.

    Returns:
        List of dictionaries, each containing:
        {
            'tag': str, 'local_path': str, 'frame_index': int,
            'timestamp_sec': float, 'width': int, 'height': int
        }
        Returns an empty list if extraction fails or video is invalid.
    """
    logger = get_run_logger()
    extracted_frames_data = []
    cap = None
    total_frames = 0
    try:
        logger.debug(f"Opening temp video file for frame extraction: {local_temp_video_path}")
        cap = cv2.VideoCapture(str(local_temp_video_path))
        if not cap.isOpened():
            if os.path.exists(local_temp_video_path):
                 raise IOError(f"Error opening video file (exists but cannot be opened by OpenCV): {local_temp_video_path}")
            else:
                 raise IOError(f"Error opening video file (does not exist): {local_temp_video_path}")

        total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        fps = cap.get(cv2.CAP_PROP_FPS)

        if fps is None or fps <= 0:
             logger.warning(f"Invalid FPS ({fps}) for {local_temp_video_path}. Using default 30.0 for timestamp calculations.")
             fps = 30.0

        logger.debug(f"Temp Video properties: Total Frames={total_frames}, FPS={fps:.2f}")

        if total_frames <= 0:
            logger.warning(f"Video file has zero frames or metadata error: {local_temp_video_path}")
            return extracted_frames_data

        frame_indices_to_extract = []
        frame_tags = []
        if strategy == 'multi':
            idx_25 = max(0, min(total_frames - 1, round(total_frames * 0.25)))
            idx_50 = max(0, min(total_frames - 1, round(total_frames * 0.50)))
            idx_75 = max(0, min(total_frames - 1, round(total_frames * 0.75)))
            indices = sorted(list(set([idx_25, idx_50, idx_75])))
            frame_indices_to_extract.extend(indices)
            frame_ids_map = {}
            if idx_25 in indices: frame_ids_map[idx_25] = "25pct"
            if idx_50 in indices: frame_ids_map[idx_50] = "50pct"
            if idx_75 in indices: frame_ids_map[idx_75] = "75pct"
            frame_tags = [frame_ids_map[idx] for idx in indices]
            if len(frame_tags) == 1 and "50pct" not in frame_tags:
                 mid_idx = max(0, min(total_frames - 1, round(total_frames * 0.50)))
                 frame_indices_to_extract = [mid_idx]
                 frame_tags = ["50pct"]
                 logger.debug("Strategy 'multi' resulted in single distinct frame index, defaulting to '50pct' tag.")
        elif strategy == 'midpoint':
            mid_idx = max(0, min(total_frames - 1, round(total_frames * 0.50)))
            frame_indices_to_extract.append(mid_idx)
            frame_tags.append("mid")
        else:
             raise ValueError(f"Unsupported keyframe strategy: {strategy}")

        # Generate sanitized base filename part
        scene_part = None
        match = re.search(r'_(clip|scene)_(\d+)$', clip_identifier)
        if match:
            scene_part = f"{match.group(1)}_{match.group(2)}"
        else:
            parts = clip_identifier.split('_')
            meaningful_part = next((part for part in reversed(parts) if any(char.isdigit() for char in part)), None)
            if meaningful_part:
                 scene_part = meaningful_part
            else:
                 sanitized_clip_id = re.sub(r'[^\w\-]+', '_', clip_identifier).strip('_')
                 scene_part = sanitized_clip_id[-50:]
                 logger.warning(f"Could not parse standard suffix from '{clip_identifier}'. Using: '{scene_part}'")

        # Clean title
        sanitized_title = re.sub(r'[^\w\-\.]+', '_', title).strip('_')
        sanitized_title = re.sub(r'_+', '_', sanitized_title)
        if not sanitized_title: sanitized_title = "untitled"
        sanitized_title = sanitized_title[:100]

        frame_tag_map = dict(zip(frame_indices_to_extract, frame_tags))

        for frame_index in frame_indices_to_extract:
            cap.set(cv2.CAP_PROP_POS_FRAMES, float(frame_index))
            ret, frame = cap.read()
            if not ret:
                logger.warning(f"Initial read failed for frame index {frame_index}, retrying read...")
                ret, frame = cap.read()

            if ret:
                frame_tag = frame_tag_map[frame_index]
                height, width = frame.shape[:2]
                timestamp_sec = frame_index / fps if fps > 0 else 0.0

                output_filename_base = f"{sanitized_title}_{scene_part}_frame_{frame_tag}.jpg"
                output_path_temp_abs = os.path.join(local_temp_output_dir, output_filename_base)

                logger.debug(f"Saving frame {frame_index} (tag: {frame_tag}, time: {timestamp_sec:.2f}s) to TEMP path {output_path_temp_abs}")
                success = cv2.imwrite(output_path_temp_abs, frame, [cv2.IMWRITE_JPEG_QUALITY, 90])

                if not success:
                    logger.error(f"Failed to write frame image to TEMP path {output_path_temp_abs}")
                else:
                    extracted_frames_data.append({
                        'tag': frame_tag,
                        'local_path': output_path_temp_abs,
                        'frame_index': frame_index,
                        'timestamp_sec': round(timestamp_sec, 3),
                        'width': width,
                        'height': height
                    })
            else:
                logger.warning(f"Could not read frame at index {frame_index} (or after retry) from {local_temp_video_path}.")

    except IOError as e:
        logger.error(f"IOError processing video file {local_temp_video_path}: {e}", exc_info=True)
        extracted_frames_data = []
        raise
    except cv2.error as cv_err:
        logger.error(f"OpenCV error processing video file {local_temp_video_path}: {cv_err}", exc_info=True)
        extracted_frames_data = []
        raise RuntimeError(f"OpenCV error during frame extraction for {local_temp_video_path}") from cv_err
    except Exception as e:
        logger.error(f"An unexpected error occurred processing {local_temp_video_path}: {e}", exc_info=True)
        extracted_frames_data = []
        raise
    finally:
        if cap and cap.isOpened():
            cap.release()
            logger.debug(f"Released video capture for {local_temp_video_path}")

    if not extracted_frames_data and total_frames > 0:
        logger.error(f"CRITICAL: No frames were successfully extracted or saved for {local_temp_video_path} despite having {total_frames} frames reported.")
    elif not extracted_frames_data and total_frames <= 0:
         logger.info(f"No frames extracted as video reported {total_frames} frames.")

    return extracted_frames_data


# --- Prefect Task ---
@task(name="Extract Clip Keyframes", retries=1, retry_delay_seconds=45)
def extract_keyframes_task(
    clip_id: int,
    strategy: str = 'midpoint',
    overwrite: bool = False
    ):
    """
    Extracts keyframes, uploads them to S3, and records them in the database.
    Updates clip state and `keyframed_at` timestamp.
    """
    logger = get_run_logger()
    logger.info(f"TASK [Keyframe]: Starting for clip_id: {clip_id}, Strategy: '{strategy}', Overwrite: {overwrite}")

    if not s3_client:
        logger.error("S3 client is not available.")
        raise RuntimeError("S3 client is not initialized or configured.")

    conn = None
    temp_dir_obj = None
    temp_dir = None
    final_status = "failed_init"
    error_message_for_db = None
    generated_artifact_s3_keys = []
    task_exception = None
    needs_processing = False
    processed_artifact_data_for_db = [] # Holds tuples for execute_values DB insert
    clip_s3_key = None
    clip_identifier = None
    title = None

    # Explicitly initialize DB pool if needed (safer in task context)
    try:
        initialize_db_pool()
    except Exception as pool_err:
        logger.error(f"Failed to initialize DB pool at task start: {pool_err}")
        raise RuntimeError("DB pool initialization failed") from pool_err

    try:
        # === Phase 1: Initial DB Check and State Update ===
        conn = get_db_connection()
        conn.autocommit = False

        try:
            with conn.cursor() as cur:
                cur.execute("SELECT pg_advisory_xact_lock(2, %s)", (clip_id,))
                logger.info(f"Acquired DB lock for clip_id: {clip_id} (Phase 1 Check)")

                cur.execute(
                    """
                    SELECT c.clip_filepath, c.clip_identifier, c.ingest_state, sv.title
                    FROM clips c JOIN source_videos sv ON c.source_video_id = sv.id
                    WHERE c.id = %s;
                    """, (clip_id,)
                )
                result = cur.fetchone()
                if not result: raise ValueError(f"Clip ID {clip_id} not found.")
                clip_s3_key, clip_identifier, current_state, source_title = result
                logger.info(f"Found clip: ID={clip_id}, Identifier='{clip_identifier}', State='{current_state}', Source Title='{source_title}', Clip S3 Key='{clip_s3_key}'")

                if not clip_s3_key: raise ValueError(f"Clip ID {clip_id} has NULL clip_filepath.")
                # Sanitize title here for consistency
                if not source_title:
                    title = f"source_{clip_identifier.split('_')[0]}"
                    logger.warning(f"Source title NULL/empty, using fallback: '{title}'")
                else:
                    title = re.sub(r'[^\w\-\.]+', '_', source_title).strip('_')
                    title = re.sub(r'_+', '_', title)
                    if not title: title = "untitled_clip"

                artifacts_exist = False
                if not overwrite:
                    cur.execute(
                        "SELECT EXISTS (SELECT 1 FROM clip_artifacts WHERE clip_id = %s AND artifact_type = %s AND strategy = %s);",
                        (clip_id, ARTIFACT_TYPE_KEYFRAME, strategy)
                    )
                    artifacts_exist = cur.fetchone()[0]
                    logger.debug(f"Checked for existing artifacts (type='{ARTIFACT_TYPE_KEYFRAME}', strategy='{strategy}'): {artifacts_exist}")

                allow_processing_state = current_state in ['review_approved', 'keyframing_failed', 'processing_post_review'] # Allow start from intermediate state too

                if artifacts_exist and not overwrite:
                     logger.info(f"Skipping clip {clip_id}: Artifacts exist and overwrite=False.")
                     final_status = "skipped_exists"
                     needs_processing = False
                elif not allow_processing_state and not overwrite:
                     logger.warning(f"Skipping clip {clip_id}: State '{current_state}' not eligible and overwrite=False.")
                     final_status = "skipped_state"
                     needs_processing = False
                elif overwrite and current_state not in ['keyframing']:
                     needs_processing = True
                     logger.info(f"Proceeding (Overwrite=True). Current state: {current_state}")
                elif allow_processing_state:
                     needs_processing = True
                     logger.info(f"Proceeding. Current state: {current_state}")
                else:
                    logger.warning(f"Skipping clip {clip_id}. Logic state: exists={artifacts_exist}, overwrite={overwrite}, state='{current_state}'.")
                    final_status = "skipped_logic"
                    needs_processing = False

                if needs_processing:
                    cur.execute(
                        """UPDATE clips SET ingest_state = 'keyframing', updated_at = NOW(), last_error = NULL WHERE id = %s;""", (clip_id,)
                    )
                    if cur.rowcount == 0: raise RuntimeError(f"Concurrency Error: Failed to set clip {clip_id} state to 'keyframing'.")
                    logger.info(f"Set clip {clip_id} state to 'keyframing'")

            conn.commit()
            logger.debug("Phase 1 DB transaction committed.")

        except (ValueError, psycopg2.DatabaseError, RuntimeError) as db_err:
             logger.error(f"DB Error during Phase 1 for clip {clip_id}: {db_err}", exc_info=True)
             if conn: conn.rollback()
             error_message_for_db = f"DB Phase 1 Error: {str(db_err)[:500]}"
             task_exception = db_err
             final_status = "failed_db_phase1"
             raise # Re-raise to main handler

        # === Exit Early if Skipped ===
        if not needs_processing:
            logger.info(f"No processing required for clip {clip_id}. Final status: {final_status}")
            return {"status": final_status, "clip_id": clip_id, "strategy": strategy, "generated_artifacts_count": 0, "artifact_s3_keys": []}

        # === Phase 2: Processing/Upload ===
        if not s3_client: raise RuntimeError("S3 client unavailable.")

        try:
            temp_dir_obj = tempfile.TemporaryDirectory(prefix=f"meatspace_keyframe_{clip_id}_")
            temp_dir = Path(temp_dir_obj.name)
            logger.info(f"Using temporary directory: {temp_dir}")

            local_temp_clip_path = temp_dir / Path(clip_s3_key).name
            logger.info(f"Downloading s3://{S3_BUCKET_NAME}/{clip_s3_key} to {local_temp_clip_path}...")
            s3_client.download_file(S3_BUCKET_NAME, clip_s3_key, str(local_temp_clip_path))
            logger.info(f"Successfully downloaded clip video for {clip_id}.")

            extracted_frames_metadata = _extract_and_save_frames_internal(
                str(local_temp_clip_path), clip_identifier, title, str(temp_dir), strategy
            )
            if not extracted_frames_metadata: raise RuntimeError(f"Frame extraction yielded no results for clip {clip_id}.")
            logger.info(f"Extracted {len(extracted_frames_metadata)} frame(s) locally for clip {clip_id}.")

            processed_artifact_data_for_db = []
            generated_artifact_s3_keys = []
            representative_tag = '50pct' if strategy == 'multi' else 'mid'
            upload_datetime = datetime.now() # Use a consistent timestamp for batch DB insert

            for frame_data in extracted_frames_metadata:
                temp_local_path = Path(frame_data['local_path'])
                frame_tag = frame_data['tag']
                s3_prefix = f"{KEYFRAMES_S3_PREFIX.strip('/')}/{title}/{strategy}/".replace("//", "/")
                frame_s3_key = f"{s3_prefix}{temp_local_path.name}"

                try:
                    logger.debug(f"Uploading {temp_local_path.name} to s3://{S3_BUCKET_NAME}/{frame_s3_key}")
                    with open(temp_local_path, "rb") as f:
                        s3_client.upload_fileobj(f, S3_BUCKET_NAME, frame_s3_key)

                    is_representative = (frame_tag == representative_tag)
                    db_metadata = {
                        "frame_number": frame_data['frame_index'],
                        "timestamp_sec": frame_data['timestamp_sec'],
                        "width": frame_data['width'],
                        "height": frame_data['height'],
                        "is_representative": is_representative
                    }

                    # Prepare tuple for DB insertion - includes placeholder for created_at/updated_at
                    processed_artifact_data_for_db.append((
                        clip_id, ARTIFACT_TYPE_KEYFRAME, strategy, frame_tag,
                        frame_s3_key, Json(db_metadata)
                    ))
                    generated_artifact_s3_keys.append(frame_s3_key)

                except ClientError as s3_upload_err:
                    raise RuntimeError(f"S3 upload failed for {frame_s3_key}") from s3_upload_err
                except Exception as upload_err:
                    logger.error(f"Unexpected error uploading {temp_local_path.name}: {upload_err}", exc_info=True)
                    raise

            if len(generated_artifact_s3_keys) != len(extracted_frames_metadata):
                 raise RuntimeError("Not all extracted frames were successfully uploaded.")
            if not processed_artifact_data_for_db:
                raise RuntimeError(f"Failed to prepare any keyframe artifact data for DB.")

            logger.info(f"Successfully uploaded {len(generated_artifact_s3_keys)} keyframe artifact(s) to S3.")

        except (IOError, cv2.error, RuntimeError, ClientError, ValueError) as phase2_err:
             logger.error(f"Error during Phase 2 for clip {clip_id}: {phase2_err}", exc_info=True)
             error_message_for_db = f"Proc/Upload Error: {str(phase2_err)[:500]}"
             task_exception = phase2_err
             final_status = "failed_processing"
             raise # Re-raise to main handler

        # === Phase 3: Final DB Update ===
        try:
            with conn.cursor() as cur:
                 cur.execute("SELECT pg_advisory_xact_lock(2, %s)", (clip_id,))
                 logger.info(f"Acquired DB lock for clip_id: {clip_id} (Phase 3 Update)")

                 if overwrite:
                     logger.info(f"Overwrite=True: Deleting existing artifacts...")
                     delete_sql = sql.SQL("DELETE FROM clip_artifacts WHERE clip_id = %s AND artifact_type = %s AND strategy = %s;")
                     cur.execute(delete_sql, (clip_id, ARTIFACT_TYPE_KEYFRAME, strategy))
                     logger.info(f"Deletion completed (affected {cur.rowcount} rows).")

                 insert_query = sql.SQL("""
                     INSERT INTO clip_artifacts (
                         clip_id, artifact_type, strategy, tag, s3_key, metadata, created_at, updated_at
                     ) VALUES %s
                     ON CONFLICT (clip_id, artifact_type, strategy, tag) DO UPDATE SET
                         s3_key = EXCLUDED.s3_key,
                         metadata = EXCLUDED.metadata,
                         updated_at = NOW();
                 """)

                 now_ts = datetime.now() # Use single timestamp for the batch
                 values_to_insert = [
                     (
                         *data_tuple, # Original tuple: clip_id -> metadata
                         now_ts,      # Value for created_at
                         now_ts       # Value for updated_at
                     )
                     for data_tuple in processed_artifact_data_for_db
                 ]

                 execute_values(cur, insert_query, values_to_insert)
                 logger.info(f"Inserted/updated {len(values_to_insert)} artifact records.")

                 update_clip_sql = sql.SQL("""
                     UPDATE clips SET
                         ingest_state = 'keyframed', keyframed_at = NOW(),
                         last_error = NULL, updated_at = NOW()
                     WHERE id = %s AND ingest_state = 'keyframing';
                 """)
                 cur.execute(update_clip_sql, (clip_id,))

                 if cur.rowcount == 1:
                      logger.info(f"Successfully updated clip {clip_id} state to 'keyframed'.")
                      final_status = "success"
                 else:
                      logger.error(f"Failed to update clip {clip_id} final state to 'keyframed'. Rowcount: {cur.rowcount}.")
                      conn.rollback()
                      final_status = "failed_state_update"
                      error_message_for_db = "Failed final state update to 'keyframed'"
                      raise RuntimeError(f"Failed to update clip {clip_id} state to 'keyframed' in final DB step.")

            conn.commit()
            logger.info("Phase 3 DB updates committed successfully.")

        except (psycopg2.DatabaseError, RuntimeError) as db_err:
             logger.error(f"DB Error during Phase 3 for clip {clip_id}: {db_err}", exc_info=True)
             if conn: conn.rollback()
             error_message_for_db = f"DB Phase 3 Error: {str(db_err)[:500]}"
             task_exception = db_err
             final_status = "failed_db_update"
             raise # Re-raise to main handler

    except (ValueError, IOError, FileNotFoundError, RuntimeError, ClientError, psycopg2.DatabaseError) as e:
        # --- Main Error Handling ---
        logger.error(f"TASK FAILED [Keyframe]: clip_id {clip_id} - Error: {e}", exc_info=True)
        # Keep specific failure status if already set
        final_status = final_status if final_status not in ["failed_init", "failed"] else "failed_processing"
        error_message_for_db = error_message_for_db or f"{type(e).__name__}: {str(e)[:500]}"
        task_exception = e

        if conn and needs_processing: # Check if conn exists and we intended to process
            try:
                logger.warning(f"Attempting rollback and state update to 'keyframing_failed' for clip {clip_id}.")
                conn.rollback()
                conn.autocommit = True # Use autocommit for fail state update
                with conn.cursor() as err_cur:
                    fail_update_sql = sql.SQL("""
                        UPDATE clips SET
                            ingest_state = 'keyframing_failed', last_error = %s,
                            retry_count = COALESCE(retry_count, 0) + 1, updated_at = NOW()
                        WHERE id = %s AND (ingest_state = 'keyframing' OR ingest_state = 'processing_post_review');
                        """) # Allow update from intermediate state too
                    err_cur.execute(fail_update_sql, (error_message_for_db, clip_id))
                logger.info(f"Attempted state update to 'keyframing_failed' for clip {clip_id}.")
            except Exception as db_fail_err:
                logger.error(f"CRITICAL: Failed to update error state in DB for clip {clip_id}: {db_fail_err}")
        elif not conn:
             logger.error("DB connection unavailable for error state update.")

    except Exception as e:
         # Catch any truly unexpected errors
         logger.error(f"UNEXPECTED TASK FAILED [Keyframe]: clip_id {clip_id} - Error: {e}", exc_info=True)
         final_status = "failed_unexpected"
         error_message_for_db = f"Unexpected:{type(e).__name__}: {str(e)[:480]}"
         task_exception = e
         if conn and needs_processing:
             try:
                conn.rollback(); conn.autocommit = True
                with conn.cursor() as err_cur:
                    err_cur.execute(sql.SQL("""UPDATE clips SET ingest_state = 'keyframing_failed', last_error = %s, retry_count = COALESCE(retry_count, 0) + 1, updated_at = NOW() WHERE id = %s AND (ingest_state = 'keyframing' OR ingest_state = 'processing_post_review');"""), (error_message_for_db, clip_id))
                logger.info(f"Attempted state update to 'keyframing_failed' after unexpected error for clip {clip_id}.")
             except Exception as db_fail_err:
                logger.error(f"CRITICAL: Failed to update error state in DB for clip {clip_id}: {db_fail_err}")

    finally:
        # --- Cleanup ---
        if conn:
            conn.autocommit = True
            release_db_connection(conn)
            logger.debug(f"DB connection released for clip_id: {clip_id}")
        if temp_dir_obj:
            try:
                shutil.rmtree(temp_dir, ignore_errors=True)
                logger.info(f"Cleaned up temporary directory: {temp_dir}")
            except Exception as cleanup_err:
                 logger.warning(f"Error cleaning up temp dir {temp_dir}: {cleanup_err}")

    # === Return Result or Raise Exception ===
    result_payload = {
        "status": final_status,
        "clip_id": clip_id,
        "strategy": strategy,
        "generated_artifacts_count": len(generated_artifact_s3_keys),
        "artifact_s3_keys": generated_artifact_s3_keys,
    }

    if final_status == "success":
        logger.info(f"TASK [Keyframe] finished successfully for clip_id {clip_id}.")
        return result_payload
    elif final_status.startswith("skipped"):
         logger.info(f"TASK [Keyframe] skipped for clip_id {clip_id}. Status: {final_status}")
         return result_payload
    else:
        logger.error(f"TASK [Keyframe] failed for clip_id {clip_id}. Status: {final_status}. Error: {error_message_for_db}.")
        if task_exception:
            raise task_exception
        else:
            raise RuntimeError(f"Keyframe task failed for clip {clip_id} with status {final_status}. Reason: {error_message_for_db or 'Unknown error'}")
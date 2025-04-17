import cv2
import os
import re
import shutil
import tempfile
import traceback
from pathlib import Path
from prefect import task, get_run_logger
import psycopg2
from psycopg2 import sql
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
import sys

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

if project_root not in sys.path:
    sys.path.insert(0, project_root)
try:
    from utils.db_utils import get_db_connection
except ImportError as e:
    print(f"Error importing DB utils in keyframe.py: {e}")
    def get_db_connection(): raise NotImplementedError("Dummy DB connection")

# --- S3 Configuration ---
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
AWS_REGION = os.getenv("AWS_REGION")

if not S3_BUCKET_NAME:
    raise ValueError("S3_BUCKET_NAME environment variable not set.")

s3_client = None
try:
    s3_client = boto3.client('s3', region_name=AWS_REGION)
    print(f"Keyframe Task: Successfully initialized S3 client for bucket: {S3_BUCKET_NAME}")
except NoCredentialsError:
     print("Keyframe Task: ERROR initializing S3 client - AWS credentials not found.")
except Exception as e:
    print(f"Keyframe Task: ERROR initializing S3 client: {e}")

# --- Configuration ---
KEYFRAMES_S3_PREFIX = os.getenv("KEYFRAMES_S3_PREFIX", "keyframes/")

# --- Core Frame Extraction Logic ---
def _extract_and_save_frames_internal(
    local_temp_video_path: str,
    clip_identifier: str,
    title: str,
    local_temp_output_dir: str,
    strategy: str = 'midpoint'
    ) -> dict:
    logger = get_run_logger()
    saved_frame_local_paths = {}
    cap = None
    try:
        logger.debug(f"Opening temp video file for frame extraction: {local_temp_video_path}")
        cap = cv2.VideoCapture(local_temp_video_path)
        if not cap.isOpened():
            raise IOError(f"Error opening temp video file: {local_temp_video_path}")

        total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        fps = cap.get(cv2.CAP_PROP_FPS)
        logger.debug(f"Temp Video properties: Total Frames={total_frames}, FPS={fps:.2f}")

        if total_frames <= 0:
            logger.warning(f"Temp video file has no frames or is invalid: {local_temp_video_path}")
            return saved_frame_local_paths

        frame_indices_to_extract = []
        frame_ids = []
        if strategy == 'multi':
            idx_25 = max(0, total_frames // 4); idx_50 = max(0, total_frames // 2); idx_75 = max(0, (total_frames * 3) // 4)
            indices = sorted(list(set([idx_25, idx_50, idx_75])))
            frame_indices_to_extract.extend(indices)
            frame_ids_map = {idx_25: "25pct", idx_50: "50pct", idx_75: "75pct"}
            frame_ids = [frame_ids_map[idx] for idx in indices]
            if len(frame_ids) == 1 and frame_ids[0] != "50pct": frame_ids = ["50pct"]
        else:
            frame_indices_to_extract.append(max(0, total_frames // 2)); frame_ids.append("mid")

        scene_part = None
        match = re.search(r'_(clip|scene)_(\d+)$', clip_identifier)
        if match: scene_part = f"{match.group(1)}_{match.group(2)}"
        else:
            parts = clip_identifier.split('_')
            if len(parts) > 1 and any(char.isdigit() for char in parts[-1]): scene_part = parts[-1]
            else:
                 sanitized_clip_id = re.sub(r'[^\w\-]+', '_', clip_identifier).strip('_'); scene_part = sanitized_clip_id[-50:]
                 logger.warning(f"Could not parse standard suffix from clip_identifier '{clip_identifier}'. Using sanitized end part: '{scene_part}'")

        frame_tags_map = dict(zip(frame_indices_to_extract, frame_ids))
        for frame_index in frame_indices_to_extract:
            cap.set(cv2.CAP_PROP_POS_FRAMES, frame_index)
            ret, frame = cap.read()
            if ret:
                frame_tag = frame_tags_map[frame_index]
                output_filename_base = f"{title}_{scene_part}_frame_{frame_tag}.jpg"
                output_filename_base = re.sub(r'[^\w\.\-]+', '_', output_filename_base)
                output_path_temp_abs = os.path.join(local_temp_output_dir, output_filename_base)
                logger.debug(f"Saving frame {frame_index} (tag: {frame_tag}) to TEMP path {output_path_temp_abs}")
                success = cv2.imwrite(output_path_temp_abs, frame)
                if not success: logger.error(f"Failed to write frame image to TEMP path {output_path_temp_abs}")
                else: saved_frame_local_paths[frame_tag] = output_path_temp_abs
            else: logger.warning(f"Could not read frame {frame_index} from {local_temp_video_path}. It might be at the very end.")
    except IOError as e: logger.error(f"IOError processing temp video {local_temp_video_path}: {e}"); raise
    except cv2.error as cv_err: logger.error(f"OpenCV error processing temp video {local_temp_video_path}: {cv_err}"); raise RuntimeError(f"OpenCV error during frame extraction") from cv_err
    except Exception as e: logger.error(f"An unexpected error occurred processing {local_temp_video_path}: {e}", exc_info=True); raise
    finally:
        if cap and cap.isOpened(): cap.release(); logger.debug(f"Released video capture for {local_temp_video_path}")
    if not saved_frame_local_paths and total_frames > 0: logger.warning(f"No frames were successfully saved for {local_temp_video_path} despite processing.")
    return saved_frame_local_paths

# --- Prefect Task ---
@task(name="Extract Clip Keyframes", retries=1, retry_delay_seconds=30)
def extract_keyframes_task(
    clip_id: int,
    strategy: str = 'midpoint',
    overwrite: bool = False
    ):
    logger = get_run_logger()
    logger.info(f"TASK [Keyframe]: Starting for clip_id: {clip_id}, Strategy: '{strategy}', Overwrite: {overwrite}")

    if not s3_client:
        logger.error("S3 client not initialized. Cannot proceed.")
        raise RuntimeError("S3 client failed to initialize.")

    conn = None
    temp_dir_obj = None
    representative_s3_key = None
    saved_s3_keys = {}
    final_status = "failed"
    needs_processing = False # Flag to track if actual processing will occur

    try:
        temp_dir_obj = tempfile.TemporaryDirectory(prefix=f"meatspace_keyframe_{clip_id}_")
        temp_dir = Path(temp_dir_obj.name)
        logger.info(f"Using temporary directory: {temp_dir}")

        conn = get_db_connection()
        conn.autocommit = False # Start manual transaction management

        # === Initial DB Check and State Update Block ===
        try:
            with conn.cursor() as cur:
                # 1. Acquire Advisory Lock
                cur.execute("SELECT pg_advisory_xact_lock(2, %s)", (clip_id,))
                logger.info(f"Acquired DB lock for clip_id: {clip_id}")

                # 2. Fetch clip details & source identifier
                cur.execute(
                    """
                    SELECT c.clip_filepath, c.clip_identifier, c.keyframe_filepath, c.ingest_state, sv.title
                    FROM clips c JOIN source_videos sv ON c.source_video_id = sv.id WHERE c.id = %s;
                    """, (clip_id,)
                )
                result = cur.fetchone()
                if not result: raise ValueError(f"Clip ID {clip_id} not found.")
                clip_s3_key, clip_identifier, current_keyframe_s3_key, current_state, title = result
                logger.info(f"Found clip: ID={clip_id}, Identifier='{clip_identifier}', State='{current_state}', Source='{title}', Clip S3 Key='{clip_s3_key}'")

                # 3. Check if processing should proceed
                allow_processing_state = current_state == 'review_approved' or \
                                         (overwrite and current_state in ['keyframing_failed', 'keyframed', 'embedding_failed', 'embedded']) or \
                                         (not current_keyframe_s3_key and current_state not in ['keyframing_failed', 'keyframed', 'embedding_failed', 'embedded'])

                if not allow_processing_state:
                    # Check S3 path match only if state doesn't allow processing outright
                    expected_s3_prefix_part = f"{KEYFRAMES_S3_PREFIX.strip('/')}/{title}/{strategy}/"
                    path_matches_strategy = current_keyframe_s3_key and current_keyframe_s3_key.startswith(expected_s3_prefix_part)

                    if path_matches_strategy and not overwrite:
                        logger.info(f"Skipping clip {clip_id}: Keyframe S3 key already exists with matching strategy and overwrite=False.")
                        final_status = "skipped"
                    elif current_state not in ['review_approved', 'keyframing_failed'] and not overwrite:
                        logger.warning(f"Skipping clip {clip_id}: State '{current_state}' not suitable and overwrite=False.")
                        final_status = "skipped"
                    else: # Should proceed due to overwrite or missing path despite state
                        needs_processing = True
                        logger.info(f"Proceeding with clip {clip_id}: State='{current_state}', Overwrite={overwrite}, S3PathMatch={path_matches_strategy}")
                else:
                     needs_processing = True # Proceed based on state/overwrite/missing path
                     logger.info(f"Proceeding with clip {clip_id} based on state/overwrite/missing keyframe.")

                # 4. Update state to 'keyframing' only if processing
                if needs_processing:
                    if not clip_s3_key: raise ValueError(f"Clip ID {clip_id} has NULL clip_filepath (S3 key).")
                    if not title:
                         fallback_source_id = clip_identifier.split('_clip_')[0].split('_scene_')[0]; title = re.sub(r'[^\w\-]+', '_', fallback_source_id).strip('_')
                         if not title: raise ValueError(f"Could not determine a valid source identifier for clip ID {clip_id}.")
                         logger.warning(f"Source identifier was NULL for clip {clip_id}, using fallback: '{title}'")

                    cur.execute(
                        "UPDATE clips SET ingest_state = 'keyframing', updated_at = NOW(), last_error = NULL WHERE id = %s",
                        (clip_id,)
                    )
                    logger.info(f"Set clip {clip_id} state to 'keyframing'")
            # ---- End Cursor Context ----

            # Commit the initial check/state update
            conn.commit()
            logger.debug("Initial check/state update transaction committed.")

        except (ValueError, psycopg2.DatabaseError) as db_err:
             logger.error(f"DB Error during initial check/update for clip {clip_id}: {db_err}", exc_info=True)
             if conn: conn.rollback() # Rollback this specific block on error
             raise # Re-raise to trigger main error handling

        # === Exit if Skipped ===
        if final_status == "skipped":
            logger.info(f"Skipped processing for clip {clip_id}.")
            # No further DB action needed as commit already happened
            return {"status": final_status, "reason": "Skipped based on state/overwrite/existing path", "clip_id": clip_id}

        # === S3 Download and Processing (Outside DB Transaction) ===
        # 5. Download Clip from S3
        local_temp_clip_path = temp_dir / Path(clip_s3_key).name
        logger.info(f"Downloading s3://{S3_BUCKET_NAME}/{clip_s3_key} to {local_temp_clip_path}...")
        try:
            s3_client.download_file(S3_BUCKET_NAME, clip_s3_key, str(local_temp_clip_path))
        except ClientError as s3_err:
            raise RuntimeError("S3 download failed") from s3_err

        # 6. Perform Extraction
        saved_frame_local_paths = _extract_and_save_frames_internal(
            str(local_temp_clip_path), clip_identifier, title, str(temp_dir), strategy
        )
        if not saved_frame_local_paths: raise RuntimeError(f"Failed to extract any keyframes locally for clip {clip_id}")

        # 7. Upload Extracted Frames to S3
        for frame_tag, temp_local_path_str in saved_frame_local_paths.items():
            temp_local_path = Path(temp_local_path_str)
            frame_s3_key = f"{KEYFRAMES_S3_PREFIX.strip('/')}/{title}/{strategy}/{temp_local_path.name}".replace("\\", "/")
            try:
                with open(temp_local_path, "rb") as f: s3_client.upload_fileobj(f, S3_BUCKET_NAME, frame_s3_key)
                saved_s3_keys[frame_tag] = frame_s3_key
            except ClientError as s3_upload_err: logger.error(f"Failed to upload keyframe {temp_local_path.name} to S3: {s3_upload_err}")
            except Exception as upload_err: logger.error(f"Unexpected error uploading keyframe {temp_local_path.name}: {upload_err}", exc_info=True)
        if len(saved_s3_keys) != len(saved_frame_local_paths): logger.warning("Not all extracted frames were successfully uploaded to S3.")
        if not saved_s3_keys: raise RuntimeError(f"Failed to upload ANY keyframes to S3 for clip {clip_id}")

        # 8. Select Representative S3 Key
        repr_tag = '50pct' if strategy == 'multi' else 'mid'
        representative_s3_key = saved_s3_keys.get(repr_tag) or next(iter(saved_s3_keys.values()), None)
        if not representative_s3_key: raise ValueError("Failed to select a representative S3 key after upload")
        logger.info(f"Selected representative S3 key: {representative_s3_key}")

        # === Final DB Update Block ===
        try:
            with conn.cursor() as cur:
                 # Acquire lock again for final update
                 cur.execute("SELECT pg_advisory_xact_lock(2, %s)", (clip_id,))

                 # Update DB on Success
                 cur.execute(
                    """
                    UPDATE clips SET keyframe_filepath = %s, ingest_state = 'keyframed',
                        keyframed_at = NOW(), last_error = NULL, updated_at = NOW()
                    WHERE id = %s AND ingest_state = 'keyframing';
                    """, (representative_s3_key, clip_id)
                 )
                 if cur.rowcount == 0: logger.warning(f"DB update for final state 'keyframed' for clip {clip_id} affected 0 rows.")
                 else: logger.info(f"Successfully updated DB for clip {clip_id}. State set to 'keyframed'."); final_status = "success"
            # --- End Cursor Context ---
            conn.commit() # Commit final update
            logger.debug("Final update transaction committed.")

        except (ValueError, psycopg2.DatabaseError) as db_err:
             logger.error(f"DB Error during final update for clip {clip_id}: {db_err}", exc_info=True)
             if conn: conn.rollback()
             raise # Re-raise to trigger main error handling

    except (ValueError, IOError, FileNotFoundError, RuntimeError, ClientError, psycopg2.DatabaseError) as e:
        # --- Main Error Handling ---
        logger.error(f"TASK FAILED [Keyframe]: clip_id {clip_id} - {e}", exc_info=True)
        final_status = "failed"
        if conn:
            try:
                conn.rollback() # Rollback any potentially open transaction
                conn.autocommit = True # Enable autocommit for error state update
                with conn.cursor() as err_cur:
                    err_cur.execute(
                        """
                        UPDATE clips SET ingest_state = 'keyframing_failed', last_error = %s,
                            retry_count = COALESCE(retry_count, 0) + 1, updated_at = NOW()
                        WHERE id = %s AND ingest_state IN ('keyframing', 'review_approved');
                        """, (f"{type(e).__name__}: {str(e)[:500]}", clip_id)
                    )
                logger.info(f"Attempted to set clip {clip_id} state to 'keyframing_failed' after error.")
            except Exception as db_err:
                logger.error(f"Failed to update error state in DB for clip {clip_id} after task failure: {db_err}")
        raise e # Re-raise original exception
    except Exception as e:
         # Catch any other unexpected errors
         logger.error(f"UNEXPECTED TASK FAILED [Keyframe]: clip_id {clip_id} - {e}", exc_info=True)
         final_status = "failed"
         if conn:
             try:
                 conn.rollback()
                 conn.autocommit = True
                 with conn.cursor() as err_cur:
                    err_cur.execute(
                        """
                        UPDATE clips SET ingest_state = 'keyframing_failed', last_error = %s,
                            retry_count = COALESCE(retry_count, 0) + 1, updated_at = NOW()
                        WHERE id = %s AND ingest_state IN ('keyframing', 'review_approved');
                        """, (f"Unexpected:{type(e).__name__}: {str(e)[:480]}", clip_id)
                    )
                 logger.info(f"Attempted to set clip {clip_id} state to 'keyframing_failed' after unexpected error.")
             except Exception as db_err:
                 logger.error(f"Failed to update error state in DB for clip {clip_id} after unexpected task failure: {db_err}")
         raise
    finally:
        # --- Cleanup ---
        if conn:
            conn.autocommit = True # Ensure reset before closing
            conn.close()
            logger.debug(f"DB connection closed for clip_id: {clip_id}")
        if temp_dir_obj:
            try:
                shutil.rmtree(temp_dir, ignore_errors=True)
                logger.info(f"Cleaned up temporary directory: {temp_dir}")
            except Exception as cleanup_err:
                 logger.warning(f"Error cleaning up temporary directory {temp_dir}: {cleanup_err}")

    return {
        "status": final_status,
        "clip_id": clip_id,
        "representative_s3_key": representative_s3_key,
        "all_s3_keys": saved_s3_keys,
        "strategy": strategy
    }

# Local run block for testing purposes
if __name__ == "__main__":
    print("Running keyframe task locally for testing (requires S3/DB setup)...")
    pass
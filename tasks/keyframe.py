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
    from utils.db_utils import get_db_connection, release_db_connection
except ImportError as e:
    print(f"Error importing DB utils in keyframe.py: {e}")
    # Define dummy functions if import fails, allowing module to load but task will fail later if DB needed
    def get_db_connection(): raise NotImplementedError("DB Utils not loaded")
    def release_db_connection(conn): pass

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
         s3_client = None # Ensure it's None if init fails
    except Exception as e:
        print(f"Keyframe Task: ERROR initializing S3 client: {e}")
        s3_client = None # Ensure it's None if init fails
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
            'tag': str,               # e.g., 'mid', '25pct', '50pct', '75pct'
            'local_path': str,        # Absolute path to the saved frame image
            'frame_index': int,       # Frame number in the original clip
            'timestamp_sec': float,   # Timestamp in seconds
            'width': int,             # Frame width
            'height': int             # Frame height
        }
        Returns an empty list if extraction fails or video is invalid.
    """
    logger = get_run_logger()
    extracted_frames_data = []
    cap = None
    total_frames = 0 # Initialize total_frames
    try:
        logger.debug(f"Opening temp video file for frame extraction: {local_temp_video_path}")
        cap = cv2.VideoCapture(str(local_temp_video_path)) # Ensure path is string
        if not cap.isOpened():
            # Provide more context if file exists but cannot be opened
            if os.path.exists(local_temp_video_path):
                 raise IOError(f"Error opening video file (exists but cannot be opened by OpenCV): {local_temp_video_path}")
            else:
                 raise IOError(f"Error opening video file (does not exist): {local_temp_video_path}")

        total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        fps = cap.get(cv2.CAP_PROP_FPS)

        # Handle cases where fps might be 0 or invalid
        if fps is None or fps <= 0:
             logger.warning(f"Invalid FPS ({fps}) detected for {local_temp_video_path}. Cannot calculate timestamps accurately. Check video integrity.")
             fps = 30.0 # Assume a reasonable default
             # Alternatively, raise error instead of assuming default FPS:
             # raise ValueError(f"Invalid FPS ({fps}) detected for video {local_temp_video_path}")
             logger.warning(f"Using default FPS {fps} for timestamp calculations.")

        logger.debug(f"Temp Video properties: Total Frames={total_frames}, FPS={fps:.2f}")

        if total_frames <= 0:
            logger.warning(f"Video file has zero frames or metadata error: {local_temp_video_path}")
            return extracted_frames_data # Return empty list

        frame_indices_to_extract = []
        frame_tags = []
        # Determine frame indices and tags based on strategy
        if strategy == 'multi':
            # Ensure indices are within valid range [0, total_frames-1]
            idx_25 = max(0, min(total_frames - 1, round(total_frames * 0.25))) # Use round for possibly better distribution
            idx_50 = max(0, min(total_frames - 1, round(total_frames * 0.50)))
            idx_75 = max(0, min(total_frames - 1, round(total_frames * 0.75)))
            # Use set to handle cases where indices might be the same for very short clips
            indices = sorted(list(set([idx_25, idx_50, idx_75])))
            frame_indices_to_extract.extend(indices)
            # Map indices back to tags robustly
            frame_ids_map = {}
            if idx_25 in indices: frame_ids_map[idx_25] = "25pct"
            if idx_50 in indices: frame_ids_map[idx_50] = "50pct"
            if idx_75 in indices: frame_ids_map[idx_75] = "75pct"
            # Ensure tags are added in the order of indices
            frame_tags = [frame_ids_map[idx] for idx in indices]
            # Handle edge case for single-frame extraction in 'multi' (should be '50pct')
            if len(frame_tags) == 1 and "50pct" not in frame_tags:
                 mid_idx = max(0, min(total_frames - 1, round(total_frames * 0.50)))
                 frame_indices_to_extract = [mid_idx]
                 frame_tags = ["50pct"] # Default to midpoint if only one distinct frame possible
                 logger.debug("Strategy 'multi' resulted in single distinct frame index, defaulting to '50pct' tag.")

        elif strategy == 'midpoint': # Default/fallback strategy
            mid_idx = max(0, min(total_frames - 1, round(total_frames * 0.50)))
            frame_indices_to_extract.append(mid_idx)
            frame_tags.append("mid")
        else:
             logger.error(f"Unsupported keyframe strategy provided: '{strategy}'")
             raise ValueError(f"Unsupported keyframe strategy: {strategy}")

        # Generate a sanitized base filename part from clip_identifier
        scene_part = None
        match = re.search(r'_(clip|scene)_(\d+)$', clip_identifier)
        if match:
            scene_part = f"{match.group(1)}_{match.group(2)}"
        else:
            # Fallback: Try to extract a meaningful part or use sanitized end
            parts = clip_identifier.split('_')
            meaningful_part = next((part for part in reversed(parts) if any(char.isdigit() for char in part)), None)
            if meaningful_part:
                 scene_part = meaningful_part
            else:
                 sanitized_clip_id = re.sub(r'[^\w\-]+', '_', clip_identifier).strip('_')
                 scene_part = sanitized_clip_id[-50:] # Limit length
                 logger.warning(f"Could not parse standard suffix from clip_identifier '{clip_identifier}'. Using sanitized end part: '{scene_part}'")

        # Clean title for use in filenames
        sanitized_title = re.sub(r'[^\w\-\.]+', '_', title).strip('_')
        sanitized_title = re.sub(r'_+', '_', sanitized_title)
        if not sanitized_title: sanitized_title = "untitled" # Ensure there's always a title part
        sanitized_title = sanitized_title[:100] # Limit length


        frame_tag_map = dict(zip(frame_indices_to_extract, frame_tags))

        # Extract, save, and collect metadata for each frame
        for frame_index in frame_indices_to_extract:
            # Setting position might be inaccurate for some codecs; reading frame-by-frame might be more reliable but slower.
            # For keyframes, seeking is usually acceptable.
            cap.set(cv2.CAP_PROP_POS_FRAMES, float(frame_index)) # Use float for potentially better seeking
            ret, frame = cap.read()
            # Sometimes the first read after seeking fails, try reading again
            if not ret:
                logger.warning(f"Initial read failed for frame index {frame_index}, retrying read...")
                ret, frame = cap.read() # Try one more read

            if ret:
                frame_tag = frame_tag_map[frame_index]
                height, width = frame.shape[:2] # Get dimensions
                timestamp_sec = frame_index / fps if fps > 0 else 0.0 # Calculate timestamp

                # Construct filename
                output_filename_base = f"{sanitized_title}_{scene_part}_frame_{frame_tag}.jpg"
                output_path_temp_abs = os.path.join(local_temp_output_dir, output_filename_base)

                logger.debug(f"Saving frame {frame_index} (tag: {frame_tag}, time: {timestamp_sec:.2f}s) to TEMP path {output_path_temp_abs}")
                # Use imwrite parameters for quality if needed, e.g., [cv2.IMWRITE_JPEG_QUALITY, 90]
                success = cv2.imwrite(output_path_temp_abs, frame)

                if not success:
                    logger.error(f"Failed to write frame image to TEMP path {output_path_temp_abs}")
                else:
                    # Store metadata for successfully saved frame
                    extracted_frames_data.append({
                        'tag': frame_tag,
                        'local_path': output_path_temp_abs,
                        'frame_index': frame_index,
                        'timestamp_sec': round(timestamp_sec, 3), # Round timestamp
                        'width': width,
                        'height': height
                    })
            else:
                # This can happen if frame_index is exactly total_frames or video is corrupted
                logger.warning(f"Could not read frame at index {frame_index} (or after retry) from {local_temp_video_path}. Video might end here or be corrupted.")

    except IOError as e:
        logger.error(f"IOError processing video file {local_temp_video_path}: {e}", exc_info=True)
        extracted_frames_data = [] # Clear results on error
        raise # Re-raise to be caught by the task
    except cv2.error as cv_err:
        logger.error(f"OpenCV error processing video file {local_temp_video_path}: {cv_err}", exc_info=True)
        extracted_frames_data = []
        raise RuntimeError(f"OpenCV error during frame extraction for {local_temp_video_path}") from cv_err
    except Exception as e:
        logger.error(f"An unexpected error occurred processing {local_temp_video_path}: {e}", exc_info=True)
        extracted_frames_data = []
        raise # Re-raise
    finally:
        if cap and cap.isOpened():
            cap.release()
            logger.debug(f"Released video capture for {local_temp_video_path}")

    if not extracted_frames_data and total_frames > 0:
        # If the loop ran but didn't save anything, log a stronger warning.
        logger.error(f"CRITICAL: No frames were successfully extracted or saved for {local_temp_video_path} despite having {total_frames} frames reported. Check video integrity and OpenCV compatibility.")
    elif not extracted_frames_data and total_frames <= 0:
         logger.info(f"No frames extracted as video reported {total_frames} frames.")


    return extracted_frames_data


# --- Prefect Task ---
@task(name="Extract Clip Keyframes", retries=1, retry_delay_seconds=45)
def extract_keyframes_task(
    clip_id: int,
    strategy: str = 'midpoint', # e.g., 'midpoint', 'multi'
    overwrite: bool = False
    ):
    """
    Extracts keyframes from a clip video, uploads them to S3,
    and records them as artifacts in the database (`clip_artifacts` table).
    Updates the clip's state and `keyframed_at` timestamp upon success.
    """
    logger = get_run_logger()
    logger.info(f"TASK [Keyframe]: Starting for clip_id: {clip_id}, Strategy: '{strategy}', Overwrite: {overwrite}")

    if not s3_client:
        logger.error("S3 client is not available. Cannot perform S3 operations.")
        raise RuntimeError("S3 client is not initialized or configured. Cannot proceed.")

    # --- Resource Management Variables ---
    conn = None
    temp_dir_obj = None
    temp_dir = None # Define temp_dir here for broader scope

    # --- State & Result Variables ---
    final_status = "failed" # Assume failure unless explicitly set to success
    error_message_for_db = None
    generated_artifact_s3_keys = [] # Store S3 keys of successfully uploaded artifacts
    task_exception = None # Store exception to re-raise after cleanup attempt
    needs_processing = False # Flag to track if actual processing should occur
    processed_artifact_data_for_db = [] # Holds tuples for execute_values DB insert

    # --- Data Fetched from DB ---
    clip_s3_key = None
    clip_identifier = None
    title = None


    try:
        # === Phase 1: Initial DB Check and State Update ===
        conn = get_db_connection()
        conn.autocommit = False # Start manual transaction management

        try:
            with conn.cursor() as cur:
                # 1. Acquire Advisory Lock for the clip
                cur.execute("SELECT pg_advisory_xact_lock(2, %s)", (clip_id,))
                logger.info(f"Acquired DB lock for clip_id: {clip_id} (Phase 1 Check)")

                # 2. Fetch clip details
                cur.execute(
                    """
                    SELECT c.clip_filepath, c.clip_identifier, c.ingest_state, sv.title
                    FROM clips c JOIN source_videos sv ON c.source_video_id = sv.id
                    WHERE c.id = %s;
                    """, (clip_id,)
                )
                result = cur.fetchone()
                if not result:
                    raise ValueError(f"Clip ID {clip_id} not found in database.")

                clip_s3_key, clip_identifier, current_state, title = result
                logger.info(f"Found clip: ID={clip_id}, Identifier='{clip_identifier}', State='{current_state}', Source Title='{title}', Clip S3 Key='{clip_s3_key}'")

                # Validate essential data needed for processing
                if not clip_s3_key:
                    raise ValueError(f"Clip ID {clip_id} has NULL clip_filepath (S3 key). Cannot process.")
                if not title:
                    # Use a safe fallback if title is missing/empty
                    title = f"source_{clip_identifier.split('_')[0]}" # Basic fallback
                    logger.warning(f"Source title was NULL or empty for clip {clip_id}, using fallback: '{title}'")
                # Sanitize title once here for consistency
                title = re.sub(r'[^\w\-\.]+', '_', title).strip('_')
                title = re.sub(r'_+', '_', title)
                if not title: title = "untitled_clip" # Absolute fallback

                # 3. Check if processing should proceed based on state and overwrite flag
                artifacts_exist = False
                if not overwrite:
                    cur.execute(
                        "SELECT EXISTS (SELECT 1 FROM clip_artifacts WHERE clip_id = %s AND artifact_type = %s AND strategy = %s);",
                        (clip_id, ARTIFACT_TYPE_KEYFRAME, strategy)
                    )
                    artifacts_exist = cur.fetchone()[0]
                    logger.debug(f"Checked for existing artifacts (type='{ARTIFACT_TYPE_KEYFRAME}', strategy='{strategy}'): {artifacts_exist}")

                # Define states that allow processing initiation
                allow_processing_state = current_state in ['review_approved', 'keyframing_failed']

                if artifacts_exist and not overwrite:
                     logger.info(f"Skipping clip {clip_id}: Keyframe artifacts for strategy '{strategy}' already exist and overwrite=False.")
                     final_status = "skipped_exists"
                     needs_processing = False
                elif not allow_processing_state and not overwrite:
                     logger.warning(f"Skipping clip {clip_id}: State '{current_state}' is not eligible for keyframing initiation and overwrite=False.")
                     final_status = "skipped_state"
                     needs_processing = False
                elif overwrite and current_state not in ['keyframing']: # Allow overwrite from stable/failed states
                     needs_processing = True
                     logger.info(f"Proceeding with clip {clip_id}: Overwrite=True (will replace existing artifacts if any). Current state: {current_state}")
                elif allow_processing_state:
                     needs_processing = True
                     logger.info(f"Proceeding with clip {clip_id}: State='{current_state}' allows keyframing initiation.")
                else: # Should not happen based on logic, but catch all
                    logger.warning(f"Skipping clip {clip_id}. Logic state: artifacts_exist={artifacts_exist}, overwrite={overwrite}, current_state='{current_state}'.")
                    final_status = "skipped_logic"
                    needs_processing = False

                # 4. Update state to 'keyframing' only if processing will occur
                if needs_processing:
                    cur.execute(
                        """
                        UPDATE clips SET
                            ingest_state = 'keyframing',
                            updated_at = NOW(),
                            last_error = NULL -- Clear previous errors
                        WHERE id = %s;
                        """, (clip_id,)
                    )
                    if cur.rowcount == 0:
                         logger.error(f"Failed to update clip {clip_id} state to 'keyframing'. Row not found or state mismatch?")
                         conn.rollback()
                         raise RuntimeError(f"Concurrency Error: Failed to set clip {clip_id} state to 'keyframing'.")
                    logger.info(f"Set clip {clip_id} state to 'keyframing'")

            # ---- End Cursor Context for Phase 1 ----
            conn.commit() # Commit the state update (or lack thereof if skipped)
            logger.debug("Phase 1 DB check/update transaction committed.")

        except (ValueError, psycopg2.DatabaseError) as db_err:
             logger.error(f"DB Error during Phase 1 check/update for clip {clip_id}: {db_err}", exc_info=True)
             if conn: conn.rollback()
             error_message_for_db = f"DB Phase 1 Error: {str(db_err)[:500]}"
             task_exception = db_err
             raise # Re-raise to be caught by the main try-except block below

        # === Exit Early if Skipped ===
        if not needs_processing:
            logger.info(f"No processing required for clip {clip_id}. Final status: {final_status}")
            return {"status": final_status, "clip_id": clip_id, "generated_artifacts_count": 0, "artifact_s3_keys": []}

        # === Phase 2: Temp Dir Setup, S3 Download, Extraction, S3 Upload ===
        # (Run outside the initial DB transaction, requires S3 client)
        if not s3_client: # Double check S3 client after phase 1 commit
             raise RuntimeError("S3 client became unavailable after initial check.")

        try:
            temp_dir_obj = tempfile.TemporaryDirectory(prefix=f"meatspace_keyframe_{clip_id}_")
            temp_dir = Path(temp_dir_obj.name)
            logger.info(f"Using temporary directory: {temp_dir}")

            # 5. Download Clip Video from S3
            local_temp_clip_path = temp_dir / Path(clip_s3_key).name
            logger.info(f"Downloading s3://{S3_BUCKET_NAME}/{clip_s3_key} to {local_temp_clip_path}...")
            s3_client.download_file(S3_BUCKET_NAME, clip_s3_key, str(local_temp_clip_path))
            logger.info(f"Successfully downloaded clip video for {clip_id}.")

            # 6. Perform Frame Extraction
            # Returns list of dicts: [{'tag': '...', 'local_path': '...', 'frame_index': ..., ...}, ...]
            extracted_frames_metadata = _extract_and_save_frames_internal(
                str(local_temp_clip_path), clip_identifier, title, str(temp_dir), strategy
            )

            if not extracted_frames_metadata:
                 # If extraction returned nothing, log critical error and fail
                 logger.error(f"Frame extraction function returned no frames for clip {clip_id}. Cannot proceed.")
                 raise RuntimeError(f"Frame extraction yielded no results for clip {clip_id}.")

            logger.info(f"Successfully extracted {len(extracted_frames_metadata)} frame(s) locally for clip {clip_id}.")

            # 7. Upload Extracted Frames to S3 and Prepare Artifact Data for DB
            processed_artifact_data_for_db = [] # Holds tuples for execute_values DB insert
            generated_artifact_s3_keys = [] # Track successful uploads

            # Define which tag should be considered representative
            representative_tag = '50pct' if strategy == 'multi' else 'mid'

            for frame_data in extracted_frames_metadata:
                temp_local_path = Path(frame_data['local_path'])
                frame_tag = frame_data['tag']

                # Construct S3 Key: Use title, strategy, and the frame filename
                # Ensure prefix ends with '/' and handle potential double slashes
                s3_prefix = f"{KEYFRAMES_S3_PREFIX.strip('/')}/{title}/{strategy}/".replace("//", "/")
                frame_s3_key = f"{s3_prefix}{temp_local_path.name}"

                try:
                    logger.debug(f"Uploading {temp_local_path.name} to s3://{S3_BUCKET_NAME}/{frame_s3_key}")
                    with open(temp_local_path, "rb") as f:
                        s3_client.upload_fileobj(f, S3_BUCKET_NAME, frame_s3_key)

                    # Prepare metadata JSON for DB
                    is_representative = (frame_tag == representative_tag)
                    db_metadata = {
                        "frame_number": frame_data['frame_index'],
                        "timestamp_sec": frame_data['timestamp_sec'],
                        "width": frame_data['width'],
                        "height": frame_data['height'],
                        "is_representative": is_representative
                        # Add digest later if needed
                    }

                    # Append data for successful upload to list for batch DB insert
                    processed_artifact_data_for_db.append((
                        clip_id,
                        ARTIFACT_TYPE_KEYFRAME,
                        strategy,
                        frame_tag, # Use the specific tag ('mid', '25pct', etc.)
                        frame_s3_key,
                        Json(db_metadata), # Use psycopg2.extras.Json for proper JSON handling
                        datetime.now() # Use consistent timestamp for batch
                    ))
                    generated_artifact_s3_keys.append(frame_s3_key)

                except ClientError as s3_upload_err:
                    logger.error(f"Failed to upload keyframe {temp_local_path.name} to S3: {s3_upload_err}")
                    # TODO: Decide if one failure should stop the whole process or just skip this frame
                    # For keyframes, maybe it's okay to proceed if at least one uploads? Depends on strategy.
                    # Let's choose to fail the task if any upload fails.
                    raise RuntimeError(f"S3 upload failed for {frame_s3_key}") from s3_upload_err
                except Exception as upload_err:
                    logger.error(f"Unexpected error uploading keyframe {temp_local_path.name}: {upload_err}", exc_info=True)
                    raise # Re-raise unexpected errors

            # --- Verification after loop ---
            if len(generated_artifact_s3_keys) != len(extracted_frames_metadata):
                 # This case should ideally be caught by exceptions during upload now
                 logger.error("Mismatch between extracted frames and uploaded frames count.")
                 raise RuntimeError("Not all extracted frames were successfully uploaded to S3.")

            if not processed_artifact_data_for_db:
                # If loop finished but list is empty (e.g., extraction worked but all uploads failed silently - unlikely now)
                logger.error(f"No keyframe artifacts were successfully processed and prepared for DB insertion for clip {clip_id}.")
                raise RuntimeError(f"Failed to prepare any keyframe artifact data for DB for clip {clip_id}.")

            logger.info(f"Successfully uploaded {len(generated_artifact_s3_keys)} keyframe artifact(s) to S3.")

        except (IOError, cv2.error, RuntimeError, ClientError, ValueError) as phase2_err:
             # Catch errors specific to file handling, OpenCV, S3, or logic errors in this phase
             logger.error(f"Error during Phase 2 (Processing/Upload) for clip {clip_id}: {phase2_err}", exc_info=True)
             error_message_for_db = f"Processing/Upload Error: {str(phase2_err)[:500]}"
             task_exception = phase2_err
             raise # Re-raise to be caught by main try-except


        # === Phase 3: Final DB Update (Insert Artifacts, Update Clip State) ===
        # (Requires DB Connection)
        try:
            with conn.cursor() as cur:
                 # 1. Acquire Lock again for the final update transaction
                 cur.execute("SELECT pg_advisory_xact_lock(2, %s)", (clip_id,))
                 logger.info(f"Acquired DB lock for clip_id: {clip_id} (Phase 3 Update)")

                 # 2. Delete existing artifacts if overwriting
                 if overwrite:
                     logger.info(f"Overwrite=True: Deleting existing '{ARTIFACT_TYPE_KEYFRAME}' artifacts with strategy '{strategy}' for clip {clip_id}...")
                     delete_sql = sql.SQL("""
                         DELETE FROM clip_artifacts
                         WHERE clip_id = %s AND artifact_type = %s AND strategy = %s;
                     """)
                     cur.execute(delete_sql, (clip_id, ARTIFACT_TYPE_KEYFRAME, strategy))
                     logger.info(f"Deletion of existing artifacts completed (affected {cur.rowcount} rows).")

                 # 3. Insert new artifact records using execute_values for efficiency
                 insert_query = sql.SQL("""
                     INSERT INTO clip_artifacts (
                         clip_id, artifact_type, strategy, tag, s3_key, metadata, created_at, updated_at
                     ) VALUES %s
                     ON CONFLICT (clip_id, artifact_type, strategy, tag) DO UPDATE SET
                         s3_key = EXCLUDED.s3_key,
                         metadata = EXCLUDED.metadata,
                         updated_at = NOW(); -- Update timestamp on conflict/update
                 """)
                 # Prepare data tuples for execute_values
                 now = datetime.now()
                 values_to_insert = [
                     (*data_tuple[:-1], now)
                     for data_tuple in processed_artifact_data_for_db
                 ]

                 execute_values(cur, insert_query, values_to_insert)
                 logger.info(f"Successfully inserted/updated {len(processed_artifact_data_for_db)} artifact records into DB for clip {clip_id}.")

                 # 4. Update Clip Status and Timestamp
                 update_clip_sql = sql.SQL("""
                     UPDATE clips SET
                         ingest_state = 'keyframed',
                         keyframed_at = NOW(), -- Set the completion timestamp
                         last_error = NULL,
                         updated_at = NOW()   -- Also update the main updated_at
                     WHERE id = %s AND ingest_state = 'keyframing'; -- Ensure state was correct
                 """)
                 cur.execute(update_clip_sql, (clip_id,))

                 if cur.rowcount == 1:
                      logger.info(f"Successfully updated clip {clip_id} state to 'keyframed' and set keyframed_at.")
                      final_status = "success" # Mark as success ONLY after final DB commit works
                 else:
                      # This indicates a potential concurrency issue or unexpected state change
                      logger.error(f"Failed to update clip {clip_id} final state to 'keyframed'. Rowcount: {cur.rowcount}. State might have changed unexpectedly.")
                      # Rollback this transaction as the final state update failed
                      conn.rollback()
                      final_status = "failed_state_update"
                      error_message_for_db = "Failed final state update to 'keyframed'"
                      # Raise an error to indicate the task didn't fully succeed
                      raise RuntimeError(f"Failed to update clip {clip_id} state to 'keyframed' in final DB step.")

            # --- End Cursor Context for Phase 3 ---
            conn.commit() # Commit the artifact inserts and clip update
            logger.info("Phase 3 DB updates committed successfully.")

        except (psycopg2.DatabaseError, RuntimeError) as db_err:
             # Catch DB errors during the final update or the RuntimeError from failed state update
             logger.error(f"DB Error during Phase 3 Update for clip {clip_id}: {db_err}", exc_info=True)
             if conn: conn.rollback() # Rollback the transaction
             error_message_for_db = f"DB Phase 3 Error: {str(db_err)[:500]}"
             task_exception = db_err
             final_status = "failed_db_update" # Explicit status
             raise # Re-raise


    except (ValueError, IOError, FileNotFoundError, RuntimeError, ClientError, psycopg2.DatabaseError) as e:
        # --- Main Error Handling Block ---
        # Catches errors from any phase that were re-raised
        logger.error(f"TASK FAILED [Keyframe]: clip_id {clip_id} - Error: {e}", exc_info=True)
        final_status = final_status if final_status != "failed" else "failed_processing" # Keep specific failure if set
        error_message_for_db = error_message_for_db or f"{type(e).__name__}: {str(e)[:500]}"
        task_exception = e # Store the caught exception

        # Attempt to update DB state to failed, but only if processing was attempted
        if conn and needs_processing: # Check if conn exists and processing started
            try:
                logger.warning(f"Attempting to roll back transaction and set clip {clip_id} state to 'keyframing_failed'.")
                conn.rollback() # Ensure any partial transaction is rolled back
                conn.autocommit = True # Use autocommit for fail state update
                with conn.cursor() as err_cur:
                    fail_update_sql = sql.SQL("""
                        UPDATE clips SET
                            ingest_state = 'keyframing_failed',
                            last_error = %s,
                            retry_count = COALESCE(retry_count, 0) + 1,
                            updated_at = NOW()
                        WHERE id = %s AND ingest_state = 'keyframing'; -- Only update if it was in 'keyframing' state
                    """)
                    err_cur.execute(fail_update_sql, (error_message_for_db, clip_id))
                logger.info(f"Attempted to set clip {clip_id} state to 'keyframing_failed' in DB.")
            except Exception as db_fail_err:
                logger.error(f"CRITICAL: Failed to update error state in DB for clip {clip_id} after task failure: {db_fail_err}")
        elif not conn:
             logger.error("DB connection was not available to update error state.")

    except Exception as e:
         # Catch any truly unexpected errors
         logger.error(f"UNEXPECTED TASK FAILED [Keyframe]: clip_id {clip_id} - Error: {e}", exc_info=True)
         final_status = "failed_unexpected"
         error_message_for_db = f"Unexpected:{type(e).__name__}: {str(e)[:480]}"
         task_exception = e
         # Attempt DB update similar to above
         if conn and needs_processing:
             try:
                conn.rollback(); conn.autocommit = True
                with conn.cursor() as err_cur:
                    err_cur.execute(sql.SQL("""UPDATE clips SET ingest_state = 'keyframing_failed', last_error = %s, retry_count = COALESCE(retry_count, 0) + 1, updated_at = NOW() WHERE id = %s AND ingest_state = 'keyframing';"""), (error_message_for_db, clip_id))
                logger.info(f"Attempted to set clip {clip_id} state to 'keyframing_failed' after unexpected error.")
             except Exception as db_fail_err:
                logger.error(f"CRITICAL: Failed to update error state in DB for clip {clip_id} after unexpected task failure: {db_fail_err}")

    finally:
        # --- Cleanup ---
        if conn:
            conn.autocommit = True # Ensure reset before releasing
            release_db_connection(conn)
            logger.debug(f"DB connection released for clip_id: {clip_id}")
        if temp_dir_obj:
            try:
                shutil.rmtree(temp_dir, ignore_errors=True)
                logger.info(f"Cleaned up temporary directory: {temp_dir}")
            except Exception as cleanup_err:
                 logger.warning(f"Error cleaning up temporary directory {temp_dir}: {cleanup_err}")

    # === Return Result or Raise Exception ===
    result_payload = {
        "status": final_status,
        "clip_id": clip_id,
        "strategy": strategy,
        "generated_artifacts_count": len(generated_artifact_s3_keys),
        "artifact_s3_keys": generated_artifact_s3_keys,
    }

    if final_status == "success":
        logger.info(f"TASK [Keyframe] finished successfully for clip_id {clip_id}. Result: {result_payload}")
        return result_payload
    elif final_status.startswith("skipped"):
         logger.info(f"TASK [Keyframe] skipped for clip_id {clip_id}. Status: {final_status}")
         return result_payload
    else:
        # Failure case: Log the final payload and re-raise the captured exception
        logger.error(f"TASK [Keyframe] failed for clip_id {clip_id}. Final Status: {final_status}. Error: {error_message_for_db}. Result payload: {result_payload}")
        if task_exception:
            # Re-raise the original exception for Prefect to handle retries/failure states
            raise task_exception
        else:
            # If no specific exception was captured, raise a generic one
            raise RuntimeError(f"Keyframe task failed for clip {clip_id} with status {final_status}. Reason: {error_message_for_db or 'Unknown error'}")


# Local run block for testing purposes
if __name__ == "__main__":
    print("Running keyframe task locally for testing (requires DB/S3 setup)...")
    # Example: prefect run python tasks/keyframe.py extract_keyframes_task --param clip_id=YOUR_CLIP_ID --param strategy=multi --param overwrite=True
    # Replace YOUR_CLIP_ID with an actual ID
    # Requires a running Prefect worker/agent or direct execution setup
    # You might need to initialize DB pool explicitly for local script runs if not managed by Prefect agent context.
    pass
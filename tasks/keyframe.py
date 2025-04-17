import cv2
import os
import re
import shutil
import traceback
from pathlib import Path
from prefect import task, get_run_logger
import psycopg2
from psycopg2 import sql

# Assuming db_utils.py is in ../utils relative to tasks/
import sys
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

try:
    from utils.db_utils import get_db_connection, get_media_base_dir
except ImportError as e:
    print(f"Error importing DB utils in keyframe.py: {e}")
    # Provide dummy functions if needed for basic loading, but task will fail
    def get_db_connection(): raise NotImplementedError("Dummy DB connection")
    def get_media_base_dir(): raise NotImplementedError("Dummy media base dir")

# --- Configuration ---
# Base subdirectory under MEDIA_BASE_DIR for keyframes
KEYFRAMES_BASE_SUBDIR = os.getenv("KEYFRAMES_SUBDIR", "keyframes")

# --- Core Frame Extraction Logic (Adapted from script) ---
def _extract_and_save_frames_internal(
    abs_video_path: str,
    clip_identifier: str,
    source_identifier: str,
    output_dir_abs: str,
    strategy: str = 'midpoint'
    ) -> dict:
    """
    Internal function to extract keyframe(s), save them, and return relative paths.
    Assumes output directory already exists. Uses standard logging.

    Args:
        abs_video_path (str): Absolute path to the input video clip file.
        clip_identifier (str): The logical identifier of the clip (e.g., Some_Long_Name_clip_00123).
        source_identifier (str): The short identifier for the source (e.g., Landline).
        output_dir_abs (str): Absolute path to the directory to save frames (includes source/strategy).
        strategy (str): 'midpoint' or 'multi'.

    Returns:
        dict: Dictionary mapping strategy-based ID ('mid', '25pct', etc.) to relative file path.
              Returns empty dict on error. Raises exceptions on critical errors.
    """
    logger = get_run_logger() # Use Prefect logger if available, else basic print
    saved_frame_rel_paths = {}
    cap = None
    try:
        logger.debug(f"Opening video file for frame extraction: {abs_video_path}")
        cap = cv2.VideoCapture(abs_video_path)
        if not cap.isOpened():
            # Raise specific error instead of just printing
            raise IOError(f"Error opening video file: {abs_video_path}")

        total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        fps = cap.get(cv2.CAP_PROP_FPS)
        logger.debug(f"Video properties: Total Frames={total_frames}, FPS={fps:.2f}")

        if total_frames <= 0:
            logger.warning(f"Video file has no frames or is invalid: {abs_video_path}")
            # Don't raise, just return empty dict as it might be a valid (empty) clip
            return saved_frame_rel_paths

        frame_indices_to_extract = []
        frame_ids = [] # Use consistent tags like '25pct', '50pct', '75pct', 'mid'

        if strategy == 'multi':
            # Calculate indices for 25%, 50%, 75%
            idx_25 = max(0, total_frames // 4)
            idx_50 = max(0, total_frames // 2)
            idx_75 = max(0, (total_frames * 3) // 4)
            indices = sorted(list(set([idx_25, idx_50, idx_75]))) # Unique, sorted indices
            frame_indices_to_extract.extend(indices)

            # Create corresponding frame tags dynamically based on unique indices found
            frame_ids_map = {idx_25: "25pct", idx_50: "50pct", idx_75: "75pct"}
            # If indices collapsed, map them correctly
            frame_ids = [frame_ids_map[idx] for idx in indices]
            # Ensure midpoint exists if indices collapse to one
            if len(frame_ids) == 1 and frame_ids[0] != "50pct":
                frame_ids = ["50pct"] # Relabel single frame as 50pct/midpoint

        else: # Default to midpoint (handles 'midpoint' explicitly)
            frame_indices_to_extract.append(max(0, total_frames // 2))
            frame_ids.append("mid")

        # NOTE: output_dir_abs is created by the calling task function
        media_base_dir = get_media_base_dir() # Assumes get_media_base_dir() is available

        # --- Parse scene/clip part from clip_identifier ---
        # Use the whole clip identifier (sanitized) for uniqueness if scene parsing fails
        # Example: Landline_SomeLongName_clip_00123 -> scene_part=clip_00123
        scene_part = None
        match = re.search(r'_(clip|scene)_(\d+)$', clip_identifier)
        if match:
            scene_part = f"{match.group(1)}_{match.group(2)}" # e.g., "clip_00123" or "scene_123"
        else:
            # Fallback: Use the last part after underscore if it contains digits
            parts = clip_identifier.split('_')
            if len(parts) > 1 and any(char.isdigit() for char in parts[-1]):
                scene_part = parts[-1]
            else:
                 # Last resort: Sanitize the whole identifier (might be long)
                 sanitized_clip_id = re.sub(r'[^\w\-]+', '_', clip_identifier).strip('_')
                 scene_part = sanitized_clip_id[-50:] # Limit length
                 logger.warning(f"Could not parse standard suffix from clip_identifier '{clip_identifier}'. Using sanitized end part: '{scene_part}'")

        # --- Generate and Save Frames ---
        frame_tags_map = dict(zip(frame_indices_to_extract, frame_ids)) # Map index to tag

        for frame_index in frame_indices_to_extract:
            logger.debug(f"Seeking to frame {frame_index}")
            cap.set(cv2.CAP_PROP_POS_FRAMES, frame_index)
            ret, frame = cap.read()
            if ret:
                frame_tag = frame_tags_map[frame_index]

                # --- Construct NEW output filename ---
                # Format: <SourceIdentifier>_<ScenePart>_frame_<FrameTag>.jpg
                # Example: Landline_clip_00123_frame_50pct.jpg
                output_filename = f"{source_identifier}_{scene_part}_frame_{frame_tag}.jpg"
                # Sanitize further just in case source/scene parts had odd chars
                output_filename = re.sub(r'[^\w\.\-]+', '_', output_filename)

                output_path_abs = os.path.join(output_dir_abs, output_filename)
                # Calculate relative path based on MEDIA_BASE_DIR for DB storage
                try:
                    output_path_rel = os.path.relpath(output_path_abs, media_base_dir)
                    # Convert to forward slashes for cross-platform consistency in DB/URLs
                    output_path_rel = output_path_rel.replace("\\", "/")
                except ValueError as e:
                    logger.error(f"Could not create relative path from '{output_path_abs}' based on '{media_base_dir}': {e}. Storing absolute path.")
                    output_path_rel = output_path_abs # Fallback, though not ideal

                logger.debug(f"Saving frame {frame_index} (tag: {frame_tag}) to {output_path_abs}")
                success = cv2.imwrite(output_path_abs, frame)
                if not success:
                    logger.error(f"Failed to write frame image to {output_path_abs}")
                    # Decide: continue with others or raise? Let's log and continue for now.
                else:
                    saved_frame_rel_paths[frame_tag] = output_path_rel # Store relative path with tag
            else:
                # This might happen if seeking goes slightly beyond the actual last frame
                logger.warning(f"Could not read frame {frame_index} from {abs_video_path}. It might be at the very end.")
        # --- End Frame Loop ---

    except IOError as e: # Catch specific IO error from opening video
        logger.error(f"IOError processing video {abs_video_path}: {e}")
        raise # Re-raise IOError to signal failure
    except cv2.error as cv_err:
         logger.error(f"OpenCV error processing video {abs_video_path}: {cv_err}")
         raise RuntimeError(f"OpenCV error during frame extraction") from cv_err
    except Exception as e:
        logger.error(f"An unexpected error occurred processing {abs_video_path}: {e}", exc_info=True)
        raise # Re-raise unexpected errors
    finally:
        if cap and cap.isOpened():
            cap.release()
            logger.debug(f"Released video capture for {abs_video_path}")

    if not saved_frame_rel_paths and total_frames > 0:
         logger.warning(f"No frames were successfully saved for {abs_video_path} despite processing.")
         # Optionally raise an error here if saving at least one frame is mandatory

    return saved_frame_rel_paths

# --- Prefect Task ---
@task(name="Extract Clip Keyframes", retries=1, retry_delay_seconds=30)
def extract_keyframes_task(
    clip_id: int,
    strategy: str = 'midpoint',
    overwrite: bool = False
    ):
    """
    Prefect task to extract keyframe(s) for a single clip, save them,
    and update the database record.

    Args:
        clip_id (int): The ID of the clip to process.
        strategy (str): 'midpoint' or 'multi'.
        overwrite (bool): If True, overwrite existing keyframes and DB path.

    Returns:
        dict: Information about the operation, including the representative
              keyframe path stored in the DB. Example:
              {'status': 'success', 'clip_id': clip_id, 'representative_path': '...', 'all_paths': {...}}
              or {'status': 'skipped', ...} or raises error on failure.
    """
    logger = get_run_logger()
    logger.info(f"TASK [Keyframe]: Starting for clip_id: {clip_id}, Strategy: '{strategy}', Overwrite: {overwrite}")
    conn = None
    representative_rel_path = None
    saved_frame_rel_paths = {}
    final_status = "failed" # Default status

    try:
        media_base_dir = get_media_base_dir()
        conn = get_db_connection()
        conn.autocommit = False # Start transaction explicitly if needed, or use context manager

        # Use transaction context manager for automatic commit/rollback
        with conn.transaction():
            # === Transaction Start ===
            with conn.cursor() as cur:
                # 1. Acquire Advisory Lock for this clip
                try:
                    cur.execute("SELECT pg_advisory_xact_lock(2, %s)", (clip_id,)) # Use type 2 for clips
                    logger.info(f"Acquired DB lock for clip_id: {clip_id}")
                except Exception as lock_err:
                    logger.error(f"Failed to acquire DB lock for clip_id {clip_id}: {lock_err}")
                    raise RuntimeError("DB Lock acquisition failed") from lock_err

                # 2. Fetch clip details and source identifier, check state
                cur.execute(
                    """
                    SELECT
                        c.clip_filepath, c.clip_identifier, c.keyframe_filepath, c.ingest_state,
                        sv.source_identifier
                    FROM clips c
                    JOIN source_videos sv ON c.source_video_id = sv.id
                    WHERE c.id = %s;
                    """,
                    (clip_id,)
                )
                result = cur.fetchone()
                if not result:
                    raise ValueError(f"Clip ID {clip_id} not found.")

                rel_clip_path, clip_identifier, current_keyframe_path, current_state, source_identifier = result
                logger.info(f"Found clip: ID={clip_id}, Identifier='{clip_identifier}', State='{current_state}', Source='{source_identifier}'")

                # 3. Check if processing should proceed
                # Allow 'review_approved' or failed states if overwrite=True
                allow_processing = current_state == 'review_approved' or \
                                  (overwrite and current_state in ['keyframing_failed', 'keyframed', 'embedding_failed', 'embedded'])
                # Also allow if keyframe path is missing and state isn't explicitly failed/complete
                allow_processing = allow_processing or (not current_keyframe_path and current_state not in ['keyframing_failed', 'keyframed', 'embedding_failed', 'embedded'])


                if not allow_processing:
                    # More robust check: Is the *correct* keyframe path already set for this strategy?
                    expected_dir_part = Path(KEYFRAMES_BASE_SUBDIR) / source_identifier / strategy
                    path_matches_strategy = False
                    if current_keyframe_path:
                        # Normalize paths for comparison
                        try:
                             current_path_norm = Path(current_keyframe_path).parent
                             if current_path_norm == expected_dir_part:
                                 path_matches_strategy = True
                        except Exception:
                            pass # Ignore errors parsing path

                    if path_matches_strategy and not overwrite:
                        logger.info(f"Skipping clip {clip_id}: Keyframe already exists with matching strategy ('{strategy}') path and overwrite=False.")
                        final_status = "skipped"
                        return {"status": final_status, "reason": "Existing keyframe path matches strategy", "clip_id": clip_id}
                    elif current_state not in ['review_approved', 'keyframing_failed'] and not overwrite:
                         logger.warning(f"Skipping clip {clip_id}: Current state '{current_state}' is not suitable for keyframing and overwrite=False.")
                         final_status = "skipped"
                         return {"status": final_status, "reason": f"Incorrect state: {current_state}", "clip_id": clip_id}
                    else:
                         logger.info(f"Proceeding with clip {clip_id}: State='{current_state}', Overwrite={overwrite}, PathMatch={path_matches_strategy}")


                if not rel_clip_path:
                    raise ValueError(f"Clip ID {clip_id} has NULL clip_filepath.")
                if not source_identifier:
                    raise ValueError(f"Source identifier is NULL for clip ID {clip_id}.")

                # Update state to 'keyframing'
                cur.execute(
                    "UPDATE clips SET ingest_state = 'keyframing', updated_at = NOW(), last_error = NULL WHERE id = %s",
                    (clip_id,)
                )
                logger.info(f"Set clip {clip_id} state to 'keyframing'")

                # --- Prepare Paths ---
                abs_clip_path = os.path.join(media_base_dir, rel_clip_path)
                if not os.path.isfile(abs_clip_path):
                    raise FileNotFoundError(f"Clip video file not found: {abs_clip_path}")

                output_dir_abs = os.path.join(media_base_dir, KEYFRAMES_BASE_SUBDIR, source_identifier, strategy)
                logger.info(f"Ensuring output directory exists: {output_dir_abs}")
                os.makedirs(output_dir_abs, exist_ok=True)

                # --- Perform Extraction ---
                logger.info(f"Calling frame extraction for clip {clip_id} ('{clip_identifier}')...")
                # Use the internal function that raises errors
                saved_frame_rel_paths = _extract_and_save_frames_internal(
                    abs_clip_path,
                    clip_identifier,
                    source_identifier,
                    output_dir_abs,
                    strategy
                )

                if not saved_frame_rel_paths:
                    # This could happen if the video was empty or frames couldn't be read/saved
                    logger.warning(f"No keyframes were extracted or saved for clip {clip_id}. Check video validity.")
                    # If no frames were saved, we can't pick a representative one. Consider this a failure?
                    # Let's treat it as a failure for now, as subsequent steps rely on a keyframe.
                    raise RuntimeError(f"Failed to extract any keyframes for clip {clip_id}")

                # --- Select Representative Keyframe ---
                representative_key = None
                if strategy == 'multi':
                    # Prefer '50pct', fallback if needed
                    representative_key = '50pct' if '50pct' in saved_frame_rel_paths else next(iter(saved_frame_rel_paths), None)
                elif strategy == 'midpoint':
                    representative_key = 'mid' if 'mid' in saved_frame_rel_paths else next(iter(saved_frame_rel_paths), None)
                # Add elif for future strategies

                if representative_key and representative_key in saved_frame_rel_paths:
                    representative_rel_path = saved_frame_rel_paths[representative_key]
                    logger.info(f"Selected representative keyframe ({representative_key}): {representative_rel_path}")
                else:
                    logger.error(f"Could not determine representative keyframe for clip {clip_id} from tags: {list(saved_frame_rel_paths.keys())}")
                    raise ValueError("Failed to select a representative keyframe")

                # --- Update DB on Success ---
                cur.execute(
                    """
                    UPDATE clips
                    SET keyframe_filepath = %s,
                        ingest_state = 'keyframed',
                        keyframed_at = NOW(),
                        last_error = NULL,
                        updated_at = NOW()
                    WHERE id = %s
                    """,
                    (representative_rel_path, clip_id)
                )
                logger.info(f"Successfully updated DB for clip {clip_id}. State set to 'keyframed'.")
                final_status = "success"

        # === Transaction End (Commit happens here if no exceptions) ===
        logger.info(f"Transaction committed for clip {clip_id}.")

    except (ValueError, IOError, FileNotFoundError, RuntimeError, psycopg2.DatabaseError) as e:
        logger.error(f"TASK FAILED [Keyframe]: clip_id {clip_id} - {e}", exc_info=True)
        if conn:
            # Don't rollback here, transaction context manager handles it.
            # Update DB state outside the transaction (requires autocommit=True)
            try:
                conn.autocommit = True # Enable autocommit for error update
                with conn.cursor() as err_cur:
                    err_cur.execute(
                        """
                        UPDATE clips
                        SET ingest_state = 'keyframing_failed',
                            last_error = %s,
                            retry_count = COALESCE(retry_count, 0) + 1,
                            updated_at = NOW()
                        WHERE id = %s AND ingest_state = 'keyframing'
                        """,
                        (f"{type(e).__name__}: {str(e)[:500]}", clip_id) # Store error message
                    )
                logger.info(f"Set clip {clip_id} state to 'keyframing_failed' after error.")
            except Exception as db_err:
                logger.error(f"Failed to update error state in DB for clip {clip_id} after task failure: {db_err}")
        # Re-raise the original exception to mark the Prefect task as failed
        raise e
    except Exception as e:
         # Catch any other unexpected errors
         logger.error(f"UNEXPECTED TASK FAILED [Keyframe]: clip_id {clip_id} - {e}", exc_info=True)
         if conn:
             try:
                 conn.autocommit = True
                 with conn.cursor() as err_cur:
                    err_cur.execute(
                        """
                        UPDATE clips SET ingest_state = 'keyframing_failed', last_error = %s, retry_count = COALESCE(retry_count, 0) + 1, updated_at = NOW()
                        WHERE id = %s AND ingest_state = 'keyframing'
                        """,
                         (f"Unexpected:{type(e).__name__}: {str(e)[:480]}", clip_id)
                    )
                 logger.info(f"Set clip {clip_id} state to 'keyframing_failed' after unexpected error.")
             except Exception as db_err:
                 logger.error(f"Failed to update error state in DB for clip {clip_id} after unexpected task failure: {db_err}")
         raise # Re-raise
    finally:
        if conn:
            conn.autocommit = True # Ensure autocommit is reset before closing
            conn.close()
            logger.debug(f"DB connection closed for clip_id: {clip_id}")

    # Return success information
    return {
        "status": final_status,
        "clip_id": clip_id,
        "representative_path": representative_rel_path,
        "all_paths": saved_frame_rel_paths,
        "strategy": strategy
    }

# Example of how to potentially run this locally (for testing syntax, etc.)
# Note: Requires DB and media files to be accessible
if __name__ == "__main__":
    print("Running keyframe task locally for testing (requires specific setup)...")
    # Replace with a valid clip_id from your DB that is in 'review_approved' state
    test_clip_id = 1
    test_strategy = 'multi' # or 'midpoint'
    test_overwrite = True # Set to True to force processing if already done

    # Make sure environment variables like DATABASE_URL, MEDIA_BASE_DIR are set
    if not os.getenv("DATABASE_URL") or not os.getenv("MEDIA_BASE_DIR"):
         print("Error: DATABASE_URL and MEDIA_BASE_DIR must be set in environment for local test.")
    else:
        try:
            # Prefect tasks can be called like regular functions for local testing
            result = extract_keyframes_task.fn( # Use .fn to bypass Prefect engine orchestration
                clip_id=test_clip_id,
                strategy=test_strategy,
                overwrite=test_overwrite
            )
            print(f"\nLocal Test Result:\n{result}")
        except Exception as e:
            print(f"\nLocal test failed: {e}")
            traceback.print_exc()
import cv2
import os
import re
import shutil
import psycopg2
from psycopg2 import sql
from psycopg2.extras import DictCursor  # To get dict‑like rows
from pathlib import Path
from prefect import task, get_run_logger
import traceback

# Add project root to sys.path
import sys

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

try:
    from utils.db_utils import get_db_connection, get_media_base_dir
except ImportError:
    print("Could not import from utils.db_utils. Ensure it's in the correct path.")
    sys.exit(1)  # Or handle differently if script part is needed

# --------------------------------------------------------------------------- #
#                               Core Logic                                    #
# --------------------------------------------------------------------------- #


def _extract_and_save_frames_logic(
    abs_clip_path: str,
    clip_identifier: str,
    source_title: str,  # Fetched from DB
    output_dir_abs: Path,  # Pre‑calculated absolute path
    strategy: str,
    media_base_dir: Path,  # Passed for relative path calculation
) -> dict:
    """
    Internal logic to extract keyframe(s), save them, and return relative paths.
    Uses provided source_title for naming.
    """
    logger = get_run_logger()  # Assumes called within a Prefect task context
    saved_frame_rel_paths = {}
    cap = None
    try:
        logger.debug(f"Opening video capture for: {abs_clip_path}")
        cap = cv2.VideoCapture(abs_clip_path)
        if not cap.isOpened():
            logger.error(f"Failed to open video file: {abs_clip_path}")
            return {}  # Return empty on open failure

        total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        if total_frames <= 0:
            logger.warning(f"Video file has no frames or is invalid: {abs_clip_path}")
            cap.release()
            return {}

        frame_indices_to_extract = []
        frame_ids = []  # Tags like '25pct', '50pct', '75pct', 'mid'

        # ----------------------- Frame‑selection strategy -------------------- #
        if strategy == "multi":
            idx_25 = max(0, total_frames // 4)
            idx_50 = max(0, total_frames // 2)
            idx_75 = max(0, (total_frames * 3) // 4)
            indices = sorted(list(set([idx_25, idx_50, idx_75])))
            frame_indices_to_extract.extend(indices)

            if len(indices) == 3:
                frame_ids = ["25pct", "50pct", "75pct"]
            elif len(indices) == 2:
                frame_ids = ["25pct", "75pct"]
            elif len(indices) == 1:
                frame_ids = ["mid"]
            logger.debug(f"Multi strategy: indices={indices}, tags={frame_ids}")

        elif strategy == "midpoint":
            frame_indices_to_extract.append(max(0, total_frames // 2))
            frame_ids.append("mid")
            logger.debug(
                f"Midpoint strategy: index={frame_indices_to_extract[0]}, tag={frame_ids[0]}"
            )
        else:
            logger.warning(
                f"Unknown keyframe strategy '{strategy}'. Defaulting to midpoint."
            )
            frame_indices_to_extract.append(max(0, total_frames // 2))
            frame_ids.append("mid")

        output_dir_abs.mkdir(parents=True, exist_ok=True)

        # -------------------- Filename helpers & sanitising ------------------ #
        sanitized_source_title = "".join(
            c if c.isalnum() or c in ["-", "_"] else "_" for c in source_title
        )
        scene_part_match = re.search(r"_scene_(\d+)$", clip_identifier)
        if scene_part_match:
            scene_part = f"scene_{scene_part_match.group(1)}"
        else:
            sanitized_clip_id = re.sub(r"[^\w\-]+", "_", clip_identifier)
            scene_part = sanitized_clip_id
            logger.warning(
                f"Could not parse 'scene_XXX' from clip_identifier "
                f"'{clip_identifier}'. Using '{scene_part}' in filename."
            )

        # ------------------------- Extract & save frames --------------------- #
        frame_tags_map = dict(zip(frame_indices_to_extract, frame_ids))

        for frame_index in frame_indices_to_extract:
            cap.set(cv2.CAP_PROP_POS_FRAMES, frame_index)
            ret, frame = cap.read()
            if not ret:
                logger.warning(
                    f"Could not read frame index {frame_index} from {abs_clip_path}"
                )
                continue

            frame_tag = frame_tags_map.get(frame_index, f"frame_{frame_index}")
            output_filename = (
                f"{sanitized_source_title}_{scene_part}_frame_{frame_tag}.jpg"
            )
            output_path_abs = output_dir_abs / output_filename
            output_path_rel_str = str(Path(os.path.relpath(output_path_abs, media_base_dir)))

            logger.debug(
                f"Saving frame index {frame_index} (tag: {frame_tag}) to {output_path_abs}"
            )
            if cv2.imwrite(str(output_path_abs), frame):
                saved_frame_rel_paths[frame_tag] = output_path_rel_str
            else:
                logger.error(f"Failed to write keyframe image to {output_path_abs}")

    except Exception as e:
        logger.error(
            f"Error during keyframe extraction for {abs_clip_path}: {e}", exc_info=True
        )
    finally:
        if cap and cap.isOpened():
            cap.release()
            logger.debug(f"Released video capture for: {abs_clip_path}")

    return saved_frame_rel_paths


# --------------------------------------------------------------------------- #
#                           Prefect Task Definition                           #
# --------------------------------------------------------------------------- #


@task(name="Extract Keyframes", retries=1, retry_delay_seconds=10)
def extract_keyframes_task(clip_id: int, strategy: str = "midpoint"):
    """
    Prefect task to extract keyframe(s) for a single clip, update the DB.
    Uses source_title fetched from DB for output path/filename.
    """
    logger = get_run_logger()
    logger.info(f"TASK [Keyframe]: Starting for clip_id: {clip_id}, Strategy: {strategy}")
    conn = None
    media_base_dir = Path(get_media_base_dir())
    representative_rel_path = None  # Will hold the path to update in DB

    try:
        conn = get_db_connection()
        conn.autocommit = False  # Manage transaction manually

        with conn.cursor(cursor_factory=DictCursor) as cur:
            # === Transaction Start ===

            # 1. Acquire Advisory Lock (domain 2 for clips)
            try:
                cur.execute("SELECT pg_advisory_xact_lock(2, %s)", (clip_id,))
                logger.info(f"Acquired DB lock for clip_id: {clip_id}")
            except Exception as lock_err:
                logger.error(f"Failed to acquire DB lock for clip_id {clip_id}: {lock_err}")
                raise RuntimeError("DB Lock acquisition failed") from lock_err

            # 2. Fetch clip info and check state
            cur.execute(
                """
                SELECT
                    c.ingest_state,
                    c.clip_filepath,
                    c.clip_identifier,
                    c.keyframe_filepath,
                    sv.title AS source_title
                FROM clips c
                JOIN source_videos sv ON c.source_video_id = sv.id
                WHERE c.id = %s;
                """,
                (clip_id,),
            )
            clip_info = cur.fetchone()
            if not clip_info:
                raise ValueError(f"Clip with ID {clip_id} not found in database.")

            current_state = clip_info["ingest_state"]
            rel_clip_path = clip_info["clip_filepath"]
            clip_identifier = clip_info["clip_identifier"]
            source_title = clip_info["source_title"]

            allowed_states = ["review_approved"]
            if current_state not in allowed_states:
                logger.warning(
                    f"Clip {clip_id} is not in an expected state for keyframing "
                    f"('{current_state}'). Skipping."
                )
                conn.rollback()
                return {"status": "skipped", "reason": f"Incorrect state: {current_state}"}

            if not rel_clip_path:
                raise ValueError(f"Clip {clip_id} has NULL clip_filepath.")
            if not source_title:
                raise ValueError(f"Source video associated with clip {clip_id} has NULL title.")

            # 3. Update state to 'keyframing'
            cur.execute(
                """
                UPDATE clips
                SET ingest_state = 'keyframing',
                    retry_count   = 0,
                    last_error    = NULL,
                    updated_at    = NOW()
                WHERE id = %s
                """,
                (clip_id,),
            )
            logger.info(f"Set state to 'keyframing' for clip_id: {clip_id}")

            # 4. Run extraction
            abs_clip_path = str(media_base_dir / rel_clip_path)
            if not os.path.isfile(abs_clip_path):
                raise FileNotFoundError(f"Clip file not found at derived path: {abs_clip_path}")

            sanitized_source_title = "".join(
                c if c.isalnum() or c in ["-", "_"] else "_" for c in source_title
            )
            output_dir_abs = media_base_dir / "keyframes" / sanitized_source_title / strategy

            relative_frame_paths_dict = _extract_and_save_frames_logic(
                abs_clip_path,
                clip_identifier,
                source_title,
                output_dir_abs,
                strategy,
                media_base_dir,
            )
            if not relative_frame_paths_dict:
                raise RuntimeError(
                    f"Keyframe extraction logic failed to produce paths for clip {clip_id}."
                )

            # 5. Pick representative frame
            representative_key = None
            if strategy == "multi":
                if "50pct" in relative_frame_paths_dict:
                    representative_key = "50pct"
                elif "mid" in relative_frame_paths_dict:
                    representative_key = "mid"
                else:
                    representative_key = next(iter(relative_frame_paths_dict), None)
            elif strategy == "midpoint":
                representative_key = "mid"

            if representative_key and representative_key in relative_frame_paths_dict:
                representative_rel_path = relative_frame_paths_dict[representative_key]
                logger.info(f"Representative keyframe path: {representative_rel_path}")
            else:
                raise ValueError("Could not determine representative keyframe path.")

            # 6. Update DB on success
            logger.info("Updating database record with keyframe details...")
            cur.execute(
                """
                UPDATE clips
                SET ingest_state      = 'keyframed',
                    keyframe_filepath = %s,
                    keyframed_at      = NOW(),
                    updated_at        = NOW(),
                    last_error        = NULL
                WHERE id = %s
                """,
                (representative_rel_path, clip_id),
            )

            conn.commit()
            logger.info(
                f"TASK [Keyframe]: Successfully processed clip_id: {clip_id}. "
                f"Representative frame: {representative_rel_path}"
            )

            return {
                "status": "success",
                "representative_keyframe": representative_rel_path,
                "all_frames": relative_frame_paths_dict,
            }

    # -------------------------- Error handling ------------------------------ #
    except Exception as e:
        task_name = "Keyframe"
        logger.error(
            f"TASK FAILED ({task_name}): clip_id {clip_id} - {e}", exc_info=True
        )
        if conn:
            try:
                conn.rollback()
                logger.info("Database transaction rolled back.")
                conn.autocommit = True
                with conn.cursor() as err_cur:
                    err_cur.execute(
                        """
                        UPDATE clips
                        SET ingest_state   = 'keyframe_failed',
                            last_error     = %s,
                            retry_count    = retry_count + 1,
                            updated_at     = NOW()
                        WHERE id = %s
                        """,
                        (f"{type(e).__name__}: {str(e)[:500]}", clip_id),
                    )
                logger.info(f"Updated DB state to 'keyframe_failed' for clip_id: {clip_id}")
            except Exception as db_err:
                logger.error(
                    f"Failed to update error state in DB after rollback: {db_err}"
                )
        raise

    # ---------------------------- Cleanup ----------------------------------- #
    finally:
        if conn:
            conn.autocommit = True
            conn.close()
            logger.debug(f"DB connection closed for clip_id: {clip_id}")
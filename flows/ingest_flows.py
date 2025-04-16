import sys
import os
from pathlib import Path
import traceback # Added for better error logging if imports fail

# Add project root to the Python path
# This assumes the script is in 'flows' directory, one level below project root 'src'
project_root = Path(__file__).parent.parent.resolve()
if str(project_root) not in sys.path:
    print(f"Adding project root to sys.path: {project_root}")
    sys.path.insert(0, str(project_root))

# Now imports should work relative to the project root
from prefect import flow, get_run_logger
# Removed deployment/schedule imports from here, they are handled by prefect.yaml
# from prefect.server.schemas.schedules import IntervalSchedule
# from datetime import timedelta

# Import tasks from their specific modules within the 'tasks' directory
# Import DB helpers from the 'utils' directory
try:
    # Task Imports - UPDATED
    from tasks.intake import intake_task # Renamed task import
    from tasks.splice import splice_video_task
    from tasks.keyframe import extract_keyframes_task
    from tasks.embed import generate_embeddings_task

    # Utility Imports
    from utils.db_utils import get_items_by_state, get_source_input_from_db

except ImportError as e:
     # Handle potential path issues during development
     print(f"ERROR Could not import required modules: {e}")
     print(f"Project root added to path was: {project_root}")
     print("Ensure tasks/*.py and utils/db_utils.py exist and are importable.")
     print("Traceback:")
     traceback.print_exc() # Print full traceback for import errors
     print("Defining dummy functions to allow script loading for deployment definition...")
     # Define dummy functions if needed for script to load without crashing
     def intake_task(**kwargs): raise NotImplementedError("Dummy Task: intake_task")
     def splice_video_task(**kwargs): raise NotImplementedError("Dummy Task: splice_video_task")
     def extract_keyframes_task(**kwargs): raise NotImplementedError("Dummy Task: extract_keyframes_task")
     def generate_embeddings_task(**kwargs): raise NotImplementedError("Dummy Task: generate_embeddings_task")
     def get_items_by_state(table, state): print(f"Dummy get_items_by_state({table}, {state}) called"); return []
     def get_source_input_from_db(sid): print(f"Dummy get_source_input_from_db({sid}) called"); return None


# --- Configuration (Ideally load from .env or config file) ---
DEFAULT_KEYFRAME_STRATEGY = os.getenv("DEFAULT_KEYFRAME_STRATEGY", "midpoint")
DEFAULT_EMBEDDING_MODEL = os.getenv("DEFAULT_EMBEDDING_MODEL", "openai/clip-vit-base-patch32")
# Derive the label used for the embedding generation based on the keyframe strategy
DEFAULT_EMBEDDING_STRATEGY_LABEL = f"keyframe_{DEFAULT_KEYFRAME_STRATEGY}"

# --- Flows ---

@flow(log_prints=True)
def process_clip_post_review(
    clip_id: int,
    keyframe_strategy: str = DEFAULT_KEYFRAME_STRATEGY,
    model_name: str = DEFAULT_EMBEDDING_MODEL
    ):
    """
    Processes a single clip after it has passed manual review.
    This flow handles keyframing and subsequent embedding for the approved clip.
    """
    logger = get_run_logger()
    logger.info(f"FLOW: Starting post-review processing for approved clip_id: {clip_id}")

    # --- 1. Keyframing ---
    # Need to ensure extract_keyframes_task is correctly implemented in tasks/keyframe.py
    logger.info(f"Submitting keyframe task for clip_id: {clip_id} with strategy: {keyframe_strategy}")
    try:
        keyframe_future = extract_keyframes_task.submit(clip_id=clip_id, strategy=keyframe_strategy)
        # .wait() is generally discouraged in scheduled flows if it can block long;
        # consider making embedding dependent on keyframing completion via Prefect events/states
        # if keyframing takes a very long time. For now, wait() is okay for simplicity.
        keyframe_state = keyframe_future.wait()

        if keyframe_state and keyframe_state.is_completed():
            logger.info(f"Keyframing task completed successfully for clip_id: {clip_id}. Proceeding to embedding.")

            # --- 2. Embedding ---
            # Need to ensure generate_embeddings_task is correctly implemented in tasks/embed.py
            embedding_strategy_label = f"keyframe_{keyframe_strategy}"
            logger.info(f"Submitting embedding task for clip_id: {clip_id} with model: {model_name}, strategy_label: {embedding_strategy_label}")
            embed_future = generate_embeddings_task.submit(
                clip_id=clip_id,
                model_name=model_name,
                generation_strategy=embedding_strategy_label
            )
            embed_state = embed_future.wait() # Wait for embedding to finish as well
            if embed_state and embed_state.is_completed():
                 logger.info(f"Embedding task completed successfully for clip_id: {clip_id}.")
            else:
                 logger.warning(f"Embedding task for clip_id: {clip_id} did not complete successfully (State: {embed_state.type.value if embed_state else 'Unknown'}).")

        else:
             logger.warning(f"Keyframing task for clip_id: {clip_id} did not complete successfully (State: {keyframe_state.type.value if keyframe_state else 'Unknown'}). Skipping embedding.")
    except Exception as e:
         logger.error(f"Error during post-review processing flow for clip_id {clip_id}: {e}", exc_info=True)
         # Depending on the error, may want to update clip state to failed

    logger.info(f"FLOW: Finished post-review processing flow for clip_id: {clip_id}")


@flow(name="Scheduled Ingest Initiator", log_prints=True)
def scheduled_ingest_initiator():
    """
    Scheduled flow to find new work at different stages and trigger the appropriate next tasks.
    This acts as the main heartbeat for progressing the ingest pipeline automatically.
    """
    logger = get_run_logger()
    logger.info("FLOW: Running Scheduled Ingest Initiator cycle...")
    error_count = 0

    # --- Stage 1: Find New Source Videos ---
    try:
        new_source_ids = get_items_by_state(table="source_videos", state="new")
        if new_source_ids:
            logger.info(f"Found {len(new_source_ids)} new source videos. Submitting intake tasks...")
            for sid in new_source_ids:
                try:
                    logger.info(f"Attempting to trigger intake_task for source_id: {sid}")
                    source_input = get_source_input_from_db(sid)
                    if source_input:
                        intake_task.submit(source_video_id=sid, input_source=source_input)
                        logger.info(f"Submitted intake_task for source_id: {sid} with input: '{source_input[:50]}...'")
                    else:
                        logger.error(f"Could not find input source (e.g., original_url) in DB for new source_video_id: {sid}. Cannot submit intake task.")
                        # Consider adding logic to update DB state to failed here if desired
                        error_count += 1
                except Exception as task_submit_err:
                     logger.error(f"Failed to submit intake_task for source_id {sid}: {task_submit_err}", exc_info=True)
                     error_count += 1
    except Exception as db_query_err:
         logger.error(f"Failed to query for 'new' source videos: {db_query_err}", exc_info=True)
         error_count += 1


    # --- Stage 2: Find Downloaded Source Videos Ready for Splicing ---
    try:
        downloaded_source_ids = get_items_by_state(table="source_videos", state="downloaded")
        if downloaded_source_ids:
            logger.info(f"Found {len(downloaded_source_ids)} downloaded sources ready for splicing. Submitting splice tasks...")
            for sid in downloaded_source_ids:
                try:
                    logger.info(f"Submitting splice_video_task for source_id: {sid}")
                    splice_video_task.submit(source_video_id=sid)
                    logger.info(f"Submitted splice_video_task for source_id: {sid}")
                except Exception as task_submit_err:
                     logger.error(f"Failed to submit splice_video_task for source_id {sid}: {task_submit_err}", exc_info=True)
                     error_count += 1
    except Exception as db_query_err:
         logger.error(f"Failed to query for 'downloaded' source videos: {db_query_err}", exc_info=True)
         error_count += 1


    # --- Stage 3: Find Clips Approved by Manual Review ---
    try:
        clips_ready_for_keyframes = get_items_by_state(table="clips", state="review_approved")
        if clips_ready_for_keyframes:
            logger.info(f"Found {len(clips_ready_for_keyframes)} clips approved by review. Submitting post-review processing flow...")
            for cid in clips_ready_for_keyframes:
                try:
                    logger.info(f"Submitting process_clip_post_review flow for clip_id: {cid}")
                    # Submit the sub-flow. It will run independently.
                    process_clip_post_review.submit(clip_id=cid)
                    logger.info(f"Submitted process_clip_post_review flow for clip_id: {cid}")
                except Exception as task_submit_err:
                     logger.error(f"Failed to submit process_clip_post_review flow for clip_id {cid}: {task_submit_err}", exc_info=True)
                     error_count += 1
    except Exception as db_query_err:
         logger.error(f"Failed to query for 'review_approved' clips: {db_query_err}", exc_info=True)
         error_count += 1


    if error_count > 0:
         logger.warning(f"FLOW: Scheduled Ingest Initiator cycle completed with {error_count} error(s).")
    else:
         logger.info("FLOW: Scheduled Ingest Initiator cycle complete.")


# --- Deployment Definition Removed ---
# The deployment is now handled by prefect.yaml and `prefect deploy`

if __name__ == "__main__":
    # This block now primarily serves for direct local testing of one cycle,
    # or potentially other script-like actions related to these flows.
    # The actual deployment registration happens via `prefect deploy`.

    print("Running one cycle of scheduled_ingest_initiator for local testing...")
    # Note: This runs the flow synchronously *in this process*.
    # It does NOT use the scheduler or worker queue. Useful for quick debugging.
    # Submitted tasks *will* still go to the queue if a worker is running.
    try:
        scheduled_ingest_initiator()
    except Exception as e:
        print(f"\nError during local test run: {e}")
        traceback.print_exc()
    print("Local test cycle finished.")
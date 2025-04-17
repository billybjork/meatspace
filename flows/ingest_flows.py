import sys
import os
from pathlib import Path
import traceback
import time

project_root = Path(__file__).parent.parent.resolve()
if str(project_root) not in sys.path:
    print(f"Adding project root to sys.path: {project_root}")
    sys.path.insert(0, str(project_root))

from prefect import flow, get_run_logger, task # Added task decorator for potential future use

# --- Task Imports ---
from tasks.intake import intake_task
from tasks.splice import splice_video_task
from tasks.keyframe import extract_keyframes_task
from tasks.embed import generate_embeddings_task
from tasks.editing import merge_clips_task, resplice_clip_task

# --- DB Util Imports ---
from utils.db_utils import (
    get_items_by_state,
    get_source_input_from_db,
    get_pending_merge_pairs,
)

# --- Configuration ---
DEFAULT_KEYFRAME_STRATEGY = os.getenv("DEFAULT_KEYFRAME_STRATEGY", "midpoint")
DEFAULT_EMBEDDING_MODEL = os.getenv("DEFAULT_EMBEDDING_MODEL", "openai/clip-vit-base-patch32")
# Derive the label used for the embedding generation based on the keyframe strategy
DEFAULT_EMBEDDING_STRATEGY_LABEL = f"keyframe_{DEFAULT_KEYFRAME_STRATEGY}"
# Delay between submitting batches of tasks to avoid overwhelming DB/API
TASK_SUBMIT_DELAY = float(os.getenv("TASK_SUBMIT_DELAY", 0.1))


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
    logger.info(f"Submitting keyframe task for clip_id: {clip_id} with strategy: {keyframe_strategy}")
    try:
        # Submit keyframing task
        keyframe_future = extract_keyframes_task.submit(clip_id=clip_id, strategy=keyframe_strategy)

        # Wait for keyframing to complete before proceeding to embedding
        keyframe_state = keyframe_future.wait(timeout=300) # Add a reasonable timeout

        if keyframe_state and keyframe_state.is_completed():
            logger.info(f"Keyframing task completed successfully for clip_id: {clip_id}. Proceeding to embedding.")

            # --- 2. Embedding ---
            embedding_strategy_label = f"keyframe_{keyframe_strategy}"
            logger.info(f"Submitting embedding task for clip_id: {clip_id} with model: {model_name}, strategy_label: {embedding_strategy_label}")
            # Submit embedding task
            embed_future = generate_embeddings_task.submit(
                clip_id=clip_id,
                model_name=model_name,
                generation_strategy=embedding_strategy_label
            )
            # Wait for embedding to finish
            embed_state = embed_future.wait(timeout=600) # Potentially longer timeout for embedding
            if embed_state and embed_state.is_completed():
                 logger.info(f"Embedding task completed successfully for clip_id: {clip_id}.")
            elif embed_state:
                 logger.warning(f"Embedding task for clip_id: {clip_id} did not complete successfully (State: {embed_state.type.value}).")
            else:
                 logger.warning(f"Embedding task for clip_id: {clip_id} timed out or failed to return state.")

        elif keyframe_state:
             logger.warning(f"Keyframing task for clip_id: {clip_id} did not complete successfully (State: {keyframe_state.type.value}). Skipping embedding.")
        else:
             logger.warning(f"Keyframing task for clip_id: {clip_id} timed out or failed to return state. Skipping embedding.")

    except Exception as e:
         logger.error(f"Error during post-review processing flow for clip_id {clip_id}: {e}", exc_info=True)
         # Depending on the error, may want to update clip state to failed using a separate task/call

    logger.info(f"FLOW: Finished post-review processing flow for clip_id: {clip_id}")


@flow(name="Scheduled Ingest Initiator", log_prints=True)
def scheduled_ingest_initiator():
    """
    Scheduled flow to find new work at different stages and trigger the appropriate next tasks/flows.
    This acts as the main heartbeat for progressing the ingest pipeline automatically.
    """
    logger = get_run_logger()
    logger.info("FLOW: Running Scheduled Ingest Initiator cycle...")
    error_count = 0
    processed_counts = {}

    # --- Stage 1: Find New Source Videos -> Submit Intake ---
    stage_name = "Intake"
    try:
        new_source_ids = get_items_by_state(table="source_videos", state="new")
        processed_counts[stage_name] = len(new_source_ids)
        if new_source_ids:
            logger.info(f"[{stage_name}] Found {len(new_source_ids)} new source videos. Submitting intake tasks...")
            for sid in new_source_ids:
                try:
                    source_input = get_source_input_from_db(sid)
                    if source_input:
                        intake_task.submit(source_video_id=sid, input_source=source_input)
                        logger.debug(f"[{stage_name}] Submitted intake_task for source_id: {sid}")
                        time.sleep(TASK_SUBMIT_DELAY) # Small delay
                    else:
                        logger.error(f"[{stage_name}] Could not find input source for new source_video_id: {sid}. Cannot submit intake task.")
                        error_count += 1
                except Exception as task_submit_err:
                     logger.error(f"[{stage_name}] Failed to submit intake_task for source_id {sid}: {task_submit_err}", exc_info=True)
                     error_count += 1
    except Exception as db_query_err:
         logger.error(f"[{stage_name}] Failed to query for 'new' source videos: {db_query_err}", exc_info=True)
         error_count += 1

    # --- Stage 2: Find Downloaded Sources -> Submit Splice ---
    stage_name = "Splice"
    try:
        downloaded_source_ids = get_items_by_state(table="source_videos", state="downloaded")
        processed_counts[stage_name] = len(downloaded_source_ids)
        if downloaded_source_ids:
            logger.info(f"[{stage_name}] Found {len(downloaded_source_ids)} downloaded sources ready for splicing. Submitting splice tasks...")
            for sid in downloaded_source_ids:
                try:
                    splice_video_task.submit(source_video_id=sid)
                    logger.debug(f"[{stage_name}] Submitted splice_video_task for source_id: {sid}")
                    time.sleep(TASK_SUBMIT_DELAY)
                except Exception as task_submit_err:
                     logger.error(f"[{stage_name}] Failed to submit splice_video_task for source_id {sid}: {task_submit_err}", exc_info=True)
                     error_count += 1
    except Exception as db_query_err:
         logger.error(f"[{stage_name}] Failed to query for 'downloaded' source videos: {db_query_err}", exc_info=True)
         error_count += 1

    # --- Stage 3: Find Approved Clips -> Initiate Post-Review Flow ---
    stage_name = "Post-Review (Keyframe/Embed)"
    try:
        clips_ready_for_keyframes = get_items_by_state(table="clips", state="review_approved")
        processed_counts[stage_name] = len(clips_ready_for_keyframes)
        if clips_ready_for_keyframes:
            logger.info(f"[{stage_name}] Found {len(clips_ready_for_keyframes)} clips approved by review. Initiating post-review processing flows...")
            for cid in clips_ready_for_keyframes:
                try:
                    # Call the flow directly to create a sub-flow run
                    process_clip_post_review(clip_id=cid) # <-- THE FIX: Call flow directly
                    logger.debug(f"[{stage_name}] Initiated process_clip_post_review sub-flow for clip_id: {cid}") # Updated log message
                    time.sleep(TASK_SUBMIT_DELAY) # Keep delay for staggering initiation
                except Exception as flow_call_err: # Renamed variable for clarity
                     logger.error(f"[{stage_name}] Failed to initiate process_clip_post_review flow for clip_id {cid}: {flow_call_err}", exc_info=True)
                     error_count += 1
    except Exception as db_query_err:
         logger.error(f"[{stage_name}] Failed to query for 'review_approved' clips: {db_query_err}", exc_info=True)
         error_count += 1

    # --- Stage 4: Find Clips Pending Merge -> Submit Merge Task ---
    stage_name = "Merge"
    try:
        # Use the new helper function from db_utils
        merge_pairs = get_pending_merge_pairs() # Expects list of tuples [(id1, id2), ...]
        processed_counts[stage_name] = len(merge_pairs)
        if merge_pairs:
             logger.info(f"[{stage_name}] Found {len(merge_pairs)} clip pairs marked for merging. Submitting merge tasks...")
             submitted_merges = set() # Prevent submitting same clip twice if query returns duplicates
             for cid1, cid2 in merge_pairs:
                  if cid1 not in submitted_merges and cid2 not in submitted_merges:
                      try:
                          merge_clips_task.submit(clip_id_1=cid1, clip_id_2=cid2)
                          submitted_merges.add(cid1)
                          submitted_merges.add(cid2)
                          logger.debug(f"[{stage_name}] Submitted merge_clips_task for pair: ({cid1}, {cid2})")
                          time.sleep(TASK_SUBMIT_DELAY)
                      except Exception as task_submit_err:
                           logger.error(f"[{stage_name}] Failed to submit merge_clips_task for pair ({cid1}, {cid2}): {task_submit_err}", exc_info=True)
                           error_count += 1
                  else:
                      logger.warning(f"[{stage_name}] Skipping duplicate merge submission involving clips {cid1} or {cid2}")

    except Exception as db_query_err:
         # Catch errors from the helper function or submission loop
         logger.error(f"[{stage_name}] Failed during merge check/submission: {db_query_err}", exc_info=True)
         error_count += 1

    # --- Stage 5: Re-Splice ---
    stage_name = "Re-Splice"
    try:
        # Use the standard get_items_by_state helper
        clips_to_resplice = get_items_by_state(table="clips", state="pending_resplice")
        processed_counts[stage_name] = len(clips_to_resplice)
        if clips_to_resplice:
            logger.info(f"[{stage_name}] Found {len(clips_to_resplice)} clips marked for re-splicing. Submitting re-splice tasks...")
            for cid in clips_to_resplice:
                try:
                    # Submit the new resplice task
                    resplice_clip_task.submit(clip_id=cid)
                    logger.debug(f"[{stage_name}] Submitted resplice_clip_task for original clip_id: {cid}")
                    time.sleep(TASK_SUBMIT_DELAY)
                except Exception as task_submit_err:
                     logger.error(f"[{stage_name}] Failed to submit resplice_clip_task for clip_id {cid}: {task_submit_err}", exc_info=True)
                     error_count += 1
    except Exception as db_query_err:
         logger.error(f"[{stage_name}] Failed during re-splice check/submission: {db_query_err}", exc_info=True)
         error_count += 1


    # --- Completion Logging ---
    summary_log = f"FLOW: Scheduled Ingest Initiator cycle complete. Processed counts: {processed_counts}."
    if error_count > 0:
         logger.warning(f"{summary_log} Completed with {error_count} error(s).")
    else:
         logger.info(summary_log)

if __name__ == "__main__":
    # This block now primarily serves for direct local testing of one cycle,
    # or potentially other script-like actions related to these flows.
    # The actual deployment registration happens via `prefect deploy`.

    print("Running one cycle of scheduled_ingest_initiator for local testing...")
    # Note: This runs the flow synchronously *in this process*.
    # It does NOT use the scheduler or worker queue. Useful for quick debugging.
    # Submitted tasks *will* still go to the queue if a worker is running.
    try:
        # Ensure dummy functions are sufficient if tasks/utils are missing
        scheduled_ingest_initiator()
    except Exception as e:
        print(f"\nError during local test run: {e}")
        traceback.print_exc()
    print("Local test cycle finished.")
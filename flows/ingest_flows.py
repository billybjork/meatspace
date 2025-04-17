import sys
import os
from pathlib import Path
import traceback
import time

project_root = Path(__file__).parent.parent.resolve()
if str(project_root) not in sys.path:
    print(f"Adding project root to sys.path: {project_root}")
    sys.path.insert(0, str(project_root))

from prefect import flow, get_run_logger, task

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
DEFAULT_EMBEDDING_STRATEGY_LABEL = f"keyframe_{DEFAULT_KEYFRAME_STRATEGY}"
TASK_SUBMIT_DELAY = float(os.getenv("TASK_SUBMIT_DELAY", 0.1))
# Add timeouts for result() calls - adjust as needed
KEYFRAME_TIMEOUT = int(os.getenv("KEYFRAME_TIMEOUT", 600)) # 10 minutes default
EMBEDDING_TIMEOUT = int(os.getenv("EMBEDDING_TIMEOUT", 900)) # 15 minutes default


# --- Flows ---

@flow(log_prints=True)
def process_clip_post_review(
    clip_id: int,
    keyframe_strategy: str = DEFAULT_KEYFRAME_STRATEGY,
    model_name: str = DEFAULT_EMBEDDING_MODEL
    ):
    """
    Processes a single clip after it has passed manual review.
    Handles keyframing and subsequent embedding for the approved clip.
    Uses .result() for task chaining.
    """
    logger = get_run_logger()
    logger.info(f"FLOW: Starting post-review processing for approved clip_id: {clip_id}")

    try:
        # --- 1. Keyframing ---
        logger.info(f"Submitting keyframe task for clip_id: {clip_id} with strategy: {keyframe_strategy}")
        keyframe_future = extract_keyframes_task.submit(clip_id=clip_id, strategy=keyframe_strategy)

        # Get the result, blocking until completion or failure within timeout
        logger.info(f"Waiting for keyframe task result (timeout: {KEYFRAME_TIMEOUT}s)...")
        keyframe_result = keyframe_future.result(timeout=KEYFRAME_TIMEOUT) # Use .result()

        # If .result() returns without error, the task succeeded.
        logger.info(f"Keyframing task completed successfully for clip_id: {clip_id}. Result: {keyframe_result}. Proceeding to embedding.")

        # --- 2. Embedding ---
        embedding_strategy_label = f"keyframe_{keyframe_strategy}"
        logger.info(f"Submitting embedding task for clip_id: {clip_id} with model: {model_name}, strategy_label: {embedding_strategy_label}")
        embed_future = generate_embeddings_task.submit(
            clip_id=clip_id,
            model_name=model_name,
            generation_strategy=embedding_strategy_label
        )

        # Get the result, blocking until completion or failure within timeout
        logger.info(f"Waiting for embedding task result (timeout: {EMBEDDING_TIMEOUT}s)...")
        embed_result = embed_future.result(timeout=EMBEDDING_TIMEOUT) # Use .result()

        # If .result() returns without error, the task succeeded.
        logger.info(f"Embedding task completed successfully for clip_id: {clip_id}. Result: {embed_result}.")

    except Exception as e:
         # This will catch exceptions from .result() if the tasks fail or time out
         logger.error(f"Error during post-review processing flow for clip_id {clip_id}: {e}", exc_info=True)
         # Optional: Add logic here to update the clip's DB state to reflect the failure stage
         # (e.g., check if keyframe_result exists to know if error was during embedding)
         # You might need a separate task or direct DB call here. For now, just log.

    logger.info(f"FLOW: Finished post-review processing flow for clip_id: {clip_id}")


# --- scheduled_ingest_initiator Flow ---
# No changes needed here based on the specific error, but be aware of the
# "future was garbage collected" warnings. If they become problematic or hide
# actual errors, you might consider collecting futures in lists within the loop
# and potentially using prefect.utilities.waiters.wait_for_task_runs if you need
# to ensure all submitted tasks reach a certain state before the initiator finishes.
# For now, leaving it as is should be fine.
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
    futures = {"intake": [], "splice": [], "merge": [], "resplice": []} # Optional: store futures

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
                        future = intake_task.submit(source_video_id=sid, input_source=source_input)
                        futures["intake"].append(future) # Optional: track future
                        logger.debug(f"[{stage_name}] Submitted intake_task for source_id: {sid}")
                        time.sleep(TASK_SUBMIT_DELAY)
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
                    future = splice_video_task.submit(source_video_id=sid)
                    futures["splice"].append(future) # Optional: track future
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
                    # Call the flow directly - Prefect handles its execution.
                    # No future object is returned here to wait on *within this flow*.
                    process_clip_post_review(clip_id=cid)
                    logger.debug(f"[{stage_name}] Initiated process_clip_post_review sub-flow for clip_id: {cid}")
                    time.sleep(TASK_SUBMIT_DELAY)
                except Exception as flow_call_err:
                     logger.error(f"[{stage_name}] Failed to initiate process_clip_post_review flow for clip_id {cid}: {flow_call_err}", exc_info=True)
                     error_count += 1
    except Exception as db_query_err:
         logger.error(f"[{stage_name}] Failed to query for 'review_approved' clips: {db_query_err}", exc_info=True)
         error_count += 1

    # --- Stage 4: Find Clips Pending Merge -> Submit Merge Task ---
    stage_name = "Merge"
    try:
        merge_pairs = get_pending_merge_pairs()
        processed_counts[stage_name] = len(merge_pairs)
        if merge_pairs:
             logger.info(f"[{stage_name}] Found {len(merge_pairs)} clip pairs marked for merging. Submitting merge tasks...")
             submitted_merges = set()
             for cid1, cid2 in merge_pairs:
                  if cid1 not in submitted_merges and cid2 not in submitted_merges:
                      try:
                          future = merge_clips_task.submit(clip_id_1=cid1, clip_id_2=cid2)
                          futures["merge"].append(future) # Optional: track future
                          submitted_merges.add(cid1); submitted_merges.add(cid2)
                          logger.debug(f"[{stage_name}] Submitted merge_clips_task for pair: ({cid1}, {cid2})")
                          time.sleep(TASK_SUBMIT_DELAY)
                      except Exception as task_submit_err:
                           logger.error(f"[{stage_name}] Failed to submit merge_clips_task for pair ({cid1}, {cid2}): {task_submit_err}", exc_info=True)
                           error_count += 1
                  else: logger.warning(f"[{stage_name}] Skipping duplicate merge submission involving clips {cid1} or {cid2}")
    except Exception as db_query_err:
         logger.error(f"[{stage_name}] Failed during merge check/submission: {db_query_err}", exc_info=True)
         error_count += 1

    # --- Stage 5: Re-Splice ---
    stage_name = "Re-Splice"
    try:
        clips_to_resplice = get_items_by_state(table="clips", state="pending_resplice")
        processed_counts[stage_name] = len(clips_to_resplice)
        if clips_to_resplice:
            logger.info(f"[{stage_name}] Found {len(clips_to_resplice)} clips marked for re-splicing. Submitting re-splice tasks...")
            for cid in clips_to_resplice:
                try:
                    future = resplice_clip_task.submit(clip_id=cid)
                    futures["resplice"].append(future) # Optional: track future
                    logger.debug(f"[{stage_name}] Submitted resplice_clip_task for original clip_id: {cid}")
                    time.sleep(TASK_SUBMIT_DELAY)
                except Exception as task_submit_err:
                     logger.error(f"[{stage_name}] Failed to submit resplice_clip_task for clip_id {cid}: {task_submit_err}", exc_info=True)
                     error_count += 1
    except Exception as db_query_err:
         logger.error(f"[{stage_name}] Failed during re-splice check/submission: {db_query_err}", exc_info=True)
         error_count += 1

    # --- Optional: Wait for submitted tasks if needed, or just log completion ---
    # If you need to ensure all tasks *submitted* in this cycle finish before the
    # initiator flow itself finishes, you could add waiting logic here.
    # Example (simple approach, adjust as needed):
    # for stage, fut_list in futures.items():
    #     for fut in fut_list:
    #         try:
    #             fut.wait(timeout=1) # Brief wait to allow state propagation
    #         except Exception:
    #             pass # Ignore errors here, just trying to prevent GC warning

    # --- Completion Logging ---
    summary_log = f"FLOW: Scheduled Ingest Initiator cycle complete. Processed counts: {processed_counts}."
    if error_count > 0:
         logger.warning(f"{summary_log} Completed with {error_count} error(s).")
    else:
         logger.info(summary_log)


if __name__ == "__main__":
    print("Running one cycle of scheduled_ingest_initiator for local testing...")
    try:
        scheduled_ingest_initiator()
    except Exception as e:
        print(f"\nError during local test run: {e}")
        traceback.print_exc()
    print("Local test cycle finished.")
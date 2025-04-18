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
from tasks.editing import merge_clips_task, split_clip_task

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
    Processes a single clip AFTER it has passed manual review (state='review_approved').
    Handles keyframing AND subsequent embedding for the approved clip.
    Uses .result() for task chaining.
    """
    logger = get_run_logger()
    logger.info(f"FLOW: Starting post-review processing for approved clip_id: {clip_id}")
    keyframe_task_succeeded = False # Flag to know if keyframing finished

    try:
        # --- 1. Keyframing ---
        logger.info(f"Submitting keyframe task for clip_id: {clip_id} with strategy: {keyframe_strategy}")
        keyframe_future = extract_keyframes_task.submit(clip_id=clip_id, strategy=keyframe_strategy)

        logger.info(f"Waiting for keyframe task result (timeout: {KEYFRAME_TIMEOUT}s)...")
        keyframe_result = keyframe_future.result(timeout=KEYFRAME_TIMEOUT)
        logger.info(f"Keyframing task completed successfully for clip_id: {clip_id}. Result: {keyframe_result}.")
        keyframe_task_succeeded = True # Mark as succeeded

        # --- 2. Embedding (Only if keyframing succeeded) ---
        embedding_strategy_label = f"keyframe_{keyframe_strategy}"
        logger.info(f"Submitting embedding task for clip_id: {clip_id} with model: {model_name}, strategy_label: {embedding_strategy_label}")
        embed_future = generate_embeddings_task.submit(
            clip_id=clip_id,
            model_name=model_name,
            generation_strategy=embedding_strategy_label
        )

        logger.info(f"Waiting for embedding task result (timeout: {EMBEDDING_TIMEOUT}s)...")
        embed_result = embed_future.result(timeout=EMBEDDING_TIMEOUT)
        logger.info(f"Embedding task completed successfully for clip_id: {clip_id}. Result: {embed_result}.")

    except Exception as e:
         # Log error and potentially update state based on where it failed
         stage = "embedding" if keyframe_task_succeeded else "keyframing"
         logger.error(f"Error during post-review processing flow (stage: {stage}) for clip_id {clip_id}: {e}", exc_info=True)
         # Consider adding DB update to set state to keyframing_failed or embedding_failed here

    logger.info(f"FLOW: Finished post-review processing flow for clip_id: {clip_id}")

@flow(name="Scheduled Ingest Initiator", log_prints=True)
def scheduled_ingest_initiator():
    """
    Scheduled flow to find new work at different stages and trigger the appropriate next tasks/flows.
    """
    logger = get_run_logger()
    logger.info("FLOW: Running Scheduled Ingest Initiator cycle...")
    error_count = 0
    processed_counts = {}
    futures = {"intake": [], "splice": [], "merge": [], "resplice": [], "embed": []}

    # --- Stage 1: Intake ---
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
                        futures["intake"].append(future)
                        logger.debug(f"[{stage_name}] Submitted intake_task for source_id: {sid}")
                        time.sleep(TASK_SUBMIT_DELAY)
                    else: logger.error(f"[{stage_name}] Could not find input source for new source_video_id: {sid}."); error_count += 1
                except Exception as task_submit_err: logger.error(f"[{stage_name}] Failed to submit intake_task for source_id {sid}: {task_submit_err}", exc_info=True); error_count += 1
    except Exception as db_query_err: logger.error(f"[{stage_name}] Failed to query for 'new' source videos: {db_query_err}", exc_info=True); error_count += 1

    # --- Stage 2: Splice ---
    stage_name = "Splice"
    try:
        downloaded_source_ids = get_items_by_state(table="source_videos", state="downloaded")
        processed_counts[stage_name] = len(downloaded_source_ids)
        if downloaded_source_ids:
            logger.info(f"[{stage_name}] Found {len(downloaded_source_ids)} downloaded sources ready for splicing. Submitting splice tasks...")
            for sid in downloaded_source_ids:
                try:
                    future = splice_video_task.submit(source_video_id=sid)
                    futures["splice"].append(future)
                    logger.debug(f"[{stage_name}] Submitted splice_video_task for source_id: {sid}")
                    time.sleep(TASK_SUBMIT_DELAY)
                except Exception as task_submit_err: logger.error(f"[{stage_name}] Failed to submit splice_video_task for source_id {sid}: {task_submit_err}", exc_info=True); error_count += 1
    except Exception as db_query_err: logger.error(f"[{stage_name}] Failed to query for 'downloaded' source videos: {db_query_err}", exc_info=True); error_count += 1

    # --- Stage 3: Find Approved Clips -> Initiate FULL Post-Review Flow ---
    # This stage kicks off the process_clip_post_review flow which handles BOTH keyframing and embedding.
    stage_name = "Post-Review Start"
    try:
        clips_to_process = get_items_by_state(table="clips", state="review_approved")
        processed_counts[stage_name] = len(clips_to_process)
        if clips_to_process:
            logger.info(f"[{stage_name}] Found {len(clips_to_process)} clips approved by review. Initiating post-review processing flows...")
            for cid in clips_to_process:
                try:
                    # This flow will handle keyframing AND embedding internally
                    process_clip_post_review(clip_id=cid) # Call flow directly
                    logger.debug(f"[{stage_name}] Initiated process_clip_post_review sub-flow for clip_id: {cid}")
                    time.sleep(TASK_SUBMIT_DELAY)
                except Exception as flow_call_err:
                     logger.error(f"[{stage_name}] Failed to initiate process_clip_post_review flow for clip_id {cid}: {flow_call_err}", exc_info=True)
                     error_count += 1
    except Exception as db_query_err:
         logger.error(f"[{stage_name}] Failed to query for 'review_approved' clips: {db_query_err}", exc_info=True)
         error_count += 1

    # --- Stage 3.5: Find KEYFRAMED Clips -> Submit Embedding Task ---
    # This stage specifically finds clips that finished keyframing
    # but might not have had their embedding started/completed yet.
    stage_name = "Embedding"
    try:
        clips_ready_for_embedding = get_items_by_state(table="clips", state="keyframed")
        processed_counts[stage_name] = len(clips_ready_for_embedding)
        if clips_ready_for_embedding:
            logger.info(f"[{stage_name}] Found {len(clips_ready_for_embedding)} keyframed clips ready for embedding. Submitting embedding tasks...")
            for cid in clips_ready_for_embedding:
                try:
                    # Directly submit only the embedding task
                    # Use default model/strategy defined at the top
                    future = generate_embeddings_task.submit(
                        clip_id=cid,
                        model_name=DEFAULT_EMBEDDING_MODEL,
                        generation_strategy=DEFAULT_EMBEDDING_STRATEGY_LABEL
                    )
                    futures["embed"].append(future) # Optional: track future
                    logger.debug(f"[{stage_name}] Submitted generate_embeddings_task for clip_id: {cid}")
                    time.sleep(TASK_SUBMIT_DELAY)
                except Exception as task_submit_err:
                     logger.error(f"[{stage_name}] Failed to submit generate_embeddings_task for clip_id {cid}: {task_submit_err}", exc_info=True)
                     error_count += 1
    except Exception as db_query_err:
         logger.error(f"[{stage_name}] Failed to query for 'keyframed' clips: {db_query_err}", exc_info=True)
         error_count += 1

    # --- Stage 4: Merge ---
    stage_name = "Merge"
    try:
        merge_pairs = get_pending_merge_pairs()
        processed_counts[stage_name] = len(merge_pairs)
        if merge_pairs:
             logger.info(f"[{stage_name}] Found {len(merge_pairs)} clip pairs for merging. Submitting merge tasks...")
             submitted_merges = set()
             for cid1, cid2 in merge_pairs:
                  if cid1 not in submitted_merges and cid2 not in submitted_merges:
                      try:
                          future = merge_clips_task.submit(clip_id_1=cid1, clip_id_2=cid2)
                          futures["merge"].append(future)
                          submitted_merges.add(cid1); submitted_merges.add(cid2)
                          logger.debug(f"[{stage_name}] Submitted merge_clips_task for pair: ({cid1}, {cid2})")
                          time.sleep(TASK_SUBMIT_DELAY)
                      except Exception as task_submit_err: logger.error(f"[{stage_name}] Failed to submit merge_clips_task for pair ({cid1}, {cid2}): {task_submit_err}", exc_info=True); error_count += 1
                  else: logger.warning(f"[{stage_name}] Skipping duplicate merge submission involving clips {cid1} or {cid2}")
    except Exception as db_query_err: logger.error(f"[{stage_name}] Failed during merge check/submission: {db_query_err}", exc_info=True); error_count += 1

    # --- Stage 5: Split ---
    stage_name = "Split"
    try:
        clips_to_split = get_items_by_state(table="clips", state="pending_split")
        processed_counts[stage_name] = len(clips_to_split)
        if clips_to_split:
            logger.info(f"[{stage_name}] Found {len(clips_to_split)} clips pending split. Submitting split tasks...")
            # Consider adding a future tracking list if needed: futures["split"] = []
            for cid in clips_to_split:
                try:
                    # Submit the new task
                    future = split_clip_task.submit(clip_id=cid)
                    # futures["split"].append(future) # Optional tracking
                    logger.debug(f"[{stage_name}] Submitted split_clip_task for original clip_id: {cid}")
                    time.sleep(TASK_SUBMIT_DELAY)
                except Exception as task_submit_err:
                     logger.error(f"[{stage_name}] Failed to submit split_clip_task for clip_id {cid}: {task_submit_err}", exc_info=True)
                     error_count += 1
    except Exception as db_query_err:
         logger.error(f"[{stage_name}] Failed during split check/submission: {db_query_err}", exc_info=True)
         error_count += 1

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
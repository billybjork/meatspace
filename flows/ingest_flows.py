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
from prefect.futures import wait

# --- Task Imports ---
from tasks.intake import intake_task
from tasks.splice import splice_video_task
from tasks.sprite_generator import generate_sprite_sheet_task
from tasks.keyframe import extract_keyframes_task
from tasks.embed import generate_embeddings_task
from tasks.merge import merge_clips_task
from tasks.split import split_clip_task

# --- DB Util Imports ---
from utils.db_utils import (
    get_items_for_processing,
    get_source_input_from_db,
    get_pending_merge_pairs,
    get_pending_split_jobs,
    initialize_db_pool,
    close_db_pool
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
    # State transition to processing within the flow start? Or rely on task?
    # Consider setting state to 'processing_post_review' here? Requires DB access.

    try:
        # --- 1. Keyframing ---
        logger.info(f"Submitting keyframe task for clip_id: {clip_id} with strategy: {keyframe_strategy}")
        # Ideally, keyframe_task itself sets state to 'keyframing'
        keyframe_future = extract_keyframes_task.submit(clip_id=clip_id, strategy=keyframe_strategy)

        logger.info(f"Waiting for keyframe task result (timeout: {KEYFRAME_TIMEOUT}s)...")
        keyframe_result = keyframe_future.result(timeout=KEYFRAME_TIMEOUT) # Wait for completion
        # Check result? Assumes task raises error on failure.
        logger.info(f"Keyframing task completed successfully for clip_id: {clip_id}.")
        keyframe_task_succeeded = True # Mark as succeeded

        # --- 2. Embedding (Only if keyframing succeeded) ---
        # State should be 'keyframed' now (set by keyframe_task)
        embedding_strategy_label = f"keyframe_{keyframe_strategy}"
        logger.info(f"Submitting embedding task for clip_id: {clip_id} with model: {model_name}, strategy_label: {embedding_strategy_label}")
        # Ideally, embedding_task sets state to 'embedding'
        embed_future = generate_embeddings_task.submit(
            clip_id=clip_id,
            model_name=model_name,
            generation_strategy=embedding_strategy_label
        )

        logger.info(f"Waiting for embedding task result (timeout: {EMBEDDING_TIMEOUT}s)...")
        embed_result = embed_future.result(timeout=EMBEDDING_TIMEOUT) # Wait for completion
        # State should be 'embedded' now (set by embedding_task)
        logger.info(f"Embedding task completed successfully for clip_id: {clip_id}.")

    except Exception as e:
         stage = "embedding" if keyframe_task_succeeded else "keyframing"
         logger.error(f"Error during post-review processing flow (stage: {stage}) for clip_id {clip_id}: {e}", exc_info=True)
         # State should have been set to 'keyframing_failed' or 'embedding_failed' by the failed task.
         # Re-raise to mark the flow run as failed? Or just log? Depends on desired behavior.
         # raise e # Optional: Propagate failure to the flow run state

    logger.info(f"FLOW: Finished post-review processing flow for clip_id: {clip_id}")


@flow(name="Scheduled Ingest Initiator", log_prints=True)
def scheduled_ingest_initiator():
    """
    Scheduled flow to find new work at different stages and trigger the appropriate next tasks/flows,
    avoiding items already being processed.
    """
    logger = get_run_logger()
    logger.info("FLOW: Running Scheduled Ingest Initiator cycle...")

    # Initialize DB Pool at the start of the flow run
    # This ensures tasks running within this flow context have the pool ready
    # Note: Tasks run in separate processes might need their own initialization if not inheriting state.
    # The get_db_connection lazy init should handle this for tasks now.
    # initialize_db_pool() # Call if needed, but lazy init in get_db_connection is likely sufficient

    error_count = 0
    processed_counts = {}
    # Optional: Track futures if you want to wait at the end
    # all_futures = []

    # Define processing states to exclude for each stage
    intake_processing_states = ['downloading', 'download_failed'] # Example
    splice_processing_states = ['splicing', 'splice_failed']
    sprite_gen_processing_states = ['generating_sprite', 'sprite_generation_failed'] # Include failed to prevent retrying failed jobs automatically here
    post_review_processing_states = ['keyframing', 'embedding', 'keyframing_failed', 'embedding_failed'] # States handled by the sub-flow
    embedding_processing_states = ['embedding', 'embedding_failed']
    merge_processing_states = ['merging', 'merge_failed']
    split_processing_states = ['splitting', 'split_failed']

    # --- Stage 1: Intake ---
    stage_name = "Intake"
    try:
        # Use get_items_for_processing
        new_source_ids = get_items_for_processing(table="source_videos", ready_state="new", processing_states=intake_processing_states)
        processed_counts[stage_name] = len(new_source_ids)
        if new_source_ids:
            logger.info(f"[{stage_name}] Found {len(new_source_ids)} sources. Submitting intake tasks...")
            for sid in new_source_ids:
                try:
                    source_input = get_source_input_from_db(sid)
                    if source_input:
                        # Submit task (fire-and-forget for now)
                        intake_task.submit(source_video_id=sid, input_source=source_input)
                        # all_futures.append(future) # Optional tracking
                        logger.debug(f"[{stage_name}] Submitted intake_task for source_id: {sid}")
                        time.sleep(TASK_SUBMIT_DELAY)
                    else: logger.error(f"[{stage_name}] Could not find input source for new source_video_id: {sid}."); error_count += 1
                except Exception as task_submit_err: logger.error(f"[{stage_name}] Failed to submit intake_task for source_id {sid}: {task_submit_err}", exc_info=True); error_count += 1
    except Exception as db_query_err: logger.error(f"[{stage_name}] Failed to query for 'new' source videos: {db_query_err}", exc_info=True); error_count += 1

    # --- Stage 2: Splice ---
    stage_name = "Splice"
    try:
        # Use get_items_for_processing
        downloaded_source_ids = get_items_for_processing(table="source_videos", ready_state="downloaded", processing_states=splice_processing_states)
        processed_counts[stage_name] = len(downloaded_source_ids)
        if downloaded_source_ids:
            logger.info(f"[{stage_name}] Found {len(downloaded_source_ids)} sources ready for splicing. Submitting splice tasks...")
            for sid in downloaded_source_ids:
                try:
                    # Submit task (fire-and-forget)
                    splice_video_task.submit(source_video_id=sid)
                    # all_futures.append(future) # Optional tracking
                    logger.debug(f"[{stage_name}] Submitted splice_video_task for source_id: {sid}")
                    time.sleep(TASK_SUBMIT_DELAY)
                except Exception as task_submit_err: logger.error(f"[{stage_name}] Failed to submit splice_video_task for source_id {sid}: {task_submit_err}", exc_info=True); error_count += 1
    except Exception as db_query_err: logger.error(f"[{stage_name}] Failed to query for 'downloaded' source videos: {db_query_err}", exc_info=True); error_count += 1

    # --- Stage 2.5: Sprite Sheet Generation ---
    stage_name = "SpriteGen"
    try:
        # Use get_items_for_processing
        clips_needing_sprites = get_items_for_processing(table="clips", ready_state="pending_sprite_generation", processing_states=sprite_gen_processing_states)
        processed_counts[stage_name] = len(clips_needing_sprites)
        if clips_needing_sprites:
            logger.info(f"[{stage_name}] Found {len(clips_needing_sprites)} clips needing sprites. Submitting generation tasks...")
            for cid in clips_needing_sprites:
                try:
                    # Submit task (fire-and-forget)
                    generate_sprite_sheet_task.submit(clip_id=cid)
                    # all_futures.append(future) # Optional tracking
                    logger.debug(f"[{stage_name}] Submitted generate_sprite_sheet_task for clip_id: {cid}")
                    time.sleep(TASK_SUBMIT_DELAY)
                except Exception as task_submit_err:
                     logger.error(f"[{stage_name}] Failed to submit generate_sprite_sheet_task for clip_id {cid}: {task_submit_err}", exc_info=True)
                     error_count += 1
    except Exception as db_query_err:
         logger.error(f"[{stage_name}] Failed to query for 'pending_sprite_generation' clips: {db_query_err}", exc_info=True)
         error_count += 1

    # --- Stage 3: Post-Review Start (Keyframing + Embedding Flow) ---
    stage_name = "Post-Review Start"
    try:
        # Use get_items_for_processing, exclude states handled by the sub-flow
        clips_to_process = get_items_for_processing(table="clips", ready_state="review_approved", processing_states=post_review_processing_states)
        processed_counts[stage_name] = len(clips_to_process)
        if clips_to_process:
            logger.info(f"[{stage_name}] Found {len(clips_to_process)} approved clips. Initiating post-review flows...")
            for cid in clips_to_process:
                try:
                    # Call the sub-flow directly (fire-and-forget from initiator's perspective)
                    # The sub-flow itself waits for its internal tasks.
                    process_clip_post_review.submit(clip_id=cid) # Submit as sub-flow run
                    # all_futures.append(future) # Optional tracking
                    logger.debug(f"[{stage_name}] Submitted process_clip_post_review sub-flow for clip_id: {cid}")
                    time.sleep(TASK_SUBMIT_DELAY)
                except Exception as flow_call_err:
                     logger.error(f"[{stage_name}] Failed to submit process_clip_post_review flow for clip_id {cid}: {flow_call_err}", exc_info=True)
                     error_count += 1
    except Exception as db_query_err:
         logger.error(f"[{stage_name}] Failed to query for 'review_approved' clips: {db_query_err}", exc_info=True)
         error_count += 1

    # --- Stage 3.5: Embedding Only (for already keyframed clips) ---
    stage_name = "Embedding"
    try:
        # Use get_items_for_processing
        clips_ready_for_embedding = get_items_for_processing(table="clips", ready_state="keyframed", processing_states=embedding_processing_states)
        processed_counts[stage_name] = len(clips_ready_for_embedding)
        if clips_ready_for_embedding:
            logger.info(f"[{stage_name}] Found {len(clips_ready_for_embedding)} keyframed clips ready for embedding. Submitting embedding tasks...")
            for cid in clips_ready_for_embedding:
                try:
                    # Submit embedding task (fire-and-forget)
                    generate_embeddings_task.submit(
                        clip_id=cid,
                        model_name=DEFAULT_EMBEDDING_MODEL,
                        generation_strategy=DEFAULT_EMBEDDING_STRATEGY_LABEL
                    )
                    # all_futures.append(future) # Optional tracking
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
        # get_pending_merge_pairs already handles finding the right pairs.
        # It implicitly avoids merging if the *next* clip is already merging/merged.
        # We might want to add an explicit check in get_pending_merge_pairs to exclude pairs where clip1 OR clip2 is already 'merging'.
        merge_pairs = get_pending_merge_pairs() # Assumes this is efficient enough
        processed_counts[stage_name] = len(merge_pairs)
        if merge_pairs:
             logger.info(f"[{stage_name}] Found {len(merge_pairs)} clip pairs for merging. Submitting merge tasks...")
             submitted_merges = set() # Keep track within this run
             for cid1, cid2 in merge_pairs:
                  # Double check if either clip is already being processed *within this run*
                  if cid1 not in submitted_merges and cid2 not in submitted_merges:
                      try:
                          # Submit task (fire-and-forget)
                          merge_clips_task.submit(clip_id_1=cid1, clip_id_2=cid2)
                          # all_futures.append(future) # Optional tracking
                          submitted_merges.add(cid1); submitted_merges.add(cid2)
                          logger.debug(f"[{stage_name}] Submitted merge_clips_task for pair: ({cid1}, {cid2})")
                          time.sleep(TASK_SUBMIT_DELAY)
                      except Exception as task_submit_err: logger.error(f"[{stage_name}] Failed to submit merge_clips_task for pair ({cid1}, {cid2}): {task_submit_err}", exc_info=True); error_count += 1
                  else: logger.warning(f"[{stage_name}] Skipping duplicate merge submission involving clips {cid1} or {cid2} in this initiator run.")
    except Exception as db_query_err: logger.error(f"[{stage_name}] Failed during merge check/submission: {db_query_err}", exc_info=True); error_count += 1

    # --- Stage 5: Split ---
    stage_name = "Split"
    try:
        # Use get_items_for_processing
        # We need the split info (frame number), so get_items_for_processing isn't sufficient alone.
        # Let's refine get_pending_split_jobs to also exclude processing states.
        # For now, fetch all pending, then submit tasks. The task itself should handle state checks/locking.
        # NOTE: Using get_pending_split_jobs which fetches split frame info. Need to ensure it also excludes 'splitting' state if called frequently.
        # Let's assume the split_clip_task handles the state check robustly for now.
        clips_to_split_data = get_pending_split_jobs() # Fetches pairs (id, frame)
        processed_counts[stage_name] = len(clips_to_split_data)

        if clips_to_split_data:
            logger.info(f"[{stage_name}] Found {len(clips_to_split_data)} clips pending split. Submitting split tasks...")
            submitted_splits = set() # Track within this run
            for cid, split_frame in clips_to_split_data:
                if cid not in submitted_splits:
                    try:
                        # Submit task (fire-and-forget), passing the frame number
                        # **IMPORTANT**: Ensure split_clip_task accepts split_at_frame argument
                        split_clip_task.submit(clip_id=cid) # Assuming task gets split_at_frame from DB metadata internally
                        # OR: split_clip_task.submit(clip_id=cid, split_at_frame=split_frame) # If task accepts it directly
                        # all_futures.append(future) # Optional tracking
                        submitted_splits.add(cid)
                        logger.debug(f"[{stage_name}] Submitted split_clip_task for original clip_id: {cid} (split at frame {split_frame})")
                        time.sleep(TASK_SUBMIT_DELAY)
                    except Exception as task_submit_err:
                         logger.error(f"[{stage_name}] Failed to submit split_clip_task for clip_id {cid}: {task_submit_err}", exc_info=True)
                         error_count += 1
                else:
                     logger.warning(f"[{stage_name}] Skipping duplicate split submission for clip {cid} in this initiator run.")

    except Exception as db_query_err:
         logger.error(f"[{stage_name}] Failed during split check/submission: {db_query_err}", exc_info=True)
         error_count += 1


    # --- Optional: Wait for submitted tasks ---
    # If you uncomment `all_futures.append(future)` above, you can wait here.
    # This makes the initiator flow run longer but resolves the "Garbage Collected Future" warnings.
    # if all_futures:
    #    logger.info(f"Waiting for {len(all_futures)} submitted tasks/flows to complete...")
    #    wait(all_futures) # Wait for all futures submitted in this run
    #    logger.info("All submitted tasks/flows for this cycle have completed.")


    # --- Completion Logging ---
    summary_log = f"FLOW: Scheduled Ingest Initiator cycle complete. Processed counts: {processed_counts}."
    if error_count > 0:
         logger.warning(f"{summary_log} Completed with {error_count} submission error(s).")
    else:
         logger.info(summary_log)

    # Optional: Close DB pool if you initialized it specifically for the flow run
    # close_db_pool() # Generally not needed if tasks manage their connections via the pool

# (Keep __main__ block for testing)
if __name__ == "__main__":
    print("Running one cycle of scheduled_ingest_initiator for local testing...")
    try:
        # Ensure pool is available for local run if tasks need it immediately
        # initialize_db_pool() # Needed if running outside Prefect agent context sometimes
        scheduled_ingest_initiator()
    except Exception as e:
        print(f"\nError during local test run: {e}")
        traceback.print_exc()
    finally:
        # close_db_pool() # Clean up pool after local test
        pass
    print("Local test cycle finished.")
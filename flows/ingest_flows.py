import sys
import os
from pathlib import Path
import traceback
import time
from datetime import timedelta

# --- Project Root Setup ---
project_root = Path(__file__).parent.parent.resolve()
if str(project_root) not in sys.path:
    print(f"Adding project root to sys.path: {project_root}")
    sys.path.insert(0, str(project_root))

# --- Prefect Imports ---
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
    get_db_connection
)

# --- S3 Import (Needed for cleanup flow) ---
# Attempt to import S3 client configuration, handle potential errors gracefully
try:
    from tasks.splice import s3_client, S3_BUCKET_NAME, ClientError
    if not S3_BUCKET_NAME or not s3_client:
        # Raise if essential parts are missing
        raise ImportError("S3_BUCKET_NAME or s3_client not configured/imported")
    S3_CONFIGURED = True
    print("S3 client configuration loaded successfully for cleanup flow.")
except ImportError as e:
    print(f"WARNING: S3 client config not fully available for cleanup flow: {e}. Cleanup may fail.", file=sys.stderr)
    S3_CONFIGURED = False
    s3_client = None
    S3_BUCKET_NAME = None
    # Define dummy ClientError if boto3 isn't installed or fully configured
    # This prevents NameErrors if the import fails but the class is referenced later
    if 'ClientError' not in globals():
        class ClientError(Exception): pass


# --- Configuration ---
#TODO: Add as env variables
DEFAULT_KEYFRAME_STRATEGY = os.getenv("DEFAULT_KEYFRAME_STRATEGY", "midpoint")
DEFAULT_EMBEDDING_MODEL = os.getenv("DEFAULT_EMBEDDING_MODEL", "openai/clip-vit-base-patch32")
DEFAULT_EMBEDDING_STRATEGY_LABEL = f"keyframe_{DEFAULT_KEYFRAME_STRATEGY}"
TASK_SUBMIT_DELAY = float(os.getenv("TASK_SUBMIT_DELAY", 0.1)) # Delay between submitting tasks
KEYFRAME_TIMEOUT = int(os.getenv("KEYFRAME_TIMEOUT", 600)) # 10 minutes default timeout
EMBEDDING_TIMEOUT = int(os.getenv("EMBEDDING_TIMEOUT", 900)) # 15 minutes default timeout
CLIP_CLEANUP_DELAY_MINUTES = int(os.getenv("CLIP_CLEANUP_DELAY_MINUTES", 30)) # Wait time before cleaning up


# =============================================================================
# ===                        PROCESSING FLOWS                             ===
# =============================================================================

@task
def submit_post_review_flow_task(clip_id: int):
    """
    Task specifically to submit the process_clip_post_review flow run.
    Allows the parent flow (initiator) to submit asynchronously.
    """
    logger = get_run_logger()
    logger.info(f"Submitting subflow 'process_clip_post_review' for clip_id: {clip_id}")
    # Call the flow function directly - Prefect handles scheduling it as a subflow run
    # This call within the task might be synchronous *relative to the task*,
    # but submitting the task itself is asynchronous from the parent flow.
    process_clip_post_review(clip_id=clip_id)
    logger.info(f"Subflow submission task finished for clip_id: {clip_id}")

@flow(log_prints=True)
def process_clip_post_review(
    clip_id: int,
    keyframe_strategy: str = DEFAULT_KEYFRAME_STRATEGY,
    model_name: str = DEFAULT_EMBEDDING_MODEL
    ):
    """
    Processes a single clip AFTER it has been finalized as 'review_approved'.
    Handles keyframing AND subsequent embedding for the approved clip.
    Uses .result() for task chaining, ensuring sequential execution.
    """
    logger = get_run_logger()
    logger.info(f"FLOW: Starting post-review processing for approved clip_id: {clip_id}")
    keyframe_task_succeeded = False # Flag to track stage for error reporting

    # Optional: Set a temporary 'processing_post_review' state here if needed
    # async with get_db_connection() as conn:
    #     await conn.execute("UPDATE clips SET ingest_state='processing_post_review', updated_at=NOW() WHERE id=$1 AND ingest_state='review_approved'", clip_id)

    try:
        # --- 1. Keyframing ---
        logger.info(f"Submitting keyframe task for clip_id: {clip_id} with strategy: {keyframe_strategy}")
        # Task itself should handle setting state to 'keyframing' and 'keyframed'/'keyframing_failed'
        keyframe_future = extract_keyframes_task.submit(clip_id=clip_id, strategy=keyframe_strategy)

        logger.info(f"Waiting for keyframe task result (timeout: {KEYFRAME_TIMEOUT}s)...")
        keyframe_result = keyframe_future.result(timeout=KEYFRAME_TIMEOUT) # Wait for completion
        logger.info(f"Keyframing task completed successfully for clip_id: {clip_id}.")
        keyframe_task_succeeded = True # Mark as succeeded for error reporting

        # --- 2. Embedding (Only if keyframing succeeded) ---
        # State should be 'keyframed' now (set by keyframe_task)
        embedding_strategy_label = f"keyframe_{keyframe_strategy}"
        logger.info(f"Submitting embedding task for clip_id: {clip_id} with model: {model_name}, strategy_label: {embedding_strategy_label}")
        # Task itself should handle setting state to 'embedding' and 'embedded'/'embedding_failed'
        embed_future = generate_embeddings_task.submit(
            clip_id=clip_id,
            model_name=model_name,
            generation_strategy=embedding_strategy_label
        )

        logger.info(f"Waiting for embedding task result (timeout: {EMBEDDING_TIMEOUT}s)...")
        embed_result = embed_future.result(timeout=EMBEDDING_TIMEOUT) # Wait for completion
        logger.info(f"Embedding task completed successfully for clip_id: {clip_id}.")
        # Final state 'embedded' should be set by the task

    except Exception as e:
         stage = "embedding" if keyframe_task_succeeded else "keyframing"
         logger.error(f"Error during post-review processing flow (stage: {stage}) for clip_id {clip_id}: {e}", exc_info=True)
         # The failed task should have set the appropriate failed state in the DB.
         # Consider re-raising if you want the *flow run itself* to be marked as failed in Prefect UI.
         # raise e # Optional: Propagate failure to the flow run state

    logger.info(f"FLOW: Finished post-review processing flow for clip_id: {clip_id}")


@flow(name="Scheduled Ingest Initiator", log_prints=True)
def scheduled_ingest_initiator():
    """
    Scheduled flow to find new work at different stages of the ingest pipeline
    and trigger the appropriate next tasks or sub-flows. Avoids submitting work
    for items already in a processing or failed state for that stage.
    """
    logger = get_run_logger()
    logger.info("FLOW: Running Scheduled Ingest Initiator cycle...")

    error_count = 0
    processed_counts = { # Initialize counts for logging
        "Intake": 0, "Splice": 0, "SpriteGen": 0, "Post-Review Start": 0,
        "Embedding": 0, "Merge": 0, "Split": 0
    }
    # all_futures = [] # Optional: Collect futures if waiting is needed

    # Define states that indicate an item is being processed or failed for a stage
    # These prevent the initiator from re-submitting work for that item in this cycle.
    intake_processing_states = ['downloading', 'download_failed']
    splice_processing_states = ['splicing', 'splice_failed']
    sprite_gen_processing_states = ['generating_sprite', 'sprite_generation_failed']
    # States handled by the process_clip_post_review sub-flow
    post_review_processing_states = ['keyframing', 'embedding', 'keyframing_failed', 'embedding_failed', 'processing_post_review']
    embedding_processing_states = ['embedding', 'embedding_failed']
    merge_processing_states = ['merging', 'merge_failed']
    split_processing_states = ['splitting', 'split_failed']

    # --- Stage 1: Intake ---
    stage_name = "Intake"
    try:
        new_source_ids = get_items_for_processing(table="source_videos", ready_state="new", processing_states=intake_processing_states)
        processed_counts[stage_name] = len(new_source_ids)
        if new_source_ids:
            logger.info(f"[{stage_name}] Found {len(new_source_ids)} sources. Submitting intake tasks...")
            for sid in new_source_ids:
                try:
                    source_input = get_source_input_from_db(sid)
                    if source_input:
                        intake_task.submit(source_video_id=sid, input_source=source_input)
                        logger.debug(f"[{stage_name}] Submitted intake_task for source_id: {sid}")
                        time.sleep(TASK_SUBMIT_DELAY) # Small delay
                    else:
                        logger.error(f"[{stage_name}] Could not find input source for new source_video_id: {sid}.")
                        error_count += 1
                except Exception as task_submit_err:
                     logger.error(f"[{stage_name}] Failed to submit intake_task for source_id {sid}: {task_submit_err}", exc_info=True)
                     error_count += 1
    except Exception as db_query_err:
         logger.error(f"[{stage_name}] Failed to query for 'new' source videos: {db_query_err}", exc_info=True)
         error_count += 1

    # --- Stage 2: Splice ---
    stage_name = "Splice"
    try:
        downloaded_source_ids = get_items_for_processing(table="source_videos", ready_state="downloaded", processing_states=splice_processing_states)
        processed_counts[stage_name] = len(downloaded_source_ids)
        if downloaded_source_ids:
            logger.info(f"[{stage_name}] Found {len(downloaded_source_ids)} sources ready for splicing. Submitting splice tasks...")
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

    # --- Stage 2.5: Sprite Sheet Generation ---
    stage_name = "SpriteGen"
    try:
        clips_needing_sprites = get_items_for_processing(table="clips", ready_state="pending_sprite_generation", processing_states=sprite_gen_processing_states)
        processed_counts[stage_name] = len(clips_needing_sprites)
        if clips_needing_sprites:
            logger.info(f"[{stage_name}] Found {len(clips_needing_sprites)} clips needing sprites. Submitting generation tasks...")
            for cid in clips_needing_sprites:
                try:
                    generate_sprite_sheet_task.submit(clip_id=cid)
                    logger.debug(f"[{stage_name}] Submitted generate_sprite_sheet_task for clip_id: {cid}")
                    time.sleep(TASK_SUBMIT_DELAY)
                except Exception as task_submit_err:
                     logger.error(f"[{stage_name}] Failed to submit generate_sprite_sheet_task for clip_id {cid}: {task_submit_err}", exc_info=True)
                     error_count += 1
    except Exception as db_query_err:
         logger.error(f"[{stage_name}] Failed to query for 'pending_sprite_generation' clips: {db_query_err}", exc_info=True)
         error_count += 1

    # --- Stage 3: Post-Review Start (Triggered by FINAL 'review_approved' state) ---
    stage_name = "Post-Review Start"
    try:
        # Query for clips finalized by the cleanup flow
        post_review_processing_states = ['keyframing', 'embedding', 'keyframing_failed', 'embedding_failed', 'processing_post_review'] # Added generic state
        clips_to_process = get_items_for_processing(table="clips", ready_state="review_approved", processing_states=post_review_processing_states)
        processed_counts[stage_name] = len(clips_to_process)
        if clips_to_process:
            logger.info(f"[{stage_name}] Found {len(clips_to_process)} finalized approved clips. Initiating post-review flows...")
            for cid in clips_to_process:
                try:
                    # Submit the WRAPPER TASK instead of the flow directly
                    submit_post_review_flow_task.submit(clip_id=cid)

                    logger.debug(f"[{stage_name}] Submitted task to trigger process_clip_post_review sub-flow for clip_id: {cid}")
                    time.sleep(TASK_SUBMIT_DELAY)
                except Exception as flow_call_err:
                     # Log error specific to submitting the trigger task
                     logger.error(f"[{stage_name}] Failed to submit task for process_clip_post_review flow for clip_id {cid}: {flow_call_err}", exc_info=True)
                     error_count += 1
    except Exception as db_query_err:
         logger.error(f"[{stage_name}] Failed to query for 'review_approved' clips: {db_query_err}", exc_info=True)
         error_count += 1

    # --- Stage 3.5: Embedding Only (If keyframed but embedding pending/failed) ---
    stage_name = "Embedding"
    try:
        clips_ready_for_embedding = get_items_for_processing(table="clips", ready_state="keyframed", processing_states=embedding_processing_states)
        processed_counts[stage_name] = len(clips_ready_for_embedding)
        if clips_ready_for_embedding:
            logger.info(f"[{stage_name}] Found {len(clips_ready_for_embedding)} keyframed clips ready for embedding. Submitting embedding tasks...")
            for cid in clips_ready_for_embedding:
                try:
                    # Submit embedding task directly
                    generate_embeddings_task.submit(
                        clip_id=cid,
                        model_name=DEFAULT_EMBEDDING_MODEL,
                        generation_strategy=DEFAULT_EMBEDDING_STRATEGY_LABEL
                    )
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
        # Assumes get_pending_merge_pairs handles necessary state checks
        merge_pairs = get_pending_merge_pairs()
        processed_counts[stage_name] = len(merge_pairs)
        if merge_pairs:
             logger.info(f"[{stage_name}] Found {len(merge_pairs)} clip pairs for merging. Submitting merge tasks...")
             submitted_merges = set() # Track clips involved in merges within this run
             for cid1, cid2 in merge_pairs:
                  if cid1 not in submitted_merges and cid2 not in submitted_merges:
                      try:
                          merge_clips_task.submit(clip_id_1=cid1, clip_id_2=cid2)
                          submitted_merges.add(cid1); submitted_merges.add(cid2) # Mark both as submitted
                          logger.debug(f"[{stage_name}] Submitted merge_clips_task for pair: ({cid1}, {cid2})")
                          time.sleep(TASK_SUBMIT_DELAY)
                      except Exception as task_submit_err:
                           logger.error(f"[{stage_name}] Failed to submit merge_clips_task for pair ({cid1}, {cid2}): {task_submit_err}", exc_info=True)
                           error_count += 1
                  else:
                      logger.warning(f"[{stage_name}] Skipping merge submission involving clips {cid1} or {cid2} already submitted in this run.")
    except Exception as db_query_err:
         logger.error(f"[{stage_name}] Failed during merge check/submission: {db_query_err}", exc_info=True)
         error_count += 1

    # --- Stage 5: Split ---
    stage_name = "Split"
    try:
        # Assumes get_pending_split_jobs finds clips in 'pending_split' state
        # and the split_clip_task itself handles idempotency / state checks robustly.
        clips_to_split_data = get_pending_split_jobs() # Fetches list of (id, frame)
        processed_counts[stage_name] = len(clips_to_split_data)

        if clips_to_split_data:
            logger.info(f"[{stage_name}] Found {len(clips_to_split_data)} clips pending split. Submitting split tasks...")
            submitted_splits = set() # Track clips submitted for split in this run
            for cid, split_frame in clips_to_split_data:
                if cid not in submitted_splits:
                    try:
                        # Assuming task gets split_at_frame from DB metadata based on previous implementation
                        # If task needs frame explicitly: split_clip_task.submit(clip_id=cid, split_at_frame=split_frame)
                        split_clip_task.submit(clip_id=cid)
                        submitted_splits.add(cid) # Mark as submitted
                        logger.debug(f"[{stage_name}] Submitted split_clip_task for original clip_id: {cid} (split requested at frame {split_frame})")
                        time.sleep(TASK_SUBMIT_DELAY)
                    except Exception as task_submit_err:
                         logger.error(f"[{stage_name}] Failed to submit split_clip_task for clip_id {cid}: {task_submit_err}", exc_info=True)
                         error_count += 1
                else:
                     logger.warning(f"[{stage_name}] Skipping duplicate split submission for clip {cid} in this initiator run.")
    except Exception as db_query_err:
         logger.error(f"[{stage_name}] Failed during split check/submission: {db_query_err}", exc_info=True)
         error_count += 1


    # --- Initiator Flow Completion Logging ---
    summary_log = f"FLOW: Scheduled Ingest Initiator cycle complete. Processed counts: {processed_counts}."
    if error_count > 0:
         logger.warning(f"{summary_log} Completed with {error_count} submission error(s).")
    else:
         logger.info(summary_log)


# =============================================================================
# ===                        CLEANUP FLOW                                 ===
# =============================================================================

@flow(name="Scheduled Clip Cleanup", log_prints=True)
async def cleanup_reviewed_clips_flow(
    cleanup_delay_minutes: int = CLIP_CLEANUP_DELAY_MINUTES
    ):
    """
    Scheduled flow to finalize clips marked for deletion after review.
    Finds clips in 'approved_pending_deletion' or 'archived_pending_deletion' state
    that haven't been updated recently (beyond the specified delay).
    Attempts to delete associated S3 sprite sheet and updates DB state to final
    'review_approved' or 'archived' state, nullifying sprite info.
    """
    logger = get_run_logger()
    logger.info(f"FLOW: Running Scheduled Clip Cleanup...")
    logger.info(f"Using cleanup delay: {cleanup_delay_minutes} minutes")

    if not S3_CONFIGURED:
        logger.error("S3 Client/Config is not available. Cannot perform S3 deletions. Exiting cleanup flow.")
        return # Cannot proceed without S3 config

    conn = None # Initialize connection variable
    processed_count = 0
    s3_deleted_count = 0
    db_updated_count = 0
    error_count = 0

    try:
        # Get an async database connection from the pool
        conn = await get_db_connection()

        # 1. Find clips ready for cleanup based on state and last update time
        # Ensure the interval calculation is compatible with asyncpg parameter binding
        # Using a timedelta object is generally preferred if supported
        delay_interval = timedelta(minutes=cleanup_delay_minutes)
        query = """
            SELECT id, sprite_sheet_filepath, ingest_state
            FROM clips
            WHERE ingest_state IN ('approved_pending_deletion', 'archived_pending_deletion')
              AND updated_at < (NOW() - $1::INTERVAL);
        """
        logger.info(f"Querying for clips in pending deletion states older than {delay_interval}...")
        clips_to_cleanup = await conn.fetch(query, delay_interval)
        processed_count = len(clips_to_cleanup)
        logger.info(f"Found {processed_count} clips ready for cleanup.")

        if not clips_to_cleanup:
            logger.info("No clips require cleanup in this cycle.")
            # No need to release connection here if using 'await get_db_connection()' context manager style
            # If not using context manager, release connection in finally block
            return

        # 2. Process each identified clip
        for clip_record in clips_to_cleanup:
            clip_id = clip_record['id']
            sprite_path = clip_record['sprite_sheet_filepath']
            current_state = clip_record['ingest_state']
            log_prefix = f"[Cleanup Clip {clip_id}]" # Prefix for logs related to this clip
            logger.info(f"{log_prefix} Processing (State: {current_state}, Sprite: {sprite_path})")

            s3_deletion_successful = False # Flag per clip

            # 3. Attempt S3 Deletion (only if a path exists)
            if sprite_path:
                if not S3_BUCKET_NAME: # Defensive check
                     logger.error(f"{log_prefix} S3_BUCKET_NAME not configured. Skipping S3 deletion.")
                     s3_deletion_successful = False # Cannot proceed with S3 deletion
                     error_count += 1
                     continue # Move to next clip if S3 isn't configured properly here

                try:
                    logger.debug(f"{log_prefix} Attempting to delete S3 object: s3://{S3_BUCKET_NAME}/{sprite_path}")
                    # Note: s3_client.delete_object is synchronous. Consider thread pool for high volume.
                    s3_client.delete_object(Bucket=S3_BUCKET_NAME, Key=sprite_path)
                    logger.info(f"{log_prefix} Successfully deleted S3 object: {sprite_path}")
                    s3_deletion_successful = True
                    s3_deleted_count += 1
                except ClientError as e:
                    # Handle specific boto3 ClientErrors
                    error_code = e.response.get('Error', {}).get('Code', 'Unknown')
                    if error_code == 'NoSuchKey':
                        logger.warning(f"{log_prefix} S3 object not found (NoSuchKey): {sprite_path}. Assuming already deleted.")
                        s3_deletion_successful = True # Treat as success for DB update
                    else:
                        logger.error(f"{log_prefix} Failed to delete S3 object {sprite_path}: {e} (Code: {error_code})")
                        error_count += 1
                        s3_deletion_successful = False # Do not update DB if critical S3 delete failed
                except Exception as e:
                     # Catch any other unexpected errors during S3 interaction
                     logger.error(f"{log_prefix} Unexpected error deleting S3 object {sprite_path}: {e}", exc_info=True)
                     error_count += 1
                     s3_deletion_successful = False
            else:
                # No sprite path associated with the clip
                logger.info(f"{log_prefix} No sprite_sheet_filepath recorded. Skipping S3 deletion.")
                s3_deletion_successful = True # Nothing to delete, allow DB update

            # 4. Update Database (only if S3 deletion was successful or skipped)
            if s3_deletion_successful:
                final_state = None
                if current_state == 'approved_pending_deletion':
                    final_state = 'review_approved'
                elif current_state == 'archived_pending_deletion':
                    final_state = 'archived'

                if final_state:
                    try:
                        # Update the clip to its final state and nullify sprite info
                        update_query = """
                            UPDATE clips
                            SET
                                sprite_sheet_filepath = NULL,
                                sprite_metadata = NULL,
                                ingest_state = $1, -- Final state
                                updated_at = NOW(),
                                last_error = NULL -- Clear any previous errors
                            WHERE id = $2 AND ingest_state = $3; -- Ensure state hasn't changed concurrently
                        """
                        # Use execute for UPDATE/DELETE, fetchval/fetchrow for SELECT
                        result = await conn.execute(update_query, final_state, clip_id, current_state)

                        # Check the result string from execute (e.g., "UPDATE 1")
                        if result == "UPDATE 1":
                             logger.info(f"{log_prefix} Successfully updated state to {final_state} and nulled sprite info.")
                             db_updated_count += 1
                        else:
                             # This might happen if the state was changed by another process
                             # between the initial SELECT and this UPDATE
                             logger.warning(f"{log_prefix} DB update command did not affect any rows (result: {result}). State might have changed.")
                             # Don't count as error necessarily, but worth noting
                             error_count += 1 # Or handle differently?

                    except Exception as e:
                        # Catch errors during the database update itself
                        logger.error(f"{log_prefix} Failed to update database after S3 handling: {e}", exc_info=True)
                        error_count += 1
                else:
                    # This case should ideally not be reachable due to the initial query filter
                    logger.error(f"{log_prefix} Could not determine final state from intermediate state '{current_state}'. Skipping DB update.")
                    error_count += 1

    except Exception as e:
        # Catch errors during DB connection or the main query
        logger.error(f"Error during cleanup flow execution: {e}", exc_info=True)
        error_count += 1 # Ensure errors outside the loop are counted
        # Consider re-raising to mark the flow run as failed in Prefect UI
        # raise e
    finally:
        # Ensure the database connection is released/closed if acquired directly
        if conn:
            # Depending on how get_db_connection manages the pool, explicit release might be needed.
            # If get_db_connection uses 'async with pool.acquire()', release is automatic.
            # await conn.close() # Or pool.release(conn) if using acquire directly
             pass # Assuming get_db_connection handles release via context or similar
        logger.info("Database connection cleanup (if necessary) complete.")


    # --- Cleanup Flow Completion Logging ---
    logger.info(
        f"FLOW: Scheduled Clip Cleanup complete. "
        f"Clips Found: {processed_count}, "
        f"S3 Deleted: {s3_deleted_count}, "
        f"DB Updated: {db_updated_count}, "
        f"Errors: {error_count}"
    )

# =============================================================================
# ===                        LOCAL TESTING BLOCK                          ===
# =============================================================================

# This block allows running flows directly for testing purposes without Prefect deployment
if __name__ == "__main__":
    print("Running flows locally for testing...")
    # Select which flow to run via environment variable or hardcode here
    flow_to_run = os.environ.get("PREFECT_FLOW_TO_RUN", "initiator") # Default to initiator
    # Example usage: PREFECT_FLOW_TO_RUN=cleanup python flows/ingest_flows.py

    print(f"Attempting to run flow: {flow_to_run}")

    try:
        # Ensure pool is available if needed (tasks might rely on lazy init)
        # initialize_db_pool() # Uncomment if direct pool init is required for local runs

        if flow_to_run == "initiator":
            print("\n--- Testing scheduled_ingest_initiator ---")
            scheduled_ingest_initiator()
            print("--- Finished scheduled_ingest_initiator ---")

        elif flow_to_run == "cleanup":
             print("\n--- Testing cleanup_reviewed_clips_flow ---")
             # Run the async cleanup flow using asyncio
             import asyncio
             # Use a very short delay for testing purposes
             asyncio.run(cleanup_reviewed_clips_flow(cleanup_delay_minutes=1))
             print("--- Finished cleanup_reviewed_clips_flow ---")

        elif flow_to_run == "post_review":
             # Test the post-review flow for a specific clip ID
             clip_id_to_test = int(os.environ.get("TEST_CLIP_ID", 0))
             if clip_id_to_test > 0:
                 print(f"\n--- Testing process_clip_post_review for Clip ID: {clip_id_to_test} ---")
                 process_clip_post_review(clip_id=clip_id_to_test)
                 print(f"--- Finished process_clip_post_review for Clip ID: {clip_id_to_test} ---")
             else:
                 print("Set TEST_CLIP_ID environment variable (e.g., TEST_CLIP_ID=123) to test process_clip_post_review.")

        else:
            print(f"Unknown flow specified: '{flow_to_run}'. Set PREFECT_FLOW_TO_RUN=initiator|cleanup|post_review")

    except Exception as e:
        print(f"\nError during local test run of flow '{flow_to_run}': {e}")
        traceback.print_exc()
    finally:
        # Clean up DB pool if it was initialized specifically for the test run
        # close_db_pool() # Uncomment if direct pool closing is required
        pass
    print("\nLocal test run finished.")

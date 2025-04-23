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
from prefect.deployments import run_deployment # For triggering independent flow runs

# --- Task Imports ---
from tasks.intake import intake_task
from tasks.splice import splice_video_task, s3_client, S3_BUCKET_NAME
from tasks.sprite import generate_sprite_sheet_task
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
    update_clip_state_sync
)

# --- Async DB and S3 client imports ---
# Used by cleanup_reviewed_clips_flow
try:
    from database import connect_db, close_db
    ASYNC_DB_CONFIGURED = True
except ImportError:
    ASYNC_DB_CONFIGURED = False
    connect_db, close_db = None, None # Define placeholders if asyncpg isn't set up

try:
    from botocore.exceptions import ClientError
    S3_CONFIGURED = True # Assume S3 is configured if botocore is installed
except ImportError:
     S3_CONFIGURED = False
     ClientError = Exception # Placeholder

# --- Configuration ---
DEFAULT_KEYFRAME_STRATEGY = os.getenv("DEFAULT_KEYFRAME_STRATEGY", "midpoint")
DEFAULT_EMBEDDING_MODEL = os.getenv("DEFAULT_EMBEDDING_MODEL", "openai/clip-vit-base-patch32")
DEFAULT_EMBEDDING_STRATEGY_LABEL = f"keyframe_{DEFAULT_KEYFRAME_STRATEGY}"
# Example for multi strategy:
# if DEFAULT_KEYFRAME_STRATEGY == "multi": DEFAULT_EMBEDDING_STRATEGY_LABEL = f"keyframe_{DEFAULT_KEYFRAME_STRATEGY}_avg"

TASK_SUBMIT_DELAY = float(os.getenv("TASK_SUBMIT_DELAY", 0.1)) # Delay between task/run submissions
KEYFRAME_TIMEOUT = int(os.getenv("KEYFRAME_TIMEOUT", 600)) # Timeout for keyframe task result (seconds)
EMBEDDING_TIMEOUT = int(os.getenv("EMBEDDING_TIMEOUT", 900)) # Timeout for embedding task result (seconds)
CLIP_CLEANUP_DELAY_MINUTES = int(os.getenv("CLIP_CLEANUP_DELAY_MINUTES", 30))

# --- Constants ---
ARTIFACT_TYPE_SPRITE_SHEET = "sprite_sheet"


# =============================================================================
# ===                        PROCESSING FLOWS                             ===
# =============================================================================

@flow(log_prints=True)
def process_clip_post_review(
    clip_id: int,
    keyframe_strategy: str = DEFAULT_KEYFRAME_STRATEGY,
    model_name: str = DEFAULT_EMBEDDING_MODEL
    ):
    """
    Processes a single clip AFTER it has been finalized as 'review_approved'.
    Handles keyframing AND subsequent embedding for the approved clip.
    This flow is typically triggered by 'scheduled_ingest_initiator'.

    Args:
        clip_id: The ID of the clip to process.
        keyframe_strategy: Strategy for keyframe extraction ('midpoint', 'multi').
        model_name: Embedding model to use.
    """
    logger = get_run_logger()
    logger.info(f"FLOW: Starting post-review processing for approved clip_id: {clip_id}")
    keyframe_task_succeeded_or_skipped = False # Track if keyframing is done or acceptably skipped
    keyframe_job = None
    embedding_job = None

    try:
        # --- 1. Keyframing ---
        logger.info(f"Submitting keyframe task for clip_id: {clip_id} with strategy: {keyframe_strategy}")
        keyframe_job = extract_keyframes_task.submit(
            clip_id=clip_id,
            strategy=keyframe_strategy,
            overwrite=False
        )
        logger.info(f"Waiting for keyframe task result for clip {clip_id} (timeout: {KEYFRAME_TIMEOUT}s)")
        keyframe_result = keyframe_job.result(timeout=KEYFRAME_TIMEOUT)

        # Process keyframe result, handling skips gracefully
        keyframe_status = None
        if isinstance(keyframe_result, dict):
            keyframe_status = keyframe_result.get("status")
        elif keyframe_result is None:
             logger.warning(f"Keyframe task for clip {clip_id} returned None. Assuming already processed or skipped.")
             keyframe_status = "skipped_or_success"
        else:
            logger.warning(f"Keyframe task for clip {clip_id} returned unexpected type: {type(keyframe_result)}. Treating as potential failure.")
            keyframe_status = "failed_unexpected_result"

        keyframe_failure_statuses = ["failed", "failed_db_phase1", "failed_processing", "failed_state_update", "failed_db_update", "failed_unexpected", "failed_unexpected_result"]
        keyframe_acceptable_skip_statuses = ["skipped_exists", "skipped_state", "skipped_logic", "skipped_or_success"]

        if keyframe_status == "success":
            logger.info(f"Keyframing task completed successfully for clip_id: {clip_id}.")
            keyframe_task_succeeded_or_skipped = True
        elif keyframe_status in keyframe_acceptable_skip_statuses:
            logger.info(f"Keyframing task for clip_id {clip_id} finished with status '{keyframe_status}'. Assuming processed/skipped acceptably.")
            keyframe_task_succeeded_or_skipped = True # Allows embedding check
        elif keyframe_status in keyframe_failure_statuses:
             raise RuntimeError(f"Keyframing task failed for clip {clip_id}. Status: {keyframe_status}")
        else: # Unknown status
             logger.error(f"Keyframing task for clip {clip_id} returned unknown status: '{keyframe_status}'. Treating as failure.")
             raise RuntimeError(f"Keyframing task failed for clip {clip_id} with unknown status: {keyframe_status}")

        # --- 2. Embedding (only if keyframing succeeded or was acceptably skipped) ---
        if keyframe_task_succeeded_or_skipped:
            # Construct embedding strategy label
            if keyframe_strategy == "multi":
                embedding_strategy_label = f"keyframe_{keyframe_strategy}_avg"
            else:
                embedding_strategy_label = f"keyframe_{keyframe_strategy}"

            logger.info(f"Submitting embedding task for clip_id: {clip_id} with model: {model_name}, derived strategy_label: {embedding_strategy_label}")
            embedding_job = generate_embeddings_task.submit(
                clip_id=clip_id,
                model_name=model_name,
                generation_strategy=embedding_strategy_label,
                overwrite=False
            )
            logger.info(f"Waiting for embedding task result for clip {clip_id} (timeout: {EMBEDDING_TIMEOUT}s)")
            embed_result = embedding_job.result(timeout=EMBEDDING_TIMEOUT)

            # Process embedding result, handling skips gracefully
            embed_status = None
            if isinstance(embed_result, dict):
                embed_status = embed_result.get("status")
            elif embed_result is None:
                logger.warning(f"Embedding task for clip {clip_id} returned None. Assuming already processed or skipped.")
                embed_status = "skipped_or_success"
            else:
                 logger.warning(f"Embedding task for clip {clip_id} returned unexpected type: {type(embed_result)}. Treating as potential failure.")
                 embed_status = "failed_unexpected_result"

            embed_failure_statuses = ["failed", "failed_db_phase1", "failed_processing", "failed_state_update", "failed_db_update", "failed_unexpected", "failed_unexpected_result"]
            # Define acceptable skip statuses for embedding (e.g., if it already exists or state prevents it)
            embed_acceptable_skip_statuses = ["skipped_exists", "skipped_state", "skipped_or_success"]

            if embed_status == "success":
                logger.info(f"Embedding task completed successfully for clip_id: {clip_id}.")
            elif embed_status in embed_acceptable_skip_statuses:
                logger.info(f"Embedding task for clip_id {clip_id} finished with status '{embed_status}'. Assuming processed/skipped acceptably.")
                # This is still a successful outcome for the flow's purpose
            elif embed_status in embed_failure_statuses:
                raise RuntimeError(f"Embedding task failed for clip {clip_id}. Status: {embed_status}")
            else: # Unknown status
                logger.error(f"Embedding task for clip {clip_id} returned unknown status: '{embed_status}'. Treating as failure.")
                raise RuntimeError(f"Embedding task failed for clip {clip_id} with unknown status: {embed_status}")
        else:
            logger.warning(f"Skipping embedding for clip {clip_id} as keyframing didn't succeed or skip acceptably.")

    except Exception as e:
         stage = "embedding" if keyframe_task_succeeded_or_skipped else "keyframing"
         logger.error(f"Error during post-review processing flow (stage: {stage}) for clip_id {clip_id}: {e}", exc_info=True)
         raise e # Re-raise to mark flow run as failed

    logger.info(f"FLOW: Finished post-review processing flow for clip_id: {clip_id}")


@flow(name="Scheduled Ingest Initiator", log_prints=True)
def scheduled_ingest_initiator():
    """
    Scheduled flow to find new work at different stages of the ingest pipeline
    and trigger the appropriate next tasks or flow runs.
    """
    logger = get_run_logger()
    logger.info("FLOW: Running Scheduled Ingest Initiator cycle...")

    try:
        initialize_db_pool()
        logger.info("Initialized DB pool for Prefect tasks.")
    except Exception as pool_init_err:
         logger.critical(f"Failed to initialize Prefect task DB pool: {pool_init_err}", exc_info=True)
         raise RuntimeError("Cannot proceed without DB pool for tasks.") from pool_init_err

    error_count = 0
    processed_counts = {
        "Intake": 0, "Splice": 0, "SpriteGen": 0, "Post-Review Start": 0,
        "Embedding": 0, "Merge": 0, "Split": 0
    }

    intake_processing_states = ['downloading', 'download_failed']
    splice_processing_states = ['splicing', 'splice_failed']
    sprite_gen_processing_states = ['generating_sprite', 'sprite_generation_failed']
    post_review_processing_states = [
        'keyframing', 'embedding', 'keyframing_failed', 'embedding_failed',
        'processing_post_review', 'embedded', # Add 'embedded' to prevent re-trigger if embedding finishes quickly
        'approved_pending_deletion', 'archived_pending_deletion'
    ]
    embedding_processing_states = ['embedding', 'embedding_failed', 'embedded'] # Add 'embedded'
    merge_processing_states = ['merging', 'merge_failed']
    split_processing_states = ['splitting', 'split_failed']

    # --- Stage 1: Intake ---
    stage_name = "Intake"
    try:
        new_source_ids = get_items_for_processing("source_videos", "new", intake_processing_states)
        processed_counts[stage_name] = len(new_source_ids)
        if new_source_ids:
            logger.info(f"[{stage_name}] Found {len(new_source_ids)} sources. Submitting tasks...")
            for sid in new_source_ids:
                try:
                    source_input = get_source_input_from_db(sid)
                    if source_input:
                        intake_task.submit(source_video_id=sid, input_source=source_input)
                        logger.debug(f"[{stage_name}] Submitted intake_task for source_id: {sid}")
                        time.sleep(TASK_SUBMIT_DELAY)
                    else: logger.error(f"[{stage_name}] No input source for ID: {sid}."); error_count += 1
                except Exception as task_submit_err: logger.error(f"[{stage_name}] Submit fail ID {sid}: {task_submit_err}", exc_info=True); error_count += 1
    except Exception as db_query_err: logger.error(f"[{stage_name}] Failed query: {db_query_err}", exc_info=True); error_count += 1

    # --- Stage 2: Splice ---
    stage_name = "Splice"
    try:
        downloaded_source_ids = get_items_for_processing("source_videos", "downloaded", splice_processing_states)
        processed_counts[stage_name] = len(downloaded_source_ids)
        if downloaded_source_ids:
            logger.info(f"[{stage_name}] Found {len(downloaded_source_ids)} sources for splicing. Submitting tasks...")
            for sid in downloaded_source_ids:
                try:
                    splice_video_task.submit(source_video_id=sid)
                    logger.debug(f"[{stage_name}] Submitted splice_task for source_id: {sid}")
                    time.sleep(TASK_SUBMIT_DELAY)
                except Exception as task_submit_err: logger.error(f"[{stage_name}] Submit fail ID {sid}: {task_submit_err}", exc_info=True); error_count += 1
    except Exception as db_query_err: logger.error(f"[{stage_name}] Failed query: {db_query_err}", exc_info=True); error_count += 1

    # --- Stage 3: Sprite Sheet Generation ---
    stage_name = "SpriteGen"
    try:
        clips_needing_sprites = get_items_for_processing("clips", "pending_sprite_generation", sprite_gen_processing_states)
        processed_counts[stage_name] = len(clips_needing_sprites)
        if clips_needing_sprites:
            logger.info(f"[{stage_name}] Found {len(clips_needing_sprites)} clips needing sprites. Submitting tasks...")
            for cid in clips_needing_sprites:
                try:
                    generate_sprite_sheet_task.submit(clip_id=cid)
                    logger.debug(f"[{stage_name}] Submitted sprite_task for clip_id: {cid}")
                    time.sleep(TASK_SUBMIT_DELAY)
                except Exception as task_submit_err: logger.error(f"[{stage_name}] Submit fail ID {cid}: {task_submit_err}", exc_info=True); error_count += 1
    except Exception as db_query_err: logger.error(f"[{stage_name}] Failed query: {db_query_err}", exc_info=True); error_count += 1

    # --- Stage 4: Post-Review Start (Trigger Separate Flow Run) ---
    stage_name = "Post-Review Start"
    try:
        clips_to_process = get_items_for_processing("clips", "review_approved", post_review_processing_states)
        processed_counts[stage_name] = len(clips_to_process)
        if clips_to_process:
            deployment_name = "process-clip-post-review/process-clip-post-review-default"
            logger.info(f"[{stage_name}] Found {len(clips_to_process)} approved clips. Triggering '{deployment_name}' runs...")
            for cid in clips_to_process:
                try:
                    logger.debug(f"[{stage_name}] Setting clip {cid} state to 'processing_post_review'.")
                    updated = update_clip_state_sync(cid, 'processing_post_review')
                    if not updated: logger.error(f"[{stage_name}] Failed state update for clip {cid}. Skipping trigger."); error_count += 1; continue

                    logger.debug(f"[{stage_name}] Submitting deployment run '{deployment_name}' for clip_id: {cid}")
                    run_deployment(name=deployment_name, parameters={"clip_id": cid, "keyframe_strategy": DEFAULT_KEYFRAME_STRATEGY, "model_name": DEFAULT_EMBEDDING_MODEL}, timeout=0)
                    time.sleep(TASK_SUBMIT_DELAY)
                except Exception as flow_trigger_err: logger.error(f"[{stage_name}] Failed trigger for '{deployment_name}', clip_id {cid}: {flow_trigger_err}", exc_info=True); error_count += 1
    except Exception as db_query_err: logger.error(f"[{stage_name}] Failed query: {db_query_err}", exc_info=True); error_count += 1

    # --- Stage 4.5: Embedding Only (If keyframed but embedding failed/pending) ---
    stage_name = "Embedding"
    try:
        clips_ready_for_embedding = get_items_for_processing("clips", "keyframed", embedding_processing_states)
        processed_counts[stage_name] = len(clips_ready_for_embedding)
        if clips_ready_for_embedding:
            logger.info(f"[{stage_name}] Found {len(clips_ready_for_embedding)} keyframed clips needing embedding. Submitting tasks...")
            for cid in clips_ready_for_embedding:
                try:
                    generate_embeddings_task.submit(clip_id=cid, model_name=DEFAULT_EMBEDDING_MODEL, generation_strategy=DEFAULT_EMBEDDING_STRATEGY_LABEL)
                    logger.debug(f"[{stage_name}] Submitted embed_task for clip_id: {cid}")
                    time.sleep(TASK_SUBMIT_DELAY)
                except Exception as task_submit_err: logger.error(f"[{stage_name}] Submit fail ID {cid}: {task_submit_err}", exc_info=True); error_count += 1
    except Exception as db_query_err: logger.error(f"[{stage_name}] Failed query: {db_query_err}", exc_info=True); error_count += 1

    # --- Stage 5.1: Merge ---
    stage_name = "Merge"
    try:
        merge_pairs = get_pending_merge_pairs()
        processed_counts[stage_name] = len(merge_pairs)
        if merge_pairs:
             logger.info(f"[{stage_name}] Found {len(merge_pairs)} pairs for merging. Submitting tasks...")
             submitted_merges = set()
             for target_id, source_id in merge_pairs:
                  if target_id not in submitted_merges and source_id not in submitted_merges:
                      try:
                          merge_clips_task.submit(clip_id_target=target_id, clip_id_source=source_id)
                          submitted_merges.add(target_id); submitted_merges.add(source_id)
                          logger.debug(f"[{stage_name}] Submitted merge_task for target={target_id}, source={source_id}")
                          time.sleep(TASK_SUBMIT_DELAY)
                      except Exception as task_submit_err: logger.error(f"[{stage_name}] Submit fail ({target_id}, {source_id}): {task_submit_err}", exc_info=True); error_count += 1
                  else: logger.warning(f"[{stage_name}] Skipping merge involving {target_id}/{source_id} - submitted.")
    except Exception as db_query_err: logger.error(f"[{stage_name}] Failed during merge check: {db_query_err}", exc_info=True); error_count += 1

    # --- Stage 5.2: Split ---
    stage_name = "Split"
    try:
        clips_to_split_data = get_pending_split_jobs()
        processed_counts[stage_name] = len(clips_to_split_data)
        if clips_to_split_data:
            logger.info(f"[{stage_name}] Found {len(clips_to_split_data)} clips pending split. Submitting tasks...")
            submitted_splits = set()
            for cid, split_frame in clips_to_split_data:
                if cid not in submitted_splits:
                    try:
                        split_clip_task.submit(clip_id=cid)
                        submitted_splits.add(cid)
                        logger.debug(f"[{stage_name}] Submitted split_task for clip_id: {cid} (frame {split_frame})")
                        time.sleep(TASK_SUBMIT_DELAY)
                    except Exception as task_submit_err: logger.error(f"[{stage_name}] Submit fail ID {cid}: {task_submit_err}", exc_info=True); error_count += 1
                else: logger.warning(f"[{stage_name}] Skipping duplicate split for clip {cid}.")
    except Exception as db_query_err: logger.error(f"[{stage_name}] Failed during split check: {db_query_err}", exc_info=True); error_count += 1

    # --- Initiator Flow Completion Logging ---
    summary_log = f"FLOW: Scheduled Ingest Initiator cycle complete. Processed counts: {processed_counts}."
    if error_count > 0:
         logger.warning(f"{summary_log} Completed with {error_count} submission/trigger error(s).")
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
    Uses asyncpg for database operations.
    """
    logger = get_run_logger()
    logger.info(f"FLOW: Running Scheduled Clip Cleanup...")
    logger.info(f"Using cleanup delay: {cleanup_delay_minutes} minutes")

    if not S3_CONFIGURED or not s3_client:
        logger.error("S3 Client/Config not available. Cannot perform S3 deletions."); return
    if not ASYNC_DB_CONFIGURED or connect_db is None:
        logger.error("Async DB not configured or import failed. Cannot perform DB operations."); return

    pool, conn = None, None
    processed_count, s3_deleted_count, db_artifact_deleted_count, db_clip_updated_count, error_count = 0, 0, 0, 0, 0

    try:
        pool = await connect_db()
        if not pool: logger.error("Failed to get asyncpg pool."); return

        async with pool.acquire() as conn:
            logger.info("Acquired asyncpg connection.")
            delay_interval = timedelta(minutes=cleanup_delay_minutes)
            query_clips = "SELECT id, ingest_state FROM clips WHERE ingest_state = ANY($1::text[]) AND updated_at < (NOW() - $2::INTERVAL) ORDER BY id ASC;"
            pending_states = ['approved_pending_deletion', 'archived_pending_deletion']
            clips_to_cleanup = await conn.fetch(query_clips, pending_states, delay_interval)
            processed_count = len(clips_to_cleanup)
            logger.info(f"Found {processed_count} clips potentially ready for cleanup.")
            if not clips_to_cleanup: logger.info("No clips require cleanup."); return

            for clip_record in clips_to_cleanup:
                clip_id = clip_record['id']; current_state = clip_record['ingest_state']
                log_prefix = f"[Cleanup Clip {clip_id}]"; logger.info(f"{log_prefix} Processing state: {current_state}")
                sprite_artifact_s3_key, s3_deletion_successful = None, False
                try:
                    query_artifact = "SELECT s3_key FROM clip_artifacts WHERE clip_id = $1 AND artifact_type = $2 LIMIT 1;"
                    artifact_record = await conn.fetchrow(query_artifact, clip_id, ARTIFACT_TYPE_SPRITE_SHEET)
                    if artifact_record: sprite_artifact_s3_key = artifact_record['s3_key']; logger.info(f"{log_prefix} Found sprite: {sprite_artifact_s3_key}")
                    else: logger.warning(f"{log_prefix} No sprite artifact found."); s3_deletion_successful = True

                    if sprite_artifact_s3_key:
                        if not S3_BUCKET_NAME: logger.error(f"{log_prefix} S3_BUCKET_NAME missing."); s3_deletion_successful = False; error_count += 1
                        else:
                            try:
                                logger.debug(f"{log_prefix} Deleting S3: s3://{S3_BUCKET_NAME}/{sprite_artifact_s3_key}")
                                s3_client.delete_object(Bucket=S3_BUCKET_NAME, Key=sprite_artifact_s3_key)
                                logger.info(f"{log_prefix} Deleted S3 object."); s3_deletion_successful = True; s3_deleted_count += 1
                            except ClientError as e:
                                error_code = e.response.get('Error', {}).get('Code', 'Unknown')
                                if error_code == 'NoSuchKey': logger.warning(f"{log_prefix} S3 object not found (NoSuchKey)."); s3_deletion_successful = True
                                else: logger.error(f"{log_prefix} Failed S3 delete: {e} (Code: {error_code})"); error_count += 1; s3_deletion_successful = False
                            except Exception as e: logger.error(f"{log_prefix} Unexpected S3 error: {e}", exc_info=True); error_count += 1; s3_deletion_successful = False

                    if s3_deletion_successful:
                        final_state_map = {'approved_pending_deletion': 'review_approved', 'archived_pending_deletion': 'archived'}
                        final_clip_state = final_state_map.get(current_state)
                        if not final_clip_state: logger.error(f"{log_prefix} Unexpected state '{current_state}'."); error_count += 1; continue
                        try:
                            async with conn.transaction():
                                if sprite_artifact_s3_key:
                                    del_res = await conn.execute("DELETE FROM clip_artifacts WHERE clip_id = $1 AND artifact_type = $2;", clip_id, ARTIFACT_TYPE_SPRITE_SHEET)
                                    if del_res.startswith("DELETE"): db_artifact_deleted_count += int(del_res.split()[-1])
                                upd_res = await conn.execute("UPDATE clips SET ingest_state=$1, updated_at=NOW(), last_error=NULL WHERE id=$2 AND ingest_state=$3;", final_clip_state, clip_id, current_state)
                                if upd_res == "UPDATE 1": logger.info(f"{log_prefix} Updated state to '{final_clip_state}'."); db_clip_updated_count += 1
                                else: raise RuntimeError(f"Clip update affected {upd_res} rows (expected 1)")
                        except Exception as tx_err: logger.error(f"{log_prefix} DB TX failed: {tx_err}", exc_info=True); error_count += 1
                    else: logger.warning(f"{log_prefix} Skipping DB updates due to S3 fail.")
                except Exception as inner_err: logger.error(f"{log_prefix} Error processing: {inner_err}", exc_info=True); error_count += 1
    except Exception as outer_err: logger.error(f"FATAL Error during cleanup flow: {outer_err}", exc_info=True); error_count += 1
    finally:
        if pool and conn: logger.info("Asyncpg connection released.")
        elif pool and not conn: logger.warning("Asyncpg pool ok but conn acquisition failed.")

    logger.info(f"FLOW: Cleanup complete. Found:{processed_count}, S3Del:{s3_deleted_count}, DBArtDel:{db_artifact_deleted_count}, DBClipUpd:{db_clip_updated_count}, Errors:{error_count}")


# =============================================================================
# ===                        LOCAL TESTING BLOCK                          ===
# =============================================================================

if __name__ == "__main__":
    import asyncio
    print("Running flows locally for testing...")
    flow_to_run = os.environ.get("PREFECT_FLOW_TO_RUN", "initiator")
    print(f"Attempting to run flow: {flow_to_run}")

    async def run_async_flow(flow_func, *args, **kwargs):
        pool = None
        try:
             if ASYNC_DB_CONFIGURED and connect_db:
                 pool = await connect_db(); print("Async DB Pool connected.")
             await flow_func(*args, **kwargs)
        except Exception as e: print(f"Error running async {flow_func.__name__}: {e}"); traceback.print_exc()
        finally:
             if pool and ASYNC_DB_CONFIGURED and close_db: print("Async DB Pool potentially closed.")

    try:
        if flow_to_run == "initiator":
            print("\n--- Testing scheduled_ingest_initiator ---"); scheduled_ingest_initiator(); print("--- Finished initiator ---")
        elif flow_to_run == "cleanup":
             print("\n--- Testing cleanup_reviewed_clips_flow ---"); asyncio.run(run_async_flow(cleanup_reviewed_clips_flow, cleanup_delay_minutes=1)); print("--- Finished cleanup ---")
        elif flow_to_run == "post_review":
             clip_id = int(os.environ.get("TEST_CLIP_ID", 0))
             if clip_id > 0:
                 print(f"\n--- Testing process_clip_post_review directly (Clip ID: {clip_id}) ---"); initialize_db_pool(); process_clip_post_review(clip_id=clip_id); print(f"--- Finished post_review (Clip ID: {clip_id}) ---")
             else: print("Set TEST_CLIP_ID env var.")
        elif flow_to_run == "merge":
             target_id = int(os.environ.get("TEST_MERGE_TARGET_ID", 0)); source_id = int(os.environ.get("TEST_MERGE_SOURCE_ID", 0))
             if target_id > 0 and source_id > 0:
                  print(f"\n--- Testing merge_clips_task (direct fn) Target:{target_id}, Source:{source_id} ---")
                  try: initialize_db_pool(); merge_clips_task.fn(clip_id_target=target_id, clip_id_source=source_id)
                  except Exception as task_exc: print(f"Error: {task_exc}"); traceback.print_exc()
                  print(f"--- Finished merge test ---")
             else: print("Set TEST_MERGE_TARGET_ID and TEST_MERGE_SOURCE_ID.")
        else: print(f"Unknown flow: '{flow_to_run}'. Options: initiator, cleanup, post_review, merge")
    except Exception as e: print(f"\nError during local run of '{flow_to_run}': {e}"); traceback.print_exc()
    finally: print("\nLocal test run finished.")
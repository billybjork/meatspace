import sys
import os
from pathlib import Path
import traceback
import time
from datetime import timedelta, datetime # Added datetime

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
from tasks.sprite_generator import generate_sprite_sheet_task # Keep for initiator
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
    # get_db_connection # Sync connector for sync parts of initiator
)
# --- Import Async DB Utils ---
# Needed for the async cleanup flow
try:
    # Assuming database.py provides these asyncpg pool management functions
    from database import connect_db, close_db
    ASYNC_DB_CONFIGURED = True
    print("Async DB configuration (asyncpg) loaded for cleanup flow.")
except ImportError as e:
     print(f"WARNING: Could not import async DB functions from database.py: {e}. Cleanup flow may fail.", file=sys.stderr)
     ASYNC_DB_CONFIGURED = False


# --- S3 Import (Needed for cleanup flow) ---
# Attempt to import S3 client configuration
try:
    # Assuming s3_client might be initialized centrally or in a task module like splice
    # Ensure it's accessible here. Central initialization is recommended.
    # For now, relying on tasks.splice import as before.
    from tasks.splice import s3_client, S3_BUCKET_NAME
    from botocore.exceptions import ClientError
    if not S3_BUCKET_NAME:
        raise ImportError("S3_BUCKET_NAME not configured/imported")
    if s3_client is None:
         # Attempt to initialize if None but bucket name exists (e.g., if splice wasn't imported yet)
         # This is a fallback - central init is better
         import boto3
         print("Attempting fallback S3 client initialization in ingest_flows.")
         s3_client = boto3.client('s3', region_name=os.getenv("AWS_REGION"))

    S3_CONFIGURED = True
    print("S3 client configuration loaded successfully for cleanup flow.")
except ImportError as e:
    print(f"WARNING: S3 client config not fully available for cleanup flow: {e}. Cleanup may fail.", file=sys.stderr)
    S3_CONFIGURED = False
    s3_client = None
    S3_BUCKET_NAME = None
    # Define ClientError if botocore couldn't be imported or lacks it
    if 'ClientError' not in globals():
        class ClientError(Exception): pass # Define dummy exception
except Exception as s3_init_err:
     print(f"ERROR: Unexpected error initializing S3 client for cleanup flow: {s3_init_err}", file=sys.stderr)
     S3_CONFIGURED = False
     s3_client = None
     S3_BUCKET_NAME = None
     if 'ClientError' not in globals():
        class ClientError(Exception): pass


# --- Configuration ---
DEFAULT_KEYFRAME_STRATEGY = os.getenv("DEFAULT_KEYFRAME_STRATEGY", "midpoint") # e.g., 'midpoint', 'multi'
DEFAULT_EMBEDDING_MODEL = os.getenv("DEFAULT_EMBEDDING_MODEL", "openai/clip-vit-base-patch32")
# Ensure embedding strategy label matches task expectations (e.g., keyframe_strategy[_aggregation])
DEFAULT_EMBEDDING_STRATEGY_LABEL = f"keyframe_{DEFAULT_KEYFRAME_STRATEGY}" # e.g., "keyframe_midpoint"
# If using 'multi' strategy by default, decide if default embedding should average:
# DEFAULT_EMBEDDING_STRATEGY_LABEL = f"keyframe_{DEFAULT_KEYFRAME_STRATEGY}_avg" # e.g., "keyframe_multi_avg"

TASK_SUBMIT_DELAY = float(os.getenv("TASK_SUBMIT_DELAY", 0.1)) # Delay between submitting tasks
KEYFRAME_TIMEOUT = int(os.getenv("KEYFRAME_TIMEOUT", 600)) # 10 minutes default timeout
EMBEDDING_TIMEOUT = int(os.getenv("EMBEDDING_TIMEOUT", 900)) # 15 minutes default timeout
CLIP_CLEANUP_DELAY_MINUTES = int(os.getenv("CLIP_CLEANUP_DELAY_MINUTES", 30)) # Wait time before cleaning up


# --- Constants ---
ARTIFACT_TYPE_SPRITE_SHEET = "sprite_sheet" # Constant for artifact type

# =============================================================================
# ===                        PROCESSING FLOWS                             ===
# =============================================================================

@task
def submit_post_review_flow_task(clip_id: int):
    """
    Task wrapper to submit the process_clip_post_review flow run asynchronously.
    """
    logger = get_run_logger()
    logger.info(f"Submitting subflow 'process_clip_post_review' for clip_id: {clip_id}")
    # Parameters passed here become parameters for the sub-flow run
    process_clip_post_review.submit(
        clip_id=clip_id,
        keyframe_strategy=DEFAULT_KEYFRAME_STRATEGY, # Pass the strategy name
        model_name=DEFAULT_EMBEDDING_MODEL
    )
    logger.info(f"Subflow submission task finished for clip_id: {clip_id}")

@flow(log_prints=True)
def process_clip_post_review(
    clip_id: int,
    keyframe_strategy: str = DEFAULT_KEYFRAME_STRATEGY, # Receives strategy name ('midpoint', 'multi')
    model_name: str = DEFAULT_EMBEDDING_MODEL
    ):
    """
    Processes a single clip AFTER it has been finalized as 'review_approved'.
    Handles keyframing AND subsequent embedding for the approved clip.

    Args:
        clip_id: The ID of the clip to process.
        keyframe_strategy: The strategy name to use for keyframe extraction (e.g., 'midpoint', 'multi').
        model_name: The embedding model to use.
    """
    logger = get_run_logger()
    logger.info(f"FLOW: Starting post-review processing for approved clip_id: {clip_id}")
    keyframe_task_succeeded = False
    keyframe_job = None
    embedding_job = None

    try:
        # --- 1. Keyframing ---
        logger.info(f"Submitting keyframe task for clip_id: {clip_id} with strategy: {keyframe_strategy}")
        keyframe_job = extract_keyframes_task.submit(
            clip_id=clip_id,
            strategy=keyframe_strategy, # Pass the strategy name directly
            overwrite=False # Usually don't overwrite in the standard post-review flow
        )
        # Wait for keyframe task to finish before proceeding to embedding
        keyframe_result = keyframe_job.result(timeout=timedelta(seconds=KEYFRAME_TIMEOUT))
        # Basic check on result if needed (e.g., status is success)
        if keyframe_result.get("status") != "success":
             raise RuntimeError(f"Keyframing task failed or skipped for clip {clip_id}. Status: {keyframe_result.get('status')}")

        logger.info(f"Keyframing task completed successfully for clip_id: {clip_id}.")
        keyframe_task_succeeded = True

        # --- 2. Embedding (Only if keyframing succeeded) ---
        # Construct the embedding strategy label based on keyframe strategy
        # Example: if keyframe_strategy is 'multi', default embedding might be 'keyframe_multi_avg'
        if keyframe_strategy == "multi":
            embedding_strategy_label = f"keyframe_{keyframe_strategy}_avg" # Decide default aggregation
        else:
            embedding_strategy_label = f"keyframe_{keyframe_strategy}" # e.g., "keyframe_midpoint"

        logger.info(f"Submitting embedding task for clip_id: {clip_id} with model: {model_name}, derived strategy_label: {embedding_strategy_label}")
        embedding_job = generate_embeddings_task.submit(
            clip_id=clip_id,
            model_name=model_name,
            generation_strategy=embedding_strategy_label, # Pass the derived label
            overwrite=False # Usually don't overwrite here
        )
        # Wait for embedding task
        embed_result = embedding_job.result(timeout=timedelta(seconds=EMBEDDING_TIMEOUT))
        if embed_result.get("status") != "success":
            raise RuntimeError(f"Embedding task failed or skipped for clip {clip_id}. Status: {embed_result.get('status')}")

        logger.info(f"Embedding task completed successfully for clip_id: {clip_id}.")

    except Exception as e:
         stage = "embedding" if keyframe_task_succeeded else "keyframing"
         # Log error with traceback
         logger.error(f"Error during post-review processing flow (stage: {stage}) for clip_id {clip_id}: {e}", exc_info=True)
         # Optionally cancel downstream task if upstream failed mid-wait
         # if stage == "keyframing" and embedding_job: embedding_job.cancel() # Prefect Cloud/Server might handle this
         # Consider re-raising if you want the flow run itself to be marked as failed in Prefect UI.
         raise e # Re-raise to mark flow run as failed

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
    processed_counts = {
        "Intake": 0, "Splice": 0, "SpriteGen": 0, "Post-Review Start": 0,
        "Embedding": 0, "Merge": 0, "Split": 0
    }

    # Define states that indicate an item is being processed or failed for that stage
    # These help prevent redundant task submissions.
    intake_processing_states = ['downloading', 'download_failed']
    splice_processing_states = ['splicing', 'splice_failed']
    # SpriteGen runs on 'pending_sprite_generation'
    sprite_gen_processing_states = ['generating_sprite', 'sprite_generation_failed']
    # Post-Review (Keyframing/Embedding via subflow) starts from 'review_approved'
    post_review_processing_states = [
        'keyframing', 'embedding', # States set by the subflow tasks
        'keyframing_failed', 'embedding_failed', # Failure states from subflow tasks
        'processing_post_review', # Optional intermediate state if set before subflow submission
        'approved_pending_deletion', 'archived_pending_deletion' # Exclude cleanup states
    ]
    # Embedding only stage starts from 'keyframed'
    embedding_processing_states = ['embedding', 'embedding_failed']
    # Merge starts from 'pending_merge_target' or 'marked_for_merge_into_previous'
    merge_processing_states = ['merging', 'merge_failed'] # Tasks set these
    # Split starts from 'pending_split'
    split_processing_states = ['splitting', 'split_failed'] # Task sets these

    # --- Stage 1: Intake ---
    stage_name = "Intake"
    try:
        new_source_ids = get_items_for_processing(
            table="source_videos",
            ready_state="new",
            processing_states=intake_processing_states
        )
        processed_counts[stage_name] = len(new_source_ids)
        if new_source_ids:
            logger.info(f"[{stage_name}] Found {len(new_source_ids)} sources. Submitting intake tasks...")
            for sid in new_source_ids:
                try:
                    source_input = get_source_input_from_db(sid) # Uses sync db_utils
                    if source_input:
                        intake_task.submit(source_video_id=sid, input_source=source_input)
                        logger.debug(f"[{stage_name}] Submitted intake_task for source_id: {sid}")
                        time.sleep(TASK_SUBMIT_DELAY)
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
        downloaded_source_ids = get_items_for_processing(
            table="source_videos",
            ready_state="downloaded",
            processing_states=splice_processing_states
        )
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
        clips_needing_sprites = get_items_for_processing(
            table="clips",
            ready_state="pending_sprite_generation", # State set after splice/review
            processing_states=sprite_gen_processing_states
        )
        processed_counts[stage_name] = len(clips_needing_sprites)
        if clips_needing_sprites:
            logger.info(f"[{stage_name}] Found {len(clips_needing_sprites)} clips needing sprites. Submitting generation tasks...")
            for cid in clips_needing_sprites:
                try:
                    # Assuming generate_sprite_sheet_task signature remains clip_id
                    generate_sprite_sheet_task.submit(clip_id=cid)
                    logger.debug(f"[{stage_name}] Submitted generate_sprite_sheet_task for clip_id: {cid}")
                    time.sleep(TASK_SUBMIT_DELAY)
                except Exception as task_submit_err:
                     logger.error(f"[{stage_name}] Failed to submit generate_sprite_sheet_task for clip_id {cid}: {task_submit_err}", exc_info=True)
                     error_count += 1
    except Exception as db_query_err:
         logger.error(f"[{stage_name}] Failed to query for 'pending_sprite_generation' clips: {db_query_err}", exc_info=True)
         error_count += 1

    # --- Stage 3: Post-Review Start (Keyframe/Embed Subflow) ---
    stage_name = "Post-Review Start"
    try:
        # Find clips approved after review (state might be set by cleanup flow or review UI)
        clips_to_process = get_items_for_processing(
            table="clips",
            ready_state="review_approved", # The state indicating ready for keyframe/embed
            processing_states=post_review_processing_states # Avoid reprocessing
        )
        processed_counts[stage_name] = len(clips_to_process)
        if clips_to_process:
            logger.info(f"[{stage_name}] Found {len(clips_to_process)} finalized approved clips. Initiating post-review flows...")
            for cid in clips_to_process:
                try:
                    # Submit the task that will trigger the sub-flow
                    # Pass necessary parameters for the subflow via the task submission if needed,
                    # but here they are picked up by the subflow definition defaults.
                    submit_post_review_flow_task.submit(clip_id=cid)
                    logger.debug(f"[{stage_name}] Submitted task to trigger process_clip_post_review sub-flow for clip_id: {cid}")
                    time.sleep(TASK_SUBMIT_DELAY)
                except Exception as flow_call_err:
                     logger.error(f"[{stage_name}] Failed to submit task for process_clip_post_review flow for clip_id {cid}: {flow_call_err}", exc_info=True)
                     error_count += 1
    except Exception as db_query_err:
         logger.error(f"[{stage_name}] Failed to query for 'review_approved' clips: {db_query_err}", exc_info=True)
         error_count += 1

    # --- Stage 3.5: Embedding Only (If keyframed but embedding pending/failed) ---
    stage_name = "Embedding"
    try:
        # Find clips that are keyframed but not yet embedded (or failed embedding)
        clips_ready_for_embedding = get_items_for_processing(
            table="clips",
            ready_state="keyframed", # State set by keyframe task
            processing_states=embedding_processing_states # Avoid reprocessing if already embedding/failed
        )
        processed_counts[stage_name] = len(clips_ready_for_embedding)
        if clips_ready_for_embedding:
            logger.info(f"[{stage_name}] Found {len(clips_ready_for_embedding)} keyframed clips ready for embedding. Submitting embedding tasks...")
            for cid in clips_ready_for_embedding:
                try:
                    # Submit embedding task directly
                    generate_embeddings_task.submit(
                        clip_id=cid,
                        model_name=DEFAULT_EMBEDDING_MODEL,
                        generation_strategy=DEFAULT_EMBEDDING_STRATEGY_LABEL # Use the configured default
                    )
                    logger.debug(f"[{stage_name}] Submitted generate_embeddings_task for clip_id: {cid}")
                    time.sleep(TASK_SUBMIT_DELAY)
                except Exception as task_submit_err:
                     logger.error(f"[{stage_name}] Failed to submit generate_embeddings_task for clip_id {cid}: {task_submit_err}", exc_info=True)
                     error_count += 1
    except Exception as db_query_err:
         logger.error(f"[{stage_name}] Failed to query for 'keyframed' clips: {db_query_err}", exc_info=True)
         error_count += 1

    # --- Stage 4: Merge (Backward Merge) ---
    stage_name = "Merge"
    try:
        merge_pairs = get_pending_merge_pairs() # Uses sync db_utils
        processed_counts[stage_name] = len(merge_pairs)

        if merge_pairs:
             logger.info(f"[{stage_name}] Found {len(merge_pairs)} clip pairs for backward merging. Submitting merge tasks...")
             submitted_merges = set() # Avoid submitting merges for the same clip twice in one run
             for target_id, source_id in merge_pairs:
                  # Check if either clip is already involved in a submitted merge this run
                  if target_id not in submitted_merges and source_id not in submitted_merges:
                      try:
                          merge_clips_task.submit(clip_id_target=target_id, clip_id_source=source_id)
                          # Mark both clips as involved in a submitted merge
                          submitted_merges.add(target_id)
                          submitted_merges.add(source_id)
                          logger.debug(f"[{stage_name}] Submitted merge_clips_task for target={target_id}, source={source_id}")
                          time.sleep(TASK_SUBMIT_DELAY)
                      except Exception as task_submit_err:
                           logger.error(f"[{stage_name}] Failed to submit merge_clips_task for pair (target={target_id}, source={source_id}): {task_submit_err}", exc_info=True)
                           error_count += 1
                  else:
                      logger.warning(f"[{stage_name}] Skipping merge submission involving target {target_id} or source {source_id} as one is already part of a submitted merge in this run.")
    except Exception as db_query_err:
         logger.error(f"[{stage_name}] Failed during backward merge check/submission: {db_query_err}", exc_info=True)
         error_count += 1

    # --- Stage 5: Split ---
    stage_name = "Split"
    try:
        clips_to_split_data = get_pending_split_jobs() # Uses sync db_utils, gets list of (id, frame)
        processed_counts[stage_name] = len(clips_to_split_data)

        if clips_to_split_data:
            logger.info(f"[{stage_name}] Found {len(clips_to_split_data)} clips pending split. Submitting split tasks...")
            submitted_splits = set() # Track submitted clip IDs to avoid duplicates this run
            for cid, split_frame in clips_to_split_data:
                if cid not in submitted_splits:
                    try:
                        # The split_clip_task retrieves the split frame itself from metadata
                        split_clip_task.submit(clip_id=cid)
                        submitted_splits.add(cid)
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
async def cleanup_reviewed_clips_flow( # Make it async
    cleanup_delay_minutes: int = CLIP_CLEANUP_DELAY_MINUTES
    ):
    """
    Scheduled flow to finalize clips marked for deletion after review.
    Finds clips in 'approved_pending_deletion' or 'archived_pending_deletion' state
    that haven't been updated recently (beyond the specified delay).
    Finds associated 'sprite_sheet' artifact, attempts to delete it from S3,
    deletes the artifact record from DB, and updates clip state to final
    'review_approved' or 'archived'. Uses asyncpg and runs S3 deletion sync.
    """
    logger = get_run_logger()
    logger.info(f"FLOW: Running Scheduled Clip Cleanup...")
    logger.info(f"Using cleanup delay: {cleanup_delay_minutes} minutes")

    # --- Dependency Checks ---
    if not S3_CONFIGURED or not s3_client: # Check s3_client instance too
        logger.error("S3 Client/Config is not available. Cannot perform S3 deletions. Exiting cleanup flow.")
        return
    if not ASYNC_DB_CONFIGURED:
        logger.error("Async DB (asyncpg) is not configured. Cannot perform DB operations. Exiting cleanup flow.")
        return

    # --- Initialization ---
    pool = None
    conn = None # asyncpg connection
    processed_count = 0
    s3_deleted_count = 0
    db_artifact_deleted_count = 0
    db_clip_updated_count = 0
    error_count = 0

    try:
        # Get the asyncpg pool
        pool = await connect_db() # Get pool from database.py
        # Acquire a connection from the pool
        async with pool.acquire() as conn:
            logger.info("Acquired asyncpg connection for cleanup flow.")

            delay_interval = timedelta(minutes=cleanup_delay_minutes)
            # Query for clips in pending deletion states, only need id and state now
            query_clips = """
                SELECT id, ingest_state
                FROM clips
                WHERE ingest_state = ANY($1::text[]) -- ['approved_pending_deletion', 'archived_pending_deletion']
                  AND updated_at < (NOW() - $2::INTERVAL)
                ORDER BY id ASC;
            """
            pending_states = ['approved_pending_deletion', 'archived_pending_deletion']
            logger.info(f"Querying for clips in states {pending_states} older than {delay_interval}...")

            clips_to_cleanup = await conn.fetch(query_clips, pending_states, delay_interval)
            processed_count = len(clips_to_cleanup)
            logger.info(f"Found {processed_count} clips potentially ready for cleanup.")

            if not clips_to_cleanup:
                logger.info("No clips require cleanup in this cycle.")
                return

            # Process each identified clip
            for clip_record in clips_to_cleanup:
                clip_id = clip_record['id']
                current_state = clip_record['ingest_state']
                log_prefix = f"[Cleanup Clip {clip_id}]"
                logger.info(f"{log_prefix} Processing clip in state: {current_state}")

                sprite_artifact_s3_key = None
                s3_deletion_successful = False

                try:
                    # 1. Find the sprite sheet artifact S3 key
                    query_artifact = """
                        SELECT s3_key FROM clip_artifacts
                        WHERE clip_id = $1 AND artifact_type = $2
                        LIMIT 1;
                    """
                    artifact_record = await conn.fetchrow(query_artifact, clip_id, ARTIFACT_TYPE_SPRITE_SHEET)

                    if artifact_record:
                        sprite_artifact_s3_key = artifact_record['s3_key']
                        logger.info(f"{log_prefix} Found sprite sheet artifact key: {sprite_artifact_s3_key}")
                    else:
                        logger.warning(f"{log_prefix} No sprite sheet artifact found in DB. Assuming already cleaned or never existed.")
                        s3_deletion_successful = True # Allow DB update to proceed

                    # 2. Attempt S3 Deletion if artifact key was found
                    if sprite_artifact_s3_key:
                        if not S3_BUCKET_NAME: # Redundant check, belt-and-suspenders
                             logger.error(f"{log_prefix} S3_BUCKET_NAME missing. Skipping S3 delete for {sprite_artifact_s3_key}.")
                             # Should we proceed? If we can't delete S3, maybe don't update DB? Let's prevent DB update.
                             s3_deletion_successful = False
                             error_count += 1
                        else:
                            try:
                                logger.debug(f"{log_prefix} Attempting to delete S3 object: s3://{S3_BUCKET_NAME}/{sprite_artifact_s3_key}")
                                # boto3 S3 client methods are synchronous.
                                # For high concurrency, consider asyncio.to_thread or aiobotocore.
                                # Here, we call it directly, blocking the event loop briefly.
                                s3_client.delete_object(Bucket=S3_BUCKET_NAME, Key=sprite_artifact_s3_key)
                                logger.info(f"{log_prefix} Successfully deleted S3 object: {sprite_artifact_s3_key}")
                                s3_deletion_successful = True
                                s3_deleted_count += 1
                            except ClientError as e:
                                error_code = e.response.get('Error', {}).get('Code', 'Unknown')
                                if error_code == 'NoSuchKey':
                                    logger.warning(f"{log_prefix} S3 object not found (NoSuchKey): {sprite_artifact_s3_key}. Assuming already deleted.")
                                    s3_deletion_successful = True # Treat as success for DB update
                                else:
                                    logger.error(f"{log_prefix} Failed to delete S3 object {sprite_artifact_s3_key}: {e} (Code: {error_code})")
                                    error_count += 1
                                    s3_deletion_successful = False # Critical S3 failure, prevent DB update
                            except Exception as e:
                                 logger.error(f"{log_prefix} Unexpected error deleting S3 object {sprite_artifact_s3_key}: {e}", exc_info=True)
                                 error_count += 1
                                 s3_deletion_successful = False # Prevent DB update on unexpected error

                    # 3. Update Database (Delete artifact row, Update clip state) - Transactionally
                    if s3_deletion_successful:
                        final_clip_state = None
                        if current_state == 'approved_pending_deletion': final_clip_state = 'review_approved'
                        elif current_state == 'archived_pending_deletion': final_clip_state = 'archived'
                        else:
                             logger.error(f"{log_prefix} Unexpected current state '{current_state}' during cleanup DB update phase.")
                             error_count += 1
                             continue # Skip DB update for this clip

                        # Use a transaction for atomicity
                        try:
                            async with conn.transaction():
                                logger.debug(f"{log_prefix} Starting DB transaction...")
                                # Delete artifact row (only if key existed, even if S3 delete was skipped due to NoSuchKey)
                                if sprite_artifact_s3_key:
                                    delete_artifact_sql = """
                                        DELETE FROM clip_artifacts
                                        WHERE clip_id = $1 AND artifact_type = $2;
                                    """
                                    del_result = await conn.execute(delete_artifact_sql, clip_id, ARTIFACT_TYPE_SPRITE_SHEET)
                                    logger.info(f"{log_prefix} Deleted artifact record from DB (Status: {del_result})")
                                    # Check result? 'DELETE 1' or 'DELETE 0'
                                    if del_result.startswith("DELETE"): db_artifact_deleted_count += int(del_result.split()[-1])


                                # Update clip state
                                update_clip_sql = """
                                    UPDATE clips
                                    SET
                                        ingest_state = $1, -- Final state
                                        updated_at = NOW(),
                                        last_error = NULL -- Clear errors
                                    WHERE id = $2 AND ingest_state = $3; -- Concurrency check
                                """
                                update_result = await conn.execute(update_clip_sql, final_clip_state, clip_id, current_state)

                                # Check update result
                                if update_result == "UPDATE 1":
                                     logger.info(f"{log_prefix} Successfully updated clip state to '{final_clip_state}'.")
                                     db_clip_updated_count += 1
                                elif update_result == "UPDATE 0":
                                     logger.warning(f"{log_prefix} Clip DB update command did not affect any rows (result: {update_result}). State might have changed from '{current_state}'. Transaction will rollback.")
                                     # Transaction rollback happens automatically on context exit if error raised
                                     raise RuntimeError("Clip state update failed (0 rows affected)")
                                else:
                                    logger.warning(f"{log_prefix} Unexpected clip DB update result: {update_result}. State might have changed. Transaction will rollback.")
                                    raise RuntimeError(f"Unexpected DB update result: {update_result}")

                            logger.debug(f"{log_prefix} DB transaction committed successfully.")

                        except Exception as tx_err:
                            # Transaction automatically rolled back
                            logger.error(f"{log_prefix} DB transaction failed: {tx_err}", exc_info=True)
                            error_count += 1
                            # Do not proceed if DB update failed

                    else:
                        logger.warning(f"{log_prefix} Skipping DB updates because S3 deletion failed or was skipped due to critical error.")

                except Exception as inner_loop_err:
                     # Catch unexpected errors within the loop for a single clip
                     logger.error(f"{log_prefix} Unexpected error processing clip: {inner_loop_err}", exc_info=True)
                     error_count += 1
                     # Continue to the next clip

    except Exception as outer_err:
        logger.error(f"FATAL Error during cleanup flow execution: {outer_err}", exc_info=True)
        error_count += 1
    finally:
        # Connection is released automatically by 'async with pool.acquire()'.
        # Pool closing (`close_db()`) should happen at application shutdown, not here.
        if pool and conn: # Log only if connection was acquired
             logger.info("Asyncpg connection released back to pool.")


    # --- Cleanup Flow Completion Logging ---
    logger.info(
        f"FLOW: Scheduled Clip Cleanup complete. "
        f"Clips Found: {processed_count}, "
        f"S3 Artifacts Deleted: {s3_deleted_count}, "
        f"DB Artifacts Deleted: {db_artifact_deleted_count}, "
        f"DB Clips Updated: {db_clip_updated_count}, "
        f"Errors: {error_count}"
    )

# =============================================================================
# ===                        LOCAL TESTING BLOCK                          ===
# =============================================================================

if __name__ == "__main__":
    import asyncio # Needed for async flows

    print("Running flows locally for testing...")
    flow_to_run = os.environ.get("PREFECT_FLOW_TO_RUN", "initiator")
    print(f"Attempting to run flow: {flow_to_run}")

    # --- Async Pool Management for Local Run ---
    # Simplified: Assume connect_db/close_db handle initialization or are no-ops if already managed.
    # Proper local testing might require explicit setup/teardown here if not run via Prefect agent/server.
    async def run_async_flow(flow_func, *args, **kwargs):
        pool = None
        try:
             if ASYNC_DB_CONFIGURED:
                 # Ensure pool is ready (connect_db might handle this)
                 pool = await connect_db()
                 print("Async DB Pool connection confirmed/established for local run.")
             await flow_func(*args, **kwargs)
        except Exception as e:
            print(f"Error running async flow {flow_func.__name__}: {e}")
            traceback.print_exc()
        finally:
             if pool and ASYNC_DB_CONFIGURED:
                 # Close the pool if this script instance owns it
                 # await close_db() # Be careful if pool is shared (e.g., with FastAPI)
                 print("Async DB Pool connection potentially closed (if managed by this script).")

    try:
        if flow_to_run == "initiator":
            print("\n--- Testing scheduled_ingest_initiator ---")
            # This flow is synchronous
            scheduled_ingest_initiator() # Assumes sync DB utils work
            print("--- Finished scheduled_ingest_initiator ---")

        elif flow_to_run == "cleanup":
             print("\n--- Testing cleanup_reviewed_clips_flow ---")
             # This flow is async
             asyncio.run(run_async_flow(cleanup_reviewed_clips_flow, cleanup_delay_minutes=1)) # Use short delay
             print("--- Finished cleanup_reviewed_clips_flow ---")

        elif flow_to_run == "post_review":
             # This flow is synchronous but calls async submissions via task wrappers
             clip_id_to_test = int(os.environ.get("TEST_CLIP_ID", 0))
             if clip_id_to_test > 0:
                 print(f"\n--- Testing process_clip_post_review for Clip ID: {clip_id_to_test} ---")
                 # Running the flow directly executes tasks synchronously in process
                 process_clip_post_review(clip_id=clip_id_to_test)
                 print(f"--- Finished process_clip_post_review for Clip ID: {clip_id_to_test} ---")
             else:
                 print("Set TEST_CLIP_ID env var to test process_clip_post_review.")

        elif flow_to_run == "merge":
             # Test merge task (assuming it's sync for local direct call)
             target_id = int(os.environ.get("TEST_MERGE_TARGET_ID", 0))
             source_id = int(os.environ.get("TEST_MERGE_SOURCE_ID", 0))
             if target_id > 0 and source_id > 0:
                  print(f"\n--- Testing merge_clips_task (direct fn call) for Target ID: {target_id}, Source ID: {source_id} ---")
                  try:
                       # Call the task's underlying function directly for simple testing
                       merge_clips_task.fn(clip_id_target=target_id, clip_id_source=source_id)
                  except Exception as task_exc:
                       print(f"Error running merge_clips_task.fn directly: {task_exc}")
                       traceback.print_exc()
                  print(f"--- Finished merge_clips_task test ---")
             else:
                  print("Set TEST_MERGE_TARGET_ID and TEST_MERGE_SOURCE_ID env vars to test merge_clips_task.")

        else:
            print(f"Unknown flow specified: '{flow_to_run}'. Options: initiator, cleanup, post_review, merge")

    except Exception as e:
        print(f"\nError during local test run of flow '{flow_to_run}': {e}")
        traceback.print_exc()
    finally:
        print("\nLocal test run finished.")
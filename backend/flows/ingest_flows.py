import sys
import os
from pathlib import Path
import traceback
import time
import psycopg2
import psycopg2.extras
from datetime import timedelta, timezone
import logging

# --- Project Root Setup ---
# Path(__file__).parent.parent is /app (This is our project root inside the Docker container)
project_root_inside_container = Path(__file__).resolve().parent.parent
if str(project_root_inside_container) not in sys.path:
    sys.path.insert(0, str(project_root_inside_container))
    print(f"DEBUG: Added to sys.path: {str(project_root_inside_container)}")
    print(f"DEBUG: Current sys.path: {sys.path}")

# --- Prefect Imports ---
from prefect import flow, get_run_logger, task
from prefect.deployments import run_deployment

# --- Local Module Imports (relative to /app) ---
from tasks.intake import intake_task
from tasks.splice import splice_video_task, s3_client, S3_BUCKET_NAME
from tasks.sprite import generate_sprite_sheet_task
from tasks.keyframe import extract_keyframes_task
from tasks.embed import generate_embeddings_task
from tasks.merge import merge_clips_task
from tasks.split import split_clip_task

from db.sync_db import (
    get_all_pending_work,
    get_source_input_from_db,
    get_pending_merge_pairs,
    get_pending_split_jobs,
    initialize_db_pool,
    update_clip_state_sync,
    update_source_video_state_sync,
    get_db_connection,
    release_db_connection
)

_flow_bootstrap_logger = logging.getLogger(__name__) # For logging import errors
try:
    from db.async_db import get_db_connection as get_async_db_connection, get_db_pool, close_db_pool
    ASYNC_DB_CONFIGURED = True
except ImportError as e:
    _flow_bootstrap_logger.error(
        f"CRITICAL: Failed to import async DB utilities from db.async_db: {e}. "
        "Async cleanup flow will not function.",
        exc_info=True
    )
    ASYNC_DB_CONFIGURED = False
    get_async_db_connection, get_db_pool, close_db_pool = None, None, None

try:
    from botocore.exceptions import ClientError
    S3_CONFIGURED = True
except ImportError:
     S3_CONFIGURED = False
     ClientError = Exception

# --- Configuration ---
DEFAULT_KEYFRAME_STRATEGY = os.getenv("DEFAULT_KEYFRAME_STRATEGY", "midpoint")
DEFAULT_EMBEDDING_MODEL = os.getenv("DEFAULT_EMBEDDING_MODEL", "openai/clip-vit-base-patch32")
DEFAULT_EMBEDDING_STRATEGY_LABEL = f"keyframe_{DEFAULT_KEYFRAME_STRATEGY}"
TASK_SUBMIT_DELAY = float(os.getenv("TASK_SUBMIT_DELAY", 0.1)) # Delay between task submissions
KEYFRAME_TIMEOUT = int(os.getenv("KEYFRAME_TIMEOUT", 600)) # Timeout for keyframe task result
EMBEDDING_TIMEOUT = int(os.getenv("EMBEDDING_TIMEOUT", 900)) # Timeout for embedding task result
CLIP_CLEANUP_DELAY_MINUTES = int(os.getenv("CLIP_CLEANUP_DELAY_MINUTES", 30))
ACTION_COMMIT_GRACE_PERIOD_SECONDS = int(os.getenv("ACTION_COMMIT_GRACE_PERIOD_SECONDS", 10))

# Configuration for limiting submissions per initiator cycle
MAX_NEW_SUBMISSIONS_PER_CYCLE = int(os.getenv("MAX_NEW_SUBMISSIONS_PER_CYCLE", 15))

# --- Constants ---
ARTIFACT_TYPE_SPRITE_SHEET = "sprite_sheet"


# =============================================================================
# ===                        COMMIT WORKER LOGIC                            ===
# =============================================================================
def _commit_pending_review_actions(grace_period_seconds: int):
    """
    Find the newest ‘selected_*’ event for every clip that is still in
    ‘pending_review’, has survived the grace period and was *not* undone,
    then atomically:

      • (for grouping) populate grouped_with_clip_id
      • (for splitting) populate processing_metadata with split_request_at_frame
      • move the clip to its post‑review ingest_state
      • stamp action_committed_at
    """
    logger = get_run_logger()

    if grace_period_seconds <= 0:
        logger.debug("Commit grace period ≤ 0 – skipping.")
        return 0

    committed_count = 0
    conn = None
    try:
        conn = get_db_connection(cursor_factory=psycopg2.extras.RealDictCursor)

        with conn:                              # Outer transaction for all actions
            with conn.cursor() as cur:

                find_sql = """
                WITH latest AS (
                  SELECT  ce.clip_id,
                          ce.action,
                          ce.created_at,
                          ce.event_data, -- Ensure event_data is selected
                          ROW_NUMBER() OVER (PARTITION BY ce.clip_id
                                             ORDER BY ce.created_at DESC) AS rn
                  FROM    clip_events ce
                  WHERE   ce.action LIKE 'selected_%%'
                    AND   ce.action <> 'selected_undo'
                )
                SELECT  l.clip_id,
                        l.action,
                        l.event_data, -- Ensure event_data is selected
                        l.created_at AS action_time
                FROM    latest l
                JOIN    clips  c ON c.id = l.clip_id
                WHERE   l.rn = 1
                  AND   c.ingest_state = 'pending_review'
                  AND   l.created_at < (NOW() - INTERVAL %s)
                  AND   NOT EXISTS (
                         SELECT 1
                         FROM   clip_events u
                         WHERE  u.clip_id = l.clip_id
                           AND  u.action  = 'selected_undo'
                           AND  u.created_at > l.created_at )
                ORDER BY l.created_at;
                """
                cur.execute(find_sql, (f'{grace_period_seconds} seconds',))
                actions = cur.fetchall()

                if not actions:
                    logger.debug("No review actions ready for commit.")
                    return 0

                logger.info(f"Committing {len(actions)} clip actions…")

                for row in actions:
                    clip_id    = row["clip_id"]
                    action     = row["action"]
                    # Ensure event_data is a dict, even if NULL/None from DB
                    event_data = row.get("event_data") or {}
                    target_state = None
                    processing_metadata_update_payload = None # For actions that need to update this field

                    if   action == "selected_approve":
                        target_state = "review_approved"
                    elif action == "selected_skip":
                        target_state = "skipped"
                    elif action == "selected_archive":
                        target_state = "archived_pending_deletion"
                    elif action == "selected_merge_source":
                        target_state = "marked_for_merge_into_previous"
                    elif action == "selected_group_source":
                        target_state = "review_approved" # Grouped clips are also approved
                        # Logic for 'grouped_with_clip_id' is handled below before the main update
                    elif action == "selected_merge_target":
                        target_state = "pending_merge_target"
                    elif action == "selected_split":
                        target_state = "pending_split"
                        split_at_frame_val = event_data.get("split_at_frame")

                        if split_at_frame_val is not None:
                            try:
                                frame_to_split = int(split_at_frame_val)
                                # The split_clip_task expects "split_request_at_frame"
                                processing_metadata_update_payload = psycopg2.extras.Json({
                                    "split_request_at_frame": frame_to_split
                                })
                                logger.debug(f"[Clip {clip_id}] Action '{action}' will set processing_metadata with split_request_at_frame: {frame_to_split}")
                            except (ValueError, TypeError):
                                logger.warning(
                                    f"[Clip {clip_id}] 'selected_split' action for clip {clip_id} has invalid "
                                    f"'split_at_frame' ('{split_at_frame_val}') in event_data. Skipping commit for this action."
                                )
                                continue # Skip this action if data is bad
                        else:
                            logger.warning(
                                f"[Clip {clip_id}] 'selected_split' action for clip {clip_id} is missing "
                                f"'split_at_frame' in event_data. Skipping commit for this action."
                            )
                            continue # Skip if essential data is missing
                    else:
                        logger.warning(f"[Clip {clip_id}] Unknown action '{action}' encountered during commit. Skipping.")
                        continue

                    # Inner transaction per clip effectively (managed by outer `with conn:` and loop structure)
                    try:
                        # Specific pre-update logic for 'selected_group_source'
                        if action == "selected_group_source":
                            group_target_id = None
                            if "group_with_clip_id" in event_data:
                                try:
                                    group_target_id = int(event_data["group_with_clip_id"])
                                except (ValueError, TypeError):
                                    logger.warning(f"[Clip {clip_id}] Invalid group_with_clip_id '{event_data.get('group_with_clip_id')}' in event_data for grouping.")
                                    group_target_id = None # Ensure it's None if conversion fails

                            if not group_target_id: # Fallback for very old events
                                cur.execute(
                                    """
                                    SELECT event_data ->> 'group_with_clip_id'
                                    FROM   clip_events
                                    WHERE  clip_id = %s AND action  = 'selected_group_source'
                                    ORDER  BY created_at DESC LIMIT  1;
                                    """, (clip_id,)
                                )
                                res = cur.fetchone()
                                if res and res[0]: # res[0] is the value of event_data ->> 'group_with_clip_id'
                                    try:
                                        group_target_id = int(res[0])
                                    except (ValueError, TypeError):
                                        logger.warning(f"[Clip {clip_id}] Invalid group_with_clip_id '{res[0]}' from DB fallback for grouping.")
                                        group_target_id = None


                            if group_target_id:
                                cur.execute(
                                    "UPDATE clips SET grouped_with_clip_id = %s WHERE id = %s;",
                                    (group_target_id, clip_id)
                                )
                                logger.info(f"[Clip {clip_id}] Updated grouped_with_clip_id to {group_target_id} for action '{action}'.")
                            else:
                                logger.warning(
                                    f"[Clip {clip_id}] Group requested but no valid target id found for '{action}'. "
                                    "Leaving grouped_with_clip_id NULL or unchanged."
                                )

                        # General update for ingest_state, action_committed_at, and conditionally processing_metadata
                        if processing_metadata_update_payload:
                            # This is for actions that need to update processing_metadata (e.g., split)
                            cur.execute(
                                """
                                UPDATE clips
                                SET    ingest_state = %s,
                                       action_committed_at = NOW(),
                                       updated_at = NOW(),
                                       processing_metadata = COALESCE(processing_metadata, '{}'::jsonb) || %s::jsonb
                                WHERE  id = %s AND ingest_state = 'pending_review';
                                """, (target_state, processing_metadata_update_payload, clip_id)
                            )
                        else:
                            # This is for actions that DO NOT update processing_metadata
                            cur.execute(
                                """
                                UPDATE clips
                                SET    ingest_state = %s,
                                       action_committed_at = NOW(),
                                       updated_at = NOW()
                                WHERE  id = %s AND ingest_state = 'pending_review';
                                """, (target_state, clip_id)
                            )

                        if cur.rowcount == 1:
                            committed_count += 1
                            logger.info(f"[Clip {clip_id}] Action '{action}' → state '{target_state}' committed.")
                            if processing_metadata_update_payload:
                                logger.info(f"[Clip {clip_id}] Also updated processing_metadata for action '{action}'.")
                        else:
                            logger.warning(
                                f"[Clip {clip_id}] State may have changed concurrently, item not found, or not in 'pending_review'; "
                                f"commit for action '{action}' skipped (rowcount: {cur.rowcount}). Expected state 'pending_review'."
                            )
                    except Exception as e_inner:
                        logger.error(f"[Clip {clip_id}] Inner commit transaction failed for action '{action}': {e_inner}", exc_info=True)
                        # This error, if not re-raised, allows other clips to be processed.
                        # The outer 'with conn:' block ensures atomicity for the entire batch if an error
                        # propagates out of this loop (e.g., from db_err below).
                        # For now, log and continue with other clips as per original structure.

    except (Exception, psycopg2.Error) as db_err: # Catch psycopg2 specific errors too
        logger.error(f"Commit-worker encountered a database error: {db_err}", exc_info=True)
        # If this block is hit, the 'with conn:' context manager will trigger a rollback for all actions in this cycle.
    finally:
        if conn:
            release_db_connection(conn)

    logger.info(f"Finished commit step. Committed {committed_count} clip action(s).")
    return committed_count


# =============================================================================
# ===                        PROCESSING FLOWS                               ===
# =============================================================================

@flow(log_prints=True)
def process_clip_post_review(
    clip_id: int,
    keyframe_strategy: str = DEFAULT_KEYFRAME_STRATEGY,
    model_name: str = DEFAULT_EMBEDDING_MODEL
    ):
    """Processes an approved clip: keyframing and embedding."""
    logger = get_run_logger()
    logger.info(f"FLOW: Starting post-review processing for approved clip_id: {clip_id}")
    keyframe_task_succeeded_or_skipped = False

    try:
        # --- 1. Keyframing ---
        logger.info(f"Submitting keyframe task for clip_id: {clip_id} with strategy: {keyframe_strategy}")
        keyframe_job = extract_keyframes_task.submit(clip_id=clip_id, strategy=keyframe_strategy, overwrite=False)
        logger.info(f"Waiting for keyframe task result for clip {clip_id} (timeout: {KEYFRAME_TIMEOUT}s)")
        keyframe_result = keyframe_job.result(timeout=KEYFRAME_TIMEOUT) # Wait for result

        keyframe_status = None
        if isinstance(keyframe_result, dict): keyframe_status = keyframe_result.get("status")
        elif keyframe_result is None: keyframe_status = "skipped_or_success" # Treat None as acceptable skip/success
        else: keyframe_status = "failed_unexpected_result"

        keyframe_failure_statuses = ["failed", "failed_db_phase1", "failed_processing", "failed_state_update", "failed_db_update", "failed_unexpected", "failed_unexpected_result"]
        keyframe_acceptable_skip_statuses = ["skipped_exists", "skipped_state", "skipped_logic", "skipped_or_success"]

        if keyframe_status == "success":
            logger.info(f"Keyframing task OK for clip_id: {clip_id}.")
            keyframe_task_succeeded_or_skipped = True
        elif keyframe_status in keyframe_acceptable_skip_statuses:
            logger.info(f"Keyframing task for clip {clip_id} status '{keyframe_status}'. Assuming acceptable skip.")
            keyframe_task_succeeded_or_skipped = True
        elif keyframe_status in keyframe_failure_statuses:
            raise RuntimeError(f"Keyframing task failed for clip {clip_id}. Status: {keyframe_status}")
        else: # Unknown status
            raise RuntimeError(f"Keyframing task for clip {clip_id} returned an unknown status: {keyframe_status}")

        # --- 2. Embedding ---
        if keyframe_task_succeeded_or_skipped:
            embedding_strategy_label = f"keyframe_{keyframe_strategy}_avg" if keyframe_strategy == "multi" else f"keyframe_{keyframe_strategy}"
            logger.info(f"Submitting embedding task for clip_id: {clip_id}, model: {model_name}, strategy: {embedding_strategy_label}")
            embedding_job = generate_embeddings_task.submit(clip_id=clip_id, model_name=model_name, generation_strategy=embedding_strategy_label, overwrite=False)
            logger.info(f"Waiting for embedding task result for clip {clip_id} (timeout: {EMBEDDING_TIMEOUT}s)")
            embed_result = embedding_job.result(timeout=EMBEDDING_TIMEOUT) # Wait for result

            embed_status = None
            if isinstance(embed_result, dict): embed_status = embed_result.get("status")
            elif embed_result is None: embed_status = "skipped_or_success" # Treat None as acceptable skip/success
            else: embed_status = "failed_unexpected_result"

            embed_failure_statuses = ["failed", "failed_db_phase1", "failed_processing", "failed_state_update", "failed_db_update", "failed_unexpected", "failed_unexpected_result"]
            embed_acceptable_skip_statuses = ["skipped_exists", "skipped_state", "skipped_or_success"]

            if embed_status == "success":
                logger.info(f"Embedding task OK for clip_id: {clip_id}.")
            elif embed_status in embed_acceptable_skip_statuses:
                logger.info(f"Embedding task for clip {clip_id} status '{embed_status}'. Assuming acceptable skip.")
            elif embed_status in embed_failure_statuses:
                raise RuntimeError(f"Embedding task failed for clip {clip_id}. Status: {embed_status}")
            else: # Unknown status
                raise RuntimeError(f"Embedding task for clip {clip_id} returned an unknown status: {embed_status}")
        else:
            logger.warning(f"Skipping embedding for clip {clip_id} due to prior keyframing outcome.")

    except Exception as e:
        stage = "embedding" if keyframe_task_succeeded_or_skipped else "keyframing"
        logger.error(f"Error during post-review processing flow (stage: {stage}) for clip_id {clip_id}: {e}", exc_info=True)
        raise # Re-raise to mark the flow run as failed
    logger.info(f"FLOW: Finished post-review processing flow for clip_id: {clip_id}")


@flow(name="Scheduled Ingest Initiator", log_prints=True)
def scheduled_ingest_initiator(limit_per_stage: int = 50):
    """
    Scheduled flow to:
    1. Commit pending review actions.
    2. Find new work based on finalized states.
    3. Trigger appropriate next tasks/flows, respecting MAX_NEW_SUBMISSIONS_PER_CYCLE.
    """
    logger = get_run_logger()
    logger.info(f"FLOW: Running Scheduled Ingest Initiator cycle (Global Submission Limit: {MAX_NEW_SUBMISSIONS_PER_CYCLE}, Stage Limit: {limit_per_stage})...")

    try:
        initialize_db_pool() # Ensure sync DB pool is ready for commit logic and state updates
    except Exception as pool_init_err:
         logger.critical(f"Failed to initialize sync DB pool for initiator: {pool_init_err}", exc_info=True)
         raise RuntimeError("Cannot proceed with Scheduled Ingest Initiator without DB pool.") from pool_init_err

    error_count = 0
    newly_submitted_in_this_cycle = 0 # Counter for MAX_NEW_SUBMISSIONS_PER_CYCLE
    from collections import defaultdict
    processed_counts = defaultdict(int) # How many items found per stage
    submitted_counts = defaultdict(int) # How many tasks/runs actually submitted per stage

    # --- First, commit pending review actions ---
    try:
        logger.info(f"Running commit step with grace period: {ACTION_COMMIT_GRACE_PERIOD_SECONDS} seconds...")
        committed_clips_count = _commit_pending_review_actions(grace_period_seconds=ACTION_COMMIT_GRACE_PERIOD_SECONDS)
        logger.info(f"Commit step finished, {committed_clips_count} clip states finalized.")
    except Exception as commit_err:
        logger.error(f"Error during the commit actions step: {commit_err}", exc_info=True)
        error_count += 1 # Log error, but continue to find other work

    # --- Fetch Work (based on potentially updated states) ---
    all_work = []
    if newly_submitted_in_this_cycle < MAX_NEW_SUBMISSIONS_PER_CYCLE: # Only fetch if we can submit more
        try:
            logger.info("Fetching work items based on committed states...")
            all_work = get_all_pending_work(limit_per_stage=limit_per_stage)
            logger.info(f"Found {len(all_work)} total work items across stages for potential processing.")
        except Exception as db_query_err:
            logger.error(f"[All Stages] Failed to query for work: {db_query_err}", exc_info=True)
            error_count += 1
    else:
        logger.info("MAX_NEW_SUBMISSIONS_PER_CYCLE reached before fetching work. Skipping work fetch.")


    work_by_stage = defaultdict(list)
    for item in all_work: # all_work might be empty if MAX_NEW_SUBMISSIONS_PER_CYCLE was hit
        work_by_stage[item['stage']].append(item['id'])
        processed_counts[item['stage'].capitalize()] += 1

    # --- Process Stages ---

    # Stage 1: Intake
    stage_name = "Intake"; intake_ids = work_by_stage.get('intake', [])
    if intake_ids and newly_submitted_in_this_cycle < MAX_NEW_SUBMISSIONS_PER_CYCLE:
        logger.info(f"[{stage_name}] Found {len(intake_ids)} sources. Submitting tasks...")
        for sid in intake_ids:
            if newly_submitted_in_this_cycle >= MAX_NEW_SUBMISSIONS_PER_CYCLE:
                logger.info(f"[{stage_name}] MAX_NEW_SUBMISSIONS_PER_CYCLE reached. Deferring remaining {stage_name} work.")
                break
            try:
                if update_source_video_state_sync(sid, 'downloading'):
                    source_input = get_source_input_from_db(sid)
                    if source_input:
                        intake_task.submit(source_video_id=sid, input_source=source_input)
                        submitted_counts[stage_name] += 1
                        newly_submitted_in_this_cycle += 1
                        time.sleep(TASK_SUBMIT_DELAY)
                    else:
                        logger.warning(f"[{stage_name}] No source_input found for ID {sid} after update. Skipping.")
                else:
                    logger.warning(f"[{stage_name}] Failed state update for source ID {sid} to 'downloading'. Skipping task.")
                    error_count += 1 # Count as an error if state update fails
            except Exception as e:
                logger.error(f"[{stage_name}] Submit failed for source ID {sid}: {e}", exc_info=True)
                error_count += 1

    # Stage 2: Splice
    stage_name = "Splice"; splice_ids = work_by_stage.get('splice', [])
    if splice_ids and newly_submitted_in_this_cycle < MAX_NEW_SUBMISSIONS_PER_CYCLE:
        logger.info(f"[{stage_name}] Found {len(splice_ids)} sources. Submitting tasks...")
        for sid in splice_ids:
            if newly_submitted_in_this_cycle >= MAX_NEW_SUBMISSIONS_PER_CYCLE:
                logger.info(f"[{stage_name}] MAX_NEW_SUBMISSIONS_PER_CYCLE reached. Deferring remaining {stage_name} work.")
                break
            try:
                if update_source_video_state_sync(sid, 'splicing'):
                    splice_video_task.submit(source_video_id=sid)
                    submitted_counts[stage_name] += 1
                    newly_submitted_in_this_cycle += 1
                    time.sleep(TASK_SUBMIT_DELAY)
                else:
                    logger.warning(f"[{stage_name}] Failed state update for source ID {sid} to 'splicing'. Skipping task.")
                    error_count += 1
            except Exception as e:
                logger.error(f"[{stage_name}] Submit failed for source ID {sid}: {e}", exc_info=True)
                error_count += 1

    # Stage 3: SpriteGen
    stage_name = "SpriteGen"; sprite_ids = work_by_stage.get('sprite', [])
    if sprite_ids and newly_submitted_in_this_cycle < MAX_NEW_SUBMISSIONS_PER_CYCLE:
        logger.info(f"[{stage_name}] Found {len(sprite_ids)} clips. Submitting tasks...")
        for cid in sprite_ids:
            if newly_submitted_in_this_cycle >= MAX_NEW_SUBMISSIONS_PER_CYCLE:
                logger.info(f"[{stage_name}] MAX_NEW_SUBMISSIONS_PER_CYCLE reached. Deferring remaining {stage_name} work.")
                break
            try:
                if update_clip_state_sync(cid, 'generating_sprite'):
                    generate_sprite_sheet_task.submit(clip_id=cid)
                    submitted_counts[stage_name] += 1
                    newly_submitted_in_this_cycle += 1
                    time.sleep(TASK_SUBMIT_DELAY)
                else:
                    logger.warning(f"[{stage_name}] Failed state update for clip ID {cid} to 'generating_sprite'. Skipping task.")
                    error_count += 1
            except Exception as e:
                logger.error(f"[{stage_name}] Submit failed for clip ID {cid}: {e}", exc_info=True)
                error_count += 1

    # Stage 4: Post-Review Start (Triggers a separate flow run)
    stage_name = "Post-Review Start"; post_review_ids = work_by_stage.get('post_review', [])
    if post_review_ids and newly_submitted_in_this_cycle < MAX_NEW_SUBMISSIONS_PER_CYCLE:
        deployment_name = "process-clip-post-review/process-clip-post-review-default" # Ensure this matches your deployment
        logger.info(f"[{stage_name}] Found {len(post_review_ids)} approved clips. Triggering '{deployment_name}' runs...")
        for cid in post_review_ids:
            if newly_submitted_in_this_cycle >= MAX_NEW_SUBMISSIONS_PER_CYCLE:
                logger.info(f"[{stage_name}] MAX_NEW_SUBMISSIONS_PER_CYCLE reached. Deferring remaining {stage_name} work.")
                break
            try:
                if update_clip_state_sync(cid, 'processing_post_review'):
                    run_deployment(
                        name=deployment_name,
                        parameters={"clip_id": cid, "keyframe_strategy": DEFAULT_KEYFRAME_STRATEGY, "model_name": DEFAULT_EMBEDDING_MODEL},
                        timeout=0 # Submit and move on, don't wait for completion
                    )
                    submitted_counts[stage_name] += 1
                    newly_submitted_in_this_cycle += 1
                    time.sleep(TASK_SUBMIT_DELAY)
                else:
                    logger.warning(f"[{stage_name}] Failed state update for clip ID {cid} to 'processing_post_review'. Skipping trigger.")
                    error_count += 1
            except Exception as e:
                logger.error(f"[{stage_name}] Failed to trigger deployment '{deployment_name}' for clip ID {cid}: {e}", exc_info=True)
                error_count += 1

    # Stage 5.1: Merge
    stage_name = "Merge"
    if newly_submitted_in_this_cycle < MAX_NEW_SUBMISSIONS_PER_CYCLE:
        try:
            merge_pairs = get_pending_merge_pairs()
            processed_counts[stage_name] = len(merge_pairs) # Count found items before submission limit
            if merge_pairs:
                 logger.info(f"[{stage_name}] Found {len(merge_pairs)} pairs for merging. Submitting tasks...")
                 submitted_merges_in_loop = set() # Track submitted items within this loop to avoid double submission
                 for target_id, source_id in merge_pairs:
                      if newly_submitted_in_this_cycle >= MAX_NEW_SUBMISSIONS_PER_CYCLE:
                          logger.info(f"[{stage_name}] MAX_NEW_SUBMISSIONS_PER_CYCLE reached. Deferring remaining {stage_name} work.")
                          break
                      if target_id not in submitted_merges_in_loop and source_id not in submitted_merges_in_loop:
                          try:
                              merge_clips_task.submit(clip_id_target=target_id, clip_id_source=source_id)
                              submitted_merges_in_loop.add(target_id); submitted_merges_in_loop.add(source_id)
                              submitted_counts[stage_name] += 1 # Count as one submission for the pair
                              newly_submitted_in_this_cycle += 1
                              time.sleep(TASK_SUBMIT_DELAY)
                          except Exception as e:
                              logger.error(f"[{stage_name}] Submit failed for merge pair (T:{target_id}, S:{source_id}): {e}", exc_info=True)
                              error_count += 1
                      else:
                          logger.warning(f"[{stage_name}] Skipping merge involving {target_id}/{source_id} as one part might be in current submission batch.")
        except Exception as e:
            logger.error(f"[{stage_name}] Failed during merge check/submission: {e}", exc_info=True)
            error_count += 1
    else:
        logger.info(f"[{stage_name}] Skipping merge processing as MAX_NEW_SUBMISSIONS_PER_CYCLE already reached.")


    # Stage 5.2: Split
    stage_name = "Split"
    if newly_submitted_in_this_cycle < MAX_NEW_SUBMISSIONS_PER_CYCLE:
        try:
            clips_to_split_data = get_pending_split_jobs()
            processed_counts[stage_name] = len(clips_to_split_data) # Count found items
            if clips_to_split_data:
                logger.info(f"[{stage_name}] Found {len(clips_to_split_data)} clips pending split. Submitting tasks...")
                submitted_splits_in_loop = set()
                for cid, split_frame in clips_to_split_data:
                    if newly_submitted_in_this_cycle >= MAX_NEW_SUBMISSIONS_PER_CYCLE:
                        logger.info(f"[{stage_name}] MAX_NEW_SUBMISSIONS_PER_CYCLE reached. Deferring remaining {stage_name} work.")
                        break
                    if cid not in submitted_splits_in_loop:
                        try:
                            split_clip_task.submit(clip_id=cid) # Assuming split_frame is handled by task or fetched again
                            submitted_splits_in_loop.add(cid)
                            submitted_counts[stage_name] += 1
                            newly_submitted_in_this_cycle += 1
                            time.sleep(TASK_SUBMIT_DELAY)
                        except Exception as e:
                            logger.error(f"[{stage_name}] Submit failed for split of clip ID {cid}: {e}", exc_info=True)
                            error_count += 1
                    else:
                        logger.warning(f"[{stage_name}] Skipping duplicate split task submission for clip {cid} in this cycle.")
        except Exception as e:
            logger.error(f"[{stage_name}] Failed during split check/submission: {e}", exc_info=True)
            error_count += 1
    else:
        logger.info(f"[{stage_name}] Skipping split processing as MAX_NEW_SUBMISSIONS_PER_CYCLE already reached.")


    # --- Completion Logging ---
    summary_log = (
        f"FLOW: Scheduled Ingest Initiator cycle complete. "
        f"Items Found (prior to submission limit): {dict(processed_counts)}. "
        f"Tasks/Runs Submitted This Cycle: {dict(submitted_counts)} (Total: {newly_submitted_in_this_cycle})."
    )
    if error_count > 0:
        logger.warning(f"{summary_log} Completed with {error_count} error(s).")
    else:
        logger.info(summary_log)


# =============================================================================
# ===                  CLEANUP FLOW                                         ===
# =============================================================================

@flow(name="Scheduled Clip Cleanup", log_prints=True)
async def cleanup_reviewed_clips_flow(
    cleanup_delay_minutes: int = CLIP_CLEANUP_DELAY_MINUTES,
):
    """
    Cleans up reviewed clips by deleting associated S3 artifacts (sprite sheets)
    and updating their database state. Uses asyncpg for DB operations.
    """
    logger = get_run_logger()
    logger.info(
        f"FLOW: Running Scheduled Clip Cleanup (Delay: {cleanup_delay_minutes} mins)..."
    )

    if not S3_CONFIGURED or not s3_client:
        logger.error("S3 Client/Config not available. Exiting cleanup flow.")
        return

    if not ASYNC_DB_CONFIGURED or not get_async_db_connection:
        logger.error("Async DB not configured or get_async_db_connection function unavailable. Exiting cleanup flow.")
        return

    processed_count = 0
    s3_deleted_count = 0
    db_artifact_deleted_count = 0
    db_clip_updated_count = 0
    error_count = 0

    try:
        async with get_async_db_connection() as conn:
            logger.info("Acquired asyncpg connection via context manager for cleanup flow.")

            delay_interval = timedelta(minutes=cleanup_delay_minutes)
            pending_states = [
                "approved_pending_deletion",
                "archived_pending_deletion",
            ]
            query_clips = """
            SELECT id, ingest_state
              FROM clips
             WHERE ingest_state = ANY($1::text[])
               AND action_committed_at IS NOT NULL
               AND action_committed_at < (NOW() - $2::INTERVAL)
             ORDER BY id ASC;
            """

            clips_to_cleanup = await conn.fetch(
                query_clips, pending_states, delay_interval
            )
            processed_count = len(clips_to_cleanup)
            logger.info(f"Found {processed_count} clips ready for S3 artifact cleanup.")

            if not clips_to_cleanup:
                logger.info("No clips require S3 artifact cleanup at this time.")
                return # Exit early if no work

            for clip_record in clips_to_cleanup:
                clip_id = clip_record["id"]
                current_state = clip_record["ingest_state"]
                log_prefix = f"[Cleanup Clip {clip_id}]"
                logger.info(f"{log_prefix} Processing for state: {current_state}")

                sprite_artifact_s3_key = None
                s3_deletion_successful = False # Assume failure until success

                try:
                    # 1. Retrieve sprite artifact S3 key
                    artifact_query = """
                    SELECT s3_key FROM clip_artifacts
                     WHERE clip_id = $1 AND artifact_type = $2 LIMIT 1;
                    """
                    artifact_record = await conn.fetchrow(
                        artifact_query, clip_id, ARTIFACT_TYPE_SPRITE_SHEET
                    )

                    if artifact_record and artifact_record["s3_key"]:
                        sprite_artifact_s3_key = artifact_record["s3_key"]
                    else:
                        logger.warning(f"{log_prefix} No sprite sheet artifact found in DB. Assuming S3 deletion step can be skipped.")
                        s3_deletion_successful = True # No S3 key to delete, so "success" for this step

                    # 2. Delete from S3 if key exists
                    if sprite_artifact_s3_key:
                        if not S3_BUCKET_NAME:
                            logger.error(f"{log_prefix} S3_BUCKET_NAME is not configured. Cannot delete artifact.")
                            error_count += 1
                            s3_deletion_successful = False # Explicitly mark as failed
                        else:
                            try:
                                logger.info(f"{log_prefix} Deleting S3 object: s3://{S3_BUCKET_NAME}/{sprite_artifact_s3_key}")
                                s3_client.delete_object(
                                    Bucket=S3_BUCKET_NAME,
                                    Key=sprite_artifact_s3_key,
                                )
                                logger.info(f"{log_prefix} Successfully deleted S3 object.")
                                s3_deletion_successful = True
                                s3_deleted_count += 1
                            except ClientError as e:
                                error_code = e.response.get("Error", {}).get("Code", "Unknown")
                                if error_code == "NoSuchKey":
                                    logger.warning(f"{log_prefix} S3 object s3://{S3_BUCKET_NAME}/{sprite_artifact_s3_key} not found (NoSuchKey). Assuming already deleted.")
                                    s3_deletion_successful = True # Treat as success for workflow progression
                                else:
                                    logger.error(f"{log_prefix} Failed to delete S3 object s3://{S3_BUCKET_NAME}/{sprite_artifact_s3_key}. Code: {error_code}, Error: {e}")
                                    s3_deletion_successful = False
                                    error_count += 1
                            except Exception as e_s3: # Catch other unexpected S3 errors
                                logger.error(f"{log_prefix} Unexpected error during S3 deletion: {e_s3}", exc_info=True)
                                s3_deletion_successful = False
                                error_count += 1

                    # 3. Proceed with DB updates ONLY if S3 deletion was successful (or skipped appropriately)
                    if s3_deletion_successful:
                        final_state_map = {
                            "approved_pending_deletion": "review_approved",
                            "archived_pending_deletion": "archived",
                        }
                        final_clip_state = final_state_map.get(current_state)

                        if not final_clip_state:
                            logger.error(f"{log_prefix} Logic error: No final state mapping for current state '{current_state}'.")
                            error_count += 1
                            continue # Skip to next clip

                        # Start a DB transaction for artifact deletion and clip state update
                        async with conn.transaction():
                            logger.debug(f"{log_prefix} Starting DB transaction for final updates.")
                            # Delete artifact record from DB if it existed
                            if sprite_artifact_s3_key: # Only delete if we had a key
                                delete_artifact_sql = """
                                DELETE FROM clip_artifacts WHERE clip_id = $1 AND artifact_type = $2;
                                """
                                del_res_str = await conn.execute(
                                    delete_artifact_sql, clip_id, ARTIFACT_TYPE_SPRITE_SHEET
                                )
                                # Parse "DELETE N" string
                                deleted_artifact_rows = int(del_res_str.split()[-1]) if del_res_str and del_res_str.startswith("DELETE") else 0
                                if deleted_artifact_rows > 0:
                                    logger.info(f"{log_prefix} Deleted {deleted_artifact_rows} artifact record(s) from DB.")
                                    db_artifact_deleted_count += deleted_artifact_rows
                                else:
                                    logger.warning(f"{log_prefix} No clip_artifact record found/deleted for S3 key {sprite_artifact_s3_key}. This might be okay if S3 key was stale.")


                            # Update clip state and clear action_committed_at
                            update_clip_sql = """
                            UPDATE clips
                               SET ingest_state = $1,
                                   updated_at = NOW(),
                                   action_committed_at = NULL, -- Clear this as action is now complete
                                   last_error = NULL           -- Clear any previous errors
                             WHERE id = $2 AND ingest_state = $3; -- Ensure current state matches
                            """
                            upd_res_str = await conn.execute(
                                update_clip_sql, final_clip_state, clip_id, current_state
                            )
                            # Parse "UPDATE N" string
                            updated_clip_rows = int(upd_res_str.split()[-1]) if upd_res_str and upd_res_str.startswith("UPDATE") else 0

                            if updated_clip_rows == 1:
                                logger.info(f"{log_prefix} Successfully updated clip state to '{final_clip_state}'.")
                                db_clip_updated_count += 1
                            else:
                                # This could happen if the state was changed by another process after fetching clips_to_cleanup
                                logger.warning(
                                    f"{log_prefix} Clip state update to '{final_clip_state}' affected {updated_clip_rows} rows "
                                    f"(expected 1). State might have changed concurrently from '{current_state}'. Transaction will rollback."
                                )
                                # Raise an error to ensure transaction rollback for this clip
                                raise RuntimeError(f"Concurrent state change detected for clip {clip_id}")
                        logger.debug(f"{log_prefix} DB transaction committed.")
                    else:
                        logger.warning(f"{log_prefix} Skipping DB updates for clip {clip_id} due to S3 deletion failure or configuration issue.")

                except Exception as inner_err: # Catch errors within the loop for a single clip
                    logger.error(f"{log_prefix} Error processing clip for cleanup: {inner_err}", exc_info=True)
                    error_count += 1
                    # Continue to the next clip

    except Exception as outer_err: # Catch errors related to DB connection or the overall flow
        logger.error(f"FATAL Error during scheduled clip cleanup flow: {outer_err}", exc_info=True)
        error_count += 1
    # No explicit pool or connection management needed here, context manager handles it.

    logger.info(
        f"FLOW: Scheduled Clip Cleanup complete. "
        f"Clips Found: {processed_count}, S3 Objects Deleted: {s3_deleted_count}, "
        f"DB Artifacts Deleted: {db_artifact_deleted_count}, DB Clips Updated: {db_clip_updated_count}, "
        f"Errors: {error_count}"
    )
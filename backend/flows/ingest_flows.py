import sys
import os
from pathlib import Path
import traceback
import time
import psycopg2
import psycopg2.extras
from datetime import timedelta, timezone

# --- Project Root Setup ---
project_root = Path(__file__).parent.parent.resolve()
if str(project_root) not in sys.path:
    print(f"Adding project root to sys.path: {project_root}")
    sys.path.insert(0, str(project_root))

# --- Prefect Imports ---
from prefect import flow, get_run_logger, task
from prefect.deployments import run_deployment # For triggering independent flow runs

from tasks.intake import intake_task
from tasks.splice import splice_video_task, s3_client, S3_BUCKET_NAME
from tasks.sprite import generate_sprite_sheet_task
from tasks.keyframe import extract_keyframes_task
from tasks.embed import generate_embeddings_task
from tasks.merge import merge_clips_task
from tasks.split import split_clip_task

# --- DB Util Imports ---
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

# --- Async DB and S3 client imports ---
try:
    from backend.db.async_db import get_db_pool, close_db_pool
    ASYNC_DB_CONFIGURED = True
except ImportError:
    ASYNC_DB_CONFIGURED = False
    get_db_pool, close_db_pool = None, None

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
TASK_SUBMIT_DELAY = float(os.getenv("TASK_SUBMIT_DELAY", 0.1))
KEYFRAME_TIMEOUT = int(os.getenv("KEYFRAME_TIMEOUT", 600))
EMBEDDING_TIMEOUT = int(os.getenv("EMBEDDING_TIMEOUT", 900))
CLIP_CLEANUP_DELAY_MINUTES = int(os.getenv("CLIP_CLEANUP_DELAY_MINUTES", 30))
ACTION_COMMIT_GRACE_PERIOD_SECONDS = int(os.getenv("ACTION_COMMIT_GRACE_PERIOD_SECONDS", 10))

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
        # RealDictCursor → rows behave like dicts
        conn = get_db_connection(cursor_factory=psycopg2.extras.RealDictCursor)

        with conn:                              # outer TX
            with conn.cursor() as cur:

                # 1) newest selected_* event per clip that’s ready to commit
                find_sql = """
                WITH latest AS (
                  SELECT  ce.clip_id,
                          ce.action,
                          ce.created_at,
                          ce.event_data,
                          ROW_NUMBER() OVER (PARTITION BY ce.clip_id
                                             ORDER BY ce.created_at DESC) AS rn
                  FROM    clip_events ce
                  WHERE   ce.action LIKE 'selected_%%'
                    AND   ce.action <> 'selected_undo'
                )
                SELECT  l.clip_id,
                        l.action,
                        l.event_data,
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

                # 2) process each clip in turn
                for row in actions:
                    clip_id    = row["clip_id"]
                    action     = row["action"]
                    event_data = row.get("event_data") or {}
                    target_state = None

                    # ---------- mapping UI action → ingest_state --------------
                    if   action == "selected_approve":
                        target_state = "review_approved"
                    elif action == "selected_skip":
                        target_state = "skipped"
                    elif action == "selected_archive":
                        target_state = "archived_pending_deletion"
                    elif action == "selected_merge_source":
                        target_state = "marked_for_merge_into_previous"
                    elif action == "selected_group_source":
                        target_state = "review_approved"
                    elif action == "selected_merge_target":
                        target_state = "pending_merge_target"
                    else:
                        logger.warning(f"[Clip {clip_id}] Unknown action '{action}'.")
                        continue

                    # ----------- one inner TX per clip ------------------------
                    try:
                        with conn.cursor() as tx:

                            # 2.a  write grouped_with_clip_id if grouping
                            if action == "selected_group_source":
                                group_target_id = None

                                # prefer payload captured when event logged
                                if "group_with_clip_id" in event_data:
                                    group_target_id = int(event_data["group_with_clip_id"])

                                # fallback for very old events (no payload)
                                if not group_target_id:
                                    tx.execute(
                                        """
                                        SELECT event_data ->> 'group_with_clip_id'
                                        FROM   clip_events
                                        WHERE  clip_id = %s
                                          AND  action  = 'selected_group_source'
                                        ORDER  BY created_at DESC
                                        LIMIT  1;
                                        """,
                                        (clip_id,),
                                    )
                                    res = tx.fetchone()
                                    if res and res[0]:
                                        group_target_id = int(res[0])

                                if group_target_id:
                                    tx.execute(
                                        """
                                        UPDATE clips
                                        SET    grouped_with_clip_id = %s
                                        WHERE  id = %s;
                                        """,
                                        (group_target_id, clip_id),
                                    )
                                else:
                                    logger.warning(
                                        f"[Clip {clip_id}] Group requested but "
                                        "no target id found – leaving grouped_with_clip_id NULL."
                                    )

                            # 2.b  set new ingest_state + action_committed_at
                            tx.execute(
                                """
                                UPDATE clips
                                SET    ingest_state        = %s,
                                       action_committed_at = NOW(),
                                       updated_at          = NOW()
                                WHERE  id = %s
                                  AND  ingest_state = 'pending_review';
                                """,
                                (target_state, clip_id),
                            )

                            if tx.rowcount == 1:
                                committed_count += 1
                                logger.info(
                                    f"[Clip {clip_id}] '{action}' → '{target_state}' committed."
                                )
                            else:
                                logger.warning(
                                    f"[Clip {clip_id}] ingest_state changed concurrently; "
                                    "commit skipped."
                                )

                    except Exception as e:
                        logger.error(f"[Clip {clip_id}] commit failed: {e}", exc_info=True)
                        # keep looping

    except (Exception, psycopg2.Error) as db_err:
        logger.error(f"Commit‑worker DB error: {db_err}", exc_info=True)
    finally:
        if conn:
            release_db_connection(conn)

    logger.info(f"Finished commit step. Committed {committed_count} clip(s).")
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
    """ (No changes needed in this flow logic itself) """
    logger = get_run_logger(); logger.info(f"FLOW: Starting post-review processing for approved clip_id: {clip_id}")
    keyframe_task_succeeded_or_skipped = False; keyframe_job = None; embedding_job = None
    try:
        # --- 1. Keyframing ---
        logger.info(f"Submitting keyframe task for clip_id: {clip_id} with strategy: {keyframe_strategy}")
        keyframe_job = extract_keyframes_task.submit(clip_id=clip_id, strategy=keyframe_strategy, overwrite=False)
        logger.info(f"Waiting for keyframe task result for clip {clip_id} (timeout: {KEYFRAME_TIMEOUT}s)")
        keyframe_result = keyframe_job.result(timeout=KEYFRAME_TIMEOUT)
        keyframe_status = None
        if isinstance(keyframe_result, dict): keyframe_status = keyframe_result.get("status")
        elif keyframe_result is None: keyframe_status = "skipped_or_success"
        else: keyframe_status = "failed_unexpected_result"
        keyframe_failure_statuses = ["failed", "failed_db_phase1", "failed_processing", "failed_state_update", "failed_db_update", "failed_unexpected", "failed_unexpected_result"]
        keyframe_acceptable_skip_statuses = ["skipped_exists", "skipped_state", "skipped_logic", "skipped_or_success"]
        if keyframe_status == "success": logger.info(f"Keyframing task OK for clip_id: {clip_id}."); keyframe_task_succeeded_or_skipped = True
        elif keyframe_status in keyframe_acceptable_skip_statuses: logger.info(f"Keyframing task for clip {clip_id} status '{keyframe_status}'. Assuming acceptable."); keyframe_task_succeeded_or_skipped = True
        elif keyframe_status in keyframe_failure_statuses: raise RuntimeError(f"Keyframing task failed for clip {clip_id}. Status: {keyframe_status}")
        else: raise RuntimeError(f"Keyframing task failed for clip {clip_id} with unknown status: {keyframe_status}")

        # --- 2. Embedding ---
        if keyframe_task_succeeded_or_skipped:
            if keyframe_strategy == "multi": embedding_strategy_label = f"keyframe_{keyframe_strategy}_avg"
            else: embedding_strategy_label = f"keyframe_{keyframe_strategy}"
            logger.info(f"Submitting embedding task for clip_id: {clip_id} model: {model_name}, strategy: {embedding_strategy_label}")
            embedding_job = generate_embeddings_task.submit(clip_id=clip_id, model_name=model_name, generation_strategy=embedding_strategy_label, overwrite=False)
            logger.info(f"Waiting for embedding task result for clip {clip_id} (timeout: {EMBEDDING_TIMEOUT}s)")
            embed_result = embedding_job.result(timeout=EMBEDDING_TIMEOUT)
            embed_status = None
            if isinstance(embed_result, dict): embed_status = embed_result.get("status")
            elif embed_result is None: embed_status = "skipped_or_success"
            else: embed_status = "failed_unexpected_result"
            embed_failure_statuses = ["failed", "failed_db_phase1", "failed_processing", "failed_state_update", "failed_db_update", "failed_unexpected", "failed_unexpected_result"]
            embed_acceptable_skip_statuses = ["skipped_exists", "skipped_state", "skipped_or_success"]
            if embed_status == "success": logger.info(f"Embedding task OK for clip_id: {clip_id}.")
            elif embed_status in embed_acceptable_skip_statuses: logger.info(f"Embedding task for clip {clip_id} status '{embed_status}'. Assuming acceptable.")
            elif embed_status in embed_failure_statuses: raise RuntimeError(f"Embedding task failed for clip {clip_id}. Status: {embed_status}")
            else: raise RuntimeError(f"Embedding task failed for clip {clip_id} with unknown status: {embed_status}")
        else: logger.warning(f"Skipping embedding for clip {clip_id} due to keyframing outcome.")
    except Exception as e: stage = "embedding" if keyframe_task_succeeded_or_skipped else "keyframing"; logger.error(f"Error during post-review processing flow (stage: {stage}) for clip_id {clip_id}: {e}", exc_info=True); raise e
    logger.info(f"FLOW: Finished post-review processing flow for clip_id: {clip_id}")


@flow(name="Scheduled Ingest Initiator", log_prints=True)
def scheduled_ingest_initiator(limit_per_stage: int = 50):
    """
    Scheduled flow to FIRST commit pending review actions, THEN find new work
    based on FINALIZED states, and trigger the appropriate next tasks/flows.
    """
    logger = get_run_logger()
    logger.info(f"FLOW: Running Scheduled Ingest Initiator cycle (Limit: {limit_per_stage}/stage)...")

    try: initialize_db_pool() # For sync state updates and commit logic
    except Exception as pool_init_err:
         logger.critical(f"Failed init sync DB pool for initiator: {pool_init_err}", exc_info=True)
         raise RuntimeError("Cannot proceed without DB pool.") from pool_init_err

    error_count = 0
    from collections import defaultdict
    processed_counts = defaultdict(int)
    submitted_counts = defaultdict(int)

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
    try:
        logger.info(f"Fetching work items based on committed states...")
        # Calls the simplified get_all_pending_work from db_utils
        all_work = get_all_pending_work(limit_per_stage=limit_per_stage)
        logger.info(f"Found {len(all_work)} total work items across stages.")
    except Exception as db_query_err:
        logger.error(f"[All Stages] Failed work query: {db_query_err}", exc_info=True)
        error_count += 1

    work_by_stage = defaultdict(list)
    for item in all_work:
        work_by_stage[item['stage']].append(item['id'])
        processed_counts[item['stage'].capitalize()] += 1

    # --- Process Stages ---

    # Stage 1: Intake
    stage_name = "Intake"; intake_ids = work_by_stage.get('intake', [])
    if intake_ids:
        logger.info(f"[{stage_name}] Found {len(intake_ids)} sources. Submitting tasks...")
        for sid in intake_ids:
            try:
                if update_source_video_state_sync(sid, 'downloading'):
                    source_input = get_source_input_from_db(sid)
                    if source_input: intake_task.submit(source_video_id=sid, input_source=source_input); submitted_counts[stage_name] += 1; time.sleep(TASK_SUBMIT_DELAY)
                else: logger.warning(f"[{stage_name}] Failed state update for {sid} to 'downloading'. Skipping."); error_count += 1
            except Exception as e: logger.error(f"[{stage_name}] Submit failed for ID {sid}: {e}", exc_info=True); error_count += 1

    # Stage 2: Splice
    stage_name = "Splice"; splice_ids = work_by_stage.get('splice', [])
    if splice_ids:
        logger.info(f"[{stage_name}] Found {len(splice_ids)} sources. Submitting tasks...")
        for sid in splice_ids:
            try:
                if update_source_video_state_sync(sid, 'splicing'): splice_video_task.submit(source_video_id=sid); submitted_counts[stage_name] += 1; time.sleep(TASK_SUBMIT_DELAY)
                else: logger.warning(f"[{stage_name}] Failed state update for {sid} to 'splicing'. Skipping."); error_count += 1
            except Exception as e: logger.error(f"[{stage_name}] Submit failed for ID {sid}: {e}", exc_info=True); error_count += 1

    # Stage 3: SpriteGen
    stage_name = "SpriteGen"; sprite_ids = work_by_stage.get('sprite', [])
    if sprite_ids:
        logger.info(f"[{stage_name}] Found {len(sprite_ids)} clips. Submitting tasks...")
        for cid in sprite_ids:
            try:
                if update_clip_state_sync(cid, 'generating_sprite'): generate_sprite_sheet_task.submit(clip_id=cid); submitted_counts[stage_name] += 1; time.sleep(TASK_SUBMIT_DELAY)
                else: logger.warning(f"[{stage_name}] Failed state update for {cid} to 'generating_sprite'. Skipping."); error_count += 1
            except Exception as e: logger.error(f"[{stage_name}] Submit failed for ID {cid}: {e}", exc_info=True); error_count += 1

    # Stage 4: Post-Review Start
    stage_name = "Post-Review Start"; post_review_ids = work_by_stage.get('post_review', [])
    if post_review_ids:
        deployment_name = "process-clip-post-review/process-clip-post-review-default"
        logger.info(f"[{stage_name}] Found {len(post_review_ids)} approved clips. Triggering '{deployment_name}' runs...")
        for cid in post_review_ids:
            try:
                if update_clip_state_sync(cid, 'processing_post_review'): run_deployment(name=deployment_name, parameters={"clip_id": cid, "keyframe_strategy": DEFAULT_KEYFRAME_STRATEGY, "model_name": DEFAULT_EMBEDDING_MODEL}, timeout=0); submitted_counts[stage_name] += 1; time.sleep(TASK_SUBMIT_DELAY)
                else: logger.warning(f"[{stage_name}] Failed state update for {cid} to 'processing_post_review'. Skipping trigger."); error_count += 1
            except Exception as e: logger.error(f"[{stage_name}] Failed trigger for '{deployment_name}', clip_id {cid}: {e}", exc_info=True); error_count += 1

    # Stage 5.1: Merge
    stage_name = "Merge"
    try:
        merge_pairs = get_pending_merge_pairs()
        processed_counts[stage_name] = len(merge_pairs)
        if merge_pairs:
             logger.info(f"[{stage_name}] Found {len(merge_pairs)} pairs for merging. Submitting tasks...")
             submitted_merges = set()
             for target_id, source_id in merge_pairs:
                  if target_id not in submitted_merges and source_id not in submitted_merges:
                      try: merge_clips_task.submit(clip_id_target=target_id, clip_id_source=source_id); submitted_merges.add(target_id); submitted_merges.add(source_id); submitted_counts[stage_name] += 1; time.sleep(TASK_SUBMIT_DELAY)
                      except Exception as e: logger.error(f"[{stage_name}] Submit failed for merge ({target_id}, {source_id}): {e}", exc_info=True); error_count += 1
                  else: logger.warning(f"[{stage_name}] Skipping merge involving {target_id}/{source_id} (already submitted).")
    except Exception as e: logger.error(f"[{stage_name}] Failed during merge check: {e}", exc_info=True); error_count += 1

    # Stage 5.2: Split
    stage_name = "Split"
    try:
        clips_to_split_data = get_pending_split_jobs()
        processed_counts[stage_name] = len(clips_to_split_data)
        if clips_to_split_data:
            logger.info(f"[{stage_name}] Found {len(clips_to_split_data)} clips pending split. Submitting tasks...")
            submitted_splits = set()
            for cid, split_frame in clips_to_split_data:
                if cid not in submitted_splits:
                    try: split_clip_task.submit(clip_id=cid); submitted_splits.add(cid); submitted_counts[stage_name] += 1; time.sleep(TASK_SUBMIT_DELAY)
                    except Exception as e: logger.error(f"[{stage_name}] Submit failed for split ID {cid}: {e}", exc_info=True); error_count += 1
                else: logger.warning(f"[{stage_name}] Skipping duplicate split task for clip {cid}.")
    except Exception as e: logger.error(f"[{stage_name}] Failed during split check: {e}", exc_info=True); error_count += 1

    # --- Completion Logging ---
    summary_log = (f"FLOW: Scheduled Ingest Initiator cycle complete. "
                   f"Items Found: {dict(processed_counts)}. "
                   f"Tasks/Runs Submitted: {dict(submitted_counts)}.")
    if error_count > 0: logger.warning(f"{summary_log} Completed with {error_count} error(s).")
    else: logger.info(summary_log)


# =============================================================================
# ===                  CLEANUP FLOW                                         ===
# =============================================================================

@flow(name="Scheduled Clip Cleanup", log_prints=True)
async def cleanup_reviewed_clips_flow(
    cleanup_delay_minutes: int = CLIP_CLEANUP_DELAY_MINUTES,
):
    """ (Code remains identical to previous version) """
    logger = get_run_logger()
    logger.info(
        f"FLOW: Running Scheduled Clip Cleanup (Delay: {cleanup_delay_minutes} mins)..."
    )

    # Validate configurations
    if not S3_CONFIGURED or not s3_client:
        logger.error("S3 Client/Config not available.")
        return

    if not ASYNC_DB_CONFIGURED or get_db_pool is None:
        logger.error("Async DB not configured.")
        return

    # Initialize counters and connections
    pool = None
    conn = None
    processed_count = 0
    s3_deleted_count = 0
    db_artifact_deleted_count = 0
    db_clip_updated_count = 0
    error_count = 0

    try:
        pool = await close_db_pool()
        if not pool:
            logger.error("Failed to get asyncpg pool.")
            return

        async with pool.acquire() as conn:
            logger.info("Acquired asyncpg connection.")

            # Fetch clips ready for cleanup
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
            logger.info(f"Found {processed_count} clips ready for cleanup.")

            if not clips_to_cleanup:
                logger.info("No clips require cleanup.")
                return

            for clip_record in clips_to_cleanup:
                clip_id = clip_record["id"]
                current_state = clip_record["ingest_state"]
                log_prefix = f"[Cleanup Clip {clip_id}]"
                logger.info(f"{log_prefix} Processing state: {current_state}")

                sprite_artifact_s3_key = None
                s3_deletion_successful = False

                try:
                    # Retrieve sprite artifact key
                    artifact_query = """
                    SELECT s3_key
                      FROM clip_artifacts
                     WHERE clip_id = $1
                       AND artifact_type = $2
                     LIMIT 1;
                    """
                    artifact_record = await conn.fetchrow(
                        artifact_query, clip_id, ARTIFACT_TYPE_SPRITE_SHEET
                    )
                    if artifact_record:
                        sprite_artifact_s3_key = artifact_record["s3_key"]
                    else:
                        logger.warning(f"{log_prefix} No sprite artifact found.")
                        s3_deletion_successful = True

                    # Delete from S3 if key exists
                    if sprite_artifact_s3_key:
                        if not S3_BUCKET_NAME:
                            logger.error(f"{log_prefix} S3_BUCKET_NAME missing.")
                            error_count += 1
                        else:
                            try:
                                s3_client.delete_object(
                                    Bucket=S3_BUCKET_NAME,
                                    Key=sprite_artifact_s3_key,
                                )
                                logger.info(f"{log_prefix} Deleted S3 object.")
                                s3_deletion_successful = True
                                s3_deleted_count += 1

                            except ClientError as e:
                                error_code = (
                                    e.response.get("Error", {}).get("Code", "Unknown")
                                )
                                if error_code == "NoSuchKey":
                                    logger.warning(
                                        f"{log_prefix} S3 object not found (NoSuchKey)."
                                    )
                                    s3_deletion_successful = True
                                else:
                                    logger.error(
                                        f"{log_prefix} Failed S3 delete: {e} (Code: {error_code})"
                                    )
                                if not s3_deletion_successful:
                                    error_count += 1

                            except Exception as e:
                                logger.error(
                                    f"{log_prefix} Unexpected S3 error: {e}",
                                    exc_info=True,
                                )
                                error_count += 1

                    # Proceed with DB updates if S3 deletion succeeded
                    if s3_deletion_successful:
                        final_state_map = {
                            "approved_pending_deletion": "review_approved",
                            "archived_pending_deletion": "archived",
                        }
                        final_clip_state = final_state_map.get(current_state)
                        if not final_clip_state:
                            logger.error(
                                f"{log_prefix} Unexpected state '{current_state}'."
                            )
                            error_count += 1
                            continue

                        try:
                            async with conn.transaction():
                                # Delete artifact record
                                if sprite_artifact_s3_key:
                                    delete_artifact_sql = """
                                    DELETE FROM clip_artifacts
                                     WHERE clip_id = $1
                                       AND artifact_type = $2;
                                    """
                                    del_res = await conn.execute(
                                        delete_artifact_sql,
                                        clip_id,
                                        ARTIFACT_TYPE_SPRITE_SHEET,
                                    )
                                    deleted = (
                                        int(del_res.split()[-1])
                                        if del_res.startswith("DELETE")
                                        else 0
                                    )
                                    db_artifact_deleted_count += deleted

                                # Update clip state
                                update_clip_sql = """
                                UPDATE clips
                                   SET ingest_state = $1,
                                       updated_at = NOW(),
                                       action_committed_at = NULL,
                                       last_error = NULL
                                 WHERE id = $2
                                   AND ingest_state = $3;
                                """
                                upd_res = await conn.execute(
                                    update_clip_sql,
                                    final_clip_state,
                                    clip_id,
                                    current_state,
                                )
                                if upd_res == "UPDATE 1":
                                    logger.info(
                                        f"{log_prefix} Updated state to '{final_clip_state}'."
                                    )
                                    db_clip_updated_count += 1
                                else:
                                    raise RuntimeError(
                                        f"Clip update affected {upd_res} rows"
                                    )

                        except Exception as tx_err:
                            logger.error(
                                f"{log_prefix} DB transaction failed: {tx_err}",
                                exc_info=True,
                            )
                            error_count += 1
                    else:
                        logger.warning(
                            f"{log_prefix} Skipping DB updates due to S3 failure."
                        )

                except Exception as inner_err:
                    logger.error(
                        f"{log_prefix} Error processing clip: {inner_err}",
                        exc_info=True,
                    )
                    error_count += 1

    except Exception as outer_err:
        logger.error(
            f"FATAL Error during cleanup flow: {outer_err}", exc_info=True
        )
        error_count += 1

    finally:
        if pool and conn:
            await pool.release(conn)
            logger.info("Released asyncpg connection.")
        elif pool:
            logger.warning(
                "Asyncpg pool initialized but connection acquisition failed."
            )

    # Summary
    logger.info(
        f"FLOW: Cleanup complete. "
        f"Found: {processed_count}, "
        f"S3Deleted: {s3_deleted_count}, "
        f"DBArtifactsDeleted: {db_artifact_deleted_count}, "
        f"DBClipsUpdated: {db_clip_updated_count}, "
        f"Errors: {error_count}"
    )
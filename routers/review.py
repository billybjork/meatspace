import json
from fastapi import APIRouter, Request, Depends, Query, Path, Body, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
import asyncpg
from asyncpg.exceptions import UniqueViolationError, PostgresError
import psycopg2 # Keep for type hints if needed, though not directly used

from config import log, CLOUDFRONT_DOMAIN
from database import get_db_connection # Using the asyncpg pool dependency
from services import format_clip_data
from schemas import ClipActionPayload, SplitActionPayload # Assume ClipActionPayload includes 'group_previous'

# --- S3 Configuration Check (Best practice) ---
try:
    from tasks.splice import s3_client, S3_BUCKET_NAME, ClientError
    if not S3_BUCKET_NAME:
        raise ImportError("S3_BUCKET_NAME not configured")
    log.info("S3 client config imported successfully for review router (context).")
except ImportError as e:
    log.warning(f"Could not import S3 client config in review.py: {e}.")
    # Define dummy ClientError if needed for exception handling below
    class ClientError(Exception): pass

# Routers for UI and API
ui_router = APIRouter(
    tags=["Review UI"]
)
api_router = APIRouter(
    prefix="/api/clips",
    tags=["Clip Actions"]
)

# --- Review UI Route (Single Clip) ---
@ui_router.get("/review", response_class=HTMLResponse, name="review_clips")
async def review_clips_ui(
    request: Request,
    conn: asyncpg.Connection = Depends(get_db_connection)
):
    """Serves the HTML page for reviewing a single clip."""
    templates = request.app.state.templates
    clip_to_review = None
    error_message = None

    try:
        # Fetch the *next* single clip for review, including previous clip info
        record = await conn.fetchrow(
            """
            WITH ClipWithLag AS (
                SELECT
                    c.id, c.clip_identifier, c.clip_filepath, c.keyframe_filepath,
                    c.start_time_seconds, c.end_time_seconds, c.source_video_id,
                    c.ingest_state, c.processing_metadata, c.last_error,
                    c.sprite_sheet_filepath, c.sprite_metadata,
                    sv.title as source_title,
                    -- Look *behind* to the previous clip in the sequence for merge/group context
                    LAG(c.id) OVER w as previous_clip_id,
                    LAG(c.ingest_state) OVER w as previous_clip_state
                FROM clips c
                JOIN source_videos sv ON c.source_video_id = sv.id
                WINDOW w AS (PARTITION BY c.source_video_id ORDER BY c.start_time_seconds, c.id)
            )
            SELECT *
            FROM ClipWithLag
            WHERE ingest_state IN (
                -- States that REQUIRE immediate user attention in THIS queue:
                'pending_review',           -- Primary state for review
                'sprite_generation_failed', -- Needs review/retry
                'merge_failed',             -- Needs re-review after failed merge
                'split_failed',             -- Needs re-review after failed split
                'keyframe_failed',          -- Needs review/retry (post-approval failure)
                'embedding_failed',         -- Needs review/retry (post-approval failure)
                'group_failed'              -- Needs review after failed grouping attempt
                -- 'review_skipped' is intentionally EXCLUDED here (but valid previous state).
            )
            ORDER BY source_video_id, start_time_seconds, id -- Ensure consistent ordering
            LIMIT 1;
            """
        )

        if record:
            record_dict = dict(record)
            prev_id = record_dict.get('previous_clip_id')
            prev_state = record_dict.get('previous_clip_state')
            log.info(f"Checking merge/group for clip {record_dict['id']}: Previous ID = {prev_id}, Previous State = '{prev_state}'")

            # Define states the *previous* clip must be in for EITHER MERGING OR GROUPING
            # Use the EXPANDED set now for both actions as requested
            valid_previous_action_states = {
                'pending_review', 'review_skipped',
                'approved_pending_deletion', 'review_approved', # If you use review_approved
                'archived_pending_deletion', 'archived',         # If you use archived
                'grouped_complete' # Can group/merge with an already grouped clip
            }

            # Calculate ONE flag based on the unified logic
            can_action_previous = (
                 prev_id is not None and
                 prev_state in valid_previous_action_states
            )

            log.info(f"Result: can_action_previous = {can_action_previous}")
            # Add the single flag to the dictionary
            record_dict['can_action_previous'] = can_action_previous

            formatted = format_clip_data(record_dict, request)
            if formatted:
                 sprite_path = record_dict.get('sprite_sheet_filepath')
                 formatted['sprite_sheet_url'] = f"https://{CLOUDFRONT_DOMAIN}/{sprite_path}" if CLOUDFRONT_DOMAIN and sprite_path else sprite_path
                 sprite_meta_db = record_dict.get('sprite_metadata')
                 if isinstance(sprite_meta_db, str):
                     try: formatted['sprite_metadata'] = json.loads(sprite_meta_db)
                     except json.JSONDecodeError: formatted['sprite_metadata'] = None
                 else: formatted['sprite_metadata'] = sprite_meta_db

                 formatted['sprite_error'] = record_dict['ingest_state'] == 'sprite_generation_failed'
                 # Pass the single flag to the template context
                 formatted['can_action_previous'] = can_action_previous
                 formatted['current_state'] = record_dict['ingest_state']
                 formatted['previous_clip_id'] = record_dict.get('previous_clip_id')
                 clip_to_review = formatted

    except asyncpg.exceptions.UndefinedTableError as e:
         log.error(f"ERROR accessing DB tables fetching review clips: {e}", exc_info=True)
         error_message = f"Database table error: {e}. Please check application setup."
    except asyncpg.exceptions.PostgresSyntaxError as e:
         log.error(f"ERROR in SQL syntax fetching review clips: {e}", exc_info=True)
         error_message = f"Database syntax error: {e}. Please check the SQL query in review.py."
    except Exception as e:
        log.error(f"Error fetching clip for review: {e}", exc_info=True)
        error_message = "Failed to load clip for review due to a server error."

    return templates.TemplateResponse("review.html", {
        "request": request,
        "clip": clip_to_review, # Pass single clip or None (contains the single flag)
        "error": error_message,
        "CLOUDFRONT_DOMAIN": CLOUDFRONT_DOMAIN
    })


# --- Clip Action API Route (Handles Merge Previous AND Group Previous) ---
@api_router.post("/{clip_db_id}/action", name="clip_action", status_code=200)
async def handle_clip_action(
    clip_db_id: int = Path(..., description="Database ID of the clip", gt=0),
    payload: ClipActionPayload = Body(...),
    conn: asyncpg.Connection = Depends(get_db_connection)
):
    action = payload.action
    log.info(f"API: Received action '{action}' for clip_db_id: {clip_db_id}")

    target_state = None
    previous_clip_id = None # Needed for merge_previous and group_previous

    # Map actions to target states
    action_to_state = {
        # Standard Actions
        "approve": "approved_pending_deletion", # Or 'review_approved' if deletion is not the goal
        "skip": "review_skipped",
        "archive": "archived_pending_deletion", # Or 'archived'
        "retry_sprite_gen": "pending_sprite_generation",
        # Actions involving previous clip
        "merge_previous": "marked_for_merge_into_previous", # Intermediate state for current clip (N)
        "group_previous": "grouped_complete",             # FINAL state for both clips (N and N-1)
    }
    target_state = action_to_state.get(action)

    if not target_state:
        raise HTTPException(status_code=400, detail=f"Invalid action specified: {action}")

    # States the CURRENT clip must be in to allow ANY action
    allowed_source_states = [
        'pending_review', 'review_skipped', 'merge_failed', 'split_failed',
        'keyframe_failed', 'embedding_failed', 'sprite_generation_failed',
        'group_failed' # Allow retrying group/other actions if group failed
    ]
    if action == 'retry_sprite_gen':
         allowed_source_states = ['sprite_generation_failed']

    # States the PREVIOUS clip must be in to allow merging OR grouping into/with it
    # ***** THIS IS THE KEY FIX FOR THE 409 ERROR *****
    valid_previous_action_states = {
        'pending_review', 'review_skipped',
        'approved_pending_deletion', 'review_approved', # If you use review_approved
        'archived_pending_deletion', 'archived',         # If you use archived
        'grouped_complete' # Can group/merge with an already grouped clip
    }

    lock_id_current = 2 # Advisory lock category for current clip
    lock_id_previous = 3 # Advisory lock category for previous clip (use different category)

    try:
        async with conn.transaction():
            # --- Acquire Lock(s) ---
            await conn.execute("SELECT pg_advisory_xact_lock($1, $2)", lock_id_current, clip_db_id)
            log.debug(f"Acquired lock ({lock_id_current}, {clip_db_id}) for action '{action}'.")

            # --- Fetch Current Clip State & Info ---
            current_clip_data = await conn.fetchrow(
                """SELECT ingest_state, processing_metadata, source_video_id, start_time_seconds
                   FROM clips WHERE id = $1 FOR UPDATE""", # Row lock
                clip_db_id
            )
            if not current_clip_data:
                raise HTTPException(status_code=404, detail="Clip not found.")
            current_state = current_clip_data['ingest_state']
            current_source_video_id = current_clip_data['source_video_id']
            current_start_time = current_clip_data['start_time_seconds']

            # --- Check Allowed Source State (for current clip) ---
            if current_state not in allowed_source_states:
                 log.warning(f"Action '{action}' rejected for clip {clip_db_id}. Current state '{current_state}'.")
                 raise HTTPException(status_code=409, detail=f"Action '{action}' cannot be performed from state '{current_state}'.")

            updated_id = None # Track if the primary clip update was successful
            update_succeeded = False

            # --- Actions Involving Previous Clip (Merge/Group) ---
            if action in ["merge_previous", "group_previous"]:
                # Find the previous clip's ID and state
                previous_clip_record = await conn.fetchrow(
                    """
                    SELECT id, ingest_state
                    FROM clips
                    WHERE source_video_id = $1 AND start_time_seconds < $2
                    ORDER BY start_time_seconds DESC, id DESC
                    LIMIT 1 FOR UPDATE; -- Add FOR UPDATE here for safety, primary lock is advisory
                    """, current_source_video_id, current_start_time
                )

                if not previous_clip_record:
                    raise HTTPException(status_code=400, detail=f"No preceding clip found to {action.split('_')[0]} with.")

                previous_clip_id = previous_clip_record['id']
                # Lock the previous clip
                await conn.execute("SELECT pg_advisory_xact_lock($1, $2)", lock_id_previous, previous_clip_id)
                log.debug(f"Acquired lock ({lock_id_previous}, {previous_clip_id}) for {action} target.")

                # Re-verify previous clip state *after* lock using the CORRECT (expanded) set of states
                previous_clip_state_locked = await conn.fetchval("SELECT ingest_state FROM clips WHERE id = $1 FOR UPDATE", previous_clip_id)
                if previous_clip_state_locked not in valid_previous_action_states: # *** USES THE CORRECT SET NOW ***
                     log.warning(f"{action.capitalize()} rejected: Previous clip {previous_clip_id} is in state '{previous_clip_state_locked}'. Allowed: {valid_previous_action_states}")
                     raise HTTPException(status_code=409, detail=f"Cannot {action.split('_')[0]}: The previous clip is in state '{previous_clip_state_locked}'.")

                # === Handle MERGE Previous ===
                if action == "merge_previous":
                    # ... (merge logic remains the same) ...
                    current_metadata = {"merge_target_clip_id": previous_clip_id}
                    previous_metadata = {"merge_source_clip_id": clip_db_id}
                    target_state_previous_clip = "pending_merge_target"

                    result_current = await conn.execute(
                         """UPDATE clips SET ingest_state = $1, updated_at = NOW(), last_error = NULL,
                            processing_metadata = $2::jsonb, reviewed_at = NOW()
                            WHERE id = $3 AND ingest_state = ANY($4::text[])""",
                         target_state, current_metadata, clip_db_id, allowed_source_states
                    )
                    result_previous = await conn.execute(
                        """UPDATE clips SET ingest_state = $1, updated_at = NOW(), last_error = NULL,
                           processing_metadata = $2::jsonb
                           WHERE id = $3 AND ingest_state = $4""",
                        target_state_previous_clip, previous_metadata, previous_clip_id, previous_clip_state_locked
                    )

                # === Handle GROUP Previous ===
                elif action == "group_previous":
                    target_state_for_group = "grouped_complete" # Final state for both

                    # Update CURRENT clip (N) to 'grouped_complete' and link it
                    result_current = await conn.execute(
                         """UPDATE clips SET
                                ingest_state = $1, grouped_with_clip_id = $2,
                                updated_at = NOW(), last_error = NULL,
                                processing_metadata = NULL, reviewed_at = NOW()
                            WHERE id = $3 AND ingest_state = ANY($4::text[])""",
                         target_state_for_group, previous_clip_id, clip_db_id, allowed_source_states
                    )
                    result_previous = await conn.execute(
                        """UPDATE clips SET
                               ingest_state = $1, updated_at = NOW(),
                               last_error = NULL, processing_metadata = NULL,
                               reviewed_at = NOW()
                           WHERE id = $2 AND ingest_state = $3""",
                        target_state_for_group, previous_clip_id, previous_clip_state_locked
                    )

                # Check results (remains the same)
                rows_affected_current = int(result_current.split()[-1])
                rows_affected_previous = int(result_previous.split()[-1])

                if rows_affected_current > 0:
                    updated_id = clip_db_id
                    update_succeeded = True
                    if rows_affected_previous == 0:
                        log.warning(f"Previous clip {previous_clip_id} state changed during {action} action. Current clip {clip_db_id} was updated.")
                else:
                    current_state_after = await conn.fetchval("SELECT ingest_state FROM clips WHERE id = $1", clip_db_id) or "NOT FOUND"
                    log.warning(f"Failed to update current clip {clip_db_id} for {action} action. State might have changed concurrently. Current: {current_state_after}")

            # --- Handle Other Actions (Approve, Skip, Archive, Retry) ---
            else:
                set_clauses = ["ingest_state = $1", "updated_at = NOW()", "last_error = NULL"]
                params = [target_state]
                param_index = 2 # Start index for next parameter

                # Clear metadata unless it's a state that uses it (merge, split)
                if action not in ["merge_previous", "pending_split"]: # Group metadata cleared above
                     set_clauses.append(f"processing_metadata = NULL")
                     # Clear group link if not grouping
                     if action != "group_previous":
                        set_clauses.append(f"grouped_with_clip_id = NULL")

                # Set reviewed_at timestamp for final actions
                if action in ["approve", "archive", "group_previous"]: # group_previous already handled above but safe to include
                    set_clauses.append(f"reviewed_at = NOW()")
                # Clear reviewed_at if skipping or retrying
                elif action in ["skip", "retry_sprite_gen"]:
                     set_clauses.append(f"reviewed_at = NULL")

                set_sql = ", ".join(set_clauses)
                query_params = params + [clip_db_id, allowed_source_states]
                where_clause = f"WHERE id = ${param_index} AND ingest_state = ANY(${param_index + 1}::text[])"

                query = f"""
                    UPDATE clips SET {set_sql}
                    {where_clause}
                    RETURNING id;
                """
                log.debug(f"Executing Action Query: {query} with params: {query_params}")
                updated_id = await conn.fetchval(query, *query_params)
                if updated_id:
                    update_succeeded = True


            # --- Final Check and Response ---
            if not update_succeeded:
                # Check state again if update failed
                current_state_after = await conn.fetchval("SELECT ingest_state FROM clips WHERE id = $1", clip_db_id) or "NOT FOUND"
                log.warning(f"Failed to update clip {clip_db_id} with action '{action}'. Current state: {current_state_after}")
                raise HTTPException(status_code=409, detail="Clip state changed. Please refresh.")

            # Transaction commits automatically upon exiting 'async with conn.transaction():'

        log.info(f"API: Successfully processed action '{action}' for clip {clip_db_id}. New state: '{target_state}'.")
        # For group_previous, the previous clip's state also changed to grouped_complete
        response_content = {"status": "success", "clip_id": clip_db_id, "new_state": target_state}
        if action == 'group_previous':
            response_content["previous_clip_id"] = previous_clip_id
            response_content["previous_clip_new_state"] = target_state # Same state

        return JSONResponse(content=response_content)

    except HTTPException:
         raise # Re-raise HTTP exceptions directly
    except (PostgresError, asyncpg.PostgresError) as db_err: # Catch asyncpg specific errors
         log.error(f"Database error during action '{action}' for clip {clip_db_id}: {db_err}", exc_info=True)
         raise HTTPException(status_code=500, detail="Database error occurred.")
    except Exception as e:
        log.error(f"Unexpected error processing action '{action}' for clip {clip_db_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error processing action.")
    # Locks released automatically by transaction end


# --- Split API Route ---
@api_router.post("/{clip_db_id}/split", name="queue_clip_split", status_code=200)
async def queue_clip_split(
    clip_db_id: int = Path(..., description="Database ID of the clip to split", gt=0),
    payload: SplitActionPayload = Body(...),
    conn: asyncpg.Connection = Depends(get_db_connection)
):
    """
    Marks a clip for splitting at the specified FRAME NUMBER.
    Updates state to 'pending_split' and stores split frame in metadata.
    """
    requested_split_frame = payload.split_request_at_frame
    log.info(f"API: Received split request for clip_db_id: {clip_db_id} at relative frame: {requested_split_frame}")

    new_state = "pending_split"
    allowed_source_states = ['pending_review', 'review_skipped'] # States allowed to initiate split
    lock_id = 2 # Clip lock category

    try:
        async with conn.transaction():
            await conn.execute("SELECT pg_advisory_xact_lock($1, $2)", lock_id, clip_db_id)
            log.debug(f"Acquired lock ({lock_id}, {clip_db_id}) for split action.")

            # Fetch clip data including sprite metadata for validation
            clip_record = await conn.fetchrow(
                """
                SELECT ingest_state, processing_metadata, sprite_metadata
                FROM clips WHERE id = $1 FOR UPDATE; -- Row lock within transaction
                """, clip_db_id
            )

            if not clip_record:
                raise HTTPException(status_code=404, detail="Clip not found.")

            current_state = clip_record['ingest_state']
            if current_state not in allowed_source_states:
                 raise HTTPException(status_code=409, detail=f"Clip cannot be split from state '{current_state}'. Refresh?")

            # --- Sprite Metadata Validation ---
            sprite_meta = clip_record['sprite_metadata']
            # Ensure sprite_meta is a dictionary (asyncpg should decode JSONB to dict)
            if not isinstance(sprite_meta, dict):
                log.error(f"Split rejected for {clip_db_id}: Sprite metadata missing or not a valid dictionary. Type: {type(sprite_meta)}")
                raise HTTPException(status_code=400, detail="Sprite metadata missing or invalid, cannot validate split frame request.")

            # --- Frame Validation ---
            total_clip_frames = sprite_meta.get('clip_total_frames')
            if not isinstance(total_clip_frames, int) or total_clip_frames <= 1:
                log.error(f"Split rejected for {clip_db_id}: Cannot determine total frames from sprite metadata. Value: {total_clip_frames}")
                raise HTTPException(status_code=400, detail="Cannot determine total frames for clip, cannot validate split.")

            min_frame_margin = 1 # Can't split at frame 0 or frame N-1
            if not (min_frame_margin <= requested_split_frame < (total_clip_frames - min_frame_margin)):
                 err_msg = (f"Split frame ({requested_split_frame}) must be between "
                            f"{min_frame_margin} and {total_clip_frames - min_frame_margin - 1} "
                            f"(inclusive) for this clip (Total Frames: {total_clip_frames}).")
                 log.warning(f"Split rejected for {clip_db_id}: Invalid frame. {err_msg}")
                 raise HTTPException(status_code=422, detail=err_msg) # Unprocessable Entity

            # --- Prepare Metadata Update ---
            # Create or update processing_metadata with the split request frame
            metadata = {"split_request_at_frame": requested_split_frame} # Simple dict for split info

            # --- Database Update ---
            # Pass the Python dictionary `metadata` directly to the parameter for $2::jsonb
            updated_id = await conn.fetchval(
                """
                UPDATE clips
                SET ingest_state = $1,
                    processing_metadata = $2::jsonb, -- Pass Python dict here
                    last_error = NULL,
                    updated_at = NOW()
                WHERE id = $3 AND ingest_state = ANY($4::text[]) -- Ensure state hasn't changed
                RETURNING id;
                """,
                new_state,
                metadata, # Pass the dictionary
                clip_db_id,
                allowed_source_states
            )

            if updated_id is None:
                current_state_after = await conn.fetchval("SELECT ingest_state FROM clips WHERE id = $1", clip_db_id) or "NOT FOUND"
                raise HTTPException(status_code=409, detail=f"Clip state changed to '{current_state_after}' while queueing split. Please refresh.")

            # Transaction commits automatically here

        log.info(f"API: Clip {clip_db_id} successfully queued for splitting at frame {requested_split_frame}. New state: '{new_state}'.")
        return {"status": "success", "clip_id": clip_db_id, "new_state": new_state, "message": "Clip queued for splitting."}

    except HTTPException:
        raise # Re-raise HTTP exceptions directly
    except (PostgresError, asyncpg.PostgresError) as db_err: # Catch specific DB errors
         log.error(f"Database error during split queue for clip {clip_db_id}: {db_err}", exc_info=True)
         raise HTTPException(status_code=500, detail="Internal server error during database operation.")
    except Exception as e:
        log.error(f"Unexpected error queueing clip {clip_db_id} for split: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error processing split request.")
    # Lock released automatically when transaction ends


# --- Undo API Route (Handles Undoing Merge Previous and Group Previous) ---
@api_router.post("/{clip_db_id}/undo", name="undo_clip_action", status_code=200)
async def undo_clip_action(
    clip_db_id: int = Path(..., gt=0),
    conn: asyncpg.Connection = Depends(get_db_connection)
):
    """
    Attempts to revert a clip's state back to 'pending_review'.
    Can undo from intermediate states like pending_deletion, merge/split/group requests,
    or completed group states.
    """
    log.info(f"API: Received UNDO request for clip_db_id: {clip_db_id}")
    target_state = "pending_review" # Always revert to pending_review for simplicity

    # Define states from which 'undo' is logically allowed
    allowed_undo_source_states = [
        'review_skipped', 'pending_split', 'merge_failed', 'split_failed', 'group_failed',
        'keyframe_failed', 'embedding_failed', 'sprite_generation_failed',
        'approved_pending_deletion', 'archived_pending_deletion',
        'marked_for_merge_into_previous', # Can undo this state
        'pending_merge_target', # Should ideally be undone via the *other* clip, but allow here too for robustness
        'grouped_complete' # NEW: Allow undoing a completed group link
    ]
    lock_id_current = 2
    lock_id_previous = 3 # Needed if undoing merge or group

    try:
        async with conn.transaction():
            await conn.execute("SELECT pg_advisory_xact_lock($1, $2)", lock_id_current, clip_db_id)
            log.debug(f"Acquired lock ({lock_id_current}, {clip_db_id}) for undo.")

            # Fetch current state, metadata, and group link
            clip_data = await conn.fetchrow(
                "SELECT ingest_state, processing_metadata, grouped_with_clip_id FROM clips WHERE id = $1 FOR UPDATE",
                clip_db_id
            )

            if not clip_data:
                raise HTTPException(status_code=404, detail="Clip not found.")
            current_state = clip_data['ingest_state']
            current_metadata = clip_data['processing_metadata'] # Could be dict or None
            current_grouped_with_id = clip_data['grouped_with_clip_id'] # Could be int or None

            # Check if undo is allowed from the current state
            if current_state not in allowed_undo_source_states:
                 # Define states considered truly final/irreversible
                 final_states = ['review_approved', 'archived', 'merged'] # Add others if they exist
                 if current_state in final_states: detail_msg = f"Cannot undo final state '{current_state}'."
                 elif current_state == target_state: detail_msg = "Clip is already pending review."
                 else: detail_msg = f"Cannot undo from state '{current_state}'."
                 log.warning(f"Undo rejected for clip {clip_db_id}: {detail_msg}")
                 raise HTTPException(status_code=409, detail=detail_msg)

            # --- Handle Undoing MERGE logic ---
            if current_state == 'marked_for_merge_into_previous':
                # ... (existing merge undo logic, finding previous_clip_id from metadata) ...
                # Ensure it reverts the *other* clip ('pending_merge_target') back to 'pending_review'
                previous_clip_id = current_metadata.get('merge_target_clip_id') if isinstance(current_metadata, dict) else None
                if not previous_clip_id or not isinstance(previous_clip_id, int):
                     log.error(f"Undo merge failed: Cannot find valid previous clip ID in metadata for clip {clip_db_id}. Metadata: {current_metadata}")
                     raise HTTPException(status_code=500, detail="Cannot undo merge: Corrupted data.")

                # Lock previous clip
                await conn.execute("SELECT pg_advisory_xact_lock($1, $2)", lock_id_previous, previous_clip_id)
                log.debug(f"Acquired lock ({lock_id_previous}, {previous_clip_id}) for undoing merge target.")

                # Revert previous clip's state
                prev_update_result = await conn.execute(
                    """UPDATE clips SET ingest_state = $1, updated_at = NOW(), processing_metadata = NULL, last_error = $2
                       WHERE id = $3 AND ingest_state = 'pending_merge_target'""",
                    'pending_review', # Revert to pending_review
                    f'Merge cancelled by undo action on clip {clip_db_id}',
                    previous_clip_id
                )
                if prev_update_result != "UPDATE 1":
                    log.warning(f"Undo merge: Previous clip {previous_clip_id} state was not 'pending_merge_target' or update failed.")
                    raise HTTPException(status_code=409, detail="Could not revert previous clip state during undo. It may have already been processed.")

            elif current_state == 'pending_merge_target':
                 # ... (existing merge undo logic, finding source_clip_id from metadata) ...
                 # Ensure it reverts the *other* clip ('marked_for_merge_into_previous') back to 'pending_review'
                 source_clip_id = current_metadata.get('merge_source_clip_id') if isinstance(current_metadata, dict) else None
                 if not source_clip_id or not isinstance(source_clip_id, int):
                      log.error(f"Undo merge failed: Cannot find valid source clip ID in metadata for target clip {clip_db_id}. Metadata: {current_metadata}")
                      raise HTTPException(status_code=500, detail="Cannot undo merge: Corrupted data.")

                 # Lock source clip
                 await conn.execute("SELECT pg_advisory_xact_lock($1, $2)", lock_id_previous, source_clip_id)
                 log.debug(f"Acquired lock ({lock_id_previous}, {source_clip_id}) for undoing merge source.")

                 # Revert source clip's state
                 source_update_result = await conn.execute(
                     """UPDATE clips SET ingest_state = $1, updated_at = NOW(), processing_metadata = NULL, last_error = $2
                        WHERE id = $3 AND ingest_state = 'marked_for_merge_into_previous'""",
                     'pending_review', # Revert source to pending
                     f'Merge cancelled by undo action on target clip {clip_db_id}',
                     source_clip_id
                 )
                 if source_update_result != "UPDATE 1":
                      log.warning(f"Undo merge: Source clip {source_clip_id} state was not 'marked_for_merge_into_previous' or update failed.")
                      raise HTTPException(status_code=409, detail="Could not revert source clip state during undo.")

            # --- Handle Undoing GROUP logic ---
            # If the current clip is the one that *was* grouped (has the link)
            elif current_state == 'grouped_complete' and current_grouped_with_id is not None:
                 # We only need to update the CURRENT clip to remove the link and change state.
                 # We *could* optionally revert the state of the clip it was grouped WITH,
                 # but that might be unexpected. Let's keep undo focused on the target clip.
                 # The other clip remains 'grouped_complete' but no longer has anything pointing to it.
                 # This seems reasonable; it can be reviewed/actioned independently later if needed.
                 log.info(f"Undo initiated for grouped clip {clip_db_id}. It was grouped with {current_grouped_with_id}. Only clip {clip_db_id} will be reverted.")
                 # No need to lock the other clip in this simple undo scenario.

            # --- Prepare SET Clauses and Error Message for Current Clip ---
            set_clauses = [
                "ingest_state = $1",            # $1 = target_state ('pending_review')
                "updated_at = NOW()",
                "reviewed_at = NULL",           # Clear review timestamp
                "processing_metadata = NULL",   # Clear any pending action metadata
                "last_error = $2"               # $2 = undo_error_message
            ]
            params = [target_state]

            undo_error_message = 'Reverted to pending review by user undo action'
            if current_state == 'marked_for_merge_into_previous':
                undo_error_message = f'Merge into clip {previous_clip_id} cancelled by user undo'
            elif current_state == 'pending_merge_target':
                 undo_error_message = f'Merge target status reverted by user undo (source: {source_clip_id})'
            elif current_state == 'grouped_complete':
                 undo_error_message = f'Group status reverted by user undo (was grouped with {current_grouped_with_id})'
                 set_clauses.append("grouped_with_clip_id = NULL") # Explicitly clear the group link

            params.append(undo_error_message) # Add error message to params

            # --- Update Current Clip (Common Undo Logic) ---
            set_sql = ", ".join(set_clauses)
            query_params = params + [clip_db_id, allowed_undo_source_states]
            param_indices = ', '.join([f'${i+1}' for i in range(len(params))])

            # Final parameter indices for WHERE clause
            where_id_index = len(params) + 1
            where_state_index = len(params) + 2

            query = f"""
                UPDATE clips
                SET {set_sql}
                WHERE id = ${where_id_index} AND ingest_state = ANY(${where_state_index}::text[])
                RETURNING id;
            """
            log.debug(f"Executing Undo Query: {query} with params: {query_params}")

            updated_id = await conn.fetchval(query, *query_params)

            if updated_id is None:
                current_state_after = await conn.fetchval("SELECT ingest_state FROM clips WHERE id = $1", clip_db_id) or "NOT FOUND"
                log.warning(f"Undo failed for clip {clip_db_id}. State changed concurrently? Current state: {current_state_after}")
                # If merge/group partner update failed earlier, this could also fail if state changed.
                raise HTTPException(status_code=409, detail="Clip state changed while processing undo. Please refresh.")

            # Transaction commits automatically upon exiting 'async with'

        log.info(f"API: Successfully reverted clip {clip_db_id} to state '{target_state}' via undo.")
        # Since the UI reloads, just return success status
        return {"status": "success", "clip_id": clip_db_id, "new_state": target_state}

    except HTTPException:
        raise # Re-raise HTTP exceptions directly
    except (PostgresError, asyncpg.PostgresError) as db_err: # Catch specific DB errors
         log.error(f"Database error during undo for clip {clip_db_id}: {db_err}", exc_info=True)
         raise HTTPException(status_code=500, detail="Internal server error during database operation.")
    except Exception as e:
        log.error(f"Unexpected error processing undo for clip {clip_db_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error processing undo request.")
    # Locks released automatically when transaction ends
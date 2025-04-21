import json
from fastapi import APIRouter, Request, Depends, Query, Path, Body, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
import asyncpg
from asyncpg.exceptions import UniqueViolationError, PostgresError
import psycopg2

from config import log, CLOUDFRONT_DOMAIN
from database import get_db_connection # Using the asyncpg pool dependency
from services import format_clip_data
from schemas import ClipActionPayload, SplitActionPayload

# --- S3 Configuration Check (Best practice) ---
try:
    from tasks.splice import s3_client, S3_BUCKET_NAME, ClientError
    if not S3_BUCKET_NAME:
        raise ImportError("S3_BUCKET_NAME not configured")
    log.info("S3 client config imported successfully for review router (context).")
except ImportError as e:
    log.warning(f"Could not import S3 client config in review.py: {e}.")
    # Define dummy ClientError if needed for exception handling below (though not used directly here now)
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
                    -- Look *behind* to the previous clip in the sequence for merge context
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
                'embedding_failed'          -- Needs review/retry (post-approval failure)
                -- 'review_skipped' is intentionally EXCLUDED here.
            )
            ORDER BY source_video_id, start_time_seconds, id -- Ensure consistent ordering
            LIMIT 1;
            """
        )

        if record:
            record_dict = dict(record)
            prev_id = record_dict.get('previous_clip_id')
            prev_state = record_dict.get('previous_clip_state')
            log.info(f"Checking merge for clip {record_dict['id']}: Previous ID = {prev_id}, Previous State = '{prev_state}'")

            # Define states the *previous* clip must be in to allow merging
            valid_merge_source_states = {'pending_review', 'review_skipped'} # Keep simple for now

            can_merge_previous = (
                 prev_id is not None and
                 prev_state in valid_merge_source_states
            )
            log.info(f"Result: can_merge_previous = {can_merge_previous} (Prev ID exists: {prev_id is not None}, Prev State valid: {prev_state in valid_merge_source_states})")
            record_dict['can_merge_previous'] = can_merge_previous

            formatted = format_clip_data(record_dict, request)
            if formatted:
                 sprite_path = record_dict.get('sprite_sheet_filepath')
                 formatted['sprite_sheet_url'] = f"https://{CLOUDFRONT_DOMAIN}/{sprite_path}" if CLOUDFRONT_DOMAIN and sprite_path else sprite_path
                 # Ensure sprite_metadata is passed as a dictionary if it exists
                 sprite_meta_db = record_dict.get('sprite_metadata')
                 if isinstance(sprite_meta_db, str): # Handle if stored as string accidentally
                     try: formatted['sprite_metadata'] = json.loads(sprite_meta_db)
                     except json.JSONDecodeError: formatted['sprite_metadata'] = None
                 else: formatted['sprite_metadata'] = sprite_meta_db # Assume it's dict/None

                 formatted['sprite_error'] = record_dict['ingest_state'] == 'sprite_generation_failed'
                 formatted['can_merge_previous'] = can_merge_previous # Add merge flag
                 formatted['current_state'] = record_dict['ingest_state']
                 formatted['previous_clip_id'] = record_dict.get('previous_clip_id') # Pass previous ID if needed by JS/template
                 clip_to_review = formatted

    except asyncpg.exceptions.UndefinedTableError as e:
         log.error(f"ERROR accessing DB tables fetching review clips: {e}", exc_info=True)
         error_message = f"Database table error: {e}. Please check application setup."
    except Exception as e:
        log.error(f"Error fetching clip for review: {e}", exc_info=True)
        error_message = "Failed to load clip for review due to a server error."

    return templates.TemplateResponse("review.html", {
        "request": request,
        "clip": clip_to_review, # Pass single clip or None
        "error": error_message,
        "CLOUDFRONT_DOMAIN": CLOUDFRONT_DOMAIN
    })


# --- Clip Action API Route (Handles Merge Previous) ---
@api_router.post("/{clip_db_id}/action", name="clip_action", status_code=200)
async def handle_clip_action(
    clip_db_id: int = Path(..., description="Database ID of the clip", gt=0),
    payload: ClipActionPayload = Body(...),
    conn: asyncpg.Connection = Depends(get_db_connection)
):
    action = payload.action
    log.info(f"API: Received action '{action}' for clip_db_id: {clip_db_id}")

    target_state = None
    previous_clip_id = None # Needed for merge_previous

    # Map actions to target states
    action_to_state = {
        "approve": "approved_pending_deletion",
        "skip": "review_skipped",
        "archive": "archived_pending_deletion",
        "merge_previous": "marked_for_merge_into_previous", # New state for current clip
        "retry_sprite_gen": "pending_sprite_generation",
    }
    target_state = action_to_state.get(action)

    if not target_state:
        raise HTTPException(status_code=400, detail=f"Invalid action specified: {action}")

    allowed_source_states = [
        'pending_review', 'review_skipped', 'merge_failed', 'split_failed',
        'keyframe_failed', 'embedding_failed', 'sprite_generation_failed'
    ]
    if action == 'retry_sprite_gen':
         allowed_source_states = ['sprite_generation_failed']

    # States the PREVIOUS clip must be in to allow merging into it
    valid_merge_source_states = {'pending_review', 'review_skipped'}

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

            # --- Check Allowed Source State ---
            if current_state not in allowed_source_states:
                 log.warning(f"Action '{action}' rejected for clip {clip_db_id}. Current state '{current_state}'.")
                 raise HTTPException(status_code=409, detail=f"Action '{action}' cannot be performed from state '{current_state}'.")

            updated_id = None
            update_query = ""
            query_params = []

            # --- Handle Merge Previous Action ---
            if action == "merge_previous":
                # Find the previous clip's ID within the same source video
                previous_clip_record = await conn.fetchrow(
                    """
                    SELECT id, ingest_state
                    FROM clips
                    WHERE source_video_id = $1 AND start_time_seconds < $2
                    ORDER BY start_time_seconds DESC, id DESC
                    LIMIT 1 FOR UPDATE;
                    """, current_clip_data['source_video_id'], current_clip_data['start_time_seconds']
                    # Added FOR UPDATE here for safety, though locking below is primary
                )

                if not previous_clip_record:
                    raise HTTPException(status_code=400, detail="No preceding clip found to merge with.")

                previous_clip_id = previous_clip_record['id']
                previous_clip_state = previous_clip_record['ingest_state']

                # Lock the previous clip
                await conn.execute("SELECT pg_advisory_xact_lock($1, $2)", lock_id_previous, previous_clip_id)
                log.debug(f"Acquired lock ({lock_id_previous}, {previous_clip_id}) for merge target.")

                # Re-verify previous clip state *after* lock (can be slightly different if LIMIT 1 FOR UPDATE missed it)
                previous_clip_state_locked = await conn.fetchval("SELECT ingest_state FROM clips WHERE id = $1 FOR UPDATE", previous_clip_id)
                if previous_clip_state_locked not in valid_merge_source_states:
                     log.warning(f"Merge rejected: Previous clip {previous_clip_id} is in state '{previous_clip_state_locked}', not mergeable.")
                     raise HTTPException(status_code=409, detail=f"Cannot merge: The previous clip is in state '{previous_clip_state_locked}'.")

                # Prepare metadata for both clips as Python dictionaries
                current_metadata = {"merge_target_clip_id": previous_clip_id}
                previous_metadata = {"merge_source_clip_id": clip_db_id}
                target_state_previous_clip = "pending_merge_target" # State for the clip being merged into

                # --- Update CURRENT clip (N) ---
                # Pass Python dict `current_metadata` directly to the query parameter for JSONB
                await conn.execute(
                     """UPDATE clips SET ingest_state = $1, updated_at = NOW(), last_error = NULL,
                        processing_metadata = $2::jsonb, -- Pass dict here
                        reviewed_at = NOW()
                        WHERE id = $3 AND ingest_state = ANY($4::text[])""",
                     target_state, # marked_for_merge_into_previous
                     current_metadata, # Pass the dictionary
                     clip_db_id,
                     allowed_source_states
                )
                # --- Update PREVIOUS clip (N-1) ---
                # Pass Python dict `previous_metadata` directly
                await conn.execute(
                    """UPDATE clips SET ingest_state = $1, updated_at = NOW(), last_error = NULL,
                       processing_metadata = $2::jsonb -- Pass dict here
                       WHERE id = $3 AND ingest_state = $4""", # Use specific state check
                    target_state_previous_clip, # pending_merge_target
                    previous_metadata, # Pass the dictionary
                    previous_clip_id,
                    previous_clip_state_locked # Ensure it didn't change
                )
                updated_id = clip_db_id # Mark success for the original clip ID

            # --- Handle Other Actions ---
            else:
                set_clauses = ["ingest_state = $1", "updated_at = NOW()", "last_error = NULL"]
                params = [target_state]
                # Clear metadata unless it's a merge/split action
                # Note: 'merge_previous' handled above, so this only excludes split now
                if action != "pending_split": # Check target state if appropriate
                     set_clauses.append("processing_metadata = NULL")
                # Set reviewed_at timestamp for final actions
                if action in ["approve", "archive"]:
                    set_clauses.append("reviewed_at = NOW()")

                set_sql = ", ".join(set_clauses)
                query_params = params + [clip_db_id, allowed_source_states]
                query = f"""
                    UPDATE clips SET {set_sql}
                    WHERE id = ${len(params) + 1} AND ingest_state = ANY(${len(params) + 2}::text[])
                    RETURNING id;
                """
                log.debug(f"Executing Action Query: {query} with params: {query_params}")
                updated_id = await conn.fetchval(query, *query_params)


            # --- Final Check and Response ---
            if updated_id is None and action != 'merge_previous': # Merge success is checked implicitly by reaching here w/o error
                current_state_after = await conn.fetchval("SELECT ingest_state FROM clips WHERE id = $1", clip_db_id) or "NOT FOUND"
                log.warning(f"Failed to update clip {clip_db_id} with action '{action}'. Current state: {current_state_after}")
                raise HTTPException(status_code=409, detail="Clip state changed. Please refresh.")
            elif action == 'merge_previous' and updated_id != clip_db_id: # Should not happen if logic is correct
                log.error(f"Internal error during merge setup for clip {clip_db_id} and {previous_clip_id}.")
                raise HTTPException(status_code=500, detail="Failed to initiate merge.")

            # Transaction commits automatically upon exiting 'async with conn.transaction():'

        log.info(f"API: Successfully processed action '{action}' for clip {clip_db_id}. New state: '{target_state}'.")
        return JSONResponse(
            content={"status": "success", "clip_id": clip_db_id, "new_state": target_state}
        )

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


# --- Undo API Route (Handles Undoing Merge Previous) ---
@api_router.post("/{clip_db_id}/undo", name="undo_clip_action", status_code=200)
async def undo_clip_action(
    clip_db_id: int = Path(..., gt=0),
    conn: asyncpg.Connection = Depends(get_db_connection)
):
    """
    Attempts to revert a clip's state back to 'pending_review'.
    Can undo from intermediate states like pending_deletion or merge requests.
    """
    log.info(f"API: Received UNDO request for clip_db_id: {clip_db_id}")
    target_state = "pending_review" # Always revert to pending_review for simplicity

    # Define states from which 'undo' is logically allowed
    allowed_undo_source_states = [
        'review_skipped', 'pending_split', 'merge_failed', 'split_failed',
        'keyframe_failed', 'embedding_failed', 'sprite_generation_failed',
        'approved_pending_deletion', 'archived_pending_deletion',
        'marked_for_merge_into_previous', # Can undo this state
        'pending_merge_target' # Should ideally be undone via the *other* clip, but allow here too for robustness
    ]
    lock_id_current = 2
    lock_id_previous = 3 # Needed if undoing merge

    try:
        async with conn.transaction():
            await conn.execute("SELECT pg_advisory_xact_lock($1, $2)", lock_id_current, clip_db_id)
            log.debug(f"Acquired lock ({lock_id_current}, {clip_db_id}) for undo.")

            # Fetch current state and metadata
            clip_data = await conn.fetchrow(
                "SELECT ingest_state, processing_metadata FROM clips WHERE id = $1 FOR UPDATE",
                clip_db_id
            )

            if not clip_data:
                raise HTTPException(status_code=404, detail="Clip not found.")
            current_state = clip_data['ingest_state']
            current_metadata = clip_data['processing_metadata'] # Could be dict or None

            # Check if undo is allowed from the current state
            if current_state not in allowed_undo_source_states:
                 final_states = ['review_approved', 'archived', 'merged']
                 if current_state in final_states: detail_msg = f"Cannot undo final state '{current_state}'."
                 elif current_state == target_state: detail_msg = "Clip is already pending review."
                 else: detail_msg = f"Cannot undo from state '{current_state}'."
                 log.warning(f"Undo rejected for clip {clip_db_id}: {detail_msg}")
                 raise HTTPException(status_code=409, detail=detail_msg)

            # --- Handle Undoing Merge ---
            # If current clip is the one MARKED for merge
            if current_state == 'marked_for_merge_into_previous':
                previous_clip_id = current_metadata.get('merge_target_clip_id') if isinstance(current_metadata, dict) else None
                if not previous_clip_id or not isinstance(previous_clip_id, int):
                     log.error(f"Undo merge failed: Cannot find valid previous clip ID in metadata for clip {clip_db_id}. Metadata: {current_metadata}")
                     raise HTTPException(status_code=500, detail="Cannot undo merge: Corrupted data.")

                # Lock previous clip
                await conn.execute("SELECT pg_advisory_xact_lock($1, $2)", lock_id_previous, previous_clip_id)
                log.debug(f"Acquired lock ({lock_id_previous}, {previous_clip_id}) for undoing merge target.")

                # Revert previous clip's state (ensure it's still 'pending_merge_target')
                prev_update_result = await conn.execute(
                    """UPDATE clips SET ingest_state = $1, updated_at = NOW(), processing_metadata = NULL, last_error = $2
                       WHERE id = $3 AND ingest_state = 'pending_merge_target'""",
                    'pending_review', # Revert to pending_review (simplest)
                    f'Merge cancelled by undo action on clip {clip_db_id}',
                    previous_clip_id
                )
                if prev_update_result != "UPDATE 1":
                    log.warning(f"Undo merge: Previous clip {previous_clip_id} state was not 'pending_merge_target' or update failed. It might have been processed or changed.")
                    # Fail the undo? Or proceed with current clip only? Failing is safer.
                    raise HTTPException(status_code=409, detail="Could not revert previous clip state during undo. It may have already been processed.")

            # If current clip is the TARGET of a merge
            elif current_state == 'pending_merge_target':
                 source_clip_id = current_metadata.get('merge_source_clip_id') if isinstance(current_metadata, dict) else None
                 if not source_clip_id or not isinstance(source_clip_id, int):
                      log.error(f"Undo merge failed: Cannot find valid source clip ID in metadata for target clip {clip_db_id}. Metadata: {current_metadata}")
                      raise HTTPException(status_code=500, detail="Cannot undo merge: Corrupted data.")

                 # Lock source clip (using previous clip lock ID here, consistent)
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

            # --- Update Current Clip (Common Undo Logic) ---
            undo_error_message = 'Reverted to pending review by user undo action'
            if current_state == 'marked_for_merge_into_previous':
                undo_error_message = f'Merge into clip {previous_clip_id} cancelled by user undo'
            elif current_state == 'pending_merge_target':
                 undo_error_message = f'Merge target status reverted by user undo (source: {source_clip_id})'

            # Update the current clip back to target_state ('pending_review')
            updated_id = await conn.fetchval(
                """
                UPDATE clips
                SET ingest_state = $1,
                    updated_at = NOW(),
                    reviewed_at = NULL,         # Clear review timestamp
                    processing_metadata = NULL, # Clear any pending action metadata
                    last_error = $2             # Set message indicating undo
                WHERE id = $3 AND ingest_state = ANY($4::text[]) # Ensure state hasn't changed concurrently
                RETURNING id;
                """,
                target_state,                  # $1 'pending_review'
                undo_error_message,            # $2 last_error
                clip_db_id,                    # $3 clip id
                allowed_undo_source_states     # $4 allowed source states for undo
            )

            if updated_id is None:
                current_state_after = await conn.fetchval("SELECT ingest_state FROM clips WHERE id = $1", clip_db_id) or "NOT FOUND"
                log.warning(f"Undo failed for clip {clip_db_id}. State changed concurrently? Current state: {current_state_after}")
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
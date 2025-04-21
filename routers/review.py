import json
from fastapi import APIRouter, Request, Depends, Query, Path, Body, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
import asyncpg
from asyncpg.exceptions import UniqueViolationError, PostgresError
import psycopg2
import httpx # For potential async background tasks if needed

from config import log, CLOUDFRONT_DOMAIN
from database import get_db_connection
from services import format_clip_data
from schemas import ClipActionPayload, SplitActionPayload

# --- S3 Configuration (Needed by the *separate* cleanup task, but good practice to check import here too) ---
try:
    # Import ClientError for potential exception handling within this file if needed elsewhere
    from tasks.splice import s3_client, S3_BUCKET_NAME, ClientError # Import ClientError too
    if not S3_BUCKET_NAME:
        raise ImportError("S3_BUCKET_NAME not configured")
    log.info("S3 client config imported successfully for review router (for context/potential future use).")
except ImportError as e:
    log.warning(f"Could not import S3 client config in review.py: {e}. The separate cleanup task MUST have access.")
    s3_client = None # This specific router doesn't use it directly now
    S3_BUCKET_NAME = None
    # Define dummy ClientError if needed for exception handling below
    class ClientError(Exception): pass # Define dummy for type hints if needed

# Separate routers for UI and API for clarity and prefixing
ui_router = APIRouter(
    tags=["Review UI"]
)
api_router = APIRouter(
    prefix="/api/clips", # Prefix for all API actions related to clips
    tags=["Clip Actions"]
)

# --- Review UI Route ---
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
                'pending_review',
                'sprite_generation_failed',
                'merge_failed',
                'split_failed',
                'keyframe_failed',
                'embedding_failed'
                -- Exclude review_skipped, and intermediate/final states like pending_deletion, approved, archived
            )
            ORDER BY source_video_id, start_time_seconds, id -- Ensure consistent ordering
            LIMIT 1;
            """
        )

        if record:
            record_dict = dict(record)
            prev_id = record_dict.get('previous_clip_id')
            prev_state = record_dict.get('previous_clip_state')
            log.info(f"Checking merge for clip {record_dict['id']}: Previous ID = {prev_id}, Previous State = '{prev_state}'") # <-- ADD LOGGING

            # Define states the *previous* clip must be in to allow merging
            valid_merge_source_states = {'pending_review', 'review_skipped'}

            can_merge_previous = (
                prev_id is not None and
                prev_state in valid_merge_source_states
            )
            log.info(f"Result: can_merge_previous = {can_merge_previous} (Prev ID exists: {prev_id is not None}, Prev State valid: {prev_state in valid_merge_source_states})") # <-- ADD DETAILED LOGGING

            record_dict['can_merge_previous'] = can_merge_previous

            formatted = format_clip_data(record_dict, request)
            if formatted:
                 sprite_path = record_dict.get('sprite_sheet_filepath')
                 formatted['sprite_sheet_url'] = f"https://{CLOUDFRONT_DOMAIN}/{sprite_path}" if CLOUDFRONT_DOMAIN and sprite_path else sprite_path
                 formatted['sprite_metadata'] = record_dict.get('sprite_metadata')
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


# --- Clip Action API Routes ---

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

            # --- Fetch Current Clip State ---
            current_clip_data = await conn.fetchrow(
                """SELECT ingest_state, processing_metadata, source_video_id, start_time_seconds
                   FROM clips WHERE id = $1 FOR UPDATE""", # Add FOR UPDATE
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
                    LIMIT 1
                    """, current_clip_data['source_video_id'], current_clip_data['start_time_seconds']
                )

                if not previous_clip_record:
                    raise HTTPException(status_code=400, detail="No preceding clip found to merge with.")

                previous_clip_id = previous_clip_record['id']
                previous_clip_state = previous_clip_record['ingest_state']

                # Lock the previous clip
                await conn.execute("SELECT pg_advisory_xact_lock($1, $2)", lock_id_previous, previous_clip_id)
                log.debug(f"Acquired lock ({lock_id_previous}, {previous_clip_id}) for merge target.")

                # Re-verify previous clip state *after* lock
                previous_clip_state_locked = await conn.fetchval("SELECT ingest_state FROM clips WHERE id = $1 FOR UPDATE", previous_clip_id)
                if previous_clip_state_locked not in valid_merge_source_states:
                     log.warning(f"Merge rejected: Previous clip {previous_clip_id} is in state '{previous_clip_state_locked}', not mergeable.")
                     raise HTTPException(status_code=409, detail=f"Cannot merge: The previous clip is in state '{previous_clip_state_locked}'.")

                # Prepare metadata for both clips
                current_metadata = {"merge_target_clip_id": previous_clip_id}
                previous_metadata = {"merge_source_clip_id": clip_db_id}
                target_state_previous_clip = "pending_merge_target" # New state for the target

                # Update CURRENT clip (N)
                await conn.execute(
                     """UPDATE clips SET ingest_state = $1, updated_at = NOW(), last_error = NULL,
                        processing_metadata = $2::jsonb, reviewed_at = NOW()
                        WHERE id = $3 AND ingest_state = ANY($4::text[])""",
                     target_state, # marked_for_merge_into_previous
                     json.dumps(current_metadata),
                     clip_db_id,
                     allowed_source_states
                )
                # Update PREVIOUS clip (N-1)
                await conn.execute(
                    """UPDATE clips SET ingest_state = $1, updated_at = NOW(), last_error = NULL,
                       processing_metadata = $2::jsonb
                       WHERE id = $3 AND ingest_state = $4""", # Be specific with prev state check
                    target_state_previous_clip, # pending_merge_target
                    json.dumps(previous_metadata),
                    previous_clip_id,
                    previous_clip_state_locked # Ensure it didn't change between fetch and update
                )
                updated_id = clip_db_id # Indicate success for the original clip

            # --- Handle Other Actions ---
            else:
                set_clauses = ["ingest_state = $1", "updated_at = NOW()", "last_error = NULL"]
                params = [target_state]
                if action not in ["merge_previous", "pending_split"]: # Clear metadata unless needed
                     set_clauses.append("processing_metadata = NULL")
                if action in ["approve", "archive"]: # Keep reviewed_at logic
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
            if updated_id is None and action != 'merge_previous': # Merge updates manually checked
                current_state_after = await conn.fetchval("SELECT ingest_state FROM clips WHERE id = $1", clip_db_id) or "NOT FOUND"
                log.warning(f"Failed to update clip {clip_db_id} with action '{action}'. Current state: {current_state_after}")
                raise HTTPException(status_code=409, detail="Clip state changed. Please refresh.")
            elif action == 'merge_previous' and not updated_id:
                # This case indicates the update for clip N failed during merge setup, should be caught by transaction
                log.error(f"Critical error during merge setup for clip {clip_db_id} and {previous_clip_id}. Transaction should roll back.")
                raise HTTPException(status_code=500, detail="Failed to initiate merge.")

            # Transaction commits automatically

        log.info(f"API: Successfully processed action '{action}' for clip {clip_db_id}. New state: '{target_state}'.")
        return JSONResponse(
            content={"status": "success", "clip_id": clip_db_id, "new_state": target_state}
        )

    except HTTPException:
         raise # Re-raise HTTP exceptions
    except (PostgresError, psycopg2.Error, asyncpg.PostgresError) as db_err:
         log.error(f"Database error during action '{action}' for clip {clip_db_id}: {db_err}", exc_info=True)
         raise HTTPException(status_code=500, detail="DB error.")
    except Exception as e:
        log.error(f"Unexpected error processing action '{action}' for clip {clip_db_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error.")
    # Locks released by transaction end


@api_router.post("/{clip_db_id}/split", name="queue_clip_split", status_code=200)
async def queue_clip_split(
    clip_db_id: int = Path(..., gt=0),
    payload: SplitActionPayload = Body(...),
    conn: asyncpg.Connection = Depends(get_db_connection)
):
    """
    Marks a clip for splitting at the specified FRAME NUMBER.
    Updates state to 'pending_split' and stores split frame in metadata.
    """
    requested_split_frame = payload.split_request_at_frame # Access from Pydantic model
    log.info(f"API: Received split request for clip_db_id: {clip_db_id} at relative frame: {requested_split_frame}")

    new_state = "pending_split"
    # States allowed to initiate split (must typically have sprite sheet available)
    allowed_source_states = ['pending_review', 'review_skipped']
    lock_id = 2 # Clip lock category

    try:
        async with conn.transaction():
            await conn.execute("SELECT pg_advisory_xact_lock($1, $2)", lock_id, clip_db_id)
            log.debug(f"Acquired lock ({lock_id}, {clip_db_id}) for split action.")

            # Fetch necessary clip data including sprite metadata for validation
            clip_record = await conn.fetchrow(
                """
                SELECT ingest_state, processing_metadata, sprite_metadata
                FROM clips WHERE id = $1 FOR UPDATE; -- Row lock within transaction
                """, clip_db_id
            )

            if not clip_record:
                raise HTTPException(status_code=404, detail="Clip not found.")

            sprite_meta = clip_record['sprite_metadata']

            current_state = clip_record['ingest_state']
            if current_state not in allowed_source_states:
                 # log.warning(f"[Split {clip_db_id}] Invalid state '{current_state}'. Allowed: {allowed_source_states}") # Logging preserved if desired
                 raise HTTPException(status_code=409, detail=f"Clip cannot be split from state '{current_state}'. Refresh?")

            # --- VALIDATION CHECK ---
            # Check if it's None or not a dictionary (asyncpg typically returns dict for jsonb)
            if sprite_meta is None or not isinstance(sprite_meta, dict):
                raise HTTPException(status_code=400, detail="Sprite metadata missing or invalid, cannot validate split frame request.")
            # --- END VALIDATION ---

            # --- Frame Validation ---
            total_clip_frames = sprite_meta.get('clip_total_frames')
            # Check if total_clip_frames exists AND is a positive integer
            if not isinstance(total_clip_frames, int) or total_clip_frames <= 1:
                raise HTTPException(status_code=400, detail=f"Cannot determine total frames for clip from sprite metadata, cannot validate split.")

            min_frame_margin = 1
            # Frame indices are 0 to total_clip_frames - 1. Valid split points are 1 to total_clip_frames - 2.
            if not (min_frame_margin <= requested_split_frame < (total_clip_frames - min_frame_margin)):
                 err_msg = f"Split frame ({requested_split_frame}) must be between {min_frame_margin} and {total_clip_frames - min_frame_margin - 1} (inclusive) for this clip (Total Frames: {total_clip_frames})."
                 raise HTTPException(status_code=422, detail=err_msg)

            # --- Prepare Metadata Update ---
            current_metadata_val = clip_record['processing_metadata'] # Could be None, dict, or string
            metadata = {}
            if isinstance(current_metadata_val, str): # Handle if it's still stored as string
                try: metadata = json.loads(current_metadata_val)
                except json.JSONDecodeError: pass
            elif isinstance(current_metadata_val, dict):
                 metadata = current_metadata_val # If asyncpg gave a dict
            elif current_metadata_val is not None: pass

            metadata['split_request_at_frame'] = requested_split_frame # Store the validated frame number

            # --- Database Update ---
            updated_id = await conn.fetchval(
                """
                UPDATE clips
                SET ingest_state = $1,
                    processing_metadata = $2::jsonb, -- Store metadata with split frame
                    last_error = NULL,              -- Clear previous errors
                    updated_at = NOW()
                WHERE id = $3 AND ingest_state = ANY($4::text[]) -- Ensure state hasn't changed concurrently
                RETURNING id;
                """,
                new_state, json.dumps(metadata), clip_db_id, allowed_source_states
            )

            if updated_id is None:
                current_state_after = await conn.fetchval("SELECT ingest_state FROM clips WHERE id = $1", clip_db_id) or "NOT FOUND"
                raise HTTPException(status_code=409, detail="Clip state changed while queueing split. Please refresh.")

            # On success:
            log.info(f"API: Clip {clip_db_id} successfully queued for splitting at frame {payload.split_request_at_frame}. New state: 'pending_split'.")
            return {"status": "success", "clip_id": clip_db_id, "new_state": "pending_split", "message": "Clip queued for splitting."}
            # Frontend will reload the page

    except HTTPException:
        raise # Re-raise HTTP exceptions directly
    except (PostgresError, psycopg2.Error) as db_err: # Catch specific DB errors
         log.error(f"Database error during split queue for clip {clip_db_id}: {db_err}", exc_info=True)
         raise HTTPException(status_code=500, detail="Internal server error during database operation.")
    except Exception as e:
        # Log the specific error before raising generic 500
        log.error(f"Unexpected error queueing clip {clip_db_id} for split: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error processing split request.")
    # Lock is automatically released when transaction ends


# --- Undo API Route (Handles Undoing Merge Previous) ---
@api_router.post("/{clip_db_id}/undo", name="undo_clip_action", status_code=200)
async def undo_clip_action(
    clip_db_id: int = Path(..., gt=0),
    conn: asyncpg.Connection = Depends(get_db_connection)
):
    log.info(f"API: Received UNDO request for clip_db_id: {clip_db_id}")
    target_state = "pending_review"
    allowed_undo_source_states = [
        'review_skipped', 'pending_split', 'merge_failed', 'split_failed',
        'keyframe_failed', 'embedding_failed', 'sprite_generation_failed',
        'approved_pending_deletion', 'archived_pending_deletion',
        'marked_for_merge_into_previous', # Can undo this state
        'pending_merge_target' # Should ideally be undone via the *other* clip
    ]
    lock_id_current = 2
    lock_id_previous = 3 # Needed if undoing merge

    try:
        async with conn.transaction():
            await conn.execute("SELECT pg_advisory_xact_lock($1, $2)", lock_id_current, clip_db_id)
            log.debug(f"Acquired lock ({lock_id_current}, {clip_db_id}) for undo.")

            clip_data = await conn.fetchrow("SELECT ingest_state, processing_metadata FROM clips WHERE id = $1 FOR UPDATE", clip_db_id)

            if not clip_data: raise HTTPException(status_code=404, detail="Clip not found.")
            current_state = clip_data['ingest_state']
            current_metadata = clip_data['processing_metadata']

            if current_state not in allowed_undo_source_states:
                 log.warning(f"Undo rejected for clip {clip_db_id}. Current state '{current_state}' not in allowed undo states: {allowed_undo_source_states}.")
                 # Provide more specific feedback based on state
                 final_states = ['review_approved', 'archived']
                 if current_state in final_states:
                     detail_msg = f"Cannot undo action for clip in final state '{current_state}'. Cleanup likely occurred."
                 elif current_state == target_state: # Already pending_review
                      detail_msg = f"Clip is already in the '{target_state}' state."
                 else: # Other states like 'pending_download', etc.
                     detail_msg = f"Cannot undo from the clip's current state ('{current_state}')."
                 raise HTTPException(status_code=409, detail=f"Cannot undo from state '{current_state}'.")

            # --- Handle Undoing Merge ---
            if current_state == 'marked_for_merge_into_previous':
                previous_clip_id = current_metadata.get('merge_target_clip_id') if isinstance(current_metadata, dict) else None
                if not previous_clip_id:
                     log.error(f"Undo merge failed: Cannot find previous clip ID in metadata for clip {clip_db_id}")
                     raise HTTPException(status_code=500, detail="Cannot undo merge: Missing data.")

                # Lock previous clip
                await conn.execute("SELECT pg_advisory_xact_lock($1, $2)", lock_id_previous, previous_clip_id)
                log.debug(f"Acquired lock ({lock_id_previous}, {previous_clip_id}) for undoing merge target.")

                # Revert previous clip state (ensure it's still 'pending_merge_target')
                prev_update_result = await conn.execute(
                    """UPDATE clips SET ingest_state = $1, updated_at = NOW(), processing_metadata = NULL, last_error = $2
                       WHERE id = $3 AND ingest_state = 'pending_merge_target'""",
                    'pending_review', # Or potentially 'review_skipped' if we track original state? Simple for now.
                    f'Merge cancelled by undo action on clip {clip_db_id}',
                    previous_clip_id
                )
                if prev_update_result != "UPDATE 1":
                    log.warning(f"Undo merge: Previous clip {previous_clip_id} state was not 'pending_merge_target' or update failed.")
                    # Decide whether to proceed with undoing clip N anyway or fail the whole transaction
                    # raise HTTPException(status_code=409, detail="Could not revert previous clip state during undo.") # Safer option

            # --- Update Current Clip (Common Undo Logic) ---
            undo_error_message = 'Reverted by user undo action'
            if current_state == 'marked_for_merge_into_previous':
                undo_error_message = f'Merge into clip {previous_clip_id} cancelled by user undo'

            updated_id = await conn.fetchval(
                """
                UPDATE clips
                SET ingest_state = $1, updated_at = NOW(), reviewed_at = NULL,
                    processing_metadata = NULL, last_error = $2
                WHERE id = $3 AND ingest_state = ANY($4::text[])
                RETURNING id;
                """,
                target_state, undo_error_message, clip_db_id, allowed_undo_source_states # Use ANY check
            )

            if updated_id is None:
                current_state_after = await conn.fetchval("SELECT ingest_state FROM clips WHERE id = $1", clip_db_id) or "NOT FOUND"
                raise HTTPException(status_code=409, detail="Undo failed. State changed concurrently?")

            # Transaction commits

        log.info(f"API: Successfully reverted clip {clip_db_id} to state '{target_state}' via undo.")
        return {"status": "success", "clip_id": clip_db_id, "new_state": target_state}

    except HTTPException: raise
    except (PostgresError, psycopg2.Error, asyncpg.PostgresError) as db_err:
        log.error(f"Database error during undo for clip {clip_db_id}: {db_err}", exc_info=True)
        raise HTTPException(status_code=500, detail="DB error during undo.")
    except Exception as e:
        log.error(f"Unexpected error processing undo for clip {clip_db_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error processing undo.")
    # Locks released by transaction
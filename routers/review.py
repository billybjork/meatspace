import json
from fastapi import APIRouter, Request, Depends, Query, Path, Body, HTTPException
from fastapi.responses import HTMLResponse
import asyncpg
from asyncpg.exceptions import UniqueViolationError

from config import log, CLOUDFRONT_DOMAIN
from database import get_db_connection
from services import format_clip_data
from schemas import ClipActionPayload, SplitActionPayload

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
    conn: asyncpg.Connection = Depends(get_db_connection),
    limit: int = Query(10, ge=1, le=50) # Limit how many clips to review at once
):
    """Serves the HTML page for reviewing clips."""
    templates = request.app.state.templates # Get templates from app state
    clips_to_review = []
    error_message = None
    try:
        # Fetch clips pending review, including adjacent clip info for merge context
        # Use window functions to determine if merge is possible with the *next* clip
        records = await conn.fetch(
            """
            WITH OrderedClips AS (
                SELECT
                    c.id, c.clip_identifier, c.clip_filepath, c.keyframe_filepath,
                    c.start_time_seconds, c.end_time_seconds, c.source_video_id,
                    c.ingest_state, c.processing_metadata, c.last_error,
                    sv.title as source_title,
                    -- Look ahead to the next clip in the sequence for the same source video
                    LEAD(c.id) OVER w as next_clip_id,
                    LEAD(c.ingest_state) OVER w as next_clip_state
                FROM clips c
                JOIN source_videos sv ON c.source_video_id = sv.id
                WINDOW w AS (PARTITION BY c.source_video_id ORDER BY c.start_time_seconds, c.id) -- Added id for tie-breaking
            )
            SELECT *
            FROM OrderedClips
            WHERE ingest_state = 'pending_review' -- Primary state we want to review
            ORDER BY source_video_id, start_time_seconds, id
            LIMIT $1;
            """,
            limit
        )

        valid_merge_target_states = {'pending_review', 'review_skipped'} # States the *next* clip can be in for merging
        for record in records:
             # Add 'can_merge_next' boolean based on the lookahead info
            can_merge = (
                 record['next_clip_id'] is not None and
                 record['next_clip_state'] in valid_merge_target_states
            )
            # Convert record to dict if it's not already (asyncpg records are dict-like)
            record_dict = dict(record)
            record_dict['can_merge_next'] = can_merge # Add the flag

            formatted = format_clip_data(record_dict, request) # Use service function
            if formatted:
                 # format_clip_data now handles most fields, just add what's missing if any
                 # or ensure format_clip_data includes start/end time etc.
                 clips_to_review.append(formatted)

    except asyncpg.exceptions.UndefinedTableError as e:
         log.error(f"ERROR accessing DB tables (e.g., '{e}') fetching review clips: {e}", exc_info=True)
         error_message = f"Database table error: {e}. Please check application setup."
    except Exception as e:
        log.error(f"Error fetching clips for review: {e}", exc_info=True)
        error_message = "Failed to load clips for review due to a server error."

    return templates.TemplateResponse("review.html", {
        "request": request,
        "clips": clips_to_review,
        "error": error_message,
        # Pass domain if needed directly by template JS, though format_clip_data handles URLs
        "CLOUDFRONT_DOMAIN": CLOUDFRONT_DOMAIN
    })

# --- Clip Action API Routes ---

@api_router.post("/{clip_db_id}/action", name="clip_action", status_code=200)
async def handle_clip_action(
    clip_db_id: int = Path(..., description="Database ID of the clip", gt=0),
    payload: ClipActionPayload = Body(...),
    conn: asyncpg.Connection = Depends(get_db_connection)
):
    """Handles user actions: approve, skip, archive, merge_next, retry_splice."""
    action = payload.action
    log.info(f"API: Received action '{action}' for clip_db_id: {clip_db_id}")

    target_state = None
    # Use a dictionary for cleaner mapping and easier addition of actions
    action_to_state = {
        "approve": "review_approved",
        "skip": "review_skipped",
        "archive": "archived",
        "merge_next": "pending_merge_with_next",
        "retry_splice": "pending_resplice", # Assuming resplice state exists
    }
    target_state = action_to_state.get(action)

    if not target_state:
        raise HTTPException(status_code=400, detail=f"Invalid action specified: {action}")

    # Define states from which actions are allowed (prevent acting on already processed clips)
    # Can be customized per action if needed
    allowed_source_states = [
        'pending_review', 'review_skipped',
        'merge_failed', 'split_failed', 'resplice_failed', # Allow retrying failed actions
        'keyframe_failed', 'embedding_failed' # Allow archiving/skipping failed steps
    ]

    # Build SET clauses dynamically (safer than f-strings)
    set_clauses = ["ingest_state = $1", "updated_at = NOW()", "last_error = NULL", "processing_metadata = NULL"]
    params = [target_state]

    if action in ["approve", "archive"]:
        set_clauses.append("reviewed_at = NOW()")

    set_sql = ", ".join(set_clauses)

    # Use advisory lock to prevent race conditions
    lock_id = 2 # Category 2 for clips (as used in original code)

    try:
        async with conn.transaction():
            # Acquire lock for the specific clip ID
            await conn.execute("SELECT pg_advisory_xact_lock($1, $2)", lock_id, clip_db_id)
            log.debug(f"Acquired lock ({lock_id}, {clip_db_id}) for action '{action}'.")

            # Verify current state *after* acquiring lock
            current_state = await conn.fetchval("SELECT ingest_state FROM clips WHERE id = $1", clip_db_id)

            if not current_state:
                raise HTTPException(status_code=404, detail="Clip not found.")

            if current_state not in allowed_source_states:
                 log.warning(f"Action '{action}' rejected for clip {clip_db_id}. Current state '{current_state}' not in allowed source states: {allowed_source_states}.")
                 # Provide a user-friendly error message
                 raise HTTPException(status_code=409, detail=f"Action '{action}' cannot be performed from the clip's current state ('{current_state}'). Please refresh.")

            # Construct the final query
            # Add clip_db_id and allowed states to parameters
            query_params = params + [clip_db_id, allowed_source_states]
            query = f"""
                UPDATE clips SET {set_sql}
                WHERE id = ${len(params) + 1} AND ingest_state = ANY(${len(params) + 2}::text[])
                RETURNING id;
            """
            # Note: Parameter indices start at $1

            log.debug(f"Executing Action Query: {query} with params: {query_params}")
            updated_id = await conn.fetchval(query, *query_params)

            if updated_id is None:
                # This means the WHERE condition (id and state) failed, likely state changed concurrently
                # Re-fetch state to confirm (optional, but good for logging)
                current_state_after = await conn.fetchval("SELECT ingest_state FROM clips WHERE id = $1", clip_db_id) or "NOT FOUND"
                log.warning(f"Failed to update clip {clip_db_id} with action '{action}'. State might have changed between check and update. Current state: {current_state_after}")
                raise HTTPException(status_code=409, detail="Clip state changed while processing the action. Please refresh and try again.")

            # If successful, the transaction commits automatically upon exiting 'async with'

        log.info(f"API: Successfully updated clip {clip_db_id} state to '{target_state}' via action '{action}'")
        return {"status": "success", "clip_id": clip_db_id, "new_state": target_state}

    except UniqueViolationError as e:
         # Handle potential unique constraint violations if relevant
         log.error(f"DB unique constraint error during clip action for {clip_db_id}: {e}", exc_info=True)
         raise HTTPException(status_code=409, detail=f"Database constraint violated: {e}")
    except HTTPException:
         raise # Re-raise HTTP exceptions directly
    except Exception as e:
        log.error(f"Error processing action '{action}' for clip {clip_db_id}: {e}", exc_info=True)
        # Generic error for unexpected issues
        raise HTTPException(status_code=500, detail=f"Internal server error processing action '{action}'.")
    # Lock is automatically released when transaction ends (commit or rollback)

@api_router.post("/{clip_db_id}/split", name="queue_clip_split", status_code=200)
async def queue_clip_split(
    clip_db_id: int = Path(..., description="Database ID of the clip to split", gt=0),
    payload: SplitActionPayload = Body(...), # Use Pydantic model from schemas.py
    conn: asyncpg.Connection = Depends(get_db_connection)
):
    """
    Marks a clip for splitting at the specified relative timecode.
    Updates state to 'pending_split' and stores split time in metadata.
    """
    requested_split_time = payload.split_time_seconds
    log.info(f"API: Received split request for clip_db_id: {clip_db_id} at relative time: {requested_split_time}s")

    new_state = "pending_split"
    min_margin = 0.1 # Min duration from start/end for split (prevent tiny clips)
    allowed_source_states = ['pending_review', 'review_skipped'] # States allowed to initiate split
    lock_id = 2 # Clip lock category

    try:
        async with conn.transaction():
            await conn.execute("SELECT pg_advisory_xact_lock($1, $2)", lock_id, clip_db_id)
            log.debug(f"Acquired lock ({lock_id}, {clip_db_id}) for split action.")

            clip_record = await conn.fetchrow(
                """
                SELECT start_time_seconds, end_time_seconds, ingest_state, processing_metadata
                FROM clips WHERE id = $1 FOR UPDATE; -- Row lock within transaction
                """, clip_db_id
            ) # Use FOR UPDATE for row-level lock within transaction

            if not clip_record:
                raise HTTPException(status_code=404, detail="Clip not found.")

            current_state = clip_record['ingest_state']
            if current_state not in allowed_source_states:
                 log.warning(f"Split requested on clip {clip_db_id} with invalid state '{current_state}'. Allowed: {allowed_source_states}")
                 raise HTTPException(status_code=409, detail=f"Clip cannot be split from its current state ('{current_state}'). Please refresh.")

            # Validate split time against clip duration
            start_time = clip_record['start_time_seconds']
            end_time = clip_record['end_time_seconds']
            if start_time is None or end_time is None:
                log.error(f"Clip {clip_db_id} is missing start/end time. Cannot calculate duration.")
                raise HTTPException(status_code=400, detail="Clip is missing time information, cannot validate split.")

            clip_duration = end_time - start_time
            if clip_duration <= (2 * min_margin): # Ensure clip is long enough to be split
                 log.warning(f"Clip {clip_db_id} duration ({clip_duration:.2f}s) is too short to split with margin {min_margin}s.")
                 raise HTTPException(status_code=422, detail=f"Clip duration ({clip_duration:.2f}s) is too short to be split.")

            # Check requested time validity (relative to clip start)
            if not (min_margin < requested_split_time < (clip_duration - min_margin)):
                 err_msg = f"Split time ({requested_split_time:.2f}s) must be between {min_margin:.2f}s and {clip_duration - min_margin:.2f}s for this clip."
                 log.warning(f"Invalid split time for clip {clip_db_id}: {err_msg}")
                 raise HTTPException(status_code=422, detail=err_msg)

            # Prepare metadata update (using current metadata if exists)
            current_metadata_str = clip_record['processing_metadata']
            metadata = {}
            if isinstance(current_metadata_str, str): # Assuming JSON string storage
                try:
                    metadata = json.loads(current_metadata_str)
                except json.JSONDecodeError:
                    log.warning(f"Could not parse existing metadata for clip {clip_db_id}. Overwriting.")
                    metadata = {} # Start fresh if parsing fails
            elif isinstance(current_metadata_str, dict): # If asyncpg parses JSONB directly
                 metadata = current_metadata_str
            elif current_metadata_str is not None:
                 log.warning(f"Unexpected metadata type ({type(current_metadata_str)}) for clip {clip_db_id}. Overwriting.")

            metadata['split_request_at'] = requested_split_time
            # metadata['previous_state_before_split'] = current_state # Optionally store previous state

            # Update the clip state and metadata
            updated_id = await conn.fetchval(
                """
                UPDATE clips
                SET ingest_state = $1,
                    processing_metadata = $2::jsonb,
                    last_error = NULL, -- Clear previous errors
                    updated_at = NOW()
                WHERE id = $3 AND ingest_state = ANY($4::text[]) -- Ensure state hasn't changed
                RETURNING id;
                """,
                new_state, json.dumps(metadata), clip_db_id, allowed_source_states
            )

            if updated_id is None:
                current_state_after = await conn.fetchval("SELECT ingest_state FROM clips WHERE id = $1", clip_db_id) or "NOT FOUND"
                log.warning(f"Split queue failed for clip {clip_db_id}. State changed concurrently. Current state: {current_state_after}")
                raise HTTPException(status_code=409, detail="Clip state changed while queueing split. Please refresh.")

        log.info(f"API: Clip {clip_db_id} successfully queued for splitting at {requested_split_time:.2f}s. New state: '{new_state}'.")
        return {"status": "success", "clip_id": clip_db_id, "new_state": new_state, "message": "Clip queued for splitting."}

    except HTTPException:
        raise # Re-raise HTTP exceptions
    except Exception as e:
        log.error(f"Error queueing clip {clip_db_id} for split: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error processing split request.")
    # Lock is released when transaction ends

@api_router.post("/{clip_db_id}/undo", name="undo_clip_action", status_code=200)
async def undo_clip_action(
    clip_db_id: int = Path(..., gt=0),
    conn: asyncpg.Connection = Depends(get_db_connection)
):
    """Attempts to revert a clip's state back to 'pending_review'."""
    log.info(f"API: Received UNDO request for clip_db_id: {clip_db_id}")

    target_state = "pending_review"
    # Define states from which 'undo' is logically allowed
    allowed_undo_source_states = [
        'review_approved', 'review_skipped', 'archived', # Common review actions
        'pending_merge_with_next', 'pending_split', 'pending_resplice', # Pending actions
        'merge_failed', 'split_failed', 'resplice_failed', # Failed actions
        'keyframe_failed', 'embedding_failed' # Allow undo from subsequent failures too
    ]
    lock_id = 2 # Clip lock category

    try:
        async with conn.transaction():
            await conn.execute("SELECT pg_advisory_xact_lock($1, $2)", lock_id, clip_db_id)
            log.debug(f"Acquired lock ({lock_id}, {clip_db_id}) for undo action.")

            current_state = await conn.fetchval("SELECT ingest_state FROM clips WHERE id = $1", clip_db_id)

            if not current_state:
                raise HTTPException(status_code=404, detail="Clip not found.")

            if current_state not in allowed_undo_source_states:
                 log.warning(f"Undo rejected for clip {clip_db_id}. Current state '{current_state}' not in allowed undo states: {allowed_undo_source_states}.")
                 raise HTTPException(status_code=409, detail=f"Cannot undo from the clip's current state ('{current_state}').")

            # Clear specific fields when reverting
            updated_id = await conn.fetchval(
                """
                UPDATE clips
                SET ingest_state = $1,
                    updated_at = NOW(),
                    reviewed_at = NULL,       -- Clear review timestamp
                    processing_metadata = NULL, -- Clear split/merge info
                    last_error = $2           -- Set message indicating undo
                WHERE id = $3 AND ingest_state = $4 -- Ensure state hasn't changed
                RETURNING id;
                """,
                target_state,                  # $1
                'Reverted to pending review by user action', # $2
                clip_db_id,                    # $3
                current_state                  # $4
            )

            if updated_id is None:
                current_state_after = await conn.fetchval("SELECT ingest_state FROM clips WHERE id = $1", clip_db_id) or "NOT FOUND"
                log.warning(f"Undo failed for clip {clip_db_id}. State changed concurrently. Current state: {current_state_after}")
                raise HTTPException(status_code=409, detail="Clip state changed while processing undo. Please refresh.")

        log.info(f"API: Successfully reverted clip {clip_db_id} to state '{target_state}' via undo.")
        return {"status": "success", "clip_id": clip_db_id, "new_state": target_state}

    except HTTPException:
        raise # Re-raise HTTP exceptions
    except Exception as e:
        log.error(f"Error processing undo for clip {clip_db_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error processing undo request.")
    # Lock is released when transaction ends
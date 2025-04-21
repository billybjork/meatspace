import json
from fastapi import APIRouter, Request, Depends, Query, Path, Body, HTTPException
# Removed BackgroundTasks import as it's no longer used here for immediate deletion
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

# --- Background S3 Deletion Function REMOVED ---
# This logic now belongs in a separate, scheduled cleanup task/flow.

# --- Review UI Route ---
@ui_router.get("/review", response_class=HTMLResponse, name="review_clips")
async def review_clips_ui(
    request: Request,
    conn: asyncpg.Connection = Depends(get_db_connection),
    limit: int = Query(10, ge=1, le=50) # Limit how many clips to review at once
):
    """Serves the HTML page for reviewing clips."""
    templates = request.app.state.templates
    clips_to_review = []
    error_message = None
    try:
        # Fetch clips for review. Included intermediate states for now,
        # they will disappear once the cleanup task finalizes them.
        # Adjust the WHERE clause based on exactly which states you want visible in the queue.
        records = await conn.fetch(
            """
            WITH OrderedClips AS (
                SELECT
                    c.id, c.clip_identifier, c.clip_filepath, c.keyframe_filepath,
                    c.start_time_seconds, c.end_time_seconds, c.source_video_id,
                    c.ingest_state, c.processing_metadata, c.last_error,
                    c.sprite_sheet_filepath, c.sprite_metadata, -- Fetch sprite info
                    sv.title as source_title,
                    -- Look ahead to the next clip in the sequence for merge context
                    LEAD(c.id) OVER w as next_clip_id,
                    LEAD(c.ingest_state) OVER w as next_clip_state
                FROM clips c
                JOIN source_videos sv ON c.source_video_id = sv.id
                WINDOW w AS (PARTITION BY c.source_video_id ORDER BY c.start_time_seconds, c.id)
            )
            SELECT *
            FROM OrderedClips
            WHERE ingest_state IN (
                'pending_review',           -- Primary state
                'sprite_generation_failed', -- Needs review/retry
                'review_skipped',           -- Still needs attention
                'merge_failed',             -- Needs re-review
                'split_failed',             -- Needs re-review
                'keyframe_failed',          -- Needs review/retry
                'embedding_failed'          -- Needs review/retry
                -- Intermediate states are NOT included here by default,
                -- so they disappear from the queue immediately after action,
                -- but are still undoable before cleanup runs.
                -- Add them below if you WANT them visible while pending deletion:
                -- 'approved_pending_deletion',
                -- 'archived_pending_deletion'
             )
            ORDER BY source_video_id, start_time_seconds, id
            LIMIT $1;
            """,
            limit
        )

        # States the *next* clip can be in for merging. Keep simple for now.
        valid_merge_target_states = {'pending_review', 'review_skipped', 'sprite_generation_failed'}

        for record in records:
            record_dict = dict(record) # Convert asyncpg record to dict

            # Determine merge possibility based on next clip's state
            can_merge = (
                 record_dict.get('next_clip_id') is not None and
                 record_dict.get('next_clip_state') in valid_merge_target_states
            )
            record_dict['can_merge_next'] = can_merge

            # Use the service function to format basic data
            formatted = format_clip_data(record_dict, request)
            if formatted:
                 # Add sprite-specific data
                 sprite_path = record_dict.get('sprite_sheet_filepath')
                 formatted['sprite_sheet_url'] = f"https://{CLOUDFRONT_DOMAIN}/{sprite_path}" if CLOUDFRONT_DOMAIN and sprite_path else sprite_path

                 formatted['sprite_metadata'] = record_dict.get('sprite_metadata') # Pass metadata (should be dict if JSONB)
                 formatted['sprite_error'] = record_dict['ingest_state'] == 'sprite_generation_failed' # Flag for UI / sprite failure
                 formatted['can_merge_next'] = can_merge # Ensure merge flag is present
                 # Add current state for potential UI indication (optional)
                 formatted['current_state'] = record_dict['ingest_state']
                 clips_to_review.append(formatted)

    except asyncpg.exceptions.UndefinedTableError as e:
         log.error(f"ERROR accessing DB tables fetching review clips: {e}", exc_info=True)
         error_message = f"Database table error: {e}. Please check application setup."
    except Exception as e:
        log.error(f"Error fetching clips for review: {e}", exc_info=True)
        error_message = "Failed to load clips for review due to a server error."

    return templates.TemplateResponse("review.html", {
        "request": request,
        "clips": clips_to_review,
        "error": error_message,
        "CLOUDFRONT_DOMAIN": CLOUDFRONT_DOMAIN # Pass domain for potential JS use
    })

# --- Clip Action API Routes ---

@api_router.post("/{clip_db_id}/action", name="clip_action", status_code=200)
async def handle_clip_action(
    # BackgroundTasks dependency removed as it's no longer needed here
    clip_db_id: int = Path(..., description="Database ID of the clip", gt=0),
    payload: ClipActionPayload = Body(...),
    conn: asyncpg.Connection = Depends(get_db_connection)
):
    """
    Handles user actions: approve/archive now go to intermediate states
    ('approved_pending_deletion', 'archived_pending_deletion') without
    immediate sprite deletion. Other actions proceed as before.
    """
    action = payload.action
    log.info(f"API: Received action '{action}' for clip_db_id: {clip_db_id}")

    target_state = None
    # sprite_to_delete variable removed

    # Map actions to target states, including intermediates
    action_to_state = {
        "approve": "approved_pending_deletion", # Intermediate state
        "skip": "review_skipped",
        "archive": "archived_pending_deletion",   # Intermediate state
        "merge_next": "pending_merge_with_next",
        "retry_sprite_gen": "pending_sprite_generation", # Retry failed sprite generation
    }
    target_state = action_to_state.get(action)

    if not target_state:
        raise HTTPException(status_code=400, detail=f"Invalid action specified: {action}")

    # Define states from which actions are allowed to start
    allowed_source_states = [
        'pending_review', 'review_skipped',
        'merge_failed', 'split_failed',
        'keyframe_failed', 'embedding_failed',
        'sprite_generation_failed' # Allow acting on failed sprite gen
    ]

    # Build SET clauses dynamically
    set_clauses = ["ingest_state = $1", "updated_at = NOW()", "last_error = NULL"]
    params = [target_state]

    # Clear processing metadata unless specifically needed by the target state
    if action not in ["merge_next", "pending_split"]: # Keep metadata for merge/split requests
         set_clauses.append("processing_metadata = NULL")

    # Handle specific actions
    # Set reviewed_at timestamp for approve/archive, but DO NOT nullify sprite fields here
    if action in ["approve", "archive"]:
        set_clauses.append("reviewed_at = NOW()")
        # Comment regarding nullification removed as it doesn't happen here anymore

    if action == 'retry_sprite_gen':
         # Only allow retry from the specific failed state
         allowed_source_states = ['sprite_generation_failed']
         # Comment regarding keeping sprite path/meta NULL removed for clarity

    set_sql = ", ".join(set_clauses)
    lock_id = 2 # Advisory lock category for clips

    try:
        async with conn.transaction():
            # Acquire advisory lock for the specific clip ID
            await conn.execute("SELECT pg_advisory_xact_lock($1, $2)", lock_id, clip_db_id)
            log.debug(f"Acquired lock ({lock_id}, {clip_db_id}) for action '{action}'.")

            # Verify current state *after* acquiring lock (don't need sprite path here anymore)
            current_state = await conn.fetchval(
                "SELECT ingest_state FROM clips WHERE id = $1 FOR UPDATE", # Add FOR UPDATE
                clip_db_id
            )

            if not current_state:
                raise HTTPException(status_code=404, detail="Clip not found.")

            # sprite_to_delete retrieval removed

            # Check if action is allowed from current state
            if current_state not in allowed_source_states:
                 log.warning(f"Action '{action}' rejected for clip {clip_db_id}. Current state '{current_state}' not in allowed states: {allowed_source_states}.")
                 raise HTTPException(status_code=409, detail=f"Action '{action}' cannot be performed from the clip's current state ('{current_state}'). Please refresh.")

            # Construct the final query
            query_params = params + [clip_db_id, allowed_source_states]
            query = f"""
                UPDATE clips SET {set_sql}
                WHERE id = ${len(params) + 1} AND ingest_state = ANY(${len(params) + 2}::text[])
                RETURNING id;
            """
            log.debug(f"Executing Action Query: {query} with params: {query_params}")
            updated_id = await conn.fetchval(query, *query_params)

            if updated_id is None:
                # Re-fetch state to confirm (optional, but good for logging)
                current_state_after = await conn.fetchval("SELECT ingest_state FROM clips WHERE id = $1", clip_db_id) or "NOT FOUND"
                log.warning(f"Failed to update clip {clip_db_id} with action '{action}'. State might have changed. Current state: {current_state_after}")
                raise HTTPException(status_code=409, detail="Clip state changed while processing the action. Please refresh and try again.")

            # If successful, the transaction commits automatically upon exiting 'async with'

        # --- Background Deletion Task Call REMOVED ---
        # Deletion is now handled by the separate scheduled cleanup process.

        log.info(f"API: Successfully updated clip {clip_db_id} state to '{target_state}' via action '{action}'")
        # Use JSONResponse (no background task needed)
        return JSONResponse(
            content={"status": "success", "clip_id": clip_db_id, "new_state": target_state}
        )

    except UniqueViolationError as e:
         log.error(f"DB unique constraint error during clip action for {clip_db_id}: {e}", exc_info=True)
         raise HTTPException(status_code=409, detail=f"Database constraint violated: {e}")
    except HTTPException:
         raise # Re-raise HTTP exceptions directly
    except (PostgresError, psycopg2.Error) as db_err: # Catch specific DB errors
         log.error(f"Database error during action '{action}' for clip {clip_db_id}: {db_err}", exc_info=True)
         raise HTTPException(status_code=500, detail="Internal server error during database operation.")
    except Exception as e:
        log.error(f"Unexpected error processing action '{action}' for clip {clip_db_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error processing action '{action}'.")
    # Lock is automatically released when transaction ends (commit or rollback)

@api_router.post("/{clip_db_id}/split", name="queue_clip_split", status_code=200)
async def queue_clip_split(
    clip_db_id: int = Path(..., description="Database ID of the clip to split", gt=0),
    payload: SplitActionPayload = Body(...), # Use updated Pydantic model
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
            # log.info(f"[Split {clip_db_id}] Fetching clip data from DB...") # Logging preserved if desired
            clip_record = await conn.fetchrow(
                """
                SELECT ingest_state, processing_metadata, sprite_metadata
                FROM clips WHERE id = $1 FOR UPDATE; -- Row lock within transaction
                """, clip_db_id
            )

            if not clip_record:
                # log.warning(f"[Split {clip_db_id}] Clip not found in DB.") # Logging preserved if desired
                raise HTTPException(status_code=404, detail="Clip not found.")

            # Logging preserved if desired
            # log.info(f"[Split {clip_db_id}] Fetched DB record: {dict(clip_record) if clip_record else 'None'}")
            sprite_meta = clip_record['sprite_metadata']
            # log.info(f"[Split {clip_db_id}] Extracted sprite_metadata: {sprite_meta}")
            # log.info(f"[Split {clip_db_id}] Type of sprite_metadata: {type(sprite_meta)}")

            current_state = clip_record['ingest_state']
            if current_state not in allowed_source_states:
                 # log.warning(f"[Split {clip_db_id}] Invalid state '{current_state}'. Allowed: {allowed_source_states}") # Logging preserved if desired
                 raise HTTPException(status_code=409, detail=f"Clip cannot be split from state '{current_state}'. Refresh?")

            # --- VALIDATION CHECK ---
            # Check if it's None or not a dictionary (asyncpg typically returns dict for jsonb)
            if sprite_meta is None or not isinstance(sprite_meta, dict):
                # log.error(f"[Split {clip_db_id}] Sprite metadata missing or invalid type ({type(sprite_meta)}). Cannot validate split frame.") # Logging preserved if desired
                raise HTTPException(status_code=400, detail="Sprite metadata missing or invalid, cannot validate split frame request.")
            # --- END VALIDATION ---

            # log.info(f"[Split {clip_db_id}] Sprite metadata appears valid (type: dict). Proceeding with frame validation.") # Logging preserved if desired

            # --- Frame Validation ---
            total_clip_frames = sprite_meta.get('clip_total_frames')
            # Check if total_clip_frames exists AND is a positive integer
            if not isinstance(total_clip_frames, int) or total_clip_frames <= 1:
                # log.error(f"[Split {clip_db_id}] Invalid total_clip_frames ({total_clip_frames}, type: {type(total_clip_frames)}) in sprite metadata.") # Logging preserved if desired
                raise HTTPException(status_code=400, detail=f"Cannot determine total frames for clip from sprite metadata, cannot validate split.")

            min_frame_margin = 1
            # Frame indices are 0 to total_clip_frames - 1. Valid split points are 1 to total_clip_frames - 2.
            if not (min_frame_margin <= requested_split_frame < (total_clip_frames - min_frame_margin)):
                 err_msg = f"Split frame ({requested_split_frame}) must be between {min_frame_margin} and {total_clip_frames - min_frame_margin - 1} (inclusive) for this clip (Total Frames: {total_clip_frames})."
                 # log.warning(f"[Split {clip_db_id}] Invalid split frame request: {err_msg}") # Logging preserved if desired
                 raise HTTPException(status_code=422, detail=err_msg)

            # --- Prepare Metadata Update ---
            # log.info(f"[Split {clip_db_id}] Split frame {requested_split_frame} validated. Updating DB...") # Logging preserved if desired
            current_metadata_val = clip_record['processing_metadata'] # Could be None, dict, or string
            metadata = {}
            if isinstance(current_metadata_val, str): # Handle if it's still stored as string
                try: metadata = json.loads(current_metadata_val)
                except json.JSONDecodeError: pass
                    # log.warning(f"[Split {clip_db_id}] Could not parse existing processing_metadata JSON: {current_metadata_val}. Overwriting.") # Logging preserved if desired
            elif isinstance(current_metadata_val, dict):
                 metadata = current_metadata_val # If asyncpg gave a dict
            elif current_metadata_val is not None: pass
                 # log.warning(f"[Split {clip_db_id}] Unexpected processing_metadata type ({type(current_metadata_val)}). Overwriting.") # Logging preserved if desired

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
                # log.warning(f"[Split {clip_db_id}] Split queue failed. State changed concurrently? Current state: {current_state_after}") # Logging preserved if desired
                raise HTTPException(status_code=409, detail="Clip state changed while queueing split. Please refresh.")

            # Transaction commits automatically here

        log.info(f"API: Clip {clip_db_id} successfully queued for splitting at frame {requested_split_frame}. New state: '{new_state}'.")
        return {"status": "success", "clip_id": clip_db_id, "new_state": new_state, "message": "Clip queued for splitting."}

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

@api_router.post("/{clip_db_id}/undo", name="undo_clip_action", status_code=200)
async def undo_clip_action(
    clip_db_id: int = Path(..., gt=0),
    conn: asyncpg.Connection = Depends(get_db_connection)
):
    """
    Attempts to revert a clip's state back to 'pending_review'.
    Can now undo from the intermediate 'pending_deletion' states.
    """
    log.info(f"API: Received UNDO request for clip_db_id: {clip_db_id}")

    # Always target 'pending_review' for simplicity.
    target_state = "pending_review"

    # === CORRECTED: Add intermediate states to allowed undo sources ===
    # Define states from which 'undo' is logically allowed
    allowed_undo_source_states = [
        # Original states still undoable
        'review_skipped', 'pending_merge_with_next', 'pending_split',
        'merge_failed', 'split_failed',
        'keyframe_failed', 'embedding_failed',
        'sprite_generation_failed', # Allow undo from sprite fail -> back to pending_review
        # New intermediate states are now undoable
        'approved_pending_deletion',
        'archived_pending_deletion',
    ]
    lock_id = 2 # Clip lock category

    try:
        async with conn.transaction():
            await conn.execute("SELECT pg_advisory_xact_lock($1, $2)", lock_id, clip_db_id)
            log.debug(f"Acquired lock ({lock_id}, {clip_db_id}) for undo action.")

            current_state = await conn.fetchval("SELECT ingest_state FROM clips WHERE id = $1 FOR UPDATE", clip_db_id) # Add FOR UPDATE

            if not current_state:
                raise HTTPException(status_code=404, detail="Clip not found.")

            # Check if undo is allowed from the current state
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
                 raise HTTPException(status_code=409, detail=detail_msg)

            # Clear specific fields when reverting, but leave sprite data intact
            updated_id = await conn.fetchval(
                """
                UPDATE clips
                SET ingest_state = $1,
                    updated_at = NOW(),
                    reviewed_at = NULL,         -- Clear review timestamp regardless of previous state
                    processing_metadata = NULL, -- Clear any pending action metadata
                    last_error = $2             -- Set message indicating undo
                    -- sprite_sheet_filepath and sprite_metadata are NOT changed
                    -- If they existed before the action being undone, they still exist now.
                WHERE id = $3 AND ingest_state = ANY($4::text[]) -- Use ANY for state check / Ensure state hasn't changed
                RETURNING id;
                """,
                target_state,                  # $1 'pending_review'
                'Reverted to pending review by user undo action', # $2 last_error
                clip_db_id,                    # $3 clip id
                allowed_undo_source_states     # $4 allowed source states for undo
            )

            if updated_id is None:
                current_state_after = await conn.fetchval("SELECT ingest_state FROM clips WHERE id = $1", clip_db_id) or "NOT FOUND"
                log.warning(f"Undo failed for clip {clip_db_id}. State changed concurrently? Current state: {current_state_after}")
                raise HTTPException(status_code=409, detail="Clip state changed while processing undo. Please refresh.")

            # Transaction commits automatically

        log.info(f"API: Successfully reverted clip {clip_db_id} to state '{target_state}' via undo.")
        return {"status": "success", "clip_id": clip_db_id, "new_state": target_state}

    except HTTPException:
        raise # Re-raise HTTP exceptions directly
    except (PostgresError, psycopg2.Error) as db_err: # Catch specific DB errors
         log.error(f"Database error during undo for clip {clip_db_id}: {db_err}", exc_info=True)
         raise HTTPException(status_code=500, detail="Internal server error during database operation.")
    except Exception as e:
        log.error(f"Unexpected error processing undo for clip {clip_db_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error processing undo request.")
    # Lock is released when transaction ends
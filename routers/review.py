import json
from fastapi import APIRouter, Request, Depends, Query, Path, Body, HTTPException, BackgroundTasks
from fastapi.responses import HTMLResponse, JSONResponse
import asyncpg
from asyncpg.exceptions import UniqueViolationError, PostgresError
import psycopg2
import httpx # For potential async background tasks if needed

from config import log, CLOUDFRONT_DOMAIN
from database import get_db_connection
from services import format_clip_data
from schemas import ClipActionPayload, SplitActionPayload

# --- S3 Configuration (needed for delete) ---
# Ensure these are correctly imported or defined based on your project structure
try:
    # Assuming s3_client setup is accessible, e.g., from a task file
    from tasks.splice import s3_client, S3_BUCKET_NAME, ClientError # Import ClientError too
    if not S3_BUCKET_NAME:
        raise ImportError("S3_BUCKET_NAME not configured")
    log.info("S3 client imported successfully for review router.")
except ImportError as e:
    log.warning(f"Could not import S3 client config in review.py: {e}. Sprite deletion might fail.")
    s3_client = None
    S3_BUCKET_NAME = None
    # Define dummy ClientError if needed for exception handling below
    class ClientError(Exception): pass

# Separate routers for UI and API for clarity and prefixing
ui_router = APIRouter(
    tags=["Review UI"]
)
api_router = APIRouter(
    prefix="/api/clips", # Prefix for all API actions related to clips
    tags=["Clip Actions"]
)

# Helper function for background S3 deletion
async def delete_s3_object_background(s3_key: str):
    """Attempts to delete an S3 object in the background."""
    logger = log # Use FastAPI logger
    if not s3_key or not s3_client or not S3_BUCKET_NAME:
        logger.warning(f"Background delete skipped: Missing key, client, or bucket name (Key: {s3_key})")
        return
    logger.info(f"Background task: Attempting to delete S3 object: s3://{S3_BUCKET_NAME}/{s3_key}")
    try:
        # Running synchronous boto3 client call in async context.
        # This WILL BLOCK the event loop briefly. For high-concurrency,
        # use a thread pool (e.g., from anyio) or an async library like aioboto3.
        response = s3_client.delete_object(Bucket=S3_BUCKET_NAME, Key=s3_key)
        status_code = response.get('ResponseMetadata', {}).get('HTTPStatusCode')
        if status_code == 204: # 204 No Content is success for delete
             logger.info(f"Background task: Successfully deleted S3 object: {s3_key}")
        else:
             logger.warning(f"Background task: S3 delete for {s3_key} returned status {status_code}. Response: {response}")

    except ClientError as e:
        # Specific AWS errors (e.g., NoSuchKey might be okay if already deleted)
        error_code = e.response.get('Error', {}).get('Code', 'Unknown') if hasattr(e, 'response') else 'Unknown'
        if error_code == 'NoSuchKey':
            logger.warning(f"Background task: S3 object not found (possibly already deleted): {s3_key}")
        else:
            logger.error(f"Background task: Failed to delete S3 object {s3_key}: ErrorCode='{error_code}', Message='{e}'")
    except Exception as e:
         logger.error(f"Background task: Unexpected error deleting S3 object {s3_key}: {e}", exc_info=True)

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
        # Fetch clips pending review, including sprite sheet info and adjacent clips
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
            WHERE ingest_state IN ('pending_review', 'sprite_generation_failed') -- Show ready clips and those that failed sprite gen
            ORDER BY source_video_id, start_time_seconds, id
            LIMIT $1;
            """,
            limit
        )

        # States the *next* clip can be in for merging
        valid_merge_target_states = {'pending_review', 'review_skipped', 'sprite_generation_failed'}

        for record in records:
            record_dict = dict(record) # Convert asyncpg record to dict

            # Determine merge possibility
            can_merge = (
                 record_dict.get('next_clip_id') is not None and
                 record_dict.get('next_clip_state') in valid_merge_target_states
            )
            record_dict['can_merge_next'] = can_merge

            # Use the service function to format basic data
            formatted = format_clip_data(record_dict, request)
            if formatted:
                 # Add sprite-specific data if not handled by format_clip_data
                 # Construct URL safely, checking CLOUDFRONT_DOMAIN
                 sprite_path = record_dict.get('sprite_sheet_filepath')
                 formatted['sprite_sheet_url'] = f"https://{CLOUDFRONT_DOMAIN}/{sprite_path}" if CLOUDFRONT_DOMAIN and sprite_path else sprite_path

                 formatted['sprite_metadata'] = record_dict.get('sprite_metadata') # Pass metadata (should be dict if JSONB)
                 formatted['sprite_error'] = record_dict['ingest_state'] == 'sprite_generation_failed' # Flag for UI
                 formatted['can_merge_next'] = can_merge # Ensure merge flag is present

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
        "CLOUDFRONT_DOMAIN": CLOUDFRONT_DOMAIN # Pass domain for potential JS use
    })

# --- Clip Action API Routes ---

@api_router.post("/{clip_db_id}/action", name="clip_action", status_code=200)
async def handle_clip_action(
    background_tasks: BackgroundTasks, # Inject BackgroundTasks
    clip_db_id: int = Path(..., description="Database ID of the clip", gt=0),
    payload: ClipActionPayload = Body(...),
    conn: asyncpg.Connection = Depends(get_db_connection)
):
    """Handles user actions: approve, skip, archive, merge_next, retry_sprite_gen."""
    action = payload.action
    log.info(f"API: Received action '{action}' for clip_db_id: {clip_db_id}")

    target_state = None
    sprite_to_delete = None # Store key of sprite to delete on success

    # Map actions to target states
    action_to_state = {
        "approve": "review_approved",
        "skip": "review_skipped",
        "archive": "archived",
        "merge_next": "pending_merge_with_next",
        "retry_sprite_gen": "pending_sprite_generation", # Retry failed sprite generation
    }
    target_state = action_to_state.get(action)

    if not target_state:
        raise HTTPException(status_code=400, detail=f"Invalid action specified: {action}")

    # Define states from which actions are allowed
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
    if action in ["approve", "archive"]:
        set_clauses.append("reviewed_at = NOW()")
        # Nullify sprite path/metadata when approving/archiving (deletion handled below)
        set_clauses.append("sprite_sheet_filepath = NULL")
        set_clauses.append("sprite_metadata = NULL")

    if action == 'retry_sprite_gen':
         # Only allow retry from the specific failed state
         allowed_source_states = ['sprite_generation_failed']
         # Keep sprite path/meta as NULL, error already cleared

    set_sql = ", ".join(set_clauses)
    lock_id = 2 # Advisory lock category for clips

    try:
        async with conn.transaction():
            # Acquire advisory lock for the specific clip ID
            await conn.execute("SELECT pg_advisory_xact_lock($1, $2)", lock_id, clip_db_id)
            log.debug(f"Acquired lock ({lock_id}, {clip_db_id}) for action '{action}'.")

            # Verify current state AND get sprite path *after* acquiring lock
            current_data = await conn.fetchrow(
                "SELECT ingest_state, sprite_sheet_filepath FROM clips WHERE id = $1 FOR UPDATE", # Add FOR UPDATE
                clip_db_id
            )

            if not current_data:
                raise HTTPException(status_code=404, detail="Clip not found.")

            current_state = current_data['ingest_state']
            # Get sprite path BEFORE update to queue deletion if needed
            sprite_to_delete = current_data['sprite_sheet_filepath']

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
                log.warning(f"Failed to update clip {clip_db_id} with action '{action}'. State might have changed between check and update. Current state: {current_state_after}")
                raise HTTPException(status_code=409, detail="Clip state changed while processing the action. Please refresh and try again.")

            # If successful, the transaction commits automatically upon exiting 'async with'

        # --- Trigger background deletion AFTER successful commit ---
        if action in ["approve", "archive"] and sprite_to_delete:
            background_tasks.add_task(delete_s3_object_background, sprite_to_delete)
            log.info(f"Queued background task to delete sprite sheet: {sprite_to_delete}")

        log.info(f"API: Successfully updated clip {clip_db_id} state to '{target_state}' via action '{action}'")
        # Use JSONResponse to include background tasks
        return JSONResponse(
            content={"status": "success", "clip_id": clip_db_id, "new_state": target_state},
            background=background_tasks
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
            log.info(f"[Split {clip_db_id}] Fetching clip data from DB...")
            clip_record = await conn.fetchrow(
                """
                SELECT ingest_state, processing_metadata, sprite_metadata
                FROM clips WHERE id = $1 FOR UPDATE; -- Row lock within transaction
                """, clip_db_id
            )

            if not clip_record:
                log.warning(f"[Split {clip_db_id}] Clip not found in DB.")
                raise HTTPException(status_code=404, detail="Clip not found.")

            # --- DETAILED LOGGING ---
            log.info(f"[Split {clip_db_id}] Fetched DB record: {dict(clip_record) if clip_record else 'None'}") # Log the whole record
            sprite_meta = clip_record['sprite_metadata']
            log.info(f"[Split {clip_db_id}] Extracted sprite_metadata: {sprite_meta}")
            log.info(f"[Split {clip_db_id}] Type of sprite_metadata: {type(sprite_meta)}")
            # --- END LOGGING ---

            current_state = clip_record['ingest_state']
            if current_state not in allowed_source_states:
                 log.warning(f"[Split {clip_db_id}] Invalid state '{current_state}'. Allowed: {allowed_source_states}")
                 raise HTTPException(status_code=409, detail=f"Clip cannot be split from state '{current_state}'. Refresh?")

            # --- VALIDATION CHECK ---
            # Check if it's None or not a dictionary (asyncpg typically returns dict for jsonb)
            if sprite_meta is None or not isinstance(sprite_meta, dict):
                log.error(f"[Split {clip_db_id}] Sprite metadata missing or invalid type ({type(sprite_meta)}). Cannot validate split frame.") # Log error before raising
                raise HTTPException(status_code=400, detail="Sprite metadata missing or invalid, cannot validate split frame request.")
            # --- END VALIDATION ---

            log.info(f"[Split {clip_db_id}] Sprite metadata appears valid (type: dict). Proceeding with frame validation.")

            # --- Frame Validation ---
            total_clip_frames = sprite_meta.get('clip_total_frames')
            # Check if total_clip_frames exists AND is a positive integer
            if not isinstance(total_clip_frames, int) or total_clip_frames <= 1:
                log.error(f"[Split {clip_db_id}] Invalid total_clip_frames ({total_clip_frames}, type: {type(total_clip_frames)}) in sprite metadata.")
                raise HTTPException(status_code=400, detail=f"Cannot determine total frames for clip from sprite metadata, cannot validate split.")

            min_frame_margin = 1
            # Frame indices are 0 to total_clip_frames - 1. Valid split points are 1 to total_clip_frames - 2.
            if not (min_frame_margin <= requested_split_frame < (total_clip_frames - min_frame_margin)):
                 err_msg = f"Split frame ({requested_split_frame}) must be between {min_frame_margin} and {total_clip_frames - min_frame_margin - 1} (inclusive) for this clip (Total Frames: {total_clip_frames})."
                 log.warning(f"[Split {clip_db_id}] Invalid split frame request: {err_msg}")
                 raise HTTPException(status_code=422, detail=err_msg)

            # --- Prepare Metadata Update ---
            log.info(f"[Split {clip_db_id}] Split frame {requested_split_frame} validated. Updating DB...")
            current_metadata_val = clip_record['processing_metadata'] # Could be None, dict, or string
            metadata = {}
            if isinstance(current_metadata_val, str): # Handle if it's still stored as string
                try: metadata = json.loads(current_metadata_val)
                except json.JSONDecodeError:
                    log.warning(f"[Split {clip_db_id}] Could not parse existing processing_metadata JSON: {current_metadata_val}. Overwriting.")
                    metadata = {}
            elif isinstance(current_metadata_val, dict):
                 metadata = current_metadata_val # If asyncpg gave a dict
            elif current_metadata_val is not None:
                 log.warning(f"[Split {clip_db_id}] Unexpected processing_metadata type ({type(current_metadata_val)}). Overwriting.")
                 metadata = {}

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
                log.warning(f"[Split {clip_db_id}] Split queue failed. State changed concurrently? Current state: {current_state_after}")
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
    """Attempts to revert a clip's state back to 'pending_review'."""
    log.info(f"API: Received UNDO request for clip_db_id: {clip_db_id}")

    # Always target 'pending_review' for simplicity.
    # If the sprite was deleted (e.g., undoing 'approve'), the UI will show 'sprite unavailable'.
    # The user could then potentially use 'retry_sprite_gen' if needed.
    target_state = "pending_review"

    # Define states from which 'undo' is logically allowed
    allowed_undo_source_states = [
        'review_approved', 'review_skipped', 'archived',
        'pending_merge_with_next', 'pending_split',
        'merge_failed', 'split_failed',
        'keyframe_failed', 'embedding_failed',
        'sprite_generation_failed' # Allow undo from sprite fail -> back to pending_review
    ]
    lock_id = 2 # Clip lock category

    try:
        async with conn.transaction():
            await conn.execute("SELECT pg_advisory_xact_lock($1, $2)", lock_id, clip_db_id)
            log.debug(f"Acquired lock ({lock_id}, {clip_db_id}) for undo action.")

            current_state = await conn.fetchval("SELECT ingest_state FROM clips WHERE id = $1 FOR UPDATE", clip_db_id) # Add FOR UPDATE

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
                    reviewed_at = NULL,         -- Clear review timestamp
                    processing_metadata = NULL, -- Clear split/merge info
                    last_error = $2             -- Set message indicating undo
                    -- Keep sprite_sheet_filepath and sprite_metadata as they are.
                    -- If they were NULLed by approve/archive, they remain NULL.
                    -- If they existed, they remain, allowing immediate review.
                WHERE id = $3 AND ingest_state = $4 -- Ensure state hasn't changed
                RETURNING id;
                """,
                target_state,                  # $1
                'Reverted to pending review by user undo action', # $2
                clip_db_id,                    # $3
                current_state                  # $4
            )

            if updated_id is None:
                current_state_after = await conn.fetchval("SELECT ingest_state FROM clips WHERE id = $1", clip_db_id) or "NOT FOUND"
                log.warning(f"Undo failed for clip {clip_db_id}. State changed concurrently. Current state: {current_state_after}")
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
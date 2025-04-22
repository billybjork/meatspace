import json
from fastapi import APIRouter, Request, Depends, Path, Body, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
import asyncpg
from asyncpg.exceptions import PostgresError, UndefinedTableError, PostgresSyntaxError

from config import log, CLOUDFRONT_DOMAIN
from database import get_db_connection # Using the asyncpg pool dependency
from services import format_clip_data
from schemas import ClipActionPayload, SplitActionPayload

# Routers for UI and API
ui_router = APIRouter(
    tags=["Review UI"]
)
api_router = APIRouter(
    prefix="/api/clips",
    tags=["Clip Actions"]
)

# Constants for Artifacts
ARTIFACT_TYPE_KEYFRAME = "keyframe"
ARTIFACT_TYPE_SPRITE_SHEET = "sprite_sheet"
REPRESENTATIVE_TAG = "representative"

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
        # Fetch the next single clip for review
        # JOIN with clip_artifacts TWICE to get both representative keyframe and sprite sheet info
        record = await conn.fetchrow(
            """
            WITH ClipWithLag AS (
                SELECT
                    c.id, c.clip_identifier, c.clip_filepath,
                    c.start_time_seconds, c.end_time_seconds, c.source_video_id,
                    c.ingest_state, c.processing_metadata, c.last_error,
                    sv.title as source_title,
                    -- Look *behind* to the previous clip in the sequence for merge/group context
                    LAG(c.id) OVER w as previous_clip_id,
                    LAG(c.ingest_state) OVER w as previous_clip_state,
                    kf.s3_key AS representative_keyframe_s3_key, -- Keyframe from artifacts
                    ss.s3_key AS sprite_sheet_s3_key,       -- Sprite sheet key from artifacts
                    ss.metadata AS sprite_artifact_metadata -- Sprite metadata from artifacts
                FROM clips c
                JOIN source_videos sv ON c.source_video_id = sv.id
                LEFT JOIN clip_artifacts kf ON c.id = kf.clip_id -- Join for keyframe
                    AND kf.artifact_type = $1 -- ARTIFACT_TYPE_KEYFRAME
                    AND kf.tag = $2 -- REPRESENTATIVE_TAG
                LEFT JOIN clip_artifacts ss ON c.id = ss.clip_id -- Join for sprite sheet
                    AND ss.artifact_type = $3 -- ARTIFACT_TYPE_SPRITE_SHEET
                WINDOW w AS (PARTITION BY c.source_video_id ORDER BY c.start_time_seconds, c.id)
            )
            SELECT *
            FROM ClipWithLag
            WHERE ingest_state IN (
                'pending_review', 'sprite_generation_failed', 'merge_failed',
                'split_failed', 'keyframe_failed', 'embedding_failed', 'group_failed'
            )
            ORDER BY source_video_id, start_time_seconds, id
            LIMIT 1;
            """,
            ARTIFACT_TYPE_KEYFRAME, REPRESENTATIVE_TAG, ARTIFACT_TYPE_SPRITE_SHEET
        )

        if record:
            record_dict = dict(record) # Convert record to mutable dict
            prev_id = record_dict.get('previous_clip_id')
            prev_state = record_dict.get('previous_clip_state')
            log.info(f"Checking merge/group for clip {record_dict['id']}: Previous ID = {prev_id}, Previous State = '{prev_state}'")

            valid_previous_action_states = {
                'pending_review', 'review_skipped', 'approved_pending_deletion',
                'review_approved', 'archived_pending_deletion', 'archived',
                'grouped_complete'
            }
            can_action_previous = (prev_id is not None and prev_state in valid_previous_action_states)
            log.info(f"Result: can_action_previous = {can_action_previous}")
            record_dict['can_action_previous'] = can_action_previous # Add flag to dict

            formatted = format_clip_data(record_dict, request)
            if formatted:
                 # Use artifact data for sprite URL and metadata
                 sprite_s3_key = record_dict.get('sprite_sheet_s3_key')
                 sprite_artifact_meta = record_dict.get('sprite_artifact_metadata') # This should be a dict

                 formatted['sprite_sheet_url'] = f"https://{CLOUDFRONT_DOMAIN.strip('/')}/{sprite_s3_key.lstrip('/')}" if CLOUDFRONT_DOMAIN and sprite_s3_key else None
                 # Metadata should already be a dict from asyncpg if JSONB
                 formatted['sprite_metadata'] = sprite_artifact_meta if isinstance(sprite_artifact_meta, dict) else None
                 if sprite_s3_key and not formatted['sprite_metadata']:
                     log.warning(f"Sprite key found for clip {record_dict['id']} but metadata is missing or invalid in artifacts table.")

                 # Determine sprite error state
                 formatted['sprite_error'] = record_dict['ingest_state'] == 'sprite_generation_failed'
                 # Pass through other necessary fields
                 formatted['can_action_previous'] = can_action_previous
                 formatted['current_state'] = record_dict['ingest_state']
                 formatted['previous_clip_id'] = record_dict.get('previous_clip_id')
                 formatted['last_error'] = record_dict.get('last_error') # Pass last error for display
                 clip_to_review = formatted
            else:
                 log.error(f"format_clip_data returned None for record ID {record_dict.get('id')}")
                 error_message = "Failed to format clip data."

        else:
             log.info("No clips found in a reviewable state.")
             # Keep error_message as None, template will show 'no clips' message

    except UndefinedTableError as e:
         log.error(f"ERROR accessing DB tables fetching review clips: {e}", exc_info=True)
         error_message = f"Database table error: {e}. Please check application setup."
    except PostgresSyntaxError as e:
         log.error(f"ERROR in SQL syntax fetching review clips: {e}", exc_info=True)
         error_message = f"Database syntax error: {e}. Please check the SQL query in intake_review.py."
    except Exception as e:
        log.error(f"Error fetching clip for review: {e}", exc_info=True)
        error_message = "Failed to load clip for review due to a server error."

    return templates.TemplateResponse("intake_review.html", {
        "request": request,
        "clip": clip_to_review,
        "error": error_message,
        "CLOUDFRONT_DOMAIN": CLOUDFRONT_DOMAIN # Pass domain if needed by template JS
    })


# --- Clip Action API Route (Handles Merge AND Group Actions) ---
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

    action_to_state = {
        "approve": "approved_pending_deletion", "skip": "review_skipped",
        "archive": "archived_pending_deletion", "retry_sprite_gen": "pending_sprite_generation",
        "merge_previous": "marked_for_merge_into_previous", "group_previous": "grouped_complete",
    }
    target_state = action_to_state.get(action)

    if not target_state:
        raise HTTPException(status_code=400, detail=f"Invalid action specified: {action}")

    allowed_source_states = [
        'pending_review', 'review_skipped', 'merge_failed', 'split_failed',
        'keyframe_failed', 'embedding_failed', 'sprite_generation_failed', 'group_failed'
    ]
    if action == 'retry_sprite_gen':
         allowed_source_states = ['sprite_generation_failed']

    valid_previous_action_states = {
        'pending_review', 'review_skipped', 'approved_pending_deletion', 'review_approved',
        'archived_pending_deletion', 'archived', 'grouped_complete'
    }

    lock_id_current = 2; lock_id_previous = 3

    try:
        async with conn.transaction():
            await conn.execute("SELECT pg_advisory_xact_lock($1, $2)", lock_id_current, clip_db_id)
            log.debug(f"Acquired lock ({lock_id_current}, {clip_db_id}) for action '{action}'.")

            current_clip_data = await conn.fetchrow(
                """SELECT ingest_state, processing_metadata, source_video_id, start_time_seconds
                   FROM clips WHERE id = $1 FOR UPDATE""", clip_db_id
            )
            if not current_clip_data: raise HTTPException(status_code=404, detail="Clip not found.")
            current_state = current_clip_data['ingest_state']
            current_source_video_id = current_clip_data['source_video_id']
            current_start_time = current_clip_data['start_time_seconds']

            if current_state not in allowed_source_states:
                 raise HTTPException(status_code=409, detail=f"Action '{action}' cannot be performed from state '{current_state}'.")

            updated_id = None; update_succeeded = False

            if action in ["merge_previous", "group_previous"]:
                previous_clip_record = await conn.fetchrow(
                    """ SELECT id, ingest_state FROM clips
                        WHERE source_video_id = $1 AND start_time_seconds < $2
                        ORDER BY start_time_seconds DESC, id DESC LIMIT 1 FOR UPDATE;""",
                    current_source_video_id, current_start_time
                )
                if not previous_clip_record: raise HTTPException(status_code=400, detail=f"No preceding clip found to {action.split('_')[0]} with.")
                previous_clip_id = previous_clip_record['id']
                await conn.execute("SELECT pg_advisory_xact_lock($1, $2)", lock_id_previous, previous_clip_id)
                previous_clip_state_locked = await conn.fetchval("SELECT ingest_state FROM clips WHERE id = $1 FOR UPDATE", previous_clip_id)
                if previous_clip_state_locked not in valid_previous_action_states:
                     raise HTTPException(status_code=409, detail=f"Cannot {action.split('_')[0]}: The previous clip is in state '{previous_clip_state_locked}'.")

                result_current = ""; result_previous = "" # Initialize
                if action == "merge_previous":
                    current_metadata = {"merge_target_clip_id": previous_clip_id}
                    previous_metadata = {"merge_source_clip_id": clip_db_id}
                    target_state_previous_clip = "pending_merge_target"
                    result_current = await conn.execute(
                         """UPDATE clips SET ingest_state = $1, processing_metadata = $2::jsonb, updated_at = NOW(), last_error = NULL, reviewed_at = NOW()
                            WHERE id = $3 AND ingest_state = ANY($4::text[])""",
                         target_state, current_metadata, clip_db_id, allowed_source_states
                    )
                    result_previous = await conn.execute(
                        """UPDATE clips SET ingest_state = $1, processing_metadata = $2::jsonb, updated_at = NOW(), last_error = NULL
                           WHERE id = $3 AND ingest_state = $4""",
                        target_state_previous_clip, previous_metadata, previous_clip_id, previous_clip_state_locked
                    )
                elif action == "group_previous":
                    target_state_for_group = "grouped_complete"
                    result_current = await conn.execute(
                         """UPDATE clips SET ingest_state = $1, grouped_with_clip_id = $2, processing_metadata = NULL, updated_at = NOW(), last_error = NULL, reviewed_at = NOW()
                            WHERE id = $3 AND ingest_state = ANY($4::text[])""",
                         target_state_for_group, previous_clip_id, clip_db_id, allowed_source_states
                    )
                    result_previous = await conn.execute(
                        """UPDATE clips SET ingest_state = $1, processing_metadata = NULL, updated_at = NOW(), last_error = NULL, reviewed_at = NOW()
                           WHERE id = $2 AND ingest_state = $3""",
                        target_state_for_group, previous_clip_id, previous_clip_state_locked
                    )

                rows_affected_current = int(result_current.split()[-1]) if result_current else 0
                rows_affected_previous = int(result_previous.split()[-1]) if result_previous else 0
                if rows_affected_current > 0:
                    updated_id = clip_db_id; update_succeeded = True
                    if rows_affected_previous == 0: log.warning(f"Previous clip {previous_clip_id} state changed during {action}.")
                else: log.warning(f"Failed to update current clip {clip_db_id} for {action}.")

            else: # Handle standard actions
                set_clauses = ["ingest_state = $1", "updated_at = NOW()", "last_error = NULL"]
                params = [target_state]; param_index = 2
                if action not in ["merge_previous", "pending_split", "group_previous"]:
                     set_clauses.append("processing_metadata = NULL")
                     set_clauses.append("grouped_with_clip_id = NULL")
                if action in ["approve", "archive", "group_previous"]: set_clauses.append("reviewed_at = NOW()")
                elif action in ["skip", "retry_sprite_gen"]: set_clauses.append("reviewed_at = NULL")

                set_sql = ", ".join(set_clauses); query_params = params + [clip_db_id, allowed_source_states]
                where_clause = f"WHERE id = ${param_index} AND ingest_state = ANY(${param_index + 1}::text[])"
                query = f"UPDATE clips SET {set_sql} {where_clause} RETURNING id;"
                log.debug(f"Executing Action Query: {query} with params: {query_params}")
                updated_id = await conn.fetchval(query, *query_params)
                if updated_id: update_succeeded = True

            if not update_succeeded:
                current_state_after = await conn.fetchval("SELECT ingest_state FROM clips WHERE id = $1", clip_db_id) or "NOT FOUND"
                raise HTTPException(status_code=409, detail=f"Clip state changed to '{current_state_after}'. Please refresh.")

        log.info(f"API: Processed action '{action}' for clip {clip_db_id}. New state: '{target_state}'.")
        response_content = {"status": "success", "clip_id": clip_db_id, "new_state": target_state}
        if action == 'group_previous':
            response_content["previous_clip_id"] = previous_clip_id
            response_content["previous_clip_new_state"] = target_state
        return JSONResponse(content=response_content)

    except HTTPException: raise
    except (PostgresError, asyncpg.PostgresError) as db_err:
         log.error(f"Database error during action '{action}' for clip {clip_db_id}: {db_err}", exc_info=True)
         raise HTTPException(status_code=500, detail="Database error occurred.")
    except Exception as e:
        log.error(f"Unexpected error processing action '{action}' for clip {clip_db_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error processing action.")


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
    Validates frame number against sprite sheet artifact metadata.
    """
    requested_split_frame = payload.split_request_at_frame
    log.info(f"API: Received split request for clip_db_id: {clip_db_id} at relative frame: {requested_split_frame}")

    new_state = "pending_split"
    allowed_source_states = ['pending_review', 'review_skipped', 'split_failed']
    lock_id = 2 # Advisory lock category for the clip row

    try:
        async with conn.transaction():
            # 1. Acquire Advisory Lock (prevents concurrent attempts on the same clip ID)
            await conn.execute("SELECT pg_advisory_xact_lock($1, $2)", lock_id, clip_db_id)
            log.debug(f"Acquired advisory lock ({lock_id}, {clip_db_id}) for split action.")

            # 2. Fetch and Lock the core clips row
            clip_base_data = await conn.fetchrow(
                """
                SELECT ingest_state, processing_metadata
                FROM clips
                WHERE id = $1
                FOR UPDATE; -- Lock the specific clips row
                """, clip_db_id
            )

            if not clip_base_data:
                raise HTTPException(status_code=404, detail="Clip not found.")

            current_state = clip_base_data['ingest_state']
            # Note: We don't need current_processing_metadata fetched here, as we overwrite it later.

            # 3. Check if the current state allows splitting
            if current_state not in allowed_source_states:
                 raise HTTPException(status_code=409, detail=f"Clip cannot be split from state '{current_state}'. Refresh?")

            # 4. Fetch the sprite artifact metadata (separate query, no FOR UPDATE needed here)
            sprite_meta_record = await conn.fetchrow(
                """
                SELECT metadata
                FROM clip_artifacts
                WHERE clip_id = $1 AND artifact_type = $2;
                """, clip_db_id, ARTIFACT_TYPE_SPRITE_SHEET
            )
            sprite_meta = sprite_meta_record['metadata'] if sprite_meta_record else None

            # --- Sprite Metadata Validation ---
            if not isinstance(sprite_meta, dict):
                log.error(f"Split rejected for {clip_db_id}: Sprite artifact metadata missing or not a valid dictionary. Type: {type(sprite_meta)}")
                raise HTTPException(status_code=400, detail="Sprite metadata missing or invalid, cannot validate split frame request.")

            total_clip_frames = sprite_meta.get('clip_total_frames_source')
            if not isinstance(total_clip_frames, int) or total_clip_frames <= 1:
                log.error(f"Split rejected for {clip_db_id}: Cannot determine total frames from sprite artifact metadata. Value: {total_clip_frames}")
                raise HTTPException(status_code=400, detail="Cannot determine total frames for clip, cannot validate split.")

            # --- Frame Validation ---
            min_frame_margin = 1 # Example: Cannot split at the very first or very last frame
            # Ensure split frame is within the valid range [margin, total_frames - margin - 1]
            if not (min_frame_margin <= requested_split_frame < (total_clip_frames - min_frame_margin)):
                 err_msg = (f"Split frame ({requested_split_frame}) must be between "
                            f"{min_frame_margin} and {total_clip_frames - min_frame_margin - 1} "
                            f"(inclusive) for this clip (Total Frames: {total_clip_frames}).")
                 log.warning(f"Split rejected for {clip_db_id}: Invalid frame. {err_msg}")
                 raise HTTPException(status_code=422, detail=err_msg) # Unprocessable Entity

            # --- Prepare Metadata Update for clips.processing_metadata ---
            # Store the validated frame number for the split task
            metadata_to_store = {"split_request_at_frame": requested_split_frame}

            # --- Database Update ---
            # Update the clips table (which is already locked FOR UPDATE)
            # Pass the Python dictionary `metadata_to_store` directly
            updated_id = await conn.fetchval(
                """
                UPDATE clips
                SET ingest_state = $1,
                    processing_metadata = $2::jsonb, -- Store split info
                    last_error = NULL,
                    updated_at = NOW()
                WHERE id = $3 AND ingest_state = ANY($4::text[]) -- Concurrency check on state
                RETURNING id;
                """,
                new_state,
                metadata_to_store, # Pass the dictionary
                clip_db_id,
                allowed_source_states
            )

            if updated_id is None:
                # If update failed, check the current state again to provide a better error
                current_state_after = await conn.fetchval("SELECT ingest_state FROM clips WHERE id = $1", clip_db_id) or "NOT FOUND"
                log.warning(f"Split queue update failed for clip {clip_db_id}. Expected states {allowed_source_states}, found '{current_state_after}'.")
                raise HTTPException(status_code=409, detail=f"Clip state changed to '{current_state_after}' while queueing split. Please refresh.")

            # Transaction commits automatically upon exiting 'async with conn.transaction():'

        log.info(f"API: Clip {clip_db_id} successfully queued for splitting at frame {requested_split_frame}. New state: '{new_state}'.")
        return {"status": "success", "clip_id": clip_db_id, "new_state": new_state, "message": "Clip queued for splitting."}

    except HTTPException:
        raise # Re-raise HTTP exceptions directly
    except (PostgresError, asyncpg.PostgresError) as db_err: # Catch specific DB errors
         # Log the specific DB error class
         log.error(f"Database error during split queue for clip {clip_db_id}: {type(db_err).__name__} - {db_err}", exc_info=True)
         # Provide a generic error message to the client
         raise HTTPException(status_code=500, detail="Internal server error during database operation.")
    except Exception as e:
        log.error(f"Unexpected error queueing clip {clip_db_id} for split: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error processing split request.")
    # Advisory lock is released automatically when the transaction ends (commit or rollback)


# --- Undo API Route ---
@api_router.post("/{clip_db_id}/undo", name="undo_clip_action", status_code=200)
async def undo_clip_action(
    clip_db_id: int = Path(..., gt=0),
    conn: asyncpg.Connection = Depends(get_db_connection)
):
    log.info(f"API: Received UNDO request for clip_db_id: {clip_db_id}")
    target_state = "pending_review"
    allowed_undo_source_states = [
        'review_skipped', 'pending_split', 'merge_failed', 'split_failed', 'group_failed',
        'keyframe_failed', 'embedding_failed', 'sprite_generation_failed',
        'approved_pending_deletion', 'archived_pending_deletion',
        'marked_for_merge_into_previous', 'pending_merge_target', 'grouped_complete'
    ]
    lock_id_current = 2; lock_id_previous = 3

    try:
        async with conn.transaction():
            await conn.execute("SELECT pg_advisory_xact_lock($1, $2)", lock_id_current, clip_db_id)
            clip_data = await conn.fetchrow(
                "SELECT ingest_state, processing_metadata, grouped_with_clip_id FROM clips WHERE id = $1 FOR UPDATE", clip_db_id
            )
            if not clip_data: raise HTTPException(status_code=404, detail="Clip not found.")
            current_state = clip_data['ingest_state']; current_metadata = clip_data['processing_metadata']
            current_grouped_with_id = clip_data['grouped_with_clip_id']

            if current_state not in allowed_undo_source_states:
                 final_states = ['review_approved', 'archived', 'merged']
                 if current_state in final_states: detail_msg = f"Cannot undo final state '{current_state}'."
                 elif current_state == target_state: detail_msg = "Clip is already pending review."
                 else: detail_msg = f"Cannot undo from state '{current_state}'."
                 raise HTTPException(status_code=409, detail=detail_msg)

            previous_clip_id = None # Initialize
            if current_state == 'marked_for_merge_into_previous':
                previous_clip_id = current_metadata.get('merge_target_clip_id') if isinstance(current_metadata, dict) else None
                if not isinstance(previous_clip_id, int): raise HTTPException(status_code=500, detail="Cannot undo merge: Corrupted data (target_id).")
                await conn.execute("SELECT pg_advisory_xact_lock($1, $2)", lock_id_previous, previous_clip_id)
                prev_update_result = await conn.execute(
                    """UPDATE clips SET ingest_state = 'pending_review', updated_at = NOW(), processing_metadata = NULL, last_error = $1
                       WHERE id = $2 AND ingest_state = 'pending_merge_target'""",
                    f'Merge cancelled by undo on clip {clip_db_id}', previous_clip_id
                )
                if prev_update_result != "UPDATE 1": raise HTTPException(status_code=409, detail="Could not revert previous clip state (target).")
            elif current_state == 'pending_merge_target':
                 source_clip_id = current_metadata.get('merge_source_clip_id') if isinstance(current_metadata, dict) else None
                 if not isinstance(source_clip_id, int): raise HTTPException(status_code=500, detail="Cannot undo merge: Corrupted data (source_id).")
                 await conn.execute("SELECT pg_advisory_xact_lock($1, $2)", lock_id_previous, source_clip_id)
                 source_update_result = await conn.execute(
                     """UPDATE clips SET ingest_state = 'pending_review', updated_at = NOW(), processing_metadata = NULL, last_error = $1
                        WHERE id = $2 AND ingest_state = 'marked_for_merge_into_previous'""",
                     f'Merge cancelled by undo on target {clip_db_id}', source_clip_id
                 )
                 if source_update_result != "UPDATE 1": raise HTTPException(status_code=409, detail="Could not revert source clip state.")

            set_clauses = ["ingest_state = $1", "updated_at = NOW()", "reviewed_at = NULL", "processing_metadata = NULL", "last_error = $2"]
            params = [target_state]
            undo_error_message = 'Reverted to pending review by user undo'
            if current_state == 'marked_for_merge_into_previous': undo_error_message = f'Merge into clip {previous_clip_id} cancelled by user undo'
            elif current_state == 'pending_merge_target': undo_error_message = f'Merge target status reverted (source was {source_clip_id})'
            elif current_state == 'grouped_complete':
                 undo_error_message = f'Group status reverted (was grouped with {current_grouped_with_id})'
                 set_clauses.append("grouped_with_clip_id = NULL")
            params.append(undo_error_message)

            set_sql = ", ".join(set_clauses); query_params = params + [clip_db_id, allowed_undo_source_states]
            where_id_index = len(params) + 1; where_state_index = len(params) + 2
            query = f"UPDATE clips SET {set_sql} WHERE id = ${where_id_index} AND ingest_state = ANY(${where_state_index}::text[]) RETURNING id;"
            log.debug(f"Executing Undo Query: {query} with params: {query_params}")
            updated_id = await conn.fetchval(query, *query_params)

            if updated_id is None:
                current_state_after = await conn.fetchval("SELECT ingest_state FROM clips WHERE id = $1", clip_db_id) or "NOT FOUND"
                raise HTTPException(status_code=409, detail=f"Clip state changed to '{current_state_after}' while processing undo.")

        log.info(f"API: Successfully reverted clip {clip_db_id} to state '{target_state}' via undo.")
        return {"status": "success", "clip_id": clip_db_id, "new_state": target_state}

    except HTTPException: raise
    except (PostgresError, asyncpg.PostgresError) as db_err:
         log.error(f"Database error during undo for clip {clip_db_id}: {db_err}", exc_info=True)
         raise HTTPException(status_code=500, detail="Internal server error during database operation.")
    except Exception as e:
        log.error(f"Unexpected error processing undo for clip {clip_db_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error processing undo request.")
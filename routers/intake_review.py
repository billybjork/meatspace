import json
from fastapi import APIRouter, Request, Depends, Path, Body, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
import asyncpg
from asyncpg.exceptions import PostgresError, UndefinedTableError, PostgresSyntaxError
from pydantic import BaseModel, Field, ValidationError # Keep ValidationError
from typing import Optional, Dict, Any

from config import log, CLOUDFRONT_DOMAIN
from database import get_db_connection
from schemas import ClipActionPayload, SplitActionPayload

# --- Schemas specific to this file ---
class ClipUndoPayload(BaseModel):
    clip_db_id: int

api_router = APIRouter(
    prefix="/api/clips",
    tags=["Clip Actions"]
)

# Constants for Artifacts
ARTIFACT_TYPE_KEYFRAME = "keyframe"
ARTIFACT_TYPE_SPRITE_SHEET = "sprite_sheet"
REPRESENTATIVE_TAG = "representative"

# --- Base Model with Pydantic V2 Config ---
class _BaseModel(BaseModel):
    model_config = {
        "populate_by_name": True, # Allow instantiation using field names even if aliases exist
        # "from_attributes": True # Use if loading from ORM objects
    }

# --- Models Inheriting from _BaseModel ---
class SpriteMeta(_BaseModel):
    tile_width: int
    tile_height: int = Field(..., alias="tile_height_calculated")
    cols: int
    rows: int
    total_sprite_frames: int
    clip_fps: float = Field(..., alias="clip_fps_source")
    clip_total_frames: int

class ClipForReview(_BaseModel):
    db_id: int  = Field(..., alias="id")
    identifier: str = Field(..., alias="clip_identifier")
    title: str
    source_video_id: int
    source_title: str
    start_s: float = Field(..., alias="start_time_seconds")
    end_s:   float = Field(..., alias="end_time_seconds")

    # all optionals MUST get a default so they’re not required
    sprite_meta:      Optional[SpriteMeta] = None
    sprite_url:       Optional[str]        = None
    keyframe_url:     Optional[str]        = None   # ← added default
    ingest_state:     str
    can_action_previous: bool
    previous_clip_id: Optional[int]        = None
    last_error:       Optional[str]        = None   # ← added default

class NextClipResponse(_BaseModel):
    done: bool
    clip: Optional[ClipForReview]


@api_router.get(
    "/review/next",
    response_model=NextClipResponse,
    # No response_model_by_alias needed here
    tags=["Review UI"]
)
async def get_next_clip_for_review(
    conn: asyncpg.Connection = Depends(get_db_connection)
):
    """
    Returns the next clip that needs review as JSON, or `{"done": true}` if none.
    Only clips whose `next_state IS NULL` (i.e. not soft-committed) are eligible.
    """
    rec = await conn.fetchrow(
        """
        WITH cte AS (
            SELECT
                c.*,                       -- includes next_state, etc.
                sv.title AS source_title,
                LAG(c.id)  OVER w AS previous_clip_id,
                LAG(c.ingest_state) OVER w AS previous_clip_state,
                kf.s3_key AS keyframe_s3_key,
                ss.s3_key AS sprite_s3_key,
                ss.metadata AS sprite_meta_raw
            FROM clips c
            JOIN source_videos sv ON sv.id = c.source_video_id
            LEFT JOIN clip_artifacts kf ON kf.clip_id = c.id
              AND kf.artifact_type = $1 AND kf.tag = $2
            LEFT JOIN clip_artifacts ss ON ss.clip_id = c.id
              AND ss.artifact_type = $3
            WINDOW w AS (PARTITION BY c.source_video_id
                         ORDER BY c.start_time_seconds, c.id)
        )
        SELECT *
        FROM cte
        WHERE ingest_state IN (
              'pending_review', 'sprite_generation_failed', 'merge_failed',
              'split_failed',   'keyframe_failed',        'embedding_failed',
              'group_failed'
        )
          AND next_state IS NULL
        ORDER BY source_video_id, start_time_seconds, id
        LIMIT 1;
        """,
        ARTIFACT_TYPE_KEYFRAME, REPRESENTATIVE_TAG, ARTIFACT_TYPE_SPRITE_SHEET,
    )

    if rec is None:
         # Return the structure FastAPI expects for validation
         # Option B1: Use model_dump explicitly
         empty_response = NextClipResponse(done=True, clip=None)
         return empty_response.model_dump(by_alias=False)
         # Option B2: Return dict (should also work due to populate_by_name)
         # return {"done": True, "clip": None}

    rec_dict = dict(rec)

    # --- Data Massaging Block (Produces dict with FIELD NAMES) ---
    # 1. Rename keys...
    if "id" in rec_dict: rec_dict["db_id"] = rec_dict.pop("id")
    if "clip_identifier" in rec_dict: rec_dict["identifier"] = rec_dict.pop("clip_identifier")
    if "start_time_seconds" in rec_dict: rec_dict["start_s"] = rec_dict.pop("start_time_seconds")
    if "end_time_seconds" in rec_dict: rec_dict["end_s"] = rec_dict.pop("end_time_seconds")
    # 2. Handle sprite_meta...
    sprite_meta_output = None
    sprite_meta_raw = rec_dict.pop("sprite_meta_raw", None)
    if isinstance(sprite_meta_raw, dict):
        sprite_meta_dict = {}
        if "tile_width" in sprite_meta_raw: sprite_meta_dict["tile_width"] = sprite_meta_raw["tile_width"]
        if "tile_height_calculated" in sprite_meta_raw: sprite_meta_dict["tile_height"] = sprite_meta_raw["tile_height_calculated"] # Use field name key
        if "cols" in sprite_meta_raw: sprite_meta_dict["cols"] = sprite_meta_raw["cols"]
        if "rows" in sprite_meta_raw: sprite_meta_dict["rows"] = sprite_meta_raw["rows"]
        if "total_sprite_frames" in sprite_meta_raw: sprite_meta_dict["total_sprite_frames"] = sprite_meta_raw["total_sprite_frames"]
        if "clip_fps_source" in sprite_meta_raw: sprite_meta_dict["clip_fps"] = sprite_meta_raw["clip_fps_source"] # Use field name key
        if "clip_total_frames_source" in sprite_meta_raw: sprite_meta_dict["clip_total_frames"] = sprite_meta_raw["clip_total_frames_source"]
        elif "clip_total_frames" in sprite_meta_raw: sprite_meta_dict["clip_total_frames"] = sprite_meta_raw["clip_total_frames"]
        sprite_meta_output = sprite_meta_dict

    rec_dict["sprite_meta"] = sprite_meta_output # Assign the dictionary or None

    # 3. Build title...
    ident = rec_dict.get("identifier", "")
    rec_dict["title"] = ident.replace("_", " ").replace("-", " ").title()
    # 4. Set can_action_previous...
    prev_state_ok = rec_dict.get("previous_clip_state") in {
        'pending_review', 'review_skipped', 'approved_pending_deletion',
        'review_approved', 'archived_pending_deletion', 'archived',
        'grouped_complete'
    }
    rec_dict["can_action_previous"] = bool(rec_dict.get("previous_clip_id") and prev_state_ok)
    # 5. CloudFront URLs...
    sprite_s3_key = rec_dict.pop("sprite_s3_key", None)
    keyframe_s3_key = rec_dict.pop("keyframe_s3_key", None)
    if CLOUDFRONT_DOMAIN and sprite_s3_key: rec_dict["sprite_url"] = f"https://{CLOUDFRONT_DOMAIN.strip('/')}/{sprite_s3_key.lstrip('/')}"
    else: rec_dict["sprite_url"] = None
    if CLOUDFRONT_DOMAIN and keyframe_s3_key: rec_dict["keyframe_url"] = f"https://{CLOUDFRONT_DOMAIN.strip('/')}/{keyframe_s3_key.lstrip('/')}"
    else: rec_dict["keyframe_url"] = None

    # Remove extra fields...
    fields_to_remove = [
        "clip_filepath", "start_frame", "end_frame", "created_at", "retry_count",
        "updated_at", "reviewed_at", "keyframed_at", "embedded_at",
        "processing_metadata", "grouped_with_clip_id", "next_state",
        "action_committed_at", "previous_clip_state",
        "embedding", "embedding_model", "embedding_strategy", "is_public",
        "clip_duration_seconds",
    ]
    for field in fields_to_remove:
        rec_dict.pop(field, None)
    # --- End Data Massaging ---

    # --- Build & return the response ------------------------------------
    try:
        # 1 – instantiate the inner model with FIELD names (works because
        #     we set populate_by_name=True in _BaseModel.model_config)
        clip_obj = ClipForReview(**rec_dict)

        # 2 – wrap in the outer response model
        response_obj = NextClipResponse(done=False, clip=clip_obj)

        # 3 – dump **without** aliases so the JSON uses start_s / end_s, etc.
        #     exclude_none=True keeps nulls out if you prefer.
        return response_obj.model_dump(by_alias=False, exclude_none=True)

    except ValidationError as e:
        log.error(
            "Pydantic validation failed creating response model: %s \nData: %s",
            e, rec_dict, exc_info=True
        )
        raise HTTPException(
            status_code=500,
            detail="Internal server error preparing response data."
        )


# --- Clip Action API Route (Handles Merge AND Group Actions - SOFT COMMIT) ---
@api_router.post("/{clip_db_id}/action", name="clip_action", status_code=200)
async def handle_clip_action(
    clip_db_id: int = Path(..., description="Database ID of the clip", gt=0),
    payload: ClipActionPayload = Body(...),
    conn: asyncpg.Connection = Depends(get_db_connection)
):
    action = payload.action
    log.info(f"API: Received action '{action}' for clip_db_id: {clip_db_id} (soft commit)")

    intended_next_state = None # Renamed for clarity
    previous_clip_id = None # Needed for merge_previous and group_previous

    action_to_state = {
        "approve": "approved_pending_deletion", "skip": "review_skipped",
        "archive": "archived_pending_deletion", "retry_sprite_gen": "pending_sprite_generation",
        "merge_previous": "marked_for_merge_into_previous", "group_previous": "grouped_complete",
    }
    intended_next_state = action_to_state.get(action)

    if not intended_next_state:
        raise HTTPException(status_code=400, detail=f"Invalid action specified: {action}")

    allowed_source_states = [
        'pending_review', 'review_skipped', 'merge_failed', 'split_failed',
        'keyframe_failed', 'embedding_failed', 'sprite_generation_failed', 'group_failed'
        # Should not already have a next_state set
    ]
    if action == 'retry_sprite_gen':
         allowed_source_states = ['sprite_generation_failed']

    valid_previous_action_states = {
        'pending_review', 'review_skipped', 'approved_pending_deletion', 'review_approved',
        'archived_pending_deletion', 'archived', 'grouped_complete'
        # Previous clip should also not have a next_state set
    }

    lock_id_current = 2; lock_id_previous = 3

    try:
        async with conn.transaction():
            await conn.execute("SELECT pg_advisory_xact_lock($1, $2)", lock_id_current, clip_db_id)
            log.debug(f"Acquired lock ({lock_id_current}, {clip_db_id}) for action '{action}'.")

            current_clip_data = await conn.fetchrow(
                """SELECT ingest_state, next_state, processing_metadata, source_video_id, start_time_seconds
                   FROM clips WHERE id = $1 FOR UPDATE""", clip_db_id
            )
            if not current_clip_data: raise HTTPException(status_code=404, detail="Clip not found.")
            current_state = current_clip_data['ingest_state']
            current_next_state = current_clip_data['next_state'] # Check if already soft-committed
            current_source_video_id = current_clip_data['source_video_id']
            current_start_time = current_clip_data['start_time_seconds']

            if current_next_state is not None:
                 raise HTTPException(status_code=409, detail=f"Action already pending for this clip (next state: '{current_next_state}'). Undo first if needed.")

            if current_state not in allowed_source_states:
                 # Allow retry from failed states even if next_state was previously set but failed
                 if not (action == 'retry_sprite_gen' and current_state == 'sprite_generation_failed'):
                    raise HTTPException(status_code=409, detail=f"Action '{action}' cannot be performed from state '{current_state}'.")

            update_succeeded = False; now_ts = "NOW()"; result_current = ""; result_previous = ""

            if action in ["merge_previous", "group_previous"]:
                previous_clip_record = await conn.fetchrow(
                    """ SELECT id, ingest_state, next_state FROM clips
                        WHERE source_video_id = $1 AND start_time_seconds < $2
                        ORDER BY start_time_seconds DESC, id DESC LIMIT 1 FOR UPDATE;""",
                    current_source_video_id, current_start_time
                )
                if not previous_clip_record: raise HTTPException(status_code=400, detail=f"No preceding clip found to {action.split('_')[0]} with.")
                previous_clip_id = previous_clip_record['id']
                await conn.execute("SELECT pg_advisory_xact_lock($1, $2)", lock_id_previous, previous_clip_id)
                previous_clip_state_locked = await conn.fetchval("SELECT ingest_state FROM clips WHERE id = $1 FOR UPDATE", previous_clip_id)
                previous_clip_next_state_locked = await conn.fetchval("SELECT next_state FROM clips WHERE id = $1 FOR UPDATE", previous_clip_id)

                if previous_clip_next_state_locked is not None:
                    raise HTTPException(status_code=409, detail=f"Cannot {action.split('_')[0]}: The previous clip already has a pending action (next state: '{previous_clip_next_state_locked}').")
                if previous_clip_state_locked not in valid_previous_action_states:
                     raise HTTPException(status_code=409, detail=f"Cannot {action.split('_')[0]}: The previous clip is in state '{previous_clip_state_locked}'.")

                if action == "merge_previous":
                    current_metadata = {"merge_target_clip_id": previous_clip_id}
                    previous_metadata = {"merge_source_clip_id": clip_db_id}
                    target_state_previous_clip = "pending_merge_target"
                    result_current = await conn.execute(
                         f"""UPDATE clips
                            SET next_state = $1,
                                action_committed_at = {now_ts},
                                processing_metadata = $2::jsonb,
                                updated_at = {now_ts},
                                last_error = NULL,
                                reviewed_at = {now_ts}
                            WHERE id = $3
                              AND ingest_state = ANY($4::text[]) AND next_state IS NULL""",
                         intended_next_state, current_metadata, clip_db_id, allowed_source_states
                    )
                    result_previous = await conn.execute(
                        f"""UPDATE clips
                           SET next_state = $1,
                               action_committed_at = {now_ts},
                               processing_metadata = $2::jsonb,
                               updated_at = {now_ts},
                               last_error = NULL
                           WHERE id = $3
                             AND ingest_state = $4 AND next_state IS NULL""",
                        target_state_previous_clip, previous_metadata, previous_clip_id, previous_clip_state_locked
                    )
                elif action == "group_previous":
                    target_state_for_group = "grouped_complete" # This is the final state for group
                    result_current = await conn.execute(
                         f"""UPDATE clips
                            SET next_state = $1, -- Group action commits immediately conceptually
                                action_committed_at = {now_ts},
                                grouped_with_clip_id = $2,
                                processing_metadata = NULL,
                                updated_at = {now_ts},
                                last_error = NULL,
                                reviewed_at = {now_ts}
                            WHERE id = $3
                              AND ingest_state = ANY($4::text[]) AND next_state IS NULL""",
                         target_state_for_group, previous_clip_id, clip_db_id, allowed_source_states
                    )
                    result_previous = await conn.execute(
                        f"""UPDATE clips
                           SET next_state = $1, -- Group action commits immediately conceptually
                               action_committed_at = {now_ts},
                               processing_metadata = NULL,
                               updated_at = {now_ts},
                               last_error = NULL,
                               reviewed_at = {now_ts} -- Mark previous as reviewed too
                           WHERE id = $2
                             AND ingest_state = $3 AND next_state IS NULL""",
                        target_state_for_group, previous_clip_id, previous_clip_state_locked
                    )

                rows_affected_current = int(result_current.split()[-1]) if result_current.startswith("UPDATE") else 0
                rows_affected_previous = int(result_previous.split()[-1]) if result_previous.startswith("UPDATE") else 0

                if rows_affected_current > 0 and rows_affected_previous > 0:
                    update_succeeded = True
                    log.info(f"Successfully set next_state for {action} on {clip_db_id} and {previous_clip_id}.")
                elif rows_affected_current > 0 and rows_affected_previous == 0:
                     # This should ideally rollback due to transaction, but log if somehow happens
                     log.warning(f"Inconsistent update for {action}: Current clip {clip_db_id} updated, but previous clip {previous_clip_id} failed or state changed.")
                     # Raise an error to ensure rollback
                     raise HTTPException(status_code=500, detail=f"Failed to update previous clip {previous_clip_id} during {action}. Rolling back.")
                elif rows_affected_current == 0 and rows_affected_previous > 0:
                     log.warning(f"Inconsistent update for {action}: Previous clip {previous_clip_id} updated, but current clip {clip_db_id} failed or state changed.")
                     raise HTTPException(status_code=500, detail=f"Failed to update current clip {clip_db_id} during {action}. Rolling back.")
                else:
                    log.warning(f"Failed to update both clips for {action}. Maybe states changed?")
                    # Check states again to provide better feedback
                    curr_clip_after = await conn.fetchrow("SELECT ingest_state, next_state FROM clips WHERE id = $1", clip_db_id)
                    prev_clip_after = await conn.fetchrow("SELECT ingest_state, next_state FROM clips WHERE id = $1", previous_clip_id)
                    raise HTTPException(status_code=409, detail=f"Could not perform {action}. Current clip state: {curr_clip_after}, Previous clip state: {prev_clip_after}. Please refresh.")

            else: # Handle standard actions (non-merge/group)
                set_clauses = ["next_state = $1", f"action_committed_at = {now_ts}", f"updated_at = {now_ts}", "last_error = NULL"]
                params = [intended_next_state]; param_index = 2

                # Clear metadata/grouping unless it's specifically for retry
                if action not in ["retry_sprite_gen"]:
                     set_clauses.append("processing_metadata = NULL")
                     set_clauses.append("grouped_with_clip_id = NULL")

                # Set reviewed timestamp appropriately
                if action in ["approve", "archive", "group_previous"]: # Group handled above but keep logic here too
                    set_clauses.append(f"reviewed_at = {now_ts}")
                elif action in ["skip", "retry_sprite_gen"]:
                    set_clauses.append("reviewed_at = NULL") # Clear reviewed status if skipping/retrying

                set_sql = ", ".join(set_clauses)
                query_params = params + [clip_db_id, allowed_source_states]
                # Add condition to only update if next_state IS NULL
                where_clause = f"WHERE id = ${param_index} AND ingest_state = ANY(${param_index + 1}::text[]) AND next_state IS NULL"
                query = f"UPDATE clips SET {set_sql} {where_clause} RETURNING id;"

                log.debug(f"Executing Action Query: {query} with params: {query_params}")
                updated_id = await conn.fetchval(query, *query_params)
                if updated_id: update_succeeded = True

            if not update_succeeded:
                # Check state again to see why it failed (likely next_state got set concurrently)
                current_data_after = await conn.fetchrow("SELECT ingest_state, next_state FROM clips WHERE id = $1", clip_db_id)
                current_state_after = current_data_after['ingest_state'] if current_data_after else "NOT FOUND"
                current_next_state_after = current_data_after['next_state'] if current_data_after else "N/A"
                if current_next_state_after is not None:
                    raise HTTPException(status_code=409, detail=f"Action already pending for this clip (next state: '{current_next_state_after}'). Please refresh.")
                else:
                    raise HTTPException(status_code=409, detail=f"Clip state changed to '{current_state_after}'. Please refresh.")

        # If transaction succeeded:
        log.info(f"API: Soft-committed action '{action}' for clip {clip_db_id}. next_state set to '{intended_next_state}'.")
        response_content = {"status": "success", "clip_id": clip_db_id, "next_state": intended_next_state}
        if action == 'group_previous' or action == 'merge_previous':
            response_content["previous_clip_id"] = previous_clip_id
            response_content["previous_clip_next_state"] = target_state_previous_clip if action == 'merge_previous' else target_state_for_group
        return JSONResponse(content=response_content)

    except HTTPException: raise # Re-raise HTTP exceptions directly
    except (PostgresError, asyncpg.PostgresError) as db_err:
         log.error(f"Database error during action '{action}' for clip {clip_db_id}: {db_err}", exc_info=True)
         raise HTTPException(status_code=500, detail="Database error occurred.")
    except Exception as e:
        log.error(f"Unexpected error processing action '{action}' for clip {clip_db_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error processing action.")


# --- Split API Route (Remains largely the same, as it updates state directly) ---
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
    NOTE: This is a direct state change, not using next_state, as split is a specific queue.
    """
    requested_split_frame = payload.split_request_at_frame
    log.info(f"API: Received split request for clip_db_id: {clip_db_id} at relative frame: {requested_split_frame}")

    new_state = "pending_split"
    # Allow splitting only from states where it makes sense, and where no other action is pending
    allowed_source_states = ['pending_review', 'review_skipped', 'split_failed']
    lock_id = 2 # Advisory lock category for the clip row

    try:
        async with conn.transaction():
            # 1. Acquire Advisory Lock
            await conn.execute("SELECT pg_advisory_xact_lock($1, $2)", lock_id, clip_db_id)
            log.debug(f"Acquired advisory lock ({lock_id}, {clip_db_id}) for split action.")

            # 2. Fetch and Lock the core clips row, check next_state
            clip_base_data = await conn.fetchrow(
                """
                SELECT ingest_state, next_state, processing_metadata
                FROM clips
                WHERE id = $1
                FOR UPDATE; -- Lock the specific clips row
                """, clip_db_id
            )

            if not clip_base_data:
                raise HTTPException(status_code=404, detail="Clip not found.")

            current_state = clip_base_data['ingest_state']
            current_next_state = clip_base_data['next_state']

            # 3. Check if the current state allows splitting AND no action is pending
            if current_next_state is not None:
                raise HTTPException(status_code=409, detail=f"Cannot split: An action is already pending (next state: '{current_next_state}'). Undo first.")
            if current_state not in allowed_source_states:
                 raise HTTPException(status_code=409, detail=f"Clip cannot be split from state '{current_state}'. Refresh?")

            # 4. Fetch the sprite artifact metadata
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
            min_frame_margin = 1
            if not (min_frame_margin <= requested_split_frame < (total_clip_frames - min_frame_margin)):
                 err_msg = (f"Split frame ({requested_split_frame}) must be between "
                            f"{min_frame_margin} and {total_clip_frames - min_frame_margin - 1} "
                            f"(inclusive) for this clip (Total Frames: {total_clip_frames}).")
                 log.warning(f"Split rejected for {clip_db_id}: Invalid frame. {err_msg}")
                 raise HTTPException(status_code=422, detail=err_msg)

            # --- Prepare Metadata Update for clips.processing_metadata ---
            metadata_to_store = {"split_request_at_frame": requested_split_frame}

            # --- Database Update ---
            # Update ingest_state directly, clear next_state/action_committed_at just in case
            updated_id = await conn.fetchval(
                """
                UPDATE clips
                SET ingest_state = $1,
                    processing_metadata = $2::jsonb,
                    last_error = NULL,
                    next_state = NULL, -- Ensure next_state is cleared
                    action_committed_at = NULL, -- Ensure commit time is cleared
                    updated_at = NOW()
                WHERE id = $3
                  AND ingest_state = ANY($4::text[]) -- Concurrency check on state
                  AND next_state IS NULL -- Double check no action pending
                RETURNING id;
                """,
                new_state,
                metadata_to_store,
                clip_db_id,
                allowed_source_states
            )

            if updated_id is None:
                # If update failed, check the current state/next_state again
                current_data_after = await conn.fetchrow("SELECT ingest_state, next_state FROM clips WHERE id = $1", clip_db_id)
                current_state_after = current_data_after['ingest_state'] if current_data_after else "NOT FOUND"
                current_next_state_after = current_data_after['next_state'] if current_data_after else "N/A"
                log.warning(f"Split queue update failed for clip {clip_db_id}. Expected state in {allowed_source_states} and next_state NULL, found state='{current_state_after}', next_state='{current_next_state_after}'.")
                if current_next_state_after is not None:
                     raise HTTPException(status_code=409, detail=f"Clip action changed to '{current_next_state_after}' while queueing split. Please refresh.")
                else:
                     raise HTTPException(status_code=409, detail=f"Clip state changed to '{current_state_after}' while queueing split. Please refresh.")

        log.info(f"API: Clip {clip_db_id} successfully queued for splitting at frame {requested_split_frame}. New state: '{new_state}'.")
        return {"status": "success", "clip_id": clip_db_id, "new_state": new_state, "message": "Clip queued for splitting."}

    except HTTPException:
        raise
    except (PostgresError, asyncpg.PostgresError) as db_err:
         log.error(f"Database error during split queue for clip {clip_db_id}: {type(db_err).__name__} - {db_err}", exc_info=True)
         raise HTTPException(status_code=500, detail="Internal server error during database operation.")
    except Exception as e:
        log.error(f"Unexpected error queueing clip {clip_db_id} for split: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error processing split request.")


# --- Undo API Route (Clears next_state and action_committed_at) ---
@api_router.post("/review/undo", name="undo_clip_action", status_code=200)
async def undo_last_action(
    payload: ClipUndoPayload, # Use Pydantic model for payload
    conn: asyncpg.Connection = Depends(get_db_connection)
):
    """
    Undoes a soft-committed action by clearing next_state and action_committed_at.
    Handles potential merge/group pairs by checking metadata.
    """
    clip_id = payload.clip_db_id
    log.info(f"API: Received UNDO request for clip_db_id: {clip_id}")

    lock_id_current = 2; lock_id_previous = 3 # Use same lock categories as action

    try:
        async with conn.transaction():
            # Lock the primary clip first
            await conn.execute("SELECT pg_advisory_xact_lock($1, $2)", lock_id_current, clip_id)
            log.debug(f"Acquired lock ({lock_id_current}, {clip_id}) for undo action.")

            # Fetch data, including processing_metadata to check for merge/group targets
            clip_data = await conn.fetchrow(
                """SELECT ingest_state, next_state, action_committed_at, processing_metadata
                   FROM clips WHERE id = $1 FOR UPDATE""",
                clip_id
            )

            if not clip_data:
                raise HTTPException(status_code=404, detail="Clip not found.")

            current_next_state = clip_data['next_state']
            current_action_committed_at = clip_data['action_committed_at']
            current_metadata = clip_data['processing_metadata']

            # Check if there's actually an action to undo (next_state is set)
            if current_next_state is None and current_action_committed_at is None:
                log.warning(f"Undo requested for clip {clip_id}, but no action is pending.")
                # Allow proceeding if state is one that *could* be undone, maybe it just committed?
                # This handles the race condition where Prefect picks it up JUST as undo is clicked.
                # However, the UPDATE later will return 0 rows affected in that case.

            # --- Identify potential related clip for merge/group ---
            ids_to_update = [clip_id]
            previous_id_to_unlock = None
            if isinstance(current_metadata, dict):
                # Check if this clip was marked to merge *into* a previous one
                merge_target_id = current_metadata.get('merge_target_clip_id')
                # Check if this clip was marked as the target *of* a merge
                merge_source_id = current_metadata.get('merge_source_clip_id')

                if merge_target_id and current_next_state == 'marked_for_merge_into_previous':
                    log.info(f"Undo involves previous clip {merge_target_id} (merge target)")
                    ids_to_update.append(merge_target_id)
                    previous_id_to_unlock = merge_target_id
                elif merge_source_id and current_next_state == 'pending_merge_target':
                     log.info(f"Undo involves source clip {merge_source_id} (merge source)")
                     ids_to_update.append(merge_source_id)
                     previous_id_to_unlock = merge_source_id
                # Add similar check here if 'group_previous' stored info in metadata

            # If a previous clip is involved, lock it too
            if previous_id_to_unlock:
                await conn.execute("SELECT pg_advisory_xact_lock($1, $2)", lock_id_previous, previous_id_to_unlock)
                log.debug(f"Acquired lock ({lock_id_previous}, {previous_id_to_unlock}) for related clip during undo.")
                # We need FOR UPDATE on the related clip as well if we're modifying it
                await conn.execute("SELECT 1 FROM clips WHERE id = $1 FOR UPDATE", previous_id_to_unlock)


            # --- Execute the Update ---
            # Clear next_state and action_committed_at for all involved clips
            # Only update rows that actually *have* a next_state set (or the primary clip if user insists)
            undo_error_message = 'Reverted pending action by user undo'
            result = await conn.execute(
                """
                UPDATE clips
                   SET next_state = NULL,
                       action_committed_at = NULL,
                       updated_at = NOW(),
                       -- Clear related metadata/grouping only if we clear next_state
                       processing_metadata = CASE WHEN next_state IS NOT NULL THEN NULL ELSE processing_metadata END,
                       grouped_with_clip_id = CASE WHEN next_state IS NOT NULL THEN NULL ELSE grouped_with_clip_id END,
                       last_error = CASE WHEN next_state IS NOT NULL THEN $1 ELSE last_error END,
                       reviewed_at = CASE WHEN next_state IS NOT NULL THEN NULL ELSE reviewed_at END -- Clear reviewed status on undo
                 WHERE id = ANY($2::int[])
                   AND (next_state IS NOT NULL OR id = $3) -- Ensure we try to update primary clip even if state changed
                """,
                undo_error_message, list(set(ids_to_update)), clip_id # Use set to handle duplicates just in case
            )

            rows_affected = int(result.split()[-1]) if result.startswith("UPDATE") else 0

            if rows_affected == 0:
                # Check again why - did the state get committed by Prefect?
                clip_check = await conn.fetchrow("SELECT ingest_state, next_state FROM clips WHERE id = $1", clip_id)
                if clip_check and clip_check['next_state'] is None and clip_check['ingest_state'] != 'pending_review':
                     log.warning(f"Undo failed for clip {clip_id}. Rows affected: 0. State may have already been committed by background process.")
                     raise HTTPException(status_code=409, detail="Too late to undo. The action has likely been processed.")
                elif clip_check and clip_check['next_state'] is None and clip_check['ingest_state'] == 'pending_review':
                     log.info(f"Undo for clip {clip_id} resulted in 0 rows updated, but state is 'pending_review'. Assuming successful or redundant undo.")
                     # Allow success response here
                else:
                     log.error(f"Undo failed unexpectedly for clip {clip_id}. Rows affected: 0. Final check state: {clip_check}")
                     raise HTTPException(status_code=500, detail="Failed to undo action for an unknown reason.")


        log.info(f"API: Successfully processed UNDO for clip(s) {ids_to_update}. Cleared pending actions.")
        return {"status": "success", "undone_clip_ids": list(set(ids_to_update)), "message": "Pending action cancelled."}

    except HTTPException:
        raise # Re-raise HTTP exceptions directly
    except (PostgresError, asyncpg.PostgresError) as db_err:
         log.error(f"Database error during undo for clip {clip_id}: {db_err}", exc_info=True)
         raise HTTPException(status_code=500, detail="Internal server error during database operation.")
    except Exception as e:
        log.error(f"Unexpected error processing undo for clip {clip_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error processing undo request.")
    # Advisory locks released automatically on transaction end
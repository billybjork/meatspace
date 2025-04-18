import os
import random
import ast
import json # <-- ADD THIS IMPORT
from typing import AsyncGenerator, Optional, List # <-- Import List if needed for type hints
from fastapi import FastAPI, Request, HTTPException, Query, Depends, APIRouter, Form, Path, Body # <-- ADD Path, Body
from fastapi.responses import RedirectResponse, HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from dotenv import load_dotenv
import asyncpg
import traceback
import logging as log
from asyncpg.exceptions import UndefinedTableError, UniqueViolationError, DataError, UndefinedFunctionError # Import specific exceptions

logger = log.getLogger("fastapi")
if not logger.hasHandlers():
    handler = log.StreamHandler()
    formatter = log.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
logger.setLevel(log.INFO)

load_dotenv()

# --- Configuration ---
DATABASE_URL = os.getenv("DATABASE_URL")
DEFAULT_MODEL_NAME = os.getenv("DEFAULT_MODEL_NAME", "openai/clip-vit-base-patch32")
DEFAULT_GENERATION_STRATEGY = os.getenv("DEFAULT_GENERATION_STRATEGY", "keyframe_midpoint")
NUM_RESULTS = int(os.getenv("NUM_RESULTS", 10))
CLOUDFRONT_DOMAIN = os.getenv("CLOUDFRONT_DOMAIN")

# --- Input Validation ---
if not DATABASE_URL:
    raise ValueError("DATABASE_URL not found in environment variables.")
if not CLOUDFRONT_DOMAIN:
    raise ValueError("CLOUDFRONT_DOMAIN not found in environment variables.")

logger.info("--- app.py Configuration ---")
logger.info(f"Using Database URL (partially hidden): {DATABASE_URL[:15]}...")
logger.info(f"Using CloudFront Domain: {CLOUDFRONT_DOMAIN}")
logger.info(f"Default Model: {DEFAULT_MODEL_NAME}")
logger.info(f"Default Strategy: {DEFAULT_GENERATION_STRATEGY}")
logger.info(f"Number of Results: {NUM_RESULTS}")
logger.info("-----------------------------")

# --- Database Connection Pool (Global) ---
# Store the pool in app.state for dependency injection
async def create_db_pool():
    try:
        pool = await asyncpg.create_pool(DATABASE_URL)
        logger.info("Successfully created database connection pool.")
        return pool
    except Exception as e:
        logger.error(f"FATAL ERROR: Failed to create database connection pool: {e}", exc_info=True)
        # Exit or raise critical error if DB pool fails? Depends on desired behavior.
        raise RuntimeError("Could not connect to database") from e

# --- Database Connection Dependency ---
async def get_db_connection(request: Request) -> AsyncGenerator[asyncpg.Connection, None]:
    """FastAPI dependency to get a connection from the pool stored in app.state."""
    pool = getattr(request.app.state, 'db_pool', None)
    if pool is None:
         logger.error("Database connection pool 'db_pool' not found in app.state.")
         raise HTTPException(status_code=503, detail="Database connection pool is not available.")
    connection = None
    try:
        connection = await pool.acquire()
        yield connection
    except Exception as e:
        logger.error(f"ERROR during database dependency usage or request handling: {e}", exc_info=True)
        # Let FastAPI handle the exception for internal server error
        raise HTTPException(status_code=500, detail="Internal server error during database operation.")
    finally:
        if connection:
            await pool.release(connection)

# --- FastAPI Application Instance ---
app = FastAPI(title="Meatspace")

# --- Startup and Shutdown Events ---
@app.on_event("startup")
async def startup_event():
    """Initialize database pool on startup."""
    app.state.db_pool = await create_db_pool()

@app.on_event("shutdown")
async def shutdown_event():
    """Close database pool on shutdown."""
    pool = getattr(app.state, 'db_pool', None)
    if pool:
        logger.info("Closing database connection pool.")
        await pool.close()

# --- Static Files and Templates ---
script_dir = os.path.dirname(os.path.abspath(__file__))
static_dir = os.path.join(script_dir, "static")
templates_dir = os.path.join(script_dir, "templates")

if not os.path.isdir(static_dir):
     logger.warning(f"Static directory not found at: {static_dir}")
if not os.path.isdir(templates_dir):
     logger.warning(f"Templates directory not found at: {templates_dir}")

app.mount("/static", StaticFiles(directory=static_dir), name="static")
templates = Jinja2Templates(directory=templates_dir)

# --- API Router ---
router = APIRouter()

# --- Helper Functions ---
def format_clip_data(record, request: Request):
    """Formats a database record into a dictionary for the template, using CloudFront URLs."""
    if not record:
        return None
    title = record.get('clip_identifier', 'Unknown Clip').replace("_", " ").replace("-", " ").title()
    keyframe_s3_key = record.get('keyframe_filepath')
    clip_s3_key = record.get('clip_filepath')

    keyframe_url = f"https://{CLOUDFRONT_DOMAIN.strip('/')}/{keyframe_s3_key.lstrip('/')}" if CLOUDFRONT_DOMAIN and keyframe_s3_key else None
    video_url = f"https://{CLOUDFRONT_DOMAIN.strip('/')}/{clip_s3_key.lstrip('/')}" if CLOUDFRONT_DOMAIN and clip_s3_key else None

    if keyframe_s3_key and not keyframe_url:
         logger.warning(f"Missing CloudFront domain? Could not construct keyframe_url for clip: {record.get('clip_identifier')}")
    if clip_s3_key and not video_url:
         logger.warning(f"Missing CloudFront domain? Could not construct video_url for clip: {record.get('clip_identifier')}")

    return {
        "clip_id": record.get('clip_identifier'), # String identifier
        "title": title,
        "keyframe_url": keyframe_url,
        "video_url": video_url,
        "clip_db_id": record.get('id') # Include DB ID
    }

async def fetch_available_embedding_options(conn: asyncpg.Connection):
    """Fetches distinct model_name/generation_strategy pairs from embeddings."""
    options = []
    try:
        records = await conn.fetch("""
            SELECT DISTINCT model_name, generation_strategy
            FROM embeddings
            WHERE model_name IS NOT NULL AND generation_strategy IS NOT NULL
            ORDER BY model_name, generation_strategy;
        """)
        options = [
            {"model_name": record["model_name"], "strategy": record["generation_strategy"]}
            for record in records
        ]
        if not options:
             logger.warning("No distinct embedding options found in the database.")
    except UndefinedTableError:
         logger.error("ERROR: 'embeddings' table not found. Cannot fetch embedding options.")
    except KeyError as e:
         logger.error(f"ERROR: Missing expected column in embeddings table: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"Error fetching available embedding options: {e}", exc_info=True)
    return options

# --- Request Body Models ---
class ClipActionPayload(BaseModel):
    action: str # 'approve', 'skip', 'archive', 'merge_next', 'retry_splice'

# --- ADD THIS NEW MODEL ---
class SplitActionPayload(BaseModel):
    split_time_seconds: float = Field(..., gt=0, description="Relative time in seconds within the clip to perform the split.")
# -------------------------

# --- API Routes (defined on the router) ---

@router.get("/", response_class=HTMLResponse, name="index")
async def index(request: Request, conn: asyncpg.Connection = Depends(get_db_connection)):
    """Redirects to a random clip's query page or shows index if no clips."""
    available_options = []
    template_context = {
        "request": request, "query": None, "results": [], "error": None,
        "available_options": [], "model_name": DEFAULT_MODEL_NAME,
        "strategy": DEFAULT_GENERATION_STRATEGY
    }
    try:
        available_options = await fetch_available_embedding_options(conn)
        template_context["available_options"] = available_options

        random_clip_record = await conn.fetchrow(
            """
            SELECT c.clip_identifier
            FROM clips c
            JOIN embeddings e ON c.id = e.clip_id
            WHERE c.ingest_state = 'embedded'
              AND e.model_name = $1
              AND e.generation_strategy = $2
            ORDER BY RANDOM()
            LIMIT 1
            """,
             DEFAULT_MODEL_NAME, DEFAULT_GENERATION_STRATEGY
        )

        if random_clip_record and random_clip_record['clip_identifier']:
            random_clip_id = random_clip_record['clip_identifier']
            default_exists = any(
                opt['model_name'] == DEFAULT_MODEL_NAME and opt['strategy'] == DEFAULT_GENERATION_STRATEGY
                for opt in available_options
            )
            final_model = DEFAULT_MODEL_NAME
            final_strategy = DEFAULT_GENERATION_STRATEGY
            if not default_exists and available_options:
                logger.warning(f"Default model/strategy not found. Using first available.")
                final_model = available_options[0]['model_name']
                final_strategy = available_options[0]['strategy']
            elif not available_options:
                 logger.warning("No embedding options available. Redirecting with defaults.")

            redirect_url = request.url_for('query_clip', clip_id=random_clip_id).include_query_params(
                 model=final_model, strategy=final_strategy
            )
            logger.info(f"Redirecting to random embedded clip: {random_clip_id} with model={final_model}, strategy={final_strategy}")
            return RedirectResponse(url=redirect_url, status_code=303)
        else:
            logger.info(f"No embedded clips found for default model/strategy. Serving index page.")
            template_context["error"] = "No embedded clips found for default criteria. Check ingest status or select different options."
            return templates.TemplateResponse("index.html", template_context)

    except UndefinedTableError as e:
         logger.error(f"DB table error (e.g., '{e}'): {e}. Check schema.", exc_info=True)
         template_context["error"] = f"Database table error: {e}. Please check setup."
         try:
             if 'clips' in str(e).lower():
                 template_context["available_options"] = await fetch_available_embedding_options(conn)
         except Exception: pass
         return templates.TemplateResponse("index.html", template_context)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in index route: {e}", exc_info=True)
        template_context["error"] = "An unexpected error occurred loading the page."
        return templates.TemplateResponse("index.html", template_context)

@router.get("/query/{clip_id}", response_class=HTMLResponse, name="query_clip")
async def query_clip(
    clip_id: str,
    request: Request,
    conn: asyncpg.Connection = Depends(get_db_connection),
    model_name: str = Query(DEFAULT_MODEL_NAME, alias="model"),
    strategy: str = Query(DEFAULT_GENERATION_STRATEGY, alias="strategy")
):
    """Displays query clip and finds similar clips based on selected embeddings."""
    logger.info(f"Querying for clip_id='{clip_id}', model='{model_name}', strategy='{strategy}'")
    template_context = {
        "request": request, "query": None, "results": [], "error": None,
        "available_options": [], "model_name": model_name, "strategy": strategy
    }

    try:
        template_context["available_options"] = await fetch_available_embedding_options(conn)

        query_clip_record = await conn.fetchrow(
            """
            SELECT
                c.id, c.clip_identifier, c.clip_filepath, c.keyframe_filepath,
                e.embedding
            FROM clips c
            LEFT JOIN embeddings e ON c.id = e.clip_id AND e.model_name = $2 AND e.generation_strategy = $3
            WHERE c.clip_identifier = $1;
            """,
            clip_id, model_name, strategy
        )

        if not query_clip_record:
            logger.warning(f"Clip identifier '{clip_id}' not found.")
            template_context["error"] = f"Requested clip '{clip_id}' not found."
            return templates.TemplateResponse("index.html", template_context, status_code=404)

        query_clip_db_id = query_clip_record['id']
        query_info = format_clip_data(query_clip_record, request)
        template_context["query"] = query_info

        if not query_info or not query_info.get("video_url"):
             logger.warning(f"Missing S3 key or URL failed for query clip {clip_id}.")

        query_embedding_data = query_clip_record['embedding']

        if not query_embedding_data:
             logger.warning(f"Embedding not found for query clip {clip_id} (DB ID: {query_clip_db_id}) model='{model_name}', strategy='{strategy}'.")
             template_context["error"] = f"Embedding not found for clip '{clip_id}' (Model: {model_name}, Strategy: {strategy}). Cannot find similar clips."
             return templates.TemplateResponse("index.html", template_context)

        query_embedding_vector = None
        try:
            # Adjusted embedding parsing
            if isinstance(query_embedding_data, (list, tuple)):
                 query_embedding_vector = list(query_embedding_data)
            elif isinstance(query_embedding_data, str) and query_embedding_data.startswith('[') and query_embedding_data.endswith(']'):
                 query_embedding_vector = ast.literal_eval(query_embedding_data) # Safely evaluate string list
            elif query_embedding_data and not isinstance(query_embedding_data, (str, list, tuple)):
                 # If asyncpg is configured correctly, it might return a specialized type
                 # Attempt conversion if possible, log warning otherwise
                 try:
                     query_embedding_vector = list(query_embedding_data)
                     logger.debug(f"Converted embedding data type {type(query_embedding_data)} to list.")
                 except TypeError:
                      raise TypeError(f"Unexpected embedding data type: {type(query_embedding_data)}. Cannot convert to list.")
            else:
                 raise ValueError(f"Invalid or empty embedding data: {query_embedding_data}")

            if not isinstance(query_embedding_vector, list):
                 raise ValueError("Processed embedding is not a list.")

            query_embedding_string = '[' + ','.join(map(str, query_embedding_vector)) + ']'

        except (ValueError, SyntaxError, TypeError) as parse_error:
            logger.error(f"ERROR parsing/handling embedding for clip {clip_id} (DB ID: {query_clip_db_id}): {parse_error}", exc_info=True)
            logger.error(f"Received type: {type(query_embedding_data)}, value (partial): {str(query_embedding_data)[:100]}...")
            template_context["error"] = f"Failed to process embedding data for query clip '{clip_id}'. Data format issue."
            return templates.TemplateResponse("index.html", template_context)

        similarity_query = """
            SELECT
                c.id, c.clip_identifier, c.clip_filepath, c.keyframe_filepath,
                1 - (e.embedding <=> $1::vector) AS similarity_score
            FROM embeddings e
            JOIN clips c ON e.clip_id = c.id
            WHERE e.model_name = $2
              AND e.generation_strategy = $3
              AND c.id != $4
              AND c.ingest_state = 'embedded'
            ORDER BY e.embedding <=> $1::vector ASC
            LIMIT $5;
        """
        similar_records = await conn.fetch(
            similarity_query, query_embedding_string, model_name, strategy, query_clip_db_id, NUM_RESULTS
        )

        results = []
        for record in similar_records:
            formatted = format_clip_data(record, request)
            if formatted:
                score = record.get('similarity_score')
                formatted["score"] = float(score) if score is not None else 0.0
                results.append(formatted)
        template_context["results"] = results

        if not results and not template_context["error"]:
            logger.info(f"No similar clips found for '{clip_id}' using {model_name}/{strategy}.")

    except UndefinedFunctionError as e:
         logger.error(f"DB ERROR: pgvector function error. Extension installed? Error: {e}", exc_info=True)
         template_context["error"] = "Database error: Vector operations not available."
    except DataError as e:
         logger.error(f"DB ERROR: Data error during query for clip {clip_id}: {e}", exc_info=True)
         template_context["error"] = f"Database data error: {e}."
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error during query for clip {clip_id}: {e}", exc_info=True)
        template_context["error"] = f"An unexpected error occurred: {e}"

    return templates.TemplateResponse("index.html", template_context)

@router.get("/review", response_class=HTMLResponse, name="review_clips")
async def review_clips_ui(
    request: Request,
    conn: asyncpg.Connection = Depends(get_db_connection),
    limit: int = Query(10, ge=1, le=50)
):
    """Serves the HTML page for reviewing clips."""
    clips_to_review = []
    error_message = None
    try:
        records = await conn.fetch(
            """
            WITH OrderedClips AS (
                SELECT
                    c.id, c.clip_identifier, c.clip_filepath, c.keyframe_filepath,
                    c.start_time_seconds, c.end_time_seconds, c.source_video_id,
                    c.ingest_state, sv.title as source_title,
                    LEAD(c.id) OVER w as next_clip_id,
                    LEAD(c.ingest_state) OVER w as next_clip_state
                FROM clips c
                JOIN source_videos sv ON c.source_video_id = sv.id
                WINDOW w AS (PARTITION BY c.source_video_id ORDER BY c.start_frame)
            )
            SELECT * FROM OrderedClips
            WHERE ingest_state = 'pending_review' OR ingest_state = 'review_skipped'
            ORDER BY source_video_id, start_time_seconds
            LIMIT $1;
            """, limit
        )

        valid_merge_target_states = {'pending_review', 'review_skipped'}
        for record in records:
            formatted = format_clip_data(record, request)
            if formatted:
                formatted.update({
                    "source_video_id": record['source_video_id'],
                    "source_title": record['source_title'],
                    "start_time": record['start_time_seconds'],
                    "end_time": record['end_time_seconds'],
                    "can_merge_next": record['next_clip_id'] is not None and record['next_clip_state'] in valid_merge_target_states,
                    "clip_db_id": record['id'] # Ensure DB ID is present for API calls
                })
                clips_to_review.append(formatted)

    except UndefinedTableError as e:
         logger.error(f"DB table error fetching review clips: {e}", exc_info=True)
         error_message = f"Database table error: {e}. Check setup."
    except Exception as e:
        logger.error(f"Error fetching clips for review: {e}", exc_info=True)
        error_message = "Failed to load clips for review."

    return templates.TemplateResponse("review.html", {
        "request": request, "clips": clips_to_review, "error": error_message,
        "CLOUDFRONT_DOMAIN": CLOUDFRONT_DOMAIN
    })

# --- API Endpoint for Clip Actions (Approve, Skip, Archive, Merge, Resplice) ---
@router.post("/api/clips/{clip_db_id}/action", name="clip_action", status_code=200)
async def handle_clip_action(
    request: Request,  # move non-default first
    clip_db_id: int = Path(..., description="Database ID of the clip", gt=0),
    payload: ClipActionPayload = Body(...),
    conn: asyncpg.Connection = Depends(get_db_connection)
):
    """Handles user actions: approve, skip, archive, merge_next, retry_splice."""
    logger.info(f"Received action '{payload.action}' for clip_db_id: {clip_db_id}")

    action = payload.action
    target_state = None
    set_clauses = ["updated_at = NOW()", "last_error = NULL", "processing_metadata = NULL"]
    params = []

    if action == "approve": target_state = "review_approved"; set_clauses.append("reviewed_at = NOW()")
    elif action == "skip": target_state = "review_skipped"
    elif action == "archive": target_state = "archived"; set_clauses.append("reviewed_at = NOW()")
    elif action == "merge_next": target_state = "pending_merge_with_next"
    elif action == "retry_splice": target_state = "pending_resplice"
    else: raise HTTPException(status_code=400, detail=f"Invalid action: {action}")

    set_clauses.insert(0, f"ingest_state = $1") # Parameter $1 will be target_state
    params.append(target_state)

    allowed_source_states = ['pending_review', 'review_skipped', 'merge_failed', 'split_failed', 'resplice_failed']

    try:
        async with conn.transaction():
            await conn.execute("SELECT pg_advisory_xact_lock(2, $1)", clip_db_id) # Lock using DB ID
            current_state = await conn.fetchval("SELECT ingest_state FROM clips WHERE id = $1", clip_db_id)
            if not current_state: raise HTTPException(status_code=404, detail="Clip not found.")
            if current_state not in allowed_source_states:
                 logger.warning(f"Action '{action}' on clip {clip_db_id} with unexpected state '{current_state}'.")
                 raise HTTPException(status_code=409, detail=f"Clip not in state for review action (state: {current_state}). Refresh.")

            set_clause_sql = ', '.join(set_clauses)
            update_query = f"""
                UPDATE clips SET {set_clause_sql}
                WHERE id = $2 AND ingest_state = ANY($3)
                RETURNING id;
            """
            # Params: [target_state, clip_db_id, allowed_source_states]
            final_params = params + [clip_db_id, allowed_source_states]

            logger.debug(f"Executing Action Query: {update_query} with params: {final_params}")
            updated_id = await conn.fetchval(update_query, *final_params)

            if updated_id is None:
                current_state_after = await conn.fetchval("SELECT ingest_state FROM clips WHERE id = $1", clip_db_id)
                logger.warning(f"Failed to update clip {clip_db_id}. State might have changed. Current state: {current_state_after}")
                raise HTTPException(status_code=409, detail="Clip state changed, action failed. Refresh.")

        logger.info(f"Successfully updated clip {clip_db_id} to state '{target_state}'")
        return {"status": "success", "clip_id": clip_db_id, "new_state": target_state}

    except UniqueViolationError as e:
         logger.error(f"DB error during clip action: {e}", exc_info=True)
         raise HTTPException(status_code=409, detail=f"DB constraint violated: {e}")
    except HTTPException: raise
    except Exception as e:
        logger.error(f"Error processing action for clip {clip_db_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error processing action.")

# --- ADD THIS NEW ENDPOINT for SPLIT ---
@router.post("/api/clips/{clip_db_id}/split", name="queue_clip_split", status_code=200)
async def queue_clip_split(
    request: Request, # Keep request if needed for context, otherwise optional
    clip_db_id: int = Path(..., description="Database ID of the clip to split", gt=0),
    payload: SplitActionPayload = Body(...),
    conn: asyncpg.Connection = Depends(get_db_connection)
):
    """
    Marks a clip for splitting at the specified relative timecode.
    Updates state to 'pending_split' and stores split time in metadata.
    """
    logger.info(f"Received split request for clip_db_id: {clip_db_id} at time: {payload.split_time_seconds}s")

    requested_split_time = payload.split_time_seconds
    new_state = "pending_split"
    # Minimum time from start/end to allow a split (adjust as needed)
    min_margin = 0.5

    # States from which splitting is allowed
    allowed_source_states = ['pending_review', 'review_skipped']

    try:
        async with conn.transaction():
            # Lock the clip row using its database ID
            await conn.execute("SELECT pg_advisory_xact_lock(2, $1)", clip_db_id)
            logger.debug(f"Acquired lock for clip {clip_db_id} for split action.")

            # Fetch necessary details
            clip_record = await conn.fetchrow(
                """
                SELECT start_time_seconds, end_time_seconds, ingest_state, processing_metadata
                FROM clips WHERE id = $1;
                """, clip_db_id
            )

            if not clip_record:
                raise HTTPException(status_code=404, detail="Clip not found.")

            current_state = clip_record['ingest_state']
            if current_state not in allowed_source_states:
                 logger.warning(f"Split requested on clip {clip_db_id} with unexpected state '{current_state}'.")
                 raise HTTPException(status_code=409, detail=f"Clip cannot be split from state: {current_state}. Please refresh.")

            # Validate split time against clip duration
            start_time = clip_record['start_time_seconds']
            end_time = clip_record['end_time_seconds']
            if start_time is None or end_time is None:
                raise HTTPException(status_code=400, detail="Clip is missing start or end time, cannot calculate duration for split.")

            clip_duration = end_time - start_time
            if clip_duration <= 0:
                 raise HTTPException(status_code=400, detail="Clip has non-positive duration, cannot split.")

            # Check requested time validity (relative to clip start)
            if requested_split_time <= min_margin:
                 raise HTTPException(status_code=422, detail=f"Split time ({requested_split_time:.2f}s) must be at least {min_margin}s after the clip start.")
            if requested_split_time >= (clip_duration - min_margin):
                 raise HTTPException(status_code=422, detail=f"Split time ({requested_split_time:.2f}s) must be at least {min_margin}s before the clip end ({clip_duration:.2f}s).")

            # Prepare metadata update
            metadata = clip_record['processing_metadata'] or {}
            metadata['split_request_at'] = requested_split_time
            metadata['previous_state_before_split'] = current_state # Store previous state

            # Update the clip
            updated_id = await conn.fetchval(
                """
                UPDATE clips
                SET ingest_state = $1,
                    processing_metadata = $2::jsonb,
                    last_error = NULL, -- Clear previous errors
                    updated_at = NOW()
                WHERE id = $3 AND ingest_state = ANY($4) -- Ensure state hasn't changed
                RETURNING id;
                """,
                new_state, json.dumps(metadata), clip_db_id, allowed_source_states
            )

            if updated_id is None:
                # State changed between check and update
                current_state_after = await conn.fetchval("SELECT ingest_state FROM clips WHERE id = $1", clip_db_id)
                logger.warning(f"Split queue failed for clip {clip_db_id}. State changed. Current state: {current_state_after}")
                raise HTTPException(status_code=409, detail="Clip state changed, split could not be queued. Please refresh.")

        logger.info(f"Clip {clip_db_id} state updated to '{new_state}' for splitting at relative time {requested_split_time:.2f}s.")
        return {"status": "success", "clip_id": clip_db_id, "new_state": new_state, "message": "Clip queued for splitting."}

    except HTTPException:
        raise # Re-raise HTTP exceptions directly
    except UniqueViolationError as e: # Catch specific DB errors if needed
         logger.error(f"Database constraint error queueing split for clip {clip_db_id}: {e}", exc_info=True)
         raise HTTPException(status_code=409, detail=f"Database constraint violated: {e}")
    except Exception as e:
        logger.error(f"Error queueing clip {clip_db_id} for split: {e}", exc_info=True)
        # Don't expose raw error details in production
        raise HTTPException(status_code=500, detail="Internal server error processing split request.")
# ----------------------------------

# --- API Endpoint for Undo ---
@router.post("/api/clips/{clip_db_id}/undo", name="undo_clip_action", status_code=200)
async def undo_clip_action(
    request: Request,
    clip_db_id: int = Path(..., gt=0),
    conn: asyncpg.Connection = Depends(get_db_connection)
):
    """Attempts to revert a clip's state back to 'pending_review'."""
    logger.info(f"Received UNDO request for clip_db_id: {clip_db_id}")

    target_state = "pending_review"
    # Add 'pending_split' and 'split_failed' to states that can be undone
    allowed_undo_states = [
        'review_approved', 'review_skipped', 'archived',
        'pending_merge_with_next', 'pending_resplice', 'pending_split', # Added pending_split
        'merge_failed', 'resplice_failed', 'split_failed' # Added split_failed
    ]

    try:
        async with conn.transaction():
            await conn.execute("SELECT pg_advisory_xact_lock(2, $1)", clip_db_id)
            current_state_record = await conn.fetchrow("SELECT ingest_state FROM clips WHERE id = $1", clip_db_id)
            if not current_state_record: raise HTTPException(status_code=404, detail="Clip not found.")

            current_state = current_state_record['ingest_state']
            if current_state not in allowed_undo_states:
                 raise HTTPException(status_code=409, detail=f"Cannot undo from current state: {current_state}")

            updated_id = await conn.fetchval(
                """
                UPDATE clips
                SET ingest_state = $1, updated_at = NOW(), reviewed_at = NULL,
                    processing_metadata = NULL, -- Clear split/merge/resplice info
                    last_error = 'Reverted by user action'
                WHERE id = $2 AND ingest_state = $3
                RETURNING id;
                """, target_state, clip_db_id, current_state
            )

            if updated_id is None:
                current_state_after = await conn.fetchval("SELECT ingest_state FROM clips WHERE id = $1", clip_db_id)
                logger.warning(f"Undo failed for clip {clip_db_id}. State changed. Current: {current_state_after}")
                raise HTTPException(status_code=409, detail="Clip state changed, undo failed. Refresh.")

        logger.info(f"Successfully reverted clip {clip_db_id} to state '{target_state}'")
        return {"status": "success", "clip_id": clip_db_id, "new_state": target_state}

    except HTTPException: raise
    except Exception as e:
        logger.error(f"Error processing undo for clip {clip_db_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error processing undo.")


# --- Include the router in the main app ---
app.include_router(router)

# --- Optional: Add a root path for basic health check or info ---
@app.get("/", include_in_schema=False)
async def root_redirect():
    # Redirect to the main query UI (which handles random clip redirection)
    # Assuming the router is mounted at root, otherwise adjust url_for path
    return RedirectResponse(url="/", status_code=303) # Use the named route 'index' from the router

if __name__ == "__main__":
    import uvicorn
    # Basic local run configuration
    uvicorn.run(app, host="0.0.0.0", port=8000)
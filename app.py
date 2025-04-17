import os
import random
import ast
import json # Added for processing_metadata
from typing import AsyncGenerator, Optional # Added Optional
from fastapi import FastAPI, Request, HTTPException, Query, Depends, APIRouter, Form # Added Form
from fastapi.responses import RedirectResponse, HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field # Added Pydantic for request bodies
from dotenv import load_dotenv
import asyncpg
import traceback
import logging as log

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

# --- Database Connection Dependency ---
async def get_db_connection(request: Request) -> AsyncGenerator[asyncpg.Connection, None]: # Correct type hint
    """FastAPI dependency to get a connection from the pool stored in app.state."""
    pool = getattr(request.app.state, 'db_pool', None)
    if pool is None:
         logger.error("Database connection pool 'db_pool' not found in app.state.")
         # Use 503 Service Unavailable for errors related to backend services
         raise HTTPException(status_code=503, detail="Database connection pool is not available.")
    connection = None # Initialize connection variable
    try:
        # Acquire connection separately to ensure it's released even if yield fails
        connection = await pool.acquire()
        yield connection
    except Exception as e: # Catch potential errors during yield/request handling
        logger.error(f"ERROR during database dependency usage or request handling: {e}", exc_info=True)
        # Re-raise or handle appropriately. Depending on error, might not proceed.
        # Let FastAPI handle the exception, or raise a specific HTTP error
        raise HTTPException(status_code=500, detail="Internal server error during database operation.")
    finally:
        if connection:
            # logger.debug("Debug: Releasing DB connection back to pool.") # Optional debug log
            await pool.release(connection)

# --- FastAPI Application Instance ---
# Assuming lifespan management is in main.py
app = FastAPI(title="Meatspace")

# --- Static Files and Templates ---
script_dir = os.path.dirname(os.path.abspath(__file__)) # Use abspath for reliability
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
def format_clip_data(record, request: Request): # Request param likely removable if base_url not needed
    """Formats a database record into a dictionary for the template, using CloudFront URLs."""
    if not record:
        return None
    # Use clip_identifier for display title if needed, or add a title column later
    title = record.get('clip_identifier', 'Unknown Clip').replace("_", " ").replace("-", " ").title()

    # Construct CloudFront URLs ---
    # ASSUMPTION: DB stores the S3 object key (e.g., 'keyframes/clip_abc.jpg')
    keyframe_s3_key = record.get('keyframe_filepath')
    clip_s3_key = record.get('clip_filepath')

    # Ensure no leading slash on the key if CloudFront domain already has one (unlikely)
    # and handle potential None values for keys or domain
    keyframe_url = f"https://{CLOUDFRONT_DOMAIN.strip('/')}/{keyframe_s3_key.lstrip('/')}" if CLOUDFRONT_DOMAIN and keyframe_s3_key else None
    video_url = f"https://{CLOUDFRONT_DOMAIN.strip('/')}/{clip_s3_key.lstrip('/')}" if CLOUDFRONT_DOMAIN and clip_s3_key else None

    # Log warning if essential paths are missing or URL couldn't be constructed
    # Only log if the specific key was expected but missing URL elements
    if keyframe_s3_key and not keyframe_url:
         logger.warning(f"Missing CloudFront domain? Could not construct keyframe_url for clip: {record.get('clip_identifier')}")
    if clip_s3_key and not video_url:
         logger.warning(f"Missing CloudFront domain? Could not construct video_url for clip: {record.get('clip_identifier')}")

    return {
        "clip_id": record.get('clip_identifier'), # String identifier
        "title": title,
        "keyframe_url": keyframe_url,
        "video_url": video_url,
        # Score is added dynamically in the embedding search route
        # Add DB ID if available in the record
        "clip_db_id": record.get('id')
    }

# --- Helper to fetch available options ---
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
    except asyncpg.exceptions.UndefinedTableError:
         logger.error("ERROR: 'embeddings' table not found. Cannot fetch embedding options.")
         # Raise a more specific error or return empty list? Let's return empty and handle in routes.
    except KeyError as e:
         logger.error(f"ERROR: Missing expected column in embeddings table: {e}", exc_info=True) # Catch missing keys
    except Exception as e:
        logger.error(f"Error fetching available embedding options: {e}", exc_info=True)
    return options


# --- Request Body Models ---
class ClipActionPayload(BaseModel):
    action: str # 'approve', 'skip', 'archive', 'merge_next', 'split', 'retry_splice' <-- Add retry_splice
    # Remove split_at_seconds as it's no longer needed for review UI actions
    # split_at_seconds: Optional[float] = Field(None, gt=0)

# --- API Routes (defined on the router) ---

@router.get("/", response_class=HTMLResponse, name="index")
async def index(request: Request, conn: asyncpg.Connection = Depends(get_db_connection)):
    """Redirects to a random clip's query page with default model/strategy."""
    available_options = [] # Initialize
    template_context = { # Define base context
        "request": request, "query": None, "results": [], "error": None,
        "available_options": [], "model_name": DEFAULT_MODEL_NAME,
        "strategy": DEFAULT_GENERATION_STRATEGY
    }
    try:
        # Fetch available options first (needed regardless of random clip result)
        available_options = await fetch_available_embedding_options(conn)
        template_context["available_options"] = available_options

        # Fetch a random clip identifier from the DB
        random_clip_record = await conn.fetchrow(
            "SELECT clip_identifier FROM clips ORDER BY RANDOM() LIMIT 1"
        )
        if random_clip_record and random_clip_record['clip_identifier']:
            random_clip_id = random_clip_record['clip_identifier']

            # Check if default model/strategy actually exists in the options
            default_exists = any(
                opt['model_name'] == DEFAULT_MODEL_NAME and opt['strategy'] == DEFAULT_GENERATION_STRATEGY
                for opt in available_options
            )

            # Use first available option if default is invalid or no options exist
            final_model = DEFAULT_MODEL_NAME
            final_strategy = DEFAULT_GENERATION_STRATEGY
            if not default_exists and available_options:
                logger.warning(f"Default model/strategy ({DEFAULT_MODEL_NAME}/{DEFAULT_GENERATION_STRATEGY}) not found in DB options. Using first available.")
                final_model = available_options[0]['model_name']
                final_strategy = available_options[0]['strategy']
            elif not available_options:
                 logger.warning("No embedding options available in DB. Redirecting with defaults, but similarity search may fail.")

            # Include default (or first available) model/strategy in the redirect URL query params
            redirect_url = request.url_for('query_clip', clip_id=random_clip_id).include_query_params(
                 model=final_model,
                 strategy=final_strategy
            )
            logger.info(f"Redirecting to random clip: {random_clip_id} with model={final_model}, strategy={final_strategy}")
            return RedirectResponse(url=redirect_url, status_code=303) # Use 303 See Other for GET redirect
        else:
            # Handle case where there are no clips in the DB yet
            logger.info("No clips found in database.")
            template_context["error"] = "No clips found in the database."
            return templates.TemplateResponse("index.html", template_context)

    except asyncpg.exceptions.UndefinedTableError as e:
         logger.error(f"ERROR accessing DB tables (e.g., '{e}'): {e}. Check schema.", exc_info=True)
         template_context["error"] = f"Database table error: {e}. Please check application setup."
         # Try to fetch options even if clips table missing
         try:
             if 'clips' in str(e).lower(): # Only if error is not about embeddings table
                 available_options = await fetch_available_embedding_options(conn)
                 template_context["available_options"] = available_options
         except Exception: pass # Ignore error during error handling
         return templates.TemplateResponse("index.html", template_context)
    except HTTPException:
        raise # Re-raise HTTP exceptions from dependencies (like DB pool)
    except Exception as e:
        logger.error(f"Error in index route: {e}", exc_info=True)
        template_context["error"] = "An unexpected error occurred while loading the page."
        # Avoid DB calls in general exception handler if initial connection might have failed
        return templates.TemplateResponse("index.html", template_context)


@router.get("/query/{clip_id}", response_class=HTMLResponse, name="query_clip")
async def query_clip(
    clip_id: str,
    request: Request,
    conn: asyncpg.Connection = Depends(get_db_connection),
    # Add query parameters for model and strategy selection
    model_name: str = Query(DEFAULT_MODEL_NAME, alias="model"),
    strategy: str = Query(DEFAULT_GENERATION_STRATEGY, alias="strategy")
):
    """Displays a query clip and finds similar clips based on selected embeddings."""
    logger.info(f"Querying for clip_id='{clip_id}', model='{model_name}', strategy='{strategy}'")

    # Define template context structure early
    template_context = {
        "request": request,
        "query": None,
        "results": [],
        "error": None,
        "available_options": [],
        "model_name": model_name, # Reflect selected model/strategy
        "strategy": strategy
    }

    try:
        # --- FETCH AVAILABLE OPTIONS FIRST ---
        available_options = await fetch_available_embedding_options(conn)
        template_context["available_options"] = available_options
        # --- END FETCH OPTIONS ---

        # 1. Fetch Query Clip Info (including its embedding and S3 keys)
        # Use the DB ID for joins, but filter by the string identifier
        query_clip_record = await conn.fetchrow(
            """
            SELECT
                c.id, c.clip_identifier, c.clip_filepath, c.keyframe_filepath,
                e.embedding
            FROM clips c
            LEFT JOIN embeddings e ON c.id = e.clip_id
                                  AND e.model_name = $2 -- Use selected model
                                  AND e.generation_strategy = $3 -- Use selected strategy
            WHERE c.clip_identifier = $1;
            """,
            clip_id, model_name, strategy
        )

        if not query_clip_record:
            logger.warning(f"Clip identifier '{clip_id}' not found in database.")
            template_context["error"] = f"Requested clip '{clip_id}' not found in the database."
            # Attempt redirect to random clip (optional, could just show error)
            # Consider removing the redirect logic here to simplify - just show the error.
            return templates.TemplateResponse("index.html", template_context, status_code=404)

        # Get the database ID for potential future use (though embedding search uses the vector)
        query_clip_db_id = query_clip_record['id']

        # Format query clip data (gets CloudFront URLs)
        query_info = format_clip_data(query_clip_record, request)
        template_context["query"] = query_info

        if not query_info or not query_info.get("video_url"): # Check only essential video url
             logger.warning(f"Missing S3 key data or URL generation failed for query clip {clip_id}.")
             # Proceed, but template should handle missing URLs gracefully

        # Check for embedding needed for similarity search
        query_embedding_data = query_clip_record['embedding']

        if not query_embedding_data:
             logger.warning(f"Embedding not found for query clip {clip_id} (DB ID: {query_clip_db_id}) with model='{model_name}', strategy='{strategy}'. Cannot perform similarity search.")
             template_context["error"] = f"Embedding not found for clip '{clip_id}' (Model: {model_name}, Strategy: {strategy}). Cannot find similar clips. Try selecting a different model/strategy or generate the missing embedding."
             # Render template showing the query clip but no results/error message
             return templates.TemplateResponse("index.html", template_context)

        # --- Parse Embedding and Perform Similarity Search ---
        query_embedding_vector = None
        try:
            # Handle vector data type (assuming asyncpg returns list/bytes or string)
             # Prefer direct handling if asyncpg/pgvector setup returns list/numpy array
            if isinstance(query_embedding_data, (list, tuple)): # Ideal case from asyncpg type codec
                 query_embedding_vector = list(query_embedding_data)
            elif isinstance(query_embedding_data, str): # Fallback: string representation '[1,2,3]'
                 query_embedding_vector = ast.literal_eval(query_embedding_data)
            elif isinstance(query_embedding_data, bytes): # Handle BYTEA if used
                 # This requires knowing the exact format (e.g., numpy.tobytes)
                 # Example placeholder if using numpy:
                 # import numpy as np
                 # query_embedding_vector = np.frombuffer(query_embedding_data, dtype=np.float32).tolist()
                 raise NotImplementedError("BYTEA embedding parsing not implemented yet.")
            else:
                 raise TypeError(f"Unexpected embedding data type: {type(query_embedding_data)}")

            if not isinstance(query_embedding_vector, list):
                 raise ValueError("Parsed/obtained embedding is not a list.")

            # Convert the vector list to the string format pgvector expects ('[1,2,3]') for the query parameter
            query_embedding_string = '[' + ','.join(map(str, query_embedding_vector)) + ']'

        except (ValueError, SyntaxError, TypeError, NotImplementedError) as parse_error:
            logger.error(f"ERROR parsing/handling embedding data for clip {clip_id} (DB ID: {query_clip_db_id}): {parse_error}", exc_info=True)
            logger.error(f"Received embedding data type: {type(query_embedding_data)}, value (partial): {str(query_embedding_data)[:100]}...")
            template_context["error"] = f"Failed to process embedding data from database for query clip '{clip_id}'. Data might be corrupt or in unexpected format."
            return templates.TemplateResponse("index.html", template_context)

        # --- Similarity Query ---
        # Fetches S3 keys for results
        similarity_query = """
            SELECT
                c.id, c.clip_identifier, -- Include DB ID
                c.clip_filepath,      -- S3 Key for video
                c.keyframe_filepath,  -- S3 Key for keyframe
                1 - (e.embedding <=> $1::vector) AS similarity_score -- Cosine Similarity
            FROM embeddings e
            JOIN clips c ON e.clip_id = c.id
            WHERE e.model_name = $2             -- Use selected model
              AND e.generation_strategy = $3    -- Use selected strategy
              AND c.id != $4                    -- Exclude query clip itself BY DB ID
            ORDER BY
                e.embedding <=> $1::vector ASC  -- Order by distance (ascending)
            LIMIT $5;
        """

        similar_records = await conn.fetch(
            similarity_query,
            query_embedding_string, # Use string representation for query parameter
            model_name,             # Pass selected model
            strategy,               # Pass selected strategy
            query_clip_db_id,       # Exclude self using DB ID
            NUM_RESULTS             # Limit number of results
        )

        # Format results (gets CloudFront URLs)
        results = []
        for record in similar_records:
            formatted = format_clip_data(record, request)
            if formatted:
                score = record.get('similarity_score')
                formatted["score"] = float(score) if score is not None else 0.0
                results.append(formatted)
        template_context["results"] = results

        if not results and not template_context["error"]:
            msg = f"No similar clips found for '{clip_id}' using Model: {model_name}, Strategy: {strategy}."
            logger.info(msg)
            # Optionally add to context as non-error message: template_context["info_message"] = msg

    except asyncpg.exceptions.UndefinedFunctionError as e:
         logger.error(f"DATABASE ERROR: pgvector function error. Is the pgvector extension installed and enabled? Error: {e}", exc_info=True)
         template_context["error"] = "Database error: Vector operations not available. Ensure pgvector extension is installed and enabled."
    except asyncpg.exceptions.DataError as e:
         # Could be invalid vector format, etc.
         logger.error(f"DATABASE ERROR: Data error during query for clip {clip_id}: {e}", exc_info=True)
         template_context["error"] = f"Database data error: {e}. Check query parameters and vector format."
    except HTTPException:
        raise # Re-raise HTTP exceptions from dependencies
    except Exception as e:
        logger.error(f"Error during query for clip {clip_id}: {e}", exc_info=True)
        template_context["error"] = f"An unexpected error occurred: {e}"

    # --- ALWAYS Pass data to template ---
    return templates.TemplateResponse("index.html", template_context)

# --- Review UI Route ---
@router.get("/review", response_class=HTMLResponse, name="review_clips")
async def review_clips_ui(
    request: Request,
    conn: asyncpg.Connection = Depends(get_db_connection),
    limit: int = Query(10, ge=1, le=50) # Limit how many clips to review at once
):
    """Serves the HTML page for reviewing clips."""
    clips_to_review = []
    error_message = None
    try:
        # Query for clips pending review, including adjacent clip info for merge context
        # Order by source_video_id, then start_frame to get them sequentially
        # Fetch necessary fields for display and formatting
        records = await conn.fetch(
            """
            WITH OrderedClips AS (
                SELECT
                    c.id, c.clip_identifier, c.clip_filepath, c.keyframe_filepath,
                    c.start_time_seconds, c.end_time_seconds, c.source_video_id,
                    c.ingest_state,
                    sv.title as source_title,
                    -- Use window functions to get previous/next clip IDs and states within the same source
                    LAG(c.id) OVER w as prev_clip_id,
                    LEAD(c.id) OVER w as next_clip_id,
                    LAG(c.ingest_state) OVER w as prev_clip_state,
                    LEAD(c.ingest_state) OVER w as next_clip_state
                FROM clips c
                JOIN source_videos sv ON c.source_video_id = sv.id
                WINDOW w AS (PARTITION BY c.source_video_id ORDER BY c.start_frame)
            )
            SELECT *
            FROM OrderedClips
            WHERE ingest_state = 'pending_review' OR ingest_state = 'review_skipped' -- Show skipped ones too
            ORDER BY source_video_id, start_time_seconds -- Ensure consistent ordering
            LIMIT $1;
            """,
            limit
        )

        valid_merge_target_states = {'pending_review', 'review_skipped'}
        for record in records:
            formatted = format_clip_data(record, request) # Use your existing helper
            if formatted:
                formatted.update({
                    "id": record['id'], # Keep original 'id' key if needed, else use clip_db_id
                    "source_video_id": record['source_video_id'],
                    "source_title": record['source_title'],
                    "start_time": record['start_time_seconds'],
                    "end_time": record['end_time_seconds'],
                    # Check if merge is possible (next clip exists and is in a valid state for merging)
                    "can_merge_next": record['next_clip_id'] is not None and record['next_clip_state'] in valid_merge_target_states,
                    "clip_db_id": record['id'] # Ensure DB ID is present
                })
                clips_to_review.append(formatted)

    except asyncpg.exceptions.UndefinedTableError as e:
         logger.error(f"ERROR accessing DB tables (e.g., '{e}') fetching review clips: {e}", exc_info=True)
         error_message = f"Database table error: {e}. Please check application setup."
    except Exception as e:
        logger.error(f"Error fetching clips for review: {e}", exc_info=True)
        error_message = "Failed to load clips for review."

    return templates.TemplateResponse("review.html", {
        "request": request,
        "clips": clips_to_review,
        "error": error_message,
        # Pass CloudFront domain if needed directly by JS, though format_clip_data handles it
        "CLOUDFRONT_DOMAIN": CLOUDFRONT_DOMAIN
    })

# --- API Endpoint for Clip Actions ---
@router.post("/api/clips/{clip_db_id}/action", name="clip_action", status_code=200)
async def handle_clip_action(
    clip_db_id: int,
    payload: ClipActionPayload,
    request: Request,
    conn: asyncpg.Connection = Depends(get_db_connection)
):
    """Handles user actions on a clip (approve, skip, archive, merge, retry_splice)."""
    logger.info(f"Received action '{payload.action}' for clip_id: {clip_db_id}")

    action = payload.action
    target_state = None
    set_clauses = ["updated_at = NOW()", "last_error = NULL", "processing_metadata = NULL"] # Clear metadata on most actions
    params = []
    param_counter = 1

    if action == "approve":
        target_state = "review_approved"
        set_clauses.append("reviewed_at = NOW()")
    elif action == "skip":
        target_state = "review_skipped"
    elif action == "archive":
        target_state = "archived"
        set_clauses.append("reviewed_at = NOW()")
    elif action == "merge_next":
        target_state = "pending_merge_with_next"
    elif action == "retry_splice":
        target_state = "pending_resplice" # New state to trigger the resplice task
        # No extra params needed here unless passing retry settings via API
    else:
        raise HTTPException(status_code=400, detail=f"Invalid action: {action}")

    if target_state is None:
        raise HTTPException(status_code=400, detail="Could not determine target state for action.")

    # Parameter binding logic (remains largely the same)
    set_clauses.append(f"ingest_state = ${param_counter}")
    params.insert(0, target_state)

    where_param_index = len(params) + 1
    params.append(clip_db_id)

    # Define states from which actions are allowed (primarily pending/skipped, add others if needed)
    allowed_source_states = ['pending_review', 'review_skipped', 'merge_failed', 'split_failed', 'resplice_failed'] # Allow retry from failed states too

    try:
        async with conn.transaction():
            await conn.execute("SELECT pg_advisory_xact_lock(2, $1)", clip_db_id)
            current_state = await conn.fetchval("SELECT ingest_state FROM clips WHERE id = $1", clip_db_id)
            if not current_state:
                raise HTTPException(status_code=404, detail="Clip not found.")

            if current_state not in allowed_source_states:
                 logger.warning(f"Action '{action}' requested on clip {clip_db_id} with unexpected state '{current_state}'. Action rejected.")
                 raise HTTPException(status_code=409, detail=f"Clip is not in a state for this review action (state: {current_state}). Please refresh.")

            set_clause_sql = ', '.join(set_clauses)
            update_query = f"""
                UPDATE clips SET {set_clause_sql}
                WHERE id = ${where_param_index}
                AND ingest_state = ANY(${where_param_index + 1})
                RETURNING id;
            """
            final_params = params + [allowed_source_states]

            logger.debug(f"Executing Action Query: {update_query} with params: {final_params}")
            updated_id = await conn.fetchval(update_query, *final_params)

            if updated_id is None:
                current_state_after = await conn.fetchval("SELECT ingest_state FROM clips WHERE id = $1", clip_db_id)
                logger.warning(f"Failed to update clip {clip_db_id}. State might have changed. Current state: {current_state_after}")
                raise HTTPException(status_code=409, detail="Clip state may have changed, action could not be applied. Please refresh.")

        logger.info(f"Successfully updated clip {clip_db_id} to state '{target_state}'")
        return {"status": "success", "clip_id": clip_db_id, "new_state": target_state}

    except asyncpg.exceptions.UniqueViolationError as e:
         logger.error(f"Database error during clip action: {e}", exc_info=True)
         raise HTTPException(status_code=409, detail=f"Database constraint violated: {e}")
    except HTTPException:
        raise # Re-raise HTTP exceptions
    except Exception as e:
        logger.error(f"Error processing action for clip {clip_db_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error processing action.")


# --- API Endpoint for Undo ---
# Update allowed_undo_states to include the new states
@router.post("/api/clips/{clip_db_id}/undo", name="undo_clip_action", status_code=200)
async def undo_clip_action(
    clip_db_id: int,
    request: Request,
    conn: asyncpg.Connection = Depends(get_db_connection)
):
    """Attempts to revert a clip's state back to 'pending_review'."""
    logger.info(f"Received UNDO request for clip_id: {clip_db_id}")

    target_state = "pending_review"
    allowed_undo_states = [
        'review_approved', 'review_skipped', 'archived',
        'pending_merge_with_next',
        # 'pending_split', # Remove split
        'pending_resplice', # Add resplice state
        'merge_failed',
        # 'split_failed', # Remove split
        'resplice_failed' # Add resplice failure state
    ]

    try:
        async with conn.transaction():
            # Lock the clip row
            await conn.execute("SELECT pg_advisory_xact_lock(2, $1)", clip_db_id)

            # Get current state
            current_state_record = await conn.fetchrow("SELECT ingest_state, processing_metadata FROM clips WHERE id = $1", clip_db_id)
            if not current_state_record:
                raise HTTPException(status_code=404, detail="Clip not found.")

            current_state = current_state_record['ingest_state']

            if current_state not in allowed_undo_states:
                 raise HTTPException(status_code=409, detail=f"Cannot undo from current state: {current_state}")

            # Perform the revert
            # Clear related fields when undoing
            updated_id = await conn.fetchval(
                """
                UPDATE clips
                SET ingest_state = $1,
                    updated_at = NOW(),
                    reviewed_at = NULL, -- Clear review timestamp
                    processing_metadata = NULL, -- Clear split time etc.
                    last_error = 'Reverted by user action' -- Add note
                WHERE id = $2 AND ingest_state = $3 -- Ensure state hasn't changed concurrently
                RETURNING id;
                """,
                target_state, clip_db_id, current_state
            )

            if updated_id is None:
                # State changed between check and update
                current_state_after = await conn.fetchval("SELECT ingest_state FROM clips WHERE id = $1", clip_db_id)
                logger.warning(f"Undo failed for clip {clip_db_id}. State may have changed. Current state: {current_state_after}")
                raise HTTPException(status_code=409, detail="Clip state changed, undo failed. Please refresh.")

        logger.info(f"Successfully reverted clip {clip_db_id} to state '{target_state}'")
        return {"status": "success", "clip_id": clip_db_id, "new_state": target_state}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error processing undo for clip {clip_db_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error processing undo.")

app.include_router(router)
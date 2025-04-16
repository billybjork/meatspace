import os
import random
import ast
from typing import AsyncGenerator
from fastapi import FastAPI, Request, HTTPException, Query, Depends, APIRouter
from fastapi.responses import RedirectResponse, HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from dotenv import load_dotenv
import asyncpg
import traceback

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

print("--- app.py Configuration ---")
print(f"Using Database URL (partially hidden): {DATABASE_URL[:15]}...")
print(f"Using CloudFront Domain: {CLOUDFRONT_DOMAIN}")
print(f"Default Model: {DEFAULT_MODEL_NAME}")
print(f"Default Strategy: {DEFAULT_GENERATION_STRATEGY}")
print(f"Number of Results: {NUM_RESULTS}")
print("-----------------------------")

# --- Database Connection Dependency ---
async def get_db_connection(request: Request) -> AsyncGenerator[asyncpg.Connection, None]: # Correct type hint
    """FastAPI dependency to get a connection from the pool stored in app.state."""
    pool = getattr(request.app.state, 'db_pool', None)
    if pool is None:
         print("ERROR: Database connection pool 'db_pool' not found in app.state.")
         # Use 503 Service Unavailable for errors related to backend services
         raise HTTPException(status_code=503, detail="Database connection pool is not available.")
    connection = None # Initialize connection variable
    try:
        # Acquire connection separately to ensure it's released even if yield fails
        connection = await pool.acquire()
        yield connection
    except Exception as e: # Catch potential errors during yield/request handling
        print(f"ERROR during database dependency usage or request handling: {e}")
        traceback.print_exc()
        # Re-raise or handle appropriately. Depending on error, might not proceed.
        # Let FastAPI handle the exception, or raise a specific HTTP error
        raise HTTPException(status_code=500, detail="Internal server error during database operation.")
    finally:
        if connection:
            # print("Debug: Releasing DB connection back to pool.") # Optional debug log
            await pool.release(connection)

# --- FastAPI Application Instance ---
# TODO: Consider adding lifespan from main.py if running this file directly for testing
app = FastAPI(title="Meatspace")

# --- Static Files and Templates ---
script_dir = os.path.dirname(os.path.abspath(__file__)) # Use abspath for reliability
static_dir = os.path.join(script_dir, "static")
templates_dir = os.path.join(script_dir, "templates")

if not os.path.isdir(static_dir):
     print(f"WARNING: Static directory not found at: {static_dir}")
if not os.path.isdir(templates_dir):
     print(f"WARNING: Templates directory not found at: {templates_dir}")

app.mount("/static", StaticFiles(directory=static_dir), name="static")
templates = Jinja2Templates(directory=templates_dir)

# --- API Router ---
router = APIRouter()

# --- Helper Functions ---
def format_clip_data(record, request: Request): # TODO: request parameter might be removable?
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
    if not keyframe_url: print(f"Warning: Missing keyframe S3 key or CloudFront domain for clip: {record.get('clip_identifier')}")
    if not video_url: print(f"Warning: Missing clip S3 key or CloudFront domain for clip: {record.get('clip_identifier')}")

    return {
        "clip_id": record.get('clip_identifier'),
        "title": title,
        "keyframe_url": keyframe_url,
        "video_url": video_url,
        # Score is added dynamically in the route
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
             print("Warning: No distinct embedding options found in the database.")
    except asyncpg.exceptions.UndefinedTableError:
         print("ERROR: 'embeddings' table not found. Cannot fetch embedding options.")
         # Raise a more specific error or return empty list? Let's return empty and handle in routes.
    except KeyError as e:
         print(f"ERROR: Missing expected column in embeddings table: {e}") # Catch missing keys
         traceback.print_exc()
    except Exception as e:
        print(f"Error fetching available embedding options: {e}")
        traceback.print_exc()
    return options


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
                print(f"Warning: Default model/strategy ({DEFAULT_MODEL_NAME}/{DEFAULT_GENERATION_STRATEGY}) not found in DB options. Using first available.")
                final_model = available_options[0]['model_name']
                final_strategy = available_options[0]['strategy']
            elif not available_options:
                 # This case should ideally be handled by the query page itself showing an error
                 # But we can add a warning here. Redirect might still work but query page will fail search.
                 print("Warning: No embedding options available in DB. Redirecting with defaults, but similarity search may fail.")

            # Include default (or first available) model/strategy in the redirect URL query params
            redirect_url = request.url_for('query_clip', clip_id=random_clip_id).include_query_params(
                 model=final_model,
                 strategy=final_strategy
            )
            print(f"Redirecting to random clip: {random_clip_id} with model={final_model}, strategy={final_strategy}")
            return RedirectResponse(url=redirect_url, status_code=303) # Use 303 See Other for GET redirect
        else:
            # Handle case where there are no clips in the DB yet
            print("No clips found in database.")
            template_context["error"] = "No clips found in the database."
            return templates.TemplateResponse("index.html", template_context)

    except asyncpg.exceptions.UndefinedTableError as e:
         print(f"ERROR accessing DB tables (e.g., '{e}'): {e}. Check schema.")
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
        print(f"Error in index route: {e}")
        traceback.print_exc()
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
    print(f"Querying for clip_id='{clip_id}', model='{model_name}', strategy='{strategy}'")

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
            print(f"Clip identifier '{clip_id}' not found in database.")
            template_context["error"] = f"Requested clip '{clip_id}' not found in the database."
            # Attempt redirect to random clip (optional, could just show error)
            # Consider removing the redirect logic here to simplify - just show the error.
            return templates.TemplateResponse("index.html", template_context, status_code=404)

        # Format query clip data (gets CloudFront URLs)
        query_info = format_clip_data(query_clip_record, request)
        template_context["query"] = query_info

        if not query_info or query_info.get("video_url") is None or query_info.get("keyframe_url") is None:
             print(f"Warning: Missing S3 key data or URL generation failed for query clip {clip_id}.")
             # Proceed, but template should handle missing URLs gracefully

        # Check for embedding needed for similarity search
        query_embedding_data = query_clip_record['embedding']

        if not query_embedding_data:
             print(f"Warning: Embedding not found for query clip {clip_id} with model='{model_name}', strategy='{strategy}'. Cannot perform similarity search.")
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
            print(f"ERROR parsing/handling embedding data for clip {clip_id}: {parse_error}")
            print(f"Received embedding data type: {type(query_embedding_data)}, value (partial): {str(query_embedding_data)[:100]}...")
            template_context["error"] = f"Failed to process embedding data from database for query clip '{clip_id}'. Data might be corrupt or in unexpected format."
            return templates.TemplateResponse("index.html", template_context)


        # --- Similarity Query ---
        # Fetches S3 keys for results
        similarity_query = """
            SELECT
                c.clip_identifier,
                c.clip_filepath,      -- S3 Key for video
                c.keyframe_filepath,  -- S3 Key for keyframe
                1 - (e.embedding <=> $1::vector) AS similarity_score -- Cosine Similarity
            FROM embeddings e
            JOIN clips c ON e.clip_id = c.id
            WHERE e.model_name = $2             -- Use selected model
              AND e.generation_strategy = $3    -- Use selected strategy
              AND c.clip_identifier != $4       -- Exclude query clip itself
            ORDER BY
                e.embedding <=> $1::vector ASC  -- Order by distance (ascending)
            LIMIT $5;
        """

        similar_records = await conn.fetch(
            similarity_query,
            query_embedding_string, # Use string representation for query parameter
            model_name,             # Pass selected model
            strategy,               # Pass selected strategy
            clip_id,                # Exclude self
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
            print(msg)
            # Optionally add to context as non-error message: template_context["info_message"] = msg

    except asyncpg.exceptions.UndefinedFunctionError as e:
         print(f"DATABASE ERROR: pgvector function error. Is the pgvector extension installed and enabled? Error: {e}")
         traceback.print_exc()
         template_context["error"] = "Database error: Vector operations not available. Ensure pgvector extension is installed and enabled."
    except asyncpg.exceptions.DataError as e:
         # Could be invalid vector format, etc.
         print(f"DATABASE ERROR: Data error during query for clip {clip_id}: {e}")
         traceback.print_exc()
         template_context["error"] = f"Database data error: {e}. Check query parameters and vector format."
    except HTTPException:
        raise # Re-raise HTTP exceptions from dependencies
    except Exception as e:
        print(f"Error during query for clip {clip_id}: {e}")
        traceback.print_exc()
        template_context["error"] = f"An unexpected error occurred: {e}"

    # --- ALWAYS Pass data to template ---
    return templates.TemplateResponse("index.html", template_context)

app.include_router(router)
import os
import random
import ast
# --- Add typing.AsyncGenerator ---
from typing import AsyncGenerator
# ----------------------------------
from fastapi import FastAPI, Request, HTTPException, Query, Depends, APIRouter
from fastapi.responses import RedirectResponse, FileResponse, HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from dotenv import load_dotenv
import asyncpg
import traceback # For detailed error logging

load_dotenv()

# --- Configuration ---
DATABASE_URL = os.getenv("DATABASE_URL")
MEDIA_BASE_DIR = os.getenv("MEDIA_BASE_DIR")
DEFAULT_MODEL_NAME = os.getenv("DEFAULT_MODEL_NAME", "openai/clip-vit-base-patch32")
DEFAULT_GENERATION_STRATEGY = os.getenv("DEFAULT_GENERATION_STRATEGY", "keyframe_midpoint")
NUM_RESULTS = int(os.getenv("NUM_RESULTS", 10))

# --- Input Validation ---
if not DATABASE_URL:
    raise ValueError("DATABASE_URL not found in environment variables. Check your .env file.")
if not MEDIA_BASE_DIR:
    raise ValueError("MEDIA_BASE_DIR not found in environment variables. Check your .env file.")
if not os.path.isdir(MEDIA_BASE_DIR):
    raise ValueError(f"MEDIA_BASE_DIR '{MEDIA_BASE_DIR}' does not exist or is not a directory.")

print("--- app.py Configuration ---")
print(f"Using Database URL (partially hidden): {DATABASE_URL[:15]}...")
print(f"Using Media Base Directory: {MEDIA_BASE_DIR}")
print(f"Default Model: {DEFAULT_MODEL_NAME}")
print(f"Default Strategy: {DEFAULT_GENERATION_STRATEGY}")
print(f"Number of Results: {NUM_RESULTS}")
print("-----------------------------")

# --- Database Connection Dependency ---
# The actual pool is managed in main.py's lifespan and stored in app.state
async def get_db_connection(request: Request) -> AsyncGenerator[asyncpg.Connection, None]: # Correct type hint
    """FastAPI dependency to get a connection from the pool stored in app.state."""
    pool = getattr(request.app.state, 'db_pool', None)
    if pool is None:
         print("ERROR: Database connection pool 'db_pool' not found in app.state.")
         raise RuntimeError("Database connection pool is not available in app.state.")
    connection = None # Initialize connection variable
    try:
        # Acquire connection separately to ensure it's released even if yield fails
        connection = await pool.acquire()
        yield connection
    except Exception as e: # Catch potential errors during yield/request handling
        print(f"ERROR during database dependency usage or request handling: {e}")
        traceback.print_exc()
        # Re-raise or handle appropriately. Depending on error, might not proceed.
        raise # Re-raise the exception after logging
    finally:
        if connection:
            # print("Debug: Releasing DB connection back to pool.") # Optional debug log
            await pool.release(connection)


# --- FastAPI Application Instance ---
# Consider adding lifespan from main.py if running this file directly for testing
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
def format_clip_data(record, request: Request):
    """Formats a database record into a dictionary for the template."""
    if not record:
        return None
    # Use clip_identifier for display title if needed, or add a title column later
    title = record.get('clip_identifier', 'Unknown Clip').replace("_", " ").replace("-", " ").title()

    # Ensure file paths are not None before creating URLs
    keyframe_rel_path = record.get('keyframe_filepath')
    clip_rel_path = record.get('clip_filepath')

    keyframe_url = request.url_for('serve_media', filepath=keyframe_rel_path) if keyframe_rel_path else None
    video_url = request.url_for('serve_media', filepath=clip_rel_path) if clip_rel_path else None

    # Log warning if essential paths are missing
    if not keyframe_url: print(f"Warning: Missing keyframe path for clip: {record.get('clip_identifier')}")
    if not video_url: print(f"Warning: Missing clip path for clip: {record.get('clip_identifier')}")


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
        # --- FIX HERE: Explicitly create the dictionary ---
        options = [
            {"model_name": record["model_name"], "strategy": record["generation_strategy"]}
            for record in records
        ]
        # -------------------------------------------------
        if not options:
             print("Warning: No distinct embedding options found in the database.")
    except asyncpg.exceptions.UndefinedTableError:
         print("ERROR: 'embeddings' table not found. Cannot fetch embedding options.")
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
    try:
        # Fetch a random clip identifier from the DB
        random_clip_record = await conn.fetchrow(
            "SELECT clip_identifier FROM clips ORDER BY RANDOM() LIMIT 1"
        )
        if random_clip_record and random_clip_record['clip_identifier']:
            random_clip_id = random_clip_record['clip_identifier']
            # Fetch available options to determine if defaults are valid
            available_options = await fetch_available_embedding_options(conn)

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
                 print("Warning: No embedding options available in DB. Cannot set model/strategy for redirect.")
                 # Cannot redirect meaningfully without options, render index page with error
                 return templates.TemplateResponse("index.html", {
                    "request": request, "query": None, "results": [],
                    "error": "No clips or embedding options found in the database.",
                    "available_options": [], "model_name": final_model, "strategy": final_strategy
                 })


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
            available_options = await fetch_available_embedding_options(conn) # Attempt to fetch options anyway
            return templates.TemplateResponse("index.html", {
                "request": request,
                "query": None,
                "results": [],
                "error": "No clips found in the database.",
                "available_options": available_options, # Pass potentially empty list
                "model_name": DEFAULT_MODEL_NAME,       # Show defaults even if unusable
                "strategy": DEFAULT_GENERATION_STRATEGY
             })

    except asyncpg.exceptions.UndefinedTableError as e:
         print(f"ERROR accessing DB tables (e.g., '{e.table_name}'): {e}. Check schema.")
         # Try to fetch options ignoring the table error if possible
         if 'clips' not in str(e): available_options = await fetch_available_embedding_options(conn)
         return templates.TemplateResponse("index.html", {
             "request": request, "query": None, "results": [],
             "error": f"Database table error: {e}. Please check application setup.",
             "available_options": available_options,
             "model_name": DEFAULT_MODEL_NAME, "strategy": DEFAULT_GENERATION_STRATEGY
         })
    except Exception as e:
        print(f"Error in index route: {e}")
        traceback.print_exc()
        # Attempt to render template even on error, showing defaults
        # Avoid calling DB again in error handler if connection failed
        if isinstance(e, HTTPException) and e.status_code == 503:
             pass # DB connection already failed
        else:
             try: available_options = await fetch_available_embedding_options(conn)
             except Exception: pass # Ignore nested error
        # Render generic error page
        return templates.TemplateResponse("index.html", {
             "request": request, "query": None, "results": [],
             "error": "An unexpected error occurred while loading the page.",
             "available_options": available_options,
             "model_name": DEFAULT_MODEL_NAME, "strategy": DEFAULT_GENERATION_STRATEGY
        })


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
    query_info = None
    results = []
    error_message = None
    available_options = [] # Initialize

    # Define template context structure early
    template_context = {
        "request": request,
        "query": None,
        "results": [],
        "error": None,
        "available_options": [],
        "model_name": model_name,
        "strategy": strategy
    }

    try:
        # --- FETCH AVAILABLE OPTIONS FIRST ---
        available_options = await fetch_available_embedding_options(conn)
        template_context["available_options"] = available_options
        # --- END FETCH OPTIONS ---


        # 1. Fetch Query Clip Info (including its embedding and file paths)
        #    The query now uses the selected model_name and strategy
        #    Ensure column names match your actual schema (e.g., 'clip_identifier')
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
            clip_id, model_name, strategy # Pass selected model/strategy
        )

        if not query_clip_record:
            print(f"Clip identifier '{clip_id}' not found in database.")
            template_context["error"] = f"Requested clip '{clip_id}' not found in the database."
            # Try to find a random clip to suggest instead
            try:
                random_clip_record = await conn.fetchrow("SELECT clip_identifier FROM clips ORDER BY RANDOM() LIMIT 1")
                if random_clip_record and random_clip_record['clip_identifier']:
                    random_clip_id = random_clip_record['clip_identifier']
                    # Redirect to the random clip using the *currently selected* model/strategy
                    redirect_url = request.url_for('query_clip', clip_id=random_clip_id).include_query_params(
                        model=model_name, strategy=strategy
                    )
                    print(f"Clip '{clip_id}' not found, redirecting to random clip '{random_clip_id}'")
                    return RedirectResponse(url=redirect_url, status_code=303)
                else:
                    # No other clips available
                    template_context["error"] += " No alternative clips available."
                    return templates.TemplateResponse("index.html", template_context, status_code=404)
            except Exception as rand_e:
                 print(f"Error finding alternative clip: {rand_e}")
                 # Stick with the original error message
                 return templates.TemplateResponse("index.html", template_context, status_code=404)


        # Format query clip data if found
        query_info = format_clip_data(query_clip_record, request)
        template_context["query"] = query_info

        if not query_info or query_info.get("video_url") is None or query_info.get("keyframe_url") is None:
             print(f"Warning: Missing file path data or URL generation failed for query clip {clip_id}.")
             # Allow proceeding, but similarity search might fail if embedding missing

        # Check for embedding needed for similarity search
        query_embedding_data = query_clip_record['embedding']

        if not query_embedding_data:
             print(f"Warning: Embedding not found for query clip {clip_id} with model='{model_name}', strategy='{strategy}'. Cannot perform similarity search.")
             template_context["error"] = f"Embedding not found for clip '{clip_id}' (Model: {model_name}, Strategy: {strategy}). Cannot find similar clips. Try selecting a different model/strategy or generate the missing embedding."
             # Render template showing the query clip but no results
             return templates.TemplateResponse("index.html", template_context)


        # --- Parse Embedding and Perform Similarity Search ---
        try:
            # Using ast.literal_eval is safer if asyncpg isn't configured for vectors
            # Ensure the data from DB is actually a string representation of a list
            if isinstance(query_embedding_data, str):
                 query_embedding_vector = ast.literal_eval(query_embedding_data)
            elif isinstance(query_embedding_data, (list, bytes)): # bytes might come from BYTEA storage
                 # If it's bytes, assume direct vector format or requires decoding
                 # This part depends heavily on how vectors are stored and fetched
                 # For now, assume if it's not string, it needs string conversion for query
                 # If asyncpg returns list directly, this works. If bytes, needs handling.
                 query_embedding_vector = query_embedding_data # Potentially needs conversion if bytes
            else:
                 raise TypeError(f"Unexpected embedding data type: {type(query_embedding_data)}")

            if not isinstance(query_embedding_vector, list):
                 # Handle cases where parsing failed or data wasn't list-like
                 # Maybe try converting bytes? Requires knowing the encoding/format.
                 raise ValueError("Parsed/obtained embedding is not a list.")

            # Convert the vector list to the string format pgvector expects ('[1,2,3]')
            query_embedding_string = '[' + ','.join(map(str, query_embedding_vector)) + ']'

        except (ValueError, SyntaxError, TypeError) as parse_error:
            print(f"ERROR parsing/handling embedding data for clip {clip_id}: {parse_error}")
            print(f"Received embedding data type: {type(query_embedding_data)}, value (partial): {str(query_embedding_data)[:100]}...")
            template_context["error"] = f"Failed to process embedding data from database for query clip '{clip_id}'. Data might be corrupt or in unexpected format."
            return templates.TemplateResponse("index.html", template_context)


        # --- Similarity Query ---
        similarity_query = """
            SELECT
                c.clip_identifier,
                c.clip_filepath,
                c.keyframe_filepath,
                -- Cosine Similarity: 1 - cosine distance
                1 - (e.embedding <=> $1::vector) AS similarity_score
            FROM embeddings e
            JOIN clips c ON e.clip_id = c.id
            WHERE e.model_name = $2             -- Use selected model
              AND e.generation_strategy = $3    -- Use selected strategy
              AND c.clip_identifier != $4       -- Exclude query clip itself
              -- Optional: Add a threshold? e.g. AND (e.embedding <=> $1::vector) < 0.8
            ORDER BY
                e.embedding <=> $1::vector ASC -- Order by distance (ascending)
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

        # Format results
        for record in similar_records:
            formatted = format_clip_data(record, request)
            if formatted:
                # Ensure score is float, handle potential None from DB calculation? Unlikely.
                score = record.get('similarity_score')
                formatted["score"] = float(score) if score is not None else 0.0
                results.append(formatted)
        template_context["results"] = results


        if not results and not template_context["error"]:
            msg = f"No similar clips found for '{clip_id}' using Model: {model_name}, Strategy: {strategy}."
            print(msg)
            # Optionally add this as a non-error message to the context
            # template_context["info_message"] = msg


    except asyncpg.exceptions.UndefinedFunctionError as e:
         print(f"DATABASE ERROR: pgvector function error. Is the pgvector extension installed and enabled? Error: {e}")
         traceback.print_exc()
         template_context["error"] = "Database error: Vector operations not available. Ensure pgvector extension is installed and enabled."
    except asyncpg.exceptions.DataError as e:
         print(f"DATABASE ERROR: Data error during query for clip {clip_id}: {e}")
         traceback.print_exc()
         template_context["error"] = f"Database data error: {e}. Check query parameters and vector format."
    except Exception as e:
        print(f"Error during query for clip {clip_id}: {e}")
        traceback.print_exc()
        template_context["error"] = f"An unexpected error occurred: {e}"

    # --- ALWAYS Pass data to template ---
    return templates.TemplateResponse("index.html", template_context)


# Separate route for media serving
@router.get("/media/{filepath:path}", name="serve_media")
async def serve_media(filepath: str):
    """Serves media files (videos, images) safely from the MEDIA_BASE_DIR."""
    if not filepath or filepath == 'None': # Added check for 'None' string path
         print(f"Warning: Attempted to serve media with invalid path: {filepath}")
         raise HTTPException(status_code=404, detail="File not found (Invalid Path)")

    safe_base_path = os.path.abspath(MEDIA_BASE_DIR)
    # Clean the filepath: replace backslashes, remove leading slashes
    cleaned_filepath = filepath.replace("\\", "/").lstrip('/')
    requested_path = os.path.abspath(os.path.join(safe_base_path, cleaned_filepath))

    # Security check: Ensure the resolved path is still within the media base directory
    if not requested_path.startswith(safe_base_path):
        print(f"Warning: Attempted directory traversal: {filepath} -> {requested_path}")
        raise HTTPException(status_code=404, detail="File not found (Invalid Path)")

    if not os.path.isfile(requested_path):
        print(f"Warning: Media file not found at resolved path: {requested_path} (from input: {filepath})")
        raise HTTPException(status_code=404, detail="File not found")

    # Optional: Add caching headers here
    # from fastapi.responses import Response
    # headers = {"Cache-Control": "public, max-age=3600"} # Cache for 1 hour
    # return FileResponse(requested_path, headers=headers)
    return FileResponse(requested_path)


# Include the router in the main app instance
app.include_router(router)

# --- Optional: Add main block for direct running (for testing) ---
# if __name__ == "__main__":
#     import uvicorn
#     print("Running FastAPI app directly using Uvicorn (for testing)...")
#     print("WARNING: Database pool might not be initialized without main.py's lifespan.")
#     uvicorn.run(app, host="0.0.0.0", port=8000)
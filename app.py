import os
import random
import ast
from fastapi import FastAPI, Request, HTTPException, Query, Depends, APIRouter
from fastapi.responses import RedirectResponse, FileResponse, HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from dotenv import load_dotenv
import asyncpg

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
print("-----------------------------")

# --- Database Connection Dependency ---
# The actual pool is managed in main.py's lifespan and stored in app.state
async def get_db_connection(request: Request) -> asyncpg.Connection:
    """FastAPI dependency to get a connection from the pool stored in app.state."""
    pool = getattr(request.app.state, 'db_pool', None)
    if pool is None:
         # This indicates a programming error (lifespan didn't run or failed)
         raise RuntimeError("Database connection pool is not available in app.state.")
    async with pool.acquire() as connection:
        yield connection

# --- FastAPI Application Instance ---
app = FastAPI(title="Meatspace")

# --- Static Files and Templates ---
script_dir = os.path.dirname(__file__)
app.mount("/static", StaticFiles(directory=os.path.join(script_dir, "static")), name="static")
templates = Jinja2Templates(directory=os.path.join(script_dir, "templates"))

# --- API Router ---
router = APIRouter()

# --- Helper Functions ---
def format_clip_data(record, request: Request):
    """Formats a database record into a dictionary for the template."""
    if not record:
        return None
    # Use clip_identifier for display title if needed, or add a title column later
    title = record['clip_identifier'].replace("_", " ").replace("-", " ").title()
    return {
        "clip_id": record['clip_identifier'],
        "title": title,
        "keyframe_url": request.url_for('serve_media', filepath=record['keyframe_filepath']) if record['keyframe_filepath'] else None,
        "video_url": request.url_for('serve_media', filepath=record['clip_filepath']) if record['clip_filepath'] else None,
        # Score is added dynamically in the route
    }

# --- API Routes (defined on the router) ---

@router.get("/", response_class=HTMLResponse, name="index")
async def index(request: Request, conn: asyncpg.Connection = Depends(get_db_connection)):
    """Redirects to a random clip's query page."""
    try:
        # Fetch a random clip identifier from the DB
        random_clip_record = await conn.fetchrow(
            "SELECT clip_identifier FROM clips ORDER BY RANDOM() LIMIT 1"
        )
        if random_clip_record:
            random_clip_id = random_clip_record['clip_identifier']
            # Include default model/strategy in the redirect URL query params
            redirect_url = request.url_for('query_clip', clip_id=random_clip_id).include_query_params(
                 model=DEFAULT_MODEL_NAME, # Use alias 'model' for URL
                 strategy=DEFAULT_GENERATION_STRATEGY
            )
            return RedirectResponse(url=redirect_url)
        else:
            # Handle case where there are no clips in the DB yet
             return templates.TemplateResponse("index.html", {"request": request, "query": None, "results": [], "error": "No clips found in the database."})

    except Exception as e:
        print(f"Error fetching random clip: {e}")
        raise HTTPException(status_code=500, detail="Error retrieving data from database.")


@router.get("/query/{clip_id}", response_class=HTMLResponse, name="query_clip")
async def query_clip(
    clip_id: str,
    request: Request,
    conn: asyncpg.Connection = Depends(get_db_connection),
    # Add query parameters for model and strategy selection
    model_name: str = Query(DEFAULT_MODEL_NAME, alias="model"),
    strategy: str = Query(DEFAULT_GENERATION_STRATEGY, alias="strategy")
):
    """Displays a query clip and finds similar clips based on embeddings."""
    print(f"Querying for clip_id='{clip_id}', model='{model_name}', strategy='{strategy}'")
    query_info = None
    results = []
    error_message = None

    try:
        # 1. Fetch Query Clip Info (including its embedding and file paths)
        query_clip_record = await conn.fetchrow(
            """
            SELECT c.id, c.clip_identifier, c.clip_filepath, c.keyframe_filepath, e.embedding
            FROM clips c
            LEFT JOIN embeddings e ON c.id = e.clip_id
                                  AND e.model_name = $2
                                  AND e.generation_strategy = $3
            WHERE c.clip_identifier = $1;
            """,
            clip_id, model_name, strategy
        )

        if not query_clip_record:
            print(f"Clip identifier '{clip_id}' not found in database.")
            try:
                random_clip_record = await conn.fetchrow("SELECT clip_identifier FROM clips ORDER BY RANDOM() LIMIT 1")
                if random_clip_record:
                    random_clip_id = random_clip_record['clip_identifier']
                    redirect_url = request.url_for('query_clip', clip_id=random_clip_id).include_query_params(model=model_name, strategy=strategy)
                    return RedirectResponse(url=redirect_url)
                else:
                    raise HTTPException(status_code=404, detail="Requested clip not found, and no other clips available.")
            except Exception as rand_e:
                 raise HTTPException(status_code=500, detail=f"Requested clip not found, error finding alternative: {rand_e}")

        query_info = format_clip_data(query_clip_record, request)

        if not query_info or query_info.get("video_url") is None:
             print(f"Warning: Missing file path data for query clip {clip_id}.")
             if not error_message:
                error_message = f"Missing file path information for clip '{clip_id}'."

        query_embedding_data = query_clip_record['embedding']

        if not query_embedding_data:
             print(f"Warning: Embedding not found for query clip {clip_id} with model='{model_name}', strategy='{strategy}'. Cannot perform similarity search.")
             error_message = f"Embedding not found for clip '{clip_id}' (Model: {model_name}, Strategy: {strategy}). Cannot find similar clips."
             return templates.TemplateResponse("index.html", {"request": request, "query": query_info, "results": [], "error": error_message, "model_name": model_name, "strategy": strategy})

        try:
            query_embedding_vector = ast.literal_eval(query_embedding_data)
            if not isinstance(query_embedding_vector, list):
                raise ValueError("Parsed embedding is not a list.")
        except (ValueError, SyntaxError, TypeError) as parse_error:
            print(f"ERROR parsing embedding string for clip {clip_id}: {parse_error}")
            print(f"Received embedding data type: {type(query_embedding_data)}, value (partial): {str(query_embedding_data)[:100]}...")
            error_message = f"Failed to parse embedding data from database for query clip '{clip_id}'. Check database integrity or format."
            return templates.TemplateResponse("index.html", {"request": request, "query": query_info, "results": [], "error": error_message, "model_name": model_name, "strategy": strategy})

        similarity_query = """
            SELECT
                c.clip_identifier,
                c.clip_filepath,
                c.keyframe_filepath,
                1 - (e.embedding <=> $1::vector) AS similarity_score
            FROM embeddings e
            JOIN clips c ON e.clip_id = c.id
            WHERE e.model_name = $2
              AND e.generation_strategy = $3
              AND c.clip_identifier != $4
            ORDER BY e.embedding <=> $1::vector
            LIMIT $5;
        """
        query_embedding_string = str(query_embedding_vector)

        similar_records = await conn.fetch(
            similarity_query,
            query_embedding_string,
            model_name,
            strategy,
            clip_id,
            NUM_RESULTS
        )

        for record in similar_records:
            formatted = format_clip_data(record, request)
            if formatted:
                formatted["score"] = record['similarity_score']
                results.append(formatted)

        if not results and not error_message:
            print(f"No similar clips found for {clip_id} with model='{model_name}', strategy='{strategy}'.")

    except asyncpg.exceptions.UndefinedFunctionError as e:
         print(f"DATABASE ERROR: pgvector function error. Is the pgvector extension installed and enabled? Error: {e}")
         error_message = "Database error: Vector operations not available. Ensure pgvector extension is installed and enabled."
    except Exception as e:
        print(f"Error during query for clip {clip_id}: {e}")
        import traceback
        traceback.print_exc()
        error_message = f"An unexpected error occurred: {e}"

    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "query": query_info,
            "results": results,
            "error": error_message,
            "model_name": model_name,
            "strategy": strategy
        }
    )

# Separate route for media serving, still makes sense to be defined here
# near the other routes, but doesn't need DB access.
@router.get("/media/{filepath:path}", name="serve_media")
async def serve_media(filepath: str):
    """Serves media files (videos, images) safely from the MEDIA_BASE_DIR."""
    safe_base_path = os.path.abspath(MEDIA_BASE_DIR)
    requested_path = os.path.abspath(os.path.join(safe_base_path, filepath))

    if os.path.commonpath([safe_base_path]) != os.path.commonpath([safe_base_path, requested_path]):
        print(f"Warning: Attempted directory traversal: {filepath}")
        raise HTTPException(status_code=404, detail="File not found (Invalid Path)")

    if not os.path.isfile(requested_path):
        print(f"Warning: Media file not found at: {requested_path}")
        raise HTTPException(status_code=404, detail="File not found")

    return FileResponse(requested_path)

# Include the router in the main app instance
app.include_router(router)
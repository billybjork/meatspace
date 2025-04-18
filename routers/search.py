import ast
from fastapi import APIRouter, Request, Depends, Query, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse
import asyncpg
from asyncpg.exceptions import UndefinedTableError, UndefinedFunctionError, DataError

# Import shared components
from config import log, DEFAULT_MODEL_NAME, DEFAULT_GENERATION_STRATEGY, NUM_RESULTS
from database import get_db_connection
from services import format_clip_data, fetch_available_embedding_options, parse_embedding_data
# Import templates object (defined in main.py) - requires careful setup or passing
# For simplicity here, assume templates is globally accessible or passed via dependency
# TODO: A cleaner way is to define templates in main.py and use it there or pass via request state.
# Let's assume it's accessible via request.app.state.templates for this example.

router = APIRouter(
    tags=["Search & Query"] # Tag for API docs grouping
)

@router.get("/", response_class=HTMLResponse, name="index")
async def index(request: Request, conn: asyncpg.Connection = Depends(get_db_connection)):
    """Redirects to a random clip's query page or shows index if no clips."""
    templates = request.app.state.templates # Get templates from app state
    available_options = []
    template_context = {
        "request": request, "query": None, "results": [], "error": None,
        "available_options": [], "model_name": DEFAULT_MODEL_NAME,
        "strategy": DEFAULT_GENERATION_STRATEGY
    }
    try:
        available_options = await fetch_available_embedding_options(conn)
        template_context["available_options"] = available_options

        # Find a random clip that *has* embeddings for the selected model/strategy
        random_clip_record = await conn.fetchrow(
            """
            SELECT c.clip_identifier
            FROM clips c
            JOIN embeddings e ON c.id = e.clip_id
            WHERE c.ingest_state = 'embedded' -- Ensure clip is ready
              AND e.model_name = $1
              AND e.generation_strategy = $2
              AND e.embedding IS NOT NULL -- Ensure embedding exists
            ORDER BY RANDOM()
            LIMIT 1
            """,
            DEFAULT_MODEL_NAME, DEFAULT_GENERATION_STRATEGY
        )

        if random_clip_record and random_clip_record['clip_identifier']:
            random_clip_id = random_clip_record['clip_identifier']
            # Determine model/strategy for redirect (use default or first available)
            default_exists = any(
                opt['model_name'] == DEFAULT_MODEL_NAME and opt['strategy'] == DEFAULT_GENERATION_STRATEGY
                for opt in available_options
            )
            final_model = DEFAULT_MODEL_NAME
            final_strategy = DEFAULT_GENERATION_STRATEGY
            if not default_exists and available_options:
                log.warning(f"Default model/strategy '{DEFAULT_MODEL_NAME}/{DEFAULT_GENERATION_STRATEGY}' not found among available embeddings. Using first available.")
                final_model = available_options[0]['model_name']
                final_strategy = available_options[0]['strategy']
            elif not available_options:
                 log.warning("No embedding options available. Redirecting with defaults anyway.")

            redirect_url = request.url_for('query_clip', clip_id=random_clip_id).include_query_params(
                 model=final_model, strategy=final_strategy
            )
            log.info(f"Redirecting to random embedded clip: {random_clip_id} with model={final_model}, strategy={final_strategy}")
            return RedirectResponse(url=redirect_url, status_code=303) # 303 See Other for POST->GET redirect pattern
        else:
            log.info(f"No embedded clips found for default model/strategy '{DEFAULT_MODEL_NAME}/{DEFAULT_GENERATION_STRATEGY}'. Serving index page.")
            template_context["error"] = "No embedded clips found for default criteria. Check ingest status or select different options."
            return templates.TemplateResponse("index.html", template_context)

    except UndefinedTableError as e:
         log.error(f"DB table error (e.g., '{e}'): {e}. Check schema.", exc_info=True)
         template_context["error"] = f"Database table error: {e}. Please check setup."
         # Attempt to fetch options even if clips/embeddings fails
         try: template_context["available_options"] = await fetch_available_embedding_options(conn)
         except Exception: pass # Ignore error here if primary query already failed
         return templates.TemplateResponse("index.html", template_context)
    except HTTPException:
        raise # Re-raise FastAPI HTTP exceptions
    except Exception as e:
        log.error(f"Error in index route: {e}", exc_info=True)
        template_context["error"] = "An unexpected error occurred loading the page."
        return templates.TemplateResponse("index.html", template_context)

@router.get("/query/{clip_id}", response_class=HTMLResponse, name="query_clip")
async def query_clip(
    clip_id: str, # Use clip_identifier (string) as path param
    request: Request,
    conn: asyncpg.Connection = Depends(get_db_connection),
    model_name: str = Query(DEFAULT_MODEL_NAME, alias="model"),
    strategy: str = Query(DEFAULT_GENERATION_STRATEGY, alias="strategy")
):
    """Displays query clip and finds similar clips based on selected embeddings."""
    templates = request.app.state.templates # Get templates from app state
    log.info(f"Querying for clip_id='{clip_id}', model='{model_name}', strategy='{strategy}'")
    template_context = {
        "request": request, "query": None, "results": [], "error": None,
        "available_options": [], "model_name": model_name, "strategy": strategy
    }

    try:
        template_context["available_options"] = await fetch_available_embedding_options(conn)

        # Fetch query clip details including the specific embedding
        query_clip_record = await conn.fetchrow(
            """
            SELECT
                c.id, c.clip_identifier, c.clip_filepath, c.keyframe_filepath,
                e.embedding -- Fetch the specific embedding
            FROM clips c
            LEFT JOIN embeddings e ON c.id = e.clip_id AND e.model_name = $2 AND e.generation_strategy = $3
            WHERE c.clip_identifier = $1;
            """,
            clip_id, model_name, strategy
        )

        if not query_clip_record:
            log.warning(f"Clip identifier '{clip_id}' not found.")
            template_context["error"] = f"Requested clip '{clip_id}' not found."
            return templates.TemplateResponse("index.html", template_context, status_code=404)

        query_clip_db_id = query_clip_record['id']
        query_info = format_clip_data(query_clip_record, request) # Use service function
        template_context["query"] = query_info

        if not query_info or (not query_info.get("video_url") and not query_info.get("keyframe_url")):
             log.warning(f"Missing S3 key or URL failed for query clip {clip_id} (DB ID: {query_clip_db_id}).")
             # Allow showing page even if media missing, but maybe add note in template

        # Check if embedding was found for the specified model/strategy
        query_embedding_data = query_clip_record['embedding']
        if not query_embedding_data:
             log.warning(f"Embedding not found for query clip {clip_id} (DB ID: {query_clip_db_id}) model='{model_name}', strategy='{strategy}'.")
             template_context["error"] = f"Embedding not found for clip '{clip_id}' (Model: {model_name}, Strategy: {strategy}). Cannot find similar clips."
             # Still return the page, but without results
             return templates.TemplateResponse("index.html", template_context)

        # Parse the embedding data into a list using the service function
        try:
            query_embedding_vector = parse_embedding_data(query_embedding_data, clip_id, query_clip_db_id)
            # Format for pgvector query (asyncpg handles list-to-array conversion often, but string is safest)
            query_embedding_string = '[' + ','.join(map(str, query_embedding_vector)) + ']'
        except ValueError as parse_error:
            template_context["error"] = str(parse_error)
            return templates.TemplateResponse("index.html", template_context)

        # Perform similarity search using pgvector operator
        # Exclude the query clip itself
        similarity_query = """
            SELECT
                c.id, c.clip_identifier, c.clip_filepath, c.keyframe_filepath,
                1 - (e.embedding <=> $1::vector) AS similarity_score -- Cosine Similarity
            FROM embeddings e
            JOIN clips c ON e.clip_id = c.id
            WHERE e.model_name = $2
              AND e.generation_strategy = $3
              AND c.id != $4 -- Exclude the query clip
              AND c.ingest_state = 'embedded' -- Only compare against ready clips
              AND e.embedding IS NOT NULL -- Ensure target embedding exists
            ORDER BY e.embedding <=> $1::vector ASC -- ASC for cosine distance (smallest distance = highest similarity)
            LIMIT $5;
        """
        similar_records = await conn.fetch(
            similarity_query, query_embedding_string, model_name, strategy, query_clip_db_id, NUM_RESULTS
        )

        results = []
        for record in similar_records:
            formatted = format_clip_data(record, request) # Use service function
            if formatted:
                # Score is already calculated in the query
                # formatted["score"] = float(score) if score is not None else 0.0 # Already handled by format_clip_data
                results.append(formatted)
        template_context["results"] = results

        if not results and not template_context["error"]:
            log.info(f"No similar clips found for '{clip_id}' using {model_name}/{strategy}.")
            # Optionally add a message like "No similar clips found." to context

    except UndefinedFunctionError as e:
         log.error(f"DB ERROR: pgvector function error (vector operator <=>). Is pgvector extension installed and enabled? Error: {e}", exc_info=True)
         template_context["error"] = "Database error: Vector operations not available. Check pgvector extension."
    except DataError as e:
         # e.g., invalid vector format
         log.error(f"DB ERROR: Data error during query for clip {clip_id}: {e}", exc_info=True)
         template_context["error"] = f"Database data error. Check vector format or query parameters."
    except HTTPException:
        raise # Re-raise FastAPI HTTP exceptions
    except Exception as e:
        log.error(f"Error during query for clip {clip_id}: {e}", exc_info=True)
        template_context["error"] = f"An unexpected server error occurred."

    return templates.TemplateResponse("index.html", template_context)
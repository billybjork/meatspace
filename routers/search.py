import ast
from fastapi import APIRouter, Request, Depends, Query, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse
import asyncpg
from asyncpg.exceptions import UndefinedTableError, UndefinedFunctionError, DataError

# Import shared components
from config import log, DEFAULT_MODEL_NAME, DEFAULT_GENERATION_STRATEGY, NUM_RESULTS
from database import get_db_connection
# Assuming services.py contains the necessary updated function
# NOTE: format_clip_data in services.py MUST be updated to handle 'representative_keyframe_s3_key'
from services import format_clip_data, fetch_available_embedding_options, parse_embedding_data

router = APIRouter(
    tags=["Search & Query"] # Tag for API docs grouping
)

# Constants for Artifacts
ARTIFACT_TYPE_KEYFRAME = "keyframe"
REPRESENTATIVE_TAG = "representative"

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
        # No change needed here as it only needs the identifier for redirect
        random_clip_record = await conn.fetchrow(
            """
            SELECT c.clip_identifier
            FROM clips c
            JOIN embeddings e ON c.id = e.clip_id
            WHERE c.ingest_state = 'embedded' -- Ensure clip is ready
              AND e.model_name = $1
              AND e.generation_strategy = $2
              AND e.embedding IS NOT NULL
            ORDER BY RANDOM()
            LIMIT 1
            """,
            DEFAULT_MODEL_NAME, DEFAULT_GENERATION_STRATEGY
        )

        if random_clip_record and random_clip_record['clip_identifier']:
            random_clip_identifier = random_clip_record['clip_identifier'] # Use identifier
            # Determine model/strategy for redirect (use default or first available)
            default_exists = any(
                opt['model_name'] == DEFAULT_MODEL_NAME and opt['strategy'] == DEFAULT_GENERATION_STRATEGY
                for opt in available_options
            )
            final_model = DEFAULT_MODEL_NAME
            final_strategy = DEFAULT_GENERATION_STRATEGY
            if not default_exists and available_options:
                log.warning(f"Default model/strategy '{DEFAULT_MODEL_NAME}/{DEFAULT_GENERATION_STRATEGY}' not found. Using first available.")
                final_model = available_options[0]['model_name']
                final_strategy = available_options[0]['strategy']
            elif not available_options:
                 log.warning("No embedding options available. Redirecting with defaults anyway.")

            redirect_url = request.url_for('query_clip', clip_identifier=random_clip_identifier).include_query_params( # Pass identifier
                 model=final_model, strategy=final_strategy
            )
            log.info(f"Redirecting to random embedded clip: {random_clip_identifier} with model={final_model}, strategy={final_strategy}")
            return RedirectResponse(url=redirect_url, status_code=303)
        else:
            log.info(f"No embedded clips found for default model/strategy '{DEFAULT_MODEL_NAME}/{DEFAULT_GENERATION_STRATEGY}'. Serving index page.")
            template_context["error"] = "No embedded clips found for default criteria. Check ingest status or select different options."
            return templates.TemplateResponse("index.html", template_context)

    except UndefinedTableError as e:
         log.error(f"DB table error (e.g., '{e}'): {e}. Check schema.", exc_info=True)
         template_context["error"] = f"Database table error: {e}. Please check setup."
         try: template_context["available_options"] = await fetch_available_embedding_options(conn)
         except Exception: pass
         return templates.TemplateResponse("index.html", template_context)
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Error in index route: {e}", exc_info=True)
        template_context["error"] = "An unexpected error occurred loading the page."
        return templates.TemplateResponse("index.html", template_context)

@router.get("/query/{clip_identifier}", response_class=HTMLResponse, name="query_clip")
async def query_clip(
    clip_identifier: str, # Use clip_identifier (string) as path param
    request: Request,
    conn: asyncpg.Connection = Depends(get_db_connection),
    model_name: str = Query(DEFAULT_MODEL_NAME, alias="model"),
    strategy: str = Query(DEFAULT_GENERATION_STRATEGY, alias="strategy")
):
    """Displays query clip and finds similar clips based on selected embeddings."""
    templates = request.app.state.templates # Get templates from app state
    log.info(f"Querying for clip_identifier='{clip_identifier}', model='{model_name}', strategy='{strategy}'")
    template_context = {
        "request": request, "query": None, "results": [], "error": None,
        "available_options": [], "model_name": model_name, "strategy": strategy
    }

    try:
        template_context["available_options"] = await fetch_available_embedding_options(conn)

        # Fetch query clip details, including the specific embedding AND representative keyframe artifact
        query_clip_sql = """
            SELECT
                c.id, c.clip_identifier, c.clip_filepath,
                c.start_time_seconds, c.end_time_seconds, -- Include time info if needed by format_clip_data
                ca.s3_key AS representative_keyframe_s3_key, -- Select keyframe S3 key from artifacts
                e.embedding -- Fetch the specific embedding
            FROM clips c
            LEFT JOIN embeddings e ON c.id = e.clip_id AND e.model_name = $2 AND e.generation_strategy = $3
            LEFT JOIN clip_artifacts ca ON c.id = ca.clip_id -- Join artifacts table
                  AND ca.artifact_type = $4 -- Filter for keyframes
                  AND ca.tag = $5 -- Filter for the representative one
            WHERE c.clip_identifier = $1;
        """
        query_clip_record = await conn.fetchrow(
            query_clip_sql,
            clip_identifier, model_name, strategy,
            ARTIFACT_TYPE_KEYFRAME, REPRESENTATIVE_TAG # Pass artifact filters as params
        )

        if not query_clip_record:
            log.warning(f"Clip identifier '{clip_identifier}' not found.")
            template_context["error"] = f"Requested clip '{clip_identifier}' not found."
            return templates.TemplateResponse("index.html", template_context, status_code=404)

        # Use the fetched DB ID
        query_clip_db_id = query_clip_record['id']
        # IMPORTANT: format_clip_data needs to be updated to use 'representative_keyframe_s3_key'
        query_info = format_clip_data(query_clip_record, request)
        template_context["query"] = query_info

        if not query_info or (not query_info.get("video_url") and not query_info.get("keyframe_url")):
             log.warning(f"Missing S3 key or URL failed for query clip {clip_identifier} (DB ID: {query_clip_db_id}). Keyframe S3 key from DB: {query_clip_record['representative_keyframe_s3_key']}")
             # Allow showing page even if media missing, maybe add note in template

        # Check if embedding was found for the specified model/strategy
        query_embedding_data = query_clip_record['embedding']
        if not query_embedding_data:
             log.warning(f"Embedding not found for query clip {clip_identifier} (DB ID: {query_clip_db_id}) model='{model_name}', strategy='{strategy}'.")
             template_context["error"] = f"Embedding not found for clip '{clip_identifier}' (Model: {model_name}, Strategy: {strategy}). Cannot find similar clips."
             return templates.TemplateResponse("index.html", template_context)

        # Parse the embedding data into a list using the service function
        try:
            query_embedding_vector = parse_embedding_data(query_embedding_data, clip_identifier, query_clip_db_id)
            query_embedding_string = '[' + ','.join(map(str, query_embedding_vector)) + ']'
        except ValueError as parse_error:
            template_context["error"] = str(parse_error)
            return templates.TemplateResponse("index.html", template_context)

        # Perform similarity search using pgvector operator
        # Fetch result clip details, including *their* representative keyframes
        similarity_query = """
            SELECT
                c.id, c.clip_identifier, c.clip_filepath,
                c.start_time_seconds, c.end_time_seconds, -- Include time info if needed
                ca.s3_key AS representative_keyframe_s3_key, -- Select keyframe S3 key from artifacts
                1 - (e.embedding <=> $1::vector) AS similarity_score -- Cosine Similarity
            FROM embeddings e
            JOIN clips c ON e.clip_id = c.id
            LEFT JOIN clip_artifacts ca ON c.id = ca.clip_id -- Join artifacts table for results
                  AND ca.artifact_type = $6 -- Filter for keyframes
                  AND ca.tag = $7 -- Filter for the representative one
            WHERE e.model_name = $2
              AND e.generation_strategy = $3
              AND c.id != $4 -- Exclude the query clip
              AND c.ingest_state = 'embedded' -- Only compare against ready clips
              AND e.embedding IS NOT NULL
            ORDER BY e.embedding <=> $1::vector ASC -- ASC for cosine distance
            LIMIT $5;
        """
        similar_records = await conn.fetch(
            similarity_query,
            query_embedding_string, model_name, strategy, query_clip_db_id, NUM_RESULTS,
            ARTIFACT_TYPE_KEYFRAME, REPRESENTATIVE_TAG # Pass artifact filters as params
        )

        results = []
        for record in similar_records:
            # IMPORTANT: format_clip_data needs to be updated to use 'representative_keyframe_s3_key'
            formatted = format_clip_data(record, request)
            if formatted:
                results.append(formatted) # Score is already in 'formatted' if added by format_clip_data
        template_context["results"] = results

        if not results and not template_context["error"]:
            log.info(f"No similar clips found for '{clip_identifier}' using {model_name}/{strategy}.")

    except UndefinedFunctionError as e:
         log.error(f"DB ERROR: pgvector function error (vector operator <=>). Is pgvector extension installed and enabled? Error: {e}", exc_info=True)
         template_context["error"] = "Database error: Vector operations not available. Check pgvector extension."
    except DataError as e:
         log.error(f"DB ERROR: Data error during query for clip {clip_identifier}: {e}", exc_info=True)
         template_context["error"] = f"Database data error. Check vector format or query parameters."
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Error during query for clip {clip_identifier}: {e}", exc_info=True)
        template_context["error"] = f"An unexpected server error occurred."

    return templates.TemplateResponse("index.html", template_context)
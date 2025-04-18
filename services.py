import ast
import asyncpg
from fastapi import Request
from asyncpg.exceptions import UndefinedTableError

from config import CLOUDFRONT_DOMAIN, log

# This file contains helper functions or business logic used by API routes

def format_clip_data(record, request: Request):
    """Formats a database record into a dictionary for the template, using CloudFront URLs."""
    if not record:
        return None

    # Use .get() for safer access in case columns are missing unexpectedly
    clip_id = record.get('clip_identifier', 'Unknown Clip')
    title = clip_id.replace("_", " ").replace("-", " ").title()
    keyframe_s3_key = record.get('keyframe_filepath')
    clip_s3_key = record.get('clip_filepath')

    keyframe_url = None
    video_url = None

    # Ensure CLOUDFRONT_DOMAIN ends with exactly one slash if needed, or handle keys better
    # Assuming CLOUDFRONT_DOMAIN does NOT end with / and keys MAY start with /
    cdn_base = f"https://{CLOUDFRONT_DOMAIN.strip('/')}"

    if CLOUDFRONT_DOMAIN and keyframe_s3_key:
        keyframe_url = f"{cdn_base}/{keyframe_s3_key.lstrip('/')}"
    elif keyframe_s3_key: # Key exists but no domain
         log.warning(f"Missing CloudFront domain? Could not construct keyframe_url for clip: {clip_id}")

    if CLOUDFRONT_DOMAIN and clip_s3_key:
        video_url = f"{cdn_base}/{clip_s3_key.lstrip('/')}"
    elif clip_s3_key: # Key exists but no domain
         log.warning(f"Missing CloudFront domain? Could not construct video_url for clip: {clip_id}")

    return {
        "clip_id": clip_id, # String identifier
        "title": title,
        "keyframe_url": keyframe_url,
        "video_url": video_url,
        "clip_db_id": record.get('id'), # Include DB ID
        # Include other fields passed in the record if needed by the template directly
        "source_video_id": record.get('source_video_id'),
        "source_title": record.get('source_title'),
        "start_time": record.get('start_time_seconds'),
        "end_time": record.get('end_time_seconds'),
        "can_merge_next": record.get('can_merge_next'), # Passed from review query
         "score": record.get('similarity_score') # Passed from similarity query
    }

async def fetch_available_embedding_options(conn: asyncpg.Connection):
    """Fetches distinct model_name/generation_strategy pairs from embeddings using asyncpg."""
    options = []
    try:
        # Use the asyncpg connection passed in
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
             log.warning("No distinct embedding options found in the database.")
    except UndefinedTableError:
         log.error("ERROR: 'embeddings' table not found. Cannot fetch embedding options.")
    except KeyError as e:
         log.error(f"ERROR: Missing expected column in embeddings table: {e}", exc_info=True)
    except Exception as e:
        log.error(f"Error fetching available embedding options: {e}", exc_info=True)
    return options

def parse_embedding_data(embedding_data, clip_id, clip_db_id):
    """Parses embedding data from DB into a list or raises ValueError."""
    query_embedding_vector = None
    try:
        if isinstance(embedding_data, (list, tuple)):
            query_embedding_vector = list(embedding_data)
        elif isinstance(embedding_data, str) and embedding_data.startswith('[') and embedding_data.endswith(']'):
            query_embedding_vector = ast.literal_eval(embedding_data) # Safely evaluate string list
        elif embedding_data and not isinstance(embedding_data, (str, list, tuple)):
            # Attempt conversion if asyncpg returns a specialized type
            try:
                query_embedding_vector = list(embedding_data)
                log.debug(f"Converted embedding data type {type(embedding_data)} to list for clip {clip_id}.")
            except TypeError:
                raise TypeError(f"Unexpected embedding data type: {type(embedding_data)}. Cannot convert to list.")
        else:
            raise ValueError(f"Invalid or empty embedding data: {embedding_data}")

        if not isinstance(query_embedding_vector, list):
            raise ValueError("Processed embedding is not a list.")

        return query_embedding_vector

    except (ValueError, SyntaxError, TypeError) as parse_error:
        log.error(f"ERROR parsing/handling embedding for clip {clip_id} (DB ID: {clip_db_id}): {parse_error}", exc_info=True)
        log.error(f"Received type: {type(embedding_data)}, value (partial): {str(embedding_data)[:100]}...")
        # Re-raise a specific error for the route handler
        raise ValueError(f"Failed to process embedding data for query clip '{clip_id}'. Data format issue.")
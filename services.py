import ast
import asyncpg
from fastapi import Request
from asyncpg.exceptions import UndefinedTableError
import re # Import re for sanitizing

from config import CLOUDFRONT_DOMAIN, log

# This file contains helper functions or business logic used by API routes

def format_clip_data(record, request: Request):
    """
    Formats a database record (or dictionary-like object) into a
    dictionary suitable for the template context, generating CloudFront URLs.
    Expects 'representative_keyframe_s3_key' for the keyframe URL.
    """
    if not record:
        return None

    # Use .get() for safer access in case columns are missing unexpectedly
    # Use clip_identifier as the primary ID displayed/used in URLs
    clip_identifier = record.get('clip_identifier')
    if not clip_identifier:
         # Fallback if identifier is missing (shouldn't happen for valid records)
         db_id = record.get('id', 'unknown_db_id')
         clip_identifier = f"clip_{db_id}"
         log.warning(f"Missing 'clip_identifier' in record with DB ID {db_id}. Using fallback '{clip_identifier}'.")

    # Generate a simple title from the identifier
    title = clip_identifier.replace("_", " ").replace("-", " ").title()
    # Sanitize title further if needed, e.g., remove common prefixes/suffixes
    title = re.sub(r'^(Clip|Scene)\s?', '', title).strip()


    # --- Get S3 Keys ---
    # *** UPDATED: Fetch the representative keyframe key ***
    representative_keyframe_s3_key = record.get('representative_keyframe_s3_key')
    clip_s3_key = record.get('clip_filepath') # Video clip path remains the same

    keyframe_url = None
    video_url = None

    # --- Construct CDN URLs ---
    # Ensure CLOUDFRONT_DOMAIN does NOT end with / and keys MAY start with /
    if CLOUDFRONT_DOMAIN:
        cdn_base = f"https://{CLOUDFRONT_DOMAIN.strip('/')}"

        if representative_keyframe_s3_key:
            keyframe_url = f"{cdn_base}/{representative_keyframe_s3_key.lstrip('/')}"
        else:
             # Log only if the key was expected but missing
             # (e.g., if record is supposed to represent a fully processed clip)
             # Don't log excessively if this function is used for partially processed data.
             log.debug(f"No 'representative_keyframe_s3_key' found in record for clip: {clip_identifier}")

        if clip_s3_key:
            video_url = f"{cdn_base}/{clip_s3_key.lstrip('/')}"
        else:
             log.debug(f"No 'clip_filepath' found in record for clip: {clip_identifier}")

    else:
         # Log warning if CDN domain is missing but keys are present
         if representative_keyframe_s3_key or clip_s3_key:
              log.warning(f"CLOUDFRONT_DOMAIN not configured. Cannot construct CDN URLs for clip: {clip_identifier}")


    # --- Assemble Result Dictionary ---
    formatted_data = {
        "clip_id": clip_identifier, # Consistent: Use identifier for display/linking
        "title": title,
        "keyframe_url": keyframe_url, # Can be None if key/domain missing
        "video_url": video_url, # Can be None if key/domain missing
        "clip_db_id": record.get('id'), # Include DB ID for reference
        "source_video_id": record.get('source_video_id'), # Optional, pass through
        "start_time": record.get('start_time_seconds'), # Optional, pass through
        "end_time": record.get('end_time_seconds'), # Optional, pass through
    }

    # Add score only if present in the record (from similarity search)
    similarity_score = record.get('similarity_score')
    if similarity_score is not None:
        try:
            formatted_data["score"] = float(similarity_score)
        except (ValueError, TypeError):
             log.warning(f"Could not convert similarity score '{similarity_score}' to float for clip {clip_identifier}")
             formatted_data["score"] = 0.0 # Default or None?

    # Add other potential fields if needed
    # e.g., "can_merge_next": record.get('can_merge_next') # Passed from review query

    return formatted_data


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
         # Return empty list, the route handler will display an error
    except Exception as e:
        log.error(f"Error fetching available embedding options: {e}", exc_info=True)
        # Return empty list on other errors as well
    return options


def parse_embedding_data(embedding_data, clip_identifier, clip_db_id):
    """
    Parses embedding data (string, list, or other iterable) from DB into a list
    of floats, or raises ValueError if parsing fails.
    """
    query_embedding_vector = None
    data_type = type(embedding_data).__name__
    log.debug(f"Parsing embedding data for clip {clip_identifier} (DB ID: {clip_db_id}), received type: {data_type}")

    try:
        if isinstance(embedding_data, str):
            # Handle string format like "[1.0, 2.0, ...]"
            if embedding_data.startswith('[') and embedding_data.endswith(']'):
                # Use ast.literal_eval for safe evaluation of list-like strings
                evaluated_data = ast.literal_eval(embedding_data)
                if isinstance(evaluated_data, list):
                    # Ensure all elements are numbers (float/int)
                    if all(isinstance(x, (float, int)) for x in evaluated_data):
                        query_embedding_vector = [float(x) for x in evaluated_data]
                    else:
                        raise ValueError("String list contains non-numeric elements.")
                else:
                    raise ValueError("String data did not evaluate to a list.")
            else:
                raise ValueError("String data is not in the expected '[...]' format.")
        elif isinstance(embedding_data, (list, tuple)):
            # Handle data already as list/tuple
             if all(isinstance(x, (float, int)) for x in embedding_data):
                 query_embedding_vector = [float(x) for x in embedding_data]
             else:
                 raise ValueError("List/tuple contains non-numeric elements.")
        elif embedding_data is None:
             raise ValueError("Received None for embedding data.")
        else:
            # Attempt conversion for other types (e.g., asyncpg array types)
            try:
                temp_list = list(embedding_data)
                if all(isinstance(x, (float, int)) for x in temp_list):
                    query_embedding_vector = [float(x) for x in temp_list]
                    log.debug(f"Successfully converted embedding data type {data_type} to list of floats.")
                else:
                    raise ValueError(f"Converted list from type {data_type} contains non-numeric elements.")
            except TypeError:
                raise TypeError(f"Unexpected embedding data type: {data_type}. Cannot convert to list.")

        # Final check if conversion resulted in a list
        if not isinstance(query_embedding_vector, list):
            # This should be caught by earlier checks, but as a safeguard:
            raise ValueError("Processed embedding data is not a list.")
        if not query_embedding_vector:
             raise ValueError("Embedding data resulted in an empty list.")

        return query_embedding_vector

    except (ValueError, SyntaxError, TypeError) as parse_error:
        log.error(f"ERROR parsing/handling embedding for clip {clip_identifier} (DB ID: {clip_db_id}): {parse_error}", exc_info=False) # Less noisy traceback usually
        log.debug(f"Received type: {data_type}, value (partial): {str(embedding_data)[:100]}...")
        # Re-raise a specific error for the route handler
        raise ValueError(f"Failed to process embedding data for query clip '{clip_identifier}'. Data format issue or invalid content.")
    except Exception as e:
        # Catch any other unexpected error during parsing
        log.error(f"Unexpected ERROR parsing embedding for clip {clip_identifier} (DB ID: {clip_db_id}): {e}", exc_info=True)
        raise ValueError(f"Unexpected error processing embedding data for query clip '{clip_identifier}'.")
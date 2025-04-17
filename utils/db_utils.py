import os
import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor # To get results as dictionaries
from dotenv import load_dotenv
from prefect import get_run_logger # Import Prefect logger

# Load environment variables from .env file in the parent directory
# Adjust path if db_utils.py is not directly under 'utils' which is under project root
try:
    dotenv_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env')
    if os.path.exists(dotenv_path):
        load_dotenv(dotenv_path=dotenv_path)
    else:
        print("Warning: .env file not found at expected location:", dotenv_path)
        # Attempt loading from current dir as fallback for some environments
        load_dotenv()
except Exception as e:
    print(f"Error loading .env file: {e}")


DATABASE_URL = os.getenv("DATABASE_URL")
MEDIA_BASE_DIR = os.getenv("MEDIA_BASE_DIR") # Used by keyframe task, keep getter


if not DATABASE_URL:
    raise ValueError("DATABASE_URL not found in environment variables. Check your .env file or environment setup.")

def get_db_connection(cursor_factory=None):
    """
    Establishes a connection to the PostgreSQL database.
    Optionally accepts a cursor_factory (e.g., RealDictCursor).
    """
    logger = get_run_logger() # Get logger instance
    try:
        conn = psycopg2.connect(DATABASE_URL, cursor_factory=cursor_factory)
        # logger.debug("Database connection established.") # Optional debug log
        return conn
    except psycopg2.OperationalError as e:
        logger.error(f"Could not connect to database using DATABASE_URL.")
        logger.error(f"Check connection details and ensure the database server is running.")
        logger.error(f"Error details: {e}", exc_info=True)
        raise # Re-raise the exception to halt execution if connection fails
    except Exception as e:
        logger.error(f"An unexpected error occurred during DB connection: {e}", exc_info=True)
        raise

def get_media_base_dir():
    """Gets the media base directory from environment variables."""
    logger = get_run_logger()
    media_dir = MEDIA_BASE_DIR # Use variable loaded at module level
    if not media_dir:
        logger.error("MEDIA_BASE_DIR not found in environment variables. Check your .env file or environment setup.")
        raise ValueError("MEDIA_BASE_DIR not found in environment variables.")
    abs_media_dir = os.path.abspath(media_dir)
    if not os.path.isdir(abs_media_dir):
         logger.error(f"MEDIA_BASE_DIR '{abs_media_dir}' does not exist or is not a directory.")
         raise ValueError(f"MEDIA_BASE_DIR '{abs_media_dir}' does not exist or is not a directory.")
    # logger.debug(f"Using MEDIA_BASE_DIR: {abs_media_dir}")
    return abs_media_dir

def get_items_by_state(table: str, state: str) -> list[int]:
    """Helper to query DB for IDs in a specific state."""
    logger = get_run_logger()
    items = []
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            # Use sql.Identifier for safe table name interpolation
            query = sql.SQL("SELECT id FROM {} WHERE ingest_state = %s ORDER BY id ASC").format(sql.Identifier(table))
            cur.execute(query, (state,))
            items = [row[0] for row in cur.fetchall()]
            # logger.debug(f"Found {len(items)} items in table '{table}' with state '{state}'.")
    except (Exception, psycopg2.DatabaseError) as e:
        logger.error(f"DB Error querying table '{table}' for state '{state}': {e}", exc_info=True)
        # Consider how errors here should be handled by the caller (e.g., initiator flow)
    finally:
        if conn:
            conn.close()
    return items

def get_source_input_from_db(source_video_id: int) -> str | None:
    """Fetches the original_url for a given source_video ID."""
    logger = get_run_logger()
    conn = None
    input_source = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            # Fetch original_url - assuming this holds the URL or initial path
            cur.execute("SELECT original_url FROM source_videos WHERE id = %s", (source_video_id,))
            result = cur.fetchone()
            if result and result[0]:
                input_source = result[0]
                # logger.debug(f"Found original_url for source_video_id {source_video_id}")
            else:
                logger.warning(f"No 'original_url' found in DB for source_video_id {source_video_id}")

    except (Exception, psycopg2.DatabaseError) as error:
         logger.error(f"DB error fetching input source for ID {source_video_id}: {error}", exc_info=True)
    finally:
        if conn:
            conn.close()
    return input_source

# --- New Helper Functions for Initiator Flow ---

def get_pending_merge_pairs() -> list[tuple[int, int]]:
    """
    Finds pairs of clips ready for merging.
    Looks for clip c1 in 'pending_merge_with_next' and its direct successor c2
    (same source, next by start_frame) in 'pending_review' or 'review_skipped'.

    Returns:
        list[tuple[int, int]]: List of (clip1_id, clip2_id) tuples.
    """
    logger = get_run_logger()
    merge_pairs = []
    conn = None
    logger.debug("Querying for pending merge pairs...")
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            # Use LEAD window function to find the next clip's ID and state within the same source video
            # Filter for rows where the current clip is pending merge and the next clip is in a valid target state
            query = sql.SQL("""
                WITH NumberedClips AS (
                    SELECT
                        id,
                        source_video_id,
                        start_frame,
                        ingest_state,
                        LEAD(id) OVER w AS next_clip_id,
                        LEAD(ingest_state) OVER w AS next_clip_state
                    FROM clips
                    WINDOW w AS (PARTITION BY source_video_id ORDER BY start_frame ASC)
                )
                SELECT
                    nc.id AS clip1_id,
                    nc.next_clip_id AS clip2_id
                FROM NumberedClips nc
                WHERE nc.ingest_state = %s -- 'pending_merge_with_next'
                  AND nc.next_clip_id IS NOT NULL
                  AND nc.next_clip_state = ANY(%s); -- ['pending_review', 'review_skipped']
            """)
            valid_next_states = ['pending_review', 'review_skipped']
            cur.execute(query, ('pending_merge_with_next', valid_next_states))
            results = cur.fetchall()
            merge_pairs = [(row[0], row[1]) for row in results]
            if merge_pairs:
                logger.info(f"Found {len(merge_pairs)} potential merge pairs.")
            # else:
                # logger.debug("No pending merge pairs found.")

    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f"DB error fetching pending merge pairs: {error}", exc_info=True)
        # Return empty list on error so flow doesn't crash, but logs the issue
    finally:
        if conn:
            conn.close()
    return merge_pairs


def get_pending_split_jobs() -> list[tuple[int, float]]:
    """
    Finds clips marked for splitting and extracts their split time.

    Returns:
        list[tuple[int, float]]: List of (clip_id, split_at_seconds_float) tuples.
                                 Returns empty list if error or no jobs found.
    """
    logger = get_run_logger()
    split_jobs = []
    conn = None
    logger.debug("Querying for pending split jobs...")
    try:
        # Use RealDictCursor to easily access JSON field by name
        conn = get_db_connection(cursor_factory=RealDictCursor)
        with conn.cursor() as cur:
            # Query for clips in 'pending_split' state
            # Extract the 'split_at_seconds' value from the JSONB column
            # Ensure the value is actually a number using a JSON path check if needed
            # NOTE: ->> extracts as TEXT, so needs casting
            query = sql.SQL("""
                SELECT
                    id,
                    processing_metadata ->> 'split_at_seconds' AS split_time_text
                FROM clips
                WHERE ingest_state = %s
                  AND processing_metadata IS NOT NULL
                  AND jsonb_typeof(processing_metadata -> 'split_at_seconds') = 'number';
            """)
            # AND processing_metadata ->> 'split_at_seconds' IS NOT NULL; might be redundant with jsonb_typeof check

            cur.execute(query, ('pending_split',))
            results = cur.fetchall()

            valid_jobs = []
            for row in results:
                try:
                    split_time_float = float(row['split_time_text'])
                    if split_time_float > 0: # Basic validation
                         valid_jobs.append((row['id'], split_time_float))
                    else:
                         logger.warning(f"Invalid split time (<= 0) found for clip {row['id']}: {split_time_float}. Skipping.")
                except (ValueError, TypeError) as conv_err:
                     logger.error(f"Could not convert split time '{row['split_time_text']}' to float for clip {row['id']}: {conv_err}. Skipping.")

            split_jobs = valid_jobs
            if split_jobs:
                logger.info(f"Found {len(split_jobs)} valid split jobs.")
            # else:
            #     logger.debug("No pending split jobs found.")

    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f"DB error fetching pending split jobs: {error}", exc_info=True)
        # Return empty list on error
    finally:
        if conn:
            conn.close()
    return split_jobs
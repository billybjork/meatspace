import os
import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor # To get results as dictionaries
from psycopg2 import pool # Import connection pooling
from dotenv import load_dotenv
from prefect import get_run_logger # Import Prefect logger
import time # For retry logic

try:
    dotenv_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env')
    if os.path.exists(dotenv_path):
        load_dotenv(dotenv_path=dotenv_path)
    else:
        print("Warning: .env file not found at expected location:", dotenv_path)
        load_dotenv()
except Exception as e:
    print(f"Error loading .env file: {e}")


DATABASE_URL = os.getenv("DATABASE_URL")
MEDIA_BASE_DIR = os.getenv("MEDIA_BASE_DIR")

# --- Connection Pooling for psycopg2 (Crucial for Prefect Tasks) ---
db_pool = None
MIN_POOL_CONN = int(os.getenv("PREFECT_DB_MIN_POOL", 1)) # Allow tuning pool size
MAX_POOL_CONN = int(os.getenv("PREFECT_DB_MAX_POOL", 5)) # Allow tuning pool size

def initialize_db_pool():
    """Initializes the psycopg2 connection pool."""
    global db_pool
    logger = get_run_logger()
    if db_pool is None:
        if not DATABASE_URL:
            logger.error("DATABASE_URL not found, cannot initialize DB pool.")
            raise ValueError("DATABASE_URL not found in environment variables.")
        try:
            logger.info(f"Initializing psycopg2 connection pool (Min: {MIN_POOL_CONN}, Max: {MAX_POOL_CONN})...")
            # Using ThreadedConnectionPool as Prefect tasks might run in threads
            db_pool = psycopg2.pool.ThreadedConnectionPool(MIN_POOL_CONN, MAX_POOL_CONN, dsn=DATABASE_URL)
            # Test connection
            conn = db_pool.getconn()
            logger.info("DB Pool initialized successfully. Test connection acquired.")
            db_pool.putconn(conn)
        except (Exception, psycopg2.DatabaseError) as e:
            logger.error(f"Failed to initialize psycopg2 connection pool: {e}", exc_info=True)
            db_pool = None # Ensure pool is None if init failed
            raise # Re-raise to prevent silent failure

def get_db_connection(cursor_factory=None):
    """Gets a connection from the pool. Retries briefly on failure."""
    logger = get_run_logger()
    if db_pool is None:
        logger.warning("DB Pool not initialized. Attempting to initialize now.")
        initialize_db_pool() # Attempt lazy initialization
        if db_pool is None: # Check again after attempt
             logger.error("DB Pool initialization failed. Cannot get connection.")
             raise ConnectionError("Database connection pool is not available.") # Use a more specific error

    conn = None
    retries = 3
    delay = 1 # seconds
    for attempt in range(retries):
        try:
            conn = db_pool.getconn()
            if cursor_factory:
                conn.cursor_factory = cursor_factory # Apply factory if needed *after* getting conn
            # logger.debug(f"Acquired DB connection {id(conn)} from pool.")
            return conn # Success
        except (psycopg2.pool.PoolError, psycopg2.OperationalError) as e:
            logger.warning(f"Attempt {attempt + 1}/{retries}: Failed to get connection from pool: {e}. Retrying in {delay}s...")
            if conn: # If getconn failed after partial acquisition? Unlikely but safe.
                 db_pool.putconn(conn, close=True) # Close potentially bad connection
                 conn = None
            time.sleep(delay)
            delay *= 2 # Exponential backoff
        except Exception as e:
             logger.error(f"Unexpected error getting connection from pool: {e}", exc_info=True)
             raise # Re-raise unexpected errors immediately

    logger.error(f"Failed to get connection from pool after {retries} attempts.")
    raise ConnectionError(f"Could not get database connection from pool after {retries} attempts.")


def release_db_connection(conn):
    """Releases a connection back to the pool."""
    logger = get_run_logger()
    if db_pool and conn:
        try:
            # Reset cursor factory to default before putting back? Optional.
            conn.cursor_factory = None
            db_pool.putconn(conn)
            # logger.debug(f"Released DB connection {id(conn)} back to pool.")
        except (Exception, psycopg2.DatabaseError) as e:
            logger.error(f"Error releasing DB connection {id(conn)} back to pool: {e}", exc_info=True)
            # If putting back fails, maybe close it to prevent reuse?
            try:
                db_pool.putconn(conn, close=True)
                logger.warning(f"Closed connection {id(conn)} instead due to release error.")
            except Exception as close_err:
                 logger.error(f"Failed to close problematic connection {id(conn)}: {close_err}")

# --- Query Logic ---

def get_items_by_state(table: str, state: str) -> list[int]:
    """DEPRECATED (potentially): Use get_items_for_processing instead to avoid race conditions."""
    logger = get_run_logger()
    logger.warning(f"Calling deprecated get_items_by_state for table '{table}', state '{state}'. Consider using get_items_for_processing.")
    # Keep original logic for now if needed elsewhere, but log warning
    items = []
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            query = sql.SQL("SELECT id FROM {} WHERE ingest_state = %s ORDER BY id ASC").format(sql.Identifier(table))
            cur.execute(query, (state,))
            items = [row[0] for row in cur.fetchall()]
    except Exception as e:
        logger.error(f"DB Error in get_items_by_state ('{table}', '{state}'): {e}", exc_info=True)
    finally:
        if conn:
            release_db_connection(conn) # Use pool release
    return items

def get_items_for_processing(table: str, ready_state: str, processing_states: list[str]) -> list[int]:
    """
    Helper to query DB for IDs in a 'ready' state, excluding those already in 'processing' states.
    """
    logger = get_run_logger()
    items = []
    conn = None
    # logger.debug(f"Querying table '{table}' for state '{ready_state}', excluding {processing_states}")
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            # Use sql.Identifier for table name, IN operator for ready state, NOT IN for processing states
            # Ensure processing_states is not empty to avoid SQL errors with empty NOT IN ()
            if not processing_states:
                 query = sql.SQL("SELECT id FROM {} WHERE ingest_state = %s ORDER BY id ASC").format(sql.Identifier(table))
                 cur.execute(query, (ready_state,))
            else:
                query = sql.SQL("SELECT id FROM {} WHERE ingest_state = %s AND ingest_state NOT IN %s ORDER BY id ASC").format(sql.Identifier(table))
                # Pass processing_states as a tuple for the IN operator
                cur.execute(query, (ready_state, tuple(processing_states)))

            items = [row[0] for row in cur.fetchall()]
            # logger.debug(f"Found {len(items)} items in table '{table}' ready for processing (state '{ready_state}', excluding {processing_states}).")
    except (Exception, psycopg2.DatabaseError) as e:
        logger.error(f"DB Error querying table '{table}' for processing (state '{ready_state}', excluding {processing_states}): {e}", exc_info=True)
    finally:
        if conn:
            release_db_connection(conn) # Use pool release
    return items


def get_source_input_from_db(source_video_id: int) -> str | None:
    """Fetches the original_url for a given source_video ID."""
    logger = get_run_logger()
    conn = None
    input_source = None
    try:
        conn = get_db_connection() # Get from pool
        with conn.cursor() as cur:
            cur.execute("SELECT original_url FROM source_videos WHERE id = %s", (source_video_id,))
            result = cur.fetchone()
            if result and result[0]:
                input_source = result[0]
            else:
                logger.warning(f"No 'original_url' found in DB for source_video_id {source_video_id}")
    except (Exception, psycopg2.DatabaseError) as error:
         logger.error(f"DB error fetching input source for ID {source_video_id}: {error}", exc_info=True)
    finally:
        if conn:
            release_db_connection(conn) # Release back to pool
    return input_source


def get_pending_merge_pairs() -> list[tuple[int, int]]:
    """
    Finds pairs of clips ready for merging. (Updated to use pool)
    """
    logger = get_run_logger()
    merge_pairs = []
    conn = None
    # logger.debug("Querying for pending merge pairs...") # Reduce noise
    try:
        conn = get_db_connection() # Get from pool
        with conn.cursor() as cur:
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
                  -- Add check to ensure next clip isn't already merging?
                  -- AND nc.next_clip_state NOT IN ('merging') -- Might be needed if merge task takes time
            """)
            valid_next_states = ['pending_review', 'review_skipped']
            cur.execute(query, ('pending_merge_with_next', valid_next_states))
            results = cur.fetchall()
            merge_pairs = [(row[0], row[1]) for row in results]
            if merge_pairs:
                logger.info(f"Found {len(merge_pairs)} potential merge pairs.")

    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f"DB error fetching pending merge pairs: {error}", exc_info=True)
    finally:
        if conn:
            release_db_connection(conn) # Release back to pool
    return merge_pairs


def get_pending_split_jobs() -> list[tuple[int, int]]:
    """
    Finds clips marked for splitting and extracts their split time. (Updated to use pool)
    Uses the frame number from metadata instead of seconds.
    Returns:
        list[tuple[int, int]]: List of (clip_id, split_at_frame_int) tuples.
    """
    logger = get_run_logger()
    split_jobs = []
    conn = None
    # logger.debug("Querying for pending split jobs...") # Reduce noise
    try:
        # Use RealDictCursor to easily access JSON field by name
        conn = get_db_connection(cursor_factory=RealDictCursor) # Get from pool
        with conn.cursor() as cur:
            # Query for clips in 'pending_split' state
            # Extract the 'split_request_at_frame' value from the JSONB column
            # Ensure the value is actually a number
            query = sql.SQL("""
                SELECT
                    id,
                    processing_metadata ->> 'split_request_at_frame' AS split_frame_text
                FROM clips
                WHERE ingest_state = %s
                  AND processing_metadata IS NOT NULL
                  AND jsonb_typeof(processing_metadata -> 'split_request_at_frame') = 'number';
            """)
            cur.execute(query, ('pending_split',))
            results = cur.fetchall()

            valid_jobs = []
            for row in results:
                try:
                    split_frame_int = int(row['split_frame_text']) # Now expect integer frame number
                    if split_frame_int >= 0: # Frame numbers are 0-based
                         valid_jobs.append((row['id'], split_frame_int))
                    else:
                         logger.warning(f"Invalid split frame (< 0) found for clip {row['id']}: {split_frame_int}. Skipping.")
                except (ValueError, TypeError, KeyError) as conv_err:
                     # Handle potential KeyError if 'split_frame_text' is missing despite query filters (unlikely)
                     err_val = row.get('split_frame_text', '<missing>')
                     logger.error(f"Could not convert split frame '{err_val}' to int for clip {row['id']}: {conv_err}. Skipping.")

            split_jobs = valid_jobs
            if split_jobs:
                logger.info(f"Found {len(split_jobs)} valid split jobs based on frame number.")

    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f"DB error fetching pending split jobs: {error}", exc_info=True)
    finally:
        if conn:
            # Important: Since we set cursor_factory, reset before releasing if needed,
            # though releasing logic handles this. Just ensure release is called.
            release_db_connection(conn) # Release back to pool
    return split_jobs

# Close the pool, e.g., on application shutdown if used outside Prefect
def close_db_pool():
    global db_pool
    logger = get_run_logger()
    if db_pool:
        logger.info("Closing psycopg2 connection pool...")
        db_pool.closeall()
        db_pool = None
        logger.info("psycopg2 connection pool closed.")
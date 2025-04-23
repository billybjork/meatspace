import os
import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
from psycopg2 import pool
from dotenv import load_dotenv
from prefect import get_run_logger
import time

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

# --- Connection Pooling for psycopg2 (crucial for Prefect Tasks) ---
db_pool = None
MIN_POOL_CONN = int(os.getenv("PREFECT_DB_MIN_POOL", 1)) # Allow tuning pool size
MAX_POOL_CONN = int(os.getenv("PREFECT_DB_MAX_POOL", 15)) # Allow tuning pool size


def initialize_db_pool():
    """Initializes the psycopg2 connection pool."""
    global db_pool
    # Use Prefect logger if available, otherwise basic print
    try:
        logger = get_run_logger()
        log_func = logger.info
        log_error = logger.error
    except: # Fallback if run outside Prefect context
        log_func = print
        log_error = print

    if db_pool is None:
        if not DATABASE_URL:
            log_error("DATABASE_URL not found, cannot initialize DB pool.")
            raise ValueError("DATABASE_URL not found in environment variables.")
        try:
            log_func(f"Initializing psycopg2 connection pool (Min: {MIN_POOL_CONN}, Max: {MAX_POOL_CONN})...")
            db_pool = psycopg2.pool.ThreadedConnectionPool(MIN_POOL_CONN, MAX_POOL_CONN, dsn=DATABASE_URL)
            conn = db_pool.getconn()
            log_func("DB Pool initialized successfully. Test connection acquired.")
            db_pool.putconn(conn)
        except (Exception, psycopg2.DatabaseError) as e:
            log_error(f"Failed to initialize psycopg2 connection pool: {e}") # No exc_info outside logger
            db_pool = None
            raise


def get_db_connection(cursor_factory=None):
    """Gets a connection from the pool. Retries briefly on failure."""
    # Use Prefect logger if available, otherwise basic print
    try:
        logger = get_run_logger()
        log_func = logger.info
        log_warning = logger.warning
        log_error = logger.error
    except: # Fallback if run outside Prefect context
        log_func = print
        log_warning = print
        log_error = print

    if db_pool is None:
        log_warning("DB Pool not initialized. Attempting to initialize now.")
        initialize_db_pool() # Attempt lazy initialization
        if db_pool is None: # Check again after attempt
             log_error("DB Pool initialization failed. Cannot get connection.")
             raise ConnectionError("Database connection pool is not available.")

    conn = None
    retries = 3
    delay = 1
    for attempt in range(retries):
        try:
            conn = db_pool.getconn()
            if cursor_factory:
                conn.cursor_factory = cursor_factory
            # logger.debug(f"Acquired DB connection {id(conn)} from pool.")
            return conn # Success
        except (psycopg2.pool.PoolError, psycopg2.OperationalError) as e:
            log_warning(f"Attempt {attempt + 1}/{retries}: Failed to get connection from pool: {e}. Retrying in {delay}s...")
            if conn:
                 db_pool.putconn(conn, close=True)
                 conn = None
            time.sleep(delay)
            delay *= 2
        except Exception as e:
             log_error(f"Unexpected error getting connection from pool: {e}")
             raise

    log_error(f"Failed to get connection from pool after {retries} attempts.")
    raise ConnectionError(f"Could not get database connection from pool after {retries} attempts.")


def release_db_connection(conn):
    """Releases a connection back to the pool."""
    # Use Prefect logger if available, otherwise basic print
    try:
        logger = get_run_logger()
        log_func = logger.info
        log_warning = logger.warning
        log_error = logger.error
    except: # Fallback if run outside Prefect context
        log_func = print
        log_warning = print
        log_error = print

    if db_pool and conn:
        try:
            conn.cursor_factory = None
            db_pool.putconn(conn)
            # logger.debug(f"Released DB connection {id(conn)} back to pool.")
        except (Exception, psycopg2.DatabaseError) as e:
            log_error(f"Error releasing DB connection {id(conn)} back to pool: {e}")
            try:
                db_pool.putconn(conn, close=True)
                log_warning(f"Closed connection {id(conn)} instead due to release error.")
            except Exception as close_err:
                 log_error(f"Failed to close problematic connection {id(conn)}: {close_err}")


# --- Query Logic ---

def get_items_for_processing(table: str, ready_state: str, processing_states: list[str]) -> list[int]:
    """
    Helper to query DB for IDs in a 'ready' state, excluding those already in 'processing' states.
    """
    logger = get_run_logger()
    items = []
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            if not processing_states:
                 query = sql.SQL("SELECT id FROM {} WHERE ingest_state = %s ORDER BY id ASC").format(sql.Identifier(table))
                 cur.execute(query, (ready_state,))
            else:
                query = sql.SQL("SELECT id FROM {} WHERE ingest_state = %s AND ingest_state NOT IN %s ORDER BY id ASC").format(sql.Identifier(table))
                cur.execute(query, (ready_state, tuple(processing_states)))
            items = [row[0] for row in cur.fetchall()]
    except (Exception, psycopg2.DatabaseError) as e:
        logger.error(f"DB Error querying table '{table}' for processing (state '{ready_state}', excluding {processing_states}): {e}", exc_info=True)
    finally:
        if conn:
            release_db_connection(conn)
    return items


def get_source_input_from_db(source_video_id: int) -> str | None:
    """Fetches the original_url for a given source_video ID."""
    logger = get_run_logger()
    conn = None
    input_source = None
    try:
        conn = get_db_connection()
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
            release_db_connection(conn)
    return input_source


def get_pending_merge_pairs() -> list[tuple[int, int]]:
    """
    Finds pairs of clips ready for backward merging.
    """
    logger = get_run_logger()
    merge_pairs = []
    conn = None
    try:
        conn = get_db_connection(cursor_factory=RealDictCursor)
        with conn.cursor() as cur:
            query = sql.SQL("""
                SELECT
                    t.id AS target_id,
                    t.processing_metadata ->> 'merge_source_clip_id' AS source_id_text,
                    s.id AS actual_source_id,
                    s.ingest_state AS source_state
                FROM clips t
                LEFT JOIN clips s ON (t.processing_metadata ->> 'merge_source_clip_id')::int = s.id
                WHERE t.ingest_state = %s
                  AND t.processing_metadata IS NOT NULL
                  AND jsonb_typeof(t.processing_metadata -> 'merge_source_clip_id') = 'number';
            """)
            cur.execute(query, ('pending_merge_target',))
            results = cur.fetchall()

            valid_pairs = []
            processed_targets = set()
            expected_source_state = 'marked_for_merge_into_previous'

            for row in results:
                target_id = row['target_id']
                if target_id in processed_targets: continue
                source_id_text = row['source_id_text']
                actual_source_id = row['actual_source_id']
                source_state = row['source_state']
                source_id = None
                try:
                    source_id = int(source_id_text)
                except (ValueError, TypeError):
                    logger.warning(f"Invalid 'merge_source_clip_id' ({source_id_text}) for target {target_id}. Skipping.")
                    continue
                if actual_source_id is None:
                     logger.warning(f"Target {target_id} points to missing source {source_id}. Skipping.")
                     continue
                if actual_source_id != source_id:
                     logger.warning(f"Metadata/Join mismatch for target {target_id} (Meta:{source_id}, Joined:{actual_source_id}). Skipping.")
                     continue
                if source_state == expected_source_state:
                    valid_pairs.append((target_id, source_id))
                    processed_targets.add(target_id)
                    logger.debug(f"Found valid backward merge pair: Target={target_id}, Source={source_id}")
                else:
                    logger.warning(f"Target {target_id} points to source {source_id} with wrong state ('{source_state}', expected '{expected_source_state}'). Skipping.")

            merge_pairs = valid_pairs
            if merge_pairs: logger.info(f"Found {len(merge_pairs)} valid backward merge pairs.")

    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f"DB error fetching pending backward merge pairs: {error}", exc_info=True)
    finally:
        if conn: release_db_connection(conn)
    return merge_pairs


def get_pending_split_jobs() -> list[tuple[int, int]]:
    """
    Finds clips marked for splitting and extracts their split frame number.
    """
    logger = get_run_logger()
    split_jobs = []
    conn = None
    try:
        conn = get_db_connection(cursor_factory=RealDictCursor)
        with conn.cursor() as cur:
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
                    split_frame_int = int(row['split_frame_text'])
                    if split_frame_int >= 0:
                         valid_jobs.append((row['id'], split_frame_int))
                    else:
                         logger.warning(f"Invalid split frame (< 0) for clip {row['id']}: {split_frame_int}. Skipping.")
                except (ValueError, TypeError, KeyError) as conv_err:
                     err_val = row.get('split_frame_text', '<missing>')
                     logger.error(f"Could not convert split frame '{err_val}' to int for clip {row['id']}: {conv_err}. Skipping.")

            split_jobs = valid_jobs
            if split_jobs: logger.info(f"Found {len(split_jobs)} valid split jobs based on frame number.")

    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f"DB error fetching pending split jobs: {error}", exc_info=True)
    finally:
        if conn: release_db_connection(conn)
    return split_jobs


# --- For Immediate State Updates ---
def update_clip_state_sync(clip_id: int, new_state: str, error_message: str | None = None) -> bool:
    """
    Synchronously updates the ingest_state and last_error for a single clip.
    Uses the psycopg2 connection pool. Intended for quick updates before triggering longer jobs.

    Args:
        clip_id: The ID of the clip to update.
        new_state: The new ingest_state value.
        error_message: Optional error message to store in last_error.

    Returns:
        True if the update was successful (1 row affected), False otherwise.
    """
    logger = get_run_logger()
    conn = None
    success = False
    log_prefix = f"[Sync Update Clip {clip_id}]"
    try:
        conn = get_db_connection() # Get connection from pool
        with conn.cursor() as cur:
            query = sql.SQL("""
                UPDATE clips
                SET
                    ingest_state = %s,
                    last_error = %s, -- Pass None if no error message
                    updated_at = NOW()
                WHERE id = %s;
            """)
            cur.execute(query, (new_state, error_message, clip_id))
            conn.commit() # Crucial: Commit the transaction

            if cur.rowcount == 1:
                logger.debug(f"{log_prefix} Successfully updated state to '{new_state}'.")
                success = True
            elif cur.rowcount == 0:
                logger.warning(f"{log_prefix} Update to state '{new_state}' affected 0 rows. Clip ID might not exist or already updated.")
                success = False # Or maybe True if 0 rows is acceptable? Depends on logic.
            else:
                # This shouldn't happen with WHERE id = %s, but good to check.
                logger.warning(f"{log_prefix} Update to state '{new_state}' affected {cur.rowcount} rows (expected 1).")
                success = False # Treat unexpected row count as failure

    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f"{log_prefix} DB error updating state to '{new_state}': {error}", exc_info=True)
        if conn:
            try:
                conn.rollback() # Rollback on error
                logger.info(f"{log_prefix} Transaction rolled back due to error.")
            except Exception as rb_err:
                logger.error(f"{log_prefix} Failed to rollback transaction: {rb_err}", exc_info=True)
        success = False
    finally:
        if conn:
            release_db_connection(conn) # Always release the connection
    return success


# Close the pool, e.g., on application shutdown if used outside Prefect
def close_db_pool():
    global db_pool
    # Use Prefect logger if available, otherwise basic print
    try:
        logger = get_run_logger()
        log_func = logger.info
    except: # Fallback if run outside Prefect context
        log_func = print

    if db_pool:
        log_func("Closing psycopg2 connection pool...")
        db_pool.closeall()
        db_pool = None
        log_func("psycopg2 connection pool closed.")
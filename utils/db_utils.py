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
        print(f"Loaded .env file from: {dotenv_path}")
    else:
        print("Warning: .env file not found at expected location:", dotenv_path)
        # Optionally try loading without path if it's discoverable elsewhere
        # load_dotenv()
except Exception as e:
    print(f"Error loading .env file: {e}")


DATABASE_URL = os.getenv("DATABASE_URL")

# --- Connection Pooling for psycopg2 (crucial for Prefect Tasks) ---
db_pool = None
MIN_POOL_CONN = int(os.getenv("PREFECT_DB_MIN_POOL", 2)) # Sensible defaults
MAX_POOL_CONN = int(os.getenv("PREFECT_DB_MAX_POOL", 10))# Sensible defaults


def initialize_db_pool():
    """Initializes the psycopg2 connection pool."""
    global db_pool
    # Use Prefect logger if available, otherwise basic print
    try:
        logger = get_run_logger()
        log_func = logger.info
        log_error = logger.error
    except Exception: # Fallback if run outside Prefect context
        log_func = print
        log_error = print

    if db_pool is None:
        if not DATABASE_URL:
            log_error("DATABASE_URL not found, cannot initialize DB pool.")
            raise ValueError("DATABASE_URL not found in environment variables.")
        try:
            log_func(f"Initializing psycopg2 connection pool (Min: {MIN_POOL_CONN}, Max: {MAX_POOL_CONN})...")
            # Use ThreadedConnectionPool suitable for multi-threaded Prefect workers
            db_pool = psycopg2.pool.ThreadedConnectionPool(MIN_POOL_CONN, MAX_POOL_CONN, dsn=DATABASE_URL)
            # Test connection acquisition
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
        log_func = logger.debug # Less verbose for connection acquisition
        log_warning = logger.warning
        log_error = logger.error
    except Exception: # Fallback if run outside Prefect context
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
    delay = 0.5 # Shorter initial delay
    for attempt in range(retries):
        try:
            conn = db_pool.getconn()
            if conn:
                # Optionally set cursor factory if requested
                if cursor_factory:
                    # Note: setting cursor_factory applies to *all* cursors created from this conn
                    conn.cursor_factory = cursor_factory
                # log_func(f"Acquired DB connection {id(conn)} from pool.")
                return conn # Success
            else:
                 raise psycopg2.pool.PoolError("db_pool.getconn() returned None")

        except (psycopg2.pool.PoolError, psycopg2.OperationalError) as e:
            log_warning(f"Attempt {attempt + 1}/{retries}: Failed to get connection from pool: {e}. Retrying in {delay}s...")
            # Ensure connection is properly released or closed if partially acquired
            if conn:
                 try:
                      db_pool.putconn(conn, close=True) # Close potentially bad connection
                 except Exception as put_err:
                      log_warning(f"Error closing connection during retry: {put_err}")
                 conn = None
            time.sleep(delay)
            delay *= 2 # Exponential backoff
        except Exception as e:
             log_error(f"Unexpected error getting connection from pool: {e}", exc_info=True)
             raise

    log_error(f"Failed to get connection from pool after {retries} attempts.")
    raise ConnectionError(f"Could not get database connection from pool after {retries} attempts.")


def release_db_connection(conn):
    """Releases a connection back to the pool."""
    # Use Prefect logger if available, otherwise basic print
    try:
        logger = get_run_logger()
        log_func = logger.debug
        log_warning = logger.warning
        log_error = logger.error
    except Exception: # Fallback if run outside Prefect context
        log_func = print
        log_warning = print
        log_error = print

    if db_pool and conn:
        try:
            # Reset cursor_factory before putting back (important if changed)
            conn.cursor_factory = None
            db_pool.putconn(conn)
            # log_func(f"Released DB connection {id(conn)} back to pool.")
        except (Exception, psycopg2.DatabaseError) as e:
            log_error(f"Error releasing DB connection {id(conn)} back to pool: {e}", exc_info=True)
            # If releasing fails, try closing it to prevent pool contamination
            try:
                db_pool.putconn(conn, close=True)
                log_warning(f"Closed connection {id(conn)} instead due to release error.")
            except Exception as close_err:
                 log_error(f"Failed to close problematic connection {id(conn)}: {close_err}")


def get_all_pending_work(limit_per_stage: int = 50) -> list[dict]:
    """
    Fetches pending work items across different stages in a single query.

    Args:
        limit_per_stage: The maximum number of items to fetch for each stage.

    Returns:
        A list of dictionaries, where each dictionary has 'id' and 'stage' keys.
        Example: [{'id': 123, 'stage': 'intake'}, {'id': 456, 'stage': 'splice'}, ...]
    """
    logger = get_run_logger()
    work_items = []
    conn = None
    try:
        conn = get_db_connection(cursor_factory=RealDictCursor) # Use RealDictCursor
        with conn.cursor() as cur:
            # Note: This query relies on subsequent state updates in the flow
            # to prevent immediate re-processing. It does *not* include complex
            # locking checks within the SELECT itself for simplicity.
            # Adjust states and tables according to your actual pipeline needs.
            query = sql.SQL("""
                (SELECT id, 'intake' AS stage FROM source_videos WHERE ingest_state = %s ORDER BY id LIMIT %s)
                UNION ALL
                (SELECT id, 'splice' AS stage FROM source_videos WHERE ingest_state = %s ORDER BY id LIMIT %s)
                UNION ALL
                (SELECT id, 'sprite' AS stage FROM clips WHERE ingest_state = %s ORDER BY id LIMIT %s)
                UNION ALL
                (SELECT id, 'post_review' AS stage FROM clips WHERE ingest_state = %s ORDER BY id LIMIT %s)
                UNION ALL
                (SELECT id, 'embedding' AS stage FROM clips WHERE ingest_state = %s ORDER BY id LIMIT %s)
            """)
            # Add more UNION ALL clauses for other states if needed

            params = [
                'new', limit_per_stage,                       # Intake
                'downloaded', limit_per_stage,                # Splice
                'pending_sprite_generation', limit_per_stage, # Sprite
                'review_approved', limit_per_stage,           # Post-Review Trigger
                'keyframed', limit_per_stage,                 # Embedding Only
            ]
            cur.execute(query, params)
            work_items = cur.fetchall() # Fetches list of dicts because of RealDictCursor
            logger.info(f"Found {len(work_items)} total pending work items across stages (limit: {limit_per_stage}/stage).")

    except (Exception, psycopg2.DatabaseError) as e:
        logger.error(f"DB Error querying for all pending work: {e}", exc_info=True)
        work_items = [] # Ensure empty list on error
    finally:
        if conn:
            release_db_connection(conn)
    return work_items


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
    (No changes needed for this function based on the request)
    """
    logger = get_run_logger()
    merge_pairs = []
    conn = None
    try:
        conn = get_db_connection(cursor_factory=RealDictCursor)
        with conn.cursor() as cur:
            # Same query as before
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
    (No changes needed for this function based on the request)
    """
    logger = get_run_logger()
    split_jobs = []
    conn = None
    try:
        conn = get_db_connection(cursor_factory=RealDictCursor)
        with conn.cursor() as cur:
            # Same query as before
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

def _update_state_sync(table_name: str, item_id: int, new_state: str, processing_state: str | None, error_message: str | None = None) -> bool:
    """Internal helper for synchronous state updates."""
    logger = get_run_logger()
    conn = None
    success = False
    log_prefix = f"[Sync Update {table_name.capitalize()} {item_id}]"
    try:
        conn = get_db_connection() # Get connection from pool
        with conn.cursor() as cur:
            # Clear retry count if moving to a processing state
            set_clauses = [
                sql.SQL("ingest_state = %s"),
                sql.SQL("last_error = %s"),
                sql.SQL("updated_at = NOW()")
            ]
            params = [new_state, error_message]

            if new_state == processing_state: # Check if it's the designated processing state
                 set_clauses.append(sql.SQL("retry_count = 0"))

            query = sql.SQL("UPDATE {} SET {} WHERE id = %s;").format(
                sql.Identifier(table_name),
                sql.SQL(', ').join(set_clauses)
            )
            params.append(item_id)

            cur.execute(query, tuple(params))
            conn.commit() # Commit the transaction

            if cur.rowcount == 1:
                logger.debug(f"{log_prefix} Successfully updated state to '{new_state}'.")
                success = True
            elif cur.rowcount == 0:
                logger.warning(f"{log_prefix} Update to state '{new_state}' affected 0 rows. Item ID might not exist or state already updated.")
                success = False # Indicate no update occurred
            else:
                logger.warning(f"{log_prefix} Update to state '{new_state}' affected {cur.rowcount} rows (expected 0 or 1).")
                success = False # Treat unexpected row count as failure

    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f"{log_prefix} DB error updating state to '{new_state}': {error}", exc_info=True)
        if conn:
            try: conn.rollback(); logger.info(f"{log_prefix} Transaction rolled back.")
            except Exception as rb_err: logger.error(f"{log_prefix} Failed rollback: {rb_err}", exc_info=True)
        success = False
    finally:
        if conn:
            release_db_connection(conn)
    return success

def update_clip_state_sync(clip_id: int, new_state: str, error_message: str | None = None) -> bool:
    """Synchronously updates the ingest_state for a clip."""
    # Define which state signifies the start of processing for clips, resetting retry_count
    processing_state = 'processing_post_review' # Or 'keyframing', 'embedding' depending on flow logic
    return _update_state_sync('clips', clip_id, new_state, processing_state, error_message)

def update_source_video_state_sync(source_video_id: int, new_state: str, error_message: str | None = None) -> bool:
    """Synchronously updates the ingest_state for a source_video."""
     # Define which state signifies the start of processing for source_videos
    processing_state = 'downloading' # Or 'processing_local'
    return _update_state_sync('source_videos', source_video_id, new_state, processing_state, error_message)


# --- Pool Management ---
def close_db_pool():
    """Closes all connections in the pool."""
    global db_pool
    # Use Prefect logger if available, otherwise basic print
    try:
        logger = get_run_logger()
        log_func = logger.info
        log_warning = logger.warning
    except Exception: # Fallback if run outside Prefect context
        log_func = print
        log_warning = print

    if db_pool:
        try:
            log_func("Closing psycopg2 connection pool...")
            db_pool.closeall()
            db_pool = None
            log_func("psycopg2 connection pool closed.")
        except Exception as e:
            log_warning(f"Error closing connection pool: {e}")
            db_pool = None # Ensure pool is marked as closed even on error

# Optional: Initialize pool when module loads? Be careful in task environments.
# initialize_db_pool()
# Provides SYNCHRONOUS database utilities using psycopg2 (suitable for Prefect tasks)
# Connects to the APPLICATION database.

from __future__ import annotations
import os
import time
import logging
from typing import List, Tuple, Dict, Any, Optional

import psycopg2
from psycopg2 import sql, pool
from psycopg2.extras import RealDictCursor
from psycopg2.extensions import connection as PgConnection # Type hint for connection
from prefect import get_run_logger

# --- Configuration ---
# Get Prefect logger for tasks, but use standard logger for pool setup issues outside tasks
log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Get Application DB URL from environment (set by docker-compose)
APP_DATABASE_URL = os.getenv("APP_DATABASE_URL")

# --- REMOVED dotenv loading ---
# Environment variables should be injected by the container runtime (docker-compose)

# --- Pool Configuration ---
# Using ThreadedConnectionPool, suitable for multi-threaded Prefect workers
db_pool: Optional[pool.ThreadedConnectionPool] = None
# Allow configuring pool size via env vars, maybe rename for clarity vs asyncpg pool
MIN_POOL_CONN = int(os.getenv("PSYCOPG2_MIN_POOL", 2)) # Example rename
MAX_POOL_CONN = int(os.getenv("PSYCOPG2_MAX_POOL", 10)) # Example rename
_pool_initialized = False # Flag to prevent redundant init attempts


# ─────────────────────────────────────────── Pool Management ───────────────────────────────────────────

def initialize_db_pool() -> None:
    """Initializes the psycopg2 ThreadedConnectionPool singleton."""
    global db_pool, _pool_initialized
    # Use a simple flag check first for performance
    if _pool_initialized:
        return

    # Check DATABASE_URL (only needs to happen once)
    if not APP_DATABASE_URL:
        log.critical("FATAL ERROR: APP_DATABASE_URL environment variable not set for db_utils (psycopg2).")
        raise RuntimeError("APP_DATABASE_URL missing for sync pool initialization")

    # Check again using the flag (more robust than checking db_pool directly for None in threaded env)
    if _pool_initialized:
        return

    # Attempt to initialize
    log_url = APP_DATABASE_URL.split('@')[-1] # Avoid logging password
    log.info(f"Creating psycopg2 ThreadedConnectionPool for APPLICATION DB ({log_url})... (Min: {MIN_POOL_CONN}, Max: {MAX_POOL_CONN})")
    try:
        db_pool = psycopg2.pool.ThreadedConnectionPool(
            MIN_POOL_CONN,
            MAX_POOL_CONN,
            dsn=APP_DATABASE_URL # Connect to the application DB
            # Consider adding application_name for easier PG monitoring:
            # application_name="prefect_worker"
        )
        # Test connection during initialization
        conn: Optional[PgConnection] = None
        try:
            conn = db_pool.getconn()
            with conn.cursor() as cur:
                cur.execute("SELECT version();")
                version = cur.fetchone()
                log.info(f"Psycopg2 pool ready. Connected to APP DB version: {version[0][:25]}...") # Log more of version
            _pool_initialized = True # Set flag *after* successful connection test
        except Exception as test_err:
             log.error(f"Psycopg2 pool created, but connection test failed: {test_err}", exc_info=True)
             # Close the partially created pool if test fails
             if db_pool:
                 db_pool.closeall()
                 db_pool = None
             _pool_initialized = False
             raise ConnectionError("Sync DB pool connection test failed") from test_err
        finally:
            if conn:
                db_pool.putconn(conn) # Release the test connection

    except (psycopg2.OperationalError, psycopg2.pool.PoolError, Exception) as e:
        log.critical(f"FATAL ERROR: Failed to create psycopg2 pool for APP DB: {e}", exc_info=True)
        db_pool = None
        _pool_initialized = False
        raise ConnectionError("Could not initialize sync DB pool") from e

def get_db_connection(cursor_factory=None) -> PgConnection:
    """
    Gets a synchronous connection from the pool. Initializes pool if needed.
    Retries on PoolError.
    """
    # Use Prefect logger within tasks, fallback to module logger otherwise
    task_log = get_run_logger() if get_run_logger() else log

    if not _pool_initialized or db_pool is None:
        task_log.info("Sync DB pool not initialized. Attempting initialization...")
        initialize_db_pool() # Attempt to initialize if not already done

    # Check again after initialization attempt
    if db_pool is None:
         task_log.error("Sync DB pool is still None after initialization attempt.")
         raise ConnectionError("Failed to initialize or get sync DB pool.")

    retries = 3
    delay = 0.5 # Initial delay in seconds
    for attempt in range(retries):
        try:
            conn = db_pool.getconn()
            conn.cursor_factory = cursor_factory # Set cursor factory if requested
            task_log.debug(f"Acquired sync DB connection (ID: {conn.info.backend_pid}). Cursor factory: {cursor_factory}")
            return conn
        except psycopg2.pool.PoolError as e:
            task_log.warning(f"Could not get connection from pool (Attempt {attempt + 1}/{retries}): {e}. Retrying in {delay}s...")
            if attempt == retries - 1: # Last attempt failed
                 task_log.error("Max retries reached trying to get connection from pool.", exc_info=True)
                 raise ConnectionError(f"Could not acquire sync DB connection after {retries} attempts") from e
            time.sleep(delay)
            delay *= 2 # Exponential backoff
        except Exception as e:
            # Catch other potential errors during getconn
            task_log.error(f"Unexpected error getting connection from pool: {e}", exc_info=True)
            raise ConnectionError("Unexpected error acquiring sync DB connection") from e

    # This line should technically be unreachable if retries are > 0
    raise ConnectionError("Failed to acquire sync DB connection (logic error).")


def release_db_connection(conn: PgConnection) -> None:
    """Releases a synchronous connection back to the pool."""
    # Use Prefect logger within tasks, fallback to module logger otherwise
    task_log = get_run_logger() if get_run_logger() else log

    if db_pool and conn:
        # Reset cursor factory to default before releasing
        conn.cursor_factory = None
        try:
            db_pool.putconn(conn)
            task_log.debug(f"Released sync DB connection (ID: {conn.info.backend_pid}) back to pool.")
        except (psycopg2.pool.PoolError, Exception) as e:
            task_log.error(f"Error releasing sync DB connection (ID: {conn.info.backend_pid}): {e}", exc_info=True)
            # Optional: Close the connection if releasing fails badly?
            # try:
            #     conn.close()
            # except Exception: pass # Ignore errors during close after release failure


def close_db_pool() -> None:
    """Closes all connections in the synchronous pool."""
    global db_pool, _pool_initialized
    # Use Prefect logger within tasks, fallback to module logger otherwise
    task_log = get_run_logger() if get_run_logger() else log

    if db_pool:
        task_log.info("Closing psycopg2 ThreadedConnectionPool...")
        try:
            db_pool.closeall()
            task_log.info("Psycopg2 pool closed.")
        except Exception as e:
            task_log.error(f"Error closing psycopg2 pool: {e}", exc_info=True)
        finally:
            db_pool = None
            _pool_initialized = False # Reset the flag


# ─────────────────────────────────── Application Helper Functions ────────────────────────────────────
# These functions interact with the application schema (source_videos, clips) using the sync pool.
# NO CHANGES needed here as they rely on the correctly configured get/release functions above.

def get_all_pending_work(limit_per_stage: int = 50) -> List[Dict[str, Any]]:
    """
    Consolidated work list for the Prefect initiator.
    Queries based on FINALIZED ingest_state (set by Commit Worker).
    """
    # Use Prefect logger within tasks
    task_log = get_run_logger() or log
    conn: Optional[PgConnection] = None
    items = []
    try:
        conn = get_db_connection(cursor_factory=RealDictCursor) # Use RealDictCursor for dict results
        # Use a context manager for the connection to ensure it's released
        with conn:
            with conn.cursor() as cur:
                # Query directly on the finalized states expected after commit worker runs
                # Make sure the states listed ('new', 'downloaded', etc.) are correct
                # Ensure table/column names ('id', 'ingest_state', 'clips', 'source_videos') are correct
                query = sql.SQL("""
                    (SELECT id, 'intake'       AS stage FROM source_videos WHERE ingest_state = %s ORDER BY id LIMIT %s)
                    UNION ALL
                    (SELECT id, 'splice'       AS stage FROM source_videos WHERE ingest_state = %s ORDER BY id LIMIT %s)
                    UNION ALL
                    (SELECT id, 'sprite'       AS stage FROM clips         WHERE ingest_state = %s ORDER BY id LIMIT %s)
                    UNION ALL
                    (SELECT id, 'post_review'  AS stage FROM clips         WHERE ingest_state = %s ORDER BY id LIMIT %s)
                    UNION ALL
                    (SELECT id, 'embedding'    AS stage FROM clips         WHERE ingest_state = %s ORDER BY id LIMIT %s)
                """)
                params = [
                    "new", limit_per_stage,
                    "downloaded", limit_per_stage,
                    "pending_sprite_generation", limit_per_stage,
                    "review_approved", limit_per_stage, # State set by Commit Worker
                    "keyframed", limit_per_stage, # Assumes 'keyframed' is set by embedding task completion
                ]
                task_log.debug(f"Executing get_all_pending_work query with limit {limit_per_stage}")
                cur.execute(query, params)
                items = cur.fetchall() # fetchall() with RealDictCursor returns list of dicts
                task_log.info(f"Found {len(items)} pending work items across stages.")
    except (Exception, psycopg2.DatabaseError) as error:
        task_log.error(f"DB error fetching pending work: {error}", exc_info=True)
        # Optionally re-raise or return empty list depending on desired flow behavior
        raise
    finally:
        if conn:
            release_db_connection(conn) # Ensure connection is released even if context manager fails (belt-and-suspenders)
    task_log.debug(f"Pending work items found: {len(items)}")
    return items

def get_source_input_from_db(source_video_id: int) -> str | None:
    """Fetches the original_url for a given source_video ID."""
    task_log = get_run_logger() or log
    conn: Optional[PgConnection] = None
    input_source: Optional[str] = None
    try:
        conn = get_db_connection() # Default cursor
        with conn:
            with conn.cursor() as cur:
                query = sql.SQL("SELECT original_url FROM source_videos WHERE id = %s")
                task_log.debug(f"Fetching original_url for source_video_id {source_video_id}")
                cur.execute(query, (source_video_id,))
                result = cur.fetchone() # Returns a tuple or None
                if result and result[0]:
                    input_source = result[0]
                    task_log.info(f"Found original_url for source_video_id {source_video_id}")
                else:
                    task_log.warning(f"No 'original_url' found for source_video_id {source_video_id}")
    except (Exception, psycopg2.DatabaseError) as error:
         task_log.error(f"DB error fetching input source for ID {source_video_id}: {error}", exc_info=True)
         # Optionally re-raise
    finally:
        if conn:
            release_db_connection(conn)
    return input_source

def get_pending_merge_pairs() -> List[Tuple[int, int]]:
    """
    Returns list of (target_id, source_id) ready for backward merge.
    Queries based on FINALIZED ingest_state (set by Commit Worker).
    """
    task_log = get_run_logger() or log
    conn: Optional[PgConnection] = None
    pairs: List[Tuple[int, int]] = []
    try:
        # Use RealDictCursor as it was used before, makes extracting named cols easier
        conn = get_db_connection(cursor_factory=RealDictCursor)
        with conn:
            with conn.cursor() as cur:
                # Ensure 'processing_metadata' is jsonb and contains 'merge_source_clip_id' as text convertible to int
                # Ensure the states 'pending_merge_target' and 'marked_for_merge_into_previous' are correct
                query = sql.SQL("""
                    SELECT
                        t.id AS target_id,
                        (t.processing_metadata ->> 'merge_source_clip_id')::int AS source_id
                    FROM clips t
                    JOIN clips s ON s.id = (t.processing_metadata ->> 'merge_source_clip_id')::int
                    WHERE t.ingest_state = 'pending_merge_target'
                      AND s.ingest_state = 'marked_for_merge_into_previous';
                """)
                task_log.debug("Fetching pending merge pairs...")
                cur.execute(query)
                # Convert list of dicts from RealDictCursor to list of tuples
                pairs = [(row["target_id"], row["source_id"]) for row in cur.fetchall()]
                task_log.info(f"Found {len(pairs)} pending merge pairs.")
    except (Exception, psycopg2.DatabaseError) as error:
        task_log.error(f"DB error fetching pending merge pairs: {error}", exc_info=True)
        # Optionally re-raise
    finally:
        if conn:
            release_db_connection(conn)
    task_log.debug(f"Merge pairs ready: {pairs}")
    return pairs

def get_pending_split_jobs() -> List[Tuple[int, int]]:
    """
    Returns list of (clip_id, split_frame).
    Queries based on FINALIZED ingest_state (set by Commit Worker).
    """
    task_log = get_run_logger() or log
    conn: Optional[PgConnection] = None
    jobs: List[Tuple[int, int]] = []
    try:
        # Use RealDictCursor as it was used before
        conn = get_db_connection(cursor_factory=RealDictCursor)
        with conn:
            with conn.cursor() as cur:
                # Ensure 'processing_metadata' is jsonb and contains 'split_request_at_frame' as text convertible to int
                # Ensure the state 'pending_split' is correct
                query = sql.SQL("""
                    SELECT id,
                           (processing_metadata ->> 'split_request_at_frame')::int AS split_frame
                    FROM clips
                    WHERE ingest_state = 'pending_split';
                """)
                task_log.debug("Fetching pending split jobs...")
                cur.execute(query)
                # Convert list of dicts from RealDictCursor to list of tuples
                jobs = [(row["id"], row["split_frame"]) for row in cur.fetchall()]
                task_log.info(f"Found {len(jobs)} pending split jobs.")
    except (Exception, psycopg2.DatabaseError) as error:
        task_log.error(f"DB error fetching pending split jobs: {error}", exc_info=True)
        # Optionally re-raise
    finally:
        if conn:
            release_db_connection(conn)
    task_log.debug(f"Split jobs ready: {jobs}")
    return jobs


# ─────────────────────────────────── Immediate State Update Helpers ────────────────────────────────────
# These are used by tasks to update state *during* processing or on completion/failure.

def _update_state_sync(
    table_name: str, item_id: int, new_state: str,
    processing_state: Optional[str] = None, # State indicating 'in progress' to reset retry count
    error_message: Optional[str] = None
) -> bool:
    """
    Internal helper to update ingest_state and potentially other fields for an item.
    Returns True if update successful (1 row affected), False otherwise.
    """
    task_log = get_run_logger() or log
    conn: Optional[PgConnection] = None
    updated = False
    try:
        conn = get_db_connection()
        with conn: # Auto-commit or rollback on exit
            with conn.cursor() as cur:
                # Base fields to update
                set_clauses = [
                    sql.SQL("ingest_state = %s"),
                    sql.SQL("last_error = %s"), # Set to NULL if error_message is None
                    sql.SQL("updated_at = NOW()")
                ]
                params: list[Any] = [new_state, error_message]

                # Conditionally reset retry_count if moving TO an 'in-progress' state
                # Note: Original logic reset when new_state == processing_state. Adjust if needed.
                # If processing_state is provided AND new_state matches it, reset retry count.
                if processing_state and new_state == processing_state:
                    set_clauses.append(sql.SQL("retry_count = 0"))
                    task_log.debug(f"Resetting retry_count for {table_name} ID {item_id} moving to state '{new_state}'.")

                # Conditionally clear action_committed_at for clips table
                # Based on original logic: Clear unless moving to specific deletion states
                if table_name == 'clips' and new_state not in ['approved_pending_deletion', 'archived_pending_deletion']:
                    set_clauses.append(sql.SQL("action_committed_at = NULL"))

                # Build the final query
                query = sql.SQL("UPDATE {} SET {} WHERE id = %s").format(
                    sql.Identifier(table_name),
                    sql.SQL(", ").join(set_clauses)
                )
                params.append(item_id) # Add the ID for the WHERE clause

                task_log.debug(f"Updating {table_name} ID {item_id} to state '{new_state}' (Error: {bool(error_message)})")
                cur.execute(query, params)

                if cur.rowcount == 1:
                    task_log.info(f"Successfully updated {table_name} ID {item_id} to state '{new_state}'.")
                    updated = True
                elif cur.rowcount == 0:
                    task_log.warning(f"Update state for {table_name} ID {item_id} to '{new_state}' affected 0 rows. Item might not exist or already in target state.")
                else:
                    # Should not happen with WHERE id = %s unless ID is not unique
                    task_log.error(f"Update state for {table_name} ID {item_id} to '{new_state}' affected {cur.rowcount} rows! Investigate.")
                    updated = False # Treat unexpected rowcount as failure

    except (Exception, psycopg2.DatabaseError) as error:
        task_log.error(f"DB error updating state for {table_name} ID {item_id} to '{new_state}': {error}", exc_info=True)
        updated = False # Ensure False is returned on error
        # Optionally re-raise
    finally:
        if conn:
            release_db_connection(conn)
    return updated

def update_clip_state_sync(clip_id: int, new_state: str, error_message: Optional[str] = None) -> bool:
    """Updates the ingest_state for a specific clip."""
    # Define potential 'in-progress' states for clips if retry reset is needed
    # e.g., processing_states = ['processing_sprite', 'processing_embedding', 'merging', 'splitting']
    # Pass the relevant one if needed, otherwise pass None.
    # Example: If calling this when starting embedding, pass 'processing_embedding'
    # If calling on completion/failure, pass None
    processing_state = None # Adjust as needed per call site
    return _update_state_sync("clips", clip_id, new_state, processing_state, error_message)

def update_source_video_state_sync(source_video_id: int, new_state: str, error_message: Optional[str] = None) -> bool:
    """Updates the ingest_state for a specific source_video."""
    # Define potential 'in-progress' states for source_videos
    processing_state = "downloading" # Example: Reset retry count when starting 'downloading'
    # Adjust 'processing_state' based on where this function is called and if retry should reset
    return _update_state_sync("source_videos", source_video_id, new_state, processing_state, error_message)


# --- Optional: Add a function to be called on worker startup/shutdown ---
def on_worker_startup():
    """Initialize resources needed by tasks, like the DB pool."""
    log.info("Worker starting up. Initializing database pool...")
    try:
        initialize_db_pool()
    except Exception as e:
        log.critical(f"Failed to initialize database pool during worker startup: {e}", exc_info=True)
        # Depending on requirements, you might want to prevent worker start here.

def on_worker_shutdown():
    """Clean up resources, like closing the DB pool."""
    log.info("Worker shutting down. Closing database pool...")
    close_db_pool()

# Prefect can call these via deployment configuration or directly in flow definitions if needed.
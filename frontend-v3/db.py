import sys
import os
import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor
from typing import Optional, Any
import contextlib
import time # Only needed if uncommenting internal timing log

# --- Configuration ---
DATABASE_URL = os.getenv("DATABASE_URL")
# Min/Max pool size - adjust as needed
DB_MIN_CONN = int(os.getenv("DB_MIN_CONN", 1))
DB_MAX_CONN = int(os.getenv("DB_MAX_CONN", 5))

# --- Global Connection Pool ---
# This variable is managed within this module.
db_pool: Optional[psycopg2.pool.SimpleConnectionPool] = None

def initialize_db_pool():
    """Initializes the database connection pool."""
    global db_pool
    if db_pool:
        return # Already initialized
    print(f"Initializing DB Pool (Min: {DB_MIN_CONN}, Max: {DB_MAX_CONN})...")

    if not DATABASE_URL:
        raise ValueError("DATABASE_URL environment variable not set.")

    try:
        db_pool = psycopg2.pool.SimpleConnectionPool(
            DB_MIN_CONN, DB_MAX_CONN, dsn=DATABASE_URL, cursor_factory=RealDictCursor
        )
        # Test connection
        conn = db_pool.getconn()
        print(f"DB Pool Initialized. Test connection successful.")
        db_pool.putconn(conn)
    except psycopg2.OperationalError as e:
        print(f"FATAL: Database connection pool initialization failed: {e}", file=sys.stderr)
        db_pool = None
        raise ConnectionError(f"DB pool initialization failure: {e}") from e
    except ValueError as e:
         print(f"FATAL: Configuration error: {e}", file=sys.stderr)
         db_pool = None
         raise ConnectionError(f"DB configuration error: {e}") from e
    except Exception as e:
        print(f"FATAL: Unexpected error initializing DB pool: {e}", file=sys.stderr)
        db_pool = None
        raise ConnectionError(f"Unexpected DB pool initialization error: {e}") from e

@contextlib.contextmanager
def get_db_conn_from_pool() -> Any:
    """Provides a connection from the pool using a context manager."""
    if db_pool is None:
        print("Warning: DB Pool was not initialized. Attempting on-demand initialization...")
        try:
            initialize_db_pool()
        except ConnectionError as init_err:
            # If on-demand init fails, it's critical
            print(f"FATAL: On-demand DB Pool initialization failed: {init_err}", file=sys.stderr)
            raise ConnectionError("Database pool could not be initialized.") from init_err
        if db_pool is None: # Still failed after attempt
             raise ConnectionError("Database pool is not initialized and on-demand init failed.")

    conn = None
    try:
        conn = db_pool.getconn()
        yield conn
        conn.commit() # Commit on successful exit of 'with' block
    except (Exception, psycopg2.DatabaseError) as e:
        if conn: conn.rollback() # Rollback on any exception
        # Log the specific error encountered within the context
        print(f"DB Error in context manager: Transaction rolled back. {type(e).__name__}: {e}", file=sys.stderr)
        raise # Re-raise the exception after rollback
    finally:
        # Ensure connection is always returned to the pool
        if conn:
             try:
                 db_pool.putconn(conn)
             except pool.PoolError as pool_err:
                 print(f"Error returning connection to pool: {pool_err}", file=sys.stderr)
                 # Decide how to handle this - maybe close the connection?
                 try: conn.close()
                 except: pass # Ignore errors during close after pool error


def close_db_pool():
    """Closes all connections in the pool."""
    global db_pool
    if db_pool:
        print("Closing DB Pool...")
        db_pool.closeall()
        db_pool = None
        print("DB Pool Closed.")

# --- General Database Helper Functions ---
def fetch_one(sql, params=()):
    """Fetches a single row using a pooled connection."""
    try:
        # Uses the context manager defined above
        with get_db_conn_from_pool() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                return cur.fetchone()
    except Exception as e:
        # Context manager already logs rollback, just log the specific operation error
        print(f"Error in fetch_one SQL execution: {type(e).__name__} - {e}", file=sys.stderr)
        return None # Indicate failure

def execute_sql(sql, params=()):
    """Executes SQL (INSERT, UPDATE, DELETE) using a pooled connection."""
    rows_affected = 0
    try:
        # Uses the context manager defined above
        with get_db_conn_from_pool() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                rows_affected = cur.rowcount
        return rows_affected # Return rows affected on success
    except Exception as e:
         # Context manager already logs rollback, just log the specific operation error
         print(f"Error in execute_sql execution: {type(e).__name__} - {e}", file=sys.stderr)
         return -1 # Indicate failure
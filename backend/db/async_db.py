# Provides ASYNCHRONOUS database utilities using asyncpg for the APPLICATION database.

import asyncio
import json
import logging
import os
from typing import AsyncGenerator, Optional
from contextlib import asynccontextmanager

import asyncpg

# --- Configuration ---
# Use standard logging
log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO) # Adjust level as needed

# Get Application DB URL from environment (set by docker-compose)
APP_DATABASE_URL = os.getenv("APP_DATABASE_URL")

# Define pool size constants (can also get from os.getenv if preferred)
MIN_POOL_SIZE = int(os.getenv("MIN_POOL_SIZE", 1))
MAX_POOL_SIZE = int(os.getenv("MAX_POOL_SIZE", 10))


# --- Internal State ---
_pool: asyncpg.Pool | None = None # Module-level variable for the pool
_pool_lock = asyncio.Lock() # Lock to prevent race conditions during pool creation


# --- Initialization for new connections ---
async def _init_connection(conn: asyncpg.Connection):
    """Initialize new connection settings, register JSON/JSONB codecs."""
    # Using id(conn) for logging can be confusing if connections are reused, but okay for init phase
    log.debug(f"Initializing new DB connection {id(conn)}...")
    try:
        # Register JSON/JSONB <-> Python dict codecs
        for json_type in ('json', 'jsonb'):
            await conn.set_type_codec(
                json_type,
                encoder=json.dumps,
                decoder=json.loads,
                schema='pg_catalog',
                format='text' # Use text format for JSON string transfer
            )
        log.debug(f"Registered JSON/JSONB type codecs for connection {id(conn)}.")
        # Add any other per-connection setup here if needed
        # E.g., await conn.execute("SET TIME ZONE 'UTC';")
    except Exception as e:
        log.error(f"Error setting type codecs for connection {id(conn)}: {e}", exc_info=True)
        # Depending on severity, you might want to raise an error to prevent pool usage
        # raise


# --- Pool Management ---
async def get_db_pool() -> asyncpg.Pool:
    """
    Creates and returns the asyncpg database connection pool singleton.
    Handles potential race conditions during initial creation.
    """
    global _pool
    # Fast path: Check if pool already exists without acquiring lock
    if _pool is not None:
        return _pool

    # Acquire lock to ensure only one coroutine creates the pool
    async with _pool_lock:
        # Check again inside the lock in case another coroutine created it while waiting
        if _pool is not None:
            return _pool

        # Proceed with pool creation
        if not APP_DATABASE_URL:
            log.critical("FATAL ERROR: APP_DATABASE_URL environment variable not set.")
            raise RuntimeError("APP_DATABASE_URL not configured.")

        log_url = APP_DATABASE_URL.split('@')[-1] # Avoid logging password
        log.info(f"Creating asyncpg pool for APPLICATION DB ({log_url})... (Min: {MIN_POOL_SIZE}, Max: {MAX_POOL_SIZE})")
        try:
            _pool = await asyncpg.create_pool(
                dsn=APP_DATABASE_URL,
                min_size=MIN_POOL_SIZE,
                max_size=MAX_POOL_SIZE,
                init=_init_connection # Setup codecs for every new connection
            )
            log.info("Successfully created asyncpg APPLICATION database connection pool.")

            # Perform a quick connection test after pool creation
            async with _pool.acquire() as connection:
                 version = await connection.fetchval("SELECT version();")
                 test_json = await connection.fetchval("SELECT '{\"test\": 1}'::jsonb;")
                 log.info(f"APP DB (asyncpg) connection test successful. PostgreSQL version: {version[:15]}..., JSONB codec test type: {type(test_json)}")

        except Exception as e:
            log.critical(f"FATAL ERROR: Failed to create asyncpg pool: {e}", exc_info=True)
            _pool = None # Ensure pool remains None if creation failed
            raise RuntimeError("Could not create asyncpg database pool") from e

    # This should now always return a valid pool or have raised an error
    if _pool is None:
         # This state should ideally not be reached due to checks above, but defensively:
         raise RuntimeError("Pool creation failed unexpectedly.")

    return _pool


async def close_db_pool():
    """Closes the asyncpg database connection pool if it exists."""
    global _pool
    # Acquire lock to prevent closing while creating or vice-versa
    async with _pool_lock:
        if _pool:
            log.info("Closing asyncpg database connection pool...")
            try:
                # Gracefully close the pool, waiting for connections to be released
                await _pool.close()
                log.info("Asyncpg database connection pool closed successfully.")
            except Exception as e:
                log.error(f"Error closing asyncpg pool: {e}", exc_info=True)
            finally:
                 _pool = None # Ensure pool variable is reset


# --- Context Manager for Connections ---
@asynccontextmanager
async def get_db_connection() -> AsyncGenerator[asyncpg.Connection, None]:
    """
    Provides an asyncpg connection from the pool using an async context manager.

    Usage:
        async with get_db_connection() as conn:
            await conn.execute(...)
    """
    pool = await get_db_pool() # Ensure pool is initialized
    connection: asyncpg.Connection | None = None
    try:
        # Acquire connection from the pool
        connection = await pool.acquire()
        log.debug(f"Acquired DB connection {id(connection)} from asyncpg pool.")
        yield connection # Provide the connection to the 'with' block
    except Exception as e:
        log.error(f"ERROR during asyncpg DB operation: {e}", exc_info=True)
        # Re-raise the error so the calling code knows something went wrong
        raise
    finally:
        if connection:
            try:
                # Release connection back to the pool
                await pool.release(connection)
                log.debug(f"Released DB connection {id(connection)} back to asyncpg pool.")
            except Exception as release_err:
                 # Log error if releasing fails, but don't obscure the original error (if any)
                 log.error(f"Error releasing DB connection {id(connection)}: {release_err}", exc_info=True)

# --- Example Usage (Optional - for testing this module directly) ---
async def _test_connection():
    print("Testing asyncpg connection...")
    try:
        async with get_db_connection() as conn:
            result = await conn.fetchval("SELECT 1 + 1;")
            print(f"Test query (1+1) result: {result}")
            json_result = await conn.fetchrow("SELECT '{\"key\": \"value\"}'::jsonb as data;")
            print(f"Test JSONB fetch: {json_result} (type: {type(json_result['data'])})")
        print("Connection test successful.")
    except Exception as e:
        print(f"Connection test failed: {e}")
    finally:
        await close_db_pool()

if __name__ == "__main__":
    # This allows running python backend/database.py to test connection
    # Make sure APP_DATABASE_URL is set in your environment if running directly
    if not APP_DATABASE_URL:
         print("Error: APP_DATABASE_URL environment variable is not set.")
         print("Please set it before running this script directly for testing.")
         # Example: export APP_DATABASE_URL='postgresql://user:pass@host:port/db'
    else:
        asyncio.run(_test_connection())
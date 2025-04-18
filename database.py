from typing import AsyncGenerator
import asyncpg
import json
from fastapi import Request, HTTPException
import os

from config import DATABASE_URL, log

# This module handles the ASYNCHRONOUS database interactions for FastAPI
# It uses asyncpg, distinct from the synchronous psycopg2 in db_utils.py

# Define pool size constants (can also get from os.getenv if preferred)
MIN_POOL_SIZE = int(os.getenv("MIN_POOL_SIZE", 1))
MAX_POOL_SIZE = int(os.getenv("MAX_POOL_SIZE", 10))

# --- Initialization function for new connections ---
async def _init_connection(conn: asyncpg.Connection):
    """Initialize new connection settings, register JSON/JSONB codecs."""
    log.info(f"Initializing new DB connection {id(conn)}...")
    try:
        # Register JSONB -> Python dict codec
        await conn.set_type_codec(
            'jsonb',
            encoder=json.dumps,  # How to encode Python obj -> JSON string for DB
            decoder=json.loads,  # How to decode JSON string from DB -> Python obj
            schema='pg_catalog',
            format='text' # Use 'text' format for JSON string transfer
        )
        # Optional: Also register for 'json' type if used
        await conn.set_type_codec(
            'json',
            encoder=json.dumps,
            decoder=json.loads,
            schema='pg_catalog',
            format='text'
        )
        log.info(f"Registered JSON/JSONB type codecs for connection {id(conn)}.")
        # Add any other per-connection initializations here if needed
        # E.g., await conn.execute("SET TIME ZONE 'UTC';")
    except Exception as e:
        log.error(f"Error setting type codecs for connection {id(conn)}: {e}", exc_info=True)
        # Depending on severity, you might want to raise an error here

# --- Pool Management ---
_pool: asyncpg.Pool | None = None # Module-level variable for the pool

async def connect_db() -> asyncpg.Pool:
    """Creates and returns the asyncpg database connection pool."""
    global _pool
    if _pool is None:
        if not DATABASE_URL:
            log.critical("FATAL ERROR: DATABASE_URL environment variable not set.")
            raise RuntimeError("DATABASE_URL not configured.")

        log.info(f"Creating asyncpg pool for {DATABASE_URL[:15]}... (Min: {MIN_POOL_SIZE}, Max: {MAX_POOL_SIZE})")
        try:
            _pool = await asyncpg.create_pool(
                dsn=DATABASE_URL,
                min_size=MIN_POOL_SIZE,
                max_size=MAX_POOL_SIZE,
                init=_init_connection
            )
            log.info("Successfully created asyncpg database connection pool.")

            # Test connection on startup using the initialized pool
            async with _pool.acquire() as connection:
                 version = await connection.fetchval("SELECT version();")
                 test_json = await connection.fetchval("SELECT '{\"test\": 1}'::jsonb;")
                 log.info(f"DB connection test successful. PostgreSQL version: {version[:15]}..., JSONB codec test type: {type(test_json)}")

        except Exception as e:
            log.critical(f"FATAL ERROR: Failed to create asyncpg pool: {e}", exc_info=True)
            _pool = None # Ensure pool remains None if creation failed
            raise RuntimeError("Could not connect to database") from e
    else:
        log.debug("Returning existing asyncpg connection pool.")

    return _pool

async def close_db():
    """Closes the asyncpg database connection pool."""
    global _pool
    if _pool:
        log.info("Closing asyncpg database connection pool...")
        try:
            await _pool.close()
            log.info("Asyncpg database connection pool closed successfully.")
        except Exception as e:
            log.error(f"Error closing asyncpg pool: {e}", exc_info=True)
        finally:
             _pool = None # Ensure pool variable is reset

# --- FastAPI Dependency ---
async def get_db_connection(request: Request) -> AsyncGenerator[asyncpg.Connection, None]:
    """
    FastAPI dependency to get an asyncpg connection from the pool.
    Assumes pool is stored in app.state.db_pool during app startup.
    """
    pool: asyncpg.Pool | None = getattr(request.app.state, 'db_pool', None)

    if pool is None:
        log.error("Database connection pool 'db_pool' not found in app.state. App startup might have failed.")
        # 503 Service Unavailable is appropriate if the pool isn't ready
        raise HTTPException(status_code=503, detail="Database connection pool is not available.")

    connection: asyncpg.Connection | None = None
    try:
        # Acquire connection from the pool. Connections initialized via pool will have codecs set.
        connection = await pool.acquire()
        log.debug(f"Acquired DB connection {id(connection)} from asyncpg pool.")
        yield connection # Provide the connection to the route function
    except Exception as e:
        # Log error during acquisition or potential errors during route execution before release
        log.error(f"ERROR acquiring/using asyncpg DB connection: {e}", exc_info=True)
        # Raising HTTPException here will be caught by FastAPI and return an error response
        # 500 is appropriate for unexpected errors during DB usage.
        raise HTTPException(status_code=500, detail="Internal server error during database operation.")
    finally:
        if connection:
            try:
                # Release connection back to the pool
                await pool.release(connection)
                log.debug(f"Released DB connection {id(connection)} back to asyncpg pool.")
            except Exception as release_err:
                 # Log error if releasing fails, but don't raise over potential original error
                 log.error(f"Error releasing DB connection {id(connection)}: {release_err}", exc_info=True)
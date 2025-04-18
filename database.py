from typing import AsyncGenerator
import asyncpg
from fastapi import Request, HTTPException

from config import DATABASE_URL, log

# This module handles the ASYNCHRONOUS database interactions for FastAPI
# It uses asyncpg, distinct from the synchronous psycopg2 in db_utils.py

async def connect_db() -> asyncpg.Pool:
    """Creates the asyncpg database connection pool."""
    try:
        log.info(f"Attempting to create asyncpg pool for {DATABASE_URL[:15]}...")
        # Example pool size configuration - adjust as needed
        pool = await asyncpg.create_pool(
            dsn=DATABASE_URL,
            min_size=1,
            max_size=10
        )
        log.info("Successfully created asyncpg database connection pool.")

        # Optional: Test connection on startup
        async with pool.acquire() as connection:
             version = await connection.fetchval("SELECT version();")
             log.info(f"Database connection test successful. PostgreSQL version: {version[:15]}...")

        return pool
    except Exception as e:
        log.critical(f"FATAL ERROR: Failed to create asyncpg pool: {e}", exc_info=True)
        # Re-raise to potentially stop app startup via lifespan manager
        raise RuntimeError("Could not connect to database") from e

async def close_db(pool: asyncpg.Pool):
    """Closes the asyncpg database connection pool."""
    if pool:
        log.info("Closing asyncpg database connection pool...")
        try:
            await pool.close()
            log.info("Asyncpg database connection pool closed successfully.")
        except Exception as e:
            log.error(f"Error closing asyncpg pool: {e}", exc_info=True)

async def get_db_connection(request: Request) -> AsyncGenerator[asyncpg.Connection, None]:
    """FastAPI dependency to get an asyncpg connection from the pool stored in app.state."""
    pool: asyncpg.Pool | None = getattr(request.app.state, 'db_pool', None)

    if pool is None:
        log.error("Database connection pool 'db_pool' not found in app.state.")
        raise HTTPException(status_code=503, detail="Database connection pool is not available.")

    connection: asyncpg.Connection | None = None
    try:
        # Acquire connection from the pool
        connection = await pool.acquire()
        log.debug(f"Acquired DB connection {id(connection)} from asyncpg pool.")
        yield connection
    except Exception as e:
        # Log error during acquisition or usage
        log.error(f"ERROR acquiring/using asyncpg DB connection: {e}", exc_info=True)
        # Let FastAPI handle the exception for internal server error response
        raise HTTPException(status_code=500, detail="Internal server error during database operation.")
    finally:
        if connection:
            # Release connection back to the pool
            await pool.release(connection)
            log.debug(f"Released DB connection {id(connection)} back to asyncpg pool.")
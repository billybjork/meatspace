import uvicorn
import asyncpg
import os
from contextlib import asynccontextmanager
from fastapi import FastAPI

from app import app, DATABASE_URL

# --- Lifespan Management for Database Pool ---

@asynccontextmanager
async def lifespan(app_instance: FastAPI):
    """
    Context manager for application lifespan events.
    Handles database connection pool creation and closing.
    """
    print("Lifespan: Startup phase starting...")
    pool = None # Initialize pool variable
    try:
        print(f"Lifespan: Attempting to create database pool for URL (partially hidden): {DATABASE_URL[:15]}...")
        pool = await asyncpg.create_pool(
            dsn=DATABASE_URL,
            min_size=1, # Example: configure pool size
            max_size=10
        )
        # Store the pool in the application state for access in dependencies
        app_instance.state.db_pool = pool
        print("Lifespan: Database pool created successfully and stored in app.state.db_pool.")

        # Test connection
        async with pool.acquire() as connection:
            version = await connection.fetchval("SELECT version();")
            print(f"Lifespan: Database connection test successful. PostgreSQL version: {version[:15]}...")

    except Exception as e:
        # Log error and potentially prevent app startup if critical
        print(f"FATAL LIFESPAN ERROR: Could not create database connection pool during startup: {e}")

    print("Lifespan: Startup phase complete. Yielding control to application.")
    yield  # Application runs here

    # --- Shutdown phase ---
    print("Lifespan: Shutdown phase starting...")
    # Retrieve the pool from app state
    pool_to_close = getattr(app_instance.state, 'db_pool', None)
    if pool_to_close:
        print("Lifespan: Closing database connection pool...")
        try:
            await pool_to_close.close()
            print("Lifespan: Database connection pool closed successfully.")
        except Exception as e:
            print(f"ERROR during database pool closing: {e}")
    else:
        print("Lifespan: No database pool found in app state to close.")
    print("Lifespan: Shutdown phase complete.")

# --- Attach Lifespan to the App ---
app.router.lifespan_context = lifespan

# --- Main Execution ---
if __name__ == '__main__':
    print("Starting FastAPI server from main.py...")
    # Use reload=True for development, False for production
    # Point uvicorn to this file (main) and the 'app' object imported from app.py
    uvicorn.run("main:app", host="127.0.0.1", port=5001, reload=True)
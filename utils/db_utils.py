import os
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
from prefect import get_run_logger

# Load environment variables from .env file in the parent directory
dotenv_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')
load_dotenv(dotenv_path=dotenv_path)

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise ValueError("DATABASE_URL not found in environment variables. Check your .env file.")

def get_db_connection():
    """Establishes a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        return conn
    except psycopg2.OperationalError as e:
        print(f"ERROR: Could not connect to database using DATABASE_URL.")
        print(f"Check connection details and ensure the database server is running.")
        print(f"Error details: {e}")
        raise # Re-raise the exception to halt execution if connection fails

def get_media_base_dir():
    """Gets the media base directory from environment variables."""
    media_dir = os.getenv("MEDIA_BASE_DIR")
    if not media_dir:
        raise ValueError("MEDIA_BASE_DIR not found in environment variables. Check your .env file.")
    if not os.path.isdir(media_dir):
         raise ValueError(f"MEDIA_BASE_DIR '{media_dir}' does not exist or is not a directory.")
    return os.path.abspath(media_dir) # Return absolute path for safety

def get_items_by_state(table: str, state: str) -> list[int]:
    """Helper to query DB for IDs in a specific state."""
    items = []
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            # Use sql.Identifier for safe table name interpolation
            query = sql.SQL("SELECT id FROM {} WHERE ingest_state = %s ORDER BY id").format(sql.Identifier(table))
            cur.execute(query, (state,))
            items = [row[0] for row in cur.fetchall()]
    except Exception as e:
        # Log error appropriately, maybe using Prefect logger if called from flow/task
        print(f"DB Error querying {table} for state {state}: {e}") # Simple print for now
        # Consider how errors here should be handled by the caller
    finally:
        if conn:
            conn.close()
    return items

def get_source_input_from_db(source_video_id: int) -> str | None:
    """Fetches the original_url for a given source_video ID."""
    conn = None
    input_source = None
    logger = get_run_logger() # Use logger if available
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            # Fetch original_url - assuming this holds the URL or initial path
            cur.execute("SELECT original_url FROM source_videos WHERE id = %s", (source_video_id,))
            result = cur.fetchone()
            if result and result[0]:
                input_source = result[0]
            else:
                # Log a warning if called but no URL found
                if logger:
                    logger.warning(f"No 'original_url' found in DB for source_video_id {source_video_id}")
                else:
                    print(f"Warning: No 'original_url' found in DB for source_video_id {source_video_id}")

    except (Exception, psycopg2.DatabaseError) as error:
         if logger:
             logger.error(f"DB error fetching input source for ID {source_video_id}: {error}")
         else:
             print(f"DB error fetching input source for ID {source_video_id}: {error}")
    finally:
        if conn:
            conn.close()
    return input_source
import os
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv

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
    """
    Fetches the input source (URL or potentially path) for a given source_video_id.
    Primarily looks for 'original_url'. Needs logic if local paths are stored differently.
    """
    input_source = None
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            # Select original_url, maybe fallback to filepath if url is null? Decide policy.
            cur.execute("SELECT original_url FROM source_videos WHERE id = %s", (source_video_id,))
            result = cur.fetchone()
            if result:
                input_source = result[0] # Fetch the first column (original_url)
            else:
                 print(f"Warning: No record found for source_video_id {source_video_id} in get_source_input_from_db")

    except Exception as e:
        print(f"DB Error fetching input source for ID {source_video_id}: {e}")
    finally:
        if conn:
            conn.close()

    # Add logic here if you store initial local paths in a different column
    # if not input_source:
    #    # try fetching filepath?
    #    pass

    if not input_source:
         print(f"Warning: Could not determine input source for source_video_id {source_video_id}.")

    return input_source
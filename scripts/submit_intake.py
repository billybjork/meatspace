import argparse
from pathlib import Path
import os
import sys
import psycopg2 # Import standard postgres library
from dotenv import load_dotenv # To load DATABASE_URL

# --- Load Environment Variables ---
# Load environment variables from .env file located in the project root (one level up from 'scripts')
project_root_for_env = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
dotenv_path = os.path.join(project_root_for_env, '.env')

if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path=dotenv_path)
    print(f"Loaded environment variables from: {dotenv_path}")
else:
    print(f"Warning: .env file not found at {dotenv_path}. Attempting to load from current environment.")
    # Attempt loading from environment directly if .env not found
    load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    print("ERROR: DATABASE_URL environment variable not found.")
    sys.exit(1)
# --- End Environment Loading ---


# --- Add Project Root to Python Path ---
# Ensures modules in 'tasks', 'utils', etc., can be imported
project_root_for_imports = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root_for_imports not in sys.path:
    sys.path.insert(0, project_root_for_imports)
# --- End Path Addition ---


try:
    from tasks.intake import intake_task
    # utils.db_utils is no longer needed here as we use psycopg2 directly for this script
except ImportError as e:
    print(f"Error importing Prefect task 'intake_task': {e}")
    print(f"Ensure you are running this script relative to the project root (e.g., python scripts/{os.path.basename(__file__)})")
    print(f"Project root added to sys.path: {project_root_for_imports}")
    print("Check if the 'tasks' directory and 'intake.py' exist correctly.")
    sys.exit(1)

def create_new_source_video_record(input_url_or_path: str, initial_title: str = None):
    """
    Inserts a new record into source_videos with state 'new', sets web_scraped flag,
    and returns the ID.
    Uses direct psycopg2 connection.
    """
    conn = None # Use standard psycopg2 connection
    new_id = None
    is_url = input_url_or_path.lower().startswith(('http://', 'https://'))
    # Use provided title or derive a basic one from input path/URL stem
    title_to_insert = initial_title if initial_title else Path(input_url_or_path).stem[:250]
    web_scraped_flag = is_url

    print(f"Attempting to create source_videos record for: {input_url_or_path}")
    print(f"Using Title: {title_to_insert}")
    print(f"Setting web_scraped: {web_scraped_flag}")

    try:
        # --- Use psycopg2 directly ---
        conn = psycopg2.connect(DATABASE_URL)
        # --- End change ---

        with conn.cursor() as cur:
            # Insert record, including original_url and web_scraped flag
            cur.execute(
                """
                INSERT INTO source_videos (title, ingest_state, original_url, web_scraped)
                VALUES (%s, 'new', %s, %s)
                RETURNING id;
                """,
                (title_to_insert,
                 input_url_or_path if is_url else None, # Store URL only if it's a URL
                 web_scraped_flag) # Set the flag
            )
            result = cur.fetchone()
            if result:
                new_id = result[0]
                print(f"Successfully created source_videos record with ID: {new_id}")
            else:
                print("ERROR: Failed to get new ID after insert.")
                return None

        conn.commit() # Commit the new record
        return new_id

    except psycopg2.Error as db_err: # Catch specific DB errors
        print(f"ERROR creating database record (psycopg2): {db_err}")
        if conn:
            conn.rollback() # Rollback on error
        return None
    except Exception as e:
        print(f"ERROR creating database record (General): {e}")
        if conn:
            conn.rollback() # Rollback on error
        return None
    finally:
        if conn:
            conn.close() # Always close the direct connection

def main():
    parser = argparse.ArgumentParser(description="Manually submit a video URL or local path for the Prefect intake workflow.")
    parser.add_argument("input_source", help="The URL (http/https) or absolute local file path of the video to process.")
    parser.add_argument("--title", help="Optional initial title for the database record (recommended for local files). If omitted, derived from filename/URL.", default=None)
    # Add back optional args if the intake_task signature needs them directly
    parser.add_argument("--no-reencode", action="store_true", help="Tell the intake task *not* to re-encode the video (default is True for re-encoding).")
    parser.add_argument("--overwrite", action="store_true", help="Tell the intake task to overwrite existing files/data if necessary (use with caution).")

    args = parser.parse_args()

    # --- Input Validation ---
    is_url = args.input_source.lower().startswith(('http://', 'https://'))
    input_path_obj = Path(args.input_source)

    if not is_url:
        # Check if local path exists if it's not a URL
        if not input_path_obj.exists():
             print(f"ERROR: Local file path provided does not exist: {args.input_source}")
             sys.exit(1)
        # Resolve to absolute path for clarity and consistency
        args.input_source = str(input_path_obj.resolve())
        print(f"Processing local file: {args.input_source}")
        if not args.title:
             print("WARNING: Processing a local file without providing an explicit --title. Title will be derived from filename.")
    else:
         print(f"Processing URL: {args.input_source}")

    # 1. Create the DB record (Uses direct psycopg2 connection)
    source_id = create_new_source_video_record(args.input_source, args.title)

    if not source_id:
        print("Failed to create database record. Aborting task submission.")
        sys.exit(1)

    # 2. Submit the Prefect task run using .delay()
    print(f"\nSubmitting intake_task for source_video_id: {source_id}...")
    print(f"  Input: {args.input_source}")
    print(f"  Re-encode: {not args.no_reencode}") # Logic inverted: --no-reencode flag means re_encode_for_qt=False
    print(f"  Overwrite: {args.overwrite}")

    try:
        # Use .delay() for submitting from outside a flow
        # Pass arguments matching the intake_task function signature
        intake_task.delay(
             source_video_id=source_id,
             input_source=args.input_source,
             re_encode_for_qt=(not args.no_reencode), # Pass the calculated boolean
             overwrite_existing=args.overwrite
        )
        print(f"\nTask submission request sent successfully using .delay() for source_id={source_id}!")
        print("Monitor your Prefect worker logs or the Prefect UI for task execution.")
    except Exception as e:
        print(f"\nERROR submitting task to Prefect using .delay(): {e}")
        # This usually indicates a problem connecting to the Prefect API
        print("Check that your Prefect server is running and accessible.")
        print(f"Verify PREFECT_API_URL environment variable if set (should point to server API, e.g., http://127.0.0.1:4200/api)")
        sys.exit(1)

if __name__ == "__main__":
    main()
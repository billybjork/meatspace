import argparse
from pathlib import Path
import os
import sys

# Add project root to sys.path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

try:
    # Import the specific task function
    from tasks.intake import intake_task
    # Import DB utilities to create the initial record
    from utils.db_utils import get_db_connection
except ImportError as e:
    print(f"Error importing modules: {e}")
    print("Ensure you are running this script from the project root,")
    print("or that the 'tasks' and 'utils' directories are in your PYTHONPATH.")
    sys.exit(1)

def create_new_source_video_record(input_url_or_path: str, initial_title: str = None):
    """
    Inserts a new record into source_videos with state 'new', sets web_scraped flag,
    and returns the ID.
    """
    conn = None
    new_id = None
    is_url = input_url_or_path.lower().startswith(('http://', 'https://'))
    # Use provided title or derive a basic one from input
    title_to_insert = initial_title if initial_title else Path(input_url_or_path).stem[:250] # Use stem if no title given
    # Determine web_scraped flag
    web_scraped_flag = is_url

    print(f"Attempting to create source_videos record for: {input_url_or_path}")
    print(f"Using Title: {title_to_insert}")
    print(f"Setting web_scraped: {web_scraped_flag}")

    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            # Insert a basic record, including original_url and web_scraped flag
            # Filepath and filename are allowed NULL now
            cur.execute(
                """
                INSERT INTO source_videos (title, ingest_state, original_url, web_scraped)
                VALUES (%s, 'new', %s, %s)
                RETURNING id;
                """,
                (title_to_insert,
                 input_url_or_path if is_url else None, # Store URL if it's a URL
                 web_scraped_flag) # Set the flag
            )
            result = cur.fetchone()
            if result:
                new_id = result[0]
                print(f"Successfully created source_videos record with ID: {new_id}")
            else:
                print("Failed to get new ID after insert.")
                return None

        conn.commit() # Commit the new record
        return new_id

    except Exception as e:
        print(f"ERROR creating database record: {e}")
        if conn:
            conn.rollback()
        return None
    finally:
        if conn:
            conn.close()


def main():
    parser = argparse.ArgumentParser(description="Manually trigger the intake task for a single video.")
    parser.add_argument("input_source", help="The URL or local file path of the video to process.")
    parser.add_argument("--title", help="Optional initial title for the database record (recommended for local files).", default=None)
    parser.add_argument("--no-reencode", action="store_true", help="Skip the ffmpeg re-encoding step.")
    parser.add_argument("--overwrite", action="store_true", help="Force processing even if DB state isn't 'new' or final file exists.")

    args = parser.parse_args()

    # --- Input Validation (Optional but good) ---
    is_url = args.input_source.lower().startswith(('http://', 'https://'))
    if not is_url and not Path(args.input_source).exists():
         print(f"ERROR: Local file path provided does not exist: {args.input_source}")
         sys.exit(1)
    # You could warn if title is missing for local files
    if not is_url and not args.title:
         print("WARNING: Processing a local file without providing an explicit --title. Title will be derived from filename.")

    # 1. Create the DB record
    source_id = create_new_source_video_record(args.input_source, args.title)

    if not source_id:
        print("Failed to create database record. Aborting task submission.")
        sys.exit(1)

    # 2. Submit the Prefect task run using .defer()
    print(f"\nSubmitting intake_task for source_video_id: {source_id}...")
    print(f"  Input: {args.input_source}")
    print(f"  Re-encode: {not args.no_reencode}")
    print(f"  Overwrite: {args.overwrite}")

    try:
        # Use .defer() for submitting outside a flow to the backend/queue
        task_run_future = intake_task.defer(
            source_video_id=source_id,
            input_source=args.input_source,
            re_encode_for_qt=(not args.no_reencode),
            overwrite_existing=args.overwrite
        )

        print(f"\nTask submitted via defer()! (Run ID: {task_run_future.task_run_id if hasattr(task_run_future, 'task_run_id') else 'N/A'})")
        print("Monitor your Prefect worker logs or UI (if running) for progress.")
        print(f"Check the database `source_videos` table (id={source_id}) and your MEDIA_BASE_DIR/source_videos directory for results.")

    except Exception as e:
        print(f"\nERROR submitting task via defer(): {e}")
        print("Check database record state and logs for details.")
        # Attempt to clean up the record maybe? Or mark as failed?
        # For now, just exit. The record is in state 'new'.
        sys.exit(1)

if __name__ == "__main__":
    main()
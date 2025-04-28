import sys
import os
from pathlib import Path
from fasthtml.common import serve

from app import app

from dotenv import load_dotenv
load_dotenv()

# --- Configuration Value Reading ---
DATABASE_URL = os.getenv("DATABASE_URL")
CLOUDFRONT_DOMAIN = os.getenv("CLOUDFRONT_DOMAIN")

# --- Main Execution Block ---
if __name__ == "__main__":
    # Perform critical checks before attempting to run
    # The DB check happens implicitly during app startup's initialize_db_pool call
    # but checking here provides an earlier exit if needed.
    if not DATABASE_URL:
        print("FATAL ERROR: DATABASE_URL environment variable not set.", file=sys.stderr)
        sys.exit(1)

    # Check for static directory (relative to this main.py file)
    static_dir = Path(__file__).parent / 'static'
    if not static_dir.is_dir():
        print(f"---\nWARNING: Static directory missing at '{static_dir}'\n---", file=sys.stderr)

    # Informational print statements
    print(f"FastHTML server starting. Ensure '.env' is configured.")
    print(f"Sprites load from: {CLOUDFRONT_DOMAIN or 'Local Placeholder'}")
    print(f"Access UI at: http://127.0.0.1:5001/review (or check Uvicorn output)")

    # Run the server with the imported app
    # The startup event in app.py will handle initializing the DB pool
    try:
         serve(app=app) # Explicitly pass app instance
    except ConnectionError as e:
         print(f"Failed to start server due to DB connection issue during startup: {e}", file=sys.stderr)
         sys.exit(1)
    except Exception as e:
         print(f"An unexpected error occurred during server startup: {e}", file=sys.stderr)
         sys.exit(1)
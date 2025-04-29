import sys
import os
from pathlib import Path
from dotenv import load_dotenv
import uvicorn          # ← NEW

load_dotenv()

# --- Configuration ---
DATABASE_URL      = os.getenv("DATABASE_URL")
CLOUDFRONT_DOMAIN = os.getenv("CLOUDFRONT_DOMAIN")

if __name__ == "__main__":
    if not DATABASE_URL:
        print("FATAL ERROR: DATABASE_URL environment variable not set.", file=sys.stderr)
        sys.exit(1)

    static_dir = Path(__file__).parent / "static"
    if not static_dir.is_dir():
        print(f"---\nWARNING: Static directory missing at '{static_dir}'\n---", file=sys.stderr)

    print("FastHTML server starting. Ensure '.env' is configured.")
    print(f"Sprites load from: {CLOUDFRONT_DOMAIN or 'Local Placeholder'}")
    print("Access UI at: http://127.0.0.1:5001/review (or check Uvicorn output)")

    try:
        # Run Uvicorn directly with an import-string the reloader understands
        uvicorn.run(
            "app:app",               # module:variable
            host="0.0.0.0",
            port=5001,
            reload=True,
            reload_dirs=[str(Path(__file__).parent)],  # watch this project dir
        )
    except ConnectionError as e:
        print(f"Failed to start server due to DB connection issue during startup: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"An unexpected error occurred during server startup: {e}", file=sys.stderr)
        sys.exit(1)
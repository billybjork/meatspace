import uvicorn
import os
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import RedirectResponse, FileResponse

# --- Import application components ---
from config import log
from database import connect_db, close_db, get_db_connection
from routers.similarity_browser import router as search_router
from routers.intake_review import ui_router as review_ui_router, api_router as review_api_router

# --- Lifespan Management for Database Pool ---

@asynccontextmanager
async def lifespan(app_instance: FastAPI):
    """
    Context manager for application lifespan events.
    Handles asyncpg database connection pool creation and closing.
    """
    log.info("Lifespan: Startup phase starting...")
    pool = None # Initialize pool variable
    try:
        pool = await connect_db()
        # Store the pool in the application state for access in dependencies
        app_instance.state.db_pool = pool
        log.info("Lifespan: Database pool created and stored in app.state.db_pool.")
    except Exception as e:
        # Log error details provided by connect_db
        log.critical(f"FATAL LIFESPAN ERROR during startup: {e}", exc_info=True)
        # Depending on severity, you might want the app to fail startup
        # raise e # Re-raise to stop startup

    log.info("Lifespan: Startup phase complete. Yielding control to application.")
    yield  # Application runs here

    # --- Shutdown phase ---
    log.info("Lifespan: Shutdown phase starting...")
    # Retrieve the pool from app state to close it
    pool_to_close = getattr(app_instance.state, 'db_pool', None)
    # Use the close_db function from database.py
    await close_db(pool_to_close)
    log.info("Lifespan: Shutdown phase complete.")

# --- Create FastAPI Application Instance ---
app = FastAPI(
    title="Meatspace API",
    lifespan=lifespan # Use the lifespan context manager
)

# --- Static Files and Templates Setup ---
script_dir = os.path.dirname(os.path.abspath(__file__))
static_dir = os.path.join(script_dir, "static")
templates_dir = os.path.join(script_dir, "templates")

if not os.path.isdir(static_dir):
     log.warning(f"Static directory not found at: {static_dir}")
if not os.path.isdir(templates_dir):
     log.warning(f"Templates directory not found at: {templates_dir}")

app.mount("/static", StaticFiles(directory=static_dir), name="static")
templates = Jinja2Templates(directory=templates_dir)

# Store templates in app state so router functions can access it via request.app.state.templates
app.state.templates = templates
log.info("Static files and Jinja2 templates configured.")

# --- Include Routers ---
app.include_router(search_router)      # Includes routes like / and /query/{id}
app.include_router(review_ui_router)   # Includes /review
app.include_router(review_api_router)  # Includes /api/clips/... actions
log.info("Included API routers.")

# --- Root path redirect ---
# Redirects the bare domain access to the main search/index page
@app.get("/", include_in_schema=False)
async def root_redirect(request: Request):
    # Redirect to the named route 'index' from the search router
    return RedirectResponse(url=request.url_for('index'), status_code=303)

@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    return FileResponse(os.path.join(static_dir, "favicon.ico"))

# --- Main Execution ---
if __name__ == '__main__':
    log.info("Starting FastAPI server using Uvicorn...")
    # Get host/port from environment variables or use defaults
    host = os.getenv("HOST", "127.0.0.1")
    port = int(os.getenv("PORT", "8000"))
    reload = os.getenv("RELOAD", "true").lower() == "true" # Control reload via env var

    log.info(f"Server configured to run on {host}:{port} with reload={'enabled' if reload else 'disabled'}")

    # Point uvicorn to this file (main) and the 'app' object defined here
    uvicorn.run("main:app", host=host, port=port, reload=reload)
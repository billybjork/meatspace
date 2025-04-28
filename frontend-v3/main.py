import sys
import os
from pathlib import Path
# Ensure fasthtml.common imports necessary components like Script, Titled, etc.
from fasthtml.common import *
import datetime
import json # For embedding metadata
import math # For frame calculations
import time # Import time module for timing
import psycopg2 # Using psycopg2 for DB interaction
from psycopg2 import pool # Import the pool module
from psycopg2.extras import RealDictCursor
from starlette.requests import Request # For accessing query params easily
from starlette.exceptions import HTTPException
from starlette.staticfiles import StaticFiles # To explicitly serve static files
from typing import Optional, Any, List, Dict # Added Any
import contextlib # For managing pool connection context

# --- Load Environment Variables ---
from dotenv import load_dotenv
load_dotenv() # Load variables from .env file into environment

# --- Configuration ---
DATABASE_URL = os.getenv("DATABASE_URL")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
CLOUDFRONT_DOMAIN = os.getenv("CLOUDFRONT_DOMAIN") # Read CloudFront domain
# Min/Max pool size - adjust as needed
DB_MIN_CONN = int(os.getenv("DB_MIN_CONN", 1))
DB_MAX_CONN = int(os.getenv("DB_MAX_CONN", 5))

# --- Check essential configuration ---
if not DATABASE_URL:
    print("FATAL ERROR: DATABASE_URL environment variable not set.", file=sys.stderr)
    sys.exit(1)
if not S3_BUCKET_NAME:
    print("WARNING: S3_BUCKET_NAME environment variable not set.", file=sys.stderr)
if not CLOUDFRONT_DOMAIN:
     print("INFO: CLOUDFRONT_DOMAIN environment variable not set. Falling back to placeholder for sprite URLs.", file=sys.stderr)

# --- Global Connection Pool ---
db_pool: Optional[psycopg2.pool.SimpleConnectionPool] = None

def initialize_db_pool():
    """Initializes the database connection pool."""
    global db_pool
    if db_pool: # Avoid re-initializing if already done
        print("DB Pool already initialized.")
        return
    print(f"Initializing DB Pool (Min: {DB_MIN_CONN}, Max: {DB_MAX_CONN})...")
    try:
        db_pool = psycopg2.pool.SimpleConnectionPool(
            DB_MIN_CONN, DB_MAX_CONN, dsn=DATABASE_URL, cursor_factory=RealDictCursor
        )
        # Test connection
        conn = db_pool.getconn()
        print(f"DB Pool Initialized. Test connection status: {conn.status}")
        db_pool.putconn(conn)
        print("Test connection returned to pool.")
    except psycopg2.OperationalError as e:
        print(f"FATAL: Database connection pool initialization failed: {e}", file=sys.stderr)
        db_pool = None
        sys.exit(f"Exiting due to DB pool initialization failure: {e}")
    except Exception as e:
        print(f"FATAL: Unexpected error initializing DB pool: {e}", file=sys.stderr)
        db_pool = None
        sys.exit(f"Exiting due to unexpected DB pool initialization error: {e}")

@contextlib.contextmanager
def get_db_conn_from_pool() -> Any:
    """Provides a connection from the pool using a context manager."""
    if db_pool is None:
        # Attempt to initialize if not already done (e.g., if startup event failed)
        print("Warning: DB Pool was not initialized. Attempting now...")
        initialize_db_pool()
        if db_pool is None: # Still failed
             raise ConnectionError("Database pool is not initialized and initialization failed.")
    conn = None
    try:
        conn = db_pool.getconn()
        yield conn
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as e:
        if conn: conn.rollback()
        print(f"DB Error in context manager: Transaction rolled back. {type(e).__name__}: {e}", file=sys.stderr)
        raise # Re-raise the exception after rollback
    finally:
        if conn: db_pool.putconn(conn)

def close_db_pool():
    """Closes all connections in the pool."""
    global db_pool
    if db_pool:
        print("Closing DB Pool...")
        db_pool.closeall()
        db_pool = None
        print("DB Pool Closed.")

# --- Database Helper Functions (Use pool context manager) ---
def fetch_one(sql, params=()):
    """Fetches a single row using a pooled connection."""
    try:
        with get_db_conn_from_pool() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                return cur.fetchone()
    except Exception as e:
        print(f"Error in fetch_one execution: {type(e).__name__} - {e}", file=sys.stderr)
        return None

def execute_sql(sql, params=()):
    """Executes SQL (INSERT, UPDATE, DELETE) using a pooled connection."""
    rows_affected = 0
    try:
        with get_db_conn_from_pool() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                rows_affected = cur.rowcount
        return rows_affected
    except Exception as e:
         print(f"Error in execute_sql execution: {type(e).__name__} - {e}", file=sys.stderr)
         return -1

# --- Specific DB Interaction Functions ---
# MODIFIED: Accepts optional parameters to find clip *after* a specific one
def get_next_pending_review_clip(after_updated_at: Optional[datetime.datetime] = None, after_id: Optional[int] = None):
    """Fetches the next clip in 'pending_review' state, optionally after a given clip."""
    sql_base = """
        SELECT c.*, sv.fps AS source_fps
        FROM clips c JOIN source_videos sv ON c.source_video_id = sv.id
        WHERE c.ingest_state = 'pending_review'"""
    params = []
    where_clauses = []
    if after_updated_at is not None and after_id is not None:
        where_clauses.append("(c.updated_at > %s OR (c.updated_at = %s AND c.id > %s))")
        params.extend([after_updated_at, after_updated_at, after_id])
    sql_where = " AND " + " AND ".join(where_clauses) if where_clauses else ""
    sql_order_limit = " ORDER BY c.updated_at ASC, c.id ASC LIMIT 1;"
    final_sql = sql_base + sql_where + sql_order_limit
    return fetch_one(final_sql, tuple(params))

def get_clip_by_id(clip_id):
    """Fetches a specific clip by its ID, joining with source_videos."""
    sql = "SELECT c.*, sv.fps AS source_fps FROM clips c JOIN source_videos sv ON c.source_video_id = sv.id WHERE c.id = %s;"
    return fetch_one(sql, (clip_id,))

def get_sprite_artifact(clip_id):
    """Fetches the latest sprite sheet artifact record for a clip."""
    sql = "SELECT * FROM clip_artifacts WHERE clip_id = %s AND artifact_type = 'sprite_sheet' ORDER BY created_at DESC LIMIT 1;"
    return fetch_one(sql, (clip_id,))

def insert_clip_event(clip_id, action, reviewer_id=None, event_data=None):
    """Logs an event to the clip_events table."""
    sql = "INSERT INTO clip_events (clip_id, action, reviewer_id, event_data, created_at) VALUES (%s, %s, %s, %s, NOW()) RETURNING id;"
    json_event_data = json.dumps(event_data) if isinstance(event_data, dict) else event_data
    params = (clip_id, action, reviewer_id, json_event_data)
    try:
        with get_db_conn_from_pool() as conn: # Use pool context manager
            with conn.cursor() as cur:
                cur.execute(sql, params)
                result = cur.fetchone()
                return result['id'] if result else None
    except Exception as e:
        print(f"DB Error (insert_clip_event): {e}", file=sys.stderr)
        return None

# --- FT Components ---
def get_sprite_display_url(s3_key):
    """Constructs the public URL for the sprite sheet."""
    if s3_key and CLOUDFRONT_DOMAIN:
        domain = CLOUDFRONT_DOMAIN.strip('/')
        key = s3_key.lstrip('/')
        url = f"https://{domain}/{key}"
        return url
    return "/static/placeholder_sprite.png"

def clip_display(clip, sprite_artifact):
    """Renders the clip info, player UI elements, and embedded metadata."""
    if not clip: return P("Clip data missing or loading error.")
    clip_id = clip.get('id')
    sprite_metadata = sprite_artifact.get('metadata') if sprite_artifact else None
    s3_key = sprite_artifact.get('s3_key') if sprite_artifact else None
    sprite_url = get_sprite_display_url(s3_key)
    player_meta = {"isValid": False, "spriteUrl": sprite_url,"cols": 0, "rows": 0, "tile_width": 0, "tile_height_calculated": None,"total_sprite_frames": 0, "clip_fps": 0, "clip_total_frames": 0}
    if sprite_metadata and isinstance(sprite_metadata, dict):
        player_meta.update({ "cols": sprite_metadata.get('cols', 0), "rows": sprite_metadata.get('rows', 0), "tile_width": sprite_metadata.get('tile_width', 0), "tile_height_calculated": sprite_metadata.get('tile_height_calculated'), "total_sprite_frames": sprite_metadata.get('total_sprite_frames', 0), "clip_fps": sprite_metadata.get('clip_fps_source') or clip.get('source_fps') or 0, "clip_total_frames": sprite_metadata.get('clip_total_frames_source') or 0 })
        is_valid_dimensions = (player_meta["cols"] > 0 and player_meta["rows"] > 0 and player_meta["tile_width"] > 0 and player_meta["tile_height_calculated"] and player_meta["tile_height_calculated"] > 0)
        is_valid_frames = (player_meta["total_sprite_frames"] > 0 and player_meta["clip_fps"] > 0)
        if is_valid_dimensions and is_valid_frames:
            if player_meta["clip_total_frames"] <= 0:
                 start_time = clip.get('start_time_seconds', 0); end_time = clip.get('end_time_seconds', 0)
                 duration = max(0, end_time - start_time)
                 if duration > 0 and player_meta["clip_fps"] > 0: player_meta["clip_total_frames"] = math.ceil(duration * player_meta["clip_fps"]) + 1
                 else: player_meta["clip_total_frames"] = 0
            if player_meta["clip_total_frames"] > 0: player_meta["isValid"] = True
    viewer_id = f"viewer-{clip_id}"; scrub_id = f"scrub-{clip_id}"; playpause_id = f"playpause-{clip_id}"
    frame_display_id = f"frame-display-{clip_id}"; meta_script_id = f"meta-{clip_id}"
    controls_disabled = not player_meta["isValid"]
    player_container = Div(
        Div(id=viewer_id, cls="sprite-viewer bg-gray-200 border border-gray-400", style="width: 320px; height: 180px; background-repeat: no-repeat; overflow: hidden; margin: 0 auto; background-position: 0px 0px;"),
        Div( Button("⏯️", id=playpause_id, cls="play-pause-btn", disabled=controls_disabled, style="font-size: 1.2em; min-width: 40px;"), Input(type="range", id=scrub_id, min="0", value="0", step="1", disabled=controls_disabled, style="flex-grow: 1; margin: 0 0.5rem;"), Span("Frame: 0", id=frame_display_id, style="min-width: 80px; text-align: right; font-family: monospace;"), cls="sprite-controls flex items-center mt-2" ),
        Script(json.dumps(player_meta), type="application/json", id=meta_script_id), data_clip_id=clip_id, cls="clip-player-instance mb-4 p-3 border rounded shadow" )
    clip_info = Div( H3(f"Clip Details (ID: {clip_id})"), P(f"Source Video ID: {clip.get('source_video_id', 'N/A')}", Title(f"Clip Path: {clip.get('clip_filepath', 'N/A')}")), P(f"Time: {clip.get('start_time_seconds', 0):.2f}s - {clip.get('end_time_seconds', 0):.2f}s"), P(f"Sprite Valid: {'Yes' if player_meta['isValid'] else 'No'}", style="font-size: 0.8rem; color: grey;"), cls="clip-metadata mt-2 text-sm" )
    return Div(player_container, clip_info, cls="clip-display-container")

def action_buttons(clip_id):
    """Renders action buttons targeting the main review container."""
    if not clip_id: return Div()
    actions = { "approve": ("Approve (A)", "primary"), "skip": ("Skip (S)", "secondary"), "archive": ("Archive (X)", "contrast") }
    buttons = []
    for key, (label, css_class) in actions.items():
        buttons.append( Button(label, hx_post=f"/api/clip/{clip_id}/select?action={key}", hx_target="#review-container", hx_swap="outerHTML", data_action=key, cls=css_class) )
    return Div(*buttons, cls="action-buttons", style="display: flex; gap: 1rem; margin-bottom: 1rem;")

def undo_toast_component(undo_context):
    """Renders the Undo toast if context exists, otherwise empty Div."""
    if not undo_context or 'clip_id' not in undo_context or 'action' not in undo_context:
        return Div(id="undo-area") # Return placeholder with consistent ID
    clip_id = undo_context['clip_id']; action_taken = undo_context['action']; toast_id = f"undo-toast-{clip_id}"
    return Div( Span(f"Last action: '{action_taken}' on clip {clip_id}. "), Button("Undo (Z)", hx_post=f"/api/clip/{clip_id}/select?action=undo", hx_target="#review-container", hx_swap="outerHTML", cls="secondary outline small", data_action="undo"), id="undo-area", _toast_instance_id=toast_id, style="padding: 0.5rem; background-color: #e0f2f7; border: 1px solid #b3e5fc; border-radius: 4px; margin-top: 1rem;" )

# Main Page Structure - UPDATED for preloading
def review_page_content(clip_to_display, sprite_artifact_display, next_clip_sprite_url=None, undo_context=None):
    """Renders the main content area, including clip display, undo toast, and preload."""
    preload_elements = ()
    if next_clip_sprite_url:
        preload_elements = ( Img(src=next_clip_sprite_url, style="position:absolute; left:-9999px; top:-9999px; width:1px; height:1px; opacity:0;", alt="preload", loading="lazy"), )
    return Div(
        clip_display(clip_to_display, sprite_artifact_display) if clip_to_display else P("Loading next clip or none available..."),
        action_buttons(clip_to_display['id'] if clip_to_display else None),
        undo_toast_component(undo_context),
        *preload_elements,
        id="review-container"
    )

def no_clips_ui(next_clip_sprite_url=None, undo_context=None):
    """Renders the 'no clips' message, including potential undo toast and preload."""
    preload_elements = ()
    if next_clip_sprite_url: preload_elements = ( Img(src=next_clip_sprite_url, style="position:absolute; left:-9999px; top:-9999px; width:1px; height:1px; opacity:0;", alt="preload", loading="lazy"), )
    return Div( H2("All clips reviewed!"), P("Check back later for more."), undo_toast_component(undo_context), *preload_elements, id="review-container" )

# --- FastHTML App Setup ---
# Define headers tuple to pass to fast_app
# REMOVED: custom_headers definition was here, now defined below before fast_app call
app_headers = (
    Script(src="/static/js/sprite-player.js", defer=True),
    Script(src="/static/js/review-main.js", defer=True),
)
# Enable sessions for storing undo_context
# Pass headers directly here
app, rt = fast_app(hdrs=app_headers, secret_key='replace-with-a-real-secret-key-or-omit-for-auto-gen')

# --- Add Startup/Shutdown Events for Pool ---
@app.on_event("startup")
async def startup_event():
    initialize_db_pool()

@app.on_event("shutdown")
async def shutdown_event():
    close_db_pool()

# --- Serve Static Files ---
static_dir = Path(__file__).parent / 'static'
if static_dir.is_dir():
    app.mount('/static', StaticFiles(directory=static_dir), name='static')
    print(f"Serving static files from: {static_dir}")
else:
    print(f"WARNING: Static directory not found at {static_dir}. JS/CSS/Images might not load.", file=sys.stderr)


# --- Routes ---
# --- Routes ---
@rt("/review")
def get(session: dict):
    # --- Consolidate DB Calls for Initial Load ---
    handler_start_time = time.time()
    undo_context = session.get('undo_context', None)
    clip = None
    sprite_artifact = None
    next_clip_for_preload = None
    next_clip_sprite_url = None

    try:
        with get_db_conn_from_pool() as conn: # Single connection block
            # Fetch first clip
            t_fetch1_start = time.time()
            sql_first = """
                SELECT c.*, sv.fps AS source_fps FROM clips c JOIN source_videos sv ON c.source_video_id = sv.id
                WHERE c.ingest_state = 'pending_review' ORDER BY c.updated_at ASC, c.id ASC LIMIT 1;"""
            with conn.cursor() as cur:
                cur.execute(sql_first)
                clip = cur.fetchone()
            t_fetch1_end = time.time()
            print(f"GET /review: DB Fetch Time (First Clip): {t_fetch1_end - t_fetch1_start:.4f}s")

            if clip:
                # Fetch its artifact
                t_art1_start = time.time()
                sql_art1 = "SELECT * FROM clip_artifacts WHERE clip_id = %s AND artifact_type = 'sprite_sheet' ORDER BY created_at DESC LIMIT 1;"
                with conn.cursor() as cur:
                    cur.execute(sql_art1, (clip['id'],))
                    sprite_artifact = cur.fetchone()
                t_art1_end = time.time()
                print(f"GET /review: DB Fetch Time (Artifact 1): {t_art1_end - t_art1_start:.4f}s")

                # Fetch next clip for preload
                t_fetch2_start = time.time()
                sql_next = """
                    SELECT c.id, c.updated_at FROM clips c
                    WHERE c.ingest_state = 'pending_review'
                    AND (c.updated_at > %s OR (c.updated_at = %s AND c.id > %s))
                    ORDER BY c.updated_at ASC, c.id ASC LIMIT 1;"""
                with conn.cursor() as cur:
                    cur.execute(sql_next, (clip['updated_at'], clip['updated_at'], clip['id']))
                    next_clip_for_preload = cur.fetchone() # Just need ID and updated_at for next query if needed
                t_fetch2_end = time.time()
                print(f"GET /review: DB Fetch Time (Next Clip ID): {t_fetch2_end - t_fetch2_start:.4f}s")

                if next_clip_for_preload:
                    # Fetch next clip's artifact
                    t_art2_start = time.time()
                    sql_art2 = "SELECT s3_key FROM clip_artifacts WHERE clip_id = %s AND artifact_type = 'sprite_sheet' ORDER BY created_at DESC LIMIT 1;"
                    with conn.cursor() as cur:
                        cur.execute(sql_art2, (next_clip_for_preload['id'],))
                        next_sprite_artifact_preload = cur.fetchone()
                    t_art2_end = time.time()
                    print(f"GET /review: DB Fetch Time (Artifact 2): {t_art2_end - t_art2_start:.4f}s")
                    if next_sprite_artifact_preload:
                        next_clip_sprite_url = get_sprite_display_url(next_sprite_artifact_preload.get('s3_key'))

    except Exception as e:
        print(f"ERROR during GET /review DB operations: {e}", file=sys.stderr)
        # Handle error appropriately, maybe return an error page component

    title = "Review Clips"
    t_render_start = time.time()
    content = review_page_content(clip, sprite_artifact, next_clip_sprite_url, undo_context) if clip else no_clips_ui(undo_context=undo_context)
    t_render_end = time.time()
    print(f"GET /review: FT Render Time: {t_render_end - t_render_start:.4f}s")

    handler_end_time = time.time()
    print(f"Total GET /review handler time: {handler_end_time - handler_start_time:.4f}s")
    return Titled(title, content)


@rt("/api/clip/{clip_id:int}/select")
def post(clip_id: int, req: Request, session: dict):
    handler_start_time = time.time()
    action = req.query_params.get('action'); reviewer = "reviewer_placeholder"; final_html = ""
    if not action: print("Error: Action missing", file=sys.stderr); return ""
    print(f"Received action '{action}' for clip {clip_id}")

    # Use a single DB block for the entire request logic
    try:
        with get_db_conn_from_pool() as conn: # Get connection once
            # --- Handle Undo Action ---
            if action == 'undo':
                t_undo_start = time.time()
                last_undo_context = session.pop('undo_context', None)
                clip_to_show = None; event_id = None; next_clip_for_preload = None; next_url = None; sprite_artifact_to_show = None

                with conn.cursor() as cur: # Use same connection
                    if not last_undo_context or last_undo_context.get('clip_id') != clip_id:
                        print(f"Warning: Undo context mismatch for clip {clip_id}.")
                        # Fetch the clip they tried to undo
                        cur.execute("SELECT c.*, sv.fps AS source_fps FROM clips c JOIN source_videos sv ON c.source_video_id = sv.id WHERE c.id = %s;", (clip_id,))
                        clip_to_show = cur.fetchone()
                    else:
                        # Log undo event
                        sql_event = "INSERT INTO clip_events (clip_id, action, reviewer_id, created_at) VALUES (%s, %s, %s, NOW()) RETURNING id;"
                        cur.execute(sql_event, (clip_id, 'undo', reviewer))
                        result = cur.fetchone(); event_id = result['id'] if result else None
                        if event_id: print(f"Undo event logged (ID: {event_id})")
                        else: print(f"Error inserting undo event for {clip_id}", file=sys.stderr)
                        # Fetch the undone clip
                        cur.execute("SELECT c.*, sv.fps AS source_fps FROM clips c JOIN source_videos sv ON c.source_video_id = sv.id WHERE c.id = %s;", (clip_id,))
                        clip_to_show = cur.fetchone()

                    # Fetch data needed for rendering
                    if clip_to_show:
                        # Artifact for clip_to_show
                        cur.execute("SELECT * FROM clip_artifacts WHERE clip_id = %s AND artifact_type = 'sprite_sheet' ORDER BY created_at DESC LIMIT 1;", (clip_to_show['id'],))
                        sprite_artifact_to_show = cur.fetchone()
                        # Next clip for preload
                        sql_next = """SELECT c.id, c.updated_at FROM clips c WHERE c.ingest_state = 'pending_review'
                                    AND (c.updated_at > %s OR (c.updated_at = %s AND c.id > %s)) ORDER BY c.updated_at ASC, c.id ASC LIMIT 1;"""
                        cur.execute(sql_next, (clip_to_show['updated_at'], clip_to_show['updated_at'], clip_to_show['id']))
                        next_clip_for_preload = cur.fetchone()
                        if next_clip_for_preload:
                            cur.execute("SELECT s3_key FROM clip_artifacts WHERE clip_id = %s AND artifact_type = 'sprite_sheet' ORDER BY created_at DESC LIMIT 1;", (next_clip_for_preload['id'],))
                            art = cur.fetchone()
                            if art: next_url = get_sprite_display_url(art.get('s3_key'))

                # Render outside the cursor/conn block if possible, or pass data out
                if clip_to_show: final_html = review_page_content(clip_to_show, sprite_artifact_to_show, next_url, undo_context=None)
                else: final_html = no_clips_ui(undo_context=None)
                t_undo_end = time.time(); print(f"Undo Handler DB + Render Time: {t_undo_end - t_undo_start:.4f}s")

            # --- Handle Regular Actions ---
            else:
                t_regular_start = time.time()
                current_clip = None; next_clip = None; clip_after_next = None;
                next_sprite_artifact = None; artifact_after_next = None; event_id = None;
                url_after_next = None; new_undo_context = None

                with conn.cursor() as cur: # Use same connection
                    # Validate current clip
                    t_val_fetch_start = time.time()
                    cur.execute("SELECT c.*, sv.fps AS source_fps FROM clips c JOIN source_videos sv ON c.source_video_id = sv.id WHERE c.id = %s;", (clip_id,))
                    current_clip = cur.fetchone()
                    t_val_fetch_end = time.time(); print(f"DB Fetch Time (Validation Clip): {t_val_fetch_end - t_val_fetch_start:.4f}s")

                    validation_passed = current_clip and current_clip.get('ingest_state') == 'pending_review'

                    if validation_passed:
                        # Log event
                        t_log_start = time.time()
                        sql_event = "INSERT INTO clip_events (clip_id, action, reviewer_id, created_at) VALUES (%s, %s, %s, NOW()) RETURNING id;"
                        cur.execute(sql_event, (clip_id, f"selected_{action}", reviewer))
                        result = cur.fetchone(); event_id = result['id'] if result else None
                        t_log_end = time.time()
                        if not event_id: print(f"Error logging event for action '{action}', clip {clip_id}", file=sys.stderr); final_html = f"<div>Error logging action '{action}'.</div>"
                        else:
                            print(f"Event logged (ID: {event_id}): Time: {t_log_end - t_log_start:.4f}s")
                            new_undo_context = {'clip_id': clip_id, 'action': action} # Prepare undo context

                    # Fetch next clips and artifacts regardless of validation outcome (unless error above)
                    # This simplifies flow - we show next clip even if validation failed
                    t_fetch_start = time.time()
                    # Use ordering info from 'current_clip' if validation passed, otherwise fetch first pending
                    after_ts = current_clip.get('updated_at') if validation_passed else None
                    after_id = current_clip.get('id') if validation_passed else None
                    sql_next = """SELECT c.*, sv.fps AS source_fps FROM clips c JOIN source_videos sv ON c.source_video_id = sv.id
                                WHERE c.ingest_state = 'pending_review' """
                    params_next = []
                    if after_ts is not None and after_id is not None:
                        sql_next += " AND (c.updated_at > %s OR (c.updated_at = %s AND c.id > %s)) "
                        params_next.extend([after_ts, after_ts, after_id])
                    sql_next += " ORDER BY c.updated_at ASC, c.id ASC LIMIT 1;"
                    cur.execute(sql_next, tuple(params_next))
                    next_clip = cur.fetchone()

                    if next_clip:
                        sql_next_next = """SELECT c.id, c.updated_at FROM clips c WHERE c.ingest_state = 'pending_review'
                                         AND (c.updated_at > %s OR (c.updated_at = %s AND c.id > %s)) ORDER BY c.updated_at ASC, c.id ASC LIMIT 1;"""
                        cur.execute(sql_next_next, (next_clip['updated_at'], next_clip['updated_at'], next_clip['id']))
                        clip_after_next = cur.fetchone() # Just need ID / timestamp
                    t_fetch_end = time.time(); print(f"DB Fetch Time (next + next-next clips): {t_fetch_end - t_fetch_start:.4f}s")

                    # Fetch artifacts
                    t_artifact_start = time.time()
                    if next_clip:
                        cur.execute("SELECT * FROM clip_artifacts WHERE clip_id = %s AND artifact_type = 'sprite_sheet' ORDER BY created_at DESC LIMIT 1;", (next_clip['id'],))
                        next_sprite_artifact = cur.fetchone()
                    if clip_after_next:
                        cur.execute("SELECT s3_key FROM clip_artifacts WHERE clip_id = %s AND artifact_type = 'sprite_sheet' ORDER BY created_at DESC LIMIT 1;", (clip_after_next['id'],))
                        artifact_after_next = cur.fetchone()
                        if artifact_after_next: url_after_next = get_sprite_display_url(artifact_after_next.get('s3_key'))
                    t_artifact_end = time.time(); print(f"DB Fetch Time (artifacts): {t_artifact_end - t_artifact_start:.4f}s")

                # Outside the 'with cursor' block now, commit/rollback handled by outer 'with conn'

                # Update session only if validation passed and event was logged
                if validation_passed and event_id:
                    session['undo_context'] = new_undo_context
                elif not validation_passed:
                     print(f"Validation failed for action '{action}' on clip {clip_id}. State: {current_clip.get('ingest_state') if current_clip else 'Not Found'}")
                     # Keep existing undo context if validation failed
                     new_undo_context = session.get('undo_context', None)
                else: # Event logging failed
                     new_undo_context = session.get('undo_context', None) # Keep existing

                # Render
                t_render_start = time.time()
                if next_clip: final_html = review_page_content(next_clip, next_sprite_artifact, url_after_next, new_undo_context)
                else: final_html = no_clips_ui(undo_context=new_undo_context)
                t_render_end = time.time(); print(f"FT Render Time: {t_render_end - t_render_start:.4f}s")

                t_regular_end = time.time(); print(f"Regular Action Handler Time: {t_regular_end - t_regular_start:.4f}s")

    # --- Handle potential DB errors from the 'with' block ---
    except (Exception, psycopg2.DatabaseError) as e:
        print(f"ERROR during POST /api/clip/... DB operations: {type(e).__name__} - {e}", file=sys.stderr)
        # Attempt to return a safe UI state - maybe the "no clips" UI with an error message?
        # For now, return a simple error message fragment. The target is #review-container.
        final_html = Div( H3("Database Error"), P("Could not process action. Please try again later."), id="review-container")
        # Ensure undo context isn't left in a misleading state if DB failed mid-request
        session.pop('undo_context', None) # Clear possibly inconsistent undo context


    handler_end_time = time.time()
    print(f"Total POST handler time for action '{action}' on clip {clip_id}: {handler_end_time - handler_start_time:.4f}s")
    if not final_html: print(f"ERROR: final_html was not set for action '{action}' on clip {clip_id}."); return ""
    return final_html

# --- Run Server ---
if __name__ == "__main__":
    # Pool initialized by startup event
    if not static_dir.is_dir(): print(f"---\nWARNING: Static directory missing at '{static_dir}'\n---")
    print(f"FastHTML server starting. Ensure '.env' is configured.")
    print(f"Sprites load from: {CLOUDFRONT_DOMAIN or 'Local Placeholder'}")
    print(f"Access UI at: http://127.0.0.1:5001/review (or check Uvicorn output)")
    serve()
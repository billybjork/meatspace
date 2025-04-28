# --- START OF FILE main.py ---

import sys
import os
from pathlib import Path
# Ensure fasthtml.common imports necessary components like Script, Titled, etc.
from fasthtml.common import *
import datetime
import json # For embedding metadata
import math # For frame calculations
import time # Still needed for insert_clip_event internal timing if uncommented
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
    if db_pool:
        return
    print(f"Initializing DB Pool (Min: {DB_MIN_CONN}, Max: {DB_MAX_CONN})...")
    try:
        db_pool = psycopg2.pool.SimpleConnectionPool(
            DB_MIN_CONN, DB_MAX_CONN, dsn=DATABASE_URL, cursor_factory=RealDictCursor
        )
        conn = db_pool.getconn()
        print(f"DB Pool Initialized. Test connection successful.")
        db_pool.putconn(conn)
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
        print("Warning: DB Pool was not initialized. Attempting now...")
        initialize_db_pool()
        if db_pool is None:
             raise ConnectionError("Database pool is not initialized and initialization failed.")
    conn = None
    try:
        conn = db_pool.getconn()
        yield conn
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as e:
        if conn: conn.rollback()
        print(f"DB Error in context manager: Transaction rolled back. {type(e).__name__}: {e}", file=sys.stderr)
        raise
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

# --- Database Helper Functions (Originals kept for potential other uses) ---
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

# MODIFIED insert_clip_event to accept an optional cursor
def insert_clip_event(clip_id, action, reviewer_id=None, event_data=None, *, cur: Optional[psycopg2.extensions.cursor] = None) -> Optional[int]:
    """
    Logs an event to the clip_events table.
    If 'cur' is provided, uses the existing cursor/transaction.
    Otherwise, creates its own connection and transaction.
    """
    sql = "INSERT INTO clip_events (clip_id, action, reviewer_id, event_data, created_at) VALUES (%s, %s, %s, %s, NOW()) RETURNING id;"
    json_event_data = json.dumps(event_data) if isinstance(event_data, dict) else event_data
    params = (clip_id, action, reviewer_id, json_event_data)
    event_id = None
    t_start = time.time() # Keep internal timer for potential debugging

    try:
        if cur:
            # Use the provided cursor (part of an existing transaction)
            cur.execute(sql, params)
            result = cur.fetchone()
            event_id = result['id'] if result else None
        else:
            # Create a separate connection and transaction
            with get_db_conn_from_pool() as conn:
                with conn.cursor() as new_cur:
                    new_cur.execute(sql, params)
                    result = new_cur.fetchone()
                    event_id = result['id'] if result else None
        t_end = time.time()
        # Internal timing log (can be uncommented for debugging specific inserts)
        # print(f"    DB Timing (insert_clip_event internal): Actual DB op took {t_end - t_start:.4f}s (Using {'Existing' if cur else 'New'} Cursor)")

    except Exception as e:
        t_end = time.time()
        print(f"DB Error (insert_clip_event internal, {t_end - t_start:.4f}s): {e}", file=sys.stderr)

    # Overall log for success/failure
    if event_id:
         print(f"Logged event '{action}' for clip {clip_id} (Event ID: {event_id})")
    else:
         print(f"ERROR logging event '{action}' for clip {clip_id}", file=sys.stderr)
    return event_id


# --- FT Components ---
def get_sprite_display_url(s3_key):
    """Constructs the public URL for the sprite sheet."""
    if s3_key and CLOUDFRONT_DOMAIN:
        domain = CLOUDFRONT_DOMAIN.strip('/')
        key = s3_key.lstrip('/')
        url = f"https://{domain}/{key}"
        return url
    return "/static/placeholder_sprite.png"

def clip_display(clip, sprite_artifact): # sprite_artifact now less used due to JOIN
    """Renders the clip info, player UI elements, and embedded metadata."""
    if not clip: return P("Clip data missing or loading error.")
    clip_id = clip.get('id')
    s3_key = clip.get('sprite_s3_key') # Prioritize joined key
    sprite_metadata = clip.get('sprite_metadata') # Prioritize joined metadata

    if not s3_key and sprite_artifact: s3_key = sprite_artifact.get('s3_key')
    if not sprite_metadata and sprite_artifact: sprite_metadata = sprite_artifact.get('metadata')

    sprite_url = get_sprite_display_url(s3_key)

    player_meta = {"isValid": False, "spriteUrl": sprite_url,"cols": 0, "rows": 0, "tile_width": 0, "tile_height_calculated": None,"total_sprite_frames": 0, "clip_fps": 0, "clip_total_frames": 0}

    if sprite_metadata and isinstance(sprite_metadata, dict):
        player_meta.update({
            "cols": sprite_metadata.get('cols', 0),
            "rows": sprite_metadata.get('rows', 0),
            "tile_width": sprite_metadata.get('tile_width', 0),
            "tile_height_calculated": sprite_metadata.get('tile_height_calculated'),
            "total_sprite_frames": sprite_metadata.get('total_sprite_frames', 0),
            "clip_fps": sprite_metadata.get('clip_fps_source') or clip.get('source_fps') or 0,
            "clip_total_frames": sprite_metadata.get('clip_total_frames_source') or 0
        })
        is_valid_dimensions = (player_meta["cols"] > 0 and player_meta["rows"] > 0 and
                               player_meta["tile_width"] > 0 and player_meta["tile_height_calculated"] and
                               player_meta["tile_height_calculated"] > 0)
        is_valid_frames_meta = player_meta["total_sprite_frames"] > 0
        has_fps = player_meta["clip_fps"] > 0

        if player_meta["clip_total_frames"] <= 0 and has_fps:
            start_time = clip.get('start_time_seconds', 0); end_time = clip.get('end_time_seconds', 0)
            try:
                duration = max(0, float(end_time) - float(start_time))
                if duration > 0:
                    player_meta["clip_total_frames"] = math.ceil(duration * player_meta["clip_fps"]) + 1
                else: player_meta["clip_total_frames"] = 0
            except (TypeError, ValueError):
                player_meta["clip_total_frames"] = 0
        is_valid_timing = has_fps and player_meta["clip_total_frames"] > 0
        player_meta["isValid"] = is_valid_dimensions and is_valid_frames_meta and is_valid_timing

    viewer_id = f"viewer-{clip_id}"; scrub_id = f"scrub-{clip_id}"; playpause_id = f"playpause-{clip_id}"
    frame_display_id = f"frame-display-{clip_id}"; meta_script_id = f"meta-{clip_id}"
    controls_disabled = not player_meta["isValid"]
    viewer_width = f"{player_meta['tile_width']}px" if player_meta["isValid"] and player_meta.get('tile_width') else "320px"
    viewer_height = f"{player_meta['tile_height_calculated']}px" if player_meta["isValid"] and player_meta.get('tile_height_calculated') else "180px"

    player_container = Div(
        Div(id=viewer_id, cls="sprite-viewer bg-gray-200 border border-gray-400",
            style=f"width: {viewer_width}; height: {viewer_height}; background-repeat: no-repeat; overflow: hidden; margin: 0 auto; background-position: 0px 0px;"),
        Div(
            Button("⏯️", id=playpause_id, cls="play-pause-btn", disabled=controls_disabled, style="font-size: 1.2em; min-width: 40px;", aria_label="Play or pause clip animation"),
            Input(type="range", id=scrub_id, min="0", value="0", step="1", disabled=controls_disabled, style="flex-grow: 1; margin: 0 0.5rem;", aria_label="Scrub through clip frames"),
            Span("Frame: 0", id=frame_display_id, style="min-width: 80px; text-align: right; font-family: monospace;", aria_live="polite"),
            cls="sprite-controls flex items-center mt-2"
        ),
        Script(json.dumps(player_meta), type="application/json", id=meta_script_id),
        data_clip_id=clip_id, cls="clip-player-instance mb-4 p-3 border rounded shadow"
    )

    clip_info = Div(
        H3(f"Clip Details (ID: {clip_id})"),
        P(f"Source Video ID: {clip.get('source_video_id', 'N/A')}", Title(f"Clip Path: {clip.get('clip_filepath', 'N/A')}")),
        P(f"Time: {clip.get('start_time_seconds', 0):.2f}s - {clip.get('end_time_seconds', 0):.2f}s"),
        P(f"Sprite Valid: {'Yes' if player_meta['isValid'] else 'No'}", style="font-size: 0.8rem; color: grey;"),
        cls="clip-metadata mt-2 text-sm"
    )

    return Div(player_container, clip_info, cls="clip-display-container")

def action_buttons(clip_id):
    """Renders action buttons targeting the main review container."""
    if not clip_id: return Div()
    actions = {
        "approve": ("Approve (A)", "primary", "a"),
        "skip": ("Skip (S)", "secondary", "s"),
        "archive": ("Archive (X)", "contrast", "x")
    }
    buttons = []
    for key, (label, css_class, key_trigger) in actions.items():
        trigger_attr = f"click, keyup[key=={key_trigger}] from:body" if key_trigger else "click"
        buttons.append(
            Button(label, hx_post=f"/api/clip/{clip_id}/select?action={key}", hx_target="#review-container", hx_swap="outerHTML", hx_trigger=trigger_attr, data_action=key, cls=css_class)
        )
    return Div(*buttons, cls="action-buttons", style="display: flex; gap: 1rem; margin-bottom: 1rem;")

def undo_toast_component(undo_context):
    """Renders the Undo toast if context exists, otherwise empty Div."""
    if not undo_context or 'clip_id' not in undo_context or 'action' not in undo_context:
        return Div(id="undo-area")

    clip_id = undo_context['clip_id']; action_taken = undo_context['action']; toast_id = f"undo-toast-{clip_id}"
    undo_button = Button("Undo (Z)", hx_post=f"/api/clip/{clip_id}/select?action=undo", hx_target="#review-container", hx_swap="outerHTML", hx_trigger="click, keyup[key==z] from:body", cls="secondary outline small", data_action="undo")

    return Div( Span(f"Last action: '{action_taken}' on clip {clip_id}. "), undo_button, id="undo-area", _toast_instance_id=toast_id, style="padding: 0.5rem; background-color: #e0f2f7; border: 1px solid #b3e5fc; border-radius: 4px; margin-top: 1rem;" )

# Main Page Structure
def review_page_content(clip_to_display, sprite_artifact_display, next_clip_sprite_url=None, undo_context=None):
    """Renders the main content area, including clip display, undo toast, and preload."""
    preload_elements = ()
    if next_clip_sprite_url:
        preload_elements = ( Img(src=next_clip_sprite_url, style="position:absolute; left:-9999px; top:-9999px; width:1px; height:1px; opacity:0;", alt="preload", loading="lazy"), )

    display_content = clip_display(clip_to_display, None) if clip_to_display else P("Loading next clip or none available...")

    return Div(
        display_content,
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
app_headers = (
    Script(src="/static/js/sprite-player.js", defer=True),
    Script(src="/static/js/review-main.js", defer=True),
)
app, rt = fast_app(
    hdrs=app_headers,
    secret_key='replace-with-a-real-secret-key-or-omit-for-auto-gen'
)

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
@rt("/review")
def get(session: dict):
    """Handles the initial load of the review page."""
    # Removed detailed timing logs from GET for operational clarity
    undo_context = session.get('undo_context', None)
    clip = None
    next_clip_sprite_url = None

    try:
        with get_db_conn_from_pool() as conn:
            with conn.cursor() as cur:
                # Fetch first clip AND its latest sprite artifact using JOIN
                sql_first_clip_join = """
                    WITH LatestSprite AS (
                        SELECT clip_id, s3_key, metadata, ROW_NUMBER() OVER(PARTITION BY clip_id ORDER BY created_at DESC) as rn
                        FROM clip_artifacts
                        WHERE artifact_type = 'sprite_sheet'
                    )
                    SELECT c.*, sv.fps AS source_fps, ls.s3_key as sprite_s3_key, ls.metadata as sprite_metadata
                    FROM clips c
                    JOIN source_videos sv ON c.source_video_id = sv.id
                    LEFT JOIN LatestSprite ls ON c.id = ls.clip_id AND ls.rn = 1
                    WHERE c.ingest_state = 'pending_review'
                    ORDER BY c.updated_at ASC, c.id ASC
                    LIMIT 1;
                """
                cur.execute(sql_first_clip_join)
                clip = cur.fetchone()

                if clip:
                    # Fetch next clip's sprite artifact FOR PRELOAD
                    sql_next_clip_id = """
                        SELECT c.id, c.updated_at FROM clips c
                        WHERE c.ingest_state = 'pending_review'
                        AND (c.updated_at > %s OR (c.updated_at = %s AND c.id > %s))
                        ORDER BY c.updated_at ASC, c.id ASC LIMIT 1;"""
                    cur.execute(sql_next_clip_id, (clip['updated_at'], clip['updated_at'], clip['id']))
                    next_clip_ref = cur.fetchone()

                    if next_clip_ref:
                        sql_next_artifact = """
                            SELECT s3_key FROM clip_artifacts
                            WHERE clip_id = %s AND artifact_type = 'sprite_sheet'
                            ORDER BY created_at DESC LIMIT 1;"""
                        cur.execute(sql_next_artifact, (next_clip_ref['id'],))
                        next_sprite_artifact_preload = cur.fetchone()
                        if next_sprite_artifact_preload:
                            next_clip_sprite_url = get_sprite_display_url(next_sprite_artifact_preload.get('s3_key'))

    except (Exception, psycopg2.DatabaseError) as e:
        print(f"ERROR during GET /review DB operations: {type(e).__name__} - {e}", file=sys.stderr)
        clip = None

    title = "Review Clips"
    content = review_page_content(clip, None, next_clip_sprite_url, undo_context) if clip else no_clips_ui(undo_context=undo_context)
    return Titled(title, content)


@rt("/api/clip/{clip_id:int}/select")
def post(clip_id: int, req: Request, session: dict):
    """Handles clip actions (approve, skip, archive, undo). Operational version (reduced timing logs)."""
    action = req.query_params.get('action'); reviewer = "reviewer_placeholder"; final_html = ""
    if not action: print("Error: Action missing", file=sys.stderr); return ""
    print(f"API Request: Action '{action}' for Clip ID {clip_id}") # Keep essential request log

    next_clip = None
    next_clip_sprite_url_preload = None
    new_undo_context = None
    event_id = None # Initialize event_id

    try:
        with get_db_conn_from_pool() as conn: # Single connection for the whole operation
            with conn.cursor() as cur: # Single cursor

                # --- Log Event FIRST using the *same* cursor/transaction ---
                if action == 'undo':
                    event_id = insert_clip_event(clip_id, 'undo', reviewer, cur=cur) # Pass cursor
                else: # approve, skip, archive
                    event_id = insert_clip_event(clip_id, f"selected_{action}", reviewer, cur=cur) # Pass cursor
                # Note: insert_clip_event now prints its own log

                # --- Proceed based on action ---
                if event_id is not None or action == 'undo':

                    if action == 'undo':
                        last_undo_context = session.pop('undo_context', None)
                        clip_to_show = None

                        # Fetch the clip that was acted upon (now needs review again)
                        sql_get_undone_clip = """
                            WITH LatestSprite AS (
                                SELECT clip_id, s3_key, metadata, ROW_NUMBER() OVER(PARTITION BY clip_id ORDER BY created_at DESC) as rn
                                FROM clip_artifacts WHERE artifact_type = 'sprite_sheet'
                            )
                            SELECT c.*, sv.fps AS source_fps, ls.s3_key as sprite_s3_key, ls.metadata as sprite_metadata
                            FROM clips c
                            JOIN source_videos sv ON c.source_video_id = sv.id
                            LEFT JOIN LatestSprite ls ON c.id = ls.clip_id AND ls.rn = 1
                            WHERE c.id = %s;
                        """
                        cur.execute(sql_get_undone_clip, (clip_id,))
                        clip_to_show = cur.fetchone()

                        if clip_to_show:
                             # Fetch the *next* clip's sprite for preload (relative to the one being shown)
                             sql_next_id = """SELECT c.id, c.updated_at FROM clips c WHERE c.ingest_state = 'pending_review'
                                              AND (c.updated_at > %s OR (c.updated_at = %s AND c.id > %s))
                                              ORDER BY c.updated_at ASC, c.id ASC LIMIT 1;"""
                             cur.execute(sql_next_id, (clip_to_show['updated_at'], clip_to_show['updated_at'], clip_to_show['id']))
                             next_clip_ref = cur.fetchone()

                             if next_clip_ref:
                                 sql_next_art = "SELECT s3_key FROM clip_artifacts WHERE clip_id = %s AND artifact_type = 'sprite_sheet' ORDER BY created_at DESC LIMIT 1;"
                                 cur.execute(sql_next_art, (next_clip_ref['id'],))
                                 art = cur.fetchone()
                                 if art: next_clip_sprite_url_preload = get_sprite_display_url(art.get('s3_key'))

                             next_clip = clip_to_show
                             new_undo_context = None # Clear undo context after successful processing
                        else:
                             # Handle case where undone clip not found (fetch first pending)
                             print(f"Warning: Could not find clip {clip_id} after undo action. Fetching first pending.")
                             sql_fallback_clip = """
                                WITH LatestSprite AS (
                                    SELECT clip_id, s3_key, metadata, ROW_NUMBER() OVER(PARTITION BY clip_id ORDER BY created_at DESC) as rn
                                    FROM clip_artifacts WHERE artifact_type = 'sprite_sheet'
                                )
                                SELECT c.*, sv.fps AS source_fps, ls.s3_key as sprite_s3_key, ls.metadata as sprite_metadata
                                FROM clips c JOIN source_videos sv ON c.source_video_id = sv.id
                                LEFT JOIN LatestSprite ls ON c.id = ls.clip_id AND ls.rn = 1
                                WHERE c.ingest_state = 'pending_review' ORDER BY c.updated_at ASC, c.id ASC LIMIT 1;
                             """
                             cur.execute(sql_fallback_clip)
                             next_clip = cur.fetchone() # Show first pending clip if undo target vanished
                             new_undo_context = None # Still clear undo context


                    else: # Regular Actions (approve, skip, archive)
                        new_undo_context = {'clip_id': clip_id, 'action': action} # Prep undo only if event logged okay

                        # Fetch the *current* clip's reference for ordering
                        cur.execute("SELECT updated_at, id FROM clips WHERE id = %s;", (clip_id,))
                        current_clip_ref = cur.fetchone()

                        if current_clip_ref:
                            # Fetch the *next* pending clip relative to the one just acted upon
                            sql_next_clip_join = """
                                WITH LatestSprite AS (
                                    SELECT clip_id, s3_key, metadata, ROW_NUMBER() OVER(PARTITION BY clip_id ORDER BY created_at DESC) as rn
                                    FROM clip_artifacts WHERE artifact_type = 'sprite_sheet'
                                )
                                SELECT c.*, sv.fps AS source_fps, ls.s3_key as sprite_s3_key, ls.metadata as sprite_metadata
                                FROM clips c JOIN source_videos sv ON c.source_video_id = sv.id
                                LEFT JOIN LatestSprite ls ON c.id = ls.clip_id AND ls.rn = 1
                                WHERE c.ingest_state = 'pending_review'
                                AND (c.updated_at > %s OR (c.updated_at = %s AND c.id > %s))
                                ORDER BY c.updated_at ASC, c.id ASC LIMIT 1;
                            """
                            cur.execute(sql_next_clip_join, (current_clip_ref['updated_at'], current_clip_ref['updated_at'], current_clip_ref['id']))
                            next_clip = cur.fetchone()

                            if next_clip:
                                # OPTIMIZED: Fetch the clip *after* next's S3 key in one go
                                sql_after_next_preload = """
                                    WITH AfterNextClip AS (
                                        SELECT c.id
                                        FROM clips c
                                        WHERE c.ingest_state = 'pending_review'
                                          AND (c.updated_at > %(next_clip_ts)s OR (c.updated_at = %(next_clip_ts)s AND c.id > %(next_clip_id)s))
                                        ORDER BY c.updated_at ASC, c.id ASC
                                        LIMIT 1
                                    ), AfterNextSprite AS (
                                        SELECT a.s3_key
                                        FROM clip_artifacts a JOIN AfterNextClip anc ON a.clip_id = anc.id
                                        WHERE a.artifact_type = 'sprite_sheet'
                                        ORDER BY a.created_at DESC
                                        LIMIT 1
                                    )
                                    SELECT s3_key FROM AfterNextSprite;
                                """
                                params_after_next = {
                                    'next_clip_ts': next_clip['updated_at'],
                                    'next_clip_id': next_clip['id']
                                }
                                cur.execute(sql_after_next_preload, params_after_next)
                                art = cur.fetchone()
                                if art:
                                     next_clip_sprite_url_preload = get_sprite_display_url(art.get('s3_key'))

                            # else: No more clips after current one
                        else:
                             # Handle case where current clip ref not found (fetch first pending)
                             print(f"Warning: Could not find reference clip {clip_id}. Fetching first pending.")
                             sql_fallback_clip = """
                                WITH LatestSprite AS (
                                    SELECT clip_id, s3_key, metadata, ROW_NUMBER() OVER(PARTITION BY clip_id ORDER BY created_at DESC) as rn
                                    FROM clip_artifacts WHERE artifact_type = 'sprite_sheet'
                                )
                                SELECT c.*, sv.fps AS source_fps, ls.s3_key as sprite_s3_key, ls.metadata as sprite_metadata
                                FROM clips c JOIN source_videos sv ON c.source_video_id = sv.id
                                LEFT JOIN LatestSprite ls ON c.id = ls.clip_id AND ls.rn = 1
                                WHERE c.ingest_state = 'pending_review' ORDER BY c.updated_at ASC, c.id ASC LIMIT 1;
                             """
                             cur.execute(sql_fallback_clip)
                             next_clip = cur.fetchone()


                else: # Event logging failed (event_id is None) for a non-undo action
                    print(f"ERROR: Failed to log event for action '{action}' on clip {clip_id}. Re-displaying current clip.")
                    new_undo_context = session.get('undo_context', None) # Keep existing undo context
                    # Try to fetch the *current* clip again to redisplay it without change
                    sql_get_current_clip = """
                         WITH LatestSprite AS (
                            SELECT clip_id, s3_key, metadata, ROW_NUMBER() OVER(PARTITION BY clip_id ORDER BY created_at DESC) as rn
                            FROM clip_artifacts WHERE artifact_type = 'sprite_sheet'
                        )
                        SELECT c.*, sv.fps AS source_fps, ls.s3_key as sprite_s3_key, ls.metadata as sprite_metadata
                        FROM clips c
                        JOIN source_videos sv ON c.source_video_id = sv.id
                        LEFT JOIN LatestSprite ls ON c.id = ls.clip_id AND ls.rn = 1
                        WHERE c.id = %s;
                    """
                    try:
                         cur.execute(sql_get_current_clip, (clip_id,))
                         next_clip = cur.fetchone() # Set next_clip to the current one for re-display
                    except Exception as refetch_err:
                          print(f"    DB Error during fallback re-fetch: {refetch_err}")
                          next_clip = None # Cannot even refetch current


                # End of cursor block (commit happens when 'with conn' exits)

            # --- Update Session State ---
            if new_undo_context is not None:
                 session['undo_context'] = new_undo_context
            elif action != 'undo' and event_id is None:
                 # If logging failed, ensure session reflects that UI hasn't changed
                 # Keeping existing context is generally correct here
                 pass # Session state remains unchanged
            elif action == 'undo': # Explicitly handle clearing for undo
                session.pop('undo_context', None)


    except (Exception, psycopg2.DatabaseError) as e:
        print(f"ERROR during POST /api/clip/{clip_id}/select DB operations: {type(e).__name__} - {e}", file=sys.stderr)
        final_html = Div( H3("Database Error"), P("Could not process action. Please try again later."), id="review-container")
        session.pop('undo_context', None) # Clear potentially inconsistent undo context on DB error

    else: # Only executes if try block completes without exception
        # --- Render Response ---
        if not final_html: # Only render if no DB error occurred earlier
            if next_clip:
                final_html = review_page_content(next_clip, None, next_clip_sprite_url_preload, new_undo_context)
            else:
                final_html = no_clips_ui(undo_context=new_undo_context)

    # Minimal final log
    # print(f"Finished POST for action '{action}' on clip {clip_id}")
    if not final_html:
        print(f"ERROR: final_html was not set for action '{action}' on clip {clip_id}.")
        return Div("Error: Response could not be generated.", id="review-container")
    return final_html


# --- Run Server ---
if __name__ == "__main__":
    # Pool initialized by startup event
    if not static_dir.is_dir(): print(f"---\nWARNING: Static directory missing at '{static_dir}'\n---")
    print(f"FastHTML server starting. Ensure '.env' is configured.")
    print(f"Sprites load from: {CLOUDFRONT_DOMAIN or 'Local Placeholder'}")
    print(f"Access UI at: http://127.0.0.1:5001/review (or check Uvicorn output)")
    serve()
# --- END OF FILE main.py ---
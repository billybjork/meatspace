# main.py (formerly review_app.py)
import sys
import os
from pathlib import Path
# Ensure fasthtml.common imports necessary components like Script, Titled, etc.
from fasthtml.common import *
import datetime
import json
import math
import psycopg2
from psycopg2.extras import RealDictCursor
from starlette.requests import Request
from starlette.exceptions import HTTPException
from starlette.staticfiles import StaticFiles

# --- Load Environment Variables ---
from dotenv import load_dotenv
load_dotenv()

# --- Configuration ---
DATABASE_URL = os.getenv("DATABASE_URL")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
CLOUDFRONT_DOMAIN = os.getenv("CLOUDFRONT_DOMAIN")

# --- Check essential configuration ---
if not DATABASE_URL:
    print("FATAL ERROR: DATABASE_URL environment variable not set.", file=sys.stderr)
    sys.exit(1)
# Warnings for S3/CloudFront if not set
if not S3_BUCKET_NAME:
    print("WARNING: S3_BUCKET_NAME environment variable not set.", file=sys.stderr)
if not CLOUDFRONT_DOMAIN:
     print("INFO: CLOUDFRONT_DOMAIN environment variable not set. Falling back to placeholder for sprite URLs.", file=sys.stderr)

# S3 Client Initialization (Optional - not used directly in this file anymore)
# try:
#     import boto3
#     s3_client = boto3.client('s3')
#     print("Initialized boto3 S3 client.")
# except ImportError:
#     s3_client = None
#     print("Warning: boto3 not installed.", file=sys.stderr)
# except Exception as e:
#     s3_client = None
#     print(f"Warning: Failed to initialize boto3 S3 client: {e}", file=sys.stderr)


# --- Database Helper Functions (Assumed correct) ---
def get_db_conn():
    try:
        conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
        return conn
    except psycopg2.OperationalError as e:
        print(f"FATAL: Database connection failed: {e}", file=sys.stderr)
        if __name__ == "__main__":
             sys.exit(f"Exiting due to DB connection failure: {e}")
        raise

def fetch_one(sql, params=()):
    try:
        with get_db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                return cur.fetchone()
    except Exception as e:
        print(f"DB Error (fetch_one): {e}", file=sys.stderr)
        return None

def execute_sql(sql, params=()):
    rows_affected = 0
    try:
        with get_db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                rows_affected = cur.rowcount
        return rows_affected
    except Exception as e:
        print(f"DB Error (execute_sql): {e}", file=sys.stderr)
        return -1

# --- Specific DB Interaction Functions (Assumed correct) ---
def get_next_pending_review_clip():
    sql = """
        SELECT c.*, sv.fps AS source_fps
        FROM clips c
        JOIN source_videos sv ON c.source_video_id = sv.id
        WHERE c.ingest_state = 'pending_review'
        ORDER BY c.updated_at ASC
        LIMIT 1;
    """
    return fetch_one(sql)

def get_clip_by_id(clip_id):
    sql = """
        SELECT c.*, sv.fps AS source_fps
        FROM clips c
        JOIN source_videos sv ON c.source_video_id = sv.id
        WHERE c.id = %s;
        """
    return fetch_one(sql, (clip_id,))

def get_sprite_artifact(clip_id):
    sql = """
        SELECT * FROM clip_artifacts
        WHERE clip_id = %s AND artifact_type = 'sprite_sheet'
        ORDER BY created_at DESC LIMIT 1;
        """
    return fetch_one(sql, (clip_id,))

def insert_clip_event(clip_id, action, reviewer_id=None, event_data=None):
    sql = """
        INSERT INTO clip_events (clip_id, action, reviewer_id, event_data, created_at)
        VALUES (%s, %s, %s, %s, NOW())
        RETURNING id;
    """
    json_event_data = json.dumps(event_data) if isinstance(event_data, dict) else event_data
    params = (clip_id, action, reviewer_id, json_event_data)
    try:
        with get_db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                event_id = cur.fetchone()['id']
                return event_id
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
    else:
        # Log warnings only once perhaps, or rely on startup checks
        return "/static/placeholder_sprite.png"

def clip_display(clip, sprite_artifact):
    # Renders the clip info, player UI elements, and embedded metadata
    if not clip: return P("Clip data missing or loading error.")

    clip_id = clip.get('id')
    sprite_metadata = sprite_artifact.get('metadata') if sprite_artifact else None
    s3_key = sprite_artifact.get('s3_key') if sprite_artifact else None
    sprite_url = get_sprite_display_url(s3_key)

    player_meta = {
        "isValid": False, "spriteUrl": sprite_url,
        "cols": 0, "rows": 0, "tile_width": 0, "tile_height_calculated": None,
        "total_sprite_frames": 0, "clip_fps": 0, "clip_total_frames": 0
    }

    if sprite_metadata and isinstance(sprite_metadata, dict):
        # Populate player_meta (same logic as before)
        player_meta.update({
            "cols": sprite_metadata.get('cols', 0), "rows": sprite_metadata.get('rows', 0),
            "tile_width": sprite_metadata.get('tile_width', 0), "tile_height_calculated": sprite_metadata.get('tile_height_calculated'),
            "total_sprite_frames": sprite_metadata.get('total_sprite_frames', 0),
            "clip_fps": sprite_metadata.get('clip_fps_source') or clip.get('source_fps') or 0,
            "clip_total_frames": sprite_metadata.get('clip_total_frames_source') or 0
        })
        # Validation logic (same logic as before)
        is_valid_dimensions = (player_meta["cols"] > 0 and player_meta["rows"] > 0 and
                               player_meta["tile_width"] > 0 and player_meta["tile_height_calculated"] and player_meta["tile_height_calculated"] > 0)
        is_valid_frames = (player_meta["total_sprite_frames"] > 0 and player_meta["clip_fps"] > 0)
        if is_valid_dimensions and is_valid_frames:
            if player_meta["clip_total_frames"] <= 0:
                 start_time = clip.get('start_time_seconds', 0); end_time = clip.get('end_time_seconds', 0)
                 duration = max(0, end_time - start_time)
                 if duration > 0 and player_meta["clip_fps"] > 0: player_meta["clip_total_frames"] = math.ceil(duration * player_meta["clip_fps"]) + 1
                 else: player_meta["clip_total_frames"] = 0
            if player_meta["clip_total_frames"] > 0: player_meta["isValid"] = True
        # else: print(f"Warning: Sprite metadata invalid for clip {clip_id}...") # Keep warning if needed

    # Player UI Elements (IDs and structure remain the same)
    viewer_id = f"viewer-{clip_id}"; scrub_id = f"scrub-{clip_id}"; playpause_id = f"playpause-{clip_id}"
    frame_display_id = f"frame-display-{clip_id}"; meta_script_id = f"meta-{clip_id}"
    controls_disabled = not player_meta["isValid"]

    player_container = Div(
        Div(id=viewer_id, cls="sprite-viewer bg-gray-200 border border-gray-400", style="width: 320px; height: 180px; background-repeat: no-repeat; overflow: hidden; margin: 0 auto; background-position: 0px 0px;"),
        Div(
            Button("⏯️", id=playpause_id, cls="play-pause-btn", disabled=controls_disabled, style="font-size: 1.2em; min-width: 40px;"),
            Input(type="range", id=scrub_id, min="0", value="0", step="1", disabled=controls_disabled, style="flex-grow: 1; margin: 0 0.5rem;"),
            Span("Frame: 0", id=frame_display_id, style="min-width: 80px; text-align: right; font-family: monospace;"),
            cls="sprite-controls flex items-center mt-2"
        ),
        Script(json.dumps(player_meta), type="application/json", id=meta_script_id),
        data_clip_id=clip_id, cls="clip-player-instance mb-4 p-3 border rounded shadow"
    )

    # Non-Player Clip Info (remains the same)
    clip_info = Div(
        H3(f"Clip Details (ID: {clip_id})"),
        P(f"Source Video ID: {clip.get('source_video_id', 'N/A')}", Title(f"Clip Path: {clip.get('clip_filepath', 'N/A')}")),
        P(f"Time: {clip.get('start_time_seconds', 0):.2f}s - {clip.get('end_time_seconds', 0):.2f}s"),
        P(f"Sprite Valid: {'Yes' if player_meta['isValid'] else 'No'}", style="font-size: 0.8rem; color: grey;"),
        cls="clip-metadata mt-2 text-sm"
    )
    return Div(player_container, clip_info, cls="clip-display-container")

# --- Action Buttons & Undo Toast (No changes needed) ---
def action_buttons(clip_id):
    if not clip_id: return Div()
    actions = { "approve": ("Approve (A)", "selected_approve", "primary"), "skip": ("Skip (S)", "selected_skip", "secondary"), "archive": ("Archive (X)", "selected_archive", "contrast") }
    buttons = []
    for key, (label, _, css_class) in actions.items():
        buttons.append( Button(label, hx_post=f"/api/clip/{clip_id}/select?action={key}", hx_target="#undo-toast-area", hx_swap="innerHTML", data_action=key, cls=css_class) )
    return Div(*buttons, cls="action-buttons", style="display: flex; gap: 1rem; margin-bottom: 1rem;")

def undo_toast_component(clip_id, action_taken):
    toast_id = f"undo-toast-{clip_id}"
    return Div( Span(f"Action '{action_taken}' selected for clip {clip_id}. "), Button("Undo (Z)", hx_post=f"/api/clip/{clip_id}/select?action=undo", hx_target="#undo-toast-area", hx_swap="innerHTML", cls="secondary outline small", data_action="undo"), id=toast_id, style="padding: 0.5rem; background-color: #e0f2f7; border: 1px solid #b3e5fc; border-radius: 4px; margin-top: 1rem;" )

# --- Main Page Structure (No changes needed) ---
def review_page_content(clip, sprite_artifact):
    return Div( clip_display(clip, sprite_artifact), action_buttons(clip['id'] if clip else None), Div(id="undo-toast-area"), id="review-container" )

def no_clips_ui():
    return Div( H2("All clips reviewed!"), P("Check back later for more."), Div(id="undo-toast-area"), id="review-container" )


# --- FastHTML App Setup ---
# Define headers to be included
# Using FastHTML's Script component ensures correct rendering
custom_headers = (
    Script(src="/static/js/sprite-player.js", defer=True),
    Script(src="/static/js/review-main.js", defer=True),
    # Add other headers like CSS links here if needed
    # e.g., Link(rel="stylesheet", href="/static/css/custom.css")
)

# Pass the headers tuple to fast_app
# `fast_app` will combine these with its defaults (like PicoCSS, HTMX)
# unless explicitly disabled (e.g., pico=False)
app, rt = fast_app(hdrs=custom_headers)


# --- Serve Static Files ---
# This MUST come AFTER app initialization with fast_app
static_dir = Path(__file__).parent / 'static'
if static_dir.is_dir():
    # Mount using the app object returned by fast_app
    app.mount('/static', StaticFiles(directory=static_dir), name='static')
    print(f"Serving static files from: {static_dir}")
else:
    print(f"WARNING: Static directory not found at {static_dir}. JS/CSS/Images might not load.", file=sys.stderr)


# --- REMOVED Custom Header Override ---
# The previous app.state.global_hdrs and _custom_html_hdrs are removed.


# --- Routes (No changes needed in route logic) ---
@rt("/review")
def get():
    clip = get_next_pending_review_clip()
    title = "Review Clips"
    content = None
    if clip:
        sprite_artifact = get_sprite_artifact(clip['id'])
        if not sprite_artifact: print(f"Warning: No sprite artifact for clip {clip['id']}.")
        content = review_page_content(clip, sprite_artifact)
    else:
        content = no_clips_ui()
    # Titled should now correctly include the headers passed to fast_app
    return Titled(title, content)

@rt("/api/clip/{clip_id:int}/select")
def post(clip_id: int, req: Request):
    action = req.query_params.get('action')
    if not action:
        print("Error: Action query parameter missing", file=sys.stderr)
        return Div(hx_swap_oob='innerHTML', id='undo-toast-area') # Use innerHTML for OOB

    print(f"Received action '{action}' for clip {clip_id}")
    reviewer = "reviewer_placeholder"

    # Validation logic
    if action != 'undo':
        clip = get_clip_by_id(clip_id)
        if not clip:
            print(f"Error: Clip {clip_id} not found for action '{action}'.", file=sys.stderr)
            return Div("Error: Clip not found.", hx_swap_oob='innerHTML', id='undo-toast-area', style="color: red; padding: 0.5rem;") # Use innerHTML for OOB
        if clip.get('ingest_state') != 'pending_review':
            print(f"Warning: Action '{action}' requested for clip {clip_id}, but state is '{clip.get('ingest_state')}'. Ignoring.", file=sys.stderr)
            return Div(hx_swap_oob='innerHTML', id='undo-toast-area') # Use innerHTML for OOB

    # Write Event to DB
    event_id = insert_clip_event(clip_id, f"selected_{action}" if action != 'undo' else 'undo', reviewer)
    if not event_id:
        print(f"Error: Failed to insert event for action '{action}', clip {clip_id}", file=sys.stderr)
        return Div(f"Error logging action '{action}'.", hx_swap_oob='innerHTML', id='undo-toast-area', style="color: red; padding: 0.5rem;") # Use innerHTML for OOB

    print(f"Event logged (ID: {event_id}): Action '{action}' for clip {clip_id}")

    # Return Response for HTMX OOB Swap
    if action == 'undo':
        return Div(hx_swap_oob='innerHTML', id='undo-toast-area') # Use innerHTML for OOB
    else:
        # Note: undo_toast_component implicitly uses hx_swap_oob='outerHTML' by default
        # when rendered directly, but here the button *targets* innerHTML.
        # To be safe, let's wrap the component in a Div with the attribute.
        # return Div(undo_toast_component(clip_id, action), hx_swap_oob='innerHTML', id='undo-toast-area') # Incorrect - targets self
        # The button definition *already* targets #undo-toast-area with innerHTML swap.
        # So just returning the component directly is correct. HTMX handles placing it inside the target.
        return undo_toast_component(clip_id, action)


# --- Run Server ---
if __name__ == "__main__":
    print("Checking database connection...")
    try:
        conn = get_db_conn()
        conn.close()
        print("Database connection successful.")
    except Exception:
        pass

    if not static_dir.is_dir():
         print(f"---\nWARNING: Static directory missing at '{static_dir}'\n         JavaScript files (sprite-player.js, review-main.js) will NOT load.\n---")

    print(f"FastHTML server starting. Ensure '.env' is configured.")
    print(f"Sprites should load from CloudFront domain: {CLOUDFRONT_DOMAIN or 'Not Set'}")
    print(f"Access UI at: http://127.0.0.1:5001/review (or check Uvicorn output for port)")
    serve()
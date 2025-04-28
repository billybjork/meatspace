import sys
import os
from pathlib import Path
from fasthtml.common import *
import datetime
import json
import math
import time
import psycopg2
from starlette.requests import Request
from starlette.staticfiles import StaticFiles
from typing import Optional, Any

from db import initialize_db_pool, close_db_pool, get_db_conn_from_pool

# --- Load Environment Variables ---
from dotenv import load_dotenv
load_dotenv()

# --- Configuration (App specific) ---
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
CLOUDFRONT_DOMAIN = os.getenv("CLOUDFRONT_DOMAIN")

# --- Specific DB Interaction Functions ---

def insert_clip_event(clip_id, action, reviewer_id=None, event_data=None, *, cur: Optional[psycopg2.extensions.cursor] = None) -> Optional[int]:
    """
    Logs an event to the clip_events table.
    If 'cur' is provided, uses the existing cursor/transaction.
    Otherwise, creates its own connection and transaction using get_db_conn_from_pool.
    """
    sql = "INSERT INTO clip_events (clip_id, action, reviewer_id, event_data, created_at) VALUES (%s, %s, %s, %s, NOW()) RETURNING id;"
    json_event_data = json.dumps(event_data) if isinstance(event_data, dict) else event_data
    params = (clip_id, action, reviewer_id, json_event_data)
    event_id = None
    t_start = time.time()

    try:
        if cur:
            # Use the provided cursor
            cur.execute(sql, params)
            result = cur.fetchone()
            event_id = result['id'] if result else None
        else:
            # Create a separate connection via the context manager from db.py
            with get_db_conn_from_pool() as conn:
                with conn.cursor() as new_cur:
                    new_cur.execute(sql, params)
                    result = new_cur.fetchone()
                    event_id = result['id'] if result else None
        t_end = time.time()
        # print(f"    DB Timing (insert_clip_event internal): Actual DB op took {t_end - t_start:.4f}s (Using {'Existing' if cur else 'New'} Cursor)")

    except Exception as e:
        t_end = time.time()
        print(f"DB Error (insert_clip_event internal, {t_end - t_start:.4f}s): {e}", file=sys.stderr)

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

def clip_display(clip, sprite_artifact):
    """Renders the clip info, player UI elements, and embedded metadata."""
    if not clip: return P("Clip data missing or loading error.")
    clip_id = clip.get('id')
    s3_key = clip.get('sprite_s3_key')
    sprite_metadata = clip.get('sprite_metadata')

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
        preload_elements = ( Img(src=next_clip_sprite_url, style="position:absolute; left:-9999px; top:-9999px; width:1px; height:1px; opacity:0;", alt="preload"), )

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
    if next_clip_sprite_url: preload_elements = ( Img(src=next_clip_sprite_url, style="position:absolute; left:-9999px; top:-9999px; width:1px; height:1px; opacity:0;", alt="preload"), )
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

# --- Startup/Shutdown Events for Pool ---
@app.on_event("startup")
async def startup_event():
    try:
        initialize_db_pool()
    except ConnectionError as e:
        print(f"CRITICAL: Failed to initialize DB pool during startup: {e}", file=sys.stderr)
        # Consider how the app should behave if DB pool fails.
        # For now, it will likely fail later when a connection is needed.
        # Alternatively, re-raise to stop the app:
        # raise RuntimeError("Database pool failed to initialize") from e

@app.on_event("shutdown")
async def shutdown_event():
    close_db_pool()

# --- Serve Static Files ---
static_dir = Path(__file__).parent / 'static'
if static_dir.is_dir():
    app.mount('/static', StaticFiles(directory=static_dir), name='static')
    # print(f"Serving static files from: {static_dir}") # Moved to main.py
else:
    # print(f"WARNING: Static directory not found at {static_dir}. JS/CSS/Images might not load.", file=sys.stderr) # Moved to main.py
    pass


# --- Routes ---

@rt("/review")
def get(session: dict):
    """Handles the initial load of the review page."""
    undo_context = session.get('undo_context', None)
    clip = None
    next_clip_sprite_url = None

    try:
        # Use the imported context manager
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
        # Error logged by context manager, just ensure clip is None
        print(f"ERROR during GET /review DB operations: {type(e).__name__} - {e}", file=sys.stderr)
        clip = None

    title = "Review Clips"
    content = review_page_content(clip, None, next_clip_sprite_url, undo_context) if clip else no_clips_ui(undo_context=undo_context)
    return Titled(title, content)


@rt("/api/clip/{clip_id:int}/select")
def post(clip_id: int, req: Request, session: dict):
    """
    Handles clip actions (approve, skip, archive, undo).
    Logs event ONLY. Does NOT change clip state directly.
    Fetches the next available 'pending_review' clip for the UI.
    """
    action = req.query_params.get('action'); reviewer = "reviewer_placeholder"; final_html = ""
    if not action: print("Error: Action missing", file=sys.stderr); return ""
    # Minimal request log
    # print(f"API Request: Action '{action}' for Clip ID {clip_id}")

    next_clip = None
    next_clip_sprite_url_preload = None
    new_undo_context = None
    event_id = None
    current_clip_ref_for_fetch = None # Store ref of acted-upon clip

    try:
        with get_db_conn_from_pool() as conn:
            with conn.cursor() as cur:

                # 1. Log Event - This is the primary action of this handler now
                if action == 'undo':
                    # For undo, we find the corresponding action event ID to potentially link or mark in event_data if needed
                    # event_data_for_undo = {'undid_action': last_undo_context.get('action')} if session.get('undo_context') else None
                    event_id = insert_clip_event(clip_id, 'undo', reviewer, cur=cur) # event_data=event_data_for_undo
                else: # approve, skip, archive
                    event_id = insert_clip_event(clip_id, f"selected_{action}", reviewer, cur=cur)
                # insert_clip_event prints its own log

                # 2. Fetch Reference for Next Clip Lookup (if action wasn't undo)
                # Still need this to find the *next* pending clip relative to the current one.
                if action != 'undo':
                    cur.execute("SELECT updated_at, id FROM clips WHERE id = %s;", (clip_id,))
                    current_clip_ref_for_fetch = cur.fetchone()

                # 3. Fetch Clip Data for UI Response
                # This logic determines what the UI should show *next*, based on the action logged.
                # It does NOT rely on any state changes performed within this request.

                if event_id is not None: # Event logged successfully
                    if action == 'undo':
                        last_undo_context = session.pop('undo_context', None) # Clear the context state since undo was logged
                        # Fetch the clip that was acted upon to display it again
                        sql_fetch_reverted = """
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
                        cur.execute(sql_fetch_reverted, (clip_id,))
                        next_clip = cur.fetchone() # Display the clip whose action was undone

                        if next_clip: # Prepare preload for the clip *after* this one
                             sql_next_id = """SELECT c.id, c.updated_at FROM clips c WHERE c.ingest_state = 'pending_review'
                                              AND (c.updated_at > %s OR (c.updated_at = %s AND c.id > %s))
                                              ORDER BY c.updated_at ASC, c.id ASC LIMIT 1;"""
                             cur.execute(sql_next_id, (next_clip['updated_at'], next_clip['updated_at'], next_clip['id']))
                             next_clip_ref = cur.fetchone()
                             if next_clip_ref:
                                 sql_next_art = "SELECT s3_key FROM clip_artifacts WHERE clip_id = %s AND artifact_type = 'sprite_sheet' ORDER BY created_at DESC LIMIT 1;"
                                 cur.execute(sql_next_art, (next_clip_ref['id'],))
                                 art = cur.fetchone()
                                 if art: next_clip_sprite_url_preload = get_sprite_display_url(art.get('s3_key'))
                        else: # Fallback if reverted clip not found immediately
                             print(f"Warning: Could not find clip {clip_id} immediately after undo event. Fetching first pending.")
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
                        new_undo_context = None # No undo context shown after an undo action

                    else: # Regular action logged successfully (approve, skip, archive)
                        # Fetch the *next* 'pending_review' clip based on the clip just acted upon
                        if current_clip_ref_for_fetch:
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
                             cur.execute(sql_next_clip_join, (current_clip_ref_for_fetch['updated_at'], current_clip_ref_for_fetch['updated_at'], current_clip_ref_for_fetch['id']))
                             next_clip = cur.fetchone()

                             if next_clip: # Prepare preload for the clip *after* this next one
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
                                 params_after_next = {'next_clip_ts': next_clip['updated_at'], 'next_clip_id': next_clip['id']}
                                 cur.execute(sql_after_next_preload, params_after_next)
                                 art = cur.fetchone()
                                 if art: next_clip_sprite_url_preload = get_sprite_display_url(art.get('s3_key'))
                        else: # Fallback if ref not found
                             print(f"Warning: Could not get reference for clip {clip_id}. Fetching first pending.")
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
                        # Set undo context for the UI response
                        new_undo_context = {'clip_id': clip_id, 'action': action}

                else: # Event logging failed
                    print(f"ERROR: Failed to log event for action '{action}' on clip {clip_id}. Re-displaying current clip.")
                    new_undo_context = session.get('undo_context', None) # Keep existing context
                    # Re-fetch current clip
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
                          print(f"DB Error during fallback re-fetch: {refetch_err}")
                          next_clip = None

                # End of cursor block (commit happens when 'with conn' exits)

            # --- Update Session State (after DB transaction completes) ---
            # Only change session if event logging was successful
            if event_id is not None:
                if new_undo_context is not None:
                     session['undo_context'] = new_undo_context
                elif action == 'undo': # If action was undo and event was logged, clear context
                     session.pop('undo_context', None)
            # If event logging failed, session remains unchanged

    except (Exception, psycopg2.DatabaseError) as e:
        # Error logged by context manager
        print(f"ERROR during POST /api/clip/{clip_id}/select DB operations: {type(e).__name__} - {e}", file=sys.stderr)
        final_html = Div( H3("Database Error"), P("Could not process action. Please try again later."), id="review-container")
        session.pop('undo_context', None) # Clear potentially inconsistent undo context on DB error

    else: # Only executes if try block completes without exception
        # --- Render Response ---
        if not final_html: # Render only if no DB error occurred earlier
            if next_clip:
                # Pass the fetched clip data, None for artifact, preload URL, and new undo context
                final_html = review_page_content(next_clip, None, next_clip_sprite_url_preload, new_undo_context)
            else: # No clip found to display (either end of queue or error during fetch)
                final_html = no_clips_ui(undo_context=new_undo_context) # Show context if applicable

    if not final_html:
        print(f"ERROR: final_html was not set for action '{action}' on clip {clip_id}.")
        return Div("Error: Response could not be generated.", id="review-container") # Return valid HTML fragment
    return final_html
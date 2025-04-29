from __future__ import annotations
import os, time
from typing import List, Tuple, Dict, Any, Optional

import psycopg2
from psycopg2 import sql, pool
from psycopg2.extras import RealDictCursor
from prefect import get_run_logger
from dotenv import load_dotenv

# ─────────────────────────────────────────── env / pool setup
dotenv_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env")
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)
DATABASE_URL = os.getenv("DATABASE_URL")

db_pool: Optional[pool.ThreadedConnectionPool] = None
MIN_POOL_CONN = int(os.getenv("PREFECT_DB_MIN_POOL", 2))
MAX_POOL_CONN = int(os.getenv("PREFECT_DB_MAX_POOL", 10))


# ─────────────────────────────────────────── pool helpers
def initialize_db_pool() -> None:
    global db_pool
    log = get_run_logger()
    if db_pool is not None: return
    if not DATABASE_URL: raise RuntimeError("DATABASE_URL missing")
    log.info("Creating psycopg2 pool …")
    db_pool = psycopg2.pool.ThreadedConnectionPool(MIN_POOL_CONN, MAX_POOL_CONN, dsn=DATABASE_URL)
    with db_pool.getconn() as _: log.info("psycopg2 pool ready")

def get_db_connection(cursor_factory=None):
    log = get_run_logger()
    if db_pool is None: initialize_db_pool()
    retries, delay = 3, 0.5
    for _ in range(retries):
        try:
            conn = db_pool.getconn()
            if cursor_factory: conn.cursor_factory = cursor_factory
            return conn
        except psycopg2.pool.PoolError:
            time.sleep(delay); delay *= 2
    raise ConnectionError("Could not acquire DB connection")

def release_db_connection(conn) -> None:
    if db_pool and conn:
        conn.cursor_factory = None
        db_pool.putconn(conn)

def close_db_pool() -> None:
    global db_pool
    if db_pool:
        get_run_logger().info("Closing psycopg2 pool …")
        db_pool.closeall()
        db_pool = None

# ─────────────────────────────────────────── public helpers
def get_all_pending_work(limit_per_stage: int = 50) -> List[Dict[str, Any]]:
    """
    Consolidated work list for the Prefect initiator.
    Queries based on FINALIZED ingest_state (set by Commit Worker).
    """
    log = get_run_logger()
    conn, items = None, []
    try:
        conn = get_db_connection(cursor_factory=RealDictCursor)
        with conn:
            with conn.cursor() as cur:
                # Query directly on the finalized states expected after commit worker runs
                cur.execute(
                    sql.SQL(
                        """
                        (SELECT id, 'intake'       AS stage FROM source_videos WHERE ingest_state = %s ORDER BY id LIMIT %s)
                        UNION ALL
                        (SELECT id, 'splice'       AS stage FROM source_videos WHERE ingest_state = %s ORDER BY id LIMIT %s)
                        UNION ALL
                        (SELECT id, 'sprite'       AS stage FROM clips         WHERE ingest_state = %s ORDER BY id LIMIT %s)
                        UNION ALL
                        (SELECT id, 'post_review'  AS stage FROM clips         WHERE ingest_state = %s ORDER BY id LIMIT %s)
                        UNION ALL
                        (SELECT id, 'embedding'    AS stage FROM clips         WHERE ingest_state = %s ORDER BY id LIMIT %s)
                        """
                    ),
                    [
                        "new", limit_per_stage,
                        "downloaded", limit_per_stage,
                        "pending_sprite_generation", limit_per_stage,
                        "review_approved", limit_per_stage, # State set by Commit Worker
                        "keyframed", limit_per_stage, # Assumes 'keyframed' is set by embedding task completion
                    ],
                )
                items = cur.fetchall()
    finally:
        if conn: release_db_connection(conn)
    log.debug(f"Pending work rows: {len(items)}")
    return items

def get_source_input_from_db(source_video_id: int) -> str | None:
    """Fetches the original_url for a given source_video ID. (Unchanged)"""
    logger = get_run_logger()
    conn = None; input_source = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT original_url FROM source_videos WHERE id = %s", (source_video_id,))
            result = cur.fetchone()
            if result and result[0]: input_source = result[0]
            else: logger.warning(f"No 'original_url' found for source_video_id {source_video_id}")
    except (Exception, psycopg2.DatabaseError) as error:
         logger.error(f"DB error fetching input source for ID {source_video_id}: {error}", exc_info=True)
    finally:
        if conn: release_db_connection(conn)
    return input_source

def get_pending_merge_pairs() -> List[Tuple[int, int]]:
    """
    Returns list of (target_id, source_id) ready for backward merge.
    Queries based on FINALIZED ingest_state (set by Commit Worker).
    """
    log = get_run_logger()
    conn, pairs = None, []
    try:
        conn = get_db_connection(cursor_factory=RealDictCursor)
        with conn:
            with conn.cursor() as cur:
                # Assumes Commit Worker sets these states
                cur.execute(
                    """
                    SELECT
                        t.id AS target_id,
                        (t.processing_metadata ->> 'merge_source_clip_id')::int AS source_id
                    FROM clips t
                    JOIN clips s ON s.id = (t.processing_metadata ->> 'merge_source_clip_id')::int
                    WHERE t.ingest_state = 'pending_merge_target'
                      AND s.ingest_state = 'marked_for_merge_into_previous';
                    """
                )
                pairs = [(row["target_id"], row["source_id"]) for row in cur.fetchall()]
    finally:
        if conn: release_db_connection(conn)
    log.debug(f"Merge pairs ready: {pairs}")
    return pairs

def get_pending_split_jobs() -> List[Tuple[int, int]]:
    """
    Returns list of (clip_id, split_frame).
    Queries based on FINALIZED ingest_state (set by Commit Worker).
    """
    log = get_run_logger()
    conn, jobs = None, []
    try:
        conn = get_db_connection(cursor_factory=RealDictCursor)
        with conn:
            with conn.cursor() as cur:
                # Assumes Commit Worker sets this state
                cur.execute(
                    """
                    SELECT id,
                           (processing_metadata ->> 'split_request_at_frame')::int AS split_frame
                    FROM clips
                    WHERE ingest_state = 'pending_split';
                    """
                )
                jobs = [(row["id"], row["split_frame"]) for row in cur.fetchall()]
    finally:
        if conn: release_db_connection(conn)
    log.debug(f"Split jobs ready: {jobs}")
    return jobs


# ─────────────────────────────────────────── immediate state helpers
# These are still used by tasks to update state *during* processing or on completion/failure
def _update_state_sync(
    table_name: str, item_id: int, new_state: str,
    processing_state: Optional[str], error_message: Optional[str] = None
) -> bool:
    log = get_run_logger()
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            sets = [
                sql.SQL("ingest_state = %s"), sql.SQL("last_error   = %s"),
                sql.SQL("updated_at   = NOW()"),
            ]
            params = [new_state, error_message]
            # Clear action_committed_at when moving *out* of a committed state
            # Only clear action_committed_at on the clips table
            if table_name == 'clips' and new_state not in ['approved_pending_deletion', 'archived_pending_deletion']:
                sets.append(sql.SQL("action_committed_at = NULL"))

            if new_state == processing_state: sets.append(sql.SQL("retry_count = 0"))

            cur.execute(
                sql.SQL("UPDATE {} SET {} WHERE id = %s").format(
                    sql.Identifier(table_name), sql.SQL(", ").join(sets)),
                params + [item_id])
            conn.commit()
            return cur.rowcount == 1
    finally:
        if conn: release_db_connection(conn)

def update_clip_state_sync(clip_id: int, new_state: str, error_message=None) -> bool:
    # Example processing state for clip might be 'processing_post_review', 'merging', 'splitting' etc.
    # Pass the relevant 'in-progress' state here if needed for retry logic reset.
    # Using None means retry_count won't be reset by this generic helper.
    return _update_state_sync("clips", clip_id, new_state, None, error_message)

def update_source_video_state_sync(source_video_id: int, new_state: str, error_message=None) -> bool:
    return _update_state_sync("source_videos", source_video_id, new_state, "downloading", error_message)
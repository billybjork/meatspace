import os
import sys
import re
import subprocess
import tempfile
from pathlib import Path
import shutil
import json
import math
from datetime import datetime

from prefect import task, get_run_logger
import psycopg2
from psycopg2 import sql, extras

from botocore.exceptions import ClientError

# --- Project Root Setup ---
try:
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
    from utils.db_utils import get_db_connection, release_db_connection
except ImportError:
    # Fallback if structure is different or running standalone
    try:
        # Try direct import if utils is in the same directory or PYTHONPATH
        from utils.db_utils import get_db_connection, release_db_connection
    except ImportError as e:
        print(f"ERROR: Cannot find db_utils. Ensure script is run from project root or PYTHONPATH is set correctly. {e}", file=sys.stderr)
        # Define dummies only if absolutely necessary for script loading
        def get_db_connection(cursor_factory=None): raise NotImplementedError("Dummy DB connection getter")
        def release_db_connection(conn): pass # Dummy release

# --- Import Shared Components ---
s3_client = None
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
FFMPEG_PATH = os.getenv("FFMPEG_PATH", "ffmpeg")

try:
    # Attempt to import from splice first (might have pre-initialized client)
    from .splice import s3_client as splice_s3_client, run_ffmpeg_command, sanitize_filename
    if splice_s3_client: s3_client = splice_s3_client
    # FFMPEG_PATH might also be defined in splice, prefer env var or default 'ffmpeg'
    # Keep run_ffmpeg_command and sanitize_filename from splice if available
    print("INFO: Imported shared components (S3 client?, ffmpeg runner, sanitizer) from tasks.splice")

except ImportError as e:
     print(f"INFO: Could not import components from .splice, using defaults/direct init. Error: {e}")
     # Define fallbacks if splice import fails
     def run_ffmpeg_command(cmd, step, cwd=None):
         # Basic fallback implementation
         logger = get_run_logger() # Get logger inside function if needed
         logger.info(f"Executing Fallback FFMPEG Step: {step}")
         logger.debug(f"Command: {' '.join(cmd)}")
         try:
             result = subprocess.run(cmd, capture_output=True, text=True, check=True, cwd=cwd, encoding='utf-8', errors='replace')
             logger.debug(f"FFMPEG Output:\n{result.stdout[:500]}...") # Log partial output
             if result.stderr: logger.warning(f"FFMPEG Stderr:\n{result.stderr[:500]}...")
             return result
         except FileNotFoundError:
              logger.error(f"ERROR: {cmd[0]} command not found at path '{FFMPEG_PATH}'.")
              raise
         except subprocess.CalledProcessError as e:
              logger.error(f"ERROR: {step} failed. Exit code: {e.returncode}")
              logger.error(f"Stderr:\n{e.stderr}")
              raise
     def sanitize_filename(name):
         # Basic fallback sanitization
         name = re.sub(r'[^\w\.\-]+', '_', name)
         name = re.sub(r'_+', '_', name).strip('_')
         return name if name else "default_filename"

# Initialize S3 client if not imported
if not s3_client and S3_BUCKET_NAME:
     try:
         import boto3
         s3_client = boto3.client('s3', region_name=os.getenv("AWS_REGION"))
         print("INFO: Initialized default Boto3 S3 client in sprite_generator.")
     except ImportError:
         print("ERROR: Boto3 required but not installed.", file=sys.stderr)
         s3_client = None # Ensure it's None
     except Exception as boto_err:
         print(f"ERROR: Failed to initialize fallback Boto3 client: {boto_err}", file=sys.stderr)
         s3_client = None # Ensure it's None

# Final check for S3 client after potential initializations
if not S3_BUCKET_NAME: print("WARNING: S3_BUCKET_NAME environment variable not set.")
# S3 client check happens within the task function

# --- Constants ---
SPRITE_SHEET_S3_PREFIX = os.getenv("SPRITE_SHEET_S3_PREFIX", "clip_artifacts/sprite_sheets/")
SPRITE_TILE_WIDTH = int(os.getenv("SPRITE_TILE_WIDTH", 480))
SPRITE_TILE_HEIGHT = int(os.getenv("SPRITE_TILE_HEIGHT", -1)) # -1 maintains aspect ratio
SPRITE_FPS = int(os.getenv("SPRITE_FPS", 24))
SPRITE_COLS = int(os.getenv("SPRITE_COLS", 5))
ARTIFACT_TYPE_SPRITE_SHEET = "sprite_sheet" # Constant for artifact type


@task(name="Generate Sprite Sheet", retries=1, retry_delay_seconds=45)
def generate_sprite_sheet_task(clip_id: int):
    """
    Generates a sprite sheet for a given clip, uploads it to S3,
    and records it as an artifact in the clip_artifacts table.
    Updates the clip state upon success or failure.
    """
    logger = get_run_logger()
    logger.info(f"TASK [SpriteGen]: Starting for clip_id: {clip_id}")

    # --- Resource Management & State ---
    conn = None
    temp_dir_obj = None
    temp_dir = None
    clip_data = {}
    task_outcome = "failed" # Default outcome status: 'success', 'success_no_sprite', 'skipped_state', 'failed', 'failed_db...'
    intended_success_state = "pending_review" # State to set on success (with or without sprite)
    failure_state = "sprite_generation_failed" # State to set on failure
    error_message = None
    sprite_s3_key = None
    sprite_artifact_id = None # Store the ID of the created/updated artifact

    # --- Dependency Checks ---
    if not s3_client:
        logger.error("S3 client is not available. Cannot proceed.")
        # Fail fast if essential dependencies are missing
        raise RuntimeError("S3 client not initialized.")
    if not shutil.which(FFMPEG_PATH):
        logger.error(f"ffmpeg command ('{FFMPEG_PATH}') not found in PATH.")
        raise FileNotFoundError(f"ffmpeg command not found at '{FFMPEG_PATH}'.")
    if not shutil.which("ffprobe"):
         logger.warning("ffprobe command not found in PATH. Metadata extraction may fail or be inaccurate.")
         # Depending on robustness of ffprobe fallback logic, this might need to be an error.

    try:
        # === Phase 1: DB Check, Lock, and State Update ===
        conn = get_db_connection(cursor_factory=extras.DictCursor) # Use DictCursor for easy column access
        conn.autocommit = False # Manual transaction control

        try:
            with conn.cursor() as cur:
                # 1. Acquire Lock (Use advisory lock within the transaction)
                # Note: pg_advisory_xact_lock locks for the current transaction
                cur.execute("SELECT pg_advisory_xact_lock(2, %s)", (clip_id,))
                logger.info(f"Acquired DB lock for clip {clip_id} for duration of transaction.")

                # 2. Fetch clip data AND source title
                cur.execute(
                    """
                    SELECT
                        c.clip_filepath, c.clip_identifier, c.start_time_seconds, c.end_time_seconds,
                        c.source_video_id, c.ingest_state, c.start_frame, c.end_frame,
                        sv.title AS source_title  -- Fetch source title
                    FROM clips c
                    JOIN source_videos sv ON c.source_video_id = sv.id -- Join source_videos
                    WHERE c.id = %s FOR UPDATE;
                    """, (clip_id,)
                )
                clip_data = cur.fetchone()

                if not clip_data:
                     raise ValueError(f"Clip {clip_id} not found.")

                # 3. Check current state
                current_state = clip_data['ingest_state']
                # Allow running if pending OR if failed (for retries)
                if current_state not in ['pending_sprite_generation', 'sprite_generation_failed']:
                    logger.warning(f"Clip {clip_id} not in 'pending_sprite_generation' or 'sprite_generation_failed' state (state: {current_state}). Skipping sprite generation.")
                    task_outcome = "skipped_state"
                    conn.rollback() # Release lock and row lock by rolling back
                    return {"status": task_outcome, "reason": f"Incorrect state: {current_state}", "clip_id": clip_id}

                # 4. Validate clip filepath
                if not clip_data['clip_filepath']:
                     raise ValueError(f"Clip {clip_id} is missing required 'clip_filepath'.")

                # 5. Update state to 'generating_sprite'
                cur.execute(
                    "UPDATE clips SET ingest_state = 'generating_sprite', updated_at = NOW(), last_error = NULL WHERE id = %s",
                    (clip_id,)
                )
                logger.info(f"Set clip {clip_id} state to 'generating_sprite'")

            # Commit the state update and keep the transaction open if needed,
            # or commit here to release the lock sooner if subsequent phases are long.
            # Let's commit here to reduce lock duration.
            conn.commit()
            logger.debug("Phase 1 DB check/update transaction committed.")

        except (ValueError, psycopg2.DatabaseError) as db_err:
            logger.error(f"DB Error during initial check/update for sprite gen clip {clip_id}: {db_err}", exc_info=True)
            if conn: conn.rollback() # Ensure rollback on error
            error_message = f"DB Init Error: {str(db_err)[:500]}"
            task_outcome = "failed_db_init"
            raise # Re-raise to main handler


        # === Phase 2: Main Processing (Download, Probe, Generate, Upload) ===
        clip_s3_key = clip_data['clip_filepath']
        clip_identifier = clip_data['clip_identifier'] # Used for filename generation

        temp_dir_obj = tempfile.TemporaryDirectory(prefix=f"meatspace_spritegen_{clip_id}_")
        temp_dir = Path(temp_dir_obj.name)
        local_clip_path = temp_dir / Path(clip_s3_key).name # Use Path object methods

        logger.info(f"Downloading clip s3://{S3_BUCKET_NAME}/{clip_s3_key} to {local_clip_path}...")
        try:
             s3_client.download_file(S3_BUCKET_NAME, clip_s3_key, str(local_clip_path))
        except ClientError as s3_err:
             logger.error(f"Failed to download clip {clip_s3_key} from S3: {s3_err}")
             raise RuntimeError(f"S3 download failed for {clip_s3_key}") from s3_err

        # --- Get Video Duration/Frames using ffprobe ---
        duration = 0.0
        fps = 0.0
        total_frames_in_clip = 0
        # Default sprite metadata in case probing fails partially
        sprite_metadata = None

        try:
             logger.info(f"Probing downloaded clip: {local_clip_path}")
             ffprobe_cmd = [
                 "ffprobe", "-v", "error", "-select_streams", "v:0",
                 "-show_entries", "stream=duration,r_frame_rate,nb_frames", # Try nb_frames first
                 "-count_frames", # Ensure frame count is attempted
                 "-of", "json", str(local_clip_path)
             ]
             result = subprocess.run(ffprobe_cmd, capture_output=True, text=True, check=True, encoding='utf-8', errors='replace')
             probe_data = json.loads(result.stdout)['streams'][0]
             logger.debug(f"ffprobe result for clip {clip_id}: {probe_data}")

             # Get Duration
             duration_str = probe_data.get('duration')
             if duration_str:
                 try: duration = float(duration_str)
                 except (ValueError, TypeError): logger.warning(f"Could not parse duration '{duration_str}' to float.")
             # Get FPS
             fps_str = probe_data.get('r_frame_rate', '0/1')
             if '/' in fps_str:
                 num_str, den_str = fps_str.split('/')
                 try:
                     num, den = int(num_str), int(den_str)
                     if den > 0: fps = num / den
                 except (ValueError, TypeError): logger.warning(f"Could not parse r_frame_rate: {fps_str}")

             # Get Frame Count (Prefer nb_frames if available and looks valid)
             nb_frames_str = probe_data.get('nb_frames')
             if nb_frames_str:
                 try:
                     total_frames_in_clip = int(nb_frames_str)
                     logger.info(f"Using ffprobe nb_frames: {total_frames_in_clip}")
                 except (ValueError, TypeError): logger.warning(f"Could not parse nb_frames '{nb_frames_str}' to int.")

             # Calculate total frames based on duration and fps if frame count is missing/invalid
             if total_frames_in_clip <= 0 and duration > 0 and fps > 0:
                 calc_frames = math.ceil(duration * fps)
                 # Add a small tolerance - sometimes duration * fps slightly underestimates
                 total_frames_in_clip = calc_frames + 1 if calc_frames > 0 else 0
                 logger.info(f"Calculated total frames from duration*fps: {total_frames_in_clip} (using ceil+1)")

             # Final validation and logging
             if duration <= 0 or fps <= 0 or total_frames_in_clip <= 0:
                  raise ValueError(f"Unable to establish valid duration ({duration:.3f}s), fps ({fps:.3f}), or total frames ({total_frames_in_clip}) for clip {clip_id}.")
             logger.info(f"Clip {clip_id} Probe Results: Duration={duration:.3f}s, FPS={fps:.3f}, Total Frames={total_frames_in_clip}")

        except subprocess.CalledProcessError as probe_err:
             logger.error(f"ffprobe command failed for {local_clip_path}. Error: {probe_err.stderr}", exc_info=False)
             raise ValueError(f"ffprobe failed, cannot generate sprite sheet.") from probe_err
        except (json.JSONDecodeError, KeyError, IndexError) as parse_err:
             logger.error(f"Failed to parse ffprobe JSON output for {local_clip_path}: {parse_err}", exc_info=True)
             raise ValueError(f"ffprobe JSON parsing failed.") from parse_err
        except ValueError as val_err: # Catch our specific validation error
             logger.error(f"Metadata validation failed: {val_err}", exc_info=False)
             raise # Re-raise the specific error
        except Exception as probe_err: # Catch any other unexpected error
             logger.error(f"Unexpected error during ffprobe/metadata calculation: {probe_err}", exc_info=True)
             raise ValueError(f"Metadata calculation failed.") from probe_err

        # --- Generate Sprite Sheet ---
        # Calculate required frames for the sprite sheet based on desired sprite FPS
        num_sprite_frames = math.ceil(duration * SPRITE_FPS)

        if num_sprite_frames <= 0:
            logger.warning(f"Clip {clip_id} duration too short ({duration:.3f}s) or SPRITE_FPS ({SPRITE_FPS}) too low. Skipping sprite sheet generation ({num_sprite_frames} frames calculated).")
            task_outcome = "success_no_sprite" # Mark as success, but indicate no sprite generated
            sprite_s3_key = None # Ensure key is None
            sprite_metadata = None # Ensure metadata is None
        else:
            # Sanitize source title for use in path
            source_title = clip_data.get('source_title', f'source_{clip_data["source_video_id"]}') # Fallback if title is missing
            sanitized_source_title = sanitize_filename(source_title)

            # Construct sprite sheet filename and S3 key including sanitized source title
            safe_clip_identifier = sanitize_filename(clip_identifier) # clip_identifier already fetched
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            sprite_filename = f"{safe_clip_identifier}_sprite_{SPRITE_FPS}fps_w{SPRITE_TILE_WIDTH}_c{SPRITE_COLS}_{timestamp}.jpg"
            local_sprite_path = temp_dir / sprite_filename
            s3_base_prefix = SPRITE_SHEET_S3_PREFIX.strip('/') + '/' # Base artifact prefix
            # Add sanitized source title to the S3 key path
            sprite_s3_key = f"{s3_base_prefix}{sanitized_source_title}/{sprite_filename}"

            # Calculate grid dimensions
            num_rows = max(1, math.ceil(num_sprite_frames / SPRITE_COLS))

            # Construct ffmpeg command using vf filtergraph
            vf_filter = f"fps={SPRITE_FPS},scale={SPRITE_TILE_WIDTH}:{SPRITE_TILE_HEIGHT}:flags=neighbor,tile={SPRITE_COLS}x{num_rows}"
            ffmpeg_sprite_cmd = [
                FFMPEG_PATH, '-y', '-i', str(local_clip_path),
                '-vf', vf_filter, '-an', '-qscale:v', '3',
                # Use -vframes instead of -frames:v for limiting output frames
                '-vframes', str(num_sprite_frames),
                str(local_sprite_path)
            ]

            logger.info(f"Generating {SPRITE_COLS}x{num_rows} sprite sheet ({num_sprite_frames} frames) for clip {clip_id}...")
            try:
                run_ffmpeg_command(ffmpeg_sprite_cmd, "ffmpeg Generate Sprite Sheet")
            except Exception as ffmpeg_err:
                 logger.error(f"ffmpeg sprite generation failed: {ffmpeg_err}", exc_info=True)
                 raise RuntimeError("FFmpeg sprite generation failed") from ffmpeg_err

            # Verify output file exists before upload
            if not local_sprite_path.is_file() or local_sprite_path.stat().st_size == 0:
                 raise RuntimeError(f"FFmpeg completed but output sprite file is missing or empty: {local_sprite_path}")

            logger.info(f"Uploading sprite sheet to s3://{S3_BUCKET_NAME}/{sprite_s3_key}")
            try:
                with open(local_sprite_path, "rb") as f:
                    s3_client.upload_fileobj(f, S3_BUCKET_NAME, sprite_s3_key)
            except ClientError as s3_upload_err:
                 logger.error(f"Failed to upload sprite sheet {sprite_s3_key}: {s3_upload_err}")
                 raise RuntimeError(f"S3 upload failed for sprite sheet {sprite_s3_key}") from s3_upload_err

            # --- Prepare and Validate Metadata (Probe the generated sprite sheet) ---
            calculated_tile_height = None # Initialize
            try:
                logger.info(f"Probing generated sprite sheet: {local_sprite_path}")
                probe_sprite_cmd = ["ffprobe", "-v", "error", "-select_streams", "v:0", "-show_entries", "stream=width,height", "-of", "json", str(local_sprite_path)]
                result_sprite = subprocess.run(probe_sprite_cmd, capture_output=True, text=True, check=True, encoding='utf-8', errors='replace')
                sprite_dims = json.loads(result_sprite.stdout)['streams'][0]

                total_sprite_height = int(sprite_dims.get('height', 0))
                if total_sprite_height > 0 and num_rows > 0:
                     calculated_tile_height = math.floor(total_sprite_height / num_rows)
                     logger.info(f"Calculated sprite tile height from probe: {calculated_tile_height}")
                else: logger.warning("Could not calculate valid tile height from sprite probe.")

            except Exception as sprite_probe_err:
                logger.warning(f"Could not probe generated sprite sheet dimensions: {sprite_probe_err}. Tile height will be null.")
                # Proceed without calculated height, frontend might need fallback logic

            # Final Metadata object for clip_artifacts.metadata
            sprite_metadata = {
                 "tile_width": SPRITE_TILE_WIDTH,
                 "tile_height_calculated": calculated_tile_height, # Can be None
                 "cols": SPRITE_COLS,
                 "rows": num_rows,
                 "total_sprite_frames": num_sprite_frames, # Frames *in* the sprite
                 "clip_fps_source": round(fps, 3), # Original clip FPS used for calcs
                 "clip_total_frames_source": total_frames_in_clip # Original clip frames
             }
            logger.info(f"Sprite sheet generated and uploaded for clip {clip_id}. Metadata: {json.dumps(sprite_metadata)}")
            # Task outcome will be set to 'success' after DB commit


        # === Phase 3: Final DB Update (Insert Artifact, Update Clip State) ===
        # We committed Phase 1, so this needs its own transaction.
        # Re-acquire the connection if necessary (though it should still be open if Phase 1 succeeded)
        # Ensure autocommit is still False for this transaction block
        conn.autocommit = False
        try:
            # Use a cursor and manage transaction manually
            with conn.cursor() as cur:
                 logger.debug(f"Starting final DB transaction for clip {clip_id}...")

                 # 1. Insert/Update Artifact Record (only if sprite was generated)
                 if sprite_s3_key and sprite_metadata:
                     logger.info(f"Inserting/updating sprite artifact record for clip {clip_id}")
                     artifact_insert_sql = sql.SQL("""
                         INSERT INTO clip_artifacts (
                             clip_id, artifact_type, strategy, tag, s3_key, metadata, created_at, updated_at
                         ) VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW())
                         ON CONFLICT (clip_id, artifact_type, strategy, tag) DO UPDATE SET
                             s3_key = EXCLUDED.s3_key,
                             metadata = EXCLUDED.metadata,
                             updated_at = NOW()
                         RETURNING id; -- Return the ID of the inserted/updated row
                     """)
                     # Strategy and tag are null for sprite sheets currently
                     artifact_strategy = None
                     artifact_tag = None
                     # Use extras.Json to wrap the metadata dict for psycopg2
                     cur.execute(
                         artifact_insert_sql,
                         (clip_id, ARTIFACT_TYPE_SPRITE_SHEET, artifact_strategy, artifact_tag,
                          sprite_s3_key, extras.Json(sprite_metadata)) # Use extras.Json
                     )
                     inserted_artifact = cur.fetchone() # Fetch the result (which is a DictRow)
                     if inserted_artifact:
                          sprite_artifact_id = inserted_artifact['id'] # Access by key
                          logger.info(f"Successfully inserted/updated artifact record, ID: {sprite_artifact_id}")
                     else:
                          # This shouldn't happen with RETURNING id unless insert failed silently?
                          logger.error("Artifact insert/update query did not return an ID.")
                          raise psycopg2.DatabaseError("Artifact insert/update failed unexpectedly.")

                 # 2. Update Clip Status
                 logger.info(f"Updating clip {clip_id} state to '{intended_success_state}'")
                 clip_update_sql = sql.SQL("""
                     UPDATE clips
                     SET ingest_state = %s,
                         updated_at = NOW(),
                         last_error = NULL -- Clear previous errors
                     WHERE id = %s AND ingest_state = 'generating_sprite'; -- Concurrency check
                 """)
                 cur.execute(clip_update_sql, (intended_success_state, clip_id))

                 # Check if the update was successful (important concurrency check)
                 if cur.rowcount == 1 or cur.statusmessage == "UPDATE 1": # Check statusmessage or rowcount
                      logger.info(f"Successfully updated clip {clip_id} state.")
                      # Set task outcome to success variants *only after* successful commit is confirmed
                      task_outcome = task_outcome if task_outcome == "success_no_sprite" else "success"
                 else:
                      logger.error(f"Final clip state update failed for clip {clip_id}. Status: '{cur.statusmessage}', Rowcount: {cur.rowcount}. State mismatch or row deleted? Rolling back transaction.")
                      # Exception below will trigger rollback
                      task_outcome = "failed_db_update_state_mismatch"
                      raise RuntimeError(f"Failed to update clip {clip_id} final state (expected 1 row, got {cur.rowcount}).")

            # If all operations within the 'with cur:' block succeed, commit.
            conn.commit()
            logger.debug(f"Final DB transaction committed for clip {clip_id}.")

        except (psycopg2.DatabaseError, psycopg2.OperationalError, RuntimeError) as db_err:
            logger.error(f"DB Error during final update/artifact insert for clip {clip_id}: {db_err}", exc_info=True)
            if conn: conn.rollback() # Rollback the failed transaction
            task_outcome = "failed_db_final_update"
            error_message = f"DB Final Update Error: {str(db_err)[:500]}"
            raise # Re-raise to main handler


    # === Main Error Handling ===
    except Exception as e:
        logger.error(f"TASK FAILED [SpriteGen]: clip_id {clip_id} - {type(e).__name__}: {e}", exc_info=True)
        # Ensure outcome reflects failure if not already set more specifically
        if not task_outcome.startswith("failed"): task_outcome = "failed"
        if not error_message: error_message = f"Task Error: {type(e).__name__}: {str(e)[:450]}"

        # Attempt to update DB state to failure state
        if conn:
            try:
                conn.rollback() # Rollback any partial work before trying error update
            except Exception as rb_err:
                logger.warning(f"Error during rollback before failure update: {rb_err}")
        error_conn = None
        try:
             # Use a separate connection with autocommit for error state update
             # Ensure db_utils provides a fresh connection correctly
             error_conn = get_db_connection()
             if not error_conn:
                 logger.critical(f"CRITICAL: Failed to get separate DB connection to update error state for clip {clip_id}")
                 raise RuntimeError("Cannot get error update DB connection") # Fail fast

             error_conn.autocommit = True # Set autocommit for immediate execution
             with error_conn.cursor() as err_cur:
                 logger.info(f"Attempting to set clip {clip_id} state to '{failure_state}' after error...")
                 err_cur.execute(
                     """
                     UPDATE clips SET
                         ingest_state = %s,
                         last_error = %s,
                         retry_count = COALESCE(retry_count, 0) + 1,
                         updated_at = NOW()
                     WHERE id = %s AND ingest_state = 'generating_sprite'; -- Only update if it was processing
                     """,
                     (failure_state, error_message, clip_id)
                 )
                 logger.info(f"DB update executed to set clip {clip_id} to '{failure_state}'. Status: {err_cur.statusmessage}")
        except Exception as db_err_update:
             # Log critical error, but don't prevent the original exception from propagating
             logger.critical(f"CRITICAL: Failed to update error state in DB for clip {clip_id}: {db_err_update}")
        finally:
             if error_conn:
                 release_db_connection(error_conn)

        # Reraise the original exception for Prefect to handle retries/failures
        raise e

    finally:
        # --- Final Logging ---
        log_message = f"TASK [SpriteGen] Result: clip_id={clip_id}, outcome={task_outcome}"
        final_db_state_expected = ""
        if task_outcome == "success":
            final_db_state_expected = intended_success_state
            log_message += f", new_state={final_db_state_expected}, artifact_id={sprite_artifact_id}, sprite_key={sprite_s3_key}"
            logger.info(log_message)
        elif task_outcome == "success_no_sprite":
            final_db_state_expected = intended_success_state
            log_message += f", new_state={final_db_state_expected} (no sprite generated)"
            logger.info(log_message)
        elif task_outcome == "skipped_state":
            log_message += " (skipped due to initial state check)"
            logger.warning(log_message)
        else: # All failure types
            final_db_state_expected = failure_state
            log_message += f", final_state_attempted={final_db_state_expected}, error='{error_message}'"
            logger.error(log_message)

        # --- Resource Cleanup ---
        if conn:
            try:
                # Ensure connection is reset before releasing if necessary
                conn.autocommit = True # Reset autocommit state
                conn.rollback() # Rollback any potentially uncommitted changes if code ended unexpectedly
            except Exception as conn_cleanup_err:
                logger.warning(f"Exception during final connection cleanup for clip {clip_id}: {conn_cleanup_err}")
            finally:
                release_db_connection(conn)
                logger.debug(f"DB connection released for clip {clip_id}.")
        if temp_dir_obj:
            try:
                shutil.rmtree(temp_dir_obj.name, ignore_errors=True)
                logger.info(f"Cleaned up temporary directory: {temp_dir_obj.name}")
            except Exception as cleanup_err:
                 logger.warning(f"Error cleaning up temp dir {temp_dir_obj.name}: {cleanup_err}")

    # Return outcome dictionary
    return {
        "status": task_outcome,
        "clip_id": clip_id,
        "sprite_sheet_key": sprite_s3_key,
        "artifact_id": sprite_artifact_id,
        "final_db_state": final_db_state_expected, # What the state *should* be based on outcome
        "error": error_message
    }
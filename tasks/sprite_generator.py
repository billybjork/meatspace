# --- START OF FILE tasks/sprite_generator.py ---
import os
import subprocess
import tempfile
from pathlib import Path
import shutil
import json
import math
from prefect import task, get_run_logger
import psycopg2
from psycopg2 import sql, extras
from botocore.exceptions import ClientError

try:
    from utils.db_utils import get_db_connection
except ImportError:
    # Basic fallback for path issues during import
    import sys
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')) # Adjust path if needed
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
    try:
        from utils.db_utils import get_db_connection
    except ImportError as e:
        print(f"ERROR importing db_utils in sprite_generator.py after path adjustment: {e}")
        def get_db_connection(): raise NotImplementedError("Dummy DB connection")


# Reuse existing components where possible
try:
    from .splice import (
        s3_client, S3_BUCKET_NAME, FFMPEG_PATH, run_ffmpeg_command,
        sanitize_filename
    )
    if not S3_BUCKET_NAME:
        raise ImportError("S3_BUCKET_NAME not configured in splice module")
    if not FFMPEG_PATH:
        FFMPEG_PATH = "ffmpeg" # Provide default if not imported
        print("Warning: FFMPEG_PATH not imported from splice, defaulting to 'ffmpeg'")

except ImportError as e:
     print(f"ERROR importing from .splice in sprite_generator.py: {e}")
     # Define fallbacks or raise error if essential components are missing
     s3_client = None # Or initialize directly here if needed
     S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
     FFMPEG_PATH = "ffmpeg"
     if not S3_BUCKET_NAME: raise ValueError("S3_BUCKET_NAME environment variable not set.")
     # Define dummy run_ffmpeg_command if needed for basic script loading
     def run_ffmpeg_command(cmd, step, cwd=None): raise NotImplementedError("Dummy ffmpeg runner")
     def sanitize_filename(name): return "sanitized_dummy"


SPRITE_SHEET_S3_PREFIX = "sprite_sheets/"
SPRITE_TILE_WIDTH = 160  # Keep desired display width (pixels)
SPRITE_TILE_HEIGHT = -1 # Auto-calculate height based on aspect ratio
SPRITE_FPS = 24          # Target FPS for sprite sheet frames
SPRITE_COLS = 5          # <<< REDUCED COLUMNS for vertical video test

@task(name="Generate Sprite Sheet", retries=1, retry_delay_seconds=45)
def generate_sprite_sheet_task(clip_id: int):
    """
    Generates a sprite sheet for a given clip, uploads it to S3,
    and updates the clip's DB record with the path and metadata.
    Transitions state from 'pending_sprite_generation' to 'pending_review'.
    """
    logger = get_run_logger()
    logger.info(f"TASK [SpriteGen]: Starting for clip_id: {clip_id} (Target FPS: {SPRITE_FPS}, Cols: {SPRITE_COLS})")
    conn = None
    temp_dir_obj = None
    clip_data = {}
    final_state = "sprite_generation_failed" # Default to failure

    # --- Dependency Checks ---
    if not s3_client:
        logger.error("S3 client not available.")
        raise RuntimeError("S3 client not initialized.")
    if not shutil.which(FFMPEG_PATH):
        logger.error(f"ffmpeg command ('{FFMPEG_PATH}') not found in PATH.")
        raise FileNotFoundError(f"ffmpeg ('{FFMPEG_PATH}') not found.")
    if not shutil.which("ffprobe"): # ffprobe is usually bundled with ffmpeg
         logger.warning("ffprobe command not found in PATH. Metadata extraction will be limited.")
         # Decide if this is fatal or just a warning based on requirements


    try:
        conn = get_db_connection()
        conn.autocommit = False # Manual transaction control

        temp_dir_obj = tempfile.TemporaryDirectory(prefix=f"meatspace_spritegen_{clip_id}_")
        temp_dir = Path(temp_dir_obj.name)
        logger.info(f"Using temporary directory: {temp_dir}")

        # === DB Check and State Update ===
        try:
            with conn.cursor(cursor_factory=extras.DictCursor) as cur:
                cur.execute("SELECT pg_advisory_xact_lock(2, %s)", (clip_id,))
                logger.debug(f"Acquired lock for clip {clip_id}")
                cur.execute(
                    """
                    SELECT clip_filepath, clip_identifier, start_time_seconds, end_time_seconds,
                           source_video_id, ingest_state, start_frame, end_frame
                    FROM clips
                    WHERE id = %s FOR UPDATE;
                    """, (clip_id,)
                )
                clip_data = cur.fetchone()

                if not clip_data: raise ValueError(f"Clip {clip_id} not found.")
                if clip_data['ingest_state'] != 'pending_sprite_generation':
                    logger.warning(f"Clip {clip_id} not in 'pending_sprite_generation' state (state: {clip_data['ingest_state']}). Skipping.")
                    conn.rollback() # Release lock
                    return {"status": "skipped", "reason": f"Incorrect state: {clip_data['ingest_state']}"}
                if not clip_data['clip_filepath']: raise ValueError("Clip filepath missing.")

                cur.execute("UPDATE clips SET ingest_state = 'generating_sprite', updated_at = NOW() WHERE id = %s", (clip_id,))
                logger.info(f"Set clip {clip_id} state to 'generating_sprite'")
            conn.commit() # Commit short transaction
            logger.debug("Initial check/state update committed.")

        except (ValueError, psycopg2.DatabaseError) as db_err:
            logger.error(f"DB Error during initial check/update for sprite gen: {db_err}", exc_info=True)
            if conn: conn.rollback()
            raise

        # === Main Processing ===
        clip_s3_key = clip_data['clip_filepath']
        clip_identifier = clip_data['clip_identifier']
        local_clip_path = temp_dir / Path(clip_s3_key).name

        logger.info(f"Downloading clip s3://{S3_BUCKET_NAME}/{clip_s3_key} to {local_clip_path}...")
        s3_client.download_file(S3_BUCKET_NAME, clip_s3_key, str(local_clip_path))

        # --- Get Video Duration/Frames using ffprobe ---
        duration = 0.0
        fps = 0.0
        total_frames_in_clip = 0
        try:
             logger.info(f"Probing original clip: {local_clip_path}")
             ffprobe_cmd = [ "ffprobe", "-v", "error", "-select_streams", "v:0",
                             "-show_entries", "stream=duration,r_frame_rate,nb_read_frames", # Use nb_read_frames for more accuracy?
                             "-count_frames", # Explicitly count frames
                             "-of", "json", str(local_clip_path) ]
             result = subprocess.run(ffprobe_cmd, capture_output=True, text=True, check=True, encoding='utf-8')
             probe_data = json.loads(result.stdout)['streams'][0]
             logger.debug(f"ffprobe result for clip {clip_id}: {probe_data}")

             # Get Duration
             duration_str = probe_data.get('duration')
             if duration_str:
                 try: duration = float(duration_str)
                 except (ValueError, TypeError): logger.warning(f"Could not parse duration '{duration_str}' to float.")
             else: logger.warning("ffprobe did not return duration for clip.")

             # Get FPS
             fps_str = probe_data.get('r_frame_rate', '0/1')
             if '/' in fps_str:
                 num_str, den_str = fps_str.split('/')
                 try:
                     num = int(num_str)
                     den = int(den_str)
                     if den > 0: fps = num / den
                     else: logger.warning(f"Invalid denominator in r_frame_rate: {fps_str}")
                 except (ValueError, TypeError): logger.warning(f"Could not parse r_frame_rate components: {fps_str}")
             else: logger.warning(f"Unexpected r_frame_rate format: {fps_str}")

             # Get Frame Count (Prefer nb_read_frames if available)
             nb_frames_str = probe_data.get('nb_read_frames')
             if nb_frames_str:
                 try:
                     total_frames_in_clip = int(nb_frames_str)
                     logger.info(f"Using nb_read_frames: {total_frames_in_clip}")
                 except (ValueError, TypeError): logger.warning(f"Could not parse nb_read_frames '{nb_frames_str}' to int.")
             else: logger.warning("ffprobe did not return nb_read_frames.")


             # Calculate total frames based on duration and fps if frame count is missing/invalid
             if total_frames_in_clip <= 0 and duration > 0 and fps > 0:
                 total_frames_in_clip = math.ceil(duration * fps)
                 logger.info(f"Calculated total frames from duration*fps: {total_frames_in_clip}")

             # Final sanity check on frames
             if total_frames_in_clip <= 0:
                 # Last resort: try calculating from DB start/end frames if available
                 db_start = clip_data.get('start_frame')
                 db_end = clip_data.get('end_frame')
                 if db_start is not None and db_end is not None:
                     calc_frames = db_end - db_start
                     if calc_frames > 0:
                         total_frames_in_clip = calc_frames
                         logger.warning(f"Using frame count from DB start/end frame difference: {total_frames_in_clip}")
                     else:
                         raise ValueError(f"Cannot determine positive frame count for clip {clip_id} from ffprobe or DB frames.")
                 else:
                     raise ValueError(f"Cannot determine positive frame count for clip {clip_id}. ffprobe failed.")

             # Recalculate duration/fps if needed and possible, based on the most reliable frame count
             if duration <= 0 and total_frames_in_clip > 0 and fps > 0:
                 duration = total_frames_in_clip / fps
                 logger.info(f"Recalculated duration from frames/fps: {duration:.3f}s")
             if fps <= 0 and total_frames_in_clip > 0 and duration > 0:
                 fps = total_frames_in_clip / duration
                 logger.info(f"Recalculated FPS from frames/duration: {fps:.3f}")

             # Final check: need positive values for sprite generation
             if duration <= 0 or fps <= 0 or total_frames_in_clip <= 0:
                  raise ValueError(f"Unable to establish valid duration ({duration:.3f}), fps ({fps:.3f}), or total frames ({total_frames_in_clip}) for clip {clip_id}.")

             logger.info(f"Final Clip {clip_id} Probe: Duration={duration:.3f}s, FPS={fps:.3f}, Total Frames={total_frames_in_clip}")

        except subprocess.CalledProcessError as probe_err:
             logger.error(f"ffprobe command failed for {local_clip_path}. Exit code: {probe_err.returncode}. Error: {probe_err.stderr}", exc_info=False) # Don't need full traceback for CalledProcessError usually
             raise ValueError(f"ffprobe failed for clip {clip_id}, cannot generate sprite sheet.") from probe_err
        except json.JSONDecodeError as json_err:
             logger.error(f"Failed to parse ffprobe JSON output for {local_clip_path}: {json_err}", exc_info=True)
             raise ValueError(f"ffprobe JSON parsing failed for clip {clip_id}") from json_err
        except KeyError as key_err:
            logger.error(f"Missing expected key in ffprobe output for {local_clip_path}: {key_err}", exc_info=True)
            raise ValueError(f"ffprobe output structure unexpected for clip {clip_id}") from key_err
        except Exception as probe_err:
             logger.error(f"Unexpected error during ffprobe/metadata calculation for {local_clip_path}: {probe_err}", exc_info=True)
             raise ValueError(f"Metadata calculation failed for clip {clip_id}") from probe_err


        # --- Generate Sprite Sheet ---
        # Calculate target number of frames for the sprite sheet
        num_sprite_frames = math.ceil(duration * SPRITE_FPS)
        logger.info(f"Targeting {num_sprite_frames} frames for sprite sheet (Duration: {duration:.2f}s, Sprite FPS: {SPRITE_FPS})")

        if num_sprite_frames <= 0:
            logger.warning(f"Clip {clip_id} duration too short or calculated sprite frames zero ({num_sprite_frames}). Skipping sprite sheet generation.")
            final_state = "pending_review" # Move to review without sprite
            sprite_s3_key = None
            sprite_metadata = None
        else:
            # Use the NEW SPRITE_COLS value in filename for clarity
            sprite_filename = f"{sanitize_filename(clip_identifier)}_sprite_{SPRITE_FPS}fps_c{SPRITE_COLS}.jpg"
            local_sprite_path = temp_dir / sprite_filename
            sprite_s3_key = f"{SPRITE_SHEET_S3_PREFIX}{sprite_filename}"

            # Calculate number of rows needed based on the NEW SPRITE_COLS, ensure at least 1
            num_rows = max(1, math.ceil(num_sprite_frames / SPRITE_COLS))

            # Construct ffmpeg command using the vf (video filter) complex filtergraph
            vf_filter = f"fps={SPRITE_FPS},scale={SPRITE_TILE_WIDTH}:{SPRITE_TILE_HEIGHT}:flags=neighbor,tile={SPRITE_COLS}x{num_rows}"
            # Added flags=neighbor for scaling - might preserve sharpness better for pixel art / text, test if needed. Default is bilinear.
            ffmpeg_sprite_cmd = [
                FFMPEG_PATH, '-y',              # Overwrite output without asking
                '-i', str(local_clip_path),     # Input file
                '-vf', vf_filter,               # Apply the filtergraph
                '-an',                          # No audio in output
                '-qscale:v', '3',               # Quality scale for JPG (2-5 is good range, 3 is often a good balance)
                '-frames:v', str(num_sprite_frames), # Explicitly limit frames to avoid potential extra frame from ceil()
                str(local_sprite_path)          # Output file path
            ]

            logger.info(f"Generating {SPRITE_COLS}x{num_rows} sprite sheet ({num_sprite_frames} frames expected) for clip {clip_id}...")
            # Ensure run_ffmpeg_command is robust
            try:
                run_ffmpeg_command(ffmpeg_sprite_cmd, "ffmpeg Generate Sprite Sheet")
            except Exception as ffmpeg_err:
                 logger.error(f"ffmpeg sprite generation failed: {ffmpeg_err}", exc_info=True)
                 raise RuntimeError("FFmpeg sprite generation failed") from ffmpeg_err


            logger.info(f"Uploading sprite sheet to s3://{S3_BUCKET_NAME}/{sprite_s3_key}")
            with open(local_sprite_path, "rb") as f:
                s3_client.upload_fileobj(f, S3_BUCKET_NAME, sprite_s3_key)

            # --- Prepare and Validate Metadata (Probe the generated sprite sheet) ---
            calculated_tile_height = 0
            try:
                logger.info(f"Probing generated sprite sheet: {local_sprite_path}")
                probe_sprite_cmd = ["ffprobe", "-v", "error", "-select_streams", "v:0", "-show_entries", "stream=width,height", "-of", "json", str(local_sprite_path)]
                result_sprite = subprocess.run(probe_sprite_cmd, capture_output=True, text=True, check=True, encoding='utf-8')
                sprite_dims = json.loads(result_sprite.stdout)['streams'][0]
                logger.debug(f"ffprobe result for sprite {clip_id}: {sprite_dims}")

                total_sprite_height = int(sprite_dims.get('height', 0))
                total_sprite_width = int(sprite_dims.get('width', 0))

                # Calculate tile height based on probed sprite dimensions and expected rows
                if total_sprite_height > 0 and num_rows > 0:
                     probed_tile_height = math.floor(total_sprite_height / num_rows)
                     logger.info(f"Calculated sprite tile height from probe: {probed_tile_height} (Total Height: {total_sprite_height}, Rows: {num_rows})")
                     calculated_tile_height = probed_tile_height # Use this more reliable value
                else:
                     logger.warning(f"Could not calculate valid tile height from sprite dimensions: {sprite_dims}, Rows: {num_rows}")

                # Optional: Verify probed width consistency
                if total_sprite_width > 0 and SPRITE_COLS > 0:
                    probed_tile_width = math.floor(total_sprite_width / SPRITE_COLS)
                    if abs(probed_tile_width - SPRITE_TILE_WIDTH) > 2: # Allow slight tolerance
                        logger.warning(f"Probed tile width ({probed_tile_width}) differs significantly from target ({SPRITE_TILE_WIDTH}). Sprite layout might be unexpected.")

            except subprocess.CalledProcessError as sprite_probe_err:
                logger.error(f"ffprobe failed for generated sprite sheet {local_sprite_path}. Error: {sprite_probe_err.stderr}", exc_info=False)
                # Decide if this is fatal. Let's proceed but log a warning, height might be inaccurate in JS.
                logger.warning("Proceeding without validated tile height from sprite probe.")
            except Exception as sprite_probe_err:
                logger.error(f"Could not probe sprite sheet dimensions: {sprite_probe_err}", exc_info=True)
                logger.warning("Proceeding without validated tile height from sprite probe.")


            # --- Final Metadata Validation ---
            # Ensure all essential values are positive numbers before storing
            if not (isinstance(fps, (int, float)) and fps > 0):
                raise ValueError(f"Invalid final FPS value ({fps}) for clip {clip_id}. Cannot save metadata.")
            if not (isinstance(total_frames_in_clip, int) and total_frames_in_clip > 0):
                raise ValueError(f"Invalid final total_frames_in_clip value ({total_frames_in_clip}) for clip {clip_id}. Cannot save metadata.")
            if not (isinstance(num_sprite_frames, int) and num_sprite_frames > 0):
                raise ValueError(f"Invalid final num_sprite_frames value ({num_sprite_frames}) for clip {clip_id}. Cannot save metadata.")
            if not (isinstance(calculated_tile_height, int) and calculated_tile_height > 0):
                # Allow saving NULL if calculation failed, but log warning for JS side.
                logger.warning(f"Final calculated_tile_height ({calculated_tile_height}) is invalid. Storing null. Sprite display might be incorrect.")
                calculated_tile_height = None # Store actual NULL

            sprite_metadata = {
                "tile_width": SPRITE_TILE_WIDTH, # The target width for scaling
                "tile_height_calculated": calculated_tile_height, # Store validated height or NULL
                "cols": SPRITE_COLS, # Store the config value used
                "rows": num_rows,
                "total_sprite_frames": num_sprite_frames, # Frames *in the sprite*
                "clip_fps": fps, # Original clip FPS
                "clip_total_frames": total_frames_in_clip # Frames *in the original clip*
            }
            logger.info(f"Final sprite metadata for DB: {json.dumps(sprite_metadata)}") # Log the JSON being sent

            logger.info(f"Sprite sheet generated and uploaded for clip {clip_id}.")
            final_state = "pending_review" # Success state

        # === Final DB Update ===
        try:
            with conn.cursor() as cur:
                 cur.execute("SELECT pg_advisory_xact_lock(2, %s)", (clip_id,))
                 # Ensure sprite_metadata is stored as JSONB
                 # Use json.dumps to ensure correct JSON formatting for DB insertion
                 cur.execute(
                     """
                     UPDATE clips
                     SET ingest_state = %s,
                         sprite_sheet_filepath = %s,
                         sprite_metadata = %s::jsonb, -- Cast string to jsonb
                         updated_at = NOW(),
                         last_error = NULL -- Clear error on success
                     WHERE id = %s AND ingest_state = 'generating_sprite';
                     """,
                     (final_state, sprite_s3_key, json.dumps(sprite_metadata) if sprite_metadata else None, clip_id)
                 )
            conn.commit()
            logger.info(f"TASK [SpriteGen]: Finished for clip {clip_id}. Final State: {final_state}. Sprite Key: {sprite_s3_key}")
            return {"status": "success", "sprite_sheet_key": sprite_s3_key, "new_state": final_state}

        except (psycopg2.DatabaseError, psycopg2.OperationalError) as db_err:
            logger.error(f"DB Error during final update for sprite gen: {db_err}", exc_info=True)
            if conn: conn.rollback()
            final_state = 'sprite_generation_failed' # Ensure state reflects failure
            raise

    # === Main Error Handling ===
    except Exception as e:
        logger.error(f"TASK FAILED [SpriteGen]: clip_id {clip_id} - {e}", exc_info=True)
        final_state = 'sprite_generation_failed'
        if conn:
            try:
                conn.rollback()
                conn.autocommit = True
                with conn.cursor() as err_cur:
                    err_cur.execute(
                        """
                        UPDATE clips SET ingest_state = %s, last_error = %s, updated_at = NOW()
                        WHERE id = %s AND ingest_state IN ('generating_sprite', 'pending_sprite_generation');
                        """,
                        (final_state, f"SpriteGen failed: {type(e).__name__}: {str(e)[:450]}", clip_id)
                    )
                logger.info(f"Attempted to set clip {clip_id} state to '{final_state}'.")
            except Exception as db_err:
                 logger.error(f"Failed to update error state in DB after sprite gen failure: {db_err}")
        # Reraise the exception for Prefect to mark the task as failed
        raise e
    finally:
        if conn:
             conn.autocommit = True # Ensure autocommit is back to default
             conn.close()
             logger.debug(f"DB connection closed for sprite gen task, clip_id: {clip_id}")
        if temp_dir_obj:
            try:
                # Use ignore_errors=True for more resilience during cleanup
                shutil.rmtree(temp_dir_obj.name, ignore_errors=True)
                logger.info(f"Attempted cleanup of temporary directory: {temp_dir_obj.name}")
            except Exception as cleanup_err:
                 # This shouldn't happen with ignore_errors=True, but log just in case
                 logger.warning(f"Error during explicit cleanup of temp dir {temp_dir_obj.name}: {cleanup_err}")

# --- END OF FILE tasks/sprite_generator.py ---
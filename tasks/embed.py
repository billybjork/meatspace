import torch
from PIL import Image
import os
import re
import numpy as np
import traceback
from pathlib import Path
import tempfile
import shutil
from datetime import datetime # Added for timestamp update

from prefect import task, get_run_logger
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values # Keep for potential future use, though not used directly now

import boto3
from botocore.exceptions import ClientError, NoCredentialsError

# Model Loading Imports
try:
    from transformers import CLIPProcessor, CLIPModel, AutoImageProcessor, AutoModel
except ImportError:
    print("Warning: transformers library not found. CLIP/some DINOv2 models will not be available.")
    CLIPProcessor, CLIPModel, AutoImageProcessor, AutoModel = None, None, None, None

import sys
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)
try:
    # Import both connection getter and releaser
    from utils.db_utils import get_db_connection, release_db_connection
except ImportError as e:
    print(f"Error importing DB utils in embed.py: {e}")
    # Define dummy functions if import fails
    def get_db_connection(): raise NotImplementedError("DB Utils not loaded")
    def release_db_connection(conn): pass

# --- S3 Configuration ---
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
AWS_REGION = os.getenv("AWS_REGION")

# --- Constants ---
ARTIFACT_TYPE_KEYFRAME = "keyframe"

# --- Initialize S3 Client ---
s3_client = None
if S3_BUCKET_NAME:
    try:
        s3_client = boto3.client('s3', region_name=AWS_REGION)
        print(f"Embed Task: Successfully initialized S3 client for bucket: {S3_BUCKET_NAME}")
    except NoCredentialsError:
         print("Embed Task: ERROR initializing S3 client - AWS credentials not found.")
         s3_client = None
    except Exception as e:
        print(f"Embed Task: ERROR initializing S3 client: {e}")
        s3_client = None
else:
     print("Embed Task: WARNING - S3_BUCKET_NAME not set. S3 operations will fail.")


# --- Global Model Cache ---
_model_cache = {}
def get_cached_model_and_processor(model_name, device='cpu'):
    """Loads model/processor or retrieves from cache."""
    logger = get_run_logger()
    cache_key = f"{model_name}_{device}" # Include device in cache key
    if cache_key in _model_cache:
        logger.debug(f"Using cached model/processor for: {cache_key}")
        return _model_cache[cache_key]
    else:
        logger.info(f"Loading and caching model/processor: {model_name} to device: {device}")
        try:
            model, processor, model_type, embedding_dim = _load_model_and_processor_internal(model_name, device)
            _model_cache[cache_key] = (model, processor, model_type, embedding_dim)
            return model, processor, model_type, embedding_dim
        except Exception as load_err:
             logger.error(f"Failed to load model {model_name}: {load_err}", exc_info=True)
             raise # Re-raise after logging

def _load_model_and_processor_internal(model_name, device='cpu'):
    """Internal function to load models."""
    logger = get_run_logger()
    logger.info(f"Attempting to load model: {model_name} to device: {device}")
    model_type_str = None
    embedding_dim = None
    model = None
    processor = None

    # --- CLIP Models ---
    if "clip" in model_name.lower():
        if CLIPModel is None: raise ImportError("transformers library required for CLIP models.")
        try:
            model = CLIPModel.from_pretrained(model_name).to(device).eval()
            processor = CLIPProcessor.from_pretrained(model_name)
            # Determine embedding dimension robustly
            if hasattr(model.config, 'projection_dim') and model.config.projection_dim: embedding_dim = model.config.projection_dim
            elif hasattr(model.config, 'hidden_size') and model.config.hidden_size: embedding_dim = model.config.hidden_size # Some CLIP variants might use this
            else: # Fallback based on common sizes
                if "large" in model_name.lower(): embedding_dim = 768
                else: embedding_dim = 512 # Default for base or unknown
            model_type_str = "clip"
            logger.info(f"Loaded CLIP model: {model_name} (Type: {model_type_str}, Dim: {embedding_dim})")
        except Exception as e:
            if "Cannot copy out of meta tensor" in str(e):
                logger.error(f"Caught meta tensor error loading CLIP ({model_name}). Ensure 'accelerate' is installed and consider device_map='auto'.", exc_info=True)
                raise ValueError(f"Meta tensor error loading CLIP: {model_name}. Check libs/config.") from e
            else:
                 logger.error(f"Failed to load CLIP model {model_name}: {e}", exc_info=True)
                 raise ValueError(f"Failed to load CLIP model: {model_name}") from e

    # --- DINOv2 Models (using Transformers) ---
    elif "dinov2" in model_name.lower():
        if AutoModel is None: raise ImportError("transformers library required for DINOv2 models.")
        try:
            processor = AutoImageProcessor.from_pretrained(model_name)
            model = AutoModel.from_pretrained(model_name).to(device).eval()
            if hasattr(model.config, 'hidden_size') and model.config.hidden_size: embedding_dim = model.config.hidden_size
            else: # Fallback based on common DINOv2 sizes
                if "base" in model_name: embedding_dim = 768
                elif "small" in model_name: embedding_dim = 384
                elif "large" in model_name: embedding_dim = 1024
                elif "giant" in model_name: embedding_dim = 1536
                else: raise ValueError(f"Cannot determine embedding dimension for DINOv2 model: {model_name}")
            model_type_str = "dino"
            logger.info(f"Loaded DINOv2 model (Transformers): {model_name} (Type: {model_type_str}, Dim: {embedding_dim})")
        except Exception as e:
             if "Cannot copy out of meta tensor" in str(e):
                 logger.error(f"Caught meta tensor error loading DINOv2 ({model_name}). Ensure 'accelerate' is installed and consider device_map='auto'.", exc_info=True)
                 raise ValueError(f"Meta tensor error loading DINOv2: {model_name}. Check libs/config.") from e
             else:
                 logger.error(f"Failed to load DINOv2 model {model_name}: {e}", exc_info=True)
                 raise ValueError(f"Failed to load DINOv2 model: {model_name}") from e

    else:
        raise ValueError(f"Model name not recognized or supported: {model_name}")

    if model is None or processor is None or model_type_str is None or embedding_dim is None:
         # This should ideally be caught by earlier specific errors
         raise RuntimeError(f"Failed to initialize one or more components for model: {model_name}")

    return model, processor, model_type_str, embedding_dim


# --- Prefect Task ---
@task(name="Generate Clip Embedding", retries=1, retry_delay_seconds=60)
def generate_embeddings_task(
    clip_id: int,
    model_name: str,
    generation_strategy: str, # e.g., "keyframe_midpoint", "keyframe_multi_avg"
    overwrite: bool = False
    ):
    """
    Generates embeddings for a clip based on specified keyframe artifacts.
    Fetches keyframe artifact S3 keys from the database based on the generation_strategy,
    downloads them, runs the embedding model, and stores the result.

    Args:
        clip_id: The ID of the clip to process.
        model_name: The name of the embedding model to use (e.g., 'openai/clip-vit-base-patch32').
        generation_strategy: String defining which keyframes to use and how to generate
                             the embedding (e.g., 'keyframe_midpoint', 'keyframe_multi_avg').
                             Expected format: "keyframe_<strategy>[_aggregation]"
        overwrite: If True, generate and overwrite existing embeddings for this model/strategy.
    """
    logger = get_run_logger()
    logger.info(f"TASK [Embedding]: Starting for clip_id: {clip_id}, Model: '{model_name}', Strategy: '{generation_strategy}', Overwrite: {overwrite}")

    if not s3_client:
        logger.error("S3 client is not available. Cannot perform S3 operations.")
        raise RuntimeError("S3 client is not initialized or configured.")

    # --- Resource Management ---
    conn = None # Use a single connection object managed across phases
    temp_dir_obj = None
    temp_dir = None

    # --- State & Result ---
    final_status = "failed" # Assume failure
    embedding_dim = -1
    needs_processing = False
    embedding_str = None # Stores the final '[1,2,3]' string
    error_message_for_db = None
    task_exception = None
    keyframe_strategy_name = None # e.g., 'midpoint', 'multi' parsed from generation_strategy
    aggregation_method = None # e.g., 'avg' or None

    # --- Parse Generation Strategy ---
    try:
        match = re.match(r"keyframe_([a-zA-Z0-9]+)(?:_(avg))?$", generation_strategy)
        if not match:
            raise ValueError(f"Invalid generation_strategy format: '{generation_strategy}'. Expected 'keyframe_<strategy>[_aggregation]'.")
        keyframe_strategy_name = match.group(1)
        aggregation_method = match.group(2) # Will be 'avg' or None
        logger.info(f"Parsed strategy: keyframe_type='{keyframe_strategy_name}', aggregation='{aggregation_method}'")
    except ValueError as e:
        logger.error(str(e))
        # Cannot proceed without valid strategy
        raise e # Fail the task early

    try:
        # === Phase 1: Initial DB Check and State Update ===
        conn = get_db_connection()
        conn.autocommit = False # Manual transaction control

        try:
            with conn.cursor() as cur:
                # 1. Acquire lock
                cur.execute("SELECT pg_advisory_xact_lock(2, %s)", (clip_id,))
                logger.info(f"Acquired DB lock for clip_id: {clip_id} (Phase 1)")

                # 2. Check clip state and existing embedding
                cur.execute(
                    """
                    SELECT c.ingest_state,
                           EXISTS (SELECT 1 FROM embeddings e WHERE e.clip_id = c.id AND e.model_name = %s AND e.generation_strategy = %s)
                    FROM clips c WHERE c.id = %s;
                    """, (model_name, generation_strategy, clip_id)
                )
                result = cur.fetchone()
                if not result:
                    raise ValueError(f"Clip ID {clip_id} not found.")

                current_state, embedding_exists = result
                logger.info(f"Clip {clip_id}: State='{current_state}', EmbeddingExists (this model/strategy)={embedding_exists}")

                # 3. Determine if processing is needed
                # Prerequisite state for embedding is 'keyframed'
                allow_processing_state = current_state == 'keyframed' or \
                                         (overwrite and current_state == 'embedding_failed') or \
                                         (overwrite and current_state == 'embedded') # Allow overwrite if already embedded

                if not allow_processing_state:
                    reason = f"state '{current_state}' is not eligible"
                    if current_state == 'embedded' and not overwrite:
                        reason = "state is 'embedded' and overwrite=False"
                    elif embedding_exists and not overwrite:
                         reason = "embedding exists for this model/strategy and overwrite=False"
                    logger.info(f"Skipping clip {clip_id}: {reason}.")
                    final_status = "skipped_state" if not embedding_exists else "skipped_exists"
                    needs_processing = False
                elif embedding_exists and not overwrite:
                    logger.info(f"Skipping clip {clip_id}: Embedding already exists for this model/strategy and overwrite=False.")
                    final_status = "skipped_exists"
                    needs_processing = False
                else:
                    needs_processing = True
                    action = "Proceeding with embedding generation"
                    if overwrite and embedding_exists: action += " (overwrite=True)."
                    elif overwrite: action += " (overwrite=True)."
                    else: action += "."
                    logger.info(f"{action} Clip {clip_id} state: {current_state}.")


                # 4. Update state if processing
                if needs_processing:
                    # Check if prerequisite keyframes exist before changing state
                    cur.execute(
                        """
                        SELECT EXISTS (
                            SELECT 1 FROM clip_artifacts
                            WHERE clip_id = %s AND artifact_type = %s AND strategy = %s
                        );
                        """, (clip_id, ARTIFACT_TYPE_KEYFRAME, keyframe_strategy_name)
                    )
                    keyframes_found = cur.fetchone()[0]
                    if not keyframes_found:
                        raise ValueError(f"Cannot start embedding: No keyframe artifacts found for clip {clip_id} with strategy '{keyframe_strategy_name}'. Ensure keyframing ran successfully.")
                    logger.info(f"Verified prerequisite keyframe artifacts exist for strategy '{keyframe_strategy_name}'.")

                    # Now update state
                    cur.execute(
                        """
                        UPDATE clips SET
                            ingest_state = 'embedding',
                            updated_at = NOW(),
                            last_error = NULL -- Clear previous error
                        WHERE id = %s;
                        """, (clip_id,)
                    )
                    if cur.rowcount == 0:
                        raise RuntimeError(f"Concurrency Error: Failed to set clip {clip_id} state to 'embedding'.")
                    logger.info(f"Set clip {clip_id} state to 'embedding'")

            # ---- End Phase 1 Cursor Context ----
            conn.commit() # Commit check/state update
            logger.debug("Phase 1 DB check/update transaction committed.")

        except (ValueError, psycopg2.DatabaseError, RuntimeError) as db_err:
            logger.error(f"DB Error during Phase 1 for clip {clip_id}: {db_err}", exc_info=True)
            if conn: conn.rollback()
            error_message_for_db = f"DB Phase 1 Error: {str(db_err)[:500]}"
            task_exception = db_err
            raise # Re-raise to main handler


        # === Exit Early if Skipped ===
        if not needs_processing:
            logger.info(f"No processing required for clip {clip_id}. Final status: {final_status}")
            return {"status": final_status, "reason": final_status, "clip_id": clip_id}


        # === Phase 2: Fetch Artifacts, Download, Process (Model Inference) ===
        # (Requires DB connection for artifact fetch, S3 for download)
        s3_keys_to_download = []
        try:
            # 5. Fetch Keyframe Artifact S3 Keys from DB
            # Use the same connection after commit, ensuring autocommit is False again if needed
            # Or get connection again for clarity if preferred. Let's reuse `conn`.
            conn.autocommit = False # Ensure we are in a transaction if needed, though this is just a read
            with conn.cursor() as cur:
                 # We don't need a lock for reads usually, but doesn't hurt if already held implicitly by backend session
                 logger.info(f"Fetching keyframe artifact paths for clip {clip_id}, strategy '{keyframe_strategy_name}'...")
                 query = sql.SQL("""
                     SELECT s3_key FROM clip_artifacts
                     WHERE clip_id = %s AND artifact_type = %s AND strategy = %s
                     ORDER BY tag; -- Order by tag ('25pct', '50pct', '75pct') for consistency
                 """)
                 cur.execute(query, (clip_id, ARTIFACT_TYPE_KEYFRAME, keyframe_strategy_name))
                 results = cur.fetchall()
                 s3_keys_to_download = [row[0] for row in results]

            # No commit needed for SELECT

            if not s3_keys_to_download:
                # This case should have been caught in Phase 1, but double check
                raise FileNotFoundError(f"No keyframe artifact S3 keys found in DB for clip {clip_id}, strategy '{keyframe_strategy_name}', though processing was initiated.")

            logger.info(f"Found {len(s3_keys_to_download)} keyframe artifact(s) in DB to process.")


            # 6. Setup Temp Dir & Download Artifacts
            if not s3_client: raise RuntimeError("S3 client became unavailable.") # Re-check S3 client
            temp_dir_obj = tempfile.TemporaryDirectory(prefix=f"meatspace_embed_{clip_id}_")
            temp_dir = Path(temp_dir_obj.name)
            logger.info(f"Using temporary directory: {temp_dir}")

            downloaded_local_paths = {}
            for s3_key in s3_keys_to_download:
                if not s3_key:
                    logger.warning("Encountered NULL or empty S3 key in list, skipping.")
                    continue
                # Sanitize S3 key to create a safe local filename part
                local_filename = Path(s3_key).name
                local_temp_path = temp_dir / local_filename
                try:
                    logger.debug(f"Downloading s3://{S3_BUCKET_NAME}/{s3_key} to {local_temp_path}")
                    s3_client.download_file(S3_BUCKET_NAME, s3_key, str(local_temp_path))
                    downloaded_local_paths[s3_key] = str(local_temp_path)
                    logger.debug(f"Successfully downloaded {s3_key}")
                except ClientError as s3_download_err:
                    logger.error(f"Failed to download {s3_key} from S3: {s3_download_err}")
                    # Fail the task if any required artifact cannot be downloaded
                    raise FileNotFoundError(f"Failed download required keyframe {s3_key}") from s3_download_err

            if len(downloaded_local_paths) != len(s3_keys_to_download):
                 raise RuntimeError(f"Mismatch: Expected {len(s3_keys_to_download)} downloads, got {len(downloaded_local_paths)}.")
            if not downloaded_local_paths:
                 # Should be caught earlier, but safety check
                 raise FileNotFoundError(f"Failed download ANY required keyframes from S3 for clip {clip_id}.")
            logger.info(f"Successfully downloaded {len(downloaded_local_paths)} keyframe image(s).")


            # 7. Load Images & Run Model Inference
            images = []
            local_paths_to_load = list(downloaded_local_paths.values()) # Keep order consistent if needed
            try:
                # Ensure Pillow handles potential errors gracefully (e.g., truncated files)
                for p in local_paths_to_load:
                     try:
                         img = Image.open(p).convert("RGB")
                         img.verify() # Verify integrity
                         # Reopen after verify
                         img = Image.open(p).convert("RGB")
                         images.append(img)
                     except (IOError, SyntaxError) as img_err:
                         logger.error(f"Failed to load or verify image {p}: {img_err}")
                         raise RuntimeError(f"Error loading image file {Path(p).name}") from img_err
            except Exception as img_load_err:
                 logger.error(f"Unexpected error loading images: {img_load_err}", exc_info=True)
                 raise RuntimeError("Unexpected error during image loading") from img_load_err

            if not images:
                raise RuntimeError(f"Image loading resulted in empty list for clip {clip_id}. Cannot proceed.")
            if len(images) != len(local_paths_to_load):
                 logger.warning(f"Loaded {len(images)} images, but expected {len(local_paths_to_load)}. Check file integrity.")
                 if not images: raise RuntimeError("Failed to load any valid images.")


            # Load model (cached)
            device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
            model, processor, model_type, embedding_dim = get_cached_model_and_processor(model_name, device)
            logger.info(f"Using Model '{model_name}' ({model_type}, Dim: {embedding_dim}) on device {device}.")

            # Generate features
            all_features_np = None
            logger.info(f"Running inference for {len(images)} image(s)...")
            with torch.no_grad():
                try:
                    if model_type == "clip":
                        inputs = processor(text=None, images=images, return_tensors="pt", padding=True).to(device)
                        image_features = model.get_image_features(**inputs)
                        all_features_np = image_features.cpu().numpy()
                    elif model_type == "dino":
                        inputs = processor(images=images, return_tensors="pt").to(device)
                        outputs = model(**inputs)
                        # Use mean pooling of the last hidden state patch tokens
                        image_features = outputs.last_hidden_state.mean(dim=1)
                        all_features_np = image_features.cpu().numpy()
                    else:
                        # This case should be caught by model loading, but defensively check
                        raise NotImplementedError(f"Embedding generation logic not implemented for model type: {model_type}")
                except Exception as infer_err:
                    logger.error(f"Model inference failed for clip {clip_id}: {infer_err}", exc_info=True)
                    raise RuntimeError(f"Model inference failed") from infer_err

            if all_features_np is None or all_features_np.shape[0] == 0:
                raise RuntimeError(f"Feature extraction yielded no results for clip {clip_id}")
            if all_features_np.shape[0] != len(images):
                logger.warning(f"Feature extraction returned {all_features_np.shape[0]} embeddings, but expected {len(images)}.")
                # Decide how to handle mismatch - fail or proceed? Let's fail for safety.
                raise RuntimeError("Mismatch between number of images processed and embeddings generated.")

            logger.info(f"Successfully generated {all_features_np.shape[0]} raw embeddings.")


            # 8. Aggregate Embeddings if Needed
            final_embedding_np = None
            if aggregation_method == 'avg' and all_features_np.shape[0] > 1:
                final_embedding_np = np.mean(all_features_np, axis=0)
                logger.info(f"Averaged {all_features_np.shape[0]} embeddings for strategy '{generation_strategy}'.")
            elif all_features_np.shape[0] == 1:
                 final_embedding_np = all_features_np[0]
                 logger.info(f"Using the single generated embedding for strategy '{generation_strategy}'.")
            elif all_features_np.shape[0] > 1 and not aggregation_method:
                 # If multiple frames were processed (e.g., strategy='multi') but no aggregation specified,
                 # we should use the *representative* one. This relies on the DB query returning them
                 # ordered, or ideally, the keyframe task marking the representative one.
                 # Let's assume for non-aggregated multi-frame, we *should* have only processed the representative.
                 # If we get here, it implies a logic mismatch. Fail for safety.
                 logger.error(f"Strategy '{generation_strategy}' resulted in {all_features_np.shape[0]} embeddings but no aggregation method was specified. Ambiguous which embedding to use.")
                 raise NotImplementedError(f"Ambiguous result: Multiple embeddings generated for non-aggregating strategy '{generation_strategy}'.")
            else: # Should not happen
                  raise RuntimeError("Could not determine final embedding from processed features.")

            if final_embedding_np is None:
                raise RuntimeError(f"Final embedding could not be determined for clip {clip_id}")

            # 9. Format Embedding String
            embedding_list = final_embedding_np.tolist()
            if len(embedding_list) != embedding_dim:
                raise ValueError(f"Generated embedding dimension ({len(embedding_list)}) mismatch with expected model dimension ({embedding_dim})")

            # Format as string vector for pgvector
            embedding_str = '[' + ','.join(map(str, embedding_list)) + ']'
            logger.info(f"Successfully prepared final embedding string (Dim: {len(embedding_list)}).")

        except (FileNotFoundError, RuntimeError, ValueError, ClientError, ImportError, NotImplementedError) as phase2_err:
            # Catch errors specific to artifact fetch, download, model load/inference
            logger.error(f"Error during Phase 2 (Processing/Inference) for clip {clip_id}: {phase2_err}", exc_info=True)
            error_message_for_db = f"Processing Error: {str(phase2_err)[:500]}"
            task_exception = phase2_err
            raise # Re-raise


        # === Phase 3: Final DB Update ===
        # (Requires DB connection, run only if embedding_str was generated)
        if not embedding_str:
             # This should only happen if an error was raised and caught above
             logger.error("Reached Phase 3 without a valid embedding string. An error likely occurred earlier.")
             if not task_exception: # If somehow no exception was stored
                  raise RuntimeError("Embedding string generation failed silently.")
             else:
                  raise task_exception # Re-raise the stored exception

        try:
            # Reuse connection `conn`
            conn.autocommit = False # Ensure transaction control
            with conn.cursor() as cur:
                # 1. Acquire lock
                cur.execute("SELECT pg_advisory_xact_lock(2, %s)", (clip_id,))
                logger.info(f"Acquired DB lock for clip_id: {clip_id} (Phase 3)")

                # 2. Insert/Update embedding in embeddings table
                # ON CONFLICT handles both insert and overwrite cases
                insert_query = sql.SQL("""
                    INSERT INTO embeddings (clip_id, embedding, model_name, generation_strategy, generated_at)
                    VALUES (%s, %s::vector, %s, %s, NOW())
                    ON CONFLICT (clip_id, model_name, generation_strategy)
                    DO UPDATE SET
                        embedding = EXCLUDED.embedding,
                        generated_at = NOW();
                """)
                cur.execute(insert_query, (clip_id, embedding_str, model_name, generation_strategy))
                logger.info(f"Successfully inserted/updated embedding record for clip {clip_id}, model '{model_name}', strategy '{generation_strategy}'.")

                # 3. Update clip status and timestamp
                update_clip_query = sql.SQL("""
                    UPDATE clips SET
                        ingest_state = 'embedded',
                        embedded_at = NOW(), -- Set completion timestamp
                        last_error = NULL,
                        updated_at = NOW()
                    WHERE id = %s AND ingest_state = 'embedding'; -- Ensure state was 'embedding'
                """)
                cur.execute(update_clip_query, (clip_id,))

                if cur.rowcount == 1:
                    logger.info(f"Successfully updated clip {clip_id} state to 'embedded' and set embedded_at.")
                    final_status = "success" # Mark success
                else:
                    # Potential concurrency issue or unexpected state change since Phase 1
                    logger.error(f"Failed to update clip {clip_id} final state to 'embedded'. Rowcount: {cur.rowcount}. State might have changed.")
                    conn.rollback() # Rollback this transaction
                    final_status = "failed_state_update"
                    error_message_for_db = "Failed final state update to 'embedded'"
                    raise RuntimeError(f"Failed to update clip {clip_id} state to 'embedded' in final DB step.")

            # ---- End Phase 3 Cursor Context ----
            conn.commit() # Commit embedding insert and clip update
            logger.debug("Phase 3 DB updates committed successfully.")

        except (psycopg2.DatabaseError, RuntimeError) as db_err:
            logger.error(f"DB Error during Phase 3 Update for clip {clip_id}: {db_err}", exc_info=True)
            if conn: conn.rollback()
            error_message_for_db = f"DB Phase 3 Error: {str(db_err)[:500]}"
            task_exception = db_err
            final_status = "failed_db_update"
            raise # Re-raise


    except (ValueError, FileNotFoundError, RuntimeError, ClientError, ImportError, NotImplementedError, psycopg2.DatabaseError) as e:
        # --- Main Error Handling Block ---
        logger.error(f"TASK ERROR [Embedding]: clip_id {clip_id} - {e}", exc_info=True)
        final_status = final_status if final_status != "failed" else "failed_processing"
        error_message_for_db = error_message_for_db or f"{type(e).__name__}: {str(e)[:500]}"
        task_exception = e

        # Attempt to update DB state to failed, only if processing was attempted
        if conn and needs_processing:
            try:
                logger.warning(f"Attempting to roll back transaction and set clip {clip_id} state to 'embedding_failed'.")
                conn.rollback()
                conn.autocommit = True # Use autocommit for fail state update
                with conn.cursor() as err_cur:
                    fail_update_sql = sql.SQL("""
                        UPDATE clips SET
                            ingest_state = 'embedding_failed',
                            last_error = %s,
                            retry_count = COALESCE(retry_count, 0) + 1,
                            updated_at = NOW()
                        WHERE id = %s AND ingest_state IN ('embedding', 'keyframed'); -- Update if started or was ready
                    """)
                    err_cur.execute(fail_update_sql, (error_message_for_db, clip_id))
                logger.info(f"Attempted to set clip {clip_id} state to 'embedding_failed' in DB.")
            except Exception as db_fail_err:
                logger.error(f"CRITICAL: Failed to update error state to 'embedding_failed' for clip {clip_id}: {db_fail_err}")
        elif not conn:
             logger.error("DB connection was not available to update error state.")

    except Exception as e:
        # Catch any truly unexpected errors
        logger.error(f"UNEXPECTED TASK ERROR [Embedding]: clip_id {clip_id} - {e}", exc_info=True)
        final_status = "failed_unexpected"
        error_message_for_db = f"Unexpected:{type(e).__name__}: {str(e)[:480]}"
        task_exception = e
        # Attempt DB update similar to above
        if conn and needs_processing:
             try:
                conn.rollback(); conn.autocommit = True
                with conn.cursor() as err_cur:
                    err_cur.execute(sql.SQL("""UPDATE clips SET ingest_state = 'embedding_failed', last_error = %s, retry_count = COALESCE(retry_count, 0) + 1, updated_at = NOW() WHERE id = %s AND ingest_state IN ('embedding', 'keyframed');"""), (error_message_for_db, clip_id))
                logger.info(f"Attempted to set clip {clip_id} state to 'embedding_failed' after unexpected error.")
             except Exception as db_fail_err:
                logger.error(f"CRITICAL: Failed to update error state for clip {clip_id} after unexpected task failure: {db_fail_err}")

    finally:
        # --- Final Actions (Guaranteed to Run) ---
        # 1. Release DB connection
        if conn:
            conn.autocommit = True # Ensure reset
            release_db_connection(conn)
            logger.debug(f"DB connection released for clip_id: {clip_id}")

        # 2. Clean up temporary directory
        if temp_dir_obj:
            try:
                shutil.rmtree(temp_dir, ignore_errors=True)
                logger.info(f"Cleaned up temporary directory: {temp_dir}")
            except Exception as cleanup_err:
                logger.warning(f"Error cleaning up temp dir {temp_dir}: {cleanup_err}")

    # === Return Result or Raise Exception ===
    result_payload = {
        "status": final_status,
        "clip_id": clip_id,
        "model_name": model_name,
        "strategy": generation_strategy, # Return the full strategy string used
        "embedding_dim": embedding_dim,
    }

    if final_status == "success":
        logger.info(f"TASK [Embedding] finished successfully for clip_id {clip_id}. Result: {result_payload}")
        return result_payload
    elif final_status.startswith("skipped"):
        logger.info(f"TASK [Embedding] skipped for clip_id {clip_id}. Status: {final_status}")
        return result_payload
    else:
        # Failure case
        logger.error(f"TASK [Embedding] failed for clip_id {clip_id}. Final Status: {final_status}. Error: {error_message_for_db}. Result payload: {result_payload}")
        if task_exception:
            raise task_exception # Re-raise the original exception
        else:
            raise RuntimeError(f"Embedding task failed for clip {clip_id} with status {final_status}. Reason: {error_message_for_db or 'Unknown error'}")


# Local run block for testing purposes
if __name__ == "__main__":
    print("Running embedding task locally for testing (requires S3/DB/Model setup)...")
    # Example: prefect run python tasks/embed.py generate_embeddings_task --param clip_id=YOUR_CLIP_ID --param model_name="openai/clip-vit-base-patch32" --param generation_strategy="keyframe_multi_avg" --param overwrite=True
    pass
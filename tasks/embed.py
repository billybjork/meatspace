import torch
from PIL import Image
import os
import re
import numpy as np
import traceback
from pathlib import Path
import tempfile
import shutil

from prefect import task, get_run_logger
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values

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
    def get_db_connection(): raise NotImplementedError("Dummy DB connection")
    def release_db_connection(conn): pass

# --- S3 Configuration ---
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
AWS_REGION = os.getenv("AWS_REGION")

if not S3_BUCKET_NAME:
    raise ValueError("S3_BUCKET_NAME environment variable not set.")

s3_client = None
try:
    s3_client = boto3.client('s3', region_name=AWS_REGION)
    print(f"Embed Task: Successfully initialized S3 client for bucket: {S3_BUCKET_NAME}")
except NoCredentialsError:
     print("Embed Task: ERROR initializing S3 client - AWS credentials not found.")
except Exception as e:
    print(f"Embed Task: ERROR initializing S3 client: {e}")

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
        model, processor, model_type, embedding_dim = _load_model_and_processor_internal(model_name, device)
        _model_cache[cache_key] = (model, processor, model_type, embedding_dim)
        return model, processor, model_type, embedding_dim

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
        if CLIPModel is None: raise ImportError("transformers required for CLIP.")
        try:
            # Removed low_cpu_mem_usage=True initially
            model = CLIPModel.from_pretrained(model_name).to(device).eval()
            processor = CLIPProcessor.from_pretrained(model_name)
            if hasattr(model.config, 'projection_dim'): embedding_dim = model.config.projection_dim
            elif "large" in model_name.lower(): embedding_dim = 768
            elif "base" in model_name.lower(): embedding_dim = 512
            else: embedding_dim = 512
            model_type_str = "clip"
            logger.info(f"Loaded CLIP model: {model_name} (Dim: {embedding_dim})")
        except Exception as e:
            if "Cannot copy out of meta tensor" in str(e):
                logger.error(f"Caught meta tensor error loading CLIP ({model_name}). Try installing 'accelerate' and using device_map='auto'.", exc_info=True)
                raise ValueError(f"Meta tensor error loading CLIP: {model_name}. Check libs or try device_map='auto'.") from e
            else:
                 raise ValueError(f"Failed load CLIP: {model_name}") from e

    # --- DINOv2 Models (using Transformers) ---
    elif "dinov2" in model_name.lower():
        if AutoModel is None: raise ImportError("transformers required for DINOv2.")
        try:
            processor = AutoImageProcessor.from_pretrained(model_name)
            model = AutoModel.from_pretrained(model_name).to(device).eval()
            if hasattr(model.config, 'hidden_size'): embedding_dim = model.config.hidden_size
            else:
                if "base" in model_name: embedding_dim = 768
                elif "small" in model_name: embedding_dim = 384
                elif "large" in model_name: embedding_dim = 1024
                elif "giant" in model_name: embedding_dim = 1536
                else: raise ValueError(f"Cannot determine embedding dimension for DINOv2: {model_name}")
            model_type_str = "dino"
            logger.info(f"Loaded DINOv2 model (Transformers): {model_name} (Dim: {embedding_dim})")
        except Exception as e:
             if "Cannot copy out of meta tensor" in str(e):
                 logger.error(f"Caught meta tensor error loading DINOv2 ({model_name}). Check libs or try device_map='auto'.", exc_info=True)
                 raise ValueError(f"Meta tensor error loading DINOv2: {model_name}.") from e
             else: raise ValueError(f"Failed to load DINOv2 model: {model_name}") from e

    else:
        raise ValueError(f"Model name not recognized or supported: {model_name}")

    if model is None or processor is None or model_type_str is None or embedding_dim is None:
         raise RuntimeError(f"Failed to initialize components for model: {model_name}")

    return model, processor, model_type_str, embedding_dim

# --- Helper to GENERATE potential sibling S3 Keys ---
def generate_potential_sibling_s3_keys(representative_s3_key):
    # (Keep this function as is)
    logger = get_run_logger(); potential_keys = []
    if not representative_s3_key: logger.warning("generate_potential_sibling_s3_keys received empty key."); return []
    try:
        key_path = Path(representative_s3_key); base_name = key_path.name; dir_name = str(key_path.parent).replace("\\", "/")
        match = re.match(r'^(.*_frame_)(\d+pct|mid)(\.[^.]+)$', base_name)
        if not match: logger.debug(f"S3 key '{base_name}' not multi-frame. Using only repr."); return [representative_s3_key]
        base_prefix, current_tag, extension = match.groups()
        if current_tag == 'mid': return [representative_s3_key]
        tags_to_check = ["25pct", "50pct", "75pct"]
        for tag in tags_to_check: potential_keys.append(f"{dir_name}/{base_prefix}{tag}{extension}")
        if not potential_keys: logger.warning(f"Logic error generating keys for {representative_s3_key}."); return [representative_s3_key]
        return sorted(list(set(potential_keys)))
    except Exception as e: logger.error(f"Error generating sibling S3 keys for {representative_s3_key}: {e}", exc_info=True); return []


# --- Prefect Task ---
@task(name="Generate Clip Embedding", retries=1, retry_delay_seconds=45)
def generate_embeddings_task(
    clip_id: int,
    model_name: str,
    generation_strategy: str,
    overwrite: bool = False
    ):
    logger = get_run_logger()
    logger.info(f"TASK [Embedding]: Starting for clip_id: {clip_id}, Model: '{model_name}', Strategy: '{generation_strategy}', Overwrite: {overwrite}")

    if not s3_client: raise RuntimeError("S3 client not initialized.")

    # --- Variables ---
    temp_dir_obj = None
    temp_dir = None
    final_status = "failed" # Assume failure unless explicitly set to success
    embedding_dim = -1
    needs_processing = False
    repr_s3_key = None
    embedding_str = None
    error_message_for_db = None # Store specific error message for DB update
    task_exception = None      # Store exception to re-raise after cleanup

    try:
        # === Phase 1: Initial DB Check and State Update ===
        conn_phase1 = None
        try:
            conn_phase1 = get_db_connection()
            conn_phase1.autocommit = False
            with conn_phase1.cursor() as cur:
                cur.execute("SELECT pg_advisory_xact_lock(2, %s)", (clip_id,))
                logger.info(f"Acquired DB lock for clip_id: {clip_id} (Phase 1)")
                cur.execute(
                    """
                    SELECT c.keyframe_filepath, c.ingest_state,
                           EXISTS (SELECT 1 FROM embeddings e WHERE e.clip_id = c.id AND e.model_name = %s AND e.generation_strategy = %s)
                    FROM clips c WHERE c.id = %s;
                    """, (model_name, generation_strategy, clip_id)
                )
                result = cur.fetchone()
                if not result: raise ValueError(f"Clip ID {clip_id} not found.")
                repr_s3_key, current_state, embedding_exists = result
                logger.info(f"Clip {clip_id}: State='{current_state}', Repr Key S3='{repr_s3_key}', EmbeddingExists={embedding_exists}")

                allow_processing_state = current_state == 'keyframed' or \
                                         (overwrite and current_state == 'embedding_failed') or \
                                         (overwrite and current_state == 'embedded')

                if not allow_processing_state:
                    reason = f"Skipped due to state '{current_state}'"
                    if current_state == 'embedded' and not overwrite: reason += " and overwrite=False."
                    if embedding_exists and not overwrite: reason = f"Skipped: Embedding exists and overwrite=False."
                    logger.info(f"Skipping clip {clip_id}: {reason}")
                    final_status = "skipped"
                elif embedding_exists and not overwrite:
                    logger.warning(f"Skipping clip {clip_id}: Embedding exists and overwrite=False.")
                    final_status = "skipped"
                else:
                    needs_processing = True

                if needs_processing:
                    if not repr_s3_key: raise ValueError(f"Clip ID {clip_id} has NULL keyframe_filepath (S3 key).")
                    cur.execute(
                        "UPDATE clips SET ingest_state = 'embedding', updated_at = NOW(), last_error = NULL WHERE id = %s",
                        (clip_id,)
                    )
                    logger.info(f"Set clip {clip_id} state to 'embedding'")
            conn_phase1.commit()
            logger.debug("Initial check/state update transaction committed.")
        except (ValueError, psycopg2.DatabaseError) as db_err:
            logger.error(f"DB Error during initial check/update for clip {clip_id}: {db_err}", exc_info=True)
            if conn_phase1: conn_phase1.rollback()
            raise # Re-raise to be caught by the main try-except
        finally:
            if conn_phase1:
                conn_phase1.autocommit = True
                release_db_connection(conn_phase1)
                logger.debug(f"DB connection released after Phase 1 for clip_id: {clip_id}")

        # === Exit Early if Skipped ===
        if final_status == "skipped":
            logger.info(f"Skipped processing for clip {clip_id}.")
            # No exception, just return skipped status
            return {"status": final_status, "reason": "Skipped", "clip_id": clip_id}

        # === Phase 2: Processing (No DB Connection Held) ===
        if needs_processing: # Ensure we only process if needed
            temp_dir_obj = tempfile.TemporaryDirectory(prefix=f"meatspace_embed_{clip_id}_")
            temp_dir = Path(temp_dir_obj.name)
            logger.info(f"Using temporary directory: {temp_dir}")

            device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
            model, processor, model_type, embedding_dim = get_cached_model_and_processor(model_name, device)
            logger.info(f"Model '{model_name}' ({model_type}, Dim: {embedding_dim}) ready on {device}.")

            is_multi_frame_strategy = generation_strategy in ['keyframe_multi_avg']
            if is_multi_frame_strategy: s3_keys_to_download = generate_potential_sibling_s3_keys(repr_s3_key)
            else: s3_keys_to_download = [repr_s3_key]
            if not s3_keys_to_download: raise ValueError(f"Could not determine S3 keys for clip {clip_id}")
            logger.info(f"Identified {len(s3_keys_to_download)} potential S3 key(s) for clip {clip_id}.")

            downloaded_local_paths = {}
            for s3_key in s3_keys_to_download:
                if not s3_key: continue
                local_temp_path = temp_dir / Path(s3_key).name
                try:
                    s3_client.download_file(S3_BUCKET_NAME, s3_key, str(local_temp_path))
                    downloaded_local_paths[s3_key] = str(local_temp_path)
                except ClientError as s3_download_err: logger.warning(f"Failed download {s3_key}: {s3_download_err}")
            if not downloaded_local_paths: raise FileNotFoundError(f"Failed download ANY keyframes from S3 for clip {clip_id}.")
            logger.info(f"Successfully downloaded {len(downloaded_local_paths)} keyframe(s).")

            images = []; local_paths_to_load = list(downloaded_local_paths.values())
            try: images = [Image.open(p).convert("RGB") for p in local_paths_to_load]
            except Exception as img_err: raise RuntimeError(f"Error loading image from temp file for clip {clip_id}: {img_err}") from img_err
            if not images: raise RuntimeError(f"Image loading resulted in empty list for clip {clip_id}")

            all_features_np = None; final_embedding_np = None
            with torch.no_grad():
                try:
                    if model_type == "clip": inputs = processor(text=None, images=images, return_tensors="pt", padding=True).to(device); image_features = model.get_image_features(**inputs); all_features_np = image_features.cpu().numpy()
                    elif model_type == "dino": inputs = processor(images=images, return_tensors="pt").to(device); outputs = model(**inputs); image_features = outputs.last_hidden_state.mean(dim=1); all_features_np = image_features.cpu().numpy()
                    else: raise NotImplementedError(f"Embedding generation not implemented for model type: {model_type}")
                except Exception as infer_err: raise RuntimeError(f"Model inference failed for clip {clip_id}: {infer_err}") from infer_err
            if all_features_np is None or all_features_np.shape[0] == 0: raise RuntimeError(f"Feature extraction yielded no results for clip {clip_id}")

            if is_multi_frame_strategy and all_features_np.shape[0] > 1:
                if generation_strategy == 'keyframe_multi_avg': final_embedding_np = np.mean(all_features_np, axis=0); logger.debug(f"Averaged {all_features_np.shape[0]} embeddings.")
                else: raise NotImplementedError(f"Aggregation logic not defined: {generation_strategy}")
            else:
                if all_features_np.shape[0] > 1: logger.warning(f"Strategy '{generation_strategy}' got {all_features_np.shape[0]} features. Using first.")
                final_embedding_np = all_features_np[0]
            if final_embedding_np is None: raise RuntimeError(f"Final embedding could not be determined for clip {clip_id}")

            embedding_list = final_embedding_np.tolist()
            if len(embedding_list) != embedding_dim: raise ValueError(f"Gen dim ({len(embedding_list)}) != expected ({embedding_dim})")
            embedding_str = '[' + ','.join(map(str, embedding_list)) + ']' # Prepare embedding string


        # === Phase 3: Final DB Update ===
        if embedding_str: # Only attempt DB update if embedding was generated
            conn_phase3 = None
            try:
                conn_phase3 = get_db_connection()
                conn_phase3.autocommit = False
                with conn_phase3.cursor() as cur:
                    cur.execute("SELECT pg_advisory_xact_lock(2, %s)", (clip_id,))
                    logger.info(f"Acquired DB lock for clip_id: {clip_id} (Phase 3)")
                    insert_query = sql.SQL("""
                        INSERT INTO embeddings (clip_id, embedding, model_name, generation_strategy, generated_at) VALUES (%s, %s::vector, %s, %s, NOW())
                        ON CONFLICT (clip_id, model_name, generation_strategy) DO UPDATE SET embedding = EXCLUDED.embedding, generated_at = NOW();
                    """)
                    cur.execute(insert_query, (clip_id, embedding_str, model_name, generation_strategy))
                    logger.info(f"Inserted/updated embedding for clip {clip_id}")

                    update_clip_query = """
                        UPDATE clips SET ingest_state = 'embedded', embedded_at = NOW(), last_error = NULL, updated_at = NOW()
                        WHERE id = %s AND ingest_state = 'embedding';
                        """
                    cur.execute(update_clip_query, (clip_id,))
                    if cur.rowcount == 0:
                        logger.warning(f"DB update for final state 'embedded' for clip {clip_id} affected 0 rows (state might have changed).")
                        # Potentially set final_status back to failed if this update is critical? Or just log it.
                    else:
                        logger.info(f"Set clip {clip_id} state to 'embedded'.")
                        final_status = "success" # Mark success ONLY if final DB update is good
                conn_phase3.commit()
                logger.debug("Final update transaction committed.")
            except (ValueError, psycopg2.DatabaseError) as db_err:
                logger.error(f"DB Error during final update for clip {clip_id}: {db_err}", exc_info=True)
                if conn_phase3: conn_phase3.rollback()
                raise # Re-raise to be caught by the main try-except
            finally:
                if conn_phase3:
                    conn_phase3.autocommit = True
                    release_db_connection(conn_phase3)
                    logger.debug(f"DB connection released after Phase 3 for clip_id: {clip_id}")
        else:
            # If embedding_str is None here but needs_processing was True, it implies an error occurred in Phase 2
            if needs_processing:
                # Ensure final_status reflects the failure if not already set by an explicit exception
                 final_status = "failed"
                 if not error_message_for_db: # Check if an error message was already captured
                     error_message_for_db = "Processing failed to generate embedding string"
                 logger.error(f"Reached Phase 3 without a valid embedding string for clip {clip_id}, likely due to earlier processing error.")


    except (ValueError, FileNotFoundError, RuntimeError, ClientError, ImportError, NotImplementedError, psycopg2.DatabaseError) as e:
        # Catch specific, expected errors from any phase
        logger.error(f"TASK ERROR [Embedding]: clip_id {clip_id} - {e}", exc_info=True)
        final_status = "failed"
        error_message_for_db = f"{type(e).__name__}: {str(e)[:500]}"
        task_exception = e # Store exception to potentially re-raise

    except Exception as e:
        # Catch any other unexpected errors
        logger.error(f"UNEXPECTED TASK ERROR [Embedding]: clip_id {clip_id} - {e}", exc_info=True)
        final_status = "failed"
        error_message_for_db = f"Unexpected:{type(e).__name__}: {str(e)[:480]}"
        task_exception = e # Store exception to potentially re-raise

    finally:
        # --- Final Actions (Guaranteed to Run) ---

        # 1. Update DB state if the task failed overall
        if final_status == "failed" and needs_processing: # Only update if processing was attempted
            conn_err_update = None
            try:
                logger.info(f"Attempting to update DB state to 'embedding_failed' for clip {clip_id} due to error: {error_message_for_db}")
                conn_err_update = get_db_connection()
                # Autocommit is default True, good for single statement update
                with conn_err_update.cursor() as err_cur:
                    err_cur.execute(
                        """
                        UPDATE clips SET
                            ingest_state = 'embedding_failed',
                            last_error = %s,
                            retry_count = COALESCE(retry_count, 0) + 1,
                            updated_at = NOW()
                        WHERE id = %s AND ingest_state IN ('embedding', 'keyframed');
                        """, (error_message_for_db, clip_id)
                    )
                # No explicit commit needed if autocommit is True
                logger.info(f"Successfully requested DB update to 'embedding_failed' for clip {clip_id}.")
            except Exception as db_update_err:
                logger.error(f"CRITICAL: Failed attempt to update DB state to 'embedding_failed' for clip {clip_id}: {db_update_err}")
                # Don't rollback if autocommit is true
            finally:
                if conn_err_update:
                    release_db_connection(conn_err_update)
                    logger.debug(f"DB connection released after error state update attempt for clip {clip_id}")

        # 2. Clean up temporary directory
        if temp_dir_obj:
            try:
                shutil.rmtree(temp_dir, ignore_errors=True)
                logger.info(f"Cleaned up temporary directory: {temp_dir}")
            except Exception as cleanup_err:
                logger.warning(f"Error cleaning up temp dir {temp_dir}: {cleanup_err}")

    # === Return Result or Raise Exception ===
    if final_status == "failed" and task_exception:
        # Re-raise the original exception after cleanup attempt
        logger.error(f"Re-raising exception for failed task run: {task_exception}")
        raise task_exception
    elif final_status == "failed":
        # Raise a generic error if no specific exception was captured but status is failed
        logger.error(f"Raising generic error for failed task run with reason: {error_message_for_db}")
        raise RuntimeError(f"Embedding task failed for clip {clip_id}. Reason: {error_message_for_db or 'Unknown error during processing'}")
    else:
        # Return success dictionary
        logger.info(f"TASK [Embedding] finished for clip_id {clip_id} with status: {final_status}")
        return {
            "status": final_status, "clip_id": clip_id, "model_name": model_name,
            "strategy": generation_strategy, "embedding_dim": embedding_dim,
        }

# Local run block for testing purposes
if __name__ == "__main__":
    print("Running embedding task locally for testing (requires S3/DB/Model setup)...")
    # Example local test (replace with actual values)
    # test_clip_id = 463
    # test_model = "openai/clip-vit-base-patch32"
    # test_strategy = "keyframe_midpoint"
    # try:
    #     # Need to initialize the DB pool for local runs if not done elsewhere
    #     from utils.db_utils import initialize_db_pool, close_db_pool
    #     initialize_db_pool()
    #
    #     result = generate_embeddings_task.fn(
    #         clip_id=test_clip_id,
    #         model_name=test_model,
    #         generation_strategy=test_strategy,
    #         overwrite=True # Set True to force reprocessing during test
    #     )
    #     print(f"Local test result: {result}")
    # except Exception as e:
    #     print(f"Local test failed: {e}")
    #     traceback.print_exc()
    # finally:
    #     # Close the pool after testing
    #     close_db_pool()
    pass
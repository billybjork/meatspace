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

# S3 Imports
import boto3
from botocore.exceptions import ClientError, NoCredentialsError

# Model Loading Imports
try:
    from transformers import CLIPProcessor, CLIPModel, AutoImageProcessor, AutoModel
except ImportError:
    print("Warning: transformers library not found.")
    CLIPProcessor, CLIPModel, AutoImageProcessor, AutoModel = None, None, None, None

# DB Utils Import
import sys
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)
try:
    from utils.db_utils import get_db_connection
except ImportError as e:
    print(f"Error importing DB utils in embed.py: {e}")
    def get_db_connection(): raise NotImplementedError("Dummy DB connection")

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
    logger = get_run_logger(); logger.debug(f"Requesting model: {model_name}")
    if model_name in _model_cache: logger.debug("Using cached model."); return _model_cache[model_name]
    else: logger.info(f"Loading/caching model: {model_name}"); model, processor, model_type, embedding_dim = _load_model_and_processor_internal(model_name, device); _model_cache[model_name] = (model, processor, model_type, embedding_dim); return model, processor, model_type, embedding_dim
def _load_model_and_processor_internal(model_name, device='cpu'):
    logger = get_run_logger(); logger.info(f"Attempting load: {model_name}")
    model_type_str = None; embedding_dim = None; model = None; processor = None
    if "clip" in model_name.lower():
        if CLIPModel is None: raise ImportError("transformers required for CLIP.")
        try: model = CLIPModel.from_pretrained(model_name).to(device).eval(); processor = CLIPProcessor.from_pretrained(model_name); embedding_dim = getattr(model.config, 'projection_dim', 512); model_type_str = "clip"; logger.info(f"Loaded CLIP: {model_name} (Dim: {embedding_dim})")
        except Exception as e: raise ValueError(f"Failed load CLIP: {model_name}") from e
    elif "dinov2" in model_name.lower():
        if AutoModel is None: raise ImportError("transformers required for DINOv2.")
        try: processor = AutoImageProcessor.from_pretrained(model_name); model = AutoModel.from_pretrained(model_name).to(device).eval(); embedding_dim = getattr(model.config, 'hidden_size', None); model_type_str = "dino"; logger.info(f"Loaded DINOv2: {model_name} (Dim: {embedding_dim})")
        except Exception as e: raise ValueError(f"Failed load DINOv2: {model_name}") from e
    else: raise ValueError(f"Model not recognized: {model_name}")
    if not all([model, processor, model_type_str, embedding_dim]): raise RuntimeError(f"Failed init components: {model_name}")
    return model, processor, model_type_str, embedding_dim

# --- Helper to GENERATE potential sibling S3 Keys ---
def generate_potential_sibling_s3_keys(representative_s3_key):
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

    conn = None
    temp_dir_obj = None
    final_status = "failed"
    embedding_dim = -1
    needs_processing = False # Flag

    try:
        temp_dir_obj = tempfile.TemporaryDirectory(prefix=f"meatspace_embed_{clip_id}_")
        temp_dir = Path(temp_dir_obj.name)
        logger.info(f"Using temporary directory: {temp_dir}")

        device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        model, processor, model_type, embedding_dim = get_cached_model_and_processor(model_name, device)
        logger.info(f"Model '{model_name}' ({model_type}, Dim: {embedding_dim}) ready on {device}.")

        conn = get_db_connection()
        conn.autocommit = False # Manual transaction control

        s3_keys_to_download = []
        repr_s3_key = None

        # === Initial DB Check and State Update Block ===
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT pg_advisory_xact_lock(2, %s)", (clip_id,))
                logger.info(f"Acquired DB lock for clip_id: {clip_id}")
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
                    if current_state == 'embedded' and not overwrite:
                         logger.info(f"Skipping clip {clip_id}: Already 'embedded' and overwrite=False.")
                         final_status = "skipped"
                    elif current_state not in ['keyframed', 'embedding_failed']:
                         logger.warning(f"Skipping clip {clip_id}: State '{current_state}' not suitable.")
                         final_status = "skipped"
                elif embedding_exists and not overwrite:
                     logger.warning(f"Skipping clip {clip_id}: Embedding exists and overwrite=False.")
                     final_status = "skipped"
                else:
                    needs_processing = True # Proceed

                if needs_processing:
                    if not repr_s3_key: raise ValueError(f"Clip ID {clip_id} has NULL keyframe_filepath (S3 key).")
                    cur.execute(
                        "UPDATE clips SET ingest_state = 'embedding', updated_at = NOW(), last_error = NULL WHERE id = %s",
                        (clip_id,)
                    )
                    logger.info(f"Set clip {clip_id} state to 'embedding'")
            # --- End Cursor Context ---
            conn.commit() # Commit initial check/state update
            logger.debug("Initial check/state update transaction committed.")

        except (ValueError, psycopg2.DatabaseError) as db_err:
             logger.error(f"DB Error during initial check/update for clip {clip_id}: {db_err}", exc_info=True)
             if conn: conn.rollback()
             raise

        # === Exit if Skipped ===
        if final_status == "skipped":
            logger.info(f"Skipped processing for clip {clip_id}.")
            return {"status": final_status, "reason": f"Skipped due to state/overwrite/existing embedding", "clip_id": clip_id}

        # === Processing Outside Initial Transaction ===
        # 5. Determine S3 Keys
        is_multi_frame_strategy = generation_strategy in ['keyframe_multi_avg']
        if is_multi_frame_strategy: s3_keys_to_download = generate_potential_sibling_s3_keys(repr_s3_key)
        else: s3_keys_to_download = [repr_s3_key]
        if not s3_keys_to_download: raise ValueError(f"Could not determine S3 keys for clip {clip_id}")
        logger.info(f"Identified {len(s3_keys_to_download)} potential S3 key(s) for clip {clip_id}.")

        # 6. Download Keyframe(s)
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

        # 7. Load Images
        images = []; local_paths_to_load = list(downloaded_local_paths.values())
        try: images = [Image.open(p).convert("RGB") for p in local_paths_to_load]
        except Exception as img_err: raise RuntimeError(f"Error loading image from temp file for clip {clip_id}: {img_err}") from img_err
        if not images: raise RuntimeError(f"Image loading resulted in empty list for clip {clip_id}")

        # 8. Generate Embedding(s)
        all_features_np = None; final_embedding_np = None
        with torch.no_grad():
            try:
                if model_type == "clip": inputs = processor(text=None, images=images, return_tensors="pt", padding=True).to(device); image_features = model.get_image_features(**inputs); all_features_np = image_features.cpu().numpy()
                elif model_type == "dino": inputs = processor(images=images, return_tensors="pt").to(device); outputs = model(**inputs); image_features = outputs.last_hidden_state.mean(dim=1); all_features_np = image_features.cpu().numpy()
                else: raise NotImplementedError(f"Embedding generation not implemented for model type: {model_type}")
            except Exception as infer_err: raise RuntimeError(f"Model inference failed for clip {clip_id}: {infer_err}") from infer_err
        if all_features_np is None or all_features_np.shape[0] == 0: raise RuntimeError(f"Feature extraction yielded no results for clip {clip_id}")

        # 9. Aggregate Features
        if is_multi_frame_strategy and all_features_np.shape[0] > 1:
             if generation_strategy == 'keyframe_multi_avg': final_embedding_np = np.mean(all_features_np, axis=0); logger.debug(f"Averaged {all_features_np.shape[0]} embeddings.")
             else: raise NotImplementedError(f"Aggregation logic not defined: {generation_strategy}")
        else:
             if all_features_np.shape[0] > 1: logger.warning(f"Strategy '{generation_strategy}' got {all_features_np.shape[0]} features. Using first.")
             final_embedding_np = all_features_np[0]
        if final_embedding_np is None: raise RuntimeError(f"Final embedding could not be determined for clip {clip_id}")

        # 10. Prepare for DB
        embedding_list = final_embedding_np.tolist()
        if len(embedding_list) != embedding_dim: raise ValueError(f"Gen dim ({len(embedding_list)}) != expected ({embedding_dim})")
        embedding_str = '[' + ','.join(map(lambda x: f"{x:.8f}", embedding_list)) + ']'

        # 11. Insert/Update Embedding and Final Clip State
        try:
            with conn.cursor() as cur:
                  cur.execute("SELECT pg_advisory_xact_lock(2, %s)", (clip_id,)) # Lock for final update
                  insert_query = sql.SQL("""
                    INSERT INTO embeddings (clip_id, embedding, model_name, generation_strategy, generated_at) VALUES (%s, %s::vector, %s, %s, NOW())
                    ON CONFLICT (clip_id, model_name, generation_strategy) DO UPDATE SET embedding = EXCLUDED.embedding, generated_at = NOW();
                  """)
                  cur.execute(insert_query, (clip_id, embedding_str, model_name, generation_strategy))
                  logger.info(f"Inserted/updated embedding for clip {clip_id}")
                  cur.execute(
                    """
                    UPDATE clips SET ingest_state = 'embedded', embedded_at = NOW(), last_error = NULL, updated_at = NOW()
                    WHERE id = %s AND ingest_state = 'embedding';
                    """, (clip_id,)
                  )
                  if cur.rowcount == 0: logger.warning(f"DB update for final state 'embedded' for clip {clip_id} affected 0 rows.")
                  else: logger.info(f"Set clip {clip_id} state to 'embedded'."); final_status = "success"
            # --- End Cursor Context ---
            conn.commit() # Commit final update
            logger.debug("Final update transaction committed.")
        except (ValueError, psycopg2.DatabaseError) as db_err:
             logger.error(f"DB Error during final update for clip {clip_id}: {db_err}", exc_info=True)
             if conn: conn.rollback()
             raise

    except (ValueError, FileNotFoundError, RuntimeError, ClientError, psycopg2.DatabaseError, ImportError, NotImplementedError) as e:
        # --- Main Error Handling ---
        logger.error(f"TASK FAILED [Embedding]: clip_id {clip_id} - {e}", exc_info=True)
        final_status = "failed"
        if conn:
            try:
                conn.rollback(); conn.autocommit = True
                with conn.cursor() as err_cur:
                    err_cur.execute(
                        """
                        UPDATE clips SET ingest_state = 'embedding_failed', last_error = %s,
                            retry_count = COALESCE(retry_count, 0) + 1, updated_at = NOW()
                        WHERE id = %s AND ingest_state IN ('embedding', 'keyframed');
                        """, (f"{type(e).__name__}: {str(e)[:500]}", clip_id)
                    )
                logger.info(f"Attempted to set clip {clip_id} state to 'embedding_failed' after error.")
            except Exception as db_err: logger.error(f"Failed to update error state in DB for clip {clip_id} after task failure: {db_err}")
        raise e
    except Exception as e:
         # Catch any other unexpected errors
         logger.error(f"UNEXPECTED TASK FAILED [Embedding]: clip_id {clip_id} - {e}", exc_info=True)
         final_status = "failed"
         if conn:
             try:
                 conn.rollback(); conn.autocommit = True
                 with conn.cursor() as err_cur:
                    err_cur.execute(
                        """
                        UPDATE clips SET ingest_state = 'embedding_failed', last_error = %s,
                            retry_count = COALESCE(retry_count, 0) + 1, updated_at = NOW()
                        WHERE id = %s AND ingest_state IN ('embedding', 'keyframed');
                        """, (f"Unexpected:{type(e).__name__}: {str(e)[:480]}", clip_id)
                    )
                 logger.info(f"Attempted to set clip {clip_id} state to 'embedding_failed' after unexpected error.")
             except Exception as db_err: logger.error(f"Failed to update error state in DB for clip {clip_id} after unexpected task failure: {db_err}")
         raise
    finally:
        # --- Cleanup ---
        if conn: conn.autocommit = True; conn.close(); logger.debug(f"DB connection closed for clip_id: {clip_id}")
        if temp_dir_obj:
            try: shutil.rmtree(temp_dir, ignore_errors=True); logger.info(f"Cleaned up temporary directory: {temp_dir}")
            except Exception as cleanup_err: logger.warning(f"Error cleaning up temp dir {temp_dir}: {cleanup_err}")

    return {
        "status": final_status, "clip_id": clip_id, "model_name": model_name,
        "strategy": generation_strategy, "embedding_dim": embedding_dim,
    }

# Local run block for testing purposes
if __name__ == "__main__":
    print("Running embedding task locally for testing (requires S3/DB/Model setup)...")
    pass
import torch
from PIL import Image
import os
import re
import numpy as np
from pathlib import Path
import tempfile
import shutil
from datetime import datetime

from prefect import task, get_run_logger
import psycopg2
from psycopg2 import sql
from psycopg2.extras import Json

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
    # Import both connection getter/releaser and initializer
    from db.sync_db import get_db_connection, release_db_connection, initialize_db_pool
except ImportError as e:
    print(f"Error importing DB utils in embed.py: {e}")
    def get_db_connection(): raise NotImplementedError("DB Utils not loaded")
    def release_db_connection(conn): pass
    def initialize_db_pool(): pass

# --- S3 Configuration ---
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
AWS_REGION = os.getenv("AWS_REGION")

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

# --- Constants ---
ARTIFACT_TYPE_KEYFRAME = "keyframe"


# --- Global Model Cache ---
_model_cache = {}
def get_cached_model_and_processor(model_name, device='cpu'):
    """Loads model/processor or retrieves from cache."""
    logger = get_run_logger()
    cache_key = f"{model_name}_{device}"
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
             raise

def _load_model_and_processor_internal(model_name, device='cpu'):
    """Internal function to load models."""
    logger = get_run_logger()
    logger.info(f"Attempting to load model: {model_name} to device: {device}")
    model_type_str = None
    embedding_dim = None
    model = None
    processor = None

    if "clip" in model_name.lower():
        if CLIPModel is None: raise ImportError("transformers library required for CLIP models.")
        try:
            model = CLIPModel.from_pretrained(model_name).to(device).eval()
            processor = CLIPProcessor.from_pretrained(model_name)
            if hasattr(model.config, 'projection_dim') and model.config.projection_dim: embedding_dim = model.config.projection_dim
            elif hasattr(model.config, 'hidden_size') and model.config.hidden_size: embedding_dim = model.config.hidden_size
            else: embedding_dim = 512 if "base" in model_name.lower() else 768 # Adjust default based on common sizes
            model_type_str = "clip"
            logger.info(f"Loaded CLIP model: {model_name} (Type: {model_type_str}, Dim: {embedding_dim})")
        except Exception as e:
            if "Cannot copy out of meta tensor" in str(e):
                 raise ValueError(f"Meta tensor error loading CLIP: {model_name}. Check accelerate/config.") from e
            else: raise ValueError(f"Failed to load CLIP model: {model_name}") from e
    elif "dinov2" in model_name.lower():
        if AutoModel is None: raise ImportError("transformers library required for DINOv2 models.")
        try:
            processor = AutoImageProcessor.from_pretrained(model_name)
            model = AutoModel.from_pretrained(model_name).to(device).eval()
            if hasattr(model.config, 'hidden_size') and model.config.hidden_size: embedding_dim = model.config.hidden_size
            else: raise ValueError(f"Cannot determine embedding dimension for DINOv2 model: {model_name}")
            model_type_str = "dino"
            logger.info(f"Loaded DINOv2 model (Transformers): {model_name} (Type: {model_type_str}, Dim: {embedding_dim})")
        except Exception as e:
             if "Cannot copy out of meta tensor" in str(e):
                  raise ValueError(f"Meta tensor error loading DINOv2: {model_name}. Check accelerate/config.") from e
             else: raise ValueError(f"Failed to load DINOv2 model: {model_name}") from e
    else:
        raise ValueError(f"Model name not recognized or supported: {model_name}")

    if model is None or processor is None or model_type_str is None or embedding_dim is None:
         raise RuntimeError(f"Failed to initialize one or more components for model: {model_name}")

    return model, processor, model_type_str, embedding_dim


# --- Prefect Task ---
@task(name="Generate Clip Embedding", retries=1, retry_delay_seconds=60)
def generate_embeddings_task(
    clip_id: int,
    model_name: str,
    generation_strategy: str,
    overwrite: bool = False
    ):
    """
    Generates embeddings for a clip based on specified keyframe artifacts.
    """
    logger = get_run_logger()
    logger.info(f"TASK [Embedding]: Starting for clip_id: {clip_id}, Model: '{model_name}', Strategy: '{generation_strategy}', Overwrite: {overwrite}")

    if not s3_client:
        logger.error("S3 client is not available.")
        raise RuntimeError("S3 client is not initialized or configured.")

    conn = None
    temp_dir_obj = None
    temp_dir = None
    final_status = "failed_init"
    embedding_dim = -1
    needs_processing = False
    embedding_str = None
    error_message_for_db = None
    task_exception = None
    keyframe_strategy_name = None
    aggregation_method = None

    try:
        match = re.match(r"keyframe_([a-zA-Z0-9]+)(?:_(avg))?$", generation_strategy)
        if not match: raise ValueError(f"Invalid generation_strategy format: '{generation_strategy}'.")
        keyframe_strategy_name = match.group(1)
        aggregation_method = match.group(2)
        logger.info(f"Parsed strategy: keyframe_type='{keyframe_strategy_name}', aggregation='{aggregation_method}'")
    except ValueError as e:
        logger.error(str(e))
        raise e

    # Explicitly initialize DB pool if needed
    try:
        initialize_db_pool()
    except Exception as pool_err:
        logger.error(f"Failed to initialize DB pool at task start: {pool_err}")
        raise RuntimeError("DB pool initialization failed") from pool_err

    try:
        # === Phase 1: Initial DB Check and State Update ===
        conn = get_db_connection()
        conn.autocommit = False

        try:
            with conn.cursor() as cur:
                # 1. Acquire lock (must happen *after* autocommit=False)
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
                if not result: raise ValueError(f"Clip ID {clip_id} not found.")
                current_state, embedding_exists = result
                logger.info(f"Clip {clip_id}: State='{current_state}', EmbeddingExists={embedding_exists}")

                # 3. Determine if processing is needed
                allow_processing_state = current_state in ['keyframed', 'embedding_failed'] or \
                                         (overwrite and current_state == 'embedded')
                if not allow_processing_state:
                    reason = f"state '{current_state}' is not eligible"
                    if embedding_exists and not overwrite: reason = "embedding exists and overwrite=False"
                    logger.info(f"Skipping clip {clip_id}: {reason}.")
                    final_status = "skipped_state" if not embedding_exists else "skipped_exists"
                    needs_processing = False
                elif embedding_exists and not overwrite:
                    logger.info(f"Skipping clip {clip_id}: Embedding exists and overwrite=False.")
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
                    cur.execute(
                        "SELECT EXISTS (SELECT 1 FROM clip_artifacts WHERE clip_id = %s AND artifact_type = %s AND strategy = %s);",
                        (clip_id, ARTIFACT_TYPE_KEYFRAME, keyframe_strategy_name)
                    )
                    keyframes_found = cur.fetchone()[0]
                    if not keyframes_found: raise ValueError(f"Prerequisite keyframes missing for clip {clip_id}, strategy '{keyframe_strategy_name}'.")
                    logger.info(f"Verified prerequisite keyframes exist for '{keyframe_strategy_name}'.")

                    cur.execute("UPDATE clips SET ingest_state = 'embedding', updated_at = NOW(), last_error = NULL WHERE id = %s;", (clip_id,))
                    if cur.rowcount == 0: raise RuntimeError(f"Concurrency Error: Failed to set clip {clip_id} state to 'embedding'.")
                    logger.info(f"Set clip {clip_id} state to 'embedding'")

            conn.commit()
            logger.debug("Phase 1 DB transaction committed.")

        except (ValueError, psycopg2.DatabaseError, RuntimeError) as db_err:
            logger.error(f"DB Error during Phase 1 for clip {clip_id}: {db_err}", exc_info=True)
            if conn: conn.rollback()
            error_message_for_db = f"DB Phase 1 Error: {str(db_err)[:500]}"
            task_exception = db_err
            final_status = "failed_db_phase1"
            raise

        # === Exit Early if Skipped ===
        if not needs_processing:
            logger.info(f"No processing required for clip {clip_id}. Final status: {final_status}")
            # Return status indicating skip reason
            return {"status": final_status, "reason": final_status, "clip_id": clip_id, "model_name": model_name, "strategy": generation_strategy}

        # === Phase 2: Fetch Artifacts, Download, Process ===
        s3_keys_to_download = []
        try:
            # 5. Fetch Keyframe Artifact S3 Keys from DB (read-only, doesn't strictly need transaction)
            # Reuse connection 'conn', ensure autocommit is True for safety if we didn't commit Phase 1 for some reason (though we should have)
            conn.autocommit = True # Set for reads
            with conn.cursor() as cur:
                 logger.info(f"Fetching keyframe paths for clip {clip_id}, strategy '{keyframe_strategy_name}'...")
                 query = sql.SQL("SELECT s3_key FROM clip_artifacts WHERE clip_id = %s AND artifact_type = %s AND strategy = %s ORDER BY tag;")
                 cur.execute(query, (clip_id, ARTIFACT_TYPE_KEYFRAME, keyframe_strategy_name))
                 s3_keys_to_download = [row[0] for row in cur.fetchall()]
            # No commit/rollback needed for SELECT

            if not s3_keys_to_download: raise FileNotFoundError(f"No keyframe S3 keys found in DB for clip {clip_id}, strategy '{keyframe_strategy_name}'.")
            logger.info(f"Found {len(s3_keys_to_download)} keyframe artifact(s) in DB.")

            # 6. Setup Temp Dir & Download
            if not s3_client: raise RuntimeError("S3 client unavailable.")
            temp_dir_obj = tempfile.TemporaryDirectory(prefix=f"meatspace_embed_{clip_id}_")
            temp_dir = Path(temp_dir_obj.name)
            logger.info(f"Using temporary directory: {temp_dir}")

            downloaded_local_paths = {}
            for s3_key in s3_keys_to_download:
                if not s3_key: continue
                local_filename = Path(s3_key).name
                local_temp_path = temp_dir / local_filename
                try:
                    logger.debug(f"Downloading s3://{S3_BUCKET_NAME}/{s3_key} to {local_temp_path}")
                    s3_client.download_file(S3_BUCKET_NAME, s3_key, str(local_temp_path))
                    downloaded_local_paths[s3_key] = str(local_temp_path)
                except ClientError as s3_err:
                    raise FileNotFoundError(f"Failed download required keyframe {s3_key}") from s3_err

            if not downloaded_local_paths: raise FileNotFoundError(f"Failed download ANY required keyframes from S3.")
            logger.info(f"Successfully downloaded {len(downloaded_local_paths)} keyframe image(s).")

            # 7. Load Images & Run Inference
            images = []
            for p in downloaded_local_paths.values():
                 try: img = Image.open(p).convert("RGB"); img.verify(); img = Image.open(p).convert("RGB"); images.append(img)
                 except Exception as img_err: raise RuntimeError(f"Error loading image {Path(p).name}") from img_err
            if not images: raise RuntimeError(f"Image loading resulted in empty list.")

            device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
            model, processor, model_type, embedding_dim = get_cached_model_and_processor(model_name, device)
            logger.info(f"Using Model '{model_name}' ({model_type}, Dim: {embedding_dim}) on device {device}.")

            all_features_np = None
            logger.info(f"Running inference for {len(images)} image(s)...")
            with torch.no_grad():
                try:
                    if model_type == "clip":
                        inputs = processor(text=None, images=images, return_tensors="pt", padding=True).to(device)
                        image_features = model.get_image_features(**inputs); all_features_np = image_features.cpu().numpy()
                    elif model_type == "dino":
                        inputs = processor(images=images, return_tensors="pt").to(device)
                        outputs = model(**inputs); image_features = outputs.last_hidden_state.mean(dim=1); all_features_np = image_features.cpu().numpy()
                    else: raise NotImplementedError(f"Embedding logic not implemented for type: {model_type}")
                except Exception as infer_err: raise RuntimeError(f"Model inference failed") from infer_err

            if all_features_np is None or all_features_np.shape[0] != len(images):
                 raise RuntimeError("Feature extraction failed or produced unexpected number of results.")
            logger.info(f"Generated {all_features_np.shape[0]} raw embeddings.")

            # 8. Aggregate Embeddings
            final_embedding_np = None
            if aggregation_method == 'avg' and all_features_np.shape[0] > 1:
                final_embedding_np = np.mean(all_features_np, axis=0); logger.info(f"Averaged {all_features_np.shape[0]} embeddings.")
            elif all_features_np.shape[0] == 1:
                 final_embedding_np = all_features_np[0]; logger.info(f"Using single generated embedding.")
            elif all_features_np.shape[0] > 1 and not aggregation_method:
                 raise NotImplementedError(f"Ambiguous result: Multiple embeddings for non-aggregating strategy '{generation_strategy}'.")
            else: raise RuntimeError("Could not determine final embedding.")

            # 9. Format Embedding String
            embedding_list = final_embedding_np.tolist()
            if len(embedding_list) != embedding_dim: raise ValueError(f"Generated embedding dim mismatch ({len(embedding_list)} vs {embedding_dim})")
            embedding_str = '[' + ','.join(map(str, embedding_list)) + ']'
            logger.info(f"Prepared final embedding string (Dim: {len(embedding_list)}).")

        except (FileNotFoundError, RuntimeError, ValueError, ClientError, ImportError, NotImplementedError) as phase2_err:
            logger.error(f"Error during Phase 2 for clip {clip_id}: {phase2_err}", exc_info=True)
            error_message_for_db = f"Processing Error: {str(phase2_err)[:500]}"
            task_exception = phase2_err
            final_status = "failed_processing"
            raise

        # === Phase 3: Final DB Update ===
        if not embedding_str:
             # Should have been caught earlier
             raise task_exception or RuntimeError("Embedding string generation failed silently.")

        try:
            conn.autocommit = False
            with conn.cursor() as cur:
                # 1. Acquire lock
                cur.execute("SELECT pg_advisory_xact_lock(2, %s)", (clip_id,))
                logger.info(f"Acquired DB lock for clip_id: {clip_id} (Phase 3)")

                # 2. Insert/Update embedding
                insert_query = sql.SQL("""
                    INSERT INTO embeddings (clip_id, embedding, model_name, generation_strategy, generated_at, embedding_dim)
                    VALUES (%s, %s::vector, %s, %s, NOW(), %s)
                    ON CONFLICT (clip_id, model_name, generation_strategy)
                    DO UPDATE SET embedding = EXCLUDED.embedding, generated_at = NOW(), embedding_dim = EXCLUDED.embedding_dim;
                """)
                cur.execute(insert_query, (clip_id, embedding_str, model_name, generation_strategy, embedding_dim))
                logger.info(f"Inserted/updated embedding record.")

                # 3. Update clip status
                update_clip_query = sql.SQL("""
                    UPDATE clips SET ingest_state = 'embedded', embedded_at = NOW(), last_error = NULL, updated_at = NOW()
                    WHERE id = %s AND ingest_state = 'embedding';
                """)
                cur.execute(update_clip_query, (clip_id,))

                if cur.rowcount == 1:
                    logger.info(f"Updated clip {clip_id} state to 'embedded'.")
                    final_status = "success"
                else:
                    logger.error(f"Failed to update clip {clip_id} final state to 'embedded'. Rowcount: {cur.rowcount}.")
                    conn.rollback()
                    final_status = "failed_state_update"
                    error_message_for_db = "Failed final state update to 'embedded'"
                    raise RuntimeError(f"Failed to update clip {clip_id} state in final DB step.")

            conn.commit()
            logger.debug("Phase 3 DB updates committed.")

        except (psycopg2.DatabaseError, RuntimeError) as db_err:
            logger.error(f"DB Error during Phase 3 for clip {clip_id}: {db_err}", exc_info=True)
            if conn: conn.rollback()
            error_message_for_db = f"DB Phase 3 Error: {str(db_err)[:500]}"
            task_exception = db_err
            final_status = "failed_db_update"
            raise

    except (ValueError, FileNotFoundError, RuntimeError, ClientError, ImportError, NotImplementedError, psycopg2.DatabaseError) as e:
        # --- Main Error Handling ---
        logger.error(f"TASK ERROR [Embedding]: clip_id {clip_id} - {e}", exc_info=True)
        final_status = final_status if final_status not in ["failed_init", "failed"] else "failed_processing"
        error_message_for_db = error_message_for_db or f"{type(e).__name__}: {str(e)[:500]}"
        task_exception = e
        if conn and needs_processing:
            try:
                logger.warning(f"Attempting rollback and state update to 'embedding_failed' for clip {clip_id}.")
                conn.rollback() # Ensure rollback happens before setting autocommit
                conn.autocommit = True
                with conn.cursor() as err_cur:
                    fail_update_sql = sql.SQL("UPDATE clips SET ingest_state = 'embedding_failed', last_error = %s, retry_count = COALESCE(retry_count, 0) + 1, updated_at = NOW() WHERE id = %s AND ingest_state IN ('embedding', 'keyframed');")
                    err_cur.execute(fail_update_sql, (error_message_for_db, clip_id))
                logger.info(f"Attempted state update to 'embedding_failed' for clip {clip_id}.")
            except Exception as db_fail_err:
                logger.error(f"CRITICAL: Failed to update error state for clip {clip_id}: {db_fail_err}")
        elif not conn: logger.error("DB connection unavailable for error state update.")

    except Exception as e:
         # Catch unexpected errors
         logger.error(f"UNEXPECTED TASK ERROR [Embedding]: clip_id {clip_id} - {e}", exc_info=True)
         final_status = "failed_unexpected"
         error_message_for_db = f"Unexpected:{type(e).__name__}: {str(e)[:480]}"
         task_exception = e
         if conn and needs_processing:
             try:
                conn.rollback(); conn.autocommit = True
                with conn.cursor() as err_cur: err_cur.execute(sql.SQL("UPDATE clips SET ingest_state = 'embedding_failed', last_error = %s, retry_count = COALESCE(retry_count, 0) + 1, updated_at = NOW() WHERE id = %s AND ingest_state IN ('embedding', 'keyframed');"), (error_message_for_db, clip_id))
                logger.info(f"Attempted state update to 'embedding_failed' after unexpected error.")
             except Exception as db_fail_err: logger.error(f"CRITICAL: Failed to update error state for clip {clip_id}: {db_fail_err}")

    finally:
        # --- Final Actions ---
        if conn:
            conn.autocommit = True
            release_db_connection(conn)
            logger.debug(f"DB connection released for clip_id: {clip_id}")
        if temp_dir_obj:
            try: shutil.rmtree(temp_dir, ignore_errors=True); logger.info(f"Cleaned up temp dir: {temp_dir}")
            except Exception as cleanup_err: logger.warning(f"Error cleaning temp dir {temp_dir}: {cleanup_err}")

    # === Return Result or Raise Exception ===
    result_payload = {
        "status": final_status,
        "clip_id": clip_id,
        "model_name": model_name,
        "strategy": generation_strategy,
        "embedding_dim": embedding_dim,
    }

    if final_status == "success":
        logger.info(f"TASK [Embedding] finished successfully for clip_id {clip_id}.")
        return result_payload
    elif final_status.startswith("skipped"):
        logger.info(f"TASK [Embedding] skipped for clip_id {clip_id}. Status: {final_status}")
        return result_payload
    else:
        logger.error(f"TASK [Embedding] failed for clip_id {clip_id}. Status: {final_status}. Error: {error_message_for_db}.")
        if task_exception: raise task_exception
        else: raise RuntimeError(f"Embedding task failed for clip {clip_id} with status {final_status}. Reason: {error_message_for_db or 'Unknown error'}")
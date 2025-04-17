import torch
from PIL import Image
import os
import re
import numpy as np
import traceback
from pathlib import Path

from prefect import task, get_run_logger
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values # Still useful for single row insert/update

# Model Loading Imports
try:
    from transformers import CLIPProcessor, CLIPModel, AutoImageProcessor, AutoModel
except ImportError:
    print("Warning: transformers library not found. CLIP/some DINOv2 models will not be available.")
    CLIPProcessor, CLIPModel, AutoImageProcessor, AutoModel = None, None, None, None

# Assuming db_utils.py is in ../utils relative to tasks/
import sys
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

try:
    from utils.db_utils import get_db_connection, get_media_base_dir
except ImportError as e:
    print(f"Error importing DB utils in embed.py: {e}")
    def get_db_connection(): raise NotImplementedError("Dummy DB connection")
    def get_media_base_dir(): raise NotImplementedError("Dummy media base dir")

# --- Global Model Cache (Optional but Recommended for Performance) ---
# Simple dictionary to cache loaded models/processors within a worker process lifespan
_model_cache = {}

def get_cached_model_and_processor(model_name, device='cpu'):
    """Loads model/processor or retrieves from cache."""
    logger = get_run_logger()
    if model_name in _model_cache:
        logger.debug(f"Using cached model/processor for: {model_name}")
        return _model_cache[model_name]
    else:
        logger.info(f"Loading and caching model/processor: {model_name}")
        # Actual loading logic (same as in the script)
        model, processor, model_type, embedding_dim = _load_model_and_processor_internal(model_name, device)
        _model_cache[model_name] = (model, processor, model_type, embedding_dim)
        return model, processor, model_type, embedding_dim

def _load_model_and_processor_internal(model_name, device='cpu'):
    """Internal function to load models (copied/adapted from script)."""
    logger = get_run_logger()
    logger.info(f"Attempting to load model: {model_name}")
    model_type_str = None
    embedding_dim = None
    model = None
    processor = None

    # --- CLIP Models ---
    if "clip" in model_name.lower():
        if CLIPModel is None: raise ImportError("transformers required for CLIP.")
        try:
            model = CLIPModel.from_pretrained(model_name).to(device).eval()
            processor = CLIPProcessor.from_pretrained(model_name)
            if hasattr(model.config, 'projection_dim'): embedding_dim = model.config.projection_dim
            elif "large" in model_name.lower(): embedding_dim = 768
            elif "base" in model_name.lower(): embedding_dim = 512
            else: embedding_dim = 512 # Default
            model_type_str = "clip"
            logger.info(f"Loaded CLIP model: {model_name} (Dim: {embedding_dim})")
        except Exception as e: raise ValueError(f"Failed to load CLIP model: {model_name}") from e

    # --- DINOv2 Models (using Transformers) ---
    elif "dinov2" in model_name.lower():
        if AutoModel is None: raise ImportError("transformers required for DINOv2.")
        try:
            processor = AutoImageProcessor.from_pretrained(model_name)
            model = AutoModel.from_pretrained(model_name).to(device).eval()
            if hasattr(model.config, 'hidden_size'): embedding_dim = model.config.hidden_size
            else: # Fallbacks
                if "base" in model_name: embedding_dim = 768
                elif "small" in model_name: embedding_dim = 384
                elif "large" in model_name: embedding_dim = 1024
                elif "giant" in model_name: embedding_dim = 1536
                else: raise ValueError(f"Cannot determine embedding dimension for DINOv2: {model_name}")
            model_type_str = "dino"
            logger.info(f"Loaded DINOv2 model (Transformers): {model_name} (Dim: {embedding_dim})")
        except Exception as e: raise ValueError(f"Failed to load DINOv2 model: {model_name}") from e

    # --- Add other model types here ---
    else:
        raise ValueError(f"Model name not recognized or supported: {model_name}")

    if model is None or processor is None or model_type_str is None or embedding_dim is None:
         raise RuntimeError(f"Failed to initialize components for model: {model_name}")

    return model, processor, model_type_str, embedding_dim
# --- End Model Loading ---

# --- Helper to find sibling keyframes (copied from script) ---
def find_sibling_keyframes(representative_rel_path, media_base_dir):
    """
    Given a representative keyframe path (e.g., ..._50pct.jpg from a 'multi' strategy),
    finds paths for potential siblings (25pct, 75pct) based on naming convention.
    Handles 'midpoint' strategy by returning just the one path.

    Returns a list of *absolute* paths of existing keyframe files found.
    Returns empty list if the representative file itself doesn't exist or on error.
    """
    logger = get_run_logger()
    sibling_abs_paths = []
    if not representative_rel_path:
        logger.warning("find_sibling_keyframes received an empty representative path.")
        return []
    representative_abs_path = os.path.join(media_base_dir, representative_rel_path)

    if not os.path.isfile(representative_abs_path):
        logger.warning(f"Representative keyframe not found: {representative_abs_path}")
        return []

    try:
        base_name = os.path.basename(representative_rel_path)
        dir_name = os.path.dirname(representative_rel_path) # Relative directory path

        # Example: Landline_clip_00123_frame_50pct.jpg -> ('Landline_clip_00123_frame_', '50pct', '.jpg')
        match = re.match(r'^(.*_frame_)(\d+pct|mid)(\.[^.]+)$', base_name)

        if not match:
            logger.debug(f"Filename '{base_name}' doesn't match multi-frame pattern. Using only representative.")
            return [representative_abs_path]

        base_prefix = match.group(1)
        current_tag = match.group(2)
        extension = match.group(3)

        if current_tag == 'mid':
            return [representative_abs_path] # Midpoint strategy only uses itself

        # Multi-frame strategy ('25pct', '50pct', '75pct')
        tags_to_check = ["25pct", "50pct", "75pct"]
        for tag in tags_to_check:
            sibling_filename = f"{base_prefix}{tag}{extension}"
            sibling_rel_path = os.path.join(dir_name, sibling_filename)
            sibling_abs_path = os.path.join(media_base_dir, sibling_rel_path)
            if os.path.isfile(sibling_abs_path):
                sibling_abs_paths.append(sibling_abs_path)
            else:
                logger.debug(f"Sibling keyframe not found: {sibling_abs_path}")

        if not sibling_abs_paths:
             logger.warning(f"Found no keyframe files matching pattern {base_prefix}* in {dir_name}. Returning only representative.")
             return [representative_abs_path] # Fallback

        # Return unique, sorted paths
        return sorted(list(set(sibling_abs_paths)))

    except Exception as e:
        logger.error(f"Error finding sibling keyframes for {representative_rel_path}: {e}", exc_info=True)
        return [] # Return empty on error

# --- Prefect Task ---
@task(name="Generate Clip Embedding", retries=1, retry_delay_seconds=45)
def generate_embeddings_task(
    clip_id: int,
    model_name: str,
    generation_strategy: str,
    overwrite: bool = False # Allow forcing overwrite via task parameter if needed later
    ):
    """
    Prefect task to generate an embedding for a single clip based on its keyframe(s)
    using the specified model and strategy, and update the database.

    Args:
        clip_id (int): The ID of the clip to process.
        model_name (str): Identifier for the embedding model.
        generation_strategy (str): Label describing how the embedding relates to keyframes.
        overwrite (bool): If True, attempt generation even if DB state indicates completion.
                          The DB insert uses ON CONFLICT DO UPDATE regardless.

    Returns:
        dict: Information about the operation, e.g.,
              {'status': 'success', 'clip_id': clip_id, 'embedding_dim': 512}
              or {'status': 'skipped', ...} or raises error on failure.
    """
    logger = get_run_logger()
    logger.info(f"TASK [Embedding]: Starting for clip_id: {clip_id}, Model: '{model_name}', Strategy: '{generation_strategy}', Overwrite: {overwrite}")
    conn = None
    final_status = "failed"
    embedding_dim = -1 # Initialize

    try:
        media_base_dir = get_media_base_dir()
        device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        logger.info(f"Using device: {device}")

        # --- Load Model (potentially cached) ---
        # Load model early, before DB transaction maybe? Depends on lock duration sensitivity.
        # Let's load it here first. If it fails, we don't need DB interaction.
        model, processor, model_type, embedding_dim = get_cached_model_and_processor(model_name, device)
        logger.info(f"Model '{model_name}' ({model_type}, Dim: {embedding_dim}) ready.")

        conn = get_db_connection()
        conn.autocommit = False # Manage transaction explicitly

        with conn.transaction(): # Use context manager for BEGIN/COMMIT/ROLLBACK
            # === Transaction Start ===
            with conn.cursor() as cur:
                # 1. Acquire Advisory Lock for this clip
                try:
                    cur.execute("SELECT pg_advisory_xact_lock(2, %s)", (clip_id,)) # Use type 2 for clips
                    logger.info(f"Acquired DB lock for clip_id: {clip_id}")
                except Exception as lock_err:
                    raise RuntimeError("DB Lock acquisition failed") from lock_err

                # 2. Fetch clip details, check state, check for existing embedding
                cur.execute(
                    """
                    SELECT
                        c.keyframe_filepath, c.ingest_state,
                        EXISTS (
                            SELECT 1 FROM embeddings e
                            WHERE e.clip_id = c.id
                              AND e.model_name = %s
                              AND e.generation_strategy = %s
                        ) AS embedding_exists
                    FROM clips c
                    WHERE c.id = %s;
                    """,
                    (model_name, generation_strategy, clip_id)
                )
                result = cur.fetchone()
                if not result:
                    raise ValueError(f"Clip ID {clip_id} not found.")

                repr_rel_path, current_state, embedding_exists = result
                logger.info(f"Clip {clip_id}: State='{current_state}', KeyframePath='{repr_rel_path}', EmbeddingExists={embedding_exists}")

                # 3. Check if processing should proceed
                # Requires 'keyframed' state, or allow retry from 'embedding_failed'
                allow_processing = current_state == 'keyframed' or \
                                  (overwrite and current_state == 'embedding_failed') or \
                                  (overwrite and current_state == 'embedded') # Allow re-embedding if overwriting

                if not allow_processing:
                    if current_state == 'embedded' and not overwrite:
                         logger.info(f"Skipping clip {clip_id}: Already in state 'embedded' and overwrite=False.")
                         final_status = "skipped"
                         # Exit the transaction block cleanly
                         return {"status": final_status, "reason": "Already embedded", "clip_id": clip_id}
                    elif current_state not in ['keyframed', 'embedding_failed']:
                         logger.warning(f"Skipping clip {clip_id}: State '{current_state}' not suitable for embedding generation (needs 'keyframed' or 'embedding_failed' w/ overwrite).")
                         final_status = "skipped"
                         return {"status": final_status, "reason": f"Incorrect state: {current_state}", "clip_id": clip_id}

                if not repr_rel_path:
                    raise ValueError(f"Clip ID {clip_id} has NULL keyframe_filepath. Cannot generate embedding.")

                # Allow proceeding even if embedding_exists is True, rely on ON CONFLICT DO UPDATE
                if embedding_exists and not overwrite:
                     logger.warning(f"Skipping clip {clip_id}: Embedding already exists for this model/strategy and overwrite=False. (State was {current_state})")
                     # Update state back to 'embedded' if it was somehow reset? Or just skip? Let's just skip.
                     final_status = "skipped"
                     return {"status": final_status, "reason": "Embedding exists, overwrite=False", "clip_id": clip_id}


                # Update state to 'embedding'
                cur.execute(
                    "UPDATE clips SET ingest_state = 'embedding', updated_at = NOW(), last_error = NULL WHERE id = %s",
                    (clip_id,)
                )
                logger.info(f"Set clip {clip_id} state to 'embedding'")

                # --- Identify Keyframe Paths ---
                logger.debug(f"Identifying keyframe paths based on strategy '{generation_strategy}' and repr path '{repr_rel_path}'")
                abs_paths_for_clip = []
                is_multi_frame_strategy = generation_strategy in ['keyframe_multi_avg'] # Add others if needed

                if is_multi_frame_strategy:
                    abs_paths_for_clip = find_sibling_keyframes(repr_rel_path, media_base_dir)
                    if not abs_paths_for_clip:
                         raise FileNotFoundError(f"No valid keyframes found for multi-strategy clip {clip_id} (repr: {repr_rel_path}).")
                    # Add strategy-specific checks (e.g., needing exactly 3 frames)
                    if generation_strategy == 'keyframe_multi_avg' and len(abs_paths_for_clip) < 1: # Need at least 1 for avg
                         raise ValueError(f"Strategy '{generation_strategy}' needs >= 1 frame for clip {clip_id}, found {len(abs_paths_for_clip)}")
                else:
                    # Single frame strategy (e.g., keyframe_midpoint)
                    abs_path = os.path.join(media_base_dir, repr_rel_path)
                    if os.path.isfile(abs_path):
                        abs_paths_for_clip = [abs_path]
                    else:
                        raise FileNotFoundError(f"Required keyframe not found for clip {clip_id}: {abs_path}")

                logger.info(f"Found {len(abs_paths_for_clip)} keyframe(s) to process for clip {clip_id}.")

                # --- Load Images ---
                try:
                    images = [Image.open(p).convert("RGB") for p in abs_paths_for_clip]
                except FileNotFoundError as e:
                     raise FileNotFoundError(f"Error opening keyframe image for clip {clip_id}: {e}") from e
                except Exception as img_err:
                     raise RuntimeError(f"Error loading image for clip {clip_id}: {img_err}") from img_err

                # --- Generate Embedding(s) ---
                all_features_np = None
                with torch.no_grad():
                    try:
                        if model_type == "clip":
                            inputs = processor(text=None, images=images, return_tensors="pt", padding=True).to(device)
                            image_features = model.get_image_features(**inputs)
                            all_features_np = image_features.cpu().numpy()

                        elif model_type == "dino":
                            inputs = processor(images=images, return_tensors="pt").to(device)
                            outputs = model(**inputs)
                            # Use mean pooling of patch tokens - common for DINOv2
                            image_features = outputs.last_hidden_state.mean(dim=1)
                            all_features_np = image_features.cpu().numpy()
                        else:
                             raise NotImplementedError(f"Embedding generation not implemented for model type: {model_type}")

                    except Exception as infer_err:
                        raise RuntimeError(f"Model inference failed for clip {clip_id}: {infer_err}") from infer_err

                if all_features_np is None or all_features_np.shape[0] == 0:
                     raise RuntimeError(f"Feature extraction yielded no results for clip {clip_id}")

                # --- Aggregate Features (if necessary) ---
                final_embedding_np = None
                if is_multi_frame_strategy:
                    if generation_strategy == 'keyframe_multi_avg':
                         final_embedding_np = np.mean(all_features_np, axis=0)
                         # Optional: L2 normalize averaged embedding
                         # norm = np.linalg.norm(final_embedding_np)
                         # if norm > 1e-6: final_embedding_np = final_embedding_np / norm
                         logger.debug(f"Averaged {all_features_np.shape[0]} frame embeddings for clip {clip_id}.")
                    # Add elif for other multi-strategies
                    else:
                         raise NotImplementedError(f"Aggregation logic not defined for strategy: {generation_strategy}")
                else:
                    # Single frame strategy
                    if all_features_np.shape[0] > 1:
                         logger.warning(f"Single frame strategy got {all_features_np.shape[0]} features for clip {clip_id}. Using first.")
                    final_embedding_np = all_features_np[0]

                if final_embedding_np is None:
                    raise RuntimeError(f"Final embedding could not be determined for clip {clip_id}")

                # --- Prepare for DB ---
                embedding_list = final_embedding_np.tolist()
                if len(embedding_list) != embedding_dim:
                     raise ValueError(f"Generated embedding dimension ({len(embedding_list)}) does not match expected ({embedding_dim}) for clip {clip_id}")

                # Convert to pgvector string format: '[1.2,3.4,...]'
                embedding_str = '[' + ','.join(map(lambda x: f"{x:.8f}", embedding_list)) + ']' # Format for precision

                # --- Insert/Update Embedding in DB ---
                # Use ON CONFLICT DO UPDATE to handle overwrites or retries gracefully
                insert_query = sql.SQL("""
                    INSERT INTO embeddings (clip_id, embedding, model_name, generation_strategy, generated_at)
                    VALUES (%s, %s::vector, %s, %s, NOW())
                    ON CONFLICT (clip_id, model_name, generation_strategy)
                    DO UPDATE SET
                        embedding = EXCLUDED.embedding,
                        generated_at = NOW();
                """)
                cur.execute(insert_query, (clip_id, embedding_str, model_name, generation_strategy))
                logger.info(f"Successfully inserted/updated embedding for clip {clip_id} (Model: {model_name}, Strategy: {generation_strategy})")

                # --- Update Clip State on Success ---
                cur.execute(
                    """
                    UPDATE clips
                    SET ingest_state = 'embedded',
                        embedded_at = NOW(),
                        last_error = NULL,
                        updated_at = NOW()
                    WHERE id = %s
                    """,
                    (clip_id,)
                )
                logger.info(f"Set clip {clip_id} state to 'embedded'")
                final_status = "success"

        # === Transaction End (Commit happens here if no exceptions) ===
        logger.info(f"Transaction committed for clip {clip_id}.")

    except (ValueError, FileNotFoundError, RuntimeError, psycopg2.DatabaseError, ImportError, NotImplementedError) as e:
        logger.error(f"TASK FAILED [Embedding]: clip_id {clip_id} - {e}", exc_info=True)
        final_status = "failed"
        if conn:
            # Rollback is handled by transaction context manager failure
            # Update DB state outside the transaction (requires autocommit=True)
            try:
                conn.autocommit = True
                with conn.cursor() as err_cur:
                    err_cur.execute(
                        """
                        UPDATE clips
                        SET ingest_state = 'embedding_failed',
                            last_error = %s,
                            retry_count = COALESCE(retry_count, 0) + 1,
                            updated_at = NOW()
                        WHERE id = %s AND ingest_state = 'embedding'
                        """,
                        (f"{type(e).__name__}: {str(e)[:500]}", clip_id)
                    )
                logger.info(f"Set clip {clip_id} state to 'embedding_failed' after error.")
            except Exception as db_err:
                logger.error(f"Failed to update error state in DB for clip {clip_id} after task failure: {db_err}")
        raise e # Re-raise the original exception for Prefect
    except Exception as e:
         # Catch any other unexpected errors
         logger.error(f"UNEXPECTED TASK FAILED [Embedding]: clip_id {clip_id} - {e}", exc_info=True)
         final_status = "failed"
         if conn:
             try:
                 conn.autocommit = True
                 with conn.cursor() as err_cur:
                    err_cur.execute(
                        """
                        UPDATE clips SET ingest_state = 'embedding_failed', last_error = %s, retry_count = COALESCE(retry_count, 0) + 1, updated_at = NOW()
                        WHERE id = %s AND ingest_state = 'embedding'
                        """,
                         (f"Unexpected:{type(e).__name__}: {str(e)[:480]}", clip_id)
                    )
                 logger.info(f"Set clip {clip_id} state to 'embedding_failed' after unexpected error.")
             except Exception as db_err:
                 logger.error(f"Failed to update error state in DB for clip {clip_id} after unexpected task failure: {db_err}")
         raise
    finally:
        if conn:
            conn.autocommit = True # Ensure reset before closing
            conn.close()
            logger.debug(f"DB connection closed for clip_id: {clip_id}")

    # Return success information
    return {
        "status": final_status,
        "clip_id": clip_id,
        "model_name": model_name,
        "strategy": generation_strategy,
        "embedding_dim": embedding_dim,
    }

# Example local test block (similar to keyframe task)
if __name__ == "__main__":
    print("Running embedding task locally for testing (requires setup)...")
    # --- CONFIGURATION FOR LOCAL TEST ---
    test_clip_id = 1 # Replace with a valid clip ID in 'keyframed' state
    test_model = "openai/clip-vit-base-patch32" # Or 'facebook/dinov2-base', etc.
    # Match this strategy to how the keyframe was generated!
    test_strategy = "keyframe_midpoint" # or 'keyframe_multi_avg'
    test_overwrite = False
    # -------------------------------------

    if not os.getenv("DATABASE_URL") or not os.getenv("MEDIA_BASE_DIR"):
         print("Error: DATABASE_URL and MEDIA_BASE_DIR must be set in environment.")
    else:
        try:
            # Note: Model loading might be slow on first run
            result = generate_embeddings_task.fn( # Use .fn for direct call
                clip_id=test_clip_id,
                model_name=test_model,
                generation_strategy=test_strategy,
                overwrite=test_overwrite
            )
            print(f"\nLocal Test Result:\n{result}")
        except Exception as e:
            print(f"\nLocal test failed: {e}")
            traceback.print_exc()
import torch
from PIL import Image
import os
import argparse
import asyncio
import numpy as np
from tqdm import tqdm
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
import re
import traceback
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Model Loading - Keep separate for clarity
try:
    from transformers import CLIPProcessor, CLIPModel
except ImportError:
    print("Warning: transformers library not found. CLIP models will not be available.")
    CLIPProcessor, CLIPModel = None, None

# Placeholder for potential future DINOv2 imports (e.g., via timm or torchvision)
# try:
#     import timm
#     from torchvision import transforms
# except ImportError:
#     print("Warning: timm or torchvision not found. DINOv2 models may not be available.")
#     timm, transforms = None, None

from utils.db_utils import get_db_connection, get_media_base_dir

# --- Model and Processor Loading ---
def get_model_and_processor(model_name, device='cpu'):
    """
    Loads the specified model and processor/transforms.
    Returns: tuple (model, processor, model_type_str, embedding_dim)
    Raises: ValueError, ImportError, NotImplementedError
    """
    print(f"Attempting to load model: {model_name}")
    model_type_str = None
    embedding_dim = None

    # --- CLIP Models (using Transformers) ---
    if "clip" in model_name.lower():
        if CLIPModel is None:
             raise ImportError("transformers library is required for CLIP models.")
        try:
            model = CLIPModel.from_pretrained(model_name).to(device).eval()
            processor = CLIPProcessor.from_pretrained(model_name)
            # Determine embedding dimension based on common CLIP variants
            if hasattr(model.config, 'projection_dim'):
                 embedding_dim = model.config.projection_dim # Preferred way
            elif "large" in model_name.lower(): embedding_dim = 768
            elif "base" in model_name.lower(): embedding_dim = 512
            else:
                 print(f"Warning: Unknown CLIP variant '{model_name}'. Trying to get dim from model, else defaulting 512.")
                 try: embedding_dim = model.text_projection.shape[1] # Heuristic
                 except AttributeError: embedding_dim = 512

            model_type_str = "clip"
            print(f"Loaded CLIP model: {model_name} (Dim: {embedding_dim})")

        except Exception as e:
            print(f"Error loading CLIP model {model_name}: {e}")
            raise ValueError(f"Failed to load CLIP model: {model_name}") from e

    # --- DINOv2 Models (Placeholder - Requires Implementation) ---
    elif "dinov2" in model_name.lower():
        # Example: model_name = 'facebook/dinov2-base' (HuggingFace Transformers)
        # Example: model_name = 'dinov2_vitb14' (timm)
        model_type_str = "dino"
        print(f"DINOv2 model loading ({model_name}) is currently a placeholder.")
        print("This section needs implementation using appropriate libraries (e.g., transformers, timm, torchvision).")

        # --- Using HuggingFace Transformers (Recommended if available) ---
        try:
             from transformers import AutoImageProcessor, AutoModel
             processor = AutoImageProcessor.from_pretrained(model_name)
             model = AutoModel.from_pretrained(model_name).to(device).eval()
             # DINOv2 embedding dimension often in config.hidden_size
             if hasattr(model.config, 'hidden_size'):
                  embedding_dim = model.config.hidden_size
             else: # Fallback based on common names
                  if "base" in model_name: embedding_dim = 768
                  elif "small" in model_name: embedding_dim = 384
                  elif "large" in model_name: embedding_dim = 1024
                  elif "giant" in model_name: embedding_dim = 1536
                  else: raise ValueError(f"Cannot determine embedding dimension for DINOv2 model: {model_name}")
             print(f"Loaded DINOv2 model via Transformers: {model_name} (Dim: {embedding_dim})")

        except ImportError:
             raise ImportError("transformers library is required for loading DINOv2 models this way.")
        except Exception as e:
             print(f"Error loading DINOv2 model {model_name} via Transformers: {e}")
             raise NotImplementedError(f"DINOv2 loading for {model_name} not fully implemented or failed.")


        # --- Using TIMM (Alternative - Requires timm library) ---
        # except ImportError: # Fallback or alternative if Transformers method fails/not preferred
        #     if timm is None: raise ImportError("timm library required for loading DINOv2 via timm.")
        #     # Requires model names like 'dinov2_vits14', 'dinov2_vitb14', etc.
        #     model = timm.create_model(model_name, pretrained=True).to(device).eval()
        #     # Need to define TIMM-specific transforms
        #     data_config = timm.data.resolve_model_data_config(model)
        #     processor = timm.data.create_transform(**data_config, is_training=False)
        #     embedding_dim = model.embed_dim # TIMM often uses 'embed_dim'
        #     print(f"Loaded DINOv2 model via TIMM: {model_name} (Dim: {embedding_dim})")
        # except Exception as e:
        #     print(f"Error loading DINOv2 model {model_name} via timm: {e}")
        #     raise NotImplementedError(f"DINOv2 loading for {model_name} via timm failed.")

    # --- Add other model types here ---
    # elif "resnet" in model_name.lower():
    #     model_type_str = "resnet"
    #     # ... implementation ...

    else:
        raise ValueError(f"Model name not recognized or supported yet: {model_name}")

    if model is None or processor is None or model_type_str is None or embedding_dim is None:
         raise RuntimeError(f"Failed to initialize components for model: {model_name}")

    return model, processor, model_type_str, embedding_dim


# --- Helper to find sibling keyframes ---
def find_sibling_keyframes(representative_rel_path, media_base_dir):
    """
    Given a representative keyframe path (e.g., ..._50pct.jpg from a 'multi' strategy),
    finds paths for potential siblings (25pct, 75pct) based on naming convention.
    Handles 'midpoint' strategy by returning just the one path.

    Returns a list of *absolute* paths of existing keyframe files found.
    Returns empty list if the representative file itself doesn't exist or on error.
    """
    sibling_abs_paths = []
    representative_abs_path = os.path.join(media_base_dir, representative_rel_path)

    if not os.path.isfile(representative_abs_path):
        print(f"Warning: Representative keyframe not found: {representative_abs_path}")
        return [] # Can't find siblings if representative is missing

    try:
        base_name = os.path.basename(representative_rel_path)
        dir_name = os.path.dirname(representative_rel_path) # Relative directory path

        # Regex to capture the base part and the frame tag (pct or mid)
        # Example: Landline_scene_123_frame_50pct.jpg -> ('Landline_scene_123_frame_', '50pct', '.jpg')
        # Example: Landline_scene_123_frame_mid.jpg -> ('Landline_scene_123_frame_', 'mid', '.jpg')
        match = re.match(r'^(.*_frame_)(\d+pct|mid)(\.[^.]+)$', base_name)

        if not match:
            # If it doesn't match the expected pattern, assume it's a single frame strategy
            # or an unknown format. Just return the representative path.
            # print(f"Debug: Filename '{base_name}' doesn't match multi-frame pattern. Using only representative.")
            return [representative_abs_path]

        base_prefix = match.group(1) # "Landline_scene_123_frame_"
        current_tag = match.group(2) # "50pct" or "mid"
        extension = match.group(3)   # ".jpg"

        # If the current tag is 'mid', it's likely from 'midpoint' strategy, only return itself.
        if current_tag == 'mid':
            return [representative_abs_path]

        # If the tag is like '50pct', assume 'multi' strategy and look for siblings.
        tags_to_check = ["25pct", "50pct", "75pct"]
        for tag in tags_to_check:
            sibling_filename = f"{base_prefix}{tag}{extension}"
            sibling_rel_path = os.path.join(dir_name, sibling_filename)
            sibling_abs_path = os.path.join(media_base_dir, sibling_rel_path)
            if os.path.isfile(sibling_abs_path):
                sibling_abs_paths.append(sibling_abs_path)
            # else: # Optional: Log if a sibling is missing
                # print(f"Debug: Sibling keyframe not found: {sibling_abs_path}")

        if not sibling_abs_paths:
             # This shouldn't happen if representative exists and matched pattern, but safety check.
             print(f"Warning: Found no keyframe files matching pattern {base_prefix}* in dir relative path {dir_name}. Returning only representative.")
             return [representative_abs_path] # Fallback

        return sorted(list(set(sibling_abs_paths))) # Return unique, sorted paths

    except Exception as e:
        print(f"Error finding sibling keyframes for {representative_rel_path}: {e}")
        traceback.print_exc()
        return [] # Return empty on error


# --- Core Embedding Generation Logic ---
async def generate_embeddings_batch(model, processor, model_type, batch_data, device, generation_strategy, media_base_dir):
    """
    Generates embeddings for a batch, handling single or multiple frames per clip.

    Args:
        model: The loaded embedding model.
        processor: The processor or transforms for the model.
        model_type (str): 'clip', 'dino', etc.
        batch_data (list): List of tuples (clip_id, representative_keyframe_rel_path).
        device: The torch device.
        generation_strategy (str): The strategy identifier (e.g., 'keyframe_midpoint', 'keyframe_multi_avg').
        media_base_dir (str): Absolute path to the media base directory.

    Returns:
        list: List of tuples (clip_id, embedding_list). Returns empty list on major errors.
    """
    embeddings_out = []
    all_images_to_process = [] # Stores tuples: (clip_id, abs_path)
    clip_id_to_indices = {}    # Maps clip_id to list of indices in all_images_to_process

    # 1. Determine required images for each clip and flatten the list
    # print(f"  Preprocessing batch: Identifying required keyframes ({generation_strategy})...") # Optional verbose log
    skipped_clips_count = 0
    required_paths_count = 0
    for clip_id, repr_rel_path in batch_data:
        if not repr_rel_path: # Should be caught by DB query, but safety check
             print(f"  Warning: Clip ID {clip_id} has null keyframe path. Skipping.")
             skipped_clips_count += 1
             continue

        abs_paths_for_clip = [] # Initialize list of paths for this clip

        # --- Determine if strategy requires multiple frames ---
        # List strategies that explicitly need multiple frames (e.g., for averaging)
        is_multi_frame_strategy = generation_strategy in ['keyframe_multi_avg']

        # --- Get image paths based on strategy ---
        if is_multi_frame_strategy:
            # Strategy requires multiple frames: Find siblings based on representative path
            # find_sibling_keyframes expects the multi-frame naming convention (_25pct, etc.)
            abs_paths_for_clip = find_sibling_keyframes(repr_rel_path, media_base_dir)
            # Validate if enough frames were found for this strategy
            if not abs_paths_for_clip:
                print(f"  Warning: No valid keyframes found for multi-strategy Clip ID {clip_id} (repr: {repr_rel_path}). Skipping.")
                skipped_clips_count += 1
                continue
            # Example: 'keyframe_multi_avg' might require exactly 3 frames
            if generation_strategy == 'keyframe_multi_avg' and len(abs_paths_for_clip) < 3:
                print(f"  Warning: Strategy '{generation_strategy}' expects 3 keyframes for Clip ID {clip_id}, but found {len(abs_paths_for_clip)}. Skipping.")
                skipped_clips_count += 1
                continue
            # Add elif for other multi-frame strategies with different requirements
        else:
            # Strategy requires a single frame: ONLY use the representative path from DB
            abs_path = os.path.join(media_base_dir, repr_rel_path)
            if os.path.isfile(abs_path):
                abs_paths_for_clip = [abs_path] # Use *only* this path
            else:
                # If the single required frame is missing, skip the clip
                print(f"  Warning: Representative keyframe not found for single-strategy Clip ID {clip_id}: {abs_path}. Skipping.")
                skipped_clips_count += 1
                continue
        # --- End image path gathering ---


        # If we still don't have paths after the logic above, skip
        if not abs_paths_for_clip:
            # This case should be covered by inner logic, but as a safeguard
            if clip_id not in {c[0] for c in all_images_to_process}: # Avoid double counting skips
                 print(f"  Warning: No paths determined for Clip ID {clip_id}. Skipping.")
                 skipped_clips_count += 1
            continue

        # Add paths to the flat list and record indices for this clip_id
        start_index = len(all_images_to_process)
        for abs_path in abs_paths_for_clip:
            all_images_to_process.append((clip_id, abs_path))
        clip_id_to_indices[clip_id] = list(range(start_index, start_index + len(abs_paths_for_clip)))
        required_paths_count += len(abs_paths_for_clip)

    # End loop through batch_data

    if not all_images_to_process:
        # print("  Batch Preprocessing: No valid images found to process in this batch.") # Optional verbose log
        return []

    # print(f"  Preprocessing batch: Identified {required_paths_count} total images for {len(clip_id_to_indices)} clips ({skipped_clips_count} clips skipped).") # Optional verbose log

    # 2. Load images and perform model inference
    # print(f"  Processing {len(all_images_to_process)} images with {model_type} model...") # Optional verbose log
    all_features_np = None
    try:
        # Load all images first
        images = [Image.open(item[1]).convert("RGB") for item in all_images_to_process]

        # Perform inference
        with torch.no_grad():
            if model_type == "clip":
                inputs = processor(text=None, images=images, return_tensors="pt", padding=True)
                inputs = {k: v.to(device) for k, v in inputs.items()}
                image_features = model.get_image_features(**inputs)
                all_features_np = image_features.cpu().numpy()

            elif model_type == "dino":
                 inputs = processor(images=images, return_tensors="pt").to(device)
                 outputs = model(**inputs)
                 last_hidden_states = outputs.last_hidden_state
                 # Mean pool - works generally well for DINOv2 base/small/large
                 image_features = last_hidden_states.mean(dim=1)
                 all_features_np = image_features.cpu().numpy()

            else:
                print(f"ERROR: Embedding generation logic not implemented for model type: {model_type}")
                return [] # Fatal error for this batch

    except FileNotFoundError as e:
        print(f"\nError: Image file not found during batch loading: {e}. Skipping entire batch.")
        # Log which file was missing if possible from `e`
        traceback.print_exc()
        return []
    except Exception as e:
        print(f"\nError during model inference for batch: {e}")
        traceback.print_exc()
        return []

    if all_features_np is None:
        print("Error: Feature extraction resulted in None. Skipping batch.")
        return []

    # print(f"  Inference complete. Aggregating embeddings...") # Optional verbose log

    # 3. Aggregate features based on strategy and clip_id
    processed_clip_ids = set(clip_id_to_indices.keys())
    original_batch_clip_ids = [item[0] for item in batch_data] # Maintain original order

    for clip_id in original_batch_clip_ids:
        if clip_id not in processed_clip_ids:
            continue # Skip clips that were filtered out during path gathering

        indices = clip_id_to_indices.get(clip_id)
        if not indices:
             print(f"  Internal Error: No indices found for processed clip ID {clip_id}. Skipping.")
             continue

        clip_features_np = all_features_np[indices]

        final_embedding = None
        if clip_features_np.shape[0] == 0:
             print(f"  Warning: No features extracted for clip ID {clip_id} despite processing. Skipping.")
             continue

        # --- Aggregation Logic ---
        if generation_strategy == 'keyframe_multi_avg':
            if clip_features_np.shape[0] >= 1: # Need at least one frame to average
                 final_embedding = np.mean(clip_features_np, axis=0)
                 # Optional: L2 normalize
                 # norm = np.linalg.norm(final_embedding)
                 # if norm > 1e-6: final_embedding = final_embedding / norm
            else: # Should be caught earlier, but safety check
                 print(f"  Warning: No features available to average for clip ID {clip_id}. Skipping.")
                 continue

        # Add elif for other multi-frame strategies
        # elif generation_strategy == 'keyframe_multi_max':
        #    final_embedding = np.max(clip_features_np, axis=0)

        else: # Default: Assume single frame strategy
            if clip_features_np.shape[0] > 1:
                # This warning should NOT appear with the refined logic above for single strategies
                print(f"  Internal Warning: Single frame strategy '{generation_strategy}' got {clip_features_np.shape[0]} features for clip {clip_id}. Using first.")
            # Take the first (and should be only) feature vector
            final_embedding = clip_features_np[0]


        if final_embedding is not None:
            embeddings_out.append((clip_id, final_embedding.tolist()))

    # print(f"  Aggregation complete. Returning {len(embeddings_out)} embeddings.") # Optional verbose log
    return embeddings_out

# --- Main Execution (Now Async) ---
async def main(): # <--- Make main async
    parser = argparse.ArgumentParser(description="Generate image embeddings using various models and strategies, storing results in PostgreSQL/pgvector.")
    parser.add_argument("--model_name", required=True, help="Identifier for the embedding model (e.g., 'openai/clip-vit-base-patch32', 'facebook/dinov2-base').")
    parser.add_argument("--generation_strategy", required=True,
                        help="Label describing how the embedding relates to keyframes (e.g., 'keyframe_midpoint', 'keyframe_multi_avg'). Used in DB.")
    parser.add_argument("--source-identifier", default=None,
                        help="Optional: Process only clips belonging to this source_identifier.")
    parser.add_argument("--batch_size", type=int, default=16,
                        help="Number of *clips* to process in each database query batch.")
    parser.add_argument("--limit", type=int, default=None, help="Limit the number of clips fetched from the database.")
    parser.add_argument("--overwrite", action="store_true",
                        help="Generate and attempt to insert embeddings even if an entry already exists for this clip/model/strategy combo.")

    args = parser.parse_args()

    print(f"--- Embedding Generation ---")
    print(f"Model: {args.model_name}")
    print(f"Strategy: {args.generation_strategy}")
    print(f"Source Filter: {args.source_identifier or 'ALL'}")
    print(f"Batch Size (Clips): {args.batch_size}")
    print(f"Overwrite Existing: {args.overwrite}")
    print(f"Limit: {args.limit or 'None'}")
    print("----------------------------")

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    print(f"Using device: {device}")

    # Initialize variables outside try/finally for summary report
    conn = None
    model = processor = model_type = None
    embedding_dim = -1
    num_clips_found = 0
    total_embeddings_generated = 0
    total_inserted_count = 0
    total_skipped_existing_count = 0
    total_error_count = 0

    try:
        print(f"Loading model and processor: {args.model_name}...")
        # Model loading is typically synchronous CPU/GPU bound work
        model, processor, model_type, embedding_dim = get_model_and_processor(args.model_name, device=device)
        print(f"Model '{args.model_name}' loaded successfully. Type: {model_type}, Dim: {embedding_dim}")

        # --- Database Connection (Assuming synchronous db_utils/psycopg2) ---
        # If get_db_connection was async (using asyncpg), you would 'await' it here.
        conn = get_db_connection()
        media_base_dir = get_media_base_dir()
        print("Database connection established.")
        # -----------------------------------------------------------------

        # Using a context manager for the cursor is good practice
        with conn.cursor() as cur:
            # --- Build Query to Fetch Clips ---
            print("Building query to fetch clips...")
            params = []
            select_clause = sql.SQL("SELECT c.id, c.keyframe_filepath FROM clips c")
            join_clauses = []
            where_conditions = [
                sql.SQL("c.keyframe_filepath IS NOT NULL"),
                sql.SQL("c.keyframe_filepath != ''")
            ]

            if args.source_identifier:
                join_clauses.append(sql.SQL("JOIN source_videos sv ON c.source_video_id = sv.id"))
                where_conditions.append(sql.SQL("sv.source_identifier = %s"))
                params.append(args.source_identifier)

            if not args.overwrite:
                join_clauses.append(sql.SQL(
                    """LEFT JOIN embeddings e ON c.id = e.clip_id
                       AND e.model_name = %s AND e.generation_strategy = %s"""
                ))
                where_conditions.append(sql.SQL("e.id IS NULL"))
                params.extend([args.model_name, args.generation_strategy])
                print("Query will skip clips already having embeddings for this model/strategy.")
            else:
                print("Query will fetch all matching clips; ON CONFLICT will handle existing during insert.")

            query = select_clause
            if join_clauses:
                # Add a space between SELECT clause and the first JOIN
                query += sql.SQL(" ") + sql.SQL(" ").join(join_clauses) # <-- Add space here
            if where_conditions:
                # Add WHERE keyword and join conditions
                query += sql.SQL(" WHERE ") + sql.SQL(" AND ").join(where_conditions)
            # Add ORDER BY
            query += sql.SQL(" ORDER BY c.id")
            # Add LIMIT if specified
            if args.limit:
                query += sql.SQL(" LIMIT %s")
                params.append(args.limit)

            # --- Execute Query (Synchronous) ---
            print("Fetching clips to process...")
            cur.execute(query, params)
            clips_to_process = cur.fetchall()
            num_clips_found = len(clips_to_process)

            if not clips_to_process:
                print("No clips found matching the criteria requiring embedding generation.")
                # Optional: Check existing count here if needed
                return # Exit early

            print(f"Found {num_clips_found} clips to process.")
            # ----------------------------------

            # --- Process in Batches ---
            num_batches = (num_clips_found + args.batch_size - 1) // args.batch_size
            for i in tqdm(range(0, num_clips_found, args.batch_size), desc=f"Generating Embeddings", total=num_batches):
                batch_raw_data = clips_to_process[i:i+args.batch_size]
                batch_clip_ids = {item[0] for item in batch_raw_data}

                # --- Call the ASYNC batch generation function ---
                # 'await' is valid now because main is async
                embeddings_batch = await generate_embeddings_batch(
                    model, processor, model_type, batch_raw_data, device, args.generation_strategy, media_base_dir
                )
                # ----------------------------------------------

                total_embeddings_generated += len(embeddings_batch)
                batch_error_clips = batch_clip_ids - {emb[0] for emb in embeddings_batch}
                if batch_error_clips:
                     total_error_count += len(batch_error_clips)

                if embeddings_batch:
                    # --- Prepare for Insert ---
                    insert_query = sql.SQL("""
                        INSERT INTO embeddings (clip_id, embedding, model_name, generation_strategy)
                        VALUES %s ON CONFLICT (clip_id, model_name, generation_strategy) DO NOTHING;
                    """)
                    values_to_insert = []
                    for clip_id, embedding_list in embeddings_batch:
                        if len(embedding_list) != embedding_dim:
                            print(f"\nERROR: Clip ID {clip_id} generated embedding dim ({len(embedding_list)}) != expected dim ({embedding_dim}). Skipping insert.")
                            total_error_count += 1
                            batch_error_clips.discard(clip_id) # Adjust error count if already counted
                            continue
                        embedding_str = '[' + ','.join(map(str, embedding_list)) + ']'
                        values_to_insert.append((clip_id, embedding_str, args.model_name, args.generation_strategy))

                    if not values_to_insert: continue # Skip if batch ended up empty after checks

                    # --- Insert into Database (Synchronous) ---
                    try:
                        execute_values(cur, insert_query, values_to_insert, page_size=len(values_to_insert))
                        conn.commit() # Commit after successful batch insert
                        batch_inserted_actual = cur.rowcount
                        total_inserted_count += batch_inserted_actual
                        if args.overwrite:
                             total_skipped_existing_count += (len(values_to_insert) - batch_inserted_actual)
                    except psycopg2.Error as db_err:
                        conn.rollback()
                        print(f"\nError inserting batch into database: {db_err}")
                        traceback.print_exc()
                        total_error_count += len(values_to_insert) # Count all intended in failed batch
                        # Remove clips from this failed insert batch from the general batch_error_clips count
                        error_clips_in_failed_insert = {val[0] for val in values_to_insert}
                        batch_error_clips -= error_clips_in_failed_insert
                    except Exception as gen_err:
                        conn.rollback()
                        print(f"\nError preparing/executing insert for batch: {gen_err}")
                        traceback.print_exc()
                        total_error_count += len(values_to_insert)
                        error_clips_in_failed_insert = {val[0] for val in values_to_insert}
                        batch_error_clips -= error_clips_in_failed_insert
                    # -----------------------------------------
            # --- End Batch Loop ---
        # --- End Cursor Context ---

    except (ValueError, psycopg2.Error, ImportError, NotImplementedError, RuntimeError, Exception) as e:
        print(f"\nA critical error occurred: {e}")
        traceback.print_exc()
        # No explicit rollback needed here if conn object wasn't successfully created
        # or if using 'with conn:' which handles rollback/close on error (if conn is context manager)
        total_error_count += 1
    finally:
        if conn: # Close synchronous connection if it was opened
            conn.close()
            print("\nDatabase connection closed.")

        # --- Print Summary ---
        print(f"\n--- Summary ---")
        print(f"Model: {args.model_name} ({model_type}, Dim: {embedding_dim if embedding_dim > 0 else 'N/A'})")
        print(f"Strategy: {args.generation_strategy}")
        print(f"Source Filter: {args.source_identifier or 'ALL'}")
        print(f"Clips Fetched from DB: {num_clips_found}")
        print(f"Embeddings Successfully Inserted/Updated: {total_inserted_count}")
        if args.overwrite:
            print(f"Skipped Insert (Already Existed): {total_skipped_existing_count}")
        print(f"Clips Failed (Errors/Skipped): {total_error_count}")
        print("---------------")


# --- Run the async main function ---
if __name__ == "__main__":
     # Use asyncio.run() to execute the async main function
     asyncio.run(main())
import torch
from PIL import Image
import os
import argparse
import numpy as np
from tqdm import tqdm
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
import re
import traceback

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

from db_utils import get_db_connection, get_media_base_dir

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
def generate_embeddings_batch(model, processor, model_type, batch_data, device, generation_strategy, media_base_dir):
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
    print(f"  Preprocessing batch: Identifying required keyframes ({generation_strategy})...")
    skipped_clips_count = 0
    required_paths_count = 0
    for clip_id, repr_rel_path in batch_data:
        if not repr_rel_path: # Should be caught by DB query, but safety check
             print(f"  Warning: Clip ID {clip_id} has null keyframe path. Skipping.")
             skipped_clips_count += 1
             continue

        # Find all necessary absolute keyframe paths for this clip based on strategy
        # The representative path itself tells find_sibling_keyframes if it's midpoint or multi implicitly
        abs_paths_for_clip = find_sibling_keyframes(repr_rel_path, media_base_dir)

        # --- Strategy-specific validation ---
        is_multi_frame_strategy = generation_strategy in ['keyframe_multi_avg'] # Add future multi-frame strategies here

        if not abs_paths_for_clip:
            print(f"  Warning: No valid keyframes found for Clip ID {clip_id} (repr: {repr_rel_path}). Skipping.")
            skipped_clips_count += 1
            continue

        if is_multi_frame_strategy and len(abs_paths_for_clip) < 3:
            # If strategy expects multiple frames (like avg), but we didn't find enough (e.g., only 1 or 2)
            print(f"  Warning: Strategy '{generation_strategy}' expects multiple keyframes for Clip ID {clip_id}, but found only {len(abs_paths_for_clip)} ({[os.path.basename(p) for p in abs_paths_for_clip]}). Skipping.")
            skipped_clips_count += 1
            continue
        elif not is_multi_frame_strategy and len(abs_paths_for_clip) > 1:
            # If strategy expects one frame (like midpoint), but we found multiple (shouldn't happen with find_sibling_keyframes logic, but check)
             print(f"  Warning: Strategy '{generation_strategy}' expects single keyframe for Clip ID {clip_id}, but found {len(abs_paths_for_clip)}. Using first: {os.path.basename(abs_paths_for_clip[0])}.")
             abs_paths_for_clip = [abs_paths_for_clip[0]] # Use only the first one found

        # Add paths to the flat list and record indices for this clip_id
        start_index = len(all_images_to_process)
        for abs_path in abs_paths_for_clip:
            all_images_to_process.append((clip_id, abs_path))
        clip_id_to_indices[clip_id] = list(range(start_index, start_index + len(abs_paths_for_clip)))
        required_paths_count += len(abs_paths_for_clip)

    if not all_images_to_process:
        print("  Batch Preprocessing: No valid images found to process in this batch.")
        return []

    print(f"  Preprocessing batch: Identified {required_paths_count} total images for {len(clip_id_to_indices)} clips ({skipped_clips_count} clips skipped).")

    # 2. Load images and perform model inference
    print(f"  Processing {len(all_images_to_process)} images with {model_type} model...")
    all_features_np = None
    try:
        # Load all images first to potentially catch file errors early
        images = [Image.open(item[1]).convert("RGB") for item in all_images_to_process]

        # Perform inference in batches if necessary (though often model processors handle internal batching)
        # For simplicity here, assume the processor/model handles the full list.
        # If OOM occurs, implement manual batching over `images` list here.
        with torch.no_grad():
            if model_type == "clip":
                inputs = processor(text=None, images=images, return_tensors="pt", padding=True)
                inputs = {k: v.to(device) for k, v in inputs.items()}
                image_features = model.get_image_features(**inputs)
                all_features_np = image_features.cpu().numpy()

            elif model_type == "dino":
                 # Using HuggingFace Transformers AutoImageProcessor/AutoModel
                 inputs = processor(images=images, return_tensors="pt").to(device)
                 outputs = model(**inputs)
                 # DINOv2 embedding is often the pooler_output or last_hidden_state[:, 0] (CLS token)
                 # Check model documentation/config. Using last_hidden_state mean pooling as robust default.
                 last_hidden_states = outputs.last_hidden_state
                 # Mean pool across sequence length dimension (dim 1)
                 image_features = last_hidden_states.mean(dim=1)
                 # Alternative: CLS token: image_features = last_hidden_states[:, 0]
                 all_features_np = image_features.cpu().numpy()

                 # --- Using TIMM DINOv2 (Alternative) ---
                 # inputs = torch.stack([processor(img) for img in images]).to(device)
                 # image_features = model.forward_features(inputs) # Get features before head
                 # # DINOv2 often uses CLS token embedding from the features
                 # if isinstance(image_features, list): image_features = image_features[-1] # Get last layer if list
                 # cls_token_features = image_features[:, 0] # Assuming CLS token is first
                 # all_features_np = cls_token_features.cpu().numpy()

            # Add elif for other model types
            else:
                print(f"ERROR: Embedding generation logic not implemented for model type: {model_type}")
                return [] # Fatal error for this batch

    except FileNotFoundError as e:
        print(f"\nError: Image file not found during batch loading: {e}. Skipping entire batch.")
        return [] # Skip entire batch if a file is missing during loading
    except Exception as e:
        print(f"\nError during model inference for batch: {e}")
        traceback.print_exc()
        return [] # Skip entire batch on inference error

    if all_features_np is None:
        print("Error: Feature extraction resulted in None. Skipping batch.")
        return []

    print(f"  Inference complete. Aggregating embeddings...")

    # 3. Aggregate features based on strategy and clip_id
    processed_clip_ids = set(clip_id_to_indices.keys()) # Clips we actually processed images for
    for clip_id in [item[0] for item in batch_data]: # Iterate original clip IDs to maintain order/completeness
        if clip_id not in processed_clip_ids:
            continue # Skip clips that were filtered out earlier

        indices = clip_id_to_indices[clip_id]
        if not indices: # Should not happen if clip_id is in processed_clip_ids
             print(f"  Internal Error: No indices found for processed clip ID {clip_id}. Skipping.")
             continue

        clip_features_np = all_features_np[indices] # Get rows corresponding to this clip's images

        final_embedding = None
        if clip_features_np.shape[0] == 0:
             print(f"  Warning: No features extracted for clip ID {clip_id} despite processing. Skipping.")
             continue

        # --- Aggregation Logic ---
        if generation_strategy == 'keyframe_multi_avg':
            # Average the embeddings for the multiple frames
            final_embedding = np.mean(clip_features_np, axis=0)
            # Optional: L2 normalize the averaged embedding
            # norm = np.linalg.norm(final_embedding)
            # if norm > 1e-6: final_embedding = final_embedding / norm

        # Add elif for other multi-frame strategies (e.g., max pooling, concatenation + projection)
        # elif generation_strategy == 'keyframe_multi_max':
        #    final_embedding = np.max(clip_features_np, axis=0)

        else: # Default: Assume single frame strategy (midpoint, or unknown treated as single)
            if clip_features_np.shape[0] > 1:
                print(f"  Warning: Strategy '{generation_strategy}' implies single frame, but got {clip_features_np.shape[0]} features for clip {clip_id}. Using first.")
            final_embedding = clip_features_np[0] # Take the first (and likely only) feature vector


        if final_embedding is not None:
            embeddings_out.append((clip_id, final_embedding.tolist()))

    print(f"  Aggregation complete. Returning {len(embeddings_out)} embeddings.")
    return embeddings_out


# --- Main Execution ---
def main():
    parser = argparse.ArgumentParser(description="Generate image embeddings using various models and strategies, storing results in PostgreSQL/pgvector.")
    parser.add_argument("--model_name", required=True, help="Identifier for the embedding model (e.g., 'openai/clip-vit-base-patch32', 'facebook/dinov2-base', 'your_custom_resnet').")
    parser.add_argument("--generation_strategy", required=True,
                        help="Label describing how the embedding relates to keyframes (e.g., 'keyframe_midpoint', 'keyframe_multi_avg'). Used in DB.")
    parser.add_argument("--source-identifier", default=None,
                        help="Optional: Process only clips belonging to this source_identifier.")
    parser.add_argument("--batch_size", type=int, default=16,
                        help="Number of *clips* to process in each database query batch. Actual images processed depends on strategy.")
    parser.add_argument("--limit", type=int, default=None, help="Limit the number of clips fetched from the database.")
    parser.add_argument("--overwrite", action="store_true",
                        help="Generate and attempt to insert embeddings even if an entry already exists for the *exact* same clip_id, model_name, and generation_strategy combination. Uses ON CONFLICT DO NOTHING.")

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

    conn = None
    model = processor = model_type = None
    embedding_dim = -1
    total_clips_fetched = 0
    total_embeddings_generated = 0
    total_inserted_count = 0
    total_skipped_existing_count = 0 # Counted only if overwrite is True
    total_error_count = 0 # Count clips that failed processing/insertion

    try:
        print(f"Loading model and processor: {args.model_name}...")
        model, processor, model_type, embedding_dim = get_model_and_processor(args.model_name, device=device)
        print(f"Model '{args.model_name}' loaded successfully. Type: {model_type}, Dim: {embedding_dim}")

        conn = get_db_connection()
        media_base_dir = get_media_base_dir()
        print("Database connection established.")

        with conn.cursor() as cur:
            # --- Build Query to Fetch Clips ---
            print("Building query to fetch clips...")
            params = [] # Initialize params as an empty list
            select_clause = sql.SQL("""
                SELECT c.id, c.keyframe_filepath
                FROM clips c
            """)
            join_clauses = []
            # We must have a representative keyframe path
            where_conditions = [
                sql.SQL("c.keyframe_filepath IS NOT NULL"),
                sql.SQL("c.keyframe_filepath != ''")
            ]

            # Optional: Check if the vector column exists and matches dimension
            # This adds complexity but can prevent errors later. Requires knowing the table structure.
            # Note: pgvector index creation usually enforces dimension, but good check.
            # where_conditions.append(sql.SQL("vector_dims(e.embedding) = %s")) # Assuming 'e.embedding' is the vector column

            # Add source filtering if requested
            if args.source_identifier:
                join_clauses.append(sql.SQL("JOIN source_videos sv ON c.source_video_id = sv.id"))
                where_conditions.append(sql.SQL("sv.source_identifier = %s"))
                params.append(args.source_identifier)

            # Add filtering based on existing embeddings if not overwriting
            if not args.overwrite:
                join_clauses.append(sql.SQL("""
                    LEFT JOIN embeddings e ON c.id = e.clip_id
                                         AND e.model_name = %s
                                         AND e.generation_strategy = %s
                """))
                where_conditions.append(sql.SQL("e.id IS NULL"))
                params.extend([args.model_name, args.generation_strategy])
                print("Query will skip clips already having embeddings for this model/strategy.")
            else:
                print("Query will fetch all matching clips; ON CONFLICT will handle existing during insert.")

            # Assemble the final query
            query = select_clause
            if join_clauses: query += sql.SQL(" ").join(join_clauses)
            if where_conditions: query += sql.SQL(" WHERE ") + sql.SQL(" AND ").join(where_conditions)
            query += sql.SQL(" ORDER BY c.id") # Consistent order is good practice
            if args.limit:
                query += sql.SQL(" LIMIT %s")
                params.append(args.limit)

            # --- Execute Query ---
            print("Fetching clips to process...")
            # print(f"DEBUG Query: {cur.mogrify(query, params).decode()}") # Uncomment for debugging query
            cur.execute(query, params)
            clips_to_process = cur.fetchall() # List of (clip_id, keyframe_filepath)
            total_clips_fetched = len(clips_to_process)

            if not clips_to_process:
                print("No clips found matching the criteria requiring embedding generation.")
                return # Exit early if nothing to do

            print(f"Found {total_clips_fetched} clips to process.")

            # --- Process in Batches ---
            num_batches = (total_clips_fetched + args.batch_size - 1) // args.batch_size
            for i in tqdm(range(0, total_clips_fetched, args.batch_size), desc=f"Generating Embeddings", total=num_batches):
                batch_raw_data = clips_to_process[i:i+args.batch_size]
                batch_clip_ids = {item[0] for item in batch_raw_data} # Set of clip IDs in this batch

                # --- Call the batch generation function ---
                embeddings_batch = generate_embeddings_batch(
                    model, processor, model_type, batch_raw_data, device, args.generation_strategy, media_base_dir
                )
                total_embeddings_generated += len(embeddings_batch)

                batch_error_clips = batch_clip_ids - {emb[0] for emb in embeddings_batch}
                if batch_error_clips:
                     total_error_count += len(batch_error_clips)
                     # print(f"\nNote: Failed to generate embeddings for {len(batch_error_clips)} clips in batch {i//args.batch_size + 1}.") # Verbose logging


                if embeddings_batch:
                    # --- Insert into Database ---
                    insert_query = sql.SQL("""
                        INSERT INTO embeddings (clip_id, embedding, model_name, generation_strategy)
                        VALUES %s
                        ON CONFLICT (clip_id, model_name, generation_strategy) DO NOTHING;
                    """)
                    # Prepare data: (clip_id, embedding_vector_as_string, model_name, strategy)
                    # Ensure embedding list is converted to string '[1.0,2.0,...]' format for pgvector
                    values_to_insert = []
                    for clip_id, embedding_list in embeddings_batch:
                        if len(embedding_list) != embedding_dim:
                            print(f"\nFATAL ERROR: Clip ID {clip_id} generated embedding dim ({len(embedding_list)}) != expected dim ({embedding_dim}). Skipping insert for this item.")
                            total_error_count += 1
                            continue # Skip this specific embedding
                        # Simple conversion to string format pgvector expects
                        embedding_str = '[' + ','.join(map(str, embedding_list)) + ']'
                        values_to_insert.append((clip_id, embedding_str, args.model_name, args.generation_strategy))

                    if not values_to_insert:
                         print(f"  Skipping database insert for batch {i//args.batch_size + 1} due to dimension errors or no valid embeddings.")
                         continue

                    try:
                        # ***** FIX HERE: Remove fetch=True *****
                        execute_values(cur, insert_query, values_to_insert, page_size=len(values_to_insert))
                        
                        # execute_values doesn't reliably return rows or rowcount in all versions/adapters.
                        # Rely on cur.rowcount *after* the execute_values call.
                        conn.commit() # Commit after each successful batch insertion
                        batch_inserted_actual = cur.rowcount # Get rows affected by the *last* command (INSERT)
                        total_inserted_count += batch_inserted_actual

                        if args.overwrite:
                             skipped_in_batch = len(values_to_insert) - batch_inserted_actual
                             total_skipped_existing_count += skipped_in_batch
                             # if skipped_in_batch > 0: print(f"  Skipped {skipped_in_batch} existing embeddings in batch.") # Verbose logging

                    except psycopg2.Error as db_err:
                        conn.rollback() # Rollback the failed batch
                        print(f"\nError inserting batch into database: {db_err}")
                        traceback.print_exc()
                        total_error_count += len(values_to_insert) # Count all attempted in batch as errors if commit failed
                    except Exception as gen_err:
                        conn.rollback()
                        print(f"\nError preparing/executing insert for batch: {gen_err}")
                        total_error_count += len(values_to_insert)


    except (ValueError, psycopg2.Error, ImportError, NotImplementedError, RuntimeError, Exception) as e:
        print(f"\nA critical error occurred: {e}")
        traceback.print_exc()
        if conn: conn.rollback()
        total_error_count += 1 # Count critical error itself
    finally:
        if conn:
            conn.close()
            print("\nDatabase connection closed.")

        print(f"\n--- Summary ---")
        print(f"Model: {args.model_name} ({model_type}, Dim: {embedding_dim if embedding_dim > 0 else 'N/A'})")
        print(f"Strategy: {args.generation_strategy}")
        print(f"Source Filter: {args.source_identifier or 'ALL'}")
        print(f"Clips Fetched from DB: {total_clips_fetched}")
        # print(f"Embeddings Generated (attempted): {total_embeddings_generated}") # Can be confusing if errors occur
        print(f"Embeddings Successfully Inserted/Updated: {total_inserted_count}")
        if args.overwrite:
            print(f"Skipped Insert (Already Existed): {total_skipped_existing_count}")
        print(f"Clips Failed (Errors/Skipped): {total_error_count}")
        print("---------------")

if __name__ == "__main__":
    main()
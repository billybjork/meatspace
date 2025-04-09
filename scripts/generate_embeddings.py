import torch
from PIL import Image
import os
import argparse
import glob
import numpy as np
from transformers import CLIPProcessor, CLIPModel
from tqdm import tqdm
import psycopg2
from psycopg2.extras import execute_values
from psycopg2 import sql

from db_utils import get_db_connection, get_media_base_dir

def get_model_and_processor(model_name="openai/clip-vit-base-patch32"):
    """Loads the specified model and processor."""
    try:
        # Determine embedding dimension (adjust as needed for models)
        if "clip-vit-base-patch32" in model_name:
            embedding_dim = 512
        elif "clip-vit-large-patch14" in model_name:
            embedding_dim = 768
        # Add dimensions for other models (e.g., DINOv2)
        # elif "dinov2" in model_name:
        #     embedding_dim = 768 # or 1024 for ViT-L/14
        else:
            # Attempt to load config to find dim, or set a default/raise error
            print(f"Warning: Unknown embedding dimension for {model_name}. Defaulting to 512. Update this function if needed.")
            embedding_dim = 512

        if "clip" in model_name.lower():
            model = CLIPModel.from_pretrained(model_name)
            processor = CLIPProcessor.from_pretrained(model_name)
            print(f"Loaded CLIP model: {model_name}")
            return model, processor, "clip", embedding_dim
        # elif "dino" in model_name.lower():
        #    from transformers import AutoImageProcessor, AutoModel
        #    processor = AutoImageProcessor.from_pretrained(model_name)
        #    model = AutoModel.from_pretrained(model_name)
        #    print(f"Loaded DINO model: {model_name}")
        #    return model, processor, "dino", embedding_dim
        else:
            raise ValueError(f"Model type not recognized or supported yet: {model_name}")

    except Exception as e:
        print(f"Error loading model {model_name}: {e}")
        raise

# --- Embedding Generation ---
def generate_embeddings_batch(model, processor, model_type, batch_data, device):
    """
    Generates embeddings for a batch of image paths.

    Args:
        model: The loaded embedding model.
        processor: The model's processor.
        model_type: String identifier ('clip', 'dino', etc.).
        batch_data (list): List of tuples: [(clip_id, abs_keyframe_path), ...].
        device: The torch device ('cuda' or 'cpu').

    Returns:
        list: List of tuples: [(clip_id, numpy_embedding_array), ...]. Returns empty list on error.
    """
    batch_clip_ids = [item[0] for item in batch_data]
    batch_paths = [item[1] for item in batch_data]
    embeddings_out = []

    try:
        images = [Image.open(p).convert("RGB") for p in batch_paths]

        # Process based on model type
        if model_type == "clip":
            inputs = processor(text=None, images=images, return_tensors="pt", padding=True)
            inputs = {k: v.to(device) for k, v in inputs.items()}
            with torch.no_grad():
                image_features = model.get_image_features(**inputs)
        # elif model_type == "dino":
            # inputs = processor(images=images, return_tensors="pt")
            # inputs = {k: v.to(device) for k, v in inputs.items()}
            # with torch.no_grad():
            #     outputs = model(**inputs)
            #     # DINOv2 often uses the CLS token embedding or average pooling
            #     image_features = outputs.last_hidden_state[:, 0] # Example: CLS token
        else:
            print(f"ERROR: Embedding generation logic not implemented for model type: {model_type}")
            return []

        # Move embeddings to CPU and convert to numpy list for psycopg2
        image_features_np = image_features.cpu().numpy()

        # Ensure embedding vectors are lists for psycopg2
        embeddings_out = [(clip_id, emb.tolist()) for clip_id, emb in zip(batch_clip_ids, image_features_np)]

    except FileNotFoundError as e:
        print(f"\nError: Image file not found during batch processing: {e}. Skipping batch.")
        return []
    except Exception as e:
        print(f"\nError processing batch starting with ID {batch_clip_ids[0]} ({batch_paths[0]}): {e}")
        print("Skipping this batch.")
        return []

    return embeddings_out

def main():
    parser = argparse.ArgumentParser(description="Generate image embeddings and store them in the database.")
    parser.add_argument("--model_name", default="openai/clip-vit-base-patch32", help="Hugging Face model identifier.")
    parser.add_argument("--generation_strategy", required=True, help="Identifier for how embeddings were made (e.g., 'keyframe_midpoint', 'keyframe_avg_3'). Should match keyframe strategy.")
    parser.add_argument("--batch_size", type=int, default=32, help="Batch size for processing images.")
    parser.add_argument("--limit", type=int, default=None, help="Limit the number of clips to process.")
    parser.add_argument("--process-all", action="store_true", help="Process all clips with keyframes, even if embedding exists (will attempt ON CONFLICT DO NOTHING).")

    args = parser.parse_args()

    # --- Device Setup ---
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    print(f"Using device: {device}")

    conn = None
    processed_count = 0
    inserted_count = 0

    try:
        # --- Load Model ---
        print(f"Loading model: {args.model_name}...")
        model, processor, model_type, embedding_dim = get_model_and_processor(args.model_name)
        model.to(device)
        model.eval()
        print("Model loaded successfully.")

        # --- Database Connection ---
        conn = get_db_connection()
        media_base_dir = get_media_base_dir()
        print("Database connection established.")

        with conn.cursor() as cur:
            # --- Query for Clips with Keyframes ---
            # Select clips that have a keyframe_filepath and optionally filter
            # out those that already have an embedding for this model/strategy.
            query = sql.SQL("""
                SELECT c.id, c.keyframe_filepath
                FROM clips c
            """)
            params = []

            # Filter out clips already processed unless --process-all is specified
            if not args.process_all:
                 # Using LEFT JOIN / IS NULL is often efficient
                query += sql.SQL("""
                    LEFT JOIN embeddings e ON c.id = e.clip_id
                        AND e.model_name = %s
                        AND e.generation_strategy = %s
                    WHERE c.keyframe_filepath IS NOT NULL
                      AND e.id IS NULL
                """)
                params.extend([args.model_name, args.generation_strategy])
            else:
                # Process all that have a keyframe path, rely on ON CONFLICT later
                 query += sql.SQL(" WHERE c.keyframe_filepath IS NOT NULL")

            query += sql.SQL(" ORDER BY c.id") # Consistent order

            if args.limit:
                query += sql.SQL(" LIMIT %s")
                params.append(args.limit)

            print("Fetching clips with keyframes to process...")
            cur.execute(query, params)
            clips_to_process = cur.fetchall()

            if not clips_to_process:
                print("No clips found requiring embedding generation for this model/strategy.")
                return

            print(f"Found {len(clips_to_process)} clips to process.")

            # --- Process in Batches ---
            for i in tqdm(range(0, len(clips_to_process), args.batch_size), desc="Generating Embeddings"):
                batch_raw = clips_to_process[i:i+args.batch_size]

                # Prepare batch data with absolute paths
                batch_data = []
                for clip_id, rel_keyframe_path in batch_raw:
                    if rel_keyframe_path: # Should always be true based on query
                         abs_keyframe_path = os.path.join(media_base_dir, rel_keyframe_path)
                         if os.path.isfile(abs_keyframe_path):
                             batch_data.append((clip_id, abs_keyframe_path))
                         else:
                              print(f"Warning: Keyframe file not found, skipping: {abs_keyframe_path} (Clip ID: {clip_id})")
                    else:
                         print(f"Warning: Clip ID {clip_id} has NULL keyframe_filepath despite query filter. Skipping.")


                if not batch_data:
                     print("Skipping empty batch.")
                     continue

                # Generate embeddings for the current batch
                embeddings_batch = generate_embeddings_batch(model, processor, model_type, batch_data, device)

                processed_count += len(batch_data) # Count attempts

                # --- Insert Embeddings into Database ---
                if embeddings_batch:
                    insert_query = sql.SQL("""
                        INSERT INTO embeddings (clip_id, embedding, model_name, generation_strategy)
                        VALUES %s
                        ON CONFLICT (clip_id, model_name, generation_strategy) DO NOTHING;
                    """)
                    # Prepare data for execute_values: list of tuples
                    values_to_insert = [
                        (clip_id, embedding_list, args.model_name, args.generation_strategy)
                        for clip_id, embedding_list in embeddings_batch
                    ]

                    try:
                        # Use execute_values for efficient batch insertion
                        inserted_rows = execute_values(cur, insert_query, values_to_insert, page_size=args.batch_size, fetch=True) # Fetch=True returns count
                        conn.commit()
                        inserted_count += len(inserted_rows) # Count actual insertions
                        # print(f"Batch inserted/skipped {len(values_to_insert)} embeddings. Actual new: {len(inserted_rows)}") # Verbose
                    except psycopg2.Error as e:
                        conn.rollback()
                        print(f"\nError inserting batch into database: {e}")
                        print("Skipping DB insert for this batch.")

    except (ValueError, psycopg2.Error, ImportError, Exception) as e: # Catch broader errors
        print(f"\nA critical error occurred: {e}")
        if conn: conn.rollback() # Rollback any transaction
    finally:
        if conn:
            conn.close()
            print("\nDatabase connection closed.")
            print(f"Finished embedding generation.")
            print(f"Clips processed (attempted): {processed_count}")
            print(f"Embeddings newly inserted: {inserted_count}")

if __name__ == "__main__":
    main()
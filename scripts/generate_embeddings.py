import torch
from PIL import Image
import os
import argparse
import glob
import numpy as np
from transformers import CLIPProcessor, CLIPModel
from tqdm import tqdm
import psycopg2
from psycopg2.extras import execute_values # For efficient batch insert
from psycopg2 import sql

# Import helpers from db_utils
from db_utils import get_db_connection, get_media_base_dir

# --- get_model_and_processor remains the same ---
def get_model_and_processor(model_name="openai/clip-vit-base-patch32"):
    """Loads the specified model and processor."""
    try:
        if "clip-vit-base-patch32" in model_name: embedding_dim = 512
        elif "clip-vit-large-patch14" in model_name: embedding_dim = 768
        else:
            print(f"Warning: Unknown embedding dimension for {model_name}. Defaulting to 512.")
            embedding_dim = 512 # Default or raise error

        if "clip" in model_name.lower():
            model = CLIPModel.from_pretrained(model_name)
            processor = CLIPProcessor.from_pretrained(model_name)
            print(f"Loaded CLIP model: {model_name}")
            return model, processor, "clip", embedding_dim
        # Add elif for other models like DINO
        else:
            raise ValueError(f"Model type not recognized or supported yet: {model_name}")
    except Exception as e:
        print(f"Error loading model {model_name}: {e}")
        raise

# --- generate_embeddings_batch remains the same ---
def generate_embeddings_batch(model, processor, model_type, batch_data, device):
    """Generates embeddings for a batch of image paths."""
    batch_clip_ids = [item[0] for item in batch_data]
    batch_paths = [item[1] for item in batch_data]
    embeddings_out = []
    try:
        images = [Image.open(p).convert("RGB") for p in batch_paths]
        if model_type == "clip":
            inputs = processor(text=None, images=images, return_tensors="pt", padding=True)
            inputs = {k: v.to(device) for k, v in inputs.items()}
            with torch.no_grad():
                image_features = model.get_image_features(**inputs)
        # Add elif for other models
        else:
            print(f"ERROR: Embedding generation logic not implemented for model type: {model_type}")
            return []
        image_features_np = image_features.cpu().numpy()
        embeddings_out = [(clip_id, emb.tolist()) for clip_id, emb in zip(batch_clip_ids, image_features_np)]
    except FileNotFoundError as e:
        print(f"\nError: Image file not found: {e}. Skipping batch.")
        return []
    except Exception as e:
        print(f"\nError processing batch starting with ID {batch_clip_ids[0]}: {e}")
        return []
    return embeddings_out

def main():
    parser = argparse.ArgumentParser(description="Generate image embeddings and store them in the database.")
    # --- Arguments ---
    parser.add_argument("--model_name", default="openai/clip-vit-base-patch32", help="Hugging Face model identifier.")
    parser.add_argument("--generation_strategy", required=True, help="Identifier for how embeddings were made (e.g., 'keyframe_midpoint').")
    # --- NEW Argument ---
    parser.add_argument("--source-identifier", default=None,
                        help="Optional: Process only clips belonging to this source_identifier.")
    # --- Other Args ---
    parser.add_argument("--batch_size", type=int, default=32, help="Batch size for processing images.")
    parser.add_argument("--limit", type=int, default=None, help="Limit the number of clips to process.")
    parser.add_argument("--overwrite", action="store_true",
                        help="Attempt to generate and insert embeddings even if they already exist (uses ON CONFLICT DO NOTHING). Default is to skip existing.")
    # Renamed --process-all to --overwrite for consistency

    args = parser.parse_args()

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    print(f"Using device: {device}")

    conn = None
    processed_count = 0
    inserted_count = 0

    try:
        print(f"Loading model: {args.model_name}...")
        model, processor, model_type, embedding_dim = get_model_and_processor(args.model_name)
        model.to(device)
        model.eval()
        print("Model loaded successfully.")

        conn = get_db_connection()
        media_base_dir = get_media_base_dir()
        print("Database connection established.")

        with conn.cursor() as cur:
            # --- Build Query Conditionally ---
            print("Building query to fetch clips...")
            params = []
            # Base selection and join needed if filtering by source
            select_clause = sql.SQL("SELECT c.id, c.keyframe_filepath FROM clips c")
            join_clauses = []
            where_conditions = [sql.SQL("c.keyframe_filepath IS NOT NULL")] # Always require a keyframe

            # Add join and filter if source_identifier is provided
            if args.source_identifier:
                join_clauses.append(sql.SQL("JOIN source_videos sv ON c.source_video_id = sv.id"))
                where_conditions.append(sql.SQL("sv.source_identifier = %s"))
                params.append(args.source_identifier)
                print(f"Filtering by source_identifier: '{args.source_identifier}'")
            else:
                print("Processing clips from all sources.")

            # Add join and filter for skipping existing embeddings unless --overwrite
            if not args.overwrite:
                join_clauses.append(sql.SQL(
                    """LEFT JOIN embeddings e ON c.id = e.clip_id
                       AND e.model_name = %s
                       AND e.generation_strategy = %s"""
                ))
                where_conditions.append(sql.SQL("e.id IS NULL"))
                params.extend([args.model_name, args.generation_strategy])
                print(f"Skipping clips that already have embeddings for model='{args.model_name}', strategy='{args.generation_strategy}'.")
            else:
                print("Processing all clips with keyframes, will use ON CONFLICT during insert for existing embeddings.")


            # Assemble the final query
            query = select_clause
            if join_clauses:
                # *** FIX: Add space before joining JOIN clauses ***
                query += sql.SQL(" ") + sql.SQL(" ").join(join_clauses)
            if where_conditions:
                 # *** FIX: Add space before WHERE and use AND with spaces ***
                query += sql.SQL(" WHERE ") + sql.SQL(" AND ").join(where_conditions)

            query += sql.SQL(" ORDER BY c.id") # Consistent order

            if args.limit:
                 # *** FIX: Add space before LIMIT ***
                query += sql.SQL(" LIMIT %s")
                params.append(args.limit)

            # --- Execute Query ---
            print("Fetching clips to process...") # Moved print statement here
            # print(f"DEBUG Query: {cur.mogrify(query, params).decode()}") # Uncomment to debug
            cur.execute(query, params)
            clips_to_process = cur.fetchall()

            if not clips_to_process:
                print("No clips found matching the criteria requiring embedding generation.")
                return

            print(f"Found {len(clips_to_process)} clips to process.")

            # --- Process in Batches ---
            for i in tqdm(range(0, len(clips_to_process), args.batch_size), desc="Generating Embeddings"):
                batch_raw = clips_to_process[i:i+args.batch_size]
                batch_data = []
                for clip_id, rel_keyframe_path in batch_raw:
                    if rel_keyframe_path: # Should always have path based on query
                         abs_keyframe_path = os.path.join(media_base_dir, rel_keyframe_path)
                         if os.path.isfile(abs_keyframe_path):
                             batch_data.append((clip_id, abs_keyframe_path))
                         else:
                              print(f"\nWarning: Keyframe file not found, skipping: {abs_keyframe_path} (Clip ID: {clip_id})")
                    # else: # Should not happen with IS NOT NULL filter
                    #      print(f"Warning: Clip ID {clip_id} has NULL keyframe_filepath. Skipping.")

                if not batch_data: continue

                embeddings_batch = generate_embeddings_batch(model, processor, model_type, batch_data, device)
                processed_count += len(batch_data)

                if embeddings_batch:
                    insert_query = sql.SQL("""
                        INSERT INTO embeddings (clip_id, embedding, model_name, generation_strategy)
                        VALUES %s
                        ON CONFLICT (clip_id, model_name, generation_strategy) DO NOTHING;
                    """)
                    values_to_insert = [
                        (clip_id, embedding_list, args.model_name, args.generation_strategy)
                        for clip_id, embedding_list in embeddings_batch
                    ]
                    try:
                        # Using fetch=True with execute_values might be specific to psycopg2 version or adapter,
                        # let's just count the successful insertions if needed or rely on rowcount.
                        # Using default fetch=False is safer.
                        execute_values(cur, insert_query, values_to_insert, page_size=args.batch_size)
                        inserted_count += cur.rowcount # Count how many rows were actually inserted (or updated if using DO UPDATE)
                        conn.commit()
                    except psycopg2.Error as e:
                        conn.rollback()
                        print(f"\nError inserting batch into database: {e}")

    except (ValueError, psycopg2.Error, ImportError, Exception) as e:
        print(f"\nA critical error occurred: {e}")
        if conn: conn.rollback()
    finally:
        if conn:
            conn.close()
            print("\nDatabase connection closed.")
            print(f"\n--- Summary ---")
            if args.source_identifier:
                print(f"Source Identifier: {args.source_identifier}")
            else:
                print(f"Source Identifier: ALL")
            print(f"Model: {args.model_name}")
            print(f"Strategy: {args.generation_strategy}")
            print(f"Clips processed (embedding attempted): {processed_count}")
            print(f"Embeddings newly inserted: {inserted_count}")

if __name__ == "__main__":
    main()
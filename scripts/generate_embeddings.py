import torch
from PIL import Image
import os
import argparse
import glob
import pickle
from transformers import CLIPProcessor, CLIPModel
from tqdm import tqdm

def get_model_and_processor(model_name="openai/clip-vit-base-patch32"):
    """Loads the specified model and processor."""
    try:
        # You might add logic here later to load different model types (CLIP, DINOv2, etc.)
        if "clip" in model_name.lower():
            model = CLIPModel.from_pretrained(model_name)
            processor = CLIPProcessor.from_pretrained(model_name)
            print(f"Loaded CLIP model: {model_name}")
            return model, processor, "clip"
        # Example for future extension (requires installing appropriate libraries for DINOv2 if used)
        # elif "dino" in model_name.lower():
        #    from transformers import AutoImageProcessor, AutoModel
        #    processor = AutoImageProcessor.from_pretrained(model_name)
        #    model = AutoModel.from_pretrained(model_name)
        #    print(f"Loaded DINO model: {model_name}")
        #    return model, processor, "dino"
        else:
            raise ValueError(f"Model type not recognized or supported yet: {model_name}")

    except Exception as e:
        print(f"Error loading model {model_name}: {e}")
        raise

def generate_clip_embeddings(model, processor, image_paths, device, batch_size=32):
    """Generates CLIP embeddings for a list of image paths."""
    embeddings = {}
    model.to(device)
    model.eval() # Set model to evaluation mode

    print(f"Generating CLIP embeddings using device: {device}")

    with torch.no_grad(): # Disable gradient calculations
        for i in tqdm(range(0, len(image_paths), batch_size), desc="Generating Embeddings"):
            batch_paths = image_paths[i:i+batch_size]
            try:
                images = [Image.open(p).convert("RGB") for p in batch_paths]
                inputs = processor(text=None, images=images, return_tensors="pt", padding=True)
                inputs = {k: v.to(device) for k, v in inputs.items()} # Move batch to device

                image_features = model.get_image_features(**inputs)

                # Move embeddings to CPU and convert to numpy
                image_features_np = image_features.cpu().numpy()

                for path, feature in zip(batch_paths, image_features_np):
                    filename = os.path.basename(path)
                    embeddings[filename] = feature # Store embedding with filename key
            except Exception as e:
                print(f"\nError processing batch starting with {batch_paths[0]}: {e}")
                print("Skipping this batch.")
                continue # Skip to the next batch

    return embeddings

# Placeholder for other model types if needed later
# def generate_dino_embeddings(model, processor, image_paths, device, batch_size=32):
#     embeddings = {}
#     # ... implementation similar to CLIP, but using the DINO model's API ...
#     print("DINO embedding generation not yet implemented.")
#     return embeddings


def main():
    parser = argparse.ArgumentParser(description="Generate image embeddings using a pre-trained model.")
    parser.add_argument("--image_dir", required=True, help="Directory containing keyframe images.")
    parser.add_argument("--output_file", required=True, help="Path to save the embeddings (e.g., embeddings.pkl).")
    parser.add_argument("--model_name", default="openai/clip-vit-base-patch32",
                        help="Hugging Face model identifier (e.g., openai/clip-vit-base-patch32).")
    parser.add_argument("--image_ext", default="jpg", help="Image file extension (e.g., jpg, png).")
    parser.add_argument("--batch_size", type=int, default=32, help="Batch size for processing images.")


    args = parser.parse_args()

    # --- Device Setup ---
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

    # --- Load Model ---
    try:
        model, processor, model_type = get_model_and_processor(args.model_name)
    except Exception:
        return # Exit if model loading fails

    # --- Find Images ---
    search_pattern = os.path.join(args.image_dir, f"*.{args.image_ext}")
    image_files = glob.glob(search_pattern)

    if not image_files:
        print(f"No images found with extension .{args.image_ext} in {args.image_dir}")
        return

    print(f"Found {len(image_files)} images. Starting embedding generation...")

    # --- Generate Embeddings ---
    if model_type == "clip":
        embeddings_dict = generate_clip_embeddings(model, processor, image_files, device, args.batch_size)
    # elif model_type == "dino":
        # embeddings_dict = generate_dino_embeddings(model, processor, image_files, device, args.batch_size)
    else:
        print(f"Embeddings generation for model type '{model_type}' is not implemented.")
        return

    # --- Save Embeddings ---
    if embeddings_dict:
        try:
            with open(args.output_file, 'wb') as f:
                pickle.dump(embeddings_dict, f)
            print(f"\nSuccessfully generated {len(embeddings_dict)} embeddings.")
            print(f"Embeddings saved to {args.output_file}")
        except Exception as e:
            print(f"\nError saving embeddings to {args.output_file}: {e}")
    else:
        print("\nNo embeddings were generated.")


if __name__ == "__main__":
    main()
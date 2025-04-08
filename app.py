import os
import pickle
import numpy as np
from flask import Flask, render_template, send_from_directory, abort, request, url_for, redirect
from sklearn.metrics.pairwise import cosine_similarity
import random

# --- Configuration ---
EMBEDDING_FILE = "/Users/billy/Dropbox/Projects/Meatspace/media/LANDLINE_embeddings/clip_embeddings_midpoint.pkl"
# IMPORTANT: Adjust these paths to the *parent* directory containing keyframes/scenes
MEDIA_BASE_DIR = "/Users/billy/Dropbox/Projects/Meatspace/media"
KEYFRAME_SUBDIR = "LANDLINE_keyframes/midpoint" # Relative to MEDIA_BASE_DIR
VIDEO_SUBDIR = "LANDLINE_scenes"              # Relative to MEDIA_BASE_DIR
KEYFRAME_SUFFIX = "_frame_mid.jpg"
VIDEO_SUFFIX = ".mp4"
CLIP_IDENTIFIER_PREFIX = "Landline_scene_" # Used to extract the 'XXX' part
NUM_RESULTS = 10 # Number of similar clips to show

# --- Flask App Setup ---
app = Flask(__name__)

# --- Data Loading ---
print("Loading embeddings...")
try:
    with open(EMBEDDING_FILE, 'rb') as f:
        # Assuming format: { "Landline_scene_071": np.array([...]), ... }
        # Or potentially { "Landline_scene_071_frame_mid.jpg": np.array([...]), ... }
        raw_embeddings = pickle.load(f)
except FileNotFoundError:
    print(f"ERROR: Embedding file not found at {EMBEDDING_FILE}")
    exit()
except Exception as e:
    print(f"ERROR: Failed to load or parse embedding file: {e}")
    exit()

print(f"Loaded {len(raw_embeddings)} embeddings.")

# --- Preprocessing and Indexing ---
print("Indexing media files and embeddings...")
clip_data = {} # Store all info: { clip_id: {"embedding": np.array, "keyframe": path, "video": path, "title": title} }
embedding_list = []
clip_id_order = [] # Keep track of the order for mapping back from similarity matrix

# Derive base paths
keyframe_base_path = os.path.join(MEDIA_BASE_DIR, KEYFRAME_SUBDIR)
video_base_path = os.path.join(MEDIA_BASE_DIR, VIDEO_SUBDIR)

# Determine the format of keys in raw_embeddings and normalize
# We want a common clip_id like "Landline_scene_071"
processed_embeddings = {}
for key, embedding in raw_embeddings.items():
    clip_id = None
    if key.startswith(CLIP_IDENTIFIER_PREFIX) and key.endswith(KEYFRAME_SUFFIX):
        clip_id = key[:-len(KEYFRAME_SUFFIX)] # e.g., "Landline_scene_071"
    elif key.startswith(CLIP_IDENTIFIER_PREFIX): # Maybe the key is already the base ID
         # Crude check: if it doesn't look like a file path/suffix is attached
        if not any(key.endswith(s) for s in [".jpg", ".png", ".mp4", ".mov"]):
             clip_id = key

    if clip_id:
        processed_embeddings[clip_id] = embedding
    else:
        print(f"Warning: Could not determine clip_id for embedding key: {key}. Skipping.")

if not processed_embeddings:
    print("ERROR: No valid embeddings found after processing keys. Check EMBEDDING_FILE format and CLIP_IDENTIFIER_PREFIX.")
    exit()

# Now build clip_data using the processed_embeddings keys
for clip_id, embedding in processed_embeddings.items():
    keyframe_filename = f"{clip_id}{KEYFRAME_SUFFIX}"
    video_filename = f"{clip_id}{VIDEO_SUFFIX}"

    keyframe_full_path = os.path.join(keyframe_base_path, keyframe_filename)
    video_full_path = os.path.join(video_base_path, video_filename)

    if os.path.exists(keyframe_full_path) and os.path.exists(video_full_path):
        clip_data[clip_id] = {
            "embedding": embedding,
            # Store paths *relative* to MEDIA_BASE_DIR for the /media route
            "keyframe_rel": os.path.join(KEYFRAME_SUBDIR, keyframe_filename),
            "video_rel": os.path.join(VIDEO_SUBDIR, video_filename),
            "title": clip_id.replace("_", " ").title() # Basic title generation
        }
        embedding_list.append(embedding)
        clip_id_order.append(clip_id)
    else:
        print(f"Warning: Missing media files for {clip_id}. Keyframe exists: {os.path.exists(keyframe_full_path)}, Video exists: {os.path.exists(video_full_path)}. Skipping.")

if not clip_data:
    print("ERROR: No clips found with corresponding embeddings and media files.")
    print(f"Searched keyframes in: {keyframe_base_path}")
    print(f"Searched videos in: {video_base_path}")
    exit()

print(f"Successfully indexed {len(clip_data)} clips with embeddings and media.")

# Convert embeddings to a NumPy array for efficient computation
embedding_matrix = np.array(embedding_list)

# --- Similarity Function ---
def find_similar_clips(query_clip_id, num_results):
    if query_clip_id not in clip_data:
        return None, []

    query_embedding = clip_data[query_clip_id]["embedding"].reshape(1, -1)

    # Calculate cosine similarities
    similarities = cosine_similarity(query_embedding, embedding_matrix)[0] # Get the first (only) row

    # Get indices of top N+1 scores (including the query itself)
    # Use argsort for efficiency
    sorted_indices = np.argsort(similarities)[::-1]

    results = []
    count = 0
    for index in sorted_indices:
        result_clip_id = clip_id_order[index]
        if result_clip_id == query_clip_id:
            continue # Skip self

        score = similarities[index]
        results.append({
            "clip_id": result_clip_id,
            "score": float(score), # Convert numpy float to python float
            "keyframe_url": url_for('serve_media', filepath=clip_data[result_clip_id]["keyframe_rel"]),
            "video_url": url_for('serve_media', filepath=clip_data[result_clip_id]["video_rel"]),
        })
        count += 1
        if count >= num_results:
            break

    query_clip_info = {
        "clip_id": query_clip_id,
        "title": clip_data[query_clip_id]["title"],
        "keyframe_url": url_for('serve_media', filepath=clip_data[query_clip_id]["keyframe_rel"]),
        "video_url": url_for('serve_media', filepath=clip_data[query_clip_id]["video_rel"]),
    }

    return query_clip_info, results


# --- Flask Routes ---
@app.route('/')
def index():
    # Pick a random clip for the initial view
    random_clip_id = random.choice(clip_id_order)
    return redirect(url_for('query_clip', clip_id=random_clip_id))

@app.route('/query/<clip_id>')
def query_clip(clip_id):
    if clip_id not in clip_data:
         # Try to find a valid one if the provided ID is bad
        valid_ids = list(clip_data.keys())
        if not valid_ids:
            return "Error: No valid clip data found.", 500
        print(f"Warning: Requested clip_id '{clip_id}' not found. Redirecting to random clip.")
        return redirect(url_for('query_clip', clip_id=random.choice(valid_ids)))


    query_info, results = find_similar_clips(clip_id, NUM_RESULTS)

    if query_info is None:
         # This shouldn't happen if the check above worked, but just in case
         return redirect(url_for('index'))


    return render_template('index.html', query=query_info, results=results)

@app.route('/media/<path:filepath>')
def serve_media(filepath):
    # Security: Ensure the path doesn't escape the intended base directory
    # os.path.abspath prevents some traversal issues, checking it starts with
    # the known base dir is another layer.
    safe_base_path = os.path.abspath(MEDIA_BASE_DIR)
    requested_path = os.path.abspath(os.path.join(MEDIA_BASE_DIR, filepath))

    if not requested_path.startswith(safe_base_path):
        print(f"Warning: Attempted directory traversal: {filepath}")
        return abort(404)

    # Check if file exists relative to the actual base directory
    if not os.path.isfile(requested_path):
         print(f"Warning: Media file not found: {requested_path}")
         return abort(404)

    # Use send_from_directory for better handling of mime types, ranges etc.
    # Need directory and filename separately
    directory = os.path.dirname(requested_path)
    filename = os.path.basename(requested_path)
    # print(f"Serving: Dir='{directory}', File='{filename}'") # Debugging
    return send_from_directory(directory, filename)


# --- Main Execution ---
if __name__ == '__main__':
    print(f"Starting Flask server...")
    print(f"Serving media from base directory: {MEDIA_BASE_DIR}")
    print(f"Embeddings loaded from: {EMBEDDING_FILE}")
    print(f"Found {len(clip_data)} clips with media and embeddings.")
    print(f"Open http://127.0.0.1:5001 in your browser.")
    # Use a port other than default 5000 in case it's common
    app.run(debug=True, port=5001)
import os
import pickle
import numpy as np
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import RedirectResponse, FileResponse, HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from sklearn.metrics.pairwise import cosine_similarity
import random

# --- Configuration ---
EMBEDDING_FILE = "/Users/billy/Dropbox/Projects/Meatspace/media/LANDLINE_embeddings/clip_embeddings_midpoint.pkl"
# IMPORTANT: Adjust these paths to the *parent* directory containing keyframes/scenes
MEDIA_BASE_DIR = "/Users/billy/Dropbox/Projects/Meatspace/media"
KEYFRAME_SUBDIR = "LANDLINE_keyframes/midpoint"  # Relative to MEDIA_BASE_DIR
VIDEO_SUBDIR = "LANDLINE_scenes"                # Relative to MEDIA_BASE_DIR
KEYFRAME_SUFFIX = "_frame_mid.jpg"
VIDEO_SUFFIX = ".mp4"
CLIP_IDENTIFIER_PREFIX = "Landline_scene_"      # Used to extract the 'XXX' part
NUM_RESULTS = 10  # Number of similar clips to show

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
clip_data = {}  # { clip_id: {"embedding": np.array, "keyframe": path, "video": path, "title": title} }
embedding_list = []
clip_id_order = []  # Keep track of the order for mapping back from similarity matrix

# Derive base paths
keyframe_base_path = os.path.join(MEDIA_BASE_DIR, KEYFRAME_SUBDIR)
video_base_path = os.path.join(MEDIA_BASE_DIR, VIDEO_SUBDIR)

# Determine the format of keys in raw_embeddings and normalize
processed_embeddings = {}
for key, embedding in raw_embeddings.items():
    clip_id = None
    if key.startswith(CLIP_IDENTIFIER_PREFIX) and key.endswith(KEYFRAME_SUFFIX):
        clip_id = key[:-len(KEYFRAME_SUFFIX)]
    elif key.startswith(CLIP_IDENTIFIER_PREFIX):
        if not any(key.endswith(s) for s in [".jpg", ".png", ".mp4", ".mov"]):
            clip_id = key

    if clip_id:
        processed_embeddings[clip_id] = embedding
    else:
        print(f"Warning: Could not determine clip_id for embedding key: {key}. Skipping.")

if not processed_embeddings:
    print("ERROR: No valid embeddings found after processing keys. Check EMBEDDING_FILE format and CLIP_IDENTIFIER_PREFIX.")
    exit()

# Build clip_data using processed_embeddings keys
for clip_id, embedding in processed_embeddings.items():
    keyframe_filename = f"{clip_id}{KEYFRAME_SUFFIX}"
    video_filename = f"{clip_id}{VIDEO_SUFFIX}"

    keyframe_full_path = os.path.join(keyframe_base_path, keyframe_filename)
    video_full_path = os.path.join(video_base_path, video_filename)

    if os.path.exists(keyframe_full_path) and os.path.exists(video_full_path):
        clip_data[clip_id] = {
            "embedding": embedding,
            "keyframe_rel": os.path.join(KEYFRAME_SUBDIR, keyframe_filename),
            "video_rel": os.path.join(VIDEO_SUBDIR, video_filename),
            "title": clip_id.replace("_", " ").title()
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
embedding_matrix = np.array(embedding_list)

# --- Similarity Function ---
def find_similar_clips(query_clip_id, num_results, request: Request):
    if query_clip_id not in clip_data:
        return None, []

    query_embedding = clip_data[query_clip_id]["embedding"].reshape(1, -1)
    similarities = cosine_similarity(query_embedding, embedding_matrix)[0]
    sorted_indices = np.argsort(similarities)[::-1]

    results = []
    count = 0
    for index in sorted_indices:
        result_clip_id = clip_id_order[index]
        if result_clip_id == query_clip_id:
            continue
        score = similarities[index]
        results.append({
            "clip_id": result_clip_id,
            "score": float(score),
            "keyframe_url": request.url_for('serve_media', filepath=clip_data[result_clip_id]["keyframe_rel"]),
            "video_url": request.url_for('serve_media', filepath=clip_data[result_clip_id]["video_rel"]),
        })
        count += 1
        if count >= num_results:
            break

    query_clip_info = {
        "clip_id": query_clip_id,
        "title": clip_data[query_clip_id]["title"],
        "keyframe_url": request.url_for('serve_media', filepath=clip_data[query_clip_id]["keyframe_rel"]),
        "video_url": request.url_for('serve_media', filepath=clip_data[query_clip_id]["video_rel"]),
    }
    return query_clip_info, results

# --- FastAPI App Setup ---
app = FastAPI()
# Mount the static files directory so that 'static' routes work in templates
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# --- FastAPI Routes ---
@app.get("/", response_class=HTMLResponse, name="index")
async def index(request: Request):
    random_clip_id = random.choice(clip_id_order)
    return RedirectResponse(url=request.url_for('query_clip', clip_id=random_clip_id))

@app.get("/query/{clip_id}", response_class=HTMLResponse, name="query_clip")
async def query_clip(clip_id: str, request: Request):
    if clip_id not in clip_data:
        valid_ids = list(clip_data.keys())
        if not valid_ids:
            return HTMLResponse("Error: No valid clip data found.", status_code=500)
        print(f"Warning: Requested clip_id '{clip_id}' not found. Redirecting to random clip.")
        return RedirectResponse(url=request.url_for('query_clip', clip_id=random.choice(valid_ids)))

    query_info, results = find_similar_clips(clip_id, NUM_RESULTS, request)
    if query_info is None:
        return RedirectResponse(url=request.url_for('index'))

    return templates.TemplateResponse("index.html", {"request": request, "query": query_info, "results": results})

@app.get("/media/{filepath:path}", name="serve_media")
async def serve_media(filepath: str):
    safe_base_path = os.path.abspath(MEDIA_BASE_DIR)
    requested_path = os.path.abspath(os.path.join(MEDIA_BASE_DIR, filepath))

    if not requested_path.startswith(safe_base_path):
        print(f"Warning: Attempted directory traversal: {filepath}")
        raise HTTPException(status_code=404, detail="File not found")

    if not os.path.isfile(requested_path):
        print(f"Warning: Media file not found: {requested_path}")
        raise HTTPException(status_code=404, detail="File not found")

    return FileResponse(requested_path)

# --- Main Execution ---
if __name__ == '__main__':
    import uvicorn
    print("Starting FastAPI server...")
    print(f"Serving media from base directory: {MEDIA_BASE_DIR}")
    print(f"Embeddings loaded from: {EMBEDDING_FILE}")
    print(f"Found {len(clip_data)} clips with media and embeddings.")
    print("Open http://127.0.0.1:5001 in your browser.")
    uvicorn.run("app:app", host="127.0.0.1", port=5001, reload=True)
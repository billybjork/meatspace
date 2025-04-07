import streamlit as st
import pandas as pd
import numpy as np
import pickle
from sklearn.metrics.pairwise import cosine_similarity
import os
import base64

# --- Configuration ---
EMBEDDINGS_PATH = "/Users/billy/Dropbox/Projects/Meatspace/media/LANDLINE_embeddings/clip_embeddings_midpoint.pkl"
# **** Make sure this is the directory CONTAINING the keyframe files ****
KEYFRAME_BASE_DIR = "/Users/billy/Dropbox/Projects/Meatspace/media/LANDLINE_keyframes/midpoint"
VIDEO_BASE_DIR = "/Users/billy/Dropbox/Projects/Meatspace/media/LANDLINE_scenes"
NUM_RESULTS = 10

# --- Helper Functions ---

@st.cache_data
def load_embeddings(path):
    """Loads embeddings from a pickle file."""
    try:
        with open(path, 'rb') as f:
            data = pickle.load(f)
        # Assuming data is a dict: {'keyframe_filename': embedding_vector}
        # Let's call the keys 'keyframe_filenames' now for clarity
        keyframe_filenames = list(data.keys())
        embeddings = np.array(list(data.values()))
        # ** Important Check: Print a sample filename to confirm format **
        if keyframe_filenames:
             print(f"DEBUG: Sample keyframe filename from pickle: {keyframe_filenames[0]}")
        else:
             print("DEBUG: No keyframe filenames found in pickle")
        return keyframe_filenames, embeddings
    except Exception as e:
        st.error(f"Error loading embeddings file: {e}")
        return None, None

def get_video_path_from_keyframe_filename(keyframe_filename): # Changed input name
    """Derives video path from keyframe filename based on naming convention."""
    # Example: Landline_scene_071_frame_mid.jpg -> Landline_scene_071
    base_name = keyframe_filename.replace('_frame_mid.jpg', '') # Adjust if naming differs
    video_filename = f"{base_name}.mp4"
    return os.path.join(VIDEO_BASE_DIR, video_filename)

def get_clip_title_from_keyframe_filename(keyframe_filename): # Changed input name
    """Derives a display title from the keyframe filename."""
    # Example: Landline_scene_071_frame_mid.jpg -> Landline_scene_071
    title = keyframe_filename.replace('_frame_mid.jpg', '').replace('_', ' ') # Basic title
    return title

# Function to encode video to base64 (keep as is)
# @st.cache_data
def get_video_b64(video_path):
    if not os.path.exists(video_path):
        print(f"DEBUG: Video not found for b64 encoding: {video_path}")
        return None
    try:
        with open(video_path, "rb") as video_file:
            return base64.b64encode(video_file.read()).decode()
    except Exception as e:
        print(f"DEBUG: Error reading video {video_path} for b64: {e}")
        return None

def find_similar_clips(query_embedding, all_embeddings, keyframe_filenames, num_results): # Changed input name
    """Finds clips most similar to the query embedding."""
    similarities = cosine_similarity(query_embedding.reshape(1, -1), all_embeddings)[0]
    sorted_indices = np.argsort(similarities)[::-1]

    results = []
    count = 0
    for i in sorted_indices:
        if count >= num_results:
            break
        results.append({
            'keyframe_filename': keyframe_filenames[i], # Store filename
            'score': similarities[i],
            'index': i
        })
        count += 1
    return results

# --- Streamlit App Layout ---
st.set_page_config(layout="wide")
st.title("Snowboard Clip Similarity Viewer")

# --- Load Data ---
# Rename variable for clarity
keyframe_filenames, embeddings = load_embeddings(EMBEDDINGS_PATH)

if keyframe_filenames is None or embeddings is None:
    st.stop()

if not keyframe_filenames:
    st.warning("No keyframes found in the embeddings file.")
    st.stop()

# --- State Management ---
if 'query_index' not in st.session_state:
    st.session_state['query_index'] = 0

# --- UI ---
query_idx = st.session_state['query_index']
query_keyframe_filename = keyframe_filenames[query_idx]

# **** CONSTRUCT FULL PATHS ****
query_keyframe_path = os.path.join(KEYFRAME_BASE_DIR, query_keyframe_filename)
query_video_path = get_video_path_from_keyframe_filename(query_keyframe_filename) # Pass filename
query_title = get_clip_title_from_keyframe_filename(query_keyframe_filename) # Pass filename

st.header("Query Clip")
st.subheader(query_title)

# Add debug prints to see the paths being checked
# st.write(f"DEBUG: Checking Query Keyframe Path: {query_keyframe_path}")
# st.write(f"DEBUG: Checking Query Video Path: {query_video_path}")

# Display Query Keyframe and Video (using HTML for hover-play)
if os.path.exists(query_keyframe_path) and os.path.exists(query_video_path):
    keyframe_b64 = base64.b64encode(open(query_keyframe_path, "rb").read()).decode()
    video_b64 = get_video_b64(query_video_path) # Get encoded video

    if video_b64: # Only display if video encoding worked
        html_str = f"""
            <div style="position: relative; width: 300px; height: auto;">
                <img src="data:image/jpeg;base64,{keyframe_b64}"
                     width="300"
                     style="display: block;"
                     onmouseover="this.style.display='none'; this.nextElementSibling.style.display='block'; this.nextElementSibling.play();"
                     alt="Query keyframe">
                <video width="300" loop muted playsinline preload="metadata"
                       style="display: none;"
                       onmouseout="this.style.display='none'; this.previousElementSibling.style.display='block'; this.pause(); this.currentTime=0;">
                       <source src="data:video/mp4;base64,{video_b64}" type="video/mp4">
                       Your browser does not support the video tag.
                </video>
            </div>
            """
        st.components.v1.html(html_str, height=200)
    else:
         st.warning(f"Could not load video preview for: {query_video_path}")
         st.image(query_keyframe_path, width=300) # Fallback to image

else:
    # Be more specific in the warning
    if not os.path.exists(query_keyframe_path):
        st.warning(f"Could not find query keyframe at expected path: {query_keyframe_path}")
    if not os.path.exists(query_video_path):
        st.warning(f"Could not find query video at expected path: {query_video_path}")


st.divider()

# --- Find and Display Similar Clips ---
st.header("Similarity Matches")

similarity_results = find_similar_clips(embeddings[query_idx], embeddings, keyframe_filenames, NUM_RESULTS + 1)
filtered_results = [res for res in similarity_results if res['index'] != query_idx][:NUM_RESULTS]

num_cols = 5
cols = st.columns(num_cols)

for i, result in enumerate(filtered_results):
    col = cols[i % num_cols]
    with col:
        res_keyframe_filename = result['keyframe_filename']
        # **** CONSTRUCT FULL PATHS FOR RESULTS ****
        res_keyframe_path = os.path.join(KEYFRAME_BASE_DIR, res_keyframe_filename)
        res_video_path = get_video_path_from_keyframe_filename(res_keyframe_filename) # Pass filename
        res_score = result['score']
        res_index = result['index']

        # st.write(f"DEBUG: Checking Result Keyframe Path: {res_keyframe_path}") # Optional debug
        # st.write(f"DEBUG: Checking Result Video Path: {res_video_path}") # Optional debug

        if os.path.exists(res_keyframe_path) and os.path.exists(res_video_path):
            res_keyframe_b64 = base64.b64encode(open(res_keyframe_path, "rb").read()).decode()
            res_video_b64 = get_video_b64(res_video_path)

            if res_video_b64: # Only display if video encoding worked
                res_html_str = f"""
                    <div style="position: relative; width: 100%; height: auto; margin-bottom: 5px;">
                        <img src="data:image/jpeg;base64,{res_keyframe_b64}"
                             width="100%"
                             style="display: block; cursor: pointer;"
                             onmouseover="this.style.display='none'; this.nextElementSibling.style.display='block'; this.nextElementSibling.play();"
                             alt="Result keyframe {i+1}">
                        <video width="100%" loop muted playsinline preload="metadata"
                               style="display: none;"
                               onmouseout="this.style.display='none'; this.previousElementSibling.style.display='block'; this.pause(); this.currentTime=0;">
                               <source src="data:video/mp4;base64,{res_video_b64}" type="video/mp4">
                               Your browser does not support the video tag.
                        </video>
                    </div>
                    """
                st.components.v1.html(res_html_str, height=130)
            else:
                st.warning(f"No video for {res_keyframe_filename}")
                st.image(res_keyframe_path, width=150) # Fallback

            st.caption(f"Score: {res_score:.4f}")

            button_key = f"query_btn_{res_index}"
            if st.button("Make Query", key=button_key):
                st.session_state['query_index'] = res_index
                st.rerun() # Use st.rerun() in newer Streamlit versions

        else:
            # More specific warning for results
            missing = []
            if not os.path.exists(res_keyframe_path):
                missing.append(f"keyframe ({res_keyframe_path})")
            if not os.path.exists(res_video_path):
                 missing.append(f"video ({res_video_path})")
            col.warning(f"Missing assets for result {i+1}: {', '.join(missing)}")
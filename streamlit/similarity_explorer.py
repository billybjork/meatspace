import streamlit as st
import pandas as pd
import numpy as np
import pickle
from sklearn.metrics.pairwise import cosine_similarity
import os
import io # Needed for byte handling with st.video

# --- Configuration ---
EMBEDDINGS_PATH = "/Users/billy/Dropbox/Projects/Meatspace/media/LANDLINE_embeddings/clip_embeddings_midpoint.pkl"
KEYFRAME_BASE_DIR = "/Users/billy/Dropbox/Projects/Meatspace/media/LANDLINE_keyframes/midpoint"
VIDEO_BASE_DIR = "/Users/billy/Dropbox/Projects/Meatspace/media/LANDLINE_scenes"
NUM_RESULTS = 10

# --- Helper Functions --- (Keep these as they are)

@st.cache_data
def load_embeddings(path):
    """Loads embeddings from a pickle file."""
    try:
        with open(path, 'rb') as f:
            data = pickle.load(f)
        keyframe_filenames = list(data.keys())
        embeddings = np.array(list(data.values()))
        return keyframe_filenames, embeddings
    except FileNotFoundError:
        st.error(f"Error: Embeddings file not found at {path}")
        return None, None
    except Exception as e:
        st.error(f"Error loading embeddings file: {e}")
        return None, None

def get_video_path_from_keyframe_filename(keyframe_filename):
    """Derives video path from keyframe filename."""
    base_name = keyframe_filename.replace('_frame_mid.jpg', '')
    video_filename = f"{base_name}.mp4"
    return os.path.join(VIDEO_BASE_DIR, video_filename)

def get_clip_title_from_keyframe_filename(keyframe_filename):
    """Derives a display title."""
    title = keyframe_filename.replace('_frame_mid.jpg', '').replace('_', ' ')
    return title

def find_similar_clips(query_embedding, all_embeddings, keyframe_filenames, num_results):
    """Finds similar clips."""
    similarities = cosine_similarity(query_embedding.reshape(1, -1), all_embeddings)[0]
    sorted_indices = np.argsort(similarities)[::-1]
    results = []
    count = 0
    for i in sorted_indices:
        if count >= num_results + 1:
            break
        results.append({
            'keyframe_filename': keyframe_filenames[i],
            'score': similarities[i],
            'index': i
        })
        count += 1
    return results

# --- Streamlit App Layout ---

st.set_page_config(layout="wide")
st.title("Snowboard Clip Similarity Viewer")

# --- Load Data ---
keyframe_filenames, embeddings = load_embeddings(EMBEDDINGS_PATH)

if keyframe_filenames is None or embeddings is None:
    st.warning("Failed to load embeddings. Cannot proceed.")
    st.stop()

if not keyframe_filenames:
    st.warning("No keyframes found in the embeddings file.")
    st.stop()

# --- State Management ---
if 'query_index' not in st.session_state:
    st.session_state['query_index'] = 0

# --- UI ---
query_idx = st.session_state['query_index']
if query_idx >= len(keyframe_filenames):
    st.warning("Query index out of bounds, resetting.")
    st.session_state['query_index'] = 0
    query_idx = 0

query_keyframe_filename = keyframe_filenames[query_idx]
query_keyframe_path = os.path.join(KEYFRAME_BASE_DIR, query_keyframe_filename)
query_video_path = get_video_path_from_keyframe_filename(query_keyframe_filename)
query_title = get_clip_title_from_keyframe_filename(query_keyframe_filename)

st.header("Query Clip")
st.subheader(query_title)

# *** MODIFIED: Display Query Video by reading bytes ***
if os.path.exists(query_video_path):
    try:
        # Read the video file as bytes
        with open(query_video_path, 'rb') as f:
            video_bytes = f.read()

        # Pass bytes directly to st.video, specifying the format
        st.video(video_bytes, format='video/mp4')

    except FileNotFoundError:
        st.warning(f"Query video file not found at: {query_video_path}")
        if os.path.exists(query_keyframe_path):
             st.image(query_keyframe_path, caption="Query Keyframe (video file missing)", width=300)
    except Exception as e:
        st.error(f"Error reading or displaying video {os.path.basename(query_video_path)}: {e}")
        # Fallback to keyframe image if video bytes fail to load/display
        if os.path.exists(query_keyframe_path):
            st.image(query_keyframe_path, caption="Query Keyframe (video error)", width=300)
        else:
            st.warning("Query video and keyframe could not be loaded.")

elif os.path.exists(query_keyframe_path):
     # Video path didn't exist, but keyframe does
     st.warning(f"Query video not found at expected path: {query_video_path}")
     st.image(query_keyframe_path, caption="Query Keyframe (video missing)", width=300)
else:
    # Neither video nor keyframe path exists
    st.warning(f"Could not find query video or keyframe.\nVideo path: {query_video_path}\nKeyframe path: {query_keyframe_path}")


st.divider()

# --- Find and Display Similar Clips (Remains the same) ---
st.header("Similarity Matches")

similarity_results = find_similar_clips(embeddings[query_idx], embeddings, keyframe_filenames, NUM_RESULTS)
filtered_results = [res for res in similarity_results if res['index'] != query_idx][:NUM_RESULTS]

num_cols = 5
cols = st.columns(num_cols)

if not filtered_results:
    st.info("No other similar clips found.")

for i, result in enumerate(filtered_results):
    col = cols[i % num_cols]
    with col:
        res_keyframe_filename = result['keyframe_filename']
        res_keyframe_path = os.path.join(KEYFRAME_BASE_DIR, res_keyframe_filename)
        res_score = result['score']
        res_index = result['index']

        # Display result keyframe ONLY
        if os.path.exists(res_keyframe_path):
            st.image(res_keyframe_path, caption=f"Score: {res_score:.4f}", use_container_width=True)

            # Button to set this result as the new query
            button_key = f"query_btn_{res_index}"
            if st.button("Make Query", key=button_key):
                st.session_state['query_index'] = res_index
                st.rerun()
        else:
            col.warning(f"Missing keyframe: {os.path.basename(res_keyframe_path)}")
            st.caption(f"Score: {res_score:.4f} (Image missing)")
            button_key = f"query_btn_{res_index}"
            if st.button("Make Query", key=button_key):
                st.session_state['query_index'] = res_index
                st.rerun()

st.divider()
st.caption(f"Displaying {len(filtered_results)} results. Click 'Make Query' to explore.")
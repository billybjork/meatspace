import cv2
import numpy as np
import os
import argparse
from tqdm import tqdm
import subprocess
import tempfile
import shutil
import psycopg2
from psycopg2 import sql
from datetime import date
from pathlib import Path
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.db_utils import get_db_connection, get_media_base_dir

def calculate_histogram(frame, bins=256, ranges=[0, 256]):
    b, g, r = cv2.split(frame)
    hist_b = cv2.calcHist([b], [0], None, [bins], ranges)
    hist_g = cv2.calcHist([g], [0], None, [bins], ranges)
    hist_r = cv2.calcHist([r], [0], None, [bins], ranges)
    hist = np.concatenate((hist_b, hist_g, hist_r))
    cv2.normalize(hist, hist)
    return hist

def detect_scenes(video_path, threshold=0.6, hist_method=cv2.HISTCMP_CORREL):
    cap = cv2.VideoCapture(video_path)
    if not cap.isOpened():
        print(f"Error: Could not open video file: {video_path}")
        return None, None, None, None

    fps = cap.get(cv2.CAP_PROP_FPS)
    width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))

    print(f"Video Info: {width}x{height}, {fps:.2f} FPS, {total_frames} Frames")

    prev_hist = None
    cut_frames = [0]
    frame_number = 0

    print("Detecting scenes...")
    with tqdm(total=total_frames, desc="Analyzing Frames") as pbar:
        while True:
            ret, frame = cap.read()
            if not ret:
                break
            current_hist = calculate_histogram(frame)
            if prev_hist is not None:
                score = cv2.compareHist(prev_hist, current_hist, hist_method)
                is_cut = False
                if hist_method in [cv2.HISTCMP_CORREL, cv2.HISTCMP_INTERSECT]:
                    if score < threshold:
                        is_cut = True
                elif hist_method in [cv2.HISTCMP_CHISQR, cv2.HISTCMP_BHATTACHARYYA]:
                    if score > threshold:
                        is_cut = True
                else:  # Default comparison
                    if score < threshold:
                        is_cut = True
                if is_cut:
                    cut_frames.append(frame_number)
            prev_hist = current_hist
            frame_number += 1
            pbar.update(1)

    if total_frames > 0 and cut_frames[-1] != total_frames:
        cut_frames.append(total_frames)

    cap.release()

    scenes = []
    if len(cut_frames) > 1:
        for i in range(len(cut_frames) - 1):
            start_frame = cut_frames[i]
            end_frame_exclusive = cut_frames[i + 1]
            if start_frame < end_frame_exclusive:
                scenes.append((start_frame, end_frame_exclusive))

    print(f"Detected {len(scenes)} potential scenes.")
    return scenes, fps, (width, height), total_frames

def get_or_create_source_video(conn, abs_video_path, input_filename, fps, width, height, total_frames):
    """Finds or creates a source_video record and returns its ID and title."""
    media_base_dir = Path(get_media_base_dir())
    try:
        # Calculate relative path carefully
        abs_video_path_obj = Path(abs_video_path)
        rel_video_path = str(abs_video_path_obj.relative_to(media_base_dir))
    except ValueError:
         # If the file is not within the media base dir (e.g., input from elsewhere)
         print(f"Warning: Input video '{abs_video_path}' is not inside MEDIA_BASE_DIR '{media_base_dir}'. Storing absolute path in DB.")
         rel_video_path = str(abs_video_path) # Store absolute path as fallback


    duration_seconds = total_frames / fps if fps > 0 else 0
    # Use input filename as initial title if creating new record
    initial_title = Path(input_filename).stem

    source_video_id = None
    source_title = None

    with conn.cursor() as cur:
        # Try to find existing record by filepath (should be unique)
        cur.execute("SELECT id, title FROM source_videos WHERE filepath = %s", (rel_video_path,))
        result = cur.fetchone()
        if result:
            source_video_id, source_title = result
            print(f"Found existing source video record (ID: {source_video_id}, Title: '{source_title}') for filepath {rel_video_path}")
            # Ensure title is not NULL if fetched
            if not source_title:
                 print(f"Warning: Existing source video record {source_video_id} has NULL title. Using filename '{initial_title}' for processing.")
                 source_title = initial_title
        else:
            # Insert new record
            print(f"Creating new source video record for {input_filename}")
            try:
                # Insert with filepath, initial title, and other metadata
                cur.execute(
                    """
                    INSERT INTO source_videos (filepath, title, duration_seconds, fps, width, height, published_date, web_scraped, ingest_state)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'splicing') -- Set state as 'splicing' or similar if needed
                    RETURNING id, title;
                    """,
                    (rel_video_path, initial_title, duration_seconds, fps, width, height, None, False)
                )
                result = cur.fetchone()
                if result:
                    source_video_id, source_title = result
                    conn.commit() # Commit after successful insert
                    print(f"Created source video record with ID: {source_video_id}, Title: '{source_title}'")
                else:
                     print("Error: Failed to retrieve ID and title after insert.")
                     conn.rollback()
                     raise RuntimeError("Failed to create source video record.")

            except psycopg2.Error as e:
                conn.rollback()
                print(f"Error inserting source video record for {input_filename}: {e}")
                raise

    if not source_video_id or not source_title:
        raise RuntimeError("Could not get or create a valid source video record with ID and Title.")

    return source_video_id, source_title


def extract_clips(db_conn, source_video_id, source_title, abs_video_path, scenes, # Use source_title
                  output_subdir_relative, fps, frame_size, total_frames,        # Use relative subdir
                  ffmpeg_path="ffmpeg", crf=23, preset="medium", audio_bitrate="128k"):
    """
    Extracts clips directly using ffmpeg. Uses source_title for identifiers and paths.
    """
    if not scenes:
        print("No scenes detected or provided to extract.")
        return

    media_base_dir = Path(get_media_base_dir())
    # Create absolute output dir based on source_title and relative subdir structure
    # Example: MEDIA_BASE_DIR / clips / source_title_sanitized
    sanitized_title = "".join(c if c.isalnum() or c in ['-', '_'] else '_' for c in source_title) # Basic sanitization
    abs_output_dir = media_base_dir / output_subdir_relative / sanitized_title
    rel_output_dir_for_db = Path(output_subdir_relative) / sanitized_title # Path relative to media base dir

    abs_output_dir.mkdir(parents=True, exist_ok=True)
    print(f"Ensured output directory exists: {abs_output_dir}")


    if not shutil.which(ffmpeg_path):
        print(f"Error: ffmpeg not found at '{ffmpeg_path}' or in PATH.")
        return

    print(f"\nExtracting {len(scenes)} clips directly with ffmpeg using Source Title: '{source_title}'")

    # Use tqdm for progress bar over scenes
    for idx, scene in enumerate(tqdm(scenes, desc="Extracting Clips")):
        start_frame, end_frame_exclusive = scene
        # Generate clip identifier using sanitized source_title and scene index
        clip_identifier = f"{sanitized_title}_scene_{idx + 1:03d}" # Use sanitized title
        final_clip_filename = f"{clip_identifier}.mp4" # Filename based on identifier

        abs_final_clip_path = abs_output_dir / final_clip_filename
        # Calculate relative path for DB storage
        rel_final_clip_path = str(rel_output_dir_for_db / final_clip_filename)

        start_time_seconds = start_frame / fps if fps > 0 else 0
        end_time_seconds = end_frame_exclusive / fps if fps > 0 else 0
        duration_seconds = max(0, end_time_seconds - start_time_seconds) # Ensure non-negative duration

        if duration_seconds <= 0:
             print(f"\nSkipping scene {idx+1} (frames {start_frame}-{end_frame_exclusive-1}): zero or negative duration.")
             continue

        ffmpeg_cmd = [
            ffmpeg_path, '-y',                  # Overwrite output without asking
            '-ss', str(start_time_seconds),     # Accurate seek using time
            '-i', str(abs_video_path),          # Input file
            '-t', str(duration_seconds),        # Duration to extract
            '-c:v', 'libx264', '-preset', preset, '-crf', str(crf), '-pix_fmt', 'yuv420p', # Video codec
            '-map', '0:v:0?', '-map', '0:a:0?'   # Map first video and audio streams if they exist
        ]
        if audio_bitrate and audio_bitrate != '0':
            ffmpeg_cmd.extend(['-c:a', 'aac', '-b:a', audio_bitrate]) # Audio codec if requested
        else:
            ffmpeg_cmd.extend(['-an']) # No audio
        ffmpeg_cmd.extend(['-movflags', '+faststart', str(abs_final_clip_path)]) # Output path

        # print(f"\nExtracting {clip_identifier}: frames {start_frame} to {end_frame_exclusive - 1} "
        #       f"(Start time: {start_time_seconds:.2f}s, Duration: {duration_seconds:.2f}s)")
        try:
            # Use the helper function from intake.py if available, otherwise adapt subprocess call
            # For simplicity, we adapt the subprocess call here. Consider centralizing run_external_command.
            process = subprocess.run(ffmpeg_cmd, check=True, capture_output=True, text=True, encoding='utf-8')
            # print(f"Successfully extracted clip: {abs_final_clip_path}") # Logged by tqdm implicitly
        except subprocess.CalledProcessError as e:
            tqdm.write(f"\nError during ffmpeg extraction for {clip_identifier}:") # Use tqdm.write inside loop
            tqdm.write(f"Command: {' '.join(e.cmd)}")
            tqdm.write(f"FFmpeg stderr:\n{e.stderr}")
            continue # Skip DB insert for failed clip
        except Exception as e:
             tqdm.write(f"\nUnexpected error during extraction for {clip_identifier}: {e}")
             continue


        # --- Insert clip record into the database ---
        try:
            with db_conn.cursor() as cur:
                # Insert new clip with state 'pending_review'
                cur.execute(
                    """
                    INSERT INTO clips (source_video_id, clip_filepath, clip_identifier,
                                         start_frame, end_frame, start_time_seconds, end_time_seconds,
                                         ingest_state) -- Set initial state
                    VALUES (%s, %s, %s, %s, %s, %s, %s, 'pending_review')
                    ON CONFLICT (clip_identifier) DO UPDATE SET
                        source_video_id = EXCLUDED.source_video_id,
                        clip_filepath = EXCLUDED.clip_filepath,
                        start_frame = EXCLUDED.start_frame,
                        end_frame = EXCLUDED.end_frame,
                        start_time_seconds = EXCLUDED.start_time_seconds,
                        end_time_seconds = EXCLUDED.end_time_seconds,
                        ingest_state = 'pending_review', -- Ensure state is set on update too
                        updated_at = NOW(); -- Update timestamp
                    """,
                    (source_video_id, rel_final_clip_path, clip_identifier,
                     start_frame, end_frame_exclusive, start_time_seconds, end_time_seconds)
                )
            db_conn.commit()
            # print(f"Successfully inserted/updated clip record for {clip_identifier}") # Logged by tqdm implicitly
        except psycopg2.Error as e:
            db_conn.rollback()
            tqdm.write(f"\nError inserting clip record for {clip_identifier}: {e}")
            # Attempt to delete the orphaned file
            if abs_final_clip_path.exists():
                tqdm.write(f"  Deleting potentially orphaned file: {abs_final_clip_path}")
                try:
                    abs_final_clip_path.unlink()
                except OSError as del_e:
                    tqdm.write(f"  Warning: Failed to delete orphaned file: {del_e}")

    print("\nClip extraction process complete.")


def main():
    parser = argparse.ArgumentParser(description="Detect scenes, extract clips with audio, and record in database.")
    parser.add_argument("input_video", help="Path to the input video file.")
    # Output subdir is now relative to MEDIA_BASE_DIR/clips/<source_title>
    parser.add_argument("-o", "--output_base_subdir", default="clips",
                        help="Base subdirectory under MEDIA_BASE_DIR to save source-specific clip folders (default: clips).")
    # Scene detection arguments
    parser.add_argument("-t", "--threshold", type=float, default=0.6, help="Scene detection threshold (default: 0.6).")
    parser.add_argument("-m", "--method", type=str, default="CORREL",
                        choices=["CORREL", "CHISQR", "INTERSECT", "BHATTACHARYYA"],
                        help="Histogram comparison method (default: CORREL).")
    # FFmpeg arguments
    parser.add_argument("--ffmpeg_path", type=str, default="ffmpeg", help="Path to the ffmpeg executable.")
    parser.add_argument("--crf", type=int, default=23, help="H.264 CRF (default: 23).")
    parser.add_argument("--preset", type=str, default="medium",
                        choices=['ultrafast', 'superfast', 'veryfast', 'faster', 'fast', 'medium', 'slow', 'slower', 'veryslow'],
                        help="H.264 preset (default: medium).")
    parser.add_argument("--audio_bitrate", type=str, default="128k", help="AAC audio bitrate (default: 128k, '0' for none).")
    # Removed --use_temp_dir as it's not relevant for direct ffmpeg extraction

    args = parser.parse_args()

    abs_input_video = Path(args.input_video).resolve() # Use pathlib
    if not abs_input_video.is_file():
        print(f"Error: Input video file not found: {abs_input_video}")
        return

    input_filename = abs_input_video.name # Get filename

    # Map the method string to the corresponding OpenCV constant
    hist_methods = { "CORREL": cv2.HISTCMP_CORREL, "CHISQR": cv2.HISTCMP_CHISQR, "INTERSECT": cv2.HISTCMP_INTERSECT, "BHATTACHARYYA": cv2.HISTCMP_BHATTACHARYYA }
    hist_method_cv = hist_methods.get(args.method.upper())
    if hist_method_cv is None:
        print(f"Error: Invalid histogram comparison method '{args.method}'"); return

    conn = None
    try:
        conn = get_db_connection()

        # 1. Detect Scenes
        print(f"Detecting scenes for: {abs_input_video}")
        scenes, fps, frame_size, total_frames = detect_scenes(str(abs_input_video), args.threshold, hist_method_cv)

        if scenes is not None and fps is not None and frame_size is not None and total_frames is not None:
            # 2. Get or Create Source Video Record & Get Title
            print("Getting/Creating source video record...")
            source_video_id, source_title = get_or_create_source_video(
                conn, str(abs_input_video), input_filename, fps, frame_size[0], frame_size[1], total_frames
            )
            print(f"Using Source Video ID: {source_video_id}, Title: '{source_title}'")


            # 3. Extract Clips and Insert Records
            # Pass the base subdir (e.g., "clips") and the fetched source_title
            extract_clips(
                db_conn=conn,
                source_video_id=source_video_id,
                source_title=source_title, # Pass the fetched title
                abs_video_path=str(abs_input_video),
                scenes=scenes,
                output_subdir_relative=args.output_base_subdir, # Base dir like 'clips'
                fps=fps,
                frame_size=frame_size,
                total_frames=total_frames,
                ffmpeg_path=args.ffmpeg_path,
                crf=args.crf,
                preset=args.preset,
                audio_bitrate=args.audio_bitrate
            )
        else:
            print("Scene detection failed or video info invalid. Cannot extract clips.")

    except (ValueError, psycopg2.Error, RuntimeError) as e: # Added RuntimeError
        print(f"A critical error occurred: {e}")
        import traceback
        traceback.print_exc() # Print full traceback for debugging
        if conn: conn.rollback() # Rollback any potential partial transaction
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    main()
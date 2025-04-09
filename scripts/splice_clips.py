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

from db_utils import get_db_connection, get_media_base_dir

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

    prev_frame = None
    prev_hist = None
    cut_frames = [0]
    frame_number = 0

    print("Detecting scenes...")
    with tqdm(total=total_frames, desc="Analyzing Frames") as pbar:
        while True:
            ret, frame = cap.read()
            if not ret: break
            current_hist = calculate_histogram(frame)
            if prev_hist is not None:
                score = cv2.compareHist(prev_hist, current_hist, hist_method)
                is_cut = False
                if hist_method in [cv2.HISTCMP_CORREL, cv2.HISTCMP_INTERSECT]:
                    if score < threshold: is_cut = True
                elif hist_method in [cv2.HISTCMP_CHISQR, cv2.HISTCMP_BHATTACHARYYA]:
                     if score > threshold: is_cut = True
                else: # Defaulting
                     if score < threshold: is_cut = True
                if is_cut: cut_frames.append(frame_number)
            prev_hist = current_hist
            frame_number += 1
            pbar.update(1)

    if total_frames > 0 and cut_frames[-1] != total_frames: # Add final frame only if video has frames
         cut_frames.append(total_frames)

    cap.release()

    scenes = []
    if len(cut_frames) > 1:
        for i in range(len(cut_frames) - 1):
            start_frame = cut_frames[i]
            end_frame_exclusive = cut_frames[i+1]
            if start_frame < end_frame_exclusive:
                scenes.append((start_frame, end_frame_exclusive))

    print(f"Detected {len(scenes)} potential scenes.")
    return scenes, fps, (width, height), total_frames

def get_or_create_source_video(conn, abs_video_path, filename, fps, width, height, total_frames):
    """Finds or creates a source_video record and returns its ID."""
    duration_seconds = total_frames / fps if fps > 0 else 0
    rel_video_path = os.path.relpath(abs_video_path, get_media_base_dir())

    with conn.cursor() as cur:
        # Try to find existing record by absolute path
        cur.execute("SELECT id FROM source_videos WHERE filepath = %s", (rel_video_path,))
        result = cur.fetchone()
        if result:
            print(f"Found existing source video record (ID: {result[0]}) for {filename}")
            return result[0]
        else:
            # Insert new record
            print(f"Creating new source video record for {filename}")
            try:
                cur.execute(
                    """
                    INSERT INTO source_videos (filepath, filename, duration_seconds, fps, width, height, published_date, web_scraped)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id;
                    """,
                    (rel_video_path, filename, duration_seconds, fps, width, height, None, False) # Add logic for published_date/web_scraped if available
                )
                source_video_id = cur.fetchone()[0]
                conn.commit() # Commit after successful insert
                print(f"Created source video record with ID: {source_video_id}")
                return source_video_id
            except psycopg2.Error as e:
                conn.rollback() # Rollback on error
                print(f"Error inserting source video record for {filename}: {e}")
                raise

def extract_clips(db_conn, source_video_id, abs_video_path, source_filename_base, scenes,
                  output_subdir, # Relative subdir for clips (e.g., "LANDLINE_scenes")
                  fps, frame_size, total_frames,
                  ffmpeg_path="ffmpeg", crf=23, preset="medium", audio_bitrate="128k",
                  use_temp_dir=False):
    """
    Extracts clips, saves them, and inserts records into the 'clips' table.
    """
    if not scenes:
        print("No scenes detected or provided to extract.")
        return

    media_base_dir = get_media_base_dir()
    # Ensure the absolute output directory exists
    abs_output_dir = os.path.join(media_base_dir, output_subdir)
    if not os.path.exists(abs_output_dir):
        print(f"Creating output directory: {abs_output_dir}")
        os.makedirs(abs_output_dir)

    if shutil.which(ffmpeg_path) is None:
        print(f"Error: ffmpeg not found at '{ffmpeg_path}' or in PATH.")
        return

    intermediate_fourcc = cv2.VideoWriter_fourcc(*'mp4v')
    intermediate_ext = '.mp4'

    print(f"\nExtracting {len(scenes)} clips (Intermediate -> FFmpeg H.264)...")

    cap = cv2.VideoCapture(abs_video_path)
    if not cap.isOpened():
        print(f"Error: Could not re-open video file for extraction: {abs_video_path}")
        return

    current_scene_index = 0
    output_writer = None
    temp_clip_path = None
    frame_number = 0

    with tqdm(total=total_frames, desc="Extracting Clips") as pbar:
        while True:
            if current_scene_index >= len(scenes): break
            ret, frame = cap.read()
            if not ret: break

            if current_scene_index < len(scenes):
                start_frame, end_frame_exclusive = scenes[current_scene_index]

                if frame_number == start_frame:
                    # --- Generate Clip Identifier and Paths ---
                    # Use source filename base + scene index for identifier
                    clip_identifier = f"{source_filename_base}_scene_{current_scene_index + 1:03d}"
                    final_clip_filename = f"{clip_identifier}.mp4"
                    abs_final_clip_path = os.path.join(abs_output_dir, final_clip_filename)
                    rel_final_clip_path = os.path.join(output_subdir, final_clip_filename) # Relative path for DB

                    # Define temporary path
                    if use_temp_dir:
                        temp_dir = tempfile.gettempdir()
                        temp_clip_path = os.path.join(temp_dir, f"{clip_identifier}_temp{intermediate_ext}")
                    else:
                        temp_clip_path = os.path.join(abs_output_dir, f"{clip_identifier}_temp{intermediate_ext}")

                    print(f"\nStarting {clip_identifier} ({start_frame}-{end_frame_exclusive-1})")
                    print(f"  Final Output: {abs_final_clip_path}")

                    output_writer = cv2.VideoWriter(temp_clip_path, intermediate_fourcc, fps, frame_size)
                    if not output_writer.isOpened():
                        print(f"Error: Could not open intermediate VideoWriter for {temp_clip_path}")
                        output_writer = None; temp_clip_path = None
                        current_scene_index += 1; continue

                if output_writer is not None and frame_number < end_frame_exclusive:
                    output_writer.write(frame)

                if output_writer is not None and frame_number == end_frame_exclusive - 1:
                    print(f"  Finished writing intermediate frames for {clip_identifier}.")
                    output_writer.release(); output_writer = None
                    ffmpeg_success = False

                    if temp_clip_path and os.path.exists(temp_clip_path):
                        print(f"  Re-encoding to H.264/AAC using ffmpeg...")
                        ffmpeg_cmd = [
                            ffmpeg_path, '-y', # Overwrite output without asking
                            '-i', temp_clip_path, '-c:v', 'libx264', '-preset', preset,
                            '-crf', str(crf), '-pix_fmt', 'yuv420p'
                        ]
                        if audio_bitrate and audio_bitrate != '0':
                            ffmpeg_cmd.extend(['-c:a', 'aac', '-b:a', audio_bitrate])
                        else:
                            ffmpeg_cmd.extend(['-an'])
                        ffmpeg_cmd.extend(['-movflags', '+faststart', abs_final_clip_path])

                        try:
                            subprocess.run(ffmpeg_cmd, check=True, capture_output=True, text=True)
                            print(f"  Successfully encoded: {abs_final_clip_path}")
                            ffmpeg_success = True
                        except subprocess.CalledProcessError as e:
                            print(f"Error during ffmpeg re-encoding for {clip_identifier}:")
                            print(f"  Command: {' '.join(e.cmd)}")
                            print(f"  FFmpeg Stderr:\n{e.stderr}")
                            temp_clip_path = None # Keep intermediate file
                        except FileNotFoundError:
                             print(f"Error: '{ffmpeg_path}' command not found.")
                             cap.release(); return # Exit if ffmpeg is missing
                        finally:
                            if temp_clip_path and os.path.exists(temp_clip_path):
                                try: os.remove(temp_clip_path)
                                except OSError as e: print(f"Warning: Could not remove temp file {temp_clip_path}: {e}")

                    # --- Insert into Database if FFmpeg was successful ---
                    if ffmpeg_success:
                        start_time_seconds = start_frame / fps if fps > 0 else 0
                        end_time_seconds = end_frame_exclusive / fps if fps > 0 else 0
                        try:
                            with db_conn.cursor() as cur:
                                cur.execute(
                                    """
                                    INSERT INTO clips (source_video_id, clip_filepath, clip_identifier,
                                                     start_frame, end_frame, start_time_seconds, end_time_seconds)
                                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                                    ON CONFLICT (clip_identifier) DO UPDATE SET -- Or DO NOTHING if you prefer
                                        source_video_id = EXCLUDED.source_video_id,
                                        clip_filepath = EXCLUDED.clip_filepath,
                                        start_frame = EXCLUDED.start_frame,
                                        end_frame = EXCLUDED.end_frame,
                                        start_time_seconds = EXCLUDED.start_time_seconds,
                                        end_time_seconds = EXCLUDED.end_time_seconds;
                                    """,
                                    (source_video_id, rel_final_clip_path, clip_identifier,
                                     start_frame, end_frame_exclusive, start_time_seconds, end_time_seconds)
                                )
                            db_conn.commit()
                            print(f"  Successfully inserted/updated clip record for {clip_identifier}")
                        except psycopg2.Error as e:
                            db_conn.rollback()
                            print(f"Error inserting clip record for {clip_identifier}: {e}")
                            # Decide how to handle: maybe delete the generated file?
                            if os.path.exists(abs_final_clip_path):
                                print(f"  Deleting potentially orphaned file: {abs_final_clip_path}")
                                try: os.remove(abs_final_clip_path)
                                except OSError as del_e: print(f"  Warning: Failed to delete orphaned file: {del_e}")

                    # Move to the next scene
                    current_scene_index += 1
                    temp_clip_path = None

            frame_number += 1
            pbar.update(1)

    # Final cleanup
    if output_writer is not None:
        print(f"Warning: Video ended mid-scene. Releasing writer.")
        output_writer.release()
        if temp_clip_path and os.path.exists(temp_clip_path):
             print(f"  Discarding incomplete intermediate file: {temp_clip_path}")
             try: os.remove(temp_clip_path)
             except OSError as e: print(f"Warning: Could not remove incomplete file {temp_clip_path}: {e}")

    cap.release()
    print("\nExtraction process complete.")

def main():
    parser = argparse.ArgumentParser(description="Detect scenes, extract clips, and record in database.")
    parser.add_argument("input_video", help="Path to the input video file.")
    parser.add_argument("-o", "--output_subdir", default="extracted_clips",
                        help="Relative subdirectory under MEDIA_BASE_DIR to save clips (default: extracted_clips).")
    # Scene detection args
    parser.add_argument("-t", "--threshold", type=float, default=0.6, help="Scene detection threshold (default: 0.6).")
    parser.add_argument("-m", "--method", type=str, default="CORREL", choices=["CORREL", "CHISQR", "INTERSECT", "BHATTACHARYYA"], help="Histogram comparison method (default: CORREL).")
    # FFmpeg args
    parser.add_argument("--ffmpeg_path", type=str, default="ffmpeg", help="Path to the ffmpeg executable.")
    parser.add_argument("--crf", type=int, default=23, help="H.264 CRF (default: 23).")
    parser.add_argument("--preset", type=str, default="medium", choices=['ultrafast', 'superfast', 'veryfast', 'faster', 'fast', 'medium', 'slow', 'slower', 'veryslow'], help="H.264 preset (default: medium).")
    parser.add_argument("--audio_bitrate", type=str, default="128k", help="AAC audio bitrate (default: 128k, '0' for none).")
    parser.add_argument("--use_temp_dir", action='store_true', help="Use system temp dir for intermediate files.")

    args = parser.parse_args()

    # Validate input video path
    abs_input_video = os.path.abspath(args.input_video)
    if not os.path.isfile(abs_input_video):
        print(f"Error: Input video file not found: {abs_input_video}")
        return

    source_filename = os.path.basename(abs_input_video)
    source_filename_base = os.path.splitext(source_filename)[0] # Filename without extension

    # Map method string to OpenCV constant
    hist_methods = { "CORREL": cv2.HISTCMP_CORREL, "CHISQR": cv2.HISTCMP_CHISQR, "INTERSECT": cv2.HISTCMP_INTERSECT, "BHATTACHARYYA": cv2.HISTCMP_BHATTACHARYYA }
    hist_method_cv = hist_methods.get(args.method.upper())
    if hist_method_cv is None:
        print(f"Error: Invalid histogram comparison method '{args.method}'"); return

    # --- Database Connection ---
    conn = None
    try:
        conn = get_db_connection()

        # 1. Detect Scenes
        scenes, fps, frame_size, total_frames = detect_scenes(abs_input_video, args.threshold, hist_method_cv)

        if scenes is not None and fps is not None and frame_size is not None:
            # 2. Get or Create Source Video Record
            source_video_id = get_or_create_source_video(
                conn, abs_input_video, source_filename, fps, frame_size[0], frame_size[1], total_frames
            )

            # 3. Extract Clips and Insert Records
            extract_clips(
                db_conn=conn,
                source_video_id=source_video_id,
                abs_video_path=abs_input_video,
                source_filename_base=source_filename_base, # Pass base filename for identifier
                scenes=scenes,
                output_subdir=args.output_subdir, # Pass relative subdir
                fps=fps,
                frame_size=frame_size,
                total_frames=total_frames,
                ffmpeg_path=args.ffmpeg_path,
                crf=args.crf,
                preset=args.preset,
                audio_bitrate=args.audio_bitrate,
                use_temp_dir=args.use_temp_dir
            )
        else:
            print("Scene detection failed. Cannot extract clips.")

    except (ValueError, psycopg2.Error) as e:
        print(f"A critical error occurred: {e}")
        # No db actions needed if connection failed initially
        if conn: conn.rollback() # Rollback any potential partial transaction
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    main()
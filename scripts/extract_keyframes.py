import cv2
import os
import argparse
import glob
from tqdm import tqdm
import psycopg2
from psycopg2 import sql

from db_utils import get_db_connection, get_media_base_dir

def extract_and_save_frames(abs_video_path, clip_identifier, output_dir_abs, strategy='midpoint'):
    """
    Extracts keyframe(s), saves them, and returns relative paths.

    Args:
        abs_video_path (str): Absolute path to the input video clip file.
        clip_identifier (str): The logical identifier of the clip (e.g., Landline_scene_071).
        output_dir_abs (str): Absolute path to the directory to save frames.
        strategy (str): 'midpoint' or 'multi'.

    Returns:
        dict: Dictionary mapping strategy-based ID ('mid', '25pct', etc.) to relative file path.
              Returns empty dict on error.
    """
    saved_frame_rel_paths = {}
    try:
        cap = cv2.VideoCapture(abs_video_path)
        if not cap.isOpened():
            print(f"Error opening video file: {abs_video_path}")
            return saved_frame_rel_paths

        total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        if total_frames <= 0: # Check for 0 or negative frames
            print(f"Video file has no frames or is invalid: {abs_video_path}")
            cap.release()
            return saved_frame_rel_paths

        frame_indices_to_extract = []
        frame_ids = [] # Corresponding identifiers like 'mid', '25pct'

        if strategy == 'midpoint':
            frame_indices_to_extract.append(max(0, total_frames // 2)) # Ensure frame 0 if total_frames=1
            frame_ids.append("mid")
        elif strategy == 'multi':
            indices = [total_frames // 4, total_frames // 2, (total_frames * 3) // 4]
            # Handle edge cases like very short videos
            unique_indices = sorted(list(set(max(0, min(idx, total_frames - 1)) for idx in indices)))
            frame_indices_to_extract.extend(unique_indices)
            for idx in unique_indices:
                 percent = int(round((idx / max(1, total_frames-1)) * 100)) # Avoid division by zero
                 frame_ids.append(f"{percent}pct")
        else:
            print(f"Unknown strategy: {strategy}. Using midpoint.")
            frame_indices_to_extract.append(max(0, total_frames // 2))
            frame_ids.append("mid")

        # Ensure output directory exists (should be created by main, but double-check)
        os.makedirs(output_dir_abs, exist_ok=True)

        media_base_dir = get_media_base_dir()

        for i, frame_index in enumerate(frame_indices_to_extract):
            cap.set(cv2.CAP_PROP_POS_FRAMES, frame_index)
            ret, frame = cap.read()
            if ret:
                frame_tag = frame_ids[i] # Get the 'mid', '25pct' tag
                output_filename = f"{clip_identifier}_frame_{frame_tag}.jpg"
                output_path_abs = os.path.join(output_dir_abs, output_filename)

                # Calculate relative path for DB storage
                output_path_rel = os.path.relpath(output_path_abs, media_base_dir)

                cv2.imwrite(output_path_abs, frame)
                saved_frame_rel_paths[frame_tag] = output_path_rel
                # print(f"Saved keyframe: {output_path_abs}") # Optional: verbose logging
            else:
                print(f"Warning: Could not read frame {frame_index} from {abs_video_path}")

        cap.release()

    except Exception as e:
        print(f"An error occurred processing {abs_video_path}: {e}")
        if 'cap' in locals() and cap.isOpened():
            cap.release()

    return saved_frame_rel_paths

def main():
    parser = argparse.ArgumentParser(description="Extract keyframes from video clips listed in the database.")
    parser.add_argument("--output_subdir", required=True, help="Relative subdirectory under MEDIA_BASE_DIR to save keyframes.")
    parser.add_argument("--strategy", default="midpoint", choices=["midpoint", "multi"],
                        help="Keyframe extraction strategy ('midpoint' or 'multi').")
    parser.add_argument("--process-all", action="store_true", help="Process all clips, potentially overwriting existing keyframes.")
    parser.add_argument("--limit", type=int, default=None, help="Limit the number of clips to process.")

    args = parser.parse_args()

    conn = None
    updated_count = 0
    processed_count = 0
    try:
        conn = get_db_connection()
        media_base_dir = get_media_base_dir()
        output_dir_abs = os.path.join(media_base_dir, args.output_subdir)
        os.makedirs(output_dir_abs, exist_ok=True) # Ensure output dir exists

        with conn.cursor() as cur:
            # Query clips to process
            query = "SELECT id, clip_filepath, clip_identifier FROM clips"
            conditions = []
            params = []
            if not args.process_all:
                conditions.append("keyframe_filepath IS NULL")

            if conditions:
                query += " WHERE " + " AND ".join(conditions)

            query += " ORDER BY id" # Process in a consistent order

            if args.limit:
                query += " LIMIT %s"
                params.append(args.limit)

            print("Fetching clips from database...")
            cur.execute(query, params)
            clips_to_process = cur.fetchall()

            if not clips_to_process:
                print("No clips found matching the criteria.")
                return

            print(f"Found {len(clips_to_process)} clips to process.")

            for clip_id, rel_clip_path, clip_identifier in tqdm(clips_to_process, desc="Processing Clips"):
                processed_count += 1
                abs_clip_path = os.path.join(media_base_dir, rel_clip_path)

                if not os.path.isfile(abs_clip_path):
                    print(f"Warning: Clip file not found, skipping: {abs_clip_path} (Clip ID: {clip_id})")
                    continue

                # Extract frames and get relative paths
                relative_frame_paths = extract_and_save_frames(
                    abs_clip_path, clip_identifier, output_dir_abs, args.strategy
                )

                if relative_frame_paths:
                    # Decide which keyframe is the 'representative' one for the DB field
                    # For 'midpoint', it's 'mid'. For 'multi', let's choose the 50pct one if available, else the first one.
                    representative_key = 'mid'
                    if args.strategy == 'multi':
                       if '50pct' in relative_frame_paths:
                           representative_key = '50pct'
                       elif relative_frame_paths: # Get first available key if 50pct missing
                           representative_key = next(iter(relative_frame_paths))

                    if representative_key in relative_frame_paths:
                        representative_rel_path = relative_frame_paths[representative_key]
                        # Update the database
                        try:
                            cur.execute(
                                "UPDATE clips SET keyframe_filepath = %s WHERE id = %s",
                                (representative_rel_path, clip_id)
                            )
                            updated_count += 1
                        except psycopg2.Error as e:
                            print(f"\nError updating DB for clip ID {clip_id}: {e}")
                            conn.rollback() # Rollback this specific update attempt
                            # Continue to next clip, but maybe log failure
                        else:
                            conn.commit() # Commit successful update
                    else:
                         print(f"Warning: Could not determine representative keyframe for clip ID {clip_id}")

                else:
                     print(f"Warning: Failed to extract any keyframes for clip ID {clip_id} ({abs_clip_path})")

    except (ValueError, psycopg2.Error) as e:
        print(f"A critical error occurred: {e}")
        if conn: conn.rollback()
    finally:
        if conn:
            conn.close()
            print("\nDatabase connection closed.")
            print(f"Finished processing.")
            print(f"Clips attempted: {processed_count}")
            print(f"Database records updated: {updated_count}")

if __name__ == "__main__":
    main()
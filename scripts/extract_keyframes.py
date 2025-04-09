import cv2
import os
import argparse
import glob
from tqdm import tqdm
import psycopg2
from psycopg2 import sql

# Import helpers from db_utils
from db_utils import get_db_connection, get_media_base_dir

# --- MODIFIED extract_and_save_frames function ---
def extract_and_save_frames(abs_video_path, clip_identifier, source_identifier, output_dir_abs, strategy='midpoint'):
    """
    Extracts keyframe(s), saves them using the source_identifier in the filename,
    and returns relative paths.

    Args:
        abs_video_path (str): Absolute path to the input video clip file.
        clip_identifier (str): The logical identifier of the clip (e.g., Some_Long_Name_scene_123).
        source_identifier (str): The short identifier for the source (e.g., Landline).
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
        if total_frames <= 0:
            print(f"Video file has no frames or is invalid: {abs_video_path}")
            cap.release()
            return saved_frame_rel_paths

        frame_indices_to_extract = []
        frame_ids = []

        if strategy == 'midpoint':
            frame_indices_to_extract.append(max(0, total_frames // 2))
            frame_ids.append("mid")
        elif strategy == 'multi':
            indices = [total_frames // 4, total_frames // 2, (total_frames * 3) // 4]
            unique_indices = sorted(list(set(max(0, min(idx, total_frames - 1)) for idx in indices)))
            frame_indices_to_extract.extend(unique_indices)
            for idx in unique_indices:
                 percent = int(round((idx / max(1, total_frames-1)) * 100))
                 frame_ids.append(f"{percent}pct")
        else: # Default to midpoint
            frame_indices_to_extract.append(max(0, total_frames // 2))
            frame_ids.append("mid")

        os.makedirs(output_dir_abs, exist_ok=True)
        media_base_dir = get_media_base_dir()

        # --- Parse scene part from clip_identifier ---
        scene_part = None
        scene_part_index = clip_identifier.rfind('_scene_')
        if scene_part_index != -1:
            scene_part = clip_identifier[scene_part_index + 1:] # e.g., "scene_123"
        else:
            print(f"Warning: Could not parse 'scene_XXX' from clip_identifier '{clip_identifier}'. Will use full clip_identifier in filename.")
            # Use the full clip_identifier as a fallback if parsing fails
            scene_part = clip_identifier

        # --- Generate and Save Frames ---
        for i, frame_index in enumerate(frame_indices_to_extract):
            cap.set(cv2.CAP_PROP_POS_FRAMES, frame_index)
            ret, frame = cap.read()
            if ret:
                frame_tag = frame_ids[i]

                # --- Construct NEW output filename ---
                # Format: <SourceIdentifier>_<ScenePart>_frame_<FrameTag>.jpg
                output_filename = f"{source_identifier}_{scene_part}_frame_{frame_tag}.jpg"

                output_path_abs = os.path.join(output_dir_abs, output_filename)
                output_path_rel = os.path.relpath(output_path_abs, media_base_dir)

                cv2.imwrite(output_path_abs, frame)
                saved_frame_rel_paths[frame_tag] = output_path_rel
            else:
                print(f"Warning: Could not read frame {frame_index} from {abs_video_path}")
        cap.release()
    except Exception as e:
        print(f"An error occurred processing {abs_video_path}: {e}")
        if 'cap' in locals() and cap.isOpened():
            cap.release()
    return saved_frame_rel_paths
# --- End of extract_and_save_frames ---


def main():
    parser = argparse.ArgumentParser(description="Extract keyframes for clips from a specific source video, using source_identifier in filenames.")
    parser.add_argument("--source-identifier", required=True,
                        help="The unique identifier of the source video (from source_videos.source_identifier) to process clips for.")
    parser.add_argument("--keyframes-base-subdir", default="keyframes",
                        help="Base subdirectory under MEDIA_BASE_DIR to save keyframes into (e.g., 'keyframes').")
    parser.add_argument("--strategy", default="midpoint", choices=["midpoint", "multi"],
                        help="Keyframe extraction strategy ('midpoint' or 'multi').")
    parser.add_argument("--overwrite", action="store_true",
                        help="Overwrite existing keyframes and update DB path even if keyframe_filepath is already set.")
    parser.add_argument("--limit", type=int, default=None,
                        help="Limit the number of clips to process for this source.")

    args = parser.parse_args()

    conn = None
    updated_count = 0
    processed_count = 0
    skipped_existing = 0
    skipped_missing_file = 0

    try:
        conn = get_db_connection()
        media_base_dir = get_media_base_dir()

        output_dir_abs = os.path.join(media_base_dir, args.keyframes_base_subdir, args.source_identifier, args.strategy)
        print(f"Ensuring output directory exists: {output_dir_abs}")
        os.makedirs(output_dir_abs, exist_ok=True)

        with conn.cursor() as cur:
            print(f"Fetching clips for source_identifier: '{args.source_identifier}'...")
            query = sql.SQL("""
                SELECT c.id, c.clip_filepath, c.clip_identifier, c.keyframe_filepath
                FROM clips c
                JOIN source_videos sv ON c.source_video_id = sv.id
                WHERE sv.source_identifier = %s
                ORDER BY c.id
            """)
            params = [args.source_identifier]
            if args.limit:
                query += sql.SQL(" LIMIT %s")
                params.append(args.limit)

            cur.execute(query, params)
            clips_to_process = cur.fetchall()

            if not clips_to_process:
                print(f"No clips found for source identifier '{args.source_identifier}'.")
                return

            print(f"Found {len(clips_to_process)} clips to process for this source.")

            for clip_id, rel_clip_path, clip_identifier, current_keyframe_path in tqdm(clips_to_process, desc="Processing Clips"):
                processed_count += 1

                if not args.overwrite and current_keyframe_path is not None:
                    skipped_existing += 1
                    continue

                abs_clip_path = os.path.join(media_base_dir, rel_clip_path)
                if not os.path.isfile(abs_clip_path):
                    print(f"\nWarning: Clip file not found, skipping keyframe extraction: {abs_clip_path} (Clip ID: {clip_id})")
                    skipped_missing_file += 1
                    continue

                # --- MODIFIED Call: Pass source_identifier ---
                relative_frame_paths = extract_and_save_frames(
                    abs_clip_path,
                    clip_identifier,
                    args.source_identifier, # Pass the identifier here
                    output_dir_abs,
                    args.strategy
                )

                # --- (Database Update Logic Remains the Same) ---
                if relative_frame_paths:
                    representative_key = 'mid'
                    if args.strategy == 'multi':
                       representative_key = '50pct' if '50pct' in relative_frame_paths else next(iter(relative_frame_paths), None)

                    if representative_key and representative_key in relative_frame_paths:
                        representative_rel_path = relative_frame_paths[representative_key]
                        try:
                            cur.execute(
                                "UPDATE clips SET keyframe_filepath = %s WHERE id = %s",
                                (representative_rel_path, clip_id)
                            )
                            updated_count += 1
                        except psycopg2.Error as e:
                            print(f"\nError updating DB for clip ID {clip_id}: {e}")
                            conn.rollback()
                    else:
                         print(f"\nWarning: Could not determine representative keyframe for clip ID {clip_id}")
                else:
                     print(f"\nWarning: Failed to extract any keyframes for clip ID {clip_id} ({abs_clip_path})")

            # --- (Final Commit Logic Remains the Same) ---
            if updated_count > 0:
                 try:
                     conn.commit()
                     print(f"\nCommitted {updated_count} database updates.")
                 except psycopg2.Error as e:
                     print(f"\nError during final commit: {e}")
                     conn.rollback()

    except (ValueError, psycopg2.Error) as e:
        print(f"A critical error occurred: {e}")
        if conn: conn.rollback()
    finally:
        if conn:
            conn.close()
            print("\nDatabase connection closed.")
            print(f"\n--- Summary ---")
            print(f"Source Identifier: {args.source_identifier}")
            print(f"Strategy: {args.strategy}")
            print(f"Clips queried: {processed_count}")
            print(f"Skipped (already had keyframe): {skipped_existing}")
            print(f"Skipped (missing clip file): {skipped_missing_file}")
            print(f"Keyframes extracted & DB records updated: {updated_count}")

if __name__ == "__main__":
    main()
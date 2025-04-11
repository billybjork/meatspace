import cv2
import os
import argparse
from tqdm import tqdm
import psycopg2
from psycopg2 import sql
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.db_utils import get_db_connection, get_media_base_dir

# --- MODIFIED extract_and_save_frames function ---
def extract_and_save_frames(abs_video_path, clip_identifier, source_identifier, output_dir_abs, strategy='midpoint'):
    """
    Extracts keyframe(s), saves them using the source_identifier and strategy in the filename/path,
    and returns relative paths.

    Args:
        abs_video_path (str): Absolute path to the input video clip file.
        clip_identifier (str): The logical identifier of the clip (e.g., Some_Long_Name_scene_123).
        source_identifier (str): The short identifier for the source (e.g., Landline).
        output_dir_abs (str): Absolute path to the directory to save frames (already includes strategy).
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
        frame_ids = [] # Use consistent tags like '25pct', '50pct', '75pct', 'mid'

        if strategy == 'multi':
            # Calculate indices for 25%, 50%, 75%
            # Ensure indices are within bounds [0, total_frames - 1]
            idx_25 = max(0, total_frames // 4)
            idx_50 = max(0, total_frames // 2)
            idx_75 = max(0, (total_frames * 3) // 4)
            # Handle cases with very few frames where indices might be identical
            indices = sorted(list(set([idx_25, idx_50, idx_75]))) # Unique, sorted indices
            frame_indices_to_extract.extend(indices)
            # Create corresponding frame tags
            # Note: If indices collapsed (e.g., total_frames=2), tags might not be exact %
            # This logic assumes distinct indices for simplicity in naming.
            if len(indices) == 3:
                 frame_ids = ["25pct", "50pct", "75pct"]
            elif len(indices) == 2:
                 # Decide how to label if only two unique points (e.g., 25/50 and 75 collapse)
                 # Let's keep it simple: map to the intended % as best possible
                 frame_ids = []
                 if indices[0] == idx_25: frame_ids.append("25pct")
                 if indices[0] == idx_50 or indices[1] == idx_50: frame_ids.append("50pct")
                 if indices[1] == idx_75: frame_ids.append("75pct")
                 # Fallback if logic is complex
                 if len(frame_ids) != 2: frame_ids = ["start", "end"] # Or handle differently
            elif len(indices) == 1:
                frame_ids = ["50pct"] # Treat as midpoint if all collapse
            else: # Should not happen
                frame_ids = []

        else: # Default to midpoint (handles 'midpoint' explicitly)
            frame_indices_to_extract.append(max(0, total_frames // 2))
            frame_ids.append("mid")

        os.makedirs(output_dir_abs, exist_ok=True)
        media_base_dir = get_media_base_dir() # Assumes get_media_base_dir() is available

        # --- Parse scene part from clip_identifier ---
        scene_part = None
        # Try to find '_scene_' followed by digits
        import re
        match = re.search(r'_scene_(\d+)$', clip_identifier)
        if match:
            scene_part = f"scene_{match.group(1)}" # e.g., "scene_123"
        else:
            # Fallback: try to find the last underscore part if it looks like a scene number
            parts = clip_identifier.split('_')
            if len(parts) > 1 and parts[-1].isdigit():
                 scene_part = f"scene_{parts[-1]}"
            else:
                 print(f"Warning: Could not parse 'scene_XXX' from clip_identifier '{clip_identifier}'. Will use full clip_identifier (sanitized) in filename.")
                 # Sanitize clip_identifier for filename use (replace spaces, special chars)
                 sanitized_clip_id = re.sub(r'[^\w\-]+', '_', clip_identifier)
                 scene_part = sanitized_clip_id # Use sanitized ID if no scene number found


        # --- Generate and Save Frames ---
        frame_tags_map = dict(zip(frame_indices_to_extract, frame_ids)) # Map index to tag

        for frame_index in frame_indices_to_extract:
            cap.set(cv2.CAP_PROP_POS_FRAMES, frame_index)
            ret, frame = cap.read()
            if ret:
                frame_tag = frame_tags_map[frame_index]

                # --- Construct NEW output filename ---
                # Format: <SourceIdentifier>_<ScenePart>_frame_<FrameTag>.jpg
                # Example: Landline_scene_123_frame_50pct.jpg
                output_filename = f"{source_identifier}_{scene_part}_frame_{frame_tag}.jpg"

                output_path_abs = os.path.join(output_dir_abs, output_filename)
                # Calculate relative path based on MEDIA_BASE_DIR
                output_path_rel = os.path.relpath(output_path_abs, media_base_dir)

                cv2.imwrite(output_path_abs, frame)
                saved_frame_rel_paths[frame_tag] = output_path_rel # Store relative path with tag
            else:
                print(f"Warning: Could not read frame {frame_index} from {abs_video_path}")
        cap.release()
    except Exception as e:
        print(f"An error occurred processing {abs_video_path}: {e}")
        import traceback
        traceback.print_exc() # Print full traceback for debugging
        if 'cap' in locals() and cap and cap.isOpened():
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
    error_count = 0

    try:
        conn = get_db_connection()
        media_base_dir = get_media_base_dir()

        # --- Determine the final output directory based on strategy ---
        output_dir_abs = os.path.join(media_base_dir, args.keyframes_base_subdir, args.source_identifier, args.strategy)
        print(f"Ensuring output directory exists: {output_dir_abs}")
        os.makedirs(output_dir_abs, exist_ok=True)

        with conn.cursor() as cur:
            print(f"Fetching clips for source_identifier: '{args.source_identifier}'...")
            # Fetch clip_filepath and clip_identifier, which are needed for processing
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

            for clip_id, rel_clip_path, clip_identifier, current_keyframe_path in tqdm(clips_to_process, desc=f"Extracting {args.strategy} Keyframes"):
                processed_count += 1

                # Skip if not overwriting and a keyframe already exists.
                # NOTE: This check is simple. It doesn't verify if the *correct type*
                # of keyframe exists (e.g., 'midpoint' vs 'multi'). Overwrite might be needed
                # if changing strategies for existing clips.
                if not args.overwrite and current_keyframe_path is not None:
                    # More robust check: See if the current path matches the expected pattern for this strategy
                    expected_dir_part = os.path.join(args.keyframes_base_subdir, args.source_identifier, args.strategy)
                    if current_keyframe_path and expected_dir_part in current_keyframe_path.replace("\\", "/"):
                       skipped_existing += 1
                       continue
                    else:
                       print(f"\nInfo: Clip ID {clip_id} has existing keyframe from different strategy/path. Processing due to path mismatch or --overwrite.")


                if not rel_clip_path:
                    print(f"\nWarning: Clip ID {clip_id} has NULL clip_filepath. Skipping.")
                    error_count += 1
                    continue

                abs_clip_path = os.path.join(media_base_dir, rel_clip_path)
                if not os.path.isfile(abs_clip_path):
                    print(f"\nWarning: Clip file not found, skipping keyframe extraction: {abs_clip_path} (Clip ID: {clip_id})")
                    skipped_missing_file += 1
                    continue

                # --- Call extraction function ---
                relative_frame_paths = extract_and_save_frames(
                    abs_clip_path,
                    clip_identifier,
                    args.source_identifier, # Pass the source identifier
                    output_dir_abs,         # Pass the strategy-specific absolute output dir
                    args.strategy
                )

                # --- Determine representative keyframe and update DB ---
                if relative_frame_paths:
                    # Choose the representative keyframe to store in DB
                    representative_key = None
                    if args.strategy == 'multi':
                        # Prefer '50pct', fallback to others if needed
                        if '50pct' in relative_frame_paths:
                           representative_key = '50pct'
                        elif 'mid' in relative_frame_paths: # Fallback if 50pct wasn't generated for some reason
                           representative_key = 'mid'
                        else: # Pick the first one available as last resort
                           representative_key = next(iter(relative_frame_paths), None)
                    elif args.strategy == 'midpoint':
                        representative_key = 'mid'
                    # Add elif for future strategies

                    if representative_key and representative_key in relative_frame_paths:
                        representative_rel_path = relative_frame_paths[representative_key]
                        # Update DB only if the path has changed or if overwriting
                        if args.overwrite or representative_rel_path != current_keyframe_path:
                            try:
                                cur.execute(
                                    sql.SQL("UPDATE clips SET keyframe_filepath = %s WHERE id = %s"),
                                    (representative_rel_path, clip_id)
                                )
                                # No need to commit here, commit once at the end
                                updated_count += 1
                            except psycopg2.Error as e:
                                print(f"\nError updating DB for clip ID {clip_id}: {e}")
                                conn.rollback() # Rollback this specific error? Or let final commit fail? Let's log and continue.
                                error_count += 1
                        # else: # Path is the same, no update needed unless overwrite forces it
                            # pass # Implicitly handled by not executing update
                    else:
                         print(f"\nWarning: Could not determine representative keyframe for clip ID {clip_id} from tags: {list(relative_frame_paths.keys())}")
                         error_count += 1
                else:
                     print(f"\nWarning: Failed to extract any keyframes for clip ID {clip_id} ({abs_clip_path})")
                     error_count += 1 # Count as an error/failure

            # --- Final Commit ---
            if updated_count > 0:
                 try:
                     conn.commit()
                     print(f"\nCommitted {updated_count} database updates.")
                 except psycopg2.Error as e:
                     print(f"\nError during final commit: {e}")
                     conn.rollback()
                     error_count += 1 # Treat commit failure as an error

    except (ValueError, psycopg2.Error) as e:
        print(f"A critical error occurred: {e}")
        import traceback
        traceback.print_exc()
        if conn: conn.rollback()
        error_count += 1 # Count critical errors
    finally:
        if conn:
            conn.close()
            print("\nDatabase connection closed.")
        print(f"\n--- Summary ---")
        print(f"Source Identifier: {args.source_identifier}")
        print(f"Strategy: {args.strategy}")
        print(f"Clips queried: {processed_count}")
        print(f"Skipped (existing matching keyframe path): {skipped_existing}")
        print(f"Skipped (missing clip file): {skipped_missing_file}")
        print(f"DB records updated/committed: {updated_count}")
        print(f"Errors/Warnings encountered: {error_count}")
        print(f"Keyframes saved to directory: {output_dir_abs}")

if __name__ == "__main__":
    main()
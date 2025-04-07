import cv2
import os
import argparse
import glob
from tqdm import tqdm

def extract_frames(video_path, output_dir, strategy='midpoint'):
    """
    Extracts keyframe(s) from a single video file based on the strategy.

    Args:
        video_path (str): Path to the input video file.
        output_dir (str): Directory to save the extracted frame images.
        strategy (str): 'midpoint' or 'multi' (for 25%, 50%, 75%).

    Returns:
        list: List of paths to the saved keyframe images.
    """
    saved_frame_paths = []
    try:
        cap = cv2.VideoCapture(video_path)
        if not cap.isOpened():
            print(f"Error opening video file: {video_path}")
            return saved_frame_paths

        total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        if total_frames == 0:
            print(f"Video file has no frames or is corrupted: {video_path}")
            cap.release()
            return saved_frame_paths

        video_filename = os.path.splitext(os.path.basename(video_path))[0]
        frame_indices_to_extract = []

        if strategy == 'midpoint':
            frame_indices_to_extract.append(total_frames // 2)
        elif strategy == 'multi':
            frame_indices_to_extract.append(total_frames // 4)
            frame_indices_to_extract.append(total_frames // 2)
            frame_indices_to_extract.append((total_frames * 3) // 4)
        else:
            print(f"Unknown strategy: {strategy}. Using midpoint.")
            frame_indices_to_extract.append(total_frames // 2)

        # Remove duplicates and ensure indices are within bounds
        frame_indices_to_extract = sorted(list(set(frame_indices_to_extract)))
        frame_indices_to_extract = [max(0, min(idx, total_frames - 1)) for idx in frame_indices_to_extract]


        for i, frame_index in enumerate(frame_indices_to_extract):
            cap.set(cv2.CAP_PROP_POS_FRAMES, frame_index)
            ret, frame = cap.read()
            if ret:
                # Construct frame identifier based on strategy and index
                if strategy == 'midpoint':
                    frame_id = "frame_mid"
                elif strategy == 'multi':
                    percent = int(round((frame_index / total_frames) * 100))
                    frame_id = f"frame_{percent}pct"
                else: # Default fallback
                     frame_id = f"frame_{frame_index}"

                output_filename = f"{video_filename}_{frame_id}.jpg"
                output_path = os.path.join(output_dir, output_filename)
                cv2.imwrite(output_path, frame)
                saved_frame_paths.append(output_path)
            else:
                print(f"Warning: Could not read frame {frame_index} from {video_path}")

        cap.release()

    except Exception as e:
        print(f"An error occurred processing {video_path}: {e}")
        if 'cap' in locals() and cap.isOpened():
            cap.release() # Ensure cap is released on error

    return saved_frame_paths

def main():
    parser = argparse.ArgumentParser(description="Extract keyframes from videos.")
    parser.add_argument("--video_dir", required=True, help="Directory containing video files.")
    parser.add_argument("--output_dir", required=True, help="Directory to save extracted keyframes.")
    parser.add_argument("--strategy", default="midpoint", choices=["midpoint", "multi"],
                        help="Keyframe extraction strategy ('midpoint' or 'multi' for 25/50/75 pct).")
    parser.add_argument("--video_ext", default="mp4", help="Video file extension (e.g., mp4, mov, avi).")

    args = parser.parse_args()

    # Create output directory if it doesn't exist
    os.makedirs(args.output_dir, exist_ok=True)

    # Find video files
    search_pattern = os.path.join(args.video_dir, f"*.{args.video_ext}")
    video_files = glob.glob(search_pattern)

    if not video_files:
        print(f"No videos found with extension .{args.video_ext} in {args.video_dir}")
        return

    print(f"Found {len(video_files)} video files. Starting keyframe extraction...")

    all_extracted_frames = []
    for video_path in tqdm(video_files, desc="Processing Videos"):
        extracted = extract_frames(video_path, args.output_dir, args.strategy)
        all_extracted_frames.extend(extracted)

    print(f"\nFinished extraction. Saved {len(all_extracted_frames)} keyframes to {args.output_dir}")

if __name__ == "__main__":
    main()
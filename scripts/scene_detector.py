import cv2
import numpy as np
import os
import argparse
from tqdm import tqdm

def calculate_histogram(frame, bins=256, ranges=[0, 256]):
    """Calculates the BGR color histogram for a frame."""
    # Split channels
    b, g, r = cv2.split(frame)
    # Calculate histogram for each channel
    hist_b = cv2.calcHist([b], [0], None, [bins], ranges)
    hist_g = cv2.calcHist([g], [0], None, [bins], ranges)
    hist_r = cv2.calcHist([r], [0], None, [bins], ranges)
    # Concatenate histograms and normalize
    hist = np.concatenate((hist_b, hist_g, hist_r))
    cv2.normalize(hist, hist) # Normalize for better comparison
    return hist

def detect_scenes(video_path, threshold=0.6, hist_method=cv2.HISTCMP_CORREL):
    """
    Detects scene cuts in a video based on histogram comparison.

    Args:
        video_path (str): Path to the input video file.
        threshold (float): Threshold for scene change detection.
                           - For HISTCMP_CORREL/HISTCMP_INTERSECT: Lower value means more cuts.
                           - For HISTCMP_CHISQR/HISTCMP_BHATTACHARYYA: Higher value means more cuts.
                           Default is 0.6 for HISTCMP_CORREL.
        hist_method (int): OpenCV histogram comparison method (e.g., cv2.HISTCMP_CORREL).

    Returns:
        list: A list of tuples, where each tuple contains the (start_frame, end_frame)
              indices for a detected scene.
        float: The frames per second (FPS) of the input video.
        tuple: The (width, height) of the video frames.
        int: The total number of frames in the video.
    """
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
    cut_frames = [0]  # Start with the first frame as the beginning of the first scene
    frame_number = 0

    print("Detecting scenes...")
    with tqdm(total=total_frames, desc="Analyzing Frames") as pbar:
        while True:
            ret, frame = cap.read()
            if not ret:
                break # End of video

            # --- Preprocessing (Optional but can help) ---
            # Convert to grayscale if color is not important or causing issues
            # gray_frame = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
            # Or apply some blur to reduce noise
            # frame = cv2.GaussianBlur(frame, (5, 5), 0)
            # ---------------------------------------------

            current_hist = calculate_histogram(frame)

            if prev_hist is not None:
                # Compare histograms
                score = cv2.compareHist(prev_hist, current_hist, hist_method)

                # --- Determine Cut based on Method and Threshold ---
                # Correlation (HISTCMP_CORREL) or Intersection (HISTCMP_INTERSECT):
                # High score = similar. Low score = different. Cut if score < threshold.
                is_cut = False
                if hist_method in [cv2.HISTCMP_CORREL, cv2.HISTCMP_INTERSECT]:
                    if score < threshold:
                        is_cut = True
                # Chi-Squared (HISTCMP_CHISQR) or Bhattacharyya (HISTCMP_BHATTACHARYYA):
                # Low score = similar. High score = different. Cut if score > threshold.
                elif hist_method in [cv2.HISTCMP_CHISQR, cv2.HISTCMP_BHATTACHARYYA]:
                     if score > threshold:
                         is_cut = True
                else:
                     print(f"Warning: Unsupported histogram comparison method: {hist_method}")
                     # Defaulting to Correlation logic
                     if score < threshold:
                         is_cut = True

                if is_cut:
                    # print(f"Cut detected between frame {frame_number-1} and {frame_number} (Score: {score:.4f})")
                    cut_frames.append(frame_number)

            prev_frame = frame
            prev_hist = current_hist
            frame_number += 1
            pbar.update(1)

    # Add the very last frame index to mark the end of the last scene
    if cut_frames[-1] != total_frames:
         cut_frames.append(total_frames) # Use total_frames as the exclusive end

    cap.release()

    # Create scene list (start_frame, end_frame)
    scenes = []
    if len(cut_frames) > 1:
        for i in range(len(cut_frames) - 1):
            start_frame = cut_frames[i]
            # End frame is exclusive for range, but inclusive for cv2 reading loop later
            # So the last frame *in* the scene is cut_frames[i+1] - 1
            end_frame_exclusive = cut_frames[i+1]
            # Ensure start frame is strictly less than end frame
            if start_frame < end_frame_exclusive:
                 # Store as (inclusive_start, exclusive_end) for consistency
                scenes.append((start_frame, end_frame_exclusive))

    print(f"Detected {len(scenes)} potential scenes.")
    return scenes, fps, (width, height), total_frames

def extract_clips(video_path, scenes, output_dir, fps, frame_size, total_frames, output_format="mp4"):
    """
    Extracts detected scenes into separate video files.

    Args:
        video_path (str): Path to the input video file.
        scenes (list): List of (start_frame, end_frame_exclusive) tuples.
        output_dir (str): Directory to save the output clips.
        fps (float): Frames per second for the output videos.
        frame_size (tuple): (width, height) for the output videos.
        total_frames (int): Total frames in the original video (for progress bar).
        output_format (str): Output video format ('mp4', 'avi', etc.).
    """
    if not scenes:
        print("No scenes detected or provided to extract.")
        return

    if not os.path.exists(output_dir):
        print(f"Creating output directory: {output_dir}")
        os.makedirs(output_dir)

    # Choose a FourCC code based on the output format
    if output_format.lower() == 'mp4':
        fourcc = cv2.VideoWriter_fourcc(*'mp4v') # Common for .mp4
    elif output_format.lower() == 'avi':
        fourcc = cv2.VideoWriter_fourcc(*'XVID') # Common for .avi
    else:
        print(f"Warning: Unsupported output format '{output_format}'. Defaulting to mp4v.")
        fourcc = cv2.VideoWriter_fourcc(*'mp4v')
        output_format = 'mp4' # Ensure correct extension

    print(f"\nExtracting {len(scenes)} clips...")

    # We need to iterate through the *original* video's frames
    cap = cv2.VideoCapture(video_path)
    if not cap.isOpened():
        print(f"Error: Could not re-open video file for extraction: {video_path}")
        return

    current_scene_index = 0
    output_writer = None
    frame_number = 0

    with tqdm(total=total_frames, desc="Extracting Clips") as pbar:
        while True:
            ret, frame = cap.read()
            if not ret:
                break # End of video

            # Check if the current frame belongs to a scene we need to write
            if current_scene_index < len(scenes):
                start_frame, end_frame_exclusive = scenes[current_scene_index]

                # If this frame is the start of the *current* target scene
                if frame_number == start_frame:
                    clip_filename = f"scene_{current_scene_index + 1:03d}.{output_format}"
                    clip_path = os.path.join(output_dir, clip_filename)
                    print(f"Starting scene {current_scene_index + 1} ({start_frame}-{end_frame_exclusive-1}) -> {clip_path}")
                    output_writer = cv2.VideoWriter(clip_path, fourcc, fps, frame_size)
                    if not output_writer.isOpened():
                        print(f"Error: Could not open VideoWriter for {clip_path}")
                        # Attempt to continue to the next scene if possible
                        output_writer = None
                        current_scene_index += 1 # Skip this scene
                        continue # Move to next frame


                # If we are currently writing a scene and haven't reached its end
                if output_writer is not None and frame_number < end_frame_exclusive:
                    output_writer.write(frame)

                # If this frame is the *end* of the current scene (exclusive index)
                if output_writer is not None and frame_number == end_frame_exclusive - 1:
                    print(f"Finished scene {current_scene_index + 1}")
                    output_writer.release()
                    output_writer = None
                    current_scene_index += 1 # Move to the next scene

            frame_number += 1
            pbar.update(1)

            # Optimization: If we've finished all scenes, we can stop reading
            if current_scene_index >= len(scenes):
                 print("All target scenes extracted.")
                 break


    # Release resources
    if output_writer is not None: # Handle case where video ends mid-scene
        print(f"Warning: Video ended before finishing scene {current_scene_index + 1}. Closing clip.")
        output_writer.release()
    cap.release()
    cv2.destroyAllWindows() # Clean up any OpenCV windows if they were used

    print("\nExtraction complete.")


def main():
    parser = argparse.ArgumentParser(description="Detect scenes in a video and extract them as clips.")
    parser.add_argument("input_video", help="Path to the input video file.")
    parser.add_argument("-o", "--output_dir", default="output_clips",
                        help="Directory to save the extracted clips (default: output_clips).")
    parser.add_argument("-t", "--threshold", type=float, default=0.6,
                        help="Scene detection threshold (default: 0.6 for Correlation). "
                             "Lower for CORREL/INTERSECT means more sensitive (more cuts). "
                             "Higher for CHISQR/BHATTACHARYYA means more sensitive.")
    parser.add_argument("-m", "--method", type=str, default="CORREL",
                        choices=["CORREL", "CHISQR", "INTERSECT", "BHATTACHARYYA"],
                        help="Histogram comparison method (default: CORREL).")
    parser.add_argument("-f", "--format", type=str, default="mp4", choices=["mp4", "avi"],
                        help="Output video format for clips (default: mp4).")

    args = parser.parse_args()

    # Map method string to OpenCV constant
    hist_methods = {
        "CORREL": cv2.HISTCMP_CORREL,
        "CHISQR": cv2.HISTCMP_CHISQR,
        "INTERSECT": cv2.HISTCMP_INTERSECT,
        "BHATTACHARYYA": cv2.HISTCMP_BHATTACHARYYA
    }
    hist_method_cv = hist_methods.get(args.method.upper())
    if hist_method_cv is None:
        print(f"Error: Invalid histogram comparison method '{args.method}'")
        return

    # 1. Detect Scenes
    scenes, fps, frame_size, total_frames = detect_scenes(args.input_video, args.threshold, hist_method_cv)

    # 2. Extract Clips
    if scenes is not None and fps is not None and frame_size is not None:
        extract_clips(args.input_video, scenes, args.output_dir, fps, frame_size, total_frames, args.format)
    else:
        print("Scene detection failed. Cannot extract clips.")

if __name__ == "__main__":
    main()
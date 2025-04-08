import cv2
import numpy as np
import os
import argparse
from tqdm import tqdm
import subprocess # <-- Import subprocess
import tempfile # <-- For creating temporary files safely
import shutil # <-- For finding ffmpeg executable

# --- (calculate_histogram and detect_scenes remain the same) ---
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
    (Code is identical to your original version - keeping it for context)
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
                    if score < threshold: is_cut = True
                elif hist_method in [cv2.HISTCMP_CHISQR, cv2.HISTCMP_BHATTACHARYYA]:
                     if score > threshold: is_cut = True
                else:
                     print(f"Warning: Unsupported histogram comparison method: {hist_method}")
                     if score < threshold: is_cut = True # Defaulting

                if is_cut:
                    cut_frames.append(frame_number)

            prev_frame = frame
            prev_hist = current_hist
            frame_number += 1
            pbar.update(1)

    if cut_frames[-1] != total_frames:
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

# --- MODIFIED extract_clips ---
def extract_clips(video_path, scenes, output_dir, fps, frame_size, total_frames,
                  ffmpeg_path="ffmpeg", # Allow specifying ffmpeg path
                  crf=23, preset="medium", audio_bitrate="128k",
                  use_temp_dir=False): # Option for temp file location
    """
    Extracts detected scenes using cv2.VideoWriter for frame gathering
    and then re-encodes using ffmpeg for web-compatible H.264/AAC output.

    Args:
        video_path (str): Path to the input video file.
        scenes (list): List of (start_frame, end_frame_exclusive) tuples.
        output_dir (str): Directory to save the final output clips.
        fps (float): Frames per second for the output videos.
        frame_size (tuple): (width, height) for the output videos.
        total_frames (int): Total frames in the original video.
        ffmpeg_path (str): Path to the ffmpeg executable.
        crf (int): Constant Rate Factor for H.264 (lower=better quality, larger file).
        preset (str): H.264 encoding preset (e.g., 'medium', 'slow', 'fast').
        audio_bitrate (str): Target audio bitrate (e.g., '128k'). Use '0' or None for no audio.
        use_temp_dir (bool): If True, create intermediate files in the system temp dir,
                             otherwise create them alongside final output before moving.
    """
    if not scenes:
        print("No scenes detected or provided to extract.")
        return

    if not os.path.exists(output_dir):
        print(f"Creating output directory: {output_dir}")
        os.makedirs(output_dir)

    # Check if ffmpeg is accessible
    if shutil.which(ffmpeg_path) is None:
        print(f"Error: ffmpeg executable not found at '{ffmpeg_path}' or in system PATH.")
        print("Please install ffmpeg or provide the correct path via --ffmpeg_path.")
        return

    # Use 'mp4v' for the intermediate OpenCV write step, as it's generally reliable *for writing*.
    # The crucial step is the ffmpeg re-encode afterwards.
    intermediate_fourcc = cv2.VideoWriter_fourcc(*'mp4v')
    intermediate_ext = '.mp4' # Keep extension consistent for intermediate step

    print(f"\nExtracting {len(scenes)} clips (Intermediate -> FFmpeg H.264)...")

    cap = cv2.VideoCapture(video_path)
    if not cap.isOpened():
        print(f"Error: Could not re-open video file for extraction: {video_path}")
        return

    current_scene_index = 0
    output_writer = None
    temp_clip_path = None
    frame_number = 0

    # Use a context manager for the progress bar
    with tqdm(total=total_frames, desc="Extracting Clips") as pbar:
        while True:
            # Check if we can stop early
            if current_scene_index >= len(scenes):
                print("All target scenes extracted.")
                break # Exit outer loop

            ret, frame = cap.read()
            if not ret:
                print("End of input video reached.")
                break # End of video

            # Logic for handling the current frame based on scenes
            if current_scene_index < len(scenes):
                start_frame, end_frame_exclusive = scenes[current_scene_index]

                # --- Start of a new scene ---
                if frame_number == start_frame:
                    base_filename = f"scene_{current_scene_index + 1:03d}"
                    final_clip_path = os.path.join(output_dir, f"{base_filename}.mp4") # Final is always .mp4

                    # Define temporary path
                    if use_temp_dir:
                        # Create in system temp directory
                        temp_dir = tempfile.gettempdir()
                        temp_clip_path = os.path.join(temp_dir, f"{base_filename}_temp{intermediate_ext}")
                    else:
                         # Create alongside final output
                        temp_clip_path = os.path.join(output_dir, f"{base_filename}_temp{intermediate_ext}")

                    print(f"\nStarting scene {current_scene_index + 1} ({start_frame}-{end_frame_exclusive-1})")
                    print(f"  Intermediate: {temp_clip_path}")
                    print(f"  Final Output: {final_clip_path}")

                    output_writer = cv2.VideoWriter(temp_clip_path, intermediate_fourcc, fps, frame_size)
                    if not output_writer.isOpened():
                        print(f"Error: Could not open intermediate VideoWriter for {temp_clip_path}")
                        output_writer = None
                        temp_clip_path = None # Ensure we don't try to process it
                        current_scene_index += 1 # Skip this scene
                        continue # Move to next frame

                # --- Write frame to current scene (if writer is open) ---
                if output_writer is not None and frame_number < end_frame_exclusive:
                    output_writer.write(frame)

                # --- End of the current scene ---
                if output_writer is not None and frame_number == end_frame_exclusive - 1:
                    print(f"  Finished writing intermediate frames for scene {current_scene_index + 1}.")
                    output_writer.release()
                    output_writer = None

                    # --- FFmpeg Re-encoding Step ---
                    if temp_clip_path and os.path.exists(temp_clip_path):
                        print(f"  Re-encoding to H.264/AAC using ffmpeg...")
                        ffmpeg_cmd = [
                            ffmpeg_path,
                            '-i', temp_clip_path,       # Input: temporary file
                            '-c:v', 'libx264',          # Video Codec: H.264
                            '-preset', preset,          # Encoding speed/compression preset
                            '-crf', str(crf),           # Quality factor
                            '-pix_fmt', 'yuv420p',      # Pixel format for compatibility
                        ]

                        # Add audio flags if needed
                        if audio_bitrate and audio_bitrate != '0':
                            ffmpeg_cmd.extend(['-c:a', 'aac', '-b:a', audio_bitrate])
                        else:
                            # Explicitly disable audio if bitrate is 0 or None
                            ffmpeg_cmd.extend(['-an'])

                        # Add faststart flag and output path
                        ffmpeg_cmd.extend(['-movflags', '+faststart', final_clip_path])

                        try:
                            # Run ffmpeg, capture output, check for errors
                            process = subprocess.run(ffmpeg_cmd, check=True, capture_output=True, text=True)
                            # print("FFmpeg stdout:\n", process.stdout) # Uncomment for detailed FFmpeg output
                            # print("FFmpeg stderr:\n", process.stderr) # Uncomment for detailed FFmpeg output (often has progress info)
                            print(f"  Successfully encoded: {final_clip_path}")
                        except subprocess.CalledProcessError as e:
                            print(f"Error during ffmpeg re-encoding for scene {current_scene_index + 1}:")
                            print(f"  Command: {' '.join(e.cmd)}") # Show command executed
                            print(f"  Return Code: {e.returncode}")
                            print(f"  FFmpeg Stderr:\n{e.stderr}") # Show error output from ffmpeg
                            # Keep the problematic temp file for inspection? Optional.
                            print(f"  Problematic intermediate file kept at: {temp_clip_path}")
                            temp_clip_path = None # Signal that deletion should be skipped
                        except FileNotFoundError:
                             print(f"Error: '{ffmpeg_path}' command not found. Make sure ffmpeg is installed and in PATH.")
                             # No point continuing if ffmpeg isn't found
                             cap.release()
                             return
                        finally:
                            # --- Cleanup Temporary File ---
                            if temp_clip_path and os.path.exists(temp_clip_path):
                                try:
                                    os.remove(temp_clip_path)
                                    # print(f"  Removed temporary file: {temp_clip_path}")
                                except OSError as e:
                                    print(f"Warning: Could not remove temporary file {temp_clip_path}: {e}")

                    # Move to the next scene regardless of ffmpeg success for this one
                    current_scene_index += 1
                    temp_clip_path = None # Reset temp path for next scene


            # Increment frame number and update progress bar
            frame_number += 1
            pbar.update(1)

            # Optimization moved to the top of the loop

    # Final cleanup
    if output_writer is not None: # Handle case where video ends mid-scene writing
        print(f"Warning: Video ended while writing intermediate scene {current_scene_index + 1}. Releasing writer.")
        output_writer.release()
        # Optionally try to re-encode the partial intermediate file here if temp_clip_path is valid?
        # For simplicity, we'll just discard it for now. Add ffmpeg call here if needed.
        if temp_clip_path and os.path.exists(temp_clip_path):
             print(f"  Discarding incomplete intermediate file: {temp_clip_path}")
             try:
                  os.remove(temp_clip_path)
             except OSError as e:
                  print(f"Warning: Could not remove incomplete intermediate file {temp_clip_path}: {e}")

    cap.release()
    # cv2.destroyAllWindows() # Usually not needed unless cv2.imshow was used

    print("\nExtraction process complete.")


def main():
    parser = argparse.ArgumentParser(description="Detect scenes in a video and extract them as clips (H.264/AAC via ffmpeg).")
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
    # Removed --format, output is now always MP4 H.264
    # parser.add_argument("-f", "--format", type=str, default="mp4", choices=["mp4", "avi"],
    #                     help="Output video format for clips (default: mp4).")

    # --- FFmpeg Specific Arguments ---
    parser.add_argument("--ffmpeg_path", type=str, default="ffmpeg",
                        help="Path to the ffmpeg executable (if not in system PATH).")
    parser.add_argument("--crf", type=int, default=23,
                        help="H.264 Constant Rate Factor (quality, lower=better). Default: 23.")
    parser.add_argument("--preset", type=str, default="medium",
                        choices=['ultrafast', 'superfast', 'veryfast', 'faster', 'fast', 'medium', 'slow', 'slower', 'veryslow'],
                        help="H.264 encoding preset (speed vs compression). Default: medium.")
    parser.add_argument("--audio_bitrate", type=str, default="128k",
                        help="Target AAC audio bitrate (e.g., '128k', '192k', '0' for no audio). Default: 128k.")
    parser.add_argument("--use_temp_dir", action='store_true',
                        help="Store intermediate video files in the system's temporary directory instead of the output directory.")


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

    # 2. Extract Clips (using the modified function)
    if scenes is not None and fps is not None and frame_size is not None:
        extract_clips(
            video_path=args.input_video,
            scenes=scenes,
            output_dir=args.output_dir,
            fps=fps,
            frame_size=frame_size,
            total_frames=total_frames,
            # Pass ffmpeg related args:
            ffmpeg_path=args.ffmpeg_path,
            crf=args.crf,
            preset=args.preset,
            audio_bitrate=args.audio_bitrate,
            use_temp_dir=args.use_temp_dir
        )
    else:
        print("Scene detection failed. Cannot extract clips.")

if __name__ == "__main__":
    main()
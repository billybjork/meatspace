# Media Processing Scripts

This directory contains Python scripts used to process source videos, extract clips and keyframes, generate embeddings, and manage the PostgreSQL database records.

## Prerequisites

1.  **`.env` File:** A `.env` file must exist in the project root directory. It should define database connection details (`DATABASE_URL`, `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD`) and the `MEDIA_BASE_DIR`.
2.  **`MEDIA_BASE_DIR`:** This variable in `.env` points to the absolute path of the root directory where all media files (source videos, clips, keyframes) are stored or will be generated. Paths stored in the database are relative to this directory.
3.  **Python Libraries:** Required libraries (opencv-python, psycopg2-binary, torch, transformers, Pillow, tqdm, python-dotenv, etc.) must be installed (see `requirements.txt` in the project root).
4.  **Database:** A running PostgreSQL instance with the `pgvector` extension enabled and the schema created (including the `source_identifier` column in `source_videos`). Source videos should have their `source_identifier` populated before processing their clips.
5.  **FFmpeg:** The `ffmpeg` command-line tool must be installed and accessible in the system PATH, or its path provided via `--ffmpeg_path` argument to `splice_clips.py`.

## Workflow

The typical workflow involves running the scripts in the following order:

1.  **`splice_clips.py`:**
    *   Detects scene cuts in a source video.
    *   Extracts these scenes as individual `.mp4` clips using FFmpeg.
    *   Records the original video information (including filename, dimensions, fps) in the `source_videos` table. **Important:** Manually update the `source_identifier` column for the newly added source video record after running this script (e.g., using `psql` or DBeaver).
    *   Records information about each extracted clip (relative path, source link, timing) in the `clips` table.
2.  **`extract_keyframes.py`:**
    *   Filters clips belonging to a specified `--source-identifier`.
    *   Reads clip information from the `clips` table.
    *   Extracts one or more keyframes based on the `--strategy` (e.g., midpoint).
    *   Saves keyframes as `.jpg` images, using a filename format like `<SourceIdentifier>_<ScenePart>_frame_<FrameTag>.jpg`.
    *   Updates the `keyframe_filepath` column in the `clips` table with the relative path to the *representative* keyframe.
3.  **`generate_embeddings.py`:**
    *   Optionally filters clips belonging to a specified `--source-identifier`.
    *   Reads clip information (specifically the `keyframe_filepath`) from the `clips` table for clips matching the filter criteria.
    *   Loads a specified pre-trained vision model (e.g., CLIP).
    *   Generates embedding vectors for the keyframes.
    *   Inserts the embeddings into the `embeddings` table, linking them to the `clip_id` and recording the `model_name` and `generation_strategy` used. Skips existing embeddings by default unless `--overwrite` is specified.

## File Management Strategy

*   **Root Directory:** All media files reside within the single directory specified by `MEDIA_BASE_DIR` in your `.env` file.
*   **Relative Paths in DB:** The `source_videos.filepath`, `clips.clip_filepath`, and `clips.keyframe_filepath` columns store paths *relative* to `MEDIA_BASE_DIR`. The application uses these relative paths (combined with `MEDIA_BASE_DIR`) to locate and serve files.
*   **Organized Subdirectories:** Files are organized based on the `source_identifier` and keyframe strategy. This organization is handled partly by arguments passed to scripts and partly by the scripts themselves.
*   **Recommended Structure:**
    ```
    <MEDIA_BASE_DIR>/
    ├── clips/
    │   ├── <SourceIdentifier1>/
    │   │   ├── <SourceVideoFilenameBase>_scene_001.mp4 # Clip filename retains original base
    │   │   └── <SourceVideoFilenameBase>_scene_002.mp4
    │   └── <SourceIdentifier2>/
    │       └── <SourceVideoFilenameBase>_scene_001.mp4
    ├── keyframes/
    │   ├── <SourceIdentifier1>/
    │   │   ├── midpoint/
    │   │   │   ├── <SourceIdentifier1>_scene_001_frame_mid.jpg # Keyframe uses identifier
    │   │   │   └── <SourceIdentifier1>_scene_002_frame_mid.jpg
    │   │   └── multi/
    │   │       ├── <SourceIdentifier1>_scene_001_frame_25pct.jpg
    │   │       └── <SourceIdentifier1>_scene_001_frame_50pct.jpg
    │   └── <SourceIdentifier2>/
    │       └── midpoint/
    │           └── <SourceIdentifier2>_scene_001_frame_mid.jpg
    └── source_videos/ (Optional: Store original source files here)
        ├── <SourceVideoFilename1>.mp4
        └── <SourceVideoFilename2>.mov
    ```
*   **Achieving the Structure:**
    *   **Clips (`splice_clips.py`):** You specify the *parent* directory for the source-specific folder using `--output_subdir`. Example: `python splice_clips.py video.mp4 --output_subdir clips/<SourceIdentifier>` (replace `<SourceIdentifier>` manually). The script saves files like `<OriginalBase>_scene_XXX.mp4` inside this directory.
    *   **Keyframes (`extract_keyframes.py`):** You provide the `--source-identifier` and `--keyframes-base-subdir`. The script automatically creates the final path `<MEDIA_BASE_DIR>/<KeyframesBaseSubdir>/<SourceIdentifier>/<Strategy>/` and saves files like `<SourceIdentifier>_scene_XXX_frame_YYY.jpg` there.

## Utility Scripts
*   **`delete_orphaned_clips.py`:**
    *   Checks for clip records in the database (for a specific `--source-identifier`) whose corresponding `.mp4` files are missing from the expected location on disk.
    *   Deletes the orphaned `clips` records and their associated `embeddings` records from the database.
    *   Run with `--execute` to perform deletions (default is dry run). Useful after manually deleting unwanted clip files.

## Script Details (Key Arguments)

*   **`splice_clips.py`:** Processes a single source video.
    *   `input_video`: Path to the source video.
    *   `--output_subdir`: Relative path under `MEDIA_BASE_DIR` *containing the desired source-specific subdirectory* (e.g., `clips/Landline`).
*   **`extract_keyframes.py`:** Processes clips for a specific source.
    *   `--source-identifier`: **Required.** The identifier from `source_videos.source_identifier`.
    *   `--keyframes-base-subdir`: Top-level keyframe directory (default: `keyframes`).
    *   `--strategy`: Method (`midpoint`, `multi`).
    *   `--overwrite`: Re-extract keyframes even if DB path exists.
*   **`generate_embeddings.py`:** Processes clips with keyframes.
    *   `--model_name`: Hugging Face identifier for the embedding model.
    *   `--generation_strategy`: Label stored with the embedding (e.g., `keyframe_midpoint`).
    *   `--source-identifier`: *Optional.* Filter to process only clips from this source.
    *   `--overwrite`: Attempt to generate/insert even if embedding exists (uses `ON CONFLICT DO NOTHING`).

*   **`db_utils.py`:** Contains helper functions. Not run directly.
*   **`update_media_paths.py`:** Utility script.
    *   `--execute`: Perform DB updates (default: dry run).
*   **`delete_orphaned_clips.py`:** Utility script.
    *   `source_identifier`: **Required.** Which source to check for orphans.
    *   `--execute`: Perform DB deletions (default: dry run).
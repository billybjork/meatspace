import os
import glob
import argparse
import psycopg2
from psycopg2 import sql
from tqdm import tqdm

# Import helpers from db_utils
from utils.db_utils import get_db_connection, get_media_base_dir

def delete_orphaned_clips(conn, source_identifier, dry_run=True):
    """
    Deletes records from the 'clips' and associated 'embeddings' tables
    if the corresponding clip file is missing from the expected location
    based on the source_identifier.
    """
    deleted_clip_count = 0
    deleted_embedding_count = 0
    skipped_missing_source_id = 0
    checked_count = 0

    # --- Configuration for Expected Structure ---
    CLIPS_BASE_SUBDIR = "clips"
    # --- End Configuration ---

    media_base_dir = get_media_base_dir()

    with conn.cursor() as cur:
        # 1. Find the source_video_id for the given identifier
        print(f"Finding source video ID for identifier: '{source_identifier}'...")
        cur.execute("SELECT id FROM source_videos WHERE source_identifier = %s", (source_identifier,))
        result = cur.fetchone()
        if not result:
            print(f"Error: Could not find source video with identifier '{source_identifier}' in the database.")
            return
        source_video_id = result[0]
        print(f"Found source_video_id: {source_video_id}")

        # 2. Get all clip records for this source video
        print(f"Fetching clips associated with source_video_id {source_video_id}...")
        cur.execute("""
            SELECT id, clip_identifier, clip_filepath -- Fetch old path too for context if needed
            FROM clips
            WHERE source_video_id = %s
            ORDER BY id;
        """, (source_video_id,))
        clips_in_db = cur.fetchall()
        print(f"Found {len(clips_in_db)} clip records in DB for this source.")

        if not clips_in_db:
            print("No clips in DB to check for this source.")
            return

        # 3. Construct expected paths and check existence
        clips_to_delete_ids = []
        print("Checking file existence for each clip record...")
        for clip_id, clip_identifier, old_clip_path in tqdm(clips_in_db, desc="Checking Files"):
            checked_count += 1
            if not clip_identifier: # Should not happen with unique constraint, but check
                print(f"\nWarning: Clip ID {clip_id} has NULL identifier. Skipping.")
                continue

            # Construct the expected *new* path based on identifier
            clip_filename = f"{clip_identifier}.mp4"
            expected_rel_path = os.path.join(CLIPS_BASE_SUBDIR, source_identifier, clip_filename)
            expected_abs_path = os.path.join(media_base_dir, expected_rel_path)

            if not os.path.exists(expected_abs_path):
                # File is missing, mark clip ID for deletion
                print(f"\nMissing file: {expected_abs_path} (Clip ID: {clip_id}) - Marked for deletion.")
                clips_to_delete_ids.append(clip_id)
            # else: # Optional: Log found files
                # print(f"\nFound file: {expected_abs_path} (Clip ID: {clip_id})")


        # 4. Perform Deletion (if not dry run and IDs found)
        if not clips_to_delete_ids:
            print("\nNo orphaned clip records found to delete.")
            return

        print(f"\nFound {len(clips_to_delete_ids)} clip records corresponding to missing files.")

        if dry_run:
            print("\n--- DRY RUN: Would delete the following Clip IDs (and related embeddings): ---")
            print(clips_to_delete_ids)
        else:
            print("\n--- EXECUTING DELETION ---")
            try:
                # Delete associated embeddings first due to foreign key constraint
                if clips_to_delete_ids: # Ensure list is not empty
                    # Use tuple for WHERE IN clause
                    ids_tuple = tuple(clips_to_delete_ids)
                    delete_embeddings_sql = sql.SQL("DELETE FROM embeddings WHERE clip_id IN %s")
                    cur.execute(delete_embeddings_sql, (ids_tuple,))
                    deleted_embedding_count = cur.rowcount
                    print(f"Deleted {deleted_embedding_count} associated embedding records.")

                    # Delete the clips themselves
                    delete_clips_sql = sql.SQL("DELETE FROM clips WHERE id IN %s")
                    cur.execute(delete_clips_sql, (ids_tuple,))
                    deleted_clip_count = cur.rowcount
                    print(f"Deleted {deleted_clip_count} clip records.")

                    conn.commit()
                    print("Deletions committed successfully.")

            except psycopg2.Error as e:
                print(f"\nError during deletion: {e}")
                conn.rollback()
                print("Rolled back deletion transaction.")

    print("\n--- Summary ---")
    print(f"Clips checked in DB for source '{source_identifier}': {checked_count}")
    if dry_run:
        print(f"Clip records proposed for deletion: {len(clips_to_delete_ids)}")
    else:
        print(f"Clip records deleted: {deleted_clip_count}")
        print(f"Associated embedding records deleted: {deleted_embedding_count}")

def main():
    parser = argparse.ArgumentParser(description="Delete orphaned clip records from the database if their files are missing.")
    parser.add_argument("source_identifier", help="The source_identifier (e.g., 'Landline') to process.")
    parser.add_argument("--execute", action="store_true", help="Actually execute the deletions. Default is a dry run.")
    args = parser.parse_args()

    conn = None
    is_dry_run = not args.execute

    try:
        conn = get_db_connection()
        delete_orphaned_clips(conn, args.source_identifier, dry_run=is_dry_run)

    except (ValueError, psycopg2.Error) as e:
        print(f"A critical error occurred: {e}")
        if conn: conn.rollback() # Ensure rollback on error
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    main()
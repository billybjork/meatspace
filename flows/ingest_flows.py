from prefect import flow, get_run_logger
from prefect.server.schemas.schedules import IntervalSchedule
from datetime import timedelta

# Import tasks from their specific modules within the 'tasks' directory
# Import DB helpers from the 'utils' directory
try:
    # Task Imports - UPDATED
    from tasks.intake import intake_task # Renamed task import
    from tasks.splice import splice_video_task
    from tasks.keyframe import extract_keyframes_task
    from tasks.embed import generate_embeddings_task

    # Utility Imports
    from utils.db_utils import get_items_by_state, get_source_input_from_db # Assuming this helper exists/will exist

except ImportError as e:
     # Handle potential path issues during development
     print(f"Could not import required modules: {e}")
     print("Ensure tasks/*py and utils/db_utils.py exist and are importable.")
     print("Defining dummy functions to allow script loading...")
     # Define dummy functions if needed for script to load without crashing
     def intake_task(**kwargs): raise NotImplementedError("Dummy Task: intake_task")
     def splice_video_task(**kwargs): raise NotImplementedError("Dummy Task: splice_video_task")
     def extract_keyframes_task(**kwargs): raise NotImplementedError("Dummy Task: extract_keyframes_task")
     def generate_embeddings_task(**kwargs): raise NotImplementedError("Dummy Task: generate_embeddings_task")
     def get_items_by_state(table, state): return []
     def get_source_input_from_db(sid): return None # Dummy for helper


# --- Configuration (Ideally load from .env or config file) ---
DEFAULT_KEYFRAME_STRATEGY = "midpoint"
DEFAULT_EMBEDDING_MODEL = "openai/clip-vit-base-patch32"
# Derive the label used for the embedding generation based on the keyframe strategy
DEFAULT_EMBEDDING_STRATEGY_LABEL = f"keyframe_{DEFAULT_KEYFRAME_STRATEGY}"

# --- Flows ---

@flow(log_prints=True)
def process_clip_post_review(
    clip_id: int,
    keyframe_strategy: str = DEFAULT_KEYFRAME_STRATEGY,
    model_name: str = DEFAULT_EMBEDDING_MODEL
    ):
    """
    Processes a single clip after it has passed manual review.
    This flow handles keyframing and subsequent embedding for the approved clip.
    """
    logger = get_run_logger()
    logger.info(f"FLOW: Starting post-review processing for approved clip_id: {clip_id}")

    # --- 1. Keyframing ---
    logger.info(f"Submitting keyframe task for clip_id: {clip_id} with strategy: {keyframe_strategy}")
    keyframe_future = extract_keyframes_task.submit(clip_id=clip_id, strategy=keyframe_strategy)
    keyframe_state = keyframe_future.wait()

    if keyframe_state.is_completed():
        logger.info(f"Keyframing task completed successfully for clip_id: {clip_id}. Proceeding to embedding.")

        # --- 2. Embedding ---
        embedding_strategy_label = f"keyframe_{keyframe_strategy}"
        logger.info(f"Submitting embedding task for clip_id: {clip_id} with model: {model_name}, strategy_label: {embedding_strategy_label}")
        embed_future = generate_embeddings_task.submit(
            clip_id=clip_id,
            model_name=model_name,
            generation_strategy=embedding_strategy_label
        )
        embed_future.wait()
        logger.info(f"Embedding task submitted and ran for clip_id: {clip_id}. Check logs/DB for final status.")

    else:
         logger.warning(f"Keyframing task for clip_id: {clip_id} did not complete successfully (State: {keyframe_state.type.value}). Skipping embedding.")

    logger.info(f"FLOW: Finished post-review processing flow for clip_id: {clip_id}")


@flow(name="Scheduled Ingest Initiator", log_prints=True)
def scheduled_ingest_initiator():
    """
    Scheduled flow to find new work at different stages and trigger the appropriate next tasks.
    This acts as the main heartbeat for progressing the ingest pipeline automatically.
    """
    logger = get_run_logger()
    logger.info("FLOW: Running Scheduled Ingest Initiator cycle...")

    # --- Stage 1: Find New Source Videos ---
    new_source_ids = get_items_by_state(table="source_videos", state="new")
    if new_source_ids:
        logger.info(f"Found {len(new_source_ids)} new source videos. Submitting intake tasks...")
        for sid in new_source_ids:
            logger.info(f"Attempting to trigger intake_task for source_id: {sid}")
            # --- Fetch input_source (URL or Path) for the task ---
            source_input = get_source_input_from_db(sid) # Fetch from DB
            if source_input:
                # Submit the intake task
                intake_task.submit(source_video_id=sid, input_source=source_input)
                logger.info(f"Submitted intake_task for source_id: {sid} with input: '{source_input[:50]}...'")
            else:
                # Log an error and potentially set state to failed if input is missing
                logger.error(f"Could not find input source (e.g., original_url) in DB for new source_video_id: {sid}. Cannot submit intake task.")
                # Optionally update DB state to indicate failure due to missing input
                # update_db_state(sid, 'source_videos', 'download_failed', 'Missing input source URL/Path in DB')

    # --- Stage 2: Find Downloaded Source Videos Ready for Splicing ---
    downloaded_source_ids = get_items_by_state(table="source_videos", state="downloaded")
    if downloaded_source_ids:
        logger.info(f"Found {len(downloaded_source_ids)} downloaded sources ready for splicing. Submitting splice tasks...")
        for sid in downloaded_source_ids:
            logger.info(f"Submitting splice_video_task for source_id: {sid}")
            splice_video_task.submit(source_video_id=sid)

    # --- Stage 3: Find Clips Approved by Manual Review ---
    clips_ready_for_keyframes = get_items_by_state(table="clips", state="review_approved")
    if clips_ready_for_keyframes:
        logger.info(f"Found {len(clips_ready_for_keyframes)} clips approved by review. Submitting post-review processing flow...")
        for cid in clips_ready_for_keyframes:
            logger.info(f"Submitting process_clip_post_review flow for clip_id: {cid}")
            process_clip_post_review.submit(clip_id=cid)

    logger.info("FLOW: Scheduled Ingest Initiator cycle complete.")


# --- Deployment (Example for the scheduled flow) ---
# Ensure you have a Prefect Agent running that polls the 'default' work queue.
# from prefect.deployments import Deployment

# deployment = Deployment.build_from_flow(
#     flow=scheduled_ingest_initiator,
#     name="Scheduled Ingest Dispatcher",
#     schedule=(IntervalSchedule(interval=timedelta(minutes=1))), # Adjust frequency as needed
#     work_queue_name="default", # Or specify a different queue if desired
#     description="Periodically checks DB for new ingest work and triggers tasks.",
#     tags=["ingest", "scheduled"],
# )

# if __name__ == "__main__":
    # Run this script once with 'python flows/ingest_flows.py' to create/update the deployment
    # deployment.apply()
    # For local testing of one cycle:
    # scheduled_ingest_initiator()
#     pass
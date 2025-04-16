from prefect import task, get_run_logger
import os
import random

@task
def extract_keyframes_task(clip_id: int, clip_identifier: str, source_identifier: str, strategy: str = 'midpoint', **kwargs):
    """
    Dummy Prefect task to simulate keyframe extraction for a video clip.
    """
    logger = get_run_logger()
    logger.info(f"TASK [Keyframe]: Starting keyframe extraction for clip_id {clip_id} using strategy '{strategy}'")

    # Simulate extraction: just pretend we're creating some file paths
    simulated_base_dir = "/media/keyframes"
    keyframe_tags = []

    if strategy == 'multi':
        keyframe_tags = ["25pct", "50pct", "75pct"]
    else:
        keyframe_tags = ["mid"]

    scene_part = f"scene_{random.randint(1, 999)}"  # Fake scene ID for dummy use
    frame_paths = {}

    for tag in keyframe_tags:
        filename = f"{source_identifier}_{scene_part}_frame_{tag}.jpg"
        rel_path = os.path.join(source_identifier, strategy, filename)
        frame_paths[tag] = os.path.join(simulated_base_dir, rel_path)

    representative_key = '50pct' if '50pct' in frame_paths else 'mid'
    representative_path = frame_paths.get(representative_key)

    logger.info(f"TASK [Keyframe]: Extracted dummy frames: {list(frame_paths.values())}")
    logger.info(f"TASK [Keyframe]: Selected representative keyframe: {representative_key}")

    return {
        "clip_id": clip_id,
        "strategy": strategy,
        "frame_paths": frame_paths,
        "representative_frame_path": representative_path
    }

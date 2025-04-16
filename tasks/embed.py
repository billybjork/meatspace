from prefect import task, get_run_logger
import time
import random

@task
def generate_embeddings_task(clip_id: int, model_name: str, generation_strategy: str, **kwargs):
    logger = get_run_logger()
    
    # Simulate the start of the task
    logger.info(f"TASK [Embed]: Starting embedding for clip_id={clip_id} using model={model_name}, strategy={generation_strategy}")
    
    # Simulate computation delay
    time.sleep(random.uniform(0.5, 1.5))
    
    # Simulate dummy embedding result
    embedding = [random.random() for _ in range(10)]  # a dummy 10-dimensional vector
    
    logger.info(f"TASK [Embed]: Completed embedding for clip_id={clip_id}. Dummy embedding: {embedding}")
    
    return {
        "clip_id": clip_id,
        "model_name": model_name,
        "strategy": generation_strategy,
        "embedding": embedding
    }
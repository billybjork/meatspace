from pydantic import BaseModel, Field

class ClipActionPayload(BaseModel):
    action: str # 'approve', 'skip', 'archive', 'merge_next', 'retry_splice'

class SplitActionPayload(BaseModel):
    split_time_seconds: float = Field(..., gt=0, description="Relative time in seconds within the clip to perform the split.")
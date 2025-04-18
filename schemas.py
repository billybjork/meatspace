from pydantic import BaseModel, Field

class ClipActionPayload(BaseModel):
    action: str # 'approve', 'skip', 'archive', 'merge_next', 'retry_sprite_gen'

class SplitActionPayload(BaseModel):
    # Changed from split_time_seconds to split_request_at_frame
    split_request_at_frame: int = Field(..., ge=0, description="Frame number within the clip (0-based) to perform the split.")
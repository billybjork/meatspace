from pydantic import BaseModel, Field

class ClipActionPayload(BaseModel):
    action: str # 'approve', 'skip', 'archive', 'merge_prev', 'retry_sprite_gen', etc.

class SplitActionPayload(BaseModel):
    split_request_at_frame: int = Field(..., ge=0, description="Frame number within the clip (0-based) to perform the split.")
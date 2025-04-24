// ui/src/lib/types.ts

// Based on Pydantic SpriteMeta
export interface SpriteMeta {
    tile_width: number;
    tile_height_calculated: number; // Matches Pydantic alias
    cols: number;
    rows: number;
    total_sprite_frames: number; // Total frames in the *sprite sheet*
    clip_fps: number; // FPS of the original clip segment
    clip_total_frames: number; // Total frames in the *clip segment*
    spriteUrl?: string; // Added for convenience within SpritePlayer component
    isValid?: boolean; // Added for internal validation check
  }
  
  // Based on Pydantic ClipForReview and API response structure
  export interface ClipForReview {
    db_id: number;
    identifier: string;
    title: string;
    source_video_id: number;
    source_title: string;
    start_s: number;
    end_s: number;
    sprite_meta?: SpriteMeta | null; // Can be null or undefined if not present
    sprite_url?: string | null;
    keyframe_url?: string | null; // Although not used in player, keep for completeness
    ingest_state: string;
    can_action_previous: boolean;
    previous_clip_id?: number | null;
    last_error?: string | null;
    // Added fields based on API response and logic needs
    sprite_error?: boolean; // Derived or explicit (e.g., from ingest_state)
    next_state?: string | null; // Track pending action
    current_state?: string; // Explicitly track current state (might be same as ingest_state)
  }
  
  // Based on API response for /api/clips/review/next
  export interface NextClipResponse {
    done: boolean;
    clip: ClipForReview | null;
  }
  
  // For API action responses (adjust based on actual API)
  export interface ActionResponse {
      status: string; // e.g., "success"
      clip_id: number;
      next_state?: string;
      previous_clip_id?: number;
      previous_clip_next_state?: string;
      message?: string; // For split success, etc.
      detail?: string; // For errors
  }
  
  // For API error responses (FastAPI validation or general errors)
  export interface ErrorResponse {
      detail: string | { msg: string; type: string }[]; // FastAPI can return string or object/array
  }
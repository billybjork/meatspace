import { z } from 'zod';

// Corresponds to Python Pydantic SpriteMeta
// Uses field names expected *after* Python data massaging
export const SpriteMetaSchema = z.object({
    tile_width: z.number().int(),
    // tile_height_calculated alias mapped to tile_height in Python massage
    tile_height: z.number().int(),
    cols: z.number().int(),
    rows: z.number().int(),
    total_sprite_frames: z.number().int(),
    // clip_fps_source alias mapped to clip_fps in Python massage
    clip_fps: z.number(),
     // clip_total_frames_source alias mapped to clip_total_frames in Python massage
    clip_total_frames: z.number().int(),
});
export type SpriteMeta = z.infer<typeof SpriteMetaSchema>;

// Corresponds to Python Pydantic ClipForReview
// Uses field names expected *after* Python data massaging
export const ClipForReviewSchema = z.object({
    // id alias mapped to db_id in Python massage
    db_id: z.number().int(),
    // clip_identifier alias mapped to identifier in Python massage
    identifier: z.string(),
    // title is generated during Python massage
    title: z.string(),
    source_video_id: z.number().int(),
    source_title: z.string(),
    // start_time_seconds alias mapped to start_s in Python massage
    start_s: z.number(),
    // end_time_seconds alias mapped to end_s in Python massage
    end_s: z.number(),
    // Optional fields
    sprite_meta: SpriteMetaSchema.optional().nullable(),
    sprite_url: z.string().url().optional().nullable(),
    keyframe_url: z.string().url().optional().nullable(),
    ingest_state: z.string(), // Consider z.enum([...]) if states are fixed
    can_action_previous: z.boolean(),
    previous_clip_id: z.number().int().optional().nullable(),
    last_error: z.string().optional().nullable(),
});
export type ClipForReview = z.infer<typeof ClipForReviewSchema>;

// Corresponds to Python Pydantic NextClipResponse
// Matches the structure returned by /api/clips/review/next
export const NextClipResponseSchema = z.object({
    done: z.boolean(),
    clip: ClipForReviewSchema.nullable(), // Clip is null when done is true
});
export type NextClipResponse = z.infer<typeof NextClipResponseSchema>;

// --- API Payload Schemas ---

// Based on usage in handle_clip_action
export const ClipActionPayloadSchema = z.object({
    action: z.enum([
        "approve",
        "skip",
        "archive",
        "retry_sprite_gen",
        "merge_previous",
        "group_previous",
    ]),
});
export type ClipActionPayload = z.infer<typeof ClipActionPayloadSchema>;

// Based on usage in queue_clip_split
export const SplitActionPayloadSchema = z.object({
    split_request_at_frame: z.number().int().positive(),
});
export type SplitActionPayload = z.infer<typeof SplitActionPayloadSchema>;

// Based on Python ClipUndoPayload
export const ClipUndoPayloadSchema = z.object({
    clip_db_id: z.number().int().positive(),
});
export type ClipUndoPayload = z.infer<typeof ClipUndoPayloadSchema>;


// --- API Response Schemas (Examples) ---

// Example for a successful action response from POST /action, POST /split etc.
// Adapt based on actual consistent structure returned by your TS API routes
export const ActionSuccessResponseSchema = z.object({
    status: z.literal("success"),
    clip_id: z.number().int(),
    // Optional fields depending on the action performed
    next_state: z.string().optional(),
    previous_clip_id: z.number().int().optional(),
    previous_clip_next_state: z.string().optional(),
    message: z.string().optional(), // e.g., "Clip queued for splitting."
});
export type ActionSuccessResponse = z.infer<typeof ActionSuccessResponseSchema>;

// Example schema for handling common API error responses
export const ApiErrorResponseSchema = z.object({
     // Handles simple string errors or FastAPI-like validation errors
    detail: z.union([
        z.string(),
        z.array(
            z.object({
                loc: z.array(z.union([z.string(), z.number()])).optional(), // location in input
                msg: z.string(), // error message
                type: z.string(), // error type
            })
        ),
         // Handle potential custom error objects if needed
        z.record(z.unknown()) // Allow other object structures as fallback
    ]),
});
export type ApiErrorResponse = z.infer<typeof ApiErrorResponseSchema>;


// --- Constants (mirroring Python) ---
export const ARTIFACT_TYPE_KEYFRAME = "keyframe";
export const ARTIFACT_TYPE_SPRITE_SHEET = "sprite_sheet";
export const REPRESENTATIVE_TAG = "representative";

// Possible ingest states for filtering in API (mirroring Python)
// Useful if you need to reference these specifically in TS logic
export const REVIEWABLE_INGEST_STATES = [
    'pending_review',
    'sprite_generation_failed',
    'merge_failed',
    'split_failed',
    'keyframe_failed',
    'embedding_failed',
    'group_failed',
];
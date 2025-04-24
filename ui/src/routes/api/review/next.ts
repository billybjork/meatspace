import { type APIEvent } from "@solidjs/start/server"; // SolidStart API route handler utils
import { PoolClient } from 'pg'; // Import PoolClient type
import { getDbClient } from "~/lib/db"; // Adjusted import path for SolidStart structure
import {
    NextClipResponseSchema,
    ClipForReviewSchema,
    SpriteMetaSchema,
    ClipForReview, // Import the TS type
    SpriteMeta,    // Import the TS type
    ARTIFACT_TYPE_KEYFRAME,
    ARTIFACT_TYPE_SPRITE_SHEET,
    REPRESENTATIVE_TAG,
    REVIEWABLE_INGEST_STATES // Use the constant array
} from "~/schemas/clip"; // Adjusted import path

// Needed for Zod schema access during server-side operations
import { z } from 'zod';

// Custom JSON response helper
function json(data: any, { status = 200 } = {}) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

// Access environment variables
const CLOUDFRONT_DOMAIN = process.env.CLOUDFRONT_DOMAIN;

/**
 * API Route Handler for GET /api/review/next
 * Fetches the next clip needing review from the database.
 */
export async function GET(event: APIEvent) {
    console.log("API: Received GET /api/review/next request");

    if (!CLOUDFRONT_DOMAIN) {
        console.error("API Error: CLOUDFRONT_DOMAIN environment variable is not set.");
        return json({ detail: "Server configuration error: CloudFront domain missing." }, { status: 500 });
    }

    let client: PoolClient | null = null;

    try {
        client = await getDbClient(); // Acquire a client from the pool

        // The SQL query, mirroring the Python version
        // Ensure $ syntax matches node-postgres parameter binding
        const query = `
            WITH cte AS (
                SELECT
                    c.id, c.clip_identifier, c.start_time_seconds, c.end_time_seconds,
                    c.source_video_id, c.ingest_state, c.last_error, c.next_state,
                    -- Add other fields from 'c.*' that ARE needed for massaging or schema
                    -- Explicitly list needed fields instead of 'c.*' for clarity & performance
                    sv.title AS source_title,
                    LAG(c.id)  OVER w AS previous_clip_id,
                    LAG(c.ingest_state) OVER w AS previous_clip_state,
                    kf.s3_key AS keyframe_s3_key,
                    ss.s3_key AS sprite_s3_key,
                    ss.metadata AS sprite_meta_raw
                    -- Explicitly exclude fields NOT needed after massaging:
                    -- c.clip_filepath, c.start_frame, c.end_frame, c.created_at, c.retry_count,
                    -- c.updated_at, c.reviewed_at, c.keyframed_at, c.embedded_at,
                    -- c.processing_metadata, c.grouped_with_clip_id, c.action_committed_at,
                    -- etc.
                FROM clips c
                JOIN source_videos sv ON sv.id = c.source_video_id
                LEFT JOIN clip_artifacts kf ON kf.clip_id = c.id
                  AND kf.artifact_type = $1 AND kf.tag = $2
                LEFT JOIN clip_artifacts ss ON ss.clip_id = c.id
                  AND ss.artifact_type = $3
                WINDOW w AS (PARTITION BY c.source_video_id
                             ORDER BY c.start_time_seconds, c.id)
            )
            SELECT *
            FROM cte
            WHERE ingest_state = ANY($4::text[]) -- Use ANY for array matching
              AND next_state IS NULL
            ORDER BY source_video_id, start_time_seconds, id
            LIMIT 1;
        `;

        const queryParams = [
            ARTIFACT_TYPE_KEYFRAME,
            REPRESENTATIVE_TAG,
            ARTIFACT_TYPE_SPRITE_SHEET,
            REVIEWABLE_INGEST_STATES // Pass the array of allowed states
        ];

        console.log("Executing DB query for next clip...");
        const result = await client.query(query, queryParams);

        if (result.rows.length === 0) {
            console.log("No clips found for review.");
            // Validate the structure for the "done" case
            const doneResponse = NextClipResponseSchema.parse({ done: true, clip: null });
            return json(doneResponse); // Use SolidStart's json helper
        }

        const rawRecord = result.rows[0];
        console.log(`Found clip ID: ${rawRecord.id} for review.`);

        // --- Data Massaging Block (TypeScript version) ---
        // Create a mutable object to hold the massaged data
        const massagedData: Partial<ClipForReview> = {}; // Use Partial initially

        // 1. Rename/Map keys & Direct Copies
        massagedData.db_id = rawRecord.id;
        massagedData.identifier = rawRecord.clip_identifier;
        massagedData.source_video_id = rawRecord.source_video_id;
        massagedData.source_title = rawRecord.source_title;
        massagedData.start_s = rawRecord.start_time_seconds;
        massagedData.end_s = rawRecord.end_time_seconds;
        massagedData.ingest_state = rawRecord.ingest_state;
        massagedData.previous_clip_id = rawRecord.previous_clip_id ?? null; // Coalesce undefined/null to null
        massagedData.last_error = rawRecord.last_error ?? null;

        // 2. Handle sprite_meta
        // node-postgres automatically parses JSONB to JS objects
        const spriteMetaRaw = rawRecord.sprite_meta_raw;
        let spriteMetaOutput: SpriteMeta | null = null;
        if (spriteMetaRaw && typeof spriteMetaRaw === 'object') {
            // Map raw keys to schema keys
            const partialMeta: Partial<SpriteMeta> = {};
             if (spriteMetaRaw.tile_width !== undefined) partialMeta.tile_width = spriteMetaRaw.tile_width;
             // Map the field name expected by the Zod schema
             if (spriteMetaRaw.tile_height_calculated !== undefined) partialMeta.tile_height = spriteMetaRaw.tile_height_calculated;
             if (spriteMetaRaw.cols !== undefined) partialMeta.cols = spriteMetaRaw.cols;
             if (spriteMetaRaw.rows !== undefined) partialMeta.rows = spriteMetaRaw.rows;
             if (spriteMetaRaw.total_sprite_frames !== undefined) partialMeta.total_sprite_frames = spriteMetaRaw.total_sprite_frames;
             // Map the field name expected by the Zod schema
             if (spriteMetaRaw.clip_fps_source !== undefined) partialMeta.clip_fps = spriteMetaRaw.clip_fps_source;
             // Map the field name expected by the Zod schema (handle potential legacy key)
             if (spriteMetaRaw.clip_total_frames_source !== undefined) partialMeta.clip_total_frames = spriteMetaRaw.clip_total_frames_source;
             else if (spriteMetaRaw.clip_total_frames !== undefined) partialMeta.clip_total_frames = spriteMetaRaw.clip_total_frames;

             // Try to parse against the schema - if successful, assign it.
             try {
                spriteMetaOutput = SpriteMetaSchema.parse(partialMeta);
                console.log("Successfully parsed sprite_meta");
             } catch (spriteParseError) {
                 console.warn(`Failed to parse sprite_meta for clip ${massagedData.db_id}. Input:`, spriteMetaRaw, "Error:", spriteParseError);
                 // Keep spriteMetaOutput as null if parsing fails
             }
        }
        massagedData.sprite_meta = spriteMetaOutput;

        // 3. Build title
        const ident = massagedData.identifier ?? "";
        massagedData.title = ident.replace(/_/g, " ").replace(/-/g, " ").split(' ')
                               .map(word => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase())
                               .join(' ');

        // 4. Set can_action_previous
        // Define valid states for the previous clip (mirroring Python)
        const validPreviousActionStates = new Set([
            'pending_review', 'review_skipped', 'approved_pending_deletion',
            'review_approved', 'archived_pending_deletion', 'archived',
            'grouped_complete'
        ]);
        const previousClipState = rawRecord.previous_clip_state;
        massagedData.can_action_previous = !!(
            massagedData.previous_clip_id &&
            previousClipState &&
            validPreviousActionStates.has(previousClipState)
        );

        // 5. CloudFront URLs
        const spriteS3Key = rawRecord.sprite_s3_key;
        const keyframeS3Key = rawRecord.keyframe_s3_key;
        const cfDomain = CLOUDFRONT_DOMAIN.replace(/\/$/, ''); // Remove trailing slash just in case

        massagedData.sprite_url = spriteS3Key
            ? `https://${cfDomain}/${spriteS3Key.replace(/^\//, '')}` // Remove leading slash
            : null;
        massagedData.keyframe_url = keyframeS3Key
            ? `https://${cfDomain}/${keyframeS3Key.replace(/^\//, '')}`
            : null;

        // --- End Data Massaging ---

        // --- Validate final structure and Return ---
        // Validate the massaged clip data
        const validatedClip = ClipForReviewSchema.parse(massagedData);

        // Wrap in the response structure and validate again
        const responsePayload = NextClipResponseSchema.parse({
            done: false,
            clip: validatedClip
        });

        console.log(`Returning clip ID: ${validatedClip.db_id}`);
        return json(responsePayload); // Use SolidStart's json helper

    } catch (error: any) {
        console.error("API Error in /api/review/next:", error);

        // Handle Zod validation errors specifically if needed
        if (error instanceof z.ZodError) {
            console.error("Data validation failed:", error.flatten());
             // This indicates an issue with the massaging logic or unexpected DB data
            return json({ detail: "Internal server error preparing response data.", errors: error.flatten() }, { status: 500 });
        }

        // Handle potential DB errors (e.g., connection issues, syntax errors)
        // You might want more specific checks based on error codes if available
        return json({ detail: "Internal server error accessing database." }, { status: 500 });

    } finally {
        if (client) {
            client.release(); // ALWAYS release the client
            console.log("DB client released.");
        }
    }
}
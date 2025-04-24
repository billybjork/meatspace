import { type APIEvent } from "@solidjs/start/server";
import { PoolClient } from 'pg';
import { z } from 'zod';

import { getDbClient } from "~/lib/db";
import {
    ClipActionPayloadSchema, // Expects only { action: "..." } in body
    ActionSuccessResponseSchema,
    ApiErrorResponseSchema
} from "~/schemas/clip";

// Custom JSON response helper
function json(data: any, { status = 200 } = {}) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

// Define allowed states for general actions (mirroring Python)
const ALLOWED_SOURCE_STATES_GENERAL = [
    'pending_review', 'review_skipped', 'merge_failed', 'split_failed',
    'keyframe_failed', 'embedding_failed', 'sprite_generation_failed', 'group_failed'
];
// TODO: Add specific allowed states for 'retry_sprite_gen' if implementing that button

// Mapping from action name to the target next_state in the DB
const ACTION_TO_STATE: Record<string, string> = {
    "approve": "approved_pending_deletion",
    "skip": "review_skipped",
    "archive": "archived_pending_deletion",
    "retry_sprite_gen": "pending_sprite_generation", // Add if needed
    // Merge/Group handled differently
    // "merge_previous": "marked_for_merge_into_previous",
    // "group_previous": "grouped_complete",
};

/**
 * API Route Handler for POST /api/clips/[id]/action
 * Handles simple clip actions like approve, skip, archive.
 * Reads clip ID from URL path parameter.
 * Uses transactions and advisory locks.
 */
export async function POST(event: APIEvent) {
    console.log("API: Received POST /api/clips/[id]/action request");
    let client: PoolClient | null = null;
    let clipId: number | null = null; // Keep track for logging

    try {
        // 1. Get Clip ID from Path Parameter
        const clipIdParam = event.params.id; // Read 'id' from the filename [id]
        if (!clipIdParam || isNaN(parseInt(clipIdParam))) {
            return json({ detail: "Invalid or missing clip ID in URL path." }, { status: 400 });
        }
        clipId = parseInt(clipIdParam);

        // 2. Parse Action from Payload Body
        let payload: z.infer<typeof ClipActionPayloadSchema>;
        try {
            const rawPayload = await event.request.json();
            // Payload schema now only expects the action field
            payload = ClipActionPayloadSchema.parse(rawPayload);
            console.log(`Action: ${payload.action}, Clip ID: ${clipId} (from path)`);
        } catch (error) {
             console.error("Payload validation failed:", error);
             if (error instanceof z.ZodError) {
                 return json({ detail: error.flatten().fieldErrors }, { status: 422 }); // Unprocessable Entity
             }
             return json({ detail: "Invalid request payload body." }, { status: 400 });
        }

        const action = payload.action;
        const intendedNextState = ACTION_TO_STATE[action];

        // --- Handle Simple Actions (Approve, Skip, Archive) ---
        if (!["approve", "skip", "archive"].includes(action)) {
            console.warn(`Action '${action}' not yet implemented in this endpoint.`);
             return json({ detail: `Action '${action}' is invalid or not supported by this endpoint.` }, { status: 400 });
        }

        if (!intendedNextState) {
            console.error(`Internal error: No next state defined for action '${action}'`);
            return json({ detail: "Internal server configuration error." }, { status: 500 });
        }

        const allowedSourceStates = ALLOWED_SOURCE_STATES_GENERAL;
        const lockIdCurrent = 2;

        // 3. Database Transaction
        client = await getDbClient();
        await client.query('BEGIN');
        console.log(`Transaction started for clip ${clipId}`);

        try {
            // 4. Acquire Advisory Lock
            await client.query('SELECT pg_advisory_xact_lock($1, $2)', [lockIdCurrent, clipId]);
            console.log(`Acquired advisory lock (${lockIdCurrent}, ${clipId})`);

            // 5. Fetch Current State (with row lock)
            const selectQuery = `
                SELECT ingest_state, next_state
                FROM clips
                WHERE id = $1
                FOR UPDATE;
            `;
            const selectResult = await client.query(selectQuery, [clipId]);

            if (selectResult.rows.length === 0) {
                throw new Error(`Clip with ID ${clipId} not found.`); // -> 404
            }

            const currentData = selectResult.rows[0];
            const currentState = currentData.ingest_state;
            const currentNextState = currentData.next_state;

            // 6. Validate State Preconditions
            if (currentNextState !== null) {
                 throw new Error(`Action already pending for this clip (next state: '${currentNextState}'). Undo first if needed.`); // -> 409
            }
            if (!allowedSourceStates.includes(currentState)) {
                 throw new Error(`Action '${action}' cannot be performed from state '${currentState}'.`); // -> 409
            }

            // 7. Perform Update
            const updateClauses = [
                'next_state = $1',
                'action_committed_at = NOW()',
                'updated_at = NOW()',
                'last_error = NULL',
                'processing_metadata = NULL',
                'grouped_with_clip_id = NULL'
            ];
            const updateParams: (string | number | null | (string | number)[])[] = [intendedNextState];
            let paramIndex = 2; // Start params at $2

            // Set reviewed_at timestamp appropriately
            if (["approve", "archive"].includes(action)) {
                updateClauses.push(`reviewed_at = NOW()`);
            } else if (action === "skip") {
                updateClauses.push(`reviewed_at = NULL`);
            }
             // Add clipId and allowed states to params for WHERE clause
             updateParams.push(clipId); paramIndex++; // Now $2 is clipId
             updateParams.push(allowedSourceStates); paramIndex++; // Now $3 is allowed states

             // Construct the final UPDATE query
             const updateQuery = `
                 UPDATE clips
                 SET ${updateClauses.join(', ')}
                 WHERE id = $2 -- Use correct parameter index
                   AND ingest_state = ANY($3::text[]) -- Use correct parameter index
                   AND next_state IS NULL
                 RETURNING id;
             `;

            console.log(`Executing update for clip ${clipId}, action ${action}...`);
            const updateResult = await client.query(updateQuery, updateParams);

            // 8. Verify Update Success
            if (updateResult.rowCount === 0) {
                const checkResult = await client.query('SELECT ingest_state, next_state FROM clips WHERE id = $1', [clipId]);
                const finalState = checkResult.rows.length > 0 ? checkResult.rows[0] : { ingest_state: 'NOT FOUND', next_state: 'N/A' };
                console.warn(`Update failed for clip ${clipId}. Row count 0. Final state check:`, finalState);
                throw new Error(`Clip state changed or action pending (State: ${finalState.ingest_state}, Next: ${finalState.next_state}). Please refresh.`); // -> 409
            }

            console.log(`Update successful for clip ${clipId}. Row count: ${updateResult.rowCount}`);

            // 9. Commit Transaction
            await client.query('COMMIT');
            console.log(`Transaction committed for clip ${clipId}`);

            // 10. Prepare and Validate Success Response
            const responsePayload = {
                status: "success",
                clip_id: clipId,
                next_state: intendedNextState
            };
            const validatedResponse = ActionSuccessResponseSchema.parse(responsePayload);

            return json(validatedResponse);

        } catch (error) {
            // 11. Rollback on any error within the transaction block
            console.error(`Error during transaction for clip ${clipId}, action ${action}:`, error);
            await client.query('ROLLBACK');
            console.log(`Transaction rolled back for clip ${clipId}`);
            throw error; // Re-throw
        }

    } catch (error: any) {
        // Outer error handling
        console.error(`API Error in POST /api/clips/[id]/action (Clip ID: ${clipId}):`, error);

        let statusCode = 500;
        let message = "Internal server error performing action.";

        if (error instanceof z.ZodError) {
            statusCode = 500;
            message = "Internal server error preparing response.";
            console.error("Response validation failed:", error.flatten());
        } else if (error.message?.includes("not found")) {
            statusCode = 404;
            message = error.message;
        } else if (error.message?.includes("Action already pending") || error.message?.includes("cannot be performed from state") || error.message?.includes("state changed")) {
            statusCode = 409; // Conflict
            message = error.message;
        }

        return json({ detail: message }, { status: statusCode });

    } finally {
        if (client) {
            client.release();
            console.log(`DB client released for clip ${clipId}`);
        }
    }
}
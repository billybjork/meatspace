import { type APIEvent } from "@solidjs/start/server";
import { PoolClient } from "pg";
import { z } from "zod";

import { getDbClient } from "~/lib/db";
import { ClipActionPayloadSchema } from "~/schemas/clip";

// Helper to return JSON responses
function json(data: any, { status = 200 } = {}) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

// Allowed states for review actions
const ALLOWED_SOURCE_STATES_GENERAL = [
  'pending_review', 'review_skipped', 'merge_failed', 'split_failed',
  'keyframe_failed', 'embedding_failed', 'sprite_generation_failed', 'group_failed'
];

// Valid ingest_state for the previous clip when merging/grouping
const VALID_PREVIOUS_ACTION_STATES = [
  'pending_review', 'review_skipped', 'approved_pending_deletion',
  'review_approved', 'archived_pending_deletion', 'archived', 'grouped_complete'
];

// Map each action to its next_state
const ACTION_TO_STATE: Record<string, string> = {
  approve:            'approved_pending_deletion',
  skip:               'review_skipped',
  archive:            'archived_pending_deletion',
  retry_sprite_gen:   'pending_sprite_generation',
  merge_previous:     'marked_for_merge_into_previous',
  group_previous:     'grouped_complete',
};

export async function POST(event: APIEvent) {
  let client: PoolClient | null = null;
  let clipId: number;

  try {
    // Validate clip ID
    const idParam = event.params.id;
    if (!idParam || isNaN(Number(idParam))) {
      return json({ detail: "Invalid clip ID." }, { status: 400 });
    }
    clipId = Number(idParam);

    // Validate and parse payload
    let payload: z.infer<typeof ClipActionPayloadSchema>;
    try {
      payload = ClipActionPayloadSchema.parse(await event.request.json());
    } catch (e: any) {
      if (e instanceof z.ZodError) {
        return json({ detail: e.flatten().fieldErrors }, { status: 422 });
      }
      return json({ detail: "Bad request payload." }, { status: 400 });
    }
    const action = payload.action;
    const intendedNextState = ACTION_TO_STATE[action];
    if (!intendedNextState) {
      return json({ detail: `Unsupported action: ${action}` }, { status: 400 });
    }

    // Determine allowed source states
    const allowedSourceStates =
      action === 'retry_sprite_gen'
        ? ['sprite_generation_failed']
        : ALLOWED_SOURCE_STATES_GENERAL;

    // Start transaction & lock current clip
    client = await getDbClient();
    await client.query('BEGIN');
    const lockIdCurrent = 2;
    const lockIdPrevious = 3;
    await client.query('SELECT pg_advisory_xact_lock($1,$2)', [lockIdCurrent, clipId]);

    // Fetch current clip
    const cur = await client.query(
      `SELECT ingest_state, next_state, source_video_id, start_time_seconds
       FROM clips WHERE id=$1 FOR UPDATE`,
      [clipId]
    );
    if (cur.rowCount === 0) {
      await client.query('ROLLBACK');
      return json({ detail: "Clip not found." }, { status: 404 });
    }
    const {
      ingest_state: currentState,
      next_state: currentNextState,
      source_video_id: srcVid,
      start_time_seconds: startS,
    } = cur.rows[0];

    if (currentNextState !== null) {
      await client.query('ROLLBACK');
      return json({ detail: `Action already pending: '${currentNextState}'.` }, { status: 409 });
    }
    if (!allowedSourceStates.includes(currentState)) {
      await client.query('ROLLBACK');
      return json(
        { detail: `Cannot perform '${action}' from state '${currentState}'.` },
        { status: 409 }
      );
    }

    // Merge / Group logic
    let prevId: number | null = null;
    let prevNextState: string | null = null;
    if (action === 'merge_previous' || action === 'group_previous') {
      // Find & lock previous clip
      const pr = await client.query(
        `SELECT id, ingest_state FROM clips
         WHERE source_video_id=$1 AND start_time_seconds<$2
         ORDER BY start_time_seconds DESC, id DESC
         LIMIT 1 FOR UPDATE`,
        [srcVid, startS]
      );
      if (pr.rowCount === 0) {
        await client.query('ROLLBACK');
        return json({ detail: `No previous clip to ${action.split('_')[0]}.` }, { status: 400 });
      }
      prevId = pr.rows[0].id;
      const prevState = pr.rows[0].ingest_state;
      await client.query('SELECT pg_advisory_xact_lock($1,$2)', [lockIdPrevious, prevId]);
      if (!VALID_PREVIOUS_ACTION_STATES.includes(prevState)) {
        await client.query('ROLLBACK');
        return json(
          { detail: `Cannot ${action.split('_')[0]}: prev clip state '${prevState}' invalid.` },
          { status: 409 }
        );
      }

      if (action === 'merge_previous') {
        // Mark for merge
        const curMeta = JSON.stringify({ merge_target_clip_id: prevId });
        const prevMeta = JSON.stringify({ merge_source_clip_id: clipId });
        const targetPrevState = 'pending_merge_target';

        const r1 = await client.query(
          `UPDATE clips
           SET next_state=$1, action_committed_at=NOW(), processing_metadata=$2::jsonb,
               updated_at=NOW(), last_error=NULL, reviewed_at=NOW()
           WHERE id=$3 AND ingest_state=ANY($4::text[])`,
          [intendedNextState, curMeta, clipId, allowedSourceStates]
        );
        const r2 = await client.query(
          `UPDATE clips
           SET next_state=$1, action_committed_at=NOW(), processing_metadata=$2::jsonb,
               updated_at=NOW(), last_error=NULL
           WHERE id=$3 AND ingest_state=$4`,
          [targetPrevState, prevMeta, prevId, prevState]
        );

        if (r1.rowCount! < 1 || r2.rowCount! < 1) {
          await client.query('ROLLBACK');
          return json(
            { detail: `merge_previous failed on clips ${clipId}/${prevId}.` },
            { status: 409 }
          );
        }
        prevNextState = targetPrevState;

      } else {
        // Mark as grouped
        const targetGrpState = 'grouped_complete';

        const r1 = await client.query(
          `UPDATE clips
           SET next_state=$1, action_committed_at=NOW(), grouped_with_clip_id=$2,
               processing_metadata=NULL, updated_at=NOW(),
               last_error=NULL, reviewed_at=NOW()
           WHERE id=$3 AND ingest_state=ANY($4::text[])`,
          [targetGrpState, prevId, clipId, allowedSourceStates]
        );
        const r2 = await client.query(
          `UPDATE clips
           SET next_state=$1, action_committed_at=NOW(), processing_metadata=NULL,
               updated_at=NOW(), last_error=NULL, reviewed_at=NOW()
           WHERE id=$2 AND ingest_state=$3`,
          [targetGrpState, prevId, prevState]
        );

        if (r1.rowCount! < 1 || r2.rowCount! < 1) {
          await client.query('ROLLBACK');
          return json(
            { detail: `group_previous failed on clips ${clipId}/${prevId}.` },
            { status: 409 }
          );
        }
        prevNextState = targetGrpState;
      }

    } else {
      // Simple actions
      const clauses = [
        'next_state=$1', 'action_committed_at=NOW()', 'updated_at=NOW()',
        'last_error=NULL', 'processing_metadata=NULL', 'grouped_with_clip_id=NULL'
      ];
      if (['approve','archive','group_previous'].includes(action)) {
        clauses.push('reviewed_at=NOW()');
      } else {
        clauses.push('reviewed_at=NULL');
      }
      const params = [intendedNextState, clipId, allowedSourceStates];
      const uq = `
        UPDATE clips SET ${clauses.join(', ')}
        WHERE id=$2 AND ingest_state=ANY($3::text[])
        RETURNING id`;
      const ur = await client.query(uq, params);
      if ((ur.rowCount ?? 0) < 1) {
        await client.query('ROLLBACK');
        return json(
          { detail: `Action '${action}' could not be applied. Refresh and retry.` },
          { status: 409 }
        );
      }
    }

    // Commit and return success
    await client.query('COMMIT');
    const resp: any = { status: 'success', clip_id: clipId, next_state: intendedNextState };
    if (prevId !== null) {
      resp.previous_clip_id = prevId;
      resp.previous_clip_next_state = prevNextState;
    }
    return json(resp);

  } catch (e: any) {
    console.error("Error processing clip action:", e);
    if (client) await client.query('ROLLBACK');
    const st = (e.statusCode as number) || 500;
    return json({ detail: e.message || "Internal server error." }, { status: st });

  } finally {
    if (client) client.release();
  }
}
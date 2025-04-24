import { createResource, Show } from 'solid-js';
import { Title } from '@solidjs/meta';
import styles from '~/styles/intake-review.css?inline';

import type { NextClipResponse, ErrorResponse } from '~/lib/types';

const API_BASE_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';

async function fetchNextClip(): Promise<NextClipResponse> {
  try {
    const response = await fetch(`${API_BASE_URL}/api/clips/review/next`, {
      method: 'GET',
      headers: { Accept: 'application/json' },
    });

    if (!response.ok) {
      let errorDetail = `HTTP error ${response.status}`;
      try {
        const errorJson: ErrorResponse = await response.json();
        if (typeof errorJson.detail === 'string') errorDetail = errorJson.detail;
        else if (Array.isArray(errorJson.detail) && errorJson.detail.length)
          errorDetail = errorJson.detail.map((d) => d.msg).join(', ');
      } catch {
        /* fallback keeps original errorDetail */
      }
      throw new Error(errorDetail);
    }
    return await response.json();
  } catch (err) {
    console.error('Failed to fetch next clip:', err);
    throw err;
  }
}

function isNextClipResponse(obj: unknown): obj is NextClipResponse {
  return typeof obj === 'object' && obj !== null && 'done' in obj && 'clip' in obj;
}

export default function IntakeReviewPage() {
  const [clipResource, { refetch }] = createResource(fetchNextClip);

  /* ---------- helper ---------------- */
  const fmtSeconds = (maybeNumber: unknown) =>
    typeof maybeNumber === 'number' ? maybeNumber.toFixed(3) : '???.???';
  /* ---------------------------------- */

  return (
    <>
      <style>{styles}</style>
      <Title>Review Clip</Title>
      <h1>Review Clip</h1>

      <div class="review-container">
        <Show when={clipResource.loading}>
          <p style="text-align:center; padding:40px 20px; color:#666;">Loading next clip...</p>
        </Show>

        <Show when={clipResource.error}>
          <p style="color: red; text-align: center;">
            Error loading clip: {clipResource.error?.message || 'Unknown error'}
          </p>
          <div style="text-align:center; margin-top:10px;">
            <button onClick={refetch}>Retry</button>
          </div>
        </Show>

        {/* ---------- main render ---------- */}
        {(() => {
          const data = clipResource();
          if (!isNextClipResponse(data)) return null;

          if (data.done || !data.clip) {
            return (
              <div>
                <p style="text-align:center; padding:40px 20px; color:#666;">
                  No clips pending review. You’re all caught up 🎉
                </p>
                <div style="text-align:center; margin-top:10px;">
                  <button onClick={() => window.location.reload()}>Refresh Page</button>
                </div>
              </div>
            );
          }

          const clip = data.clip;

          // Gracefully support either start_s / end_s OR start_time_seconds / end_time_seconds
          const startSeconds =
            clip.start_s ?? (clip as unknown as { start_time_seconds?: number }).start_time_seconds;
          const endSeconds =
            clip.end_s ?? (clip as unknown as { end_time_seconds?: number }).end_time_seconds;

          return (
            <div
              class="clip-review-item"
              id={`clip-${clip.db_id}`}
              data-clip-id={clip.db_id}
              data-autoplay="true"
              data-previous-clip-id={clip.previous_clip_id ?? ''}
            >
              <div class="clip-info">
                <strong>{clip.title}</strong>
                (Source: {clip.source_title})<br />
                Clip ID: {clip.db_id} | Source ID: {clip.source_video_id} | Time:{' '}
                {fmtSeconds(startSeconds)}s – {fmtSeconds(endSeconds)}s
                <Show when={clip.ingest_state === 'sprite_generation_failed'}>
                  {' | '}
                  <strong style="color:orange;">Sprite Failed</strong>
                </Show>{' '}
                | Status:{' '}
                <code style="background:#eee; padding:1px 4px; border-radius:3px;">
                  {clip.ingest_state}
                </code>
                <Show when={clip.last_error}>
                  <br />
                  <span style="color:red; font-size: 0.9em;">Last Error: {clip.last_error}</span>
                </Show>
              </div>

              <div class="sprite-viewer" id={`sprite-viewer-${clip.db_id}`} tabindex="0">
                Sprite Player Component Placeholder
              </div>

              <div class="playback-controls" id={`controls-${clip.db_id}`}>
                Playback Controls Placeholder
              </div>

              <div class="clip-actions">Action Buttons Placeholder</div>

              <div
                class="split-controls"
                id={`split-controls-${clip.db_id}`}
                style={{ display: 'none' }}
              >
                Split Controls Placeholder
              </div>

              <div class="action-feedback"></div>
            </div>
          );
        })()}
        {/* ---------- /main render ---------- */}
      </div>
    </>
  );
}
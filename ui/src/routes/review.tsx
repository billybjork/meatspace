// src/routes/review.tsx

import { Show, createSignal, onMount } from 'solid-js';
import { Title } from '@solidjs/meta';
import { useQuery, useQueryClient, useMutation } from '@tanstack/solid-query';
import type SpritePlayerClass from '~/lib/sprite-player';
import ClipActions from '~/components/ClipActions';
import SpritePlayer from '~/components/SpritePlayer';
import SplitControls from '~/components/SplitControls';

import type { NextClipResponse, ApiErrorResponse } from '~/schemas/clip';

// Fetch the next clip to review
const fetchNextClip = async (): Promise<NextClipResponse> => {
  console.log('Fetching /api/clips/review/next...');
  const response = await fetch('/api/clips/review/next', {
    method: 'GET',
    headers: { Accept: 'application/json' },
  });
  if (!response.ok) {
    let errorDetail = `HTTP error! status: ${response.status}`;
    try {
      const errorData: ApiErrorResponse = await response.json();
      if (typeof errorData.detail === 'string') {
        errorDetail = errorData.detail;
      } else if (
        Array.isArray(errorData.detail) &&
        errorData.detail.length > 0 &&
        'msg' in errorData.detail[0]
      ) {
        errorDetail = errorData.detail.map((d) => d.msg).join(', ');
      } else if (
        typeof errorData.detail === 'object' &&
        errorData.detail !== null
      ) {
        errorDetail = JSON.stringify(errorData.detail);
      }
    } catch {
      console.warn(
        'Response was not valid JSON on error:',
        await response.text().catch(() => '')
      );
    }
    console.error('Fetch failed:', errorDetail);
    throw new Error(errorDetail);
  }
  const data: NextClipResponse = await response.json();
  console.log('Received data:', data);
  return data;
};

// Helper to format seconds
const fmtSeconds = (maybeNumber: unknown): string =>
  typeof maybeNumber === 'number' ? maybeNumber.toFixed(3) : '???.???';

export default function IntakeReviewPage() {
  const queryClient = useQueryClient();

  // TanStack Query for next clip (only enabled on client)
  const query = useQuery<NextClipResponse, Error>(() => ({
    queryKey: ['nextClipForReview'],
    queryFn: fetchNextClip,
    refetchOnWindowFocus: false,
    retry: 1,
    staleTime: 1000 * 60,
    experimental_prefetchInRender: true,
  }));

  // Split-mode state
  const [splitMode, setSplitMode] = createSignal(false);
  const [splitFrame, setSplitFrame] = createSignal(0);

  // References
  let spritePlayerRef!: SpritePlayerClass;
  let currentClipDbId = 0;

  // Mutation for split
  const splitMutation = useMutation<unknown, Error, { id: number; frame: number }>(
    () => ({
      mutationFn: async ({ id, frame }) => {
        const res = await fetch(`/api/clips/${id}/split`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
          body: JSON.stringify({ split_request_at_frame: frame }),
        });
        const data = await res.json().catch(() => null);
        if (!res.ok) throw new Error(data?.detail ?? res.statusText);
        return data;
      },
      onSuccess: () => {
        queryClient.invalidateQueries({ queryKey: ['nextClipForReview'] });
      },
      onError: (err: Error) => {
        console.error('Split request failed:', err.message);
      },
    })
  );

  // Global key handler
  function handleKeyDown(e: KeyboardEvent) {
    if (!spritePlayerRef) return;

    // Enter split mode on arrow keys
    if (!splitMode() && (e.key === 'ArrowLeft' || e.key === 'ArrowRight')) {
      setSplitMode(true);
      spritePlayerRef.pause();
      e.preventDefault();
      return;
    }

    // In split mode: step, confirm, or cancel
    if (splitMode()) {
      switch (e.key) {
        case 'ArrowLeft':
          spritePlayerRef.updateFrame(spritePlayerRef.getCurrentFrame() - 1, true);
          e.preventDefault();
          break;
        case 'ArrowRight':
          spritePlayerRef.updateFrame(spritePlayerRef.getCurrentFrame() + 1, true);
          e.preventDefault();
          break;
        case 'Enter':
          splitMutation.mutate({
            id: currentClipDbId,
            frame: spritePlayerRef.getCurrentFrame(),
          });
          e.preventDefault();
          break;
        case 'Escape':
          setSplitMode(false);
          spritePlayerRef.play();
          e.preventDefault();
          break;
      }
    }
  }

  onMount(() => {
    // only in browser
    if (typeof window === 'undefined') return;
    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  });

  return (
    <main class="p-4">
      <Title>Intake Review</Title>
      <h1 class="text-2xl font-bold mb-4">Clip Review</h1>

      <div class="review-container">
        {/* Loading */}
        <Show when={query.isLoading}>
          <div class="text-center p-6 text-gray-600">Loading next clip...</div>
        </Show>

        {/* Error */}
        <Show when={query.isError}>
          <div class="bg-red-100 border border-red-400 text-red-700 px-4 py-3 rounded mb-4" role="alert">
            <strong class="font-bold">Error Loading Clip:</strong>
            <span class="block sm:inline ml-2">{query.error?.message}</span>
            <div class="text-center mt-2">
              <button
                class="bg-blue-500 hover:bg-blue-700 text-white font-bold py-1 px-3 rounded text-sm"
                onClick={() => query.refetch()}
                disabled={query.isFetching}
              >
                Retry
              </button>
            </div>
          </div>
        </Show>

        {/* Success */}
        <Show when={query.isSuccess && query.data} keyed>
          {/* No more clips */}
          <Show when={query.data!.done} keyed>
            <div class="bg-green-100 border border-green-400 text-green-700 px-4 py-3 rounded text-center">
              No clips pending review. You’re all caught up 🎉
              <div class="mt-2">
                <button
                  class="bg-blue-500 hover:bg-blue-700 text-white font-bold py-1 px-3 rounded text-sm"
                  onClick={() => query.refetch()}
                  disabled={query.isFetching}
                >
                  Check Again
                </button>
              </div>
            </div>
          </Show>

          {/* Clip for review */}
          <Show when={!query.data!.done && query.data!.clip} keyed>
            {(clip) => {
              currentClipDbId = clip.db_id;
              return (
                <div
                  class="clip-review-item border rounded p-4 shadow"
                  id={`clip-${clip.db_id}`}
                  data-clip-id={clip.db_id}
                  tabindex="0"
                >
                  {/* Clip Info */}
                  <div class="clip-info mb-4">
                    <h2 class="text-xl font-semibold mb-2">
                      {clip.title}{' '}
                      <span class="text-sm text-gray-500">(ID: {clip.db_id})</span>
                    </h2>
                    <div class="grid grid-cols-1 md:grid-cols-2 gap-4 text-sm">
                      <div>
                        <strong>Source:</strong> {clip.source_title} (ID: {clip.source_video_id})
                      </div>
                      <div>
                        <strong>Identifier:</strong> {clip.identifier}
                      </div>
                      <div>
                        <strong>Time:</strong> {fmtSeconds(clip.start_s)}s – {fmtSeconds(clip.end_s)}s
                      </div>
                      <div>
                        <strong>State:</strong>{' '}
                        <code class="font-mono bg-gray-200 px-1 rounded text-xs">{clip.ingest_state}</code>
                        <Show when={clip.ingest_state === 'sprite_generation_failed'}>
                          <span class="ml-1 text-orange-600 font-bold">(Sprite Failed!)</span>
                        </Show>
                      </div>
                      <div>
                        <strong>Prev. Clip Actionable:</strong>{' '}
                        {clip.can_action_previous ? 'Yes' : 'No'}{' '}
                        {clip.previous_clip_id && `(ID: ${clip.previous_clip_id})`}
                      </div>
                      <Show when={clip.last_error} keyed>
                        <div class="md:col-span-2 text-red-600 text-xs mt-1">
                          <strong>Last Error:</strong> {clip.last_error}
                        </div>
                      </Show>
                    </div>
                  </div>

                  {/* Sprite Player */}
                  <SpritePlayer
                    clipId={clip.db_id}
                    spriteUrl={clip.sprite_url!}
                    spriteMeta={JSON.stringify(clip.sprite_meta!)}
                    autoplay
                    onSplitFrame={setSplitFrame}
                    playerRef={(p) => (spritePlayerRef = p)}
                  />

                  {/* Actions + Split */}
                  <div class="flex flex-wrap items-center gap-2 mt-4">
                    <ClipActions
                      clipId={clip.db_id}
                      canActionPrevious={clip.can_action_previous}
                    />
                    <button
                      class="action-btn split-mode-btn"
                      onClick={() => {
                        setSplitMode(true);
                        spritePlayerRef?.pause?.();
                      }}
                      disabled={
                        !clip.sprite_url || splitMode() || splitMutation.isPending
                      }
                      title="Split (←/→, Enter)"
                    >
                      ✂️ Split
                    </button>
                  </div>

                  {/* Split Controls */}
                  <Show when={splitMode()} keyed>
                    <SplitControls
                      frame={splitFrame()}
                      totalFrames={clip.sprite_meta?.clip_total_frames ?? 0}
                      fps={clip.sprite_meta?.clip_fps ?? 0}
                      isPending={splitMutation.isPending}
                      error={splitMutation.error?.message}
                      onConfirm={() =>
                        splitMutation.mutate({
                          id: clip.db_id,
                          frame: splitFrame(),
                        })
                      }
                      onCancel={() => {
                        setSplitMode(false);
                        spritePlayerRef?.play?.();
                      }}
                    />
                  </Show>

                  {/* Feedback */}
                  <div class="action-feedback min-h-[1em] mt-2 text-center"></div>
                </div>
              );
            }}
          </Show>
        </Show>
      </div>
    </main>
  );
}
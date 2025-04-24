import { Show } from 'solid-js';
import { Title } from '@solidjs/meta';
import { useQuery, useQueryClient } from '@tanstack/solid-query';
import ClipActions from '~/components/ClipActions';

// Import the TS type for the response and the clip itself
import type { NextClipResponse, ApiErrorResponse } from '~/schemas/clip';

// Define the fetcher function for TanStack Query
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
    } catch (e) {
      console.warn('Response was not valid JSON on error:', await response.text().catch(() => ''));
    }
    console.error('Fetch failed:', errorDetail);
    throw new Error(errorDetail);
  }

  const data: NextClipResponse = await response.json();
  console.log('Received data:', data);
  return data;
};

// Helper function for formatting seconds
const fmtSeconds = (maybeNumber: unknown): string =>
  typeof maybeNumber === 'number' ? maybeNumber.toFixed(3) : '???.???';

export default function IntakeReviewPage() {
  const queryClient = useQueryClient();

  // --- TanStack Query ---
  const query = useQuery<NextClipResponse, Error>(() => ({
    queryKey: ['nextClipForReview'],
    queryFn: fetchNextClip,
    refetchOnWindowFocus: false,
    retry: 1,
    staleTime: 1000 * 60,
  }));

  return (
    <main class="p-4">
      <Title>Intake Review</Title>
      <h1 class="text-2xl font-bold mb-4">Clip Review</h1>

      <div class="review-container">
        {/* Loading State */}
        <Show when={query.isLoading}>
          <div class="text-center p-6 text-gray-600">Loading next clip...</div>
        </Show>

        {/* Error State */}
        <Show when={query.isError}>
          <div class="bg-red-100 border border-red-400 text-red-700 px-4 py-3 rounded relative mb-4" role="alert">
            <strong class="font-bold">Error Loading Clip:</strong>
            <span class="block sm:inline ml-2">{query.error?.message || 'An unknown error occurred'}</span>
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

        {/* Success State */}
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

          {/* Clip available for review */}
          <Show when={!query.data!.done && query.data!.clip} keyed>
            {(clip) => (
              <div
                class="clip-review-item border rounded p-4 shadow"
                id={`clip-${clip.db_id}`}
                data-clip-id={clip.db_id}
                data-previous-clip-id={clip.previous_clip_id ?? ''}
              >
                {/* Clip Info Section */}
                <div class="clip-info mb-4">
                  <h2 class="text-xl font-semibold mb-2">
                    {clip.title}{' '}
                    <span class="text-sm font-normal text-gray-500">
                      (ID: {clip.db_id})
                    </span>
                  </h2>
                  <div class="grid grid-cols-1 md:grid-cols-2 gap-x-4 gap-y-1 text-sm">
                    <div>
                      <strong>Source:</strong> {clip.source_title} (ID: {clip.source_video_id})
                    </div>
                    <div>
                      <strong>Identifier:</strong> {clip.identifier}
                    </div>
                    <div>
                      <strong>Time:</strong> {fmtSeconds(clip.start_s)}s - {fmtSeconds(clip.end_s)}s
                    </div>
                    <div>
                      <strong>State:</strong>{' '}
                      <code class="font-mono bg-gray-200 px-1 rounded text-xs">
                        {clip.ingest_state}
                      </code>
                      <Show when={clip.ingest_state === 'sprite_generation_failed'}>
                        <span class="ml-1 text-orange-600 font-bold">
                          (Sprite Failed!)
                        </span>
                      </Show>
                    </div>
                    <div>
                      <strong>Prev. Clip Actionable:</strong>{' '}
                      {clip.can_action_previous ? 'Yes' : 'No'}{' '}
                      {clip.previous_clip_id ? `(ID: ${clip.previous_clip_id})` : ''}
                    </div>
                    <Show when={clip.last_error} keyed>
                      <div class="md:col-span-2 text-red-600 text-xs mt-1">
                        <strong>Last Error:</strong> {clip.last_error}
                      </div>
                    </Show>
                  </div>
                </div>

                {/* Sprite Player Placeholder */}
                <div
                  class="sprite-viewer bg-gray-100 border border-dashed border-gray-400 p-4 my-4 text-center"
                  id={`sprite-viewer-${clip.db_id}`}
                  tabindex="0"
                >
                  <p class="text-gray-600">Sprite Player Area</p>
                  <Show when={clip.sprite_url} fallback={<p class="text-xs mt-1 text-orange-600">(No sprite artifact found)</p>} keyed>
                    <p class="text-xs mt-2 break-all">Sprite URL: {clip.sprite_url}</p>
                    <Show when={clip.sprite_meta} keyed>
                      <pre class="text-xs mt-1 text-left bg-gray-200 p-1 rounded overflow-x-auto max-h-24">
                        {JSON.stringify(clip.sprite_meta, null, 2)}
                      </pre>
                    </Show>
                  </Show>
                </div>

                {/* Playback Controls Placeholder */}
                <div class="playback-controls bg-gray-100 border border-dashed border-gray-400 p-4 my-4 text-center" id={`controls-${clip.db_id}`}>
                  <p class="text-gray-600">Playback Controls Area</p>
                </div>

                {/* Action Buttons via ClipActions component */}
                <ClipActions clipId={clip.db_id} />

                {/* Split Controls Placeholder */}
                <div
                  class="split-controls bg-gray-100 border border-dashed border-gray-400 p-4 my-4 text-center"
                  id={`split-controls-${clip.db_id}`}
                  style={{ display: 'none' }}
                >
                  <p class="text-gray-600">Split Controls Area</p>
                </div>

                {/* Feedback Placeholder for other actions */}
                <div class="action-feedback min-h-[1em] mt-2 text-center"></div>
              </div>
            )}
          </Show>
        </Show>
      </div>
    </main>
  );
}
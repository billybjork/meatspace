// ui/src/routes/review.tsx
import { Show } from 'solid-js'; // Removed createSignal, onMount, createResource
import { Title } from '@solidjs/meta';
import { useQuery, useQueryClient } from "@tanstack/solid-query"; // Use TanStack Query

// Import the TS type for the response and the clip itself
import type { NextClipResponse, ClipForReview, ApiErrorResponse } from "~/schemas/clip"; // Added ApiErrorResponse

// Optional: Import CSS if you have specific styles
// import styles from '~/styles/intake-review.css?inline';

// Define the fetcher function for TanStack Query
const fetchNextClip = async (): Promise<NextClipResponse> => {
  console.log("Fetching /api/clips/review/next...");
  // Use a relative path to fetch from the same origin (your SolidStart server)
  const response = await fetch("/api/clips/review/next", {
    method: 'GET',
    headers: { Accept: 'application/json' },
  });

  if (!response.ok) {
    // Attempt to parse error detail if available
    let errorDetail = `HTTP error! status: ${response.status}`;
    try {
        const errorData: ApiErrorResponse = await response.json(); // Use ApiErrorResponse type
        if (typeof errorData.detail === 'string') {
            errorDetail = errorData.detail;
        } else if (Array.isArray(errorData.detail) && errorData.detail.length > 0 && 'msg' in errorData.detail[0]) {
             // Handle FastAPI-like validation error array
            errorDetail = errorData.detail.map((d) => d.msg).join(', ');
        } else if (typeof errorData.detail === 'object' && errorData.detail !== null) {
            // Handle other potential object structures
            errorDetail = JSON.stringify(errorData.detail);
        }
        // If parsing fails or detail is not recognized, errorDetail retains the HTTP status message
    } catch (e) {
        // Ignore JSON parsing error if body is not JSON or empty
        console.warn("Response was not valid JSON on error:", await response.text().catch(() => ''));
    }
    console.error("Fetch failed:", errorDetail);
    throw new Error(errorDetail); // Throw an error for TanStack Query to catch
  }

  const data: NextClipResponse = await response.json();
  console.log("Received data:", data);
  // Optional: Add Zod parsing here for client-side safety belt if desired
  // try {
  //   return NextClipResponseSchema.parse(data);
  // } catch (validationError) {
  //    console.error("Client-side validation failed:", validationError);
  //    throw new Error("Received invalid data format from API.");
  // }
  return data;
};

// Helper function for formatting seconds
const fmtSeconds = (maybeNumber: unknown): string =>
  typeof maybeNumber === 'number' ? maybeNumber.toFixed(3) : '???.???';


export default function IntakeReviewPage() {
  const queryClient = useQueryClient(); // Get query client instance for potential invalidation later

  // --- TanStack Query ---
  // Use useQuery to fetch and manage the state of the next clip
  const query = useQuery<NextClipResponse, Error>(() => ({ // Use arrow function for options object
    queryKey: ["nextClipForReview"], // Unique key for this query
    queryFn: fetchNextClip,          // The function that fetches the data
    refetchOnWindowFocus: false,     // Prevent refetching on window focus
    retry: 1,                        // Retry once on failure
    staleTime: 1000 * 60,            // Consider data fresh for 1 minute
  }));

  // --- Component Rendering ---
  return (
    <main class="p-4">
      {/* Optional: Add inline styles if you removed the CSS import */}
      {/* <style>{styles}</style> */}
      <Title>Intake Review</Title>
      <h1 class="text-2xl font-bold mb-4">Clip Review</h1>

      <div class="review-container"> {/* Add a container if needed */}
          {/* Loading State */}
          <Show when={query.isLoading}>
            <div class="text-center p-6 text-gray-600">Loading next clip...</div>
          </Show>

          {/* Error State */}
          <Show when={query.isError}>
            <div class="bg-red-100 border border-red-400 text-red-700 px-4 py-3 rounded relative mb-4" role="alert">
              <strong class="font-bold">Error Loading Clip:</strong>
              <span class="block sm:inline ml-2">{query.error?.message || "An unknown error occurred"}</span>
               {/* Add a retry button */}
               <div class="text-center mt-2">
                 <button
                    class="bg-blue-500 hover:bg-blue-700 text-white font-bold py-1 px-3 rounded text-sm"
                    onClick={() => query.refetch()} // Use query.refetch() from TanStack Query
                    disabled={query.isFetching} // Disable while refetching
                 >
                   Retry
                 </button>
               </div>
            </div>
          </Show>

          {/* Success State */}
          <Show when={query.isSuccess && query.data}>
            {/* Case 1: No more clips */}
            <Show when={query.data!.done}>
              <div class="bg-green-100 border border-green-400 text-green-700 px-4 py-3 rounded text-center">
                No clips pending review. You’re all caught up 🎉
                <div class="mt-2">
                   <button
                      class="bg-blue-500 hover:bg-blue-700 text-white font-bold py-1 px-3 rounded text-sm"
                      onClick={() => query.refetch()} // Allow manual refresh check
                      disabled={query.isFetching}
                   >
                     Check Again
                   </button>
                </div>
              </div>
            </Show>

            {/* Case 2: Clip available for review */}
            <Show when={!query.data!.done && query.data!.clip}>
              {(clip) => ( // Use Show's render prop feature to get non-nullable clip
                <div
                  class="clip-review-item border rounded p-4 shadow" // Combine classes
                  id={`clip-${clip().db_id}`}
                  data-clip-id={clip().db_id}
                  // Consider managing autoplay via component state later if needed
                  // data-autoplay="true"
                  data-previous-clip-id={clip().previous_clip_id ?? ''}
                >
                  {/* --- Clip Info Section --- */}
                  <div class="clip-info mb-4">
                     <h2 class="text-xl font-semibold mb-2">{clip().title} <span class="text-sm font-normal text-gray-500">(ID: {clip().db_id})</span></h2>
                     <div class="grid grid-cols-1 md:grid-cols-2 gap-x-4 gap-y-1 text-sm">
                       <div><strong>Source:</strong> {clip().source_title} (ID: {clip().source_video_id})</div>
                       <div><strong>Identifier:</strong> {clip().identifier}</div>
                       {/* Use fmtSeconds helper */}
                       <div><strong>Time:</strong> {fmtSeconds(clip().start_s)}s - {fmtSeconds(clip().end_s)}s</div>
                       <div>
                         <strong>State:</strong> <code class="font-mono bg-gray-200 px-1 rounded text-xs">{clip().ingest_state}</code>
                         {/* Highlight specific states */}
                         <Show when={clip().ingest_state === 'sprite_generation_failed'}>
                           <span class="ml-1 text-orange-600 font-bold">(Sprite Failed!)</span>
                         </Show>
                       </div>
                       <div>
                           <strong>Prev. Clip Actionable:</strong> {clip().can_action_previous ? "Yes" : "No"} {clip().previous_clip_id ? `(ID: ${clip().previous_clip_id})` : ''}
                       </div>
                       {/* Show last error if present */}
                       <Show when={clip().last_error}>
                         <div class="md:col-span-2 text-red-600 text-xs mt-1">
                           <strong>Last Error:</strong> {clip().last_error}
                         </div>
                       </Show>
                     </div>
                  </div>

                  {/* --- Placeholder for Sprite Player --- */}
                  <div class="sprite-viewer bg-gray-100 border border-dashed border-gray-400 p-4 my-4 text-center" id={`sprite-viewer-${clip().db_id}`} tabindex="0">
                    <p class="text-gray-600">Sprite Player Area</p>
                    {/* Display sprite URL and meta if available for debugging */}
                    <Show when={clip().sprite_url} fallback={<p class="text-xs mt-1 text-orange-600">(No sprite artifact found)</p>}>
                        <p class="text-xs mt-2 break-all">Sprite URL: {clip().sprite_url}</p>
                        <Show when={clip().sprite_meta}>
                           <pre class="text-xs mt-1 text-left bg-gray-200 p-1 rounded overflow-x-auto max-h-24">
                             {JSON.stringify(clip().sprite_meta, null, 2)}
                           </pre>
                        </Show>
                    </Show>
                  </div>

                  {/* --- Placeholder for Playback Controls --- */}
                  <div class="playback-controls bg-gray-100 border border-dashed border-gray-400 p-4 my-4 text-center" id={`controls-${clip().db_id}`}>
                    <p class="text-gray-600">Playback Controls Area</p>
                  </div>

                  {/* --- Placeholder for Action Buttons --- */}
                  <div class="clip-actions bg-gray-100 border border-dashed border-gray-400 p-4 my-4 text-center">
                     <p class="text-gray-600">Action Buttons Area</p>
                  </div>

                  {/* --- Placeholder for Split Controls (Initially Hidden) --- */}
                  <div
                    class="split-controls bg-gray-100 border border-dashed border-gray-400 p-4 my-4 text-center"
                    id={`split-controls-${clip().db_id}`}
                    style={{ display: 'none' }} // Keep hidden for now
                  >
                    <p class="text-gray-600">Split Controls Area</p>
                  </div>

                  {/* --- Placeholder for Feedback --- */}
                  <div class="action-feedback min-h-[1em] mt-2 text-center">
                     {/* Feedback messages will go here */}
                  </div>
                </div>
              )}
            </Show>
          </Show>
      </div> {/* End review-container */}
    </main>
  );
}
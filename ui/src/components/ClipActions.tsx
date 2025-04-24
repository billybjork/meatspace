import { Show } from 'solid-js';
import { useMutation, useQueryClient } from '@tanstack/solid-query';

type ActionVariables = { id: number | string; action: string };

/**
 * Renders Approve/Skip/Archive buttons for a clip.
 * Uses Solid Query's useMutation wrapped in a function (per Solid API).
 */
export default function ClipActions(props: { clipId: number | string }) {
  const queryClient = useQueryClient();

  const mutation = useMutation<unknown, Error, ActionVariables>(() => ({
    // The async mutation function
    mutationFn: async ({ id, action }: ActionVariables) => {
      const response = await fetch(`/api/clips/${id}/action`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
        body: JSON.stringify({ action }),
      });
      const data = await response.json().catch(() => null);
      if (!response.ok) {
        const detail = data?.detail ?? response.statusText;
        throw new Error(typeof detail === 'string' ? detail : JSON.stringify(detail));
      }
      return data;
    },
    // On success, refetch the next clip
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['nextClipForReview'] });
    },
  }));

  return (
    <div class="clip-actions bg-gray-100 border border-dashed border-gray-400 p-4 my-4 text-center">
      <Show when={!mutation.isPending} fallback={<div class="text-gray-600">Processing...</div>}>
        <button
          class="action-btn approve-btn mr-2"
          onClick={() => mutation.mutate({ id: props.clipId, action: 'approve' })}
          disabled={mutation.isPending}
          title="A + Enter"
        >
          ✅ Approve
        </button>
        <button
          class="action-btn skip-btn mr-2"
          onClick={() => mutation.mutate({ id: props.clipId, action: 'skip' })}
          disabled={mutation.isPending}
          title="S + Enter"
        >
          ⏭️ Skip
        </button>
        <button
          class="action-btn archive-btn"
          onClick={() => mutation.mutate({ id: props.clipId, action: 'archive' })}
          disabled={mutation.isPending}
          title="D + Enter"
        >
          🗑️ Archive
        </button>
      </Show>
      <Show when={mutation.isError} keyed>
        <div class="text-red-600 mt-2">Error: {mutation.error!.message}</div>
      </Show>
    </div>
  );
}
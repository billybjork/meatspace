defmodule FrontendWeb.ReviewLive do
  @moduledoc """
  Optimistic-UI review queue.

  * **current**  – clip on screen
  * **future**   – pre-fetched queue (max 5)
  * **history**  – last 5 clips, supports Undo (via back button)
  """

  use FrontendWeb, :live_view
  import FrontendWeb.ReviewComponents,
    only: [sprite_player: 1, review_buttons: 1] # Removed undo_toast import

  alias Frontend.Clips
  alias Frontend.Clips.Clip

  @prefetch          6   # 1 current + 5 future
  @refill_threshold  3
  @history_limit     5

  # -------------------------------------------------------------------------
  # Mount: Initialize queue on socket assigns
  # -------------------------------------------------------------------------
  @impl true
  def mount(_params, _session, socket) do
    clips = Clips.next_pending_review_clips(@prefetch)

    case clips do
      [] ->
        {:ok, assign(socket,
                page_state: :empty,
                current:    nil,
                future:     [],
                history:    [])} # Removed undo_ctx

      [cur | fut] ->
        {:ok, socket
          |> assign(
            current:    cur,
            future:     fut,
            history:    [], # Removed undo_ctx
            page_state: :reviewing)}
    end
  end

  # -------------------------------------------------------------------------
  # Events: Handle user actions
  # -------------------------------------------------------------------------

  # Merge action: logs merge, optimistically advances UI
  @impl true
  def handle_event("select", %{"action" => "merge"},
                   %{assigns: %{current: curr, history: [prev | _]}} = socket) do
    socket =
      socket
      |> push_history(curr)
      |> advance_queue()
      # Removed assign(:undo_ctx, ...)
      |> refill_future()

    {:noreply, persist_async(socket, {:merge, prev, curr})}
  end

  # Other actions: approve, skip, archive
  @impl true
  def handle_event("select", %{"action" => action}, %{assigns: %{current: clip}} = socket) do
    socket =
      socket
      |> push_history(clip)
      |> advance_queue()
      # Removed assign(:undo_ctx, ...)
      |> refill_future()

    {:noreply, persist_async(socket, clip.id, action)}
  end

  # Undo with no history: should not be clickable, but handle defensively
  @impl true
  def handle_event("undo", _params, %{assigns: %{history: []}} = socket) do
    # The button should be disabled, but if event fires, do nothing harmful
    {:noreply, socket} # Simplified return
  end

  # Undo with history: revert to previous clip
  @impl true
  def handle_event("undo", _params, %{assigns: %{history: [prev | rest], current: cur, future: fut}} = socket) do
    socket =
      socket
      |> assign(
           current:  prev,
           future:   [cur | fut],
           history:  rest,
           # Removed undo_ctx: nil
           page_state: :reviewing
         )
      |> refill_future()

    {:noreply, persist_async(socket, prev.id, "undo")}
  end

  # -------------------------------------------------------------------------
  # Background persistence: fire off DB writes asynchronously
  # -------------------------------------------------------------------------

  # Merge: call request_merge_and_fetch_next to log both clips
  defp persist_async(socket, {:merge, prev, curr}) do
    Phoenix.LiveView.start_async(socket, {:merge_pair, {prev.id, curr.id}}, fn ->
      Clips.request_merge_and_fetch_next(prev, curr)
    end)
  end

  # Single-clip actions: call select_clip_and_fetch_next
  defp persist_async(socket, clip_id, action) do
    Phoenix.LiveView.start_async(socket, {:persist, clip_id}, fn ->
      Clips.select_clip_and_fetch_next(%Clip{id: clip_id}, action)
    end)
  end

  # -------------------------------------------------------------------------
  # Queue refill: fetch more clips when running low
  # -------------------------------------------------------------------------

  # No refill when empty
  defp refill_future(%{assigns: %{current: nil}} = socket), do: socket

  defp refill_future(%{assigns: assigns} = socket) do
    if length(assigns.future) < @refill_threshold do
      exclude_ids =
        [assigns.current | assigns.future ++ assigns.history]
        |> Enum.filter(& &1)
        |> Enum.map(& &1.id)

      needed = @prefetch - (length(assigns.future) + 1)
      new_clips = Clips.next_pending_review_clips(needed, exclude_ids)

      update(socket, :future, &(&1 ++ new_clips))
    else
      socket
    end
  end

  # -------------------------------------------------------------------------
  # Helpers: push_history and advance_queue
  # -------------------------------------------------------------------------

  defp push_history(socket, clip) do
    update(socket, :history, fn history ->
      [clip | Enum.take(history, @history_limit - 1)]
    end)
  end

  defp advance_queue(%{assigns: %{future: []}} = socket) do
    assign(socket, current: nil, page_state: :empty)
  end

  defp advance_queue(%{assigns: %{future: [next | rest]}} = socket) do
    assign(socket, current: next, future: rest, page_state: :reviewing)
  end

  # -------------------------------------------------------------------------
  # Async callbacks: handle completion of DB writes
  # -------------------------------------------------------------------------

  # Generic single-clip persistence success
  @impl true
  def handle_async({:persist, _clip_id}, {:ok, _result}, socket) do
    {:noreply, socket}
  end

  # Generic single-clip persistence failure
  @impl true
  def handle_async({:persist, clip_id}, {:exit, reason}, socket) do
    require Logger
    Logger.error("Persist task for clip #{clip_id} crashed: #{inspect(reason)}")
    {:noreply, socket}
  end

  # Merge success: nothing else to update
  @impl true
  def handle_async({:merge_pair, _ids}, {:ok, _result}, socket) do
    {:noreply, socket}
  end

  # Merge failure: log both IDs
  @impl true
  def handle_async({:merge_pair, {prev_id, curr_id}}, {:exit, reason}, socket) do
    require Logger
    Logger.error("Merge task for clips #{prev_id} → #{curr_id} crashed: #{inspect(reason)}")
    {:noreply, socket}
  end
end

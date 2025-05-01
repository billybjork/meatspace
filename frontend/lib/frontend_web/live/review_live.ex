defmodule FrontendWeb.ReviewLive do
  @moduledoc """
  Optimistic-UI review queue.

  * **current**  – clip on screen
  * **future**   – pre-fetched queue (max 5)
  * **history**  – last 5 clips, supports Undo
  """

  use FrontendWeb, :live_view
  import FrontendWeb.ReviewComponents, only: [sprite_player: 1, review_buttons: 1, undo_toast: 1]
  alias Frontend.Clips
  alias Frontend.Clips.Clip

  @prefetch          6   # 1 current + 5 future
  @refill_threshold  3
  @history_limit     5

  ## -----------------------------------------------------------------------
  ## lifecycle
  ## -----------------------------------------------------------------------

  @impl true
  def mount(_params, _session, socket) do
    clips = Clips.next_pending_review_clips(@prefetch)

    case clips do
      [] ->
        {:ok,
         assign(socket,
           page_state: :empty,
           current:    nil,
           future:     [],
           history:    [],
           undo_ctx:   nil)}

      [cur | fut] ->
        {:ok,
         socket
         |> assign(
           current:  cur,
           future:   fut,
           history:  [],
           undo_ctx: nil,
           page_state: :reviewing)}
    end
  end

  ## -----------------------------------------------------------------------
  ## events
  ## -----------------------------------------------------------------------

  # ---------- main click ---------------------------------------------------

  @impl true
  def handle_event("select", %{"action" => "merge"},
                  %{assigns: %{current: curr, history: [prev | _]}} = s) do
    s =
      s
      |> push_history(curr)
      |> advance_queue()
      |> assign(:undo_ctx, %{clip_id: curr.id, action: "merge", extra: %{target_id: prev.id}})
      |> refill_future()

    {:noreply, persist_async(s, {:merge, prev, curr})}
  end

  @impl true
  def handle_event("select", %{"action" => act}, %{assigns: %{current: c}} = s) do
    s =
      s
      |> push_history(c)
      |> advance_queue()
      |> assign(:undo_ctx, %{clip_id: c.id, action: act})
      |> refill_future()

    {:noreply, persist_async(s, c.id, act)}
  end

  # ---------- undo ---------------------------------------------------------

  @impl true
  def handle_event("undo", _params, %{assigns: %{history: []}} = s) do
    {:noreply, assign(s, undo_ctx: nil)}   # F
  end

  def handle_event("undo", _params, %{assigns: %{history: [prev | rest]}} = s) do
    %{current: cur, future: fut} = s.assigns

    s =
      s
      |> assign(
        current:   prev,
        future:    [cur | fut],
        history:   rest,
        undo_ctx:  nil,
        page_state: :reviewing)
      |> refill_future()

    {:noreply, persist_async(s, prev.id, "undo")}
  end

  ## -----------------------------------------------------------------------
  ## helpers
  ## -----------------------------------------------------------------------

  # -- state transforms -----------------------------------------------------

  defp push_history(socket, clip) do
    update(socket, :history, fn h ->
      [clip | Enum.take(h, @history_limit - 1)]
    end)
  end

  defp advance_queue(%{assigns: %{future: []}} = s) do
    s |> assign(current: nil, page_state: :empty)
  end

  defp advance_queue(%{assigns: %{future: [next | rest]}} = s) do
    s |> assign(current: next, future: rest, page_state: :reviewing)
  end

  # -- background persistence ----------------------------------------------

  defp persist_async(socket, {:merge, prev, curr}) do
    Phoenix.LiveView.start_async(socket, {:persist, {prev.id, curr.id}}, fn ->
      Clips.request_merge_and_fetch_next(prev, curr)
    end)
  end

  defp persist_async(socket, clip_id, action) do
    Phoenix.LiveView.start_async(socket, {:persist, clip_id}, fn ->
      Clips.select_clip_and_fetch_next(%Clip{id: clip_id}, action)
    end)
  end

  # -- queue refill ---------------------------------------------------------

  # G – skip if queue is finished
  defp refill_future(%{assigns: %{current: nil}} = s), do: s

  defp refill_future(%{assigns: assigns} = s) do
    if length(assigns.future) < @refill_threshold do
      exclude_ids =
        [assigns.current | assigns.future ++ assigns.history]
        |> Enum.filter(& &1)      # drop nil
        |> Enum.map(& &1.id)

      needed = @prefetch - (length(assigns.future) + 1)
      new = Clips.next_pending_review_clips(needed, exclude_ids)

      update(s, :future, &(&1 ++ new))
    else
      s
    end
  end

  @impl true
  def handle_async({:persist, _clip_id}, {:ok, _result}, socket) do
    # No update needed — optimistic UI already updated the assigns
    {:noreply, socket}
  end

  @impl true
  def handle_async({:persist, clip_id}, {:exit, reason}, socket) do
    require Logger
    Logger.error("Persist task for clip #{clip_id} crashed: #{inspect(reason)}")
    {:noreply, socket}
  end

end

defmodule FrontendWeb.ReviewLive do
  @moduledoc """
  Optimistic-UI review queue for `pending_review` clips.

  ## Queue terms

  * **current**  – clip on screen
  * **future**   – pre-fetched queue  (max 5 after the current one)
  * **history**  – last 5 reviewed clips (undo support)

  ## New “ID-mode” additions

  * **`id_mode?`**      – boolean; toggled by ⌘+Space (sent by JS)
  * **`sibling_page`**  – current page in the sibling-grid pagination
  * **`siblings`**      – list of *other* clips from the same `source_video`
                          (lazy-loaded one page at a time)

  No persistence-layer changes were required – we simply call the existing
  `Clips.request_{merge,group}_and_fetch_next/3` with the *explicit* target
  clip (looked-up by ID).
  """

  use FrontendWeb, :live_view
  import Phoenix.LiveView, only: [put_flash: 3, clear_flash: 1]

  # Components / helpers
  import FrontendWeb.SpritePlayer, only: [sprite_player: 1, sprite_url: 1]

  alias Frontend.Clips
  alias Frontend.Clips.Clip

  @prefetch            6      # 1 current + 5 future
  @refill_threshold    3
  @history_limit       5
  @sibling_page_size   24     # thumbnails per page in the grid

  # -------------------------------------------------------------------------
  # Mount – build initial queue
  # -------------------------------------------------------------------------

  @impl true
  def mount(_params, _session, socket) do
    clips = Clips.next_pending_review_clips(@prefetch)

    socket =
      socket
      |> assign(
        flash_action:  nil,
        id_mode?:      false,
        sibling_page:  1,
        sibling_page_size: @sibling_page_size,
        siblings:      []
      )

    case clips do
      [] ->
        {:ok,
         assign(socket,
           page_state: :empty,
           current:    nil,
           future:     [],
           history:    [])}

      [cur | fut] ->
        {:ok,
         socket
         |> assign(
           current:      cur,
           future:       fut,
           history:      [],
           page_state:   :reviewing)
         |> assign_siblings(cur, 1)}
    end
  end

  # -------------------------------------------------------------------------
  # Event handlers
  # -------------------------------------------------------------------------

  # ─────────────────────────────────────────────────────────────────────────
  # “ID-mode” toggle sent by JS (⌘ + Space)
  # ─────────────────────────────────────────────────────────────────────────
  @impl true
  def handle_event("toggle-id-mode", _params, socket) do
    {:noreply, assign(socket, id_mode?: !socket.assigns.id_mode?)}
  end

  # ─────────────────────────────────────────────────────────────────────────
  # Pagination for sibling grid
  # ─────────────────────────────────────────────────────────────────────────
  @impl true
  def handle_event("change-page", %{"page" => page_str}, %{assigns: %{current: cur}} = socket) do
    {page_int, _} = Integer.parse(page_str)

    socket =
      socket
      |> assign(:sibling_page, page_int)
      |> assign_siblings(cur, page_int)

    {:noreply, socket}
  end

  # ─────────────────────────────────────────────────────────────────────────
  # SPLIT (must precede generic "select" clause)
  # ─────────────────────────────────────────────────────────────────────────
  @impl true
  def handle_event("select", %{"action" => "split", "frame" => frame_val},
                   %{assigns: %{current: clip}} = socket) do
    frame =
      case frame_val do
        v when is_integer(v) -> v
        v when is_binary(v)  -> Integer.parse(v) |> elem(0)
      end

    socket =
      socket
      |> assign(flash_action: "split")
      |> push_history(clip)
      |> advance_queue()
      |> refill_future()
      |> put_flash(:info, "#{flash_verb("split")} clip #{clip.id}")

    {:noreply, persist_split_async(socket, clip, frame)}
  end

  # ─────────────────────────────────────────────────────────────────────────
  # MERGE & GROUP – explicit target_id variant (ID-mode)
  # ─────────────────────────────────────────────────────────────────────────
  for action <- ["merge", "group"] do
    @impl true
    def handle_event("select",
                     %{"action" => unquote(action), "target_id" => tgt_str},
                     %{assigns: %{current: curr}} = socket)
                     when is_binary(tgt_str) and tgt_str != "" do
      with {tgt_id, ""}      <- Integer.parse(tgt_str),
           %Clip{} = tgt      <- Clips.get_clip!(tgt_id),
           true               <- tgt.source_video_id == curr.source_video_id do
        socket =
          socket
          |> assign(flash_action: unquote(action))
          |> push_history(curr)
          |> advance_queue()
          |> refill_future()
          |> put_flash(:info, "#{flash_verb(unquote(action))} #{tgt.id} ↔ #{curr.id}")

        {:noreply,
         persist_async(socket, {String.to_atom(unquote(action)), tgt, curr})}
      else
        _ ->
          {:noreply,
           put_flash(socket, :error, "Invalid target clip ID for #{unquote(action)}")}
      end
    end
  end

  # ─────────────────────────────────────────────────────────────────────────
  # MERGE & GROUP (original “with previous clip” behaviour)
  # ─────────────────────────────────────────────────────────────────────────
  @impl true
  def handle_event("select", %{"action" => "merge"},
                   %{assigns: %{current: curr, history: [prev | _]}} = socket) do
    socket =
      socket
      |> assign(flash_action: "merge")
      |> push_history(curr)
      |> advance_queue()
      |> refill_future()
      |> put_flash(:info, "#{flash_verb("merge")} #{prev.id} ↔ #{curr.id}")

    {:noreply, persist_async(socket, {:merge, prev, curr})}
  end

  @impl true
  def handle_event("select", %{"action" => "group"},
                   %{assigns: %{current: curr, history: [prev | _]}} = socket) do
    socket =
      socket
      |> assign(flash_action: "group")
      |> push_history(curr)
      |> advance_queue()
      |> refill_future()
      |> put_flash(:info, "#{flash_verb("group")} #{prev.id} ↔ #{curr.id}")

    {:noreply, persist_async(socket, {:group, prev, curr})}
  end

  # Ignore merge / group if no previous clip (defence-in-depth)
  @impl true
  def handle_event("select", %{"action" => action},
                   %{assigns: %{history: []}} = socket)
      when action in ["merge", "group"] do
    {:noreply, socket}
  end

  # ─────────────────────────────────────────────────────────────────────────
  # Generic SELECT (approve, skip, archive, …)
  # ─────────────────────────────────────────────────────────────────────────
  @impl true
  def handle_event("select", %{"action" => action},
                   %{assigns: %{current: clip}} = socket) do
    socket =
      socket
      |> assign(flash_action: action)
      |> push_history(clip)
      |> advance_queue()
      |> refill_future()
      |> put_flash(:info, "#{flash_verb(action)} clip #{clip.id}")

    {:noreply, persist_async(socket, clip.id, action)}
  end

  # ─────────────────────────────────────────────────────────────────────────
  # Undo
  # ─────────────────────────────────────────────────────────────────────────
  @impl true
  def handle_event("undo", _params, %{assigns: %{history: []}} = socket),
    do: {:noreply, socket}

  @impl true
  def handle_event("undo", _params,
                   %{assigns: %{history: [prev | rest], current: cur, future: fut}} = socket) do
    socket =
      socket
      |> assign(
           flash_action: nil,
           current:      prev,
           future:       [cur | fut],
           history:      rest,
           page_state:   :reviewing)
      |> refill_future()
      |> assign_siblings(prev, 1)
      |> clear_flash()

    {:noreply, persist_async(socket, prev.id, "undo")}
  end

  # -------------------------------------------------------------------------
  # Async persistence helpers
  # -------------------------------------------------------------------------

  defp persist_split_async(socket, clip, frame) do
    Phoenix.LiveView.start_async(socket, {:split, clip.id}, fn ->
      Clips.request_split_and_fetch_next(clip, frame)
    end)
  end

  defp persist_async(socket, {:merge, prev, curr}) do
    Phoenix.LiveView.start_async(socket, {:merge_pair, {prev.id, curr.id}}, fn ->
      Clips.request_merge_and_fetch_next(prev, curr)
    end)
  end

  defp persist_async(socket, {:group, prev, curr}) do
    Phoenix.LiveView.start_async(socket, {:group_pair, {prev.id, curr.id}}, fn ->
      Clips.request_group_and_fetch_next(prev, curr)
    end)
  end

  defp persist_async(socket, clip_id, action) do
    Phoenix.LiveView.start_async(socket, {:persist, clip_id}, fn ->
      Clips.select_clip_and_fetch_next(%Clip{id: clip_id}, action)
    end)
  end

  # -------------------------------------------------------------------------
  # Queue helpers
  # -------------------------------------------------------------------------

  defp refill_future(%{assigns: %{current: nil}} = socket), do: socket

  defp refill_future(%{assigns: assigns} = socket) do
    if length(assigns.future) < @refill_threshold do
      exclude_ids =
        [assigns.current | assigns.future ++ assigns.history]
        |> Enum.filter(& &1)
        |> Enum.map(& &1.id)

      needed    = @prefetch - (length(assigns.future) + 1)
      new_clips = Clips.next_pending_review_clips(needed, exclude_ids)

      update(socket, :future, &(&1 ++ new_clips))
    else
      socket
    end
  end

  defp push_history(socket, clip) do
    update(socket, :history, fn history ->
      [clip | Enum.take(history, @history_limit - 1)]
    end)
  end

  defp advance_queue(%{assigns: %{future: []}} = socket) do
    assign(socket, current: nil, page_state: :empty, siblings: [])
  end

  defp advance_queue(%{assigns: %{future: [next | rest]}} = socket) do
    socket
    |> assign(current: next, future: rest, page_state: :reviewing)
    |> assign_siblings(next, 1)
  end

  # -------------------------------------------------------------------------
  # Sibling-grid helpers
  # -------------------------------------------------------------------------

  @doc false
  defp assign_siblings(socket, %Clip{} = clip, page) do
    sibs = Clips.for_source_video(clip.source_video_id, clip.id, page, @sibling_page_size)
    assign(socket, siblings: sibs, sibling_page: page)
  end

  # -------------------------------------------------------------------------
  # Async callbacks (unchanged except for flash_verb)
  # -------------------------------------------------------------------------

  @impl true
  def handle_async({:persist, _}, {:ok, _}, socket),   do: {:noreply, socket}
  @impl true
  def handle_async({:persist, clip_id}, {:exit, reason}, socket) do
    require Logger
    Logger.error("Persist for clip #{clip_id} crashed: #{inspect(reason)}")
    {:noreply, socket}
  end

  @impl true
  def handle_async({:merge_pair, _}, {:ok, _}, socket),   do: {:noreply, socket}
  @impl true
  def handle_async({:merge_pair, {prev_id, curr_id}}, {:exit, reason}, socket) do
    require Logger
    Logger.error("Merge #{prev_id}→#{curr_id} crashed: #{inspect(reason)}")
    {:noreply, socket}
  end

  @impl true
  def handle_async({:group_pair, _}, {:ok, _}, socket),   do: {:noreply, socket}
  @impl true
  def handle_async({:group_pair, {prev_id, curr_id}}, {:exit, reason}, socket) do
    require Logger
    Logger.error("Group #{prev_id}→#{curr_id} crashed: #{inspect(reason)}")
    {:noreply, socket}
  end

  @impl true
  def handle_async({:split, _}, {:ok, _}, socket),   do: {:noreply, socket}
  @impl true
  def handle_async({:split, clip_id}, {:exit, reason}, socket) do
    require Logger
    Logger.error("Split for clip #{clip_id} crashed: #{inspect(reason)}")
    {:noreply, socket}
  end

  # -------------------------------------------------------------------------
  # Flash verb helper
  # -------------------------------------------------------------------------
  defp flash_verb("approve"), do: "Approved"
  defp flash_verb("skip"),    do: "Skipped"
  defp flash_verb("archive"), do: "Archived"
  defp flash_verb("merge"),   do: "Merged"
  defp flash_verb("group"),   do: "Grouped"
  defp flash_verb("split"),   do: "Split"
  defp flash_verb(other),     do: String.capitalize(other)
end

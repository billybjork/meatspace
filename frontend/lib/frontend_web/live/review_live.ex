defmodule FrontendWeb.ReviewLive do
  @moduledoc """
  Optimistic-UI review queue.

  * **current**  – clip on screen
  * **future**   – pre-fetched queue (max 5)
  * **history**  – last 5 clips, supports Undo (via back button)
  """

  use FrontendWeb, :live_view

  import FrontendWeb.SpritePlayer, only: [sprite_player: 1, sprite_url: 1]

  alias Frontend.Clips
  alias Frontend.Clips.Clip

  require Logger # For logging

  @prefetch          6   # 1 current + 5 future
  @refill_threshold  3
  @history_limit     5

  # -------------------------------------------------------------------------
  # Mount: Initialize queue on socket assigns
  # -------------------------------------------------------------------------
  @impl true
  def mount(_params, _session, socket) do
    clips = Clips.next_pending_review_clips(@prefetch)

    base_assigns = %{
      split_mode_active?: false,
      selected_split_frame: nil,
      current_sprite_player_target: nil
    }

    socket =
      case clips do
        [] ->
          assign(socket, Map.merge(base_assigns, %{
            page_state: :empty,
            current:    nil,
            future:     [],
            history:    []
          }))

        [cur | fut] ->
          assign(socket, Map.merge(base_assigns, %{
            current:    cur,
            future:     fut,
            history:    [],
            page_state: :reviewing,
            current_sprite_player_target: "#viewer-#{cur.id}"
          }))
      end
    {:ok, socket}
  end

  # -------------------------------------------------------------------------
  # Events: Handle user actions
  # (Run `mix format` to correctly group all handle_event clauses)
  # -------------------------------------------------------------------------

  @impl true
  def handle_event("handle_keydown", %{"key" => key}, socket) do
    socket =
      cond do
        key == "Escape" && socket.assigns.split_mode_active? ->
          handle_split_cancel_logic(socket)
        key in ["ArrowLeft", "ArrowRight"] && socket.assigns.current ->
          handle_arrow_key_split_nav(key, socket)
        true ->
          socket
      end
    {:noreply, socket}
  end

  @impl true
  def handle_event("split_initiate", _payload, socket) do
    if socket.assigns.current && !socket.assigns.split_mode_active? do
      Logger.debug("Entering split mode via 'Split Clip' button.")
      socket =
        assign(socket, split_mode_active?: true, selected_split_frame: nil)
        |> push_event_to_sprite_player("set_player_split_mode", %{active: true, frame: nil})
      {:noreply, socket}
    else
      Logger.warning("Split initiate ignored: No current clip or already in split mode.")
      {:noreply, socket}
    end
  end

  @impl true
  def handle_event("split_confirm", _payload, socket) do
    %{current: clip, selected_split_frame: frame, split_mode_active?: active} = socket.assigns
    if active && clip && !is_nil(frame) && is_integer(frame) do
      Logger.info("Confirming split for clip #{clip.id} at frame #{frame}.")
      socket =
        socket
        |> push_history(clip)
        |> advance_queue()
        |> refill_future()
        |> assign(split_mode_active?: false, selected_split_frame: nil)
      {:noreply, persist_async(socket, {:split, clip, frame})}
    else
      Logger.warning("Split confirm ignored: Requirements not met. Active: #{active}, Clip: #{!is_nil(clip)}, Frame: #{inspect(frame)}")
      {:noreply, socket}
    end
  end

  @impl true
  def handle_event("split_cancel", _payload, socket) do
    {:noreply, handle_split_cancel_logic(socket)}
  end

  @impl true
  def handle_event("split_frame_update", %{"frame" => frame_num_str, "clip_id" => clip_id_str}, socket) do
    current_clip_id = socket.assigns.current && socket.assigns.current.id
    if socket.assigns.split_mode_active? && "#{current_clip_id}" == clip_id_str do
      try do
        frame = String.to_integer(frame_num_str)
        {:noreply, assign(socket, selected_split_frame: frame)}
      rescue
        ArgumentError ->
          Logger.warning("Invalid frame number in split_frame_update: #{frame_num_str}")
          {:noreply, socket}
      end
    else
      {:noreply, socket}
    end
  end

  @impl true
  def handle_event("select", %{"action" => "merge"}, socket) do
    if socket.assigns.split_mode_active? do
      Logger.debug("Action 'merge' ignored: In split mode.")
      {:noreply, socket}
    else
      case socket.assigns do
        %{current: curr, history: [prev | _]} ->
          socket =
            socket
            |> push_history(curr)
            |> advance_queue()
            |> refill_future()
          {:noreply, persist_async(socket, {:merge, prev, curr})}
        _ ->
          Logger.warning("Merge action ignored: conditions not met (e.g., no history).")
          {:noreply, socket}
      end
    end
  end

  @impl true
  def handle_event("select", %{"action" => "group"}, socket) do
    if socket.assigns.split_mode_active? do
      Logger.debug("Action 'group' ignored: In split mode.")
      {:noreply, socket}
    else
      case socket.assigns do
        %{current: curr, history: [prev | _]} ->
          socket =
            socket
            |> push_history(curr)
            |> advance_queue()
            |> refill_future()
          {:noreply, persist_async(socket, {:group, prev, curr})}
        _ ->
          Logger.warning("Group action ignored: conditions not met (e.g., no history).")
          {:noreply, socket}
      end
    end
  end

  @impl true
  def handle_event("select", %{"action" => action}, %{assigns: %{history: []}} = socket)
      when action in ["merge", "group"] do
    if socket.assigns.split_mode_active? do
      Logger.debug("Action '#{action}' with empty history ignored: In split mode.")
      {:noreply, socket}
    else
      Logger.warning("Select action '#{action}' ignored: history is empty for merge/group.")
      {:noreply, socket}
    end
  end

  @impl true
  def handle_event("select", %{"action" => action}, socket)
      when action not in ["merge", "group", "split_initiate", "split_confirm", "split_cancel"] do
    if socket.assigns.split_mode_active? do
      Logger.debug("Action '#{action}' ignored: In split mode.")
      {:noreply, socket}
    else
      if clip = socket.assigns.current do
        socket =
          socket
          |> push_history(clip)
          |> advance_queue()
          |> refill_future()
        {:noreply, persist_async(socket, clip.id, action)}
      else
        Logger.warning("Select action '#{action}' ignored: no current clip.")
        {:noreply, socket}
      end
    end
  end

  @impl true
  def handle_event("undo", _params, %{assigns: %{history: []}} = socket) do
    socket_after_split_cancel = handle_split_cancel_logic(socket)
    Logger.debug("Undo ignored: No history.")
    {:noreply, socket_after_split_cancel}
  end

  @impl true
  def handle_event("undo", _params, socket) do
    socket_after_split_cancel = handle_split_cancel_logic(socket)
    if socket_after_split_cancel.assigns.history == [] do
        Logger.debug("Undo after split cancel resulted in empty history, ignoring.")
        {:noreply, socket_after_split_cancel}
    else
        Logger.debug("Processing undo.")
        %{assigns: %{history: [prev | rest_history], current: cur, future: fut}} = socket_after_split_cancel
        new_socket =
            socket_after_split_cancel
            |> assign(
                current:    prev,
                future:     (if cur, do: [cur | fut], else: fut),
                history:    rest_history,
                page_state: :reviewing,
                current_sprite_player_target: "#viewer-#{prev.id}",
                split_mode_active?: false,
                selected_split_frame: nil
            )
            |> refill_future()
        {:noreply, persist_async(new_socket, prev.id, "undo")}
    end
  end

  # -------------------------------------------------------------------------
  # Helper functions (private)
  # -------------------------------------------------------------------------
  defp handle_arrow_key_split_nav(key, socket) do
    direction = if key == "ArrowLeft", do: :prev, else: :next
    socket =
      if !socket.assigns.split_mode_active? do
        Logger.debug("Entering split mode via arrow key.")
        assign(socket, split_mode_active?: true, selected_split_frame: nil)
        |> push_event_to_sprite_player("set_player_split_mode", %{active: true, frame: nil})
      else
        socket
      end
    push_event_to_sprite_player(socket, "adjust_player_frame", %{direction: direction})
  end

  defp handle_split_cancel_logic(socket) do
    if socket.assigns.split_mode_active? do
      Logger.debug("Cancelling split mode.")
      socket
      |> assign(split_mode_active?: false, selected_split_frame: nil)
      |> push_event_to_sprite_player("set_player_split_mode", %{active: false})
    else
      socket
    end
  end

  defp push_event_to_sprite_player(socket, event_name, payload) do
    if target = socket.assigns.current_sprite_player_target do
      # This is the standard way to call push_event with a target.
      # It's available through `use Phoenix.LiveView`.
      push_event(socket, event_name, Map.put(payload, :target, target))
      socket
    else
      Logger.warning("Cannot push_event_to_sprite_player: No current_sprite_player_target assign.")
      socket
    end
  end

  # -------------------------------------------------------------------------
  # Background persistence
  # -------------------------------------------------------------------------
  defp persist_async(socket, {:split, clip, frame_number}) when is_integer(frame_number) do
    Logger.info("Persisting split request for clip #{clip.id} at frame #{frame_number}")
    Phoenix.LiveView.start_async(socket, {:split_clip, {clip.id, frame_number}}, fn ->
      Clips.request_split_and_fetch_next(clip, frame_number)
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
  # Queue refill
  # -------------------------------------------------------------------------
  defp refill_future(%{assigns: %{current: nil}} = socket), do: socket

  defp refill_future(%{assigns: assigns} = socket) do
    if length(assigns.future) < @refill_threshold && !assigns.split_mode_active? do
      current_id_list = if assigns.current, do: [assigns.current.id], else: []
      future_ids = Enum.map(assigns.future, &(&1.id))
      history_ids = Enum.map(assigns.history, &(&1.id))
      exclude_ids = (current_id_list ++ future_ids ++ history_ids) |> Enum.uniq()
      num_currently_loaded = (if assigns.current, do: 1, else: 0) + length(assigns.future)
      needed = @prefetch - num_currently_loaded
      if needed > 0 do
        new_clips = Clips.next_pending_review_clips(needed, exclude_ids)
        if new_clips != [] do
          Logger.debug("Fetched #{length(new_clips)} new clips for future queue.")
        end
        update(socket, :future, &(&1 ++ new_clips))
      else
        socket
      end
    else
      socket
    end
  end

  # -------------------------------------------------------------------------
  # Helpers: push_history and advance_queue
  # -------------------------------------------------------------------------
  defp push_history(socket, clip) do
    if clip do
      update(socket, :history, fn history ->
        [clip | Enum.take(history, @history_limit - 1)]
      end)
    else
      socket
    end
  end

  defp advance_queue(%{assigns: %{future: []}} = socket) do
    Logger.debug("Advancing queue: Future is empty. Setting to empty state.")
    assign(socket, current: nil, page_state: :empty, current_sprite_player_target: nil)
  end

  defp advance_queue(%{assigns: %{future: [next_clip | rest_future]}} = socket) do
    Logger.debug("Advancing queue: Next clip ID #{next_clip.id}")
    assign(socket,
      current: next_clip,
      future: rest_future,
      page_state: :reviewing,
      current_sprite_player_target: "#viewer-#{next_clip.id}"
    )
  end

  # -------------------------------------------------------------------------
  # Async callbacks
  # -------------------------------------------------------------------------
  @impl true
  def handle_async({:split_clip, {clip_id, frame_num}}, {:ok, {_next_clip, ctx}}, socket) do
    Logger.info("Async split persistence successful for clip #{clip_id} at frame #{frame_num}. Context: #{inspect(ctx)}")
    {:noreply, socket}
  end
  @impl true
  def handle_async({:split_clip, {clip_id, frame_num}}, {:error, reason}, socket) do
    Logger.error("Async split persistence failed for clip #{clip_id} at frame #{frame_num}: #{inspect(reason)}")
    {:noreply, socket}
  end
  @impl true
  def handle_async({:split_clip, {clip_id, frame_num}}, {:exit, reason}, socket) do
    Logger.error("Async task for split clip #{clip_id} at frame #{frame_num} EXITED: #{inspect(reason)}")
    {:noreply, socket}
  end

  @impl true
  def handle_async({:persist, _clip_id}, {:ok, _ctx}, socket) do
    {:noreply, socket}
  end
  @impl true
  def handle_async({:persist, clip_id}, {:exit, reason}, socket) do
    Logger.error("Persist for clip #{clip_id} crashed: #{inspect(reason)}")
    {:noreply, socket}
  end

  @impl true
  def handle_async({:merge_pair, _pair}, {:ok, _ctx}, socket) do
    {:noreply, socket}
  end
  @impl true
  def handle_async({:merge_pair, {prev_id, curr_id}}, {:exit, reason}, socket) do
    Logger.error("Merge #{prev_id}→#{curr_id} crashed: #{inspect(reason)}")
    {:noreply, socket}
  end

  @impl true
  def handle_async({:group_pair, _pair}, {:ok, _ctx}, socket) do
    {:noreply, socket}
  end
  @impl true
  def handle_async({:group_pair, {prev_id, curr_id}}, {:exit, reason}, socket) do
    Logger.error("Group #{prev_id}→#{curr_id} crashed: #{inspect(reason)}")
    {:noreply, socket}
  end

end

defmodule FrontendWeb.ReviewLive do
  use FrontendWeb, :live_view
  import FrontendWeb.ReviewComponents
  alias Frontend.Clips

  @impl true
  def mount(_params, _session, socket) do
    {:ok, assign_first_clip(socket)}
  end

  @impl true
  def handle_event("select", %{"id" => id, "action" => action}, socket) do
    clip = Clips.get_clip!(id)
    {:ok, {next, undo}} = Clips.select_clip_and_fetch_next(clip, action)

    socket =
      if is_nil(next) do
        socket
        |> assign(:clip, nil)
        |> assign(:undo_ctx, undo)
        |> assign(:page_state, :empty)
      else
        socket
        |> assign(:clip, next)
        |> assign(:undo_ctx, undo)
        |> assign(:page_state, :reviewing)
      end

    {:noreply, socket}
  end

  @impl true
  def handle_event("undo", %{"id" => id}, socket) do
    clip = Clips.get_clip!(id)
    {:ok, {same_clip, _}} = Clips.select_clip_and_fetch_next(clip, "undo")

    socket =
      if is_nil(same_clip) do
        assign(socket, clip: nil, undo_ctx: nil, page_state: :empty)
      else
        assign(socket, clip: same_clip, undo_ctx: nil, page_state: :reviewing)
      end

    {:noreply, socket}
  end

  # -- helpers --------------------------------------------------------------

  defp assign_first_clip(socket) do
    case Clips.next_pending_review_clip() do
      nil  -> assign(socket, :page_state, :empty)
      clip -> assign(socket, clip: clip, page_state: :reviewing, undo_ctx: nil)
    end
  end
end

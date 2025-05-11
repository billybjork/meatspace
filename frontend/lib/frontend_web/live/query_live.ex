defmodule FrontendWeb.QueryLive do
  use FrontendWeb, :live_view

  alias Frontend.Clips
  alias Frontend.Clips.Clip

  @per_page 24

  def mount(_params, _session, socket) do
    # 1) load dropdown options
    opts = Clips.embedded_filter_opts()

    # 2) default filters (nil means “Any”)
    filters = %{model_name: nil, generation_strategy: nil, source_video_id: nil}

    # 3) pick an initial main_clip (first match or random)
    main_clip =
      case Clips.random_embedded_clip(filters) do
        nil -> nil
        clip -> clip
      end

    # 4) fetch its neighbors
    similars =
      if main_clip do
        Clips.similar_clips(main_clip.id, filters, true, 1, @per_page)
      else
        []
      end

    socket =
      socket
      |> assign(
           filter_opts: opts,
           filters: filters,
           main_clip: main_clip,
           similars: similars,
           page: 1,
           sort_asc?: true
         )

    {:ok, socket}
  end

  # Whenever any filter changes
  def handle_event("filter_changed", %{"filters" => new_f}, socket) do
    filters = %{
      model_name: maybe_nil(new_f["model_name"]),
      generation_strategy: maybe_nil(new_f["generation_strategy"]),
      source_video_id: to_int_or_nil(new_f["source_video_id"])
    }

    main_clip = Clips.random_embedded_clip(filters)
    similars  = if main_clip, do: Clips.similar_clips(main_clip.id, filters, true, 1, @per_page), else: []

    {:noreply,
     socket
     |> assign(filters: filters, main_clip: main_clip, similars: similars, page: 1, sort_asc?: true)}
  end

  # Pagination
  def handle_event("paginate", %{"page" => page_str}, socket) do
    page = String.to_integer(page_str)
    %{main_clip: mc, filters: f, sort_asc?: sa} = socket.assigns
    similars = Clips.similar_clips(mc.id, f, sa, page, @per_page)
    {:noreply, assign(socket, similars: similars, page: page)}
  end

  # Toggle sort direction
  def handle_event("toggle_sort", _params, socket) do
    %{main_clip: mc, filters: f, sort_asc?: sa, page: page} = socket.assigns
    similars = Clips.similar_clips(mc.id, f, !sa, page, @per_page)
    {:noreply, assign(socket, similars: similars, sort_asc?: !sa)}
  end

  # Randomize main clip
  def handle_event("randomize", _params, socket) do
    %{filters: f, sort_asc?: sa} = socket.assigns
    main_clip = Clips.random_embedded_clip(f)
    similars = if main_clip, do: Clips.similar_clips(main_clip.id, f, sa, 1, @per_page), else: []
    {:noreply, assign(socket, main_clip: main_clip, similars: similars, page: 1)}
  end

  defp clip_url(%Clip{clip_filepath: path}) do
    # If clip_filepath is a full URL you can just return it:
    path

    # Otherwise, build it however your app serves video blobs, for example:
    # Routes.static_url(@socket, "/uploads/#{path}")
  end

  defp maybe_nil(""), do: nil
  defp maybe_nil(x),  do: x

  defp to_int_or_nil(nil),    do: nil
  defp to_int_or_nil(""),     do: nil
  defp to_int_or_nil(s) when is_binary(s), do: String.to_integer(s)
end

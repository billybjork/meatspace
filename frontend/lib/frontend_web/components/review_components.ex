defmodule FrontendWeb.ReviewComponents do
  @moduledoc false
  use Phoenix.Component

  # ------------------------------------------------------------------------
  # public helpers
  # ------------------------------------------------------------------------

  @doc """
  Return the CDN sprite-sheet URL for a clip (or a local placeholder).

  Used by the `<link rel="preload">` tag in *review_live.html.heex* and by
  any place that needs the sprite image outside the main player.
  """
  @spec sprite_url(Frontend.Clips.Clip.t()) :: String.t()
  def sprite_url(%Frontend.Clips.Clip{clip_artifacts: arts}) do
    case Enum.find(arts, &(&1.artifact_type == "sprite_sheet")) do
      nil -> "/images/placeholder_sprite.png"
      art -> cdn_url(art.s3_key)
    end
  end

  # ------------------------------------------------------------------------
  # live-component pieces
  # ------------------------------------------------------------------------

  @doc "Renders the sprite-sheet player for one clip."
  attr :clip, :map, required: true
  def sprite_player(assigns) do
    clip       = assigns.clip
    sprite_art = Enum.find(clip.clip_artifacts, &(&1.artifact_type == "sprite_sheet"))
    meta       = build_player_meta(clip, sprite_art)
    json_meta  = Jason.encode!(meta)

    assigns =
      assigns
      |> assign(:meta, meta)
      |> assign(:json_meta, json_meta)

    ~H"""
    <div class="clip-display-container">
      <div id={"viewer-#{@clip.id}"}
           phx-hook="SpritePlayer"
           data-clip-id={@clip.id}
           data-player={@json_meta}
           class="sprite-viewer bg-gray-200 border border-gray-400"
           style={"width: #{@meta["tile_width"]}px; height: #{@meta["tile_height_calculated"]}px;
                   background-repeat:no-repeat; overflow:hidden; margin:0 auto;"}>
      </div>

      <div class="sprite-controls flex items-center mt-2">
        <button id={"playpause-#{@clip.id}"} data-action="toggle">⏯️</button>
        <input  id={"scrub-#{@clip.id}"} type="range" min="0" step="1">
        <span   id={"frame-display-#{@clip.id}"}>Frame: 0</span>
      </div>
    </div>
    """
  end

  @doc "Review action buttons."
  attr :clip, :map, required: true
  attr :history, :list, default: []
  def review_buttons(assigns) do
    ~H"""
    <div class="review-buttons flex space-x-4 mt-4">
      <button phx-click="select" phx-value-action="approve">✅ Approve</button>
      <button phx-click="select" phx-value-action="skip">➡️ Skip</button>
      <button phx-click="select" phx-value-action="archive">🗑️ Archive</button>
      <button phx-click="select"
              phx-value-action="merge"
              disabled={@history == []}
              class={if @history == [], do: "opacity-40 cursor-not-allowed", else: ""}>
        🔀 Merge&nbsp;(with previous)
      </button>
    </div>
    """
  end

  @doc "Undo toast."
  attr :undo_ctx, :map
  def undo_toast(assigns) do
    ~H"""
    <%= if @undo_ctx do %>
      <div class="undo-toast fixed bottom-4 right-4 bg-yellow-300 p-4 rounded">
        <p>Action recorded. Click to undo.</p>
        <button phx-click="undo">↩️ Undo</button>
      </div>
    <% end %>
    """
  end

  # ------------------------------------------------------------------------
  # private helpers
  # ------------------------------------------------------------------------

  defp build_player_meta(clip, art) do
    base = art.metadata || %{}

    cols    = base["cols"]               || 5
    rows    = base["rows"]               || 5
    tile_w  = base["tile_width"]         || 160
    tile_h  = base["tile_height_calculated"] || round(tile_w * 9 / 16)
    fps     = base["clip_fps"]           || clip.source_video.fps || 24

    frames =
      base["clip_total_frames"] ||
        if clip.start_time_seconds && clip.end_time_seconds && fps > 0 do
          Float.ceil((clip.end_time_seconds - clip.start_time_seconds) * fps)
        else
          cols * rows
        end

    %{
      "cols"                   => cols,
      "rows"                   => rows,
      "tile_width"             => tile_w,
      "tile_height_calculated" => tile_h,
      "total_sprite_frames"    => cols * rows,
      "clip_fps"               => fps,
      "clip_total_frames"      => frames,
      "spriteUrl"              => cdn_url(art.s3_key),
      "isValid"                => true
    }
  end

  defp cdn_url(nil), do: "/images/placeholder_sprite.png"

  defp cdn_url(key) do
    domain = System.get_env("CLOUDFRONT_DOMAIN") ||
               raise "CLOUDFRONT_DOMAIN is not set"
    "https://#{domain}/#{String.trim_leading(key, "/")}"
  end
end

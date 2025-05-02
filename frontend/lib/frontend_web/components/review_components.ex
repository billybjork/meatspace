defmodule FrontendWeb.ReviewComponents do
  @moduledoc false
  use Phoenix.Component
  alias Frontend.Clips.Clip

  # ------------------------------------------------------------------------
  # public helpers (No changes needed here)
  # ------------------------------------------------------------------------

  @doc """
  Return the CDN sprite-sheet URL for a clip (or a local placeholder).

  Used by the `<link rel="preload">` tag in *review_live.html.heex* and by
  any place that needs the sprite image outside the main player.
  """
  @spec sprite_url(Clip.t()) :: String.t()
  def sprite_url(%Clip{clip_artifacts: arts}) do
    case Enum.find(arts, &(&1.artifact_type == "sprite_sheet")) do
      nil -> "/images/placeholder_sprite.png"
      art -> cdn_url(art.s3_key)
    end
  end

  # ------------------------------------------------------------------------
  # live-component pieces
  # ------------------------------------------------------------------------

  @doc "Renders the sprite-sheet player for one clip."
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
    <%# Apply mx-auto to center this container horizontally. %>
    <div class="clip-display-container mx-auto"  # <--- CHANGED THIS LINE
         style={"width: #{@meta["tile_width"]}px;"}>

      <%# This div contains the actual sprite background (No class changes needed) %>
      <div id={"viewer-#{@clip.id}"}
           phx-hook="SpritePlayer"
           data-clip-id={@clip.id}
           data-player={@json_meta}
           class="sprite-viewer bg-gray-200 border border-gray-400"
           style={"width: #{@meta["tile_width"]}px; height: #{@meta["tile_height_calculated"]}px;
                  background-repeat:no-repeat; overflow:hidden;"}>
           <%# Intentionally empty - background is styled by the hook %>
      </div>

      <%# These are the controls below the sprite viewer %>
      <%# Use flex items-center justify-center to center controls *within this bar*. %>
      <div class="sprite-controls flex items-center justify-center mt-2">
        <button id={"playpause-#{@clip.id}"} data-action="toggle">⏯️</button>
        <input id={"scrub-#{@clip.id}"} type="range" min="0" step="1" class="mx-2 flex-grow">
        <span id={"frame-display-#{@clip.id}"} class="text-sm min-w-[70px] text-right">Frame: 0</span>
      </div>
    </div>
    """
  end

  @doc "Review action buttons."
  attr :clip, :map, required: true
  attr :history, :list, default: []
  def review_buttons(assigns) do
    ~H"""
    <%# Use mx-auto to center the button bar horizontally. %>
    <%# Use flex justify-center to center the buttons *within* the bar. %>
    <div class="review-buttons flex justify-center space-x-4 mx-auto">
      <button phx-click="select" phx-value-action="approve">✅ Approve</button>
      <button phx-click="select" phx-value-action="skip">➡️ Skip</button>
      <button phx-click="select" phx-value-action="archive">🗑️ Archive</button>
      <button phx-click="select"
              phx-value-action="merge"
              disabled={@history == []}
              class={if @history == [], do: "opacity-40 cursor-not-allowed", else: ""}>
        🔀 Merge (with previous)
      </button>
    </div>
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
               raise "CLOUDFRONT_DOMAIN environment variable is not set"
    "https://#{domain}/#{String.trim_leading(key, "/")}"
  end
end

defmodule FrontendWeb.ReviewComponents do
  use Phoenix.Component
  alias Jason, as: JSON

  @doc "Renders the sprite-sheet player for one clip."
  attr :clip, :map, required: true

  def sprite_player(assigns) do
    clip         = assigns.clip
    sprite_art   = Enum.find(clip.clip_artifacts, &(&1.artifact_type == "sprite_sheet"))
    meta         = build_player_meta(clip, sprite_art)
    viewer_style = "width: #{meta.tile_width}px; " <>
                   "height: #{meta.tile_height_calculated}px; " <>
                   "background-repeat: no-repeat; overflow: hidden; margin: 0 auto;"

    assigns =
      assigns
      |> assign(:viewer_style, viewer_style)
      |> assign(:meta_json, JSON.encode!(meta))

    ~H"""
    <div class="clip-display-container">
      <div id={"viewer-#{@clip.id}"} class="sprite-viewer bg-gray-200 border border-gray-400"
           style={@viewer_style} phx-hook="SpritePlayer"
           data-meta={@meta_json}>
      </div>

      <div class="sprite-controls flex items-center mt-2">
        <button id={"playpause-#{@clip.id}"}>⏯️</button>
        <input   id={"scrub-#{@clip.id}"}   type="range" min="0" step="1" />
        <span    id={"frame-display-#{@clip.id}"}>Frame: 0</span>
      </div>
    </div>
    """
  end

@doc "Renders review action buttons."
attr :clip, :map, required: true

def review_buttons(assigns) do
  ~H"""
  <div class="review-buttons flex space-x-4 mt-4">
    <button phx-click="select" phx-value-id={@clip.id} phx-value-action="approve">✅ Approve</button>
    <button phx-click="select" phx-value-id={@clip.id} phx-value-action="skip">➡️ Skip</button>
    <button phx-click="select" phx-value-id={@clip.id} phx-value-action="archive">🗑️ Archive</button>
  </div>
  """
end

@doc "Renders an undo toast if there is an undo context."
attr :undo_ctx, :map

def undo_toast(assigns) do
  ~H"""
  <%= if @undo_ctx do %>
    <div class="undo-toast fixed bottom-4 right-4 bg-yellow-300 p-4 rounded">
      <p>Action undone.</p>
      <button phx-click="undo" phx-value-id={@undo_ctx.clip_id}>↩️ Undo</button>
    </div>
  <% end %>
  """
end

  # ---- helpers ------------------------------------------------------------

  defp build_player_meta(_clip, sprite_art) do
    meta = sprite_art.metadata || %{}

    %{
      tile_width:            meta["tile_width"],
      tile_height_calculated: meta["tile_height_calculated"],
      spriteUrl:             cdn_url(sprite_art.s3_key),
      isValid:               true
    }
  end

  defp cdn_url(nil), do: "/images/placeholder_sprite.png"
  defp cdn_url(key) do
    domain = System.get_env("CLOUDFRONT_DOMAIN") || raise "CLOUDFRONT_DOMAIN is not set"
    "https://#{domain}/#{String.trim_leading(key, "/")}"
  end
end

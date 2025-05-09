defmodule FrontendWeb.ReviewButtons do
  @moduledoc false
  use Phoenix.Component

  @doc "Review action buttons."
  attr :clip, :map, required: true
  attr :history, :list, default: []

  def review_buttons(assigns) do
    ~H"""
    <div class="review-buttons">
      <button
        phx-click="undo"
        disabled={@history == []}
        aria-label="Go back to previous clip"
        title={if @history == [], do: "No previous clip", else: "Go back to previous clip"}>
        ⬅️
      </button>

      <button
        phx-click="select"
        phx-value-action="approve">
        ✅ Approve
      </button>

      <button
        phx-click="select"
        phx-value-action="skip">
        ➡️ Skip
      </button>

      <button
        phx-click="select"
        phx-value-action="archive">
        🗑️ Archive
      </button>

      <button
        phx-click="select"
        phx-value-action="merge"
        disabled={@history == []}
        title={if @history == [], do: "No previous clip to merge with",
                                   else: "Merge with previous clip"}>
        🔗 Merge (with previous)
      </button>

      <button
        phx-click="select"
        phx-value-action="group"
        disabled={@history == []}
        title={if @history == [], do: "No previous clip to group with",
                                   else: "Group with previous clip"}>
        🖇️ Group (with previous)
      </button>

      <!-- Split button (no phx-hook needed) -->
      <button
        id={"split-#{@clip.id}"}
        data-clip-id={@clip.id}>
        ✂️ Split
      </button>
    </div>
    """
  end
end

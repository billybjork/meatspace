defmodule FrontendWeb.ReviewButtons do
  @moduledoc false
  use Phoenix.Component

  @doc "Review action buttons."
  attr :clip, :map, required: true
  attr :history, :list, default: []

  def review_buttons(assigns) do
    ~H"""
    <div class="review-buttons flex items-center justify-center space-x-4 mx-auto">
      <button
        phx-click="undo"
        disabled={@history == []}
        class="px-3 py-1 rounded text-2xl font-semibold text-gray-700 hover:text-gray-900 hover:bg-gray-200 disabled:opacity-40 disabled:cursor-not-allowed"
        aria-label="Go back to previous clip"
        title={if @history == [], do: "No previous clip", else: "Go back to previous clip"}>
        ⬅️
      </button>

      <button phx-click="select" phx-value-action="approve" class="px-4 py-2 text-black hover:bg-green-600">
        ✅ Approve
      </button>
      <button phx-click="select" phx-value-action="skip" class="px-4 py-2 text-black hover:bg-gray-600">
        ➡️ Skip
      </button>
      <button phx-click="select" phx-value-action="archive" class="px-4 py-2 text-black hover:bg-red-600">
        🗑️ Archive
      </button>

      <button
        phx-click="select"
        phx-value-action="merge"
        disabled={@history == []}
        class="px-4 py-2 text-black hover:bg-purple-600 disabled:opacity-40 disabled:cursor-not-allowed">
        🔀 Merge (with previous)
      </button>
    </div>
    """
  end
end

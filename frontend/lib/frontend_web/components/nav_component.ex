defmodule FrontendWeb.NavComponent do
  @moduledoc "Site-wide top toolbar (Review / Query links)."

  use Phoenix.Component
  use FrontendWeb, :verified_routes   # <─ supplies the ~p sigil

  # handy later for an “active” class; not used yet
  attr :current_path, :string, required: true

  def nav(assigns) do
    ~H"""
    <header class="site-nav border-b">
      <nav class="container flex items-center gap-6 py-3">
        <.link navigate={~p"/review"} class="nav-link">Review</.link>
        <.link navigate={~p"/query"}  class="nav-link">Query</.link>
        <.link href="http://localhost:4200/" target="_blank" class="nav-link">
          Prefect
        </.link>
      </nav>
    </header>
    """
  end
end

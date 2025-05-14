defmodule FrontendWeb.NavComponent do
  @moduledoc "Site-wide top toolbar (Review / Query links)."

  use Phoenix.Component
  use FrontendWeb, :verified_routes   # <─ supplies the ~p sigil
  alias Frontend.Clips

  # handy later for an “active” class; not used yet
  attr :current_path, :string, required: true

  def nav(assigns) do
    assigns =
      assign_new(assigns, :pending_count, fn -> Clips.pending_review_count() end)

    ~H"""
    <header class="site-nav border-b">
      <nav class="container flex justify-center items-center gap-6 py-3">

        <!-- Review link + badge (hidden when zero) -->
        <.link navigate={~p"/review"} class="nav-link relative">
          Review
          <%= if @pending_count > 0 do %>
            <span class="badge"><%= @pending_count %></span>
          <% end %>
        </.link>

        <.link navigate={~p"/query"} class="nav-link">Query</.link>

        <.link href="http://localhost:4200/" target="_blank" class="nav-link">
          Prefect
        </.link>

        <!-- ───────── Quick-submit form ───────── -->
        <form action={~p"/submit_video"} method="post" class="submit-form flex gap-2">
          <input type="hidden" name="_csrf_token" value={Plug.CSRFProtection.get_csrf_token()} />
          <input type="url"
                 name="url"
                 placeholder="Paste video URL…"
                 required
                 class="submit-input"/>
          <button class="btn-small" type="submit">Submit</button>
        </form>
      </nav>
    </header>
    """
  end
end

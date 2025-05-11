# This file is responsible for configuring the application
# and its dependencies with the aid of the Config module.

import Config

# General application configuration
config :frontend,
  ecto_repos: [Frontend.Repo],
  generators: [timestamp_type: :utc_datetime]

# Tell Ecto/Postgres about our custom types (for the `vector` column)
config :frontend, Frontend.Repo,
  types: Frontend.PostgresTypes

# Configures the endpoint
config :frontend, FrontendWeb.Endpoint,
  url: [host: "localhost"],
  adapter: Phoenix.Endpoint.Cowboy2Adapter,
  render_errors: [
    formats: [html: FrontendWeb.ErrorHTML, json: FrontendWeb.ErrorJSON],
    layout: false
  ],
  pubsub_server: Frontend.PubSub,
  live_view: [signing_salt: "KWvQc1I2"] # Replace with a secure salt from `mix phx.gen.secret 32`

# Configures Elixir's Logger
config :logger, :console,
  format: "$time $metadata[$level] $message\n",
  metadata: [:request_id]

# Use Jason for JSON parsing in Phoenix
config :phoenix, :json_library, Jason

# Configure esbuild (the bundler used for JS/CSS)
config :esbuild,
  version: "0.18.17", # Use the latest appropriate version
  default: [
    args:
      ~w(
        js/app.js
        --bundle
        --target=es2017
        --outdir=../priv/static/assets
        --external:/fonts/*
        --external:/images/*
      ),
    cd: Path.expand("../assets", __DIR__),
    env: %{"NODE_PATH" => Path.expand("../deps", __DIR__)}
  ]

# Import environment-specific config. This must remain at the bottom
# so it overrides the configuration defined above.
import_config "#{config_env()}.exs"

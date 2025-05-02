import Config

# … your existing DB, endpoint, live_reload, logger, etc. configs above …

# Watchers: bundle JS/CSS in dev
# If DISABLE_ASSET_WATCHERS is set, we turn them off (useful in Docker).
watchers =
  if System.get_env("DISABLE_ASSET_WATCHERS") do
    []
  else
    [
      # JS compilation via esbuild (native binary, not a JS script)
      node: [
        "node_modules/esbuild/bin/esbuild",
        "--watch",
        "--bundle", "js/app.js",
        "--target=es2017",
        "--outdir=../priv/static/assets",
        "--external:/fonts/*",
        "--external:/images/*",
        cd: Path.expand("../assets", __DIR__),
        env: %{"NODE_PATH" => Path.expand("../deps", __DIR__)}
      ],
      # CSS compilation via Tailwind CLI (JS script)
      node: [
        "node_modules/tailwindcss/lib/cli.js",
        "--input=css/app.css",
        "--output=../priv/static/assets/app.css",
        "--config=tailwind.config.js",
        "--watch",
        cd: Path.expand("../assets", __DIR__)
      ]
    ]
  end

config :frontend, FrontendWeb.Endpoint,
  http: [ip: {0, 0, 0, 0}, port: 4000],
  check_origin: false,
  code_reloader: true,
  debug_errors: true,
  secret_key_base: "Hcf01GGw0FYxDuBv8DYlZVdkYmxTfFai9aTdCBHzKowVAZE6/nQrGcYm5btt06wI",
  watchers: watchers

# Watch static and templates for browser reloading.
config :frontend, FrontendWeb.Endpoint,
  live_reload: [
    patterns: [
      ~r"priv/static/assets/.*(js|css|png|jpeg|jpg|gif|svg)$",
      ~r"priv/gettext/.*(po)$",
      ~r"lib/frontend_web/(controllers|live|components|layouts)/.*(ex|heex)$"
    ]
  ]

# … rest of your dev.exs unchanged …

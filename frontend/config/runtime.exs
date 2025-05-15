import Config

# Enable the server if PHX_SERVER=true
if System.get_env("PHX_SERVER") do
  config :frontend, FrontendWeb.Endpoint, server: true
end

# ──────────────────────────────────────────────────────────────────────────────
# Universal runtime config
# ──────────────────────────────────────────────────────────────────────────────

# Pull in the DATABASE_URL, error if missing
database_url =
  System.get_env("DATABASE_URL") ||
    raise """
    environment variable DATABASE_URL is missing.
    Expected something like: postgres://USER:PASS@HOST/DATABASE
    """

# Convert the postgres:// scheme into ecto:// so Ecto’s parser is happy
database_url = String.replace_prefix(database_url, "postgres://", "ecto://")

config :frontend, :cloudfront_domain,
  System.fetch_env!("CLOUDFRONT_DOMAIN")

maybe_ipv6 = if System.get_env("ECTO_IPV6") in ~w(true 1), do: [:inet6], else: []

config :frontend, Frontend.Repo,
  url: database_url,
  pool_size: String.to_integer(System.get_env("POOL_SIZE") || "10"),
  socket_options: maybe_ipv6

# ──────────────────────────────────────────────────────────────────────────────
# Production-only runtime config
# ──────────────────────────────────────────────────────────────────────────────
if config_env() == :prod do
  # Secret key for session encryption
  secret_key_base =
    System.get_env("SECRET_KEY_BASE") ||
      raise """
      environment variable SECRET_KEY_BASE is missing.
      You can generate one with: mix phx.gen.secret
      """

  # Render will inject PORT (dynamic) and you should set PHX_HOST to e.g.
  #   your-service-name.onrender.com
  host = System.get_env("PHX_HOST") || "phoenix-web-yupe.onrender.com"
  port = String.to_integer(System.get_env("PORT") || "4000")

  # Optional: DNS cluster query if you’re using multiple nodes
  config :frontend, :dns_cluster_query, System.get_env("DNS_CLUSTER_QUERY")

  config :frontend, FrontendWeb.Endpoint,
    url: [host: host, port: 443, scheme: "https"],
    http: [
      ip: {0, 0, 0, 0, 0, 0, 0, 0},
      port: port
    ],
    check_origin: [
      "//" <> host,        # primary Render URL
      "//localhost"       # optional – dev convenience
    ],
    secret_key_base: secret_key_base,
    server: true

  # If you want to enforce SSL redirects (recommended in prod)
  # uncomment the following:
  # config :frontend, FrontendWeb.Endpoint,
  #   force_ssl: [hsts: true]
end

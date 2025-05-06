# -----------------------
# 1) Node stage: build JS assets
# -----------------------
    FROM node:18-slim AS node_build

    WORKDIR /app/assets
    
    # Copy only what we need for npm
    COPY assets/package.json assets/package-lock.json ./
    COPY assets/js     ./js
    COPY assets/css    ./css
    COPY assets/postcss.config.js ./postcss.config.js
    
    # Ensure output dir exists (esbuild deploy writes to ../priv/static/assets)
    RUN mkdir -p ../priv/static/assets
    
    # Install deps & bundle
    RUN npm ci
    RUN npm run deploy
    
    # -----------------------
    # 2) Elixir build stage
    # -----------------------
    FROM hexpm/elixir:1.16.2-erlang-26.2.4-alpine-3.19.1 AS build
    
    ENV MIX_ENV=prod \
        LANG=C.UTF-8
    
    WORKDIR /app
    
    # Install build-time tools
    RUN apk add --no-cache build-base git
    
    # 2a) Get Elixir deps
    COPY mix.exs mix.lock ./
    RUN mix local.hex --force \
     && mix local.rebar --force \
     && mix deps.get --only prod \
     && mix deps.compile
    
    # 2b) Copy over the JS assets we already built
    COPY --from=node_build /app/priv/static/assets ./priv/static/assets
    
    # 2c) Copy the rest of your app and compile
    COPY lib       ./lib
    COPY config    ./config
    COPY priv/repo ./priv/repo
    # (priv/static is already populated)
    RUN mix compile
    
    # -----------------------
    # 3) Runtime image
    # -----------------------
    FROM build AS app
    
    # Install Node, npm, and inotify-tools so watchers work
    RUN apk add --no-cache nodejs npm inotify-tools
    
    WORKDIR /app
    
    # Copy entrypoint script
    COPY entrypoint.sh /app/entrypoint.sh
    RUN chmod +x /app/entrypoint.sh
    
    # Expose Phoenix port
    EXPOSE 4000
    
    # Use entrypoint to rebuild assets then start server
    ENTRYPOINT ["/app/entrypoint.sh"]
    
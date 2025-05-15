#!/usr/bin/env bash
set -euo pipefail

# 1) Render injects this from the DB connection string we ask for below
RAW="$PREFECT_CONNECTION_STRING"

# 2) Turn “postgres://” → “postgresql+asyncpg://” and force SSL
ASYNC="${RAW/postgres/postgresql+asyncpg}?sslmode=require"

export PREFECT_API_DATABASE_CONNECTION_URL="$ASYNC"

echo "→ Using DB URL: $PREFECT_API_DATABASE_CONNECTION_URL"
exec prefect server start --host 0.0.0.0 --port 4200
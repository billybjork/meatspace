#!/usr/bin/env bash
set -euo pipefail

RAW="$PREFECT_CONNECTION_STRING"

# Convert the leading scheme only
ASYNC=$(echo "$RAW" \
        | sed -E 's|^postgres(ql)?://|postgresql+asyncpg://|')

# Require SSL
export PREFECT_API_DATABASE_CONNECTION_URL="${ASYNC}?sslmode=require"

echo "→ Using DB URL: $PREFECT_API_DATABASE_CONNECTION_URL"
exec prefect server start --host 0.0.0.0 --port 4200
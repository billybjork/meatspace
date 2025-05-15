#!/usr/bin/env bash
set -euo pipefail

RAW="$PREFECT_CONNECTION_STRING"

# 1) Make the scheme async-capable, without touching the trailing “…ql”
ASYNC=$(echo "$RAW" | sed -E 's|^postgres(ql)?://|postgresql+asyncpg://|')

# 2) Add SSL flag the way asyncpg wants it  (ssl=require)
sep="?"
[[ "$ASYNC" == *\?* ]] && sep="&"
export PREFECT_API_DATABASE_CONNECTION_URL="${ASYNC}${sep}ssl=require"

echo "→ Using DB URL: $PREFECT_API_DATABASE_CONNECTION_URL"
exec prefect server start --host 0.0.0.0 --port 4200
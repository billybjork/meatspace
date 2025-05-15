#!/usr/bin/env bash
set -euo pipefail

RAW="$PREFECT_CONNECTION_STRING"
ASYNC=$(echo "$RAW" | sed -E 's|^postgres(ql)?://|postgresql+asyncpg://|')
sep="?"
[[ "$ASYNC" == *\?* ]] && sep="&"
export PREFECT_API_DATABASE_CONNECTION_URL="${ASYNC}${sep}ssl=require"

echo "→ Using DB URL: $PREFECT_API_DATABASE_CONNECTION_URL"

# NEW: build-and-apply every deployment defined in prefect.yaml
prefect deploy --all --skip-upload  # idempotent; safe on every boot

exec prefect server start --host 0.0.0.0 --port 4200
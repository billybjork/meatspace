#!/usr/bin/env bash
set -euo pipefail

# 1) Build the async DB URL exactly as before
RAW="$PREFECT_CONNECTION_STRING"
ASYNC=$(echo "$RAW" | sed -E 's|^postgres(ql)?://|postgresql+asyncpg://|')
sep="?"
[[ "$ASYNC" == *\?* ]] && sep="&"
export PREFECT_API_DATABASE_CONNECTION_URL="${ASYNC}${sep}ssl=require"

echo "→ Using DB URL: $PREFECT_API_DATABASE_CONNECTION_URL"

# 2) Register all your deployments from backend/prefect.yaml 
#    (this is idempotent so safe on every startup)
prefect deployment apply /app/prefect.yaml --project "Default" --skip-upload

# 3) Finally, start the server
exec prefect server start --host 0.0.0.0 --port 4200
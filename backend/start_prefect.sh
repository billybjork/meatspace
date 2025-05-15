#!/usr/bin/env bash
set -euo pipefail

##############################################################################
# 1. Build async-pg connection string + force TLS
##############################################################################
RAW="$PREFECT_CONNECTION_STRING"
ASYNC=$(echo "$RAW" | sed -E 's|^postgres(ql)?://|postgresql+asyncpg://|')
sep="?" ; [[ "$ASYNC" == *\?* ]] && sep="&"
export PREFECT_API_DATABASE_CONNECTION_URL="${ASYNC}${sep}ssl=require"

echo "→ Using DB URL: $PREFECT_API_DATABASE_CONNECTION_URL"

##############################################################################
# 2. (Re)register every deployment declared in prefect.yaml
#    ────────────  NO --apply, --yes, or --skip-upload  ────────────
##############################################################################
prefect deploy --all                       \
               --pool default-agent-pool   \
               --override work_pool.job_variables.image="$RENDER_IMAGE"

##############################################################################
# 3. Start Prefect Server
##############################################################################
exec prefect server start --host 0.0.0.0 --port 4200
#!/usr/bin/env bash
set -euo pipefail        # keep – but allow an *empty* RENDER_IMAGE
shopt -s expand_aliases

##############################################################################
# 1. Build DB URL (same as before)
##############################################################################
RAW="$PREFECT_CONNECTION_STRING"
ASYNC="${RAW/postgres/postgresql+asyncpg}"
sep="?" ; [[ $ASYNC == *\?* ]] && sep="&"
export PREFECT_API_DATABASE_CONNECTION_URL="${ASYNC}${sep}ssl=require"
echo "→ Using DB URL: $PREFECT_API_DATABASE_CONNECTION_URL"

##############################################################################
# 2. Register / update deployments
##############################################################################
EXTRA_ARGS=()
if [[ -n "${RENDER_IMAGE:-}" ]]; then          # <- only if it’s set
  EXTRA_ARGS+=(--override "work_pool.job_variables.image=$RENDER_IMAGE")
fi

prefect deploy --all \
               --pool default-agent-pool \
               "${EXTRA_ARGS[@]}"

##############################################################################
# 3. Launch the Prefect API/UI
##############################################################################
exec prefect server start --host 0.0.0.0 --port 4200
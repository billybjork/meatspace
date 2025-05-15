#!/usr/bin/env bash
set -euo pipefail
shopt -s expand_aliases

##############################################################################
# 1. Build DB URL (idempotent)
##############################################################################
RAW="${PREFECT_CONNECTION_STRING:?PREFECT_CONNECTION_STRING not set}"

if [[ $RAW == postgres://* ]]; then                  # only fix *once*
  ASYNC="${RAW/postgres/postgresql+asyncpg}"
else
  ASYNC="$RAW"
fi

sep="?" ; [[ $ASYNC == *\?* ]] && sep="&"
export PREFECT_API_DATABASE_CONNECTION_URL="${ASYNC}${sep}ssl=require"
echo "→ Using DB URL: $PREFECT_API_DATABASE_CONNECTION_URL"

##############################################################################
# 2. Optional image override (only when Render passes it)
##############################################################################
EXTRA_ARGS=()
if [[ -n "${RENDER_IMAGE:-}" ]]; then
  EXTRA_ARGS+=(--override "work_pool.job_variables.image=$RENDER_IMAGE")
fi

##############################################################################
# 3. Register / update all deployments
##############################################################################
prefect deploy --all --pool default-agent-pool "${EXTRA_ARGS[@]}"

##############################################################################
# 4. Start the Prefect API / UI
##############################################################################
exec prefect server start --host 0.0.0.0 --port 4200
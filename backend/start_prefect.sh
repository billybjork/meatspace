#!/usr/bin/env bash
set -euo pipefail
shopt -s expand_aliases

##############################################################################
# 1. Build DB URL (idempotent)
##############################################################################
RAW="${PREFECT_CONNECTION_STRING:?PREFECT_CONNECTION_STRING not set}"

# convert *both* postgres://… and postgresql://… only once
if [[ $RAW == postgres://* ]]; then
  ASYNC="${RAW/postgres/postgresql+asyncpg}"
elif [[ $RAW == postgresql://* && $RAW != postgresql+asyncpg://* ]]; then
  ASYNC="${RAW/postgresql/postgresql+asyncpg}"
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
# 2b. Ensure the work-pool exists (idempotent)
##############################################################################
POOL="default-agent-pool"
if ! prefect work-pool inspect "$POOL" &>/dev/null; then
  echo "→ Creating work pool '$POOL' (type=process)…"
  # ↓↓↓ REMOVE the old --pause flag ↓↓↓
  prefect work-pool create --type process "$POOL"
fi

##############################################################################
# 3. Register / update all deployments
##############################################################################
prefect deploy --all "${EXTRA_ARGS[@]}"

##############################################################################
# 4. Start the Prefect API / UI
##############################################################################
exec prefect server start --host 0.0.0.0 --port 4200
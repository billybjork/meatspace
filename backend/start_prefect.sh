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
# 2. Register (or update) every deployment declared in prefect.yaml
#    --apply   = “yes, go ahead and create/update it”   (replaces --yes)
##############################################################################
prefect deploy --all --apply        # no --skip-upload in 3.4+

##############################################################################
# 3. Launch the Prefect API / UI
##############################################################################
exec prefect server start --host 0.0.0.0 --port 4200
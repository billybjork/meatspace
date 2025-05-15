#!/usr/bin/env sh
set -e

# wait for the API to come up (use the shared PREFECT_API_URL)
until curl -fsS "${PREFECT_API_URL}/health"; do
  sleep 5
done

# Hand off to the Prefect worker
exec prefect worker start --pool default-agent-pool
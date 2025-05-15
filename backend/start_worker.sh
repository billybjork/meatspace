#!/usr/bin/env sh
set -e

# Wait for Prefect API to come up
until curl -fsS http://prefect-server:4200/api/health; do
  sleep 5
done

# Hand off to the Prefect worker
exec prefect worker start --pool default-agent-pool
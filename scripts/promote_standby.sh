#!/usr/bin/env bash
set -euo pipefail

# Promote standby to master (manual failover)
# Usage: sudo bash promote_standby.sh

echo "Promoting standby..."
sudo -u postgres psql -c "SELECT pg_promote(wait => true);" >/dev/null

# Wait until recovery is off
for i in {1..30}; do
  IN_RECOVERY=$(sudo -u postgres psql -Atqc "select pg_is_in_recovery();")
  if [[ "${IN_RECOVERY}" == "f" ]]; then
    echo "Standby promoted to master."
    exit 0
  fi
  sleep 1
 done

echo "Timed out waiting for promotion. Check PostgreSQL logs." >&2
exit 1 
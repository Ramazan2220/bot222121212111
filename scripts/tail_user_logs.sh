#!/usr/bin/env bash
set -euo pipefail

USER_ID=${1:-}
if [[ -z "$USER_ID" ]]; then
  echo "Usage: ./scripts/tail_user_logs.sh <USER_ID>"
  exit 1
fi

PROJECT_ROOT=$(cd "$(dirname "$0")"/.. && pwd)
LOG_FILE="$PROJECT_ROOT/data/users/$USER_ID/logs/user.log"

if [[ ! -f "$LOG_FILE" ]]; then
  echo "No log file yet: $LOG_FILE"
  exit 1
fi

tail -n 200 -f "$LOG_FILE" 
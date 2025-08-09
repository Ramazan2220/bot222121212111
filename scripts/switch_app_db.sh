#!/usr/bin/env bash
set -euo pipefail

# Switch application DATABASE_URL in project .env for manual failover
# Usage: ./switch_app_db.sh <NEW_DATABASE_URL>

NEW_URL=${1:-}
if [[ -z "${NEW_URL}" ]]; then
  echo "Usage: ./switch_app_db.sh <NEW_DATABASE_URL>"
  exit 1
fi

PROJECT_ROOT=$(cd "$(dirname "$0")"/.. && pwd)
ENV_FILE="${PROJECT_ROOT}/.env"

if [[ ! -f "${ENV_FILE}" ]]; then
  touch "${ENV_FILE}"
fi

if grep -q '^DATABASE_URL=' "${ENV_FILE}"; then
  sed -i.bak "s#^DATABASE_URL=.*#DATABASE_URL=${NEW_URL//#/\\#}#" "${ENV_FILE}"
else
  echo "DATABASE_URL=${NEW_URL}" >> "${ENV_FILE}"
fi

echo "DATABASE_URL updated in ${ENV_FILE}. Restart the app process to apply changes." 
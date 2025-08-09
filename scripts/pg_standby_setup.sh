#!/usr/bin/env bash
set -euo pipefail

# PostgreSQL Standby setup for streaming replication
# Usage (run on STANDBY):
#   sudo bash pg_standby_setup.sh <MASTER_IP> <REPL_USER> <REPL_PASSWORD> [SLOT_NAME] [PG_VERSION]
# Example:
#   sudo bash pg_standby_setup.sh 10.0.0.11 replicator 'strong-pass' standby_slot1 14

MASTER_IP=${1:-}
REPL_USER=${2:-}
REPL_PASSWORD=${3:-}
SLOT_NAME=${4:-standby_slot1}
PG_VERSION=${5:-}

if [[ -z "${MASTER_IP}" || -z "${REPL_USER}" || -z "${REPL_PASSWORD}" ]]; then
  echo "Usage: sudo bash pg_standby_setup.sh <MASTER_IP> <REPL_USER> <REPL_PASSWORD> [SLOT_NAME] [PG_VERSION]"
  exit 1
fi

if [[ -z "${PG_VERSION}" ]]; then
  if command -v psql >/dev/null 2>&1; then
    PG_VERSION=$(psql -V | awk '{print $3}' | cut -d. -f1)
  else
    PG_VERSION=14
  fi
fi

PG_DATA_DIR="/var/lib/postgresql/${PG_VERSION}/main"
PG_CONF_DIR="/etc/postgresql/${PG_VERSION}/main"

if ! id postgres >/dev/null 2>&1; then
  echo "Installing PostgreSQL ${PG_VERSION}..."
  apt-get update -y
  apt-get install -y postgresql-${PG_VERSION} postgresql-contrib-${PG_VERSION}
fi

systemctl enable postgresql || true
systemctl stop postgresql || true

# Cleanup data dir for basebackup
rm -rf ${PG_DATA_DIR}/*

# Base backup with recovery config (-R writes primary_conninfo)
PGPASSWORD="${REPL_PASSWORD}" sudo -u postgres pg_basebackup \
  -h ${MASTER_IP} -D ${PG_DATA_DIR} -U ${REPL_USER} -X stream -P -R -C -S ${SLOT_NAME}

# Ensure hot_standby is on
sed -i "s/^#*\?hot_standby.*/hot_standby = on/" ${PG_CONF_DIR}/postgresql.conf || echo "hot_standby = on" >> ${PG_CONF_DIR}/postgresql.conf

systemctl start postgresql

echo "Standby setup completed. Verify with: sudo -u postgres psql -c 'select pg_is_in_recovery();'" 
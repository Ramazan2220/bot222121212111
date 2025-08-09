#!/usr/bin/env bash
set -euo pipefail

# PostgreSQL Master setup for streaming replication
# Usage (run on MASTER):
#   sudo bash pg_master_setup.sh <REPL_USER> <REPL_PASSWORD> <STANDBY_IP> [PG_VERSION]
# Example:
#   sudo bash pg_master_setup.sh replicator 'strong-pass' 10.0.0.12 14

REPL_USER=${1:-}
REPL_PASSWORD=${2:-}
STANDBY_IP=${3:-}
PG_VERSION=${4:-}

if [[ -z "${REPL_USER}" || -z "${REPL_PASSWORD}" || -z "${STANDBY_IP}" ]]; then
  echo "Usage: sudo bash pg_master_setup.sh <REPL_USER> <REPL_PASSWORD> <STANDBY_IP> [PG_VERSION]"
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

systemctl enable postgresql
systemctl start postgresql

# Configure postgresql.conf
sed -i "s/^#*\?listen_addresses.*/listen_addresses = '*'/'" ${PG_CONF_DIR}/postgresql.conf || true
sed -i "s/^#*\?wal_level.*/wal_level = replica/" ${PG_CONF_DIR}/postgresql.conf || echo "wal_level = replica" >> ${PG_CONF_DIR}/postgresql.conf
sed -i "s/^#*\?max_wal_senders.*/max_wal_senders = 10/" ${PG_CONF_DIR}/postgresql.conf || echo "max_wal_senders = 10" >> ${PG_CONF_DIR}/postgresql.conf
sed -i "s/^#*\?wal_keep_size.*/wal_keep_size = 1024MB/" ${PG_CONF_DIR}/postgresql.conf || echo "wal_keep_size = 1024MB" >> ${PG_CONF_DIR}/postgresql.conf
sed -i "s/^#*\?hot_standby.*/hot_standby = on/" ${PG_CONF_DIR}/postgresql.conf || echo "hot_standby = on" >> ${PG_CONF_DIR}/postgresql.conf

# Allow replication from standby
HBA_LINE="host    replication     ${REPL_USER}      ${STANDBY_IP}/32         md5"
if ! grep -q "${HBA_LINE}" ${PG_CONF_DIR}/pg_hba.conf; then
  echo "${HBA_LINE}" >> ${PG_CONF_DIR}/pg_hba.conf
fi

systemctl restart postgresql

# Create replication user
sudo -u postgres psql -tc "SELECT 1 FROM pg_roles WHERE rolname='${REPL_USER}'" | grep -q 1 || \
  sudo -u postgres psql -c "CREATE ROLE ${REPL_USER} WITH REPLICATION LOGIN PASSWORD '${REPL_PASSWORD}';"

# Create a physical replication slot (optional but recommended)
SLOT_NAME="standby_slot1"
sudo -u postgres psql -tc "SELECT 1 FROM pg_replication_slots WHERE slot_name='${SLOT_NAME}'" | grep -q 1 || \
  sudo -u postgres psql -c "SELECT * FROM pg_create_physical_replication_slot('${SLOT_NAME}');"

echo "Master setup completed. Ensure firewall allows TCP 5432 from ${STANDBY_IP}." 
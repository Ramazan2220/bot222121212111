#!/usr/bin/env python3
import os
import sys
import time
import psycopg2

MASTER_DB_URL = os.getenv('MASTER_DB_URL')
STANDBY_DB_URL = os.getenv('STANDBY_DB_URL')

if not MASTER_DB_URL or not STANDBY_DB_URL:
    print("Set MASTER_DB_URL and STANDBY_DB_URL env vars.")
    sys.exit(1)

def query(conn_str, sql):
    with psycopg2.connect(conn_str) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            return cur.fetchall()

def main():
    # On master
    master_role = query(MASTER_DB_URL, "select pg_is_in_recovery();")[0][0]
    if master_role:
        print("WARNING: MASTER_DB_URL points to a standby (in recovery)")
    repl = query(MASTER_DB_URL, "SELECT client_addr, state, sync_state, sent_lsn, write_lsn, flush_lsn, replay_lsn FROM pg_stat_replication;")

    # On standby
    standby_role = query(STANDBY_DB_URL, "select pg_is_in_recovery();")[0][0]
    if not standby_role:
        print("WARNING: STANDBY_DB_URL points to a master (not in recovery)")
    lsn_rows = query(STANDBY_DB_URL, "SELECT pg_last_wal_receive_lsn(), pg_last_wal_replay_lsn();")
    receive_lsn, replay_lsn = lsn_rows[0]

    # Lag in bytes (requires access to master too)
    lag_bytes = None
    try:
        if receive_lsn:
            lag_bytes = query(MASTER_DB_URL, f"SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), '{receive_lsn}');")[0][0]
    except Exception:
        pass

    print("Master -> pg_stat_replication:")
    for row in repl:
        print(f"  client={row[0]} state={row[1]} sync={row[2]} sent={row[3]} write={row[4]} flush={row[5]} replay={row[6]}")

    print(f"Standby -> in_recovery={standby_role} receive_lsn={receive_lsn} replay_lsn={replay_lsn} lag_bytes={lag_bytes}")

if __name__ == '__main__':
    main() 
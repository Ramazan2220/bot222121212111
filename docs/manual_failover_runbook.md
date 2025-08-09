### Manual failover (Master -> Standby) quick runbook

Prereqs:
- Master and Standby configured with streaming replication.
- App reads DATABASE_URL from .env (config.py supports env override).

1) Check replication health
- Set env:
  - `export MASTER_DB_URL='postgresql://postgres:PASS@MASTER_IP:5432/postgres'`
  - `export STANDBY_DB_URL='postgresql://postgres:PASS@STANDBY_IP:5432/postgres'`
- Run:
  - `python scripts/check_replication.py`

2) Planned switch (no failure)
- Stop app processes connecting to DB.
- Promote standby on the standby host:
  - `sudo bash scripts/promote_standby.sh`
- Update app `.env` on app host:
  - `./scripts/switch_app_db.sh "$STANDBY_DB_URL"`
- Start app. Old master can be re-added later as a new standby.

3) Emergency switch (master down)
- Ensure standby is up.
- Promote standby:
  - `sudo bash scripts/promote_standby.sh`
- Update `.env` and restart app as above.

4) Rebuild the failed master as new standby (after incident)
- Fix/replace server. Then run on that node:
  - On fixed node: `scripts/pg_standby_setup.sh <NEW_MASTER_IP> <REPL_USER> <REPL_PASS> standby_slot1`

Notes
- Firewall: allow 5432 only between DB nodes and the app host.
- Backups still required; replication ≠ backup.
- Test failover quarterly to keep procedure fresh. 
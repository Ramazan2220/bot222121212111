#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import time
import threading
from pathlib import Path

from utils.user_logger import log_user_info, log_user_error, get_user_logger
from config import DATA_DIR

USERS = [1111111111, 2222222222]
LOG_ROOT = Path(DATA_DIR) / 'users'


def writer(user_id: int, prefix: str, n: int = 20, delay: float = 0.01):
    # ensure logger created
    get_user_logger(user_id)
    for i in range(n):
        log_user_info(user_id, f"{prefix} step={i}")
        if i % 7 == 0:
            log_user_error(user_id, f"{prefix} minor simulated issue at step {i}")
        time.sleep(delay)


def read_tail(path: Path, lines: int = 10) -> str:
    if not path.exists():
        return "<no file>"
    with path.open('r', encoding='utf-8', errors='ignore') as f:
        content = f.read().splitlines()
        return "\n".join(content[-lines:])


def main():
    print("=== Smoke test: per-user logs separation ===")
    # Clean previous small files if needed (do not delete history)
    for uid in USERS:
        user_log = LOG_ROOT / str(uid) / 'logs' / 'user.log'
        user_log.parent.mkdir(parents=True, exist_ok=True)
        # append a separator
        log_user_info(uid, "--- NEW TEST SESSION ---")

    # Start concurrent writers
    threads = []
    threads.append(threading.Thread(target=writer, args=(USERS[0], "PUBLISH", 50, 0.005)))
    threads.append(threading.Thread(target=writer, args=(USERS[1], "WARMUP", 50, 0.007)))
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # Read tails
    for uid in USERS:
        user_log = LOG_ROOT / str(uid) / 'logs' / 'user.log'
        print(f"\n--- Tail for user {uid} ({user_log}) ---")
        print(read_tail(user_log, 12))

    # Quick verification: ensure user marker present and files differ
    u1_log = (LOG_ROOT / str(USERS[0]) / 'logs' / 'user.log').read_text(encoding='utf-8', errors='ignore')
    u2_log = (LOG_ROOT / str(USERS[1]) / 'logs' / 'user.log').read_text(encoding='utf-8', errors='ignore')
    ok_marker = (f"user={USERS[0]}" in u1_log) and (f"user={USERS[1]}" in u2_log)
    separated = "PUBLISH" in u1_log and "WARMUP" in u2_log

    print("\nRESULT:")
    print(f"marker_ok={ok_marker}, separated_ok={separated}")
    if ok_marker and separated:
        print("✅ Per-user logging works and logs are separated")
    else:
        print("❌ Per-user logging check failed")


if __name__ == '__main__':
    main() 
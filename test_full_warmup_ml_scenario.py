#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import time
import json
from pathlib import Path
from datetime import datetime

from database.db_manager import init_db, get_session
from database.models import InstagramAccount, WarmupTask, WarmupStatus
from instagram.health_monitor import AdvancedHealthMonitor
from instagram.predictive_monitor import PredictiveMonitor
from utils.async_warmup_queue import start_async_warmup_queue, stop_async_warmup_queue
from utils.user_logger import log_user_info
from config import DATA_DIR

U1 = 3000000
U2 = 3000001
LOG_U1 = Path(DATA_DIR) / 'users' / str(U1) / 'logs' / 'user.log'
LOG_U2 = Path(DATA_DIR) / 'users' / str(U2) / 'logs' / 'user.log'


def ensure_accounts():
    s = get_session()
    try:
        suffix = str(int(time.time()))
        def mk(u, name):
            acc = InstagramAccount(
                username=f"{name}_{suffix}",
                password='pass',
                email=f'{name}_{suffix}@test.local',
                full_name=f'{name} Full',
                biography='test bio',
                user_id=u,
                is_active=True,
                status='active',
                created_at=datetime.now(),
                updated_at=datetime.now()
            )
            s.add(acc)
            s.flush()
            return acc.id
        u1_post = mk(U1, 'u1_post')
        u1_warm = mk(U1, 'u1_warm')
        u2_post = mk(U2, 'u2_post')
        u2_warm = mk(U2, 'u2_warm')
        s.commit()
        return u1_post, u1_warm, u2_post, u2_warm
    finally:
        s.close()


def tail(p: Path, n=40):
    if not p.exists():
        return '<no file>'
    return '\n'.join(p.read_text(encoding='utf-8', errors='ignore').splitlines()[-n:])


def main():
    print('=== FULL WARMUP + ML SCENARIO ===')
    init_db()
    stop_async_warmup_queue()  # ensure clean start

    # markers in user logs
    log_user_info(U1, '--- FULL TEST START ---')
    log_user_info(U2, '--- FULL TEST START ---')

    u1_post, u1_warm, u2_post, u2_warm = ensure_accounts()

    # schedule warmups (force_passive to demonstrate safe mode)
    s = get_session()
    try:
        for acc_id, uid in [(u1_warm, U1), (u2_warm, U2)]:
            t = WarmupTask(
                account_id=acc_id,
                status=WarmupStatus.PENDING,
                settings=json.dumps({
                    'user_id': uid,
                    'warmup_speed': 'NORMAL',
                    'force_passive': True
                }),
                created_at=datetime.now()
            )
            s.add(t)
        s.commit()
    finally:
        s.close()

    # start queue
    start_async_warmup_queue(max_workers=2)

    # wait for processing
    time.sleep(6)

    # fetch progress and print statuses
    s = get_session()
    try:
        tasks = s.query(WarmupTask).order_by(WarmupTask.id.desc()).limit(10).all()
        print('\n--- Warmup Tasks Status ---')
        for t in tasks:
            prog = t.progress or {}
            print({
                'id': t.id,
                'account_id': t.account_id,
                'status': t.status.value if t.status else None,
                'next_attempt_at': prog.get('next_attempt_at')
            })
    finally:
        s.close()

    # health + risk demo
    hm = AdvancedHealthMonitor()
    pm = PredictiveMonitor()
    for acc_id, uid in [(u1_warm, U1), (u2_warm, U2)]:
        h = hm.calculate_comprehensive_health_score(acc_id)
        r = pm.calculate_ban_risk_score(acc_id)
        print(f"Account {acc_id}: health={h}, risk={r}")

    # show user logs tails
    print('\n--- Tail U1 ---')
    print(tail(LOG_U1, 40))
    print('\n--- Tail U2 ---')
    print(tail(LOG_U2, 40))

    print('\nDone. See statuses, next_attempt_at (if failures), and per-user logs with passive mode notes.')


if __name__ == '__main__':
    main() 
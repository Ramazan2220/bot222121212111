#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import time
import json
from pathlib import Path
from datetime import datetime

from database.db_manager import init_db, get_session
from database.models import InstagramAccount, WarmupTask, WarmupStatus
from utils.async_warmup_queue import start_async_warmup_queue, stop_async_warmup_queue
from utils.user_logger import log_user_info
from config import DATA_DIR
from instagram.health_monitor import AdvancedHealthMonitor
from instagram.predictive_monitor import PredictiveMonitor

U1 = 3000000
U2 = 3000001
LOG_U1 = Path(DATA_DIR) / 'users' / str(U1) / 'logs' / 'user.log'
LOG_U2 = Path(DATA_DIR) / 'users' / str(U2) / 'logs' / 'user.log'
REPORTS_DIR = Path(DATA_DIR) / 'reports'


def emulate_environment():
    # Patch InstagramClient in async_warmup_queue to a stub
    import utils.async_warmup_queue as aq

    class StubClient:
        def __init__(self, account_id):
            self.account_id = account_id
        def check_login(self):
            return True
    aq.InstagramClient = StubClient

    # Patch WarmupManager.perform_human_warmup_session to emulate success
    import utils.warmup_manager as wm
    def fake_session(self, settings: dict):
        # emulate passive vs normal
        passive = settings.get('force_passive')
        actions = {'view_feed': 5, 'view_stories': 3} if passive else {'view_feed': 4, 'like_posts': 2, 'watch_reels': 2}
        return {
            'actions_performed': actions,
            'errors': [],
            'session_duration': 12,
            'session_type': 'emulated_passive' if passive else 'emulated_active',
            'interest_based': False
        }
    wm.WarmupManager.perform_human_warmup_session = fake_session


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
        u1_warm = mk(U1, 'u1_warm')
        u2_warm = mk(U2, 'u2_warm')
        s.commit()
        return u1_warm, u2_warm
    finally:
        s.close()


def tail(p: Path, n=40):
    if not p.exists():
        return '<no file>'
    return '\n'.join(p.read_text(encoding='utf-8', errors='ignore').splitlines()[-n:])


def main():
    print('=== EMULATED WARMUP + ML SCENARIO ===')
    init_db()
    stop_async_warmup_queue()
    emulate_environment()

    log_user_info(U1, '--- EMU TEST START ---')
    log_user_info(U2, '--- EMU TEST START ---')

    u1_warm, u2_warm = ensure_accounts()

    # schedule emulated warmups (passive for U1, normal for U2)
    s = get_session()
    try:
        for acc_id, uid, passive in [(u1_warm, U1, True), (u2_warm, U2, False)]:
            t = WarmupTask(
                account_id=acc_id,
                status=WarmupStatus.PENDING,
                settings=json.dumps({
                    'user_id': uid,
                    'warmup_speed': 'NORMAL',
                    'force_passive': passive
                }),
                created_at=datetime.now()
            )
            s.add(t)
        s.commit()
    finally:
        s.close()

    start_async_warmup_queue(max_workers=2)

    # wait for emulated processing short time
    time.sleep(2)

    s = get_session()
    try:
        tasks = s.query(WarmupTask).order_by(WarmupTask.id.desc()).limit(10).all()
        print('\n--- Warmup Tasks Status ---')
        rows = []
        for t in tasks:
            prog = t.progress or {}
            actions = (prog.get('last_session_results') or {}).get('actions_performed')
            row = {
                'id': t.id,
                'account_id': t.account_id,
                'status': t.status.value if t.status else None,
                'next_attempt_at': prog.get('next_attempt_at'),
                'actions': actions
            }
            print(row)
            rows.append(row)
    finally:
        s.close()

    # compute health/risk for involved accounts
    hm = AdvancedHealthMonitor()
    pm = PredictiveMonitor()
    acc_list = [u1_warm, u2_warm]
    acc_metrics = {}
    for acc_id in acc_list:
        acc_metrics[acc_id] = {
            'health': hm.calculate_comprehensive_health_score(acc_id),
            'risk': pm.calculate_ban_risk_score(acc_id)
        }

    # write JSON report
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    report = {
        'generated_at': datetime.utcnow().isoformat(),
        'accounts': []
    }
    for r in rows:
        acc_id = r['account_id']
        metrics = acc_metrics.get(acc_id, {})
        # extract user_id
        s = get_session()
        try:
            t = s.query(WarmupTask).filter(WarmupTask.account_id == acc_id).order_by(WarmupTask.id.desc()).first()
            uid = None
            if t and t.settings:
                try:
                    st = t.settings if isinstance(t.settings, dict) else json.loads(t.settings)
                    uid = st.get('user_id')
                except Exception:
                    uid = None
        finally:
            s.close()
        report['accounts'].append({
            'user_id': uid,
            'account_id': acc_id,
            'warmup_status': r['status'],
            'actions': r['actions'],
            'next_attempt_at': r['next_attempt_at'],
            'health': metrics.get('health'),
            'risk': metrics.get('risk')
        })
    out_path = REPORTS_DIR / 'warmup_ml_report.json'
    out_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
    print(f"\nSaved report: {out_path}")

    # show user logs tails
    print('\n--- Tail U1 ---')
    print(tail(LOG_U1, 40))
    print('\n--- Tail U2 ---')
    print(tail(LOG_U2, 40))

    print('\nDone. Emulated warmup should show COMPLETED with actions; JSON report saved.')


if __name__ == '__main__':
    main() 
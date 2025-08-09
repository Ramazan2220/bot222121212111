#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import time
from pathlib import Path
from datetime import datetime

from database.db_manager import init_db, get_session
from database.models import InstagramAccount, TaskType
from database.db_manager import create_publish_task
from utils.task_queue import add_task_to_queue, process_task
from utils.async_warmup_queue import start_async_warmup_queue
from utils.user_logger import log_user_info
from config import DATA_DIR

U1 = 3000000  # test user id (small int to fit DB Integer)
U2 = 3000001

LOG_U1 = Path(DATA_DIR) / 'users' / str(U1) / 'logs' / 'user.log'
LOG_U2 = Path(DATA_DIR) / 'users' / str(U2) / 'logs' / 'user.log'


def ensure_accounts():
    session = get_session()
    try:
        suffix = str(int(time.time()))
        # create 2 accounts per user
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
            session.add(acc)
            session.flush()
            return acc.id
        ids = []
        ids.append(mk(U1, 'u1_post_acc'))
        ids.append(mk(U1, 'u1_warm_acc'))
        ids.append(mk(U2, 'u2_post_acc'))
        ids.append(mk(U2, 'u2_warm_acc'))
        session.commit()
        return ids
    finally:
        session.close()


def main():
    print('=== Realistic per-user logs scenario ===')
    init_db()

    # 0) separator marks
    log_user_info(U1, '--- REALISTIC TEST START ---')
    log_user_info(U2, '--- REALISTIC TEST START ---')

    # 1) prepare accounts
    acc_ids = ensure_accounts()
    u1_post, u1_warm, u2_post, u2_warm = acc_ids

    # 2) create publish tasks (u1 + u2)
    ok1, t1 = create_publish_task(
        account_id=u1_post,
        task_type=TaskType.PHOTO,
        media_path=str(Path('test_content')/ 'sample.jpg'),
        caption='Hello from U1',
        user_id=U1
    )
    ok2, t2 = create_publish_task(
        account_id=u2_post,
        task_type=TaskType.PHOTO,
        media_path=str(Path('test_content')/ 'sample.jpg'),
        caption='Hello from U2',
        user_id=U2
    )

    # 3) process publish tasks synchronously to ensure logs
    from types import SimpleNamespace
    dummy_bot = None
    if ok1:
        process_task(t1, chat_id=U1, bot=dummy_bot)
    if ok2:
        process_task(t2, chat_id=U2, bot=dummy_bot)

    # 4) start warmup queue and schedule warmups via settings carrying user_id
    start_async_warmup_queue(max_workers=2)
    from database.db_manager import get_session
    from database.models import WarmupTask, WarmupStatus
    import json
    session = get_session()
    try:
        for acc_id, uid in [(u1_warm, U1), (u2_warm, U2)]:
            task = WarmupTask(
                account_id=acc_id,
                settings=json.dumps({'user_id': uid, 'warmup_speed': 'NORMAL'}),
                status=WarmupStatus.PENDING,
                created_at=datetime.now()
            )
            session.add(task)
        session.commit()
    finally:
        session.close()

    # 5) wait processing a bit longer for warmup
    time.sleep(5)

    # 6) show tails
    def tail(p: Path, n=40):
        if not p.exists(): return '<no file>'
        return '\n'.join(p.read_text(encoding='utf-8', errors='ignore').splitlines()[-n:])

    print('\n--- Tail U1 ---')
    print(tail(LOG_U1, 40))
    print('\n--- Tail U2 ---')
    print(tail(LOG_U2, 40))

    print('\nDone. U1 лог должен содержать публикацию и прогрев U1; U2 — свои события. Логи разделены.')


if __name__ == '__main__':
    main() 
#!/usr/bin/env python3
"""
Скрипт для проверки назначенных прокси аккаунтам
"""

# Инициализируем базу данных
from database.db_manager import init_db, get_session
init_db()

from database.models import InstagramAccount, Proxy
session = get_session()

try:
    # Получаем все аккаунты
    accounts = session.query(InstagramAccount).all()
    print(f"📊 Всего аккаунтов в базе: {len(accounts)}")
    
    # Считаем аккаунты с прокси
    accounts_with_proxy = [acc for acc in accounts if acc.proxy_id]
    print(f"🔗 Аккаунтов с назначенными прокси: {len(accounts_with_proxy)}")
    
    # Получаем все прокси
    proxies = session.query(Proxy).all()
    print(f"🌐 Всего прокси в базе: {len(proxies)}")
    
    # Показываем первые 5 аккаунтов с прокси
    print("\n📋 Примеры аккаунтов с прокси:")
    for i, account in enumerate(accounts_with_proxy[:5]):
        proxy = session.query(Proxy).filter_by(id=account.proxy_id).first()
        if proxy:
            print(f"   • {account.username} -> {proxy.protocol}://{proxy.host}:{proxy.port}")
    
    # Показываем первые 5 аккаунтов без прокси
    accounts_without_proxy = [acc for acc in accounts if not acc.proxy_id]
    if accounts_without_proxy:
        print(f"\n⚠️  Аккаунтов БЕЗ прокси: {len(accounts_without_proxy)}")
        print("📋 Примеры аккаунтов без прокси:")
        for account in accounts_without_proxy[:5]:
            print(f"   • {account.username}")
    
finally:
    session.close()
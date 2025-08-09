#!/usr/bin/env python3
"""
Проверка распределения прокси по аккаунтам
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database.db_manager import get_session
from database.models import InstagramAccount, Proxy

def check_proxy_distribution():
    """Проверяем как распределены прокси"""
    
    session = get_session()
    
    print("🔍 РАСПРЕДЕЛЕНИЕ ПРОКСИ:")
    print("=" * 60)
    
    # Получаем все прокси
    proxies = session.query(Proxy).filter_by(is_active=True).all()
    print(f"\n📊 Активных прокси: {len(proxies)}")
    
    # Для каждого прокси показываем аккаунты
    for proxy in proxies:
        accounts = session.query(InstagramAccount).filter_by(proxy_id=proxy.id).all()
        print(f"\n🌐 Прокси: {proxy.host}:{proxy.port}")
        print(f"   📍 Статус: {'✅ Активен' if proxy.is_active else '❌ Неактивен'}")
        print(f"   👥 Аккаунтов: {len(accounts)}")
        
        if accounts:
            print("   📱 Аккаунты:")
            for acc in accounts[:5]:  # Показываем первые 5
                print(f"      • @{acc.username} (ID: {acc.id})")
            if len(accounts) > 5:
                print(f"      ... и еще {len(accounts) - 5} аккаунтов")
    
    # Аккаунты без прокси
    accounts_without_proxy = session.query(InstagramAccount).filter_by(proxy_id=None).all()
    
    print(f"\n\n🚫 Аккаунтов БЕЗ прокси: {len(accounts_without_proxy)}")
    if accounts_without_proxy:
        print("   Примеры:")
        for acc in accounts_without_proxy[:10]:
            print(f"   • @{acc.username} (ID: {acc.id})")
    
    # Проблемные аккаунты
    print("\n\n🔍 ПРОБЛЕМНЫЕ АККАУНТЫ:")
    problem_ids = [6, 7, 22]
    for acc_id in problem_ids:
        account = session.query(InstagramAccount).filter_by(id=acc_id).first()
        if account:
            if account.proxy_id:
                proxy = session.query(Proxy).filter_by(id=account.proxy_id).first()
                print(f"   • @{account.username} → {proxy.host}:{proxy.port if proxy else 'НЕТ'}")
            else:
                print(f"   • @{account.username} → 🚫 БЕЗ ПРОКСИ!")
    
    session.close()

if __name__ == "__main__":
    check_proxy_distribution() 
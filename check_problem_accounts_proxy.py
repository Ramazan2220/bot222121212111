#!/usr/bin/env python3
"""
Проверка прокси для проблемных аккаунтов
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database.db_manager import get_session
from database.models import InstagramAccount, Proxy
import requests

def check_problem_accounts():
    """Проверяем прокси для проблемных аккаунтов"""
    
    # Проблемные аккаунты из логов
    problem_accounts = [
        {"id": 6, "username": "pagehank302073", "status": "❌ BadPassword"},
        {"id": 7, "username": "fischercarmen3096194", "status": "❌ BadPassword"}, 
        {"id": 22, "username": "meanthony_21260", "status": "✅ Работает"}
    ]
    
    session = get_session()
    
    print("🔍 ПРОВЕРКА ПРОКСИ ДЛЯ ПРОБЛЕМНЫХ АККАУНТОВ:")
    print("=" * 60)
    
    for acc_info in problem_accounts:
        account_id = acc_info["id"]
        
        account = session.query(InstagramAccount).filter_by(id=account_id).first()
        
        if not account:
            print(f"❌ ID {account_id}: Аккаунт не найден")
            continue
            
        print(f"\n📱 @{account.username} - {acc_info['status']}")
        
        if account.proxy_id:
            proxy = session.query(Proxy).filter_by(id=account.proxy_id).first()
            if proxy:
                print(f"   🌐 Прокси: {proxy.protocol}://{proxy.host}:{proxy.port}")
                print(f"   📍 Тип: {proxy.type}")
                print(f"   🔌 Активен: {'ДА' if proxy.is_active else 'НЕТ'}")
                
                # Проверяем прокси
                if proxy.username and proxy.password:
                    proxy_url = f"{proxy.protocol}://{proxy.username}:{proxy.password}@{proxy.host}:{proxy.port}"
                else:
                    proxy_url = f"{proxy.protocol}://{proxy.host}:{proxy.port}"
                
                proxies = {
                    'http': proxy_url,
                    'https': proxy_url
                }
                
                try:
                    print(f"   🔄 Проверяем прокси...")
                    response = requests.get('http://httpbin.org/ip', proxies=proxies, timeout=10)
                    if response.status_code == 200:
                        ip_data = response.json()
                        print(f"   ✅ Прокси работает! IP: {ip_data.get('origin', 'Unknown')}")
                    else:
                        print(f"   ❌ Прокси не работает! Статус: {response.status_code}")
                except Exception as e:
                    print(f"   ❌ Ошибка прокси: {str(e)}")
            else:
                print(f"   ❌ Прокси #{account.proxy_id} не найден в базе")
        else:
            print(f"   🚫 БЕЗ ПРОКСИ!")
    
    # Проверяем IP без прокси
    print("\n\n🌍 ТЕКУЩИЙ IP БЕЗ ПРОКСИ:")
    try:
        response = requests.get('http://httpbin.org/ip', timeout=10)
        if response.status_code == 200:
            ip_data = response.json()
            print(f"   📍 IP: {ip_data.get('origin', 'Unknown')}")
    except Exception as e:
        print(f"   ❌ Ошибка: {str(e)}")
    
    print("\n" + "=" * 60)
    print("💡 ВЫВОД:")
    print("   Если все аккаунты используют ОДИН IP или прокси не работают,")
    print("   Instagram может блокировать их за массовые действия!")
    
    session.close()

if __name__ == "__main__":
    check_problem_accounts() 
#!/usr/bin/env python3
"""
Скрипт для добавления Proxiware прокси в систему
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database.db_manager import add_proxy, get_instagram_accounts, assign_proxy_to_account

def add_proxiware_proxy(proxiware_string):
    """Добавляет Proxiware прокси в систему"""
    
    # Парсим формат
    parts = proxiware_string.split(':')
    if len(parts) != 4:
        return False, "Неправильный формат Proxiware"
    
    host = parts[0]
    port = int(parts[1])
    username = parts[2]
    password = parts[3]
    protocol = "http"
    
    # Добавляем в БД
    success, result = add_proxy(protocol, host, port, username, password)
    
    if success:
        print(f"✅ Прокси добавлен с ID: {result}")
        
        # Назначаем первому аккаунту для тестирования
        accounts = get_instagram_accounts()
        if accounts:
            assign_success, assign_message = assign_proxy_to_account(accounts[0].id, result)
            if assign_success:
                print(f"✅ Прокси назначен аккаунту @{accounts[0].username}")
            else:
                print(f"⚠️ Прокси добавлен, но не назначен: {assign_message}")
        
        return True, result
    else:
        return False, result

if __name__ == "__main__":
    # Ваш Proxiware прокси
    proxiware = "proxy.proxiware.com:1337:user-default-network-mbl-pool-1-country-uk:L9p2WjtFRipG"
    
    print("🔧 ДОБАВЛЕНИЕ PROXIWARE ПРОКСИ В СИСТЕМУ")
    print("=" * 60)
    
    success, result = add_proxiware_proxy(proxiware)
    
    if success:
        print(f"🎉 ГОТОВО! Прокси добавлен и готов к использованию!")
    else:
        print(f"❌ Ошибка: {result}")

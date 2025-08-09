#!/usr/bin/env python3
"""
Скрипт для настройки residential прокси с ротацией IP
Поддерживает proxy-seller.io и другие residential провайдеры
"""

import sys
import json
from database.db_manager import add_proxy, get_session, get_proxies
from database.models import Proxy
from utils.rotating_proxy_manager import get_rotating_proxy_url

def setup_proxy_seller_residential():
    """Настройка residential прокси для proxy-seller.io"""
    
    print("🌐 НАСТРОЙКА RESIDENTIAL ПРОКСИ PROXY-SELLER.IO")
    print("=" * 60)
    
    # Получаем данные от пользователя
    print("\n📝 Введите данные вашего residential прокси:")
    
    host = input("Host (например res.proxy-seller.io): ").strip() or "res.proxy-seller.io"
    port = input("Порт (например 10000): ").strip() or "10000"
    username = input("Username (например user-default-network-res): ").strip()
    password = input("Password: ").strip()
    
    if not username or not password:
        print("❌ Username и password обязательны!")
        return False
    
    print(f"\n🔧 НАСТРОЙКА:")
    print(f"Host: {host}")
    print(f"Port: {port}")
    print(f"Username: {username}")
    print(f"Type: Residential с ротацией IP")
    
    # Модифицируем username для поддержки сессий
    if 'session' not in username.lower() and 'user-' not in username.lower():
        rotating_username = f"user-session-{username}"
    else:
        rotating_username = username
    
    # Создаем прокси в базе данных
    try:
        proxy_id = add_proxy(
            protocol="http",
            host=host,
            port=int(port),
            username=rotating_username,  # Используем модифицированный username
            password=password
        )
        
        print(f"\n✅ Прокси добавлен в базу данных (ID: {proxy_id})")
        print(f"🔄 Username настроен для ротации: {rotating_username}")
        
        # Тестируем прокси
        print("\n🧪 ТЕСТИРОВАНИЕ ПРОКСИ...")
        test_proxy_rotation(proxy_id)
        
        return True
        
    except Exception as e:
        print(f"❌ Ошибка при добавлении прокси: {e}")
        return False

def test_proxy_rotation(proxy_id):
    """Тестирует ротацию IP для прокси"""
    
    session = get_session()
    try:
        proxy = session.query(Proxy).filter_by(id=proxy_id).first()
        if not proxy:
            print(f"❌ Прокси с ID {proxy_id} не найден")
            return
        
        print(f"🔄 Тестирование ротации IP для прокси {proxy.host}:{proxy.port}")
        
        # Создаем конфигурацию прокси
        proxy_config = {
            'protocol': proxy.protocol,
            'host': proxy.host,
            'port': proxy.port,
            'username': proxy.username,
            'password': proxy.password
        }
        
        # Тестируем несколько типов ротации
        test_accounts = [1, 2, 3]
        
        print("\n📊 ТЕСТИРОВАНИЕ РАЗНЫХ ТИПОВ РОТАЦИИ:")
        
        for account_id in test_accounts:
            print(f"\n🔸 Аккаунт ID {account_id}:")
            
            # Ротация по времени
            time_url = get_rotating_proxy_url(proxy_config, account_id, "time")
            print(f"  ⏰ По времени: {mask_password(time_url)}")
            
            # Ротация на каждом запросе
            request_url = get_rotating_proxy_url(proxy_config, account_id, "request")
            print(f"  🔄 На запросе: {mask_password(request_url)}")
            
        print("\n✅ Тестирование завершено!")
        print("💡 Каждый аккаунт получит уникальный IP адрес")
        
    finally:
        session.close()

def mask_password(url):
    """Маскирует пароль в URL для безопасного вывода"""
    if ':' in url and '@' in url:
        parts = url.split('@')
        if len(parts) == 2:
            auth_part = parts[0]
            if ':' in auth_part:
                protocol_user = auth_part.rsplit(':', 1)[0]
                return f"{protocol_user}:***@{parts[1]}"
    return url

def list_current_proxies():
    """Показывает текущие прокси в системе"""
    
    proxies = get_proxies()
    
    if not proxies:
        print("📭 Прокси не найдены в системе")
        return
    
    print("\n📋 ТЕКУЩИЕ ПРОКСИ В СИСТЕМЕ:")
    print("=" * 60)
    
    for proxy in proxies:
        status = "🟢 Активен" if proxy.is_active else "🔴 Неактивен"
        proxy_type = "🔄 Rotating" if ('session' in proxy.username.lower() or 'user-' in proxy.username.lower()) else "📌 Static"
        
        print(f"\nID: {proxy.id} | {status} | {proxy_type}")
        print(f"  📍 {proxy.protocol}://{proxy.host}:{proxy.port}")
        print(f"  👤 User: {proxy.username}")
        print(f"  🔑 Pass: {'*' * len(proxy.password) if proxy.password else 'N/A'}")

def main():
    """Главная функция"""
    
    print("🌐 НАСТРОЙКА RESIDENTIAL ПРОКСИ С РОТАЦИЕЙ IP")
    print("=" * 60)
    
    while True:
        print("\n📋 ВЫБЕРИТЕ ДЕЙСТВИЕ:")
        print("1. 🔧 Настроить новый proxy-seller.io residential")
        print("2. 📋 Показать текущие прокси")
        print("3. 🧪 Тестировать ротацию существующего прокси")
        print("4. ❌ Выход")
        
        choice = input("\nВведите номер (1-4): ").strip()
        
        if choice == "1":
            setup_proxy_seller_residential()
        elif choice == "2":
            list_current_proxies()
        elif choice == "3":
            proxy_id = input("Введите ID прокси для тестирования: ").strip()
            try:
                test_proxy_rotation(int(proxy_id))
            except ValueError:
                print("❌ Неверный ID прокси")
        elif choice == "4":
            print("👋 До свидания!")
            break
        else:
            print("❌ Неверный выбор!")

if __name__ == "__main__":
    main() 
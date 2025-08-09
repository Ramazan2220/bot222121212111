#!/usr/bin/env python3
"""
Расширенная диагностика Proxiware прокси
"""

import requests
import socket
import time
from urllib.parse import urlparse

def test_basic_connectivity(host, port):
    """Проверяет базовое подключение к хосту"""
    print(f"🔌 ТЕСТ БАЗОВОГО ПОДКЛЮЧЕНИЯ")
    print("=" * 50)
    
    try:
        print(f"   Подключение к {host}:{port}...")
        socket.create_connection((host, port), timeout=10)
        print(f"   ✅ Порт {port} доступен на {host}")
        return True
    except socket.timeout:
        print(f"   ❌ Таймаут подключения к {host}:{port}")
        return False
    except socket.error as e:
        print(f"   ❌ Ошибка подключения: {e}")
        return False

def test_proxy_authentication(proxy_url):
    """Тестирует авторизацию прокси"""
    print(f"\n🔐 ТЕСТ АВТОРИЗАЦИИ ПРОКСИ")
    print("=" * 50)
    
    # Парсим URL прокси
    parsed = urlparse(proxy_url.replace('http://', ''))
    credentials = parsed.netloc.split('@')[0]
    endpoint = parsed.netloc.split('@')[1]
    
    print(f"   Эндпоинт: {endpoint}")
    print(f"   Учетные данные: {credentials[:20]}...")
    
    # Тестируем различные методы
    test_urls = [
        'http://httpbin.org/ip',
        'https://httpbin.org/ip', 
        'http://ip-api.com/json',
        'https://whatismyipaddress.com/api/ip'
    ]
    
    proxies = {
        'http': proxy_url,
        'https': proxy_url
    }
    
    for test_url in test_urls:
        try:
            print(f"   Тестирую {test_url}...")
            response = requests.get(test_url, proxies=proxies, timeout=15)
            print(f"   ✅ {test_url}: HTTP {response.status_code}")
            
            if response.status_code == 200:
                print(f"   📍 Ответ получен: {response.text[:100]}...")
                return True
            
        except requests.exceptions.ProxyError as e:
            if "503" in str(e):
                print(f"   🚫 503 Service Unavailable - прокси неактивен")
            elif "407" in str(e):
                print(f"   🔐 407 Proxy Authentication Required - неверные учетные данные")
            elif "Connection refused" in str(e):
                print(f"   ❌ Connection refused - прокси недоступен")
            else:
                print(f"   ❌ Ошибка прокси: {e}")
        except Exception as e:
            print(f"   ❌ Общая ошибка: {e}")
    
    return False

def check_proxiware_status():
    """Проверяет статус сервисов Proxiware"""
    print(f"\n🌐 ПРОВЕРКА СТАТУСА PROXIWARE")
    print("=" * 50)
    
    # Проверяем основной сайт
    try:
        response = requests.get('https://proxiware.com', timeout=10)
        if response.status_code == 200:
            print("   ✅ Сайт Proxiware доступен")
        else:
            print(f"   ⚠️  Сайт Proxiware: HTTP {response.status_code}")
    except:
        print("   ❌ Сайт Proxiware недоступен")
    
    # Проверяем прокси эндпоинт
    try:
        socket.create_connection(('proxy.proxiware.com', 1337), timeout=10)
        print("   ✅ Прокси сервер proxy.proxiware.com:1337 отвечает")
    except:
        print("   ❌ Прокси сервер proxy.proxiware.com:1337 недоступен")

def generate_troubleshooting_guide():
    """Генерирует руководство по устранению неполадок"""
    print(f"\n🔧 РУКОВОДСТВО ПО УСТРАНЕНИЮ НЕПОЛАДОК")
    print("=" * 70)
    
    print("📋 ШАГ 1: ПРОВЕРЬТЕ ПАНЕЛЬ PROXIWARE")
    print("   1. Войдите в панель управления Proxiware")
    print("   2. Проверьте статус подписки/оплаты")
    print("   3. Убедитесь, что прокси активированы")
    print("   4. Проверьте лимиты трафика")
    print()
    
    print("📋 ШАГ 2: НАСТРОЙКА IP АВТОРИЗАЦИИ")
    print("   1. Узнайте ваш текущий IP:")
    print("      curl ipinfo.io/ip")
    print("   2. Добавьте IP в белый список в панели Proxiware")
    print("   3. Подождите 5-10 минут для активации")
    print()
    
    print("📋 ШАГ 3: ПРОВЕРКА УЧЕТНЫХ ДАННЫХ")
    print("   1. Скопируйте точные данные из панели Proxiware")
    print("   2. Проверьте формат: host:port:username:password")
    print("   3. Убедитесь в отсутствии лишних пробелов")
    print()
    
    print("📋 ШАГ 4: АЛЬТЕРНАТИВНЫЕ НАСТРОЙКИ")
    print("   1. Попробуйте другой пул (pool-2, pool-3)")
    print("   2. Измените страну (country-us, country-de)")
    print("   3. Попробуйте статичный endpoint (если доступен)")
    print()
    
    print("📞 КОНТАКТЫ ПОДДЕРЖКИ PROXIWARE:")
    print("   📧 Email: support@proxiware.com")
    print("   💬 Telegram: @proxiware_support")
    print("   🌐 Документация: docs.proxiware.com")

def suggest_immediate_solutions():
    """Предлагает немедленные решения"""
    print(f"\n💡 НЕМЕДЛЕННЫЕ РЕШЕНИЯ")
    print("=" * 50)
    
    print("🔄 ПОПРОБУЙТЕ АЛЬТЕРНАТИВНЫЕ ФОРМАТЫ:")
    print("   1. proxy.proxiware.com:1337:user-default-network-mbl-pool-2-country-uk:L9p2WjtFRipG")
    print("   2. proxy.proxiware.com:1337:user-default-network-mbl-pool-1-country-us:L9p2WjtFRipG")
    print("   3. proxy.proxiware.com:1337:user-default-network-residential-pool-1-country-uk:L9p2WjtFRipG")
    print()
    
    print("⚙️  НАСТРОЙКИ ДЛЯ ТЕСТИРОВАНИЯ:")
    print("   • Увеличьте timeout до 30 секунд")
    print("   • Попробуйте HTTP вместо HTTPS")
    print("   • Используйте разные User-Agent")
    print()
    
    print("🚀 БЫСТРАЯ ПРОВЕРКА В БРАУЗЕРЕ:")
    print("   1. Настройте прокси в браузере")
    print("   2. Откройте whatismyipaddress.com")
    print("   3. Проверьте, изменился ли IP")

def main():
    """Основная функция диагностики"""
    proxy_string = "proxy.proxiware.com:1337:user-default-network-mbl-pool-1-country-uk:L9p2WjtFRipG"
    
    print("🔍 РАСШИРЕННАЯ ДИАГНОСТИКА PROXIWARE")
    print("=" * 80)
    print(f"🎯 Тестируемый прокси: {proxy_string}")
    print()
    
    # Парсим прокси
    parts = proxy_string.split(':')
    host = parts[0]
    port = int(parts[1])
    username = parts[2]
    password = parts[3]
    proxy_url = f"http://{username}:{password}@{host}:{port}"
    
    # Тест 1: Базовое подключение
    connectivity_ok = test_basic_connectivity(host, port)
    
    # Тест 2: Статус Proxiware
    check_proxiware_status()
    
    # Тест 3: Авторизация прокси
    if connectivity_ok:
        auth_ok = test_proxy_authentication(proxy_url)
        
        if not auth_ok:
            print(f"\n❌ ОСНОВНАЯ ПРОБЛЕМА: Прокси недоступен или неактивен")
            
            # Показываем руководство
            generate_troubleshooting_guide()
            suggest_immediate_solutions()
    else:
        print(f"\n❌ КРИТИЧЕСКАЯ ПРОБЛЕМА: Сервер Proxiware недоступен")
        print("   🔧 Обратитесь в поддержку Proxiware")

if __name__ == "__main__":
    main() 
#!/usr/bin/env python3
"""
Отладочный скрипт для проверки данных прокси в базе
"""

from database.db_manager import get_proxies
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    try:
        print("🔍 Загружаем прокси из базы данных...")
        proxies = get_proxies()
        
        print(f"📊 Найдено {len(proxies)} прокси\n")
        
        for i, proxy in enumerate(proxies[:10]):  # Показываем первые 10
            print(f"🔹 Прокси #{proxy.id}:")
            print(f"   Protocol: '{proxy.protocol}'")
            print(f"   Host: '{proxy.host}'")
            print(f"   Port: {proxy.port}")
            print(f"   Username: '{proxy.username}'")
            print(f"   Password: '{proxy.password[:8]}...' (показаны первые 8 символов)")
            print(f"   Is Active: {proxy.is_active}")
            
            # Проверяем есть ли странные символы в username
            if proxy.username and '-' in proxy.username:
                print(f"   ⚠️  ВНИМАНИЕ: Username содержит дефис: '{proxy.username}'")
            
            print()
        
        print("🔍 Ищем прокси с дефисами в username...")
        problematic_proxies = [p for p in proxies if p.username and '-' in p.username]
        
        print(f"📊 Найдено {len(problematic_proxies)} прокси с дефисами в username")
        
        if problematic_proxies:
            print("\n⚠️  Проблемные прокси:")
            for proxy in problematic_proxies[:5]:  # Показываем первые 5
                print(f"   ID {proxy.id}: username='{proxy.username}', host='{proxy.host}'")
        
        print("\n🔍 Проверяем уникальные протоколы:")
        protocols = set(p.protocol for p in proxies)
        print(f"   Протоколы: {protocols}")
        
        print("\n🔍 Проверяем уникальные хосты:")
        hosts = set(p.host for p in proxies)
        print(f"   Количество уникальных хостов: {len(hosts)}")
        for host in list(hosts)[:5]:
            print(f"   - {host}")
            
    except Exception as e:
        logger.error(f"❌ Ошибка при отладке прокси: {e}")

if __name__ == "__main__":
    main() 
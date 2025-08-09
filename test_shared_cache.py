#!/usr/bin/env python3
"""
Тест системы общего кеша доступов
"""

import sys
import os
import json
import time

sys.path.insert(0, './utils')

def test_shared_cache():
    print("🧪 ТЕСТ СИСТЕМЫ ОБЩЕГО КЕША")
    print("=" * 50)
    
    test_user_id = 6626270112
    cache_file = "data/shared_access_cache.json"
    
    try:
        from access_manager import get_access_manager, has_access
        
        print("✅ Импорт успешен")
        
        # Получаем менеджер
        manager = get_access_manager()
        
        # Проверяем наличие новых методов
        if hasattr(manager, '_load_shared_cache'):
            print("✅ Метод _load_shared_cache найден")
        else:
            print("❌ Метод _load_shared_cache НЕ найден")
            
        if hasattr(manager, '_save_shared_cache'):
            print("✅ Метод _save_shared_cache найден")
        else:
            print("❌ Метод _save_shared_cache НЕ найден")
            
        if hasattr(manager, '_sync_with_shared_cache'):
            print("✅ Метод _sync_with_shared_cache найден")
        else:
            print("❌ Метод _sync_with_shared_cache НЕ найден")
        
        # Выполняем синхронизацию
        print("\n🔄 Выполняем синхронизацию...")
        manager.force_sync()
        
        # Проверяем файл кеша
        if os.path.exists(cache_file):
            print(f"✅ Файл кеша создан: {cache_file}")
            
            with open(cache_file, 'r', encoding='utf-8') as f:
                cache_data = json.load(f)
                
            print(f"📊 Размер кеша: {len(cache_data.get('cache', {}))}")
            print(f"⏰ Последнее обновление: {cache_data.get('last_update', 'N/A')}")
            print(f"🔧 Обновлено: {cache_data.get('updated_by', 'N/A')}")
            
            # Проверяем наличие тестового пользователя
            cache = cache_data.get('cache', {})
            user_key = str(test_user_id)
            if user_key in cache:
                print(f"✅ Пользователь {test_user_id} найден в кеше")
                user_data = cache[user_key]
                print(f"   is_active: {user_data.get('is_active')}")
                print(f"   role: {user_data.get('role')}")
            else:
                print(f"❌ Пользователь {test_user_id} НЕ найден в кеше")
                
        else:
            print(f"❌ Файл кеша НЕ создан: {cache_file}")
        
        # Проверяем has_access
        result = has_access(test_user_id)
        print(f"\n📊 has_access({test_user_id}): {result}")
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_shared_cache() 
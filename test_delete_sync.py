#!/usr/bin/env python3
"""
ТЕСТ СИНХРОНИЗАЦИИ ПРИ УДАЛЕНИИ
"""

import sys
import os
import json

sys.path.insert(0, './utils')
sys.path.insert(0, './admin_bot')

def test_delete_sync():
    print("🗑️ ТЕСТ СИНХРОНИЗАЦИИ ПРИ УДАЛЕНИИ")
    print("=" * 80)
    
    user_id = 6626270112
    
    # 1. Проверяем что пользователь есть
    print("\n1️⃣ ПРОВЕРЯЕМ НАЛИЧИЕ ПОЛЬЗОВАТЕЛЯ:")
    
    try:
        from access_manager import get_access_manager, has_access, delete_user_completely
        
        access_result = has_access(user_id)
        print(f"📊 has_access({user_id}): {access_result}")
        
        # Проверяем shared cache
        cache_file = "data/shared_access_cache.json"
        if os.path.exists(cache_file):
            with open(cache_file, 'r', encoding='utf-8') as f:
                cache_data = json.load(f)
            
            cache = cache_data.get('cache', {})
            user_key = str(user_id)
            
            if user_key in cache:
                print(f"✅ Пользователь {user_id} в shared cache")
            else:
                print(f"❌ Пользователь {user_id} НЕ в shared cache")
                
    except Exception as e:
        print(f"❌ Ошибка проверки: {e}")
    
    # 2. Удаляем пользователя
    print(f"\n2️⃣ УДАЛЯЕМ ПОЛЬЗОВАТЕЛЯ:")
    
    try:
        success = delete_user_completely(user_id)
        if success:
            print(f"✅ delete_user_completely({user_id}) выполнена успешно")
        else:
            print(f"❌ delete_user_completely({user_id}) неудачно")
    except Exception as e:
        print(f"❌ Ошибка удаления: {e}")
    
    # 3. Проверяем результат удаления
    print(f"\n3️⃣ ПРОВЕРЯЕМ РЕЗУЛЬТАТ УДАЛЕНИЯ:")
    
    try:
        access_result = has_access(user_id)
        print(f"📊 has_access({user_id}): {access_result}")
        
        # Проверяем shared cache
        cache_file = "data/shared_access_cache.json"
        if os.path.exists(cache_file):
            with open(cache_file, 'r', encoding='utf-8') as f:
                cache_data = json.load(f)
            
            cache = cache_data.get('cache', {})
            user_key = str(user_id)
            
            if user_key in cache:
                print(f"❌ Пользователь {user_id} ОСТАЛСЯ в shared cache")
            else:
                print(f"✅ Пользователь {user_id} удален из shared cache")
                
            print(f"📄 В shared cache остались: {list(cache.keys())}")
            print(f"📅 Last update: {cache_data.get('last_update', 'N/A')}")
                
    except Exception as e:
        print(f"❌ Ошибка проверки: {e}")
    
    # 4. Симуляция основного бота после удаления
    print(f"\n4️⃣ СИМУЛЯЦИЯ ОСНОВНОГО БОТА ПОСЛЕ УДАЛЕНИЯ:")
    
    try:
        # Создаем новый AccessManager (как в основном боте)
        from access_manager import AccessManager
        main_bot_access_manager = AccessManager()
        
        main_bot_result = main_bot_access_manager.has_access(user_id)
        print(f"📊 Основной бот has_access({user_id}): {main_bot_result}")
        
        if not main_bot_result:
            print(f"🎉 УСПЕХ! Основной бот НЕ видит удаленного пользователя!")
        else:
            print(f"❌ ПРОБЛЕМА! Основной бот ВИДИТ удаленного пользователя!")
            
    except Exception as e:
        print(f"❌ Ошибка симуляции основного бота: {e}")

def main():
    test_delete_sync()

if __name__ == "__main__":
    main() 
"""
ТЕСТ СИНХРОНИЗАЦИИ ПРИ УДАЛЕНИИ
"""

import sys
import os
import json

sys.path.insert(0, './utils')
sys.path.insert(0, './admin_bot')

def test_delete_sync():
    print("🗑️ ТЕСТ СИНХРОНИЗАЦИИ ПРИ УДАЛЕНИИ")
    print("=" * 80)
    
    user_id = 6626270112
    
    # 1. Проверяем что пользователь есть
    print("\n1️⃣ ПРОВЕРЯЕМ НАЛИЧИЕ ПОЛЬЗОВАТЕЛЯ:")
    
    try:
        from access_manager import get_access_manager, has_access, delete_user_completely
        
        access_result = has_access(user_id)
        print(f"📊 has_access({user_id}): {access_result}")
        
        # Проверяем shared cache
        cache_file = "data/shared_access_cache.json"
        if os.path.exists(cache_file):
            with open(cache_file, 'r', encoding='utf-8') as f:
                cache_data = json.load(f)
            
            cache = cache_data.get('cache', {})
            user_key = str(user_id)
            
            if user_key in cache:
                print(f"✅ Пользователь {user_id} в shared cache")
            else:
                print(f"❌ Пользователь {user_id} НЕ в shared cache")
                
    except Exception as e:
        print(f"❌ Ошибка проверки: {e}")
    
    # 2. Удаляем пользователя
    print(f"\n2️⃣ УДАЛЯЕМ ПОЛЬЗОВАТЕЛЯ:")
    
    try:
        success = delete_user_completely(user_id)
        if success:
            print(f"✅ delete_user_completely({user_id}) выполнена успешно")
        else:
            print(f"❌ delete_user_completely({user_id}) неудачно")
    except Exception as e:
        print(f"❌ Ошибка удаления: {e}")
    
    # 3. Проверяем результат удаления
    print(f"\n3️⃣ ПРОВЕРЯЕМ РЕЗУЛЬТАТ УДАЛЕНИЯ:")
    
    try:
        access_result = has_access(user_id)
        print(f"📊 has_access({user_id}): {access_result}")
        
        # Проверяем shared cache
        cache_file = "data/shared_access_cache.json"
        if os.path.exists(cache_file):
            with open(cache_file, 'r', encoding='utf-8') as f:
                cache_data = json.load(f)
            
            cache = cache_data.get('cache', {})
            user_key = str(user_id)
            
            if user_key in cache:
                print(f"❌ Пользователь {user_id} ОСТАЛСЯ в shared cache")
            else:
                print(f"✅ Пользователь {user_id} удален из shared cache")
                
            print(f"📄 В shared cache остались: {list(cache.keys())}")
            print(f"📅 Last update: {cache_data.get('last_update', 'N/A')}")
                
    except Exception as e:
        print(f"❌ Ошибка проверки: {e}")
    
    # 4. Симуляция основного бота после удаления
    print(f"\n4️⃣ СИМУЛЯЦИЯ ОСНОВНОГО БОТА ПОСЛЕ УДАЛЕНИЯ:")
    
    try:
        # Создаем новый AccessManager (как в основном боте)
        from access_manager import AccessManager
        main_bot_access_manager = AccessManager()
        
        main_bot_result = main_bot_access_manager.has_access(user_id)
        print(f"📊 Основной бот has_access({user_id}): {main_bot_result}")
        
        if not main_bot_result:
            print(f"🎉 УСПЕХ! Основной бот НЕ видит удаленного пользователя!")
        else:
            print(f"❌ ПРОБЛЕМА! Основной бот ВИДИТ удаленного пользователя!")
            
    except Exception as e:
        print(f"❌ Ошибка симуляции основного бота: {e}")

def main():
    test_delete_sync()

if __name__ == "__main__":
    main() 
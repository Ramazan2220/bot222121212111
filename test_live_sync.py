#!/usr/bin/env python3
"""
Тест реальной синхронизации между админ ботом и основным ботом
"""

import sys
import os
import time

# Добавляем пути
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def test_live_sync():
    print("🚀 ТЕСТ ЖИВОЙ СИНХРОНИЗАЦИИ")
    print("=" * 60)
    
    # Импортируем систему
    try:
        from utils.access_manager import has_access, add_user_access, remove_user_access
        print("✅ Импорт access_manager успешен")
    except Exception as e:
        print(f"❌ Ошибка импорта: {e}")
        return False
    
    test_user_id = 123456789
    
    print(f"\n📊 ПРОВЕРКА ФАЙЛА СИНХРОНИЗАЦИИ:")
    try:
        with open('data/fake_redis_sync.json', 'r') as f:
            import json
            data = json.load(f)
            print(f"   Ключей в файле: {len(data.get('data', {}))}")
            print(f"   Содержимое: {data.get('data', {})}")
    except Exception as e:
        print(f"   Ошибка чтения файла: {e}")
    
    print(f"\n1️⃣ Проверяем начальное состояние пользователя {test_user_id}:")
    initial_access = has_access(test_user_id)
    print(f"   has_access({test_user_id}): {initial_access}")
    
    print(f"\n2️⃣ Добавляем пользователя {test_user_id}:")
    add_result = add_user_access(test_user_id)
    print(f"   add_user_access({test_user_id}): {add_result}")
    
    print(f"\n📊 ПРОВЕРКА ФАЙЛА ПОСЛЕ ДОБАВЛЕНИЯ:")
    try:
        with open('data/fake_redis_sync.json', 'r') as f:
            import json
            data = json.load(f)
            print(f"   Ключей в файле: {len(data.get('data', {}))}")
            print(f"   Содержимое: {data.get('data', {})}")
    except Exception as e:
        print(f"   Ошибка чтения файла: {e}")
    
    print(f"\n3️⃣ Ждем 2 секунды для синхронизации...")
    time.sleep(2)
    
    print(f"\n4️⃣ Проверяем доступ после добавления:")
    access_after_add = has_access(test_user_id)
    print(f"   has_access({test_user_id}): {access_after_add}")
    
    print(f"\n5️⃣ Удаляем пользователя {test_user_id}:")
    remove_result = remove_user_access(test_user_id)
    print(f"   remove_user_access({test_user_id}): {remove_result}")
    
    print(f"\n📊 ПРОВЕРКА ФАЙЛА ПОСЛЕ УДАЛЕНИЯ:")
    try:
        with open('data/fake_redis_sync.json', 'r') as f:
            import json
            data = json.load(f)
            print(f"   Ключей в файле: {len(data.get('data', {}))}")
            print(f"   Содержимое: {data.get('data', {})}")
    except Exception as e:
        print(f"   Ошибка чтения файла: {e}")
    
    print(f"\n6️⃣ Ждем 2 секунды для синхронизации...")
    time.sleep(2)
    
    print(f"\n7️⃣ Проверяем доступ после удаления:")
    access_after_remove = has_access(test_user_id)
    print(f"   has_access({test_user_id}): {access_after_remove}")
    
    # Проверяем результаты
    success = (
        initial_access == False and
        add_result == True and
        access_after_add == True and
        remove_result == True and
        access_after_remove == False
    )
    
    print("\n" + "=" * 60)
    if success:
        print("🎉 ТЕСТ СИНХРОНИЗАЦИИ ПРОШЕЛ УСПЕШНО!")
        return True
    else:
        print("❌ ТЕСТ СИНХРОНИЗАЦИИ НЕ ПРОШЕЛ!")
        print(f"   Детали: initial={initial_access}, add={add_result}")
        print(f"           after_add={access_after_add}, remove={remove_result}")
        print(f"           after_remove={access_after_remove}")
        return False

if __name__ == "__main__":
    print("🚀 ЗАПУСК ТЕСТА ЖИВОЙ СИНХРОНИЗАЦИИ")
    print("=" * 80)
    
    success = test_live_sync()
    
    if success:
        print("\n✅ ВСЕ РАБОТАЕТ! Теперь попробуйте в Telegram боте!")
        exit(0)
    else:
        print("\n❌ ПРОБЛЕМЫ С СИНХРОНИЗАЦИЕЙ!")
        exit(1) 
"""
Тест реальной синхронизации между админ ботом и основным ботом
"""

import sys
import os
import time

# Добавляем пути
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def test_live_sync():
    print("🚀 ТЕСТ ЖИВОЙ СИНХРОНИЗАЦИИ")
    print("=" * 60)
    
    # Импортируем систему
    try:
        from utils.access_manager import has_access, add_user_access, remove_user_access
        print("✅ Импорт access_manager успешен")
    except Exception as e:
        print(f"❌ Ошибка импорта: {e}")
        return False
    
    test_user_id = 123456789
    
    print(f"\n📊 ПРОВЕРКА ФАЙЛА СИНХРОНИЗАЦИИ:")
    try:
        with open('data/fake_redis_sync.json', 'r') as f:
            import json
            data = json.load(f)
            print(f"   Ключей в файле: {len(data.get('data', {}))}")
            print(f"   Содержимое: {data.get('data', {})}")
    except Exception as e:
        print(f"   Ошибка чтения файла: {e}")
    
    print(f"\n1️⃣ Проверяем начальное состояние пользователя {test_user_id}:")
    initial_access = has_access(test_user_id)
    print(f"   has_access({test_user_id}): {initial_access}")
    
    print(f"\n2️⃣ Добавляем пользователя {test_user_id}:")
    add_result = add_user_access(test_user_id)
    print(f"   add_user_access({test_user_id}): {add_result}")
    
    print(f"\n📊 ПРОВЕРКА ФАЙЛА ПОСЛЕ ДОБАВЛЕНИЯ:")
    try:
        with open('data/fake_redis_sync.json', 'r') as f:
            import json
            data = json.load(f)
            print(f"   Ключей в файле: {len(data.get('data', {}))}")
            print(f"   Содержимое: {data.get('data', {})}")
    except Exception as e:
        print(f"   Ошибка чтения файла: {e}")
    
    print(f"\n3️⃣ Ждем 2 секунды для синхронизации...")
    time.sleep(2)
    
    print(f"\n4️⃣ Проверяем доступ после добавления:")
    access_after_add = has_access(test_user_id)
    print(f"   has_access({test_user_id}): {access_after_add}")
    
    print(f"\n5️⃣ Удаляем пользователя {test_user_id}:")
    remove_result = remove_user_access(test_user_id)
    print(f"   remove_user_access({test_user_id}): {remove_result}")
    
    print(f"\n📊 ПРОВЕРКА ФАЙЛА ПОСЛЕ УДАЛЕНИЯ:")
    try:
        with open('data/fake_redis_sync.json', 'r') as f:
            import json
            data = json.load(f)
            print(f"   Ключей в файле: {len(data.get('data', {}))}")
            print(f"   Содержимое: {data.get('data', {})}")
    except Exception as e:
        print(f"   Ошибка чтения файла: {e}")
    
    print(f"\n6️⃣ Ждем 2 секунды для синхронизации...")
    time.sleep(2)
    
    print(f"\n7️⃣ Проверяем доступ после удаления:")
    access_after_remove = has_access(test_user_id)
    print(f"   has_access({test_user_id}): {access_after_remove}")
    
    # Проверяем результаты
    success = (
        initial_access == False and
        add_result == True and
        access_after_add == True and
        remove_result == True and
        access_after_remove == False
    )
    
    print("\n" + "=" * 60)
    if success:
        print("🎉 ТЕСТ СИНХРОНИЗАЦИИ ПРОШЕЛ УСПЕШНО!")
        return True
    else:
        print("❌ ТЕСТ СИНХРОНИЗАЦИИ НЕ ПРОШЕЛ!")
        print(f"   Детали: initial={initial_access}, add={add_result}")
        print(f"           after_add={access_after_add}, remove={remove_result}")
        print(f"           after_remove={access_after_remove}")
        return False

if __name__ == "__main__":
    print("🚀 ЗАПУСК ТЕСТА ЖИВОЙ СИНХРОНИЗАЦИИ")
    print("=" * 80)
    
    success = test_live_sync()
    
    if success:
        print("\n✅ ВСЕ РАБОТАЕТ! Теперь попробуйте в Telegram боте!")
        exit(0)
    else:
        print("\n❌ ПРОБЛЕМЫ С СИНХРОНИЗАЦИЕЙ!")
        exit(1) 
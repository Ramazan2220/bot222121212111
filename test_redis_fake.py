#!/usr/bin/env python3
"""
Тест Redis синхронизации с FakeRedis
"""

import sys
import os
import time
from datetime import datetime

# Добавляем пути
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def test_redis_with_fake():
    print("🧪 ТЕСТИРОВАНИЕ REDIS С FAKE REDIS")
    print("=" * 60)
    
    # Импортируем систему
    try:
        from redis_access_sync import RedisAccessSync, get_redis_sync
        print("✅ Импорт redis_access_sync успешен")
    except Exception as e:
        print(f"❌ Ошибка импорта: {e}")
        return False
    
    # Создаем экземпляр
    try:
        sync = RedisAccessSync()
        print("✅ RedisAccessSync создан")
    except Exception as e:
        print(f"❌ Ошибка создания: {e}")
        return False
    
    # Тестируем базовые операции
    test_user_id = 12345
    
    print(f"\n1️⃣ Проверяем начальное состояние пользователя {test_user_id}:")
    has_access_initial = sync.has_access(test_user_id)
    print(f"   has_access({test_user_id}): {has_access_initial}")
    
    print(f"\n2️⃣ Добавляем пользователя {test_user_id}:")
    user_data = {
        'telegram_id': test_user_id,
        'is_active': True,
        'subscription_end': (datetime.now()).isoformat(),
        'role': 'trial'
    }
    add_result = sync.add_user(test_user_id, user_data)
    print(f"   add_user({test_user_id}): {add_result}")
    
    # Проверяем после добавления
    has_access_after_add = sync.has_access(test_user_id)
    print(f"   has_access({test_user_id}) после добавления: {has_access_after_add}")
    
    print(f"\n3️⃣ Удаляем пользователя {test_user_id}:")
    remove_result = sync.remove_user(test_user_id)
    print(f"   remove_user({test_user_id}): {remove_result}")
    
    # Проверяем после удаления
    has_access_after_remove = sync.has_access(test_user_id)
    print(f"   has_access({test_user_id}) после удаления: {has_access_after_remove}")
    
    print(f"\n4️⃣ Статистика:")
    stats = sync.get_stats()
    print(f"   {stats}")
    
    # Проверяем результаты
    success = (
        has_access_initial == False and
        add_result == True and
        has_access_after_add == True and
        remove_result == True and
        has_access_after_remove == False
    )
    
    if success:
        print("\n🎉 ВСЕ ТЕСТЫ ПРОШЛИ УСПЕШНО!")
        return True
    else:
        print("\n❌ НЕКОТОРЫЕ ТЕСТЫ НЕ ПРОШЛИ!")
        return False

def test_global_functions():
    print("\n🔧 ТЕСТИРОВАНИЕ ГЛОБАЛЬНЫХ ФУНКЦИЙ")
    print("=" * 60)
    
    # Импортируем глобальные функции
    try:
        from redis_access_sync import has_access_redis, add_user_redis, remove_user_redis
        print("✅ Импорт глобальных функций успешен")
    except Exception as e:
        print(f"❌ Ошибка импорта: {e}")
        return False
    
    test_user_id = 54321
    
    print(f"\n1️⃣ Тестируем has_access_redis({test_user_id}):")
    result1 = has_access_redis(test_user_id)
    print(f"   Результат: {result1}")
    
    print(f"\n2️⃣ Тестируем add_user_redis({test_user_id}):")
    user_data = {
        'telegram_id': test_user_id,
        'is_active': True,
        'role': 'premium'
    }
    result2 = add_user_redis(test_user_id, user_data)
    print(f"   Результат: {result2}")
    
    print(f"\n3️⃣ Проверяем доступ после добавления:")
    result3 = has_access_redis(test_user_id)
    print(f"   Результат: {result3}")
    
    print(f"\n4️⃣ Тестируем remove_user_redis({test_user_id}):")
    result4 = remove_user_redis(test_user_id)
    print(f"   Результат: {result4}")
    
    print(f"\n5️⃣ Проверяем доступ после удаления:")
    result5 = has_access_redis(test_user_id)
    print(f"   Результат: {result5}")
    
    success = (result1 == False and result2 == True and result3 == True and result4 == True and result5 == False)
    
    if success:
        print("\n🎉 ГЛОБАЛЬНЫЕ ФУНКЦИИ РАБОТАЮТ!")
        return True
    else:
        print("\n❌ ПРОБЛЕМЫ С ГЛОБАЛЬНЫМИ ФУНКЦИЯМИ!")
        return False

if __name__ == "__main__":
    print("🚀 ЗАПУСК ТЕСТОВ REDIS С FAKE REDIS")
    print("=" * 80)
    
    test1_passed = test_redis_with_fake()
    test2_passed = test_global_functions()
    
    print("\n" + "=" * 80)
    print("🏁 ИТОГОВЫЕ РЕЗУЛЬТАТЫ:")
    print(f"   Базовый тест: {'✅' if test1_passed else '❌'}")
    print(f"   Глобальные функции: {'✅' if test2_passed else '❌'}")
    
    if test1_passed and test2_passed:
        print("\n🎉 ВСЕ ТЕСТЫ REDIS ПРОШЛИ УСПЕШНО!")
        print("💡 Теперь можно интегрировать Redis в проект!")
        exit(0)
    else:
        print("\n❌ НЕКОТОРЫЕ ТЕСТЫ НЕ ПРОШЛИ!")
        exit(1) 
"""
Тест Redis синхронизации с FakeRedis
"""

import sys
import os
import time
from datetime import datetime

# Добавляем пути
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def test_redis_with_fake():
    print("🧪 ТЕСТИРОВАНИЕ REDIS С FAKE REDIS")
    print("=" * 60)
    
    # Импортируем систему
    try:
        from redis_access_sync import RedisAccessSync, get_redis_sync
        print("✅ Импорт redis_access_sync успешен")
    except Exception as e:
        print(f"❌ Ошибка импорта: {e}")
        return False
    
    # Создаем экземпляр
    try:
        sync = RedisAccessSync()
        print("✅ RedisAccessSync создан")
    except Exception as e:
        print(f"❌ Ошибка создания: {e}")
        return False
    
    # Тестируем базовые операции
    test_user_id = 12345
    
    print(f"\n1️⃣ Проверяем начальное состояние пользователя {test_user_id}:")
    has_access_initial = sync.has_access(test_user_id)
    print(f"   has_access({test_user_id}): {has_access_initial}")
    
    print(f"\n2️⃣ Добавляем пользователя {test_user_id}:")
    user_data = {
        'telegram_id': test_user_id,
        'is_active': True,
        'subscription_end': (datetime.now()).isoformat(),
        'role': 'trial'
    }
    add_result = sync.add_user(test_user_id, user_data)
    print(f"   add_user({test_user_id}): {add_result}")
    
    # Проверяем после добавления
    has_access_after_add = sync.has_access(test_user_id)
    print(f"   has_access({test_user_id}) после добавления: {has_access_after_add}")
    
    print(f"\n3️⃣ Удаляем пользователя {test_user_id}:")
    remove_result = sync.remove_user(test_user_id)
    print(f"   remove_user({test_user_id}): {remove_result}")
    
    # Проверяем после удаления
    has_access_after_remove = sync.has_access(test_user_id)
    print(f"   has_access({test_user_id}) после удаления: {has_access_after_remove}")
    
    print(f"\n4️⃣ Статистика:")
    stats = sync.get_stats()
    print(f"   {stats}")
    
    # Проверяем результаты
    success = (
        has_access_initial == False and
        add_result == True and
        has_access_after_add == True and
        remove_result == True and
        has_access_after_remove == False
    )
    
    if success:
        print("\n🎉 ВСЕ ТЕСТЫ ПРОШЛИ УСПЕШНО!")
        return True
    else:
        print("\n❌ НЕКОТОРЫЕ ТЕСТЫ НЕ ПРОШЛИ!")
        return False

def test_global_functions():
    print("\n🔧 ТЕСТИРОВАНИЕ ГЛОБАЛЬНЫХ ФУНКЦИЙ")
    print("=" * 60)
    
    # Импортируем глобальные функции
    try:
        from redis_access_sync import has_access_redis, add_user_redis, remove_user_redis
        print("✅ Импорт глобальных функций успешен")
    except Exception as e:
        print(f"❌ Ошибка импорта: {e}")
        return False
    
    test_user_id = 54321
    
    print(f"\n1️⃣ Тестируем has_access_redis({test_user_id}):")
    result1 = has_access_redis(test_user_id)
    print(f"   Результат: {result1}")
    
    print(f"\n2️⃣ Тестируем add_user_redis({test_user_id}):")
    user_data = {
        'telegram_id': test_user_id,
        'is_active': True,
        'role': 'premium'
    }
    result2 = add_user_redis(test_user_id, user_data)
    print(f"   Результат: {result2}")
    
    print(f"\n3️⃣ Проверяем доступ после добавления:")
    result3 = has_access_redis(test_user_id)
    print(f"   Результат: {result3}")
    
    print(f"\n4️⃣ Тестируем remove_user_redis({test_user_id}):")
    result4 = remove_user_redis(test_user_id)
    print(f"   Результат: {result4}")
    
    print(f"\n5️⃣ Проверяем доступ после удаления:")
    result5 = has_access_redis(test_user_id)
    print(f"   Результат: {result5}")
    
    success = (result1 == False and result2 == True and result3 == True and result4 == True and result5 == False)
    
    if success:
        print("\n🎉 ГЛОБАЛЬНЫЕ ФУНКЦИИ РАБОТАЮТ!")
        return True
    else:
        print("\n❌ ПРОБЛЕМЫ С ГЛОБАЛЬНЫМИ ФУНКЦИЯМИ!")
        return False

if __name__ == "__main__":
    print("🚀 ЗАПУСК ТЕСТОВ REDIS С FAKE REDIS")
    print("=" * 80)
    
    test1_passed = test_redis_with_fake()
    test2_passed = test_global_functions()
    
    print("\n" + "=" * 80)
    print("🏁 ИТОГОВЫЕ РЕЗУЛЬТАТЫ:")
    print(f"   Базовый тест: {'✅' if test1_passed else '❌'}")
    print(f"   Глобальные функции: {'✅' if test2_passed else '❌'}")
    
    if test1_passed and test2_passed:
        print("\n🎉 ВСЕ ТЕСТЫ REDIS ПРОШЛИ УСПЕШНО!")
        print("💡 Теперь можно интегрировать Redis в проект!")
        exit(0)
    else:
        print("\n❌ НЕКОТОРЫЕ ТЕСТЫ НЕ ПРОШЛИ!")
        exit(1) 
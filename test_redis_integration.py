#!/usr/bin/env python3
"""
Тест интеграции Redis с access_manager
"""

import sys
import os
import time

# Добавляем пути
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def test_integration():
    print("🚀 ТЕСТИРОВАНИЕ ИНТЕГРАЦИИ REDIS С ACCESS_MANAGER")
    print("=" * 80)
    
    # Импортируем access_manager
    try:
        from utils.access_manager import has_access, add_user_access, remove_user_access, delete_user_completely, force_sync_access
        print("✅ Импорт access_manager успешен")
    except Exception as e:
        print(f"❌ Ошибка импорта: {e}")
        return False
    
    test_user_id = 99999
    
    print(f"\n1️⃣ Проверяем начальное состояние пользователя {test_user_id}:")
    initial_access = has_access(test_user_id)
    print(f"   has_access({test_user_id}): {initial_access}")
    
    print(f"\n2️⃣ Добавляем пользователя {test_user_id}:")
    add_result = add_user_access(test_user_id)
    print(f"   add_user_access({test_user_id}): {add_result}")
    
    print(f"\n3️⃣ Проверяем доступ после добавления:")
    access_after_add = has_access(test_user_id)
    print(f"   has_access({test_user_id}): {access_after_add}")
    
    print(f"\n4️⃣ Удаляем пользователя {test_user_id}:")
    remove_result = remove_user_access(test_user_id)
    print(f"   remove_user_access({test_user_id}): {remove_result}")
    
    print(f"\n5️⃣ Проверяем доступ после удаления:")
    access_after_remove = has_access(test_user_id)
    print(f"   has_access({test_user_id}): {access_after_remove}")
    
    print(f"\n6️⃣ Принудительная синхронизация:")
    sync_result = force_sync_access()
    print(f"   force_sync_access(): {sync_result}")
    
    # Проверяем результаты
    success = (
        initial_access == False and
        add_result == True and
        access_after_add == True and
        remove_result == True and
        access_after_remove == False and
        sync_result == True
    )
    
    if success:
        print("\n🎉 ВСЕ ТЕСТЫ ИНТЕГРАЦИИ ПРОШЛИ УСПЕШНО!")
        return True
    else:
        print("\n❌ НЕКОТОРЫЕ ТЕСТЫ ИНТЕГРАЦИИ НЕ ПРОШЛИ!")
        print(f"   Детали: initial={initial_access}, add={add_result}, after_add={access_after_add}")
        print(f"           remove={remove_result}, after_remove={access_after_remove}, sync={sync_result}")
        return False

def test_multiple_users():
    print("\n🔧 ТЕСТИРОВАНИЕ НЕСКОЛЬКИХ ПОЛЬЗОВАТЕЛЕЙ")
    print("=" * 80)
    
    from utils.access_manager import has_access, add_user_access, remove_user_access
    
    users = [11111, 22222, 33333]
    
    print(f"📊 Добавляем {len(users)} пользователей:")
    for user_id in users:
        result = add_user_access(user_id)
        print(f"   add_user_access({user_id}): {result}")
    
    print(f"\n📊 Проверяем доступ всех пользователей:")
    all_have_access = True
    for user_id in users:
        access = has_access(user_id)
        print(f"   has_access({user_id}): {access}")
        if not access:
            all_have_access = False
    
    print(f"\n📊 Удаляем всех пользователей:")
    for user_id in users:
        result = remove_user_access(user_id)
        print(f"   remove_user_access({user_id}): {result}")
    
    print(f"\n📊 Проверяем, что доступ удален:")
    all_removed = True
    for user_id in users:
        access = has_access(user_id)
        print(f"   has_access({user_id}): {access}")
        if access:
            all_removed = False
    
    success = all_have_access and all_removed
    
    if success:
        print("\n🎉 ТЕСТ НЕСКОЛЬКИХ ПОЛЬЗОВАТЕЛЕЙ ПРОШЕЛ!")
        return True
    else:
        print("\n❌ ТЕСТ НЕСКОЛЬКИХ ПОЛЬЗОВАТЕЛЕЙ НЕ ПРОШЕЛ!")
        return False

if __name__ == "__main__":
    print("🚀 ЗАПУСК ТЕСТОВ ИНТЕГРАЦИИ REDIS")
    print("=" * 80)
    
    test1_passed = test_integration()
    test2_passed = test_multiple_users()
    
    print("\n" + "=" * 80)
    print("🏁 ИТОГОВЫЕ РЕЗУЛЬТАТЫ:")
    print(f"   Базовая интеграция: {'✅' if test1_passed else '❌'}")
    print(f"   Несколько пользователей: {'✅' if test2_passed else '❌'}")
    
    if test1_passed and test2_passed:
        print("\n🎉 ВСЯ ИНТЕГРАЦИЯ REDIS РАБОТАЕТ!")
        print("💡 Теперь можно запускать боты с Redis!")
        exit(0)
    else:
        print("\n❌ ИНТЕГРАЦИЯ ИМЕЕТ ПРОБЛЕМЫ!")
        exit(1) 
"""
Тест интеграции Redis с access_manager
"""

import sys
import os
import time

# Добавляем пути
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def test_integration():
    print("🚀 ТЕСТИРОВАНИЕ ИНТЕГРАЦИИ REDIS С ACCESS_MANAGER")
    print("=" * 80)
    
    # Импортируем access_manager
    try:
        from utils.access_manager import has_access, add_user_access, remove_user_access, delete_user_completely, force_sync_access
        print("✅ Импорт access_manager успешен")
    except Exception as e:
        print(f"❌ Ошибка импорта: {e}")
        return False
    
    test_user_id = 99999
    
    print(f"\n1️⃣ Проверяем начальное состояние пользователя {test_user_id}:")
    initial_access = has_access(test_user_id)
    print(f"   has_access({test_user_id}): {initial_access}")
    
    print(f"\n2️⃣ Добавляем пользователя {test_user_id}:")
    add_result = add_user_access(test_user_id)
    print(f"   add_user_access({test_user_id}): {add_result}")
    
    print(f"\n3️⃣ Проверяем доступ после добавления:")
    access_after_add = has_access(test_user_id)
    print(f"   has_access({test_user_id}): {access_after_add}")
    
    print(f"\n4️⃣ Удаляем пользователя {test_user_id}:")
    remove_result = remove_user_access(test_user_id)
    print(f"   remove_user_access({test_user_id}): {remove_result}")
    
    print(f"\n5️⃣ Проверяем доступ после удаления:")
    access_after_remove = has_access(test_user_id)
    print(f"   has_access({test_user_id}): {access_after_remove}")
    
    print(f"\n6️⃣ Принудительная синхронизация:")
    sync_result = force_sync_access()
    print(f"   force_sync_access(): {sync_result}")
    
    # Проверяем результаты
    success = (
        initial_access == False and
        add_result == True and
        access_after_add == True and
        remove_result == True and
        access_after_remove == False and
        sync_result == True
    )
    
    if success:
        print("\n🎉 ВСЕ ТЕСТЫ ИНТЕГРАЦИИ ПРОШЛИ УСПЕШНО!")
        return True
    else:
        print("\n❌ НЕКОТОРЫЕ ТЕСТЫ ИНТЕГРАЦИИ НЕ ПРОШЛИ!")
        print(f"   Детали: initial={initial_access}, add={add_result}, after_add={access_after_add}")
        print(f"           remove={remove_result}, after_remove={access_after_remove}, sync={sync_result}")
        return False

def test_multiple_users():
    print("\n🔧 ТЕСТИРОВАНИЕ НЕСКОЛЬКИХ ПОЛЬЗОВАТЕЛЕЙ")
    print("=" * 80)
    
    from utils.access_manager import has_access, add_user_access, remove_user_access
    
    users = [11111, 22222, 33333]
    
    print(f"📊 Добавляем {len(users)} пользователей:")
    for user_id in users:
        result = add_user_access(user_id)
        print(f"   add_user_access({user_id}): {result}")
    
    print(f"\n📊 Проверяем доступ всех пользователей:")
    all_have_access = True
    for user_id in users:
        access = has_access(user_id)
        print(f"   has_access({user_id}): {access}")
        if not access:
            all_have_access = False
    
    print(f"\n📊 Удаляем всех пользователей:")
    for user_id in users:
        result = remove_user_access(user_id)
        print(f"   remove_user_access({user_id}): {result}")
    
    print(f"\n📊 Проверяем, что доступ удален:")
    all_removed = True
    for user_id in users:
        access = has_access(user_id)
        print(f"   has_access({user_id}): {access}")
        if access:
            all_removed = False
    
    success = all_have_access and all_removed
    
    if success:
        print("\n🎉 ТЕСТ НЕСКОЛЬКИХ ПОЛЬЗОВАТЕЛЕЙ ПРОШЕЛ!")
        return True
    else:
        print("\n❌ ТЕСТ НЕСКОЛЬКИХ ПОЛЬЗОВАТЕЛЕЙ НЕ ПРОШЕЛ!")
        return False

if __name__ == "__main__":
    print("🚀 ЗАПУСК ТЕСТОВ ИНТЕГРАЦИИ REDIS")
    print("=" * 80)
    
    test1_passed = test_integration()
    test2_passed = test_multiple_users()
    
    print("\n" + "=" * 80)
    print("🏁 ИТОГОВЫЕ РЕЗУЛЬТАТЫ:")
    print(f"   Базовая интеграция: {'✅' if test1_passed else '❌'}")
    print(f"   Несколько пользователей: {'✅' if test2_passed else '❌'}")
    
    if test1_passed and test2_passed:
        print("\n🎉 ВСЯ ИНТЕГРАЦИЯ REDIS РАБОТАЕТ!")
        print("💡 Теперь можно запускать боты с Redis!")
        exit(0)
    else:
        print("\n❌ ИНТЕГРАЦИЯ ИМЕЕТ ПРОБЛЕМЫ!")
        exit(1) 
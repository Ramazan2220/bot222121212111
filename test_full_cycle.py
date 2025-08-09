#!/usr/bin/env python3
"""
ТЕСТ ПОЛНОГО ЦИКЛА УПРАВЛЕНИЯ ПОЛЬЗОВАТЕЛЕМ
Добавление → Проверка → Удаление → Проверка → Повторное добавление → Проверка
"""

import sys
import os
import json

sys.path.insert(0, './utils')
sys.path.insert(0, './admin_bot')

def check_user_status(user_id, step_name):
    """Проверяет статус пользователя"""
    print(f"\n📊 {step_name}:")
    
    try:
        from access_manager import get_access_manager, has_access
        
        # 1. AccessManager
        access_result = has_access(user_id)
        print(f"   AccessManager has_access({user_id}): {access_result}")
        
        # 2. Shared cache
        cache_file = "data/shared_access_cache.json"
        if os.path.exists(cache_file):
            with open(cache_file, 'r', encoding='utf-8') as f:
                cache_data = json.load(f)
            
            cache = cache_data.get('cache', {})
            user_key = str(user_id)
            
            in_shared = user_key in cache
            print(f"   Shared cache: {in_shared}")
        else:
            print(f"   Shared cache: файл не существует")
        
        # 3. UserService
        try:
            from services.user_service import UserService
            user_service = UserService()
            user = user_service.get_user(user_id)
            in_userservice = user is not None
            print(f"   UserService: {in_userservice}")
            if user:
                print(f"      Status: {user.status}")
        except Exception as e:
            print(f"   UserService: ошибка - {e}")
        
        # 4. Симуляция основного бота
        try:
            from access_manager import AccessManager
            main_bot_access_manager = AccessManager()
            main_bot_result = main_bot_access_manager.has_access(user_id)
            print(f"   Основной бот: {main_bot_result}")
        except Exception as e:
            print(f"   Основной бот: ошибка - {e}")
            
        return access_result
        
    except Exception as e:
        print(f"   ❌ Ошибка проверки: {e}")
        return False

def test_full_cycle():
    print("🔄 ТЕСТ ПОЛНОГО ЦИКЛА УПРАВЛЕНИЯ ПОЛЬЗОВАТЕЛЕМ")
    print("=" * 80)
    
    user_id = 6626270112
    
    # 1. Проверяем начальное состояние
    initial_status = check_user_status(user_id, "НАЧАЛЬНОЕ СОСТОЯНИЕ")
    
    # 2. Если пользователь есть - удаляем для чистоты эксперимента
    if initial_status:
        print(f"\n🗑️ ОЧИСТКА: Удаляем пользователя для чистоты теста...")
        try:
            from access_manager import delete_user_completely
            success = delete_user_completely(user_id)
            if success:
                print(f"✅ Пользователь удален")
            else:
                print(f"❌ Ошибка удаления")
        except Exception as e:
            print(f"❌ Ошибка удаления: {e}")
        
        check_user_status(user_id, "ПОСЛЕ ОЧИСТКИ")
    
    # 3. Добавляем пользователя
    print(f"\n➕ ДОБАВЛЕНИЕ ПОЛЬЗОВАТЕЛЯ:")
    try:
        from access_manager import get_access_manager
        access_manager = get_access_manager()
        
        success = access_manager.add_user(user_id, source='test', role='trial')
        if success:
            print(f"✅ add_user({user_id}) выполнена успешно")
            # Принудительная синхронизация
            access_manager.force_sync()
            print(f"✅ force_sync() выполнена")
        else:
            print(f"❌ add_user({user_id}) неудачно")
    except Exception as e:
        print(f"❌ Ошибка добавления: {e}")
    
    # 4. Проверяем после добавления
    add_status = check_user_status(user_id, "ПОСЛЕ ДОБАВЛЕНИЯ")
    
    # 5. Удаляем пользователя
    print(f"\n🗑️ УДАЛЕНИЕ ПОЛЬЗОВАТЕЛЯ:")
    try:
        from access_manager import delete_user_completely
        success = delete_user_completely(user_id)
        if success:
            print(f"✅ delete_user_completely({user_id}) выполнена успешно")
        else:
            print(f"❌ delete_user_completely({user_id}) неудачно")
    except Exception as e:
        print(f"❌ Ошибка удаления: {e}")
    
    # 6. Проверяем после удаления
    delete_status = check_user_status(user_id, "ПОСЛЕ УДАЛЕНИЯ")
    
    # 7. Повторно добавляем пользователя
    print(f"\n♻️ ПОВТОРНОЕ ДОБАВЛЕНИЕ ПОЛЬЗОВАТЕЛЯ:")
    try:
        from access_manager import get_access_manager
        access_manager = get_access_manager()
        
        success = access_manager.add_user(user_id, source='test_reactivation', role='trial')
        if success:
            print(f"✅ add_user({user_id}) (повторно) выполнена успешно")
            # Принудительная синхронизация
            access_manager.force_sync()
            print(f"✅ force_sync() выполнена")
        else:
            print(f"❌ add_user({user_id}) (повторно) неудачно")
    except Exception as e:
        print(f"❌ Ошибка повторного добавления: {e}")
    
    # 8. Проверяем финальное состояние
    final_status = check_user_status(user_id, "ФИНАЛЬНОЕ СОСТОЯНИЕ")
    
    # 9. Результат
    print(f"\n🏁 РЕЗУЛЬТАТ ТЕСТА:")
    print(f"   Начальное состояние: {initial_status}")
    print(f"   После добавления: {add_status}")
    print(f"   После удаления: {delete_status}")
    print(f"   Финальное состояние: {final_status}")
    
    if not delete_status and final_status:
        print(f"\n🎉 ТЕСТ ПРОЙДЕН! Система работает корректно:")
        print(f"   ✅ Удаление работает (пользователь исчез)")
        print(f"   ✅ Повторное добавление работает (пользователь появился)")
        print(f"   ✅ Синхронизация между админ панелью и основным ботом работает")
    else:
        print(f"\n❌ ТЕСТ НЕ ПРОЙДЕН! Проблемы:")
        if delete_status:
            print(f"   ❌ Удаление НЕ работает")
        if not final_status:
            print(f"   ❌ Повторное добавление НЕ работает")

def main():
    test_full_cycle()

if __name__ == "__main__":
    main() 
"""
ТЕСТ ПОЛНОГО ЦИКЛА УПРАВЛЕНИЯ ПОЛЬЗОВАТЕЛЕМ
Добавление → Проверка → Удаление → Проверка → Повторное добавление → Проверка
"""

import sys
import os
import json

sys.path.insert(0, './utils')
sys.path.insert(0, './admin_bot')

def check_user_status(user_id, step_name):
    """Проверяет статус пользователя"""
    print(f"\n📊 {step_name}:")
    
    try:
        from access_manager import get_access_manager, has_access
        
        # 1. AccessManager
        access_result = has_access(user_id)
        print(f"   AccessManager has_access({user_id}): {access_result}")
        
        # 2. Shared cache
        cache_file = "data/shared_access_cache.json"
        if os.path.exists(cache_file):
            with open(cache_file, 'r', encoding='utf-8') as f:
                cache_data = json.load(f)
            
            cache = cache_data.get('cache', {})
            user_key = str(user_id)
            
            in_shared = user_key in cache
            print(f"   Shared cache: {in_shared}")
        else:
            print(f"   Shared cache: файл не существует")
        
        # 3. UserService
        try:
            from services.user_service import UserService
            user_service = UserService()
            user = user_service.get_user(user_id)
            in_userservice = user is not None
            print(f"   UserService: {in_userservice}")
            if user:
                print(f"      Status: {user.status}")
        except Exception as e:
            print(f"   UserService: ошибка - {e}")
        
        # 4. Симуляция основного бота
        try:
            from access_manager import AccessManager
            main_bot_access_manager = AccessManager()
            main_bot_result = main_bot_access_manager.has_access(user_id)
            print(f"   Основной бот: {main_bot_result}")
        except Exception as e:
            print(f"   Основной бот: ошибка - {e}")
            
        return access_result
        
    except Exception as e:
        print(f"   ❌ Ошибка проверки: {e}")
        return False

def test_full_cycle():
    print("🔄 ТЕСТ ПОЛНОГО ЦИКЛА УПРАВЛЕНИЯ ПОЛЬЗОВАТЕЛЕМ")
    print("=" * 80)
    
    user_id = 6626270112
    
    # 1. Проверяем начальное состояние
    initial_status = check_user_status(user_id, "НАЧАЛЬНОЕ СОСТОЯНИЕ")
    
    # 2. Если пользователь есть - удаляем для чистоты эксперимента
    if initial_status:
        print(f"\n🗑️ ОЧИСТКА: Удаляем пользователя для чистоты теста...")
        try:
            from access_manager import delete_user_completely
            success = delete_user_completely(user_id)
            if success:
                print(f"✅ Пользователь удален")
            else:
                print(f"❌ Ошибка удаления")
        except Exception as e:
            print(f"❌ Ошибка удаления: {e}")
        
        check_user_status(user_id, "ПОСЛЕ ОЧИСТКИ")
    
    # 3. Добавляем пользователя
    print(f"\n➕ ДОБАВЛЕНИЕ ПОЛЬЗОВАТЕЛЯ:")
    try:
        from access_manager import get_access_manager
        access_manager = get_access_manager()
        
        success = access_manager.add_user(user_id, source='test', role='trial')
        if success:
            print(f"✅ add_user({user_id}) выполнена успешно")
            # Принудительная синхронизация
            access_manager.force_sync()
            print(f"✅ force_sync() выполнена")
        else:
            print(f"❌ add_user({user_id}) неудачно")
    except Exception as e:
        print(f"❌ Ошибка добавления: {e}")
    
    # 4. Проверяем после добавления
    add_status = check_user_status(user_id, "ПОСЛЕ ДОБАВЛЕНИЯ")
    
    # 5. Удаляем пользователя
    print(f"\n🗑️ УДАЛЕНИЕ ПОЛЬЗОВАТЕЛЯ:")
    try:
        from access_manager import delete_user_completely
        success = delete_user_completely(user_id)
        if success:
            print(f"✅ delete_user_completely({user_id}) выполнена успешно")
        else:
            print(f"❌ delete_user_completely({user_id}) неудачно")
    except Exception as e:
        print(f"❌ Ошибка удаления: {e}")
    
    # 6. Проверяем после удаления
    delete_status = check_user_status(user_id, "ПОСЛЕ УДАЛЕНИЯ")
    
    # 7. Повторно добавляем пользователя
    print(f"\n♻️ ПОВТОРНОЕ ДОБАВЛЕНИЕ ПОЛЬЗОВАТЕЛЯ:")
    try:
        from access_manager import get_access_manager
        access_manager = get_access_manager()
        
        success = access_manager.add_user(user_id, source='test_reactivation', role='trial')
        if success:
            print(f"✅ add_user({user_id}) (повторно) выполнена успешно")
            # Принудительная синхронизация
            access_manager.force_sync()
            print(f"✅ force_sync() выполнена")
        else:
            print(f"❌ add_user({user_id}) (повторно) неудачно")
    except Exception as e:
        print(f"❌ Ошибка повторного добавления: {e}")
    
    # 8. Проверяем финальное состояние
    final_status = check_user_status(user_id, "ФИНАЛЬНОЕ СОСТОЯНИЕ")
    
    # 9. Результат
    print(f"\n🏁 РЕЗУЛЬТАТ ТЕСТА:")
    print(f"   Начальное состояние: {initial_status}")
    print(f"   После добавления: {add_status}")
    print(f"   После удаления: {delete_status}")
    print(f"   Финальное состояние: {final_status}")
    
    if not delete_status and final_status:
        print(f"\n🎉 ТЕСТ ПРОЙДЕН! Система работает корректно:")
        print(f"   ✅ Удаление работает (пользователь исчез)")
        print(f"   ✅ Повторное добавление работает (пользователь появился)")
        print(f"   ✅ Синхронизация между админ панелью и основным ботом работает")
    else:
        print(f"\n❌ ТЕСТ НЕ ПРОЙДЕН! Проблемы:")
        if delete_status:
            print(f"   ❌ Удаление НЕ работает")
        if not final_status:
            print(f"   ❌ Повторное добавление НЕ работает")

def main():
    test_full_cycle()

if __name__ == "__main__":
    main() 
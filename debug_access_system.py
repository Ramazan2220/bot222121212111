#!/usr/bin/env python3
"""
Полная диагностика системы управления доступами
"""

import sys
import os
from datetime import datetime

# Добавляем пути для импортов
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)
sys.path.insert(0, os.path.join(current_dir, 'admin_bot'))
sys.path.insert(0, os.path.join(current_dir, 'utils'))

def test_access_system():
    """Комплексная диагностика системы доступов"""
    
    print("🔍 ДИАГНОСТИКА СИСТЕМЫ ДОСТУПОВ")
    print("=" * 60)
    
    test_user_id = 6626270112
    
    # 1. Проверяем импорты
    print("\n1️⃣ ПРОВЕРКА ИМПОРТОВ:")
    try:
        # Прямые импорты
        sys.path.insert(0, current_dir)
        import access_manager
        from access_manager import get_access_manager, has_access
        
        sys.path.insert(0, os.path.join(current_dir, 'admin_bot'))
        from services.user_service import UserService
        from models.user import User, UserStatus, SubscriptionPlan
        print("   ✅ Все импорты успешны")
    except Exception as e:
        print(f"   ❌ Ошибка импорта: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # 2. Проверяем базу данных админ панели
    print("\n2️⃣ ПРОВЕРКА БАЗЫ АДМИН ПАНЕЛИ:")
    try:
        user_service = UserService()
        user = user_service.get_user(test_user_id)
        if user:
            print(f"   ✅ Пользователь найден в БД админ панели")
            print(f"      ID: {user.telegram_id}")
            print(f"      Username: {user.username}")
            print(f"      План: {user.subscription_plan}")
            print(f"      Статус: {user.status}")
            print(f"      Активен: {user.is_active}")
            print(f"      Окончание: {user.subscription_end}")
        else:
            print(f"   ❌ Пользователь НЕ найден в БД админ панели")
    except Exception as e:
        print(f"   ❌ Ошибка проверки БД: {e}")
    
    # 3. Проверяем AccessManager
    print("\n3️⃣ ПРОВЕРКА ACCESS MANAGER:")
    try:
        access_manager = get_access_manager()
        
        # Принудительная синхронизация
        access_manager.force_sync()
        print("   ✅ Принудительная синхронизация выполнена")
        
        # Проверяем has_access
        has_access_result = access_manager.has_access(test_user_id)
        print(f"   📊 has_access({test_user_id}): {has_access_result}")
        
        # Получаем информацию о пользователе
        user_info = access_manager.get_user_info(test_user_id)
        if user_info:
            print("   📋 Информация о пользователе:")
            for key, value in user_info.items():
                print(f"      {key}: {value}")
        else:
            print("   ❌ Информация о пользователе не найдена")
            
    except Exception as e:
        print(f"   ❌ Ошибка AccessManager: {e}")
    
    # 4. Проверяем функцию has_access напрямую
    print("\n4️⃣ ПРОВЕРКА ФУНКЦИИ has_access:")
    try:
        direct_access = has_access(test_user_id)
        print(f"   📊 has_access({test_user_id}) напрямую: {direct_access}")
    except Exception as e:
        print(f"   ❌ Ошибка прямого вызова: {e}")
    
    # 5. Проверяем config.py
    print("\n5️⃣ ПРОВЕРКА CONFIG.PY:")
    try:
        import config
        admin_user_ids = getattr(config, 'ADMIN_USER_IDS', [])
        super_admin_user_ids = getattr(config, 'SUPER_ADMIN_USER_IDS', [])
        print(f"   📊 ADMIN_USER_IDS: {admin_user_ids}")
        print(f"   📊 SUPER_ADMIN_USER_IDS: {super_admin_user_ids}")
        print(f"   📊 Пользователь {test_user_id} в config.py: {test_user_id in admin_user_ids or test_user_id in super_admin_user_ids}")
    except Exception as e:
        print(f"   ❌ Ошибка проверки config.py: {e}")
    
    # 6. Проверяем кеш
    print("\n6️⃣ ПРОВЕРКА КЕША:")
    try:
        access_manager = get_access_manager()
        
        # Проверяем внутренний кеш (если доступен)
        if hasattr(access_manager, '_access_cache'):
            cache = access_manager._access_cache
            print(f"   📊 Размер кеша: {len(cache)}")
            user_key = str(test_user_id)
            if user_key in cache:
                print(f"   ✅ Пользователь найден в кеше:")
                cache_data = cache[user_key]
                for key, value in cache_data.items():
                    print(f"      {key}: {value}")
            else:
                print(f"   ❌ Пользователь НЕ найден в кеше")
                print("   📋 Все пользователи в кеше:")
                for key, data in cache.items():
                    print(f"      {key}: {data.get('is_active', 'N/A')}")
        else:
            print("   ⚠️ Кеш недоступен")
    except Exception as e:
        print(f"   ❌ Ошибка проверки кеша: {e}")
    
    print("\n" + "=" * 60)
    print("🏁 ДИАГНОСТИКА ЗАВЕРШЕНА")

def add_user_test():
    """Тест добавления пользователя"""
    print("\n\n🧪 ТЕСТ ДОБАВЛЕНИЯ ПОЛЬЗОВАТЕЛЯ")
    print("=" * 60)
    
    test_user_id = 6626270112
    test_username = "lock0125"
    test_plan = "trial_3"
    
    try:
        from utils.access_manager import add_user_access
        
        print(f"Добавляем пользователя: {test_user_id}, {test_username}, {test_plan}")
        success = add_user_access(test_user_id)
        print(f"Результат добавления: {success}")
        
        if success:
            # Проверяем результат
            from utils.access_manager import has_access
            result = has_access(test_user_id)
            print(f"Проверка доступа после добавления: {result}")
        
    except Exception as e:
        print(f"❌ Ошибка теста добавления: {e}")

def remove_user_test():
    """Тест удаления пользователя"""
    print("\n\n🗑️ ТЕСТ УДАЛЕНИЯ ПОЛЬЗОВАТЕЛЯ")
    print("=" * 60)
    
    test_user_id = 6626270112
    
    try:
        from utils.access_manager import delete_user_completely, has_access
        
        print(f"Удаляем пользователя: {test_user_id}")
        success = delete_user_completely(test_user_id)
        print(f"Результат удаления: {success}")
        
        if success:
            # Проверяем результат
            result = has_access(test_user_id)
            print(f"Проверка доступа после удаления: {result}")
        
    except Exception as e:
        print(f"❌ Ошибка теста удаления: {e}")

if __name__ == "__main__":
    # Запускаем диагностику
    test_access_system()
    
    # Запрашиваем у пользователя что делать дальше
    print("\n🔧 ВОЗМОЖНЫЕ ДЕЙСТВИЯ:")
    print("1. Добавить пользователя 6626270112")
    print("2. Удалить пользователя 6626270112")
    print("3. Повторить диагностику")
    print("4. Выход")
    
    choice = input("\nВыберите действие (1-4): ").strip()
    
    if choice == "1":
        add_user_test()
        print("\n🔄 Повторная диагностика после добавления:")
        test_access_system()
    elif choice == "2":
        remove_user_test()
        print("\n🔄 Повторная диагностика после удаления:")
        test_access_system()
    elif choice == "3":
        test_access_system()
    else:
        print("👋 Выход") 
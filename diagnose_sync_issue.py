#!/usr/bin/env python3
"""
ДИАГНОСТИКА ПРОБЛЕМЫ СИНХРОНИЗАЦИИ
Проверяем где именно хранятся данные и почему они не синхронизируются
"""

import sys
import os
import json

sys.path.insert(0, './utils')
sys.path.insert(0, './admin_bot')

def diagnose_user_storage():
    """Диагностируем где хранятся данные пользователей"""
    
    print("🔍 ДИАГНОСТИКА ХРАНЕНИЯ ДАННЫХ ПОЛЬЗОВАТЕЛЕЙ")
    print("=" * 80)
    
    user_id = 6626270112
    
    # 1. Проверяем UserService (admin_bot/data/users.json)
    print("\n1️⃣ USERSERVICE (admin_bot/data/users.json):")
    try:
        userservice_file = "admin_bot/data/users.json"
        if os.path.exists(userservice_file):
            with open(userservice_file, 'r', encoding='utf-8') as f:
                userservice_data = json.load(f)
            
            user_key = str(user_id)
            if user_key in userservice_data:
                user_data = userservice_data[user_key]
                print(f"✅ НАЙДЕН в UserService:")
                print(f"   Username: {user_data.get('username', 'N/A')}")
                print(f"   Status: {user_data.get('status', 'N/A')}")
                print(f"   Subscription: {user_data.get('subscription_plan', 'N/A')}")
                print(f"   End Date: {user_data.get('subscription_end', 'N/A')}")
            else:
                print(f"❌ НЕТ в UserService")
                print(f"📄 В файле найдены пользователи: {list(userservice_data.keys())}")
        else:
            print(f"❌ Файл {userservice_file} не существует")
    except Exception as e:
        print(f"❌ Ошибка чтения UserService: {e}")
    
    # 2. Проверяем AccessManager кеш
    print(f"\n2️⃣ ACCESSMANAGER CACHE:")
    try:
        from access_manager import get_access_manager, has_access
        
        access_manager = get_access_manager()
        
        # Проверяем has_access
        access_result = has_access(user_id)
        print(f"📊 has_access({user_id}): {access_result}")
        
        # Проверяем кеш напрямую
        user_key = str(user_id)
        if hasattr(access_manager, '_access_cache'):
            cache = access_manager._access_cache
            if user_key in cache:
                cache_data = cache[user_key]
                print(f"✅ НАЙДЕН в кеше AccessManager:")
                print(f"   {cache_data}")
            else:
                print(f"❌ НЕТ в кеше AccessManager")
                print(f"📄 В кеше найдены пользователи: {list(cache.keys())}")
        else:
            print(f"❌ AccessManager не имеет атрибута _access_cache")
            
    except Exception as e:
        print(f"❌ Ошибка проверки AccessManager: {e}")
    
    # 3. Проверяем shared cache файл
    print(f"\n3️⃣ SHARED CACHE FILE (data/shared_access_cache.json):")
    try:
        cache_file = "data/shared_access_cache.json"
        if os.path.exists(cache_file):
            with open(cache_file, 'r', encoding='utf-8') as f:
                cache_data = json.load(f)
                
            cache = cache_data.get('cache', {})
            user_key = str(user_id)
            
            if user_key in cache:
                user_cache_data = cache[user_key]
                print(f"✅ НАЙДЕН в shared cache:")
                print(f"   {user_cache_data}")
            else:
                print(f"❌ НЕТ в shared cache")
                print(f"📄 В shared cache найдены пользователи: {list(cache.keys())}")
                
            print(f"📅 Last update: {cache_data.get('last_update', 'N/A')}")
        else:
            print(f"❌ Файл {cache_file} не существует")
    except Exception as e:
        print(f"❌ Ошибка чтения shared cache: {e}")
    
    # 4. Проверяем config.py
    print(f"\n4️⃣ CONFIG.PY ADMIN_USER_IDS:")
    try:
        import config
        admin_ids = getattr(config, 'ADMIN_USER_IDS', [])
        if user_id in admin_ids:
            print(f"✅ НАЙДЕН в config.py")
        else:
            print(f"❌ НЕТ в config.py")
            print(f"📄 В config.py админы: {admin_ids}")
    except Exception as e:
        print(f"❌ Ошибка чтения config.py: {e}")

def check_sync_flow():
    """Проверяем поток синхронизации"""
    
    print(f"\n🔄 ПРОВЕРКА ПОТОКА СИНХРОНИЗАЦИИ")
    print("=" * 80)
    
    try:
        from access_manager import get_access_manager
        
        access_manager = get_access_manager()
        
        # Проверяем методы синхронизации
        print(f"🔍 Доступные методы AccessManager:")
        methods = [method for method in dir(access_manager) if not method.startswith('_')]
        for method in methods:
            print(f"   - {method}")
        
        # Проверяем источники данных
        print(f"\n📊 ИСТОЧНИКИ ДАННЫХ:")
        
        # Config админы
        try:
            config_admins = access_manager._get_config_admin_ids()
            print(f"   Config админы: {list(config_admins)}")
        except Exception as e:
            print(f"   ❌ Ошибка получения config админов: {e}")
        
        # Админ панель пользователи
        try:
            panel_users = access_manager._get_admin_panel_users()
            print(f"   Панель пользователи: {len(panel_users)} шт")
            for user_id, user_info in panel_users.items():
                print(f"      {user_id}: {user_info}")
        except Exception as e:
            print(f"   ❌ Ошибка получения панель пользователей: {e}")
            
    except Exception as e:
        print(f"❌ Ошибка проверки синхронизации: {e}")

def main():
    print("🚨 ДИАГНОСТИКА ПРОБЛЕМЫ СИНХРОНИЗАЦИИ")
    print("=" * 80)
    
    # Диагностируем хранение данных
    diagnose_user_storage()
    
    # Проверяем поток синхронизации
    check_sync_flow()
    
    print(f"\n🏁 ДИАГНОСТИКА ЗАВЕРШЕНА")

if __name__ == "__main__":
    main() 
"""
ДИАГНОСТИКА ПРОБЛЕМЫ СИНХРОНИЗАЦИИ
Проверяем где именно хранятся данные и почему они не синхронизируются
"""

import sys
import os
import json

sys.path.insert(0, './utils')
sys.path.insert(0, './admin_bot')

def diagnose_user_storage():
    """Диагностируем где хранятся данные пользователей"""
    
    print("🔍 ДИАГНОСТИКА ХРАНЕНИЯ ДАННЫХ ПОЛЬЗОВАТЕЛЕЙ")
    print("=" * 80)
    
    user_id = 6626270112
    
    # 1. Проверяем UserService (admin_bot/data/users.json)
    print("\n1️⃣ USERSERVICE (admin_bot/data/users.json):")
    try:
        userservice_file = "admin_bot/data/users.json"
        if os.path.exists(userservice_file):
            with open(userservice_file, 'r', encoding='utf-8') as f:
                userservice_data = json.load(f)
            
            user_key = str(user_id)
            if user_key in userservice_data:
                user_data = userservice_data[user_key]
                print(f"✅ НАЙДЕН в UserService:")
                print(f"   Username: {user_data.get('username', 'N/A')}")
                print(f"   Status: {user_data.get('status', 'N/A')}")
                print(f"   Subscription: {user_data.get('subscription_plan', 'N/A')}")
                print(f"   End Date: {user_data.get('subscription_end', 'N/A')}")
            else:
                print(f"❌ НЕТ в UserService")
                print(f"📄 В файле найдены пользователи: {list(userservice_data.keys())}")
        else:
            print(f"❌ Файл {userservice_file} не существует")
    except Exception as e:
        print(f"❌ Ошибка чтения UserService: {e}")
    
    # 2. Проверяем AccessManager кеш
    print(f"\n2️⃣ ACCESSMANAGER CACHE:")
    try:
        from access_manager import get_access_manager, has_access
        
        access_manager = get_access_manager()
        
        # Проверяем has_access
        access_result = has_access(user_id)
        print(f"📊 has_access({user_id}): {access_result}")
        
        # Проверяем кеш напрямую
        user_key = str(user_id)
        if hasattr(access_manager, '_access_cache'):
            cache = access_manager._access_cache
            if user_key in cache:
                cache_data = cache[user_key]
                print(f"✅ НАЙДЕН в кеше AccessManager:")
                print(f"   {cache_data}")
            else:
                print(f"❌ НЕТ в кеше AccessManager")
                print(f"📄 В кеше найдены пользователи: {list(cache.keys())}")
        else:
            print(f"❌ AccessManager не имеет атрибута _access_cache")
            
    except Exception as e:
        print(f"❌ Ошибка проверки AccessManager: {e}")
    
    # 3. Проверяем shared cache файл
    print(f"\n3️⃣ SHARED CACHE FILE (data/shared_access_cache.json):")
    try:
        cache_file = "data/shared_access_cache.json"
        if os.path.exists(cache_file):
            with open(cache_file, 'r', encoding='utf-8') as f:
                cache_data = json.load(f)
                
            cache = cache_data.get('cache', {})
            user_key = str(user_id)
            
            if user_key in cache:
                user_cache_data = cache[user_key]
                print(f"✅ НАЙДЕН в shared cache:")
                print(f"   {user_cache_data}")
            else:
                print(f"❌ НЕТ в shared cache")
                print(f"📄 В shared cache найдены пользователи: {list(cache.keys())}")
                
            print(f"📅 Last update: {cache_data.get('last_update', 'N/A')}")
        else:
            print(f"❌ Файл {cache_file} не существует")
    except Exception as e:
        print(f"❌ Ошибка чтения shared cache: {e}")
    
    # 4. Проверяем config.py
    print(f"\n4️⃣ CONFIG.PY ADMIN_USER_IDS:")
    try:
        import config
        admin_ids = getattr(config, 'ADMIN_USER_IDS', [])
        if user_id in admin_ids:
            print(f"✅ НАЙДЕН в config.py")
        else:
            print(f"❌ НЕТ в config.py")
            print(f"📄 В config.py админы: {admin_ids}")
    except Exception as e:
        print(f"❌ Ошибка чтения config.py: {e}")

def check_sync_flow():
    """Проверяем поток синхронизации"""
    
    print(f"\n🔄 ПРОВЕРКА ПОТОКА СИНХРОНИЗАЦИИ")
    print("=" * 80)
    
    try:
        from access_manager import get_access_manager
        
        access_manager = get_access_manager()
        
        # Проверяем методы синхронизации
        print(f"🔍 Доступные методы AccessManager:")
        methods = [method for method in dir(access_manager) if not method.startswith('_')]
        for method in methods:
            print(f"   - {method}")
        
        # Проверяем источники данных
        print(f"\n📊 ИСТОЧНИКИ ДАННЫХ:")
        
        # Config админы
        try:
            config_admins = access_manager._get_config_admin_ids()
            print(f"   Config админы: {list(config_admins)}")
        except Exception as e:
            print(f"   ❌ Ошибка получения config админов: {e}")
        
        # Админ панель пользователи
        try:
            panel_users = access_manager._get_admin_panel_users()
            print(f"   Панель пользователи: {len(panel_users)} шт")
            for user_id, user_info in panel_users.items():
                print(f"      {user_id}: {user_info}")
        except Exception as e:
            print(f"   ❌ Ошибка получения панель пользователей: {e}")
            
    except Exception as e:
        print(f"❌ Ошибка проверки синхронизации: {e}")

def main():
    print("🚨 ДИАГНОСТИКА ПРОБЛЕМЫ СИНХРОНИЗАЦИИ")
    print("=" * 80)
    
    # Диагностируем хранение данных
    diagnose_user_storage()
    
    # Проверяем поток синхронизации
    check_sync_flow()
    
    print(f"\n🏁 ДИАГНОСТИКА ЗАВЕРШЕНА")

if __name__ == "__main__":
    main() 
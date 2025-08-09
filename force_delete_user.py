#!/usr/bin/env python3
"""
ПРИНУДИТЕЛЬНОЕ ПОЛНОЕ УДАЛЕНИЕ ПОЛЬЗОВАТЕЛЯ
Удаляет пользователя из всех возможных мест
"""

import sys
import os
import json

sys.path.insert(0, './utils')
sys.path.insert(0, './admin_bot')

def force_delete_user_completely(user_id: int):
    """Принудительно удаляет пользователя отовсюду"""
    
    print(f"🗑️ ПРИНУДИТЕЛЬНОЕ УДАЛЕНИЕ ПОЛЬЗОВАТЕЛЯ {user_id}")
    print("=" * 60)
    
    success_count = 0
    
    # 1. Удаляем из админ панели (UserService)
    try:
        from services.user_service import UserService
        user_service = UserService()
        
        if user_service.get_user(user_id):
            result = user_service.delete_user(user_id)
            if result:
                print(f"✅ Удален из UserService")
                success_count += 1
            else:
                print(f"❌ Ошибка удаления из UserService")
        else:
            print(f"ℹ️ Пользователь не найден в UserService")
            
    except Exception as e:
        print(f"❌ Ошибка UserService: {e}")
    
    # 2. Удаляем из AccessManager
    try:
        from access_manager import get_access_manager
        
        access_manager = get_access_manager()
        
        # Проверяем наличие в кеше
        user_info = access_manager.get_user_info(user_id)
        if user_info:
            # Удаляем из кеша напрямую
            user_key = str(user_id)
            if user_key in access_manager._access_cache:
                del access_manager._access_cache[user_key]
                print(f"✅ Удален из кеша AccessManager")
                success_count += 1
        else:
            print(f"ℹ️ Пользователь не найден в кеше AccessManager")
            
    except Exception as e:
        print(f"❌ Ошибка AccessManager: {e}")
    
    # 3. Удаляем из файла общего кеша
    try:
        cache_file = "data/shared_access_cache.json"
        if os.path.exists(cache_file):
            with open(cache_file, 'r', encoding='utf-8') as f:
                cache_data = json.load(f)
                
            cache = cache_data.get('cache', {})
            user_key = str(user_id)
            
            if user_key in cache:
                del cache[user_key]
                cache_data['cache'] = cache
                cache_data['last_update'] = time.time()
                
                with open(cache_file, 'w', encoding='utf-8') as f:
                    json.dump(cache_data, f, ensure_ascii=False, indent=2, default=str)
                    
                print(f"✅ Удален из файла общего кеша")
                success_count += 1
            else:
                print(f"ℹ️ Пользователь не найден в файле кеша")
        else:
            print(f"ℹ️ Файл общего кеша не найден")
            
    except Exception as e:
        print(f"❌ Ошибка файла кеша: {e}")
    
    # 4. Удаляем персональное хранилище пользователя
    try:
        user_data_dir = f"data/users/{user_id}"
        if os.path.exists(user_data_dir):
            import shutil
            shutil.rmtree(user_data_dir)
            print(f"✅ Удалено персональное хранилище")
            success_count += 1
        else:
            print(f"ℹ️ Персональное хранилище не найдено")
            
    except Exception as e:
        print(f"❌ Ошибка удаления хранилища: {e}")
    
    # 5. Принудительная синхронизация
    try:
        from access_manager import get_access_manager
        access_manager = get_access_manager()
        access_manager.force_sync()
        print(f"✅ Выполнена принудительная синхронизация")
        success_count += 1
    except Exception as e:
        print(f"❌ Ошибка синхронизации: {e}")
    
    print(f"\n🏁 РЕЗУЛЬТАТ: {success_count} операций выполнено")
    
    if success_count >= 3:
        print("🎉 ПОЛЬЗОВАТЕЛЬ ПОЛНОСТЬЮ УДАЛЕН!")
        return True
    else:
        print("⚠️ Удаление может быть неполным")
        return False

def verify_user_deleted(user_id: int):
    """Проверяет что пользователь действительно удален"""
    
    print(f"\n🔍 ПРОВЕРКА УДАЛЕНИЯ ПОЛЬЗОВАТЕЛЯ {user_id}")
    print("=" * 60)
    
    found_anywhere = False
    
    # Проверяем UserService
    try:
        from services.user_service import UserService
        user_service = UserService()
        user = user_service.get_user(user_id)
        if user:
            print(f"❌ НАЙДЕН в UserService: {user.username}, статус: {user.status}")
            found_anywhere = True
        else:
            print(f"✅ НЕТ в UserService")
    except Exception as e:
        print(f"❌ Ошибка проверки UserService: {e}")
    
    # Проверяем AccessManager
    try:
        from access_manager import has_access, get_access_manager
        
        access_result = has_access(user_id)
        print(f"📊 has_access({user_id}): {access_result}")
        
        if access_result:
            found_anywhere = True
            
        access_manager = get_access_manager()
        user_info = access_manager.get_user_info(user_id)
        if user_info:
            print(f"❌ НАЙДЕН в AccessManager: {user_info}")
            found_anywhere = True
        else:
            print(f"✅ НЕТ в AccessManager")
            
    except Exception as e:
        print(f"❌ Ошибка проверки AccessManager: {e}")
    
    # Проверяем файл кеша
    try:
        cache_file = "data/shared_access_cache.json"
        if os.path.exists(cache_file):
            with open(cache_file, 'r', encoding='utf-8') as f:
                cache_data = json.load(f)
                
            cache = cache_data.get('cache', {})
            user_key = str(user_id)
            
            if user_key in cache:
                print(f"❌ НАЙДЕН в файле кеша: {cache[user_key]}")
                found_anywhere = True
            else:
                print(f"✅ НЕТ в файле кеша")
        else:
            print(f"ℹ️ Файл кеша не существует")
    except Exception as e:
        print(f"❌ Ошибка проверки файла кеша: {e}")
    
    if not found_anywhere:
        print(f"\n🎉 ПОЛЬЗОВАТЕЛЬ {user_id} ПОЛНОСТЬЮ УДАЛЕН!")
        return True
    else:
        print(f"\n❌ ПОЛЬЗОВАТЕЛЬ {user_id} НАЙДЕН В СИСТЕМЕ!")
        return False

def main():
    import time
    
    print("🧹 ПРИНУДИТЕЛЬНАЯ ОЧИСТКА ПОЛЬЗОВАТЕЛЯ")
    print("=" * 80)
    
    user_id = 6626270112
    
    # 1. Проверяем текущее состояние
    print("1️⃣ ПРОВЕРКА ТЕКУЩЕГО СОСТОЯНИЯ:")
    verify_user_deleted(user_id)
    
    # 2. Принудительное удаление
    print("\n2️⃣ ПРИНУДИТЕЛЬНОЕ УДАЛЕНИЕ:")
    success = force_delete_user_completely(user_id)
    
    # 3. Проверяем результат
    print("\n3️⃣ ПРОВЕРКА РЕЗУЛЬТАТА:")
    is_deleted = verify_user_deleted(user_id)
    
    if is_deleted:
        print(f"\n✅ ГОТОВО! Теперь можно добавить пользователя {user_id} заново")
    else:
        print(f"\n❌ ПРОБЛЕМА! Пользователь {user_id} не полностью удален")

if __name__ == "__main__":
    import time
    main() 
"""
ПРИНУДИТЕЛЬНОЕ ПОЛНОЕ УДАЛЕНИЕ ПОЛЬЗОВАТЕЛЯ
Удаляет пользователя из всех возможных мест
"""

import sys
import os
import json

sys.path.insert(0, './utils')
sys.path.insert(0, './admin_bot')

def force_delete_user_completely(user_id: int):
    """Принудительно удаляет пользователя отовсюду"""
    
    print(f"🗑️ ПРИНУДИТЕЛЬНОЕ УДАЛЕНИЕ ПОЛЬЗОВАТЕЛЯ {user_id}")
    print("=" * 60)
    
    success_count = 0
    
    # 1. Удаляем из админ панели (UserService)
    try:
        from services.user_service import UserService
        user_service = UserService()
        
        if user_service.get_user(user_id):
            result = user_service.delete_user(user_id)
            if result:
                print(f"✅ Удален из UserService")
                success_count += 1
            else:
                print(f"❌ Ошибка удаления из UserService")
        else:
            print(f"ℹ️ Пользователь не найден в UserService")
            
    except Exception as e:
        print(f"❌ Ошибка UserService: {e}")
    
    # 2. Удаляем из AccessManager
    try:
        from access_manager import get_access_manager
        
        access_manager = get_access_manager()
        
        # Проверяем наличие в кеше
        user_info = access_manager.get_user_info(user_id)
        if user_info:
            # Удаляем из кеша напрямую
            user_key = str(user_id)
            if user_key in access_manager._access_cache:
                del access_manager._access_cache[user_key]
                print(f"✅ Удален из кеша AccessManager")
                success_count += 1
        else:
            print(f"ℹ️ Пользователь не найден в кеше AccessManager")
            
    except Exception as e:
        print(f"❌ Ошибка AccessManager: {e}")
    
    # 3. Удаляем из файла общего кеша
    try:
        cache_file = "data/shared_access_cache.json"
        if os.path.exists(cache_file):
            with open(cache_file, 'r', encoding='utf-8') as f:
                cache_data = json.load(f)
                
            cache = cache_data.get('cache', {})
            user_key = str(user_id)
            
            if user_key in cache:
                del cache[user_key]
                cache_data['cache'] = cache
                cache_data['last_update'] = time.time()
                
                with open(cache_file, 'w', encoding='utf-8') as f:
                    json.dump(cache_data, f, ensure_ascii=False, indent=2, default=str)
                    
                print(f"✅ Удален из файла общего кеша")
                success_count += 1
            else:
                print(f"ℹ️ Пользователь не найден в файле кеша")
        else:
            print(f"ℹ️ Файл общего кеша не найден")
            
    except Exception as e:
        print(f"❌ Ошибка файла кеша: {e}")
    
    # 4. Удаляем персональное хранилище пользователя
    try:
        user_data_dir = f"data/users/{user_id}"
        if os.path.exists(user_data_dir):
            import shutil
            shutil.rmtree(user_data_dir)
            print(f"✅ Удалено персональное хранилище")
            success_count += 1
        else:
            print(f"ℹ️ Персональное хранилище не найдено")
            
    except Exception as e:
        print(f"❌ Ошибка удаления хранилища: {e}")
    
    # 5. Принудительная синхронизация
    try:
        from access_manager import get_access_manager
        access_manager = get_access_manager()
        access_manager.force_sync()
        print(f"✅ Выполнена принудительная синхронизация")
        success_count += 1
    except Exception as e:
        print(f"❌ Ошибка синхронизации: {e}")
    
    print(f"\n🏁 РЕЗУЛЬТАТ: {success_count} операций выполнено")
    
    if success_count >= 3:
        print("🎉 ПОЛЬЗОВАТЕЛЬ ПОЛНОСТЬЮ УДАЛЕН!")
        return True
    else:
        print("⚠️ Удаление может быть неполным")
        return False

def verify_user_deleted(user_id: int):
    """Проверяет что пользователь действительно удален"""
    
    print(f"\n🔍 ПРОВЕРКА УДАЛЕНИЯ ПОЛЬЗОВАТЕЛЯ {user_id}")
    print("=" * 60)
    
    found_anywhere = False
    
    # Проверяем UserService
    try:
        from services.user_service import UserService
        user_service = UserService()
        user = user_service.get_user(user_id)
        if user:
            print(f"❌ НАЙДЕН в UserService: {user.username}, статус: {user.status}")
            found_anywhere = True
        else:
            print(f"✅ НЕТ в UserService")
    except Exception as e:
        print(f"❌ Ошибка проверки UserService: {e}")
    
    # Проверяем AccessManager
    try:
        from access_manager import has_access, get_access_manager
        
        access_result = has_access(user_id)
        print(f"📊 has_access({user_id}): {access_result}")
        
        if access_result:
            found_anywhere = True
            
        access_manager = get_access_manager()
        user_info = access_manager.get_user_info(user_id)
        if user_info:
            print(f"❌ НАЙДЕН в AccessManager: {user_info}")
            found_anywhere = True
        else:
            print(f"✅ НЕТ в AccessManager")
            
    except Exception as e:
        print(f"❌ Ошибка проверки AccessManager: {e}")
    
    # Проверяем файл кеша
    try:
        cache_file = "data/shared_access_cache.json"
        if os.path.exists(cache_file):
            with open(cache_file, 'r', encoding='utf-8') as f:
                cache_data = json.load(f)
                
            cache = cache_data.get('cache', {})
            user_key = str(user_id)
            
            if user_key in cache:
                print(f"❌ НАЙДЕН в файле кеша: {cache[user_key]}")
                found_anywhere = True
            else:
                print(f"✅ НЕТ в файле кеша")
        else:
            print(f"ℹ️ Файл кеша не существует")
    except Exception as e:
        print(f"❌ Ошибка проверки файла кеша: {e}")
    
    if not found_anywhere:
        print(f"\n🎉 ПОЛЬЗОВАТЕЛЬ {user_id} ПОЛНОСТЬЮ УДАЛЕН!")
        return True
    else:
        print(f"\n❌ ПОЛЬЗОВАТЕЛЬ {user_id} НАЙДЕН В СИСТЕМЕ!")
        return False

def main():
    import time
    
    print("🧹 ПРИНУДИТЕЛЬНАЯ ОЧИСТКА ПОЛЬЗОВАТЕЛЯ")
    print("=" * 80)
    
    user_id = 6626270112
    
    # 1. Проверяем текущее состояние
    print("1️⃣ ПРОВЕРКА ТЕКУЩЕГО СОСТОЯНИЯ:")
    verify_user_deleted(user_id)
    
    # 2. Принудительное удаление
    print("\n2️⃣ ПРИНУДИТЕЛЬНОЕ УДАЛЕНИЕ:")
    success = force_delete_user_completely(user_id)
    
    # 3. Проверяем результат
    print("\n3️⃣ ПРОВЕРКА РЕЗУЛЬТАТА:")
    is_deleted = verify_user_deleted(user_id)
    
    if is_deleted:
        print(f"\n✅ ГОТОВО! Теперь можно добавить пользователя {user_id} заново")
    else:
        print(f"\n❌ ПРОБЛЕМА! Пользователь {user_id} не полностью удален")

if __name__ == "__main__":
    import time
    main() 
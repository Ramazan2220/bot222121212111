#!/usr/bin/env python3
"""
ТЕСТ НОВЫХ СИСТЕМ СИНХРОНИЗАЦИИ
"""

import sys
import time
import threading
from datetime import datetime, timedelta

def test_multiprocessing_sync():
    """Тестирует multiprocessing синхронизацию"""
    print("🔧 ТЕСТ MULTIPROCESSING СИНХРОНИЗАЦИИ")
    print("=" * 60)
    
    try:
        from multiprocessing_access_sync import get_mp_sync, has_access_mp, add_user_mp, remove_user_mp
        
        sync = get_mp_sync()
        user_id = 6626270112
        
        # Начальная проверка
        print(f"1️⃣ Начальное состояние:")
        has_access_result = has_access_mp(user_id)
        print(f"   has_access({user_id}): {has_access_result}")
        
        # Добавляем пользователя
        print(f"2️⃣ Добавляем пользователя:")
        user_data = {
            'telegram_id': user_id,
            'is_active': True,
            'subscription_end': (datetime.now() + timedelta(days=30)).isoformat(),
            'role': 'trial'
        }
        
        add_result = add_user_mp(user_id, user_data)
        print(f"   add_user({user_id}): {add_result}")
        
        # Ждем синхронизации
        time.sleep(0.5)
        
        # Проверяем доступ
        has_access_result = has_access_mp(user_id)
        print(f"   has_access({user_id}) после добавления: {has_access_result}")
        
        # Удаляем пользователя
        print(f"3️⃣ Удаляем пользователя:")
        remove_result = remove_user_mp(user_id)
        print(f"   remove_user({user_id}): {remove_result}")
        
        # Ждем синхронизации
        time.sleep(0.5)
        
        # Проверяем доступ
        has_access_result = has_access_mp(user_id)
        print(f"   has_access({user_id}) после удаления: {has_access_result}")
        
        # Статистика
        stats = sync.get_stats()
        print(f"4️⃣ Статистика: {stats}")
        
        if not has_access_result:
            print(f"🎉 MULTIPROCESSING СИНХРОНИЗАЦИЯ РАБОТАЕТ!")
        else:
            print(f"❌ Проблема с multiprocessing синхронизацией")
            
        return not has_access_result
        
    except Exception as e:
        print(f"❌ Ошибка тестирования multiprocessing: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_redis_sync():
    """Тестирует Redis синхронизацию"""
    print("\n🔧 ТЕСТ REDIS СИНХРОНИЗАЦИИ")
    print("=" * 60)
    
    try:
        # Проверяем доступность Redis
        import redis
        r = redis.Redis(host='localhost', port=6379, db=0)
        r.ping()
        
    except Exception as e:
        print(f"⚠️ Redis недоступен: {e}")
        print(f"💡 Установите Redis: brew install redis (macOS) или sudo apt install redis-server (Ubuntu)")
        return False
    
    try:
        from redis_access_sync import get_redis_sync, has_access_redis, add_user_redis, remove_user_redis
        
        sync = get_redis_sync()
        user_id = 6626270112
        
        # Начальная проверка
        print(f"1️⃣ Начальное состояние:")
        has_access_result = has_access_redis(user_id)
        print(f"   has_access({user_id}): {has_access_result}")
        
        # Добавляем пользователя
        print(f"2️⃣ Добавляем пользователя:")
        user_data = {
            'telegram_id': user_id,
            'is_active': True,
            'subscription_end': (datetime.now() + timedelta(days=30)).isoformat(),
            'role': 'trial'
        }
        
        add_result = add_user_redis(user_id, user_data)
        print(f"   add_user({user_id}): {add_result}")
        
        # Ждем синхронизации
        time.sleep(0.2)
        
        # Проверяем доступ
        has_access_result = has_access_redis(user_id)
        print(f"   has_access({user_id}) после добавления: {has_access_result}")
        
        # Удаляем пользователя
        print(f"3️⃣ Удаляем пользователя:")
        remove_result = remove_user_redis(user_id)
        print(f"   remove_user({user_id}): {remove_result}")
        
        # Ждем синхронизации
        time.sleep(0.2)
        
        # Проверяем доступ
        has_access_result = has_access_redis(user_id)
        print(f"   has_access({user_id}) после удаления: {has_access_result}")
        
        # Статистика
        stats = sync.get_stats()
        print(f"4️⃣ Статистика: {stats}")
        
        if not has_access_result:
            print(f"🎉 REDIS СИНХРОНИЗАЦИЯ РАБОТАЕТ!")
        else:
            print(f"❌ Проблема с Redis синхронизацией")
            
        return not has_access_result
        
    except Exception as e:
        print(f"❌ Ошибка тестирования Redis: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_real_time_sync():
    """Тестирует real-time синхронизацию"""
    print("\n🔧 ТЕСТ REAL-TIME СИНХРОНИЗАЦИИ")
    print("=" * 60)
    
    # Выбираем доступную систему
    sync_system = None
    
    try:
        import redis
        r = redis.Redis(host='localhost', port=6379, db=0)
        r.ping()
        from redis_access_sync import get_redis_sync, has_access_redis, add_user_redis, remove_user_redis
        sync_system = "redis"
        sync = get_redis_sync()
        has_access_func = has_access_redis
        add_user_func = add_user_redis
        remove_user_func = remove_user_redis
        print("🟢 Используем Redis")
    except:
        from multiprocessing_access_sync import get_mp_sync, has_access_mp, add_user_mp, remove_user_mp
        sync_system = "multiprocessing"
        sync = get_mp_sync()
        has_access_func = has_access_mp
        add_user_func = add_user_mp
        remove_user_func = remove_user_mp
        print("🟡 Используем Multiprocessing")
    
    user_id = 6626270112
    
    def monitor_access():
        """Мониторит доступ в отдельном потоке"""
        for i in range(10):
            access = has_access_func(user_id)
            print(f"   [{time.strftime('%H:%M:%S')}] has_access({user_id}): {access}")
            time.sleep(0.5)
    
    # Запускаем мониторинг в отдельном потоке
    monitor_thread = threading.Thread(target=monitor_access, daemon=True)
    monitor_thread.start()
    
    time.sleep(1)
    
    # Добавляем пользователя
    print(f"➕ Добавляем пользователя...")
    user_data = {
        'telegram_id': user_id,
        'is_active': True,
        'subscription_end': (datetime.now() + timedelta(days=30)).isoformat(),
        'role': 'trial'
    }
    add_user_func(user_id, user_data)
    
    time.sleep(2)
    
    # Удаляем пользователя
    print(f"🗑️ Удаляем пользователя...")
    remove_user_func(user_id)
    
    time.sleep(2)
    
    monitor_thread.join(timeout=1)
    
    print(f"🎉 REAL-TIME ТЕСТ ЗАВЕРШЕН!")
    
def main():
    print("🚀 ТЕСТИРОВАНИЕ НОВЫХ СИСТЕМ СИНХРОНИЗАЦИИ")
    print("=" * 80)
    
    # Тестируем multiprocessing
    mp_result = test_multiprocessing_sync()
    
    # Тестируем Redis (если доступен)
    redis_result = test_redis_sync()
    
    # Тестируем real-time
    test_real_time_sync()
    
    print(f"\n🏁 РЕЗУЛЬТАТЫ:")
    print(f"   Multiprocessing: {'✅' if mp_result else '❌'}")
    print(f"   Redis: {'✅' if redis_result else '⚠️ недоступен'}")
    
    if mp_result or redis_result:
        print(f"\n🎉 НОВЫЕ СИСТЕМЫ СИНХРОНИЗАЦИИ РАБОТАЮТ!")
        print(f"💡 Теперь нужно интегрировать их в ваш проект")
    else:
        print(f"\n❌ Требуется дополнительная настройка")

if __name__ == "__main__":
    main() 
"""
ТЕСТ НОВЫХ СИСТЕМ СИНХРОНИЗАЦИИ
"""

import sys
import time
import threading
from datetime import datetime, timedelta

def test_multiprocessing_sync():
    """Тестирует multiprocessing синхронизацию"""
    print("🔧 ТЕСТ MULTIPROCESSING СИНХРОНИЗАЦИИ")
    print("=" * 60)
    
    try:
        from multiprocessing_access_sync import get_mp_sync, has_access_mp, add_user_mp, remove_user_mp
        
        sync = get_mp_sync()
        user_id = 6626270112
        
        # Начальная проверка
        print(f"1️⃣ Начальное состояние:")
        has_access_result = has_access_mp(user_id)
        print(f"   has_access({user_id}): {has_access_result}")
        
        # Добавляем пользователя
        print(f"2️⃣ Добавляем пользователя:")
        user_data = {
            'telegram_id': user_id,
            'is_active': True,
            'subscription_end': (datetime.now() + timedelta(days=30)).isoformat(),
            'role': 'trial'
        }
        
        add_result = add_user_mp(user_id, user_data)
        print(f"   add_user({user_id}): {add_result}")
        
        # Ждем синхронизации
        time.sleep(0.5)
        
        # Проверяем доступ
        has_access_result = has_access_mp(user_id)
        print(f"   has_access({user_id}) после добавления: {has_access_result}")
        
        # Удаляем пользователя
        print(f"3️⃣ Удаляем пользователя:")
        remove_result = remove_user_mp(user_id)
        print(f"   remove_user({user_id}): {remove_result}")
        
        # Ждем синхронизации
        time.sleep(0.5)
        
        # Проверяем доступ
        has_access_result = has_access_mp(user_id)
        print(f"   has_access({user_id}) после удаления: {has_access_result}")
        
        # Статистика
        stats = sync.get_stats()
        print(f"4️⃣ Статистика: {stats}")
        
        if not has_access_result:
            print(f"🎉 MULTIPROCESSING СИНХРОНИЗАЦИЯ РАБОТАЕТ!")
        else:
            print(f"❌ Проблема с multiprocessing синхронизацией")
            
        return not has_access_result
        
    except Exception as e:
        print(f"❌ Ошибка тестирования multiprocessing: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_redis_sync():
    """Тестирует Redis синхронизацию"""
    print("\n🔧 ТЕСТ REDIS СИНХРОНИЗАЦИИ")
    print("=" * 60)
    
    try:
        # Проверяем доступность Redis
        import redis
        r = redis.Redis(host='localhost', port=6379, db=0)
        r.ping()
        
    except Exception as e:
        print(f"⚠️ Redis недоступен: {e}")
        print(f"💡 Установите Redis: brew install redis (macOS) или sudo apt install redis-server (Ubuntu)")
        return False
    
    try:
        from redis_access_sync import get_redis_sync, has_access_redis, add_user_redis, remove_user_redis
        
        sync = get_redis_sync()
        user_id = 6626270112
        
        # Начальная проверка
        print(f"1️⃣ Начальное состояние:")
        has_access_result = has_access_redis(user_id)
        print(f"   has_access({user_id}): {has_access_result}")
        
        # Добавляем пользователя
        print(f"2️⃣ Добавляем пользователя:")
        user_data = {
            'telegram_id': user_id,
            'is_active': True,
            'subscription_end': (datetime.now() + timedelta(days=30)).isoformat(),
            'role': 'trial'
        }
        
        add_result = add_user_redis(user_id, user_data)
        print(f"   add_user({user_id}): {add_result}")
        
        # Ждем синхронизации
        time.sleep(0.2)
        
        # Проверяем доступ
        has_access_result = has_access_redis(user_id)
        print(f"   has_access({user_id}) после добавления: {has_access_result}")
        
        # Удаляем пользователя
        print(f"3️⃣ Удаляем пользователя:")
        remove_result = remove_user_redis(user_id)
        print(f"   remove_user({user_id}): {remove_result}")
        
        # Ждем синхронизации
        time.sleep(0.2)
        
        # Проверяем доступ
        has_access_result = has_access_redis(user_id)
        print(f"   has_access({user_id}) после удаления: {has_access_result}")
        
        # Статистика
        stats = sync.get_stats()
        print(f"4️⃣ Статистика: {stats}")
        
        if not has_access_result:
            print(f"🎉 REDIS СИНХРОНИЗАЦИЯ РАБОТАЕТ!")
        else:
            print(f"❌ Проблема с Redis синхронизацией")
            
        return not has_access_result
        
    except Exception as e:
        print(f"❌ Ошибка тестирования Redis: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_real_time_sync():
    """Тестирует real-time синхронизацию"""
    print("\n🔧 ТЕСТ REAL-TIME СИНХРОНИЗАЦИИ")
    print("=" * 60)
    
    # Выбираем доступную систему
    sync_system = None
    
    try:
        import redis
        r = redis.Redis(host='localhost', port=6379, db=0)
        r.ping()
        from redis_access_sync import get_redis_sync, has_access_redis, add_user_redis, remove_user_redis
        sync_system = "redis"
        sync = get_redis_sync()
        has_access_func = has_access_redis
        add_user_func = add_user_redis
        remove_user_func = remove_user_redis
        print("🟢 Используем Redis")
    except:
        from multiprocessing_access_sync import get_mp_sync, has_access_mp, add_user_mp, remove_user_mp
        sync_system = "multiprocessing"
        sync = get_mp_sync()
        has_access_func = has_access_mp
        add_user_func = add_user_mp
        remove_user_func = remove_user_mp
        print("🟡 Используем Multiprocessing")
    
    user_id = 6626270112
    
    def monitor_access():
        """Мониторит доступ в отдельном потоке"""
        for i in range(10):
            access = has_access_func(user_id)
            print(f"   [{time.strftime('%H:%M:%S')}] has_access({user_id}): {access}")
            time.sleep(0.5)
    
    # Запускаем мониторинг в отдельном потоке
    monitor_thread = threading.Thread(target=monitor_access, daemon=True)
    monitor_thread.start()
    
    time.sleep(1)
    
    # Добавляем пользователя
    print(f"➕ Добавляем пользователя...")
    user_data = {
        'telegram_id': user_id,
        'is_active': True,
        'subscription_end': (datetime.now() + timedelta(days=30)).isoformat(),
        'role': 'trial'
    }
    add_user_func(user_id, user_data)
    
    time.sleep(2)
    
    # Удаляем пользователя
    print(f"🗑️ Удаляем пользователя...")
    remove_user_func(user_id)
    
    time.sleep(2)
    
    monitor_thread.join(timeout=1)
    
    print(f"🎉 REAL-TIME ТЕСТ ЗАВЕРШЕН!")
    
def main():
    print("🚀 ТЕСТИРОВАНИЕ НОВЫХ СИСТЕМ СИНХРОНИЗАЦИИ")
    print("=" * 80)
    
    # Тестируем multiprocessing
    mp_result = test_multiprocessing_sync()
    
    # Тестируем Redis (если доступен)
    redis_result = test_redis_sync()
    
    # Тестируем real-time
    test_real_time_sync()
    
    print(f"\n🏁 РЕЗУЛЬТАТЫ:")
    print(f"   Multiprocessing: {'✅' if mp_result else '❌'}")
    print(f"   Redis: {'✅' if redis_result else '⚠️ недоступен'}")
    
    if mp_result or redis_result:
        print(f"\n🎉 НОВЫЕ СИСТЕМЫ СИНХРОНИЗАЦИИ РАБОТАЮТ!")
        print(f"💡 Теперь нужно интегрировать их в ваш проект")
    else:
        print(f"\n❌ Требуется дополнительная настройка")

if __name__ == "__main__":
    main() 
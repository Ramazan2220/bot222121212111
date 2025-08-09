#!/usr/bin/env python3
"""
ТЕСТ НОВОЙ СИСТЕМЫ В РЕАЛЬНЫХ УСЛОВИЯХ
Имитирует операции админ бота и основного бота
"""

import sys
import time
import threading
from datetime import datetime

# Добавляем пути
sys.path.insert(0, '.')
sys.path.insert(0, './utils')

def test_as_admin_bot():
    """Имитирует операции админ бота"""
    print("🔧 ТЕСТ: ОПЕРАЦИИ АДМИН БОТА")
    print("=" * 60)
    
    user_id = 6626270112
    
    try:
        # Импортируем как админ бот
        from utils.access_manager import delete_user_completely, add_user_access, has_access
        
        print(f"✅ Админ бот: Функции импортированы")
        
        # 1. Проверяем начальное состояние
        print(f"\n1️⃣ Проверяем пользователя {user_id}...")
        initial_access = has_access(user_id)
        print(f"   Начальный доступ: {initial_access}")
        
        # 2. Добавляем пользователя (как админ)
        print(f"\n2️⃣ Админ добавляет пользователя...")
        add_result = add_user_access(user_id)
        print(f"   add_user_access({user_id}): {add_result}")
        
        # 3. Проверяем сразу
        after_add = has_access(user_id)
        print(f"   Доступ после добавления: {after_add}")
        
        return user_id, after_add
        
    except Exception as e:
        print(f"❌ Ошибка в админ боте: {e}")
        return None, False

def test_as_main_bot(user_id):
    """Имитирует проверки основного бота"""
    print(f"\n🤖 ТЕСТ: ОСНОВНОЙ БОТ ПРОВЕРЯЕТ ПОЛЬЗОВАТЕЛЯ {user_id}")
    print("=" * 60)
    
    try:
        # Импортируем как основной бот
        from utils.access_manager import has_access
        
        print(f"✅ Основной бот: Функции импортированы")
        
        # Проверяем доступ
        access = has_access(user_id)
        print(f"   has_access({user_id}): {access}")
        
        if access:
            print(f"   ✅ Основной бот ВИДИТ пользователя!")
        else:
            print(f"   ❌ Основной бот НЕ ВИДИТ пользователя!")
            
        return access
        
    except Exception as e:
        print(f"❌ Ошибка в основном боте: {e}")
        return False

def test_admin_delete(user_id):
    """Имитирует удаление админом"""
    print(f"\n🗑️ ТЕСТ: АДМИН УДАЛЯЕТ ПОЛЬЗОВАТЕЛЯ {user_id}")
    print("=" * 60)
    
    try:
        from utils.access_manager import delete_user_completely, has_access
        
        print(f"✅ Админ бот: Удаляю пользователя...")
        
        # Удаляем
        delete_result = delete_user_completely(user_id)
        print(f"   delete_user_completely({user_id}): {delete_result}")
        
        # Проверяем сразу
        access_after = has_access(user_id)
        print(f"   Доступ после удаления: {access_after}")
        
        return delete_result and not access_after
        
    except Exception as e:
        print(f"❌ Ошибка при удалении: {e}")
        return False

def monitor_access_changes(user_id, duration=10):
    """Мониторит изменения доступа в реальном времени"""
    print(f"\n📊 МОНИТОРИНГ ДОСТУПА ДЛЯ {user_id} ({duration} сек)")
    print("=" * 60)
    
    try:
        from utils.access_manager import has_access
        
        end_time = time.time() + duration
        while time.time() < end_time:
            access = has_access(user_id)
            timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
            status = "🟢 ЕСТЬ" if access else "🔴 НЕТ"
            print(f"   [{timestamp}] Доступ: {status}")
            time.sleep(1)
            
    except Exception as e:
        print(f"❌ Ошибка мониторинга: {e}")

def main():
    print("🚀 ТЕСТ НОВОЙ СИСТЕМЫ В РЕАЛЬНЫХ УСЛОВИЯХ")
    print("=" * 80)
    print(f"🕐 Время: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}")
    print()
    
    # Тест 1: Админ бот добавляет пользователя
    user_id, add_success = test_as_admin_bot()
    
    if user_id and add_success:
        # Тест 2: Основной бот проверяет пользователя
        main_bot_sees = test_as_main_bot(user_id)
        
        if main_bot_sees:
            print(f"\n✅ СИНХРОНИЗАЦИЯ ДОБАВЛЕНИЯ РАБОТАЕТ!")
        else:
            print(f"\n❌ ПРОБЛЕМА: Основной бот не видит добавленного пользователя")
        
        # Тест 3: Мониторинг в отдельном потоке
        monitor_thread = threading.Thread(
            target=monitor_access_changes, 
            args=(user_id, 8), 
            daemon=True
        )
        monitor_thread.start()
        
        # Тест 4: Админ удаляет пользователя через 3 секунды
        time.sleep(3)
        delete_success = test_admin_delete(user_id)
        
        # Ждем завершения мониторинга
        monitor_thread.join(timeout=6)
        
        # Финальная проверка основным ботом
        final_check = test_as_main_bot(user_id)
        
        # Результаты
        print(f"\n🏆 ИТОГОВЫЕ РЕЗУЛЬТАТЫ:")
        print("=" * 80)
        print(f"   ➕ Добавление пользователя: {'✅' if add_success else '❌'}")
        print(f"   👀 Основной бот видит добавленного: {'✅' if main_bot_sees else '❌'}")
        print(f"   🗑️ Удаление пользователя: {'✅' if delete_success else '❌'}")
        print(f"   👀 Основной бот НЕ видит удаленного: {'✅' if not final_check else '❌'}")
        
        if add_success and main_bot_sees and delete_success and not final_check:
            print(f"\n🎉 ВСЯ СИСТЕМА РАБОТАЕТ ИДЕАЛЬНО!")
            print(f"✅ Мгновенная синхронизация между админ ботом и основным ботом")
            print(f"✅ Добавление и удаление работают корректно")
            print(f"🚀 МОЖНО ТЕСТИРОВАТЬ В БОТЕ!")
        else:
            print(f"\n⚠️ ЕСТЬ ПРОБЛЕМЫ:")
            if not add_success:
                print(f"   ❌ Проблема с добавлением пользователя")
            if not main_bot_sees:
                print(f"   ❌ Основной бот не видит изменения")
            if not delete_success:
                print(f"   ❌ Проблема с удалением пользователя")
            if final_check:
                print(f"   ❌ Пользователь не удален полностью")
    else:
        print(f"\n❌ КРИТИЧЕСКАЯ ОШИБКА: Не удалось добавить пользователя")

if __name__ == "__main__":
    main() 
"""
ТЕСТ НОВОЙ СИСТЕМЫ В РЕАЛЬНЫХ УСЛОВИЯХ
Имитирует операции админ бота и основного бота
"""

import sys
import time
import threading
from datetime import datetime

# Добавляем пути
sys.path.insert(0, '.')
sys.path.insert(0, './utils')

def test_as_admin_bot():
    """Имитирует операции админ бота"""
    print("🔧 ТЕСТ: ОПЕРАЦИИ АДМИН БОТА")
    print("=" * 60)
    
    user_id = 6626270112
    
    try:
        # Импортируем как админ бот
        from utils.access_manager import delete_user_completely, add_user_access, has_access
        
        print(f"✅ Админ бот: Функции импортированы")
        
        # 1. Проверяем начальное состояние
        print(f"\n1️⃣ Проверяем пользователя {user_id}...")
        initial_access = has_access(user_id)
        print(f"   Начальный доступ: {initial_access}")
        
        # 2. Добавляем пользователя (как админ)
        print(f"\n2️⃣ Админ добавляет пользователя...")
        add_result = add_user_access(user_id)
        print(f"   add_user_access({user_id}): {add_result}")
        
        # 3. Проверяем сразу
        after_add = has_access(user_id)
        print(f"   Доступ после добавления: {after_add}")
        
        return user_id, after_add
        
    except Exception as e:
        print(f"❌ Ошибка в админ боте: {e}")
        return None, False

def test_as_main_bot(user_id):
    """Имитирует проверки основного бота"""
    print(f"\n🤖 ТЕСТ: ОСНОВНОЙ БОТ ПРОВЕРЯЕТ ПОЛЬЗОВАТЕЛЯ {user_id}")
    print("=" * 60)
    
    try:
        # Импортируем как основной бот
        from utils.access_manager import has_access
        
        print(f"✅ Основной бот: Функции импортированы")
        
        # Проверяем доступ
        access = has_access(user_id)
        print(f"   has_access({user_id}): {access}")
        
        if access:
            print(f"   ✅ Основной бот ВИДИТ пользователя!")
        else:
            print(f"   ❌ Основной бот НЕ ВИДИТ пользователя!")
            
        return access
        
    except Exception as e:
        print(f"❌ Ошибка в основном боте: {e}")
        return False

def test_admin_delete(user_id):
    """Имитирует удаление админом"""
    print(f"\n🗑️ ТЕСТ: АДМИН УДАЛЯЕТ ПОЛЬЗОВАТЕЛЯ {user_id}")
    print("=" * 60)
    
    try:
        from utils.access_manager import delete_user_completely, has_access
        
        print(f"✅ Админ бот: Удаляю пользователя...")
        
        # Удаляем
        delete_result = delete_user_completely(user_id)
        print(f"   delete_user_completely({user_id}): {delete_result}")
        
        # Проверяем сразу
        access_after = has_access(user_id)
        print(f"   Доступ после удаления: {access_after}")
        
        return delete_result and not access_after
        
    except Exception as e:
        print(f"❌ Ошибка при удалении: {e}")
        return False

def monitor_access_changes(user_id, duration=10):
    """Мониторит изменения доступа в реальном времени"""
    print(f"\n📊 МОНИТОРИНГ ДОСТУПА ДЛЯ {user_id} ({duration} сек)")
    print("=" * 60)
    
    try:
        from utils.access_manager import has_access
        
        end_time = time.time() + duration
        while time.time() < end_time:
            access = has_access(user_id)
            timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
            status = "🟢 ЕСТЬ" if access else "🔴 НЕТ"
            print(f"   [{timestamp}] Доступ: {status}")
            time.sleep(1)
            
    except Exception as e:
        print(f"❌ Ошибка мониторинга: {e}")

def main():
    print("🚀 ТЕСТ НОВОЙ СИСТЕМЫ В РЕАЛЬНЫХ УСЛОВИЯХ")
    print("=" * 80)
    print(f"🕐 Время: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}")
    print()
    
    # Тест 1: Админ бот добавляет пользователя
    user_id, add_success = test_as_admin_bot()
    
    if user_id and add_success:
        # Тест 2: Основной бот проверяет пользователя
        main_bot_sees = test_as_main_bot(user_id)
        
        if main_bot_sees:
            print(f"\n✅ СИНХРОНИЗАЦИЯ ДОБАВЛЕНИЯ РАБОТАЕТ!")
        else:
            print(f"\n❌ ПРОБЛЕМА: Основной бот не видит добавленного пользователя")
        
        # Тест 3: Мониторинг в отдельном потоке
        monitor_thread = threading.Thread(
            target=monitor_access_changes, 
            args=(user_id, 8), 
            daemon=True
        )
        monitor_thread.start()
        
        # Тест 4: Админ удаляет пользователя через 3 секунды
        time.sleep(3)
        delete_success = test_admin_delete(user_id)
        
        # Ждем завершения мониторинга
        monitor_thread.join(timeout=6)
        
        # Финальная проверка основным ботом
        final_check = test_as_main_bot(user_id)
        
        # Результаты
        print(f"\n🏆 ИТОГОВЫЕ РЕЗУЛЬТАТЫ:")
        print("=" * 80)
        print(f"   ➕ Добавление пользователя: {'✅' if add_success else '❌'}")
        print(f"   👀 Основной бот видит добавленного: {'✅' if main_bot_sees else '❌'}")
        print(f"   🗑️ Удаление пользователя: {'✅' if delete_success else '❌'}")
        print(f"   👀 Основной бот НЕ видит удаленного: {'✅' if not final_check else '❌'}")
        
        if add_success and main_bot_sees and delete_success and not final_check:
            print(f"\n🎉 ВСЯ СИСТЕМА РАБОТАЕТ ИДЕАЛЬНО!")
            print(f"✅ Мгновенная синхронизация между админ ботом и основным ботом")
            print(f"✅ Добавление и удаление работают корректно")
            print(f"🚀 МОЖНО ТЕСТИРОВАТЬ В БОТЕ!")
        else:
            print(f"\n⚠️ ЕСТЬ ПРОБЛЕМЫ:")
            if not add_success:
                print(f"   ❌ Проблема с добавлением пользователя")
            if not main_bot_sees:
                print(f"   ❌ Основной бот не видит изменения")
            if not delete_success:
                print(f"   ❌ Проблема с удалением пользователя")
            if final_check:
                print(f"   ❌ Пользователь не удален полностью")
    else:
        print(f"\n❌ КРИТИЧЕСКАЯ ОШИБКА: Не удалось добавить пользователя")

if __name__ == "__main__":
    main() 
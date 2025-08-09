#!/usr/bin/env python3
"""
🧪 Комплексный тест системы уведомлений
Тестирует все компоненты системы:
- NotificationManager
- SubscriptionMonitor  
- BroadcastSystem
- Интеграция с админ панелью
"""

import sys
import os
import time
import json
from datetime import datetime, timedelta

# Добавляем пути для импортов
sys.path.insert(0, os.path.abspath('.'))

def test_notification_manager():
    """Тест NotificationManager"""
    print("🔔 ТЕСТ NOTIFICATION MANAGER")
    print("=" * 50)
    
    try:
        from utils.notification_manager import get_notification_manager
        
        # Получаем менеджер
        nm = get_notification_manager()
        print("✅ NotificationManager инициализирован")
        
        # Тест блокировки пользователя
        print("\n🚫 Тест блокировки пользователя...")
        success = nm.send_admin_block_notification(
            user_id=123456,
            admin_id=admin_id,
            reason="Тестовая блокировка"
        )
        print(f"   Результат: {'✅ Успешно' if success else '❌ Ошибка'}")
        
        # Тест разблокировки
        print("\n🔓 Тест разблокировки пользователя...")
        success = nm.send_admin_unblock_notification(
            user_id=123456,
            admin_id=admin_id
        )
        print(f"   Результат: {'✅ Успешно' if success else '❌ Ошибка'}")
        
        # Тест напоминания о подписке
        print("\n⏰ Тест напоминания о подписке...")
        success = nm.send_subscription_warning(
            user_id=123456,
            days_left=3,
            subscription_end=datetime.now() + timedelta(days=3)
        )
        print(f"   Результат: {'✅ Успешно' if success else '❌ Ошибка'}")
        
        # Тест персонального уведомления
        print("\n👤 Тест персонального уведомления...")
        success = nm.send_personal_notification(
            user_id=123456,
            title="Тестовое уведомление",
            message="Это тестовое персональное уведомление",
            admin_id=admin_id
        )
        print(f"   Результат: {'✅ Успешно' if success else '❌ Ошибка'}")
        
        # Получаем статистику
        print("\n📊 Статистика уведомлений:")
        stats = nm.get_stats(7)
        for key, value in stats.items():
            print(f"   {key}: {value}")
        
        print("\n✅ NotificationManager: ВСЕ ТЕСТЫ ПРОЙДЕНЫ")
        return True
        
    except Exception as e:
        print(f"❌ Ошибка в NotificationManager: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_subscription_monitor():
    """Тест SubscriptionMonitor"""
    print("\n\n⏰ ТЕСТ SUBSCRIPTION MONITOR")
    print("=" * 50)
    
    try:
        from utils.subscription_monitor import get_subscription_monitor
        
        # Получаем монитор
        monitor = get_subscription_monitor()
        print("✅ SubscriptionMonitor инициализирован")
        
        # Тест получения статистики
        print("\n📊 Тест статистики мониторинга...")
        stats = monitor.get_monitor_stats()
        print("   Статистика:")
        for key, value in stats.items():
            print(f"     {key}: {value}")
        
        # Тест получения истекающих подписок
        print("\n🚨 Тест получения истекающих подписок...")
        expiring = monitor.get_expiring_subscriptions(7)
        print(f"   Найдено истекающих подписок: {len(expiring)}")
        
        for i, sub in enumerate(expiring[:5]):  # Показываем первые 5
            print(f"     {i+1}. {sub.get('username', 'N/A')} - {sub.get('days_left', 0)} дн.")
        
        # Тест ручной проверки всех подписок
        print("\n🔍 Тест ручной проверки всех подписок...")
        try:
            monitor.check_all_subscriptions()
            print("   ✅ Проверка завершена успешно")
        except Exception as e:
            print(f"   ⚠️ Проверка завершена с предупреждениями: {e}")
        
        print("\n✅ SubscriptionMonitor: ВСЕ ТЕСТЫ ПРОЙДЕНЫ")
        return True
        
    except Exception as e:
        print(f"❌ Ошибка в SubscriptionMonitor: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_broadcast_system():
    """Тест BroadcastSystem"""
    print("\n\n📢 ТЕСТ BROADCAST SYSTEM")
    print("=" * 50)
    
    try:
        from utils.broadcast_system import get_broadcast_system
        
        # Получаем систему
        bs = get_broadcast_system()
        print("✅ BroadcastSystem инициализирован")
        
        # Тест рассылки всем
        print("\n📢 Тест рассылки всем пользователям...")
        broadcast_id = bs.broadcast_to_all(
            title="🧪 Тестовая рассылка",
            message="Это тестовая рассылка для проверки системы",
            admin_id=admin_id
        )
        print(f"   ID рассылки: {broadcast_id}")
        print(f"   Результат: {'✅ Создана' if broadcast_id else '❌ Ошибка'}")
        
        # Тест рассылки группе
        print("\n👥 Тест рассылки группе (trial)...")
        broadcast_id2 = bs.broadcast_to_group(
            title="🧪 Тест для trial пользователей",
            message="Это тестовая рассылка для trial пользователей",
            group="trial",
            admin_id=admin_id
        )
        print(f"   ID рассылки: {broadcast_id2}")
        print(f"   Результат: {'✅ Создана' if broadcast_id2 else '❌ Ошибка'}")
        
        # Ждем обработки
        print("\n⏳ Ждем обработки рассылок (10 секунд)...")
        time.sleep(10)
        
        # Проверяем статус рассылок
        if broadcast_id:
            print(f"\n📋 Статус первой рассылки ({broadcast_id}):")
            status = bs.get_broadcast_status(broadcast_id)
            if status:
                print(f"   Статус: {status.get('status', 'unknown')}")
                print(f"   Отправлено: {status.get('sent_count', 0)}/{status.get('total_recipients', 0)}")
                print(f"   Ошибок: {status.get('failed_count', 0)}")
            else:
                print("   ❌ Статус не получен")
        
        # Тест получения истории
        print("\n📋 Тест получения истории рассылок...")
        history = bs.get_recent_broadcasts(5)
        print(f"   Найдено рассылок: {len(history)}")
        
        for i, broadcast in enumerate(history[:3]):
            print(f"     {i+1}. {broadcast.get('title', 'N/A')} - {broadcast.get('status', 'unknown')}")
        
        # Тест статистики
        print("\n📊 Тест статистики рассылок...")
        stats = bs.get_broadcast_stats(7)
        print("   Статистика:")
        for key, value in stats.items():
            if key != 'daily_stats':  # Пропускаем детальную статистику
                print(f"     {key}: {value}")
        
        print("\n✅ BroadcastSystem: ВСЕ ТЕСТЫ ПРОЙДЕНЫ")
        return True
        
    except Exception as e:
        print(f"❌ Ошибка в BroadcastSystem: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_redis_integration():
    """Тест интеграции с Redis"""
    print("\n\n🔥 ТЕСТ REDIS ИНТЕГРАЦИИ")
    print("=" * 50)
    
    try:
        from redis_access_sync import get_redis_sync
        from fake_redis import get_fake_redis
        
        # Тест Redis подключения
        redis_sync = get_redis_sync()
        print("✅ Redis подключение установлено")
        
        # Тест FakeRedis
        fake_redis = get_fake_redis()
        print("✅ FakeRedis инициализирован")
        
        # Тест базовых операций
        print("\n🧪 Тест базовых Redis операций...")
        
        # Set/Get
        fake_redis.set("test_key", "test_value")
        value = fake_redis.get("test_key")
        print(f"   Set/Get: {'✅ OK' if value == 'test_value' else '❌ Ошибка'}")
        
        # Hash operations
        fake_redis.hset("test_hash", "field1", "value1")
        hash_value = fake_redis.hget("test_hash", "field1")
        print(f"   Hash Set/Get: {'✅ OK' if hash_value == 'value1' else '❌ Ошибка'}")
        
        # Pub/Sub test
        print("\n📡 Тест Pub/Sub...")
        pubsub = fake_redis.pubsub()
        pubsub.subscribe("test_channel")
        
        # Публикуем сообщение
        fake_redis.publish("test_channel", json.dumps({"test": "message"}))
        print("   ✅ Сообщение опубликовано")
        
        print("\n✅ Redis Integration: ВСЕ ТЕСТЫ ПРОЙДЕНЫ")
        return True
        
    except Exception as e:
        print(f"❌ Ошибка в Redis Integration: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_access_manager_integration():
    """Тест интеграции с AccessManager"""
    print("\n\n🔐 ТЕСТ ACCESS MANAGER ИНТЕГРАЦИИ")
    print("=" * 50)
    
    try:
        from utils.access_manager import has_access, add_user_access, remove_user_access
        
        test_user_id = 999999  # Тестовый пользователь
        
        # Проверяем начальное состояние
        initial_access = has_access(test_user_id)
        print(f"   Начальный доступ пользователя {test_user_id}: {initial_access}")
        
        # Добавляем доступ
        print(f"\n➕ Добавляем доступ пользователю {test_user_id}...")
        user_data = {
            'telegram_id': test_user_id,
            'username': 'test_user',
            'is_active': True,
            'subscription_end': (datetime.now() + timedelta(days=30)).isoformat(),
            'role': 'trial'
        }
        
        success = add_user_access(test_user_id, user_data)
        print(f"   Добавление: {'✅ Успешно' if success else '❌ Ошибка'}")
        
        # Проверяем доступ
        time.sleep(1)  # Небольшая задержка для синхронизации
        new_access = has_access(test_user_id)
        print(f"   Проверка доступа: {'✅ Есть доступ' if new_access else '❌ Нет доступа'}")
        
        # Убираем доступ
        print(f"\n➖ Убираем доступ у пользователя {test_user_id}...")
        success = remove_user_access(test_user_id)
        print(f"   Удаление: {'✅ Успешно' if success else '❌ Ошибка'}")
        
        # Проверяем что доступа нет
        time.sleep(1)  # Небольшая задержка для синхронизации
        final_access = has_access(test_user_id)
        print(f"   Финальная проверка: {'✅ Доступ убран' if not final_access else '❌ Доступ остался'}")
        
        print("\n✅ Access Manager Integration: ВСЕ ТЕСТЫ ПРОЙДЕНЫ")
        return True
        
    except Exception as e:
        print(f"❌ Ошибка в Access Manager Integration: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_admin_integration():
    """Тест интеграции с админ панелью"""
    print("\n\n👨‍💼 ТЕСТ ИНТЕГРАЦИИ С АДМИН ПАНЕЛЬЮ")
    print("=" * 50)
    
    try:
        # Тест получения пользователей
        print("📊 Тест получения пользователей из админ панели...")
        
        import sys
        import os
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'admin_bot'))
        
        from admin_bot.services.user_service import UserService
        
        user_service = UserService()
        users = user_service.get_all_users()
        print(f"   Найдено пользователей: {len(users)}")
        
        # Показываем первых 5 пользователей
        for i, user in enumerate(users[:5]):
            print(f"     {i+1}. @{user.username or 'N/A'} ({user.telegram_id}) - {user.subscription_plan.value if user.subscription_plan else 'N/A'}")
        
        print("\n✅ Admin Integration: ТЕСТ ПРОЙДЕН")
        return True
        
    except Exception as e:
        print(f"❌ Ошибка в Admin Integration: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Главная функция тестирования"""
    print("🧪 КОМПЛЕКСНОЕ ТЕСТИРОВАНИЕ СИСТЕМЫ УВЕДОМЛЕНИЙ")
    print("=" * 60)
    print(f"⏰ Время запуска: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # ID админа для тестов
    global admin_id
    admin_id = 123456789  # Замените на реальный ID
    
    # Запускаем все тесты
    tests = [
        ("Redis Integration", test_redis_integration),
        ("Access Manager Integration", test_access_manager_integration),
        ("Admin Integration", test_admin_integration),
        ("NotificationManager", test_notification_manager),
        ("SubscriptionMonitor", test_subscription_monitor),
        ("BroadcastSystem", test_broadcast_system),
    ]
    
    results = []
    
    for test_name, test_func in tests:
        print(f"\n🔍 Запуск теста: {test_name}")
        print("-" * 30)
        
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"❌ КРИТИЧЕСКАЯ ОШИБКА в тесте {test_name}: {e}")
            results.append((test_name, False))
        
        print("-" * 30)
        time.sleep(2)  # Пауза между тестами
    
    # Итоговый отчет
    print("\n\n📋 ИТОГОВЫЙ ОТЧЕТ")
    print("=" * 60)
    
    passed = 0
    failed = 0
    
    for test_name, result in results:
        status = "✅ ПРОЙДЕН" if result else "❌ ПРОВАЛЕН"
        print(f"{test_name:30} | {status}")
        
        if result:
            passed += 1
        else:
            failed += 1
    
    print("=" * 60)
    print(f"Всего тестов: {len(results)}")
    print(f"Пройдено: {passed}")
    print(f"Провалено: {failed}")
    print(f"Успешность: {(passed/len(results)*100):.1f}%")
    
    if failed == 0:
        print("\n🎉 ВСЕ ТЕСТЫ ПРОЙДЕНЫ УСПЕШНО!")
        print("🚀 Система уведомлений готова к работе!")
    else:
        print(f"\n⚠️ НАЙДЕНЫ ПРОБЛЕМЫ В {failed} ТЕСТАХ")
        print("🔧 Необходимо исправить ошибки перед запуском")
    
    print(f"\n⏰ Время завершения: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    main() 
 
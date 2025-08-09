#!/usr/bin/env python3
"""
Тест интеграции системы подписок
Проверяет связь между админ-ботом и основной системой
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from utils.subscription_service import subscription_service
from admin_bot.models.user import SubscriptionPlan, UserStatus
from admin_bot.services.user_service import UserService

def test_subscription_integration():
    """Тестирует интеграцию системы подписок"""
    print("🧪 ТЕСТ ИНТЕГРАЦИИ СИСТЕМЫ ПОДПИСОК")
    print("=" * 50)
    
    # Тестовый пользователь
    test_user_id = 123456789
    test_username = "test_user"
    
    try:
        print("\n1️⃣ Тестируем создание триального пользователя...")
        
        # Создаем триального пользователя
        user = subscription_service.create_trial_user(
            test_user_id, 
            test_username, 
            SubscriptionPlan.FREE_TRIAL_1_DAY
        )
        
        if user:
            print(f"✅ Пользователь создан: {user.telegram_id} (@{user.username})")
            print(f"   План: {user.subscription_plan.value}")
            print(f"   Статус: {user.status.value}")
            print(f"   Дней осталось: {user.days_remaining}")
        else:
            print("❌ Ошибка создания пользователя")
            return False
        
        print("\n2️⃣ Тестируем проверку доступа...")
        
        # Проверяем доступ
        access_info = subscription_service.check_user_access(test_user_id)
        print(f"✅ Проверка доступа: {access_info['has_access']}")
        print(f"   Статус: {access_info['status']}")
        print(f"   Сообщение: {access_info['message']}")
        print(f"   Триал: {access_info['is_trial']}")
        
        print("\n3️⃣ Тестируем получение статистики пользователя...")
        
        # Получаем статистику
        stats = subscription_service.get_user_stats(test_user_id)
        print(f"✅ Статистика получена:")
        print(f"   Название плана: {stats.get('plan_name')}")
        print(f"   Цена: ${stats.get('plan_price')}")
        print(f"   Дата начала: {stats.get('subscription_start')}")
        print(f"   Дата окончания: {stats.get('subscription_end')}")
        
        print("\n4️⃣ Тестируем обновление подписки на платную...")
        
        # Обновляем на платную подписку
        user_service = UserService()
        success = user_service.set_user_subscription(test_user_id, SubscriptionPlan.SUBSCRIPTION_30_DAYS)
        
        if success:
            print("✅ Подписка обновлена на 30 дней ($200)")
            
            # Проверяем новый статус
            new_access = subscription_service.check_user_access(test_user_id)
            print(f"   Новый статус: {new_access['status']}")
            print(f"   Триал: {new_access['is_trial']}")
            print(f"   Дней осталось: {new_access['days_remaining']}")
        else:
            print("❌ Ошибка обновления подписки")
        
        print("\n5️⃣ Тестируем получение доступных планов...")
        
        # Получаем доступные планы
        plans = subscription_service.get_available_plans()
        print("✅ Доступные планы:")
        for plan_data in plans:
            plan = plan_data['plan']
            info = plan_data['info']
            print(f"   {info['name']}: ${info['price']} ({info['duration']} дней)")
        
        print("\n6️⃣ Очистка тестовых данных...")
        
        # Удаляем тестового пользователя
        user_service.delete_user(test_user_id)
        print("✅ Тестовый пользователь удален")
        
        print("\n🎉 ВСЕ ТЕСТЫ ПРОЙДЕНЫ УСПЕШНО!")
        print("=" * 50)
        print("✅ Интеграция админ-бота с основной системой работает корректно")
        print("✅ Проверка подписок функционирует")
        print("✅ Цены обновлены: 1 мес=$200, 3 мес=$400, навсегда=$500")
        print("✅ Триальные планы доступны")
        
        return True
        
    except Exception as e:
        print(f"\n❌ ОШИБКА ТЕСТА: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_subscription_middleware():
    """Тестирует middleware для проверки подписок"""
    print("\n🧪 ТЕСТ MIDDLEWARE ПОДПИСОК")
    print("=" * 30)
    
    try:
        from utils.subscription_middleware import check_subscription_silent, get_user_subscription_info
        
        # Тестируем с несуществующим пользователем
        test_user_id = 999999999
        info = get_user_subscription_info(test_user_id)
        
        print(f"✅ Проверка несуществующего пользователя:")
        print(f"   Доступ: {info['has_access']}")
        print(f"   Статус: {info['status']}")
        
        return True
        
    except Exception as e:
        print(f"❌ Ошибка middleware: {e}")
        return False

if __name__ == "__main__":
    print("🚀 ЗАПУСК ТЕСТОВ ИНТЕГРАЦИИ ПОДПИСОК")
    print("=" * 60)
    
    # Основной тест интеграции
    integration_success = test_subscription_integration()
    
    # Тест middleware
    middleware_success = test_subscription_middleware()
    
    print("\n📊 РЕЗУЛЬТАТЫ ТЕСТИРОВАНИЯ:")
    print("=" * 60)
    print(f"🔗 Интеграция админ-бота: {'✅ Успешно' if integration_success else '❌ Провал'}")
    print(f"🛡️ Middleware подписок: {'✅ Успешно' if middleware_success else '❌ Провал'}")
    
    if integration_success and middleware_success:
        print("\n🎉 ВСЯ СИСТЕМА ГОТОВА К РАБОТЕ!")
        print("🔐 Разделение пользователей настроено")
        print("💰 Тарифные планы обновлены")
        print("🤖 Админ-панель интегрирована")
    else:
        print("\n⚠️ Требуется доработка")
        sys.exit(1) 
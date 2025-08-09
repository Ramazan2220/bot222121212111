#!/usr/bin/env python3
"""
Тест системы контроля доступа к Instagram Telegram Bot
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from utils.subscription_service import subscription_service
from admin_bot.services.user_service import UserService
from admin_bot.models.user import SubscriptionPlan, UserStatus

def test_access_control():
    """Тестирует систему контроля доступа"""
    print("🧪 ТЕСТИРОВАНИЕ СИСТЕМЫ КОНТРОЛЯ ДОСТУПА\n")
    
    user_service = UserService()
    
    # Тестовые пользователи
    test_users = [
        {'id': 111111111, 'username': 'testuser1', 'plan': None},  # Без доступа
        {'id': 222222222, 'username': 'testuser2', 'plan': SubscriptionPlan.FREE_TRIAL_1_DAY},  # Триал
        {'id': 333333333, 'username': 'testuser3', 'plan': SubscriptionPlan.SUBSCRIPTION_30_DAYS},  # Платная подписка
    ]
    
    # Создаем тестовых пользователей
    print("1️⃣ Создание тестовых пользователей:")
    for user_data in test_users:
        # Проверяем существует ли пользователь
        existing_user = user_service.get_user(user_data['id'])
        if existing_user:
            print(f"   ⚠️ Пользователь {user_data['id']} уже существует")
            if user_data['plan']:
                existing_user.set_subscription(user_data['plan'])
                user_service.update_user(existing_user)
                print(f"   ✅ Обновлен план для {user_data['username']}")
        else:
            user = user_service.create_user(user_data['id'], user_data['username'])
            if user_data['plan']:
                user.set_subscription(user_data['plan'])
                user_service.update_user(user)
            print(f"   ✅ Создан пользователь {user_data['username']} (ID: {user_data['id']})")
    
    print("\n2️⃣ Проверка доступа для каждого пользователя:")
    
    for user_data in test_users:
        user_id = user_data['id']
        username = user_data['username']
        
        print(f"\n👤 Тестируем пользователя: @{username} (ID: {user_id})")
        
        # Проверяем доступ через subscription_service
        access_info = subscription_service.check_user_access(user_id)
        
        print(f"   📊 Статус доступа: {'✅ Разрешен' if access_info['has_access'] else '❌ Заблокирован'}")
        print(f"   📋 План: {access_info.get('plan', 'Нет плана')}")
        print(f"   ⏰ Дней осталось: {access_info.get('days_remaining', 'N/A')}")
        print(f"   🆓 Триал: {'Да' if access_info.get('is_trial', False) else 'Нет'}")
        print(f"   💬 Сообщение: {access_info.get('message', 'N/A')}")
        
        # Тестируем автосоздание пользователя
        subscription_service.ensure_user_exists(user_id, username)
        print(f"   🔄 Автосоздание: Пользователь проверен/создан")
    
    print("\n3️⃣ Проверка создания нового пользователя:")
    new_user_id = 999999999
    new_username = "newuser"
    
    # Удаляем если существует
    existing = user_service.get_user(new_user_id)
    if existing:
        user_service.delete_user(new_user_id)
        print(f"   🗑️ Удален существующий пользователь {new_user_id}")
    
    # Тестируем автосоздание
    result = subscription_service.ensure_user_exists(new_user_id, new_username)
    if result:
        print(f"   ✅ Автоматически создан пользователь @{new_username} (ID: {new_user_id})")
        
        # Проверяем доступ
        access_info = subscription_service.check_user_access(new_user_id)
        print(f"   📊 Доступ нового пользователя: {'✅ Разрешен' if access_info['has_access'] else '❌ Заблокирован (ожидается)'}")
    else:
        print(f"   ❌ Ошибка создания пользователя")
    
    print("\n4️⃣ Статистика пользователей:")
    stats = user_service.get_statistics()
    print(f"   👥 Всего пользователей: {stats['total_users']}")
    print(f"   ✅ Активных: {stats['active_users']}")
    print(f"   🆓 На триале: {stats['trial_users']}")
    print(f"   ❌ Заблокированных: {stats['blocked_users']}")
    print(f"   ⏰ Истекших: {stats['expired_users']}")
    print(f"   💰 Оценочный доход: ${stats['estimated_revenue']:.2f}")
    
    print("\n✅ ТЕСТИРОВАНИЕ ЗАВЕРШЕНО!")
    print("\n📋 ИНСТРУКЦИЯ ДЛЯ ПОЛЬЗОВАТЕЛЕЙ:")
    print("1. При первом /start создается пользователь без доступа")
    print("2. Пользователь видит свой ID и инструкции")
    print("3. Администратор через админ-бот выдает доступ")
    print("4. После активации пользователь может использовать все функции")
    
    return True

if __name__ == "__main__":
    try:
        test_access_control()
    except Exception as e:
        print(f"❌ Ошибка тестирования: {e}")
        import traceback
        traceback.print_exc() 
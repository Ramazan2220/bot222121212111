#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Демо-скрипт для создания тестовых пользователей с разными тарифными планами
"""

import sys
import os
from datetime import datetime, timedelta

# Добавляем путь к проекту
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from admin_bot.services.user_service import UserService
from admin_bot.models.user import SubscriptionPlan, UserStatus

def create_demo_users():
    """Создает демо пользователей с разными тарифными планами"""
    
    user_service = UserService()
    
    # Демо пользователи
    demo_users = [
        {
            'telegram_id': 111111111,
            'username': 'test_user_1day',
            'plan': SubscriptionPlan.FREE_TRIAL_1_DAY,
            'days_ago': 0  # Только что зарегистрировался
        },
        {
            'telegram_id': 222222222,
            'username': 'test_user_3days',
            'plan': SubscriptionPlan.FREE_TRIAL_3_DAYS,
            'days_ago': 1  # Зарегистрировался вчера
        },
        {
            'telegram_id': 333333333,
            'username': 'test_user_7days',
            'plan': SubscriptionPlan.FREE_TRIAL_7_DAYS,
            'days_ago': 5  # Зарегистрировался 5 дней назад
        },
        {
            'telegram_id': 444444444,
            'username': 'test_user_30days',
            'plan': SubscriptionPlan.SUBSCRIPTION_30_DAYS,
            'days_ago': 10  # Зарегистрировался 10 дней назад
        },
        {
            'telegram_id': 555555555,
            'username': 'test_user_90days',
            'plan': SubscriptionPlan.SUBSCRIPTION_90_DAYS,
            'days_ago': 30  # Зарегистрировался месяц назад
        },
        {
            'telegram_id': 666666666,
            'username': 'test_user_lifetime',
            'plan': SubscriptionPlan.SUBSCRIPTION_LIFETIME,
            'days_ago': 60  # Зарегистрировался 2 месяца назад
        },
        {
            'telegram_id': 777777777,
            'username': 'expired_user',
            'plan': SubscriptionPlan.FREE_TRIAL_3_DAYS,
            'days_ago': 5,  # Триал истек 2 дня назад
            'status': UserStatus.EXPIRED
        },
        {
            'telegram_id': 888888888,
            'username': 'blocked_user',
            'plan': SubscriptionPlan.SUBSCRIPTION_30_DAYS,
            'days_ago': 15,
            'status': UserStatus.BLOCKED
        }
    ]
    
    print("🎭 СОЗДАНИЕ ДЕМО ПОЛЬЗОВАТЕЛЕЙ")
    print("=" * 50)
    
    created_count = 0
    
    for user_data in demo_users:
        telegram_id = user_data['telegram_id']
        username = user_data['username']
        plan = user_data['plan']
        days_ago = user_data['days_ago']
        status = user_data.get('status')
        
        # Проверяем, не существует ли уже пользователь
        existing_user = user_service.get_user(telegram_id)
        if existing_user:
            print(f"⏭️  Пользователь {username} уже существует")
            continue
        
        # Создаем пользователя
        user = user_service.create_user(telegram_id=telegram_id, username=username)
        
        # Устанавливаем подписку
        user.set_subscription(plan)
        
        # Корректируем даты для реалистичности
        start_date = datetime.now() - timedelta(days=days_ago)
        user.created_at = start_date
        user.subscription_start = start_date
        
        # Для планов с ограниченным временем корректируем дату окончания
        if user.subscription_end:
            user.subscription_end = start_date + (user.subscription_end - datetime.now())
        
        # Устанавливаем статус если нужно
        if status:
            user.status = status
        
        # Добавляем случайное количество аккаунтов
        import random
        user.accounts_count = random.randint(1, 50)
        
        # Обновляем пользователя
        user_service.update_user(user)
        
        # Статус для отображения
        status_emoji = "✅" if user.is_active else "❌" if user.status == UserStatus.BLOCKED else "⏰"
        days_left = user.days_remaining if user.days_remaining >= 0 else "∞"
        
        print(f"✅ {status_emoji} {username}")
        print(f"   ID: {telegram_id}")
        print(f"   План: {plan.value}")
        print(f"   Дней осталось: {days_left}")
        print(f"   Аккаунтов: {user.accounts_count}")
        print()
        
        created_count += 1
    
    print(f"🎉 Создано {created_count} демо пользователей!")
    
    # Показываем статистику
    print("\n📊 СТАТИСТИКА ПОСЛЕ СОЗДАНИЯ:")
    print("=" * 40)
    
    stats = user_service.get_statistics()
    print(f"• Всего пользователей: {stats['total_users']}")
    print(f"• Активных: {stats['active_users']}")
    print(f"• На триале: {stats['trial_users']}")
    print(f"• Заблокированных: {stats['blocked_users']}")
    print(f"• Истекших: {stats['expired_users']}")
    print(f"• Оценочный доход: ${stats['estimated_revenue']:.2f}")
    
    return created_count

def clear_demo_users():
    """Удаляет всех демо пользователей"""
    user_service = UserService()
    
    demo_telegram_ids = [
        111111111, 222222222, 333333333, 444444444,
        555555555, 666666666, 777777777, 888888888
    ]
    
    print("🗑️  УДАЛЕНИЕ ДЕМО ПОЛЬЗОВАТЕЛЕЙ")
    print("=" * 40)
    
    deleted_count = 0
    for telegram_id in demo_telegram_ids:
        if user_service.delete_user(telegram_id):
            print(f"✅ Удален пользователь {telegram_id}")
            deleted_count += 1
        else:
            print(f"⏭️  Пользователь {telegram_id} не найден")
    
    print(f"\n🎉 Удалено {deleted_count} демо пользователей!")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Управление демо пользователями админ бота")
    parser.add_argument("--create", action="store_true", help="Создать демо пользователей")
    parser.add_argument("--clear", action="store_true", help="Удалить демо пользователей")
    parser.add_argument("--recreate", action="store_true", help="Пересоздать демо пользователей")
    
    args = parser.parse_args()
    
    if args.recreate:
        clear_demo_users()
        print()
        create_demo_users()
    elif args.clear:
        clear_demo_users()
    elif args.create:
        create_demo_users()
    else:
        print("🎭 ДЕМО ПОЛЬЗОВАТЕЛИ АДМИН БОТА")
        print("=" * 40)
        print()
        print("Использование:")
        print("  python admin_bot/demo_users.py --create     # Создать демо пользователей")
        print("  python admin_bot/demo_users.py --clear      # Удалить демо пользователей")
        print("  python admin_bot/demo_users.py --recreate   # Пересоздать демо пользователей")
        print()
        print("После создания демо пользователей вы сможете:")
        print("• Увидеть их в админ панели")
        print("• Протестировать управление подписками")
        print("• Проверить статистику и аналитику")
        print("• Протестировать уведомления об истекающих подписках") 
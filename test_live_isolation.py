#!/usr/bin/env python3
"""
🔴 LIVE ТЕСТ изоляции пользователей
Проверяет изоляцию аккаунтов между реальными пользователями
"""

import time
import logging
from database.db_manager import get_instagram_accounts, add_instagram_account_for_user
from utils.access_manager import has_access

logger = logging.getLogger(__name__)

def test_live_user_isolation():
    """Тестирует изоляцию между реальными пользователями системы"""
    
    print("🔴 LIVE ТЕСТ ИЗОЛЯЦИИ ПОЛЬЗОВАТЕЛЕЙ")
    print("="*60)
    
    # Реальные пользователи из системы
    user1_id = 6499246016  # ysim0r 
    user2_id = 6626270112  # lock0125
    
    print(f"👤 Тестируем пользователей:")
    print(f"   User 1: {user1_id} (ysim0r)")
    print(f"   User 2: {user2_id} (lock0125)")
    print()
    
    # 1. Проверяем доступ пользователей к системе
    print("1️⃣ ПРОВЕРКА ДОСТУПА К СИСТЕМЕ:")
    
    user1_access = has_access(user1_id)
    user2_access = has_access(user2_id)
    
    print(f"🔐 Пользователь {user1_id}: {'✅ Доступ есть' if user1_access else '❌ Доступа нет'}")
    print(f"🔐 Пользователь {user2_id}: {'✅ Доступ есть' if user2_access else '❌ Доступа нет'}")
    print()
    
    # 2. Проверяем изоляцию аккаунтов Instagram
    print("2️⃣ ПРОВЕРКА ИЗОЛЯЦИИ INSTAGRAM АККАУНТОВ:")
    
    user1_accounts = get_instagram_accounts(user_id=user1_id)
    user2_accounts = get_instagram_accounts(user_id=user2_id)
    all_legacy_accounts = get_instagram_accounts()  # Все аккаунты (включая старые)
    
    print(f"📊 СТАТИСТИКА АККАУНТОВ:")
    print(f"   👤 User {user1_id}: {len(user1_accounts)} аккаунтов")
    print(f"   👤 User {user2_id}: {len(user2_accounts)} аккаунтов")
    print(f"   🌍 Всего в системе: {len(all_legacy_accounts)} аккаунтов")
    print()
    
    # 3. Показываем детали аккаунтов для каждого пользователя
    print("3️⃣ ДЕТАЛИ АККАУНТОВ ПО ПОЛЬЗОВАТЕЛЯМ:")
    
    print(f"🔒 АККАУНТЫ ПОЛЬЗОВАТЕЛЯ {user1_id}:")
    if user1_accounts:
        for i, account in enumerate(user1_accounts, 1):
            print(f"   {i}. {account.username} (ID: {account.id}, user_id: {account.user_id})")
    else:
        print("   📭 Нет аккаунтов")
    
    print(f"\n🔒 АККАУНТЫ ПОЛЬЗОВАТЕЛЯ {user2_id}:")
    if user2_accounts:
        for i, account in enumerate(user2_accounts, 1):
            print(f"   {i}. {account.username} (ID: {account.id}, user_id: {account.user_id})")
    else:
        print("   📭 Нет аккаунтов")
    print()
    
    # 4. Проверяем старые аккаунты (user_id=0)
    legacy_accounts = [acc for acc in all_legacy_accounts if acc.user_id == 0]
    print(f"4️⃣ СТАРЫЕ АККАУНТЫ БЕЗ ИЗОЛЯЦИИ (user_id=0): {len(legacy_accounts)}")
    if legacy_accounts:
        print("⚠️ Эти аккаунты требуют ручного назначения владельца:")
        for i, account in enumerate(legacy_accounts[:5], 1):  # Показываем первые 5
            print(f"   {i}. {account.username} (ID: {account.id})")
        if len(legacy_accounts) > 5:
            print(f"   ... и ещё {len(legacy_accounts) - 5} аккаунтов")
    print()
    
    # 5. Тестируем добавление нового аккаунта
    print("5️⃣ ТЕСТ ДОБАВЛЕНИЯ НОВОГО АККАУНТА:")
    
    timestamp = int(time.time())
    test_username = f"isolation_test_{user1_id}_{timestamp}"
    
    new_account = add_instagram_account_for_user(
        user_id=user1_id,
        username=test_username,
        password="test_password_123",
        email=f"test_{timestamp}@example.com"
    )
    
    if new_account:
        print(f"✅ Аккаунт {test_username} добавлен для пользователя {user1_id}")
        
        # Проверяем, что другой пользователь его НЕ видит
        user1_after = get_instagram_accounts(user_id=user1_id)
        user2_after = get_instagram_accounts(user_id=user2_id)
        
        print(f"📊 ПОСЛЕ ДОБАВЛЕНИЯ:")
        print(f"   👤 User {user1_id}: {len(user1_after)} аккаунтов (+{len(user1_after) - len(user1_accounts)})")
        print(f"   👤 User {user2_id}: {len(user2_after)} аккаунтов (не изменилось)")
        
        if len(user2_after) == len(user2_accounts):
            print("✅ ИЗОЛЯЦИЯ РАБОТАЕТ! Пользователь 2 не видит новый аккаунт пользователя 1")
        else:
            print("❌ ОШИБКА ИЗОЛЯЦИИ! Пользователь 2 видит чужой аккаунт")
    else:
        print(f"❌ Не удалось добавить тестовый аккаунт")
    
    print()
    
    # 6. Финальная оценка изоляции
    print("6️⃣ ФИНАЛЬНАЯ ОЦЕНКА ИЗОЛЯЦИИ:")
    
    user1_usernames = {acc.username for acc in get_instagram_accounts(user_id=user1_id)}
    user2_usernames = {acc.username for acc in get_instagram_accounts(user_id=user2_id)}
    
    intersection = user1_usernames & user2_usernames
    
    if intersection:
        print(f"❌ КРИТИЧЕСКАЯ ОШИБКА! Общие аккаунты: {intersection}")
        return False
    else:
        print("✅ ИЗОЛЯЦИЯ ПОЛНОСТЬЮ РАБОТАЕТ!")
        print("🔒 Каждый пользователь видит только свои аккаунты")
        return True

def check_system_status():
    """Проверяет статус системы"""
    print("📊 СТАТУС СИСТЕМЫ:")
    print("-" * 40)
    
    try:
        all_accounts = get_instagram_accounts()
        isolated_accounts = [acc for acc in all_accounts if acc.user_id != 0]
        legacy_accounts = [acc for acc in all_accounts if acc.user_id == 0]
        
        print(f"📈 Всего аккаунтов: {len(all_accounts)}")
        print(f"🔒 С изоляцией: {len(isolated_accounts)}")
        print(f"⚠️ Без изоляции (legacy): {len(legacy_accounts)}")
        
        if legacy_accounts:
            print(f"📋 Процент изоляции: {(len(isolated_accounts)/len(all_accounts)*100):.1f}%")
        else:
            print("🎉 100% аккаунтов изолированы!")
        
        # Проверяем уникальных пользователей
        unique_users = set(acc.user_id for acc in isolated_accounts)
        print(f"👥 Активных пользователей: {len(unique_users)}")
        
    except Exception as e:
        print(f"❌ Ошибка проверки: {e}")
    
    print()

if __name__ == "__main__":
    # Настройка логирования
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    print("🚀 ЗАПУСК LIVE ТЕСТИРОВАНИЯ ИЗОЛЯЦИИ...")
    print()
    
    check_system_status()
    success = test_live_user_isolation()
    
    print("\n" + "="*60)
    if success:
        print("🎉 LIVE ТЕСТ ПРОШЁЛ УСПЕШНО!")
        print("✅ Система изоляции работает корректно")
        print("🔒 Пользователи полностью изолированы друг от друга")
    else:
        print("💥 LIVE ТЕСТ ПРОВАЛЕН!")
        print("❌ Обнаружены проблемы с изоляцией")
        print("🔧 Требуется дополнительная настройка")
    print("="*60) 
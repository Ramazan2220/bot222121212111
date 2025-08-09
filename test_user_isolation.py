#!/usr/bin/env python3
"""
🧪 Тест изоляции данных пользователей
Проверяет, что пользователи видят только свои аккаунты
"""

import logging
from database.db_manager import get_instagram_accounts, add_instagram_account_for_user

logger = logging.getLogger(__name__)

def test_user_isolation():
    """Тестирует изоляцию данных между пользователями"""
    
    print("🧪 ТЕСТ ИЗОЛЯЦИИ ПОЛЬЗОВАТЕЛЕЙ")
    print("="*50)
    
    # Тестовые пользователи
    user1_id = 6499246016  # ysim0r
    user2_id = 6626270112  # lock0125
    
    print(f"👤 Пользователь 1: {user1_id}")
    print(f"👤 Пользователь 2: {user2_id}")
    print()
    
    # 1. Проверяем текущие аккаунты для каждого пользователя
    print("1️⃣ ПРОВЕРКА ТЕКУЩИХ АККАУНТОВ:")
    
    user1_accounts = get_instagram_accounts(user_id=user1_id)
    user2_accounts = get_instagram_accounts(user_id=user2_id)
    all_accounts = get_instagram_accounts()  # Без фильтрации
    
    print(f"🔒 Пользователь {user1_id}: {len(user1_accounts)} аккаунтов")
    print(f"🔒 Пользователь {user2_id}: {len(user2_accounts)} аккаунтов")
    print(f"🌍 Всего в системе: {len(all_accounts)} аккаунтов")
    print()
    
    # 2. Добавляем тестовый аккаунт для пользователя 1
    print("2️⃣ ДОБАВЛЕНИЕ ТЕСТОВОГО АККАУНТА:")
    
    test_account1 = add_instagram_account_for_user(
        user_id=user1_id,
        username=f"test_user1_{user1_id}",
        password="test_password_123",
        email="test1@example.com"
    )
    
    if test_account1:
        print(f"✅ Аккаунт {test_account1.username} добавлен для пользователя {user1_id}")
    else:
        print(f"❌ Не удалось добавить аккаунт для пользователя {user1_id}")
    print()
    
    # 3. Добавляем тестовый аккаунт для пользователя 2
    test_account2 = add_instagram_account_for_user(
        user_id=user2_id,
        username=f"test_user2_{user2_id}",
        password="test_password_456",
        email="test2@example.com"
    )
    
    if test_account2:
        print(f"✅ Аккаунт {test_account2.username} добавлен для пользователя {user2_id}")
    else:
        print(f"❌ Не удалось добавить аккаунт для пользователя {user2_id}")
    print()
    
    # 4. Проверяем изоляцию после добавления
    print("3️⃣ ПРОВЕРКА ИЗОЛЯЦИИ:")
    
    user1_accounts_after = get_instagram_accounts(user_id=user1_id)
    user2_accounts_after = get_instagram_accounts(user_id=user2_id)
    
    print(f"🔒 Пользователь {user1_id}: {len(user1_accounts_after)} аккаунтов")
    for account in user1_accounts_after:
        print(f"    - {account.username} (user_id: {account.user_id})")
    
    print(f"🔒 Пользователь {user2_id}: {len(user2_accounts_after)} аккаунтов")
    for account in user2_accounts_after:
        print(f"    - {account.username} (user_id: {account.user_id})")
    print()
    
    # 5. Проверяем, что аккаунты НЕ пересекаются
    print("4️⃣ ПРОВЕРКА ПЕРЕСЕЧЕНИЙ:")
    
    user1_usernames = {acc.username for acc in user1_accounts_after}
    user2_usernames = {acc.username for acc in user2_accounts_after}
    
    intersection = user1_usernames & user2_usernames
    
    if intersection:
        print(f"❌ ОШИБКА ИЗОЛЯЦИИ! Общие аккаунты: {intersection}")
        return False
    else:
        print("✅ ИЗОЛЯЦИЯ РАБОТАЕТ! Пользователи не видят чужие аккаунты")
        return True

if __name__ == "__main__":
    # Настройка логирования
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    print("🚀 Запуск тестирования изоляции пользователей...")
    success = test_user_isolation()
    
    print("\n" + "="*50)
    if success:
        print("🎉 ТЕСТ ИЗОЛЯЦИИ ПРОШЁЛ УСПЕШНО!")
        print("✅ Пользователи изолированы друг от друга")
    else:
        print("💥 ТЕСТ ИЗОЛЯЦИИ ПРОВАЛЕН!")
        print("❌ Требуется исправление системы изоляции")
    print("="*50) 
#!/usr/bin/env python3
"""
🔄 ПРОСТОЙ ТЕСТ ИЗОЛЯЦИИ БЕЗ TELEGRAM
Проверяет изоляцию данных пользователей прямо в SQLite
"""

import sqlite3
import os

def test_database_isolation():
    """Тестирует изоляцию в базе данных"""
    
    print("🔄 ПРОСТОЙ ТЕСТ ИЗОЛЯЦИИ БАЗЫ ДАННЫХ")
    print("="*50)
    
    db_path = "data/database.sqlite"
    
    if not os.path.exists(db_path):
        print("❌ База данных не найдена!")
        return False
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Проверяем структуру таблицы
        print("1️⃣ ПРОВЕРКА СТРУКТУРЫ ТАБЛИЦЫ:")
        cursor.execute("PRAGMA table_info(instagram_accounts)")
        columns = cursor.fetchall()
        
        print("Столбцы в таблице instagram_accounts:")
        for col in columns:
            print(f"   - {col[1]} ({col[2]})")
        
        # Проверяем наличие поля user_id
        column_names = [col[1] for col in columns]
        if 'user_id' not in column_names:
            print("❌ Поле user_id отсутствует!")
            return False
        else:
            print("✅ Поле user_id найдено!")
        
        print()
        
        # Статистика аккаунтов
        print("2️⃣ СТАТИСТИКА АККАУНТОВ:")
        
        # Всего аккаунтов
        cursor.execute("SELECT COUNT(*) FROM instagram_accounts")
        total_accounts = cursor.fetchone()[0]
        print(f"📊 Всего аккаунтов: {total_accounts}")
        
        # Аккаунты с изоляцией (user_id != 0)
        cursor.execute("SELECT COUNT(*) FROM instagram_accounts WHERE user_id != 0")
        isolated_accounts = cursor.fetchone()[0]
        print(f"🔒 С изоляцией: {isolated_accounts}")
        
        # Старые аккаунты без изоляции
        cursor.execute("SELECT COUNT(*) FROM instagram_accounts WHERE user_id = 0")
        legacy_accounts = cursor.fetchone()[0]
        print(f"⚠️ Без изоляции: {legacy_accounts}")
        
        # Процент изоляции
        if total_accounts > 0:
            isolation_percent = (isolated_accounts / total_accounts) * 100
            print(f"📈 Процент изоляции: {isolation_percent:.1f}%")
        
        print()
        
        # Проверяем конкретных пользователей
        print("3️⃣ ТЕСТ КОНКРЕТНЫХ ПОЛЬЗОВАТЕЛЕЙ:")
        
        user1_id = 6499246016  # ysim0r
        user2_id = 6626270112  # lock0125
        
        # Аккаунты пользователя 1
        cursor.execute("SELECT username, user_id FROM instagram_accounts WHERE user_id = ?", (user1_id,))
        user1_accounts = cursor.fetchall()
        print(f"👤 Пользователь {user1_id}: {len(user1_accounts)} аккаунтов")
        for acc in user1_accounts:
            print(f"   - {acc[0]} (user_id: {acc[1]})")
        
        # Аккаунты пользователя 2
        cursor.execute("SELECT username, user_id FROM instagram_accounts WHERE user_id = ?", (user2_id,))
        user2_accounts = cursor.fetchall()
        print(f"👤 Пользователь {user2_id}: {len(user2_accounts)} аккаунтов")
        for acc in user2_accounts:
            print(f"   - {acc[0]} (user_id: {acc[1]})")
        
        print()
        
        # Проверяем пересечения
        print("4️⃣ ПРОВЕРКА ПЕРЕСЕЧЕНИЙ:")
        
        user1_usernames = {acc[0] for acc in user1_accounts}
        user2_usernames = {acc[0] for acc in user2_accounts}
        
        intersection = user1_usernames & user2_usernames
        
        if intersection:
            print(f"❌ ОШИБКА! Общие аккаунты: {intersection}")
            return False
        else:
            print("✅ ИЗОЛЯЦИЯ РАБОТАЕТ! Нет общих аккаунтов")
        
        # Уникальные пользователи
        cursor.execute("SELECT DISTINCT user_id FROM instagram_accounts WHERE user_id != 0")
        unique_users = cursor.fetchall()
        print(f"👥 Уникальных активных пользователей: {len(unique_users)}")
        
        for user in unique_users:
            cursor.execute("SELECT COUNT(*) FROM instagram_accounts WHERE user_id = ?", (user[0],))
            count = cursor.fetchone()[0]
            print(f"   - User {user[0]}: {count} аккаунтов")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        return False

def add_test_account(user_id, username):
    """Добавляет тестовый аккаунт"""
    
    db_path = "data/database.sqlite"
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Проверяем существование
        cursor.execute("SELECT id FROM instagram_accounts WHERE username = ? AND user_id = ?", 
                      (username, user_id))
        existing = cursor.fetchone()
        
        if existing:
            print(f"⚠️ Аккаунт {username} уже существует у пользователя {user_id}")
            return True
        
        # Добавляем новый аккаунт
        cursor.execute("""
            INSERT INTO instagram_accounts (username, password, user_id, is_active, created_at)
            VALUES (?, ?, ?, ?, datetime('now'))
        """, (username, "test_password_123", user_id, 0))
        
        conn.commit()
        conn.close()
        
        print(f"✅ Тестовый аккаунт {username} добавлен для пользователя {user_id}")
        return True
        
    except Exception as e:
        print(f"❌ Ошибка добавления аккаунта: {e}")
        return False

if __name__ == "__main__":
    print("🚀 ЗАПУСК ПРОСТОГО ТЕСТА ИЗОЛЯЦИИ...")
    print()
    
    # Добавляем тестовые аккаунты
    print("🔧 ДОБАВЛЕНИЕ ТЕСТОВЫХ АККАУНТОВ:")
    add_test_account(6499246016, f"simple_test_1_{int(__import__('time').time())}")
    add_test_account(6626270112, f"simple_test_2_{int(__import__('time').time())}")
    print()
    
    # Тестируем изоляцию
    success = test_database_isolation()
    
    print("\n" + "="*50)
    if success:
        print("🎉 ТЕСТ ИЗОЛЯЦИИ ПРОШЁЛ УСПЕШНО!")
        print("✅ База данных корректно изолирует пользователей")
    else:
        print("💥 ТЕСТ ИЗОЛЯЦИИ ПРОВАЛЕН!")
        print("❌ Требуется исправление изоляции")
    print("="*50) 
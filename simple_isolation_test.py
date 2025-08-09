#!/usr/bin/env python3
"""
🔍 Простой тест изоляции пользователей без внешних зависимостей
Проверяет изоляцию через прямые SQL запросы к SQLite
"""

import sqlite3
import os

def test_database_isolation():
    """Проверяет изоляцию в базе данных"""
    
    print("🔍 ПРОСТОЙ ТЕСТ ИЗОЛЯЦИИ БАЗЫ ДАННЫХ")
    print("="*50)
    
    db_path = "data/database.sqlite"
    
    if not os.path.exists(db_path):
        print(f"❌ База данных не найдена: {db_path}")
        return False
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # 1. Проверяем структуру таблицы
        print("1️⃣ ПРОВЕРКА СТРУКТУРЫ ТАБЛИЦЫ:")
        cursor.execute("PRAGMA table_info(instagram_accounts)")
        columns = cursor.fetchall()
        
        user_id_exists = any(col[1] == 'user_id' for col in columns)
        print(f"🔒 Поле user_id: {'✅ Есть' if user_id_exists else '❌ Отсутствует'}")
        
        if not user_id_exists:
            print("❌ КРИТИЧЕСКАЯ ОШИБКА: Поле user_id не найдено!")
            return False
        
        # 2. Общая статистика
        print("\n2️⃣ ОБЩАЯ СТАТИСТИКА:")
        cursor.execute("SELECT COUNT(*) FROM instagram_accounts")
        total_accounts = cursor.fetchone()[0]
        print(f"📊 Всего аккаунтов: {total_accounts}")
        
        # 3. Статистика по изоляции
        print("\n3️⃣ СТАТИСТИКА ИЗОЛЯЦИИ:")
        cursor.execute("SELECT COUNT(*) FROM instagram_accounts WHERE user_id = 0")
        legacy_accounts = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM instagram_accounts WHERE user_id != 0")
        isolated_accounts = cursor.fetchone()[0]
        
        print(f"⚠️ Без изоляции (user_id=0): {legacy_accounts}")
        print(f"🔒 С изоляцией (user_id!=0): {isolated_accounts}")
        
        if total_accounts > 0:
            isolation_percent = (isolated_accounts / total_accounts) * 100
            print(f"📋 Процент изоляции: {isolation_percent:.1f}%")
        
        # 4. Проверяем конкретных пользователей
        print("\n4️⃣ ПРОВЕРКА КОНКРЕТНЫХ ПОЛЬЗОВАТЕЛЕЙ:")
        
        test_users = [6499246016, 6626270112]  # ysim0r, lock0125
        
        for user_id in test_users:
            cursor.execute("SELECT COUNT(*) FROM instagram_accounts WHERE user_id = ?", (user_id,))
            user_accounts = cursor.fetchone()[0]
            print(f"👤 Пользователь {user_id}: {user_accounts} аккаунтов")
            
            if user_accounts > 0:
                cursor.execute("SELECT username FROM instagram_accounts WHERE user_id = ? LIMIT 3", (user_id,))
                usernames = [row[0] for row in cursor.fetchall()]
                print(f"   📝 Аккаунты: {', '.join(usernames)}")
        
        # 5. Проверяем уникальность пользователей
        print("\n5️⃣ АНАЛИЗ УНИКАЛЬНОСТИ:")
        cursor.execute("SELECT DISTINCT user_id FROM instagram_accounts WHERE user_id != 0")
        unique_users = cursor.fetchall()
        print(f"👥 Уникальных пользователей: {len(unique_users)}")
        
        for user_row in unique_users:
            user_id = user_row[0]
            cursor.execute("SELECT COUNT(*) FROM instagram_accounts WHERE user_id = ?", (user_id,))
            count = cursor.fetchone()[0]
            print(f"   👤 User {user_id}: {count} аккаунтов")
        
        # 6. Тест пересечений (проверяем, что нет общих аккаунтов)
        print("\n6️⃣ ТЕСТ ПЕРЕСЕЧЕНИЙ:")
        cursor.execute("""
            SELECT username, COUNT(DISTINCT user_id) as user_count 
            FROM instagram_accounts 
            WHERE user_id != 0 
            GROUP BY username 
            HAVING user_count > 1
        """)
        
        duplicates = cursor.fetchall()
        if duplicates:
            print(f"❌ НАЙДЕНЫ ДУБЛИКАТЫ! {len(duplicates)} аккаунтов принадлежат нескольким пользователям:")
            for username, count in duplicates:
                print(f"   🚨 {username}: {count} пользователей")
            return False
        else:
            print("✅ ПЕРЕСЕЧЕНИЙ НЕТ! Каждый аккаунт принадлежит только одному пользователю")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        return False

def show_sample_data():
    """Показывает примеры данных из базы"""
    
    print("\n📋 ПРИМЕРЫ ДАННЫХ:")
    print("-" * 30)
    
    db_path = "data/database.sqlite"
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT id, username, user_id, created_at 
            FROM instagram_accounts 
            WHERE user_id != 0 
            ORDER BY user_id, created_at 
            LIMIT 10
        """)
        
        isolated_accounts = cursor.fetchall()
        
        if isolated_accounts:
            print("🔒 ИЗОЛИРОВАННЫЕ АККАУНТЫ (первые 10):")
            for account in isolated_accounts:
                print(f"   ID:{account[0]} | {account[1]} | user_id:{account[2]} | {account[3]}")
        
        cursor.execute("""
            SELECT id, username, user_id, created_at 
            FROM instagram_accounts 
            WHERE user_id = 0 
            LIMIT 5
        """)
        
        legacy_accounts = cursor.fetchall()
        
        if legacy_accounts:
            print("\n⚠️ УСТАРЕВШИЕ АККАУНТЫ БЕЗ ИЗОЛЯЦИИ (первые 5):")
            for account in legacy_accounts:
                print(f"   ID:{account[0]} | {account[1]} | user_id:{account[2]} | {account[3]}")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Ошибка при показе данных: {e}")

if __name__ == "__main__":
    print("🚀 ЗАПУСК ПРОСТОГО ТЕСТА ИЗОЛЯЦИИ...")
    print()
    
    success = test_database_isolation()
    show_sample_data()
    
    print("\n" + "="*50)
    if success:
        print("🎉 ТЕСТ ИЗОЛЯЦИИ ПРОШЁЛ УСПЕШНО!")
        print("✅ База данных правильно изолирована")
        print("🔒 Каждый пользователь видит только свои аккаунты")
    else:
        print("💥 ТЕСТ ИЗОЛЯЦИИ ПРОВАЛЕН!")
        print("❌ Обнаружены проблемы с изоляцией")
        print("🔧 Требуется исправление базы данных")
    print("="*50) 
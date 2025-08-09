#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Скрипт для исправления схемы базы данных
"""

import sqlite3
import os
from pathlib import Path

# Путь к базе данных
DB_PATH = Path("data/database.sqlite")

def fix_database_schema():
    """Добавляет недостающие колонки в базу данных"""
    
    if not DB_PATH.exists():
        print(f"❌ База данных не найдена: {DB_PATH}")
        return False
    
    try:
        # Подключаемся к базе данных
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Проверяем, существует ли колонка device_id
        cursor.execute("PRAGMA table_info(instagram_accounts)")
        columns = [column[1] for column in cursor.fetchall()]
        
        if 'device_id' not in columns:
            print("📝 Добавляем колонку device_id в таблицу instagram_accounts...")
            cursor.execute("ALTER TABLE instagram_accounts ADD COLUMN device_id VARCHAR(255)")
            conn.commit()
            print("✅ Колонка device_id успешно добавлена")
        else:
            print("✅ Колонка device_id уже существует")
        
        # Проверяем другие таблицы
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        print(f"\n📊 Найдено таблиц в БД: {len(tables)}")
        for table in tables:
            print(f"  - {table[0]}")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Ошибка при обновлении схемы БД: {e}")
        return False

if __name__ == "__main__":
    print("🔧 Запуск исправления схемы базы данных...")
    if fix_database_schema():
        print("\n✅ Схема базы данных успешно обновлена!")
    else:
        print("\n❌ Не удалось обновить схему базы данных") 
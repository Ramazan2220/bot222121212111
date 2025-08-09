#!/usr/bin/env python3
import sqlite3

def fix_database():
    conn = sqlite3.connect('database/bot.db')
    cursor = conn.cursor()
    
    # Добавляем отсутствующую колонку device_id
    try:
        cursor.execute('ALTER TABLE instagram_accounts ADD COLUMN device_id TEXT')
        print('Колонка device_id добавлена')
    except Exception as e:
        print(f'Колонка уже существует или ошибка: {e}')
    
    # Проверяем структуру таблицы
    cursor.execute('PRAGMA table_info(instagram_accounts)')
    columns = cursor.fetchall()
    print('Колонки в таблице instagram_accounts:')
    for col in columns:
        print(f'  {col[1]} ({col[2]})')
    
    conn.close()
    print('Готово!')

if __name__ == "__main__":
    fix_database() 
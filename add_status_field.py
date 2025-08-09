#!/usr/bin/env python3
"""
Добавление поля status в таблицу instagram_accounts
"""

import sqlite3
import logging
import os

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def add_status_field():
    """Добавляет поле status в таблицу instagram_accounts"""
    
    db_path = 'data/database.sqlite'
    
    if not os.path.exists(db_path):
        logger.error(f"База данных не найдена: {db_path}")
        return False
    
    try:
        # Подключение к базе данных
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Проверяем, есть ли уже поле status
        cursor.execute("PRAGMA table_info(instagram_accounts);")
        columns = [column[1] for column in cursor.fetchall()]
        
        if 'status' in columns:
            logger.info("Поле 'status' уже существует в таблице instagram_accounts")
            return True
        
        logger.info("Добавляем поле 'status' в таблицу instagram_accounts...")
        
        # Добавляем поле status
        cursor.execute("ALTER TABLE instagram_accounts ADD COLUMN status VARCHAR(50) DEFAULT 'active'")
        
        # Обновляем все существующие записи
        cursor.execute("UPDATE instagram_accounts SET status = 'active' WHERE status IS NULL")
        
        # Сохраняем изменения
        conn.commit()
        
        logger.info("Поле 'status' успешно добавлено!")
        
        # Проверяем результат
        cursor.execute("PRAGMA table_info(instagram_accounts);")
        columns = [column[1] for column in cursor.fetchall()]
        
        if 'status' in columns:
            logger.info("Миграция завершена успешно!")
            return True
        else:
            logger.error("Поле 'status' не было добавлено")
            return False
            
    except Exception as e:
        logger.error(f"Ошибка при выполнении миграции: {e}")
        return False
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    success = add_status_field()
    if success:
        print("✅ Миграция выполнена успешно!")
    else:
        print("❌ Ошибка при выполнении миграции!") 
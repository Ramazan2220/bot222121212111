#!/usr/bin/env python3
"""
🔒 Миграция для добавления изоляции пользователей
Добавляет поле user_id в таблицу instagram_accounts
"""

import sqlite3
import logging
import os
from datetime import datetime

logger = logging.getLogger(__name__)

def migrate_add_user_isolation():
    """Добавляет поле user_id для изоляции данных пользователей"""
    
    db_path = "data/database.sqlite"
    
    if not os.path.exists(db_path):
        logger.error(f"❌ База данных не найдена: {db_path}")
        return False
    
    try:
        # Подключаемся к базе данных
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Проверяем, есть ли уже столбец user_id
        cursor.execute("PRAGMA table_info(instagram_accounts)")
        columns = [column[1] for column in cursor.fetchall()]
        
        if 'user_id' in columns:
            logger.info("✅ Столбец user_id уже существует")
            return True
        
        logger.info("🔄 Начинаем миграцию добавления user_id...")
        
        # Добавляем столбец user_id
        cursor.execute("""
            ALTER TABLE instagram_accounts 
            ADD COLUMN user_id INTEGER DEFAULT 0 NOT NULL
        """)
        
        # Создаём индекс для быстрого поиска по пользователям
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_instagram_accounts_user_id 
            ON instagram_accounts(user_id)
        """)
        
        # Получаем количество существующих аккаунтов
        cursor.execute("SELECT COUNT(*) FROM instagram_accounts")
        total_accounts = cursor.fetchone()[0]
        
        if total_accounts > 0:
            logger.warning(f"⚠️ Обнаружено {total_accounts} существующих аккаунтов БЕЗ изоляции!")
            logger.warning("🚨 КРИТИЧНО: Все существующие аккаунты будут назначены user_id=0")
            logger.warning("📋 ДЕЙСТВИЕ ТРЕБУЕТСЯ: Вручную назначьте правильные user_id для существующих аккаунтов")
            
            # Сохраняем бэкап существующих аккаунтов
            cursor.execute("""
                SELECT id, username, email, created_at 
                FROM instagram_accounts 
                ORDER BY created_at
            """)
            existing_accounts = cursor.fetchall()
            
            backup_file = f"data/accounts_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
            with open(backup_file, 'w', encoding='utf-8') as f:
                f.write("# Бэкап аккаунтов Instagram перед миграцией изоляции\n")
                f.write(f"# Дата: {datetime.now()}\n")
                f.write("# Формат: ID | Username | Email | Created_At\n\n")
                
                for account in existing_accounts:
                    f.write(f"{account[0]} | {account[1]} | {account[2] or 'N/A'} | {account[3]}\n")
            
            logger.info(f"💾 Бэкап сохранён в: {backup_file}")
        
        # Сохраняем изменения
        conn.commit()
        conn.close()
        
        logger.info("✅ Миграция изоляции пользователей завершена успешно!")
        logger.info("🔒 Теперь все новые аккаунты будут изолированы по пользователям")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Ошибка миграции: {e}")
        if conn:
            conn.rollback()
            conn.close()
        return False

if __name__ == "__main__":
    # Настройка логирования
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    logger.info("🚀 Запуск миграции изоляции пользователей...")
    success = migrate_add_user_isolation()
    
    if success:
        logger.info("🎉 Миграция завершена успешно!")
        print("\n" + "="*60)
        print("✅ МИГРАЦИЯ ИЗОЛЯЦИИ ПОЛЬЗОВАТЕЛЕЙ ЗАВЕРШЕНА")
        print("="*60)
        print("🔒 Добавлено поле user_id в таблицу instagram_accounts")
        print("📊 Создан индекс для быстрого поиска по пользователям")
        print("💾 Создан бэкап существующих аккаунтов")
        print("\n⚠️  ВАЖНО:")
        print("1. Все существующие аккаунты назначены user_id=0")
        print("2. Вручную назначьте правильные user_id для существующих аккаунтов")
        print("3. Обновите код для фильтрации по user_id")
        print("="*60)
    else:
        logger.error("💥 Миграция провалена!")
        print("\n❌ ОШИБКА МИГРАЦИИ!")
        print("Проверьте логи для деталей.") 
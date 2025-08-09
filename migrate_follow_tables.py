#!/usr/bin/env python3
"""
Скрипт миграции базы данных для добавления таблиц автоподписок
"""

import logging
from sqlalchemy import inspect
from database.db_manager import engine, init_db
from database.models import Base, FollowTask, FollowHistory

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def migrate_database():
    """Создать новые таблицы для автоподписок"""
    try:
        logger.info("🔄 Начало миграции базы данных...")
        
        # Создаем инспектор для проверки существующих таблиц
        inspector = inspect(engine)
        existing_tables = inspector.get_table_names()
        
        # Проверяем, существуют ли уже таблицы
        tables_to_create = []
        
        if 'follow_tasks' not in existing_tables:
            tables_to_create.append('follow_tasks')
            logger.info("📋 Таблица follow_tasks будет создана")
        else:
            logger.info("✅ Таблица follow_tasks уже существует")
        
        if 'follow_history' not in existing_tables:
            tables_to_create.append('follow_history')
            logger.info("📋 Таблица follow_history будет создана")
        else:
            logger.info("✅ Таблица follow_history уже существует")
        
        # Создаем таблицы
        if tables_to_create:
            # Создаем только новые таблицы
            Base.metadata.create_all(engine, checkfirst=True)
            logger.info(f"✅ Созданы таблицы: {', '.join(tables_to_create)}")
        else:
            logger.info("ℹ️ Все таблицы уже существуют")
        
        # Проверяем результат
        inspector = inspect(engine)
        new_tables = inspector.get_table_names()
        
        if 'follow_tasks' in new_tables and 'follow_history' in new_tables:
            logger.info("✅ Миграция успешно завершена!")
            
            # Выводим информацию о структуре таблиц
            logger.info("\n📊 Структура таблицы follow_tasks:")
            columns = inspector.get_columns('follow_tasks')
            for col in columns:
                logger.info(f"  - {col['name']}: {col['type']}")
            
            logger.info("\n📊 Структура таблицы follow_history:")
            columns = inspector.get_columns('follow_history')
            for col in columns:
                logger.info(f"  - {col['name']}: {col['type']}")
        else:
            logger.error("❌ Не все таблицы были созданы")
            
    except Exception as e:
        logger.error(f"❌ Ошибка при миграции: {e}")
        raise


if __name__ == '__main__':
    try:
        # Инициализируем базу данных (создаст все недостающие таблицы)
        init_db()
        
        # Выполняем миграцию
        migrate_database()
        
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        exit(1) 
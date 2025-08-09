#!/usr/bin/env python3
"""
Скрипт для пересоздания таблиц автоподписок с новыми полями
"""

import os
import sys
from sqlalchemy import create_engine, MetaData, text
from database.models import Base, FollowTask, FollowHistory
from database.db_manager import init_db
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def recreate_follow_tables():
    """Пересоздаем таблицы follow_tasks и follow_history"""
    
    # Получаем путь к базе данных
    DATABASE_URL = os.getenv('DATABASE_URL', 'sqlite:///instagram_bot.db')
    engine = create_engine(DATABASE_URL)
    
    try:
        # Удаляем старые таблицы если они существуют
        logger.info("🗑️ Удаление старых таблиц...")
        with engine.connect() as conn:
            # Проверяем существование таблиц
            result = conn.execute(text(
                "SELECT name FROM sqlite_master WHERE type='table' AND name IN ('follow_tasks', 'follow_history')"
            ))
            existing_tables = [row[0] for row in result]
            
            # Удаляем существующие таблицы
            for table_name in existing_tables:
                logger.info(f"  Удаление таблицы {table_name}")
                conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
                conn.commit()
        
        # Создаем таблицы заново
        logger.info("📋 Создание новых таблиц...")
        
        # Создаем только таблицы follow_tasks и follow_history
        metadata = MetaData()
        for table in Base.metadata.tables.values():
            if table.name in ['follow_tasks', 'follow_history']:
                logger.info(f"  Создание таблицы {table.name}")
                table.create(bind=engine, checkfirst=True)
        
        # Проверяем структуру созданных таблиц
        logger.info("\n✅ Таблицы успешно созданы!")
        logger.info("\n📊 Структура новых таблиц:")
        
        with engine.connect() as conn:
            # Проверяем follow_tasks
            result = conn.execute(text("PRAGMA table_info(follow_tasks)"))
            columns = result.fetchall()
            
            logger.info("\n📋 Таблица follow_tasks:")
            for col in columns:
                logger.info(f"  - {col[1]}: {col[2]}")
            
            # Проверяем follow_history
            result = conn.execute(text("PRAGMA table_info(follow_history)"))
            columns = result.fetchall()
            
            logger.info("\n📋 Таблица follow_history:")
            for col in columns:
                logger.info(f"  - {col[1]}: {col[2]}")
        
        logger.info("\n✅ Все готово! Таблицы автоподписок пересозданы с новыми полями.")
        
    except Exception as e:
        logger.error(f"❌ Ошибка при пересоздании таблиц: {e}")
        raise

if __name__ == "__main__":
    try:
        recreate_follow_tables()
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
        sys.exit(1) 
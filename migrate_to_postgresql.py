#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Миграция с SQLite на PostgreSQL
"""

import os
import logging
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Конфигурация PostgreSQL
POSTGRES_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': os.getenv('POSTGRES_PORT', '5432'),
    'database': os.getenv('POSTGRES_DB', 'instagram_bot'),
    'user': os.getenv('POSTGRES_USER', 'instagram_bot'),
    'password': os.getenv('POSTGRES_PASSWORD', 'secure_password')
}

def create_postgres_database():
    """Создать базу данных PostgreSQL если не существует"""
    try:
        # Подключаемся к PostgreSQL
        conn = psycopg2.connect(
            host=POSTGRES_CONFIG['host'],
            port=POSTGRES_CONFIG['port'],
            user=POSTGRES_CONFIG['user'],
            password=POSTGRES_CONFIG['password'],
            database='postgres'  # Подключаемся к системной БД
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        # Проверяем существование БД
        cursor.execute(
            "SELECT 1 FROM pg_database WHERE datname = %s",
            (POSTGRES_CONFIG['database'],)
        )
        
        if not cursor.fetchone():
            # Создаем БД
            cursor.execute(f"CREATE DATABASE {POSTGRES_CONFIG['database']}")
            logger.info(f"✅ База данных {POSTGRES_CONFIG['database']} создана")
        else:
            logger.info(f"База данных {POSTGRES_CONFIG['database']} уже существует")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"❌ Ошибка создания БД: {e}")
        raise

def migrate_data():
    """Мигрировать данные из SQLite в PostgreSQL"""
    try:
        # Подключение к SQLite
        sqlite_engine = create_engine('sqlite:///data/database.sqlite')
        SqliteSession = sessionmaker(bind=sqlite_engine)
        sqlite_session = SqliteSession()
        
        # Подключение к PostgreSQL
        postgres_url = f"postgresql://{POSTGRES_CONFIG['user']}:{POSTGRES_CONFIG['password']}@{POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}/{POSTGRES_CONFIG['database']}"
        postgres_engine = create_engine(postgres_url)
        
        # Импортируем модели
        from database.models import Base, InstagramAccount, Proxy, PublishTask, TelegramUser, Setting
        
        # Создаем таблицы в PostgreSQL
        Base.metadata.create_all(postgres_engine)
        logger.info("✅ Таблицы созданы в PostgreSQL")
        
        PostgresSession = sessionmaker(bind=postgres_engine)
        postgres_session = PostgresSession()
        
        # Мигрируем данные по таблицам
        tables_to_migrate = [
            (Proxy, "прокси"),
            (InstagramAccount, "аккаунты"),
            (PublishTask, "задачи"),
            (TelegramUser, "пользователи"),
            (Setting, "настройки")
        ]
        
        for model, name in tables_to_migrate:
            logger.info(f"📋 Миграция {name}...")
            
            # Получаем все записи из SQLite
            records = sqlite_session.query(model).all()
            
            # Копируем в PostgreSQL
            for record in records:
                # Создаем новый объект с теми же данными
                new_record = model()
                for column in model.__table__.columns:
                    setattr(new_record, column.name, getattr(record, column.name))
                
                postgres_session.add(new_record)
            
            postgres_session.commit()
            logger.info(f"✅ Мигрировано {len(records)} записей {name}")
        
        sqlite_session.close()
        postgres_session.close()
        
        logger.info("✅ Миграция завершена успешно!")
        
        # Обновляем config.py
        config_update = f"""
# Обновите DATABASE_URL в config.py:
DATABASE_URL = "postgresql://{POSTGRES_CONFIG['user']}:{POSTGRES_CONFIG['password']}@{POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}/{POSTGRES_CONFIG['database']}"
"""
        logger.info(config_update)
        
    except Exception as e:
        logger.error(f"❌ Ошибка миграции: {e}")
        raise

if __name__ == "__main__":
    print("🚀 МИГРАЦИЯ НА PostgreSQL")
    print("\nПеред началом убедитесь, что:")
    print("1. PostgreSQL установлен и запущен")
    print("2. Создан пользователь с правами создания БД")
    print("3. Установлен psycopg2: pip install psycopg2-binary")
    print("4. Сделан бэкап SQLite БД")
    
    response = input("\nПродолжить? (yes/no): ")
    
    if response.lower() == 'yes':
        create_postgres_database()
        migrate_data()
    else:
        print("❌ Операция отменена") 
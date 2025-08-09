#!/usr/bin/env python3
"""
Скрипт миграции для добавления таблиц групп аккаунтов
"""

import sys
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# Добавляем путь к проекту
sys.path.append('.')

from config import DATABASE_URL
from database.models import Base, AccountGroup

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def migrate_database():
    """Добавляет таблицы для групп аккаунтов"""
    try:
        # Создаем подключение к БД
        engine = create_engine(DATABASE_URL)
        
        # Создаем таблицы (если их еще нет)
        logger.info("Создание таблиц групп...")
        Base.metadata.create_all(engine)
        
        # Проверяем, что таблицы созданы
        Session = sessionmaker(bind=engine)
        session = Session()
        
        # Проверяем таблицу групп
        result = session.execute(text("SELECT name FROM sqlite_master WHERE type='table' AND name='account_groups_table'"))
        if result.fetchone():
            logger.info("✅ Таблица account_groups_table создана успешно")
        else:
            logger.error("❌ Таблица account_groups_table не создана")
            return False
        
        # Проверяем связующую таблицу
        result = session.execute(text("SELECT name FROM sqlite_master WHERE type='table' AND name='account_groups'"))
        if result.fetchone():
            logger.info("✅ Таблица account_groups создана успешно")
        else:
            logger.error("❌ Таблица account_groups не создана")
            return False
        
        # Создаем группы по умолчанию
        logger.info("Создание групп по умолчанию...")
        
        default_groups = [
            ("🔥 Активные", "Активные рабочие аккаунты", "🔥"),
            ("🆕 Новые", "Новые аккаунты для прогрева", "🆕"),
            ("⚠️ Проблемные", "Аккаунты с ограничениями", "⚠️"),
            ("💎 VIP", "Важные аккаунты", "💎"),
            ("🧪 Тестовые", "Аккаунты для тестирования", "🧪"),
        ]
        
        for name, description, icon in default_groups:
            # Проверяем, существует ли уже такая группа
            existing = session.query(AccountGroup).filter_by(name=name).first()
            if not existing:
                group = AccountGroup(name=name, description=description, icon=icon)
                session.add(group)
                logger.info(f"  + Создана группа: {icon} {name}")
            else:
                logger.info(f"  - Группа уже существует: {icon} {name}")
        
        session.commit()
        session.close()
        
        logger.info("\n✅ Миграция завершена успешно!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Ошибка при миграции: {e}")
        return False

if __name__ == "__main__":
    if migrate_database():
        print("\n✅ База данных успешно обновлена для поддержки групп аккаунтов!")
        print("\nТеперь вы можете:")
        print("1. Создавать группы аккаунтов")
        print("2. Добавлять аккаунты в группы")
        print("3. Управлять аккаунтами по группам")
        print("\nЗапустите бота и используйте меню 'Аккаунты' -> 'Группы аккаунтов'")
    else:
        print("\n❌ Ошибка при обновлении базы данных")
        sys.exit(1) 
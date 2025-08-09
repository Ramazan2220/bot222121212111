#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
🐘 МИГРАЦИЯ НА SUPABASE PostgreSQL
Переносим все данные из SQLite в облачный PostgreSQL
"""

import logging
import os
import sqlite3
import sys
from datetime import datetime
from typing import Dict, List, Any

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    from sqlalchemy import create_engine, text
    from sqlalchemy.orm import sessionmaker
    
    # Импорты нашей системы
    from database.models import Base, InstagramAccount, PublishTask, WarmupTask, TelegramUser
    from database.db_manager import DATABASE_URL as SQLITE_URL
    
except ImportError as e:
    logger.error(f"❌ Ошибка импорта: {e}")
    logger.error("💡 Убедитесь что установлен psycopg2-binary")
    sys.exit(1)

class SupabaseMigration:
    """Миграция данных в Supabase PostgreSQL"""
    
    def __init__(self, postgres_url: str):
        self.postgres_url = postgres_url
        self.sqlite_url = SQLITE_URL
        self.migration_stats = {
            'tables_created': 0,
            'data_migrated': {},
            'errors': [],
            'start_time': datetime.now()
        }
        
    def validate_connection_strings(self):
        """Проверка строк подключения"""
        logger.info("🔗 Проверка подключений...")
        
        # Проверяем SQLite
        try:
            if self.sqlite_url.startswith('sqlite:///'):
                db_file = self.sqlite_url.replace('sqlite:///', '')
                if not os.path.exists(db_file):
                    logger.error(f"❌ SQLite файл не найден: {db_file}")
                    return False
                logger.info(f"✅ SQLite найден: {db_file}")
            else:
                logger.error(f"❌ Неверный формат SQLite URL: {self.sqlite_url}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Ошибка проверки SQLite: {e}")
            return False
            
        # Проверяем PostgreSQL URL
        if not self.postgres_url.startswith('postgresql://'):
            logger.error(f"❌ Неверный формат PostgreSQL URL")
            logger.info("💡 Формат должен быть: postgresql://user:password@host:port/database")
            return False
            
        logger.info("✅ Строки подключения валидны")
        return True
        
    def test_postgres_connection(self):
        """Тест подключения к PostgreSQL"""
        logger.info("🐘 Тестирование подключения к PostgreSQL...")
        
        try:
            # Тестируем через psycopg2
            conn = psycopg2.connect(self.postgres_url)
            with conn.cursor() as cursor:
                cursor.execute("SELECT version();")
                version = cursor.fetchone()[0]
                logger.info(f"✅ PostgreSQL подключен: {version}")
                
            conn.close()
            
            # Тестируем через SQLAlchemy
            engine = create_engine(self.postgres_url)
            with engine.connect() as connection:
                result = connection.execute(text("SELECT current_database();"))
                db_name = result.fetchone()[0]
                logger.info(f"✅ SQLAlchemy подключен к БД: {db_name}")
                
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка подключения к PostgreSQL: {e}")
            logger.info("💡 Проверьте:")
            logger.info("   - URL правильный")
            logger.info("   - База данных существует") 
            logger.info("   - Пользователь имеет права")
            logger.info("   - Сеть доступна")
            return False
            
    def create_postgres_schema(self):
        """Создание схемы в PostgreSQL"""
        logger.info("🏗️ Создание схемы PostgreSQL...")
        
        try:
            # Создаем engine для PostgreSQL
            engine = create_engine(self.postgres_url)
            
            # Создаем все таблицы
            Base.metadata.create_all(engine)
            
            logger.info("✅ Схема PostgreSQL создана")
            self.migration_stats['tables_created'] = len(Base.metadata.tables)
            logger.info(f"📊 Создано таблиц: {self.migration_stats['tables_created']}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка создания схемы: {e}")
            self.migration_stats['errors'].append(f"Schema creation: {e}")
            return False
            
    def get_sqlite_data(self, table_name: str) -> List[Dict[str, Any]]:
        """Получение данных из SQLite"""
        try:
            # Подключаемся к SQLite
            sqlite_engine = create_engine(self.sqlite_url)
            
            with sqlite_engine.connect() as connection:
                # Получаем все данные из таблицы
                result = connection.execute(text(f"SELECT * FROM {table_name}"))
                rows = result.fetchall()
                
                if rows:
                    # Преобразуем в список словарей
                    columns = result.keys()
                    data = [dict(zip(columns, row)) for row in rows]
                    logger.info(f"📊 Получено {len(data)} записей из {table_name}")
                    return data
                else:
                    logger.info(f"📊 Таблица {table_name} пуста")
                    return []
                    
        except Exception as e:
            logger.error(f"❌ Ошибка чтения {table_name}: {e}")
            return []
            
    def migrate_table_data(self, table_name: str, model_class):
        """Миграция данных одной таблицы"""
        logger.info(f"🔄 Миграция таблицы: {table_name}")
        
        try:
            # Получаем данные из SQLite
            sqlite_data = self.get_sqlite_data(table_name)
            
            if not sqlite_data:
                logger.info(f"⏭️ Пропускаем пустую таблицу: {table_name}")
                self.migration_stats['data_migrated'][table_name] = 0
                return True
                
            # Подключаемся к PostgreSQL
            postgres_engine = create_engine(self.postgres_url)
            Session = sessionmaker(bind=postgres_engine)
            
            migrated_count = 0
            
            with Session() as session:
                for row_data in sqlite_data:
                    try:
                        # Создаем объект модели
                        obj = model_class(**row_data)
                        session.add(obj)
                        migrated_count += 1
                        
                        # Коммитим батчами по 100 записей
                        if migrated_count % 100 == 0:
                            session.commit()
                            logger.info(f"✅ Мигрировано {migrated_count}/{len(sqlite_data)} записей")
                            
                    except Exception as e:
                        logger.warning(f"⚠️ Ошибка миграции записи: {e}")
                        session.rollback()
                        continue
                        
                # Финальный коммит
                session.commit()
                
            logger.info(f"✅ Таблица {table_name}: {migrated_count}/{len(sqlite_data)} записей мигрировано")
            self.migration_stats['data_migrated'][table_name] = migrated_count
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка миграции {table_name}: {e}")
            self.migration_stats['errors'].append(f"{table_name}: {e}")
            return False
            
    def verify_migration(self):
        """Проверка успешности миграции"""
        logger.info("🔍 Проверка результатов миграции...")
        
        try:
            # Подключаемся к PostgreSQL
            postgres_engine = create_engine(self.postgres_url)
            
            with postgres_engine.connect() as connection:
                # Проверяем каждую мигрированную таблицу
                for table_name, expected_count in self.migration_stats['data_migrated'].items():
                    result = connection.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                    actual_count = result.fetchone()[0]
                    
                    if actual_count == expected_count:
                        logger.info(f"✅ {table_name}: {actual_count} записей ОК")
                    else:
                        logger.error(f"❌ {table_name}: ожидали {expected_count}, получили {actual_count}")
                        
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка проверки: {e}")
            return False
            
    def print_migration_summary(self):
        """Вывод итогов миграции"""
        end_time = datetime.now()
        duration = (end_time - self.migration_stats['start_time']).total_seconds()
        
        logger.info("=" * 80)
        logger.info("🐘 ИТОГИ МИГРАЦИИ НА SUPABASE PostgreSQL")
        logger.info("=" * 80)
        
        logger.info(f"⏱️ Время выполнения: {duration:.2f} секунд")
        logger.info(f"🏗️ Создано таблиц: {self.migration_stats['tables_created']}")
        
        logger.info("📊 Мигрированные данные:")
        total_records = 0
        for table_name, count in self.migration_stats['data_migrated'].items():
            logger.info(f"   {table_name}: {count} записей")
            total_records += count
            
        logger.info(f"📈 Всего записей: {total_records}")
        
        if self.migration_stats['errors']:
            logger.warning(f"⚠️ Ошибки: {len(self.migration_stats['errors'])}")
            for error in self.migration_stats['errors']:
                logger.warning(f"   - {error}")
        else:
            logger.info("✅ Ошибок нет!")
            
        logger.info("=" * 80)
        
    def run_full_migration(self):
        """Запуск полной миграции"""
        logger.info("🚀" * 80)
        logger.info("🚀 НАЧАЛО МИГРАЦИИ НА SUPABASE PostgreSQL")
        logger.info("🚀" * 80)
        
        # 1. Проверка подключений
        if not self.validate_connection_strings():
            logger.error("💥 Миграция прервана: неверные строки подключения")
            return False
            
        if not self.test_postgres_connection():
            logger.error("💥 Миграция прервана: не удается подключиться к PostgreSQL")
            return False
            
        # 2. Создание схемы
        if not self.create_postgres_schema():
            logger.error("💥 Миграция прервана: не удается создать схему")
            return False
            
        # 3. Миграция данных
        tables_to_migrate = [
            ('telegram_users', TelegramUser),
            ('instagram_accounts', InstagramAccount),
            ('publish_tasks', PublishTask),
            ('warmup_tasks', WarmupTask),
        ]
        
        success_count = 0
        
        for table_name, model_class in tables_to_migrate:
            if self.migrate_table_data(table_name, model_class):
                success_count += 1
                
        # 4. Проверка результатов
        self.verify_migration()
        
        # 5. Итоги
        self.print_migration_summary()
        
        if success_count == len(tables_to_migrate):
            logger.info("🏆 МИГРАЦИЯ ЗАВЕРШЕНА УСПЕШНО!")
            logger.info("🎉 Теперь можно переключить систему на PostgreSQL!")
            return True
        else:
            logger.error("💥 МИГРАЦИЯ ЗАВЕРШЕНА С ОШИБКАМИ!")
            logger.error("🔧 Проверьте логи и исправьте проблемы")
            return False

def main():
    """Главная функция миграции"""
    print("🐘 МИГРАЦИЯ НА SUPABASE PostgreSQL")
    print("=" * 50)
    
    # Получаем URL PostgreSQL от пользователя
    postgres_url = input("Введите PostgreSQL URL от Supabase: ").strip()
    
    if not postgres_url:
        logger.error("❌ PostgreSQL URL не может быть пустым")
        return
        
    # Запускаем миграцию
    migration = SupabaseMigration(postgres_url)
    success = migration.run_full_migration()
    
    if success:
        print("\n🎉 СЛЕДУЮЩИЕ ШАГИ:")
        print("1. Обновите config.py:")
        print(f"   DATABASE_URL = '{postgres_url}'")
        print("2. Перезапустите систему")
        print("3. Проведите тесты изоляции на PostgreSQL")
        exit(0)
    else:
        print("\n💥 МИГРАЦИЯ НЕ УДАЛАСЬ!")
        print("Проверьте логи выше для деталей")
        exit(1)

if __name__ == "__main__":
    main() 
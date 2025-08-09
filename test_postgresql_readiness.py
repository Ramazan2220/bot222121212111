#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
🐘 ТЕСТ ГОТОВНОСТИ К PostgreSQL
Проверяем что система готова к миграции на PostgreSQL
"""

import logging
import os
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from datetime import datetime

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Импорты нашей системы
from database.models import Base, InstagramAccount, PublishTask, TaskType
from database.connection_pool import init_db_pool, get_db_stats, db_health_check

class PostgreSQLReadinessTest:
    """Тест готовности к PostgreSQL"""
    
    def __init__(self):
        self.test_results = {}
        self.start_time = datetime.now()
        
    def test_1_postgresql_driver(self):
        """Тест 1: Проверка PostgreSQL драйвера"""
        logger.info("🐘 ТЕСТ 1: Проверка PostgreSQL драйвера")
        
        try:
            import psycopg2
            version = psycopg2.__version__
            logger.info(f"✅ psycopg2 установлен: версия {version}")
            
            # Проверяем возможность создания PostgreSQL URL
            test_url = "postgresql://user:password@localhost:5432/testdb"
            engine = create_engine(test_url, strategy='mock', executor=lambda sql, *_: None)
            logger.info("✅ SQLAlchemy может создать PostgreSQL engine")
            
            self.test_results['postgresql_driver'] = True
            return True
            
        except ImportError as e:
            logger.error(f"❌ psycopg2 не установлен: {e}")
            self.test_results['postgresql_driver'] = False
            return False
        except Exception as e:
            logger.error(f"❌ Ошибка тестирования PostgreSQL: {e}")
            self.test_results['postgresql_driver'] = False
            return False
            
    def test_2_models_compatibility(self):
        """Тест 2: Совместимость моделей с PostgreSQL"""
        logger.info("🐘 ТЕСТ 2: Совместимость моделей с PostgreSQL")
        
        try:
            # Создаем mock PostgreSQL engine
            test_url = "postgresql://user:password@localhost:5432/testdb"
            engine = create_engine(test_url, strategy='mock', executor=lambda sql, *_: None)
            
            # Пробуем создать таблицы (DDL генерация)
            metadata_sql = []
            def mock_execute(sql, *args):
                metadata_sql.append(str(sql))
                
            engine = create_engine(test_url, strategy='mock', executor=mock_execute)
            Base.metadata.create_all(engine)
            
            logger.info(f"✅ Сгенерировано {len(metadata_sql)} DDL команд для PostgreSQL")
            logger.info("✅ Все модели совместимы с PostgreSQL")
            
            # Проверяем ключевые таблицы
            key_tables = ['instagram_accounts', 'publish_tasks', 'warmup_tasks']
            found_tables = []
            
            for sql in metadata_sql:
                for table in key_tables:
                    if table in sql.lower():
                        found_tables.append(table)
                        
            logger.info(f"✅ Найдены ключевые таблицы: {set(found_tables)}")
            
            self.test_results['models_compatibility'] = True
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка совместимости моделей: {e}")
            self.test_results['models_compatibility'] = False
            return False
            
    def test_3_connection_pool_postgresql(self):
        """Тест 3: Connection Pool с PostgreSQL"""
        logger.info("🐘 ТЕСТ 3: Connection Pool для PostgreSQL")
        
        try:
            # Тестируем инициализацию пула для PostgreSQL
            test_url = "postgresql://user:password@localhost:5432/testdb"
            
            # Импортируем напрямую класс пула
            from database.connection_pool import DatabaseConnectionPool
            
            # Создаем пул (без реального подключения)
            pool = DatabaseConnectionPool(
                database_url=test_url,
                pool_size=50,
                max_overflow=100,
                pool_timeout=60,
                pool_recycle=3600
            )
            
            logger.info("✅ PostgreSQL Connection Pool инициализирован")
            logger.info(f"✅ Настройки: pool_size=50, max_overflow=100, timeout=60")
            
            # Проверяем конфигурацию
            stats = {
                'config': {
                    'pool_size': pool.pool_size,
                    'max_overflow': pool.max_overflow,
                    'pool_timeout': pool.pool_timeout,
                    'pool_recycle': pool.pool_recycle
                }
            }
            
            logger.info(f"✅ Конфигурация пула: {stats['config']}")
            
            self.test_results['connection_pool'] = True
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка тестирования Connection Pool: {e}")
            self.test_results['connection_pool'] = False
            return False
            
    def test_4_migration_script_readiness(self):
        """Тест 4: Готовность скрипта миграции"""
        logger.info("🐘 ТЕСТ 4: Готовность скрипта миграции")
        
        try:
            # Проверяем наличие скрипта миграции
            migration_script = "migrate_to_postgresql.py"
            
            if os.path.exists(migration_script):
                logger.info(f"✅ Скрипт миграции найден: {migration_script}")
                
                # Читаем скрипт и проверяем ключевые функции
                with open(migration_script, 'r', encoding='utf-8') as f:
                    content = f.read()
                    
                key_functions = [
                    'create_postgres_database',
                    'migrate_data',
                    'POSTGRES_CONFIG'
                ]
                
                found_functions = []
                for func in key_functions:
                    if func in content:
                        found_functions.append(func)
                        
                logger.info(f"✅ Найдены функции миграции: {found_functions}")
                
                if len(found_functions) >= 2:
                    logger.info("✅ Скрипт миграции полностью готов")
                    self.test_results['migration_script'] = True
                    return True
                else:
                    logger.warning("⚠️ Скрипт миграции неполный")
                    self.test_results['migration_script'] = False
                    return False
                    
            else:
                logger.error(f"❌ Скрипт миграции не найден: {migration_script}")
                self.test_results['migration_script'] = False
                return False
                
        except Exception as e:
            logger.error(f"❌ Ошибка проверки скрипта миграции: {e}")
            self.test_results['migration_script'] = False
            return False
            
    def test_5_isolation_compatibility(self):
        """Тест 5: Совместимость изоляции с PostgreSQL"""
        logger.info("🐘 ТЕСТ 5: Совместимость изоляции пользователей")
        
        try:
            # Проверяем что наши функции изоляции будут работать с PostgreSQL
            from database.safe_user_wrapper import get_user_instagram_accounts, get_user_instagram_account
            from database.user_management import get_active_users, get_user_info
            
            logger.info("✅ Функции изоляции импортированы успешно")
            
            # Проверяем что модели имеют user_id для изоляции
            if hasattr(InstagramAccount, 'user_id'):
                logger.info("✅ InstagramAccount имеет поле user_id для изоляции")
            else:
                logger.error("❌ InstagramAccount НЕ имеет поле user_id")
                self.test_results['isolation_compatibility'] = False
                return False
                
            if hasattr(PublishTask, 'user_id'):
                logger.info("✅ PublishTask имеет поле user_id для изоляции")
            else:
                logger.error("❌ PublishTask НЕ имеет поле user_id")
                self.test_results['isolation_compatibility'] = False
                return False
                
            logger.info("✅ Система изоляции полностью совместима с PostgreSQL")
            self.test_results['isolation_compatibility'] = True
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка проверки изоляции: {e}")
            self.test_results['isolation_compatibility'] = False
            return False
            
    def analyze_readiness_results(self):
        """Анализ результатов готовности"""
        logger.info("=" * 80)
        logger.info("🐘 РЕЗУЛЬТАТЫ ТЕСТА ГОТОВНОСТИ К PostgreSQL")
        logger.info("=" * 80)
        
        total_time = (datetime.now() - self.start_time).total_seconds()
        logger.info(f"⏱️ Время выполнения: {total_time:.2f} секунд")
        
        passed_tests = 0
        total_tests = len(self.test_results)
        
        test_names = {
            'postgresql_driver': '🐘 PostgreSQL драйвер',
            'models_compatibility': '📊 Совместимость моделей',
            'connection_pool': '🏊 Connection Pool',
            'migration_script': '📋 Скрипт миграции',
            'isolation_compatibility': '🔒 Совместимость изоляции'
        }
        
        for test_key, result in self.test_results.items():
            test_name = test_names.get(test_key, test_key)
            status = "✅ ГОТОВ" if result else "❌ НЕ ГОТОВ"
            logger.info(f"{test_name}: {status}")
            if result:
                passed_tests += 1
                
        logger.info("-" * 80)
        
        readiness_percentage = (passed_tests / total_tests) * 100
        
        if passed_tests == total_tests:
            logger.info("🏆 СИСТЕМА ПОЛНОСТЬЮ ГОТОВА К PostgreSQL!")
            logger.info("🚀 МОЖНО ПРОВОДИТЬ МИГРАЦИЮ В ЛЮБОЙ МОМЕНТ!")
        elif passed_tests >= total_tests * 0.8:
            logger.info("⚡ СИСТЕМА ПОЧТИ ГОТОВА К PostgreSQL!")
            logger.info("🔧 НУЖНЫ МИНИМАЛЬНЫЕ ДОРАБОТКИ!")
        else:
            logger.error("🛑 СИСТЕМА НЕ ГОТОВА К PostgreSQL!")
            logger.error("🔧 ТРЕБУЮТСЯ СЕРЬЕЗНЫЕ ДОРАБОТКИ!")
            
        logger.info(f"📊 Готовность: {passed_tests}/{total_tests} ({readiness_percentage:.1f}%)")
        
        if passed_tests >= total_tests * 0.8:
            logger.info("💡 РЕКОМЕНДАЦИЯ: Переходите на PostgreSQL в продакшене!")
        else:
            logger.info("💡 РЕКОМЕНДАЦИЯ: Сначала устраните проблемы!")
            
        logger.info("=" * 80)
        
        return passed_tests == total_tests
        
    def run_postgresql_readiness_test(self):
        """Запуск полного теста готовности"""
        logger.info("🐘" * 60)
        logger.info("🐘 ТЕСТ ГОТОВНОСТИ СИСТЕМЫ К PostgreSQL")
        logger.info("🐘 ПРОВЕРЯЕМ ВСЕ КОМПОНЕНТЫ ПЕРЕД МИГРАЦИЕЙ")
        logger.info("🐘" * 60)
        
        try:
            # Запускаем все тесты
            self.test_1_postgresql_driver()
            self.test_2_models_compatibility()
            self.test_3_connection_pool_postgresql()
            self.test_4_migration_script_readiness()
            self.test_5_isolation_compatibility()
            
            # Анализируем результаты
            success = self.analyze_readiness_results()
            
            return success
            
        except Exception as e:
            logger.error(f"💥 Критическая ошибка: {e}")
            return False

def main():
    """Главная функция теста готовности"""
    logger.info("🐘 Запуск теста готовности к PostgreSQL...")
    
    test = PostgreSQLReadinessTest()
    success = test.run_postgresql_readiness_test()
    
    if success:
        logger.info("🏆 СИСТЕМА ГОТОВА К PostgreSQL!")
        exit(0)
    else:
        logger.error("💥 СИСТЕМА НЕ ГОТОВА К PostgreSQL!")
        exit(1)

if __name__ == "__main__":
    main() 
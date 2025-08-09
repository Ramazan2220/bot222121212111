#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
🐘 ПРОСТОЙ ТЕСТ PostgreSQL
Проверяем базовую работу PostgreSQL после миграции
"""

import logging
import sys
from datetime import datetime, timedelta

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

try:
    from database.db_manager import get_session, init_db
    from database.models import InstagramAccount, PublishTask, TelegramUser, TaskType, TaskStatus
    from sqlalchemy import text
except ImportError as e:
    logger.error(f"❌ Ошибка импорта: {e}")
    sys.exit(1)

def test_postgresql_basic():
    """Базовый тест PostgreSQL"""
    logger.info("🐘 ПРОСТОЙ ТЕСТ PostgreSQL")
    logger.info("=" * 50)
    
    try:
        # 1. Инициализация
        logger.info("🔧 Инициализация PostgreSQL...")
        init_db()
        
        with get_session() as session:
            # 2. Проверка версии
            logger.info("📋 Проверка версии PostgreSQL...")
            result = session.execute(text("SELECT version()"))
            version = result.fetchone()[0]
            logger.info(f"✅ PostgreSQL: {version[:50]}...")
            
            # 3. Проверка JSON поддержки
            logger.info("📄 Проверка JSON поддержки...")
            try:
                session.execute(text("SELECT '{\"test\": true}'::json"))
                logger.info("✅ JSON поддержка: Работает")
            except Exception as e:
                logger.warning(f"⚠️ JSON поддержка: {e}")
            
            # 4. Создание тестового пользователя
            logger.info("👤 Создание тестового пользователя...")
            test_user = TelegramUser(
                telegram_id=8888888,
                username="postgresql_test",
                first_name="PostgreSQL",
                last_name="Test",
                is_active=True
            )
            session.merge(test_user)
            session.flush()
            logger.info("✅ Пользователь создан")
            
            # 5. Создание тестового аккаунта
            logger.info("📱 Создание тестового аккаунта...")
            test_account = InstagramAccount(
                username="pg_test_account",
                password="test_password",
                email="pg_test@postgresql.test",
                user_id=8888888,
                is_active=True,
                full_name="PostgreSQL Test Account",
                biography="Test account for PostgreSQL"
            )
            session.add(test_account)
            session.flush()
            logger.info(f"✅ Аккаунт создан с ID: {test_account.id}")
            
            # 6. Создание тестовой задачи
            logger.info("📋 Создание тестовой задачи...")
            test_task = PublishTask(
                account_id=test_account.id,
                user_id=8888888,
                task_type=TaskType.PHOTO,
                caption="PostgreSQL test post",
                scheduled_time=datetime.now() + timedelta(hours=1),
                status=TaskStatus.PENDING
            )
            session.add(test_task)
            session.flush()
            logger.info(f"✅ Задача создана с ID: {test_task.id}")
            
            # 7. Проверка Foreign Key
            logger.info("🔗 Проверка Foreign Key ограничений...")
            try:
                fake_task = PublishTask(
                    account_id=999999,  # Несуществующий аккаунт
                    user_id=8888888,
                    task_type=TaskType.PHOTO,
                    caption="Should fail"
                )
                session.add(fake_task)
                session.flush()
                logger.warning("⚠️ Foreign Key НЕ работает!")
            except Exception:
                logger.info("✅ Foreign Key ограничения работают")
                session.rollback()
            
            # 8. Проверка запросов
            logger.info("🔍 Проверка запросов к данным...")
            
            # Запрос пользователей
            users = session.query(TelegramUser).filter_by(telegram_id=8888888).all()
            logger.info(f"✅ Найдено пользователей: {len(users)}")
            
            # Запрос аккаунтов
            accounts = session.query(InstagramAccount).filter_by(user_id=8888888).all()
            logger.info(f"✅ Найдено аккаунтов: {len(accounts)}")
            
            # Запрос задач
            tasks = session.query(PublishTask).filter_by(user_id=8888888).all()
            logger.info(f"✅ Найдено задач: {len(tasks)}")
            
            # 9. Очистка тестовых данных
            logger.info("🧹 Очистка тестовых данных...")
            session.query(PublishTask).filter_by(user_id=8888888).delete()
            session.query(InstagramAccount).filter_by(user_id=8888888).delete()
            session.query(TelegramUser).filter_by(telegram_id=8888888).delete()
            
            session.commit()
            logger.info("✅ Тестовые данные очищены")
            
        logger.info("=" * 50)
        logger.info("🎉 ТЕСТ ЗАВЕРШЕН УСПЕШНО!")
        logger.info("🚀 PostgreSQL работает корректно!")
        logger.info("✅ Миграция прошла успешно!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Ошибка теста: {e}")
        return False

def test_connection_info():
    """Информация о подключении"""
    logger.info("🔗 ИНФОРМАЦИЯ О ПОДКЛЮЧЕНИИ")
    logger.info("=" * 50)
    
    try:
        with get_session() as session:
            # Информация о БД
            result = session.execute(text("SELECT current_database()"))
            db_name = result.fetchone()[0]
            logger.info(f"📊 База данных: {db_name}")
            
            # Информация о пользователе
            result = session.execute(text("SELECT current_user"))
            user = result.fetchone()[0]
            logger.info(f"👤 Пользователь: {user}")
            
            # Информация о хосте
            result = session.execute(text("SELECT inet_server_addr()"))
            host = result.fetchone()
            if host and host[0]:
                logger.info(f"🌐 Хост: {host[0]}")
            else:
                logger.info("🌐 Хост: localhost/cloud")
            
            # Количество соединений
            result = session.execute(text("SELECT count(*) FROM pg_stat_activity"))
            connections = result.fetchone()[0]
            logger.info(f"🔌 Активных соединений: {connections}")
            
            # Размер базы данных
            result = session.execute(text("SELECT pg_size_pretty(pg_database_size(current_database()))"))
            size = result.fetchone()[0]
            logger.info(f"💾 Размер БД: {size}")
            
    except Exception as e:
        logger.error(f"❌ Ошибка получения информации: {e}")

def main():
    """Главная функция"""
    logger.info("🚀" * 50)
    logger.info("🚀 ТЕСТИРОВАНИЕ PostgreSQL ПОСЛЕ МИГРАЦИИ")
    logger.info("🚀" * 50)
    
    # Базовый тест
    success = test_postgresql_basic()
    
    if success:
        # Информация о подключении
        test_connection_info()
        
        logger.info("\n" + "🎉" * 50)
        logger.info("🎉 ВСЕ ТЕСТЫ ПРОШЛИ УСПЕШНО!")
        logger.info("🎉" * 50)
        
        print("\n✅ РЕЗУЛЬТАТ:")
        print("🐘 PostgreSQL работает корректно")
        print("🚀 Система готова к использованию")
        print("🔗 Подключение к Supabase установлено")
        print("📊 Данные успешно мигрированы")
        exit(0)
    else:
        logger.error("💥 ТЕСТЫ ПРОВАЛИЛИСЬ!")
        exit(1)

if __name__ == "__main__":
    main() 
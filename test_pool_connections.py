#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
🔧 ТЕСТ ПУЛА СОЕДИНЕНИЙ
Проверяем правильность настроек и работу пула соединений
"""

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(threadName)s] - %(message)s'
)
logger = logging.getLogger(__name__)

# Импорты системы
from database.db_manager import init_db, get_session
from database.connection_pool import get_db_stats, db_health_check
from database.models import InstagramAccount

class PoolConnectionTest:
    """Тест пула соединений"""
    
    def __init__(self):
        self.start_time = datetime.now()
        self.created_accounts = []
        
    def test_pool_initialization(self):
        """Тест инициализации пула"""
        logger.info("🔧 ТЕСТ 1: Инициализация пула соединений")
        
        # Инициализируем БД
        init_db()
        logger.info("✅ База данных инициализирована")
        
        # Проверяем статистику пула
        stats = get_db_stats()
        logger.info(f"📊 Статистика пула: {stats}")
        
        # Проверяем здоровье
        health = db_health_check()
        logger.info(f"❤️ Здоровье пула: {'✅ OK' if health else '❌ FAILED'}")
        
        return health
        
    def test_concurrent_sessions(self, num_threads: int = 20):
        """Тест одновременных сессий"""
        logger.info(f"🔧 ТЕСТ 2: {num_threads} одновременных сессий")
        
        successes = 0
        errors = 0
        
        def session_worker(worker_id):
            nonlocal successes, errors
            try:
                with get_session() as session:
                    # Создаем тестовый аккаунт
                    account = InstagramAccount(
                        user_id=1000000 + worker_id,
                        username=f"pool_test_{worker_id}_{int(time.time())}",
                        password=f"test_pass_{worker_id}",
                        email=f"pool_test_{worker_id}@test.com",
                        full_name=f"Pool Test {worker_id}",
                        biography="Pool connection test account"
                    )
                    session.add(account)
                    session.commit()
                    session.refresh(account)
                    
                    self.created_accounts.append(account.id)
                    successes += 1
                    logger.info(f"✅ Воркер {worker_id}: аккаунт {account.id} создан")
                    
            except Exception as e:
                errors += 1
                logger.error(f"❌ Воркер {worker_id}: ошибка - {e}")
                
        # Запускаем воркеры
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(session_worker, i) for i in range(num_threads)]
            
            # Ждем завершения
            for future in futures:
                future.result()
                
        logger.info(f"📊 Результат: {successes} успехов, {errors} ошибок")
        
        # Показываем статистику пула
        stats = get_db_stats()
        logger.info(f"📊 Финальная статистика: {stats}")
        
        return errors == 0
        
    def test_extreme_load(self, num_threads: int = 100):
        """Тест экстремальной нагрузки"""
        logger.info(f"🔧 ТЕСТ 3: {num_threads} потоков экстремальной нагрузки")
        
        operations = 0
        errors = 0
        
        def extreme_worker(worker_id):
            nonlocal operations, errors
            
            for i in range(10):  # 10 операций на воркер
                try:
                    with get_session() as session:
                        # Простая операция чтения
                        result = session.query(InstagramAccount).filter_by(user_id=1000000 + worker_id).first()
                        operations += 1
                        
                        if i % 5 == 0:
                            logger.info(f"⚡ Воркер {worker_id}: операция {i+1}/10")
                            
                except Exception as e:
                    errors += 1
                    logger.error(f"❌ Воркер {worker_id}, операция {i}: {e}")
                    
        # Запускаем экстремальную нагрузку
        start_time = time.time()
        
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(extreme_worker, i) for i in range(num_threads)]
            
            # Ждем завершения
            for future in futures:
                future.result()
                
        end_time = time.time()
        
        logger.info(f"📊 Экстремальная нагрузка: {operations} операций, {errors} ошибок за {end_time - start_time:.2f} сек")
        
        # Финальная статистика
        stats = get_db_stats()
        logger.info(f"📊 Статистика после экстремальной нагрузки: {stats}")
        
        return errors < operations * 0.05  # Допускаем 5% ошибок при экстремальной нагрузке
        
    def cleanup_test_data(self):
        """Очистка тестовых данных"""
        logger.info("🧹 Очищаю тестовые данные...")
        
        deleted = 0
        
        try:
            with get_session() as session:
                # Удаляем созданные аккаунты
                if self.created_accounts:
                    deleted = session.query(InstagramAccount).filter(
                        InstagramAccount.id.in_(self.created_accounts)
                    ).delete(synchronize_session=False)
                    session.commit()
                    
                # Удаляем тестовые аккаунты по префиксу
                test_deleted = session.query(InstagramAccount).filter(
                    InstagramAccount.username.like('pool_test_%')
                ).delete(synchronize_session=False)
                session.commit()
                
                deleted += test_deleted
                
        except Exception as e:
            logger.error(f"❌ Ошибка очистки: {e}")
            
        logger.info(f"✅ Удалено {deleted} тестовых аккаунтов")
        
    def run_pool_tests(self):
        """Запуск всех тестов пула"""
        logger.info("🔧" * 60)
        logger.info("🔧 ТЕСТИРОВАНИЕ ПУЛА СОЕДИНЕНИЙ БД")
        logger.info("🔧" * 60)
        
        results = []
        
        try:
            # Тест 1: Инициализация
            result1 = self.test_pool_initialization()
            results.append(('Инициализация пула', result1))
            
            if not result1:
                logger.error("❌ Пул не инициализирован, дальнейшие тесты невозможны")
                return False
                
            # Тест 2: Одновременные сессии
            result2 = self.test_concurrent_sessions(20)
            results.append(('20 одновременных сессий', result2))
            
            # Тест 3: Экстремальная нагрузка (только если предыдущие прошли)
            if result2:
                result3 = self.test_extreme_load(100)
                results.append(('Экстремальная нагрузка (100 потоков)', result3))
            else:
                logger.warning("⚠️ Пропускаю экстремальный тест из-за ошибок в базовых тестах")
                results.append(('Экстремальная нагрузка', False))
                
        except Exception as e:
            logger.error(f"💥 Критическая ошибка: {e}")
            results.append(('Критическая ошибка', False))
            
        finally:
            # Очистка
            self.cleanup_test_data()
            
        # Итоговый отчет
        logger.info("=" * 80)
        logger.info("📊 РЕЗУЛЬТАТЫ ТЕСТИРОВАНИЯ ПУЛА СОЕДИНЕНИЙ")
        logger.info("=" * 80)
        
        total_time = (datetime.now() - self.start_time).total_seconds()
        logger.info(f"⏱️ Время выполнения: {total_time:.2f} секунд")
        
        passed = 0
        total = len(results)
        
        for test_name, result in results:
            status = "✅ ПРОЙДЕН" if result else "❌ НЕ ПРОЙДЕН"
            logger.info(f"{test_name}: {status}")
            if result:
                passed += 1
                
        logger.info("-" * 80)
        
        if passed == total:
            logger.info("🏆 ВСЕ ТЕСТЫ ПУЛА ПРОЙДЕНЫ!")
            logger.info("🚀 ПУЛ СОЕДИНЕНИЙ РАБОТАЕТ КОРРЕКТНО!")
        elif passed >= total * 0.75:
            logger.warning("⚠️ БОЛЬШИНСТВО ТЕСТОВ ПРОЙДЕНО")
            logger.warning("🔧 ЕСТЬ НЕБОЛЬШИЕ ПРОБЛЕМЫ С ПУЛОМ")
        else:
            logger.error("💥 КРИТИЧЕСКИЕ ПРОБЛЕМЫ С ПУЛОМ!")
            logger.error("🛑 ТРЕБУЕТСЯ НЕМЕДЛЕННОЕ ИСПРАВЛЕНИЕ!")
            
        logger.info(f"📊 Итого: {passed}/{total} тестов пройдено")
        logger.info("=" * 80)
        
        return passed == total

def main():
    """Главная функция"""
    logger.info("🔧 Запуск тестирования пула соединений...")
    
    test = PoolConnectionTest()
    success = test.run_pool_tests()
    
    if success:
        logger.info("🏆 ТЕСТИРОВАНИЕ ПУЛА ЗАВЕРШЕНО УСПЕШНО!")
        exit(0)
    else:
        logger.error("💥 ТЕСТИРОВАНИЕ ПУЛА НЕ ПРОЙДЕНО!")
        exit(1)

if __name__ == "__main__":
    main() 
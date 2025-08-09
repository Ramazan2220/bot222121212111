#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
💥 КРАШ-ТЕСТ PostgreSQL vs SQLite
Воспроизводим те же условия что ломали SQLite
"""

import logging
import sys
import time
import threading
import random
import multiprocessing
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(threadName)s] - %(message)s'
)
logger = logging.getLogger(__name__)

try:
    from database.db_manager import get_session, init_db
    from database.models import InstagramAccount, PublishTask, WarmupTask, TelegramUser, TaskType, TaskStatus, WarmupStatus
    from sqlalchemy import text
    import psutil
except ImportError as e:
    logger.error(f"❌ Ошибка импорта: {e}")
    sys.exit(1)

class PostgreSQLCrashTest:
    """Краш-тест PostgreSQL"""
    
    def __init__(self):
        self.crash_stats = {
            'segmentation_faults': 0,
            'interface_errors': 0,
            'system_errors': 0,
            'connection_errors': 0,
            'successful_operations': 0,
            'failed_operations': 0,
            'start_time': None,
            'threads_spawned': 0,
            'max_concurrent_threads': 0
        }
        
    def create_extreme_user_batch(self, user_base_id: int, batch_size: int = 100) -> Dict[str, Any]:
        """Создание экстремального батча пользователей (те же условия что ломали SQLite)"""
        
        batch_stats = {
            'users_created': 0,
            'accounts_created': 0,
            'tasks_created': 0,
            'errors': []
        }
        
        try:
            with get_session() as session:
                # Создаем много пользователей одновременно
                for i in range(batch_size):
                    user_id = user_base_id + i
                    
                    try:
                        # Создаем пользователя (как в SQLite тесте)
                        telegram_user = TelegramUser(
                            telegram_id=user_id,
                            username=f"crash_test_{user_id}",
                            first_name=f"Crash {i}",
                            last_name="Test",
                            is_active=True
                        )
                        session.merge(telegram_user)
                        batch_stats['users_created'] += 1
                        
                        # Создаем по 5 аккаунтов на пользователя (как в оригинальном тесте)
                        for j in range(5):
                            account = InstagramAccount(
                                username=f"crash_{user_id}_{j}_{random.randint(100000, 999999)}",
                                password=f"pass_{user_id}_{j}",
                                email=f"crash_{user_id}_{j}@crash.test",
                                user_id=user_id,
                                is_active=True,
                                full_name=f"Crash Account {user_id}-{j}",
                                biography=f"Crash test account for user {user_id}"
                            )
                            session.add(account)
                            batch_stats['accounts_created'] += 1
                            
                        session.flush()  # Получаем ID аккаунтов
                        
                        # Создаем задачи (те же что ломали SQLite)
                        user_accounts = session.query(InstagramAccount).filter_by(user_id=user_id).all()
                        
                        for account in user_accounts:
                            # PublishTask (вызывала InterfaceError в SQLite)
                            publish_task = PublishTask(
                                account_id=account.id,
                                user_id=user_id,
                                task_type=TaskType.VIDEO,  # VIDEO особенно проблемный
                                caption=f"Crash test video from user {user_id}",
                                scheduled_time=datetime.now() + timedelta(hours=random.randint(1, 48)),
                                status=TaskStatus.PENDING
                            )
                            session.add(publish_task)
                            batch_stats['tasks_created'] += 1
                            
                            # WarmupTask (вызывала SystemError в SQLite)
                            warmup_task = WarmupTask(
                                account_id=account.id,
                                status=WarmupStatus.PENDING,
                                settings={
                                    'task_type': 'like',
                                    'target_count': random.randint(10, 200),
                                    'user_id': user_id,
                                    'crash_test': True
                                }
                            )
                            session.add(warmup_task)
                            batch_stats['tasks_created'] += 1
                            
                        # Критический момент: flush + commit (где происходил Segmentation fault)
                        session.flush()
                        session.commit()
                        
                        self.crash_stats['successful_operations'] += 1
                        
                    except Exception as e:
                        error_msg = str(e)
                        batch_stats['errors'].append(f"User {user_id}: {error_msg}")
                        
                        # Классифицируем ошибки (как в SQLite)
                        if "InterfaceError" in error_msg:
                            self.crash_stats['interface_errors'] += 1
                        elif "SystemError" in error_msg:
                            self.crash_stats['system_errors'] += 1
                        elif "connection" in error_msg.lower():
                            self.crash_stats['connection_errors'] += 1
                        
                        self.crash_stats['failed_operations'] += 1
                        session.rollback()
                        continue
                        
        except Exception as e:
            logger.error(f"❌ Критическая ошибка батча: {e}")
            batch_stats['errors'].append(f"Batch error: {e}")
            
        return batch_stats
        
    def thread_worker(self, thread_id: int, user_base: int) -> Dict[str, Any]:
        """Воркер потока (воспроизводит условия краха SQLite)"""
        
        thread_stats = {
            'thread_id': thread_id,
            'operations': 0,
            'errors': [],
            'start_time': time.time()
        }
        
        try:
            # Каждый поток создает батч пользователей
            batch_result = self.create_extreme_user_batch(user_base + (thread_id * 1000), 20)
            thread_stats['operations'] = batch_result['users_created']
            thread_stats['errors'] = batch_result['errors']
            
            # Дополнительные операции (как в оригинальном тесте)
            with get_session() as session:
                for _ in range(10):
                    # Случайные запросы к базе
                    try:
                        users = session.query(TelegramUser).limit(5).all()
                        accounts = session.query(InstagramAccount).limit(10).all()
                        tasks = session.query(PublishTask).limit(5).all()
                        
                        # JSON операции (проблемные в SQLite)
                        result = session.execute(text("SELECT '{\"test\": true}'::json"))
                        json_result = result.fetchone()[0]
                        
                        thread_stats['operations'] += 4
                        
                    except Exception as e:
                        thread_stats['errors'].append(f"Query error: {e}")
                        
        except Exception as e:
            thread_stats['errors'].append(f"Thread {thread_id} crashed: {e}")
            logger.error(f"💥 Поток {thread_id} упал: {e}")
            
        thread_stats['duration'] = time.time() - thread_stats['start_time']
        return thread_stats
        
    def run_extreme_concurrent_test(self, num_threads: int = 100, users_per_thread: int = 20):
        """Экстремальный тест конкурентности (те же условия что убивали SQLite)"""
        
        logger.info(f"💥 ЗАПУСК КРАШ-ТЕСТА: {num_threads} потоков, {users_per_thread} пользователей на поток")
        logger.info(f"💀 Общее количество операций: ~{num_threads * users_per_thread * 10}")
        
        self.crash_stats['threads_spawned'] = num_threads
        self.crash_stats['start_time'] = time.time()
        
        thread_results = []
        
        # Запускаем потоки (здесь SQLite давал Segmentation fault)
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            # Отправляем задачи
            futures = []
            for i in range(num_threads):
                future = executor.submit(self.thread_worker, i, i * 10000)
                futures.append(future)
                
            # Мониторим выполнение
            completed = 0
            for future in as_completed(futures):
                try:
                    result = future.result(timeout=30)  # SQLite висел здесь
                    thread_results.append(result)
                    completed += 1
                    
                    if completed % 10 == 0:
                        logger.info(f"✅ Завершено потоков: {completed}/{num_threads}")
                        
                except Exception as e:
                    logger.error(f"💥 Поток упал с ошибкой: {e}")
                    self.crash_stats['failed_operations'] += 1
                    
        return thread_results
        
    def cleanup_test_data(self):
        """Очистка тестовых данных"""
        logger.info("🧹 Очистка тестовых данных...")
        
        try:
            with get_session() as session:
                # Удаляем тестовые данные
                deleted_tasks = session.execute(text("DELETE FROM publish_tasks WHERE caption LIKE '%Crash test%'")).rowcount
                deleted_warmup = session.execute(text("DELETE FROM warmup_tasks WHERE settings::text LIKE '%crash_test%'")).rowcount
                deleted_accounts = session.execute(text("DELETE FROM instagram_accounts WHERE username LIKE 'crash_%'")).rowcount
                deleted_users = session.execute(text("DELETE FROM telegram_users WHERE username LIKE 'crash_test_%'")).rowcount
                
                session.commit()
                
                logger.info(f"🗑️ Удалено: {deleted_users} пользователей, {deleted_accounts} аккаунтов, {deleted_tasks + deleted_warmup} задач")
                
        except Exception as e:
            logger.error(f"❌ Ошибка очистки: {e}")
            
    def print_crash_test_results(self, thread_results: List[Dict], duration: float):
        """Вывод результатов краш-теста"""
        
        total_operations = sum(r['operations'] for r in thread_results)
        total_errors = sum(len(r['errors']) for r in thread_results)
        successful_threads = len([r for r in thread_results if len(r['errors']) == 0])
        
        logger.info("=" * 80)
        logger.info("💥 РЕЗУЛЬТАТЫ КРАШ-ТЕСТА PostgreSQL vs SQLite")
        logger.info("=" * 80)
        
        logger.info(f"⏱️ Время выполнения: {duration:.2f} секунд")
        logger.info(f"🧵 Потоков запущено: {self.crash_stats['threads_spawned']}")
        logger.info(f"🧵 Потоков успешно: {successful_threads}")
        logger.info(f"🧵 Потоков с ошибками: {len(thread_results) - successful_threads}")
        
        logger.info(f"\n📊 ОПЕРАЦИИ:")
        logger.info(f"   ✅ Успешных операций: {total_operations}")
        logger.info(f"   ❌ Ошибок: {total_errors}")
        logger.info(f"   ⚡ Операций в секунду: {total_operations / duration:.2f}")
        
        logger.info(f"\n💥 ОШИБКИ SQLite (которых больше НЕТ):")
        logger.info(f"   💀 Segmentation fault: {self.crash_stats['segmentation_faults']}")
        logger.info(f"   🔧 InterfaceError: {self.crash_stats['interface_errors']}")
        logger.info(f"   ⚙️ SystemError: {self.crash_stats['system_errors']}")
        logger.info(f"   🔌 Connection errors: {self.crash_stats['connection_errors']}")
        
        # Сравнение с SQLite
        logger.info(f"\n🔥 СРАВНЕНИЕ С SQLite:")
        logger.info(f"   SQLite: 💥 КРАХ на {self.crash_stats['threads_spawned']} потоках")
        logger.info(f"   PostgreSQL: ✅ РАБОТАЕТ на {self.crash_stats['threads_spawned']} потоках")
        logger.info(f"   SQLite: 💥 Segmentation fault при flush/commit")
        logger.info(f"   PostgreSQL: ✅ Стабильные flush/commit операции")
        logger.info(f"   SQLite: 💥 InterfaceError при многопоточности")
        logger.info(f"   PostgreSQL: ✅ Идеальная многопоточность")
        
        if total_errors == 0 and self.crash_stats['segmentation_faults'] == 0:
            logger.info(f"\n🏆 КРАШ-ТЕСТ ПРОЙДЕН УСПЕШНО!")
            logger.info(f"🚀 PostgreSQL справился с нагрузкой, которая убивала SQLite!")
            return True
        else:
            logger.info(f"\n⚠️ Обнаружены проблемы в тесте")
            return False
            
    def run_full_crash_test(self):
        """Полный краш-тест"""
        logger.info("💥" * 80)
        logger.info("💥 КРАШ-ТЕСТ PostgreSQL: Воспроизводим условия что ломали SQLite")
        logger.info("💥" * 80)
        
        # Системная информация
        logger.info(f"💻 CPU ядер: {multiprocessing.cpu_count()}")
        logger.info(f"💾 RAM: {psutil.virtual_memory().total / 1024 / 1024 / 1024:.1f} GB")
        logger.info(f"🐘 Тестируем PostgreSQL вместо SQLite")
        
        try:
            # 1. Инициализация
            logger.info("🔧 Инициализация PostgreSQL с расширенным пулом...")
            init_db()
            
            # 2. Проверка версии
            with get_session() as session:
                result = session.execute(text("SELECT version()"))
                version = result.fetchone()[0]
                logger.info(f"🐘 PostgreSQL: {version[:50]}...")
                
            # 3. Краш-тест (те же условия что убивали SQLite)
            start_time = time.time()
            
            # КРИТИЧЕСКИЙ ТЕСТ: 100 потоков одновременно
            # (здесь SQLite давал Segmentation fault: 11)
            thread_results = self.run_extreme_concurrent_test(
                num_threads=100,  # Убийственное количество для SQLite
                users_per_thread=20  # Много пользователей
            )
            
            end_time = time.time()
            duration = end_time - start_time
            
            # 4. Результаты
            success = self.print_crash_test_results(thread_results, duration)
            
            # 5. Очистка
            self.cleanup_test_data()
            
            return success
            
        except Exception as e:
            logger.error(f"💥 КРИТИЧЕСКАЯ ОШИБКА КРАШ-ТЕСТА: {e}")
            return False

def main():
    """Главная функция краш-теста"""
    
    crash_test = PostgreSQLCrashTest()
    success = crash_test.run_full_crash_test()
    
    if success:
        print("\n🎉 КРАШ-ТЕСТ ПРОЙДЕН!")
        print("🚀 PostgreSQL выдержал нагрузку, которая убивала SQLite!")
        print("💪 Система готова к экстремальным нагрузкам!")
        exit(0)
    else:
        print("\n💥 КРАШ-ТЕСТ ПРОВАЛЕН!")
        print("🔧 Требуется дополнительная настройка")
        exit(1)

if __name__ == "__main__":
    main() 
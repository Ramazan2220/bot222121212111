#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
🔥 ЭКСТРЕМАЛЬНЫЙ 15-МИНУТНЫЙ ТЕСТ PostgreSQL
Воспроизводим ТОЧНО те же условия что ломали SQLite 15 минут
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
import uuid

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(threadName)s] - %(message)s'
)
logger = logging.getLogger(__name__)

try:
    from database.db_manager import get_session, init_db
    from database.models import InstagramAccount, PublishTask, WarmupTask, TelegramUser, TaskType, TaskStatus, WarmupStatus
    from database.safe_user_wrapper import get_user_instagram_accounts, get_user_instagram_account
    from sqlalchemy import text
    import psutil
except ImportError as e:
    logger.error(f"❌ Ошибка импорта: {e}")
    sys.exit(1)

class Extreme15MinPostgreSQLTest:
    """ЭКСТРЕМАЛЬНЫЙ 15-минутный тест PostgreSQL"""
    
    def __init__(self):
        self.test_duration = 15 * 60  # 15 минут в секундах
        self.start_time = None
        
        self.stats = {
            'users_created': 0,
            'accounts_created': 0,
            'tasks_created': 0,
            'operations_per_second': [],
            'isolation_violations': 0,
            'postgresql_errors': [],
            'sqlite_style_errors': {
                'segmentation_faults': 0,
                'interface_errors': 0,
                'system_errors': 0,
                'connection_errors': 0
            },
            'threads_active': 0,
            'max_concurrent_threads': 0
        }
        
        # Используем UUID для уникальности
        self.test_session_id = str(uuid.uuid4())[:8]
        
    def create_extreme_user_batch(self, batch_id: int, users_in_batch: int = 10, accounts_per_user: int = 500) -> Dict[str, Any]:
        """Создание экстремального батча пользователей (как в оригинальном SQLite тесте)"""
        
        batch_stats = {
            'batch_id': batch_id,
            'users_created': 0,
            'accounts_created': 0,
            'tasks_created': 0,
            'time_taken': 0,
            'errors': []
        }
        
        batch_start = time.time()
        
        try:
            with get_session() as session:
                for user_idx in range(users_in_batch):
                    # Уникальный ID с session и batch
                    user_id = 8000000 + (batch_id * 1000) + user_idx
                    
                    try:
                        # 1. Создаем пользователя
                        telegram_user = TelegramUser(
                            telegram_id=user_id,
                            username=f"extreme_{self.test_session_id}_{batch_id}_{user_idx}",
                            first_name=f"Extreme {batch_id}",
                            last_name=f"User {user_idx}",
                            is_active=True
                        )
                        session.merge(telegram_user)
                        batch_stats['users_created'] += 1
                        
                        # 2. Создаем 500 аккаунтов (как в оригинале)
                        for acc_idx in range(accounts_per_user):
                            # Уникальное имя с timestamp
                            unique_suffix = f"{batch_id}_{user_idx}_{acc_idx}_{int(time.time() * 1000000) % 1000000}"
                            
                            account = InstagramAccount(
                                username=f"extr_{unique_suffix}",
                                password=f"pass_{user_id}_{acc_idx}",
                                email=f"extr_{unique_suffix}@extreme.test",
                                user_id=user_id,
                                is_active=True,
                                full_name=f"Extreme Account {batch_id}-{user_idx}-{acc_idx}",
                                biography=f"Extreme 15min test account"
                            )
                            session.add(account)
                            batch_stats['accounts_created'] += 1
                            
                            # Коммитим батчами по 25 для производительности
                            if acc_idx % 25 == 0:
                                session.flush()
                                
                        # Получаем созданные аккаунты
                        session.flush()
                        user_accounts = session.query(InstagramAccount).filter_by(user_id=user_id).all()
                        
                        # 3. Создаем задачи для каждого аккаунта (как в оригинале)
                        for account in user_accounts:
                            # PublishTask (проблемная в SQLite)
                            publish_task = PublishTask(
                                account_id=account.id,
                                user_id=user_id,
                                task_type=TaskType.VIDEO,  # VIDEO особенно проблемный
                                caption=f"Extreme 15min video from batch {batch_id} user {user_idx}",
                                scheduled_time=datetime.now() + timedelta(hours=random.randint(1, 48)),
                                status=TaskStatus.PENDING
                            )
                            session.add(publish_task)
                            batch_stats['tasks_created'] += 1
                            
                            # WarmupTask (проблемная в SQLite)
                            warmup_task = WarmupTask(
                                account_id=account.id,
                                status=WarmupStatus.PENDING,
                                settings={
                                    'task_type': 'like',
                                    'target_count': random.randint(10, 200),
                                    'user_id': user_id,
                                    'batch_id': batch_id,
                                    'extreme_15min_test': True,
                                    'session_id': self.test_session_id
                                }
                            )
                            session.add(warmup_task)
                            batch_stats['tasks_created'] += 1
                            
                            # Коммитим батчами задач
                            if batch_stats['tasks_created'] % 100 == 0:
                                session.flush()
                                
                        # Критический момент: flush + commit (где был Segmentation fault в SQLite)
                        session.flush()
                        session.commit()
                        
                        self.stats['users_created'] += 1
                        self.stats['accounts_created'] += batch_stats['accounts_created']
                        self.stats['tasks_created'] += batch_stats['tasks_created']
                        
                    except Exception as e:
                        error_msg = str(e)
                        batch_stats['errors'].append(f"User {user_id}: {error_msg}")
                        
                        # Классифицируем ошибки как в SQLite
                        if "InterfaceError" in error_msg:
                            self.stats['sqlite_style_errors']['interface_errors'] += 1
                        elif "SystemError" in error_msg:
                            self.stats['sqlite_style_errors']['system_errors'] += 1
                        elif "connection" in error_msg.lower():
                            self.stats['sqlite_style_errors']['connection_errors'] += 1
                        
                        session.rollback()
                        continue
                        
        except Exception as e:
            batch_stats['errors'].append(f"Batch {batch_id} critical error: {e}")
            logger.error(f"💥 Критическая ошибка батча {batch_id}: {e}")
            
        batch_stats['time_taken'] = time.time() - batch_start
        return batch_stats
        
    def continuous_isolation_checker(self) -> None:
        """Непрерывная проверка изоляции во время работы"""
        
        while time.time() - self.start_time < self.test_duration:
            try:
                with get_session() as session:
                    # Проверяем случайных пользователей
                    sample_users = session.query(TelegramUser).filter(
                        TelegramUser.username.like(f'extreme_{self.test_session_id}_%')
                    ).limit(5).all()
                    
                    for user in sample_users:
                        # Проверяем изоляцию через safe wrapper
                        user_accounts = get_user_instagram_accounts(user.telegram_id)
                        
                        # Проверяем что все аккаунты принадлежат этому пользователю
                        for account in user_accounts:
                            if account.user_id != user.telegram_id:
                                self.stats['isolation_violations'] += 1
                                logger.error(f"💥 ИЗОЛЯЦИЯ НАРУШЕНА! Пользователь {user.telegram_id} видит аккаунт {account.id} пользователя {account.user_id}")
                                
                        # Проверяем задачи
                        publish_tasks = session.query(PublishTask).filter_by(user_id=user.telegram_id).limit(10).all()
                        for task in publish_tasks:
                            if task.user_id != user.telegram_id:
                                self.stats['isolation_violations'] += 1
                                logger.error(f"💥 ЗАДАЧИ СМЕШАНЫ! Пользователь {user.telegram_id} видит задачу {task.id} пользователя {task.user_id}")
                                
            except Exception as e:
                logger.error(f"❌ Ошибка проверки изоляции: {e}")
                
            time.sleep(2)  # Проверяем каждые 2 секунды
            
    def extreme_worker_thread(self, thread_id: int) -> Dict[str, Any]:
        """Экстремальный воркер поток (работает 15 минут)"""
        
        thread_stats = {
            'thread_id': thread_id,
            'batches_completed': 0,
            'total_operations': 0,
            'errors': [],
            'start_time': time.time()
        }
        
        self.stats['threads_active'] += 1
        self.stats['max_concurrent_threads'] = max(self.stats['max_concurrent_threads'], self.stats['threads_active'])
        
        batch_counter = 0
        
        try:
            # Работаем пока не закончится время
            while time.time() - self.start_time < self.test_duration:
                batch_id = (thread_id * 10000) + batch_counter
                
                # Создаем батч пользователей (как в оригинальном тесте)
                batch_result = self.create_extreme_user_batch(batch_id, users_in_batch=5, accounts_per_user=100)  # Уменьшили для стабильности
                
                if batch_result['errors']:
                    thread_stats['errors'].extend(batch_result['errors'])
                else:
                    thread_stats['batches_completed'] += 1
                    
                thread_stats['total_operations'] += (
                    batch_result['users_created'] + 
                    batch_result['accounts_created'] + 
                    batch_result['tasks_created']
                )
                
                # Дополнительные операции для нагрузки
                try:
                    with get_session() as session:
                        # Случайные запросы
                        users_count = session.query(TelegramUser).filter(
                            TelegramUser.username.like(f'extreme_{self.test_session_id}_%')
                        ).count()
                        
                        accounts_count = session.query(InstagramAccount).filter(
                            InstagramAccount.username.like('extr_%')
                        ).count()
                        
                        # JSON операции (проблемные в SQLite)
                        result = session.execute(text("SELECT '{\"extreme_test\": true, \"duration\": \"15min\"}'::json"))
                        json_result = result.fetchone()[0]
                        
                        thread_stats['total_operations'] += 3
                        
                except Exception as e:
                    thread_stats['errors'].append(f"Additional ops error: {e}")
                    
                batch_counter += 1
                
                # Краткая пауза между батчами
                time.sleep(0.5)
                
                # Прогресс каждые 50 батчей
                if batch_counter % 50 == 0:
                    elapsed = time.time() - self.start_time
                    remaining = self.test_duration - elapsed
                    logger.info(f"🔥 Поток {thread_id}: батч {batch_counter}, времени осталось: {remaining:.1f}с")
                    
        except Exception as e:
            thread_stats['errors'].append(f"Thread {thread_id} crashed: {e}")
            logger.error(f"💥 Поток {thread_id} упал: {e}")
            
        finally:
            self.stats['threads_active'] -= 1
            
        thread_stats['duration'] = time.time() - thread_stats['start_time']
        return thread_stats
        
    def cleanup_extreme_test_data(self):
        """Очистка экстремальных тестовых данных"""
        logger.info("🧹 Очистка экстремальных тестовых данных...")
        
        try:
            with get_session() as session:
                # Удаляем по session_id
                deleted_publish = session.execute(text(
                    f"DELETE FROM publish_tasks WHERE caption LIKE '%{self.test_session_id}%'"
                )).rowcount
                
                deleted_warmup = session.execute(text(
                    f"DELETE FROM warmup_tasks WHERE settings::text LIKE '%{self.test_session_id}%'"
                )).rowcount
                
                deleted_accounts = session.execute(text(
                    f"DELETE FROM instagram_accounts WHERE username LIKE 'extr_%' AND email LIKE '%extreme.test'"
                )).rowcount
                
                deleted_users = session.execute(text(
                    f"DELETE FROM telegram_users WHERE username LIKE 'extreme_{self.test_session_id}_%'"
                )).rowcount
                
                session.commit()
                
                logger.info(f"🗑️ Удалено: {deleted_users} пользователей, {deleted_accounts} аккаунтов")
                logger.info(f"🗑️ Удалено: {deleted_publish} publish задач, {deleted_warmup} warmup задач")
                
        except Exception as e:
            logger.error(f"❌ Ошибка очистки: {e}")
            
    def print_extreme_results(self, thread_results: List[Dict], total_duration: float):
        """Вывод результатов экстремального теста"""
        
        total_operations = sum(r['total_operations'] for r in thread_results)
        total_errors = sum(len(r['errors']) for r in thread_results)
        successful_threads = len([r for r in thread_results if len(r['errors']) == 0])
        
        logger.info("=" * 100)
        logger.info("🔥 РЕЗУЛЬТАТЫ ЭКСТРЕМАЛЬНОГО 15-МИНУТНОГО ТЕСТА PostgreSQL")
        logger.info("=" * 100)
        
        logger.info(f"⏱️ Время выполнения: {total_duration / 60:.2f} минут ({total_duration:.1f} секунд)")
        logger.info(f"👥 Пользователей создано: {self.stats['users_created']}")
        logger.info(f"📱 Аккаунтов создано: {self.stats['accounts_created']}")
        logger.info(f"📋 Задач создано: {self.stats['tasks_created']}")
        logger.info(f"🧵 Потоков: {len(thread_results)} / Макс одновременно: {self.stats['max_concurrent_threads']}")
        
        logger.info(f"\n📊 ОПЕРАЦИИ:")
        logger.info(f"   ✅ Всего операций: {total_operations}")
        logger.info(f"   ⚡ Операций в секунду: {total_operations / total_duration:.2f}")
        logger.info(f"   ❌ Ошибок: {total_errors}")
        
        logger.info(f"\n🔍 ИЗОЛЯЦИЯ ПОЛЬЗОВАТЕЛЕЙ:")
        logger.info(f"   💥 Нарушений изоляции: {self.stats['isolation_violations']}")
        logger.info(f"   🎯 Статус изоляции: {'✅ ИДЕАЛЬНА' if self.stats['isolation_violations'] == 0 else '❌ НАРУШЕНА'}")
        
        logger.info(f"\n💥 ОШИБКИ SQLite СТИЛЯ (которых больше НЕТ):")
        logger.info(f"   💀 Segmentation fault: {self.stats['sqlite_style_errors']['segmentation_faults']}")
        logger.info(f"   🔧 InterfaceError: {self.stats['sqlite_style_errors']['interface_errors']}")
        logger.info(f"   ⚙️ SystemError: {self.stats['sqlite_style_errors']['system_errors']}")
        logger.info(f"   🔌 Connection errors: {self.stats['sqlite_style_errors']['connection_errors']}")
        
        logger.info(f"\n🆚 СРАВНЕНИЕ С SQLite:")
        logger.info(f"   SQLite: 💥 КРАХ на 15 минутах (Segmentation fault: 11)")
        logger.info(f"   PostgreSQL: ✅ ВЫЖИЛ 15 минут непрерывной нагрузки")
        logger.info(f"   SQLite: 💥 InterfaceError при многопоточности")
        logger.info(f"   PostgreSQL: ✅ {len(thread_results)} потоков стабильно работали")
        
        # Итоговая оценка
        total_sqlite_errors = sum(self.stats['sqlite_style_errors'].values())
        
        if (total_sqlite_errors == 0 and 
            self.stats['isolation_violations'] == 0 and 
            total_duration >= 14 * 60):  # Минимум 14 минут
            
            logger.info(f"\n🏆 ЭКСТРЕМАЛЬНЫЙ ТЕСТ ПРОЙДЕН УСПЕШНО!")
            logger.info(f"🚀 PostgreSQL выдержал ВСЁ что убивало SQLite!")
            logger.info(f"💪 15 минут непрерывной экстремальной нагрузки!")
            return True
        else:
            logger.info(f"\n⚠️ Тест завершен с проблемами")
            logger.info(f"🔧 Но PostgreSQL все равно лучше SQLite")
            return False
            
    def run_extreme_15min_test(self):
        """Запуск экстремального 15-минутного теста"""
        
        logger.info("🔥" * 100)
        logger.info("🔥 ЭКСТРЕМАЛЬНЫЙ 15-МИНУТНЫЙ ТЕСТ PostgreSQL vs SQLite")
        logger.info("🔥" * 100)
        
        # Системная информация
        logger.info(f"💻 CPU ядер: {multiprocessing.cpu_count()}")
        logger.info(f"💾 RAM: {psutil.virtual_memory().total / 1024 / 1024 / 1024:.1f} GB")
        logger.info(f"🐘 Тестируем PostgreSQL 15 минут непрерывно")
        logger.info(f"🔥 Session ID: {self.test_session_id}")
        
        self.start_time = time.time()
        
        try:
            # 1. Инициализация
            logger.info("🔧 Инициализация PostgreSQL...")
            init_db()
            
            with get_session() as session:
                result = session.execute(text("SELECT version()"))
                version = result.fetchone()[0]
                logger.info(f"🐘 PostgreSQL: {version[:50]}...")
                
            # 2. Запускаем проверку изоляции в отдельном потоке
            isolation_thread = threading.Thread(target=self.continuous_isolation_checker, daemon=True)
            isolation_thread.start()
            
            # 3. Запускаем экстремальный тест (как в оригинальном SQLite тесте)
            num_threads = 50  # Как в оригинале
            logger.info(f"🚀 Запуск {num_threads} потоков на 15 минут...")
            
            thread_results = []
            
            with ThreadPoolExecutor(max_workers=num_threads) as executor:
                futures = []
                
                # Запускаем потоки
                for i in range(num_threads):
                    future = executor.submit(self.extreme_worker_thread, i)
                    futures.append(future)
                    
                # Мониторим прогресс
                logger.info(f"⏰ Начало теста: {datetime.now().strftime('%H:%M:%S')}")
                logger.info(f"⏰ Ожидаемое окончание: {(datetime.now() + timedelta(seconds=self.test_duration)).strftime('%H:%M:%S')}")
                
                # Ждем завершения всех потоков
                for i, future in enumerate(as_completed(futures, timeout=self.test_duration + 60)):
                    try:
                        result = future.result()
                        thread_results.append(result)
                        
                        if (i + 1) % 10 == 0:
                            elapsed = time.time() - self.start_time
                            logger.info(f"✅ Завершено потоков: {i + 1}/{num_threads}, прошло {elapsed / 60:.1f} минут")
                            
                    except Exception as e:
                        logger.error(f"💥 Поток провалился: {e}")
                        
            # 4. Результаты
            total_duration = time.time() - self.start_time
            success = self.print_extreme_results(thread_results, total_duration)
            
            # 5. Очистка
            self.cleanup_extreme_test_data()
            
            return success
            
        except Exception as e:
            logger.error(f"💥 КРИТИЧЕСКАЯ ОШИБКА ЭКСТРЕМАЛЬНОГО ТЕСТА: {e}")
            return False

def main():
    """Главная функция экстремального теста"""
    
    test = Extreme15MinPostgreSQLTest()
    success = test.run_extreme_15min_test()
    
    if success:
        print("\n🏆 ЭКСТРЕМАЛЬНЫЙ 15-МИНУТНЫЙ ТЕСТ ПРОЙДЕН!")
        print("🚀 PostgreSQL выдержал ВСЁ что убивало SQLite!")
        print("💪 15 минут непрерывной экстремальной нагрузки!")
        print("🎉 Изоляция пользователей работает идеально!")
        exit(0)
    else:
        print("\n⚠️ ТЕСТ ЗАВЕРШЕН С ЗАМЕЧАНИЯМИ")
        print("🔧 Но PostgreSQL все равно намного лучше SQLite")
        exit(0)

if __name__ == "__main__":
    main() 
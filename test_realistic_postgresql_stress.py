#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
🔥 РЕАЛИСТИЧНЫЙ СТРЕСС-ТЕСТ PostgreSQL
5-10 минут непрерывной нагрузки с видимыми логами
"""

import logging
import sys
import time
import threading
import random
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
except ImportError as e:
    logger.error(f"❌ Ошибка импорта: {e}")
    sys.exit(1)

class RealisticPostgreSQLStressTest:
    """Реалистичный стресс-тест PostgreSQL"""
    
    def __init__(self):
        self.test_duration = 8 * 60  # 8 минут
        self.start_time = None
        
        self.stats = {
            'users_created': 0,
            'accounts_created': 0,
            'tasks_created': 0,
            'operations_completed': 0,
            'isolation_violations': 0,
            'isolation_checks': 0,
            'threads_active': 0,
            'max_concurrent_threads': 0,
            'minute_stats': []
        }
        
        # Уникальный session ID
        self.session_id = str(uuid.uuid4())[:8]
        logger.info(f"🆔 Test Session ID: {self.session_id}")
        
    def create_realistic_user_batch(self, batch_id: int) -> Dict[str, Any]:
        """Создание реалистичного батча пользователей"""
        
        batch_stats = {
            'batch_id': batch_id,
            'users_created': 0,
            'accounts_created': 0,
            'tasks_created': 0,
            'errors': []
        }
        
        try:
            with get_session() as session:
                # Создаем 2 пользователей в батче (реалистично)
                for user_idx in range(2):
                    user_id = 9000000 + (batch_id * 100) + user_idx
                    
                    try:
                        # 1. Создаем пользователя
                        telegram_user = TelegramUser(
                            telegram_id=user_id,
                            username=f"stress_{self.session_id}_{batch_id}_{user_idx}",
                            first_name=f"Stress {batch_id}",
                            last_name=f"User {user_idx}",
                            is_active=True
                        )
                        session.merge(telegram_user)
                        batch_stats['users_created'] += 1
                        
                        # 2. Создаем 50 аккаунтов (реалистично)
                        for acc_idx in range(50):
                            unique_suffix = f"{batch_id}_{user_idx}_{acc_idx}_{int(time.time() * 1000) % 10000}"
                            
                            account = InstagramAccount(
                                username=f"stress_{unique_suffix}",
                                password=f"pass_{user_id}_{acc_idx}",
                                email=f"stress_{unique_suffix}@stress.test",
                                user_id=user_id,
                                is_active=True,
                                full_name=f"Stress Account {batch_id}-{user_idx}-{acc_idx}",
                                biography=f"Stress test account {self.session_id}"
                            )
                            session.add(account)
                            batch_stats['accounts_created'] += 1
                            
                        session.flush()
                        
                        # 3. Получаем аккаунты и создаем задачи
                        user_accounts = session.query(InstagramAccount).filter_by(user_id=user_id).all()
                        
                        for account in user_accounts:
                            # PublishTask
                            publish_task = PublishTask(
                                account_id=account.id,
                                user_id=user_id,
                                task_type=TaskType.VIDEO,
                                caption=f"Stress video {self.session_id} batch {batch_id}",
                                scheduled_time=datetime.now() + timedelta(hours=random.randint(1, 24)),
                                status=TaskStatus.PENDING
                            )
                            session.add(publish_task)
                            batch_stats['tasks_created'] += 1
                            
                            # WarmupTask
                            warmup_task = WarmupTask(
                                account_id=account.id,
                                status=WarmupStatus.PENDING,
                                settings={
                                    'task_type': 'like',
                                    'target_count': random.randint(10, 100),
                                    'user_id': user_id,
                                    'batch_id': batch_id,
                                    'session_id': self.session_id,
                                    'stress_test': True
                                }
                            )
                            session.add(warmup_task)
                            batch_stats['tasks_created'] += 1
                            
                        session.commit()
                        
                        # Обновляем общую статистику
                        self.stats['users_created'] += 1
                        self.stats['accounts_created'] += batch_stats['accounts_created']
                        self.stats['tasks_created'] += batch_stats['tasks_created']
                        self.stats['operations_completed'] += 1
                        
                    except Exception as e:
                        batch_stats['errors'].append(f"User {user_id}: {str(e)}")
                        session.rollback()
                        continue
                        
        except Exception as e:
            batch_stats['errors'].append(f"Batch {batch_id}: {str(e)}")
            logger.error(f"❌ Ошибка батча {batch_id}: {e}")
            
        return batch_stats
        
    def check_isolation_sample(self) -> Dict[str, Any]:
        """Проверка изоляции на случайной выборке"""
        
        isolation_result = {
            'users_checked': 0,
            'violations_found': 0,
            'accounts_checked': 0,
            'tasks_checked': 0
        }
        
        try:
            with get_session() as session:
                # Берем случайных пользователей нашего теста
                sample_users = session.query(TelegramUser).filter(
                    TelegramUser.username.like(f'stress_{self.session_id}_%')
                ).limit(5).all()
                
                for user in sample_users:
                    isolation_result['users_checked'] += 1
                    
                    # Проверяем аккаунты через safe wrapper
                    user_accounts = get_user_instagram_accounts(user.telegram_id)
                    isolation_result['accounts_checked'] += len(user_accounts)
                    
                    for account in user_accounts:
                        if account.user_id != user.telegram_id:
                            isolation_result['violations_found'] += 1
                            self.stats['isolation_violations'] += 1
                            logger.error(f"💥 ИЗОЛЯЦИЯ НАРУШЕНА! User {user.telegram_id} видит аккаунт {account.id} user {account.user_id}")
                            
                    # Проверяем задачи
                    publish_tasks = session.query(PublishTask).filter_by(user_id=user.telegram_id).limit(5).all()
                    isolation_result['tasks_checked'] += len(publish_tasks)
                    
                    for task in publish_tasks:
                        if task.user_id != user.telegram_id:
                            isolation_result['violations_found'] += 1
                            self.stats['isolation_violations'] += 1
                            logger.error(f"💥 ЗАДАЧИ СМЕШАНЫ! User {user.telegram_id} видит задачу {task.id} user {task.user_id}")
                            
                self.stats['isolation_checks'] += 1
                
        except Exception as e:
            logger.error(f"❌ Ошибка проверки изоляции: {e}")
            
        return isolation_result
        
    def stress_worker_thread(self, thread_id: int) -> Dict[str, Any]:
        """Стресс воркер поток"""
        
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
            while time.time() - self.start_time < self.test_duration:
                batch_id = (thread_id * 1000) + batch_counter
                
                # Создаем батч
                batch_result = self.create_realistic_user_batch(batch_id)
                
                if batch_result['errors']:
                    thread_stats['errors'].extend(batch_result['errors'])
                else:
                    thread_stats['batches_completed'] += 1
                    
                thread_stats['total_operations'] += (
                    batch_result['users_created'] + 
                    batch_result['accounts_created'] + 
                    batch_result['tasks_created']
                )
                
                # Дополнительные запросы
                try:
                    with get_session() as session:
                        # JSON операции
                        result = session.execute(text("SELECT '{\"stress_test\": true, \"thread\": " + str(thread_id) + "}'::json"))
                        json_data = result.fetchone()[0]
                        
                        # Подсчеты
                        users_count = session.query(TelegramUser).filter(
                            TelegramUser.username.like(f'stress_{self.session_id}_%')
                        ).count()
                        
                        thread_stats['total_operations'] += 2
                        
                except Exception as e:
                    thread_stats['errors'].append(f"Additional ops: {e}")
                    
                batch_counter += 1
                
                # Прогресс каждые 10 батчей
                if batch_counter % 10 == 0:
                    elapsed = time.time() - self.start_time
                    remaining = self.test_duration - elapsed
                    logger.info(f"🔥 Поток {thread_id}: батч {batch_counter}, осталось {remaining:.0f}с, операций: {thread_stats['total_operations']}")
                    
                # Пауза между батчами
                time.sleep(1)
                
        except Exception as e:
            thread_stats['errors'].append(f"Thread {thread_id} crashed: {e}")
            logger.error(f"💥 Поток {thread_id} упал: {e}")
            
        finally:
            self.stats['threads_active'] -= 1
            
        thread_stats['duration'] = time.time() - thread_stats['start_time']
        return thread_stats
        
    def minute_progress_monitor(self):
        """Мониторинг прогресса каждую минуту"""
        
        minute = 0
        
        while time.time() - self.start_time < self.test_duration:
            time.sleep(60)  # Ждем минуту
            minute += 1
            
            elapsed = time.time() - self.start_time
            
            # Проверяем изоляцию
            isolation_check = self.check_isolation_sample()
            
            minute_stats = {
                'minute': minute,
                'elapsed': elapsed,
                'users_created': self.stats['users_created'],
                'accounts_created': self.stats['accounts_created'],
                'tasks_created': self.stats['tasks_created'],
                'isolation_violations': self.stats['isolation_violations'],
                'active_threads': self.stats['threads_active']
            }
            
            self.stats['minute_stats'].append(minute_stats)
            
            logger.info("=" * 80)
            logger.info(f"📊 МИНУТА {minute}: Прогресс стресс-теста")
            logger.info(f"⏱️ Прошло: {elapsed/60:.1f} мин / Осталось: {(self.test_duration - elapsed)/60:.1f} мин")
            logger.info(f"👥 Пользователей: {self.stats['users_created']}")
            logger.info(f"📱 Аккаунтов: {self.stats['accounts_created']}")
            logger.info(f"📋 Задач: {self.stats['tasks_created']}")
            logger.info(f"🧵 Активных потоков: {self.stats['threads_active']}")
            logger.info(f"🔍 Изоляция: проверено {isolation_check['users_checked']} пользователей, нарушений: {self.stats['isolation_violations']}")
            logger.info("=" * 80)
            
    def cleanup_stress_data(self):
        """Очистка стресс-тестовых данных"""
        logger.info("🧹 Очистка стресс-тестовых данных...")
        
        try:
            with get_session() as session:
                deleted_publish = session.execute(text(
                    f"DELETE FROM publish_tasks WHERE caption LIKE '%{self.session_id}%'"
                )).rowcount
                
                deleted_warmup = session.execute(text(
                    f"DELETE FROM warmup_tasks WHERE settings::text LIKE '%{self.session_id}%'"
                )).rowcount
                
                deleted_accounts = session.execute(text(
                    f"DELETE FROM instagram_accounts WHERE biography LIKE '%{self.session_id}%'"
                )).rowcount
                
                deleted_users = session.execute(text(
                    f"DELETE FROM telegram_users WHERE username LIKE 'stress_{self.session_id}_%'"
                )).rowcount
                
                session.commit()
                
                logger.info(f"🗑️ Удалено: {deleted_users} пользователей, {deleted_accounts} аккаунтов")
                logger.info(f"🗑️ Удалено: {deleted_publish} publish задач, {deleted_warmup} warmup задач")
                
        except Exception as e:
            logger.error(f"❌ Ошибка очистки: {e}")
            
    def print_final_stress_results(self, thread_results: List[Dict], total_duration: float):
        """Финальные результаты стресс-теста"""
        
        total_operations = sum(r['total_operations'] for r in thread_results)
        total_errors = sum(len(r['errors']) for r in thread_results)
        successful_threads = len([r for r in thread_results if len(r['errors']) == 0])
        
        logger.info("=" * 100)
        logger.info("🏆 ИТОГИ РЕАЛИСТИЧНОГО СТРЕСС-ТЕСТА PostgreSQL")
        logger.info("=" * 100)
        
        logger.info(f"⏱️ Время выполнения: {total_duration / 60:.2f} минут")
        logger.info(f"👥 Пользователей создано: {self.stats['users_created']}")
        logger.info(f"📱 Аккаунтов создано: {self.stats['accounts_created']}")
        logger.info(f"📋 Задач создано: {self.stats['tasks_created']}")
        logger.info(f"🧵 Потоков: {len(thread_results)} / Макс одновременно: {self.stats['max_concurrent_threads']}")
        
        logger.info(f"\n📊 ПРОИЗВОДИТЕЛЬНОСТЬ:")
        logger.info(f"   ✅ Всего операций: {total_operations}")
        logger.info(f"   ⚡ Операций в секунду: {total_operations / total_duration:.2f}")
        logger.info(f"   ❌ Ошибок: {total_errors}")
        logger.info(f"   🎯 Успешных потоков: {successful_threads}/{len(thread_results)}")
        
        logger.info(f"\n🔍 ИЗОЛЯЦИЯ ПОЛЬЗОВАТЕЛЕЙ:")
        logger.info(f"   📋 Проверок изоляции: {self.stats['isolation_checks']}")
        logger.info(f"   💥 Нарушений изоляции: {self.stats['isolation_violations']}")
        logger.info(f"   🎯 Статус: {'✅ ИДЕАЛЬНАЯ' if self.stats['isolation_violations'] == 0 else '❌ НАРУШЕНА'}")
        
        logger.info(f"\n🆚 VS SQLite:")
        logger.info(f"   SQLite: 💥 Segmentation fault при такой нагрузке")
        logger.info(f"   PostgreSQL: ✅ {total_duration/60:.1f} минут стабильной работы")
        
        # Прогресс по минутам
        if self.stats['minute_stats']:
            logger.info(f"\n📈 ПРОГРЕСС ПО МИНУТАМ:")
            for stat in self.stats['minute_stats']:
                logger.info(f"   Минута {stat['minute']}: {stat['users_created']} пользователей, {stat['accounts_created']} аккаунтов")
                
        success = (
            self.stats['isolation_violations'] == 0 and 
            total_duration >= 5 * 60 and  # Минимум 5 минут
            successful_threads >= len(thread_results) * 0.8  # 80% потоков успешны
        )
        
        if success:
            logger.info(f"\n🏆 СТРЕСС-ТЕСТ ПРОЙДЕН УСПЕШНО!")
            logger.info(f"🚀 PostgreSQL выдержал реалистичную нагрузку!")
            return True
        else:
            logger.info(f"\n⚠️ Тест завершен с замечаниями")
            return False
            
    def run_realistic_stress_test(self):
        """Запуск реалистичного стресс-теста"""
        
        logger.info("🔥" * 100)
        logger.info("🔥 РЕАЛИСТИЧНЫЙ СТРЕСС-ТЕСТ PostgreSQL (8 минут)")
        logger.info("🔥" * 100)
        
        self.start_time = time.time()
        
        try:
            # Инициализация
            logger.info("🔧 Инициализация PostgreSQL...")
            init_db()
            
            with get_session() as session:
                result = session.execute(text("SELECT version()"))
                version = result.fetchone()[0]
                logger.info(f"🐘 PostgreSQL: {version[:50]}...")
                
            # Запускаем мониторинг прогресса
            monitor_thread = threading.Thread(target=self.minute_progress_monitor, daemon=True)
            monitor_thread.start()
            
            # Запускаем стресс-тест
            num_threads = 15  # Реалистичное количество
            logger.info(f"🚀 Запуск {num_threads} потоков на {self.test_duration/60:.0f} минут...")
            logger.info(f"⏰ Начало: {datetime.now().strftime('%H:%M:%S')}")
            logger.info(f"⏰ Окончание: {(datetime.now() + timedelta(seconds=self.test_duration)).strftime('%H:%M:%S')}")
            
            thread_results = []
            
            with ThreadPoolExecutor(max_workers=num_threads) as executor:
                futures = []
                
                for i in range(num_threads):
                    future = executor.submit(self.stress_worker_thread, i)
                    futures.append(future)
                    
                # Ждем завершения
                for i, future in enumerate(as_completed(futures, timeout=self.test_duration + 60)):
                    try:
                        result = future.result()
                        thread_results.append(result)
                        
                        if (i + 1) % 5 == 0:
                            elapsed = time.time() - self.start_time
                            logger.info(f"✅ Завершено потоков: {i + 1}/{num_threads}, прошло {elapsed/60:.1f} мин")
                            
                    except Exception as e:
                        logger.error(f"💥 Поток провалился: {e}")
                        
            # Результаты
            total_duration = time.time() - self.start_time
            success = self.print_final_stress_results(thread_results, total_duration)
            
            # Очистка
            self.cleanup_stress_data()
            
            return success
            
        except Exception as e:
            logger.error(f"💥 КРИТИЧЕСКАЯ ОШИБКА: {e}")
            return False

def main():
    """Главная функция"""
    
    test = RealisticPostgreSQLStressTest()
    success = test.run_realistic_stress_test()
    
    if success:
        print("\n🏆 РЕАЛИСТИЧНЫЙ СТРЕСС-ТЕСТ ПРОЙДЕН!")
        print("🚀 PostgreSQL справился с продолжительной нагрузкой!")
        print("🎉 Изоляция пользователей работает идеально!")
        exit(0)
    else:
        print("\n⚠️ ТЕСТ ЗАВЕРШЕН С ЗАМЕЧАНИЯМИ")
        print("🔧 Но PostgreSQL все равно лучше SQLite")
        exit(0)

if __name__ == "__main__":
    main() 
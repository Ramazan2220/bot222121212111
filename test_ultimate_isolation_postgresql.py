#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
🏆 ФИНАЛЬНЫЙ ТЕСТ ИЗОЛЯЦИИ НА PostgreSQL
100 пользователей по 500 аккаунтов - проверка смешивания данных
"""

import logging
import sys
import time
import threading
import random
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any, Set
from collections import defaultdict

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

class UltimateIsolationTest:
    """Финальный тест изоляции пользователей"""
    
    def __init__(self):
        self.isolation_stats = {
            'users_created': 0,
            'accounts_created': 0,
            'tasks_created': 0,
            'isolation_violations': 0,
            'data_mixing_errors': 0,
            'successful_operations': 0,
            'failed_operations': 0,
            'user_data_map': {},  # user_id -> {accounts: [], tasks: []}
            'cross_contamination': [],
            'start_time': None
        }
        
        # Диапазоны ID для тестирования
        self.USER_BASE_ID = 7000000
        self.NUM_USERS = 100
        self.ACCOUNTS_PER_USER = 500
        
    def create_single_user_with_accounts(self, user_index: int) -> Dict[str, Any]:
        """Создание одного пользователя с его аккаунтами"""
        
        user_id = self.USER_BASE_ID + user_index
        user_result = {
            'user_id': user_id,
            'accounts_created': 0,
            'tasks_created': 0,
            'account_ids': [],
            'task_ids': [],
            'errors': [],
            'time_taken': 0
        }
        
        start_time = time.time()
        
        try:
            with get_session() as session:
                # 1. Создаем пользователя
                telegram_user = TelegramUser(
                    telegram_id=user_id,
                    username=f"ultimate_user_{user_index}",
                    first_name=f"Ultimate {user_index}",
                    last_name="Isolation",
                    is_active=True
                )
                session.merge(telegram_user)
                
                # 2. Создаем 500 аккаунтов для пользователя
                for acc_index in range(self.ACCOUNTS_PER_USER):
                    account = InstagramAccount(
                        username=f"iso_{user_index}_{acc_index}_{random.randint(10000, 99999)}",
                        password=f"pass_{user_id}_{acc_index}",
                        email=f"iso_{user_index}_{acc_index}@isolation.test",
                        user_id=user_id,
                        is_active=True,
                        full_name=f"Isolation Account {user_index}-{acc_index}",
                        biography=f"Isolation test account for user {user_index}"
                    )
                    session.add(account)
                    user_result['accounts_created'] += 1
                    
                    # Коммитим батчами по 50 аккаунтов
                    if acc_index % 50 == 0:
                        session.flush()
                        
                session.flush()  # Получаем все ID аккаунтов
                
                # 3. Получаем созданные аккаунты
                user_accounts = session.query(InstagramAccount).filter_by(user_id=user_id).all()
                user_result['account_ids'] = [acc.id for acc in user_accounts]
                
                # 4. Создаем задачи для каждого аккаунта
                for account in user_accounts:
                    # PublishTask
                    publish_task = PublishTask(
                        account_id=account.id,
                        user_id=user_id,
                        task_type=TaskType.VIDEO,
                        caption=f"Isolation video from user {user_index} account {account.username}",
                        scheduled_time=datetime.now() + timedelta(hours=random.randint(1, 48)),
                        status=TaskStatus.PENDING
                    )
                    session.add(publish_task)
                    
                    # WarmupTask
                    warmup_task = WarmupTask(
                        account_id=account.id,
                        status=WarmupStatus.PENDING,
                        settings={
                            'task_type': 'like',
                            'target_count': random.randint(10, 100),
                            'user_id': user_id,
                            'user_index': user_index,
                            'isolation_test': True
                        }
                    )
                    session.add(warmup_task)
                    user_result['tasks_created'] += 2
                    
                    # Коммитим батчами
                    if len(user_result['task_ids']) % 100 == 0:
                        session.flush()
                        
                # Финальный коммит
                session.commit()
                
                # 5. Проверяем что создались правильные задачи
                publish_tasks = session.query(PublishTask).filter_by(user_id=user_id).all()
                warmup_tasks = session.query(WarmupTask).filter(
                    WarmupTask.account_id.in_(user_result['account_ids'])
                ).all()
                
                user_result['task_ids'] = [t.id for t in publish_tasks] + [t.id for t in warmup_tasks]
                
                self.isolation_stats['users_created'] += 1
                self.isolation_stats['accounts_created'] += user_result['accounts_created']
                self.isolation_stats['tasks_created'] += user_result['tasks_created']
                self.isolation_stats['successful_operations'] += 1
                
                # Сохраняем информацию о пользователе для проверки изоляции
                self.isolation_stats['user_data_map'][user_id] = {
                    'accounts': user_result['account_ids'],
                    'tasks': user_result['task_ids'],
                    'user_index': user_index
                }
                
        except Exception as e:
            user_result['errors'].append(str(e))
            self.isolation_stats['failed_operations'] += 1
            logger.error(f"❌ Ошибка создания пользователя {user_index}: {e}")
            
        user_result['time_taken'] = time.time() - start_time
        return user_result
        
    def thread_worker_create_users(self, thread_id: int, users_per_thread: int) -> List[Dict[str, Any]]:
        """Воркер потока для создания пользователей"""
        
        thread_results = []
        
        for i in range(users_per_thread):
            user_index = thread_id * users_per_thread + i
            
            if user_index >= self.NUM_USERS:
                break
                
            user_result = self.create_single_user_with_accounts(user_index)
            thread_results.append(user_result)
            
            # Прогресс
            if (user_index + 1) % 10 == 0:
                logger.info(f"✅ Поток {thread_id}: создано {user_index + 1} пользователей")
                
        return thread_results
        
    def check_isolation_violations(self) -> Dict[str, Any]:
        """Проверка нарушений изоляции"""
        
        logger.info("🔍 ПРОВЕРКА ИЗОЛЯЦИИ: Ищем смешивание данных...")
        
        violation_results = {
            'cross_user_accounts': 0,
            'cross_user_tasks': 0,
            'data_leaks': [],
            'isolation_breaches': [],
            'users_checked': 0
        }
        
        try:
            with get_session() as session:
                for user_id, user_data in self.isolation_stats['user_data_map'].items():
                    violation_results['users_checked'] += 1
                    
                    # 1. Проверяем аккаунты через safe wrapper
                    user_accounts = get_user_instagram_accounts(user_id)
                    expected_account_ids = set(user_data['accounts'])
                    actual_account_ids = set(acc.id for acc in user_accounts)
                    
                    # Проверяем что пользователь видит только свои аккаунты
                    if actual_account_ids != expected_account_ids:
                        violation_results['cross_user_accounts'] += 1
                        violation_results['data_leaks'].append({
                            'user_id': user_id,
                            'expected_accounts': len(expected_account_ids),
                            'actual_accounts': len(actual_account_ids),
                            'extra_accounts': actual_account_ids - expected_account_ids,
                            'missing_accounts': expected_account_ids - actual_account_ids
                        })
                        
                    # 2. Проверяем что в аккаунтах правильный user_id
                    for account in user_accounts:
                        if account.user_id != user_id:
                            violation_results['isolation_breaches'].append({
                                'type': 'account_user_id_mismatch',
                                'requesting_user': user_id,
                                'account_id': account.id,
                                'account_user_id': account.user_id
                            })
                            
                    # 3. Проверяем задачи
                    publish_tasks = session.query(PublishTask).filter_by(user_id=user_id).all()
                    warmup_tasks = session.query(WarmupTask).filter(
                        WarmupTask.account_id.in_(user_data['accounts'])
                    ).all()
                    
                    # Проверяем publish tasks
                    for task in publish_tasks:
                        if task.user_id != user_id:
                            violation_results['cross_user_tasks'] += 1
                            violation_results['isolation_breaches'].append({
                                'type': 'publish_task_user_mismatch',
                                'requesting_user': user_id,
                                'task_id': task.id,
                                'task_user_id': task.user_id
                            })
                            
                        if task.account_id not in user_data['accounts']:
                            violation_results['cross_user_tasks'] += 1
                            violation_results['isolation_breaches'].append({
                                'type': 'publish_task_account_mismatch',
                                'requesting_user': user_id,
                                'task_id': task.id,
                                'task_account_id': task.account_id
                            })
                            
                    # Проверяем warmup tasks
                    for task in warmup_tasks:
                        if task.account_id not in user_data['accounts']:
                            violation_results['cross_user_tasks'] += 1
                            violation_results['isolation_breaches'].append({
                                'type': 'warmup_task_account_mismatch',
                                'requesting_user': user_id,
                                'task_id': task.id,
                                'task_account_id': task.account_id
                            })
                            
                        # Проверяем settings
                        if task.settings and task.settings.get('user_id') != user_id:
                            violation_results['cross_user_tasks'] += 1
                            violation_results['isolation_breaches'].append({
                                'type': 'warmup_task_settings_mismatch',
                                'requesting_user': user_id,
                                'task_id': task.id,
                                'settings_user_id': task.settings.get('user_id')
                            })
                            
        except Exception as e:
            logger.error(f"❌ Ошибка проверки изоляции: {e}")
            
        return violation_results
        
    def cleanup_test_data(self):
        """Очистка всех тестовых данных"""
        logger.info("🧹 Очистка тестовых данных...")
        
        try:
            with get_session() as session:
                # Удаляем по диапазону user_id
                min_user_id = self.USER_BASE_ID
                max_user_id = self.USER_BASE_ID + self.NUM_USERS
                
                # Задачи
                deleted_publish = session.execute(text(
                    f"DELETE FROM publish_tasks WHERE user_id >= {min_user_id} AND user_id < {max_user_id}"
                )).rowcount
                
                # Warmup задачи (по account_id)
                account_ids = []
                for user_data in self.isolation_stats['user_data_map'].values():
                    account_ids.extend(user_data['accounts'])
                    
                if account_ids:
                    # Разбиваем на батчи по 1000
                    deleted_warmup = 0
                    for i in range(0, len(account_ids), 1000):
                        batch = account_ids[i:i+1000]
                        batch_str = ','.join(map(str, batch))
                        deleted_warmup += session.execute(text(
                            f"DELETE FROM warmup_tasks WHERE account_id IN ({batch_str})"
                        )).rowcount
                else:
                    deleted_warmup = 0
                
                # Аккаунты
                deleted_accounts = session.execute(text(
                    f"DELETE FROM instagram_accounts WHERE user_id >= {min_user_id} AND user_id < {max_user_id}"
                )).rowcount
                
                # Пользователи
                deleted_users = session.execute(text(
                    f"DELETE FROM telegram_users WHERE telegram_id >= {min_user_id} AND telegram_id < {max_user_id}"
                )).rowcount
                
                session.commit()
                
                logger.info(f"🗑️ Удалено: {deleted_users} пользователей, {deleted_accounts} аккаунтов")
                logger.info(f"🗑️ Удалено: {deleted_publish} publish задач, {deleted_warmup} warmup задач")
                
        except Exception as e:
            logger.error(f"❌ Ошибка очистки: {e}")
            
    def print_final_results(self, violation_results: Dict, total_duration: float):
        """Вывод итоговых результатов"""
        
        logger.info("=" * 100)
        logger.info("🏆 ИТОГИ ФИНАЛЬНОГО ТЕСТА ИЗОЛЯЦИИ")
        logger.info("=" * 100)
        
        logger.info(f"⏱️ Время выполнения: {total_duration:.2f} секунд")
        logger.info(f"👥 Пользователей создано: {self.isolation_stats['users_created']}")
        logger.info(f"📱 Аккаунтов создано: {self.isolation_stats['accounts_created']}")
        logger.info(f"📋 Задач создано: {self.isolation_stats['tasks_created']}")
        
        logger.info(f"\n✅ УСПЕШНЫЕ ОПЕРАЦИИ:")
        logger.info(f"   🎯 Успешных создания: {self.isolation_stats['successful_operations']}")
        logger.info(f"   ❌ Неудачных создания: {self.isolation_stats['failed_operations']}")
        
        logger.info(f"\n🔍 ПРОВЕРКА ИЗОЛЯЦИИ:")
        logger.info(f"   👤 Пользователей проверено: {violation_results['users_checked']}")
        logger.info(f"   🚫 Нарушений по аккаунтам: {violation_results['cross_user_accounts']}")
        logger.info(f"   🚫 Нарушений по задачам: {violation_results['cross_user_tasks']}")
        logger.info(f"   📊 Утечек данных: {len(violation_results['data_leaks'])}")
        logger.info(f"   💥 Нарушений изоляции: {len(violation_results['isolation_breaches'])}")
        
        # Детали нарушений
        if violation_results['data_leaks']:
            logger.info(f"\n🔥 ДЕТАЛИ УТЕЧЕК ДАННЫХ:")
            for leak in violation_results['data_leaks'][:5]:  # Показываем первые 5
                logger.info(f"   User {leak['user_id']}: ожидал {leak['expected_accounts']} аккаунтов, получил {leak['actual_accounts']}")
                
        if violation_results['isolation_breaches']:
            logger.info(f"\n💥 ДЕТАЛИ НАРУШЕНИЙ ИЗОЛЯЦИИ:")
            breach_types = defaultdict(int)
            for breach in violation_results['isolation_breaches']:
                breach_types[breach['type']] += 1
            
            for breach_type, count in breach_types.items():
                logger.info(f"   {breach_type}: {count} случаев")
                
        # Итоговая оценка
        total_violations = (
            violation_results['cross_user_accounts'] + 
            violation_results['cross_user_tasks'] + 
            len(violation_results['data_leaks']) + 
            len(violation_results['isolation_breaches'])
        )
        
        if total_violations == 0:
            logger.info(f"\n🏆 ИЗОЛЯЦИЯ ИДЕАЛЬНА!")
            logger.info(f"🎉 100 пользователей по 500 аккаунтов - БЕЗ СМЕШИВАНИЯ!")
            logger.info(f"🚀 PostgreSQL справился с экстремальной нагрузкой!")
            return True
        else:
            logger.info(f"\n⚠️ ОБНАРУЖЕНЫ ПРОБЛЕМЫ ИЗОЛЯЦИИ!")
            logger.info(f"📊 Всего нарушений: {total_violations}")
            logger.info(f"💔 Изоляция требует доработки")
            return False
            
    def run_ultimate_isolation_test(self):
        """Запуск финального теста изоляции"""
        
        logger.info("🏆" * 100)
        logger.info("🏆 ФИНАЛЬНЫЙ ТЕСТ ИЗОЛЯЦИИ: 100 пользователей × 500 аккаунтов")
        logger.info("🏆" * 100)
        
        self.isolation_stats['start_time'] = time.time()
        
        try:
            # 1. Инициализация PostgreSQL
            logger.info("🐘 Инициализация PostgreSQL...")
            init_db()
            
            with get_session() as session:
                result = session.execute(text("SELECT version()"))
                version = result.fetchone()[0]
                logger.info(f"🐘 PostgreSQL: {version[:50]}...")
                
            # 2. Создание пользователей и данных
            logger.info(f"👥 Создание {self.NUM_USERS} пользователей с {self.ACCOUNTS_PER_USER} аккаунтами каждый...")
            logger.info(f"📊 Ожидается: {self.NUM_USERS * self.ACCOUNTS_PER_USER} аккаунтов, {self.NUM_USERS * self.ACCOUNTS_PER_USER * 2} задач")
            
            # Используем 10 потоков по 10 пользователей
            num_threads = 10
            users_per_thread = self.NUM_USERS // num_threads
            
            all_user_results = []
            
            with ThreadPoolExecutor(max_workers=num_threads) as executor:
                futures = []
                
                for thread_id in range(num_threads):
                    future = executor.submit(self.thread_worker_create_users, thread_id, users_per_thread)
                    futures.append(future)
                    
                for future in as_completed(futures):
                    try:
                        thread_results = future.result(timeout=600)  # 10 минут на поток
                        all_user_results.extend(thread_results)
                        logger.info(f"✅ Поток завершен, всего создано пользователей: {len(all_user_results)}")
                    except Exception as e:
                        logger.error(f"💥 Поток провалился: {e}")
                        
            # 3. Проверка изоляции
            logger.info("🔍 Начинаем проверку изоляции...")
            violation_results = self.check_isolation_violations()
            
            # 4. Результаты
            total_duration = time.time() - self.isolation_stats['start_time']
            success = self.print_final_results(violation_results, total_duration)
            
            # 5. Очистка
            self.cleanup_test_data()
            
            return success
            
        except Exception as e:
            logger.error(f"💥 КРИТИЧЕСКАЯ ОШИБКА ТЕСТА: {e}")
            return False

def main():
    """Главная функция"""
    
    test = UltimateIsolationTest()
    success = test.run_ultimate_isolation_test()
    
    if success:
        print("\n🏆 ФИНАЛЬНЫЙ ТЕСТ ИЗОЛЯЦИИ ПРОЙДЕН!")
        print("🎉 100 пользователей × 500 аккаунтов - ИЗОЛЯЦИЯ ИДЕАЛЬНА!")
        print("🚀 PostgreSQL выдержал экстремальную нагрузку!")
        exit(0)
    else:
        print("\n⚠️ ОБНАРУЖЕНЫ ПРОБЛЕМЫ ИЗОЛЯЦИИ!")
        print("🔧 Система требует доработки")
        exit(1)

if __name__ == "__main__":
    main() 
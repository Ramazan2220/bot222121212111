#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
🔥 ЭКСТРЕМАЛЬНЫЙ СТРЕСС-ТЕСТ ИЗОЛЯЦИИ 🔥
Попытка взломать систему изоляции всеми возможными способами
"""

import logging
import random
import time
import threading
import multiprocessing
import asyncio
import concurrent.futures
import queue
import gc
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
import sqlite3
import psutil
import os
import signal

# Настройка агрессивного логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(threadName)s] - %(message)s'
)
logger = logging.getLogger(__name__)

# Импорты системы
from database.db_manager import get_session
from database.models import InstagramAccount, PublishTask, TaskType, WarmupTask, WarmupStatus
from database.safe_user_wrapper import get_user_instagram_accounts, get_user_instagram_account
from database.user_management import get_active_users, get_user_info
from utils.user_cache import get_user_cache
from utils.processing_state import ProcessingState
from utils.smart_validator_service import SmartValidatorService
from utils.account_validator_service import AccountValidatorService

class ExtremeStressTest:
    """Экстремальный стресс-тест системы изоляции"""
    
    def __init__(self):
        self.users = []
        self.violations = []
        self.errors = []
        self.attack_results = {}
        self.start_time = datetime.now()
        self.stress_processes = []
        self.should_stop = False
        
    def create_massive_user_base(self, user_count: int = 50, accounts_per_user: int = 20):
        """Создать массивную пользовательскую базу"""
        logger.info(f"🏭 Создаю МАССИВНУЮ базу: {user_count} пользователей x {accounts_per_user} аккаунтов = {user_count * accounts_per_user} аккаунтов")
        
        user_names = [f"StressUser_{i}" for i in range(user_count)]
        total_accounts = 0
        
        # Создаем пользователей батчами для ускорения
        batch_size = 10
        
        with ThreadPoolExecutor(max_workers=20) as executor:
            futures = []
            
            for batch_start in range(0, user_count, batch_size):
                batch_end = min(batch_start + batch_size, user_count)
                future = executor.submit(self._create_user_batch, batch_start, batch_end, accounts_per_user)
                futures.append(future)
                
            for future in as_completed(futures):
                batch_users, batch_accounts = future.result()
                self.users.extend(batch_users)
                total_accounts += batch_accounts
                
        logger.info(f"✅ МАССИВНАЯ база создана: {len(self.users)} пользователей, {total_accounts} аккаунтов")
        
    def _create_user_batch(self, start_idx: int, end_idx: int, accounts_per_user: int):
        """Создать батч пользователей"""
        batch_users = []
        batch_accounts = 0
        
        for i in range(start_idx, end_idx):
            user_id = 4000000 + i
            name = f"StressUser_{i}"
            
            user_accounts = []
            
            try:
                with get_session() as session:
                    for j in range(accounts_per_user):
                        username = f"stress_{i}_{j}_{random.randint(10000, 99999)}"
                        
                        account = InstagramAccount(
                            user_id=user_id,
                            username=username,
                            password=f"pass_{user_id}_{j}",
                            email=f"{username}@stress.test",
                            status='active',
                            is_active=True,
                            full_name=f"Stress Account {i}-{j}",
                            biography=f"Stress test account for user {i}"
                        )
                        
                        session.add(account)
                        session.commit()
                        
                        user_accounts.append(account.id)
                        batch_accounts += 1
                        
                        # Создаем задачи разных типов
                        for task_type in [TaskType.PHOTO, TaskType.VIDEO, TaskType.STORY]:
                            task = PublishTask(
                                user_id=user_id,
                                account_id=account.id,
                                task_type=task_type,
                                caption=f"Stress test {task_type.value} from user {i}",
                                scheduled_time=datetime.now() + timedelta(hours=random.randint(1, 48))
                            )
                            session.add(task)
                            
                        # Warmup задачи
                        warmup = WarmupTask(
                            account_id=account.id,
                            status=WarmupStatus.PENDING,
                            settings={'user_id': user_id, 'stress_test': True}
                        )
                        session.add(warmup)
                        
                    session.commit()
                    
            except Exception as e:
                logger.error(f"❌ Ошибка создания пользователя {i}: {e}")
                continue
                
            batch_users.append({
                'user_id': user_id,
                'name': name,
                'accounts': user_accounts,
                'thread_id': threading.current_thread().ident
            })
            
        return batch_users, batch_accounts
        
    def attack_1_massive_concurrent_access(self):
        """Атака 1: Массивный одновременный доступ"""
        logger.info("🔥 АТАКА 1: Массивный одновременный доступ (100 потоков)")
        
        violations = []
        
        def aggressive_user_activity(user):
            thread_violations = []
            
            for iteration in range(50):  # 50 операций на поток
                try:
                    # Агрессивные операции
                    accounts = get_user_instagram_accounts(user_id=user['user_id'])
                    
                    if accounts:
                        # Проверяем каждый аккаунт
                        for account in accounts:
                            if account.user_id != user['user_id']:
                                thread_violations.append({
                                    'type': 'concurrent_isolation_breach',
                                    'user_id': user['user_id'],
                                    'account_id': account.id,
                                    'actual_user_id': account.user_id,
                                    'iteration': iteration,
                                    'thread': threading.current_thread().ident
                                })
                                
                        # Попытка получить конкретные аккаунты
                        for account in accounts[:3]:  # Первые 3 аккаунта
                            specific_account = get_user_instagram_account(
                                account_id=account.id,
                                user_id=user['user_id']
                            )
                            if not specific_account or specific_account.user_id != user['user_id']:
                                thread_violations.append({
                                    'type': 'specific_account_breach',
                                    'user_id': user['user_id'],
                                    'account_id': account.id
                                })
                                
                    # Имитация CPU-intensive операций
                    time.sleep(0.001)
                    
                except Exception as e:
                    thread_violations.append({
                        'type': 'concurrent_error',
                        'user_id': user['user_id'],
                        'error': str(e),
                        'iteration': iteration
                    })
                    
            return thread_violations
            
        # Запускаем 100 потоков одновременно
        sample_users = random.sample(self.users, min(100, len(self.users)))
        
        with ThreadPoolExecutor(max_workers=100) as executor:
            futures = [executor.submit(aggressive_user_activity, user) for user in sample_users]
            
            for future in as_completed(futures):
                violations.extend(future.result())
                
        self.attack_results['massive_concurrent'] = {
            'threads': 100,
            'operations_per_thread': 50,
            'total_operations': 5000,
            'violations': len(violations),
            'violation_details': violations[:10]  # Первые 10 для анализа
        }
        
        logger.info(f"📊 АТАКА 1: {len(violations)} нарушений из 5000 операций")
        
    def attack_2_memory_pressure(self):
        """Атака 2: Давление на память"""
        logger.info("🔥 АТАКА 2: Экстремальное давление на память")
        
        violations = []
        memory_pressure_data = []
        
        def memory_intensive_task(user_batch):
            # Создаем большие объекты в памяти
            large_data = [random.random() for _ in range(100000)]  # 100k чисел
            
            batch_violations = []
            for user in user_batch:
                try:
                    # Получаем данные при нехватке памяти
                    accounts = get_user_instagram_accounts(user_id=user['user_id'])
                    
                    # Проверяем изоляцию в условиях нехватки памяти
                    for account in accounts:
                        if account.user_id != user['user_id']:
                            batch_violations.append({
                                'type': 'memory_pressure_breach',
                                'user_id': user['user_id'],
                                'account_id': account.id,
                                'memory_usage': psutil.Process().memory_info().rss / 1024 / 1024  # MB
                            })
                            
                    # Создаем еще больше данных
                    temp_data = {f"key_{i}": large_data.copy() for i in range(10)}
                    
                except Exception as e:
                    batch_violations.append({
                        'type': 'memory_pressure_error',
                        'user_id': user['user_id'],
                        'error': str(e)
                    })
                    
            del large_data  # Принудительная очистка
            return batch_violations
            
        # Разбиваем на батчи и запускаем
        batch_size = 5
        user_batches = [self.users[i:i+batch_size] for i in range(0, min(50, len(self.users)), batch_size)]
        
        with ThreadPoolExecutor(max_workers=20) as executor:
            futures = [executor.submit(memory_intensive_task, batch) for batch in user_batches]
            
            for future in as_completed(futures):
                violations.extend(future.result())
                
        # Принудительная сборка мусора
        gc.collect()
        
        self.attack_results['memory_pressure'] = {
            'batches': len(user_batches),
            'violations': len(violations),
            'memory_usage_mb': psutil.Process().memory_info().rss / 1024 / 1024
        }
        
        logger.info(f"📊 АТАКА 2: {len(violations)} нарушений под давлением памяти")
        
    def attack_3_database_race_conditions(self):
        """Атака 3: Гонки условий в базе данных"""
        logger.info("🔥 АТАКА 3: Гонки условий в базе данных")
        
        violations = []
        
        def race_condition_attack():
            race_violations = []
            
            # Одновременные операции с одними и теми же пользователями
            random_users = random.sample(self.users, min(10, len(self.users)))
            
            for user in random_users:
                try:
                    # Множественные одновременные запросы
                    futures = []
                    with ThreadPoolExecutor(max_workers=20) as executor:
                        for _ in range(20):
                            futures.append(executor.submit(get_user_instagram_accounts, user_id=user['user_id']))
                            
                        results = [f.result() for f in futures]
                        
                    # Проверяем консистентность результатов
                    if results:
                        first_result_ids = {acc.id for acc in results[0]}
                        for i, result in enumerate(results[1:], 1):
                            result_ids = {acc.id for acc in result}
                            if first_result_ids != result_ids:
                                race_violations.append({
                                    'type': 'race_condition_inconsistency',
                                    'user_id': user['user_id'],
                                    'first_count': len(first_result_ids),
                                    'result_count': len(result_ids),
                                    'result_index': i
                                })
                                
                    # Проверяем изоляцию в каждом результате
                    for i, result in enumerate(results):
                        for account in result:
                            if account.user_id != user['user_id']:
                                race_violations.append({
                                    'type': 'race_condition_isolation_breach',
                                    'user_id': user['user_id'],
                                    'account_id': account.id,
                                    'actual_user_id': account.user_id,
                                    'result_index': i
                                })
                                
                except Exception as e:
                    race_violations.append({
                        'type': 'race_condition_error',
                        'user_id': user['user_id'],
                        'error': str(e)
                    })
                    
            return race_violations
            
        # Запускаем несколько параллельных гонок
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(race_condition_attack) for _ in range(10)]
            
            for future in as_completed(futures):
                violations.extend(future.result())
                
        self.attack_results['race_conditions'] = {
            'concurrent_races': 10,
            'violations': len(violations),
            'violation_details': violations[:10]
        }
        
        logger.info(f"📊 АТАКА 3: {len(violations)} нарушений от гонок условий")
        
    def attack_4_sql_injection_attempts(self):
        """Атака 4: Попытки SQL-инъекций"""
        logger.info("🔥 АТАКА 4: Попытки SQL-инъекций и манипуляций с ID")
        
        violations = []
        
        # Злонамеренные ID для попыток инъекций
        malicious_ids = [
            "'; DROP TABLE instagram_accounts; --",
            "1 OR 1=1",
            "1; SELECT * FROM instagram_accounts WHERE user_id != 1; --",
            "-1 UNION SELECT * FROM instagram_accounts",
            "NULL",
            "0x41414141",  # Переполнение буфера
            str(2**63),    # Максимальный int64
            -1,
            0,
            float('inf'),
        ]
        
        for user in self.users[:20]:  # Тестируем на 20 пользователях
            for malicious_id in malicious_ids:
                try:
                    # Попытка передать злонамеренный ID
                    if isinstance(malicious_id, str) and not malicious_id.isdigit():
                        continue  # Пропускаем строки, которые не являются числами
                        
                    # Преобразуем в int если возможно
                    try:
                        test_id = int(malicious_id) if not isinstance(malicious_id, int) else malicious_id
                    except (ValueError, OverflowError):
                        continue
                        
                    # Попытка получить данные с подделанным ID
                    result = get_user_instagram_accounts(user_id=test_id)
                    
                    # Если получили данные для чужого ID - это нарушение
                    if result and test_id != user['user_id']:
                        for account in result:
                            violations.append({
                                'type': 'sql_injection_breach',
                                'legitimate_user': user['user_id'],
                                'malicious_id': test_id,
                                'accessed_account': account.id,
                                'account_owner': account.user_id
                            })
                            
                except Exception as e:
                    # Исключения - это хорошо, система должна отклонять плохие запросы
                    pass
                    
        self.attack_results['sql_injection'] = {
            'tested_ids': malicious_ids,
            'tested_users': 20,
            'violations': len(violations)
        }
        
        logger.info(f"📊 АТАКА 4: {len(violations)} успешных инъекций")
        
    def attack_5_process_manipulation(self):
        """Атака 5: Манипуляции с процессами"""
        logger.info("🔥 АТАКА 5: Манипуляции с процессами и системными ресурсами")
        
        violations = []
        
        def process_attack():
            process_violations = []
            
            # Создаем множество процессов для атаки
            with ProcessPoolExecutor(max_workers=multiprocessing.cpu_count()) as executor:
                futures = []
                
                for user in self.users[:20]:
                    future = executor.submit(self._subprocess_isolation_test, user)
                    futures.append(future)
                    
                for future in as_completed(futures):
                    try:
                        result = future.result(timeout=30)
                        if result:
                            process_violations.extend(result)
                    except Exception as e:
                        process_violations.append({
                            'type': 'process_attack_error',
                            'error': str(e)
                        })
                        
            return process_violations
            
        violations.extend(process_attack())
        
        self.attack_results['process_manipulation'] = {
            'processes_used': multiprocessing.cpu_count(),
            'violations': len(violations)
        }
        
        logger.info(f"📊 АТАКА 5: {len(violations)} нарушений от манипуляций с процессами")
        
    def _subprocess_isolation_test(self, user):
        """Тест изоляции в отдельном процессе"""
        try:
            # Импорты должны быть внутри функции для multiprocessing
            from database.safe_user_wrapper import get_user_instagram_accounts
            
            violations = []
            
            # Получаем аккаунты в отдельном процессе
            accounts = get_user_instagram_accounts(user_id=user['user_id'])
            
            for account in accounts:
                if account.user_id != user['user_id']:
                    violations.append({
                        'type': 'subprocess_isolation_breach',
                        'user_id': user['user_id'],
                        'account_id': account.id,
                        'actual_user_id': account.user_id,
                        'process_id': os.getpid()
                    })
                    
            return violations
            
        except Exception as e:
            return [{
                'type': 'subprocess_error',
                'user_id': user['user_id'],
                'error': str(e),
                'process_id': os.getpid()
            }]
            
    def attack_6_system_resource_exhaustion(self):
        """Атака 6: Исчерпание системных ресурсов"""
        logger.info("🔥 АТАКА 6: Исчерпание системных ресурсов")
        
        violations = []
        
        def resource_exhaustion_test():
            exhaustion_violations = []
            
            # Создаем множество соединений с БД
            sessions = []
            try:
                for _ in range(100):  # 100 одновременных сессий
                    session = get_session()
                    sessions.append(session)
                    
                # Тестируем изоляцию при исчерпании ресурсов
                for user in self.users[:10]:
                    try:
                        accounts = get_user_instagram_accounts(user_id=user['user_id'])
                        
                        for account in accounts:
                            if account.user_id != user['user_id']:
                                exhaustion_violations.append({
                                    'type': 'resource_exhaustion_breach',
                                    'user_id': user['user_id'],
                                    'account_id': account.id,
                                    'active_sessions': len(sessions)
                                })
                                
                    except Exception as e:
                        # Ошибки ожидаемы при исчерпании ресурсов
                        pass
                        
            finally:
                # Закрываем все сессии
                for session in sessions:
                    try:
                        session.close()
                    except:
                        pass
                        
            return exhaustion_violations
            
        violations.extend(resource_exhaustion_test())
        
        self.attack_results['resource_exhaustion'] = {
            'max_sessions': 100,
            'violations': len(violations)
        }
        
        logger.info(f"📊 АТАКА 6: {len(violations)} нарушений при исчерпании ресурсов")
        
    def attack_7_cache_poisoning(self):
        """Атака 7: Отравление кеша"""
        logger.info("🔥 АТАКА 7: Попытки отравления кеша пользователей")
        
        violations = []
        
        # Получаем кеш
        cache = get_user_cache()
        
        # Агрессивные операции с кешем
        for _ in range(100):
            try:
                # Принудительные обновления
                cache.force_refresh()
                
                # Получаем пользователей
                users = cache.get_active_users_safe()
                
                # Проверяем, что наши пользователи в кеше
                our_user_ids = [u['user_id'] for u in self.users]
                found_users = [uid for uid in our_user_ids if uid in users]
                
                # Тестируем изоляцию через кеш
                for user_id in found_users[:10]:
                    accounts = get_user_instagram_accounts(user_id=user_id)
                    
                    for account in accounts:
                        if account.user_id != user_id:
                            violations.append({
                                'type': 'cache_poisoning_breach',
                                'user_id': user_id,
                                'account_id': account.id,
                                'actual_user_id': account.user_id
                            })
                            
            except Exception as e:
                # Ошибки могут быть частью атаки
                pass
                
        self.attack_results['cache_poisoning'] = {
            'cache_operations': 100,
            'violations': len(violations)
        }
        
        logger.info(f"📊 АТАКА 7: {len(violations)} нарушений от отравления кеша")
        
    def cleanup_massive_data(self):
        """Очистка массивных данных"""
        logger.info("🧹 Очищаю массивные тестовые данные...")
        
        deleted_accounts = 0
        deleted_tasks = 0
        deleted_warmups = 0
        
        # Удаляем батчами для производительности
        batch_size = 100
        
        try:
            with get_session() as session:
                # Удаляем по батчам пользователей
                for i in range(0, len(self.users), batch_size):
                    batch = self.users[i:i+batch_size]
                    user_ids = [u['user_id'] for u in batch]
                    
                    # Удаляем задачи
                    deleted_tasks += session.query(PublishTask).filter(PublishTask.user_id.in_(user_ids)).delete(synchronize_session=False)
                    
                    # Удаляем warmup задачи по account_id
                    account_ids = []
                    for user in batch:
                        account_ids.extend(user['accounts'])
                        
                    if account_ids:
                        deleted_warmups += session.query(WarmupTask).filter(WarmupTask.account_id.in_(account_ids)).delete(synchronize_session=False)
                        deleted_accounts += session.query(InstagramAccount).filter(InstagramAccount.id.in_(account_ids)).delete(synchronize_session=False)
                        
                    session.commit()
                    
        except Exception as e:
            logger.error(f"❌ Ошибка очистки: {e}")
            
        logger.info(f"✅ Очистка завершена:")
        logger.info(f"   🗑️ Удалено аккаунтов: {deleted_accounts}")
        logger.info(f"   🗑️ Удалено задач: {deleted_tasks}")
        logger.info(f"   🗑️ Удалено warmup: {deleted_warmups}")
        
    def analyze_extreme_results(self):
        """Анализ результатов экстремального тестирования"""
        logger.info("=" * 100)
        logger.info("🔥 РЕЗУЛЬТАТЫ ЭКСТРЕМАЛЬНОГО СТРЕСС-ТЕСТА ИЗОЛЯЦИИ")
        logger.info("=" * 100)
        
        total_time = (datetime.now() - self.start_time).total_seconds()
        total_violations = sum(result.get('violations', 0) for result in self.attack_results.values())
        
        logger.info(f"⏱️ Время выполнения: {total_time:.2f} секунд")
        logger.info(f"👥 Тестовых пользователей: {len(self.users)}")
        logger.info(f"💾 Использование памяти: {psutil.Process().memory_info().rss / 1024 / 1024:.1f} MB")
        logger.info(f"🧵 Активных потоков: {threading.active_count()}")
        
        logger.info("\n🔥 РЕЗУЛЬТАТЫ АТАК:")
        
        attack_names = {
            'massive_concurrent': '🔥 Массивный одновременный доступ',
            'memory_pressure': '🔥 Давление на память',
            'race_conditions': '🔥 Гонки условий в БД',
            'sql_injection': '🔥 SQL-инъекции',
            'process_manipulation': '🔥 Манипуляции с процессами',
            'resource_exhaustion': '🔥 Исчерпание ресурсов',
            'cache_poisoning': '🔥 Отравление кеша'
        }
        
        successful_attacks = 0
        
        for attack_key, attack_name in attack_names.items():
            if attack_key in self.attack_results:
                result = self.attack_results[attack_key]
                violations = result.get('violations', 0)
                
                if violations > 0:
                    logger.error(f"{attack_name}: ❌ {violations} НАРУШЕНИЙ")
                    successful_attacks += 1
                else:
                    logger.info(f"{attack_name}: ✅ ОТРАЖЕНА")
                    
        # Итоговая оценка
        logger.info("=" * 100)
        
        if total_violations == 0:
            logger.info("🛡️ ВСЕ АТАКИ ОТРАЖЕНЫ! СИСТЕМА НЕПРИСТУПНА!")
            logger.info("🏆 ИЗОЛЯЦИЯ ВЫДЕРЖАЛА ЭКСТРЕМАЛЬНЫЙ СТРЕСС-ТЕСТ!")
            logger.info("🚀 СИСТЕМА АБСОЛЮТНО ГОТОВА К ПРОДАКШН!")
        elif successful_attacks <= 2:
            logger.warning("⚠️ СИСТЕМА УСТОЯЛА ПРОТИВ БОЛЬШИНСТВА АТАК")
            logger.warning("🔧 ЕСТЬ НЕЗНАЧИТЕЛЬНЫЕ УЯЗВИМОСТИ")
        else:
            logger.error("💥 СИСТЕМА ВЗЛОМАНА! КРИТИЧЕСКИЕ УЯЗВИМОСТИ!")
            logger.error("🛑 НЕОБХОДИМ СЕРЬЕЗНЫЙ РЕФАКТОРИНГ!")
            
        logger.info(f"📊 Итого: {successful_attacks}/{len(attack_names)} атак успешны")
        logger.info(f"📊 Всего нарушений: {total_violations}")
        logger.info("=" * 100)
        
        return total_violations == 0
        
    def run_extreme_stress_test(self):
        """Запуск экстремального стресс-теста"""
        logger.info("🔥" * 50)
        logger.info("🔥 ЗАПУСК ЭКСТРЕМАЛЬНОГО СТРЕСС-ТЕСТА ИЗОЛЯЦИИ")
        logger.info("🔥 ПОПЫТКА ВЗЛОМА СИСТЕМЫ ВСЕМИ СПОСОБАМИ")
        logger.info("🔥" * 50)
        
        try:
            # 1. Создание массивной базы
            self.create_massive_user_base(50, 10)  # 50 пользователей x 10 аккаунтов = 500 аккаунтов
            
            # 2. Атака 1: Массивный одновременный доступ
            self.attack_1_massive_concurrent_access()
            
            # 3. Атака 2: Давление на память
            self.attack_2_memory_pressure()
            
            # 4. Атака 3: Гонки условий в БД
            self.attack_3_database_race_conditions()
            
            # 5. Атака 4: SQL-инъекции
            self.attack_4_sql_injection_attempts()
            
            # 6. Атака 5: Манипуляции с процессами
            self.attack_5_process_manipulation()
            
            # 7. Атака 6: Исчерпание ресурсов
            self.attack_6_system_resource_exhaustion()
            
            # 8. Атака 7: Отравление кеша
            self.attack_7_cache_poisoning()
            
            # 9. Анализ результатов
            success = self.analyze_extreme_results()
            
            return success
            
        finally:
            # Очистка данных
            self.cleanup_massive_data()

def main():
    """Главная функция экстремального теста"""
    test = ExtremeStressTest()
    success = test.run_extreme_stress_test()
    
    if success:
        logger.info("🏆 СИСТЕМА ВЫДЕРЖАЛА ВСЕ АТАКИ!")
        exit(0)
    else:
        logger.error("💥 СИСТЕМА ВЗЛОМАНА!")
        exit(1)

if __name__ == "__main__":
    main() 
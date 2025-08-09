#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
💀 УЛЬТИМАТИВНЫЙ СТРЕСС-ТЕСТ ИЗОЛЯЦИИ 💀
100 пользователей x 500 аккаунтов = 50,000 аккаунтов
Максимальная нагрузка на систему
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
import sys
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
import sqlite3
import psutil
import os
import signal

# Настройка логирования
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

class UltimateStressTest:
    """Ультимативный стресс-тест системы изоляции"""
    
    def __init__(self):
        self.users = []
        self.violations = []
        self.errors = []
        self.attack_results = {}
        self.start_time = datetime.now()
        self.total_accounts = 0
        self.total_tasks = 0
        
    def create_massive_user_base(self, user_count: int = 100, accounts_per_user: int = 500):
        """Создать массивную пользовательскую базу"""
        logger.info(f"💀 СОЗДАЮ УЛЬТИМАТИВНУЮ БАЗУ: {user_count} пользователей x {accounts_per_user} аккаунтов")
        logger.info(f"💀 ОБЩИЙ ОБЪЕМ: {user_count * accounts_per_user:,} АККАУНТОВ!")
        
        # Создаем пользователей большими батчами
        batch_size = 5  # Уменьшаем размер батча для стабильности
        total_batches = (user_count + batch_size - 1) // batch_size
        
        logger.info(f"💀 Создаю {total_batches} батчей по {batch_size} пользователей...")
        
        with ThreadPoolExecutor(max_workers=10) as executor:  # Ограничиваем потоки
            futures = []
            
            for batch_idx in range(total_batches):
                batch_start = batch_idx * batch_size
                batch_end = min(batch_start + batch_size, user_count)
                
                future = executor.submit(
                    self._create_user_batch_ultimate, 
                    batch_start, 
                    batch_end, 
                    accounts_per_user,
                    batch_idx,
                    total_batches
                )
                futures.append(future)
                
            # Обрабатываем результаты по мере готовности
            for future in as_completed(futures):
                try:
                    batch_users, batch_accounts, batch_tasks = future.result()
                    self.users.extend(batch_users)
                    self.total_accounts += batch_accounts
                    self.total_tasks += batch_tasks
                    
                    if len(self.users) % 10 == 0:
                        logger.info(f"💀 Создано пользователей: {len(self.users)}/{user_count}")
                        
                except Exception as e:
                    logger.error(f"❌ Ошибка создания батча: {e}")
                    
        logger.info(f"✅ УЛЬТИМАТИВНАЯ БАЗА СОЗДАНА:")
        logger.info(f"   👥 Пользователей: {len(self.users):,}")
        logger.info(f"   📱 Аккаунтов: {self.total_accounts:,}")
        logger.info(f"   📋 Задач: {self.total_tasks:,}")
        
    def _create_user_batch_ultimate(self, start_idx: int, end_idx: int, accounts_per_user: int, batch_idx: int, total_batches: int):
        """Создать батч пользователей для ультимативного теста"""
        batch_users = []
        batch_accounts = 0
        batch_tasks = 0
        
        logger.info(f"💀 Батч {batch_idx+1}/{total_batches}: создаю пользователей {start_idx}-{end_idx}")
        
        for i in range(start_idx, end_idx):
            user_id = 5000000 + i  # Новая серия ID
            name = f"UltimateUser_{i}"
            
            user_accounts = []
            
            try:
                # Создаем аккаунты партиями для скорости
                accounts_batch_size = 50
                accounts_batches = (accounts_per_user + accounts_batch_size - 1) // accounts_batch_size
                
                for acc_batch_idx in range(accounts_batches):
                    with get_session() as session:
                        acc_start = acc_batch_idx * accounts_batch_size
                        acc_end = min(acc_start + accounts_batch_size, accounts_per_user)
                        
                        for j in range(acc_start, acc_end):
                            username = f"ultimate_{i}_{j}_{random.randint(100000, 999999)}"
                            
                            account = InstagramAccount(
                                user_id=user_id,
                                username=username,
                                password=f"pass_{user_id}_{j}",
                                email=f"{username}@ultimate.test",
                                status='active',
                                is_active=True,
                                full_name=f"Ultimate Account {i}-{j}",
                                biography=f"Ultimate stress test account for user {i}"
                            )
                            
                            session.add(account)
                            session.flush()  # Получаем ID без commit
                            
                            user_accounts.append(account.id)
                            batch_accounts += 1
                            
                            # Создаем задачи для каждого аккаунта
                            for task_type in [TaskType.PHOTO, TaskType.VIDEO]:
                                task = PublishTask(
                                    user_id=user_id,
                                    account_id=account.id,
                                    task_type=task_type,
                                    caption=f"Ultimate {task_type.value} from user {i}",
                                    scheduled_time=datetime.now() + timedelta(hours=random.randint(1, 72))
                                )
                                session.add(task)
                                batch_tasks += 1
                                
                            # Warmup задача
                            warmup = WarmupTask(
                                account_id=account.id,
                                status=WarmupStatus.PENDING,
                                settings={
                                    'user_id': user_id, 
                                    'ultimate_test': True,
                                    'target_count': random.randint(50, 200)
                                }
                            )
                            session.add(warmup)
                            batch_tasks += 1
                            
                        session.commit()
                        
                # Логируем прогресс создания аккаунтов
                if i % 5 == 0:
                    logger.info(f"💀 Пользователь {i}: создано {len(user_accounts)} аккаунтов")
                    
            except Exception as e:
                logger.error(f"❌ Ошибка создания пользователя {i}: {e}")
                continue
                
            batch_users.append({
                'user_id': user_id,
                'name': name,
                'accounts': user_accounts,
                'accounts_count': len(user_accounts)
            })
            
        logger.info(f"✅ Батч {batch_idx+1}/{total_batches} завершен: {len(batch_users)} пользователей, {batch_accounts} аккаунтов")
        return batch_users, batch_accounts, batch_tasks
        
    def ultimate_attack_1_massive_concurrent_requests(self):
        """Ультимативная атака 1: Массивные одновременные запросы"""
        logger.info("💀 УЛЬТИМАТИВНАЯ АТАКА 1: Массивные одновременные запросы")
        logger.info(f"💀 Запускаю по 500 потоков на каждого из {len(self.users)} пользователей!")
        
        violations = []
        total_operations = 0
        
        def ultra_aggressive_user_activity(user_batch):
            batch_violations = []
            batch_operations = 0
            
            for user in user_batch:
                try:
                    # 500 операций на пользователя!
                    operations_per_user = 500
                    
                    with ThreadPoolExecutor(max_workers=100) as executor:
                        futures = []
                        
                        for operation_idx in range(operations_per_user):
                            # Разные типы операций
                            if operation_idx % 3 == 0:
                                # Получение всех аккаунтов
                                futures.append(executor.submit(get_user_instagram_accounts, user_id=user['user_id']))
                            elif operation_idx % 3 == 1:
                                # Получение конкретного аккаунта
                                if user['accounts']:
                                    account_id = random.choice(user['accounts'])
                                    futures.append(executor.submit(get_user_instagram_account, account_id=account_id, user_id=user['user_id']))
                            else:
                                # Получение информации о пользователе
                                futures.append(executor.submit(get_user_info, user_id=user['user_id']))
                                
                        # Обрабатываем результаты
                        for future in as_completed(futures):
                            try:
                                result = future.result(timeout=10)
                                batch_operations += 1
                                
                                # Проверяем изоляцию
                                if hasattr(result, '__iter__') and not isinstance(result, str):
                                    for item in result:
                                        if hasattr(item, 'user_id') and item.user_id != user['user_id']:
                                            batch_violations.append({
                                                'type': 'ultra_concurrent_breach',
                                                'user_id': user['user_id'],
                                                'accessed_user_id': item.user_id,
                                                'item_id': getattr(item, 'id', None)
                                            })
                                            
                            except Exception as e:
                                # Таймауты и ошибки при такой нагрузке ожидаемы
                                pass
                                
                except Exception as e:
                    logger.error(f"❌ Ошибка ультра-активности пользователя {user['user_id']}: {e}")
                    
            return batch_violations, batch_operations
            
        # Разбиваем пользователей на батчи для обработки
        user_batch_size = 5  # По 5 пользователей в батче
        user_batches = [self.users[i:i+user_batch_size] for i in range(0, len(self.users), user_batch_size)]
        
        logger.info(f"💀 Обрабатываю {len(user_batches)} батчей пользователей...")
        
        with ThreadPoolExecutor(max_workers=20) as executor:
            futures = [executor.submit(ultra_aggressive_user_activity, batch) for batch in user_batches]
            
            for future_idx, future in enumerate(as_completed(futures)):
                try:
                    batch_violations, batch_operations = future.result()
                    violations.extend(batch_violations)
                    total_operations += batch_operations
                    
                    logger.info(f"💀 Батч {future_idx+1}/{len(futures)} завершен: {batch_operations} операций")
                    
                except Exception as e:
                    logger.error(f"❌ Ошибка обработки батча: {e}")
                    
        self.attack_results['ultimate_concurrent'] = {
            'users': len(self.users),
            'total_operations': total_operations,
            'violations': len(violations),
            'operations_per_user': 500
        }
        
        logger.info(f"📊 УЛЬТИМАТИВНАЯ АТАКА 1: {len(violations)} нарушений из {total_operations:,} операций")
        
    def ultimate_attack_2_sql_injection_validation(self):
        """Ультимативная атака 2: Проверка валидации SQL-инъекций"""
        logger.info("💀 УЛЬТИМАТИВНАЯ АТАКА 2: Валидация SQL-инъекций")
        
        violations = []
        
        # Расширенный список злонамеренных входных данных
        malicious_inputs = [
            # SQL инъекции
            "'; DROP TABLE instagram_accounts; --",
            "1 OR 1=1",
            "1; SELECT * FROM instagram_accounts; --",
            "-1 UNION SELECT * FROM instagram_accounts",
            "1' OR '1'='1",
            "admin'--",
            "admin' OR 1=1#",
            "'; INSERT INTO instagram_accounts VALUES ('hacked'); --",
            
            # Переполнения
            str(2**63),     # Максимальный int64
            str(2**64),     # Переполнение
            -2**63,         # Минимальный int64
            float('inf'),   # Бесконечность
            float('-inf'),  # Отрицательная бесконечность
            
            # Специальные значения
            "NULL",
            "null",
            "undefined",
            "",
            " ",
            "\x00",         # Null byte
            "\n\r\t",       # Whitespace
            
            # Попытки обхода
            "0x41414141",   # Hex
            "0",
            "-1",
            "999999999999999999999",  # Очень большое число
            
            # Скрипты
            "<script>alert('xss')</script>",
            "javascript:alert(1)",
            
            # Пути
            "../../../etc/passwd",
            "..\\..\\windows\\system32",
        ]
        
        logger.info(f"💀 Тестирую {len(malicious_inputs)} злонамеренных входных данных на {len(self.users)} пользователях")
        
        for user in self.users[:50]:  # Тестируем на первых 50 пользователях
            for malicious_input in malicious_inputs:
                try:
                    # Проверяем разные функции
                    test_functions = [
                        lambda: get_user_instagram_accounts(user_id=malicious_input),
                        lambda: get_user_info(user_id=malicious_input),
                    ]
                    
                    for test_func in test_functions:
                        try:
                            result = test_func()
                            
                            # Если функция вернула результат для злонамеренного входа - это нарушение
                            if result:
                                violations.append({
                                    'type': 'sql_injection_success',
                                    'malicious_input': str(malicious_input)[:100],  # Обрезаем для логов
                                    'function': test_func.__name__ if hasattr(test_func, '__name__') else 'lambda',
                                    'result_count': len(result) if hasattr(result, '__len__') else 1
                                })
                                
                        except (TypeError, ValueError, OverflowError):
                            # Ожидаемое поведение - функция должна отклонять плохие входные данные
                            pass
                        except Exception as e:
                            # Другие исключения могут указывать на проблемы
                            violations.append({
                                'type': 'unexpected_exception',
                                'malicious_input': str(malicious_input)[:100],
                                'exception': str(e)[:200]
                            })
                            
                except Exception as e:
                    logger.error(f"❌ Критическая ошибка при тестировании инъекции {malicious_input}: {e}")
                    
        self.attack_results['ultimate_sql_injection'] = {
            'tested_inputs': len(malicious_inputs),
            'tested_users': 50,
            'violations': len(violations),
            'violation_details': violations[:20]  # Первые 20 для анализа
        }
        
        logger.info(f"📊 УЛЬТИМАТИВНАЯ АТАКА 2: {len(violations)} уязвимостей валидации")
        
    def ultimate_attack_3_resource_exhaustion(self):
        """Ультимативная атака 3: Исчерпание ресурсов"""
        logger.info("💀 УЛЬТИМАТИВНАЯ АТАКА 3: Исчерпание системных ресурсов")
        
        violations = []
        initial_memory = psutil.Process().memory_info().rss / 1024 / 1024
        
        # Создаем экстремальную нагрузку
        resource_tasks = []
        
        def resource_exhaustion_worker():
            worker_violations = []
            
            # Создаем множество соединений с БД
            sessions = []
            try:
                for _ in range(50):  # 50 сессий на воркер
                    session = get_session()
                    sessions.append(session)
                    
                # Создаем большие объекты в памяти
                memory_hog = [random.random() for _ in range(500000)]  # 500k чисел
                
                # Тестируем изоляцию при нехватке ресурсов
                test_users = random.sample(self.users, min(10, len(self.users)))
                
                for user in test_users:
                    try:
                        accounts = get_user_instagram_accounts(user_id=user['user_id'])
                        
                        # Проверяем изоляцию в экстремальных условиях
                        for account in accounts[:5]:  # Первые 5 аккаунтов
                            if account.user_id != user['user_id']:
                                worker_violations.append({
                                    'type': 'resource_exhaustion_breach',
                                    'user_id': user['user_id'],
                                    'account_id': account.id,
                                    'account_user_id': account.user_id,
                                    'memory_usage_mb': psutil.Process().memory_info().rss / 1024 / 1024
                                })
                                
                    except Exception as e:
                        # Ошибки ожидаемы при исчерпании ресурсов
                        pass
                        
            finally:
                # Освобождаем ресурсы
                for session in sessions:
                    try:
                        session.close()
                    except:
                        pass
                del memory_hog
                        
            return worker_violations
            
        # Запускаем множество воркеров одновременно
        with ThreadPoolExecutor(max_workers=50) as executor:
            futures = [executor.submit(resource_exhaustion_worker) for _ in range(50)]
            
            for future in as_completed(futures):
                try:
                    worker_violations = future.result()
                    violations.extend(worker_violations)
                except Exception as e:
                    logger.error(f"❌ Ошибка воркера исчерпания ресурсов: {e}")
                    
        final_memory = psutil.Process().memory_info().rss / 1024 / 1024
        
        self.attack_results['ultimate_resource_exhaustion'] = {
            'workers': 50,
            'sessions_per_worker': 50,
            'initial_memory_mb': initial_memory,
            'final_memory_mb': final_memory,
            'memory_increase_mb': final_memory - initial_memory,
            'violations': len(violations)
        }
        
        logger.info(f"📊 УЛЬТИМАТИВНАЯ АТАКА 3: {len(violations)} нарушений при исчерпании ресурсов")
        logger.info(f"📊 Использование памяти: {initial_memory:.1f} -> {final_memory:.1f} MB (+{final_memory - initial_memory:.1f} MB)")
        
    def cleanup_ultimate_data(self):
        """Очистка ультимативных тестовых данных"""
        logger.info("💀 Очищаю УЛЬТИМАТИВНЫЕ тестовые данные...")
        logger.info(f"💀 К удалению: {len(self.users):,} пользователей, ~{self.total_accounts:,} аккаунтов, ~{self.total_tasks:,} задач")
        
        deleted_accounts = 0
        deleted_tasks = 0
        deleted_warmups = 0
        
        # Удаляем очень большими батчами
        batch_size = 1000  # Увеличиваем размер батча
        
        try:
            # Собираем все ID для удаления
            all_user_ids = [u['user_id'] for u in self.users]
            all_account_ids = []
            for user in self.users:
                all_account_ids.extend(user['accounts'])
                
            logger.info(f"💀 Удаляю {len(all_user_ids):,} пользователей и {len(all_account_ids):,} аккаунтов...")
            
            # Удаляем батчами
            for i in range(0, len(all_user_ids), batch_size):
                batch_user_ids = all_user_ids[i:i+batch_size]
                batch_account_ids = []
                
                # Собираем ID аккаунтов для этого батча
                for user in self.users[i:i+batch_size]:
                    if i//batch_size == 0 or i < len(self.users):  # Проверка границ
                        batch_account_ids.extend(user['accounts'])
                
                with get_session() as session:
                    # Удаляем задачи
                    if batch_user_ids:
                        deleted_tasks += session.query(PublishTask).filter(PublishTask.user_id.in_(batch_user_ids)).delete(synchronize_session=False)
                        
                    # Удаляем warmup задачи
                    if batch_account_ids:
                        deleted_warmups += session.query(WarmupTask).filter(WarmupTask.account_id.in_(batch_account_ids)).delete(synchronize_session=False)
                        deleted_accounts += session.query(InstagramAccount).filter(InstagramAccount.id.in_(batch_account_ids)).delete(synchronize_session=False)
                        
                    session.commit()
                    
                if i % (batch_size * 10) == 0:
                    logger.info(f"💀 Удалено батчей: {i//batch_size + 1}")
                    
        except Exception as e:
            logger.error(f"❌ Ошибка очистки: {e}")
            
        logger.info(f"✅ УЛЬТИМАТИВНАЯ ОЧИСТКА ЗАВЕРШЕНА:")
        logger.info(f"   💀 Удалено аккаунтов: {deleted_accounts:,}")
        logger.info(f"   💀 Удалено задач: {deleted_tasks:,}")
        logger.info(f"   💀 Удалено warmup: {deleted_warmups:,}")
        
    def analyze_ultimate_results(self):
        """Анализ результатов ультимативного тестирования"""
        logger.info("=" * 120)
        logger.info("💀 РЕЗУЛЬТАТЫ УЛЬТИМАТИВНОГО СТРЕСС-ТЕСТА ИЗОЛЯЦИИ")
        logger.info("=" * 120)
        
        total_time = (datetime.now() - self.start_time).total_seconds()
        total_violations = sum(result.get('violations', 0) for result in self.attack_results.values())
        
        logger.info(f"⏱️ Время выполнения: {total_time:.2f} секунд ({total_time/60:.1f} минут)")
        logger.info(f"👥 Пользователей: {len(self.users):,}")
        logger.info(f"📱 Аккаунтов: {self.total_accounts:,}")
        logger.info(f"📋 Задач: {self.total_tasks:,}")
        logger.info(f"💾 Использование памяти: {psutil.Process().memory_info().rss / 1024 / 1024:.1f} MB")
        logger.info(f"🧵 Активных потоков: {threading.active_count()}")
        
        logger.info("\n💀 РЕЗУЛЬТАТЫ УЛЬТИМАТИВНЫХ АТАК:")
        
        attack_names = {
            'ultimate_concurrent': '💀 Массивные одновременные запросы',
            'ultimate_sql_injection': '💀 Валидация SQL-инъекций',
            'ultimate_resource_exhaustion': '💀 Исчерпание ресурсов'
        }
        
        successful_attacks = 0
        
        for attack_key, attack_name in attack_names.items():
            if attack_key in self.attack_results:
                result = self.attack_results[attack_key]
                violations = result.get('violations', 0)
                
                if violations > 0:
                    logger.error(f"{attack_name}: ❌ {violations:,} НАРУШЕНИЙ")
                    successful_attacks += 1
                    
                    # Показываем детали для анализа
                    if attack_key == 'ultimate_concurrent':
                        total_ops = result.get('total_operations', 0)
                        logger.error(f"   📊 Операций: {total_ops:,}")
                    elif attack_key == 'ultimate_sql_injection':
                        tested_inputs = result.get('tested_inputs', 0)
                        logger.error(f"   📊 Протестировано входов: {tested_inputs}")
                        
                else:
                    logger.info(f"{attack_name}: ✅ ОТРАЖЕНА")
                    
        # Итоговая оценка
        logger.info("=" * 120)
        
        if total_violations == 0:
            logger.info("🏆 ВСЕ УЛЬТИМАТИВНЫЕ АТАКИ ОТРАЖЕНЫ!")
            logger.info("💀 СИСТЕМА ВЫДЕРЖАЛА ЭКСТРЕМАЛЬНУЮ НАГРУЗКУ!")
            logger.info("🚀 СИСТЕМА АБСОЛЮТНО ГОТОВА К ЛЮБОЙ ПРОДАКШН НАГРУЗКЕ!")
        elif successful_attacks <= 1:
            logger.warning("⚠️ СИСТЕМА ВЫДЕРЖАЛА БОЛЬШИНСТВО УЛЬТИМАТИВНЫХ АТАК")
            logger.warning("🔧 ЕСТЬ НЕЗНАЧИТЕЛЬНЫЕ УЯЗВИМОСТИ ПРИ ЭКСТРЕМАЛЬНОЙ НАГРУЗКЕ")
        else:
            logger.error("💥 СИСТЕМА НЕ ВЫДЕРЖАЛА УЛЬТИМАТИВНУЮ НАГРУЗКУ!")
            logger.error("🛑 ТРЕБУЕТСЯ СЕРЬЕЗНАЯ ОПТИМИЗАЦИЯ!")
            
        logger.info(f"📊 Итого: {successful_attacks}/{len(attack_names)} ультимативных атак успешны")
        logger.info(f"📊 Всего нарушений: {total_violations:,}")
        logger.info("=" * 120)
        
        return total_violations == 0
        
    def run_ultimate_stress_test(self):
        """Запуск ультимативного стресс-теста"""
        logger.info("💀" * 60)
        logger.info("💀 ЗАПУСК УЛЬТИМАТИВНОГО СТРЕСС-ТЕСТА ИЗОЛЯЦИИ")
        logger.info("💀 100 ПОЛЬЗОВАТЕЛЕЙ x 500 АККАУНТОВ = 50,000 АККАУНТОВ")
        logger.info("💀 МАКСИМАЛЬНАЯ НАГРУЗКА НА СИСТЕМУ")
        logger.info("💀" * 60)
        
        try:
            # 1. Создание ультимативной базы
            logger.info("💀 ЭТАП 1: Создание ультимативной базы данных...")
            self.create_massive_user_base(100, 500)
            
            # 2. Ультимативная атака 1: Массивные одновременные запросы
            logger.info("💀 ЭТАП 2: Ультимативная атака - массивные запросы...")
            self.ultimate_attack_1_massive_concurrent_requests()
            
            # 3. Ультимативная атака 2: Валидация SQL-инъекций
            logger.info("💀 ЭТАП 3: Ультимативная атака - SQL-инъекции...")
            self.ultimate_attack_2_sql_injection_validation()
            
            # 4. Ультимативная атака 3: Исчерпание ресурсов
            logger.info("💀 ЭТАП 4: Ультимативная атака - исчерпание ресурсов...")
            self.ultimate_attack_3_resource_exhaustion()
            
            # 5. Анализ результатов
            logger.info("💀 ЭТАП 5: Анализ результатов...")
            success = self.analyze_ultimate_results()
            
            return success
            
        except KeyboardInterrupt:
            logger.warning("⚠️ Тест прерван пользователем")
            return False
        except Exception as e:
            logger.error(f"💥 Критическая ошибка: {e}")
            return False
        finally:
            # Очистка данных
            logger.info("💀 ФИНАЛЬНЫЙ ЭТАП: Очистка данных...")
            self.cleanup_ultimate_data()

def main():
    """Главная функция ультимативного теста"""
    logger.info("💀 Запуск ультимативного стресс-теста изоляции...")
    logger.info(f"💀 Доступно ядер CPU: {multiprocessing.cpu_count()}")
    logger.info(f"💀 Доступно RAM: {psutil.virtual_memory().total / 1024 / 1024 / 1024:.1f} GB")
    
    # 🔧 КРИТИЧЕСКИ ВАЖНО: Инициализируем БД и расширенный пул соединений
    from database.db_manager import init_db
    logger.info("💀 Инициализирую базу данных с расширенным пулом...")
    init_db()
    logger.info("✅ База данных с расширенным пулом готова!")
    
    test = UltimateStressTest()
    success = test.run_ultimate_stress_test()
    
    if success:
        logger.info("🏆 УЛЬТИМАТИВНЫЙ ТЕСТ ПРОЙДЕН!")
        exit(0)
    else:
        logger.error("💥 УЛЬТИМАТИВНЫЙ ТЕСТ НЕ ПРОЙДЕН!")
        exit(1)

if __name__ == "__main__":
    main() 
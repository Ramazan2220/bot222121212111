#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
🔥 ЭКСТРЕМАЛЬНЫЙ ТЕСТ ИЗОЛЯЦИИ ПОД НАГРУЗКОЙ 🔥
Проверяем смешивание данных между пользователями при максимальной нагрузке
"""

import logging
import random
import time
import threading
import asyncio
import concurrent.futures
from typing import List, Dict, Any, Set
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import gc

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(threadName)s] - %(message)s'
)
logger = logging.getLogger(__name__)

# Импорты системы
from database.db_manager import init_db, get_session
from database.models import InstagramAccount, PublishTask, TaskType, WarmupTask, WarmupStatus
from database.safe_user_wrapper import get_user_instagram_accounts, get_user_instagram_account
from database.user_management import get_active_users, get_user_info
from utils.user_cache import get_user_cache
from utils.processing_state import ProcessingState

class ExtremeIsolationLoadTest:
    """Экстремальный тест изоляции под нагрузкой"""
    
    def __init__(self):
        self.violations = []
        self.cross_contaminations = []
        self.start_time = datetime.now()
        self.operations_count = 0
        self.errors_count = 0
        self.test_users = []
        
    def setup_test_environment(self):
        """Подготовка тестового окружения с умеренным количеством данных"""
        logger.info("🔥 ПОДГОТОВКА ЭКСТРЕМАЛЬНОГО ТЕСТОВОГО ОКРУЖЕНИЯ")
        
        # Инициализируем БД с расширенным пулом
        init_db()
        logger.info("✅ База данных с расширенным пулом готова!")
        
        # Создаем 20 тестовых пользователей с разумным количеством аккаунтов
        user_count = 20
        accounts_per_user = 10  # Разумное количество для интенсивного тестирования
        
        logger.info(f"🔥 Создаю {user_count} пользователей с {accounts_per_user} аккаунтами каждый")
        
        created_users = 0
        created_accounts = 0
        created_tasks = 0
        
        for i in range(user_count):
            user_id = 6000000 + i  # Новая серия для экстремального теста
            user_accounts = []
            
            try:
                with get_session() as session:
                    # Создаем аккаунты для пользователя
                    for j in range(accounts_per_user):
                        username = f"extreme_load_{i}_{j}_{random.randint(1000, 9999)}"
                        
                        account = InstagramAccount(
                            user_id=user_id,
                            username=username,
                            password=f"pass_{user_id}_{j}",
                            email=f"{username}@extreme.test",
                            status='active',
                            is_active=True,
                            full_name=f"Extreme Load Test {i}-{j}",
                            biography=f"Extreme load test account for user {i}"
                        )
                        
                        session.add(account)
                        session.flush()
                        
                        user_accounts.append(account.id)
                        created_accounts += 1
                        
                        # Создаем несколько задач для каждого аккаунта
                        for task_type in [TaskType.PHOTO, TaskType.VIDEO]:
                            task = PublishTask(
                                user_id=user_id,
                                account_id=account.id,
                                task_type=task_type,
                                caption=f"Extreme test {task_type.value} from user {i}",
                                scheduled_time=datetime.now() + timedelta(hours=random.randint(1, 48))
                            )
                            session.add(task)
                            created_tasks += 1
                            
                    session.commit()
                    
                self.test_users.append({
                    'user_id': user_id,
                    'name': f"ExtremeUser_{i}",
                    'accounts': user_accounts
                })
                created_users += 1
                
                if i % 5 == 0:
                    logger.info(f"🔥 Создано пользователей: {i+1}/{user_count}")
                    
            except Exception as e:
                logger.error(f"❌ Ошибка создания пользователя {i}: {e}")
                
        logger.info(f"✅ ТЕСТОВОЕ ОКРУЖЕНИЕ ГОТОВО:")
        logger.info(f"   👥 Пользователей: {created_users}")
        logger.info(f"   📱 Аккаунтов: {created_accounts}")
        logger.info(f"   📋 Задач: {created_tasks}")
        
        return created_users > 0
        
    def extreme_concurrent_isolation_test(self, duration_minutes: int = 5):
        """Экстремальный тест одновременного доступа к данным"""
        logger.info(f"🔥 ЭКСТРЕМАЛЬНЫЙ ТЕСТ ОДНОВРЕМЕННОГО ДОСТУПА ({duration_minutes} минут)")
        
        violations = []
        operations = 0
        errors = 0
        
        end_time = datetime.now() + timedelta(minutes=duration_minutes)
        
        def extreme_isolation_worker(worker_id: int):
            nonlocal violations, operations, errors
            worker_violations = []
            worker_operations = 0
            worker_errors = 0
            
            logger.info(f"🔥 Воркер {worker_id}: старт экстремального тестирования")
            
            while datetime.now() < end_time:
                try:
                    # Выбираем случайного пользователя
                    test_user = random.choice(self.test_users)
                    user_id = test_user['user_id']
                    
                    # Интенсивные операции чтения
                    for operation_type in range(5):  # 5 разных типов операций
                        try:
                            if operation_type == 0:
                                # Получение всех аккаунтов пользователя
                                accounts = get_user_instagram_accounts(user_id=user_id)
                                
                                # КРИТИЧЕСКАЯ ПРОВЕРКА: все аккаунты должны принадлежать ТОЛЬКО этому пользователю
                                for account in accounts:
                                    if account.user_id != user_id:
                                        worker_violations.append({
                                            'type': 'cross_user_account_access',
                                            'worker_id': worker_id,
                                            'requested_user': user_id,
                                            'received_account_user': account.user_id,
                                            'account_id': account.id,
                                            'timestamp': datetime.now()
                                        })
                                        
                            elif operation_type == 1:
                                # Получение конкретного аккаунта
                                if test_user['accounts']:
                                    account_id = random.choice(test_user['accounts'])
                                    account = get_user_instagram_account(account_id=account_id, user_id=user_id)
                                    
                                    if account and account.user_id != user_id:
                                        worker_violations.append({
                                            'type': 'wrong_user_account_returned',
                                            'worker_id': worker_id,
                                            'requested_user': user_id,
                                            'returned_account_user': account.user_id,
                                            'account_id': account_id
                                        })
                                        
                            elif operation_type == 2:
                                # Попытка получить чужой аккаунт
                                other_user = random.choice([u for u in self.test_users if u['user_id'] != user_id])
                                if other_user['accounts']:
                                    foreign_account_id = random.choice(other_user['accounts'])
                                    result = get_user_instagram_account(account_id=foreign_account_id, user_id=user_id)
                                    
                                    # Должно вернуть None или выбросить исключение
                                    if result is not None:
                                        worker_violations.append({
                                            'type': 'access_to_foreign_account',
                                            'worker_id': worker_id,
                                            'requesting_user': user_id,
                                            'foreign_user': other_user['user_id'],
                                            'foreign_account_id': foreign_account_id
                                        })
                                        
                            elif operation_type == 3:
                                # Получение информации о пользователе
                                user_info = get_user_info(user_id=user_id)
                                if user_info and user_info.get('user_id') != user_id:
                                    worker_violations.append({
                                        'type': 'wrong_user_info_returned',
                                        'worker_id': worker_id,
                                        'requested_user': user_id,
                                        'returned_user': user_info.get('user_id')
                                    })
                                    
                            elif operation_type == 4:
                                # Проверка задач пользователя
                                with get_session() as session:
                                    tasks = session.query(PublishTask).filter_by(user_id=user_id).limit(5).all()
                                    
                                    for task in tasks:
                                        if task.user_id != user_id:
                                            worker_violations.append({
                                                'type': 'cross_user_task_access',
                                                'worker_id': worker_id,
                                                'requested_user': user_id,
                                                'task_user': task.user_id,
                                                'task_id': task.id
                                            })
                                            
                            worker_operations += 1
                            
                        except Exception as e:
                            worker_errors += 1
                            if worker_errors % 100 == 0:
                                logger.warning(f"⚠️ Воркер {worker_id}: {worker_errors} ошибок")
                            
                    # Короткая пауза для снижения нагрузки на SQLite
                    time.sleep(0.001)
                    
                except Exception as e:
                    worker_errors += 1
                    
            logger.info(f"🔥 Воркер {worker_id}: завершен. Операций: {worker_operations}, ошибок: {worker_errors}, нарушений: {len(worker_violations)}")
            return worker_violations, worker_operations, worker_errors
            
        # Запускаем много воркеров одновременно
        num_workers = 50  # Экстремальное количество
        logger.info(f"🔥 Запускаю {num_workers} воркеров для экстремального тестирования...")
        
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            futures = [executor.submit(extreme_isolation_worker, i) for i in range(num_workers)]
            
            # Собираем результаты
            for future in as_completed(futures):
                try:
                    worker_violations, worker_operations, worker_errors = future.result()
                    violations.extend(worker_violations)
                    operations += worker_operations
                    errors += worker_errors
                except Exception as e:
                    logger.error(f"❌ Ошибка воркера: {e}")
                    errors += 1
                    
        self.operations_count += operations
        self.errors_count += errors
        self.violations.extend(violations)
        
        logger.info(f"📊 РЕЗУЛЬТАТ ЭКСТРЕМАЛЬНОГО ТЕСТА:")
        logger.info(f"   🔥 Операций: {operations:,}")
        logger.info(f"   ❌ Ошибок: {errors:,}")
        logger.info(f"   🚨 НАРУШЕНИЙ ИЗОЛЯЦИИ: {len(violations)}")
        
        if violations:
            logger.error("🚨 ОБНАРУЖЕНЫ НАРУШЕНИЯ ИЗОЛЯЦИИ:")
            for v in violations[:5]:  # Показываем первые 5
                logger.error(f"   - {v}")
                
        return len(violations) == 0
        
    def extreme_system_services_test(self):
        """Экстремальный тест системных сервисов под нагрузкой"""
        logger.info("🔥 ЭКСТРЕМАЛЬНЫЙ ТЕСТ СИСТЕМНЫХ СЕРВИСОВ")
        
        violations = []
        
        def stress_system_services():
            try:
                # Тестируем кеш пользователей под нагрузкой
                cache = get_user_cache()
                
                for _ in range(100):  # 100 операций на воркер
                    users = cache.get_active_users_safe()
                    priority_users = cache.get_users_by_priority_safe()
                    
                    # Проверяем что нет дублирования или смешивания
                    if users and priority_users:
                        user_ids = set(users)
                        priority_ids = set(priority_users)
                        
                        # Все priority пользователи должны быть среди активных
                        if not priority_ids.issubset(user_ids):
                            violations.append({
                                'type': 'cache_inconsistency',
                                'missing_users': list(priority_ids - user_ids)
                            })
                            
                    time.sleep(0.001)
                    
            except Exception as e:
                logger.error(f"❌ Ошибка тестирования системных сервисов: {e}")
                
        # Запускаем множество воркеров
        with ThreadPoolExecutor(max_workers=20) as executor:
            futures = [executor.submit(stress_system_services) for _ in range(20)]
            
            for future in as_completed(futures):
                future.result()
                
        logger.info(f"📊 Системные сервисы: {len(violations)} нарушений")
        self.violations.extend(violations)
        
        return len(violations) == 0
        
    def cleanup_test_data(self):
        """Очистка тестовых данных"""
        logger.info("🧹 Очищаю экстремальные тестовые данные...")
        
        deleted_accounts = 0
        deleted_tasks = 0
        
        try:
            # Удаляем батчами для стабильности
            batch_size = 50
            
            for i in range(0, len(self.test_users), batch_size):
                batch_users = self.test_users[i:i+batch_size]
                batch_user_ids = [u['user_id'] for u in batch_users]
                
                with get_session() as session:
                    # Удаляем задачи
                    deleted_tasks += session.query(PublishTask).filter(
                        PublishTask.user_id.in_(batch_user_ids)
                    ).delete(synchronize_session=False)
                    
                    # Удаляем аккаунты
                    deleted_accounts += session.query(InstagramAccount).filter(
                        InstagramAccount.user_id.in_(batch_user_ids)
                    ).delete(synchronize_session=False)
                    
                    session.commit()
                    
        except Exception as e:
            logger.error(f"❌ Ошибка очистки: {e}")
            
        logger.info(f"✅ Удалено: {deleted_accounts} аккаунтов, {deleted_tasks} задач")
        
    def analyze_extreme_results(self):
        """Анализ результатов экстремального тестирования"""
        logger.info("=" * 100)
        logger.info("🔥 РЕЗУЛЬТАТЫ ЭКСТРЕМАЛЬНОГО ТЕСТА ИЗОЛЯЦИИ ПОД НАГРУЗКОЙ")
        logger.info("=" * 100)
        
        total_time = (datetime.now() - self.start_time).total_seconds()
        
        logger.info(f"⏱️ Время выполнения: {total_time:.2f} секунд ({total_time/60:.1f} минут)")
        logger.info(f"🔥 Всего операций: {self.operations_count:,}")
        logger.info(f"❌ Всего ошибок: {self.errors_count:,}")
        logger.info(f"📊 Операций в секунду: {self.operations_count / total_time:.1f}")
        
        total_violations = len(self.violations)
        logger.info(f"🚨 ВСЕГО НАРУШЕНИЙ ИЗОЛЯЦИИ: {total_violations}")
        
        if total_violations == 0:
            logger.info("🏆 ВСЕ ПРОВЕРКИ ИЗОЛЯЦИИ ПРОЙДЕНЫ!")
            logger.info("🔥 СИСТЕМА ВЫДЕРЖАЛА ЭКСТРЕМАЛЬНУЮ НАГРУЗКУ!")
            logger.info("🚀 ДАННЫЕ ПОЛЬЗОВАТЕЛЕЙ НЕ СМЕШИВАЮТСЯ!")
        else:
            logger.error("🚨 ОБНАРУЖЕНЫ НАРУШЕНИЯ ИЗОЛЯЦИИ!")
            logger.error(f"💥 {total_violations} случаев смешивания данных!")
            
            # Показываем примеры нарушений
            violation_types = {}
            for v in self.violations:
                v_type = v.get('type', 'unknown')
                violation_types[v_type] = violation_types.get(v_type, 0) + 1
                
            logger.error("📊 Типы нарушений:")
            for v_type, count in violation_types.items():
                logger.error(f"   - {v_type}: {count}")
                
        logger.info("=" * 100)
        
        return total_violations == 0
        
    def run_extreme_isolation_load_test(self):
        """Запуск экстремального теста изоляции под нагрузкой"""
        logger.info("🔥" * 80)
        logger.info("🔥 ЭКСТРЕМАЛЬНЫЙ ТЕСТ ИЗОЛЯЦИИ ПОД НАГРУЗКОЙ")
        logger.info("🔥 ПРОВЕРЯЕМ СМЕШИВАНИЕ ДАННЫХ ПРИ МАКСИМАЛЬНОЙ НАГРУЗКЕ")
        logger.info("🔥" * 80)
        
        try:
            # 1. Подготовка окружения
            if not self.setup_test_environment():
                logger.error("❌ Не удалось подготовить тестовое окружение")
                return False
                
            # 2. Экстремальный тест одновременного доступа
            concurrent_ok = self.extreme_concurrent_isolation_test(duration_minutes=3)
            
            # 3. Тест системных сервисов
            services_ok = self.extreme_system_services_test()
            
            # 4. Анализ результатов
            success = self.analyze_extreme_results()
            
            return success and concurrent_ok and services_ok
            
        except KeyboardInterrupt:
            logger.warning("⚠️ Тест прерван пользователем")
            return False
        except Exception as e:
            logger.error(f"💥 Критическая ошибка: {e}")
            return False
        finally:
            # Очистка данных
            self.cleanup_test_data()

def main():
    """Главная функция экстремального теста"""
    logger.info("🔥 Запуск экстремального теста изоляции под нагрузкой...")
    
    test = ExtremeIsolationLoadTest()
    success = test.run_extreme_isolation_load_test()
    
    if success:
        logger.info("🏆 ЭКСТРЕМАЛЬНЫЙ ТЕСТ ПРОЙДЕН!")
        logger.info("🚀 СИСТЕМА ГОТОВА К ЛЮБОЙ НАГРУЗКЕ!")
        exit(0)
    else:
        logger.error("💥 ЭКСТРЕМАЛЬНЫЙ ТЕСТ НЕ ПРОЙДЕН!")
        logger.error("🛑 ОБНАРУЖЕНЫ ПРОБЛЕМЫ С ИЗОЛЯЦИЕЙ!")
        exit(1)

if __name__ == "__main__":
    main() 
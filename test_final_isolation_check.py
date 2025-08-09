#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Финальный быстрый тест изоляции пользователей
Проверяет все аспекты изоляции за 2-3 минуты
"""

import logging
import random
import time
import threading
from typing import List, Dict, Any
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Импорты системы
from database.db_manager import get_session
from database.models import InstagramAccount, PublishTask, TaskType
from database.safe_user_wrapper import get_user_instagram_accounts, get_user_instagram_account
from database.user_management import get_active_users, get_user_info
from utils.user_cache import get_user_cache
from utils.processing_state import ProcessingState
from utils.smart_validator_service import SmartValidatorService
from utils.account_validator_service import AccountValidatorService

class QuickIsolationTest:
    """Быстрый финальный тест изоляции"""
    
    def __init__(self):
        self.users = []
        self.test_results = {}
        self.violations = []
        self.start_time = datetime.now()
        
    def create_test_users(self, count: int = 10):
        """Создать тестовых пользователей с аккаунтами"""
        logger.info(f"👥 Создаю {count} тестовых пользователей...")
        
        user_names = ["Alex", "Beth", "Carl", "Dana", "Eric", "Faye", "Greg", "Hope", "Ivan", "Jane"]
        
        total_accounts = 0
        
        for i in range(count):
            user_id = 3000000 + i  # Новые уникальные ID
            name = user_names[i % len(user_names)] + f"_final_{i+1}"
            accounts_count = random.randint(2, 6)
            
            logger.info(f"👤 Создаю пользователя {name} с {accounts_count} аккаунтами...")
            
            user_accounts = []
            
            # Создаем аккаунты для пользователя
            with get_session() as session:
                for j in range(accounts_count):
                    username = f"{name.lower()}_acc_{j+1}_{random.randint(1000, 9999)}"
                    
                    account = InstagramAccount(
                        user_id=user_id,
                        username=username,
                        password=f"pass_{user_id}_{j+1}",
                        email=f"{username}@test.com",
                        status='active',
                        is_active=True,
                        full_name=f"{name} Account {j+1}",
                        biography=f"Test account for {name}"
                    )
                    
                    session.add(account)
                    session.commit()
                    
                    user_accounts.append(account.id)
                    total_accounts += 1
                    
                    # Создаем задачу для аккаунта
                    task = PublishTask(
                        user_id=user_id,
                        account_id=account.id,
                        task_type=TaskType.PHOTO,
                        caption=f"Test post from {name}",
                        scheduled_time=datetime.now() + timedelta(hours=random.randint(1, 24))
                    )
                    session.add(task)
                    session.commit()
                    
            self.users.append({
                'user_id': user_id,
                'name': name,
                'accounts': user_accounts,
                'accounts_count': len(user_accounts)
            })
            
        logger.info(f"✅ Создано {len(self.users)} пользователей с {total_accounts} аккаунтами")
        
    def test_basic_isolation(self):
        """Тест базовой изоляции"""
        logger.info("🔒 Тестирую базовую изоляцию...")
        
        violations = []
        
        for user in self.users:
            user_id = user['user_id']
            
            # Получаем аккаунты пользователя
            user_accounts = get_user_instagram_accounts(user_id=user_id)
            
            # Проверяем количество
            expected = user['accounts_count']
            actual = len(user_accounts)
            
            if actual != expected:
                violations.append({
                    'type': 'account_count_mismatch',
                    'user_id': user_id,
                    'expected': expected,
                    'actual': actual
                })
                
            # Проверяем изоляцию
            for account in user_accounts:
                if account.user_id != user_id:
                    violations.append({
                        'type': 'wrong_user_id',
                        'user_id': user_id,
                        'account_id': account.id,
                        'account_user_id': account.user_id
                    })
                    
        self.test_results['basic_isolation'] = {
            'tested_users': len(self.users),
            'violations': violations,
            'success': len(violations) == 0
        }
        
        logger.info(f"📊 Базовая изоляция: {len(violations)} нарушений из {len(self.users)} пользователей")
        
    def test_cross_access_prevention(self):
        """Тест предотвращения перекрестного доступа"""
        logger.info("🚫 Тестирую защиту от перекрестного доступа...")
        
        violations = []
        attempts = 0
        
        for i, user1 in enumerate(self.users):
            for j, user2 in enumerate(self.users):
                if i != j and user2['accounts']:  # Разные пользователи
                    attempts += 1
                    
                    try:
                        # Пытаемся получить чужой аккаунт
                        foreign_account = get_user_instagram_account(
                            account_id=user2['accounts'][0],
                            user_id=user1['user_id']
                        )
                        
                        if foreign_account:
                            violations.append({
                                'type': 'unauthorized_access',
                                'requesting_user': user1['user_id'],
                                'accessed_account': user2['accounts'][0],
                                'owner_user': user2['user_id']
                            })
                            
                    except Exception:
                        # Ожидаемое поведение - доступ должен быть заблокирован
                        pass
                        
        self.test_results['cross_access'] = {
            'attempts': attempts,
            'violations': violations,
            'success': len(violations) == 0
        }
        
        logger.info(f"📊 Перекрестный доступ: {len(violations)} нарушений из {attempts} попыток")
        
    def test_concurrent_access(self):
        """Тест одновременного доступа"""
        logger.info("⚡ Тестирую одновременный доступ...")
        
        results = []
        errors = []
        
        def user_activity(user):
            try:
                # Имитируем активную работу пользователя
                for _ in range(10):  # 10 операций
                    # Получаем аккаунты
                    accounts = get_user_instagram_accounts(user_id=user['user_id'])
                    
                    # Проверяем конкретный аккаунт
                    if accounts:
                        account = get_user_instagram_account(
                            account_id=accounts[0].id,
                            user_id=user['user_id']
                        )
                        if account and account.user_id == user['user_id']:
                            results.append({
                                'user_id': user['user_id'],
                                'operation': 'success',
                                'accounts_found': len(accounts)
                            })
                        else:
                            errors.append({
                                'user_id': user['user_id'],
                                'error': 'isolation_violation'
                            })
                    
                    time.sleep(0.1)  # Небольшая пауза
                    
            except Exception as e:
                errors.append({
                    'user_id': user['user_id'],
                    'error': str(e)
                })
                
        # Запускаем всех пользователей одновременно
        with ThreadPoolExecutor(max_workers=len(self.users)) as executor:
            futures = [executor.submit(user_activity, user) for user in self.users]
            
            for future in futures:
                future.result(timeout=30)
                
        self.test_results['concurrent_access'] = {
            'successful_operations': len(results),
            'errors': len(errors),
            'error_details': errors,
            'success': len(errors) == 0
        }
        
        logger.info(f"📊 Одновременный доступ: {len(results)} успехов, {len(errors)} ошибок")
        
    def test_system_services_isolation(self):
        """Тест изоляции системных сервисов"""
        logger.info("🔧 Тестирую изоляцию системных сервисов...")
        
        # Обновляем кеш
        cache = get_user_cache()
        cache.force_refresh()
        
        # Получаем всех активных пользователей
        active_users = cache.get_active_users_safe()
        our_users = [u['user_id'] for u in self.users]
        found_users = [uid for uid in our_users if uid in active_users]
        
        # Тестируем обработку через ProcessingState
        processing_state = ProcessingState("final_test")
        processing_state.start_cycle(found_users)
        
        processed_successfully = 0
        for user_id in found_users:
            try:
                processing_state.start_user_processing(user_id)
                
                # Получаем аккаунты через системные методы
                accounts = get_user_instagram_accounts(user_id=user_id)
                user_info = get_user_info(user_id)
                
                processing_state.complete_user_processing(user_id, True)
                processed_successfully += 1
                
            except Exception as e:
                processing_state.complete_user_processing(user_id, False, str(e))
                
        processing_state.complete_cycle()
        
        self.test_results['system_services'] = {
            'total_test_users': len(our_users),
            'found_in_system': len(found_users),
            'processed_successfully': processed_successfully,
            'success': processed_successfully == len(found_users)
        }
        
        logger.info(f"📊 Системные сервисы: {processed_successfully}/{len(found_users)} пользователей обработано")
        
    def test_validator_services(self):
        """Тест сервисов валидации"""
        logger.info("🔍 Тестирую сервисы валидации...")
        
        try:
            # Создаем валидаторы
            smart_validator = SmartValidatorService()
            account_validator = AccountValidatorService()
            
            # Тестируем их создание
            validator_creation = True
            
        except Exception as e:
            logger.error(f"❌ Ошибка создания валидаторов: {e}")
            validator_creation = False
            
        self.test_results['validator_services'] = {
            'smart_validator_created': validator_creation,
            'account_validator_created': validator_creation,
            'success': validator_creation
        }
        
        logger.info(f"📊 Валидаторы: {'✅ Созданы' if validator_creation else '❌ Ошибка'}")
        
    def cleanup_test_data(self):
        """Очистка тестовых данных"""
        logger.info("🧹 Очищаю тестовые данные...")
        
        deleted_accounts = 0
        deleted_tasks = 0
        
        try:
            with get_session() as session:
                for user in self.users:
                    # Удаляем задачи пользователя
                    tasks_deleted = session.query(PublishTask).filter_by(user_id=user['user_id']).delete()
                    deleted_tasks += tasks_deleted
                    
                    # Удаляем аккаунты пользователя
                    for account_id in user['accounts']:
                        deleted = session.query(InstagramAccount).filter_by(id=account_id).delete()
                        deleted_accounts += deleted
                        
                session.commit()
                
        except Exception as e:
            logger.error(f"❌ Ошибка очистки: {e}")
            
        logger.info(f"✅ Удалено: {deleted_accounts} аккаунтов, {deleted_tasks} задач")
        
    def analyze_results(self):
        """Анализ результатов"""
        logger.info("=" * 80)
        logger.info("📊 ФИНАЛЬНЫЕ РЕЗУЛЬТАТЫ ТЕСТА ИЗОЛЯЦИИ")
        logger.info("=" * 80)
        
        total_time = (datetime.now() - self.start_time).total_seconds()
        
        logger.info(f"⏱️ Время выполнения: {total_time:.2f} секунд")
        logger.info(f"👥 Тестовых пользователей: {len(self.users)}")
        
        # Проверяем каждый тест
        tests = [
            ('🔒 Базовая изоляция', 'basic_isolation'),
            ('🚫 Защита от перекрестного доступа', 'cross_access'),
            ('⚡ Одновременный доступ', 'concurrent_access'),
            ('🔧 Системные сервисы', 'system_services'),
            ('🔍 Сервисы валидации', 'validator_services')
        ]
        
        passed_tests = 0
        total_tests = len(tests)
        
        for test_name, test_key in tests:
            if test_key in self.test_results:
                result = self.test_results[test_key]
                success = result.get('success', False)
                status = "✅ ПРОЙДЕН" if success else "❌ НЕ ПРОЙДЕН"
                
                logger.info(f"{test_name}: {status}")
                
                if success:
                    passed_tests += 1
                else:
                    # Показываем детали ошибок
                    if 'violations' in result and result['violations']:
                        logger.warning(f"   Нарушений: {len(result['violations'])}")
                    if 'errors' in result and result['errors']:
                        logger.warning(f"   Ошибок: {result['errors']}")
                        
        # Итоговая оценка
        logger.info("=" * 80)
        
        success_rate = passed_tests / total_tests
        
        if success_rate == 1.0:
            logger.info("🎉 ВСЕ ТЕСТЫ ПРОЙДЕНЫ! СИСТЕМА ИЗОЛЯЦИИ РАБОТАЕТ ИДЕАЛЬНО!")
            logger.info("🚀 СИСТЕМА ГОТОВА К ПРОДАКШН!")
        elif success_rate >= 0.8:
            logger.warning("⚠️ БОЛЬШИНСТВО ТЕСТОВ ПРОЙДЕНО, НО ЕСТЬ ПРОБЛЕМЫ")
            logger.warning("🔧 ТРЕБУЕТСЯ ДОРАБОТКА ПЕРЕД ПРОДАКШН")
        else:
            logger.error("❌ КРИТИЧЕСКИЕ ПРОБЛЕМЫ С ИЗОЛЯЦИЕЙ!")
            logger.error("🛑 СИСТЕМА НЕ ГОТОВА К ПРОДАКШН!")
            
        logger.info(f"📊 Результат: {passed_tests}/{total_tests} тестов пройдено ({success_rate:.1%})")
        logger.info("=" * 80)
        
        return success_rate
        
    def run_final_test(self):
        """Запуск финального теста"""
        logger.info("🚀 ЗАПУСК ФИНАЛЬНОГО ТЕСТА ИЗОЛЯЦИИ ПОЛЬЗОВАТЕЛЕЙ")
        logger.info("=" * 80)
        
        try:
            # 1. Создание тестовых пользователей
            self.create_test_users(10)
            
            # 2. Тест базовой изоляции
            self.test_basic_isolation()
            
            # 3. Тест защиты от перекрестного доступа
            self.test_cross_access_prevention()
            
            # 4. Тест одновременного доступа
            self.test_concurrent_access()
            
            # 5. Тест системных сервисов
            self.test_system_services_isolation()
            
            # 6. Тест валидаторов
            self.test_validator_services()
            
            # 7. Анализ результатов
            success_rate = self.analyze_results()
            
            return success_rate
            
        finally:
            # Очистка данных
            self.cleanup_test_data()

def main():
    """Главная функция"""
    test = QuickIsolationTest()
    success_rate = test.run_final_test()
    
    if success_rate == 1.0:
        exit(0)  # Успех
    else:
        exit(1)  # Есть проблемы

if __name__ == "__main__":
    main() 
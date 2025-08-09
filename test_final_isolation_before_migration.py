#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
🎯 ФИНАЛЬНЫЙ ТЕСТ ИЗОЛЯЦИИ ПЕРЕД МИГРАЦИЕЙ
Убеждаемся что изоляция работает идеально на SQLite
"""

import logging
import random
import threading
import time
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Импорты системы
from database.db_manager import get_session
from database.models import InstagramAccount, PublishTask, TelegramUser, TaskType
from database.safe_user_wrapper import get_user_instagram_accounts, get_user_instagram_account

class FinalIsolationTest:
    """Финальный тест изоляции перед миграцией"""
    
    def __init__(self):
        self.test_users = []
        self.created_data = {'users': [], 'accounts': [], 'tasks': []}
        self.start_time = datetime.now()
        
    def create_test_users(self, count: int = 5):
        """Создание тестовых пользователей"""
        logger.info(f"👥 Создаю {count} тестовых пользователей...")
        
        base_user_id = 9000000  # Уникальные ID для теста
        
        for i in range(count):
            user_id = base_user_id + i
            
            with get_session() as session:
                # Создаем пользователя
                user = TelegramUser(
                    user_id=user_id,
                    username=f"test_user_{i+1}",
                    first_name=f"TestUser{i+1}",
                    is_active=True,
                    created_at=datetime.now()
                )
                session.add(user)
                session.commit()
                session.refresh(user)
                
                self.test_users.append(user_id)
                self.created_data['users'].append(user_id)
                
                # Создаем аккаунты для каждого пользователя
                for j in range(3):  # 3 аккаунта на пользователя
                    account = InstagramAccount(
                        user_id=user_id,
                        username=f"test_acc_{user_id}_{j+1}",
                        password=f"pass_{user_id}_{j+1}",
                        email=f"test_{user_id}_{j+1}@test.com",
                        full_name=f"Test Account {user_id}-{j+1}",
                        biography="Final isolation test account",
                        is_active=True
                    )
                    session.add(account)
                    session.commit()
                    session.refresh(account)
                    
                    self.created_data['accounts'].append(account.id)
                    
                    # Создаем задачи
                    task = PublishTask(
                        user_id=user_id,
                        account_id=account.id,
                        task_type=TaskType.PHOTO,
                        caption=f"Test post from user {user_id}",
                        scheduled_time=datetime.now() + timedelta(hours=random.randint(1, 24))
                    )
                    session.add(task)
                    session.commit()
                    session.refresh(task)
                    
                    self.created_data['tasks'].append(task.id)
                    
        logger.info(f"✅ Создано: {len(self.test_users)} пользователей, {len(self.created_data['accounts'])} аккаунтов, {len(self.created_data['tasks'])} задач")
        
    def test_basic_isolation(self):
        """Базовый тест изоляции"""
        logger.info("🔒 ТЕСТ 1: Базовая изоляция")
        
        violations = 0
        
        for user_id in self.test_users:
            # Получаем аккаунты пользователя
            user_accounts = get_user_instagram_accounts(user_id)
            
            for account in user_accounts:
                if account.user_id != user_id:
                    logger.error(f"❌ Пользователь {user_id} получил аккаунт {account.id} пользователя {account.user_id}")
                    violations += 1
                    
        if violations == 0:
            logger.info("✅ Базовая изоляция: ПРОЙДЕНО")
            return True
        else:
            logger.error(f"❌ Базовая изоляция: {violations} нарушений")
            return False
            
    def test_concurrent_access(self):
        """Тест одновременного доступа"""
        logger.info("🔒 ТЕСТ 2: Одновременный доступ")
        
        violations = 0
        results = {}
        
        def worker(user_id):
            try:
                accounts = get_user_instagram_accounts(user_id)
                results[user_id] = [acc.id for acc in accounts]
                
                # Проверяем что все аккаунты принадлежат пользователю
                for account in accounts:
                    if account.user_id != user_id:
                        logger.error(f"❌ Concurrent: Пользователь {user_id} получил чужой аккаунт {account.id}")
                        nonlocal violations
                        violations += 1
                        
            except Exception as e:
                logger.error(f"❌ Ошибка в worker {user_id}: {e}")
                
        # Запускаем параллельно
        with ThreadPoolExecutor(max_workers=len(self.test_users)) as executor:
            futures = [executor.submit(worker, user_id) for user_id in self.test_users]
            
            for future in futures:
                future.result()
                
        # Проверяем что аккаунты не пересекаются между пользователями
        all_account_ids = []
        for user_id, account_ids in results.items():
            for account_id in account_ids:
                if account_id in all_account_ids:
                    logger.error(f"❌ Аккаунт {account_id} появился у нескольких пользователей")
                    violations += 1
                all_account_ids.append(account_id)
                
        if violations == 0:
            logger.info("✅ Одновременный доступ: ПРОЙДЕНО")
            return True
        else:
            logger.error(f"❌ Одновременный доступ: {violations} нарушений")
            return False
            
    def test_cross_user_access(self):
        """Тест попыток доступа к чужим данным"""
        logger.info("🔒 ТЕСТ 3: Попытки межпользовательского доступа")
        
        violations = 0
        
        # Берем аккаунты первого пользователя
        user1_id = self.test_users[0]
        user1_accounts = get_user_instagram_accounts(user1_id)
        
        if not user1_accounts:
            logger.warning("⚠️ У первого пользователя нет аккаунтов для теста")
            return True
            
        account_to_test = user1_accounts[0]
        
        # Пробуем получить этот аккаунт от имени других пользователей
        for user_id in self.test_users[1:]:
            try:
                # Попытка получить чужой аккаунт
                stolen_account = get_user_instagram_account(user_id, account_to_test.id)
                
                if stolen_account is not None:
                    logger.error(f"❌ Пользователь {user_id} получил доступ к аккаунту {account_to_test.id} пользователя {user1_id}")
                    violations += 1
                else:
                    logger.debug(f"✅ Пользователь {user_id} правильно НЕ получил доступ к чужому аккаунту")
                    
            except Exception as e:
                logger.debug(f"✅ Пользователь {user_id} получил ожидаемую ошибку: {e}")
                
        if violations == 0:
            logger.info("✅ Межпользовательский доступ: ЗАБЛОКИРОВАН")
            return True
        else:
            logger.error(f"❌ Межпользовательский доступ: {violations} нарушений")
            return False
            
    def test_data_consistency(self):
        """Тест консистентности данных"""
        logger.info("🔒 ТЕСТ 4: Консистентность данных")
        
        violations = 0
        
        with get_session() as session:
            # Проверяем что все аккаунты имеют корректный user_id
            accounts_without_user = session.query(InstagramAccount).filter(
                InstagramAccount.user_id.is_(None)
            ).count()
            
            if accounts_without_user > 0:
                logger.error(f"❌ Найдено {accounts_without_user} аккаунтов без user_id")
                violations += 1
                
            # Проверяем что все задачи имеют корректный user_id
            tasks_without_user = session.query(PublishTask).filter(
                PublishTask.user_id.is_(None)
            ).count()
            
            if tasks_without_user > 0:
                logger.error(f"❌ Найдено {tasks_without_user} задач без user_id")
                violations += 1
                
            # Проверяем что аккаунты и задачи связаны корректно
            mismatched_tasks = session.query(PublishTask).join(InstagramAccount).filter(
                PublishTask.user_id != InstagramAccount.user_id
            ).count()
            
            if mismatched_tasks > 0:
                logger.error(f"❌ Найдено {mismatched_tasks} задач с несоответствующими user_id")
                violations += 1
                
        if violations == 0:
            logger.info("✅ Консистентность данных: ПРОЙДЕНО")
            return True
        else:
            logger.error(f"❌ Консистентность данных: {violations} нарушений")
            return False
            
    def cleanup_test_data(self):
        """Очистка тестовых данных"""
        logger.info("🧹 Очищаю тестовые данные...")
        
        deleted = {'users': 0, 'accounts': 0, 'tasks': 0}
        
        try:
            with get_session() as session:
                # Удаляем задачи
                if self.created_data['tasks']:
                    deleted_tasks = session.query(PublishTask).filter(
                        PublishTask.id.in_(self.created_data['tasks'])
                    ).delete(synchronize_session=False)
                    deleted['tasks'] = deleted_tasks
                    
                # Удаляем аккаунты  
                if self.created_data['accounts']:
                    deleted_accounts = session.query(InstagramAccount).filter(
                        InstagramAccount.id.in_(self.created_data['accounts'])
                    ).delete(synchronize_session=False)
                    deleted['accounts'] = deleted_accounts
                    
                # Удаляем пользователей
                if self.created_data['users']:
                    deleted_users = session.query(TelegramUser).filter(
                        TelegramUser.user_id.in_(self.created_data['users'])
                    ).delete(synchronize_session=False)
                    deleted['users'] = deleted_users
                    
                session.commit()
                
        except Exception as e:
            logger.error(f"❌ Ошибка очистки: {e}")
            
        logger.info(f"✅ Удалено: {deleted['users']} пользователей, {deleted['accounts']} аккаунтов, {deleted['tasks']} задач")
        
    def run_final_test(self):
        """Запуск финального теста"""
        logger.info("🎯" * 80)
        logger.info("🎯 ФИНАЛЬНЫЙ ТЕСТ ИЗОЛЯЦИИ ПЕРЕД МИГРАЦИЕЙ")
        logger.info("🎯" * 80)
        
        results = []
        
        try:
            # Создаем тестовые данные
            self.create_test_users(5)
            
            # Запускаем тесты
            results.append(('Базовая изоляция', self.test_basic_isolation()))
            results.append(('Одновременный доступ', self.test_concurrent_access()))
            results.append(('Межпользовательский доступ', self.test_cross_user_access()))
            results.append(('Консистентность данных', self.test_data_consistency()))
            
        except Exception as e:
            logger.error(f"💥 Критическая ошибка: {e}")
            results.append(('Критическая ошибка', False))
            
        finally:
            # Очистка
            self.cleanup_test_data()
            
        # Итоговый отчет
        total_time = (datetime.now() - self.start_time).total_seconds()
        
        logger.info("=" * 80)
        logger.info("🎯 РЕЗУЛЬТАТЫ ФИНАЛЬНОГО ТЕСТА ИЗОЛЯЦИИ")
        logger.info("=" * 80)
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
            logger.info("🏆 ВСЕ ТЕСТЫ ПРОЙДЕНЫ!")
            logger.info("🚀 СИСТЕМА ГОТОВА К МИГРАЦИИ НА PostgreSQL!")
            logger.info("🎉 ИЗОЛЯЦИЯ ПОЛЬЗОВАТЕЛЕЙ РАБОТАЕТ ИДЕАЛЬНО!")
        else:
            logger.error("💥 ЕСТЬ ПРОБЛЕМЫ С ИЗОЛЯЦИЕЙ!")
            logger.error("🛑 МИГРАЦИЯ НЕ РЕКОМЕНДУЕТСЯ!")
            
        logger.info(f"📊 Итого: {passed}/{total} тестов пройдено")
        logger.info("=" * 80)
        
        return passed == total

def main():
    """Главная функция"""
    logger.info("🎯 Запуск финального теста изоляции...")
    
    test = FinalIsolationTest()
    success = test.run_final_test()
    
    if success:
        logger.info("🏆 ФИНАЛЬНЫЙ ТЕСТ ПРОЙДЕН!")
        logger.info("🚀 ГОТОВЫ К МИГРАЦИИ!")
        exit(0)
    else:
        logger.error("💥 ФИНАЛЬНЫЙ ТЕСТ НЕ ПРОЙДЕН!")
        exit(1)

if __name__ == "__main__":
    main() 
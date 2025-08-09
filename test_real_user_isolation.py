#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Реалистичный тест изоляции пользователей
Создает виртуальных пользователей, их аккаунты и задачи для проверки изоляции
"""

import logging
import random
import time
import asyncio
import threading
from typing import List, Dict, Any
from datetime import datetime, timedelta

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('test_isolation_detailed.log')
    ]
)
logger = logging.getLogger(__name__)

# Импорты системы
from database.db_manager import get_session, add_instagram_account
from database.models import InstagramAccount, Proxy, PublishTask, WarmupTask, TaskType, WarmupStatus
from database.safe_user_wrapper import get_user_instagram_accounts, get_user_instagram_account
from database.user_management import get_active_users, get_user_info
from utils.user_cache import get_user_cache, process_users_with_limits
from utils.processing_state import ProcessingState, health_check_processing_states
from utils.health_monitor import get_health_monitor
from utils.smart_validator_service import SmartValidatorService
from utils.account_validator_service import AccountValidatorService
from utils.proxy_manager import distribute_proxies, get_proxies

class VirtualUser:
    """Виртуальный пользователь для тестирования"""
    
    def __init__(self, user_id: int, name: str, accounts_count: int = 3):
        self.user_id = user_id
        self.name = name
        self.accounts_count = accounts_count
        self.accounts = []
        self.tasks = []
        self.processing_results = {}
        
    def create_accounts(self):
        """Создать Instagram аккаунты для пользователя"""
        logger.info(f"👤 {self.name}: Создаю {self.accounts_count} аккаунтов...")
        
        with get_session() as session:
            for i in range(self.accounts_count):
                username = f"{self.name.lower()}_account_{i+1}_{random.randint(1000,9999)}"
                password = f"password_{self.user_id}_{i+1}"
                
                account = InstagramAccount(
                    user_id=self.user_id,
                    username=username,
                    password=password,
                    status='active',
                    is_active=True,
                    email=f"{username}@test.com"
                )
                
                session.add(account)
                session.commit()
                
                self.accounts.append(account.id)
                logger.info(f"   ✅ Создан аккаунт: {username} (ID: {account.id})")
                
        logger.info(f"👤 {self.name}: Создано {len(self.accounts)} аккаунтов")
        
    def create_tasks(self):
        """Создать задачи для пользователя"""
        logger.info(f"👤 {self.name}: Создаю задачи...")
        
        with get_session() as session:
            # Создаем задачи публикации
            for account_id in self.accounts:
                # Publish task
                publish_task = PublishTask(
                    user_id=self.user_id,
                    account_id=account_id,
                    task_type=TaskType.PHOTO,
                    caption=f"Test post from {self.name}",
                    scheduled_time=datetime.now() + timedelta(hours=random.randint(1, 24))
                )
                session.add(publish_task)
                self.tasks.append(('publish', publish_task))
                
                # Warmup task
                warmup_task = WarmupTask(
                    account_id=account_id,
                    status=WarmupStatus.PENDING,
                    settings={
                        'task_type': 'like',
                        'target_count': random.randint(10, 50),
                        'user_id': self.user_id  # Сохраняем в настройках
                    }
                )
                session.add(warmup_task)
                self.tasks.append(('warmup', warmup_task))
                
            session.commit()
            
        logger.info(f"👤 {self.name}: Создано {len(self.tasks)} задач")
        
    def verify_isolation(self) -> Dict[str, Any]:
        """Проверить изоляцию данных пользователя"""
        logger.info(f"🔍 {self.name}: Проверяю изоляцию данных...")
        
        results = {
            'user_id': self.user_id,
            'name': self.name,
            'accounts_created': len(self.accounts),
            'accounts_accessible': 0,
            'accounts_isolated': True,
            'foreign_accounts_accessible': 0,
            'tasks_isolated': True,
            'errors': []
        }
        
        try:
            # Проверяем доступ к собственным аккаунтам
            user_accounts = get_user_instagram_accounts(user_id=self.user_id)
            results['accounts_accessible'] = len(user_accounts)
            
            # Проверяем, что получаем только свои аккаунты
            for account in user_accounts:
                if account.user_id != self.user_id:
                    results['accounts_isolated'] = False
                    results['errors'].append(f"Получен чужой аккаунт: {account.username} (user_id: {account.user_id})")
                    
            # Проверяем, что не можем получить чужие аккаунты
            all_users = get_active_users()
            for other_user_id in all_users:
                if other_user_id != self.user_id:
                    other_accounts = get_user_instagram_accounts(user_id=other_user_id)
                    if other_accounts:
                        # Пытаемся получить чужой аккаунт через наш user_id
                        try:
                            foreign_account = get_user_instagram_account(
                                account_id=other_accounts[0].id, 
                                user_id=self.user_id
                            )
                            if foreign_account:
                                results['foreign_accounts_accessible'] += 1
                                results['accounts_isolated'] = False
                                results['errors'].append(f"Получен доступ к чужому аккаунту: {foreign_account.username}")
                        except:
                            # Это хорошо - доступ должен быть запрещен
                            pass
                            
            logger.info(f"   ✅ {self.name}: Изоляция аккаунтов {'✅ ОК' if results['accounts_isolated'] else '❌ НАРУШЕНА'}")
            
        except Exception as e:
            results['errors'].append(f"Ошибка проверки изоляции: {str(e)}")
            logger.error(f"❌ {self.name}: Ошибка проверки изоляции: {e}")
            
        return results

class RealUserIsolationTest:
    """Комплексный тест изоляции с виртуальными пользователями"""
    
    def __init__(self, num_users: int = 8):
        self.num_users = num_users
        self.virtual_users = []
        self.test_results = {}
        self.start_time = datetime.now()
        
    def create_virtual_users(self):
        """Создать виртуальных пользователей"""
        logger.info(f"🚀 Создаю {self.num_users} виртуальных пользователей...")
        
        user_names = [
            "Alice", "Bob", "Charlie", "Diana", "Emma", 
            "Frank", "Grace", "Henry", "Ivy", "Jack"
        ]
        
        for i in range(self.num_users):
            # Создаем уникальные user_id (начинаем с 1000000 чтобы не пересекаться с реальными)
            user_id = 1000000 + i
            name = user_names[i % len(user_names)] + f"_{i+1}"
            accounts_count = random.randint(2, 5)
            
            virtual_user = VirtualUser(user_id, name, accounts_count)
            self.virtual_users.append(virtual_user)
            
        logger.info(f"✅ Создано {len(self.virtual_users)} виртуальных пользователей")
        
    def setup_user_data(self):
        """Настроить данные для всех пользователей"""
        logger.info("📦 Настраиваю данные пользователей...")
        
        for user in self.virtual_users:
            logger.info(f"👤 Настраиваю пользователя: {user.name}")
            user.create_accounts()
            user.create_tasks()
            time.sleep(0.1)  # Небольшая пауза между пользователями
            
        logger.info("✅ Все пользователи настроены")
        
    def test_user_isolation(self):
        """Тест изоляции для каждого пользователя"""
        logger.info("🔍 Тестирую изоляцию пользователей...")
        
        isolation_results = []
        
        for user in self.virtual_users:
            result = user.verify_isolation()
            isolation_results.append(result)
            
        self.test_results['isolation'] = isolation_results
        
        # Анализируем результаты
        total_users = len(isolation_results)
        isolated_users = sum(1 for r in isolation_results if r['accounts_isolated'])
        users_with_errors = sum(1 for r in isolation_results if r['errors'])
        
        logger.info(f"📊 Результаты изоляции:")
        logger.info(f"   👥 Всего пользователей: {total_users}")
        logger.info(f"   ✅ Изолированных: {isolated_users}")
        logger.info(f"   ❌ С нарушениями: {total_users - isolated_users}")
        logger.info(f"   ⚠️ С ошибками: {users_with_errors}")
        
        return isolation_results
        
    def test_concurrent_processing(self):
        """Тест одновременной обработки пользователей"""
        logger.info("🔄 Тестирую одновременную обработку...")
        
        def process_user_accounts(user: VirtualUser):
            """Обработка аккаунтов пользователя"""
            try:
                logger.info(f"🔄 {user.name}: Начинаю обработку аккаунтов...")
                
                # Получаем аккаунты пользователя
                accounts = get_user_instagram_accounts(user_id=user.user_id)
                
                processed_accounts = []
                for account in accounts:
                    # Имитируем обработку аккаунта
                    processing_time = random.uniform(0.1, 0.5)
                    time.sleep(processing_time)
                    
                    processed_accounts.append({
                        'account_id': account.id,
                        'username': account.username,
                        'user_id': account.user_id,
                        'processing_time': processing_time
                    })
                    
                user.processing_results = {
                    'processed_accounts': processed_accounts,
                    'total_time': sum(a['processing_time'] for a in processed_accounts),
                    'success': True
                }
                
                logger.info(f"✅ {user.name}: Обработано {len(processed_accounts)} аккаунтов")
                
            except Exception as e:
                user.processing_results = {
                    'error': str(e),
                    'success': False
                }
                logger.error(f"❌ {user.name}: Ошибка обработки: {e}")
                
        # Запускаем обработку для всех пользователей одновременно
        threads = []
        for user in self.virtual_users:
            thread = threading.Thread(target=process_user_accounts, args=(user,))
            threads.append(thread)
            thread.start()
            
        # Ждем завершения всех потоков
        for thread in threads:
            thread.join()
            
        # Анализируем результаты
        successful_processing = sum(1 for u in self.virtual_users if u.processing_results.get('success', False))
        
        logger.info(f"📊 Результаты одновременной обработки:")
        logger.info(f"   👥 Всего пользователей: {len(self.virtual_users)}")
        logger.info(f"   ✅ Успешно обработано: {successful_processing}")
        logger.info(f"   ❌ С ошибками: {len(self.virtual_users) - successful_processing}")
        
        self.test_results['concurrent_processing'] = {
            'total_users': len(self.virtual_users),
            'successful': successful_processing,
            'failed': len(self.virtual_users) - successful_processing
        }
        
    def test_system_services_isolation(self):
        """Тест изоляции в системных сервисах"""
        logger.info("🔧 Тестирую изоляцию в системных сервисах...")
        
        # Обновляем кеш пользователей
        cache = get_user_cache()
        cache.force_refresh()
        
        # Получаем активных пользователей (включая наших виртуальных)
        active_users = cache.get_active_users_safe()
        virtual_user_ids = [u.user_id for u in self.virtual_users]
        
        logger.info(f"👥 Активных пользователей в системе: {len(active_users)}")
        logger.info(f"👥 Наших виртуальных пользователей: {len(virtual_user_ids)}")
        
        # Проверяем, что наши пользователи в списке
        found_virtual_users = [uid for uid in virtual_user_ids if uid in active_users]
        logger.info(f"👥 Найдено в системе виртуальных пользователей: {len(found_virtual_users)}")
        
        # Тестируем обработку пользователей с ограничениями
        processing_state = ProcessingState("test_system_services")
        processing_state.start_cycle(found_virtual_users)
        
        processed_users = []
        for user_id in found_virtual_users:
            try:
                processing_state.start_user_processing(user_id)
                
                # Получаем аккаунты пользователя через системный метод
                user_accounts = get_user_instagram_accounts(user_id=user_id)
                user_info = get_user_info(user_id)
                
                processed_users.append({
                    'user_id': user_id,
                    'accounts_count': len(user_accounts),
                    'user_info': user_info
                })
                
                processing_state.complete_user_processing(user_id, True)
                logger.info(f"   ✅ Пользователь {user_id}: {len(user_accounts)} аккаунтов")
                
            except Exception as e:
                processing_state.complete_user_processing(user_id, False, str(e))
                logger.error(f"   ❌ Пользователь {user_id}: Ошибка - {e}")
                
        processing_state.complete_cycle()
        
        self.test_results['system_services'] = {
            'total_virtual_users': len(virtual_user_ids),
            'found_in_system': len(found_virtual_users),
            'processed_successfully': len(processed_users),
            'processing_details': processed_users
        }
        
    def test_data_cross_contamination(self):
        """Тест на перекрестное загрязнение данных"""
        logger.info("🔬 Тестирую перекрестное загрязнение данных...")
        
        contamination_found = False
        contamination_details = []
        
        for user in self.virtual_users:
            user_accounts = get_user_instagram_accounts(user_id=user.user_id)
            
            for account in user_accounts:
                # Проверяем, что аккаунт принадлежит правильному пользователю
                if account.user_id != user.user_id:
                    contamination_found = True
                    contamination_details.append({
                        'requesting_user': user.user_id,
                        'account_id': account.id,
                        'account_username': account.username,
                        'actual_owner': account.user_id,
                        'issue': 'wrong_user_id'
                    })
                    
        # Проверяем доступ к аккаунтам других пользователей
        for i, user1 in enumerate(self.virtual_users):
            for j, user2 in enumerate(self.virtual_users):
                if i != j:  # Разные пользователи
                    try:
                        # Пытаемся получить аккаунты user2 от имени user1
                        user2_accounts = get_user_instagram_accounts(user_id=user2.user_id)
                        if user2_accounts:
                            # Пытаемся получить первый аккаунт user2 через user1
                            foreign_account = get_user_instagram_account(
                                account_id=user2_accounts[0].id,
                                user_id=user1.user_id
                            )
                            if foreign_account:
                                contamination_found = True
                                contamination_details.append({
                                    'requesting_user': user1.user_id,
                                    'account_id': foreign_account.id,
                                    'account_username': foreign_account.username,
                                    'actual_owner': user2.user_id,
                                    'issue': 'unauthorized_access'
                                })
                    except:
                        # Это хорошо - доступ должен быть заблокирован
                        pass
                        
        logger.info(f"🔬 Результаты проверки загрязнения:")
        logger.info(f"   ❌ Загрязнение найдено: {'Да' if contamination_found else 'Нет'}")
        logger.info(f"   📋 Случаев загрязнения: {len(contamination_details)}")
        
        if contamination_details:
            logger.warning("⚠️ Обнаружены случаи загрязнения данных:")
            for detail in contamination_details:
                logger.warning(f"   - Пользователь {detail['requesting_user']} получил доступ к аккаунту {detail['account_username']} пользователя {detail['actual_owner']}")
                
        self.test_results['contamination'] = {
            'found': contamination_found,
            'cases': len(contamination_details),
            'details': contamination_details
        }
        
    def cleanup_test_data(self):
        """Очистка тестовых данных"""
        logger.info("🧹 Очищаю тестовые данные...")
        
        with get_session() as session:
            # Удаляем аккаунты виртуальных пользователей
            deleted_accounts = 0
            for user in self.virtual_users:
                for account_id in user.accounts:
                    account = session.query(InstagramAccount).filter_by(id=account_id).first()
                    if account:
                        session.delete(account)
                        deleted_accounts += 1
                        
            # Удаляем задачи
            deleted_tasks = 0
            for user in self.virtual_users:
                # Удаляем publish tasks
                publish_tasks = session.query(PublishTask).filter_by(user_id=user.user_id).all()
                for task in publish_tasks:
                    session.delete(task)
                    deleted_tasks += 1
                    
                # Удаляем warmup tasks (по account_id, так как user_id в settings)
                user_account_ids = [aid for aid in user.accounts]
                if user_account_ids:
                    warmup_tasks = session.query(WarmupTask).filter(WarmupTask.account_id.in_(user_account_ids)).all()
                    for task in warmup_tasks:
                        # Проверяем что это задача нашего пользователя
                        if task.settings and task.settings.get('user_id') == user.user_id:
                            session.delete(task)
                            deleted_tasks += 1
                    
            session.commit()
            
        logger.info(f"✅ Очистка завершена:")
        logger.info(f"   🗑️ Удалено аккаунтов: {deleted_accounts}")
        logger.info(f"   🗑️ Удалено задач: {deleted_tasks}")
        
    def run_comprehensive_test(self):
        """Запуск полного комплексного теста"""
        logger.info("=" * 100)
        logger.info("🚀 ЗАПУСК КОМПЛЕКСНОГО ТЕСТА ИЗОЛЯЦИИ С ВИРТУАЛЬНЫМИ ПОЛЬЗОВАТЕЛЯМИ")
        logger.info("=" * 100)
        
        try:
            # 1. Создание виртуальных пользователей
            self.create_virtual_users()
            
            # 2. Настройка данных
            self.setup_user_data()
            
            # 3. Тест изоляции пользователей
            self.test_user_isolation()
            
            # 4. Тест одновременной обработки
            self.test_concurrent_processing()
            
            # 5. Тест системных сервисов
            self.test_system_services_isolation()
            
            # 6. Тест перекрестного загрязнения
            self.test_data_cross_contamination()
            
            # 7. Итоговый анализ
            self.analyze_results()
            
        finally:
            # Очистка тестовых данных
            self.cleanup_test_data()
            
    def analyze_results(self):
        """Анализ итоговых результатов"""
        logger.info("=" * 100)
        logger.info("📊 АНАЛИЗ РЕЗУЛЬТАТОВ КОМПЛЕКСНОГО ТЕСТА")
        logger.info("=" * 100)
        
        total_time = (datetime.now() - self.start_time).total_seconds()
        
        # Общая статистика
        logger.info(f"⏱️ Время выполнения: {total_time:.2f} секунд")
        logger.info(f"👥 Виртуальных пользователей: {len(self.virtual_users)}")
        
        # Изоляция пользователей
        if 'isolation' in self.test_results:
            isolation_results = self.test_results['isolation']
            isolated_users = sum(1 for r in isolation_results if r['accounts_isolated'])
            logger.info(f"🔒 Изоляция пользователей: {isolated_users}/{len(isolation_results)} ({'✅ ОК' if isolated_users == len(isolation_results) else '❌ НАРУШЕНА'})")
            
        # Одновременная обработка
        if 'concurrent_processing' in self.test_results:
            concurrent = self.test_results['concurrent_processing']
            logger.info(f"🔄 Одновременная обработка: {concurrent['successful']}/{concurrent['total_users']} ({'✅ ОК' if concurrent['failed'] == 0 else '❌ ПРОБЛЕМЫ'})")
            
        # Системные сервисы
        if 'system_services' in self.test_results:
            services = self.test_results['system_services']
            logger.info(f"🔧 Системные сервисы: {services['processed_successfully']}/{services['found_in_system']} ({'✅ ОК' if services['processed_successfully'] == services['found_in_system'] else '❌ ПРОБЛЕМЫ'})")
            
        # Перекрестное загрязнение
        if 'contamination' in self.test_results:
            contamination = self.test_results['contamination']
            logger.info(f"🔬 Загрязнение данных: {'❌ НАЙДЕНО' if contamination['found'] else '✅ НЕ НАЙДЕНО'} ({contamination['cases']} случаев)")
            
        # Итоговая оценка
        all_tests_passed = (
            self.test_results.get('isolation', [{}])[0].get('accounts_isolated', False) and
            self.test_results.get('concurrent_processing', {}).get('failed', 1) == 0 and
            not self.test_results.get('contamination', {}).get('found', True)
        )
        
        logger.info("=" * 100)
        if all_tests_passed:
            logger.info("🎉 ВСЕ ТЕСТЫ ПРОЙДЕНЫ! Система изоляции работает корректно!")
        else:
            logger.error("❌ ОБНАРУЖЕНЫ ПРОБЛЕМЫ! Необходимо исправление системы изоляции!")
        logger.info("=" * 100)

def main():
    """Главная функция запуска тестов"""
    test = RealUserIsolationTest(num_users=8)
    test.run_comprehensive_test()

if __name__ == "__main__":
    main() 
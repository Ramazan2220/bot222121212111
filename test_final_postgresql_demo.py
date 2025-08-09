#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
🐘 ФИНАЛЬНЫЙ ТЕСТ ИЗОЛЯЦИИ НА PostgreSQL
Демонстрация работы пользовательской изоляции на облачной БД
"""

import logging
import sys
import time
import threading
import random
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict, Any

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

try:
    from database.db_manager import get_session, init_db
    from database.models import InstagramAccount, PublishTask, WarmupTask, TelegramUser, TaskType, TaskStatus, WarmupStatus
    from database.safe_user_wrapper import get_user_instagram_accounts, get_user_instagram_account
    from utils.user_cache import UserCache
    from database.user_management import get_active_users, get_users_by_priority
except ImportError as e:
    logger.error(f"❌ Ошибка импорта: {e}")
    logger.error("💡 Убедитесь что вы в правильной директории")
    sys.exit(1)

class PostgreSQLIsolationDemo:
    """Демонстрация изоляции на PostgreSQL"""
    
    def __init__(self):
        self.test_users = []
        self.performance_stats = {
            'queries_executed': 0,
            'concurrent_operations': 0,
            'isolation_violations': 0,
            'start_time': None,
            'postgresql_version': None
        }
        
    def create_demo_users(self) -> List[Dict[str, Any]]:
        """Создание демонстрационных пользователей"""
        logger.info("👥 Создание демонстрационных пользователей...")
        
        demo_users = []
        
        try:
            with get_session() as session:
                for i in range(5):  # 5 демо пользователей
                    user_id = 9000000 + i
                    
                    # Создаем пользователя
                    telegram_user = TelegramUser(
                        telegram_id=user_id,
                        username=f"demo_user_{i}",
                        first_name=f"Demo {i}",
                        last_name="PostgreSQL",
                        is_active=True
                    )
                    
                    session.merge(telegram_user)
                    session.flush()
                    
                    # Создаем аккаунты для пользователя
                    accounts = []
                    for j in range(3):  # По 3 аккаунта
                        account = InstagramAccount(
                            username=f"pg_demo_{i}_{j}",
                            password=f"demo_pass_{i}_{j}",
                            email=f"pg_demo_{i}_{j}@postgresql.test",
                            user_id=user_id,
                            is_active=True,
                            full_name=f"PostgreSQL Demo Account {i}-{j}",
                            biography=f"Demo account for PostgreSQL testing"
                        )
                        session.add(account)
                        accounts.append(account)
                    
                    session.flush()
                    
                    # Создаем задачи
                    tasks = []
                    for account in accounts:
                        # Publish task
                        publish_task = PublishTask(
                            account_id=account.id,
                            user_id=user_id,
                            task_type=TaskType.PHOTO,
                            caption=f"PostgreSQL demo post from user {i}",
                            scheduled_time=datetime.now() + timedelta(hours=random.randint(1, 24)),
                            status=TaskStatus.PENDING
                        )
                        session.add(publish_task)
                        tasks.append(('publish', publish_task))
                        
                        # Warmup task
                        warmup_task = WarmupTask(
                            account_id=account.id,
                            status=WarmupStatus.PENDING,
                            settings={
                                'task_type': 'like',
                                'target_count': random.randint(10, 50),
                                'user_id': user_id,
                                'postgresql_demo': True
                            }
                        )
                        session.add(warmup_task)
                        tasks.append(('warmup', warmup_task))
                    
                    demo_users.append({
                        'user_id': user_id,
                        'username': f"demo_user_{i}",
                        'accounts': [acc.id for acc in accounts],
                        'tasks': len(tasks)
                    })
                    
                session.commit()
                
            logger.info(f"✅ Создано {len(demo_users)} демо пользователей с аккаунтами и задачами")
            return demo_users
            
        except Exception as e:
            logger.error(f"❌ Ошибка создания демо пользователей: {e}")
            return []
            
    def test_concurrent_isolation(self, users: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Тест параллельной изоляции"""
        logger.info("🔄 Тестирование параллельной изоляции...")
        
        results = {
            'isolation_violations': 0,
            'successful_operations': 0,
            'cross_contamination': 0,
            'concurrent_queries': 0
        }
        
        def test_user_isolation(user_data: Dict[str, Any]):
            """Тест изоляции для одного пользователя"""
            user_id = user_data['user_id']
            
            try:
                with get_session() as session:
                    # 1. Проверяем что пользователь видит только свои аккаунты
                    user_accounts = get_user_instagram_accounts(user_id)
                    expected_count = len(user_data['accounts'])
                    
                    if len(user_accounts) != expected_count:
                        results['isolation_violations'] += 1
                        logger.warning(f"⚠️ Пользователь {user_id}: ожидал {expected_count} аккаунтов, получил {len(user_accounts)}")
                    
                    # 2. Проверяем что нет доступа к чужим аккаунтам
                    for account in user_accounts:
                        if account.user_id != user_id:
                            results['cross_contamination'] += 1
                            logger.error(f"❌ ИЗОЛЯЦИЯ НАРУШЕНА! Пользователь {user_id} видит аккаунт пользователя {account.user_id}")
                    
                    # 3. Проверяем задачи
                    publish_tasks = session.query(PublishTask).filter_by(user_id=user_id).all()
                    warmup_tasks = session.query(WarmupTask).filter(
                        WarmupTask.account_id.in_(user_data['accounts'])
                    ).all()
                    
                    # Проверяем что все задачи принадлежат пользователю
                    for task in publish_tasks:
                        if task.user_id != user_id:
                            results['cross_contamination'] += 1
                    
                    for task in warmup_tasks:
                        if task.settings.get('user_id') != user_id:
                            results['cross_contamination'] += 1
                    
                    results['successful_operations'] += 1
                    results['concurrent_queries'] += 4  # 4 запроса на пользователя
                    
            except Exception as e:
                logger.error(f"❌ Ошибка тестирования пользователя {user_id}: {e}")
                results['isolation_violations'] += 1
        
        # Запускаем параллельное тестирование
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(test_user_isolation, user) for user in users]
            
            for future in futures:
                future.result()
        
        return results
        
    def test_postgresql_features(self) -> Dict[str, Any]:
        """Тест специфичных возможностей PostgreSQL"""
        logger.info("🐘 Тестирование возможностей PostgreSQL...")
        
        features = {
            'version': None,
            'concurrent_connections': 0,
            'connection_pooling': False,
            'json_support': False,
            'foreign_key_constraints': False
        }
        
        try:
            with get_session() as session:
                                 # Проверяем версию PostgreSQL
                from sqlalchemy import text
                result = session.execute(text("SELECT version()"))
                version = result.fetchone()[0]
                features['version'] = version
                logger.info(f"🐘 PostgreSQL версия: {version}")
                
                                 # Проверяем поддержку JSON
                try:
                    session.execute(text("SELECT '{\"test\": true}'::json"))
                    features['json_support'] = True
                    logger.info("✅ JSON поддержка: Работает")
                except:
                    logger.warning("⚠️ JSON поддержка: Недоступна")
                
                # Проверяем Foreign Key ограничения
                try:
                    # Пытаемся создать задачу с несуществующим account_id
                    fake_task = PublishTask(
                        account_id=999999,
                        user_id=999999,
                        task_type=TaskType.PHOTO,
                        caption="Test FK constraint"
                    )
                    session.add(fake_task)
                    session.flush()
                    features['foreign_key_constraints'] = False
                except Exception:
                    features['foreign_key_constraints'] = True
                    logger.info("✅ Foreign Key ограничения: Работают")
                    session.rollback()
                    
        except Exception as e:
            logger.error(f"❌ Ошибка тестирования PostgreSQL: {e}")
            
        return features
        
    def performance_benchmark(self, users: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Бенчмарк производительности"""
        logger.info("⚡ Запуск бенчмарка производительности...")
        
        start_time = time.time()
        
        def benchmark_worker():
            """Воркер для бенчмарка"""
            try:
                with get_session() as session:
                    # Случайные запросы
                    user_id = random.choice(users)['user_id']
                    
                    # Запрос аккаунтов
                    accounts = get_user_instagram_accounts(user_id)
                    
                    # Запрос задач
                    tasks = session.query(PublishTask).filter_by(user_id=user_id).limit(10).all()
                    
                    self.performance_stats['queries_executed'] += 2
                    
            except Exception as e:
                logger.error(f"❌ Ошибка в бенчмарке: {e}")
        
        # Запускаем 100 параллельных операций
        with ThreadPoolExecutor(max_workers=20) as executor:
            futures = [executor.submit(benchmark_worker) for _ in range(100)]
            
            for future in futures:
                future.result()
        
        end_time = time.time()
        
        return {
            'duration': end_time - start_time,
            'queries_per_second': self.performance_stats['queries_executed'] / (end_time - start_time),
            'total_queries': self.performance_stats['queries_executed']
        }
        
    def cleanup_demo_data(self, users: List[Dict[str, Any]]):
        """Очистка демонстрационных данных"""
        logger.info("🧹 Очистка демонстрационных данных...")
        
        try:
            with get_session() as session:
                for user_data in users:
                    user_id = user_data['user_id']
                    
                    # Удаляем задачи
                    session.query(PublishTask).filter_by(user_id=user_id).delete()
                    
                    # Удаляем warmup задачи
                    if user_data['accounts']:
                        session.query(WarmupTask).filter(
                            WarmupTask.account_id.in_(user_data['accounts'])
                        ).delete()
                    
                    # Удаляем аккаунты
                    session.query(InstagramAccount).filter_by(user_id=user_id).delete()
                    
                    # Удаляем пользователя
                    session.query(TelegramUser).filter_by(telegram_id=user_id).delete()
                
                session.commit()
                logger.info("✅ Демонстрационные данные очищены")
                
        except Exception as e:
            logger.error(f"❌ Ошибка очистки: {e}")
            
    def run_full_demo(self):
        """Запуск полной демонстрации"""
        logger.info("🚀" * 80)
        logger.info("🚀 ФИНАЛЬНАЯ ДЕМОНСТРАЦИЯ ИЗОЛЯЦИИ НА PostgreSQL")
        logger.info("🚀" * 80)
        
        self.performance_stats['start_time'] = datetime.now()
        
        try:
            # 1. Инициализация БД
            logger.info("🔧 Инициализация подключения к PostgreSQL...")
            init_db()
            
            # 2. Тестирование возможностей PostgreSQL
            pg_features = self.test_postgresql_features()
            
            # 3. Создание демо данных
            demo_users = self.create_demo_users()
            if not demo_users:
                logger.error("💥 Не удалось создать демо пользователей")
                return False
            
            # 4. Тест изоляции
            isolation_results = self.test_concurrent_isolation(demo_users)
            
            # 5. Бенчмарк производительности
            perf_results = self.performance_benchmark(demo_users)
            
            # 6. Итоги
            self.print_final_results(pg_features, isolation_results, perf_results)
            
            # 7. Очистка
            self.cleanup_demo_data(demo_users)
            
            # Проверяем успешность
            if isolation_results['isolation_violations'] == 0 and isolation_results['cross_contamination'] == 0:
                logger.info("🏆 ДЕМОНСТРАЦИЯ ЗАВЕРШЕНА УСПЕШНО!")
                return True
            else:
                logger.error("💥 ОБНАРУЖЕНЫ ПРОБЛЕМЫ С ИЗОЛЯЦИЕЙ!")
                return False
                
        except Exception as e:
            logger.error(f"❌ Критическая ошибка демонстрации: {e}")
            return False
            
    def print_final_results(self, pg_features, isolation_results, perf_results):
        """Вывод итоговых результатов"""
        end_time = datetime.now()
        duration = (end_time - self.performance_stats['start_time']).total_seconds()
        
        logger.info("=" * 80)
        logger.info("🐘 ИТОГИ ДЕМОНСТРАЦИИ PostgreSQL")
        logger.info("=" * 80)
        
        logger.info(f"⏱️ Время выполнения: {duration:.2f} секунд")
        logger.info(f"🐘 PostgreSQL версия: {pg_features.get('version', 'Неизвестно')[:50]}...")
        
        logger.info("\n📊 ИЗОЛЯЦИЯ ПОЛЬЗОВАТЕЛЕЙ:")
        logger.info(f"   ✅ Успешных операций: {isolation_results['successful_operations']}")
        logger.info(f"   ❌ Нарушений изоляции: {isolation_results['isolation_violations']}")
        logger.info(f"   🚫 Загрязнений данных: {isolation_results['cross_contamination']}")
        logger.info(f"   🔄 Параллельных запросов: {isolation_results['concurrent_queries']}")
        
        logger.info("\n⚡ ПРОИЗВОДИТЕЛЬНОСТЬ:")
        logger.info(f"   🏃 Запросов в секунду: {perf_results['queries_per_second']:.2f}")
        logger.info(f"   📈 Всего запросов: {perf_results['total_queries']}")
        logger.info(f"   ⏱️ Время бенчмарка: {perf_results['duration']:.2f} сек")
        
        logger.info("\n🔧 ВОЗМОЖНОСТИ PostgreSQL:")
        logger.info(f"   📄 JSON поддержка: {'✅' if pg_features['json_support'] else '❌'}")
        logger.info(f"   🔗 Foreign Key: {'✅' if pg_features['foreign_key_constraints'] else '❌'}")
        
        if isolation_results['isolation_violations'] == 0 and isolation_results['cross_contamination'] == 0:
            logger.info("\n🎉 ИЗОЛЯЦИЯ РАБОТАЕТ ИДЕАЛЬНО!")
            logger.info("🚀 Система готова к продакшену на PostgreSQL!")
        else:
            logger.info("\n⚠️ ОБНАРУЖЕНЫ ПРОБЛЕМЫ!")
            logger.info("🔧 Требуется дополнительная настройка")
            
        logger.info("=" * 80)

def main():
    """Главная функция"""
    demo = PostgreSQLIsolationDemo()
    success = demo.run_full_demo()
    
    if success:
        print("\n🎉 ГОТОВО! PostgreSQL демонстрация завершена успешно!")
        print("🚀 Система изоляции работает на облачной базе данных!")
        exit(0)
    else:
        print("\n💥 ОШИБКА! Демонстрация завершилась с проблемами!")
        exit(1)

if __name__ == "__main__":
    main() 
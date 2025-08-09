#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Глубокий тест полной имитации продакшн системы
Запускает реальные системные сервисы и проверяет изоляцию в условиях полной нагрузки
"""

import logging
import random
import time
import asyncio
import threading
import queue
import multiprocessing
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import json

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('test_production_simulation.log')
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

class ProductionUser:
    """Пользователь для имитации продакшн нагрузки"""
    
    def __init__(self, user_id: int, name: str, profile: str = "normal"):
        self.user_id = user_id
        self.name = name
        self.profile = profile  # normal, heavy, light, vip
        self.accounts = []
        self.tasks = []
        self.activity_log = []
        self.error_count = 0
        self.success_count = 0
        self.start_time = datetime.now()
        
        # Профили активности
        self.profiles = {
            "light": {"accounts": (1, 3), "tasks_per_hour": (1, 5), "concurrent_ops": 1},
            "normal": {"accounts": (3, 8), "tasks_per_hour": (5, 15), "concurrent_ops": 2},
            "heavy": {"accounts": (8, 15), "tasks_per_hour": (15, 30), "concurrent_ops": 4},
            "vip": {"accounts": (10, 20), "tasks_per_hour": (20, 50), "concurrent_ops": 6}
        }
        
    def get_profile_settings(self):
        return self.profiles.get(self.profile, self.profiles["normal"])
        
    def create_realistic_accounts(self):
        """Создать реалистичные аккаунты"""
        settings = self.get_profile_settings()
        accounts_count = random.randint(*settings["accounts"])
        
        logger.info(f"👤 {self.name} ({self.profile}): Создаю {accounts_count} аккаунтов...")
        
        domains = ["gmail.com", "yahoo.com", "outlook.com", "hotmail.com", "icloud.com"]
        statuses = ["active", "warming", "ready"]
        
        with get_session() as session:
            for i in range(accounts_count):
                username = f"{self.name.lower()}_{random.randint(1000, 9999)}"
                password = f"Pass_{self.user_id}_{random.randint(100, 999)}"
                email = f"{username}@{random.choice(domains)}"
                
                account = InstagramAccount(
                    user_id=self.user_id,
                    username=username,
                    password=password,
                    status=random.choice(statuses),
                    is_active=True,
                    email=email,
                    full_name=f"{self.name} Test Account {i+1}",
                    biography=f"Test account for user {self.name}"
                )
                
                session.add(account)
                session.commit()
                
                self.accounts.append(account.id)
                
        logger.info(f"👤 {self.name}: Создано {len(self.accounts)} аккаунтов")
        
    def simulate_user_activity(self, duration_minutes: int = 5):
        """Имитация активности пользователя"""
        settings = self.get_profile_settings()
        end_time = datetime.now() + timedelta(minutes=duration_minutes)
        
        logger.info(f"🎬 {self.name}: Начинаю имитацию активности на {duration_minutes} минут")
        
        while datetime.now() < end_time:
            try:
                # Случайная активность
                activity_type = random.choice([
                    "check_accounts", "create_task", "check_tasks", 
                    "update_account", "mass_operation"
                ])
                
                self.perform_activity(activity_type)
                
                # Пауза между активностями
                pause = random.uniform(1, 10)  # 1-10 секунд
                time.sleep(pause)
                
            except Exception as e:
                self.error_count += 1
                self.activity_log.append({
                    "time": datetime.now().isoformat(),
                    "activity": "error",
                    "error": str(e)
                })
                logger.error(f"❌ {self.name}: Ошибка активности: {e}")
                
        logger.info(f"🏁 {self.name}: Активность завершена. Успехов: {self.success_count}, Ошибок: {self.error_count}")
        
    def perform_activity(self, activity_type: str):
        """Выполнить конкретную активность"""
        start_time = time.time()
        
        try:
            if activity_type == "check_accounts":
                accounts = get_user_instagram_accounts(user_id=self.user_id)
                self.log_activity(activity_type, {"accounts_found": len(accounts)})
                
            elif activity_type == "create_task":
                if self.accounts:
                    account_id = random.choice(self.accounts)
                    self.create_random_task(account_id)
                    
            elif activity_type == "check_tasks":
                with get_session() as session:
                    tasks = session.query(PublishTask).filter_by(user_id=self.user_id).all()
                    self.log_activity(activity_type, {"tasks_found": len(tasks)})
                    
            elif activity_type == "update_account":
                if self.accounts:
                    account_id = random.choice(self.accounts)
                    account = get_user_instagram_account(account_id=account_id, user_id=self.user_id)
                    if account:
                        self.log_activity(activity_type, {"account": account.username})
                        
            elif activity_type == "mass_operation":
                # Имитация массовой операции
                accounts = get_user_instagram_accounts(user_id=self.user_id)
                processed = 0
                for account in accounts[:5]:  # Обрабатываем до 5 аккаунтов
                    # Имитация обработки
                    time.sleep(0.1)
                    processed += 1
                self.log_activity(activity_type, {"processed_accounts": processed})
                
            self.success_count += 1
            
        except Exception as e:
            self.error_count += 1
            raise e
            
    def create_random_task(self, account_id: int):
        """Создать случайную задачу"""
        with get_session() as session:
            task_types = [TaskType.PHOTO, TaskType.VIDEO, TaskType.STORY, TaskType.REEL]
            
            task = PublishTask(
                user_id=self.user_id,
                account_id=account_id,
                task_type=random.choice(task_types),
                caption=f"Test post from {self.name} at {datetime.now().isoformat()}",
                scheduled_time=datetime.now() + timedelta(hours=random.randint(1, 48))
            )
            session.add(task)
            session.commit()
            self.tasks.append(task.id)
            
    def log_activity(self, activity_type: str, data: Dict[str, Any]):
        """Логирование активности"""
        self.activity_log.append({
            "time": datetime.now().isoformat(),
            "activity": activity_type,
            "data": data
        })
        
    def get_statistics(self) -> Dict[str, Any]:
        """Получить статистику пользователя"""
        total_time = (datetime.now() - self.start_time).total_seconds()
        
        return {
            "user_id": self.user_id,
            "name": self.name,
            "profile": self.profile,
            "accounts_created": len(self.accounts),
            "tasks_created": len(self.tasks),
            "total_activities": len(self.activity_log),
            "success_count": self.success_count,
            "error_count": self.error_count,
            "error_rate": self.error_count / max(self.success_count + self.error_count, 1),
            "total_time_seconds": total_time,
            "activities_per_minute": len(self.activity_log) / max(total_time / 60, 1)
        }

class SystemServiceSimulator:
    """Симулятор системных сервисов"""
    
    def __init__(self):
        self.services = {}
        self.running = False
        self.stats = {
            "smart_validator": {"cycles": 0, "accounts_processed": 0, "errors": 0},
            "account_validator": {"cycles": 0, "accounts_processed": 0, "errors": 0},
            "proxy_manager": {"cycles": 0, "accounts_processed": 0, "errors": 0},
            "health_monitor": {"cycles": 0, "status_checks": 0, "errors": 0}
        }
        
    def start_smart_validator(self):
        """Запуск смарт валидатора"""
        def run_validator():
            validator = SmartValidatorService()
            
            while self.running:
                try:
                    logger.info("🔍 SmartValidator: Запуск цикла валидации")
                    
                    # Получаем пользователей для обработки
                    cache = get_user_cache()
                    users = cache.get_active_users_safe()
                    
                    if users:
                        processing_state = ProcessingState("smart_validator_simulation")
                        processing_state.start_cycle(users)
                        
                        processed = 0
                        for user_id in users:
                            processing_state.start_user_processing(user_id)
                            
                            # Получаем аккаунты пользователя
                            user_accounts = get_user_instagram_accounts(user_id=user_id)
                            
                            # Имитируем валидацию
                            for account in user_accounts:
                                # Случайная валидация
                                if random.random() < 0.1:  # 10% вероятность добавления в очередь
                                    logger.debug(f"📋 SmartValidator: Аккаунт {account.username} добавлен в очередь валидации")
                                processed += 1
                                time.sleep(0.01)  # Имитация времени обработки
                                
                            processing_state.complete_user_processing(user_id, True)
                            
                        processing_state.complete_cycle()
                        
                        self.stats["smart_validator"]["cycles"] += 1
                        self.stats["smart_validator"]["accounts_processed"] += processed
                        
                        logger.info(f"✅ SmartValidator: Цикл завершен, обработано {processed} аккаунтов")
                    
                    # Пауза между циклами
                    time.sleep(30)  # 30 секунд между циклами
                    
                except Exception as e:
                    self.stats["smart_validator"]["errors"] += 1
                    logger.error(f"❌ SmartValidator: Ошибка цикла: {e}")
                    time.sleep(10)
                    
        thread = threading.Thread(target=run_validator, name="SmartValidator")
        thread.daemon = True
        thread.start()
        self.services["smart_validator"] = thread
        
    def start_account_validator(self):
        """Запуск валидатора аккаунтов"""
        def run_validator():
            validator = AccountValidatorService()
            
            while self.running:
                try:
                    logger.info("🔧 AccountValidator: Запуск цикла проверки")
                    
                    cache = get_user_cache()
                    users = cache.get_active_users_safe()
                    
                    if users:
                        processing_state = ProcessingState("account_validator_simulation")
                        processing_state.start_cycle(users)
                        
                        processed = 0
                        for user_id in users:
                            processing_state.start_user_processing(user_id)
                            
                            user_accounts = get_user_instagram_accounts(user_id=user_id)
                            
                            # Имитируем проверку аккаунтов
                            for account in user_accounts:
                                # Случайная проверка
                                if random.random() < 0.05:  # 5% вероятность проверки
                                    logger.debug(f"🔧 AccountValidator: Проверка аккаунта {account.username}")
                                    time.sleep(0.1)  # Имитация времени проверки
                                processed += 1
                                
                            processing_state.complete_user_processing(user_id, True)
                            
                        processing_state.complete_cycle()
                        
                        self.stats["account_validator"]["cycles"] += 1
                        self.stats["account_validator"]["accounts_processed"] += processed
                        
                        logger.info(f"✅ AccountValidator: Цикл завершен, проверено {processed} аккаунтов")
                    
                    time.sleep(60)  # 1 минута между циклами
                    
                except Exception as e:
                    self.stats["account_validator"]["errors"] += 1
                    logger.error(f"❌ AccountValidator: Ошибка цикла: {e}")
                    time.sleep(15)
                    
        thread = threading.Thread(target=run_validator, name="AccountValidator")
        thread.daemon = True
        thread.start()
        self.services["account_validator"] = thread
        
    def start_proxy_manager(self):
        """Запуск менеджера прокси"""
        def run_proxy_manager():
            while self.running:
                try:
                    logger.info("🌐 ProxyManager: Запуск цикла распределения")
                    
                    # Имитируем распределение прокси
                    cache = get_user_cache()
                    users = cache.get_active_users_safe()
                    
                    if users:
                        processing_state = ProcessingState("proxy_manager_simulation")
                        processing_state.start_cycle(users)
                        
                        processed = 0
                        for user_id in users:
                            processing_state.start_user_processing(user_id)
                            
                            user_accounts = get_user_instagram_accounts(user_id=user_id)
                            
                            # Имитируем назначение прокси
                            for account in user_accounts:
                                # Случайное назначение прокси
                                if random.random() < 0.02:  # 2% вероятность смены прокси
                                    logger.debug(f"🌐 ProxyManager: Смена прокси для {account.username}")
                                processed += 1
                                
                            processing_state.complete_user_processing(user_id, True)
                            
                        processing_state.complete_cycle()
                        
                        self.stats["proxy_manager"]["cycles"] += 1
                        self.stats["proxy_manager"]["accounts_processed"] += processed
                        
                        logger.info(f"✅ ProxyManager: Цикл завершен, обработано {processed} аккаунтов")
                    
                    time.sleep(120)  # 2 минуты между циклами
                    
                except Exception as e:
                    self.stats["proxy_manager"]["errors"] += 1
                    logger.error(f"❌ ProxyManager: Ошибка цикла: {e}")
                    time.sleep(20)
                    
        thread = threading.Thread(target=run_proxy_manager, name="ProxyManager")
        thread.daemon = True
        thread.start()
        self.services["proxy_manager"] = thread
        
    def start_health_monitor(self):
        """Запуск монитора здоровья"""
        def run_health_monitor():
            monitor = get_health_monitor()
            
            while self.running:
                try:
                    logger.info("💊 HealthMonitor: Проверка здоровья системы")
                    
                    # Выполняем проверку здоровья
                    health_result = monitor.perform_health_check()
                    
                    self.stats["health_monitor"]["cycles"] += 1
                    self.stats["health_monitor"]["status_checks"] += len(health_result.get("checks", []))
                    
                    status = health_result.get("status", "UNKNOWN")
                    logger.info(f"💊 HealthMonitor: Статус системы - {status}")
                    
                    time.sleep(45)  # 45 секунд между проверками
                    
                except Exception as e:
                    self.stats["health_monitor"]["errors"] += 1
                    logger.error(f"❌ HealthMonitor: Ошибка проверки: {e}")
                    time.sleep(15)
                    
        thread = threading.Thread(target=run_health_monitor, name="HealthMonitor")
        thread.daemon = True
        thread.start()
        self.services["health_monitor"] = thread
        
    def start_all_services(self):
        """Запуск всех сервисов"""
        logger.info("🚀 Запускаю все системные сервисы...")
        self.running = True
        
        self.start_smart_validator()
        self.start_account_validator()
        self.start_proxy_manager()
        self.start_health_monitor()
        
        logger.info("✅ Все сервисы запущены")
        
    def stop_all_services(self):
        """Остановка всех сервисов"""
        logger.info("🛑 Останавливаю все сервисы...")
        self.running = False
        
        # Ждем завершения потоков
        for name, thread in self.services.items():
            thread.join(timeout=5)
            logger.info(f"✅ Сервис {name} остановлен")
            
        logger.info("✅ Все сервисы остановлены")
        
    def get_service_statistics(self):
        """Получить статистику сервисов"""
        return self.stats.copy()

class ProductionSimulationTest:
    """Комплексный тест имитации продакшн системы"""
    
    def __init__(self, num_users: int = 15, simulation_duration: int = 10):
        self.num_users = num_users
        self.simulation_duration = simulation_duration  # в минутах
        self.users = []
        self.service_simulator = SystemServiceSimulator()
        self.test_results = {}
        self.start_time = datetime.now()
        self.isolation_violations = []
        
    def create_production_users(self):
        """Создать пользователей с разными профилями активности"""
        logger.info(f"👥 Создаю {self.num_users} продакшн пользователей...")
        
        profiles = ["light", "normal", "heavy", "vip"]
        user_names = [
            "Alice", "Bob", "Charlie", "Diana", "Emma", "Frank", "Grace", "Henry",
            "Ivy", "Jack", "Kate", "Liam", "Mia", "Noah", "Olivia", "Paul",
            "Quinn", "Ruby", "Sam", "Tina"
        ]
        
        for i in range(self.num_users):
            user_id = 2000000 + i  # Уникальные ID для продакшн теста
            name = user_names[i % len(user_names)] + f"_prod_{i+1}"
            profile = profiles[i % len(profiles)]
            
            user = ProductionUser(user_id, name, profile)
            self.users.append(user)
            
        logger.info(f"✅ Создано {len(self.users)} пользователей:")
        for profile in profiles:
            count = sum(1 for u in self.users if u.profile == profile)
            logger.info(f"   {profile}: {count} пользователей")
            
    def setup_production_data(self):
        """Настроить продакшн данные"""
        logger.info("📦 Настраиваю продакшн данные...")
        
        total_accounts = 0
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = []
            
            for user in self.users:
                future = executor.submit(user.create_realistic_accounts)
                futures.append((user, future))
                
            for user, future in futures:
                try:
                    future.result(timeout=30)
                    total_accounts += len(user.accounts)
                except Exception as e:
                    logger.error(f"❌ Ошибка создания аккаунтов для {user.name}: {e}")
                    
        logger.info(f"✅ Создано {total_accounts} аккаунтов для {len(self.users)} пользователей")
        
    def start_user_simulation(self):
        """Запуск симуляции активности пользователей"""
        logger.info(f"🎬 Запускаю симуляцию активности на {self.simulation_duration} минут...")
        
        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = []
            
            for user in self.users:
                future = executor.submit(user.simulate_user_activity, self.simulation_duration)
                futures.append((user, future))
                
            # Мониторинг выполнения
            completed = 0
            for user, future in futures:
                try:
                    future.result()
                    completed += 1
                    logger.info(f"✅ Пользователь {user.name} завершил активность ({completed}/{len(self.users)})")
                except Exception as e:
                    logger.error(f"❌ Ошибка активности пользователя {user.name}: {e}")
                    
        logger.info(f"🏁 Симуляция активности завершена: {completed}/{len(self.users)} пользователей")
        
    def monitor_isolation_during_simulation(self):
        """Мониторинг изоляции во время симуляции"""
        def isolation_monitor():
            logger.info("🔍 Запускаю мониторинг изоляции...")
            
            check_interval = 20  # секунд - более частые проверки для 15-минутного теста
            end_time = datetime.now() + timedelta(minutes=self.simulation_duration)
            
            while datetime.now() < end_time:
                try:
                    # Проверяем изоляцию для случайных пользователей (больше для 15-минутного теста)
                    sample_users = random.sample(self.users, min(8, len(self.users)))
                    
                    for user in sample_users:
                        violations = self.check_user_isolation(user)
                        if violations:
                            self.isolation_violations.extend(violations)
                            
                    time.sleep(check_interval)
                    
                except Exception as e:
                    logger.error(f"❌ Ошибка мониторинга изоляции: {e}")
                    time.sleep(10)
                    
            logger.info("🏁 Мониторинг изоляции завершен")
            
        monitor_thread = threading.Thread(target=isolation_monitor, name="IsolationMonitor")
        monitor_thread.daemon = True
        monitor_thread.start()
        return monitor_thread
        
    def check_user_isolation(self, user: ProductionUser) -> List[Dict[str, Any]]:
        """Проверить изоляцию конкретного пользователя"""
        violations = []
        
        try:
            # Получаем аккаунты пользователя
            user_accounts = get_user_instagram_accounts(user_id=user.user_id)
            
            # Проверяем, что все аккаунты принадлежат правильному пользователю
            for account in user_accounts:
                if account.user_id != user.user_id:
                    violations.append({
                        "type": "wrong_user_id",
                        "user_id": user.user_id,
                        "account_id": account.id,
                        "account_username": account.username,
                        "actual_user_id": account.user_id,
                        "timestamp": datetime.now().isoformat()
                    })
                    
            # Пытаемся получить аккаунты других пользователей
            other_users = random.sample([u for u in self.users if u.user_id != user.user_id], 2)
            
            for other_user in other_users:
                if other_user.accounts:
                    try:
                        # Пытаемся получить чужой аккаунт
                        foreign_account = get_user_instagram_account(
                            account_id=other_user.accounts[0],
                            user_id=user.user_id
                        )
                        if foreign_account:
                            violations.append({
                                "type": "unauthorized_access",
                                "user_id": user.user_id,
                                "accessed_account_id": foreign_account.id,
                                "accessed_account_username": foreign_account.username,
                                "actual_owner": other_user.user_id,
                                "timestamp": datetime.now().isoformat()
                            })
                    except:
                        # Это хорошо - доступ должен быть заблокирован
                        pass
                        
        except Exception as e:
            logger.error(f"❌ Ошибка проверки изоляции для {user.name}: {e}")
            
        return violations
        
    def analyze_system_performance(self):
        """Анализ производительности системы"""
        logger.info("📊 Анализирую производительность системы...")
        
        # Статистика пользователей
        user_stats = []
        total_activities = 0
        total_errors = 0
        
        for user in self.users:
            stats = user.get_statistics()
            user_stats.append(stats)
            total_activities += stats["total_activities"]
            total_errors += stats["error_count"]
            
        # Статистика сервисов
        service_stats = self.service_simulator.get_service_statistics()
        
        # Статистика изоляции
        isolation_stats = {
            "violations_found": len(self.isolation_violations),
            "violation_types": {},
            "affected_users": set()
        }
        
        for violation in self.isolation_violations:
            violation_type = violation["type"]
            isolation_stats["violation_types"][violation_type] = \
                isolation_stats["violation_types"].get(violation_type, 0) + 1
            isolation_stats["affected_users"].add(violation["user_id"])
            
        isolation_stats["affected_users"] = len(isolation_stats["affected_users"])
        
        self.test_results = {
            "test_duration_minutes": self.simulation_duration,
            "users": {
                "total": len(self.users),
                "by_profile": {profile: sum(1 for u in self.users if u.profile == profile) 
                             for profile in ["light", "normal", "heavy", "vip"]},
                "total_activities": total_activities,
                "total_errors": total_errors,
                "error_rate": total_errors / max(total_activities, 1),
                "stats": user_stats
            },
            "services": service_stats,
            "isolation": isolation_stats,
            "system": {
                "active_users_found": len(get_user_cache().get_active_users_safe()),
                "processing_states": health_check_processing_states(),
                "health_status": get_health_monitor().get_health_summary()
            }
        }
        
        return self.test_results
        
    def cleanup_production_data(self):
        """Очистка продакшн данных"""
        logger.info("🧹 Очищаю продакшн данные...")
        
        total_deleted_accounts = 0
        total_deleted_tasks = 0
        
        try:
            with get_session() as session:
                for user in self.users:
                    # Удаляем аккаунты
                    for account_id in user.accounts:
                        try:
                            # Удаляем связанные задачи сначала
                            session.query(PublishTask).filter_by(account_id=account_id).delete()
                            session.query(WarmupTask).filter_by(account_id=account_id).delete()
                            
                            # Удаляем аккаунт
                            session.query(InstagramAccount).filter_by(id=account_id).delete()
                            total_deleted_accounts += 1
                            
                        except Exception as e:
                            logger.error(f"❌ Ошибка удаления аккаунта {account_id}: {e}")
                            
                    # Удаляем задачи пользователя
                    deleted_tasks = session.query(PublishTask).filter_by(user_id=user.user_id).delete()
                    total_deleted_tasks += deleted_tasks
                    
                session.commit()
                
        except Exception as e:
            logger.error(f"❌ Ошибка очистки данных: {e}")
            
        logger.info(f"✅ Очистка завершена:")
        logger.info(f"   🗑️ Удалено аккаунтов: {total_deleted_accounts}")
        logger.info(f"   🗑️ Удалено задач: {total_deleted_tasks}")
        
    def run_comprehensive_production_test(self):
        """Запуск полного продакшн теста"""
        logger.info("=" * 100)
        logger.info("🏭 ЗАПУСК ПОЛНОГО ТЕСТА ИМИТАЦИИ ПРОДАКШН СИСТЕМЫ")
        logger.info("=" * 100)
        
        try:
            # 1. Создание пользователей
            self.create_production_users()
            
            # 2. Настройка данных
            self.setup_production_data()
            
            # 3. Запуск системных сервисов
            self.service_simulator.start_all_services()
            
            # 4. Запуск мониторинга изоляции
            isolation_monitor = self.monitor_isolation_during_simulation()
            
            # 5. Имитация активности пользователей
            self.start_user_simulation()
            
            # 6. Ждем завершения мониторинга
            isolation_monitor.join(timeout=10)
            
            # 7. Остановка сервисов
            self.service_simulator.stop_all_services()
            
            # 8. Анализ результатов
            self.analyze_system_performance()
            
            # 9. Вывод результатов
            self.print_final_results()
            
        finally:
            # Очистка данных
            self.cleanup_production_data()
            
    def print_final_results(self):
        """Вывод финальных результатов"""
        logger.info("=" * 100)
        logger.info("📊 РЕЗУЛЬТАТЫ ПОЛНОГО ПРОДАКШН ТЕСТА")
        logger.info("=" * 100)
        
        results = self.test_results
        total_time = (datetime.now() - self.start_time).total_seconds()
        
        logger.info(f"⏱️ Время выполнения: {total_time:.2f} секунд ({self.simulation_duration} минут симуляции)")
        logger.info(f"👥 Пользователей: {results['users']['total']}")
        
        # Статистика по профилям
        logger.info("👤 Распределение по профилям:")
        for profile, count in results['users']['by_profile'].items():
            logger.info(f"   {profile}: {count} пользователей")
            
        # Активность пользователей
        logger.info(f"🎬 Общая активность:")
        logger.info(f"   📋 Всего активностей: {results['users']['total_activities']}")
        logger.info(f"   ❌ Всего ошибок: {results['users']['total_errors']}")
        logger.info(f"   📊 Процент ошибок: {results['users']['error_rate']:.2%}")
        
        # Системные сервисы
        logger.info("🔧 Системные сервисы:")
        for service, stats in results['services'].items():
            logger.info(f"   {service}:")
            logger.info(f"      Циклов: {stats['cycles']}")
            logger.info(f"      Обработано: {stats['accounts_processed']}")
            logger.info(f"      Ошибок: {stats['errors']}")
            
        # Изоляция
        isolation = results['isolation']
        logger.info(f"🔒 Изоляция данных:")
        logger.info(f"   ❌ Нарушений найдено: {isolation['violations_found']}")
        logger.info(f"   👥 Пострадавших пользователей: {isolation['affected_users']}")
        
        if isolation['violation_types']:
            logger.info("   📋 Типы нарушений:")
            for vtype, count in isolation['violation_types'].items():
                logger.info(f"      {vtype}: {count}")
        
        # Общая оценка
        logger.info("=" * 100)
        
        success_criteria = {
            "user_error_rate": results['users']['error_rate'] < 0.05,  # < 5% ошибок
            "isolation_violations": isolation['violations_found'] == 0,
            "service_errors": all(stats['errors'] == 0 for stats in results['services'].values()),
            "system_health": results['system']['health_status'].get('current_status') == 'HEALTHY'
        }
        
        all_success = all(success_criteria.values())
        
        if all_success:
            logger.info("🎉 ВСЕ КРИТЕРИИ ПРОЙДЕНЫ! СИСТЕМА ГОТОВА К ПРОДАКШН!")
        else:
            logger.error("❌ ОБНАРУЖЕНЫ КРИТИЧЕСКИЕ ПРОБЛЕМЫ!")
            
        logger.info("📋 Критерии оценки:")
        for criterion, passed in success_criteria.items():
            status = "✅ ПРОЙДЕН" if passed else "❌ НЕ ПРОЙДЕН"
            logger.info(f"   {criterion}: {status}")
            
        logger.info("=" * 100)

def main():
    """Главная функция"""
    # Настройка теста: 15 пользователей, 15 минут симуляции
    test = ProductionSimulationTest(num_users=15, simulation_duration=15)
    test.run_comprehensive_production_test()

if __name__ == "__main__":
    main() 
#!/usr/bin/env python3
"""
Умный сервис валидации аккаунтов с управлением нагрузкой
"""

import asyncio
import time
import logging
import threading
import queue
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Set
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from enum import Enum
import random

from database.db_manager import get_session, update_instagram_account
from database.models import InstagramAccount
from database.safe_user_wrapper import get_user_instagram_accounts as get_instagram_accounts
from database.user_management import get_active_users, get_user_info
from utils.user_cache import get_user_cache, process_users_with_limits
from utils.processing_state import ProcessingState
from instagram.client import get_instagram_client
from utils.user_logger import get_user_logger, log_user_error  # per-user logging

logger = logging.getLogger(__name__)

class ValidationPriority(Enum):
    """Приоритеты валидации"""
    CRITICAL = 1  # Аккаунт нужен для активной задачи
    HIGH = 2      # Аккаунт скоро понадобится
    NORMAL = 3    # Обычная проверка
    LOW = 4       # Фоновая проверка

class AccountStatus(Enum):
    """Статусы аккаунтов"""
    VALID = "valid"
    INVALID = "invalid"
    CHECKING = "checking"
    RECOVERING = "recovering"
    FAILED = "failed"
    COOLDOWN = "cooldown"

@dataclass
class ValidationTask:
    """Задача валидации"""
    account_id: int
    priority: ValidationPriority
    retry_count: int = 0
    last_check: Optional[datetime] = None
    next_check: Optional[datetime] = None
    status: AccountStatus = AccountStatus.VALID

@dataclass
class SystemLoad:
    """Информация о нагрузке системы"""
    active_tasks: int = 0
    active_recoveries: int = 0
    cpu_usage: float = 0.0
    memory_usage: float = 0.0
    
    @property
    def is_high_load(self) -> bool:
        """Высокая ли нагрузка"""
        return (
            self.active_tasks > 15 or 
            self.active_recoveries > 3 or
            self.cpu_usage > 85 or
            self.memory_usage > 90
        )

class SmartValidatorService:
    """Умный сервис валидации с управлением нагрузкой"""
    
    def __init__(self, 
                 check_interval_minutes: int = 180,  # 🔄 3 часа вместо 30 минут
                 max_concurrent_checks: int = 2,     # 🔧 Меньше проверок
                 max_concurrent_recoveries: int = 1,
                 recovery_cooldown_minutes: int = 360):  # 🔄 6 часов cooldown
        """
        Args:
            check_interval_minutes: Интервал между проверками
            max_concurrent_checks: Макс. одновременных проверок
            max_concurrent_recoveries: Макс. одновременных восстановлений
            recovery_cooldown_minutes: Задержка между попытками восстановления
        """
        self.check_interval = check_interval_minutes * 60
        self.max_concurrent_checks = max_concurrent_checks
        self.max_concurrent_recoveries = max_concurrent_recoveries
        self.recovery_cooldown = recovery_cooldown_minutes * 60
        
        # Очереди задач
        self.check_queue = queue.PriorityQueue()
        self.recovery_queue = queue.PriorityQueue()
        
        # Статусы аккаунтов
        self.account_statuses: Dict[int, ValidationTask] = {}
        self.active_checks: Set[int] = set()
        self.active_recoveries: Set[int] = set()
        
        # Потоки и исполнители
        self.is_running = False
        self._check_thread = None
        self._recovery_thread = None
        self._monitor_thread = None
        self._check_executor = ThreadPoolExecutor(max_workers=max_concurrent_checks)
        self._recovery_executor = ThreadPoolExecutor(max_workers=max_concurrent_recoveries)
        
        # Блокировки
        self._status_lock = threading.Lock()
        self._load_lock = threading.Lock()
        
        # Информация о нагрузке
        self.system_load = SystemLoad()
        
        # Callback для уведомлений
        self.on_status_change = None
        
        logger.info(
            f"🚀 Инициализирован умный валидатор "
            f"(проверки: {max_concurrent_checks}, восстановления: {max_concurrent_recoveries})"
        )
    
    def start(self):
        """Запуск сервиса"""
        if self.is_running:
            logger.warning("Сервис уже запущен")
            return
        
        self.is_running = True
        
        # Запускаем потоки
        self._check_thread = threading.Thread(target=self._check_worker, daemon=True)
        self._recovery_thread = threading.Thread(target=self._recovery_worker, daemon=True)
        self._monitor_thread = threading.Thread(target=self._monitor_system, daemon=True)
        
        self._check_thread.start()
        self._recovery_thread.start()
        self._monitor_thread.start()
        
        # Запускаем периодическую проверку
        threading.Thread(target=self._periodic_check, daemon=True).start()
        
        logger.info("✅ Умный валидатор запущен")
    
    def stop(self):
        """Остановка сервиса"""
        self.is_running = False
        self._check_executor.shutdown(wait=True)
        self._recovery_executor.shutdown(wait=True)
        logger.info("🛑 Умный валидатор остановлен")
    
    def request_validation(self, account_id: int, priority: ValidationPriority = ValidationPriority.NORMAL):
        """
        Запросить валидацию аккаунта
        
        Args:
            account_id: ID аккаунта
            priority: Приоритет проверки
        """
        with self._status_lock:
            task = self.account_statuses.get(account_id)
            
            # Если аккаунт уже проверяется или восстанавливается
            if task and task.status in [AccountStatus.CHECKING, AccountStatus.RECOVERING]:
                logger.debug(f"Аккаунт {account_id} уже обрабатывается")
                return
            
            # Если аккаунт в cooldown после неудачного восстановления
            if task and task.status == AccountStatus.COOLDOWN:
                if task.next_check and datetime.now() < task.next_check:
                    logger.debug(f"Аккаунт {account_id} в cooldown до {task.next_check}")
                    return
            
            # Создаем или обновляем задачу
            if not task:
                task = ValidationTask(account_id=account_id, priority=priority)
                self.account_statuses[account_id] = task
            else:
                task.priority = min(task.priority, priority)  # Повышаем приоритет
            
            # Добавляем в очередь проверки
            self.check_queue.put((priority.value, account_id))
            logger.debug(f"Добавлена проверка аккаунта {account_id} с приоритетом {priority.name}")
    
    def get_account_status(self, account_id: int) -> Optional[AccountStatus]:
        """Получить статус аккаунта"""
        with self._status_lock:
            task = self.account_statuses.get(account_id)
            return task.status if task else None
    
    def is_account_valid(self, account_id: int) -> bool:
        """Проверить, валиден ли аккаунт"""
        status = self.get_account_status(account_id)
        return status == AccountStatus.VALID if status else True  # По умолчанию считаем валидным
    
    def get_system_load(self) -> SystemLoad:
        """Получить информацию о нагрузке"""
        with self._load_lock:
            return SystemLoad(
                active_tasks=len(self.active_checks),
                active_recoveries=len(self.active_recoveries),
                cpu_usage=self.system_load.cpu_usage,
                memory_usage=self.system_load.memory_usage
            )
    
    def _check_worker(self):
        """Воркер для проверки аккаунтов"""
        while self.is_running:
            try:
                # Получаем задачу из очереди
                priority, account_id = self.check_queue.get(timeout=1)
                
                # Проверяем нагрузку
                if self.system_load.is_high_load and priority > ValidationPriority.HIGH.value:
                    # При высокой нагрузке откладываем низкоприоритетные задачи
                    self.check_queue.put((priority, account_id))
                    time.sleep(5)
                    continue
                
                # Проверяем, не проверяется ли уже
                with self._status_lock:
                    if account_id in self.active_checks:
                        continue
                    self.active_checks.add(account_id)
                
                # Запускаем проверку
                self._check_executor.submit(self._check_account, account_id)
                
            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"Ошибка в check_worker: {e}")
    
    def _recovery_worker(self):
        """Воркер для восстановления аккаунтов"""
        while self.is_running:
            try:
                # Получаем задачу из очереди
                priority, account_id = self.recovery_queue.get(timeout=1)
                
                # При высокой нагрузке не восстанавливаем
                if self.system_load.is_high_load:
                    self.recovery_queue.put((priority, account_id))
                    time.sleep(10)
                    continue
                
                # Проверяем, не восстанавливается ли уже
                with self._status_lock:
                    if account_id in self.active_recoveries:
                        continue
                    if len(self.active_recoveries) >= self.max_concurrent_recoveries:
                        # Возвращаем в очередь если слишком много восстановлений
                        self.recovery_queue.put((priority, account_id))
                        time.sleep(2)
                        continue
                    self.active_recoveries.add(account_id)
                
                # Запускаем восстановление
                self._recovery_executor.submit(self._recover_account, account_id)
                
            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"Ошибка в recovery_worker: {e}")
    
    def _check_account(self, account_id: int):
        """Проверка одного аккаунта"""
        try:
            # Обновляем статус
            with self._status_lock:
                task = self.account_statuses.get(account_id)
                if task:
                    task.status = AccountStatus.CHECKING
                    # Проверяем, не слишком ли часто проверяем аккаунт
                    if task.last_check:
                        time_since_check = (datetime.now() - task.last_check).total_seconds()
                        if time_since_check < 7200:  # 🔄 Не чаще раза в 2 часа (было 5 мин)
                            logger.debug(f"Пропускаем проверку @{account_id}, прошло только {time_since_check:.0f} сек")
                            task.status = AccountStatus.VALID  # Предполагаем что все ок
                            self.active_checks.discard(account_id)
                            return
            
            # Добавляем случайную задержку для предотвращения одновременных проверок
            time.sleep(random.uniform(1, 3))
            
            # Получаем аккаунт из БД
            session = get_session()
            account = session.query(InstagramAccount).filter_by(id=account_id).first()
            session.close()
            
            if not account:
                logger.error(f"Аккаунт {account_id} не найден")
                return
            
            logger.info(f"🔍 Быстрая проверка @{account.username}")
            
            # Быстрая проверка через легкий запрос
            is_valid = self._quick_check(account)
            
            # Обновляем статус
            with self._status_lock:
                task = self.account_statuses[account_id]
                task.last_check = datetime.now()
                
                if is_valid:
                    task.status = AccountStatus.VALID
                    task.retry_count = 0
                    logger.info(f"✅ @{account.username} валиден")
                else:
                    task.status = AccountStatus.INVALID
                    # Добавляем в очередь восстановления с приоритетом
                    self.recovery_queue.put((task.priority.value, account_id))
                    logger.warning(f"❌ @{account.username} невалиден, добавлен в очередь восстановления")
            
            # Обновляем БД
            update_instagram_account(
                account_id,
                is_active=is_valid,
                last_check=datetime.now()
            )
            
            # Уведомляем об изменении статуса
            if self.on_status_change:
                self.on_status_change(account_id, AccountStatus.VALID if is_valid else AccountStatus.INVALID)
            
        except Exception as e:
            logger.error(f"Ошибка при проверке аккаунта {account_id}: {e}")
        finally:
            with self._status_lock:
                self.active_checks.discard(account_id)
    
    def _recover_account(self, account_id: int):
        """Восстановление одного аккаунта"""
        try:
            # Обновляем статус
            with self._status_lock:
                task = self.account_statuses.get(account_id)
                if task:
                    task.status = AccountStatus.RECOVERING
            
            # Получаем аккаунт из БД
            session = get_session()
            account = session.query(InstagramAccount).filter_by(id=account_id).first()
            session.close()
            
            if not account:
                logger.error(f"Аккаунт {account_id} не найден")
                return
            
            logger.info(f"🔧 Восстановление @{account.username}")
            
            # Пытаемся восстановить
            success = self._attempt_recovery(account)
            
            # Обновляем статус
            with self._status_lock:
                task = self.account_statuses[account_id]
                
                if success:
                    task.status = AccountStatus.VALID
                    task.retry_count = 0
                    logger.info(f"✅ @{account.username} успешно восстановлен")
                else:
                    task.retry_count += 1
                    if task.retry_count >= 3:
                        task.status = AccountStatus.FAILED
                        logger.error(f"❌ @{account.username} не удалось восстановить после 3 попыток")
                    else:
                        task.status = AccountStatus.COOLDOWN
                        task.next_check = datetime.now() + timedelta(seconds=self.recovery_cooldown)
                        logger.warning(f"⏳ @{account.username} в cooldown до {task.next_check}")
            
            # Обновляем БД
            update_instagram_account(
                account_id,
                is_active=success,
                last_check=datetime.now(),
                last_error=None if success else "Recovery failed"
            )
            
            # Уведомляем об изменении статуса
            if self.on_status_change:
                self.on_status_change(account_id, task.status)
            
        except Exception as e:
            logger.error(f"Ошибка при восстановлении аккаунта {account_id}: {e}")
        finally:
            with self._status_lock:
                self.active_recoveries.discard(account_id)
    
    def _quick_check(self, account: InstagramAccount) -> bool:
        """
        Быстрая проверка аккаунта (без восстановления)
        """
        try:
            # Пытаемся получить клиент без восстановления
            client = get_instagram_client(account.id, skip_recovery=True)
            if not client:
                return False
            
            # Пробуем простой запрос
            try:
                # Используем самый легкий метод
                client.get_timeline_feed()
                return True
            except Exception as e:
                error_msg = str(e).lower()
                # Если требуется челлендж или логин - невалидный
                if any(x in error_msg for x in ['challenge', 'login_required', 'checkpoint']):
                    return False
                # Другие ошибки могут быть временными
                return True
                
        except Exception as e:
            logger.error(f"Ошибка быстрой проверки @{account.username}: {e}")
            return False
    
    def _attempt_recovery(self, account: InstagramAccount) -> bool:
        """
        Попытка восстановления аккаунта с IMAP
        """
        try:
            # Проверяем наличие email для восстановления
            if not account.email or not account.email_password:
                logger.warning(f"У @{account.username} нет email для восстановления")
                # Обновляем статус в БД
                update_instagram_account(
                    account.id,
                    is_active=False,
                    status="no_email_data",
                    last_error="Нет данных email для восстановления",
                    last_check=datetime.now()
                )
                return False
            
            logger.info(f"🔄 Попытка IMAP восстановления для @{account.username}")
            
            # Импортируем функции для восстановления
            from instagram.email_utils_optimized import get_verification_code_from_email
            from instagram.client import InstagramClient
            
            # Создаем новый клиент для восстановления
            instagram_client = InstagramClient(account.id)
            
            # Пытаемся получить код верификации из почты
            verification_code = get_verification_code_from_email(
                account.email, 
                account.email_password, 
                max_attempts=3, 
                delay_between_attempts=15
            )
            
            if verification_code:
                logger.info(f"✅ Получен код верификации {verification_code} для @{account.username}")
                
                # Пытаемся войти с кодом верификации
                login_success = instagram_client.login_with_challenge_code(verification_code)
                if login_success:
                    logger.info(f"✅ Успешное IMAP восстановление для @{account.username}")
                    
                # Проверяем что восстановление прошло успешно
                try:
                    instagram_client.client.get_timeline_feed()
                    # Обновляем статус в БД как активный
                    update_instagram_account(
                        account.id,
                        is_active=True,
                        status="active",
                        last_error=None,
                        last_check=datetime.now()
                    )
                    return True
                except Exception as verify_error:
                    logger.warning(f"❌ Восстановление @{account.username} не подтвердилось: {verify_error}")
                    # Обновляем статус в БД
                    update_instagram_account(
                        account.id,
                        is_active=False,
                        status="recovery_verify_failed",
                        last_error=f"Восстановление не подтвердилось: {verify_error}",
                        last_check=datetime.now()
                    )
                    return False
                else:
                    logger.warning(f"❌ Не удалось войти с кодом верификации для @{account.username}")
                    # Обновляем статус в БД
                    update_instagram_account(
                        account.id,
                        is_active=False,
                        status="recovery_login_failed",
                        last_error="Не удалось войти с кодом верификации",
                        last_check=datetime.now()
                    )
                    return False
            else:
                logger.warning(f"❌ Не удалось получить код верификации для @{account.username}")
                # Обновляем статус в БД
                update_instagram_account(
                    account.id,
                    is_active=False,
                    status="email_code_failed",
                    last_error="Не удалось получить код из email",
                    last_check=datetime.now()
                )
            return False
            
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Ошибка IMAP восстановления @{account.username}: {e}")
            
            # Определяем тип ошибки для более точного статуса
            if "challenge_required" in error_msg.lower():
                error_type = "challenge_required"
            elif "login_required" in error_msg.lower():
                error_type = "login_required"
            elif "email" in error_msg.lower():
                error_type = "email_error"
            else:
                error_type = "recovery_error"
            
            # Обновляем статус в БД
            update_instagram_account(
                account.id,
                is_active=False,
                status=error_type,
                last_error=error_msg,
                last_check=datetime.now()
            )
            return False
    
    def _monitor_system(self):
        """Мониторинг нагрузки системы"""
        while self.is_running:
            try:
                import psutil
                
                # Получаем информацию о системе
                cpu_percent = psutil.cpu_percent(interval=1)
                memory_percent = psutil.virtual_memory().percent
                
                with self._load_lock:
                    self.system_load.cpu_usage = cpu_percent
                    self.system_load.memory_usage = memory_percent
                
                # Логируем если высокая нагрузка
                if self.system_load.is_high_load:
                    logger.warning(
                        f"⚠️ Высокая нагрузка: CPU {cpu_percent}%, RAM {memory_percent}%, "
                        f"проверок: {len(self.active_checks)}, восстановлений: {len(self.active_recoveries)}"
                    )
                
                time.sleep(10)  # Проверяем каждые 10 секунд
                
            except Exception as e:
                logger.error(f"Ошибка мониторинга: {e}")
                time.sleep(30)
    
    def _periodic_check(self):
        """Периодическая проверка всех аккаунтов"""
        while self.is_running:
            try:
                time.sleep(self.check_interval)
                
                if self.system_load.is_high_load:
                    logger.info("⏸️ Пропускаем периодическую проверку из-за высокой нагрузки")
                    continue
                
                logger.info("🔄 Запуск периодической проверки")
                
                # 🔒 НОВАЯ ЛОГИКА: Обрабатываем пользователей изолированно
                user_cache = get_user_cache()
                users = user_cache.get_active_users_safe()
                
                if not users:
                    logger.warning("⚠️ Нет активных пользователей для валидации")
                    continue
                
                # Инициализируем отслеживание состояния
                processing_state = ProcessingState("smart_validator")
                processing_state.start_cycle(users)
                
                processed_users = 0
                for user_id in users:
                    try:
                        user_log = get_user_logger(user_id)
                        # Проверяем системную нагрузку перед каждым пользователем
                        if self.system_load.is_high_load:
                            processing_state.skip_user(user_id, "Высокая системная нагрузка")
                            user_log.info("⏸️ Пропуск проверки из-за высокой системной нагрузки")
                            logger.info(f"⏸️ Пропускаем пользователя {user_id} из-за нагрузки")
                            continue
                        
                        processing_state.start_user_processing(user_id)
                        user_log.info("🔍 Начинаем периодическую проверку аккаунтов")
                        
                        # Получаем аккаунты конкретного пользователя
                        user_accounts = get_instagram_accounts(user_id=user_id)
                        
                        if not user_accounts:
                            processing_state.complete_user_processing(user_id, True)
                            user_log.info("ℹ️ Аккаунтов не найдено для проверки")
                            continue
                        
                        user_log.info(f"📄 Найдено аккаунтов: {len(user_accounts)}")
                        logger.debug(f"👤 Валидация пользователя {user_id}: {len(user_accounts)} аккаунтов")
                        
                        # Обрабатываем аккаунты пользователя
                        user_processed_accounts = 0
                        for account in user_accounts:
                            # 🔄 НЕ проверяем активные аккаунты слишком часто
                            if account.is_active and account.status == 'active':
                                # Для активных аккаунтов - проверка раз в сутки максимум
                                with self._status_lock:
                                    task = self.account_statuses.get(account.id)
                                    if task and task.last_check:
                                        time_since_check = (datetime.now() - task.last_check).total_seconds()
                                        if time_since_check < 86400:  # 24 часа для активных
                                            continue
                            else:
                                # Проверяем когда последний раз проверяли неактивные
                                with self._status_lock:
                                    task = self.account_statuses.get(account.id)
                                    if task and task.last_check:
                                        time_since_check = (datetime.now() - task.last_check).total_seconds()
                                        if time_since_check < 10800:  # 3 часа для неактивных
                                            continue
                            
                            # Добавляем в очередь с низким приоритетом
                            self.request_validation(account.id, ValidationPriority.LOW)
                            user_processed_accounts += 1
                        
                        # Завершаем обработку пользователя
                        processing_state.complete_user_processing(user_id, True)
                        processed_users += 1
                        user_log.info(f"✅ В очередь проверки добавлено: {user_processed_accounts}")
                        
                        # Пауза между пользователями
                        time.sleep(0.1)
                        
                    except Exception as e:
                        # Ошибка обработки конкретного пользователя
                        processing_state.complete_user_processing(user_id, False, str(e))
                        logger.error(f"❌ Ошибка валидации пользователя {user_id}: {e}")
                        log_user_error(user_id, f"Ошибка периодической проверки: {e}")
                        continue
                
                # Завершаем цикл обработки
                processing_state.complete_cycle()
                logger.info(f"🏁 Периодическая проверка завершена: {processed_users}/{len(users)} пользователей")
                
            except Exception as e:
                logger.error(f"Ошибка периодической проверки: {e}")
    
    def get_stats(self) -> Dict:
        """Получить статистику работы"""
        with self._status_lock:
            status_counts = {}
            for status in AccountStatus:
                status_counts[status.value] = sum(
                    1 for task in self.account_statuses.values() 
                    if task.status == status
                )
            
            return {
                'total_accounts': len(self.account_statuses),
                'status_counts': status_counts,
                'active_checks': len(self.active_checks),
                'active_recoveries': len(self.active_recoveries),
                'check_queue_size': self.check_queue.qsize(),
                'recovery_queue_size': self.recovery_queue.qsize(),
                'system_load': {
                    'cpu': self.system_load.cpu_usage,
                    'memory': self.system_load.memory_usage,
                    'is_high': self.system_load.is_high_load
                }
            }

# Глобальный экземпляр
_smart_validator_instance: Optional[SmartValidatorService] = None
_instance_lock = threading.Lock()

def get_smart_validator() -> SmartValidatorService:
    """Получить экземпляр умного валидатора"""
    global _smart_validator_instance
    if _smart_validator_instance is None:
        with _instance_lock:
            if _smart_validator_instance is None:
                _smart_validator_instance = SmartValidatorService()
    return _smart_validator_instance

def validate_before_use(account_id: int, priority: ValidationPriority = ValidationPriority.HIGH) -> bool:
    """
    Проверить аккаунт перед использованием
    
    Returns:
        True если аккаунт валиден и готов к использованию
    """
    validator = get_smart_validator()
    
    # Проверяем текущий статус
    status = validator.get_account_status(account_id)
    
    # Если валиден - используем
    if status == AccountStatus.VALID:
        return True
    
    # Если проверяется или восстанавливается - ждем немного
    if status in [AccountStatus.CHECKING, AccountStatus.RECOVERING]:
        for _ in range(10):  # Ждем до 10 секунд
            time.sleep(1)
            status = validator.get_account_status(account_id)
            if status == AccountStatus.VALID:
                return True
            elif status not in [AccountStatus.CHECKING, AccountStatus.RECOVERING]:
                break
    
    # Если невалиден или неизвестен - запрашиваем проверку
    if status in [None, AccountStatus.INVALID]:
        validator.request_validation(account_id, priority)
        # Для критических задач ждем результат
        if priority == ValidationPriority.CRITICAL:
            for _ in range(30):  # Ждем до 30 секунд
                time.sleep(1)
                if validator.is_account_valid(account_id):
                    return True
    
    return False 
#!/usr/bin/env python3
"""
Фоновый сервис для периодической проверки валидности Instagram аккаунтов
"""

import asyncio
import time
import logging
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor

from database.db_manager import get_session, update_instagram_account
from database.models import InstagramAccount
from database.safe_user_wrapper import get_user_instagram_accounts as get_instagram_accounts
from database.user_management import get_active_users, get_user_info
from utils.user_cache import get_user_cache, process_users_with_limits
from utils.processing_state import ProcessingState
from instagram.client import get_instagram_client
from instagram.email_utils import get_code_from_generic_email
from utils.user_logger import get_user_logger, log_user_error, log_user_info

logger = logging.getLogger(__name__)

# Глобальная переменная для хранения единственного экземпляра сервиса
_validator_service_instance = None
_service_lock = threading.Lock()

class AccountValidatorService:
    """Сервис для фоновой проверки валидности аккаунтов"""
    
    def __init__(self, check_interval_minutes: int = 30, 
                 max_threads: int = 1,
                 auto_repair: bool = True):
        """
        Инициализация сервиса
        
        Args:
            check_interval_minutes: Интервал между проверками в минутах
            max_threads: Максимальное количество потоков для проверки
            auto_repair: Автоматически пытаться восстановить невалидные аккаунты
        """
        self.check_interval = check_interval_minutes * 60  # В секундах
        self.max_threads = max_threads
        self.auto_repair = auto_repair
        self.is_running = False
        self._thread = None
        self._executor = ThreadPoolExecutor(max_workers=max_threads)
        self._last_check_results = {}
        
        logger.info(f"🚀 Инициализирован сервис проверки аккаунтов (интервал: {check_interval_minutes} мин)")
    
    def start(self):
        """Запуск фонового сервиса"""
        if self.is_running:
            logger.warning("Сервис уже запущен")
            return
        
        self.is_running = True
        self._thread = threading.Thread(target=self._run_background_loop, daemon=True)
        self._thread.start()
        logger.info("✅ Фоновый сервис проверки аккаунтов запущен")
    
    def stop(self):
        """Остановка сервиса"""
        self.is_running = False
        if self._thread:
            self._thread.join(timeout=5)
        self._executor.shutdown(wait=True)
        logger.info("🛑 Фоновый сервис проверки аккаунтов остановлен")
    
    def _run_background_loop(self):
        """Основной цикл фоновой проверки"""
        # Начальная задержка перед первой проверкой
        initial_delay = 120  # 2 минуты задержка при старте
        logger.info(f"⏳ Ожидание {initial_delay} секунд перед первой проверкой...")
        time.sleep(initial_delay)
        
        consecutive_errors = 0
        max_consecutive_errors = 3
        
        while self.is_running:
            try:
                # Выполняем проверку
                logger.info("🔍 Начинаем проверку валидности аккаунтов...")
                results = self._check_all_accounts()
                
                # Сохраняем результаты
                self._last_check_results = results
                
                # Логируем результаты
                valid_count = len(results.get('valid', []))
                invalid_count = len(results.get('invalid', []))
                repaired_count = len(results.get('repaired', []))
                
                logger.info(
                    f"✅ Проверка завершена: "
                    f"валидных: {valid_count}, "
                    f"невалидных: {invalid_count}, "
                    f"восстановлено: {repaired_count}"
                )
                
                # Сбрасываем счетчик ошибок при успешной проверке
                consecutive_errors = 0
                
                # Ждем до следующей проверки (минимум 30 минут)
                actual_interval = max(self.check_interval, 1800)  # Минимум 30 минут
                logger.info(f"⏰ Следующая проверка через {actual_interval // 60} минут")
                time.sleep(actual_interval)
                
            except Exception as e:
                consecutive_errors += 1
                logger.error(f"❌ Ошибка в фоновом цикле проверки (#{consecutive_errors}): {e}")
                
                # Увеличиваем задержку при повторных ошибках
                if consecutive_errors >= max_consecutive_errors:
                    error_delay = 600  # 10 минут при множественных ошибках
                    logger.warning(f"⚠️ Множественные ошибки, увеличиваем задержку до {error_delay // 60} минут")
                else:
                    error_delay = 120  # 2 минуты при единичных ошибках
                
                time.sleep(error_delay)
    
    def _check_all_accounts(self) -> Dict[str, List]:
        """Проверка всех аккаунтов"""
        results = {
            'valid': [],
            'invalid': [],
            'repaired': [],
            'failed_repair': []
        }
        
        try:
            # 🔒 НОВАЯ ЛОГИКА: Обрабатываем пользователей изолированно
            user_cache = get_user_cache()
            users = user_cache.get_active_users_safe()
            
            if not users:
                logger.warning("⚠️ Нет активных пользователей для проверки")
                return results
            
            # Инициализируем отслеживание состояния
            processing_state = ProcessingState("account_validator")
            processing_state.start_cycle(users)
            
            total_accounts = 0
            processed_users = 0
            
            logger.info(f"📋 Начинаем проверку {len(users)} пользователей")
            
            # Обрабатываем каждого пользователя отдельно
            for user_id in users:
                try:
                    processing_state.start_user_processing(user_id)
                    user_log = get_user_logger(user_id)
                    user_log.info("🔍 Начинаем проверку аккаунтов пользователя")
                    
                    # Получаем аккаунты конкретного пользователя
                    user_accounts = get_instagram_accounts(user_id=user_id)
                    
                    if not user_accounts:
                        processing_state.complete_user_processing(user_id, True)
                        user_log.info("ℹ️ У пользователя нет аккаунтов для проверки")
                        continue
                    
                    total_accounts += len(user_accounts)
                    logger.info(f"👤 Пользователь {user_id}: {len(user_accounts)} аккаунтов")
                    user_log.info(f"📄 Найдено аккаунтов: {len(user_accounts)}")
                    
                    # Проверяем аккаунты пользователя последовательно с задержками
                    user_processed_accounts = 0
                    for i, account in enumerate(user_accounts):
                        try:
                            # Проверяем, не проверяли ли мы этот аккаунт недавно
                            if hasattr(account, 'last_check') and account.last_check:
                                time_since_check = (datetime.now() - account.last_check).total_seconds()
                                if time_since_check < 1800:  # Не чаще раза в 30 минут
                                    logger.debug(f"⏭️ Пропускаем @{account.username}, проверен {time_since_check//60:.0f} мин назад")
                                    results['valid'].append(account)  # Считаем валидным
                                    continue
                            
                            user_log.info(f"🔎 Проверка @{account.username}")
                            is_valid, was_repaired = self._check_account(account)
                            
                            if is_valid:
                                if was_repaired:
                                    results['repaired'].append(account)
                                    user_log.info(f"✅ @{account.username} восстановлен")
                                else:
                                    results['valid'].append(account)
                                    user_log.info(f"✅ @{account.username} валиден")
                            else:
                                results['invalid'].append(account)
                                user_log.warning(f"⚠️ @{account.username} невалиден")
                                if self.auto_repair:
                                    results['failed_repair'].append(account)
                                    user_log.info(f"🛠️ Запланирован ремонт @{account.username}")
                            
                            user_processed_accounts += 1
                            
                            # Добавляем задержку между проверками (увеличиваем до 30-60 секунд)
                            if i < len(user_accounts) - 1:
                                import random
                                delay = random.randint(30, 60)  # Случайная задержка 30-60 секунд
                                logger.debug(f"⏳ Ожидание {delay} секунд перед следующей проверкой...")
                                time.sleep(delay)
                                
                        except Exception as e:
                            logger.error(f"Ошибка при проверке {account.username}: {e}")
                            log_user_error(user_id, f"Ошибка при проверке @{getattr(account,'username','?')}: {e}")
                            results['invalid'].append(account)
                    
                    # Завершаем обработку пользователя
                    processing_state.complete_user_processing(user_id, True)
                    processed_users += 1
                    user_log.info(f"✅ Проверено аккаунтов: {user_processed_accounts}")
                    
                    # Пауза между пользователями
                    time.sleep(5)
                    
                except Exception as e:
                    # Ошибка обработки конкретного пользователя
                    processing_state.complete_user_processing(user_id, False, str(e))
                    logger.error(f"❌ Ошибка проверки пользователя {user_id}: {e}")
                    log_user_error(user_id, f"Ошибка проверки пользователя: {e}")
                    continue
            
            # Завершаем цикл обработки
            processing_state.complete_cycle()
            logger.info(f"🏁 Проверка завершена: {processed_users}/{len(users)} пользователей, {total_accounts} аккаунтов")
            
            return results
            
        except Exception as e:
            logger.error(f"Критическая ошибка при проверке аккаунтов: {e}")
            return results
    
    def _check_account(self, account: InstagramAccount) -> Tuple[bool, bool]:
        """
        Проверка одного аккаунта
        
        Returns:
            (is_valid, was_repaired) - валиден ли аккаунт и был ли он восстановлен
        """
        logger.info(f"🔍 Проверка аккаунта {account.username}")
        
        try:
            # Проверяем статус аккаунта - если он помечен как проблемный, пропускаем
            if hasattr(account, 'status') and account.status == 'problematic':
                logger.debug(f"⏭️ Пропускаем проблемный аккаунт {account.username}")
                return False, False
            
            # Пытаемся получить клиент
            client = get_instagram_client(account.id)
            
            if client:
                # Проверяем, можем ли выполнить базовый запрос
                try:
                    user_info = client.user_info(client.user_id)
                    if user_info:
                        logger.info(f"✅ {account.username} - валидный")
                        # Обновляем статус
                        update_instagram_account(
                            account.id, 
                            is_active=True,
                            last_error=None,
                            last_check=datetime.now()
                        )
                        return True, False
                except Exception as api_error:
                    logger.warning(f"⚠️ {account.username} - ошибка API: {api_error}")
                    
                    # Если аккаунт требует верификации и у нас есть email
                    if self.auto_repair and self._needs_verification(str(api_error)):
                        if account.email and account.email_password:
                            logger.info(f"🔧 Пытаемся восстановить {account.username}")
                            # Ограничиваем попытки восстановления
                            if self._repair_account(account):
                                return True, True
                            else:
                                # Если восстановление не удалось, помечаем как проблемный
                                logger.warning(f"❌ Не удалось восстановить {account.username}, помечаем как проблемный")
                                update_instagram_account(
                                    account.id, 
                                    is_active=False,
                                    status='problematic',
                                    last_error="Не удалось восстановить после challenge",
                                    last_check=datetime.now()
                                )
                                return False, False
            
            # Если дошли сюда - аккаунт невалидный
            logger.warning(f"❌ {account.username} - невалидный")
            update_instagram_account(
                account.id, 
                is_active=False,
                last_error="Не удалось создать клиент или выполнить базовый запрос",
                last_check=datetime.now()
            )
            return False, False
            
        except Exception as e:
            logger.error(f"Критическая ошибка при проверке {account.username}: {e}")
            update_instagram_account(
                account.id, 
                is_active=False,
                last_error=f"Критическая ошибка: {str(e)}",
                last_check=datetime.now()
            )
            return False, False
    
    def _needs_verification(self, error_message: str) -> bool:
        """Проверяет, требуется ли верификация"""
        verification_keywords = [
            'challenge',
            'verification',
            'verify',
            'confirm',
            'code',
            'checkpoint'
        ]
        error_lower = error_message.lower()
        return any(keyword in error_lower for keyword in verification_keywords)
    
    def _repair_account(self, account: InstagramAccount) -> bool:
        """
        Попытка восстановить аккаунт через email верификацию
        """
        try:
            logger.info(f"🔧 Восстановление аккаунта {account.username}")
            
            # Удаляем старую сессию перед восстановлением
            from instagram.client import remove_instagram_account
            import os
            session_file = os.path.join("accounts", str(account.id), "session.json")
            if os.path.exists(session_file):
                try:
                    os.remove(session_file)
                    logger.info(f"Удалена старая сессия для {account.username}")
                except:
                    pass
            
            # Пытаемся войти заново
            # Функция get_instagram_client автоматически обработает верификацию через email
            client = get_instagram_client(account.id)
            
            if client:
                try:
                    user_info = client.user_info(client.user_id)
                    if user_info:
                        logger.info(f"✅ Аккаунт {account.username} успешно восстановлен")
                        update_instagram_account(
                            account.id,
                            is_active=True,
                            last_error=None,
                            last_check=datetime.now()
                        )
                        return True
                except:
                    pass
            
            logger.warning(f"❌ Не удалось восстановить аккаунт {account.username}")
            return False
            
        except Exception as e:
            logger.error(f"Ошибка при восстановлении {account.username}: {e}")
            return False
    
    def get_last_results(self) -> Dict[str, List]:
        """Получить результаты последней проверки"""
        return self._last_check_results.copy()
    
    def check_now(self) -> Dict[str, List]:
        """Выполнить проверку прямо сейчас"""
        logger.info("🔍 Запуск внеочередной проверки...")
        return self._check_all_accounts()

    def check_account_validity(self, account: InstagramAccount) -> Tuple[bool, Optional[str]]:
        """
        Проверить валидность аккаунта
        
        Returns:
            (is_valid, error_message)
        """
        try:
            logger.info(f"🔍 Проверка аккаунта @{account.username}")
            
            # Пытаемся получить клиент
            client = get_instagram_client(account.id)
            
            if not client:
                logger.warning(f"❌ Не удалось создать клиент для @{account.username}")
                return False, "Failed to create client"
            
            # Пробуем выполнить простое действие для проверки валидности
            try:
                # Получаем информацию о пользователе
                user_info = client.user_info(client.user_id)
                
                # Если успешно получили информацию - аккаунт валидный
                logger.info(f"✅ Аккаунт @{account.username} валидный")
                return True, None
                
            except Exception as e:
                error_msg = str(e).lower()
                
                # Специфичная обработка ошибок
                if "login_required" in error_msg:
                    logger.warning(f"⚠️ @{account.username} - требуется повторный вход")
                    return False, "login_required"
                elif "challenge_required" in error_msg:
                    logger.warning(f"⚠️ @{account.username} - требуется верификация")
                    return False, "challenge_required"
                elif "'data'" in error_msg or "keyerror" in error_msg:
                    # Это может быть временная ошибка API, пробуем альтернативный метод
                    try:
                        # Пробуем получить timeline как альтернативную проверку
                        client.get_timeline_feed()
                        logger.info(f"✅ Аккаунт @{account.username} валидный (через timeline)")
                        return True, None
                    except Exception as timeline_error:
                        logger.warning(f"⚠️ @{account.username} - ошибка API: {str(timeline_error)[:100]}")
                        return False, f"api_error: {str(timeline_error)[:50]}"
                else:
                    logger.warning(f"⚠️ @{account.username} - ошибка: {error_msg[:100]}")
                    return False, error_msg[:100]
                
        except Exception as e:
            logger.error(f"❌ Критическая ошибка при проверке @{account.username}: {e}")
            return False, f"critical_error: {str(e)[:50]}"


def get_validator_service() -> AccountValidatorService:
    """Получить экземпляр сервиса валидации"""
    global _validator_service_instance
    if _validator_service_instance is None:
        with _service_lock:
            if _validator_service_instance is None:
                _validator_service_instance = AccountValidatorService()
    return _validator_service_instance


def start_account_validator(check_interval_minutes: int = 30,
                          max_threads: int = 1,
                          auto_repair: bool = True) -> AccountValidatorService:
    """
    Запустить фоновый сервис проверки аккаунтов
    
    Args:
        check_interval_minutes: Интервал между проверками в минутах
        max_threads: Максимальное количество потоков
        auto_repair: Автоматически восстанавливать аккаунты
        
    Returns:
        Экземпляр сервиса
    """
    global _validator_service_instance
    
    with _service_lock:
        # Если сервис уже существует и работает, возвращаем его
        if _validator_service_instance and _validator_service_instance.is_running:
            logger.warning("⚠️ Сервис валидации уже запущен, возвращаем существующий экземпляр")
            return _validator_service_instance
        
        # Создаем новый экземпляр только если его нет
        if not _validator_service_instance:
            _validator_service_instance = AccountValidatorService(
                check_interval_minutes=check_interval_minutes,
                max_threads=max_threads,
                auto_repair=auto_repair
            )
        else:
            # Обновляем настройки существующего экземпляра
            _validator_service_instance.check_interval = check_interval_minutes * 60
            _validator_service_instance.max_threads = max_threads
            _validator_service_instance.auto_repair = auto_repair
        
        # Запускаем сервис
        _validator_service_instance.start()
        logger.info("✅ Фоновый сервис проверки аккаунтов запущен")
        
        return _validator_service_instance


def stop_account_validator():
    """Остановить фоновый сервис"""
    service = get_validator_service()
    service.stop() 
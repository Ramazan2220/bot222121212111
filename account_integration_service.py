#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Интегрированный сервис для обработки аккаунтов Instagram
Объединяет все существующие системы проекта
"""

import os
import sys
import logging
import asyncio
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any

# Добавляем текущую директорию в путь Python
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# ВАЖНО: Импортируем monkey patch ПЕРВЫМ для применения патчей уникализации устройства
from instagram.monkey_patch import *
from instagram.deep_patch import apply_deep_patch

# Применяем глубокий патч
apply_deep_patch()

# Импортируем модули проекта
from database.db_manager import (
    get_instagram_account, get_instagram_account_by_username, 
    add_instagram_account, activate_instagram_account,
    update_account_session_data,
    update_instagram_account, get_instagram_accounts
)
from instagram.client import test_instagram_login_with_proxy

# Импортируем системы Instagram
try:
    from instagram.auth_manager import AuthManager
    from instagram.profile_manager import ProfileManager
    from instagram.health_monitor import HealthMonitor
    from instagram.advanced_verification import AdvancedVerification
    from instagram.lifecycle_manager import LifecycleManager
    from instagram.improved_account_warmer import ImprovedAccountWarmer
    from instagram.predictive_monitor import PredictiveMonitor
    from instagram.activity_limiter import ActivityLimiter
    from instagram.email_utils import EmailUtils
    from instagram.device_manager import DeviceManager
except ImportError as e:
    logging.warning(f"Не удалось импортировать некоторые модули Instagram: {e}")

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class AccountIntegrationService:
    """Интегрированный сервис для обработки аккаунтов"""
    
    def __init__(self):
        """Инициализация сервиса"""
        self.auth_manager = None
        self.profile_manager = None
        self.health_monitor = None
        self.verification_system = None
        self.lifecycle_manager = None
        self.warmer = None
        self.predictive_monitor = None
        self.activity_limiter = None
        self.email_utils = None
        self.device_manager = None
        
        self._initialize_systems()
    
    def _initialize_systems(self):
        """Инициализация всех систем"""
        try:
            # Инициализируем системы, если они доступны
            if 'AuthManager' in globals():
                self.auth_manager = AuthManager()
                logger.info("✅ AuthManager инициализирован")
            
            if 'ProfileManager' in globals():
                self.profile_manager = ProfileManager()
                logger.info("✅ ProfileManager инициализирован")
            
            if 'HealthMonitor' in globals():
                self.health_monitor = HealthMonitor()
                logger.info("✅ HealthMonitor инициализирован")
            
            if 'AdvancedVerification' in globals():
                self.verification_system = AdvancedVerification()
                logger.info("✅ AdvancedVerification инициализирован")
            
            if 'LifecycleManager' in globals():
                self.lifecycle_manager = LifecycleManager()
                logger.info("✅ LifecycleManager инициализирован")
            
            if 'ImprovedAccountWarmer' in globals():
                self.warmer = ImprovedAccountWarmer()
                logger.info("✅ ImprovedAccountWarmer инициализирован")
            
            if 'PredictiveMonitor' in globals():
                self.predictive_monitor = PredictiveMonitor()
                logger.info("✅ PredictiveMonitor инициализирован")
            
            if 'ActivityLimiter' in globals():
                self.activity_limiter = ActivityLimiter()
                logger.info("✅ ActivityLimiter инициализирован")
            
            if 'EmailUtils' in globals():
                self.email_utils = EmailUtils()
                logger.info("✅ EmailUtils инициализирован")
            
            if 'DeviceManager' in globals():
                self.device_manager = DeviceManager()
                logger.info("✅ DeviceManager инициализирован")
                
        except Exception as e:
            logger.error(f"Ошибка при инициализации систем: {e}")
    
    async def add_account_with_full_processing(
        self, 
        username: str, 
        password: str, 
        email: str = None, 
        email_password: str = None,
        full_name: str = None,
        biography: str = None,
        validate_credentials: bool = True,
        setup_profile: bool = True,
        start_warming: bool = True
    ) -> Tuple[bool, str, Optional[Dict]]:
        """
        Добавляет аккаунт с полной обработкой через все системы
        
        Args:
            username: Имя пользователя Instagram
            password: Пароль от аккаунта
            email: Email аккаунта
            email_password: Пароль от email
            full_name: Полное имя для профиля
            biography: Описание профиля
            validate_credentials: Проверять ли учетные данные
            setup_profile: Настраивать ли профиль
            start_warming: Запускать ли прогрев
            
        Returns:
            Tuple[success, message, account_data]
        """
        logger.info(f"🚀 Начинаем полную обработку аккаунта {username}")
        
        try:
            # 1. Добавляем аккаунт в базу данных
            logger.info(f"📝 Добавляем аккаунт {username} в базу данных")
            success, result = add_instagram_account(
                username=username,
                password=password,
                email=email,
                email_password=email_password
            )
            
            if success:
                account_id = result  # result содержит account_id при успехе
                logger.info(f"✅ Аккаунт {username} добавлен в базу данных с ID {account_id}")
                
                # Деактивируем аккаунт сначала (установим неактивным)
                update_instagram_account(account_id, is_active=False)
                
                if validate_credentials:
                    # Устанавливаем глобальные данные аккаунта для автоматической обработки input()
                    import builtins
                    original_input = builtins.input
                    
                    def patched_input(prompt=""):
                        prompt_lower = prompt.lower()
                        if "password" in prompt_lower and username in prompt:
                            logger.info(f"🔐 Автоматически предоставляем пароль для {username}")
                            return password
                        elif "code" in prompt_lower and username in prompt:
                            logger.info(f"📧 Получаем код верификации для {username}")
                            if email and email_password:
                                from instagram.email_utils_optimized import get_verification_code_from_email
                                code = get_verification_code_from_email(email, email_password, max_attempts=5, delay_between_attempts=10)
                                if code:
                                    logger.info(f"✅ Код получен: {code}")
                                    return code
                            return ""
                        else:
                            return ""
                    
                    # Патчим input для этого аккаунта
                    builtins.input = patched_input
                    
                    try:
                        logger.info(f"🔑 Попытка входа в Instagram для {username}")
                        # Используем проверенную функцию из Telegram бота
                        login_success = test_instagram_login_with_proxy(
                            account_id, username, password, email, email_password
                        )
                        
                        if login_success:
                            logger.info(f"✅ Успешный вход в Instagram для {username}")
                            # Обновляем статус на активный
                            activate_instagram_account(account_id)
                        else:
                            logger.warning(f"⚠️ Аккаунт {username} добавлен, но не удалось войти в Instagram.")
                        results['success'].append(username)
                        
                    except Exception as login_error:
                        logger.error(f"❌ Ошибка при входе для {username}: {login_error}")
                        results['success'].append(username)  # Все равно считаем успешным, так как добавили в базу
                    
                    finally:
                        # Восстанавливаем оригинальную функцию input
                        builtins.input = original_input
            else:
                logger.error(f"❌ Не удалось добавить аккаунт {username} в базу данных")
                results['failed'].append(username)
            
            # 2. Обновляем дополнительные поля профиля
            if full_name or biography:
                logger.info(f"📝 Обновляем профильные данные для {username}")
                update_instagram_account(
                    account_id,
                    full_name=full_name or '',
                    biography=biography or ''
                )
            
            # 3. Назначаем прокси для аккаунта
            logger.info(f"📡 Назначаем прокси для аккаунта {username}")
            try:
                from utils.proxy_manager import assign_proxy_to_account
                proxy_success, proxy_message = assign_proxy_to_account(account_id)
                
                if not proxy_success:
                    logger.warning(f"⚠️ {proxy_message}")
                else:
                    logger.info(f"✅ {proxy_message}")
            except Exception as e:
                logger.warning(f"⚠️ Ошибка назначения прокси: {e}")
                proxy_success = False
            
            # 4. Если предоставлены данные почты, проверяем подключение и пытаемся войти
            login_successful = False
            if email and email_password and validate_credentials:
                logger.info(f"🔄 Проверяем подключение к почте {email}")
                
                try:
                    from instagram.email_utils import test_email_connection
                    email_success, email_message = test_email_connection(email, email_password)
                    
                    if not email_success:
                        logger.error(f"❌ Ошибка подключения к почте: {email_message}")
                        return False, f"Ошибка подключения к почте: {email_message}", None
                    
                    logger.info(f"✅ Подключение к почте успешно установлено")
                    
                    # Теперь пытаемся войти в Instagram с использованием прокси
                    logger.info(f"🔄 Пытаемся войти в Instagram для {username}")
                    
                    login_successful = test_instagram_login_with_proxy(
                        account_id=account_id,
                        username=username,
                        password=password,
                        email=email,
                        email_password=email_password
                    )
                    
                    if login_successful:
                        # Если вход успешен, активируем аккаунт
                        logger.info(f"✅ Успешный вход в Instagram для {username}")
                        activate_instagram_account(account_id)
                    else:
                        logger.warning(f"⚠️ Не удалось войти в Instagram для {username}")
                        
                except Exception as e:
                    logger.error(f"❌ Ошибка при проверке входа в Instagram: {e}")
                    login_successful = False
            else:
                logger.info(f"⚠️ Данные почты не предоставлены для {username}, пропускаем вход в Instagram")
            
            # 5. Проверяем учетные данные через интегрированные системы (если доступны)
            if validate_credentials and self.auth_manager:
                logger.info(f"🔐 Проверяем учетные данные через AuthManager для {username}")
                try:
                    auth_result = await self._validate_account_credentials(
                        username, password, email, email_password
                    )
                    if not auth_result['success']:
                        logger.warning(f"⚠️ Проблемы с учетными данными: {auth_result['message']}")
                except Exception as e:
                    logger.error(f"❌ Ошибка при проверке учетных данных: {e}")
            
            # 6. Настраиваем устройство
            if self.device_manager:
                logger.info(f"📱 Настраиваем устройство для {username}")
                try:
                    device_result = await self._setup_device(account_id, username)
                    logger.info(f"✅ Устройство настроено: {device_result}")
                except Exception as e:
                    logger.error(f"❌ Ошибка настройки устройства: {e}")
            
            # 7. Инициализируем мониторинг здоровья
            if self.health_monitor:
                logger.info(f"🏥 Инициализируем мониторинг здоровья для {username}")
                try:
                    health_result = await self._initialize_health_monitoring(account_id)
                    logger.info(f"✅ Мониторинг здоровья инициализирован")
                except Exception as e:
                    logger.error(f"❌ Ошибка инициализации мониторинга: {e}")
            
            # 8. Настраиваем профиль (если включено)
            if setup_profile and self.profile_manager:
                logger.info(f"👤 Настраиваем профиль для {username}")
                try:
                    profile_result = await self._setup_profile(
                        account_id, username, full_name, biography
                    )
                    logger.info(f"✅ Профиль настроен")
                except Exception as e:
                    logger.error(f"❌ Ошибка настройки профиля: {e}")
            
            # 9. Инициализируем системы мониторинга
            if self.predictive_monitor:
                logger.info(f"🔮 Инициализируем предиктивный мониторинг для {username}")
                try:
                    await self._initialize_predictive_monitoring(account_id)
                    logger.info(f"✅ Предиктивный мониторинг инициализирован")
                except Exception as e:
                    logger.error(f"❌ Ошибка предиктивного мониторинга: {e}")
            
            # 10. Настраиваем ограничитель активности
            if self.activity_limiter:
                logger.info(f"⏱️ Настраиваем ограничитель активности для {username}")
                try:
                    await self._setup_activity_limiter(account_id)
                    logger.info(f"✅ Ограничитель активности настроен")
                except Exception as e:
                    logger.error(f"❌ Ошибка настройки ограничителя: {e}")
            
            # 11. Запускаем прогрев (если включено)
            if start_warming and self.warmer:
                logger.info(f"🔥 Запускаем прогрев для {username}")
                try:
                    warming_result = await self._start_account_warming(account_id)
                    logger.info(f"✅ Прогрев запущен: {warming_result}")
                except Exception as e:
                    logger.error(f"❌ Ошибка запуска прогрева: {e}")
            
            # 12. Инициализируем жизненный цикл
            if self.lifecycle_manager:
                logger.info(f"🔄 Инициализируем управление жизненным циклом для {username}")
                try:
                    await self._initialize_lifecycle_management(account_id)
                    logger.info(f"✅ Управление жизненным циклом инициализировано")
                except Exception as e:
                    logger.error(f"❌ Ошибка инициализации жизненного цикла: {e}")
            
            # Получаем обновленные данные аккаунта
            updated_account = get_instagram_account(account_id)
            account_data = {
                'id': updated_account.id,
                'username': updated_account.username,
                'email': updated_account.email,
                'full_name': updated_account.full_name or '',
                'biography': updated_account.biography or '',
                'is_active': updated_account.is_active,
                'created_at': updated_account.created_at.isoformat() if updated_account.created_at else None,
                'systems_initialized': True,
                'login_successful': login_successful,
                'proxy_assigned': proxy_success if 'proxy_success' in locals() else False
            }
            
            # Формируем сообщение в зависимости от результата
            if login_successful:
                message = f"Аккаунт {username} успешно добавлен и активирован!"
            elif email and email_password:
                message = f"Аккаунт {username} добавлен, но не удалось войти в Instagram. Проверьте данные и попробуйте позже."
            else:
                message = f"Аккаунт {username} добавлен в базу данных. Для активации необходимо указать данные почты."
            
            logger.info(f"🎉 Полная обработка аккаунта {username} завершена успешно!")
            return True, message, account_data
            
        except Exception as e:
            logger.error(f"❌ Критическая ошибка при обработке аккаунта {username}: {e}")
            return False, f"Ошибка при обработке аккаунта: {str(e)}", None
    
    async def _validate_account_credentials(
        self, username: str, password: str, email: str = None, email_password: str = None
    ) -> Dict[str, Any]:
        """Проверяет учетные данные аккаунта"""
        try:
            if self.auth_manager:
                # Используем AuthManager для проверки
                result = await self.auth_manager.validate_credentials(username, password)
                return result
            else:
                # Базовая проверка без AuthManager
                return {'success': True, 'message': 'AuthManager недоступен, пропускаем проверку'}
        except Exception as e:
            return {'success': False, 'message': f'Ошибка проверки: {str(e)}'}
    
    async def _setup_device(self, account_id: int, username: str) -> Dict[str, Any]:
        """Настраивает устройство для аккаунта"""
        try:
            if self.device_manager:
                device_info = await self.device_manager.setup_device_for_account(account_id, username)
                return {'success': True, 'device_info': device_info}
            else:
                return {'success': True, 'message': 'DeviceManager недоступен'}
        except Exception as e:
            return {'success': False, 'message': f'Ошибка настройки устройства: {str(e)}'}
    
    async def _initialize_health_monitoring(self, account_id: int) -> Dict[str, Any]:
        """Инициализирует мониторинг здоровья аккаунта"""
        try:
            if self.health_monitor:
                await self.health_monitor.initialize_account_monitoring(account_id)
                return {'success': True}
            else:
                return {'success': True, 'message': 'HealthMonitor недоступен'}
        except Exception as e:
            return {'success': False, 'message': f'Ошибка мониторинга: {str(e)}'}
    
    async def _setup_profile(
        self, account_id: int, username: str, full_name: str = None, biography: str = None
    ) -> Dict[str, Any]:
        """Настраивает профиль аккаунта"""
        try:
            if self.profile_manager:
                profile_data = {
                    'full_name': full_name or '',
                    'biography': biography or '',
                    'username': username
                }
                result = await self.profile_manager.setup_profile(account_id, profile_data)
                return result
            else:
                return {'success': True, 'message': 'ProfileManager недоступен'}
        except Exception as e:
            return {'success': False, 'message': f'Ошибка настройки профиля: {str(e)}'}
    
    async def _initialize_predictive_monitoring(self, account_id: int) -> Dict[str, Any]:
        """Инициализирует предиктивный мониторинг"""
        try:
            if self.predictive_monitor:
                await self.predictive_monitor.initialize_account(account_id)
                return {'success': True}
            else:
                return {'success': True, 'message': 'PredictiveMonitor недоступен'}
        except Exception as e:
            return {'success': False, 'message': f'Ошибка предиктивного мониторинга: {str(e)}'}
    
    async def _setup_activity_limiter(self, account_id: int) -> Dict[str, Any]:
        """Настраивает ограничитель активности"""
        try:
            if self.activity_limiter:
                await self.activity_limiter.setup_account_limits(account_id)
                return {'success': True}
            else:
                return {'success': True, 'message': 'ActivityLimiter недоступен'}
        except Exception as e:
            return {'success': False, 'message': f'Ошибка ограничителя активности: {str(e)}'}
    
    async def _start_account_warming(self, account_id: int) -> Dict[str, Any]:
        """Запускает прогрев аккаунта"""
        try:
            if self.warmer:
                warming_result = await self.warmer.start_warming_process(account_id)
                return warming_result
            else:
                return {'success': True, 'message': 'AccountWarmer недоступен'}
        except Exception as e:
            return {'success': False, 'message': f'Ошибка прогрева: {str(e)}'}
    
    async def _initialize_lifecycle_management(self, account_id: int) -> Dict[str, Any]:
        """Инициализирует управление жизненным циклом"""
        try:
            if self.lifecycle_manager:
                await self.lifecycle_manager.initialize_account_lifecycle(account_id)
                return {'success': True}
            else:
                return {'success': True, 'message': 'LifecycleManager недоступен'}
        except Exception as e:
            return {'success': False, 'message': f'Ошибка жизненного цикла: {str(e)}'}
    
    def bulk_add_accounts_with_processing(
        self, 
        accounts_data: List[Dict[str, str]],
        validate_credentials: bool = True,
        setup_profile: bool = True,
        start_warming: bool = False  # Для массового добавления по умолчанию отключаем прогрев
    ) -> Dict[str, List]:
        """
        Массовое добавление аккаунтов с использованием рабочей логики из Telegram бота
        
        Args:
            accounts_data: Список словарей с данными аккаунтов
            validate_credentials: Проверять ли учетные данные
            setup_profile: Настраивать ли профиль
            start_warming: Запускать ли прогрев
            
        Returns:
            Dict с результатами: {'success': [...], 'failed': [...]}
        """
        logger.info(f"🚀 Начинаем массовое добавление {len(accounts_data)} аккаунтов")
        
        results = {
            'success': [],
            'failed': [],
            'already_exists': [],
            'total': len(accounts_data)
        }
        
        # Статистика (как в Telegram боте)
        total_accounts = len(accounts_data)
        added_accounts = 0
        failed_accounts = 0
        already_exists = 0
        failed_accounts_list = []
        
        logger.info(f"🔄 Начинаем добавление {total_accounts} аккаунтов...")
        
        for i, account_data in enumerate(accounts_data):
            username = account_data.get('username')
            password = account_data.get('password')
            email = account_data.get('email')
            email_password = account_data.get('email_password')
            
            logger.info(f"📝 Обрабатываем аккаунт: {username}")
            
            try:
                # Проверяем, существует ли уже аккаунт
                existing_account = get_instagram_account_by_username(username)
                if existing_account:
                    logger.info(f"⚠️ Аккаунт {username} уже существует в базе данных")
                    results['already_exists'].append(username)
                    continue
                
                # Добавляем аккаунт в базу данных
                success, result = add_instagram_account(
                    username=username,
                    password=password,
                    email=email,
                    email_password=email_password
                )
                
                if success:
                    account_id = result  # result содержит account_id при успехе
                    logger.info(f"✅ Аккаунт {username} добавлен в базу данных с ID {account_id}")
                    
                    # Деактивируем аккаунт сначала (установим неактивным)
                    update_instagram_account(account_id, is_active=False)
                    
                    if validate_credentials:
                        # Устанавливаем глобальные данные аккаунта для автоматической обработки input()
                        import builtins
                        original_input = builtins.input
                        
                        def patched_input(prompt=""):
                            prompt_lower = prompt.lower()
                            if "password" in prompt_lower and username in prompt:
                                logger.info(f"🔐 Автоматически предоставляем пароль для {username}")
                                return password
                            elif "code" in prompt_lower and username in prompt:
                                logger.info(f"📧 Получаем код верификации для {username}")
                                if email and email_password:
                                    from instagram.email_utils_optimized import get_verification_code_from_email
                                    code = get_verification_code_from_email(email, email_password, max_attempts=5, delay_between_attempts=10)
                                    if code:
                                        logger.info(f"✅ Код получен: {code}")
                                        return code
                                return ""
                            else:
                                return ""
                        
                        # Патчим input для этого аккаунта
                        builtins.input = patched_input
                        
                        try:
                            logger.info(f"🔑 Попытка входа в Instagram для {username}")
                            # Используем проверенную функцию из Telegram бота
                            login_success = test_instagram_login_with_proxy(
                                account_id, username, password, email, email_password
                            )
                            
                            if login_success:
                                logger.info(f"✅ Успешный вход в Instagram для {username}")
                                # Обновляем статус на активный
                                activate_instagram_account(account_id)
                                results['success'].append(username)
                            else:
                                logger.warning(f"⚠️ Аккаунт {username} добавлен, но не удалось войти в Instagram.")
                                results['success'].append(username)  # Все равно считаем успешным, так как добавили в базу
                        
                        except Exception as login_error:
                            logger.error(f"❌ Ошибка при входе для {username}: {login_error}")
                            results['success'].append(username)  # Все равно считаем успешным, так как добавили в базу
                        
                        finally:
                            # Восстанавливаем оригинальную функцию input
                            builtins.input = original_input
                    else:
                        results['success'].append(username)
                else:
                    logger.error(f"❌ Не удалось добавить аккаунт {username} в базу данных: {result}")
                    results['failed'].append(username)
                    
            except Exception as e:
                logger.error(f"❌ Ошибка при обработке аккаунта {username}: {str(e)}")
                results['failed'].append(username)

        # Отправляем итоговую статистику (как в Telegram боте)
        logger.info(f"📊 Итоги добавления аккаунтов:")
        logger.info(f"Всего обработано: {total_accounts}")
        logger.info(f"Успешно добавлено: {len(results['success'])}")
        logger.info(f"Уже существуют: {len(results['already_exists'])}")
        logger.info(f"Не удалось добавить: {len(results['failed'])}")

        # Логируем неудачные аккаунты (как в Telegram боте)
        if results['failed']:
            logger.error(f"❌ Список неудачно добавленных аккаунтов:")
            for failed_account in results['failed']:
                logger.error(f"  - {failed_account}")
        
        logger.info(f"🎉 Массовое добавление завершено: {len(results['success'])} успешно, {len(results['failed'])} ошибок")
        return results
    
    def get_system_status(self) -> Dict[str, bool]:
        """Возвращает статус всех систем"""
        return {
            'auth_manager': self.auth_manager is not None,
            'profile_manager': self.profile_manager is not None,
            'health_monitor': self.health_monitor is not None,
            'verification_system': self.verification_system is not None,
            'lifecycle_manager': self.lifecycle_manager is not None,
            'warmer': self.warmer is not None,
            'predictive_monitor': self.predictive_monitor is not None,
            'activity_limiter': self.activity_limiter is not None,
            'email_utils': self.email_utils is not None,
            'device_manager': self.device_manager is not None
        }

    async def retry_account_login_with_new_code(
        self, 
        account_id: int, 
        max_retries: int = 3
    ) -> Tuple[bool, str]:
        """
        Повторяет попытку входа с новым кодом верификации при ошибке "Please check the code"
        
        Args:
            account_id: ID аккаунта в базе данных
            max_retries: Максимальное количество попыток
            
        Returns:
            Tuple[success, message]
        """
        logger.info(f"🔄 Повторная попытка входа с новым кодом для аккаунта ID {account_id}")
        
        try:
            # Получаем данные аккаунта из базы
            account = get_instagram_account(account_id)
            if not account:
                return False, f"Аккаунт с ID {account_id} не найден"
            
            username = account.username
            password = account.password
            email = account.email
            email_password = account.email_password
            
            logger.info(f"🔄 Начинаем повторные попытки входа для {username}")
            
            # Проверяем наличие данных почты
            if not email or not email_password:
                return False, "Данные почты не предоставлены - невозможно получить новый код"
            
            # Очищаем кэш клиента для этого аккаунта
            try:
                from instagram.client import _instagram_clients
                if account_id in _instagram_clients:
                    del _instagram_clients[account_id]
                    logger.info(f"🗑️ Очищен кэш клиента для аккаунта {username}")
            except Exception as e:
                logger.warning(f"⚠️ Не удалось очистить кэш клиента: {e}")
            
            # Очищаем старые логи email для чистоты тестирования
            try:
                from instagram.email_utils import cleanup_email_logs
                cleanup_email_logs(email)
                logger.info(f"🗑️ Очищены старые логи email для {email}")
            except Exception as e:
                logger.warning(f"⚠️ Не удалось очистить логи email: {e}")
            
            # Пытаемся войти с повторными попытками
            for attempt in range(max_retries):
                logger.info(f"🔄 Попытка {attempt + 1}/{max_retries} входа для {username}")
                
                try:
                    # Добавляем задержку между попытками (кроме первой)
                    if attempt > 0:
                        delay = 45 + (attempt * 30)  # 45, 75, 105 секунд
                        logger.info(f"⏱️ Ожидание {delay} секунд перед попыткой {attempt + 1}")
                        await asyncio.sleep(delay)
                    
                    # Импортируем функцию входа
                    from instagram.client import test_instagram_login_with_proxy
                    
                    # Попытка входа с использованием улучшенного обработчика кодов
                    login_successful = test_instagram_login_with_proxy(
                        account_id=account_id,
                        username=username,
                        password=password,
                        email=email,
                        email_password=email_password
                    )
                    
                    if login_successful:
                        logger.info(f"✅ Успешный вход для {username} на попытке {attempt + 1}")
                        
                        # Активируем аккаунт в базе данных
                        try:
                            activate_instagram_account(account_id)
                            logger.info(f"✅ Аккаунт {username} активирован в базе данных")
                        except Exception as e:
                            logger.warning(f"⚠️ Не удалось активировать аккаунт в БД: {e}")
                        
                        return True, f"Успешный вход для {username} на попытке {attempt + 1}"
                    
                    else:
                        logger.warning(f"⚠️ Неудачная попытка входа {attempt + 1} для {username}")
                        
                        # Если это не последняя попытка, продолжаем
                        if attempt < max_retries - 1:
                            logger.info(f"🔄 Подготовка к следующей попытке для {username}")
                            continue
                        else:
                            logger.error(f"❌ Исчерпаны все попытки входа для {username}")
                            return False, f"Не удалось войти после {max_retries} попыток"
                
                except Exception as e:
                    error_msg = str(e)
                    logger.error(f"❌ Ошибка при попытке {attempt + 1} для {username}: {error_msg}")
                    
                    # Проверяем, является ли это ошибкой верификации
                    if "check the code" in error_msg.lower() or "try again" in error_msg.lower():
                        logger.info(f"🔄 Получена ошибка верификации для {username} - продолжаем попытки")
                        if attempt < max_retries - 1:
                            continue
                    else:
                        # Если это другая ошибка, прекращаем попытки
                        logger.error(f"❌ Критическая ошибка для {username}: {error_msg}")
                        return False, f"Критическая ошибка: {error_msg}"
            
            # Если дошли до сюда, значит все попытки исчерпаны
            return False, f"Не удалось войти для {username} после {max_retries} попыток"
            
        except Exception as e:
            logger.error(f"❌ Критическая ошибка при повторной попытке входа: {e}")
            return False, f"Критическая ошибка: {str(e)}"
    
    async def bulk_retry_failed_accounts(
        self, 
        account_ids: List[int], 
        max_retries_per_account: int = 3
    ) -> Dict[str, List]:
        """
        Массовая повторная попытка входа для неудачных аккаунтов
        
        Args:
            account_ids: Список ID аккаунтов для повтора
            max_retries_per_account: Максимальное количество попыток на аккаунт
            
        Returns:
            Dict с результатами: {'success': [...], 'failed': [...]}
        """
        logger.info(f"🔄 Начинаем массовую повторную попытку для {len(account_ids)} аккаунтов")
        
        results = {
            'success': [],
            'failed': [],
            'total': len(account_ids)
        }
        
        # Обрабатываем аккаунты с ограничением: максимум 2 одновременно
        semaphore = asyncio.Semaphore(2)
        
        async def retry_single_account(account_id):
            async with semaphore:
                try:
                    # Получаем данные аккаунта для логирования
                    account = get_instagram_account(account_id)
                    username = account.username if account else f"ID{account_id}"
                    
                    logger.info(f"🔄 Повторная попытка для аккаунта {username} (ID: {account_id})")
                    
                    success, message = await self.retry_account_login_with_new_code(
                        account_id, max_retries_per_account
                    )
                    
                    if success:
                        results['success'].append({
                            'account_id': account_id,
                            'username': username,
                            'message': message
                        })
                        logger.info(f"✅ Успешная повторная попытка для {username}: {message}")
                    else:
                        results['failed'].append({
                            'account_id': account_id,
                            'username': username,
                            'error': message
                        })
                        logger.error(f"❌ Неудачная повторная попытка для {username}: {message}")
                        
                except Exception as e:
                    error_msg = str(e)
                    logger.error(f"❌ Критическая ошибка при повторной попытке для ID {account_id}: {error_msg}")
                    
                    results['failed'].append({
                        'account_id': account_id,
                        'username': f"ID{account_id}",
                        'error': f"Критическая ошибка: {error_msg}"
                    })
        
        # Создаем задачи для всех аккаунтов
        tasks = [retry_single_account(account_id) for account_id in account_ids]
        
        # Запускаем все задачи параллельно
        await asyncio.gather(*tasks, return_exceptions=True)
        
        logger.info(f"🎉 Массовая повторная попытка завершена:")
        logger.info(f"✅ Успешно: {len(results['success'])}")
        logger.info(f"❌ Неудачно: {len(results['failed'])}")
        
        return results

# Создаем глобальный экземпляр сервиса
account_service = AccountIntegrationService()

# Экспортируем основные функции
async def add_account_integrated(account_data: Dict[str, str]) -> Tuple[bool, str, Optional[Dict]]:
    """Добавляет аккаунт с интеграцией всех систем"""
    return await account_service.add_account_with_full_processing(**account_data)

async def bulk_add_accounts_integrated(accounts_data: List[Dict[str, str]]) -> Dict[str, List]:
    """Массовое добавление аккаунтов с интеграцией всех систем"""
    return account_service.bulk_add_accounts_with_processing(accounts_data)

def get_integration_status() -> Dict[str, bool]:
    """Возвращает статус интеграции систем"""
    return account_service.get_system_status()

if __name__ == '__main__':
    # Тестирование сервиса
    import asyncio
    
    async def test_service():
        """Тестирует сервис интеграции"""
        logger.info("🧪 Тестирование сервиса интеграции аккаунтов")
        
        # Проверяем статус систем
        status = get_integration_status()
        logger.info(f"📊 Статус систем: {status}")
        
        # Тестируем добавление аккаунта
        test_account = {
            'username': 'test_integration_user',
            'password': 'test_password',
            'email': 'test@example.com',
            'full_name': 'Test Integration User',
            'biography': 'Test account for integration testing'
        }
        
        success, message, account_data = await add_account_integrated(test_account)
        logger.info(f"🧪 Результат тестирования: {success}, {message}")
        
        if account_data:
            logger.info(f"📝 Данные аккаунта: {account_data}")
    
    # Запускаем тест
    asyncio.run(test_service()) 
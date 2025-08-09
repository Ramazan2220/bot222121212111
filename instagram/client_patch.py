from instagrapi import Client
from instagrapi.mixins.private import PrivateRequestMixin
import logging
import builtins
import re
from instagram.email_utils import get_verification_code_from_email
from database.db_manager import get_instagram_account

logger = logging.getLogger(__name__)

# Сохраняем оригинальный метод
original_send_private_request = PrivateRequestMixin._send_private_request

# Создаем усиленный патч для метода _send_private_request
def patched_send_private_request(self, *args, **kwargs):
    """
    Усиленный патч для метода _send_private_request, который принудительно
    устанавливает правильный User-Agent из настроек устройства
    """
    # Принудительно устанавливаем User-Agent из настроек
    if hasattr(self, "settings") and "user_agent" in self.settings:
        custom_user_agent = self.settings["user_agent"]
        
        # Устанавливаем в разных местах для надежности
        self.user_agent = custom_user_agent
        
        # Устанавливаем в заголовки сессии
        if hasattr(self, 'private') and hasattr(self.private, 'headers'):
            self.private.headers['User-Agent'] = custom_user_agent
            
        # Устанавливаем в заголовки запроса
        if len(args) > 0 and hasattr(args[0], 'headers'):
            args[0].headers['User-Agent'] = custom_user_agent
            
        logger.info(f"🔧 ПАТЧ: Принудительно установлен User-Agent: {custom_user_agent}")
        
        # Извлекаем имя устройства для логов
        if "device_name" in self.settings:
            logger.info(f"📱 УСТРОЙСТВО: {self.settings['device_name']}")

    # Вызываем оригинальный метод
    return original_send_private_request(self, *args, **kwargs)

# Применяем патч
PrivateRequestMixin._send_private_request = patched_send_private_request

# Патч для метода set_settings
original_set_settings = Client.set_settings

def patched_set_settings(self, settings):
    """
    Усиленный патч для метода set_settings, который принудительно
    устанавливает правильный User-Agent из настроек устройства
    """
    # Вызываем оригинальный метод
    result = original_set_settings(self, settings)

    # Принудительно устанавливаем User-Agent
    if "user_agent" in settings:
        custom_user_agent = settings["user_agent"]
        
        # Устанавливаем в разных местах для надежности
        self.user_agent = custom_user_agent
        
        # Устанавливаем в заголовки сессии
        if hasattr(self, 'private') and hasattr(self.private, 'headers'):
            self.private.headers['User-Agent'] = custom_user_agent
            
        # Также устанавливаем в настройки сессии
        if hasattr(self, 'private'):
            self.private.user_agent = custom_user_agent
            
        logger.info(f"🔧 SET_SETTINGS: Принудительно установлен User-Agent: {custom_user_agent}")
        
        # Логируем имя устройства
        if "device_name" in settings:
            logger.info(f"📱 ПРИМЕНЕНО УСТРОЙСТВО: {settings['device_name']}")

    return result

# Применяем патч
Client.set_settings = patched_set_settings

# Дополнительный патч для метода login
original_login = Client.login

def patched_login(self, username, password=None, relogin=False):
    """
    Усиленный патч для метода login, который принудительно
    устанавливает правильный User-Agent перед логином
    """
    # Принудительно применяем User-Agent перед логином
    if hasattr(self, "settings") and "user_agent" in self.settings:
        custom_user_agent = self.settings["user_agent"]
        self.user_agent = custom_user_agent
        
        # Устанавливаем в заголовки сессии
        if hasattr(self, 'private') and hasattr(self.private, 'headers'):
            self.private.headers['User-Agent'] = custom_user_agent
            
        logger.info(f"🔧 LOGIN_PATCH: Установлен User-Agent перед логином: {custom_user_agent}")
        
        # Логируем устройство
        if "device_name" in self.settings:
            logger.info(f"📱 ЛОГИН С УСТРОЙСТВОМ: {self.settings['device_name']}")

    # Вызываем оригинальный метод login
    return original_login(self, username, password, relogin)

# Применяем патч для login
Client.login = patched_login

# Патч для метода __init__ клиента
original_init = Client.__init__

def patched_init(self, settings=None, proxy=None, delay_range=None, **kwargs):
    """
    Патч для метода __init__, который устанавливает правильный User-Agent
    """
    # Если settings не переданы или равны None, используем пустой словарь
    if settings is None:
        settings = {}
    
    # Вызываем оригинальный метод
    result = original_init(self, settings, proxy, delay_range, **kwargs)
    
    # Принудительно проверяем что self.settings не None после инициализации
    if not hasattr(self, 'settings') or self.settings is None:
        self.settings = {}
        logger.warning("🔧 INIT_PATCH: Принудительно установлен пустой словарь settings")
    
    # Если переданы настройки, принудительно применяем User-Agent
    if settings and "user_agent" in settings:
        custom_user_agent = settings["user_agent"]
        self.user_agent = custom_user_agent
        
        # Устанавливаем в заголовки сессии
        if hasattr(self, 'private') and hasattr(self.private, 'headers'):
            self.private.headers['User-Agent'] = custom_user_agent
            
        logger.info(f"🔧 INIT_PATCH: Установлен User-Agent в конструкторе: {custom_user_agent}")
    
    return result

# Применяем патч для __init__
Client.__init__ = patched_init

logger.info("🚀 Усиленные патчи для instagrapi успешно применены")

# Сохраняем оригинальную функцию input
_original_input = builtins.input

# Флаг для определения, работаем ли мы в веб-версии
_is_web_mode = False

# Кэш для хранения данных текущих аккаунтов
_current_accounts_cache = {}

# Счетчик попыток для предотвращения бесконечных циклов
_password_attempts = {}

def set_web_mode(enabled=True):
    """Включает или выключает веб-режим"""
    global _is_web_mode
    _is_web_mode = enabled
    if enabled:
        logger.info("✅ Включен веб-режим - автоматическая обработка всех запросов input()")
        apply_input_patch()
    else:
        logger.info("❌ Выключен веб-режим")
        remove_input_patch()

def add_account_to_cache(account_id, username, email=None, email_password=None):
    """Добавляет аккаунт в кэш для автоматической обработки"""
    _current_accounts_cache[username] = {
        'account_id': account_id,
        'email': email,
        'email_password': email_password
    }
    # Сбрасываем счетчик попыток при добавлении аккаунта
    global _password_attempts
    if username in _password_attempts:
        _password_attempts[username] = 0
    logger.info(f"Аккаунт {username} добавлен в кэш для автоматической обработки")

def patched_input(prompt=""):
    """Патченная функция input для веб-версии"""
    global _current_accounts_cache, _password_attempts
    
    if not _is_web_mode:
        return _original_input(prompt)
    
    prompt_str = str(prompt)
    logger.info(f"🔍 Перехвачен запрос input(): {prompt_str}")
    
    # Логируем текущее состояние кэша
    logger.debug(f"📦 Текущий кэш аккаунтов: {list(_current_accounts_cache.keys())}")
    logger.debug(f"🔢 Счетчики попыток паролей: {_password_attempts}")
    
    # Проверяем, это запрос кода верификации
    # Форматы:
    # "Enter code (6 digits) for username (ChallengeChoice.EMAIL):"
    # "Enter code (6 digits) for username (EMAIL):"
    # Используем более гибкое регулярное выражение
    code_patterns = [
        r"Enter code.*?for\s+(\w+)\s*\(.*?EMAIL.*?\)",
        r"Enter code.*?for\s+(\w+)",
        r"code.*?digits.*?for\s+(\w+)",
        r"verification.*?code.*?(\w+)"
    ]
    
    username = None
    for pattern in code_patterns:
        match = re.search(pattern, prompt_str, re.IGNORECASE)
        if match:
            username = match.group(1)
            break
    
    if username:
        logger.info(f"📧 Обнаружен запрос кода верификации для {username}")
        
        # Ищем данные аккаунта в кэше
        if username in _current_accounts_cache:
            account_data = _current_accounts_cache[username]
            email = account_data.get('email')
            email_password = account_data.get('email_password')
            
            if email and email_password:
                logger.info(f"🔑 Получаем код из email {email} для {username}")
                
                # Получаем код из почты
                code = get_verification_code_from_email(
                    email, 
                    email_password, 
                    max_attempts=5, 
                    delay_between_attempts=10
                )
                
                if code:
                    logger.info(f"✅ Код получен: {code}")
                    return code
                else:
                    logger.warning(f"❌ Не удалось получить код из email для {username}")
                    return ""
            else:
                logger.warning(f"⚠️ Email данные не найдены для {username}")
                return ""
        else:
            logger.warning(f"⚠️ Аккаунт {username} не найден в кэше")
            # Пытаемся найти по частичному совпадению
            for cached_username in _current_accounts_cache:
                if cached_username.lower() in username.lower() or username.lower() in cached_username.lower():
                    logger.info(f"🔍 Найдено частичное совпадение: {cached_username}")
                    account_data = _current_accounts_cache[cached_username]
                    email = account_data.get('email')
                    email_password = account_data.get('email_password')
                    
                    if email and email_password:
                        code = get_verification_code_from_email(email, email_password, max_attempts=5, delay_between_attempts=10)
                        if code:
                            logger.info(f"✅ Код получен: {code}")
                            return code
            
            return ""
    
    # Проверяем, это запрос пароля
    password_patterns = [
        r"password.*?for\s+(\w+)",
        r"enter.*?password.*?(\w+)"
    ]
    
    for pattern in password_patterns:
        match = re.search(pattern, prompt_str, re.IGNORECASE)
        if match:
            username = match.group(1)
            
            # Проверяем количество попыток
            if username not in _password_attempts:
                _password_attempts[username] = 0
            
            _password_attempts[username] += 1
            
            if _password_attempts[username] > 3:
                logger.error(f"❌ Превышено количество попыток запроса пароля для {username}. Прерываем цикл.")
                # Сбрасываем счетчик
                _password_attempts[username] = 0
                # Возвращаем специальное значение для прерывания
                raise KeyboardInterrupt(f"Слишком много попыток запроса пароля для {username}")
            
            logger.info(f"🔐 Обнаружен запрос пароля для {username} (попытка {_password_attempts[username]}/3)")
            
            # Пытаемся получить пароль из БД
            try:
                # Сначала ищем в кэше
                if username in _current_accounts_cache:
                    account_id = _current_accounts_cache[username].get('account_id')
                    if account_id:
                        from database.db_manager import get_instagram_account as get_account_by_id
                        account = get_account_by_id(account_id)
                        if account and account.password:
                            logger.info(f"✅ Пароль найден в БД для {username} (ID: {account_id})")
                            return account.password
                
                # Если не нашли в кэше, пробуем найти по username
                from database.db_manager import get_session
                from database.models import InstagramAccount
                session = get_session()
                account = session.query(InstagramAccount).filter_by(username=username).first()
                session.close()
                
                if account and account.password:
                    logger.info(f"✅ Пароль найден в БД для {username}")
                    return account.password
            except Exception as e:
                logger.error(f"Ошибка при получении пароля из БД: {e}")
            
            # В веб-версии не можем запрашивать пароль, возвращаем пустую строку
            logger.warning(f"⚠️ Не удалось получить пароль для {username}")
            return ""
    
    # Для всех остальных запросов
    logger.warning(f"⚠️ Неопознанный запрос input(): {prompt_str}")
    return ""

def reset_password_attempts():
    """Сбрасывает все счетчики попыток паролей"""
    global _password_attempts
    _password_attempts = {}
    logger.info("🔄 Счетчики попыток паролей сброшены")

def apply_input_patch():
    """Применяет патч для функции input"""
    builtins.input = patched_input
    logger.info("✅ Патч для input() применен")

def remove_input_patch():
    """Удаляет патч для функции input"""
    builtins.input = _original_input
    logger.info("❌ Патч для input() удален")

# Автоматически включаем веб-режим при импорте из web_api.py
import sys
import os

# Проверяем, запущен ли web_api.py
if any('web_api.py' in arg for arg in sys.argv) or 'web_api' in sys.modules:
    set_web_mode(True)
    logger.info("🌐 Обнаружен запуск web_api.py - автоматически включен веб-режим")

def patch_public_graphql_request(client):
    """Патчим public_graphql_request для обработки ошибок с отсутствующим data"""
    # Проверяем, не пропатчен ли уже метод
    if hasattr(client.public_graphql_request, '_is_patched'):
        return
        
    original_method = client.public_graphql_request
    
    def patched_public_graphql_request(*args, **kwargs):
        try:
            # Вызываем оригинальный метод
            return original_method(*args, **kwargs)
        except KeyError as e:
            if str(e) == "'data'":
                logger.warning("🔧 ПАТЧ: Instagram вернул ответ без поля 'data', вероятно прокси заблокирован")
                # Возвращаем пустой результат вместо ошибки
                if paginate:
                    return {"edges": [], "page_info": {"has_next_page": False, "end_cursor": None}}
                return {"edges": []}
            raise
        except Exception as e:
            logger.error(f"🔧 ПАТЧ: Ошибка в public_graphql_request: {e}")
            # Для других ошибок также возвращаем пустой результат
            if paginate:
                return {"edges": [], "page_info": {"has_next_page": False, "end_cursor": None}}
            return {"edges": []}
    
    # Помечаем как пропатченный
    patched_public_graphql_request._is_patched = True
    
    # Просто заменяем метод без лишних биндингов
    client.public_graphql_request = patched_public_graphql_request
    logger.info("🔧 Пропатчен public_graphql_request для обработки ошибок")

def apply_all_patches(client, device_info=None):
    """Применяет все патчи к клиенту Instagram"""
    # Применяем существующие патчи
    patch_user_agent(client, device_info)
    patch_headers(client)
    
    # Применяем новый патч для public_graphql_request
    patch_public_graphql_request(client)
    
    return client
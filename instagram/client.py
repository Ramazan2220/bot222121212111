import os
import json
import logging
import time
import random
from pathlib import Path
from .custom_client import CustomClient as Client
from instagrapi.exceptions import LoginRequired, BadPassword, ChallengeRequired
from .email_utils import get_verification_code_from_email, cleanup_email_logs
from config import ACCOUNTS_DIR
from database.db_manager import get_instagram_account, update_account_session_data, get_proxy_for_account, get_instagram_account_by_username
from device_manager import generate_device_settings, get_or_create_device_settings
from .client_patch import *
from utils.rotating_proxy_manager import get_rotating_proxy_url
#from instagram.clip_upload_patch import *
import builtins
import sys
import threading
from unittest.mock import patch

logger = logging.getLogger(__name__)

# Глобальные переменные для хранения текущих данных аккаунта
_current_username = None
_current_password = None
_current_email = None
_current_email_password = None

# Кэш для хранения клиентов Instagram
_instagram_clients = {}

# Блокировки для каждого аккаунта
_account_locks = {}
_lock_creation_lock = threading.Lock()

def get_account_lock(account_id):
    """Получить блокировку для конкретного аккаунта"""
    global _account_locks
    if account_id not in _account_locks:
        with _lock_creation_lock:
            if account_id not in _account_locks:
                _account_locks[account_id] = threading.Lock()
    return _account_locks[account_id]

# Данные текущего аккаунта (для обратной совместимости)
current_account_data = {}

def set_current_account_data(username, password, email=None, email_password=None):
    """Устанавливает текущие данные аккаунта для использования в обработчиках"""
    global _current_username, _current_password, _current_email, _current_email_password
    _current_username = username
    _current_password = password
    _current_email = email
    _current_email_password = email_password

class InstagramClient:
    def __init__(self, account_id):
        """
        Инициализирует клиент Instagram для указанного аккаунта.

        Args:
        account_id (int): ID аккаунта Instagram в базе данных
        """
        self.account_id = account_id
        self.account = get_instagram_account(account_id)
        self.client = Client(settings={})
        self.is_logged_in = False

    def login(self, challenge_handler=None):
        """
        Выполняет вход в Instagram.

        Args:
        challenge_handler: Обработчик для кодов подтверждения

        Returns:
        bool: True, если вход успешен, False в противном случае
        """
        # Получаем блокировку для этого аккаунта
        account_lock = get_account_lock(self.account_id)
        
        # Пытаемся получить блокировку с таймаутом
        if not account_lock.acquire(blocking=True, timeout=30):
            logger.warning(f"Не удалось получить блокировку для аккаунта {self.account.username} в течение 30 секунд")
            return False
        
        try:
            # Проверяем, не вошли ли мы уже
            if self.is_logged_in:
                try:
                    # Проверяем, активна ли сессия
                    self.client.get_timeline_feed()
                    return True
                except:
                    # Если сессия не активна, продолжаем вход
                    self.is_logged_in = False

            if not self.account:
                logger.error(f"Аккаунт с ID {self.account_id} не найден")
                return False

            # Проверяем, активна ли текущая сессия
            try:
                # Пробуем выполнить простой запрос для проверки сессии
                self.client.get_timeline_feed()
                logger.info(f"Сессия уже активна для {self.account.username}")
                self.is_logged_in = True
                return True
            except Exception as e:
                # Если сессия не активна, продолжаем с обычным входом
                logger.info(f"Сессия не активна для {self.account.username}, выполняется вход: {e}")

            try:
                # Пытаемся использовать сохраненную сессию
                session_file = os.path.join(ACCOUNTS_DIR, str(self.account_id), "session.json")

                if os.path.exists(session_file):
                    logger.info(f"Найден файл сессии для аккаунта {self.account.username}")

                    try:
                        # Загружаем данные сессии
                        with open(session_file, 'r') as f:
                            session_data = json.load(f)

                        # Устанавливаем настройки клиента из сессии
                        if 'settings' in session_data:
                            self.client.set_settings(session_data['settings'])
                            logger.info(f"Загружены сохраненные настройки устройства для {self.account.username}")

                            # Пытаемся использовать сохраненную сессию
                            self.client.login(self.account.username, self.account.password)
                            self.is_logged_in = True
                            logger.info(f"Успешный вход по сохраненной сессии для {self.account.username}")
                            return True
                        else:
                            logger.warning(f"Не удалось использовать сохраненную сессию для {self.account.username}: {e}")
                            # Продолжаем с обычным входом
                    except Exception as e:
                        logger.warning(f"Не удалось использовать сохраненную сессию для {self.account.username}: {e}")
                        # Продолжаем с обычным входом
                else:
                    # Если сессия не найдена, генерируем новые настройки устройства
                    logger.info(f"Файл сессии не найден для {self.account.username}, генерируем новые настройки устройства")
                    device_settings = generate_device_settings(self.account_id)
                    self.client.set_settings(device_settings)
                    logger.info(f"Применены новые настройки устройства для {self.account.username}")

                # Если у аккаунта есть email и email_password, настраиваем автоматическое получение кода
                if hasattr(self.account, 'email') and hasattr(self.account, 'email_password') and self.account.email and self.account.email_password:
                    # Определяем функцию-обработчик для получения кода
                    def auto_challenge_code_handler(username, choice):
                        logger.info(f"Запрошен код подтверждения для {username}, тип: {choice}")
                        # Пытаемся получить код из почты
                        verification_code = get_verification_code_from_email(self.account.email, self.account.email_password, max_attempts=5, delay_between_attempts=10)
                        if verification_code:
                            logger.info(f"Получен код подтверждения из почты: {verification_code}")
                            return verification_code
                        else:
                            logger.warning(f"Не удалось получить код из почты для {username}")
                            # Возвращаем None вместо запроса через консоль для веб-версии
                            return None

                    # Устанавливаем обработчик
                    self.client.challenge_code_handler = auto_challenge_code_handler
                # Если предоставлен обработчик запросов на подтверждение
                elif challenge_handler:
                    # Устанавливаем обработчик для клиента
                    self.client.challenge_code_handler = lambda username, choice: challenge_handler.handle_challenge(username, choice)

                # Настраиваем обработчик кодов подтверждения
                if hasattr(self.account, 'email') and hasattr(self.account, 'email_password') and self.account.email and self.account.email_password:
                    def web_safe_challenge_handler(username, choice):
                        """Безопасный для веб-версии обработчик challenge кодов с повторными попытками и обработкой ошибок верификации"""
                        max_attempts = 5  # Увеличено до 5 попыток получения кодов
                        max_verification_attempts = 3  # Максимум попыток отправки одного кода
                        
                        for code_attempt in range(max_attempts):
                            try:
                                logger.info(f"Запрос кода подтверждения для {username}, тип: {choice}, попытка получения кода {code_attempt + 1}/{max_attempts}")
                                
                                # Добавляем задержку между попытками (кроме первой)
                                if code_attempt > 0:
                                    delay = 30 + (code_attempt * 15)  # 30, 45, 60, 75, 90 секунд задержки
                                    logger.info(f"Ожидание {delay} секунд перед повторным запросом кода для {username}")
                                    time.sleep(delay)
                                
                                # Получаем новый код с увеличенными таймаутами
                                code = get_verification_code_from_email(
                                    self.account.email, self.account.email_password, 
                                    max_attempts=7,  # Больше попыток поиска в email
                                    delay_between_attempts=15  # Больше задержка между попытками поиска
                                )
                                
                                if code:
                                    logger.info(f"Получен код подтверждения из email: {code} (попытка получения {code_attempt + 1})")
                                    
                                    # Пробуем отправить код с повторными попытками
                                    for verify_attempt in range(max_verification_attempts):
                                        try:
                                            logger.info(f"Отправка кода {code} для {username}, попытка верификации {verify_attempt + 1}/{max_verification_attempts}")
                                            
                                            # Небольшая задержка перед отправкой кода
                                            time.sleep(5)
                                            
                                            # Возвращаем код для отправки
                                            return code
                                            
                                        except Exception as verify_error:
                                            error_msg = str(verify_error) if verify_error else "Unknown error"
                                            logger.error(f"Ошибка при отправке кода {code} для {username} (попытка {verify_attempt + 1}): {error_msg}")
                                            
                                            # Если Instagram говорит проверить код, пробуем получить новый
                                            if error_msg and ("check the code" in error_msg.lower() or "try again" in error_msg.lower()):
                                                logger.warning(f"Instagram просит проверить код для {username} - получаем новый код")
                                                break  # Выходим из внутреннего цикла, чтобы получить новый код
                                            
                                            # Если это не последняя попытка верификации, ждем и пробуем снова
                                            if verify_attempt < max_verification_attempts - 1:
                                                time.sleep(10)
                                                continue
                                
                                else:
                                    logger.warning(f"Не удалось получить код из email для {username} (попытка получения {code_attempt + 1})")
                                    
                                    # Если это не последняя попытка получения кода, продолжаем
                                    if code_attempt < max_attempts - 1:
                                        logger.info(f"Повторная попытка получения кода для {username}")
                                        continue
                                
                            except Exception as e:
                                logger.error(f"Ошибка при получении кода для {username} (попытка получения {code_attempt + 1}): {e}")
                                
                                # Если это не последняя попытка получения кода, продолжаем
                                if code_attempt < max_attempts - 1:
                                    continue
                        
                        # Если все попытки исчерпаны
                        logger.error(f"Исчерпаны все попытки получения и отправки кода для {username}")
                        return None
                    
                    # Создаем универсальный обработчик для всех типов запросов
                    def universal_challenge_handler(prompt):
                        """Универсальный обработчик для всех запросов challenge"""
                        prompt_lower = prompt.lower()
                        
                        # Если это запрос кода
                        if "code" in prompt_lower or "verification" in prompt_lower:
                            return web_safe_challenge_handler(self.account.username, "EMAIL")
                            
                        # Если это запрос пароля - возвращаем пароль автоматически
                        elif "password" in prompt_lower:
                            logger.info(f"Автоматически предоставляем пароль для {self.account.username}")
                            return self.account.password
                            
                        # Для любых других запросов возвращаем пустую строку
                        else:
                            logger.warning(f"Неизвестный запрос challenge для {self.account.username}: {prompt}")
                            return ""
                    
                    # Устанавливаем универсальный обработчик
                    self.client.challenge_code_handler = universal_challenge_handler
                    
                    # Также добавляем патч для функции input, чтобы перехватывать все запросы
                    import builtins
                    original_input = builtins.input
                    
                    def patched_input(prompt=""):
                        """Патченная функция input для автоматической обработки запросов пароля"""
                        prompt_lower = prompt.lower()
                        
                        # Увеличиваем счетчик попыток
                        if not hasattr(patched_input, 'attempt_count'):
                            patched_input.attempt_count = 0
                        patched_input.attempt_count += 1
                        
                        # Ограничиваем количество попыток
                        if patched_input.attempt_count > 20:
                            logger.error(f"Превышено количество попыток ввода для {self.account.username}, прерываем")
                            raise Exception(f"Too many input attempts for {self.account.username}")
                        
                        # Если это запрос пароля и имя пользователя в prompt
                        if "password" in prompt_lower and "enter password for" in prompt_lower:
                            # Извлекаем username из prompt типа "Enter password for username:"
                            try:
                                # Ищем имя пользователя между "for " и ":"
                                start = prompt_lower.find("for ") + 4
                                end = prompt.find(":", start)
                                if start > 3 and end > start:
                                    prompt_username = prompt[start:end].strip()
                                    # Проверяем, это наш пользователь?
                                    if prompt_username == self.account.username:
                                        if self.account.password:
                                            logger.info(f"Автоматически предоставляем пароль для {self.account.username}")
                                            return self.account.password
                                        else:
                                            logger.error(f"Пароль не найден для {self.account.username} - пропускаем аккаунт")
                                            raise Exception(f"Password not found for {self.account.username}")
                                    else:
                                        logger.warning(f"Запрос пароля для другого пользователя: {prompt_username} (ожидался {self.account.username})")
                                        # Пробуем вернуть пароль для запрашиваемого пользователя из БД
                                        requested_account = get_instagram_account_by_username(prompt_username)
                                        if requested_account and requested_account.password:
                                            logger.info(f"Найден пароль для {prompt_username} в БД")
                                            return requested_account.password
                                        else:
                                            logger.error(f"Пароль не найден для {prompt_username} - пропускаем")
                                            raise Exception(f"Password not found for {prompt_username}")
                            except Exception as e:
                                logger.error(f"Ошибка при извлечении username из prompt: {e}")
                                raise e
                        
                        # Если это запрос кода и наш пользователь в prompt
                        elif "code" in prompt_lower and self.account.username in prompt:
                            logger.info(f"Получаем код через патченную input() для {self.account.username}")
                            code = get_verification_code_from_email(self.account.email, self.account.email_password, max_attempts=5, delay_between_attempts=10)
                            if code:
                                logger.info(f"Код получен через патченную input(): {code}")
                                return code
                            else:
                                logger.warning(f"Не удалось получить код через патченную input() для {self.account.username}")
                                return ""
                        
                        # Для других запросов возвращаем пустую строку
                        else:
                            logger.warning(f"Патченная input() получила неизвестный запрос: {prompt}")
                            return ""
                    
                    # Временно заменяем input
                    import builtins
                    builtins.input = patched_input
                    
                    # Сохраняем ссылку для восстановления
                    self._original_input = original_input
                    
                    logger.info(f"Настроен улучшенный обработчик кодов подтверждения и патч input() для {self.account.username}")
                else:
                    # Если email не предоставлен, устанавливаем пустой обработчик
                    self.client.challenge_code_handler = lambda username, choice: None
                    logger.info(f"Установлен пустой обработчик кодов (email не предоставлен) для {self.account.username}")

                # Добавляем случайную задержку перед входом
                delay = random.uniform(2, 5)
                logger.info(f"Добавлена задержка {delay:.2f} секунд перед входом для {self.account.username}")
                time.sleep(delay)

                # Выполняем вход с таймаутом чтобы избежать зависания
                import threading
                
                login_success = False
                login_error = None
                
                def login_thread():
                    nonlocal login_success, login_error
                    try:
                        self.client.login(self.account.username, self.account.password)
                        logger.info(f"Успешный вход для {self.account.username}")
                        login_success = True
                    except Exception as e:
                        login_error = e
                
                # Запускаем вход в отдельном потоке с таймаутом 120 секунд (увеличено)
                thread = threading.Thread(target=login_thread)
                thread.daemon = True
                thread.start()
                thread.join(timeout=120)
                
                # Восстанавливаем оригинальный input после попытки входа
                if hasattr(self, '_original_input'):
                    try:
                        import builtins
                        builtins.input = self._original_input
                        logger.info(f"Восстановлен оригинальный input() после попытки входа для {self.account.username}")
                    except Exception as e:
                        logger.warning(f"Ошибка при восстановлении input() для {self.account.username}: {e}")
                
                if thread.is_alive():
                    logger.warning(f"Таймаут входа для {self.account.username} - процесс превысил 120 секунд")
                    return False
                elif login_error:
                    error_msg = str(login_error) if login_error else "Unknown error"
                    logger.error(f"Ошибка при входе для {self.account.username}: {error_msg}")
                    
                    # Если это ошибка с кодом подтверждения, пробуем еще раз с новым кодом
                    if error_msg and ("check the code" in error_msg.lower() or "try again" in error_msg.lower()):
                        logger.info(f"Получена ошибка верификации для {self.account.username} - система автоматически попробует новый код")
                        # Обработчик уже настроен на повторные попытки, просто возвращаем False
                        # чтобы система могла попробовать еще раз
                    
                    return False
                elif login_success:
                    # Сохраняем сессию
                    self._save_session()
                    return True
                else:
                    logger.warning(f"Неизвестная ошибка входа для {self.account.username}")
                    return False

            except BadPassword:
                logger.error(f"Неверный пароль для пользователя {self.account.username}")
                return False

            except ChallengeRequired as e:
                logger.error(f"Требуется подтверждение для пользователя {self.account.username}: {e}")
                return False

            except LoginRequired:
                logger.error(f"Не удалось войти для пользователя {self.account.username}")
                return False

            except Exception as e:
                logger.error(f"Ошибка при входе для пользователя {self.account.username}: {str(e)}")
                return False
            finally:
                # Всегда восстанавливаем input при выходе из метода
                if hasattr(self, '_original_input'):
                    try:
                        import builtins
                        builtins.input = self._original_input
                        logger.info(f"Восстановлен оригинальный input() в блоке finally для {self.account.username}")
                    except Exception as e:
                        logger.warning(f"Ошибка при восстановлении input() в finally для {self.account.username}: {e}")

        except Exception as e:
            logger.error(f"Ошибка при входе для пользователя {self.account.username}: {str(e)}")
            return False
        finally:
            # Освобождаем блокировку
            account_lock.release()
            logger.debug(f"Освобождена блокировка для аккаунта {self.account.username}")

    def _save_session(self):
        """Сохраняет данные сессии"""
        try:
            # Создаем директорию для аккаунта, если она не существует
            account_dir = os.path.join(ACCOUNTS_DIR, str(self.account_id))
            os.makedirs(account_dir, exist_ok=True)

            # Получаем настройки клиента
            settings = self.client.get_settings()

            # Формируем данные сессии
            session_data = {
                'username': self.account.username,
                'account_id': self.account_id,
                'last_login': time.strftime('%Y-%m-%d %H:%M:%S'),
                'settings': settings
            }

            # Сохраняем в файл
            session_file = os.path.join(account_dir, "session.json")
            with open(session_file, 'w') as f:
                json.dump(session_data, f)

            # Обновляем данные сессии в базе данных
            update_account_session_data(self.account_id, json.dumps(session_data))

            logger.info(f"Сессия сохранена для пользователя {self.account.username}")

        except Exception as e:
            logger.error(f"Ошибка при сохранении сессии для {self.account.username}: {e}")

    def check_login(self):
        """
        Проверяет статус входа и выполняет вход при необходимости.

        Returns:
        bool: True, если вход выполнен, False в противном случае
        """
        if not self.is_logged_in:
            return self.login()

        try:
            # Проверяем, активна ли сессия
            self.client.get_timeline_feed()
            return True
        except Exception:
            # Если сессия не активна, пытаемся войти снова
            logger.info(f"Сессия не активна для {self.account.username}, выполняется повторный вход")
            return self.login()

    def login_with_challenge_code(self, verification_code):
        """
        Выполняет вход с использованием кода верификации
        
        Args:
            verification_code (str): 6-значный код верификации
            
        Returns:
            bool: True если вход успешен, False в противном случае
        """
        try:
            logger.info(f"Попытка входа с кодом верификации для {self.account.username}")
            
            # Создаем обработчик кода верификации
            def verification_handler(username, choice):
                logger.info(f"Используем предоставленный код верификации {verification_code} для {username}")
                return verification_code
            
            # Устанавливаем обработчик
            self.client.challenge_code_handler = verification_handler
            
            # Выполняем вход
            success = self.login()
            
            if success:
                logger.info(f"✅ Успешный вход с кодом верификации для {self.account.username}")
                return True
            else:
                logger.warning(f"❌ Неудачный вход с кодом верификации для {self.account.username}")
                return False
                
        except Exception as e:
            logger.error(f"Ошибка при входе с кодом верификации для {self.account.username}: {e}")
            return False

    def logout(self):
        """Выполняет выход из аккаунта Instagram"""
        if self.is_logged_in:
            try:
                self.client.logout()
                self.is_logged_in = False
                logger.info(f"Выход выполнен для пользователя {self.account.username}")
                return True
            except Exception as e:
                logger.error(f"Ошибка при выходе для пользователя {self.account.username}: {str(e)}")
                return False
        return True

def test_instagram_login(username, password, email=None, email_password=None, account_id=None):
    """
    Тестирует вход в Instagram с указанными учетными данными.

    Args:
    username (str): Имя пользователя Instagram
    password (str): Пароль пользователя Instagram
    email (str, optional): Email для получения кода подтверждения
    email_password (str, optional): Пароль от email
    account_id (int, optional): ID аккаунта для генерации уникальных настроек устройства

    Returns:
    bool: True, если вход успешен, False в противном случае
    """
    try:
        logger.info(f"Тестирование входа для пользователя {username}")

        # Создаем клиент Instagram
        client = Client(settings={})
        
        # Применяем патч для обработки ошибок публичных запросов
        patch_public_graphql_request(client)

        # Если предоставлен account_id, генерируем и применяем уникальные настройки устройства
        if account_id:
            device_settings = generate_device_settings(account_id)
            client.set_settings(device_settings)
            logger.info(f"Применены уникальные настройки устройства для {username}")

        # Если предоставлены данные почты, настраиваем автоматическое получение кода
        if email and email_password:
            # Определяем функцию-обработчик для получения кода
            def auto_challenge_code_handler(username, choice):
                print(f"[DEBUG] Запрошен код подтверждения для {username}, тип: {choice}")
                # Пытаемся получить код из почты
                verification_code = get_verification_code_from_email(email, email_password, max_attempts=5, delay_between_attempts=10)
                if verification_code:
                    print(f"[DEBUG] Получен код подтверждения из почты: {verification_code}")
                    return verification_code
                else:
                    print(f"[DEBUG] Не удалось получить код из почты, запрашиваем через консоль")
                    # Если не удалось получить код из почты, запрашиваем через консоль
                    return input(f"Enter code (6 digits) for {username} ({choice}): ")

            # Устанавливаем обработчик
            client.challenge_code_handler = auto_challenge_code_handler

        # Пытаемся войти
        client.login(username, password)

        # Если дошли до этой точки, значит вход успешен
        logger.info(f"Вход успешен для пользователя {username}")

        # Выходим из аккаунта
        client.logout()

        return True

    except BadPassword:
        logger.error(f"Неверный пароль для пользователя {username}")
        return False

    except ChallengeRequired:
        logger.error(f"Требуется подтверждение для пользователя {username}")
        return False

    except LoginRequired:
        logger.error(f"Не удалось войти для пользователя {username}")
        return False

    except Exception as e:
        logger.error(f"Ошибка при входе для пользователя {username}: {str(e)}")
        return False

def test_instagram_login_with_proxy(account_id, username, password, email=None, email_password=None):
    # Получаем блокировку для этого аккаунта
    account_lock = get_account_lock(account_id)
    
    # Пытаемся получить блокировку с таймаутом
    if not account_lock.acquire(blocking=True, timeout=30):
        logger.warning(f"Не удалось получить блокировку для аккаунта {username} в течение 30 секунд")
        return False
    
    try:
        logger.info(f"Тестирование входа для пользователя {username} с прокси")
        
        # Используем сервис для дешифровки пароля (локальный импорт)
        try:
            # from services.instagram_service import instagram_service  # ВРЕМЕННО ОТКЛЮЧЕН
            # decrypted_password = instagram_service.get_decrypted_password(account_id)
            # if decrypted_password:
            #     password = decrypted_password
            pass  # Временно отключен
        except ImportError:
            logger.warning("Сервис instagram_service недоступен, используется оригинальный пароль")

        # Проверяем наличие файла сессии
        session_file = os.path.join(ACCOUNTS_DIR, str(account_id), "session.json")

        # Создаем клиент Instagram с пустыми настройками (избегаем None)
        client = Client(settings={})
        logger.info(f"Создан клиент Instagram для {username}")

        # Проверяем существующую сессию
        if os.path.exists(session_file):
            logger.info(f"Найден файл сессии для аккаунта {username}")
            try:
                # Загружаем данные сессии
                with open(session_file, 'r') as f:
                    session_data = json.load(f)

                # Устанавливаем настройки клиента из сессии
                if 'settings' in session_data:
                    client.set_settings(session_data['settings'])
                    logger.info(f"Загружены сохраненные настройки устройства для {username}")

                # Получаем прокси для аккаунта с ротацией IP
                proxy = get_proxy_for_account(account_id)
                if proxy:
                    # Проверяем поддержку ротации IP
                    supports_rotation = (proxy.username and 
                                       ('session' in proxy.username.lower() or 
                                        'user-' in proxy.username.lower() or
                                        'rotating' in proxy.username.lower()))
                    
                    if supports_rotation:
                        # Rotating прокси
                        proxy_config = {
                            'protocol': proxy.protocol,
                            'host': proxy.host,
                            'port': proxy.port,
                            'username': proxy.username,
                            'password': proxy.password
                        }
                        proxy_url = get_rotating_proxy_url(proxy_config, account_id, "time")
                    else:
                        # Статический прокси
                        proxy_url = f"{proxy.protocol}://{proxy.host}:{proxy.port}"
                        if (proxy.username and proxy.password and 
                            proxy.username is not None and proxy.password is not None and
                            proxy.username.strip() and proxy.password.strip()):
                            proxy_url = f"{proxy.protocol}://{proxy.username}:{proxy.password}@{proxy.host}:{proxy.port}"
                    client.set_proxy(proxy_url)

                # Пробуем использовать сохраненную сессию
                client.login(username, password)
                logger.info(f"Успешный вход по сохраненной сессии для {username}")

                # Сохраняем клиент в глобальный кэш
                _instagram_clients[account_id] = client
                return True
            except Exception as e:
                logger.warning(f"Не удалось использовать сохраненную сессию для {username}: {e}")
                # Удаляем недействительный файл сессии
                try:
                    os.remove(session_file)
                    logger.info(f"Удален недействительный файл сессии для {username}")
                except Exception as del_error:
                    logger.warning(f"Не удалось удалить файл сессии для {username}: {del_error}")

                # Очищаем клиент перед повторной попыткой
                client = Client(settings={})
                logger.info(f"Создан новый клиент Instagram для {username} после неудачной попытки входа")

        # Если сессия не найдена или недействительна, выполняем обычный вход
        # Получаем прокси для аккаунта
        proxy = get_proxy_for_account(account_id)

        # Генерируем и применяем настройки устройства
        device_settings = generate_device_settings(account_id)
        client.set_settings(device_settings)

        # Устанавливаем прокси с ротацией IP
        if proxy:
            # Проверяем поддерживает ли прокси ротацию IP
            supports_rotation = (proxy.username and 
                               ('session' in proxy.username.lower() or 
                                'user-' in proxy.username.lower() or
                                'rotating' in proxy.username.lower()))
            
            if supports_rotation:
                # Используем rotating прокси с автоматической сменой IP
                proxy_config = {
                    'protocol': proxy.protocol,
                    'host': proxy.host,
                    'port': proxy.port,
                    'username': proxy.username,
                    'password': proxy.password
                }
                proxy_url = get_rotating_proxy_url(proxy_config, account_id, "time")
                logger.info(f"🔄 Установлен ROTATING прокси для {username}")
            else:
                # Обычный статический прокси
                proxy_url = f"{proxy.protocol}://{proxy.host}:{proxy.port}"
                if (proxy.username and proxy.password and 
                    proxy.username is not None and proxy.password is not None and
                    proxy.username.strip() and proxy.password.strip()):
                    proxy_url = f"{proxy.protocol}://{proxy.username}:{proxy.password}@{proxy.host}:{proxy.port}"
                logger.info(f"📌 Установлен статический прокси для {username}")
            
            client.set_proxy(proxy_url)
        else:
            logger.warning(f"Прокси не найден для аккаунта {username}")

        # Настраиваем обработчик кодов подтверждения
        if email and email_password:
            def web_safe_challenge_handler(username, choice):
                """Безопасный для веб-версии обработчик challenge кодов с повторными попытками и обработкой ошибок верификации"""
                max_attempts = 5  # Увеличено до 5 попыток получения кодов
                max_verification_attempts = 3  # Максимум попыток отправки одного кода
                
                for code_attempt in range(max_attempts):
                    try:
                        logger.info(f"Запрос кода подтверждения для {username}, тип: {choice}, попытка получения кода {code_attempt + 1}/{max_attempts}")
                        
                        # Добавляем задержку между попытками (кроме первой)
                        if code_attempt > 0:
                            delay = 30 + (code_attempt * 15)  # 30, 45, 60, 75, 90 секунд задержки
                            logger.info(f"Ожидание {delay} секунд перед повторным запросом кода для {username}")
                            time.sleep(delay)
                        
                        # Получаем новый код с увеличенными таймаутами
                        code = get_verification_code_from_email(
                            email, email_password, 
                            max_attempts=7,  # Больше попыток поиска в email
                            delay_between_attempts=15  # Больше задержка между попытками поиска
                        )
                        
                        if code:
                            logger.info(f"Получен код подтверждения из email: {code} (попытка получения {code_attempt + 1})")
                            
                            # Пробуем отправить код с повторными попытками
                            for verify_attempt in range(max_verification_attempts):
                                try:
                                    logger.info(f"Отправка кода {code} для {username}, попытка верификации {verify_attempt + 1}/{max_verification_attempts}")
                                    
                                    # Небольшая задержка перед отправкой кода
                                    time.sleep(5)
                                    
                                    # Возвращаем код для отправки
                                    return code
                                    
                                except Exception as verify_error:
                                    error_msg = str(verify_error) if verify_error else "Unknown error"
                                    logger.error(f"Ошибка при отправке кода {code} для {username} (попытка {verify_attempt + 1}): {error_msg}")
                                    
                                    # Если Instagram говорит проверить код, пробуем получить новый
                                    if error_msg and ("check the code" in error_msg.lower() or "try again" in error_msg.lower()):
                                        logger.warning(f"Instagram просит проверить код для {username} - получаем новый код")
                                        break  # Выходим из внутреннего цикла, чтобы получить новый код
                                    
                                    # Если это не последняя попытка верификации, ждем и пробуем снова
                                    if verify_attempt < max_verification_attempts - 1:
                                        time.sleep(10)
                                        continue
                        
                        else:
                            logger.warning(f"Не удалось получить код из email для {username} (попытка получения {code_attempt + 1})")
                            
                            # Если это не последняя попытка получения кода, продолжаем
                            if code_attempt < max_attempts - 1:
                                logger.info(f"Повторная попытка получения кода для {username}")
                                continue
                                
                    except Exception as e:
                        logger.error(f"Ошибка при получении кода для {username} (попытка получения {code_attempt + 1}): {e}")
                        
                        # Если это не последняя попытка получения кода, продолжаем
                        if code_attempt < max_attempts - 1:
                            continue
                
                # Если все попытки исчерпаны
                logger.error(f"Исчерпаны все попытки получения и отправки кода для {username}")
                return None
            
            # Добавляем патч для функции input, чтобы перехватывать запросы пароля
            import builtins
            original_input = builtins.input
            
            def patched_input(prompt=""):
                """Патченная функция input для автоматической обработки запросов пароля"""
                prompt_lower = prompt.lower()
                
                # Увеличиваем счетчик попыток
                if not hasattr(patched_input, 'attempt_count'):
                    patched_input.attempt_count = 0
                patched_input.attempt_count += 1
                
                # Ограничиваем количество попыток
                if patched_input.attempt_count > 20:
                    logger.error(f"Превышено количество попыток ввода для {username}, прерываем")
                    raise Exception(f"Too many input attempts for {username}")
                
                # Если это запрос пароля и имя пользователя в prompt
                if "password" in prompt_lower and "enter password for" in prompt_lower:
                    # Извлекаем username из prompt типа "Enter password for username:"
                    try:
                        # Ищем имя пользователя между "for " и ":"
                        start = prompt_lower.find("for ") + 4
                        end = prompt.find(":", start)
                        if start > 3 and end > start:
                            prompt_username = prompt[start:end].strip()
                            # Проверяем, это наш пользователь?
                            if prompt_username == username:
                                if password:
                                    logger.info(f"Автоматически предоставляем пароль для {username}")
                                    return password
                                else:
                                    logger.error(f"Пароль не найден для {username} - пропускаем аккаунт")
                                    raise Exception(f"Password not found for {username}")
                            else:
                                logger.warning(f"Запрос пароля для другого пользователя: {prompt_username} (ожидался {username})")
                                # Пробуем вернуть пароль для запрашиваемого пользователя из БД
                                requested_account = get_instagram_account_by_username(prompt_username)
                                if requested_account and requested_account.password:
                                    logger.info(f"Найден пароль для {prompt_username} в БД")
                                    return requested_account.password
                                else:
                                    logger.error(f"Пароль не найден для {prompt_username} - пропускаем")
                                    raise Exception(f"Password not found for {prompt_username}")
                    except Exception as e:
                        logger.error(f"Ошибка при извлечении username из prompt: {e}")
                        return ""
                
                # Если это запрос кода и наш пользователь в prompt
                elif "code" in prompt_lower and username in prompt:
                    logger.info(f"Получаем код через патченную input() для {username}")
                    code = get_verification_code_from_email(email, email_password, max_attempts=5, delay_between_attempts=10)
                    if code:
                        logger.info(f"Код получен через патченную input(): {code}")
                        return code
                    else:
                        logger.warning(f"Не удалось получить код через патченную input() для {username}")
                        return ""
                
                # Для других запросов возвращаем пустую строку
                else:
                    logger.warning(f"Патченная input() получила неизвестный запрос: {prompt}")
                    return ""
            
            # Временно заменяем input
            builtins.input = patched_input
            
            client.challenge_code_handler = web_safe_challenge_handler
            logger.info(f"Настроен улучшенный обработчик кодов подтверждения и патч input() для {username}")
        else:
            # Если email не предоставлен, устанавливаем пустой обработчик
            client.challenge_code_handler = lambda username, choice: None
            logger.info(f"Установлен пустой обработчик кодов (email не предоставлен) для {username}")

        # Добавляем случайную задержку перед входом
        delay = random.uniform(2, 5)
        logger.info(f"Добавлена задержка {delay:.2f} секунд перед входом для {username}")
        time.sleep(delay)

        # Выполняем вход с таймаутом чтобы избежать зависания
        import threading
        
        login_success = False
        login_error = None
        
        def login_thread():
            nonlocal login_success, login_error
            try:
                client.login(username, password)
                logger.info(f"Успешный вход для {username}")
                login_success = True
            except Exception as e:
                login_error = e
        
        # Запускаем вход в отдельном потоке с таймаутом 120 секунд (увеличено)
        thread = threading.Thread(target=login_thread)
        thread.daemon = True
        thread.start()
        thread.join(timeout=120)
        
        # Восстанавливаем оригинальный input после попытки входа
        if email and email_password:
            try:
                builtins.input = original_input
                logger.info(f"Восстановлен оригинальный input() после попытки входа для {username}")
            except Exception as e:
                logger.warning(f"Ошибка при восстановлении input() для {username}: {e}")
        
        if thread.is_alive():
            logger.warning(f"Таймаут входа для {username} - процесс превысил 120 секунд")
            return False
        elif login_error:
            error_msg = str(login_error) if login_error else "Unknown error"
            logger.error(f"Ошибка при входе для {username}: {error_msg}")
            
            # Если это ошибка с кодом подтверждения, пробуем еще раз с новым кодом
            if error_msg and ("check the code" in error_msg.lower() or "try again" in error_msg.lower()):
                logger.info(f"Получена ошибка верификации для {username} - система автоматически попробует новый код")
                # Обработчик уже настроен на повторные попытки, просто возвращаем False
                # чтобы система могла попробовать еще раз
            
            return False
        elif login_success:
            # Сохраняем сессию
            account_dir = os.path.join(ACCOUNTS_DIR, str(account_id))
            os.makedirs(account_dir, exist_ok=True)

            session_data = {
                'username': username,
                'account_id': account_id,
                'last_login': time.strftime('%Y-%m-%d %H:%M:%S'),
                'settings': client.get_settings()
            }

            with open(session_file, 'w') as f:
                json.dump(session_data, f)
            logger.info(f"Сохранена сессия для {username}")

            # Обновляем статус аккаунта в базе данных как активный
            from database.db_manager import update_instagram_account
            from datetime import datetime
            try:
                update_instagram_account(
                    account_id,
                    is_active=True,
                    status="active",
                    last_error=None,
                    last_check=datetime.now()
                )
                logger.info(f"✅ Статус аккаунта {username} обновлен как активный в базе данных")
            except Exception as db_error:
                logger.error(f"❌ Ошибка при обновлении статуса аккаунта {username}: {db_error}")

            # Сохраняем клиент в глобальный кэш
            _instagram_clients[account_id] = client
            return True
        else:
            logger.warning(f"Неизвестная ошибка входа для {username}")
            return False
    except Exception as e:
        import traceback
        error_msg = str(e) if e else "Unknown error"
        logger.error(f"Ошибка при входе для пользователя {username}: {error_msg}")
        logger.error(f"📍 TRACEBACK ДЛЯ ОТЛАДКИ: {traceback.format_exc()}")
        # Если файл сессии существует, удаляем его
        if os.path.exists(session_file):
            try:
                os.remove(session_file)
                logger.info(f"Удален файл сессии после ошибки входа для {username}")
            except Exception as del_error:
                logger.warning(f"Не удалось удалить файл сессии для {username}: {del_error}")
        return False
    finally:
        # Освобождаем блокировку
        account_lock.release()
        logger.debug(f"Освобождена блокировка для аккаунта {username}")

def login_with_session(username, password, account_id, email=None, email_password=None):
    """
    Выполняет вход в Instagram с использованием сохраненной сессии.

    Args:
    username (str): Имя пользователя Instagram
    password (str): Пароль пользователя Instagram
    account_id (int): ID аккаунта в базе данных
    email (str, optional): Email для получения кода подтверждения
    email_password (str, optional): Пароль от email

    Returns:
    Client: Клиент Instagram или None в случае ошибки
    """
    try:
        logger.info(f"Вход с сессией для пользователя {username}")

        # Создаем клиент Instagram
        client = Client(settings={})
        
        # Применяем патч для обработки ошибок публичных запросов
        patch_public_graphql_request(client)
        
        # ВАЖНО: Устанавливаем прокси ПЕРЕД логином!
        proxy = get_proxy_for_account(account_id)
        if proxy:
            # Проверяем поддержку ротации IP
            supports_rotation = (proxy.username and 
                               ('session' in proxy.username.lower() or 
                                'user-' in proxy.username.lower() or
                                'rotating' in proxy.username.lower()))
            
            if supports_rotation:
                # Rotating прокси
                proxy_config = {
                    'protocol': proxy.protocol,
                    'host': proxy.host,
                    'port': proxy.port,
                    'username': proxy.username,
                    'password': proxy.password
                }
                proxy_url = get_rotating_proxy_url(proxy_config, account_id, "time")
                logger.info(f"🔄 Установлен ROTATING прокси для {username}")
            else:
                # Статический прокси
                proxy_url = f"{proxy.protocol}://{proxy.host}:{proxy.port}"
                if (proxy.username and proxy.password and 
                    proxy.username is not None and proxy.password is not None and
                    proxy.username.strip() and proxy.password.strip()):
                    proxy_url = f"{proxy.protocol}://{proxy.username}:{proxy.password}@{proxy.host}:{proxy.port}"
                logger.info(f"📌 Установлен статический прокси для {username}")
            
            client.set_proxy(proxy_url)
            logger.info(f"✅ Прокси настроен: {proxy.host}:{proxy.port}")
        else:
            logger.warning(f"⚠️ Прокси НЕ найден для аккаунта {username} - используется прямое подключение!")

        # Если предоставлены данные почты, настраиваем автоматическое получение кода
        if email and email_password:
            # Определяем функцию-обработчик для получения кода
            def auto_challenge_code_handler(username, choice):
                logger.info(f"🔐 CHALLENGE ВЫЗВАН для {username}, тип: {choice}")
                
                try:
                    # Пытаемся получить код из почты
                    verification_code = get_verification_code_from_email(email, email_password, max_attempts=5, delay_between_attempts=10)
                    if verification_code:
                        logger.info(f"✅ Получен код подтверждения из почты: {verification_code}")
                        return verification_code
                    else:
                        logger.warning(f"❌ Не удалось получить код из почты для {username}")
                        # Возвращаем None вместо запроса через консоль для веб-версии
                        return None
                except Exception as e:
                    logger.error(f"💥 ОШИБКА в challenge handler для {username}: {e}")
                    return None

            # Устанавливаем обработчик
            client.challenge_code_handler = auto_challenge_code_handler
            logger.info(f"📧 CHALLENGE HANDLER установлен для {email}")
        else:
            # Если email не предоставлен, устанавливаем пустой обработчик
            client.challenge_code_handler = lambda username, choice: None
            logger.info(f"🚫 Пустой challenge handler установлен (нет email данных)")

        # Проверяем наличие файла сессии
        session_file = os.path.join(ACCOUNTS_DIR, str(account_id), "session.json")

        if os.path.exists(session_file):
            logger.info(f"Найден файл сессии для аккаунта {username}")

            try:
                # Загружаем данные сессии
                with open(session_file, 'r') as f:
                    session_data = json.load(f)

                # Устанавливаем настройки клиента из сессии
                if 'settings' in session_data:
                    client.set_settings(session_data['settings'])
                    logger.info(f"Загружены сохраненные настройки устройства для {username}")

                # Пытаемся использовать сохраненную сессию
                client.login(username, password)
                logger.info(f"Успешный вход по сохраненной сессии для {username}")
                return client
            except Exception as e:
                logger.warning(f"Не удалось использовать сохраненную сессию для {username}: {e}")
                # Продолжаем с обычным входом
        else:
            # Если сессия не найдена, генерируем новые настройки устройства
            logger.info(f"Файл сессии не найден для {username}, генерируем новые настройки устройства")
            device_settings = generate_device_settings(account_id)
            client.set_settings(device_settings)
            logger.info(f"Применены новые настройки устройства для {username}")

        # Обычный вход
        logger.info(f"🔑 Начинаем обычный вход для {username}")
        try:
            client.login(username, password)
            logger.info(f"✅ УСПЕШНЫЙ обычный вход для {username}")
        except Exception as login_error:
            logger.error(f"❌ ОШИБКА обычного входа для {username}: {login_error}")
            
            # Если это challenge ошибка, то challenge handler должен был сработать
            if "challenge" in str(login_error).lower():
                logger.info(f"🔍 Обнаружен challenge для {username}, ожидаем обработку...")
            
            # Re-raise исключение чтобы его поймал внешний try-catch
            raise

        # Сохраняем сессию
        try:
            # Создаем директорию для аккаунта, если она не существует
            account_dir = os.path.join(ACCOUNTS_DIR, str(account_id))
            os.makedirs(account_dir, exist_ok=True)

            # Получаем настройки клиента
            settings = client.get_settings()

            # Формируем данные сессии
            session_data = {
                'username': username,
                'account_id': account_id,
                'last_login': time.strftime('%Y-%m-%d %H:%M:%S'),
                'settings': settings
            }

            # Сохраняем в файл
            with open(session_file, 'w') as f:
                json.dump(session_data, f)

            # Обновляем данные сессии в базе данных
            update_account_session_data(account_id, json.dumps(session_data))

            logger.info(f"Сессия сохранена для пользователя {username}")

        except Exception as e:
            logger.error(f"Ошибка при сохранении сессии для {username}: {e}")

        return client

    except Exception as e:
        logger.error(f"💥 ОБЩАЯ ОШИБКА при входе для пользователя {username}: {str(e)}")
        logger.error(f"🔍 Тип ошибки: {type(e).__name__}")
        
        # Если это challenge-related ошибка, но challenge handler не сработал
        if "challenge" in str(e).lower():
            logger.error(f"🚨 Challenge ошибка НЕ была обработана для {username}!")
            if email and email_password:
                logger.error(f"📧 При том что email данные ЕСТЬ: {email}")
            else:
                logger.error(f"🚫 Email данных НЕТ для автоматической обработки")
        
        import traceback
        logger.error(f"📜 Traceback: {traceback.format_exc()}")
        return None

def check_login_challenge(self, username, password, email=None, email_password=None):
    """
    Проверяет, требуется ли проверка при входе, и обрабатывает ее

    Args:
    username (str): Имя пользователя Instagram
    password (str): Пароль от аккаунта Instagram
    email (str, optional): Адрес электронной почты для получения кода
    email_password (str, optional): Пароль от почты

    Returns:
    bool: True, если вход выполнен успешно, False в противном случае
    """
    print(f"[DEBUG] check_login_challenge вызван для {username}")

    # Максимальное количество попыток обработки проверок
    max_challenge_attempts = 3

    for attempt in range(max_challenge_attempts):
        try:
            # Пытаемся войти
            self.client.login(username, password)
            print(f"[DEBUG] Вход выполнен успешно для {username}")
            return True
        except ChallengeRequired as e:
            print(f"[DEBUG] Требуется проверка для {username}, попытка {attempt+1}")

            # Получаем API-путь для проверки
            api_path = self.client.last_json.get('challenge', {}).get('api_path')
            if not api_path:
                print(f"[DEBUG] Не удалось получить API-путь для проверки")
                return False

            # Получаем информацию о проверке
            try:
                self.client.get_challenge_url(api_path)
                challenge_type = self.client.last_json.get('step_name')
                print(f"[DEBUG] Тип проверки: {challenge_type}")

                # Выбираем метод проверки (email)
                if challenge_type == 'select_verify_method':
                    self.client.challenge_send_code(ChallengeChoice.EMAIL)
                    print(f"[DEBUG] Запрошен код подтверждения для {username}, тип: {ChallengeChoice.EMAIL}")

                # Получаем код подтверждения
                if email and email_password:
                    print(f"[DEBUG] Получение кода подтверждения из почты {email}")
                    from instagram.email_utils import get_verification_code_from_email

                    verification_code = get_verification_code_from_email(email, email_password)
                    if verification_code:
                        print(f"[DEBUG] Получен код подтверждения из почты: {verification_code}")
                        # Отправляем код
                        self.client.challenge_send_security_code(verification_code)

                        # Проверяем, успешно ли прошла проверка
                        if self.client.last_json.get('status') == 'ok':
                            print(f"[DEBUG] Код подтверждения принят для {username}")

                            # Пытаемся снова войти после успешной проверки
                            try:
                                self.client.login(username, password)
                                print(f"[DEBUG] Вход выполнен успешно после проверки для {username}")
                                return True
                            except Exception as login_error:
                                print(f"[DEBUG] Ошибка при повторном входе: {str(login_error)}")
                                # Продолжаем цикл для обработки следующей проверки
                                continue
                        else:
                            print(f"[DEBUG] Код подтверждения не принят для {username}")
                    else:
                        print(f"[DEBUG] Не удалось получить код из почты, запрашиваем через консоль")
                        # Если не удалось получить код из почты, запрашиваем через консоль
                        self.client.challenge_send_security_code(
                            self.client.challenge_code_handler(username, ChallengeChoice.EMAIL)
                        )
                else:
                    print(f"[DEBUG] Email не указан, запрашиваем код через консоль")
                    # Если email не указан, запрашиваем код через консоль
                    self.client.challenge_send_security_code(
                        self.client.challenge_code_handler(username, ChallengeChoice.EMAIL)
                    )

                # Пытаемся снова войти после проверки
                try:
                    self.client.login(username, password)
                    print(f"[DEBUG] Вход выполнен успешно после проверки для {username}")
                    return True
                except Exception as login_error:
                    print(f"[DEBUG] Ошибка при повторном входе: {str(login_error)}")
                    # Если это последняя попытка, возвращаем False
                    if attempt == max_challenge_attempts - 1:
                        return False
                    # Иначе продолжаем цикл для обработки следующей проверки
                    continue

            except Exception as challenge_error:
                print(f"[DEBUG] Ошибка при обработке проверки: {str(challenge_error)}")
                return False

        except Exception as e:
            print(f"[DEBUG] Ошибка при входе для {username}: {str(e)}")
            logger.error(f"Ошибка при входе для пользователя {username}: {str(e)}")
            return False

    print(f"[DEBUG] Не удалось войти после {max_challenge_attempts} попыток обработки проверок")
    return False

def submit_challenge_code(username, password, code, challenge_info=None):
    """
    Отправляет код подтверждения

    Возвращает:
    - success: True, если код принят
    - result: Результат операции или сообщение об ошибке
    """
    print(f"[DEBUG] submit_challenge_code вызван для {username} с кодом {code}")
    try:
        client = Client(settings={})
        
        # Применяем патч для обработки ошибок публичных запросов
        patch_public_graphql_request(client)

        # Восстанавливаем состояние клиента, если предоставлена информация о запросе
        if challenge_info and 'client_settings' in challenge_info:
            print(f"[DEBUG] Восстанавливаем настройки клиента для {username}")
            client.set_settings(challenge_info['client_settings'])

        # Отправляем код подтверждения
        print(f"[DEBUG] Отправляем код подтверждения {code} для {username}")
        client.challenge_code(code)

        # Пробуем войти снова
        print(f"[DEBUG] Пробуем войти снова для {username}")
        client.login(username, password)
        print(f"[DEBUG] Вход успешен для {username}")

        return True, "Код подтверждения принят"
    except Exception as e:
        print(f"[DEBUG] Ошибка при отправке кода подтверждения для {username}: {str(e)}")
        logger.error(f"Ошибка при отправке кода подтверждения: {str(e)}")
        return False, str(e)

def get_instagram_client(account_id, skip_recovery=False, force_login=False):
    """
    Возвращает инициализированный клиент Instagram для указанного аккаунта.
    Использует кэширование для предотвращения повторных входов.

    Args:
        account_id (int): ID аккаунта Instagram в базе данных
        skip_recovery (bool): Если True, не пытаться восстановить аккаунт
        force_login (bool): Если True, принудительно войти заново

    Returns:
        Client: Инициализированный клиент Instagram или None в случае ошибки
    """
    global _instagram_clients

    # Если force_login, удаляем из кэша
    if force_login and account_id in _instagram_clients:
        del _instagram_clients[account_id]
        logger.info(f"Принудительное обновление сессии для аккаунта {account_id}")

    # Проверяем, есть ли клиент в кэше
    if account_id in _instagram_clients and not force_login:
        client = _instagram_clients[account_id]
        # Проверяем, активна ли сессия
        try:
            client.get_timeline_feed()
            logger.info(f"Используем кэшированный клиент для аккаунта {account_id}")
            return client
        except Exception as e:
            # Если сессия не активна, удаляем из кэша
            logger.info(f"Кэшированный клиент не активен для аккаунта {account_id}: {e}")
            del _instagram_clients[account_id]
            
            # Если skip_recovery, возвращаем None
            if skip_recovery:
                logger.warning(f"Пропускаем восстановление для аккаунта {account_id}")
                return None

    # Получаем данные аккаунта из базы
    account = get_instagram_account(account_id)
    if not account:
        logger.error(f"Аккаунт с ID {account_id} не найден")
        return None

    # Если skip_recovery, пробуем только использовать существующую сессию
    if skip_recovery:
        try:
            session_file = os.path.join(ACCOUNTS_DIR, str(account_id), "session.json")
            if os.path.exists(session_file):
                client = Client(settings={})
                
                # Применяем патч для обработки ошибок публичных запросов
                patch_public_graphql_request(client)
                
                with open(session_file, 'r') as f:
                    session_data = json.load(f)
                if 'settings' in session_data:
                    client.set_settings(session_data['settings'])
                    # Пробуем использовать сессию без восстановления
                    try:
                        client.login(account.username, account.password)
                        client.get_timeline_feed()  # Проверяем что работает
                        _instagram_clients[account_id] = client
                        return client
                    except:
                        return None
        except:
            return None

    # Пробуем войти и сохранить сессию
    try:
        # Используем функцию для входа с прокси
        success = test_instagram_login_with_proxy(
            account_id,
            account.username,
            account.password,
            getattr(account, 'email', None),
            getattr(account, 'email_password', None)
        )

        if success and account_id in _instagram_clients:
            return _instagram_clients[account_id]
        else:
            logger.error(f"Не удалось войти в аккаунт с ID {account_id}")
            return None
    except Exception as e:
        logger.error(f"Ошибка при получении клиента Instagram для аккаунта {account_id}: {e}")
        return None

def refresh_instagram_sessions():
    """
    Периодически обновляет сессии всех аккаунтов для поддержания их активности.
    Эту функцию можно запускать по расписанию, например, раз в день.
    """
    from database.db_manager import get_all_instagram_accounts

    logger.info("Начинаем обновление сессий Instagram аккаунтов")

    accounts = get_all_instagram_accounts()
    for account in accounts:
        try:
            # Проверяем, есть ли клиент в кэше
            if account.id in _instagram_clients:
                client = _instagram_clients[account.id]
                try:
                    # Выполняем простое действие для обновления сессии
                    client.get_timeline_feed()
                    logger.info(f"Сессия обновлена для аккаунта {account.username}")

                    # Обновляем время последнего входа в session.json
                    session_file = os.path.join(ACCOUNTS_DIR, str(account.id), "session.json")
                    if os.path.exists(session_file):
                        with open(session_file, 'r') as f:
                            session_data = json.load(f)

                        session_data['last_login'] = time.strftime('%Y-%m-%d %H:%M:%S')

                        with open(session_file, 'w') as f:
                            json.dump(session_data, f)
                except Exception as e:
                    logger.warning(f"Ошибка при обновлении сессии для {account.username}: {e}")
                    # Удаляем из кэша и пробуем войти заново
                    del _instagram_clients[account.id]
                    get_instagram_client(account.id)
            else:
                # Если клиента нет в кэше, пробуем войти
                get_instagram_client(account.id)
        except Exception as e:
            logger.error(f"Ошибка при обработке аккаунта {account.username}: {e}")

    logger.info("Обновление сессий Instagram аккаунтов завершено")

def remove_instagram_account(account_id):
    """
    Удаляет аккаунт Instagram и его сессию.

    Args:
        account_id (int): ID аккаунта Instagram в базе данных

    Returns:
        bool: True, если удаление успешно, False в противном случае
    """
    global _instagram_clients

    try:
        # Если клиент в кэше, выходим из аккаунта и удаляем из кэша
        if account_id in _instagram_clients:
            try:
                _instagram_clients[account_id].logout()
                logger.info(f"Выполнен выход из аккаунта {account_id}")
            except Exception as e:
                logger.warning(f"Ошибка при выходе из аккаунта {account_id}: {e}")

            del _instagram_clients[account_id]

        # Удаляем файлы сессии
        session_dir = os.path.join(ACCOUNTS_DIR, str(account_id))
        if os.path.exists(session_dir):
            import shutil
            shutil.rmtree(session_dir)
            logger.info(f"Удалены файлы сессии для аккаунта {account_id}")

        # Удаляем аккаунт из базы данных
        from database.db_manager import delete_instagram_account
        success = delete_instagram_account(account_id)

        if success:
            logger.info(f"Аккаунт {account_id} успешно удален")
        else:
            logger.error(f"Не удалось удалить аккаунт {account_id} из базы данных")

        return success

    except Exception as e:
        logger.error(f"Ошибка при удалении аккаунта {account_id}: {e}")
        return False

        # Удаляем аккаунт из базы данных
        from database.db_manager import delete_instagram_account
        success = delete_instagram_account(account_id)

        if success:
            logger.info(f"Аккаунт {account_id} успешно удален")
        else:
            logger.error(f"Не удалось удалить аккаунт {account_id} из базы данных")

        return success

    except Exception as e:
        logger.error(f"Ошибка при удалении аккаунта {account_id}: {e}")
        return False

        # Удаляем аккаунт из базы данных
        from database.db_manager import delete_instagram_account
        success = delete_instagram_account(account_id)

        if success:
            logger.info(f"Аккаунт {account_id} успешно удален")
        else:
            logger.error(f"Не удалось удалить аккаунт {account_id} из базы данных")

        return success

    except Exception as e:
        logger.error(f"Ошибка при удалении аккаунта {account_id}: {e}")
        return False

        # Удаляем аккаунт из базы данных
        from database.db_manager import delete_instagram_account
        success = delete_instagram_account(account_id)

        if success:
            logger.info(f"Аккаунт {account_id} успешно удален")
        else:
            logger.error(f"Не удалось удалить аккаунт {account_id} из базы данных")

        return success


# ============================================================================
# UNIVERSAL CLIENT ADAPTER INTEGRATION (ОБРАТНАЯ СОВМЕСТИМОСТЬ)
# ============================================================================

# Сохраняем оригинальную функцию
_original_get_instagram_client = get_instagram_client

def get_instagram_client_with_adapter(account_id, skip_recovery=False, force_login=False):
    """
    Универсальная функция получения клиента с поддержкой Lazy Loading
    Полностью совместима с оригинальной get_instagram_client()
    """
    try:
        # Пытаемся использовать Universal Client Adapter
        from instagram.client_adapter import get_universal_client, is_lazy_mode
        
        if is_lazy_mode():
            # Используем lazy клиент через адаптер
            lazy_client = get_universal_client(account_id)
            
            # Проверяем параметры совместимости
            if force_login and hasattr(lazy_client, 'destroy'):
                # Для lazy клиентов - уничтожаем для пересоздания
                lazy_client.destroy()
                lazy_client = get_universal_client(account_id)
            
            logger.debug(f"Используем lazy клиент для аккаунта {account_id}")
            return lazy_client
        else:
            # Fallback на оригинальную функцию
            return _original_get_instagram_client(account_id, skip_recovery, force_login)
            
    except Exception as e:
        logger.warning(f"Ошибка в Universal Client Adapter: {e}, используем оригинальную функцию")
        # При любой ошибке fallback на оригинальную функцию
        return _original_get_instagram_client(account_id, skip_recovery, force_login)


# Заменяем оригинальную функцию на адаптер (ОБРАТНАЯ СОВМЕСТИМОСТЬ)
get_instagram_client = get_instagram_client_with_adapter


# Функция для получения статистики клиентов
def get_instagram_client_stats():
    """Получает статистику работы Instagram клиентов"""
    try:
        from instagram.client_adapter import get_client_stats
        return get_client_stats()
    except ImportError:
        return {'mode': 'normal', 'adapter_not_available': True}


# Функция для очистки клиентов
def cleanup_instagram_clients():
    """Очищает неактивные Instagram клиенты"""
    try:
        from instagram.client_adapter import cleanup_clients
        cleanup_clients()
        logger.debug("Выполнена очистка Instagram клиентов")
    except ImportError:
        logger.debug("Client Adapter недоступен для очистки")


# Функция для проверки режима
def is_using_lazy_loading():
    """Проверяет используется ли Lazy Loading"""
    try:
        from instagram.client_adapter import is_lazy_mode
        return is_lazy_mode()
    except ImportError:
        return False
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Глобальный патч для функции input() для автоматической обработки запросов Instagram
"""

import builtins
import logging

logger = logging.getLogger(__name__)

# Глобальные переменные для хранения данных текущего аккаунта
_current_account_data = {}

def set_current_account_data(username, password, email=None, email_password=None):
    """Устанавливает данные текущего аккаунта для глобального обработчика"""
    global _current_account_data
    _current_account_data = {
        'username': username,
        'password': password,
        'email': email,
        'email_password': email_password
    }
    logger.info(f"🔧 Установлены данные для автоматической обработки запросов: {username}")

def clear_current_account_data():
    """Очищает данные текущего аккаунта"""
    global _current_account_data
    _current_account_data = {}
    logger.info("🧹 Данные аккаунта очищены")

def global_input_handler(prompt=""):
    """Глобальный обработчик input() для автоматической обработки запросов Instagram"""
    global _current_account_data
    
    prompt_lower = prompt.lower()
    
    # Если есть данные текущего аккаунта
    if _current_account_data:
        username = _current_account_data.get('username', '')
        password = _current_account_data.get('password', '')
        email = _current_account_data.get('email')
        email_password = _current_account_data.get('email_password')
        
        # Если это запрос пароля для текущего пользователя
        if "password" in prompt_lower and username and username in prompt:
            logger.info(f"🔐 Автоматически предоставляем пароль для {username}")
            return password
        
        # Если это запрос кода для текущего пользователя
        elif "code" in prompt_lower and username and username in prompt:
            logger.info(f"📧 Получаем код верификации для {username}")
            if email and email_password:
                from instagram.email_utils_optimized import get_verification_code_from_email
                code = get_verification_code_from_email(email, email_password, max_attempts=5, delay_between_attempts=10)
                if code:
                    logger.info(f"✅ Код получен: {code}")
                    return code
                else:
                    logger.warning(f"❌ Не удалось получить код для {username}")
                    return ""
            else:
                logger.warning(f"❌ Email данные не предоставлены для {username}")
                return ""
    
    # Для всех остальных случаев возвращаем пустую строку
    logger.warning(f"⚠️ Неопознанный запрос input(): {prompt}")
    return ""

# Сохраняем оригинальную функцию input
_original_input = builtins.input

def apply_patch():
    """Применяет патч для функции input()"""
    builtins.input = global_input_handler
    logger.info("🔧 Глобальный патч input() применен")

def restore_patch():
    """Восстанавливает оригинальную функцию input()"""
    builtins.input = _original_input
    logger.info("🔄 Оригинальная функция input() восстановлена")

# Автоматически применяем патч при импорте
apply_patch() 
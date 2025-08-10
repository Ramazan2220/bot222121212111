#!/usr/bin/env python3
"""
Middleware для проактивной проверки доступа
Проверяет доступ пользователя при каждом сообщении
"""

import logging
from telegram import Update
from telegram.ext import CallbackContext
from utils.access_manager import has_access

logger = logging.getLogger(__name__)

class AccessCheckMiddleware:
    """Middleware для проверки доступа"""
    
    def __init__(self):
        self.blocked_users = set()  # Кеш заблокированных пользователей
        logger.info("🛡️ AccessCheckMiddleware инициализирован")
    
    def check_access(self, update: Update, context: CallbackContext) -> bool:
        """
        Проверяет доступ пользователя при каждом сообщении
        Возвращает True если доступ разрешен, False если запрещен
        """
        if not update.effective_user:
            return False
        
        user_id = update.effective_user.id
        
        # Быстрая проверка кеша заблокированных
        if user_id in self.blocked_users:
            return False
        
        # Проверяем доступ через Redis
        access_granted = has_access(user_id)
        
        if not access_granted:
            # Добавляем в кеш заблокированных
            self.blocked_users.add(user_id)
            
            # Отправляем сообщение о блокировке
            self._send_access_denied_message(update, context, user_id)
            
            logger.warning(f"🚫 Доступ запрещен для пользователя {user_id}")
            return False
        else:
            # Удаляем из кеша заблокированных (если был)
            self.blocked_users.discard(user_id)
            return True
    
    def _send_access_denied_message(self, update: Update, context: CallbackContext, user_id: int):
        """Отправляет сообщение о запрещенном доступе"""
        try:
            message = """🚫 **ДОСТУП ЗАПРЕЩЕН**

❌ У вас нет доступа к боту
🔑 Обратитесь к администратору для получения доступа

👨‍💼 **Контакты:**
📧 Email: support@yourbot.com
💬 Telegram: @admin

⚠️ Все сообщения будут игнорироваться до получения доступа."""

            if update.message:
                update.message.reply_text(message, parse_mode='Markdown')
            elif update.callback_query:
                update.callback_query.answer("🚫 Доступ запрещен")
                if update.callback_query.message:
                    update.callback_query.message.reply_text(message, parse_mode='Markdown')
                    
        except Exception as e:
            logger.error(f"Ошибка отправки сообщения о блокировке: {e}")

# Глобальный экземпляр middleware
access_middleware = AccessCheckMiddleware()

def access_required(func):
    """
    Декоратор для проверки доступа перед выполнением функции
    """
    def wrapper(update: Update, context: CallbackContext, *args, **kwargs):
        if access_middleware.check_access(update, context):
            return func(update, context, *args, **kwargs)
        else:
            # Доступ запрещен - функция не выполняется
            return
    
    return wrapper

def check_user_access(update: Update, context: CallbackContext) -> bool:
    """
    Простая функция для проверки доступа
    Используется в начале обработчиков
    """
    return access_middleware.check_access(update, context) 
"""
Middleware для проактивной проверки доступа
Проверяет доступ пользователя при каждом сообщении
"""

import logging
from telegram import Update
from telegram.ext import CallbackContext
from utils.access_manager import has_access

logger = logging.getLogger(__name__)

class AccessCheckMiddleware:
    """Middleware для проверки доступа"""
    
    def __init__(self):
        self.blocked_users = set()  # Кеш заблокированных пользователей
        logger.info("🛡️ AccessCheckMiddleware инициализирован")
    
    def check_access(self, update: Update, context: CallbackContext) -> bool:
        """
        Проверяет доступ пользователя при каждом сообщении
        Возвращает True если доступ разрешен, False если запрещен
        """
        if not update.effective_user:
            return False
        
        user_id = update.effective_user.id
        
        # Быстрая проверка кеша заблокированных
        if user_id in self.blocked_users:
            return False
        
        # Проверяем доступ через Redis
        access_granted = has_access(user_id)
        
        if not access_granted:
            # Добавляем в кеш заблокированных
            self.blocked_users.add(user_id)
            
            # Отправляем сообщение о блокировке
            self._send_access_denied_message(update, context, user_id)
            
            logger.warning(f"🚫 Доступ запрещен для пользователя {user_id}")
            return False
        else:
            # Удаляем из кеша заблокированных (если был)
            self.blocked_users.discard(user_id)
            return True
    
    def _send_access_denied_message(self, update: Update, context: CallbackContext, user_id: int):
        """Отправляет сообщение о запрещенном доступе"""
        try:
            message = """🚫 **ДОСТУП ЗАПРЕЩЕН**

❌ У вас нет доступа к боту
🔑 Обратитесь к администратору для получения доступа

👨‍💼 **Контакты:**
📧 Email: support@yourbot.com
💬 Telegram: @admin

⚠️ Все сообщения будут игнорироваться до получения доступа."""

            if update.message:
                update.message.reply_text(message, parse_mode='Markdown')
            elif update.callback_query:
                update.callback_query.answer("🚫 Доступ запрещен")
                if update.callback_query.message:
                    update.callback_query.message.reply_text(message, parse_mode='Markdown')
                    
        except Exception as e:
            logger.error(f"Ошибка отправки сообщения о блокировке: {e}")

# Глобальный экземпляр middleware
access_middleware = AccessCheckMiddleware()

def access_required(func):
    """
    Декоратор для проверки доступа перед выполнением функции
    """
    def wrapper(update: Update, context: CallbackContext, *args, **kwargs):
        if access_middleware.check_access(update, context):
            return func(update, context, *args, **kwargs)
        else:
            # Доступ запрещен - функция не выполняется
            return
    
    return wrapper

def check_user_access(update: Update, context: CallbackContext) -> bool:
    """
    Простая функция для проверки доступа
    Используется в начале обработчиков
    """
    return access_middleware.check_access(update, context) 
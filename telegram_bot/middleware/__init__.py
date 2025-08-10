#!/usr/bin/env python3
"""
Middleware для Telegram бота
Содержит декораторы для проверки подписки и доступа
"""

import logging
from functools import wraps
from telegram import Update
from telegram.ext import CallbackContext
from utils.access_manager import has_access

logger = logging.getLogger(__name__)

def subscription_required(func):
    """
    Декоратор для проверки подписки пользователя
    """
    @wraps(func)
    def wrapper(update: Update, context: CallbackContext, *args, **kwargs):
        user_id = update.effective_user.id
        
        if not has_access(user_id):
            update.message.reply_text(
                "🚫 **Доступ ограничен**\n\n"
                "❌ У вас нет активной подписки\n"
                "🔑 Обратитесь к администратору для получения доступа",
                parse_mode='Markdown'
            )
            return
        
        return func(update, context, *args, **kwargs)
    
    return wrapper

def trial_allowed(func):
    """
    Декоратор для функций, доступных в trial режиме
    """
    @wraps(func)
    def wrapper(update: Update, context: CallbackContext, *args, **kwargs):
        user_id = update.effective_user.id
        
        # Trial пользователи тоже имеют доступ через has_access
        if not has_access(user_id):
            update.message.reply_text(
                "🚫 **Доступ ограничен**\n\n"
                "❌ У вас нет доступа к боту\n"
                "🎯 Получите trial или полную подписку у администратора",
                parse_mode='Markdown'
            )
            return
        
        return func(update, context, *args, **kwargs)
    
    return wrapper

def premium_only(func):
    """
    Декоратор для premium функций
    """
    @wraps(func)
    def wrapper(update: Update, context: CallbackContext, *args, **kwargs):
        user_id = update.effective_user.id
        
        if not has_access(user_id):
            update.message.reply_text(
                "💎 **Premium функция**\n\n"
                "❌ Эта функция доступна только Premium пользователям\n"
                "🔑 Обратитесь к администратору для получения Premium доступа",
                parse_mode='Markdown'
            )
            return
        
        return func(update, context, *args, **kwargs)
    
    return wrapper

# Импортируем умную проверку доступа из нашего нового middleware
try:
    from .smart_access_check import check_user_access_smart, smart_access
    __all__ = ['subscription_required', 'trial_allowed', 'premium_only', 'check_user_access_smart', 'smart_access']
except ImportError:
    logger.warning("Smart access check middleware не найден")
    __all__ = ['subscription_required', 'trial_allowed', 'premium_only']

"""
Middleware для Telegram бота
Содержит декораторы для проверки подписки и доступа
"""

import logging
from functools import wraps
from telegram import Update
from telegram.ext import CallbackContext
from utils.access_manager import has_access

logger = logging.getLogger(__name__)

def subscription_required(func):
    """
    Декоратор для проверки подписки пользователя
    """
    @wraps(func)
    def wrapper(update: Update, context: CallbackContext, *args, **kwargs):
        user_id = update.effective_user.id
        
        if not has_access(user_id):
            update.message.reply_text(
                "🚫 **Доступ ограничен**\n\n"
                "❌ У вас нет активной подписки\n"
                "🔑 Обратитесь к администратору для получения доступа",
                parse_mode='Markdown'
            )
            return
        
        return func(update, context, *args, **kwargs)
    
    return wrapper

def trial_allowed(func):
    """
    Декоратор для функций, доступных в trial режиме
    """
    @wraps(func)
    def wrapper(update: Update, context: CallbackContext, *args, **kwargs):
        user_id = update.effective_user.id
        
        # Trial пользователи тоже имеют доступ через has_access
        if not has_access(user_id):
            update.message.reply_text(
                "🚫 **Доступ ограничен**\n\n"
                "❌ У вас нет доступа к боту\n"
                "🎯 Получите trial или полную подписку у администратора",
                parse_mode='Markdown'
            )
            return
        
        return func(update, context, *args, **kwargs)
    
    return wrapper

def premium_only(func):
    """
    Декоратор для premium функций
    """
    @wraps(func)
    def wrapper(update: Update, context: CallbackContext, *args, **kwargs):
        user_id = update.effective_user.id
        
        if not has_access(user_id):
            update.message.reply_text(
                "💎 **Premium функция**\n\n"
                "❌ Эта функция доступна только Premium пользователям\n"
                "🔑 Обратитесь к администратору для получения Premium доступа",
                parse_mode='Markdown'
            )
            return
        
        return func(update, context, *args, **kwargs)
    
    return wrapper

# Импортируем умную проверку доступа из нашего нового middleware
try:
    from .smart_access_check import check_user_access_smart, smart_access
    __all__ = ['subscription_required', 'trial_allowed', 'premium_only', 'check_user_access_smart', 'smart_access']
except ImportError:
    logger.warning("Smart access check middleware не найден")
    __all__ = ['subscription_required', 'trial_allowed', 'premium_only']



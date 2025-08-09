#!/usr/bin/env python3
"""
🛡️ БЕЗОПАСНАЯ ОБЁРТКА ДЛЯ ИЗОЛЯЦИИ ПОЛЬЗОВАТЕЛЕЙ
Простое решение без сложных патчей - автоматически подставляет user_id из контекста
"""

import logging
from typing import List, Optional
from telegram.ext import CallbackContext
from database.db_manager import get_instagram_accounts as _original_get_accounts
from database.db_manager import get_instagram_account as _original_get_account
from database.models import InstagramAccount

logger = logging.getLogger(__name__)

def get_user_instagram_accounts(context: CallbackContext = None, user_id: int = None) -> List[InstagramAccount]:
    """
    🔒 БЕЗОПАСНАЯ версия get_instagram_accounts с автоматической изоляцией
    
    Args:
        context: Telegram context (для извлечения user_id)
        user_id: Прямо переданный user_id (приоритет над context)
    
    Returns:
        List[InstagramAccount]: Аккаунты только для текущего пользователя
    """
    # Получаем user_id из разных источников
    if user_id is None and context:
        try:
            # Пытаемся извлечь из разных мест в context
            if hasattr(context, 'user_data') and 'user_id' in context.user_data:
                user_id = context.user_data['user_id']
            elif hasattr(context, 'user') and context.user:
                user_id = context.user.id
            elif hasattr(context, '_user_id'):
                user_id = context._user_id
        except Exception as e:
            logger.warning(f"⚠️ Не удалось извлечь user_id из context: {e}")
    
    if user_id is None:
        logger.error("❌ КРИТИЧЕСКАЯ ОШИБКА: Нет user_id для изоляции!")
        return []  # Возвращаем пустой список вместо всех аккаунтов
    
    logger.debug(f"🔒 Получение аккаунтов для пользователя {user_id}")
    return _original_get_accounts(user_id)

def get_user_instagram_account(account_id: int, context: CallbackContext = None, user_id: int = None) -> Optional[InstagramAccount]:
    """
    🔒 БЕЗОПАСНАЯ версия get_instagram_account с автоматической изоляцией
    
    Args:
        account_id: ID аккаунта Instagram
        context: Telegram context (для извлечения user_id)
        user_id: Прямо переданный user_id (приоритет над context)
    
    Returns:
        InstagramAccount: Аккаунт только если принадлежит пользователю
    """
    # Получаем user_id из разных источников
    if user_id is None and context:
        try:
            if hasattr(context, 'user_data') and 'user_id' in context.user_data:
                user_id = context.user_data['user_id']
            elif hasattr(context, 'user') and context.user:
                user_id = context.user.id
            elif hasattr(context, '_user_id'):
                user_id = context._user_id
        except Exception as e:
            logger.warning(f"⚠️ Не удалось извлечь user_id из context: {e}")
    
    if user_id is None:
        logger.error(f"❌ КРИТИЧЕСКАЯ ОШИБКА: Нет user_id для получения аккаунта {account_id}!")
        return None  # Возвращаем None вместо аккаунта
    
    logger.debug(f"🔒 Получение аккаунта {account_id} для пользователя {user_id}")
    return _original_get_account(account_id, user_id)

def extract_user_id_from_update(update, context: CallbackContext) -> Optional[int]:
    """Извлекает user_id из Update объекта"""
    try:
        if update.effective_user:
            return update.effective_user.id
        elif update.message and update.message.from_user:
            return update.message.from_user.id
        elif update.callback_query and update.callback_query.from_user:
            return update.callback_query.from_user.id
        return None
    except Exception as e:
        logger.error(f"Ошибка извлечения user_id: {e}")
        return None 
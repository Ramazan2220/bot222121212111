"""
Middleware для авторизации админов
"""

import logging
from functools import wraps
from typing import Callable, Any, List
from telegram import Update
from telegram.ext import CallbackContext

from ..config.admin_list import is_admin, has_permission, Permission, get_user_role
from ..config.settings import MESSAGES

logger = logging.getLogger(__name__)

def admin_required(func: Callable) -> Callable:
    """
    Декоратор для проверки прав админа
    """
    @wraps(func)
    def wrapper(update: Update, context: CallbackContext, *args, **kwargs):
        user_id = update.effective_user.id
        
        if not is_admin(user_id):
            logger.warning(f"Unauthorized access attempt from user {user_id}")
            update.message.reply_text(
                MESSAGES['unauthorized'], 
                parse_mode='HTML'
            )
            return
        
        logger.info(f"Admin {user_id} accessed {func.__name__}")
        return func(update, context, *args, **kwargs)
    
    return wrapper

def permission_required(permission: Permission):
    """
    Декоратор для проверки конкретного права доступа
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(update: Update, context: CallbackContext, *args, **kwargs):
            user_id = update.effective_user.id
            
            if not is_admin(user_id):
                logger.warning(f"Unauthorized access attempt from user {user_id}")
                update.message.reply_text(
                    MESSAGES['unauthorized'], 
                    parse_mode='HTML'
                )
                return
            
            if not has_permission(user_id, permission):
                role = get_user_role(user_id)
                logger.warning(f"Permission denied for user {user_id} (role: {role.value}) for {permission.value}")
                update.message.reply_text(
                    f"❌ <b>Недостаточно прав</b>\n\n"
                    f"Для выполнения этого действия требуется право: <code>{permission.value}</code>\n"
                    f"Ваша роль: <code>{role.value}</code>",
                    parse_mode='HTML'
                )
                return
            
            logger.info(f"Admin {user_id} used permission {permission.value} in {func.__name__}")
            return func(update, context, *args, **kwargs)
        
        return wrapper
    return decorator

class AdminAuthMiddleware:
    """
    Middleware класс для проверки авторизации
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def check_admin_access(self, update: Update, context: CallbackContext) -> bool:
        """Проверить доступ админа"""
        user_id = update.effective_user.id
        
        if not is_admin(user_id):
            self.logger.warning(f"Unauthorized access attempt from user {user_id}")
            
            # Попробуем отправить сообщение
            try:
                if update.message:
                    update.message.reply_text(
                        MESSAGES['unauthorized'], 
                        parse_mode='HTML'
                    )
                elif update.callback_query:
                    update.callback_query.answer("🔒 Доступ запрещен")
                    update.callback_query.message.reply_text(
                        MESSAGES['unauthorized'], 
                        parse_mode='HTML'
                    )
            except Exception as e:
                self.logger.error(f"Error sending unauthorized message: {e}")
            
            return False
        
        return True
    
    def check_permission(self, update: Update, context: CallbackContext, permission: Permission) -> bool:
        """Проверить конкретное право доступа"""
        user_id = update.effective_user.id
        
        if not self.check_admin_access(update, context):
            return False
        
        if not has_permission(user_id, permission):
            role = get_user_role(user_id)
            self.logger.warning(f"Permission denied for user {user_id} (role: {role.value}) for {permission.value}")
            
            try:
                error_msg = (
                    f"❌ <b>Недостаточно прав</b>\n\n"
                    f"Для выполнения этого действия требуется право: <code>{permission.value}</code>\n"
                    f"Ваша роль: <code>{role.value}</code>"
                )
                
                if update.message:
                    update.message.reply_text(error_msg, parse_mode='HTML')
                elif update.callback_query:
                    update.callback_query.answer("❌ Недостаточно прав")
                    update.callback_query.message.reply_text(error_msg, parse_mode='HTML')
            except Exception as e:
                self.logger.error(f"Error sending permission denied message: {e}")
            
            return False
        
        return True
    
    def get_user_info(self, user_id: int) -> dict:
        """Получить информацию о пользователе-админе"""
        if not is_admin(user_id):
            return {}
        
        role = get_user_role(user_id)
        permissions = has_permission(user_id, Permission.VIEW_USERS)  # Проверим хотя бы одно право
        
        return {
            'user_id': user_id,
            'role': role.value,
            'is_admin': True,
            'has_basic_permissions': permissions
        } 
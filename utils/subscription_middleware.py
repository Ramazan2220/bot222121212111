"""
Middleware для проверки подписок пользователей
Декораторы для интеграции проверки доступа во все модули
"""

import logging
from functools import wraps
from typing import Callable, Any
from telegram import Update
from telegram.ext import CallbackContext

from utils.subscription_service import subscription_service

logger = logging.getLogger(__name__)

def subscription_required(allow_trial: bool = True, send_message: bool = True):
    """
    Декоратор для проверки подписки пользователя
    
    Args:
        allow_trial: Разрешить доступ пользователям на триале
        send_message: Отправлять сообщение об ошибке доступа
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(update: Update, context: CallbackContext, *args, **kwargs) -> Any:
            try:
                user_id = update.effective_user.id
                username = update.effective_user.username
                
                # Обновляем активность пользователя
                subscription_service.update_user_activity(user_id)
                
                # Проверяем доступ
                access_info = subscription_service.check_user_access(user_id)
                
                # Логируем попытку доступа
                logger.info(f"🔐 Пользователь {user_id} (@{username}) пытается получить доступ: {access_info['status']}")
                
                # Если доступ разрешен
                if access_info['has_access']:
                    # Проверяем, можно ли триальным пользователям использовать эту функцию
                    if not allow_trial and access_info['is_trial']:
                        if send_message:
                            message = "🔒 Эта функция доступна только для полных подписок.\nОформите платную подписку для получения доступа."
                            if update.callback_query:
                                update.callback_query.answer(message, show_alert=True)
                            else:
                                update.message.reply_text(message)
                        return None
                    
                    # Выполняем основную функцию
                    return func(update, context, *args, **kwargs)
                
                # Доступ запрещен
                if access_info['status'] == 'not_registered':
                    # Предлагаем триал
                    if send_message:
                        message = (
                            "🚫 У вас нет доступа к системе.\n\n"
                            "💡 Хотите получить бесплатный триал на 1 день?\n"
                            "Обратитесь к администратору: @admin"
                        )
                        if update.callback_query:
                            update.callback_query.answer(message, show_alert=True)
                        else:
                            update.message.reply_text(message)
                elif access_info['status'] == 'expired':
                    if send_message:
                        message = (
                            "⏰ Ваша подписка истекла.\n\n"
                            "💳 Продлите подписку для продолжения работы.\n"
                            "Обратитесь к администратору: @admin"
                        )
                        if update.callback_query:
                            update.callback_query.answer(message, show_alert=True)
                        else:
                            update.message.reply_text(message)
                elif access_info['status'] == 'blocked':
                    if send_message:
                        message = (
                            "🚫 Ваш аккаунт заблокирован.\n\n"
                            "📞 Для разблокировки обратитесь к администратору: @admin"
                        )
                        if update.callback_query:
                            update.callback_query.answer(message, show_alert=True)
                        else:
                            update.message.reply_text(message)
                else:
                    if send_message:
                        message = access_info.get('message', '❌ Нет доступа к системе')
                        if update.callback_query:
                            update.callback_query.answer(message, show_alert=True)
                        else:
                            update.message.reply_text(message)
                
                return None
                
            except Exception as e:
                logger.error(f"Ошибка проверки подписки в {func.__name__}: {e}")
                if send_message:
                    message = "❌ Ошибка проверки доступа. Попробуйте позже."
                    if update.callback_query:
                        update.callback_query.answer(message, show_alert=True)
                    else:
                        update.message.reply_text(message)
                return None
        
        return wrapper
    return decorator

def trial_allowed(func: Callable) -> Callable:
    """Сокращенный декоратор для функций, доступных триальным пользователям"""
    return subscription_required(allow_trial=True, send_message=True)(func)

def premium_only(func: Callable) -> Callable:
    """Сокращенный декоратор для функций, доступных только платным пользователям"""
    return subscription_required(allow_trial=False, send_message=True)(func)

def check_subscription_silent(func: Callable) -> Callable:
    """Проверка подписки без отправки сообщения об ошибке"""
    return subscription_required(allow_trial=True, send_message=False)(func)

def get_user_subscription_info(user_id: int) -> dict:
    """Получает информацию о подписке пользователя"""
    return subscription_service.get_user_stats(user_id)

def create_trial_user_if_needed(user_id: int, username: str = None) -> bool:
    """
    Создает триального пользователя если он не существует
    Используется для автоматической регистрации новых пользователей
    """
    try:
        access_info = subscription_service.check_user_access(user_id)
        
        if access_info['status'] == 'not_registered':
            user = subscription_service.create_trial_user(user_id, username)
            if user:
                logger.info(f"Автоматически создан триальный пользователь {user_id}")
                return True
        
        return False
        
    except Exception as e:
        logger.error(f"Ошибка создания триального пользователя {user_id}: {e}")
        return False 
"""
Middleware для проверки доступа пользователей к функциям бота
"""
import logging
from functools import wraps
from telegram import Update
from telegram.ext import CallbackContext

logger = logging.getLogger(__name__)

def subscription_required(func):
    """Декоратор для проверки активной подписки пользователя"""
    @wraps(func)
    def wrapper(update: Update, context: CallbackContext, *args, **kwargs):
        user_id = update.effective_user.id
        username = update.effective_user.username or "Неизвестно"
        
        # Получаем статус подписки
        from utils.subscription_service import subscription_service
        access_info = subscription_service.check_user_access(user_id)
        
        if not access_info['has_access']:
            # Пользователь не имеет доступа
            status = access_info.get('status', 'unknown')
            
            blocked_message = f"🔒 **Доступ ограничен**\n\n"
            blocked_message += f"👤 @{username} (ID: `{user_id}`)\n"
            blocked_message += f"📊 Статус: {access_info.get('message', 'Нет доступа')}\n\n"
            
            if status == 'not_registered':
                blocked_message += "💳 **Для использования бота необходима подписка:**\n"
                blocked_message += "• 🆓 Триал 1-7 дней - Бесплатно\n"
                blocked_message += "• 💳 1 месяц - $200\n"
                blocked_message += "• 💳 3 месяца - $400\n"
                blocked_message += "• 💎 Навсегда - $500\n\n"
                blocked_message += f"📞 Обратитесь к администратору: @admin\n"
                blocked_message += f"📨 Сообщите ваш ID: `{user_id}`"
            elif status == 'expired':
                blocked_message += "⏰ **Подписка истекла. Продлите для продолжения работы.**\n"
                blocked_message += f"📞 Обратитесь к администратору: @admin"
            elif status == 'blocked':
                blocked_message += "🚫 **Аккаунт заблокирован.**\n"
                blocked_message += f"📞 Обратитесь к администратору: @admin"
            
            if update.message:
                update.message.reply_text(blocked_message, parse_mode='Markdown')
            elif update.callback_query:
                update.callback_query.answer("🔒 Доступ ограничен. Необходима подписка.")
                if update.callback_query.message:
                    update.callback_query.message.reply_text(blocked_message, parse_mode='Markdown')
            
            logger.warning(f"🔒 Заблокирован доступ для пользователя {user_id} (@{username}) к функции {func.__name__}")
            return
        
        # Пользователь имеет доступ - выполняем функцию
        logger.info(f"✅ Доступ разрешен для пользователя {user_id} (@{username}) к функции {func.__name__}")
        return func(update, context, *args, **kwargs)
    
    return wrapper

def admin_only(func):
    """Декоратор для функций только для администраторов"""
    @wraps(func)
    def wrapper(update: Update, context: CallbackContext, *args, **kwargs):
        user_id = update.effective_user.id
        
        # Список ID администраторов (можно вынести в конфиг)
        admin_ids = [6499246016]  # Замените на реальные ID админов
        
        if user_id not in admin_ids:
            error_message = "❌ Эта функция доступна только администраторам."
            
            if update.message:
                update.message.reply_text(error_message)
            elif update.callback_query:
                update.callback_query.answer(error_message)
            
            logger.warning(f"🚫 Попытка доступа к админ функции {func.__name__} от пользователя {user_id}")
            return
        
        return func(update, context, *args, **kwargs)
    
    return wrapper

def trial_allowed(func):
    """Декоратор для функций доступных на триале"""
    @wraps(func)
    def wrapper(update: Update, context: CallbackContext, *args, **kwargs):
        user_id = update.effective_user.id
        
        # Получаем статус подписки
        from utils.subscription_service import subscription_service
        access_info = subscription_service.check_user_access(user_id)
        
        # Разрешаем доступ если есть активная подписка (включая триал)
        if access_info['has_access']:
            return func(update, context, *args, **kwargs)
        
        # Блокируем доступ
        blocked_message = "🔒 **Функция недоступна без подписки**\n\n"
        blocked_message += "💳 Получите доступ:\n"
        blocked_message += f"📞 Напишите администратору: @admin\n"
        blocked_message += f"📨 Ваш ID: `{user_id}`"
        
        if update.message:
            update.message.reply_text(blocked_message, parse_mode='Markdown')
        elif update.callback_query:
            update.callback_query.answer("🔒 Необходима подписка")
        
        return
    
    return wrapper

def premium_only(func):
    """Декоратор для функций только для платных подписок"""
    @wraps(func)
    def wrapper(update: Update, context: CallbackContext, *args, **kwargs):
        user_id = update.effective_user.id
        
        # Получаем статус подписки
        from utils.subscription_service import subscription_service
        access_info = subscription_service.check_user_access(user_id)
        
        # Проверяем что есть доступ и это не триал
        if access_info['has_access'] and not access_info.get('is_trial', False):
            return func(update, context, *args, **kwargs)
        
        # Блокируем доступ
        if access_info.get('is_trial', False):
            blocked_message = "💎 **Эта функция доступна только в платной версии**\n\n"
            blocked_message += "💳 Обновите подписку:\n"
            blocked_message += "• 1 месяц - $200\n"
            blocked_message += "• 3 месяца - $400\n"
            blocked_message += "• Навсегда - $500\n\n"
            blocked_message += f"📞 Обратитесь к администратору: @admin"
        else:
            blocked_message = "🔒 **Функция недоступна без подписки**\n\n"
            blocked_message += f"📞 Напишите администратору: @admin"
        
        if update.message:
            update.message.reply_text(blocked_message, parse_mode='Markdown')
        elif update.callback_query:
            update.callback_query.answer("💎 Только для платной подписки")
        
        return
    
    return wrapper 
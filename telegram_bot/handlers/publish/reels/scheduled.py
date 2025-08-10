"""
Обработчики для планирования публикации Reels
"""

import logging
from datetime import datetime, timedelta

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, ParseMode
from telegram.ext import ConversationHandler, MessageHandler, Filters

from ..common import is_admin, format_scheduled_time
from ..states import CHOOSE_SCHEDULE
from .handlers import reels_handler

logger = logging.getLogger(__name__)


def start_schedule_reels(update, context):
    """Начинает процесс планирования Reels используя существующий механизм публикации"""
    # Устанавливаем флаг что это запланированные Reels
    context.user_data['is_scheduled_post'] = True
    context.user_data['publish_type'] = 'reels'
    
    # Используем существующий механизм выбора аккаунтов для Reels
    from .handlers import start_reels_publish
    return start_reels_publish(update, context)


def handle_schedule_time_input(update, context):
    """Обработка ввода времени для планирования Reels"""
    time_text = update.message.text.strip()
    
    try:
        # Парсим время
        scheduled_time = datetime.strptime(time_text, "%d.%m.%Y %H:%M")
        
        # Проверяем, что время в будущем
        if scheduled_time <= datetime.now():
            update.message.reply_text(
                "❌ Время должно быть в будущем!\n"
                "Попробуйте еще раз."
            )
            return CHOOSE_SCHEDULE
        
        # Сохраняем время
        context.user_data['scheduled_time'] = scheduled_time
        
        # Создаем запланированные задачи
        task_ids = reels_handler.create_publish_tasks(context, scheduled_time)
        
        if task_ids:
            message = f"✅ Запланировано Reels: {len(task_ids)}\n\n"
            message += f"📅 Время публикации: {format_scheduled_time(scheduled_time)}\n\n"
            message += "Reels будут опубликованы автоматически в указанное время."
            
            keyboard = [
                [InlineKeyboardButton("📅 Запланированные", callback_data="scheduled_posts")],
                [InlineKeyboardButton("🏠 Главное меню", callback_data="start_menu")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            update.message.reply_text(message, reply_markup=reply_markup)
        else:
            update.message.reply_text("❌ Ошибка при создании запланированных задач")
        
        # Очищаем данные
        reels_handler.cleanup_user_data(context)
        
        return ConversationHandler.END
        
    except ValueError:
        update.message.reply_text(
            "❌ Неверный формат времени!\n\n"
            "Используйте формат: `ДД.ММ.ГГГГ ЧЧ:ММ`\n"
            "Например: `25.12.2023 15:30`",
            parse_mode=ParseMode.MARKDOWN
        )
        return CHOOSE_SCHEDULE


def schedule_reels_publish(update, context, scheduled_time, user_id):
    """Создает запланированные задачи для публикации Reels"""
    from .handlers import reels_handler
    
    try:
        # Сохраняем время
        context.user_data['scheduled_time'] = scheduled_time
        
        # Создаем запланированные задачи
        task_ids = reels_handler.create_publish_tasks(context, scheduled_time)
        
        if task_ids:
            message = f"✅ Запланировано Reels: {len(task_ids)}\n\n"
            message += f"📅 Время публикации: {format_scheduled_time(scheduled_time)}\n\n"
            message += "Reels будут опубликованы автоматически в указанное время."
            
            keyboard = [
                [InlineKeyboardButton("📅 Запланированные", callback_data="scheduled_posts")],
                [InlineKeyboardButton("🏠 Главное меню", callback_data="start_menu")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            if update.message:
                update.message.reply_text(message, reply_markup=reply_markup)
            else:
                update.callback_query.edit_message_text(message, reply_markup=reply_markup)
        else:
            error_message = "❌ Ошибка при создании запланированных задач"
            if update.message:
                update.message.reply_text(error_message)
            else:
                update.callback_query.edit_message_text(error_message)
        
        # Очищаем данные
        reels_handler.cleanup_user_data(context)
        
        return ConversationHandler.END
        
    except Exception as e:
        logger.error(f"Error scheduling reels publish: {e}")
        error_message = "❌ Ошибка при планировании публикации"
        if update.message:
            update.message.reply_text(error_message)
        else:
            update.callback_query.edit_message_text(error_message)
        return ConversationHandler.END 
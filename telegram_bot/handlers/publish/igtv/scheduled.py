"""
Обработчики для планирования публикации IGTV
"""

import logging

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

logger = logging.getLogger(__name__)


def start_schedule_igtv(update, context):
    """Планирование IGTV с информативной заглушкой"""
    message = (
        "🎬 *Планирование IGTV*\n\n"
        "🚧 Модуль планирования IGTV находится в разработке\n\n"
        "📋 *Планируемые функции:*\n"
        "• Планирование длинных видео\n"
        "• Автоматическая публикация по расписанию\n"
        "• Массовое планирование\n"
        "• Уникализация контента\n\n"
        "⏰ *Ожидаемый срок:* Ближайшие обновления\n\n"
        "📱 Пока что используйте планирование:\n"
        "• 📸 Постов\n"
        "• 📱 Stories\n"
        "• 🎥 Reels"
    )
    
    keyboard = [
        [
            InlineKeyboardButton("📸 Запланировать пост", callback_data="schedule_post"),
            InlineKeyboardButton("🎥 Запланировать Reels", callback_data="schedule_reels")
        ],
        [
            InlineKeyboardButton("📱 Запланировать Stories", callback_data="schedule_story")
        ],
        [InlineKeyboardButton("🔙 Назад", callback_data="menu_scheduled")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    if update.callback_query:
        update.callback_query.edit_message_text(
            message,
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )
    else:
        update.message.reply_text(
            message,
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )
    
    return None


def schedule_igtv_publish(context, task_id: int):
    """Выполнение запланированной публикации IGTV (заглушка)"""
    logger.warning(f"Scheduled IGTV task {task_id} - module not implemented yet")
    pass 
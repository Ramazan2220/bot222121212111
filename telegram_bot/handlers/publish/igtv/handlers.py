"""
Обработчики для публикации IGTV
"""

import logging

from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ConversationHandler, CallbackQueryHandler

from ..common import is_admin

logger = logging.getLogger(__name__)


def start_igtv_publish(update, context):
    """Запуск публикации IGTV с информативной заглушкой"""
    user_id = update.effective_user.id
    
    if not is_admin(user_id):
        if update.message:
            update.message.reply_text("У вас нет прав для выполнения этой команды.")
        else:
            update.callback_query.answer("У вас нет прав", show_alert=True)
        return ConversationHandler.END
    
    # Показываем информативную заглушку
    message = (
        "🎬 *IGTV публикация*\n\n"
        "🚧 Модуль находится в активной разработке\n\n"
        "📋 *Планируемые функции:*\n"
        "• Загрузка длинных видео (до 60 минут)\n"
        "• Автоматическая обрезка превью\n"
        "• Настройка обложки\n"
        "• Добавление описания и хештегов\n"
        "• Планирование публикации\n"
        "• Уникализация для множественной публикации\n\n"
        "⏰ *Ожидаемый срок:* Ближайшие обновления\n\n"
        "📱 Пока что используйте:\n"
        "• 📸 Публикация постов\n"
        "• 📱 Stories\n"
        "• 🎥 Reels\n\n"
        "Спасибо за понимание! 🙏"
    )
    
    keyboard = [
        [
            InlineKeyboardButton("📸 Посты", callback_data="publish_post"),
            InlineKeyboardButton("🎥 Reels", callback_data="publish_reels")
        ],
        [
            InlineKeyboardButton("📱 Stories", callback_data="publish_story")
        ],
        [InlineKeyboardButton("🔙 Назад к публикациям", callback_data="menu_publications")]
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
    
    return ConversationHandler.END


def get_igtv_conversation():
    """Возвращает минимальный ConversationHandler для IGTV"""
    return ConversationHandler(
        entry_points=[
            CallbackQueryHandler(start_igtv_publish, pattern='^publish_igtv$')
        ],
        states={},
        fallbacks=[],
        per_message=False
    )


def get_igtv_handlers():
    """Возвращает обработчики для IGTV"""
    return [
        get_igtv_conversation(),
        CallbackQueryHandler(start_igtv_publish, pattern='^publish_igtv$')
    ] 
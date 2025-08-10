"""
Общие функции и импорты для модулей публикации
"""

import os
import tempfile
import json
import logging
import threading
from datetime import datetime

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, ParseMode
from telegram.ext import ConversationHandler

from database.db_manager import get_instagram_account, get_instagram_accounts, create_publish_task
from database.models import TaskType, TaskStatus
from utils.task_queue import add_task_to_queue, get_task_status
from telegram_bot.utils.account_selection import create_account_selector

# Добавляем импорт для uuid
import uuid

logger = logging.getLogger(__name__)

# Общие состояния для публикации
CHOOSE_ACCOUNT = 'choose_account'
UPLOAD_MEDIA = 'upload_media'
ENTER_CAPTION = 'enter_caption'
ENTER_HASHTAGS = 'enter_hashtags'
CONFIRM_PUBLISH = 'confirm_publish'
CHOOSE_SCHEDULE = 'choose_schedule'

def is_admin(user_id):
    """Проверяет, является ли пользователь администратором"""
    from telegram_bot.bot import is_admin as bot_is_admin
    return bot_is_admin(user_id)

def cleanup_user_data(context):
    """Очищает данные пользователя после завершения публикации"""
    keys_to_remove = [
        'publish_type', 'publish_media_type', 'publish_account_ids', 
        'publish_account_usernames', 'publish_account_id', 'publish_account_username',
        'publish_media_files', 'publish_caption', 'publish_hashtags',
        'publish_to_all_accounts', 'is_scheduled_post', 'schedule_publish_type',
        'waiting_for_schedule_time', 'story_text', 'story_mentions', 'story_link',
        'story_text_color', 'story_text_position', 'reels_caption', 'reels_hashtags',
        'reels_options', 'selected_post_accounts'
    ]
    
    for key in keys_to_remove:
        context.user_data.pop(key, None)

def cancel_publish(update, context):
    """Отменяет публикацию и очищает данные"""
    query = update.callback_query
    if query:
        query.answer()
        query.edit_message_text(
            "❌ Публикация отменена",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 В главное меню", callback_data="start_menu")
            ]])
        )
    else:
        update.message.reply_text("❌ Публикация отменена")
    
    cleanup_user_data(context)
    return ConversationHandler.END

def show_scheduled_posts(update, context):
    """Показывает запланированные публикации"""
    query = update.callback_query
    query.answer()
    
    from database.db_manager import get_session
    from database.models import PublishTask, TaskStatus
    
    session = get_session()
    try:
        # Получаем запланированные задачи
        scheduled_tasks = session.query(PublishTask).filter(
            PublishTask.status == TaskStatus.SCHEDULED
        ).order_by(PublishTask.scheduled_time).limit(10).all()
        
        if not scheduled_tasks:
            query.edit_message_text(
                "📅 Нет запланированных публикаций",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Назад", callback_data="menu_publications")]
                ])
            )
            return
        
        message = "📅 Запланированные публикации:\n\n"
        
        for task in scheduled_tasks:
            account = get_instagram_account(task.account_id)
            
            scheduled_time = task.scheduled_time.strftime("%d.%m.%Y %H:%M")
            task_type = task.task_type.value if hasattr(task.task_type, 'value') else str(task.task_type)
            
            message += f"• *{task_type.upper()}* в @{account.username if account else 'Unknown'}\n"
            message += f"  📅 {scheduled_time}\n"
            message += f"  📝 {task.caption[:50]}{'...' if len(task.caption or '') > 50 else ''}\n\n"
        
        keyboard = [
            [InlineKeyboardButton("🔄 Обновить", callback_data="scheduled_posts")],
            [InlineKeyboardButton("🔙 Назад", callback_data="menu_publications")]
        ]
        
        query.edit_message_text(
            message,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN
        )
        
    except Exception as e:
        logger.error(f"Ошибка при получении запланированных публикаций: {e}")
        query.edit_message_text(
            "❌ Ошибка при загрузке запланированных публикаций",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Назад", callback_data="menu_publications")]
            ])
        )
    finally:
        session.close()

def show_publication_history(update, context):
    """Показывает историю публикаций"""
    query = update.callback_query
    query.answer()
    
    from database.db_manager import get_session
    from database.models import PublishTask, TaskStatus
    
    session = get_session()
    try:
        # Получаем последние выполненные задачи
        completed_tasks = session.query(PublishTask).filter(
            PublishTask.status.in_([TaskStatus.COMPLETED, TaskStatus.FAILED])
        ).order_by(PublishTask.completed_time.desc()).limit(10).all()
        
        if not completed_tasks:
            query.edit_message_text(
                "📊 История публикаций пуста",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Назад", callback_data="menu_publications")]
                ])
            )
            return
        
        message = "📊 *История публикаций:*\n\n"
        
        for task in completed_tasks:
            account = get_instagram_account(task.account_id)
            
            completed_time = task.completed_time.strftime("%d.%m.%Y %H:%M") if task.completed_time else "Не завершена"
            task_type = task.task_type.value if hasattr(task.task_type, 'value') else str(task.task_type)
            status_emoji = "✅" if task.status == TaskStatus.COMPLETED else "❌"
            
            message += f"{status_emoji} *{task_type.upper()}* в @{account.username if account else 'Unknown'}\n"
            message += f"  📅 {completed_time}\n"
            
            if task.status == TaskStatus.FAILED and task.error_message:
                message += f"  ❌ {task.error_message[:50]}{'...' if len(task.error_message) > 50 else ''}\n"
            elif task.media_id:
                message += f"  🔗 ID: {task.media_id}\n"
            
            message += "\n"
        
        keyboard = [
            [InlineKeyboardButton("🔄 Обновить", callback_data="publication_history")],
            [InlineKeyboardButton("🔙 Назад", callback_data="menu_publications")]
        ]
        
        query.edit_message_text(
            message,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN
        )
        
    except Exception as e:
        logger.error(f"Ошибка при получении истории публикаций: {e}")
        query.edit_message_text(
            "❌ Ошибка при загрузке истории публикаций",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Назад", callback_data="menu_publications")]
            ])
        )
    finally:
        session.close()

def check_task_status_handler(update, context):
    """Проверяет статус задачи публикации"""
    query = update.callback_query
    query.answer()
    
    # Здесь можно добавить логику проверки статуса конкретной задачи
    # Пока что показываем общую информацию
    query.edit_message_text(
        "🔍 Проверка статуса задач...\n\n"
        "Эта функция в разработке",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 Назад", callback_data="menu_publications")]
        ])
    )


def get_accounts_by_folder(folder_name):
    """Получает аккаунты из определенной папки"""
    from database.db_manager import get_instagram_accounts
    accounts = get_instagram_accounts()
    return [acc for acc in accounts if acc.is_active and acc.folder == folder_name]


def format_scheduled_time(scheduled_time):
    """Форматирует время для отображения"""
    if scheduled_time:
        return scheduled_time.strftime("%d.%m.%Y %H:%M")
    return "Не запланировано"


def show_publish_confirmation(update, context, is_callback=False):
    """Показывает общее подтверждение публикации"""
    # Получаем данные
    publish_type = context.user_data.get('publish_type', 'post')
    account_ids = context.user_data.get('publish_account_ids', [])
    caption = context.user_data.get('publish_caption', '')
    hashtags = context.user_data.get('publish_hashtags', [])
    is_scheduled = context.user_data.get('is_scheduled_post', False)
    
    # Информация об аккаунтах
    if len(account_ids) == 1:
        account = get_instagram_account(account_ids[0])
        account_info = f"👤 Аккаунт: @{account.username}"
    else:
        account_info = f"👥 Аккаунты: {len(account_ids)} шт."
    
    # Клавиатура
    if is_scheduled:
        keyboard = [
            [InlineKeyboardButton("🗓️ Выбрать время", callback_data='schedule_publish')],
            [InlineKeyboardButton("❌ Отмена", callback_data='cancel_publish')]
        ]
    else:
        keyboard = [
            [
                InlineKeyboardButton("✅ Опубликовать", callback_data='confirm_publish_now'),
                InlineKeyboardButton("⏰ Запланировать", callback_data='schedule_publish')
            ],
            [InlineKeyboardButton("❌ Отмена", callback_data='cancel_publish')]
        ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    # Сообщение
    type_names = {
        'post': 'Пост',
        'story': 'История',
        'reels': 'Reels',
        'igtv': 'IGTV'
    }
    
    message = f"*Подтверждение публикации*\n\n"
    message += f"{account_info}\n"
    message += f"📄 Тип: {type_names.get(publish_type, 'Публикация')}\n"
    
    if caption:
        message += f"✏️ Подпись: {caption[:50]}{'...' if len(caption) > 50 else ''}\n"
    
    if hashtags:
        message += f"#️⃣ Хештеги: {' '.join(hashtags[:5])}{'...' if len(hashtags) > 5 else ''}\n"
    
    message += "\nЧто вы хотите сделать?"
    
    if is_callback:
        update.callback_query.edit_message_text(
            message, 
            reply_markup=reply_markup,
            parse_mode=ParseMode.MARKDOWN
        )
    else:
        update.message.reply_text(
            message,
            reply_markup=reply_markup, 
            parse_mode=ParseMode.MARKDOWN
    ) 
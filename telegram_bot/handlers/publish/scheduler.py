"""
Модуль для планирования публикаций
"""

import logging
from datetime import datetime, timedelta

from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ConversationHandler

from .common import (
    logger, cleanup_user_data, cancel_publish, format_scheduled_time,
    create_time_selection_keyboard, create_minute_selection_keyboard,
    CHOOSE_SCHEDULE
)

from database.db_manager import create_publish_task
from database.models import TaskType, TaskStatus

def choose_schedule(update, context):
    """Выбор времени для планирования публикации"""
    query = update.callback_query
    query.answer()
    
    # Создаем клавиатуру для выбора времени
    keyboard = []
    
    # Быстрые варианты времени
    now = datetime.now()
    
    # Через 1 час
    hour_later = now + timedelta(hours=1)
    keyboard.append([InlineKeyboardButton(
        f"⏰ Через 1 час ({hour_later.strftime('%H:%M')})",
        callback_data=f"quick_schedule_{hour_later.strftime('%Y%m%d_%H%M')}"
    )])
    
    # Через 3 часа
    three_hours_later = now + timedelta(hours=3)
    keyboard.append([InlineKeyboardButton(
        f"⏰ Через 3 часа ({three_hours_later.strftime('%H:%M')})",
        callback_data=f"quick_schedule_{three_hours_later.strftime('%Y%m%d_%H%M')}"
    )])
    
    # Завтра в то же время
    tomorrow = now + timedelta(days=1)
    keyboard.append([InlineKeyboardButton(
        f"📅 Завтра в {tomorrow.strftime('%H:%M')}",
        callback_data=f"quick_schedule_{tomorrow.strftime('%Y%m%d_%H%M')}"
    )])
    
    # Выбрать конкретное время
    keyboard.append([InlineKeyboardButton(
        "🕐 Выбрать конкретное время",
        callback_data="custom_schedule"
    )])
    
    # Кнопки навигации
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="back_to_confirmation")])
    keyboard.append([InlineKeyboardButton("❌ Отмена", callback_data="cancel_publish")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    query.edit_message_text(
        "🗓️ Выберите время для публикации:",
        reply_markup=reply_markup
    )
    return CHOOSE_SCHEDULE

def handle_quick_schedule(update, context):
    """Обработка быстрого выбора времени"""
    query = update.callback_query
    query.answer()
    
    # Извлекаем время из callback_data
    time_str = query.data.split('_')[-1]
    scheduled_time = datetime.strptime(time_str, '%Y%m%d_%H%M')
    
    return schedule_publication(update, context, scheduled_time)

def handle_custom_schedule(update, context):
    """Обработка выбора конкретного времени"""
    query = update.callback_query
    query.answer()
    
    # Показываем календарь для выбора даты
    keyboard = create_time_selection_keyboard()
    
    query.edit_message_text(
        "🕐 Выберите час для публикации:",
        reply_markup=keyboard
    )
    return CHOOSE_SCHEDULE

def handle_hour_selection(update, context):
    """Обработка выбора часа"""
    query = update.callback_query
    query.answer()
    
    hour = int(query.data.split('_')[-1])
    context.user_data['selected_hour'] = hour
    
    # Показываем выбор минут
    keyboard = create_minute_selection_keyboard(hour)
    
    query.edit_message_text(
        f"🕐 Выберите минуты для {hour:02d}:xx:",
        reply_markup=keyboard
    )
    return CHOOSE_SCHEDULE

def handle_minute_selection(update, context):
    """Обработка выбора минут"""
    query = update.callback_query
    query.answer()
    
    # Извлекаем час и минуты
    parts = query.data.split('_')
    hour = int(parts[-2])
    minute = int(parts[-1])
    
    # Создаем время для сегодня
    now = datetime.now()
    scheduled_time = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    
    # Если время уже прошло, планируем на завтра
    if scheduled_time <= now:
        scheduled_time += timedelta(days=1)
    
    return schedule_publication(update, context, scheduled_time)

def schedule_publication(update, context, scheduled_time):
    """Планирование публикации на указанное время"""
    query = update.callback_query
    user_id = update.effective_user.id
    
    try:
        # Получаем данные для публикации
        publish_type = context.user_data.get('publish_type', 'post')
        media_path = context.user_data.get('publish_media_path')
        media_paths = context.user_data.get('publish_media_paths', [])
        media_type = context.user_data.get('publish_media_type')
        caption = context.user_data.get('publish_caption', '')
        hashtags = context.user_data.get('publish_hashtags', '')
        
        # Объединяем подпись и хештеги
        full_caption = caption
        if hashtags:
            if full_caption:
                full_caption += f"\n\n{hashtags}"
            else:
                full_caption = hashtags
        
        account_ids = context.user_data.get('publish_account_ids', [])
        
        # Определяем тип задачи
        task_type_map = {
            'post': TaskType.POST,
            'scheduled_post': TaskType.POST,
            'story': TaskType.STORY,
            'reels': TaskType.REELS,
            'igtv': TaskType.IGTV
        }
        
        task_type = task_type_map.get(publish_type, TaskType.POST)
        
        # Создаем задачи для каждого аккаунта
        task_ids = []
        for account_id in account_ids:
            task_id = create_publish_task(
                user_id=user_id,
                task_type=task_type,
                account_id=account_id,
                media_path=media_path or str(media_paths),
                caption=full_caption,
                scheduled_time=scheduled_time,
                status=TaskStatus.SCHEDULED
            )
            task_ids.append(task_id)
        
        # Определяем тип публикации для сообщения
        type_names = {
            'post': 'Пост',
            'scheduled_post': 'Пост', 
            'story': 'История',
            'reels': 'Reels',
            'igtv': 'IGTV'
        }
        
        type_name = type_names.get(publish_type, 'Пост')
        
        # Отправляем подтверждение
        if len(account_ids) == 1:
            account_username = context.user_data.get('publish_account_username')
            message = f"✅ {type_name} запланирован на {format_scheduled_time(scheduled_time)}!\n\n"
            message += f"👤 Аккаунт: @{account_username}\n"
            message += f"📱 Медиа: {media_type}\n"
            message += f"🆔 ID задачи: {task_ids[0]}"
        else:
            message = f"✅ {type_name} запланирован на {format_scheduled_time(scheduled_time)}!\n\n"
            message += f"👥 Аккаунты: {len(account_ids)} аккаунтов\n"
            message += f"📱 Медиа: {media_type}\n"
            message += f"🆔 ID задач: {', '.join(map(str, task_ids))}"
        
        query.edit_message_text(message)
        
        # Очищаем данные пользователя
        cleanup_user_data(context)
        
        return ConversationHandler.END
        
    except Exception as e:
        logger.error(f"Ошибка при планировании публикации: {e}")
        query.edit_message_text(f"❌ Ошибка при планировании: {str(e)}")
        return ConversationHandler.END

def schedule_story_publish(update, context, scheduled_time, user_id):
    """Планирование публикации истории"""
    try:
        # Получаем данные для публикации
        media_path = context.user_data.get('publish_media_path')
        caption = context.user_data.get('publish_caption', '')
        account_ids = context.user_data.get('publish_account_ids', [])
        
        # Создаем задачи для каждого аккаунта
        task_ids = []
        for account_id in account_ids:
            task_id = create_publish_task(
                user_id=user_id,
                task_type=TaskType.STORY,
                account_id=account_id,
                media_path=media_path,
                caption=caption,
                scheduled_time=scheduled_time,
                status=TaskStatus.SCHEDULED
            )
            task_ids.append(task_id)
        
        return task_ids
        
    except Exception as e:
        logger.error(f"Ошибка при планировании истории: {e}")
        raise

def schedule_reels_publish(update, context, scheduled_time, user_id):
    """Планирование публикации Reels"""
    try:
        # Получаем данные для публикации
        media_path = context.user_data.get('publish_media_path')
        caption = context.user_data.get('publish_caption', '')
        account_ids = context.user_data.get('publish_account_ids', [])
        
        # Создаем задачи для каждого аккаунта
        task_ids = []
        for account_id in account_ids:
            task_id = create_publish_task(
                user_id=user_id,
                task_type=TaskType.REELS,
                account_id=account_id,
                media_path=media_path,
                caption=caption,
                scheduled_time=scheduled_time,
                status=TaskStatus.SCHEDULED
            )
            task_ids.append(task_id)
        
        return task_ids
        
    except Exception as e:
        logger.error(f"Ошибка при планировании Reels: {e}")
        raise

def schedule_post_publish(update, context, scheduled_time, user_id):
    """Планирование публикации поста"""
    try:
        # Получаем данные для публикации
        media_path = context.user_data.get('publish_media_path')
        media_paths = context.user_data.get('publish_media_paths', [])
        caption = context.user_data.get('publish_caption', '')
        hashtags = context.user_data.get('publish_hashtags', '')
        account_ids = context.user_data.get('publish_account_ids', [])
        
        # Объединяем подпись и хештеги
        full_caption = caption
        if hashtags:
            if full_caption:
                full_caption += f"\n\n{hashtags}"
            else:
                full_caption = hashtags
        
        # Создаем задачи для каждого аккаунта
        task_ids = []
        for account_id in account_ids:
            task_id = create_publish_task(
                user_id=user_id,
                task_type=TaskType.POST,
                account_id=account_id,
                media_path=media_path or str(media_paths),
                caption=full_caption,
                scheduled_time=scheduled_time,
                status=TaskStatus.SCHEDULED
            )
            task_ids.append(task_id)
        
        return task_ids
        
    except Exception as e:
        logger.error(f"Ошибка при планировании поста: {e}")
        raise

def get_schedule_handlers():
    """Возвращает обработчики для планирования"""
    from telegram.ext import CallbackQueryHandler
    
    return [
        CallbackQueryHandler(handle_quick_schedule, pattern='^quick_schedule_'),
        CallbackQueryHandler(handle_custom_schedule, pattern='^custom_schedule$'),
        CallbackQueryHandler(handle_hour_selection, pattern='^time_hour_'),
        CallbackQueryHandler(handle_minute_selection, pattern='^time_minute_'),
        CallbackQueryHandler(choose_schedule, pattern='^back_to_hour$')
    ] 
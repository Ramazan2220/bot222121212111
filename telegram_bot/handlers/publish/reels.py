"""
Модуль для обработки публикации Reels
"""

import os
import tempfile
import logging
from datetime import datetime

from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ConversationHandler, CallbackQueryHandler, MessageHandler, Filters

from .common import (
    is_admin, cleanup_user_data, cancel_publish, logger, show_publish_confirmation,
    CHOOSE_ACCOUNT, UPLOAD_MEDIA, ENTER_CAPTION, ENTER_HASHTAGS, CONFIRM_PUBLISH, CHOOSE_SCHEDULE
)

from database.db_manager import get_instagram_account, create_publish_task
from database.models import TaskType, TaskStatus
from utils.task_queue import add_task_to_queue
from telegram_bot.utils.account_selection import AccountSelector

# Создаем селектор аккаунтов для Reels
reels_selector = AccountSelector(
    callback_prefix="reels_select",
    title="🎥 Публикация Reels",
    allow_multiple=True,
    show_status=True,
    show_folders=True,
    back_callback="menu_publish"
)

def start_reels_publish(update, context):
    """Запуск публикации Reels"""
    user_id = update.effective_user.id
    
    if not is_admin(user_id):
        if update.message:
            update.message.reply_text("У вас нет прав для выполнения этой команды.")
        else:
            update.callback_query.answer("У вас нет прав для выполнения этой команды.", show_alert=True)
        return ConversationHandler.END
    
    # Устанавливаем тип публикации
    context.user_data['publish_type'] = 'reels'
    
    # Проверяем, это запланированная публикация
    is_scheduled = context.user_data.get('is_scheduled_post', False)
    
    # Если аккаунты уже выбраны (для запланированных публикаций), переходим к загрузке медиа
    if is_scheduled and context.user_data.get('publish_account_ids'):
        account_ids = context.user_data.get('publish_account_ids')
        accounts = [get_instagram_account(acc_id) for acc_id in account_ids]
        usernames = [acc.username for acc in accounts if acc]
        
        if len(account_ids) == 1:
            text = f"🎥 Выбран аккаунт: @{usernames[0]}\n\n"
        else:
            text = f"🎥 Выбрано аккаунтов: {len(account_ids)}\n"
            text += f"Аккаунты: {', '.join([f'@{u}' for u in usernames[:3]])}"
            if len(usernames) > 3:
                text += f" и ещё {len(usernames) - 3}..."
            text += "\n\n"
        
        text += "🎥 Отправьте видео для Reels (до 90 секунд):"
        
        if update.callback_query:
            update.callback_query.edit_message_text(text)
        else:
            update.message.reply_text(text)
        
        return ConversationHandler.END
    
    # Определяем callback для обработки выбранных аккаунтов
    def on_accounts_selected(account_ids: list, update_inner, context_inner):
        if account_ids:
            context_inner.user_data['publish_account_ids'] = account_ids
            context_inner.user_data['publish_type'] = 'reels'
            context_inner.user_data['publish_to_all_accounts'] = len(account_ids) > 1
            
            # Получаем информацию об аккаунтах
            accounts = [get_instagram_account(acc_id) for acc_id in account_ids]
            usernames = [acc.username for acc in accounts if acc]
            context_inner.user_data['publish_account_usernames'] = usernames
            
            if len(account_ids) == 1:
                context_inner.user_data['publish_account_id'] = account_ids[0]
                context_inner.user_data['publish_account_username'] = usernames[0]
            
            # Переходим к загрузке медиа
            if len(account_ids) == 1:
                text = f"🎥 Выбран аккаунт: @{usernames[0]}\n\n"
            else:
                text = f"🎥 Выбрано аккаунтов: {len(account_ids)}\n"
                text += f"Аккаунты: {', '.join([f'@{u}' for u in usernames[:3]])}"
                if len(usernames) > 3:
                    text += f" и ещё {len(usernames) - 3}..."
                text += "\n\n"
            
            text += "🎥 Отправьте видео для Reels (до 90 секунд):"
            
            update_inner.callback_query.edit_message_text(text)
            return ConversationHandler.END
    
    # Запускаем селектор аккаунтов
    return reels_selector.start_selection(update, context, on_accounts_selected)

def handle_reels_media_upload(update, context):
    """Обработчик загрузки видео для Reels"""
    user_id = update.effective_user.id

    if not is_admin(user_id):
        update.message.reply_text("У вас нет прав для выполнения этой команды.")
        return ConversationHandler.END

    # Проверяем, что это для Reels и аккаунты выбраны
    publish_type = context.user_data.get('publish_type')
    account_ids = context.user_data.get('publish_account_ids', [])
    
    if publish_type != 'reels' or not account_ids:
        logger.info(f"🎥 REELS: Игнорируем - publish_type={publish_type}, account_ids={len(account_ids)}")
        return None

    # Получаем информацию о видео
    video_file = None
    file_extension = '.mp4'
    
    if update.message.video:
        video_file = update.message.video
    elif update.message.document:
        if update.message.document.mime_type.startswith('video/'):
            video_file = update.message.document
        else:
            update.message.reply_text("❌ Для Reels нужно отправить видео файл.")
            return ConversationHandler.END
    
    if not video_file:
        update.message.reply_text("🎥 Пожалуйста, отправьте видео для Reels.")
        return ConversationHandler.END

    file_id = video_file.file_id

    # Скачиваем видео
    media = context.bot.get_file(file_id)

    # Создаем временный файл для сохранения видео
    with tempfile.NamedTemporaryFile(delete=False, suffix=file_extension) as temp_file:
        media_path = temp_file.name

    # Скачиваем видео во временный файл
    media.download(media_path)

    # Сохраняем путь к медиа и тип медиа
    context.user_data['publish_media_path'] = media_path
    context.user_data['publish_media_type'] = 'VIDEO'

    # Показываем меню настроек для Reels
    return show_reels_settings_menu(update, context)

def show_reels_settings_menu(update, context):
    """Показ меню настроек для Reels"""
    # Получаем информацию об аккаунтах
    account_ids = context.user_data.get('publish_account_ids', [])
    if len(account_ids) == 1:
        account_username = context.user_data.get('publish_account_username')
        account_info = f"👤 Аккаунт: @{account_username}"
    else:
        account_info = f"👥 Аккаунты: {len(account_ids)} аккаунтов"
    
    # Получаем текущие настройки
    caption = context.user_data.get('publish_caption', '')
    hashtags = context.user_data.get('publish_hashtags', '')
    
    # Создаем клавиатуру
    keyboard = [
        [InlineKeyboardButton("✏️ Добавить подпись", callback_data="reels_add_caption")],
        [InlineKeyboardButton("#️⃣ Добавить хештеги", callback_data="reels_add_hashtags")]
    ]
    
    # Проверяем, запланированная ли это публикация
    is_scheduled = context.user_data.get('is_scheduled_post', False)
    
    if is_scheduled:
        keyboard.append([InlineKeyboardButton("🗓️ Выбрать время публикации", callback_data="schedule_publish")])
    else:
        keyboard.append([
            InlineKeyboardButton("✅ Опубликовать", callback_data="reels_confirm_publish"),
            InlineKeyboardButton("⏰ Запланировать", callback_data="schedule_publish")
        ])
    
    keyboard.append([InlineKeyboardButton("❌ Отмена", callback_data="cancel_publish")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    # Формируем текст с текущими настройками
    text = f"🎥 Настройки Reels:\n\n"
    text += f"{account_info}\n"
    text += f"📱 Медиа: VIDEO\n"
    text += f"✏️ Подпись: {caption or '(не задана)'}\n"
    text += f"#️⃣ Хештеги: {hashtags or '(не заданы)'}\n\n"
    text += "Выберите действие:"
    
    update.message.reply_text(text, reply_markup=reply_markup)
    return ENTER_CAPTION

def handle_reels_caption_input(update, context):
    """Обработка ввода подписи для Reels"""
    caption = update.message.text
    context.user_data['publish_caption'] = caption
    
    keyboard = [
        [InlineKeyboardButton("✅ Сохранить", callback_data="reels_save_caption")],
        [InlineKeyboardButton("🔙 Назад", callback_data="reels_back_to_settings")],
        [InlineKeyboardButton("❌ Отмена", callback_data="cancel_publish")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    update.message.reply_text(
        f"✅ Подпись сохранена:\n\n{caption}\n\nЧто делать дальше?",
        reply_markup=reply_markup
    )
    return ENTER_CAPTION

def handle_reels_hashtags_input(update, context):
    """Обработка ввода хештегов для Reels"""
    hashtags = update.message.text
    context.user_data['publish_hashtags'] = hashtags
    
    keyboard = [
        [InlineKeyboardButton("✅ Сохранить", callback_data="reels_save_hashtags")],
        [InlineKeyboardButton("🔙 Назад", callback_data="reels_back_to_settings")],
        [InlineKeyboardButton("❌ Отмена", callback_data="cancel_publish")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    update.message.reply_text(
        f"✅ Хештеги сохранены:\n\n{hashtags}\n\nЧто делать дальше?",
        reply_markup=reply_markup
    )
    return ENTER_HASHTAGS

def reels_confirm_publish_handler(update, context):
    """Подтверждение публикации Reels"""
    query = update.callback_query
    query.answer()
    
    # Получаем все данные для публикации
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
    if len(account_ids) == 1:
        account_username = context.user_data.get('publish_account_username')
        account_info = f"👤 Аккаунт: @{account_username}"
    else:
        account_info = f"👥 Аккаунты: {len(account_ids)} аккаунтов"
    
    # Создаем клавиатуру
    keyboard = [
        [InlineKeyboardButton("✅ Опубликовать сейчас", callback_data="execute_reels_publish")],
        [InlineKeyboardButton("⏰ Запланировать", callback_data="schedule_publish")],
        [InlineKeyboardButton("🔙 Назад", callback_data="reels_back_to_settings")],
        [InlineKeyboardButton("❌ Отмена", callback_data="cancel_publish")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    # Формируем текст подтверждения
    text = f"🎥 Подтверждение публикации Reels:\n\n"
    text += f"{account_info}\n"
    text += f"📱 Медиа: VIDEO\n"
    text += f"✏️ Подпись: {full_caption or '(без подписи)'}\n\n"
    text += "Подтвердите публикацию:"
    
    query.edit_message_text(text, reply_markup=reply_markup)
    return CONFIRM_PUBLISH

def execute_reels_publish(update, context):
    """Выполнение публикации Reels"""
    query = update.callback_query
    user_id = update.effective_user.id
    
    try:
        # Получаем данные для публикации
        media_path = context.user_data.get('publish_media_path')
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
        
        # Создаем задачи для каждого аккаунта
        task_ids = []
        for account_id in account_ids:
            # Подготавливаем данные для задачи
            task_data = {
                'media_path': media_path,
                'media_type': 'VIDEO',
                'caption': full_caption,
                'account_id': account_id
            }
            
            # Создаем задачу в базе данных
            task_id = create_publish_task(
                user_id=user_id,
                task_type=TaskType.REELS,
                account_id=account_id,
                media_path=media_path,
                caption=full_caption,
                status=TaskStatus.PENDING
            )
            
            # Добавляем задачу в очередь
            add_task_to_queue(task_id, task_data)
            task_ids.append(task_id)
        
        # Отправляем подтверждение
        if len(account_ids) == 1:
            account_username = context.user_data.get('publish_account_username')
            message = f"✅ Reels добавлен в очередь публикации!\n\n"
            message += f"👤 Аккаунт: @{account_username}\n"
            message += f"📱 Медиа: VIDEO\n"
            message += f"🆔 ID задачи: {task_ids[0]}"
        else:
            message = f"✅ Reels добавлен в очередь публикации!\n\n"
            message += f"👥 Аккаунты: {len(account_ids)} аккаунтов\n"
            message += f"📱 Медиа: VIDEO\n"
            message += f"🆔 ID задач: {', '.join(map(str, task_ids))}"
        
        query.edit_message_text(message)
        
        # Очищаем данные пользователя
        cleanup_user_data(context)
        
        return ConversationHandler.END
        
    except Exception as e:
        logger.error(f"Ошибка при создании задачи публикации Reels: {e}")
        query.edit_message_text(f"❌ Ошибка при создании задачи: {str(e)}")
        return ConversationHandler.END

def schedule_publish_callback(update, context):
    """Обработка запроса на планирование публикации"""
    from .scheduler import choose_schedule
    return choose_schedule(update, context)

def get_reels_conversation():
    """Возвращает ConversationHandler для Reels"""
    return ConversationHandler(
        entry_points=[
            CallbackQueryHandler(start_reels_publish, pattern='^publish_reels$'),
            CallbackQueryHandler(start_reels_publish, pattern='^schedule_reels$'),
            MessageHandler(Filters.video | Filters.document, handle_reels_media_upload)
        ],
        states={
            ENTER_CAPTION: [
                MessageHandler(Filters.text & ~Filters.command, handle_reels_caption_input),
                CallbackQueryHandler(show_reels_settings_menu, pattern='^reels_back_to_settings$'),
                CallbackQueryHandler(show_reels_settings_menu, pattern='^reels_save_caption$'),
                CallbackQueryHandler(reels_confirm_publish_handler, pattern='^reels_confirm_publish$'),
                CallbackQueryHandler(schedule_publish_callback, pattern='^schedule_publish$')
            ],
            ENTER_HASHTAGS: [
                MessageHandler(Filters.text & ~Filters.command, handle_reels_hashtags_input),
                CallbackQueryHandler(show_reels_settings_menu, pattern='^reels_back_to_settings$'),
                CallbackQueryHandler(show_reels_settings_menu, pattern='^reels_save_hashtags$')
            ],
            CONFIRM_PUBLISH: [
                CallbackQueryHandler(execute_reels_publish, pattern='^execute_reels_publish$'),
                CallbackQueryHandler(schedule_publish_callback, pattern='^schedule_publish$'),
                CallbackQueryHandler(show_reels_settings_menu, pattern='^reels_back_to_settings$')
            ]
        },
        fallbacks=[
            CallbackQueryHandler(cancel_publish, pattern='^cancel_publish$'),
            CommandHandler('cancel', cancel_publish)
        ]
    )

def get_reels_selector():
    """Возвращает селектор аккаунтов для Reels"""
    return reels_selector

# Обработчики для запланированных публикаций
def reels_schedule_publish_handler(update, context):
    """Обработчик запланированной публикации Reels"""
    user_id = update.effective_user.id
    
    if not is_admin(user_id):
        update.callback_query.answer("У вас нет прав для выполнения этой команды.", show_alert=True)
        return ConversationHandler.END
    
    # Устанавливаем флаг запланированной публикации
    context.user_data['is_scheduled_post'] = True
    
    # Проверяем, выбраны ли уже аккаунты
    if context.user_data.get('publish_account_ids'):
        # Аккаунты уже выбраны, переходим к вводу времени
        from .scheduler import choose_schedule
        return choose_schedule(update, context)
    else:
        # Аккаунты не выбраны, запускаем выбор
        return start_reels_publish(update, context) 
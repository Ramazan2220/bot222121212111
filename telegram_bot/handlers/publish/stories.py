"""
Модуль для обработки публикации Stories
"""

import os
import tempfile
import json
import logging
from datetime import datetime

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, ParseMode
from telegram.ext import ConversationHandler, CallbackQueryHandler, MessageHandler, Filters

from .common import (
    is_admin, cleanup_user_data, cancel_publish, logger, show_publish_confirmation,
    CHOOSE_ACCOUNT, UPLOAD_MEDIA, ENTER_CAPTION, ENTER_HASHTAGS, CONFIRM_PUBLISH, CHOOSE_SCHEDULE,
    STORY_ADD_FEATURES, STORY_ADD_MENTIONS, STORY_ADD_LINK, STORY_ADD_LOCATION, STORY_ADD_HASHTAGS, STORY_ADD_TEXT
)

from database.db_manager import get_instagram_account, get_instagram_accounts, create_publish_task
from database.models import TaskType, TaskStatus
from utils.task_queue import add_task_to_queue
from telegram_bot.utils.account_selection import AccountSelector

# Создаем селектор аккаунтов для Stories
story_selector = AccountSelector(
    callback_prefix="story_select",
    title="📱 Публикация Stories",
    allow_multiple=True,
    show_status=True,
    show_folders=True,
    back_callback="menu_publish"
)

def start_story_publish(update, context):
    """Запуск публикации Stories"""
    user_id = update.effective_user.id
    
    if not is_admin(user_id):
        if update.message:
            update.message.reply_text("У вас нет прав для выполнения этой команды.")
        else:
            update.callback_query.answer("У вас нет прав для выполнения этой команды.", show_alert=True)
        return ConversationHandler.END
    
    # Устанавливаем тип публикации
    context.user_data['publish_type'] = 'story'
    
    # Проверяем, это запланированная публикация
    is_scheduled = context.user_data.get('is_scheduled_post', False)
    
    # Если аккаунты уже выбраны (для запланированных публикаций), переходим к загрузке медиа
    if is_scheduled and context.user_data.get('publish_account_ids'):
        account_ids = context.user_data.get('publish_account_ids')
        accounts = [get_instagram_account(acc_id) for acc_id in account_ids]
        usernames = [acc.username for acc in accounts if acc]
        
        if len(account_ids) == 1:
            text = f"📱 Выбран аккаунт: @{usernames[0]}\n\n"
        else:
            text = f"📱 Выбрано аккаунтов: {len(account_ids)}\n"
            text += f"Аккаунты: {', '.join([f'@{u}' for u in usernames[:3]])}"
            if len(usernames) > 3:
                text += f" и ещё {len(usernames) - 3}..."
            text += "\n\n"
        
        text += "📱 Отправьте фото или видео для истории:"
        
        if update.callback_query:
            update.callback_query.edit_message_text(text)
        else:
            update.message.reply_text(text)
        
        return ConversationHandler.END
    
    # Определяем callback для обработки выбранных аккаунтов
    def on_accounts_selected(account_ids: list, update_inner, context_inner):
        if account_ids:
            context_inner.user_data['publish_account_ids'] = account_ids
            context_inner.user_data['publish_type'] = 'story'
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
                text = f"📱 Выбран аккаунт: @{usernames[0]}\n\n"
            else:
                text = f"📱 Выбрано аккаунтов: {len(account_ids)}\n"
                text += f"Аккаунты: {', '.join([f'@{u}' for u in usernames[:3]])}"
                if len(usernames) > 3:
                    text += f" и ещё {len(usernames) - 3}..."
                text += "\n\n"
            
            text += "📱 Отправьте фото или видео для истории:"
            
            update_inner.callback_query.edit_message_text(text)
            return ConversationHandler.END
    
    # Запускаем селектор аккаунтов
    return story_selector.start_selection(update, context, on_accounts_selected)

def handle_story_media_upload(update, context):
    """Обработчик загрузки медиа для Stories"""
    user_id = update.effective_user.id

    if not is_admin(user_id):
        update.message.reply_text("У вас нет прав для выполнения этой команды.")
        return ConversationHandler.END

    # Проверяем, что это для Stories и аккаунты выбраны
    publish_type = context.user_data.get('publish_type')
    account_ids = context.user_data.get('publish_account_ids', [])
    
    if publish_type != 'story' or not account_ids:
        # Если это не для Stories или аккаунты не выбраны, игнорируем
        logger.info(f"📱 STORY: Игнорируем - publish_type={publish_type}, account_ids={len(account_ids)}")
        return None

    # Получаем информацию о медиа
    media_file = None
    media_type = None
    file_extension = '.jpg'
    
    if update.message.photo:
        media_file = update.message.photo[-1]
        media_type = 'PHOTO'
        file_extension = '.jpg'
    elif update.message.video:
        media_file = update.message.video
        media_type = 'VIDEO'
        file_extension = '.mp4'
    elif update.message.document:
        media_file = update.message.document
        if media_file.mime_type.startswith('video/'):
            media_type = 'VIDEO'
            file_extension = '.mp4'
        else:
            media_type = 'PHOTO'
            file_extension = '.jpg'
    
    if not media_file:
        update.message.reply_text("📱 Пожалуйста, отправьте фото или видео для истории.")
        return ConversationHandler.END

    file_id = media_file.file_id

    # Скачиваем медиа
    media = context.bot.get_file(file_id)

    # Создаем временный файл для сохранения медиа
    with tempfile.NamedTemporaryFile(delete=False, suffix=file_extension) as temp_file:
        media_path = temp_file.name

    # Скачиваем медиа во временный файл
    media.download(media_path)

    # Сохраняем путь к медиа и тип медиа
    context.user_data['publish_media_path'] = media_path
    context.user_data['publish_media_type'] = media_type

    # Инициализируем данные для Stories
    context.user_data['story_features'] = {}
    context.user_data['story_mentions'] = []
    context.user_data['story_link'] = ''
    context.user_data['story_location'] = ''
    context.user_data['story_hashtags'] = []
    context.user_data['story_text'] = ''

    # Показываем меню функций для Stories
    return show_story_features_menu(update, context)

def show_story_features_menu(update, context):
    """Показ меню функций для Stories"""
    # Получаем информацию об аккаунтах
    account_ids = context.user_data.get('publish_account_ids', [])
    if len(account_ids) == 1:
        account_username = context.user_data.get('publish_account_username')
        account_info = f"👤 Аккаунт: @{account_username}"
    else:
        account_info = f"👥 Аккаунты: {len(account_ids)} аккаунтов"
    
    # Получаем текущие настройки
    media_type = context.user_data.get('publish_media_type')
    story_text = context.user_data.get('story_text', '')
    story_mentions = context.user_data.get('story_mentions', [])
    story_link = context.user_data.get('story_link', '')
    story_location = context.user_data.get('story_location', '')
    story_hashtags = context.user_data.get('story_hashtags', [])
    
    # Создаем клавиатуру
    keyboard = [
        [InlineKeyboardButton("📝 Добавить текст", callback_data="story_add_text")],
        [InlineKeyboardButton("👥 Добавить упоминания", callback_data="story_add_mentions")],
        [InlineKeyboardButton("🔗 Добавить ссылку", callback_data="story_add_link")],
        [InlineKeyboardButton("📍 Добавить локацию", callback_data="story_add_location")],
        [InlineKeyboardButton("#️⃣ Добавить хештеги", callback_data="story_add_hashtags")]
    ]
    
    # Проверяем, запланированная ли это публикация
    is_scheduled = context.user_data.get('is_scheduled_post', False)
    
    if is_scheduled:
        keyboard.append([InlineKeyboardButton("🗓️ Выбрать время публикации", callback_data="schedule_publish")])
    else:
        keyboard.append([
            InlineKeyboardButton("✅ Опубликовать", callback_data="story_confirm_publish"),
            InlineKeyboardButton("⏰ Запланировать", callback_data="schedule_publish")
        ])
    
    keyboard.append([InlineKeyboardButton("❌ Отмена", callback_data="cancel_publish")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    # Формируем текст с текущими настройками
    text = f"📱 Настройки истории:\n\n"
    text += f"{account_info}\n"
    text += f"📱 Медиа: {media_type}\n"
    text += f"📝 Текст: {story_text or '(не задан)'}\n"
    text += f"👥 Упоминания: {len(story_mentions)} пользователей\n"
    text += f"🔗 Ссылка: {story_link or '(не задана)'}\n"
    text += f"📍 Локация: {story_location or '(не задана)'}\n"
    text += f"#️⃣ Хештеги: {len(story_hashtags)} штук\n\n"
    text += "Выберите действие:"
    
    if hasattr(update, 'callback_query'):
        update.callback_query.edit_message_text(text, reply_markup=reply_markup)
    else:
        update.message.reply_text(text, reply_markup=reply_markup)
    
    return STORY_ADD_FEATURES

def story_add_text_handler(update, context):
    """Обработчик добавления текста к истории"""
    query = update.callback_query
    query.answer()
    
    current_text = context.user_data.get('story_text', '')
    
    keyboard = [
        [InlineKeyboardButton("🔙 Назад к настройкам", callback_data="back_to_story_features")],
        [InlineKeyboardButton("❌ Отмена", callback_data="cancel_publish")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    text = "📝 Введите текст для истории:"
    if current_text:
        text += f"\n\nТекущий текст:\n{current_text}"
    
    query.edit_message_text(text, reply_markup=reply_markup)
    return STORY_ADD_TEXT

def handle_story_text_input(update, context):
    """Обработка ввода текста для истории"""
    story_text = update.message.text
    context.user_data['story_text'] = story_text
    
    keyboard = [
        [InlineKeyboardButton("✅ Сохранить текст", callback_data="save_story_text")],
        [InlineKeyboardButton("✏️ Изменить текст", callback_data="story_add_text")],
        [InlineKeyboardButton("🔙 Назад к настройкам", callback_data="back_to_story_features")],
        [InlineKeyboardButton("❌ Отмена", callback_data="cancel_publish")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    update.message.reply_text(
        f"✅ Текст сохранен:\n\n{story_text}\n\nЧто делать дальше?",
        reply_markup=reply_markup
    )
    return STORY_ADD_TEXT

def story_add_mentions_handler(update, context):
    """Обработчик добавления упоминаний"""
    query = update.callback_query
    query.answer()
    
    current_mentions = context.user_data.get('story_mentions', [])
    
    keyboard = [
        [InlineKeyboardButton("🔙 Назад к настройкам", callback_data="back_to_story_features")],
        [InlineKeyboardButton("❌ Отмена", callback_data="cancel_publish")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    text = "👥 Введите упоминания (через запятую, без @):\n"
    text += "Например: username1, username2, username3"
    
    if current_mentions:
        text += f"\n\nТекущие упоминания:\n{', '.join(current_mentions)}"
    
    query.edit_message_text(text, reply_markup=reply_markup)
    return STORY_ADD_MENTIONS

def handle_story_mentions_input(update, context):
    """Обработка ввода упоминаний"""
    mentions_text = update.message.text
    mentions = [mention.strip() for mention in mentions_text.split(',') if mention.strip()]
    context.user_data['story_mentions'] = mentions
    
    keyboard = [
        [InlineKeyboardButton("✅ Сохранить упоминания", callback_data="save_story_mentions")],
        [InlineKeyboardButton("✏️ Изменить упоминания", callback_data="story_add_mentions")],
        [InlineKeyboardButton("🔙 Назад к настройкам", callback_data="back_to_story_features")],
        [InlineKeyboardButton("❌ Отмена", callback_data="cancel_publish")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    update.message.reply_text(
        f"✅ Упоминания сохранены:\n\n{', '.join(mentions)}\n\nЧто делать дальше?",
        reply_markup=reply_markup
    )
    return STORY_ADD_MENTIONS

def story_add_link_handler(update, context):
    """Обработчик добавления ссылки"""
    query = update.callback_query
    query.answer()
    
    current_link = context.user_data.get('story_link', '')
    
    keyboard = [
        [InlineKeyboardButton("🔙 Назад к настройкам", callback_data="back_to_story_features")],
        [InlineKeyboardButton("❌ Отмена", callback_data="cancel_publish")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    text = "🔗 Введите ссылку для истории:\n"
    text += "Например: https://example.com"
    
    if current_link:
        text += f"\n\nТекущая ссылка:\n{current_link}"
    
    query.edit_message_text(text, reply_markup=reply_markup)
    return STORY_ADD_LINK

def handle_story_link_input(update, context):
    """Обработка ввода ссылки"""
    link = update.message.text
    context.user_data['story_link'] = link
    
    keyboard = [
        [InlineKeyboardButton("✅ Сохранить ссылку", callback_data="save_story_link")],
        [InlineKeyboardButton("✏️ Изменить ссылку", callback_data="story_add_link")],
        [InlineKeyboardButton("🔙 Назад к настройкам", callback_data="back_to_story_features")],
        [InlineKeyboardButton("❌ Отмена", callback_data="cancel_publish")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    update.message.reply_text(
        f"✅ Ссылка сохранена:\n\n{link}\n\nЧто делать дальше?",
        reply_markup=reply_markup
    )
    return STORY_ADD_LINK

def story_add_location_handler(update, context):
    """Обработчик добавления локации"""
    query = update.callback_query
    query.answer()
    
    current_location = context.user_data.get('story_location', '')
    
    keyboard = [
        [InlineKeyboardButton("🔙 Назад к настройкам", callback_data="back_to_story_features")],
        [InlineKeyboardButton("❌ Отмена", callback_data="cancel_publish")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    text = "📍 Введите название локации:\n"
    text += "Например: Москва, Россия"
    
    if current_location:
        text += f"\n\nТекущая локация:\n{current_location}"
    
    query.edit_message_text(text, reply_markup=reply_markup)
    return STORY_ADD_LOCATION

def handle_story_location_input(update, context):
    """Обработка ввода локации"""
    location = update.message.text
    context.user_data['story_location'] = location
    
    keyboard = [
        [InlineKeyboardButton("✅ Сохранить локацию", callback_data="save_story_location")],
        [InlineKeyboardButton("✏️ Изменить локацию", callback_data="story_add_location")],
        [InlineKeyboardButton("🔙 Назад к настройкам", callback_data="back_to_story_features")],
        [InlineKeyboardButton("❌ Отмена", callback_data="cancel_publish")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    update.message.reply_text(
        f"✅ Локация сохранена:\n\n{location}\n\nЧто делать дальше?",
        reply_markup=reply_markup
    )
    return STORY_ADD_LOCATION

def story_add_hashtags_handler(update, context):
    """Обработчик добавления хештегов"""
    query = update.callback_query
    query.answer()
    
    current_hashtags = context.user_data.get('story_hashtags', [])
    
    keyboard = [
        [InlineKeyboardButton("🔙 Назад к настройкам", callback_data="back_to_story_features")],
        [InlineKeyboardButton("❌ Отмена", callback_data="cancel_publish")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    text = "#️⃣ Введите хештеги (через запятую, без #):\n"
    text += "Например: travel, moscow, russia"
    
    if current_hashtags:
        text += f"\n\nТекущие хештеги:\n{', '.join(current_hashtags)}"
    
    query.edit_message_text(text, reply_markup=reply_markup)
    return STORY_ADD_HASHTAGS

def handle_story_hashtags_input(update, context):
    """Обработка ввода хештегов"""
    hashtags_text = update.message.text
    hashtags = [hashtag.strip() for hashtag in hashtags_text.split(',') if hashtag.strip()]
    context.user_data['story_hashtags'] = hashtags
    
    keyboard = [
        [InlineKeyboardButton("✅ Сохранить хештеги", callback_data="save_story_hashtags")],
        [InlineKeyboardButton("✏️ Изменить хештеги", callback_data="story_add_hashtags")],
        [InlineKeyboardButton("🔙 Назад к настройкам", callback_data="back_to_story_features")],
        [InlineKeyboardButton("❌ Отмена", callback_data="cancel_publish")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    update.message.reply_text(
        f"✅ Хештеги сохранены:\n\n{', '.join(hashtags)}\n\nЧто делать дальше?",
        reply_markup=reply_markup
    )
    return STORY_ADD_HASHTAGS

def back_to_story_features(update, context):
    """Возврат к меню функций Stories"""
    return show_story_features_menu(update, context)

def story_confirm_publish_handler(update, context):
    """Подтверждение публикации истории"""
    query = update.callback_query
    query.answer()
    
    # Получаем все данные для публикации
    media_type = context.user_data.get('publish_media_type')
    story_text = context.user_data.get('story_text', '')
    story_mentions = context.user_data.get('story_mentions', [])
    story_link = context.user_data.get('story_link', '')
    story_location = context.user_data.get('story_location', '')
    story_hashtags = context.user_data.get('story_hashtags', [])
    
    account_ids = context.user_data.get('publish_account_ids', [])
    if len(account_ids) == 1:
        account_username = context.user_data.get('publish_account_username')
        account_info = f"👤 Аккаунт: @{account_username}"
    else:
        account_info = f"👥 Аккаунты: {len(account_ids)} аккаунтов"
    
    # Формируем подпись с хештегами
    caption_parts = []
    if story_text:
        caption_parts.append(story_text)
    if story_hashtags:
        hashtag_text = ' '.join([f'#{tag}' for tag in story_hashtags])
        caption_parts.append(hashtag_text)
    
    full_caption = '\n\n'.join(caption_parts)
    
    # Создаем клавиатуру
    keyboard = [
        [InlineKeyboardButton("✅ Опубликовать сейчас", callback_data="execute_story_publish")],
        [InlineKeyboardButton("⏰ Запланировать", callback_data="schedule_publish")],
        [InlineKeyboardButton("🔙 Назад к настройкам", callback_data="back_to_story_features")],
        [InlineKeyboardButton("❌ Отмена", callback_data="cancel_publish")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    # Формируем текст подтверждения
    text = f"📱 Подтверждение публикации истории:\n\n"
    text += f"{account_info}\n"
    text += f"📱 Медиа: {media_type}\n"
    text += f"📝 Текст: {story_text or '(не задан)'}\n"
    text += f"👥 Упоминания: {', '.join(story_mentions) if story_mentions else '(не заданы)'}\n"
    text += f"🔗 Ссылка: {story_link or '(не задана)'}\n"
    text += f"📍 Локация: {story_location or '(не задана)'}\n"
    text += f"#️⃣ Хештеги: {', '.join(story_hashtags) if story_hashtags else '(не заданы)'}\n\n"
    text += "Подтвердите публикацию:"
    
    query.edit_message_text(text, reply_markup=reply_markup)
    return CONFIRM_PUBLISH

def execute_story_publish(update, context):
    """Выполнение публикации истории"""
    query = update.callback_query
    user_id = update.effective_user.id
    
    try:
        # Получаем данные для публикации
        media_path = context.user_data.get('publish_media_path')
        media_type = context.user_data.get('publish_media_type')
        story_text = context.user_data.get('story_text', '')
        story_mentions = context.user_data.get('story_mentions', [])
        story_link = context.user_data.get('story_link', '')
        story_location = context.user_data.get('story_location', '')
        story_hashtags = context.user_data.get('story_hashtags', [])
        
        # Формируем подпись
        caption_parts = []
        if story_text:
            caption_parts.append(story_text)
        if story_hashtags:
            hashtag_text = ' '.join([f'#{tag}' for tag in story_hashtags])
            caption_parts.append(hashtag_text)
        
        full_caption = '\n\n'.join(caption_parts)
        
        account_ids = context.user_data.get('publish_account_ids', [])
        
        # Создаем задачи для каждого аккаунта
        task_ids = []
        for account_id in account_ids:
            # Подготавливаем данные для задачи
            task_data = {
                'media_path': media_path,
                'media_type': media_type,
                'caption': full_caption,
                'mentions': story_mentions,
                'link': story_link,
                'location': story_location,
                'hashtags': story_hashtags,
                'account_id': account_id
            }
            
            # Создаем задачу в базе данных
            task_id = create_publish_task(
                user_id=user_id,
                task_type=TaskType.STORY,
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
            message = f"✅ История добавлена в очередь публикации!\n\n"
            message += f"👤 Аккаунт: @{account_username}\n"
            message += f"📱 Медиа: {media_type}\n"
            message += f"🆔 ID задачи: {task_ids[0]}"
        else:
            message = f"✅ История добавлена в очередь публикации!\n\n"
            message += f"👥 Аккаунты: {len(account_ids)} аккаунтов\n"
            message += f"📱 Медиа: {media_type}\n"
            message += f"🆔 ID задач: {', '.join(map(str, task_ids))}"
        
        query.edit_message_text(message)
        
        # Очищаем данные пользователя
        cleanup_user_data(context)
        
        return ConversationHandler.END
        
    except Exception as e:
        logger.error(f"Ошибка при создании задачи публикации истории: {e}")
        query.edit_message_text(f"❌ Ошибка при создании задачи: {str(e)}")
        return ConversationHandler.END

def schedule_publish_callback(update, context):
    """Обработка запроса на планирование публикации"""
    from .scheduler import choose_schedule
    return choose_schedule(update, context)

def get_story_conversation():
    """Возвращает ConversationHandler для Stories"""
    return ConversationHandler(
        entry_points=[
            CallbackQueryHandler(start_story_publish, pattern='^publish_story$'),
            CallbackQueryHandler(start_story_publish, pattern='^schedule_story$'),
            # MessageHandler убран из entry_points - будет только в states
        ],
        states={
            UPLOAD_MEDIA: [
                MessageHandler(Filters.photo | Filters.video | Filters.document, handle_story_media_upload)
            ],
            STORY_ADD_FEATURES: [
                CallbackQueryHandler(story_add_text_handler, pattern='^story_add_text$'),
                CallbackQueryHandler(story_add_mentions_handler, pattern='^story_add_mentions$'),
                CallbackQueryHandler(story_add_link_handler, pattern='^story_add_link$'),
                CallbackQueryHandler(story_add_location_handler, pattern='^story_add_location$'),
                CallbackQueryHandler(story_add_hashtags_handler, pattern='^story_add_hashtags$'),
                CallbackQueryHandler(story_confirm_publish_handler, pattern='^story_confirm_publish$'),
                CallbackQueryHandler(schedule_publish_callback, pattern='^schedule_publish$')
            ],
            STORY_ADD_TEXT: [
                MessageHandler(Filters.text & ~Filters.command, handle_story_text_input),
                CallbackQueryHandler(back_to_story_features, pattern='^save_story_text$'),
                CallbackQueryHandler(story_add_text_handler, pattern='^story_add_text$'),
                CallbackQueryHandler(back_to_story_features, pattern='^back_to_story_features$')
            ],
            STORY_ADD_MENTIONS: [
                MessageHandler(Filters.text & ~Filters.command, handle_story_mentions_input),
                CallbackQueryHandler(back_to_story_features, pattern='^save_story_mentions$'),
                CallbackQueryHandler(story_add_mentions_handler, pattern='^story_add_mentions$'),
                CallbackQueryHandler(back_to_story_features, pattern='^back_to_story_features$')
            ],
            STORY_ADD_LINK: [
                MessageHandler(Filters.text & ~Filters.command, handle_story_link_input),
                CallbackQueryHandler(back_to_story_features, pattern='^save_story_link$'),
                CallbackQueryHandler(story_add_link_handler, pattern='^story_add_link$'),
                CallbackQueryHandler(back_to_story_features, pattern='^back_to_story_features$')
            ],
            STORY_ADD_LOCATION: [
                MessageHandler(Filters.text & ~Filters.command, handle_story_location_input),
                CallbackQueryHandler(back_to_story_features, pattern='^save_story_location$'),
                CallbackQueryHandler(story_add_location_handler, pattern='^story_add_location$'),
                CallbackQueryHandler(back_to_story_features, pattern='^back_to_story_features$')
            ],
            STORY_ADD_HASHTAGS: [
                MessageHandler(Filters.text & ~Filters.command, handle_story_hashtags_input),
                CallbackQueryHandler(back_to_story_features, pattern='^save_story_hashtags$'),
                CallbackQueryHandler(story_add_hashtags_handler, pattern='^story_add_hashtags$'),
                CallbackQueryHandler(back_to_story_features, pattern='^back_to_story_features$')
            ],
            CONFIRM_PUBLISH: [
                CallbackQueryHandler(execute_story_publish, pattern='^execute_story_publish$'),
                CallbackQueryHandler(schedule_publish_callback, pattern='^schedule_publish$'),
                CallbackQueryHandler(back_to_story_features, pattern='^back_to_story_features$')
            ]
        },
        fallbacks=[
            CallbackQueryHandler(cancel_publish, pattern='^cancel_publish$'),
            CommandHandler('cancel', cancel_publish)
        ]
    )

def get_story_selector():
    """Возвращает селектор аккаунтов для Stories"""
    return story_selector

# Обработчики для запланированных публикаций
def story_schedule_publish_handler(update, context):
    """Обработчик запланированной публикации историй"""
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
        return start_story_publish(update, context) 
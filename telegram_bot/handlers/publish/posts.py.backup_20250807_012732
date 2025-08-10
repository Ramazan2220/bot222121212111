"""
Модуль для обработки публикации постов
"""

import os
import tempfile
import json
import logging
from datetime import datetime

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, ParseMode
from telegram.ext import ConversationHandler, CallbackQueryHandler, MessageHandler, Filters, CommandHandler

from .common import (
    is_admin, cleanup_user_data, cancel_publish, logger, get_accounts_by_folder,
    show_publish_confirmation, format_scheduled_time,
    CHOOSE_ACCOUNT, UPLOAD_MEDIA, ENTER_CAPTION, ENTER_HASHTAGS, CONFIRM_PUBLISH, CHOOSE_SCHEDULE
)

from database.db_manager import get_instagram_account, get_instagram_accounts, create_publish_task
from database.models import TaskType, TaskStatus
from utils.task_queue import add_task_to_queue
from telegram_bot.utils.account_selection import create_account_selector, AccountSelector

# Состояния для постов
POST_SOURCE_SELECTION = 100
POST_FOLDER_SELECTION = 101
POST_ACCOUNTS_LIST = 102
POST_CONFIRM_SELECTION = 103
POST_MEDIA_UPLOAD = 104
POST_MEDIA_ACTIONS = 105
POST_CAPTION_INPUT = 106
POST_CAPTION_ACTIONS = 107
POST_HASHTAGS_INPUT = 108
POST_HASHTAGS_ACTIONS = 109
POST_FINAL_CONFIRMATION = 110

# Создаем селектор аккаунтов для постов
post_selector = AccountSelector(
    callback_prefix="post_select",
    title="📸 Публикация поста",
    allow_multiple=True,
    show_status=True,
    show_folders=True,
    back_callback="menu_publish"
)

def start_post_publish(update, context):
    """Запуск публикации поста"""
    user_id = update.effective_user.id
    
    if not is_admin(user_id):
        if update.message:
            update.message.reply_text("У вас нет прав для выполнения этой команды.")
        else:
            update.callback_query.answer("У вас нет прав для выполнения этой команды.", show_alert=True)
        return ConversationHandler.END
    
    # Устанавливаем тип публикации
    context.user_data['publish_type'] = 'post'
    
    # Проверяем, это запланированная публикация
    is_scheduled = context.user_data.get('is_scheduled_post', False)
    if is_scheduled:
        context.user_data['publish_type'] = 'scheduled_post'
    
    # Определяем callback для обработки выбранных аккаунтов
    def on_accounts_selected(account_ids: list, update_inner, context_inner):
        if account_ids:
            context_inner.user_data['publish_account_ids'] = account_ids
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
                text = f"📸 Выбран аккаунт: @{usernames[0]}\n\n"
                context_inner.user_data['publish_account_id'] = account_ids[0]
                context_inner.user_data['publish_account_username'] = usernames[0]
            else:
                text = f"📸 Выбрано аккаунтов: {len(account_ids)}\n"
                text += f"Аккаунты: {', '.join([f'@{u}' for u in usernames[:3]])}"
                if len(usernames) > 3:
                    text += f" и ещё {len(usernames) - 3}..."
                text += "\n\n"
            
            text += "📸 Отправьте фото или видео для поста:"
            
            update_inner.callback_query.edit_message_text(text)
            return POST_MEDIA_UPLOAD
    
    # Запускаем селектор аккаунтов
    return post_selector.start_selection(update, context, on_accounts_selected)

def handle_post_source_selection(update, context):
    """Обработка выбора источника аккаунтов"""
    query = update.callback_query
    query.answer()
    
    source = query.data.split('_')[-1]
    
    if source == 'all':
        # Выбираем все аккаунты
        accounts = get_instagram_accounts()
        active_accounts = [acc for acc in accounts if acc.is_active]
        
        if not active_accounts:
            query.edit_message_text("❌ Нет активных аккаунтов для публикации.")
            return ConversationHandler.END
        
        account_ids = [acc.id for acc in active_accounts]
        usernames = [acc.username for acc in active_accounts]
        
        context.user_data['publish_account_ids'] = account_ids
        context.user_data['publish_account_usernames'] = usernames
        context.user_data['publish_to_all_accounts'] = True
        
        query.edit_message_text(
            f"📸 Выбраны все аккаунты ({len(active_accounts)}):\n"
            f"{', '.join(usernames)}\n\n"
            f"📸 Отправьте фото или видео для поста:"
        )
        return POST_MEDIA_UPLOAD
        
    elif source == 'folder':
        # Показываем список папок
        return show_post_folders(update, context)
    
    elif source == 'select':
        # Показываем список аккаунтов для выбора
        return show_post_accounts_list(update, context, "select")

def show_post_folders(update, context):
    """Показ списка папок для выбора"""
    query = update.callback_query
    accounts = get_instagram_accounts()
    
    # Собираем уникальные папки
    folders = set()
    for account in accounts:
        if account.is_active and account.folder:
            folders.add(account.folder)
    
    if not folders:
        query.edit_message_text("❌ Нет папок с активными аккаунтами.")
        return ConversationHandler.END
    
    keyboard = []
    for folder in sorted(folders):
        keyboard.append([InlineKeyboardButton(
            f"📁 {folder}", 
            callback_data=f"post_folder_{folder}"
        )])
    
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="post_back_to_source")])
    keyboard.append([InlineKeyboardButton("❌ Отмена", callback_data="cancel_publish")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    query.edit_message_text(
        "📁 Выберите папку:",
        reply_markup=reply_markup
    )
    return POST_FOLDER_SELECTION

def handle_post_folder_selection(update, context):
    """Обработка выбора папки"""
    query = update.callback_query
    query.answer()
    
    folder_name = query.data.replace('post_folder_', '')
    
    # Получаем аккаунты из папки
    folder_accounts = get_accounts_by_folder(folder_name)
    
    if not folder_accounts:
        query.edit_message_text(f"❌ В папке '{folder_name}' нет активных аккаунтов.")
        return ConversationHandler.END
    
    account_ids = [acc.id for acc in folder_accounts]
    usernames = [acc.username for acc in folder_accounts]
    
    context.user_data['publish_account_ids'] = account_ids
    context.user_data['publish_account_usernames'] = usernames
    context.user_data['publish_to_all_accounts'] = len(account_ids) > 1
    
    query.edit_message_text(
        f"📁 Выбрана папка '{folder_name}' ({len(folder_accounts)} аккаунтов):\n"
        f"{', '.join(usernames)}\n\n"
        f"📸 Отправьте фото или видео для поста:"
    )
    return POST_MEDIA_UPLOAD

def show_post_accounts_list(update, context, folder_name_or_accounts, page=0):
    """Показ списка аккаунтов для выбора"""
    query = update.callback_query
    
    if folder_name_or_accounts == "select":
        accounts = get_instagram_accounts()
        active_accounts = [acc for acc in accounts if acc.is_active]
    else:
        active_accounts = get_accounts_by_folder(folder_name_or_accounts)
    
    if not active_accounts:
        query.edit_message_text("❌ Нет активных аккаунтов для публикации.")
        return ConversationHandler.END
    
    # Пагинация
    per_page = 10
    start_idx = page * per_page
    end_idx = start_idx + per_page
    page_accounts = active_accounts[start_idx:end_idx]
    
    # Получаем уже выбранные аккаунты
    selected_accounts = context.user_data.get('selected_post_accounts', [])
    
    keyboard = []
    for account in page_accounts:
        status = "✅" if account.id in selected_accounts else "⚪"
        keyboard.append([InlineKeyboardButton(
            f"{status} {account.username}",
            callback_data=f"post_toggle_{account.id}"
        )])
    
    # Кнопки навигации
    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton("⬅️ Назад", callback_data=f"post_page_{page-1}"))
    if end_idx < len(active_accounts):
        nav_buttons.append(InlineKeyboardButton("➡️ Далее", callback_data=f"post_page_{page+1}"))
    
    if nav_buttons:
        keyboard.append(nav_buttons)
    
    # Кнопки действий
    if selected_accounts:
        keyboard.append([InlineKeyboardButton(
            f"✅ Подтвердить выбор ({len(selected_accounts)})",
            callback_data="post_confirm_selection"
        )])
    
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="post_back_to_source")])
    keyboard.append([InlineKeyboardButton("❌ Отмена", callback_data="cancel_publish")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    text = f"📸 Выберите аккаунты для публикации поста:\n\n"
    text += f"Страница {page + 1} из {(len(active_accounts) - 1) // per_page + 1}\n"
    text += f"Выбрано: {len(selected_accounts)} аккаунтов"
    
    query.edit_message_text(text, reply_markup=reply_markup)
    return POST_ACCOUNTS_LIST

def handle_post_account_toggle(update, context):
    """Обработка переключения выбора аккаунта"""
    query = update.callback_query
    query.answer()
    
    account_id = int(query.data.split('_')[-1])
    selected_accounts = context.user_data.get('selected_post_accounts', [])
    
    if account_id in selected_accounts:
        selected_accounts.remove(account_id)
    else:
        selected_accounts.append(account_id)
    
    context.user_data['selected_post_accounts'] = selected_accounts
    
    # Обновляем список
    return show_post_accounts_list(update, context, "select")

def handle_post_confirm_selection(update, context):
    """Подтверждение выбора аккаунтов"""
    query = update.callback_query
    query.answer()
    
    selected_accounts = context.user_data.get('selected_post_accounts', [])
    
    if not selected_accounts:
        query.answer("❌ Не выбрано ни одного аккаунта!", show_alert=True)
        return POST_ACCOUNTS_LIST
    
    # Получаем информацию об аккаунтах
    accounts = [get_instagram_account(acc_id) for acc_id in selected_accounts]
    usernames = [acc.username for acc in accounts if acc]
    
    context.user_data['publish_account_ids'] = selected_accounts
    context.user_data['publish_account_usernames'] = usernames
    context.user_data['publish_to_all_accounts'] = len(selected_accounts) > 1
    
    if len(selected_accounts) == 1:
        context.user_data['publish_account_id'] = selected_accounts[0]
        context.user_data['publish_account_username'] = usernames[0]
    
    query.edit_message_text(
        f"📸 Выбрано аккаунтов: {len(selected_accounts)}\n"
        f"Аккаунты: {', '.join(usernames)}\n\n"
        f"📸 Отправьте фото или видео для поста:"
    )
    return POST_MEDIA_UPLOAD

def handle_media_upload(update, context):
    """Обработка загрузки медиа для поста"""
    user_id = update.effective_user.id
    
    if not is_admin(user_id):
        update.message.reply_text("У вас нет прав для выполнения этой команды.")
        return ConversationHandler.END
    
    # Проверяем, что это для постов и аккаунты выбраны
    publish_type = context.user_data.get('publish_type')
    account_ids = context.user_data.get('publish_account_ids', [])
    
    if publish_type not in ['post', 'scheduled_post'] or not account_ids:
        logger.info(f"📸 POST: Игнорируем - publish_type={publish_type}, account_ids={len(account_ids)}")
        return None
    
    # Получаем информацию о медиа
    media_files = []
    media_type = None
    
    if update.message.photo:
        media_files = [update.message.photo[-1]]
        media_type = 'PHOTO'
    elif update.message.video:
        media_files = [update.message.video]
        media_type = 'VIDEO'
    elif update.message.document:
        media_file = update.message.document
        if media_file.mime_type.startswith('video/'):
            media_files = [media_file]
            media_type = 'VIDEO'
        elif media_file.mime_type.startswith('image/'):
            media_files = [media_file]
            media_type = 'PHOTO'
    elif update.message.media_group:
        # Обработка медиа группы (карусель)
        media_files = []
        for media in update.message.media_group:
            if media.photo:
                media_files.append(media.photo[-1])
            elif media.video:
                media_files.append(media.video)
        media_type = 'CAROUSEL'
    
    if not media_files:
        update.message.reply_text("❌ Пожалуйста, отправьте фото или видео.")
        return POST_MEDIA_UPLOAD
    
    # Сохраняем медиа файлы
    media_paths = []
    for media_file in media_files:
        file_id = media_file.file_id
        media = context.bot.get_file(file_id)
        
        # Определяем расширение файла
        if media_type == 'VIDEO':
            file_extension = '.mp4'
        else:
            file_extension = '.jpg'
        
        # Создаем временный файл
        with tempfile.NamedTemporaryFile(delete=False, suffix=file_extension) as temp_file:
            media_path = temp_file.name
        
        # Скачиваем медиа
        media.download(media_path)
        media_paths.append(media_path)
    
    # Сохраняем пути к медиа
    if len(media_paths) == 1:
        context.user_data['publish_media_path'] = media_paths[0]
    else:
        context.user_data['publish_media_paths'] = media_paths
    
    context.user_data['publish_media_type'] = media_type
    
    # Создаем клавиатуру для действий с медиа
    keyboard = [
        [InlineKeyboardButton("✏️ Добавить подпись", callback_data="post_add_caption")],
        [InlineKeyboardButton("#️⃣ Добавить хештеги", callback_data="post_add_hashtags")],
        [InlineKeyboardButton("✅ Продолжить без подписи", callback_data="post_skip_caption")],
        [InlineKeyboardButton("❌ Отмена", callback_data="cancel_publish")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    # Информация о загруженном медиа
    if media_type == 'CAROUSEL':
        media_info = f"📸 Карусель ({len(media_paths)} файлов)"
    else:
        media_info = f"📸 {media_type}"
    
    # Информация об аккаунтах
    account_ids = context.user_data.get('publish_account_ids', [])
    if len(account_ids) == 1:
        account_username = context.user_data.get('publish_account_username')
        account_info = f"👤 Аккаунт: @{account_username}"
    else:
        account_info = f"👥 Аккаунты: {len(account_ids)} аккаунтов"
    
    update.message.reply_text(
        f"✅ Медиа успешно загружено!\n\n"
        f"{account_info}\n"
        f"{media_info}\n\n"
        f"Что вы хотите сделать дальше?",
        reply_markup=reply_markup
    )
    
    return POST_MEDIA_ACTIONS

def handle_media_actions(update, context):
    """Обработка действий с медиа"""
    query = update.callback_query
    query.answer()
    
    action = query.data.split('_')[-1]
    
    if action == 'caption':
        return show_caption_input(update, context)
    elif action == 'hashtags':
        return show_hashtags_input(update, context)
    elif action == 'skip_caption':
        # Пропускаем подпись и переходим к финальному подтверждению
        context.user_data['publish_caption'] = ""
        return show_final_confirmation(update, context)

def show_caption_input(update, context):
    """Показ интерфейса ввода подписи"""
    query = update.callback_query
    
    current_caption = context.user_data.get('publish_caption', '')
    
    keyboard = [
        [InlineKeyboardButton("🔙 Назад к медиа", callback_data="post_back_to_media")],
        [InlineKeyboardButton("❌ Отмена", callback_data="cancel_publish")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    text = "✏️ Введите подпись к публикации:"
    if current_caption:
        text += f"\n\nТекущая подпись:\n{current_caption}"
    
    query.edit_message_text(text, reply_markup=reply_markup)
    return POST_CAPTION_INPUT

def handle_caption_input(update, context):
    """Обработка ввода подписи"""
    caption = update.message.text
    context.user_data['publish_caption'] = caption
    
    keyboard = [
        [InlineKeyboardButton("✅ Сохранить подпись", callback_data="post_save_caption")],
        [InlineKeyboardButton("✏️ Изменить подпись", callback_data="post_edit_caption")],
        [InlineKeyboardButton("🔙 Назад к медиа", callback_data="post_back_to_media")],
        [InlineKeyboardButton("❌ Отмена", callback_data="cancel_publish")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    update.message.reply_text(
        f"✅ Подпись сохранена:\n\n{caption}\n\nЧто делать дальше?",
        reply_markup=reply_markup
    )
    return POST_CAPTION_ACTIONS

def handle_caption_actions(update, context):
    """Обработка действий с подписью"""
    query = update.callback_query
    query.answer()
    
    action = query.data.split('_')[-1]
    
    if action == 'save_caption':
        return show_final_confirmation(update, context)
    elif action == 'edit_caption':
        return show_caption_input(update, context)
    elif action == 'media':
        return show_media_actions_menu(update, context)

def show_media_actions_menu(update, context):
    """Показ меню действий с медиа"""
    query = update.callback_query
    
    # Получаем информацию о медиа и аккаунтах
    media_type = context.user_data.get('publish_media_type')
    caption = context.user_data.get('publish_caption', '')
    hashtags = context.user_data.get('publish_hashtags', '')
    
    account_ids = context.user_data.get('publish_account_ids', [])
    if len(account_ids) == 1:
        account_username = context.user_data.get('publish_account_username')
        account_info = f"👤 Аккаунт: @{account_username}"
    else:
        account_info = f"👥 Аккаунты: {len(account_ids)} аккаунтов"
    
    keyboard = [
        [InlineKeyboardButton("✏️ Добавить подпись", callback_data="post_add_caption")],
        [InlineKeyboardButton("#️⃣ Добавить хештеги", callback_data="post_add_hashtags")],
        [InlineKeyboardButton("✅ Перейти к публикации", callback_data="post_to_confirmation")],
        [InlineKeyboardButton("❌ Отмена", callback_data="cancel_publish")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    text = f"📸 Данные для публикации:\n\n"
    text += f"{account_info}\n"
    text += f"📱 Медиа: {media_type}\n"
    text += f"✏️ Подпись: {caption or '(не задана)'}\n"
    text += f"#️⃣ Хештеги: {hashtags or '(не заданы)'}\n\n"
    text += "Что вы хотите сделать?"
    
    query.edit_message_text(text, reply_markup=reply_markup)
    return POST_MEDIA_ACTIONS

def show_hashtags_input(update, context):
    """Показ интерфейса ввода хештегов"""
    query = update.callback_query
    
    current_hashtags = context.user_data.get('publish_hashtags', '')
    
    keyboard = [
        [InlineKeyboardButton("🔙 Назад к медиа", callback_data="post_back_to_media")],
        [InlineKeyboardButton("❌ Отмена", callback_data="cancel_publish")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    text = "#️⃣ Введите хештеги для публикации:"
    if current_hashtags:
        text += f"\n\nТекущие хештеги:\n{current_hashtags}"
    
    query.edit_message_text(text, reply_markup=reply_markup)
    return POST_HASHTAGS_INPUT

def handle_hashtags_input(update, context):
    """Обработка ввода хештегов"""
    hashtags = update.message.text
    context.user_data['publish_hashtags'] = hashtags
    
    keyboard = [
        [InlineKeyboardButton("✅ Сохранить хештеги", callback_data="post_save_hashtags")],
        [InlineKeyboardButton("✏️ Изменить хештеги", callback_data="post_edit_hashtags")],
        [InlineKeyboardButton("🔙 Назад к медиа", callback_data="post_back_to_media")],
        [InlineKeyboardButton("❌ Отмена", callback_data="cancel_publish")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    update.message.reply_text(
        f"✅ Хештеги сохранены:\n\n{hashtags}\n\nЧто делать дальше?",
        reply_markup=reply_markup
    )
    return POST_HASHTAGS_ACTIONS

def handle_hashtags_actions(update, context):
    """Обработка действий с хештегами"""
    query = update.callback_query
    query.answer()
    
    action = query.data.split('_')[-1]
    
    if action == 'save_hashtags':
        return show_final_confirmation(update, context)
    elif action == 'edit_hashtags':
        return show_hashtags_input(update, context)
    elif action == 'media':
        return show_media_actions_menu(update, context)

def show_final_confirmation(update, context):
    """Показ финального подтверждения публикации"""
    query = update.callback_query
    
    # Получаем все данные для публикации
    media_type = context.user_data.get('publish_media_type')
    caption = context.user_data.get('publish_caption', '')
    hashtags = context.user_data.get('publish_hashtags', '')
    is_scheduled = context.user_data.get('is_scheduled_post', False)
    
    account_ids = context.user_data.get('publish_account_ids', [])
    if len(account_ids) == 1:
        account_username = context.user_data.get('publish_account_username')
        account_info = f"👤 Аккаунт: @{account_username}"
    else:
        account_info = f"👥 Аккаунты: {len(account_ids)} аккаунтов"
    
    # Объединяем подпись и хештеги
    full_caption = caption
    if hashtags:
        if full_caption:
            full_caption += f"\n\n{hashtags}"
        else:
            full_caption = hashtags
    
    # Создаем клавиатуру
    if is_scheduled:
        keyboard = [
            [InlineKeyboardButton("🗓️ Выбрать время публикации", callback_data="schedule_publish")],
            [InlineKeyboardButton("🔙 Назад к медиа", callback_data="post_back_to_media")],
            [InlineKeyboardButton("❌ Отмена", callback_data="cancel_publish")]
        ]
        title_prefix = "📅 Запланированная публикация"
    else:
        keyboard = [
            [
                InlineKeyboardButton("✅ Опубликовать сейчас", callback_data="confirm_publish_now"),
                InlineKeyboardButton("⏰ Запланировать", callback_data="schedule_publish")
            ],
            [InlineKeyboardButton("🔙 Назад к медиа", callback_data="post_back_to_media")],
            [InlineKeyboardButton("❌ Отмена", callback_data="cancel_publish")]
        ]
        title_prefix = "📸 Публикация поста"
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    text = f"*{title_prefix}*\n\n"
    text += f"{account_info}\n"
    text += f"📱 Медиа: {media_type}\n"
    text += f"✏️ Подпись: {full_caption or '(без подписи)'}\n\n"
    text += "Подтвердите публикацию:"
    
    query.edit_message_text(text, reply_markup=reply_markup, parse_mode='Markdown')
    return POST_FINAL_CONFIRMATION

def handle_final_confirmation(update, context):
    """Обработка финального подтверждения"""
    query = update.callback_query
    query.answer()
    
    action = query.data
    
    if action == 'confirm_publish_now':
        return execute_publish_task(update, context)
    elif action == 'schedule_publish':
        return schedule_publish_callback(update, context)
    elif action == 'post_back_to_media':
        return show_media_actions_menu(update, context)
    elif action == 'cancel_publish':
        return cancel_publish(update, context)

def execute_publish_task(update, context):
    """Выполнение задачи публикации"""
    query = update.callback_query
    user_id = update.effective_user.id
    
    try:
        # Получаем данные для публикации
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
        
        # Создаем задачи для каждого аккаунта
        task_ids = []
        for account_id in account_ids:
            # Подготавливаем данные для задачи
            task_data = {
                'media_path': media_path,
                'media_paths': media_paths,
                'media_type': media_type,
                'caption': full_caption,
                'account_id': account_id
            }
            
            # Создаем задачу в базе данных
            task_id = create_publish_task(
                user_id=user_id,
                task_type=TaskType.POST,
                account_id=account_id,
                media_path=media_path or json.dumps(media_paths),
                caption=full_caption,
                status=TaskStatus.PENDING
            )
            
            # Добавляем задачу в очередь
            add_task_to_queue(task_id, task_data)
            task_ids.append(task_id)
        
        # Отправляем подтверждение
        if len(account_ids) == 1:
            account_username = context.user_data.get('publish_account_username')
            message = f"✅ Пост добавлен в очередь публикации!\n\n"
            message += f"👤 Аккаунт: @{account_username}\n"
            message += f"📱 Медиа: {media_type}\n"
            message += f"🆔 ID задачи: {task_ids[0]}"
        else:
            message = f"✅ Пост добавлен в очередь публикации!\n\n"
            message += f"👥 Аккаунты: {len(account_ids)} аккаунтов\n"
            message += f"📱 Медиа: {media_type}\n"
            message += f"🆔 ID задач: {', '.join(map(str, task_ids))}"
        
        query.edit_message_text(message)
        
        # Очищаем данные пользователя
        cleanup_user_data(context)
        
        return ConversationHandler.END
        
    except Exception as e:
        logger.error(f"Ошибка при создании задачи публикации: {e}")
        query.edit_message_text(f"❌ Ошибка при создании задачи: {str(e)}")
        return ConversationHandler.END

def schedule_publish_callback(update, context):
    """Обработка запроса на планирование публикации"""
    # Импортируем функцию планирования
    from .scheduler import choose_schedule
    return choose_schedule(update, context)

def get_post_conversation():
    """Возвращает ConversationHandler для постов"""
    return ConversationHandler(
        entry_points=[
            CallbackQueryHandler(start_post_publish, pattern='^publish_post$'),
            CallbackQueryHandler(start_post_publish, pattern='^schedule_post$')
        ],
        states={
            POST_SOURCE_SELECTION: [
                CallbackQueryHandler(handle_post_source_selection, pattern='^post_source_')
            ],
            POST_FOLDER_SELECTION: [
                CallbackQueryHandler(handle_post_folder_selection, pattern='^post_folder_'),
                CallbackQueryHandler(handle_post_source_selection, pattern='^post_back_to_source$')
            ],
            POST_ACCOUNTS_LIST: [
                CallbackQueryHandler(handle_post_account_toggle, pattern='^post_toggle_'),
                CallbackQueryHandler(show_post_accounts_list, pattern='^post_page_'),
                CallbackQueryHandler(handle_post_confirm_selection, pattern='^post_confirm_selection$'),
                CallbackQueryHandler(handle_post_source_selection, pattern='^post_back_to_source$')
            ],
            POST_MEDIA_UPLOAD: [
                MessageHandler(Filters.photo | Filters.video | Filters.document, handle_media_upload)
            ],
            POST_MEDIA_ACTIONS: [
                CallbackQueryHandler(handle_media_actions, pattern='^post_add_'),
                CallbackQueryHandler(handle_media_actions, pattern='^post_skip_'),
                CallbackQueryHandler(show_final_confirmation, pattern='^post_to_confirmation$'),
                CallbackQueryHandler(show_media_actions_menu, pattern='^post_back_to_media$')
            ],
            POST_CAPTION_INPUT: [
                MessageHandler(Filters.text & ~Filters.command, handle_caption_input),
                CallbackQueryHandler(show_media_actions_menu, pattern='^post_back_to_media$')
            ],
            POST_CAPTION_ACTIONS: [
                CallbackQueryHandler(handle_caption_actions, pattern='^post_save_'),
                CallbackQueryHandler(handle_caption_actions, pattern='^post_edit_'),
                CallbackQueryHandler(show_media_actions_menu, pattern='^post_back_to_media$')
            ],
            POST_HASHTAGS_INPUT: [
                MessageHandler(Filters.text & ~Filters.command, handle_hashtags_input),
                CallbackQueryHandler(show_media_actions_menu, pattern='^post_back_to_media$')
            ],
            POST_HASHTAGS_ACTIONS: [
                CallbackQueryHandler(handle_hashtags_actions, pattern='^post_save_'),
                CallbackQueryHandler(handle_hashtags_actions, pattern='^post_edit_'),
                CallbackQueryHandler(show_media_actions_menu, pattern='^post_back_to_media$')
            ],
            POST_FINAL_CONFIRMATION: [
                CallbackQueryHandler(handle_final_confirmation, pattern='^confirm_publish_now$'),
                CallbackQueryHandler(handle_final_confirmation, pattern='^schedule_publish$'),
                CallbackQueryHandler(show_media_actions_menu, pattern='^post_back_to_media$')
            ]
        },
        fallbacks=[
            CallbackQueryHandler(cancel_publish, pattern='^cancel_publish$'),
            CommandHandler('cancel', cancel_publish)
        ]
    )

def get_post_selector():
    """Возвращает селектор аккаунтов для постов"""
    return post_selector 
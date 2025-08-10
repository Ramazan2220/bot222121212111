"""
Обработчики для публикации постов (фото/видео)
"""

import os
import tempfile
import json
import logging
import threading
from datetime import datetime

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, ParseMode
from telegram.ext import ConversationHandler, CallbackQueryHandler, MessageHandler, Filters

from database.db_manager import get_instagram_account, get_instagram_accounts, create_publish_task
from database.models import TaskType, TaskStatus
from utils.task_queue import add_task_to_queue, get_task_status
from telegram_bot.utils.account_selection import create_account_selector

# Добавляем импорт для uuid
import uuid

logger = logging.getLogger(__name__)

# Состояния для публикации
CHOOSE_ACCOUNT, UPLOAD_MEDIA, ENTER_CAPTION, ENTER_HASHTAGS, CONFIRM_PUBLISH, CHOOSE_SCHEDULE, CHOOSE_HIDE_FROM_FEED = range(10, 17)

def is_admin(user_id):
    """Проверка прав администратора"""
    return True  # Временно для всех пользователей

def start_post_publish(update, context):
    """Начинает процесс публикации поста"""
    query = update.callback_query
    query.answer()
    
    # Устанавливаем тип публикации
    context.user_data['publish_type'] = 'post'
    context.user_data['publish_media_type'] = 'PHOTO'
    
    # Показываем меню выбора источника аккаунтов
    keyboard = []
    
    # Получаем папки
    from database.db_manager import get_account_groups
    folders = get_account_groups()
    if folders:
        keyboard.append([InlineKeyboardButton("📁 Выбрать из папки", callback_data="post_from_folders")])
    
    keyboard.append([InlineKeyboardButton("📋 Все аккаунты", callback_data="post_all_accounts")])
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="menu_publications")])
    
    query.edit_message_text(
        "📸 Публикация поста\n\nВыберите источник аккаунтов:",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    
    return PostStates.CHOOSE_ACCOUNT

def handle_post_source_selection(update, context):
    """Обрабатывает выбор источника аккаунтов для постов"""
    query = update.callback_query
    query.answer()
    
    if query.data == "post_from_folders":
        # Показываем список папок
        return show_post_folders(update, context)
    
    elif query.data == "post_all_accounts":
        # Показываем все аккаунты
        return show_post_accounts_list(update, context, "all")
    
    elif query.data == "post_back_to_menu":
        # Возвращаемся в меню публикаций
        from telegram_bot.handlers.system_handlers import show_publish_menu
        return show_publish_menu(update, context)

def show_post_folders(update, context):
    """Показывает список папок для выбора"""
    query = update.callback_query
    
    from database.db_manager import get_account_groups
    folders = get_account_groups()
    
    if not folders:
        query.edit_message_text(
            "❌ Папки не найдены",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Назад", callback_data="post_back_to_menu")]
            ])
        )
        return PostStates.CHOOSE_ACCOUNT
    
    keyboard = []
    for folder in folders:
        keyboard.append([InlineKeyboardButton(
            f"📁 {folder.name}",
            callback_data=f"post_folder_{folder.id}"
        )])
    
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="post_back_to_menu")])
    
    query.edit_message_text(
        "📁 Выберите папку:",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    
    return PostStates.CHOOSE_ACCOUNT

def show_post_accounts_list(update, context, folder_id_or_all, page=0):
    """Показывает список аккаунтов для выбора"""
    query = update.callback_query
    
    from database.db_manager import get_instagram_accounts
    
    # Получаем аккаунты
    if folder_id_or_all == "all":
        accounts = get_instagram_accounts()
        folder_name = "Все аккаунты"
    else:
        # Получаем аккаунты из папки
        from database.db_manager import get_accounts_by_group
        accounts = get_accounts_by_group(folder_id_or_all)
        folder_name = f"Папка {folder_id_or_all}"
    
    if not accounts:
        query.edit_message_text(
            "❌ Аккаунты не найдены",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Назад", callback_data="post_back_to_menu")]
            ])
        )
        return PostStates.CHOOSE_ACCOUNT
    
    # Инициализируем выбранные аккаунты
    if 'selected_post_accounts' not in context.user_data:
        context.user_data['selected_post_accounts'] = []
    
    # Пагинация
    accounts_per_page = 8
    total_pages = (len(accounts) + accounts_per_page - 1) // accounts_per_page
    page = max(0, min(page, total_pages - 1))
    
    start_idx = page * accounts_per_page
    end_idx = start_idx + accounts_per_page
    page_accounts = accounts[start_idx:end_idx]
    
    # Формируем клавиатуру
    keyboard = []
    
    # Кнопки аккаунтов
    for account in page_accounts:
        selected = account.id in context.user_data['selected_post_accounts']
        checkbox = "✅" if selected else "☐"
        status = "✅" if account.is_active else "❌"
        
        keyboard.append([InlineKeyboardButton(
            f"{checkbox} {status} @{account.username}",
            callback_data=f"post_toggle_{account.id}"
        )])
    
    # Кнопки управления
    control_buttons = []
    if len(context.user_data['selected_post_accounts']) > 0:
        control_buttons.append(InlineKeyboardButton("❌ Сбросить все", callback_data="post_deselect_all"))
    
    if len(context.user_data['selected_post_accounts']) < len(accounts):
        control_buttons.append(InlineKeyboardButton("✅ Выбрать все", callback_data="post_select_all"))
    
    if control_buttons:
        keyboard.append(control_buttons)
    
    # Пагинация
    if total_pages > 1:
        nav_buttons = []
        if page > 0:
            nav_buttons.append(InlineKeyboardButton("◀️", callback_data=f"post_page_{page-1}"))
        
        nav_buttons.append(InlineKeyboardButton(f"{page+1}/{total_pages}", callback_data="post_page_info"))
        
        if page < total_pages - 1:
            nav_buttons.append(InlineKeyboardButton("▶️", callback_data=f"post_page_{page+1}"))
        
        keyboard.append(nav_buttons)
    
    # Кнопки действий
    action_buttons = []
    if len(context.user_data['selected_post_accounts']) > 0:
        action_buttons.append(InlineKeyboardButton(
            f"📤 Продолжить ({len(context.user_data['selected_post_accounts'])})",
            callback_data="post_confirm_selection"
        ))
    
    action_buttons.append(InlineKeyboardButton("🔙 Назад", callback_data="post_back_to_menu"))
    keyboard.append(action_buttons)
    
    # Формируем текст
    selected_count = len(context.user_data['selected_post_accounts'])
    
    text = f"🎯 Выбор аккаунтов для публикации поста\n\n"
    text += f"📁 {folder_name}\n"
    text += f"Выбрано: {selected_count} из {len(accounts)}\n\n"
    
    if selected_count > 1:
        text += "⚠️ При выборе нескольких аккаунтов контент будет автоматически уникализирован\n\n"
    
    text += "Выберите аккаунты для публикации:"
    
    query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
    
    return PostStates.CHOOSE_ACCOUNT

def handle_post_account_toggle(update, context):
    """Обрабатывает переключение выбора аккаунта"""
    query = update.callback_query
    query.answer()
    
    account_id = int(query.data.replace("post_toggle_", ""))
    
    if 'selected_post_accounts' not in context.user_data:
        context.user_data['selected_post_accounts'] = []
    
    if account_id in context.user_data['selected_post_accounts']:
        context.user_data['selected_post_accounts'].remove(account_id)
    else:
        context.user_data['selected_post_accounts'].append(account_id)
    
    # Обновляем клавиатуру
    return show_post_accounts_list(update, context, "all")

def handle_post_confirm_selection(update, context):
    """Подтверждает выбор аккаунтов и переходит к загрузке медиа"""
    query = update.callback_query
    query.answer()
    
    selected_accounts = context.user_data.get('selected_post_accounts', [])
    
    if not selected_accounts:
        query.edit_message_text(
            "❌ Не выбран ни один аккаунт",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Назад", callback_data="post_back_to_menu")]
            ])
        )
        return PostStates.CHOOSE_ACCOUNT
    
    # Сохраняем выбранные аккаунты
    context.user_data['selected_accounts'] = selected_accounts
    
    # Получаем информацию об аккаунтах
    from database.db_manager import get_instagram_account
    accounts = [get_instagram_account(acc_id) for acc_id in selected_accounts]
    usernames = [acc.username for acc in accounts if acc]
    
    # Показываем запрос на загрузку медиа
    text = f"📸 Публикация поста\n\n"
    text += f"👥 Выбрано аккаунтов: {len(selected_accounts)}\n"
    text += f"📤 Аккаунты: {', '.join([f'@{u}' for u in usernames[:3]])}"
    if len(usernames) > 3:
        text += f" и ещё {len(usernames) - 3}..."
    text += "\n\n"
    
    if len(selected_accounts) > 1:
        text += "🎨 Контент будет уникализирован для каждого аккаунта\n\n"
    
    text += "📎 Отправьте фото или видео для публикации:"
    
    query.edit_message_text(text)
    
    return PostStates.MEDIA_UPLOAD

def handle_post_media_upload(update, context):
    """Обрабатывает загрузку медиа для поста"""
    try:
        # Инициализируем список медиа файлов
        if 'media_files' not in context.user_data:
            context.user_data['media_files'] = []
        
        media_files = context.user_data['media_files']
        
        # Проверяем тип сообщения
        if update.message.photo:
            # Фото
            photo = update.message.photo[-1]
            file_obj = context.bot.get_file(photo.file_id)
            
            # Создаем уникальное имя файла
            import uuid
            filename = f"photo_{uuid.uuid4().hex[:8]}.jpg"
            file_path = os.path.join("media", filename)
            
            # Создаем папку если нужно
            os.makedirs("media", exist_ok=True)
            
            # Скачиваем файл
            file_obj.download(file_path)
            
            media_files.append({
                'type': 'photo',
                'path': file_path,
                'original_filename': filename
            })
            
        elif update.message.video:
            # Видео
            video = update.message.video
            file_obj = context.bot.get_file(video.file_id)
            
            # Создаем уникальное имя файла
            import uuid
            filename = f"video_{uuid.uuid4().hex[:8]}.mp4"
            file_path = os.path.join("media", filename)
            
            # Создаем папку если нужно
            os.makedirs("media", exist_ok=True)
            
            # Скачиваем файл
            file_obj.download(file_path)
            
            media_files.append({
                'type': 'video',
                'path': file_path,
                'original_filename': filename
            })
            
        elif update.message.document:
            # Документ (может быть медиа файлом)
            document = update.message.document
            file_name = document.file_name or "unknown"
            file_ext = os.path.splitext(file_name)[1].lower()
            
            # Проверяем, что это медиа файл
            if file_ext in ['.jpg', '.jpeg', '.png', '.webp', '.mp4', '.mov', '.avi', '.mkv']:
                file_obj = context.bot.get_file(document.file_id)
                
                # Создаем уникальное имя файла
                import uuid
                filename = f"media_{uuid.uuid4().hex[:8]}{file_ext}"
                file_path = os.path.join("media", filename)
                
                # Создаем папку если нужно
                os.makedirs("media", exist_ok=True)
                
                # Скачиваем файл
                file_obj.download(file_path)
                
                media_type = 'photo' if file_ext in ['.jpg', '.jpeg', '.png', '.webp'] else 'video'
                media_files.append({
                    'type': media_type,
                    'path': file_path,
                    'original_filename': file_name
                })
            else:
                update.message.reply_text("❌ Неподдерживаемый формат файла. Поддерживаются: JPG, PNG, MP4, MOV")
                return PostStates.MEDIA_UPLOAD
        else:
            update.message.reply_text("❌ Отправьте фото, видео или медиа файл")
            return PostStates.MEDIA_UPLOAD
        
        # Обновляем список медиа файлов
        context.user_data['media_files'] = media_files
        
        # Показываем информацию о загруженных файлах
        total_files = len(media_files)
        
        text = f"✅ Загружено файлов: {total_files}\n\n"
        
        for i, file_info in enumerate(media_files, 1):
            file_type = "📷 Фото" if file_info['type'] == 'photo' else "🎥 Видео"
            text += f"{i}. {file_type} - {file_info['original_filename']}\n"
        
        # Определяем тип публикации
        if total_files == 1:
            if media_files[0]['type'] == 'photo':
                text += "\n📤 Тип публикации: Обычное фото"
            else:
                text += "\n📤 Тип публикации: Видео"
        else:
            # Проверяем, что все файлы - фото
            all_photos = all(f['type'] == 'photo' for f in media_files)
            if all_photos and total_files <= 10:
                text += f"\n🎠 Тип публикации: Карусель ({total_files} фото)"
            elif not all_photos:
                text += "\n❌ Ошибка: Для карусели можно использовать только фото"
                text += "\nУдалите видео файлы или начните заново"
            else:
                text += f"\n❌ Ошибка: Максимум 10 фото в карусели (загружено {total_files})"
        
        text += "\n\n📎 Отправьте еще файлы или нажмите 'Продолжить'"
        
        # Клавиатура
        keyboard = [
            [InlineKeyboardButton("📝 Продолжить", callback_data="post_continue_to_caption")],
            [InlineKeyboardButton("🗑 Очистить все", callback_data="post_clear_media")],
            [InlineKeyboardButton("❌ Отмена", callback_data="cancel_publish")]
        ]
        
        update.message.reply_text(
            text,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        
        return PostStates.MEDIA_UPLOAD
        
    except Exception as e:
        logger.error(f"Ошибка при загрузке медиа: {e}")
        update.message.reply_text(f"❌ Ошибка при загрузке файла: {e}")
        return PostStates.MEDIA_UPLOAD

def handle_post_media_actions(update, context):
    """Обрабатывает действия с медиа файлами"""
    query = update.callback_query
    query.answer()
    
    if query.data == "post_continue_to_caption":
        # Проверяем медиа файлы
        media_files = context.user_data.get('media_files', [])
        
        if not media_files:
            query.edit_message_text(
                "❌ Не загружено ни одного медиа файла",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("◀️ Назад", callback_data="post_back_to_accounts")
                ]])
            )
            return PostStates.MEDIA_UPLOAD
        
        # Проверяем корректность для карусели
        if len(media_files) > 1:
            all_photos = all(f['type'] == 'photo' for f in media_files)
            if not all_photos:
                query.edit_message_text(
                    "❌ Для карусели можно использовать только фото",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("🗑 Очистить все", callback_data="post_clear_media"),
                        InlineKeyboardButton("❌ Отмена", callback_data="cancel_publish")
                    ]])
                )
                return PostStates.MEDIA_UPLOAD
            
            if len(media_files) > 10:
                query.edit_message_text(
                    f"❌ Максимум 10 фото в карусели (загружено {len(media_files)})",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("🗑 Очистить все", callback_data="post_clear_media"),
                        InlineKeyboardButton("❌ Отмена", callback_data="cancel_publish")
                    ]])
                )
                return PostStates.MEDIA_UPLOAD
        
        # Переходим к вводу описания
        return show_post_caption_input(update, context)
        
    elif query.data == "post_clear_media":
        # Очищаем медиа файлы
        media_files = context.user_data.get('media_files', [])
        
        # Удаляем файлы с диска
        for file_info in media_files:
            try:
                if os.path.exists(file_info['path']):
                    os.remove(file_info['path'])
            except:
                pass
        
        # Очищаем из контекста
        context.user_data['media_files'] = []
        
        query.edit_message_text("🗑 Все файлы очищены\n\n📎 Отправьте медиа файлы для публикации:")
        
        return PostStates.MEDIA_UPLOAD

def show_post_caption_input(update, context):
    """Показывает экран ввода описания"""
    query = update.callback_query
    
    selected_accounts = context.user_data.get('selected_accounts', [])
    media_files = context.user_data.get('media_files', [])
    
    text = f"📝 Введите описание для публикации\n\n"
    text += f"📤 Аккаунтов: {len(selected_accounts)}\n"
    text += f"📁 Файлов: {len(media_files)}\n"
    
    if len(media_files) > 1:
        text += f"🎠 Тип: Карусель\n"
    elif media_files[0]['type'] == 'photo':
        text += f"📷 Тип: Фото\n"
    else:
        text += f"🎥 Тип: Видео\n"
    
    if len(selected_accounts) > 1:
        text += "\n🎨 Контент будет уникализирован для каждого аккаунта\n"
    
    text += "\n✍️ Отправьте текст описания или нажмите 'Без описания':"
    
    keyboard = [
        [InlineKeyboardButton("📝 Без описания", callback_data="post_no_caption")],
        [InlineKeyboardButton("◀️ Назад к медиа", callback_data="post_back_to_media")]
    ]
    
    query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    
    return PostStates.ENTER_CAPTION

def handle_post_caption_input(update, context):
    """Обрабатывает ввод описания"""
    try:
        caption = update.message.text
        context.user_data['caption'] = caption
        
        # Переходим к вводу хештегов
        return show_post_hashtags_input(update, context)
        
    except Exception as e:
        logger.error(f"Ошибка при обработке описания: {e}")
        update.message.reply_text(f"❌ Ошибка: {e}")
        return PostStates.ENTER_CAPTION

def handle_post_caption_actions(update, context):
    """Обрабатывает действия с описанием"""
    query = update.callback_query
    query.answer()
    
    if query.data == "post_no_caption":
        # Без описания
        context.user_data['caption'] = ""
        return show_post_hashtags_input(update, context)
        
    elif query.data == "post_back_to_media":
        # Возвращаемся к медиа
        media_files = context.user_data.get('media_files', [])
        
        if not media_files:
            query.edit_message_text("📎 Отправьте медиа файлы для публикации:")
            return PostStates.MEDIA_UPLOAD
        
        # Показываем текущие медиа файлы
        text = f"📁 Загружено файлов: {len(media_files)}\n\n"
        
        for i, file_info in enumerate(media_files, 1):
            file_type = "📷 Фото" if file_info['type'] == 'photo' else "🎥 Видео"
            text += f"{i}. {file_type} - {file_info['original_filename']}\n"
        
        text += "\n📎 Отправьте еще файлы или нажмите 'Продолжить'"
        
        keyboard = [
            [InlineKeyboardButton("📝 Продолжить", callback_data="post_continue_to_caption")],
            [InlineKeyboardButton("🗑 Очистить все", callback_data="post_clear_media")],
            [InlineKeyboardButton("❌ Отмена", callback_data="cancel_publish")]
        ]
        
        query.edit_message_text(
            text,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        
        return PostStates.MEDIA_UPLOAD

def show_post_hashtags_input(update, context):
    """Показывает экран ввода хештегов"""
    try:
        selected_accounts = context.user_data.get('selected_accounts', [])
        media_files = context.user_data.get('media_files', [])
        caption = context.user_data.get('caption', "")
        
        text = f"🏷 Введите хештеги для публикации\n\n"
        text += f"📤 Аккаунтов: {len(selected_accounts)}\n"
        text += f"📁 Файлов: {len(media_files)}\n"
        
        if caption:
            preview = caption[:50] + "..." if len(caption) > 50 else caption
            text += f"📝 Описание: {preview}\n"
        else:
            text += f"📝 Описание: Без описания\n"
        
        text += "\n🏷 Введите хештеги (например: #nature #beautiful #photo)\n"
        text += "или нажмите 'Без хештегов':"
        
        keyboard = [
            [InlineKeyboardButton("🏷 Без хештегов", callback_data="post_no_hashtags")],
            [InlineKeyboardButton("◀️ Назад к описанию", callback_data="post_back_to_caption")]
        ]
        
        if hasattr(update, 'callback_query') and update.callback_query:
            update.callback_query.edit_message_text(
                text,
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        else:
            update.message.reply_text(
                text,
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        
        return PostStates.ENTER_HASHTAGS
        
    except Exception as e:
        logger.error(f"Ошибка при показе ввода хештегов: {e}")
        if hasattr(update, 'callback_query') and update.callback_query:
            update.callback_query.edit_message_text(f"❌ Ошибка: {e}")
        else:
            update.message.reply_text(f"❌ Ошибка: {e}")
        return PostStates.ENTER_HASHTAGS

def handle_post_hashtags_input(update, context):
    """Обрабатывает ввод хештегов"""
    try:
        hashtags = update.message.text
        context.user_data['hashtags'] = hashtags
        
        # Переходим к финальному подтверждению
        return show_post_final_confirmation(update, context)
        
    except Exception as e:
        logger.error(f"Ошибка при обработке хештегов: {e}")
        update.message.reply_text(f"❌ Ошибка: {e}")
        return PostStates.ENTER_HASHTAGS

def handle_post_hashtags_actions(update, context):
    """Обрабатывает действия с хештегами"""
    query = update.callback_query
    query.answer()
    
    if query.data == "post_no_hashtags":
        # Без хештегов
        context.user_data['hashtags'] = ""
        return show_post_final_confirmation(update, context)
        
    elif query.data == "post_back_to_caption":
        # Возвращаемся к описанию
        return show_post_caption_input(update, context)

def show_post_final_confirmation(update, context):
    """Показывает финальное подтверждение публикации"""
    try:
        selected_accounts = context.user_data.get('selected_accounts', [])
        media_files = context.user_data.get('media_files', [])
        caption = context.user_data.get('caption', "")
        hashtags = context.user_data.get('hashtags', "")
        
        # Получаем информацию о аккаунтах
        from database.db_manager import get_instagram_account
        accounts = []
        for account_id in selected_accounts:
            account = get_instagram_account(account_id)
            if account:
                accounts.append(account)
        
        # Формируем текст подтверждения
        text = f"**Подтверждение публикации**\n\n"
        
        # Информация о медиа
        if len(media_files) == 1:
            if media_files[0]['type'] == 'photo':
                media_info = "PHOTO"
            else:
                media_info = "VIDEO"
        else:
            media_info = "CAROUSEL"
        
        text += f"👥 Аккаунты: {len(accounts)} шт.\n"
        text += f"📄 Тип: Пост\n"
        text += f"📱 Медиа: {media_info}\n"
        
        # Информация об описании
        if caption:
            preview = caption[:100] + "..." if len(caption) > 100 else caption
            text += f"✏️ Подпись: {preview}\n"
        
        # Информация о хештегах
        if hashtags:
            hashtags_preview = hashtags[:50] + "..." if len(hashtags) > 50 else hashtags
            text += f"#️⃣ Хештеги: {hashtags_preview}\n"
        
        # Проверяем, это запланированная публикация или нет
        is_scheduled = context.user_data.get('is_scheduled_post', False)
        
        # Клавиатура
        if is_scheduled:
            # Для запланированных постов только кнопка планирования
            keyboard = [
                [InlineKeyboardButton("🗓️ Выбрать время публикации", callback_data="post_schedule_time")],
                [InlineKeyboardButton("🔙 Изменить подпись", callback_data="post_back_to_caption")],
                [InlineKeyboardButton("❌ Отмена", callback_data="cancel_publish")]
            ]
        else:
            # Для обычных постов - публикация и планирование
            keyboard = [
                [InlineKeyboardButton("✅ Опубликовать", callback_data="post_confirm_publish"), 
                 InlineKeyboardButton("⏰ Запланировать", callback_data="post_schedule_publish")],
                [InlineKeyboardButton("🔙 Изменить подпись", callback_data="post_back_to_caption")],
                [InlineKeyboardButton("❌ Отмена", callback_data="cancel_publish")]
            ]
        
        if hasattr(update, 'callback_query') and update.callback_query:
            update.callback_query.edit_message_text(
                text,
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        else:
            update.message.reply_text(
                text,
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        
        return PostStates.CONFIRM_PUBLISH
        
    except Exception as e:
        logger.error(f"Ошибка при показе подтверждения: {e}")
        if hasattr(update, 'callback_query') and update.callback_query:
            update.callback_query.edit_message_text(f"❌ Ошибка: {e}")
        else:
            update.message.reply_text(f"❌ Ошибка: {e}")
        return PostStates.CONFIRM_PUBLISH

def handle_post_final_confirmation(update, context):
    """Обрабатывает финальное подтверждение"""
    query = update.callback_query
    query.answer()
    
    if query.data == "post_confirm_publish":
        # Публикуем сейчас
        return execute_post_publish(update, context)
        
    elif query.data == "post_schedule_publish":
        # Устанавливаем флаг планирования и показываем выбор времени
        context.user_data['is_scheduled_post'] = True
        return show_post_schedule_time(update, context)
        
    elif query.data == "post_back_to_caption":
        # Возвращаемся к описанию
        return show_post_caption_input(update, context)

def show_post_schedule_time(update, context):
    """Показывает выбор времени для планирования"""
    query = update.callback_query
    
    text = "🗓️ Выберите время публикации:\n\n"
    text += "Введите дату и время в формате:\n"
    text += "ДД.ММ.ГГГГ ЧЧ:ММ\n\n"
    text += "Например: 25.12.2024 15:30"
    
    keyboard = [
        [InlineKeyboardButton("◀️ Назад к подтверждению", callback_data="post_back_to_confirmation")],
        [InlineKeyboardButton("❌ Отмена", callback_data="cancel_publish")]
    ]
    
    query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    
    return PostStates.SCHEDULE_TIME

def handle_post_schedule_time(update, context):
    """Обрабатывает ввод времени для планирования"""
    try:
        time_str = update.message.text
        
        # Парсим время
        from datetime import datetime
        try:
            scheduled_time = datetime.strptime(time_str, "%d.%m.%Y %H:%M")
        except ValueError:
            update.message.reply_text(
                "❌ Неверный формат времени. Используйте: ДД.ММ.ГГГГ ЧЧ:ММ"
            )
            return PostStates.SCHEDULE_TIME
        
        # Проверяем, что время в будущем
        if scheduled_time <= datetime.now():
            update.message.reply_text(
                "❌ Время должно быть в будущем"
            )
            return PostStates.SCHEDULE_TIME
        
        # Сохраняем время и планируем публикацию
        context.user_data['scheduled_time'] = scheduled_time
        
        # Создаем запланированную задачу
        return execute_post_schedule(update, context)
        
    except Exception as e:
        logger.error(f"Ошибка при обработке времени: {e}")
        update.message.reply_text(f"❌ Ошибка: {e}")
        return PostStates.SCHEDULE_TIME

def execute_post_publish(update, context):
    """Выполняет публикацию поста"""
    query = update.callback_query
    
    try:
        # Получаем данные
        selected_accounts = context.user_data.get('selected_accounts', [])
        media_files = context.user_data.get('media_files', [])
        caption = context.user_data.get('caption', "")
        hashtags = context.user_data.get('hashtags', "")
        
        if not selected_accounts or not media_files:
            query.edit_message_text("❌ Недостаточно данных для публикации")
            return ConversationHandler.END
        
        # Подготавливаем данные для публикации
        media_paths = [f['path'] for f in media_files]
        full_caption = f"{caption}\n\n{hashtags}".strip()
        
        # Создаем задачи публикации
        from database.db_manager import create_publish_task
        from database.models import TaskType
        
        task_type = TaskType.PHOTO if media_files[0]['type'] == 'photo' else TaskType.VIDEO
        if len(media_files) > 1:
            task_type = TaskType.CAROUSEL
        
        tasks_created = 0
        for account_id in selected_accounts:
            try:
                # Создаем задачу
                task = create_publish_task(
                    account_id=account_id,
                    task_type=task_type,
                    media_path=media_paths[0] if len(media_paths) == 1 else media_paths,
                    caption=full_caption,
                    hashtags=hashtags
                )
                
                if task:
                    tasks_created += 1
                    
            except Exception as e:
                logger.error(f"Ошибка при создании задачи для аккаунта {account_id}: {e}")
        
        # Показываем результат
        if tasks_created > 0:
            query.edit_message_text(
                f"✅ Публикация запущена!\n\n"
                f"📤 Создано задач: {tasks_created}\n"
                f"⏳ Задачи будут выполнены в ближайшее время"
            )
        else:
            query.edit_message_text("❌ Не удалось создать задачи публикации")
        
        # Очищаем данные
        cleanup_post_data(context)
        
        return ConversationHandler.END
        
    except Exception as e:
        logger.error(f"Ошибка при публикации: {e}")
        query.edit_message_text(f"❌ Ошибка при публикации: {e}")
        return ConversationHandler.END

def execute_post_schedule(update, context):
    """Выполняет планирование поста"""
    try:
        # Получаем данные
        selected_accounts = context.user_data.get('selected_accounts', [])
        media_files = context.user_data.get('media_files', [])
        caption = context.user_data.get('caption', "")
        hashtags = context.user_data.get('hashtags', "")
        scheduled_time = context.user_data.get('scheduled_time')
        
        if not selected_accounts or not media_files or not scheduled_time:
            update.message.reply_text("❌ Недостаточно данных для планирования")
            return ConversationHandler.END
        
        # Подготавливаем данные для публикации
        media_paths = [f['path'] for f in media_files]
        full_caption = f"{caption}\n\n{hashtags}".strip()
        
        # Создаем запланированные задачи
        from database.db_manager import create_publish_task
        from database.models import TaskType
        
        task_type = TaskType.PHOTO if media_files[0]['type'] == 'photo' else TaskType.VIDEO
        if len(media_files) > 1:
            task_type = TaskType.CAROUSEL
        
        tasks_created = 0
        for account_id in selected_accounts:
            try:
                # Создаем запланированную задачу
                task = create_publish_task(
                    account_id=account_id,
                    task_type=task_type,
                    media_path=media_paths[0] if len(media_paths) == 1 else media_paths,
                    caption=full_caption,
                    hashtags=hashtags,
                    scheduled_time=scheduled_time
                )
                
                if task:
                    tasks_created += 1
                    
            except Exception as e:
                logger.error(f"Ошибка при создании запланированной задачи для аккаунта {account_id}: {e}")
        
        # Показываем результат
        if tasks_created > 0:
            time_str = scheduled_time.strftime("%d.%m.%Y %H:%M")
            update.message.reply_text(
                f"✅ Публикация запланирована!\n\n"
                f"📅 Время: {time_str}\n"
                f"📤 Создано задач: {tasks_created}\n"
                f"⏳ Публикация будет выполнена автоматически"
            )
        else:
            update.message.reply_text("❌ Не удалось создать запланированные задачи")
        
        # Очищаем данные
        cleanup_post_data(context)
        
        return ConversationHandler.END
        
    except Exception as e:
        logger.error(f"Ошибка при планировании: {e}")
        update.message.reply_text(f"❌ Ошибка при планировании: {e}")
        return ConversationHandler.END

def cleanup_post_data(context):
    """Очищает данные поста"""
    try:
        # Удаляем медиа файлы
        media_files = context.user_data.get('media_files', [])
        for file_info in media_files:
            try:
                if os.path.exists(file_info['path']):
                    os.remove(file_info['path'])
            except:
                pass
        
        # Очищаем данные из контекста
        keys_to_remove = [
            'selected_accounts', 'selected_post_accounts', 'media_files',
            'caption', 'hashtags', 'scheduled_time', 'is_scheduled_post'
        ]
        
        for key in keys_to_remove:
            context.user_data.pop(key, None)
            
    except Exception as e:
        logger.error(f"Ошибка при очистке данных: {e}")

# Запланированные публикации
def start_schedule_post(update, context):
    """Начинает процесс планирования поста"""
    # Устанавливаем флаг планирования
    context.user_data['is_scheduled_post'] = True
    
    # Запускаем обычный процесс создания поста
    return start_post_publish(update, context) 
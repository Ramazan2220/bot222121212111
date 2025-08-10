"""
Обработчики для публикации Stories
"""

import os
import logging
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ConversationHandler

from database.db_manager import get_instagram_account, get_instagram_accounts
from telegram_bot.handlers.publish.states import StoryStates
from utils.content_uniquifier import ContentUniquifier

logger = logging.getLogger(__name__)

def is_admin(user_id):
    """Проверка прав администратора"""
    return True  # Временно для всех пользователей

def start_story_publish(update, context):
    """Начинает процесс публикации истории"""
    query = update.callback_query
    query.answer()
    
    # Устанавливаем тип публикации
    context.user_data['publish_type'] = 'story'
    context.user_data['publish_media_type'] = 'STORY'
    
    # Показываем меню выбора источника аккаунтов
    keyboard = []
    
    # Получаем папки
    from database.db_manager import get_account_groups
    folders = get_account_groups()
    if folders:
        keyboard.append([InlineKeyboardButton("📁 Выбрать из папки", callback_data="story_from_folders")])
    
    keyboard.append([InlineKeyboardButton("📋 Все аккаунты", callback_data="story_all_accounts")])
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="menu_publications")])
    
    query.edit_message_text(
        "📱 Публикация истории\n\nВыберите источник аккаунтов:",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    
    return StoryStates.CHOOSE_ACCOUNT

def handle_story_source_selection(update, context):
    """Обрабатывает выбор источника аккаунтов"""
    query = update.callback_query
    query.answer()
    
    if query.data == "story_from_folders":
        return show_story_folders(update, context)
    elif query.data == "story_all_accounts":
        return show_story_accounts_list(update, context, "all")
    elif query.data == "story_back_to_menu":
        from telegram_bot.handlers.system_handlers import show_publish_menu
        return show_publish_menu(update, context)

def show_story_folders(update, context):
    """Показывает список папок"""
    query = update.callback_query
    
    from database.db_manager import get_account_groups
    folders = get_account_groups()
    
    if not folders:
        query.edit_message_text(
            "❌ Папки не найдены",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Назад", callback_data="story_back_to_menu")]
            ])
        )
        return StoryStates.CHOOSE_ACCOUNT
    
    keyboard = []
    for folder in folders:
        keyboard.append([InlineKeyboardButton(
            f"📁 {folder.name}",
            callback_data=f"story_folder_{folder.id}"
        )])
    
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="story_back_to_menu")])
    
    query.edit_message_text(
        "📁 Выберите папку:",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    
    return StoryStates.CHOOSE_ACCOUNT

def show_story_accounts_list(update, context, folder_id_or_all, page=0):
    """Показывает список аккаунтов"""
    query = update.callback_query
    
    # Получаем аккаунты
    if folder_id_or_all == "all":
        accounts = get_instagram_accounts()
        folder_name = "Все аккаунты"
    else:
        from database.db_manager import get_accounts_by_group
        accounts = get_accounts_by_group(folder_id_or_all)
        folder_name = f"Папка {folder_id_or_all}"
    
    if not accounts:
        query.edit_message_text(
            "❌ Аккаунты не найдены",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Назад", callback_data="story_back_to_menu")]
            ])
        )
        return StoryStates.CHOOSE_ACCOUNT
    
    # Инициализируем выбранные аккаунты
    if 'selected_story_accounts' not in context.user_data:
        context.user_data['selected_story_accounts'] = []
    
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
        selected = account.id in context.user_data['selected_story_accounts']
        checkbox = "✅" if selected else "☐"
        status = "✅" if account.is_active else "❌"
        
        keyboard.append([InlineKeyboardButton(
            f"{checkbox} {status} @{account.username}",
            callback_data=f"story_toggle_{account.id}"
        )])
    
    # Кнопки управления
    control_buttons = []
    if len(context.user_data['selected_story_accounts']) > 0:
        control_buttons.append(InlineKeyboardButton("❌ Сбросить все", callback_data="story_deselect_all"))
    
    if len(context.user_data['selected_story_accounts']) < len(accounts):
        control_buttons.append(InlineKeyboardButton("✅ Выбрать все", callback_data="story_select_all"))
    
    if control_buttons:
        keyboard.append(control_buttons)
    
    # Пагинация
    if total_pages > 1:
        nav_buttons = []
        if page > 0:
            nav_buttons.append(InlineKeyboardButton("◀️", callback_data=f"story_page_{page-1}"))
        
        nav_buttons.append(InlineKeyboardButton(f"{page+1}/{total_pages}", callback_data="story_page_info"))
        
        if page < total_pages - 1:
            nav_buttons.append(InlineKeyboardButton("▶️", callback_data=f"story_page_{page+1}"))
        
        keyboard.append(nav_buttons)
    
    # Кнопки действий
    action_buttons = []
    if len(context.user_data['selected_story_accounts']) > 0:
        action_buttons.append(InlineKeyboardButton(
            f"📤 Продолжить ({len(context.user_data['selected_story_accounts'])})",
            callback_data="story_confirm_selection"
        ))
    
    action_buttons.append(InlineKeyboardButton("🔙 Назад", callback_data="story_back_to_menu"))
    keyboard.append(action_buttons)
    
    # Формируем текст
    selected_count = len(context.user_data['selected_story_accounts'])
    
    text = f"🎯 Выбор аккаунтов для публикации истории\n\n"
    text += f"📁 {folder_name}\n"
    text += f"Выбрано: {selected_count} из {len(accounts)}\n\n"
    
    if selected_count > 1:
        text += "⚠️ При выборе нескольких аккаунтов контент будет автоматически уникализирован\n\n"
    
    text += "Выберите аккаунты для публикации:"
    
    query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
    
    return StoryStates.CHOOSE_ACCOUNT

def handle_story_account_toggle(update, context):
    """Переключение выбора аккаунта"""
    query = update.callback_query
    query.answer()
    
    account_id = int(query.data.replace("story_toggle_", ""))
    
    if 'selected_story_accounts' not in context.user_data:
        context.user_data['selected_story_accounts'] = []
    
    if account_id in context.user_data['selected_story_accounts']:
        context.user_data['selected_story_accounts'].remove(account_id)
    else:
        context.user_data['selected_story_accounts'].append(account_id)
    
    # Обновляем клавиатуру
    return show_story_accounts_list(update, context, "all")

def handle_story_confirm_selection(update, context):
    """Подтверждение выбора аккаунтов"""
    query = update.callback_query
    query.answer()
    
    selected_accounts = context.user_data.get('selected_story_accounts', [])
    
    if not selected_accounts:
        query.edit_message_text(
            "❌ Не выбран ни один аккаунт",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Назад", callback_data="story_back_to_menu")]
            ])
        )
        return StoryStates.CHOOSE_ACCOUNT
    
    # Сохраняем выбранные аккаунты
    context.user_data['selected_accounts'] = selected_accounts
    
    # Получаем информацию об аккаунтах
    accounts = [get_instagram_account(acc_id) for acc_id in selected_accounts]
    usernames = [acc.username for acc in accounts if acc]
    
    # Показываем запрос на загрузку медиа
    text = f"📱 Публикация истории\n\n"
    text += f"👥 Выбрано аккаунтов: {len(selected_accounts)}\n"
    text += f"📤 Аккаунты: {', '.join([f'@{u}' for u in usernames[:3]])}"
    if len(usernames) > 3:
        text += f" и ещё {len(usernames) - 3}..."
    text += "\n\n"
    
    if len(selected_accounts) > 1:
        text += "🎨 Контент будет уникализирован для каждого аккаунта\n\n"
    
    text += "📎 Отправьте фото или видео для истории:"
    
    query.edit_message_text(text)
    
    return StoryStates.MEDIA_UPLOAD

def handle_story_media_upload(update, context):
    """Обработка загрузки медиа"""
    try:
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
            update.message.reply_text("❌ Отправьте фото или видео для истории")
            return StoryStates.MEDIA_UPLOAD
        
        # Скачиваем медиа
        file_obj = context.bot.get_file(media_file.file_id)
        
        # Создаем уникальное имя файла
        import uuid
        filename = f"story_{uuid.uuid4().hex[:8]}{file_extension}"
        file_path = os.path.join("media", filename)
        
        # Создаем папку если нужно
        os.makedirs("media", exist_ok=True)
        
        # Скачиваем файл
        file_obj.download(file_path)
        
        # Сохраняем путь к медиа
        context.user_data['media_path'] = file_path
        context.user_data['media_type'] = media_type
        
        # Переходим к вводу подписи
        return show_story_caption_input(update, context)
        
    except Exception as e:
        logger.error(f"Ошибка при загрузке медиа: {e}")
        update.message.reply_text(f"❌ Ошибка при загрузке файла: {e}")
        return StoryStates.MEDIA_UPLOAD

def show_story_caption_input(update, context):
    """Показывает ввод подписи"""
    selected_accounts = context.user_data.get('selected_accounts', [])
    media_type = context.user_data.get('media_type', 'PHOTO')
    
    text = f"📝 Введите подпись для истории\n\n"
    text += f"📤 Аккаунтов: {len(selected_accounts)}\n"
    text += f"📱 Тип: {media_type}\n\n"
    
    if len(selected_accounts) > 1:
        text += "🎨 Контент будет уникализирован для каждого аккаунта\n\n"
    
    text += "✍️ Отправьте текст подписи или нажмите 'Без подписи':"
    
    keyboard = [
        [InlineKeyboardButton("📝 Без подписи", callback_data="story_no_caption")],
        [InlineKeyboardButton("◀️ Назад к медиа", callback_data="story_back_to_media")]
    ]
    
    update.message.reply_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    
    return StoryStates.ENTER_CAPTION

def handle_story_caption_input(update, context):
    """Обработка ввода подписи"""
    try:
        caption = update.message.text
        context.user_data['caption'] = caption
        
        # Переходим к финальному подтверждению
        return show_story_final_confirmation(update, context)
        
    except Exception as e:
        logger.error(f"Ошибка при обработке подписи: {e}")
        update.message.reply_text(f"❌ Ошибка: {e}")
        return StoryStates.ENTER_CAPTION

def handle_story_caption_actions(update, context):
    """Обработка действий с подписью"""
    query = update.callback_query
    query.answer()
    
    if query.data == "story_no_caption":
        # Без подписи
        context.user_data['caption'] = ""
        return show_story_final_confirmation(update, context)
        
    elif query.data == "story_back_to_media":
        # Возвращаемся к медиа
        query.edit_message_text("📎 Отправьте фото или видео для истории:")
        return StoryStates.MEDIA_UPLOAD

def show_story_final_confirmation(update, context):
    """Показывает финальное подтверждение"""
    try:
        selected_accounts = context.user_data.get('selected_accounts', [])
        media_type = context.user_data.get('media_type', 'PHOTO')
        caption = context.user_data.get('caption', "")
        
        # Получаем информацию об аккаунтах
        accounts = []
        for account_id in selected_accounts:
            account = get_instagram_account(account_id)
            if account:
                accounts.append(account)
        
        # Формируем текст подтверждения
        text = f"**Подтверждение публикации истории**\n\n"
        text += f"👥 Аккаунты: {len(accounts)} шт.\n"
        text += f"📄 Тип: История\n"
        text += f"📱 Медиа: {media_type}\n"
        
        if caption:
            preview = caption[:100] + "..." if len(caption) > 100 else caption
            text += f"✏️ Подпись: {preview}\n"
        
        # Проверяем, это запланированная публикация или нет
        is_scheduled = context.user_data.get('is_scheduled_story', False)
        
        # Клавиатура
        if is_scheduled:
            keyboard = [
                [InlineKeyboardButton("🗓️ Выбрать время публикации", callback_data="story_schedule_time")],
                [InlineKeyboardButton("🔙 Изменить подпись", callback_data="story_back_to_caption")],
                [InlineKeyboardButton("❌ Отмена", callback_data="cancel_publish")]
            ]
        else:
            keyboard = [
                [InlineKeyboardButton("✅ Опубликовать", callback_data="story_confirm_publish"), 
                 InlineKeyboardButton("⏰ Запланировать", callback_data="story_schedule_publish")],
                [InlineKeyboardButton("🔙 Изменить подпись", callback_data="story_back_to_caption")],
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
        
        return StoryStates.CONFIRM_PUBLISH
        
    except Exception as e:
        logger.error(f"Ошибка при показе подтверждения: {e}")
        if hasattr(update, 'callback_query') and update.callback_query:
            update.callback_query.edit_message_text(f"❌ Ошибка: {e}")
        else:
            update.message.reply_text(f"❌ Ошибка: {e}")
        return StoryStates.CONFIRM_PUBLISH

def handle_story_final_confirmation(update, context):
    """Обработка финального подтверждения"""
    query = update.callback_query
    query.answer()
    
    if query.data == "story_confirm_publish":
        # Публикуем сейчас
        return execute_story_publish(update, context)
        
    elif query.data == "story_schedule_publish":
        # Устанавливаем флаг планирования
        context.user_data['is_scheduled_story'] = True
        return show_story_schedule_time(update, context)
        
    elif query.data == "story_back_to_caption":
        # Возвращаемся к подписи
        return show_story_caption_input(update, context)

def show_story_schedule_time(update, context):
    """Показывает выбор времени планирования"""
    query = update.callback_query
    
    text = "🗓️ Выберите время публикации истории:\n\n"
    text += "Введите дату и время в формате:\n"
    text += "ДД.ММ.ГГГГ ЧЧ:ММ\n\n"
    text += "Например: 25.12.2024 15:30"
    
    keyboard = [
        [InlineKeyboardButton("◀️ Назад к подтверждению", callback_data="story_back_to_confirmation")],
        [InlineKeyboardButton("❌ Отмена", callback_data="cancel_publish")]
    ]
    
    query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    
    return StoryStates.SCHEDULE_TIME

def handle_story_schedule_time(update, context):
    """Обработка ввода времени планирования"""
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
            return StoryStates.SCHEDULE_TIME
        
        # Проверяем, что время в будущем
        if scheduled_time <= datetime.now():
            update.message.reply_text(
                "❌ Время должно быть в будущем"
            )
            return StoryStates.SCHEDULE_TIME
        
        # Сохраняем время и планируем публикацию
        context.user_data['scheduled_time'] = scheduled_time
        
        # Создаем запланированную задачу
        return execute_story_schedule(update, context)
        
    except Exception as e:
        logger.error(f"Ошибка при обработке времени: {e}")
        update.message.reply_text(f"❌ Ошибка: {e}")
        return StoryStates.SCHEDULE_TIME

def execute_story_publish(update, context):
    """Выполняет публикацию истории"""
    query = update.callback_query
    
    try:
        # Получаем данные
        selected_accounts = context.user_data.get('selected_accounts', [])
        media_path = context.user_data.get('media_path')
        caption = context.user_data.get('caption', "")
        
        if not selected_accounts or not media_path:
            query.edit_message_text("❌ Недостаточно данных для публикации")
            return ConversationHandler.END
        
        # Создаем задачи публикации
        from database.db_manager import create_publish_task
        from database.models import TaskType
        
        tasks_created = 0
        for account_id in selected_accounts:
            try:
                # Создаем задачу
                task = create_publish_task(
                    account_id=account_id,
                    task_type=TaskType.STORY,
                    media_path=media_path,
                    caption=caption
                )
                
                if task:
                    tasks_created += 1
                    
            except Exception as e:
                logger.error(f"Ошибка при создании задачи для аккаунта {account_id}: {e}")
        
        # Показываем результат
        if tasks_created > 0:
            query.edit_message_text(
                f"✅ Истории запущены!\n\n"
                f"📤 Создано задач: {tasks_created}\n"
                f"⏳ Задачи будут выполнены в ближайшее время"
            )
        else:
            query.edit_message_text("❌ Не удалось создать задачи публикации")
        
        # Очищаем данные
        cleanup_story_data(context)
        
        return ConversationHandler.END
        
    except Exception as e:
        logger.error(f"Ошибка при публикации: {e}")
        query.edit_message_text(f"❌ Ошибка при публикации: {e}")
        return ConversationHandler.END

def execute_story_schedule(update, context):
    """Выполняет планирование истории"""
    try:
        # Получаем данные
        selected_accounts = context.user_data.get('selected_accounts', [])
        media_path = context.user_data.get('media_path')
        caption = context.user_data.get('caption', "")
        scheduled_time = context.user_data.get('scheduled_time')
        
        if not selected_accounts or not media_path or not scheduled_time:
            update.message.reply_text("❌ Недостаточно данных для планирования")
            return ConversationHandler.END
        
        # Создаем запланированные задачи
        from database.db_manager import create_publish_task
        from database.models import TaskType
        
        tasks_created = 0
        for account_id in selected_accounts:
            try:
                # Создаем запланированную задачу
                task = create_publish_task(
                    account_id=account_id,
                    task_type=TaskType.STORY,
                    media_path=media_path,
                    caption=caption,
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
                f"✅ Истории запланированы!\n\n"
                f"📅 Время: {time_str}\n"
                f"📤 Создано задач: {tasks_created}\n"
                f"⏳ Публикация будет выполнена автоматически"
            )
        else:
            update.message.reply_text("❌ Не удалось создать запланированные задачи")
        
        # Очищаем данные
        cleanup_story_data(context)
        
        return ConversationHandler.END
        
    except Exception as e:
        logger.error(f"Ошибка при планировании: {e}")
        update.message.reply_text(f"❌ Ошибка при планировании: {e}")
        return ConversationHandler.END

def cleanup_story_data(context):
    """Очищает данные истории"""
    try:
        # Удаляем медиа файл
        media_path = context.user_data.get('media_path')
        if media_path and os.path.exists(media_path):
            try:
                os.remove(media_path)
            except:
                pass
        
        # Очищаем данные из контекста
        keys_to_remove = [
            'selected_accounts', 'selected_story_accounts', 'media_path',
            'media_type', 'caption', 'scheduled_time', 'is_scheduled_story'
        ]
        
        for key in keys_to_remove:
            context.user_data.pop(key, None)
            
    except Exception as e:
        logger.error(f"Ошибка при очистке данных: {e}")

# Запланированные публикации
def start_schedule_story(update, context):
    """Начинает процесс планирования истории"""
    # Устанавливаем флаг планирования
    context.user_data['is_scheduled_story'] = True
    
    # Запускаем обычный процесс создания истории
    return start_story_publish(update, context) 
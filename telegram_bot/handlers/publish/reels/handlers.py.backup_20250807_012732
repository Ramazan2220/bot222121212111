"""
Обработчики для публикации Reels
"""

import os
import logging
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ConversationHandler

from database.db_manager import get_instagram_account, get_instagram_accounts
from telegram_bot.handlers.publish.states import ReelsStates
from utils.content_uniquifier import ContentUniquifier

logger = logging.getLogger(__name__)

def is_admin(user_id):
    """Проверка прав администратора"""
    return True  # Временно для всех пользователей

def start_reels_publish(update, context):
    """Начинает процесс публикации Reels"""
    query = update.callback_query
    query.answer()
    
    # Устанавливаем тип публикации
    context.user_data['publish_type'] = 'reels'
    context.user_data['publish_media_type'] = 'VIDEO'
    
    # Показываем меню выбора источника аккаунтов
    keyboard = []
    
    # Получаем папки
    from database.db_manager import get_account_groups
    folders = get_account_groups()
    if folders:
        keyboard.append([InlineKeyboardButton("📁 Выбрать из папки", callback_data="reels_from_folders")])
    
    keyboard.append([InlineKeyboardButton("📋 Все аккаунты", callback_data="reels_all_accounts")])
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="menu_publications")])
    
    query.edit_message_text(
        "🎥 Публикация Reels\n\nВыберите источник аккаунтов:",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    
    return ReelsStates.CHOOSE_ACCOUNT

def handle_reels_source_selection(update, context):
    """Обрабатывает выбор источника аккаунтов"""
    query = update.callback_query
    query.answer()
    
    if query.data == "reels_from_folders":
        return show_reels_folders(update, context)
    elif query.data == "reels_all_accounts":
        return show_reels_accounts_list(update, context, "all")
    elif query.data == "reels_back_to_menu":
        from telegram_bot.handlers.system_handlers import show_publish_menu
        return show_publish_menu(update, context)

def show_reels_folders(update, context):
    """Показывает список папок"""
    query = update.callback_query
    
    from database.db_manager import get_account_groups
    folders = get_account_groups()
    
    if not folders:
        query.edit_message_text(
            "❌ Папки не найдены",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Назад", callback_data="reels_back_to_menu")]
            ])
        )
        return ReelsStates.CHOOSE_ACCOUNT
    
    keyboard = []
    for folder in folders:
        keyboard.append([InlineKeyboardButton(
            f"📁 {folder.name}",
            callback_data=f"reels_folder_{folder.id}"
        )])
    
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="reels_back_to_menu")])
    
    query.edit_message_text(
        "📁 Выберите папку:",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    
    return ReelsStates.CHOOSE_ACCOUNT

def show_reels_accounts_list(update, context, folder_id_or_all, page=0):
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
                [InlineKeyboardButton("🔙 Назад", callback_data="reels_back_to_menu")]
            ])
        )
        return ReelsStates.CHOOSE_ACCOUNT
    
    # Инициализируем выбранные аккаунты
    if 'selected_reels_accounts' not in context.user_data:
        context.user_data['selected_reels_accounts'] = []
    
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
        selected = account.id in context.user_data['selected_reels_accounts']
        checkbox = "✅" if selected else "☐"
        status = "✅" if account.is_active else "❌"
        
        keyboard.append([InlineKeyboardButton(
            f"{checkbox} {status} @{account.username}",
            callback_data=f"reels_toggle_{account.id}"
        )])
    
    # Кнопки управления
    control_buttons = []
    if len(context.user_data['selected_reels_accounts']) > 0:
        control_buttons.append(InlineKeyboardButton("❌ Сбросить все", callback_data="reels_deselect_all"))
    
    if len(context.user_data['selected_reels_accounts']) < len(accounts):
        control_buttons.append(InlineKeyboardButton("✅ Выбрать все", callback_data="reels_select_all"))
    
    if control_buttons:
        keyboard.append(control_buttons)
    
    # Пагинация
    if total_pages > 1:
        nav_buttons = []
        if page > 0:
            nav_buttons.append(InlineKeyboardButton("◀️", callback_data=f"reels_page_{page-1}"))
        
        nav_buttons.append(InlineKeyboardButton(f"{page+1}/{total_pages}", callback_data="reels_page_info"))
        
        if page < total_pages - 1:
            nav_buttons.append(InlineKeyboardButton("▶️", callback_data=f"reels_page_{page+1}"))
        
        keyboard.append(nav_buttons)
    
    # Кнопки действий
    action_buttons = []
    if len(context.user_data['selected_reels_accounts']) > 0:
        action_buttons.append(InlineKeyboardButton(
            f"📤 Продолжить ({len(context.user_data['selected_reels_accounts'])})",
            callback_data="reels_confirm_selection"
        ))
    
    action_buttons.append(InlineKeyboardButton("🔙 Назад", callback_data="reels_back_to_menu"))
    keyboard.append(action_buttons)
    
    # Формируем текст
    selected_count = len(context.user_data['selected_reels_accounts'])
    
    text = f"🎯 Выбор аккаунтов для публикации Reels\n\n"
    text += f"📁 {folder_name}\n"
    text += f"Выбрано: {selected_count} из {len(accounts)}\n\n"
    
    if selected_count > 1:
        text += "⚠️ При выборе нескольких аккаунтов контент будет автоматически уникализирован\n\n"
    
    text += "Выберите аккаунты для публикации:"
    
    query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
    
    return ReelsStates.CHOOSE_ACCOUNT

def handle_reels_account_toggle(update, context):
    """Переключение выбора аккаунта"""
    query = update.callback_query
    query.answer()
    
    account_id = int(query.data.replace("reels_toggle_", ""))
    
    if 'selected_reels_accounts' not in context.user_data:
        context.user_data['selected_reels_accounts'] = []
    
    if account_id in context.user_data['selected_reels_accounts']:
        context.user_data['selected_reels_accounts'].remove(account_id)
    else:
        context.user_data['selected_reels_accounts'].append(account_id)
    
    # Обновляем клавиатуру
    return show_reels_accounts_list(update, context, "all")

def handle_reels_confirm_selection(update, context):
    """Подтверждение выбора аккаунтов"""
    query = update.callback_query
    query.answer()
    
    selected_accounts = context.user_data.get('selected_reels_accounts', [])
    
    if not selected_accounts:
        query.edit_message_text(
            "❌ Не выбран ни один аккаунт",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Назад", callback_data="reels_back_to_menu")]
            ])
        )
        return ReelsStates.CHOOSE_ACCOUNT
    
    # Сохраняем выбранные аккаунты
    context.user_data['selected_accounts'] = selected_accounts
    
    # Получаем информацию об аккаунтах
    accounts = [get_instagram_account(acc_id) for acc_id in selected_accounts]
    usernames = [acc.username for acc in accounts if acc]
    
    # Показываем запрос на загрузку видео
    text = f"🎥 Публикация Reels\n\n"
    text += f"👥 Выбрано аккаунтов: {len(selected_accounts)}\n"
    text += f"📤 Аккаунты: {', '.join([f'@{u}' for u in usernames[:3]])}"
    if len(usernames) > 3:
        text += f" и ещё {len(usernames) - 3}..."
    text += "\n\n"
    
    if len(selected_accounts) > 1:
        text += "🎨 Контент будет уникализирован для каждого аккаунта\n\n"
    
    text += "🎥 Отправьте видео для Reels (до 90 секунд):"
    
    query.edit_message_text(text)
    
    return ReelsStates.MEDIA_UPLOAD

def handle_reels_media_upload(update, context):
    """Обработка загрузки видео"""
    try:
        # Получаем информацию о видео
        media_file = None
        file_extension = '.mp4'
        
        if update.message.video:
            media_file = update.message.video
        elif update.message.document:
            media_file = update.message.document
            if not media_file.mime_type.startswith('video/'):
                update.message.reply_text("❌ Для Reels нужно отправить видео")
                return ReelsStates.MEDIA_UPLOAD
        
        if not media_file:
            update.message.reply_text("❌ Отправьте видео для Reels")
            return ReelsStates.MEDIA_UPLOAD
        
        # Скачиваем видео
        file_obj = context.bot.get_file(media_file.file_id)
        
        # Создаем уникальное имя файла
        import uuid
        filename = f"reels_{uuid.uuid4().hex[:8]}.mp4"
        file_path = os.path.join("media", filename)
        
        # Создаем папку если нужно
        os.makedirs("media", exist_ok=True)
        
        # Скачиваем файл
        file_obj.download(file_path)
        
        # Сохраняем путь к видео
        context.user_data['media_path'] = file_path
        context.user_data['media_type'] = 'VIDEO'
        
        # Переходим к вводу подписи
        return show_reels_caption_input(update, context)
        
    except Exception as e:
        logger.error(f"Ошибка при загрузке видео: {e}")
        update.message.reply_text(f"❌ Ошибка при загрузке файла: {e}")
        return ReelsStates.MEDIA_UPLOAD

def show_reels_caption_input(update, context):
    """Показывает ввод подписи"""
    selected_accounts = context.user_data.get('selected_accounts', [])
    
    text = f"📝 Введите подпись для Reels\n\n"
    text += f"📤 Аккаунтов: {len(selected_accounts)}\n"
    text += f"🎥 Тип: VIDEO\n\n"
    
    if len(selected_accounts) > 1:
        text += "🎨 Контент будет уникализирован для каждого аккаунта\n\n"
    
    text += "✍️ Отправьте текст подписи или нажмите 'Без подписи':"
    
    keyboard = [
        [InlineKeyboardButton("📝 Без подписи", callback_data="reels_no_caption")],
        [InlineKeyboardButton("◀️ Назад к видео", callback_data="reels_back_to_media")]
    ]
    
    update.message.reply_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    
    return ReelsStates.ENTER_CAPTION

def handle_reels_caption_input(update, context):
    """Обработка ввода подписи"""
    try:
        caption = update.message.text
        context.user_data['caption'] = caption
        
        # Переходим к финальному подтверждению
        return show_reels_final_confirmation(update, context)
        
    except Exception as e:
        logger.error(f"Ошибка при обработке подписи: {e}")
        update.message.reply_text(f"❌ Ошибка: {e}")
        return ReelsStates.ENTER_CAPTION

def handle_reels_caption_actions(update, context):
    """Обработка действий с подписью"""
    query = update.callback_query
    query.answer()
    
    if query.data == "reels_no_caption":
        # Без подписи
        context.user_data['caption'] = ""
        return show_reels_final_confirmation(update, context)
        
    elif query.data == "reels_back_to_media":
        # Возвращаемся к видео
        query.edit_message_text("🎥 Отправьте видео для Reels (до 90 секунд):")
        return ReelsStates.MEDIA_UPLOAD

def show_reels_final_confirmation(update, context):
    """Показывает финальное подтверждение"""
    try:
        selected_accounts = context.user_data.get('selected_accounts', [])
        caption = context.user_data.get('caption', "")
        
        # Получаем информацию об аккаунтах
        accounts = []
        for account_id in selected_accounts:
            account = get_instagram_account(account_id)
            if account:
                accounts.append(account)
        
        # Формируем текст подтверждения
        text = f"**Подтверждение публикации Reels**\n\n"
        text += f"👥 Аккаунты: {len(accounts)} шт.\n"
        text += f"📄 Тип: Reels\n"
        text += f"🎥 Медиа: VIDEO\n"
        
        if caption:
            preview = caption[:100] + "..." if len(caption) > 100 else caption
            text += f"✏️ Подпись: {preview}\n"
        
        # Проверяем, это запланированная публикация или нет
        is_scheduled = context.user_data.get('is_scheduled_reels', False)
        
        # Клавиатура
        if is_scheduled:
            keyboard = [
                [InlineKeyboardButton("🗓️ Выбрать время публикации", callback_data="reels_schedule_time")],
                [InlineKeyboardButton("🔙 Изменить подпись", callback_data="reels_back_to_caption")],
                [InlineKeyboardButton("❌ Отмена", callback_data="cancel_publish")]
            ]
        else:
            keyboard = [
                [InlineKeyboardButton("✅ Опубликовать", callback_data="reels_confirm_publish"), 
                 InlineKeyboardButton("⏰ Запланировать", callback_data="reels_schedule_publish")],
                [InlineKeyboardButton("🔙 Изменить подпись", callback_data="reels_back_to_caption")],
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
        
        return ReelsStates.CONFIRM_PUBLISH
        
    except Exception as e:
        logger.error(f"Ошибка при показе подтверждения: {e}")
        if hasattr(update, 'callback_query') and update.callback_query:
            update.callback_query.edit_message_text(f"❌ Ошибка: {e}")
        else:
            update.message.reply_text(f"❌ Ошибка: {e}")
        return ReelsStates.CONFIRM_PUBLISH

def handle_reels_final_confirmation(update, context):
    """Обработка финального подтверждения"""
    query = update.callback_query
    query.answer()
    
    if query.data == "reels_confirm_publish":
        # Публикуем сейчас
        return execute_reels_publish(update, context)
        
    elif query.data == "reels_schedule_publish":
        # Устанавливаем флаг планирования
        context.user_data['is_scheduled_reels'] = True
        return show_reels_schedule_time(update, context)
        
    elif query.data == "reels_back_to_caption":
        # Возвращаемся к подписи
        return show_reels_caption_input(update, context)

def show_reels_schedule_time(update, context):
    """Показывает выбор времени планирования"""
    query = update.callback_query
    
    text = "🗓️ Выберите время публикации Reels:\n\n"
    text += "Введите дату и время в формате:\n"
    text += "ДД.ММ.ГГГГ ЧЧ:ММ\n\n"
    text += "Например: 25.12.2024 15:30"
    
    keyboard = [
        [InlineKeyboardButton("◀️ Назад к подтверждению", callback_data="reels_back_to_confirmation")],
        [InlineKeyboardButton("❌ Отмена", callback_data="cancel_publish")]
    ]
    
    query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    
    return ReelsStates.SCHEDULE_TIME

def handle_reels_schedule_time(update, context):
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
            return ReelsStates.SCHEDULE_TIME
        
        # Проверяем, что время в будущем
        if scheduled_time <= datetime.now():
            update.message.reply_text(
                "❌ Время должно быть в будущем"
            )
            return ReelsStates.SCHEDULE_TIME
        
        # Сохраняем время и планируем публикацию
        context.user_data['scheduled_time'] = scheduled_time
        
        # Создаем запланированную задачу
        return execute_reels_schedule(update, context)
        
    except Exception as e:
        logger.error(f"Ошибка при обработке времени: {e}")
        update.message.reply_text(f"❌ Ошибка: {e}")
        return ReelsStates.SCHEDULE_TIME

def execute_reels_publish(update, context):
    """Выполняет публикацию Reels"""
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
                    task_type=TaskType.REELS,
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
                f"✅ Reels запущены!\n\n"
                f"📤 Создано задач: {tasks_created}\n"
                f"⏳ Задачи будут выполнены в ближайшее время"
            )
        else:
            query.edit_message_text("❌ Не удалось создать задачи публикации")
        
        # Очищаем данные
        cleanup_reels_data(context)
        
        return ConversationHandler.END
        
    except Exception as e:
        logger.error(f"Ошибка при публикации: {e}")
        query.edit_message_text(f"❌ Ошибка при публикации: {e}")
        return ConversationHandler.END

def execute_reels_schedule(update, context):
    """Выполняет планирование Reels"""
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
                    task_type=TaskType.REELS,
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
                f"✅ Reels запланированы!\n\n"
                f"📅 Время: {time_str}\n"
                f"📤 Создано задач: {tasks_created}\n"
                f"⏳ Публикация будет выполнена автоматически"
            )
        else:
            update.message.reply_text("❌ Не удалось создать запланированные задачи")
        
        # Очищаем данные
        cleanup_reels_data(context)
        
        return ConversationHandler.END
        
    except Exception as e:
        logger.error(f"Ошибка при планировании: {e}")
        update.message.reply_text(f"❌ Ошибка при планировании: {e}")
        return ConversationHandler.END

def cleanup_reels_data(context):
    """Очищает данные Reels"""
    try:
        # Удаляем видео файл
        media_path = context.user_data.get('media_path')
        if media_path and os.path.exists(media_path):
            try:
                os.remove(media_path)
            except:
                pass
        
        # Очищаем данные из контекста
        keys_to_remove = [
            'selected_accounts', 'selected_reels_accounts', 'media_path',
            'media_type', 'caption', 'scheduled_time', 'is_scheduled_reels'
        ]
        
        for key in keys_to_remove:
            context.user_data.pop(key, None)
            
    except Exception as e:
        logger.error(f"Ошибка при очистке данных: {e}")

# Запланированные публикации
def start_schedule_reels(update, context):
    """Начинает процесс планирования Reels"""
    # Устанавливаем флаг планирования
    context.user_data['is_scheduled_reels'] = True
    
    # Запускаем обычный процесс создания Reels
    return start_reels_publish(update, context) 
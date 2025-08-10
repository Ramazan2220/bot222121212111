import os
import tempfile
import json
import logging
import threading
from datetime import datetime

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, ParseMode
from telegram.ext import ConversationHandler

from database.db_manager import get_instagram_account, get_instagram_accounts, create_publish_task
from instagram_api.publisher import publish_video
from database.models import TaskType, TaskStatus
from instagram.reels_manager import publish_reels_in_parallel
from utils.task_queue import add_task_to_queue, get_task_status
from telegram_bot.utils.account_selection import create_account_selector

# Добавляем импорт для uuid
import uuid

logger = logging.getLogger(__name__)

# Состояния для публикации видео
CHOOSE_ACCOUNT, UPLOAD_MEDIA, ENTER_CAPTION, ENTER_HASHTAGS, CONFIRM_PUBLISH, CHOOSE_SCHEDULE, CHOOSE_HIDE_FROM_FEED = range(10, 17)

# Новые состояния для Stories
from telegram_bot.states import STORY_ADD_FEATURES, STORY_ADD_MENTIONS, STORY_ADD_LINK, STORY_ADD_LOCATION, STORY_ADD_HASHTAGS, STORY_ADD_TEXT, REELS_UPLOAD_COVER, REELS_TIME_COVER, REELS_BULK_USERTAGS

def is_admin(user_id):
    from telegram_bot.bot import is_admin
    return is_admin(user_id)

def publish_now_handler(update, context):
    """Обработчик команды публикации контента с новым селектором"""
    user_id = update.effective_user.id

    if not is_admin(user_id):
        if update.message:
            update.message.reply_text("У вас нет прав для выполнения этой команды.")
        else:
            update.callback_query.answer("У вас нет прав для выполнения этой команды.", show_alert=True)
        return ConversationHandler.END

    # Создаем селектор аккаунтов для публикации
    selector = create_account_selector(
        callback_prefix="publish_select",
        title="📤 Публикация контента",
        allow_multiple=True,  # Разрешаем выбор нескольких аккаунтов
        show_status=True,
        show_folders=True,
        back_callback="menu_publish"
    )
    
    # Определяем callback для обработки выбранных аккаунтов
    def on_accounts_selected(account_ids: list, update_inner, context_inner):
        if account_ids:
            context_inner.user_data['selected_accounts'] = account_ids
            context_inner.user_data['publish_account_ids'] = account_ids
            
            # Если выбран один аккаунт
            if len(account_ids) == 1:
                account = get_instagram_account(account_ids[0])
                context_inner.user_data['publish_account_id'] = account_ids[0]
                context_inner.user_data['publish_account_username'] = account.username
                context_inner.user_data['publish_to_all_accounts'] = False
            else:
                # Если выбрано несколько аккаунтов
                context_inner.user_data['publish_to_all_accounts'] = True
                accounts = [get_instagram_account(acc_id) for acc_id in account_ids]
                context_inner.user_data['publish_account_usernames'] = [acc.username for acc in accounts]
            
            # Проверяем, есть ли уже медиафайл
            if 'publish_media_path' in context_inner.user_data:
                # Если медиафайл уже загружен, переходим к вводу подписи
                query = update_inner.callback_query
                publish_type = context_inner.user_data.get('publish_type', 'post')
                
                # Определяем какой тип медиа запрашивать
                if publish_type == 'story':
                    media_prompt = "фото или видео для истории"
                elif publish_type == 'reels':
                    media_prompt = "видео для Reels"
                elif publish_type == 'igtv':
                    media_prompt = "видео для IGTV"
                else:  # post
                    media_prompt = "фото или видео для поста"
                
                if len(account_ids) == 1:
                    account = get_instagram_account(account_ids[0])
                    query.edit_message_text(
                        f"Выбран аккаунт: *{account.username}*\n\n"
                        f"Теперь отправьте {media_prompt}:",
                        parse_mode=ParseMode.MARKDOWN
                    )
                else:
                    accounts_str = ", ".join(context_inner.user_data['publish_account_usernames'])
                    query.edit_message_text(
                        f"Выбраны аккаунты ({len(account_ids)}):\n{accounts_str}\n\n"
                        f"Теперь отправьте {media_prompt}:"
                    )
                return UPLOAD_MEDIA
            else:
                # Если медиафайла нет, просим загрузить
                query = update_inner.callback_query
                publish_type = context_inner.user_data.get('publish_type', 'post')
                
                # Определяем какой тип медиа запрашивать
                if publish_type == 'story':
                    media_prompt = "фото или видео для истории"
                elif publish_type == 'reels':
                    media_prompt = "видео для Reels"
                elif publish_type == 'igtv':
                    media_prompt = "видео для IGTV"
                else:  # post
                    media_prompt = "фото или видео для поста"
                
                if len(account_ids) == 1:
                    account = get_instagram_account(account_ids[0])
                    query.edit_message_text(
                        f"Выбран аккаунт: *{account.username}*\n\n"
                        f"Теперь отправьте {media_prompt}:",
                        
                    )
                else:
                    accounts_str = ", ".join(context_inner.user_data['publish_account_usernames'])
                    query.edit_message_text(
                        f"Выбраны аккаунты ({len(account_ids)}):\n{accounts_str}\n\n"
                        f"Теперь отправьте {media_prompt}:"
                    )
                return ConversationHandler.END
    
    # Запускаем процесс выбора
    return selector.start_selection(update, context, on_accounts_selected)

def choose_account_callback(update, context):
    """Обработчик выбора аккаунта для публикации"""
    query = update.callback_query
    query.answer()

    # Получаем ID аккаунта из callback_data
    account_id = int(query.data.split('_')[-1])
    context.user_data['publish_account_id'] = account_id

    # Добавляем аккаунт в список выбранных (для совместимости)
    if 'selected_accounts' not in context.user_data:
        context.user_data['selected_accounts'] = []
    context.user_data['selected_accounts'].append(account_id)

    # Получаем аккаунт
    account = get_instagram_account(account_id)
    context.user_data['publish_account_username'] = account.username

    # Проверяем, есть ли уже медиафайл
    if 'publish_media_path' in context.user_data:
        # Если медиафайл уже загружен, переходим к вводу подписи
        query.edit_message_text(
            f"Выбран аккаунт: *{account.username}*\n\n"
            f"Теперь введите подпись к публикации (или отправьте /skip для публикации без подписи):",
            parse_mode=ParseMode.MARKDOWN
        )
        return ENTER_CAPTION
    else:
        # Если медиафайла нет, просим загрузить
        publish_type = context.user_data.get('publish_type', 'post')
        
        # Определяем какой тип медиа запрашивать
        if publish_type == 'story':
            media_prompt = "фото или видео для истории"
        elif publish_type == 'reels':
            media_prompt = "видео для Reels"
        elif publish_type == 'igtv':
            media_prompt = "видео для IGTV"
        else:  # post
            media_prompt = "фото или видео для поста"
        
        query.edit_message_text(
            f"Выбран аккаунт: *{account.username}*\n\n"
            f"Теперь отправьте {media_prompt}:",
            
        )
        return UPLOAD_MEDIA

def choose_all_accounts_callback(update, context):
    """Обработчик выбора всех аккаунтов для публикации"""
    query = update.callback_query
    query.answer()

    # Получаем все активные аккаунты
    accounts = get_instagram_accounts()
    active_accounts = [account for account in accounts if account.is_active]

    if not active_accounts:
        query.edit_message_text("Нет активных аккаунтов для публикации.")
        return ConversationHandler.END

    # Сохраняем список ID всех аккаунтов
    account_ids = [account.id for account in active_accounts]
    context.user_data['publish_account_ids'] = account_ids
    context.user_data['publish_to_all_accounts'] = True

    # Сохраняем имена пользователей для отображения
    account_usernames = [account.username for account in active_accounts]
    context.user_data['publish_account_usernames'] = account_usernames

    # Формируем список имен аккаунтов для отображения
    account_names = [account.username for account in active_accounts]
    accounts_str = ", ".join(account_names)

    # Проверяем, есть ли уже медиафайл
    if 'publish_media_path' in context.user_data:
        # Если медиафайл уже загружен, переходим к вводу подписи
        query.edit_message_text(
            f"Выбраны все аккаунты ({len(active_accounts)}):\n{accounts_str}\n\n"
            f"Теперь введите подпись к публикации (или отправьте /skip для публикации без подписи):"
        )
        return ENTER_CAPTION
    else:
        # Если медиафайла нет, просим загрузить
        publish_type = context.user_data.get('publish_type', 'post')
        
        # Определяем какой тип медиа запрашивать
        if publish_type == 'story':
            media_prompt = "фото или видео для истории"
        elif publish_type == 'reels':
            media_prompt = "видео для Reels"
        elif publish_type == 'igtv':
            media_prompt = "видео для IGTV"
        else:  # post
            media_prompt = "фото или видео для поста"
            
        query.edit_message_text(
            f"Выбраны все аккаунты ({len(active_accounts)}):\n{accounts_str}\n\n"
            f"Теперь отправьте {media_prompt}:"
        )
        return ConversationHandler.END  # Здесь мы завершаем разговор, чтобы пользователь мог загрузить видео

def choose_category_callback(update, context):
    """Обработчик выбора категории аккаунтов (заглушка)"""
    query = update.callback_query
    query.answer()

    query.edit_message_text(
        "🚧 Функция выбора категории находится в разработке.\n\n"
        "Пожалуйста, выберите конкретный аккаунт или все аккаунты."
    )

    # Возвращаемся к выбору аккаунта
    return publish_now_handler(update, context)

def media_upload_handler(update, context):
    """Обработчик загрузки медиа для нового интерфейса (используется в старых conversation handlers)"""
    # Для обратной совместимости с другими типами публикации (story, reels, igtv)
    publish_type = context.user_data.get('publish_type', 'post')
    
    if publish_type == 'post':
        # Для постов используем новый интерфейс
        return handle_media_upload(update, context)
    else:
        # Для других типов используем старый интерфейс
        return old_media_upload_handler(update, context)

def old_media_upload_handler(update, context):
    """Старый обработчик загрузки медиа (для story, reels, igtv)"""
    user_id = update.effective_user.id

    if not is_admin(user_id):
        update.message.reply_text("У вас нет прав для выполнения этой команды.")
        return

    # Проверяем, выбран ли аккаунт или аккаунты
    if 'publish_account_id' not in context.user_data and 'publish_account_ids' not in context.user_data:
        # Если аккаунт не выбран, запускаем процесс выбора аккаунта
        # Store the media file information for later use
        if update.message.photo:
            context.user_data['pending_media'] = update.message.photo[-1]
            context.user_data['pending_media_type'] = 'PHOTO'
        elif update.message.video:
            context.user_data['pending_media'] = update.message.video
            context.user_data['pending_media_type'] = 'VIDEO'
        elif update.message.document:
            context.user_data['pending_media'] = update.message.document
            context.user_data['pending_media_type'] = 'VIDEO' if update.message.document.mime_type.startswith('video/') else 'PHOTO'
        
        return publish_now_handler(update, context)

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
        update.message.reply_text("Пожалуйста, отправьте фото или видео.")
        return

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

    # Запрашиваем подпись
    publish_type = context.user_data.get('publish_type', 'post')
    
    if publish_type == 'story':
        update.message.reply_text(
            "📱 Медиа для истории успешно загружено!\n\n"
            "Теперь введите подпись к истории (или отправьте /skip для публикации без подписи):"
        )
    elif publish_type == 'reels':
        update.message.reply_text(
            "🎥 Видео для Reels успешно загружено!\n\n"
            "Теперь введите подпись к Reels (или отправьте /skip для публикации без подписи):"
        )
    elif publish_type == 'igtv':
        update.message.reply_text(
            "🎬 Видео для IGTV успешно загружено!\n\n"
            "Теперь введите подпись к IGTV (или отправьте /skip для публикации без подписи):"
        )
    elif publish_type == 'scheduled_post':
        update.message.reply_text(
            "📸 Медиа для запланированного поста успешно загружено!\n\n"
            "Теперь введите подпись к публикации (или отправьте /skip для публикации без подписи):"
        )
    else:
        update.message.reply_text(
            "📸 Медиа для поста успешно загружено!\n\n"
            "Теперь введите подпись к публикации (или отправьте /skip для публикации без подписи):"
        )

    return ENTER_CAPTION

def enter_caption(update, context):
    """Обработчик ввода подписи к публикации"""
    if update.message.text == '/skip':
        context.user_data['publish_caption'] = ""
    else:
        context.user_data['publish_caption'] = update.message.text

    publish_type = context.user_data.get('publish_type', 'post')
    media_type = context.user_data.get('publish_media_type')

    # Для рилсов (видео) спрашиваем о видимости в основной сетке
    if publish_type == 'reels' and media_type == 'VIDEO':
        keyboard = [
            [
                InlineKeyboardButton("✅ Оставить в основной сетке", callback_data='keep_in_feed'),
                InlineKeyboardButton("❌ Удалить из основной сетки", callback_data='hide_from_feed')
            ],
            [InlineKeyboardButton("🔙 Отмена", callback_data='cancel_publish')]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        update.message.reply_text(
            "Хотите ли вы удалить рилс из основной сетки профиля?\n"
            "(Рилс останется в разделе Reels, но не будет отображаться в основной сетке фотографий)",
            reply_markup=reply_markup
        )
        return CHOOSE_HIDE_FROM_FEED
    else:
        # Для других типов контента переходим сразу к подтверждению
        return show_publish_confirmation(update, context)

def choose_hide_from_feed(update, context):
    """Обработчик выбора видимости рилса в основной сетке"""
    query = update.callback_query
    query.answer()

    if query.data == 'hide_from_feed':
        context.user_data['hide_from_feed'] = True
        query.edit_message_text("Рилс будет удален из основной сетки профиля.")
    else:  # keep_in_feed
        context.user_data['hide_from_feed'] = False
        query.edit_message_text("Рилс останется в основной сетке профиля.")

    # Показываем подтверждение публикации
    return show_publish_confirmation(update, context, is_callback=True)

def show_publish_confirmation(update, context, is_callback=False):
    """Показывает подтверждение публикации"""
    # Получаем данные для публикации
    media_type = context.user_data.get('publish_media_type')
    publish_type = context.user_data.get('publish_type', 'post')
    caption = context.user_data.get('publish_caption')
    hide_from_feed = context.user_data.get('hide_from_feed', False)
    is_scheduled = context.user_data.get('is_scheduled_post', False)

    # Проверяем, публикуем на один аккаунт или на несколько
    if context.user_data.get('publish_to_all_accounts'):
        account_ids = context.user_data.get('publish_account_ids', [])
        accounts = [get_instagram_account(account_id) for account_id in account_ids]
        account_usernames = [account.username for account in accounts]
        account_info = f"👥 *Аккаунты:* {len(account_usernames)} аккаунтов"
    else:
        account_id = context.user_data.get('publish_account_id')
        account_username = context.user_data.get('publish_account_username')
        account_info = f"👤 *Аккаунт:* {account_username}"

    # Создаем клавиатуру для подтверждения
    if is_scheduled:
        # Для запланированных постов показываем только кнопку планирования
        keyboard = [
            [InlineKeyboardButton("🗓️ Выбрать время публикации", callback_data='schedule_publish')],
            [InlineKeyboardButton("❌ Отмена", callback_data='cancel_publish')]
        ]
        title_prefix = "📅 Запланированный "
    else:
        keyboard = [
            [
                InlineKeyboardButton("✅ Опубликовать сейчас", callback_data='confirm_publish_now'),
                InlineKeyboardButton("⏰ Запланировать", callback_data='schedule_publish')
            ],
            [InlineKeyboardButton("❌ Отмена", callback_data='cancel_publish')]
        ]
        title_prefix = ""
    
    reply_markup = InlineKeyboardMarkup(keyboard)

    # Определяем эмодзи для типа публикации
    type_emojis = {
        'post': '📸',
        'story': '📱',
        'reels': '🎥',
        'igtv': '🎬'
    }
    
    type_names = {
        'post': 'Пост',
        'story': 'История',
        'reels': 'Reels',
        'igtv': 'IGTV'
    }

    message_text = (
        f"*{title_prefix}Данные для публикации:*\n\n"
        f"{account_info}\n"
        f"{type_emojis.get(publish_type, '📸')} *Тип:* {type_names.get(publish_type, 'Пост')}\n"
        f"📱 *Медиа:* {media_type}\n"
        f"✏️ *Подпись:* {caption or '(без подписи)'}\n"
    )

    # Добавляем информацию о видимости в основной сетке только для рилсов
    if publish_type == 'reels':
        message_text += f"🔍 *Видимость:* {'Скрыт из основной сетки' if hide_from_feed else 'В основной сетке'}\n"

    message_text += "\nЧто вы хотите сделать?"

    if is_callback:
        query = update.callback_query
        query.edit_message_text(
            message_text,
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )
    else:
        update.message.reply_text(
            message_text,
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )

    return CONFIRM_PUBLISH

def confirm_publish_now(update, context):
    """Обработчик подтверждения немедленной публикации"""
    query = update.callback_query
    query.answer()

    # Получаем данные для публикации
    media_path = context.user_data.get('publish_media_path')
    media_type = context.user_data.get('publish_media_type')
    publish_type = context.user_data.get('publish_type', 'post')
    caption = context.user_data.get('publish_caption', '')
    hide_from_feed = context.user_data.get('hide_from_feed', False)
    user_id = query.from_user.id

    # Определяем TaskType на основе типа публикации
    if publish_type == 'story':
        task_type = TaskType.STORY
    elif publish_type == 'reels':
        task_type = TaskType.VIDEO  # Reels используют VIDEO тип
    elif publish_type == 'igtv':
        task_type = TaskType.VIDEO  # IGTV тоже видео
    else:  # post
        task_type = TaskType.PHOTO if media_type == 'PHOTO' else TaskType.VIDEO

    # Отправляем сообщение о начале публикации
    status_message = query.edit_message_text(
        f"⏳ Начинаем публикацию {publish_type}... Это может занять некоторое время."
    )

    # Проверяем, публикуем на один аккаунт или на несколько
    if 'publish_account_ids' in context.user_data:
        # Публикация на несколько аккаунтов
        account_ids = context.user_data.get('publish_account_ids')

        # Обновляем статус
        context.bot.edit_message_text(
            f"⏳ Подготовка к публикации {publish_type} на {len(account_ids)} аккаунтах...",
            chat_id=status_message.chat_id,
            message_id=status_message.message_id
        )

        # Создаем задачи для каждого аккаунта
        task_ids = []
        for account_id in account_ids:
            # Подготавливаем дополнительные данные
            additional_data = {
                'hide_from_feed': hide_from_feed,
                'publish_type': publish_type
            }

            # Создаем задачу на публикацию
            success, task_id = create_publish_task(
                account_id=account_id,
                task_type=task_type,
                media_path=media_path,
                caption=caption,
                additional_data=json.dumps(additional_data)
            )

            if success:
                # Добавляем задачу в очередь
                from utils.task_queue import add_task_to_queue
                if add_task_to_queue(task_id, update.effective_chat.id, context.bot):
                    account = get_instagram_account(account_id)
                    task_ids.append((task_id, account.username))

        if task_ids:
            # Регистрируем пакет задач для итогового отчета
            from utils.task_queue import register_task_batch
            just_task_ids = [task_id for task_id, _ in task_ids]
            register_task_batch(just_task_ids, update.effective_chat.id, context.bot)
            
            # Формируем сообщение о созданных задачах
            message = f"✅ Созданы задачи на публикацию {publish_type}:\n\n"
            for task_id, username in task_ids:
                message += f"• Задача #{task_id} для аккаунта {username}\n"

            message += "\nВы получите уведомления о результатах публикации и итоговый отчет."

            # Отправляем новое сообщение с результатами
            context.bot.send_message(
                chat_id=status_message.chat_id,
                text=message
            )
        else:
            context.bot.edit_message_text(
                f"❌ Не удалось создать задачи на публикацию {publish_type}.",
                chat_id=status_message.chat_id,
                message_id=status_message.message_id
            )
    else:
        # Публикация на один аккаунт
        account_id = context.user_data.get('publish_account_id')

        # Обновляем статус
        context.bot.edit_message_text(
            f"⏳ Подготовка к публикации {publish_type}...",
            chat_id=status_message.chat_id,
            message_id=status_message.message_id
        )

        # Подготавливаем дополнительные данные
        additional_data = {
            'hide_from_feed': hide_from_feed,
            'publish_type': publish_type
        }

        # Создаем задачу на публикацию
        success, task_id = create_publish_task(
            account_id=account_id,
            task_type=task_type,
            media_path=media_path,
            caption=caption,
            additional_data=json.dumps(additional_data)
        )

        if not success:
            context.bot.edit_message_text(
                f"❌ Ошибка при создании задачи: {task_id}",
                chat_id=status_message.chat_id,
                message_id=status_message.message_id
            )
            return ConversationHandler.END

        # Добавляем задачу в очередь
        from utils.task_queue import add_task_to_queue
        if add_task_to_queue(task_id, update.effective_chat.id, context.bot):
            account = get_instagram_account(account_id)
            context.bot.edit_message_text(
                f"✅ Задача #{task_id} на публикацию {publish_type} в аккаунт {account.username} добавлена в очередь.\n"
                f"Вы получите уведомление после завершения публикации.",
                chat_id=status_message.chat_id,
                message_id=status_message.message_id
            )
        else:
            context.bot.edit_message_text(
                f"❌ Ошибка при добавлении задачи в очередь.",
                chat_id=status_message.chat_id,
                message_id=status_message.message_id
            )

    # Очищаем данные пользователя
    cleanup_user_data(context)

    return ConversationHandler.END

def cleanup_user_data(context):
    """Очищает данные пользователя после публикации"""
    keys_to_remove = [
        'publish_account_id', 'publish_account_username', 'publish_account_ids',
        'publish_to_all_accounts', 'publish_account_usernames', 'publish_media_path',
        'publish_media_type', 'publish_caption', 'hide_from_feed', 'publish_type',
        'is_scheduled_post'
    ]
    
    for key in keys_to_remove:
        if key in context.user_data:
            del context.user_data[key]

def schedule_publish_callback(update, context):
    """Обработчик запланированной публикации"""
    query = update.callback_query
    query.answer()

    query.edit_message_text(
        "Введите дату и время публикации в формате ДД.ММ.ГГГГ ЧЧ:ММ\n"
        "Например: 25.12.2023 15:30"
    )

    return CHOOSE_SCHEDULE

def choose_schedule(update, context):
    """Обработчик выбора времени для запланированной публикации"""
    try:
        # Парсим дату и время
        scheduled_time = datetime.strptime(update.message.text, "%d.%m.%Y %H:%M")
        
        # Проверяем, что время в будущем
        if scheduled_time <= datetime.now():
            update.message.reply_text(
                "❌ Время публикации должно быть в будущем. Пожалуйста, выберите другое время."
            )
            return CHOOSE_SCHEDULE

        publish_type = context.user_data.get('publish_type', 'post')
        user_id = update.effective_user.id

        # Обрабатываем разные типы публикаций
        if publish_type == 'story':
            # Планирование Stories
            return schedule_story_publish(update, context, scheduled_time, user_id)
        elif publish_type == 'reels':
            # Планирование Reels
            return schedule_reels_publish(update, context, scheduled_time, user_id)
        else:
            # Планирование постов (старая логика)
            return schedule_post_publish(update, context, scheduled_time, user_id)

    except ValueError:
        update.message.reply_text(
            "❌ Неверный формат даты и времени. Пожалуйста, используйте формат ДД.ММ.ГГГГ ЧЧ:ММ\n"
            "Например: 25.12.2023 15:30"
        )
        return CHOOSE_SCHEDULE

def schedule_story_publish(update, context, scheduled_time, user_id):
    """Планирование публикации Stories"""
    # Получаем данные для публикации Stories
    media_path = context.user_data.get('publish_media_path')
    caption = context.user_data.get('publish_caption', '')
    mentions = context.user_data.get('story_mentions', [])
    link = context.user_data.get('story_link', '')
    story_text = context.user_data.get('story_text', '')
    story_text_color = context.user_data.get('story_text_color', '#ffffff')
    story_text_position = context.user_data.get('story_text_position', {})
    
    # Получаем аккаунты
    if context.user_data.get('publish_to_all_accounts'):
        account_ids = context.user_data.get('publish_account_ids', [])
    else:
        account_ids = [context.user_data.get('publish_account_id')]
    
    # Подготавливаем дополнительные данные для Stories
    additional_data = {
        'publish_type': 'story',
        'mentions': mentions,
        'link': link,
        'story_text': story_text,
        'story_text_color': story_text_color,
        'story_text_position': story_text_position
    }
    
    # Создаем задачи для каждого аккаунта
    task_ids = []
    for account_id in account_ids:
        if not account_id:
            continue
            
        account = get_instagram_account(account_id)
        if not account:
            continue
            
        success, task_id = create_publish_task(
            account_id=account_id,
            task_type=TaskType.STORY,
            media_path=media_path,
            caption=caption,
            scheduled_time=scheduled_time,
            additional_data=json.dumps(additional_data),
            user_id=user_id
        )
        
        if success:
            task_ids.append((task_id, account.username))
            logger.info(f"✅ Создана запланированная задача Stories #{task_id} для @{account.username} на {scheduled_time}")
    
    if task_ids:
        # Формируем сообщение о созданных задачах
        message = f"✅ Запланированы публикации Stories на {scheduled_time.strftime('%d.%m.%Y %H:%M')}:\n\n"
        for task_id, username in task_ids:
            message += f"• Задача #{task_id} для @{username}\n"
        
        message += f"\n📅 Время публикации: {scheduled_time.strftime('%d.%m.%Y %H:%M')}"
        message += "\n🔔 Вы получите уведомления о результатах публикации."
        
        keyboard = [[InlineKeyboardButton("🔙 К меню", callback_data='menu_scheduled')]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        update.message.reply_text(message, reply_markup=reply_markup)
    else:
        update.message.reply_text("❌ Не удалось создать задачи на запланированную публикацию Stories.")

    # Очищаем данные пользователя
    cleanup_user_data(context)
    return ConversationHandler.END

def schedule_reels_publish(update, context, scheduled_time, user_id):
    """Планирование публикации Reels"""
    # Получаем данные для публикации Reels
    video_path = context.user_data.get('reels_video_path')
    caption = context.user_data.get('reels_caption', '')
    options = context.user_data.get('reels_options', {})
    
    # Получаем аккаунты
    account_ids = context.user_data.get('publish_account_ids', [])
    
    # Подготавливаем дополнительные данные для Reels
    music_track = options.get('music_track')
    music_track_dict = None
    if music_track:
        music_track_dict = {
            'id': getattr(music_track, 'id', None),
            'title': getattr(music_track, 'title', 'Unknown'),
            'artist': getattr(music_track, 'artist', 'Unknown'),
            'duration': getattr(music_track, 'duration', 30)
        }
    
    task_data = {
        'publish_type': 'reels',
        'hashtags': options.get('hashtags', []),
        'usertags': options.get('usertags', []),
        'distributed_usertags': options.get('distributed_usertags', []),
        'location': options.get('location'),
        'music_track': music_track_dict,
        'cover_time': options.get('cover_time', 0),
        'thumbnail_path': options.get('thumbnail_path'),
        'uniquify_content': len(account_ids) > 1  # Уникализируем только для множественной публикации
    }
    
    # Создаем задачи для каждого аккаунта
    task_ids = []
    for account_id in account_ids:
        account = get_instagram_account(account_id)
        if not account:
            continue
            
        success, task_id = create_publish_task(
            account_id=account_id,
            task_type=TaskType.VIDEO,  # Reels используют VIDEO тип
            media_path=video_path,
            caption=caption,
            scheduled_time=scheduled_time,
            additional_data=json.dumps(task_data),
            user_id=user_id
        )
        
        if success:
            task_ids.append((task_id, account.username))
            logger.info(f"✅ Создана запланированная задача Reels #{task_id} для @{account.username} на {scheduled_time}")
    
    if task_ids:
        # Формируем сообщение о созданных задачах
        message = f"✅ Запланированы публикации Reels на {scheduled_time.strftime('%d.%m.%Y %H:%M')}:\n\n"
        for task_id, username in task_ids:
            message += f"• Задача #{task_id} для @{username}\n"
        
        message += f"\n📅 Время публикации: {scheduled_time.strftime('%d.%m.%Y %H:%M')}"
        message += "\n🔔 Вы получите уведомления о результатах публикации."
        
        keyboard = [[InlineKeyboardButton("🔙 К меню", callback_data='menu_scheduled')]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        update.message.reply_text(message, reply_markup=reply_markup)
    else:
        update.message.reply_text("❌ Не удалось создать задачи на запланированную публикацию Reels.")

    # Очищаем данные пользователя
    cleanup_reels_data(context)
    return ConversationHandler.END

def schedule_post_publish(update, context, scheduled_time, user_id):
    """Планирование публикации постов (старая логика)"""
    # Получаем данные для публикации из старого формата
    selected_accounts = context.user_data.get('selected_accounts', [])
    media_files = context.user_data.get('media_files', [])
    caption = context.user_data.get('caption', '')
    hashtags = context.user_data.get('hashtags', '')
    hide_from_feed = context.user_data.get('hide_from_feed', False)
    publish_type = context.user_data.get('publish_type', 'post')

    # Объединяем описание и хештеги
    full_caption = caption
    if hashtags:
        if full_caption:
            full_caption += "\n\n" + hashtags
        else:
            full_caption = hashtags

    # Определяем тип задачи и медиа путь
    if not media_files:
        update.message.reply_text("❌ Медиа файлы не найдены.")
        return ConversationHandler.END

    if len(media_files) == 1:
        # Одиночный файл
        media_path = media_files[0]['path']
        if media_files[0]['type'] == 'photo':
            task_type = TaskType.PHOTO
        else:
            task_type = TaskType.VIDEO
    else:
        # Карусель
        media_paths = [f['path'] for f in media_files]
        media_path = json.dumps(media_paths)
        task_type = TaskType.CAROUSEL

    # Подготавливаем дополнительные данные
    additional_data = {
        'hide_from_feed': hide_from_feed,
        'publish_type': publish_type,
        'uniquify_content': len(selected_accounts) > 1,
        'is_carousel': task_type == TaskType.CAROUSEL
    }

    # Создаем задачи для каждого аккаунта
    task_ids = []
    for account_id in selected_accounts:
        account = get_instagram_account(account_id)
        if not account:
            continue
            
        # Добавляем информацию об аккаунте в additional_data
        account_data = additional_data.copy()
        account_data.update({
            'account_username': account.username,
            'account_email': account.email,
            'account_email_password': account.email_password
        })
        
        success, task_id = create_publish_task(
            account_id=account_id,
            task_type=task_type,
            media_path=media_path,
            caption=full_caption,
            scheduled_time=scheduled_time,
            additional_data=json.dumps(account_data),
            user_id=user_id
        )
        
        if success:
            task_ids.append((task_id, account.username))
            logger.info(f"✅ Создана запланированная задача #{task_id} для @{account.username} на {scheduled_time}")
    
    if task_ids:
        # Формируем сообщение о созданных задачах
        message = f"✅ Запланированы публикации на {scheduled_time.strftime('%d.%m.%Y %H:%M')}:\n\n"
        for task_id, username in task_ids:
            message += f"• Задача #{task_id} для @{username}\n"
        
        message += f"\n📅 Время публикации: {scheduled_time.strftime('%d.%m.%Y %H:%M')}"
        message += "\n🔔 Вы получите уведомления о результатах публикации."
        
        keyboard = [[InlineKeyboardButton("🔙 К меню", callback_data='menu_scheduled')]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        update.message.reply_text(message, reply_markup=reply_markup)
    else:
        update.message.reply_text("❌ Не удалось создать задачи на запланированную публикацию.")

    # Очищаем данные пользователя
    cleanup_user_data(context)
    return ConversationHandler.END

def cancel_publish(update, context):
    """Обработчик отмены публикации"""
    query = update.callback_query
    query.answer()

    # Очищаем данные
    if 'publish_account_id' in context.user_data:
        del context.user_data['publish_account_id']
    if 'publish_account_username' in context.user_data:
        del context.user_data['publish_account_username']
    if 'publish_account_ids' in context.user_data:
        del context.user_data['publish_account_ids']
    if 'publish_to_all_accounts' in context.user_data:
        del context.user_data['publish_to_all_accounts']
    if 'selected_accounts' in context.user_data:
        del context.user_data['selected_accounts']
    if 'publish_media_path' in context.user_data:
        # Удаляем временный файл
        try:
            os.remove(context.user_data['publish_media_path'])
        except:
            pass
        del context.user_data['publish_media_path']
    if 'publish_media_type' in context.user_data:
        del context.user_data['publish_media_type']
    if 'publish_caption' in context.user_data:
        del context.user_data['publish_caption']
    if 'hide_from_feed' in context.user_data:
        del context.user_data['hide_from_feed']

    keyboard = [[InlineKeyboardButton("🔙 К меню задач", callback_data='menu_tasks')]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    query.edit_message_text(
        "❌ Публикация отменена.",
        reply_markup=reply_markup
    )

    return ConversationHandler.END

def check_task_status_handler(update, context):
    """Обработчик для проверки статуса задачи"""
    user_id = update.effective_user.id

    if not is_admin(user_id):
        update.message.reply_text("У вас нет прав для выполнения этой команды.")
        return

    # Проверяем, указан ли ID задачи
    if not context.args or not context.args[0].isdigit():
        update.message.reply_text(
            "❌ Пожалуйста, укажите ID задачи. Например: /task_status 123"
        )
        return

    task_id = int(context.args[0])

    # Получаем статус задачи
    status = get_task_status(task_id)

    if not status:
        update.message.reply_text(
            f"❌ Задача с ID {task_id} не найдена."
        )
        return

    # Формируем сообщение о статусе
    if status['success']:
        message = f"✅ Задача #{task_id} успешно выполнена!\n"
        if 'result' in status and status['result']:
            message += f"Результат: {status['result']}\n"
    else:
        message = f"❌ Задача #{task_id} завершилась с ошибкой:\n{status['result']}\n"

    if 'completed_at' in status and status['completed_at']:
        message += f"Время завершения: {status['completed_at'].strftime('%d.%m.%Y %H:%M:%S')}"

    update.message.reply_text(message)

def get_publish_handlers():
    """Возвращает обработчики для публикации контента"""
    from telegram.ext import CommandHandler, CallbackQueryHandler, MessageHandler, Filters

    # Создаем отдельные ConversationHandler для каждого типа публикации
    post_conversation = ConversationHandler(
        entry_points=[
            CallbackQueryHandler(start_post_publish, pattern='^publish_post$'),
            CallbackQueryHandler(start_schedule_post, pattern='^schedule_post$')
        ],
        states={
            CHOOSE_ACCOUNT: [
                CallbackQueryHandler(handle_post_source_selection, pattern='^post_source_'),
                CallbackQueryHandler(handle_post_folder_selection, pattern='^post_folder_'),
                CallbackQueryHandler(handle_post_account_toggle, pattern='^post_account_'),
                CallbackQueryHandler(handle_post_confirm_selection, pattern='^post_confirm$'),
                CallbackQueryHandler(handle_post_source_selection, pattern="^post_from_folders$"),
                CallbackQueryHandler(handle_post_source_selection, pattern="^post_all_accounts$"),
                CallbackQueryHandler(handle_post_source_selection, pattern="^post_back_to_menu$"),
                CallbackQueryHandler(handle_post_folder_selection, pattern="^post_folder_"),
                CallbackQueryHandler(handle_post_folder_selection, pattern="^post_back_to_source$"),
                CallbackQueryHandler(handle_post_account_toggle, pattern="^post_toggle_"),
                CallbackQueryHandler(handle_post_account_toggle, pattern="^post_select_all$"),
                CallbackQueryHandler(handle_post_account_toggle, pattern="^post_deselect_all$"),
                CallbackQueryHandler(handle_post_account_toggle, pattern="^post_page_"),
                CallbackQueryHandler(handle_post_confirm_selection, pattern="^post_confirm_selection$"),
            ],
            UPLOAD_MEDIA: [
                MessageHandler(Filters.photo | Filters.video | Filters.document, handle_media_upload),
                CallbackQueryHandler(handle_media_actions, pattern="^continue_to_caption$"),
                CallbackQueryHandler(handle_media_actions, pattern="^clear_media_files$"),
                CallbackQueryHandler(handle_media_actions, pattern="^cancel_publish$"),
                CallbackQueryHandler(handle_media_actions, pattern="^back_to_accounts$"),
            ],
            ENTER_CAPTION: [
                MessageHandler(Filters.text & ~Filters.command, handle_caption_input),
                CallbackQueryHandler(handle_caption_actions, pattern="^no_caption$"),
                CallbackQueryHandler(handle_caption_actions, pattern="^back_to_media$"),
            ],
            ENTER_HASHTAGS: [
                MessageHandler(Filters.text & ~Filters.command, handle_hashtags_input),
                CallbackQueryHandler(handle_hashtags_actions, pattern="^no_hashtags$"),
                CallbackQueryHandler(handle_hashtags_actions, pattern="^back_to_caption_from_hashtags$"),
            ],
            CHOOSE_HIDE_FROM_FEED: [
                CallbackQueryHandler(choose_hide_from_feed, pattern='^(hide_from_feed|keep_in_feed)$'),
                CallbackQueryHandler(cancel_publish, pattern='^cancel_publish$')
            ],
            CONFIRM_PUBLISH: [
                CallbackQueryHandler(handle_final_confirmation, pattern="^confirm_publish$"),
                CallbackQueryHandler(handle_final_confirmation, pattern="^back_to_caption$"),
                CallbackQueryHandler(handle_final_confirmation, pattern="^back_to_hashtags$"),
                CallbackQueryHandler(handle_final_confirmation, pattern="^back_to_media$"),
                CallbackQueryHandler(handle_final_confirmation, pattern="^cancel_publish$"),
                CallbackQueryHandler(confirm_publish_now, pattern="^confirm_publish_now$"),
                CallbackQueryHandler(schedule_publish_callback, pattern="^schedule_publish$"),
            ],
            CHOOSE_SCHEDULE: [
                MessageHandler(Filters.text & ~Filters.command, choose_schedule)
            ]
        },
        fallbacks=[
            CommandHandler("cancel", lambda update, context: ConversationHandler.END),
            CallbackQueryHandler(cancel_publish, pattern='^cancel_publish$')
        ],
        per_message=False
    )

    # Создаем отдельный ConversationHandler для Stories
    story_conversation = ConversationHandler(
        entry_points=[
            MessageHandler(Filters.photo | Filters.video | Filters.document, handle_story_media_upload,
                          pass_user_data=True)
        ],
        states={
            STORY_ADD_FEATURES: [
                CallbackQueryHandler(story_add_caption_handler, pattern='^story_add_caption$'),
                CallbackQueryHandler(story_add_mentions_handler, pattern='^story_add_mentions$'),
                CallbackQueryHandler(story_add_link_handler, pattern='^story_add_link$'),
                CallbackQueryHandler(story_add_location_handler, pattern='^story_add_location$'),
                CallbackQueryHandler(story_add_hashtags_handler, pattern='^story_add_hashtags$'),
                CallbackQueryHandler(story_add_text_handler, pattern='^story_add_text$'),
                CallbackQueryHandler(story_confirm_publish_handler, pattern='^story_confirm_publish$'),
                CallbackQueryHandler(story_schedule_publish_handler, pattern='^story_schedule_publish$'),
                CallbackQueryHandler(cancel_publish, pattern='^cancel_publish$')
            ],
            ENTER_CAPTION: [
                MessageHandler(Filters.text & ~Filters.command, handle_story_caption_input),
                CommandHandler("skip", handle_story_caption_input)
            ],
            STORY_ADD_MENTIONS: [
                MessageHandler(Filters.text & ~Filters.command, handle_story_mentions_input),
                CommandHandler("skip", handle_story_mentions_input)
            ],
            STORY_ADD_LINK: [
                MessageHandler(Filters.text & ~Filters.command, handle_story_link_input),
                CommandHandler("skip", handle_story_link_input)
            ],
            STORY_ADD_LOCATION: [
                MessageHandler(Filters.text & ~Filters.command, handle_story_location_input),
                CommandHandler("skip", handle_story_location_input)
            ],
            STORY_ADD_HASHTAGS: [
                MessageHandler(Filters.text & ~Filters.command, handle_story_hashtags_input),
                CommandHandler("skip", handle_story_hashtags_input)
            ],
            STORY_ADD_TEXT: [
                MessageHandler(Filters.text & ~Filters.command, handle_story_text_input),
                CommandHandler("skip", handle_story_text_input)
            ],
            CHOOSE_SCHEDULE: [
                MessageHandler(Filters.text & ~Filters.command, choose_schedule)
            ]
        },
        fallbacks=[
            CommandHandler("cancel", lambda update, context: ConversationHandler.END),
            CallbackQueryHandler(cancel_publish, pattern='^cancel_publish$')
        ]
    )
    
    # Простой обработчик для запуска Stories
    story_start_handler = CallbackQueryHandler(start_story_publish, pattern='^publish_story$')

    # Простой обработчик для запуска Reels
    reels_start_handler = CallbackQueryHandler(start_reels_publish, pattern='^publish_reels$')

    # Новый ConversationHandler для Reels с правильной последовательностью
    from telegram_bot.states import (
        REELS_UPLOAD_MEDIA, REELS_ADD_CAPTION, REELS_ADD_HASHTAGS, REELS_ADD_FEATURES,
        REELS_ADD_USERTAGS, REELS_ADD_LOCATION, REELS_ADD_MUSIC, REELS_CHOOSE_COVER,
        REELS_CONFIRM_PUBLISH
    )
    
    reels_conversation = ConversationHandler(
        entry_points=[
            MessageHandler(Filters.video | Filters.document, handle_reels_media_upload)
        ],
        states={
            REELS_UPLOAD_MEDIA: [
                MessageHandler(Filters.video | Filters.document, handle_reels_media_upload)
            ],
            REELS_ADD_CAPTION: [
                MessageHandler(Filters.text & ~Filters.command, handle_reels_caption_input),
                CallbackQueryHandler(handle_reels_caption_actions, pattern='^reels_(no_caption|back_to_video)$')
            ],
            REELS_ADD_HASHTAGS: [
                MessageHandler(Filters.text & ~Filters.command, handle_reels_hashtags_input),
                CallbackQueryHandler(handle_reels_hashtags_actions, pattern='^reels_(no_hashtags|back_to_caption)$')
            ],
            REELS_ADD_FEATURES: [
                CallbackQueryHandler(handle_reels_callbacks, pattern='^reels_')
            ],
            REELS_ADD_USERTAGS: [
                MessageHandler(Filters.text & ~Filters.command, handle_reels_usertags_input)
            ],
            REELS_ADD_LOCATION: [
                MessageHandler(Filters.text & ~Filters.command, handle_reels_location_input)
            ],
            REELS_ADD_MUSIC: [
                MessageHandler(Filters.text & ~Filters.command, handle_reels_music_input),
                CallbackQueryHandler(handle_reels_callbacks, pattern='^reels_')
            ],
            REELS_CHOOSE_COVER: [
                MessageHandler(Filters.text & ~Filters.command, handle_reels_cover_input),
                CallbackQueryHandler(handle_reels_callbacks, pattern='^reels_')
            ],
            REELS_UPLOAD_COVER: [
                MessageHandler(Filters.photo, handle_reels_cover_upload)
            ],
            REELS_TIME_COVER: [
                MessageHandler(Filters.text & ~Filters.command, handle_reels_cover_input)
            ],
            REELS_BULK_USERTAGS: [
                MessageHandler(Filters.text | Filters.document, handle_reels_bulk_usertags)
            ],
            REELS_CONFIRM_PUBLISH: [
                CallbackQueryHandler(handle_reels_callbacks, pattern='^reels_')
            ],
            CHOOSE_SCHEDULE: [
                MessageHandler(Filters.text & ~Filters.command, choose_schedule)
            ]
        },
        fallbacks=[
            CallbackQueryHandler(lambda u, c: ConversationHandler.END, pattern='^cancel_reels$'),
            CallbackQueryHandler(lambda u, c: ConversationHandler.END, pattern='^main_menu$'),
            CommandHandler("cancel", lambda update, context: ConversationHandler.END)
        ],
        per_message=False
    )

    igtv_conversation = ConversationHandler(
        entry_points=[
            CallbackQueryHandler(start_igtv_publish, pattern='^publish_igtv$')
        ],
        states={
            CHOOSE_ACCOUNT: [
                MessageHandler(Filters.photo | Filters.video | Filters.document, media_upload_handler)
            ],
            ENTER_CAPTION: [
                MessageHandler(Filters.text & ~Filters.command, enter_caption),
                CommandHandler("skip", enter_caption)
            ],
            CONFIRM_PUBLISH: [
                CallbackQueryHandler(confirm_publish_now, pattern='^confirm_publish_now$'),
                CallbackQueryHandler(schedule_publish_callback, pattern='^schedule_publish$'),
                CallbackQueryHandler(cancel_publish, pattern='^cancel_publish$')
            ],
            CHOOSE_SCHEDULE: [
                MessageHandler(Filters.text & ~Filters.command, choose_schedule)
            ]
        },
        fallbacks=[
            CommandHandler("cancel", lambda update, context: ConversationHandler.END),
            CallbackQueryHandler(cancel_publish, pattern='^cancel_publish$')
        ]
    )

    # Создаем селектор для публикации
    publish_selector = create_account_selector(
        callback_prefix="publish_select",
        title="📤 Публикация контента",
        allow_multiple=True,
        show_status=True,
        show_folders=True,
        back_callback="menu_publications"
    )

    # Обработчики медиа файлов
    video_handler = MessageHandler(Filters.video | Filters.document.video, media_upload_handler)
    photo_handler = MessageHandler(Filters.photo, media_upload_handler)
    task_status_handler = CommandHandler("task_status", check_task_status_handler)

    # Создаем селекторы для каждого типа публикации
    post_selector = create_account_selector(
        callback_prefix="post_select",
        title="📸 Публикация поста",
        allow_multiple=True,
        show_status=True,
        show_folders=True,
        back_callback="menu_publications"
    )
    
    story_selector = create_account_selector(
        callback_prefix="story_select",
        title="📱 Публикация истории",
        allow_multiple=True,
        show_status=True,
        show_folders=True,
        back_callback="menu_publications"
    )
    
    reels_selector = create_account_selector(
        callback_prefix="reels_select",
        title="🎥 Публикация Reels",
        allow_multiple=True,
        show_status=True,
        show_folders=True,
        back_callback="menu_publications"
    )
    
    igtv_selector = create_account_selector(
        callback_prefix="igtv_select",
        title="🎬 Публикация IGTV",
        allow_multiple=True,
        show_status=True,
        show_folders=True,
        back_callback="menu_publications"
    )
    


    # Обработчик для начала публикации Reels
    reels_start_handler = CallbackQueryHandler(start_reels_publish, pattern='^publish_reels$')
    
    return [
        post_conversation,
        reels_conversation,   # ConversationHandler для Reels (перемещен ВЫШЕ story_conversation)
        story_conversation,  # Заменяем story_media_conversation на story_conversation
        story_start_handler,
        igtv_conversation,
        # Убираем общие video_handler и photo_handler - они перехватывают медиа!
        # Каждый ConversationHandler сам обрабатывает медиа в нужных состояниях
        task_status_handler,
        publish_selector.get_conversation_handler(),  # Общий селектор
        post_selector.get_conversation_handler(),     # Селектор для постов
        story_selector.get_conversation_handler(),    # Селектор для историй
        reels_selector.get_conversation_handler(),    # Селектор для Reels
        igtv_selector.get_conversation_handler(),     # Селектор для IGTV
    ]

# Новые обработчики для разных типов публикаций

def start_post_publish(update, context):
    """Начинает процесс публикации поста с продвинутым селектором"""
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
    
    return CHOOSE_ACCOUNT

def start_story_publish(update, context):
    """Начинает процесс публикации истории с продвинутым селектором"""
    from telegram_bot.utils.account_selection import create_account_selector
    
    # Устанавливаем тип публикации
    context.user_data['publish_type'] = 'story'
    context.user_data['publish_media_type'] = 'STORY'
    
    # Очищаем старые данные Stories
    context.user_data.pop('story_mentions', None)
    context.user_data.pop('story_link', None)
    context.user_data.pop('story_text', None)
    context.user_data.pop('story_text_color', None)
    context.user_data.pop('story_text_position', None)
    
    # Создаем селектор аккаунтов для публикации историй
    selector = create_account_selector(
        callback_prefix="story_select",
        title="📱 Публикация истории",
        allow_multiple=True,  # Разрешаем множественный выбор
        show_status=True,     # Показываем статус аккаунтов
        show_folders=True,    # Показываем папки
        back_callback="menu_publications"
    )
    
    # Определяем callback для обработки выбранных аккаунтов
    def on_accounts_selected(account_ids: list, update_inner, context_inner):
        if account_ids:
            # Сохраняем выбранные аккаунты
            context_inner.user_data['publish_account_ids'] = account_ids
            context_inner.user_data['publish_type'] = 'story'
            context_inner.user_data['publish_to_all_accounts'] = len(account_ids) > 1
            
            # Получаем информацию об аккаунтах
            from database.db_manager import get_instagram_account
            accounts = [get_instagram_account(acc_id) for acc_id in account_ids]
            usernames = [acc.username for acc in accounts if acc]
            context_inner.user_data['publish_account_usernames'] = usernames
            
            query = update_inner.callback_query
            
            if len(account_ids) == 1:
                account = accounts[0]
                text = f"📱 Выбран аккаунт: *@{account.username}*\n\n"
                context_inner.user_data['publish_account_id'] = account_ids[0]
                context_inner.user_data['publish_account_username'] = account.username
            else:
                text = f"📱 Выбрано аккаунтов: {len(account_ids)}\n"
                text += f"Аккаунты: {', '.join([f'@{u}' for u in usernames[:3]])}"
                if len(usernames) > 3:
                    text += f" и ещё {len(usernames) - 3}..."
                text += "\n\n"
            
            text += "Теперь отправьте фото или видео для истории:"
            
            query.edit_message_text(text)
            
            # Переходим к состоянию ожидания медиа
            return UPLOAD_MEDIA
    
    # Запускаем процесс выбора
    # AccountSelector управляет своими состояниями, поэтому возвращаем END
    selector.start_selection(update, context, on_accounts_selected)
    return ConversationHandler.END

def start_reels_publish(update, context):
    """Начинает процесс публикации Reels с продвинутым селектором"""
    from telegram_bot.utils.account_selection import AccountSelector
    
    # Устанавливаем тип публикации
    context.user_data['publish_type'] = 'reels'
    context.user_data['publish_media_type'] = 'VIDEO'
    
    # Создаем селектор аккаунтов для публикации Reels
    selector = AccountSelector(
        callback_prefix="reels_select",
        title="🎥 Публикация Reels",
        allow_multiple=True,  # Разрешаем множественный выбор
        show_status=True,     # Показываем статус аккаунтов
        show_folders=True,    # Показываем папки
        back_callback="menu_publications"
    )
    
    # Определяем callback для обработки выбранных аккаунтов
    def on_accounts_selected(account_ids: list, update_inner, context_inner):
        if account_ids:
            # Сохраняем выбранные аккаунты
            context_inner.user_data['publish_account_ids'] = account_ids
            context_inner.user_data['publish_type'] = 'reels'
            context_inner.user_data['publish_to_all_accounts'] = len(account_ids) > 1
            
            # Получаем информацию об аккаунтах
            from database.db_manager import get_instagram_account
            accounts = [get_instagram_account(acc_id) for acc_id in account_ids]
            usernames = [acc.username for acc in accounts if acc]
            context_inner.user_data['publish_account_usernames'] = usernames
            
            # Инициализируем данные для Reels
            context_inner.user_data['reels_options'] = {
                'hashtags': [],
                'usertags': [],
                'location': None,
                'hide_from_feed': False,
                'cover_time': 0
            }
            
            if len(account_ids) == 1:
                text = f"🎥 Выбран аккаунт: @{usernames[0]}\n\n"
                context_inner.user_data['publish_account_id'] = account_ids[0]
                context_inner.user_data['publish_account_username'] = usernames[0]
            else:
                text = f"🎥 Выбрано аккаунтов: {len(account_ids)}\n"
                text += f"Аккаунты: {', '.join([f'@{u}' for u in usernames[:3]])}"
                if len(usernames) > 3:
                    text += f" и ещё {len(usernames) - 3}..."
                text += "\n\n"
            
            text += "🎥 Отправьте видео для Reels (до 90 секунд):"
            
            # Убираем промежуточную кнопку и сразу переходим к загрузке видео
            update_inner.callback_query.edit_message_text(text)
            
            # Не возвращаем состояние, а завершаем текущий handler
            # Пользователь должен отправить видео, которое будет обработано handle_reels_media_upload
            return ConversationHandler.END
    
    # Запускаем процесс выбора аккаунтов
    return selector.start_selection(update, context, on_accounts_selected)

def start_reels_upload(update, context):
    """Начинает загрузку видео для Reels после выбора аккаунтов"""
    query = update.callback_query
    query.answer()
    
    # Проверяем, что аккаунты выбраны
    account_ids = context.user_data.get('publish_account_ids', [])
    if not account_ids:
        from telegram import InlineKeyboardButton, InlineKeyboardMarkup
        query.edit_message_text(
            "❌ Аккаунты не выбраны. Вернитесь к выбору аккаунтов.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Назад", callback_data="menu_publications")]
            ])
        )
        return ConversationHandler.END
    
    # Показываем сообщение о загрузке видео
    usernames = context.user_data.get('publish_account_usernames', [])
    
    if len(account_ids) == 1:
        text = f"🎥 Публикация Reels в аккаунт: *@{usernames[0]}*\n\n"
    else:
        text = f"🎥 Публикация Reels в {len(account_ids)} аккаунтов\n\n"
        text += "⚠️ Контент будет уникализирован для каждого аккаунта\n\n"
    
    text += "📹 Отправьте видео для Reels (максимум 90 секунд)"
    
    query.edit_message_text(text)
    
    from telegram_bot.states import REELS_UPLOAD_MEDIA
    return REELS_UPLOAD_MEDIA

def start_igtv_publish(update, context):
    """Начинает процесс публикации IGTV с продвинутым селектором"""
    from telegram_bot.utils.account_selection import AccountSelector
    
    # Устанавливаем тип публикации
    context.user_data['publish_type'] = 'igtv'
    context.user_data['publish_media_type'] = 'VIDEO'
    
    # Создаем селектор аккаунтов для публикации IGTV
    selector = AccountSelector(
        callback_prefix="igtv_select",
        title="🎬 Публикация IGTV",
        allow_multiple=True,  # Разрешаем множественный выбор
        show_status=True,     # Показываем статус аккаунтов
        show_folders=True,    # Показываем папки
        back_callback="menu_publications"
    )
    
    # Определяем callback для обработки выбранных аккаунтов
    def on_accounts_selected(account_ids: list, update_inner, context_inner):
        if account_ids:
            # Сохраняем выбранные аккаунты
            context_inner.user_data['publish_account_ids'] = account_ids
            context_inner.user_data['publish_type'] = 'igtv'
            context_inner.user_data['publish_to_all_accounts'] = len(account_ids) > 1
            
            # Получаем информацию об аккаунтах
            from database.db_manager import get_instagram_account
            accounts = [get_instagram_account(acc_id) for acc_id in account_ids]
            usernames = [acc.username for acc in accounts if acc]
            context_inner.user_data['publish_account_usernames'] = usernames
            
            if len(account_ids) == 1:
                text = f"🎬 Выбран аккаунт: *{usernames[0]}*\n\n"
                context_inner.user_data['publish_account_id'] = account_ids[0]
                context_inner.user_data['publish_account_username'] = usernames[0]
            else:
                text = f"🎬 Выбрано аккаунтов: {len(account_ids)}\n"
                text += f"Аккаунты: {', '.join(usernames[:5])}"
                if len(usernames) > 5:
                    text += f" и ещё {len(usernames) - 5}..."
                text += "\n\n"
            
            text += "Теперь отправьте видео для IGTV:"
            
            update_inner.callback_query.edit_message_text(
                text,
                
            )
            
            # Переходим к состоянию ожидания медиа
            return CHOOSE_ACCOUNT
    
    # Запускаем процесс выбора
    return selector.start_selection(update, context, on_accounts_selected)

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
            from database.db_manager import get_instagram_account
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
            from database.db_manager import get_instagram_account
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

# Обработчики выбора аккаунтов для публикации постов

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

def handle_post_folder_selection(update, context):
    """Обрабатывает выбор папки для постов"""
    query = update.callback_query
    query.answer()
    
    if query.data.startswith("post_folder_"):
        folder_id = query.data.replace("post_folder_", "")
        return show_post_accounts_list(update, context, folder_id)
    elif query.data == "post_back_to_source":
        return handle_post_source_selection(update, context)

def show_post_accounts_list(update, context, folder_name_or_accounts, page=0):
    """Показывает список аккаунтов для выбора с пагинацией"""
    try:
        # Если передан folder_name (строка), получаем аккаунты
        if isinstance(folder_name_or_accounts, str):
            folder_name = folder_name_or_accounts
            if folder_name == "all":
                accounts = get_instagram_accounts()
            else:
                accounts = get_accounts_by_folder(folder_name)
        else:
            # Если передан список аккаунтов
            accounts = folder_name_or_accounts
            folder_name = "unknown"
        
        if not accounts:
            query = update.callback_query
            query.edit_message_text(
                f"❌ В папке нет аккаунтов",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("◀️ Назад", callback_data="post_back_to_source")
                ]])
            )
            return CHOOSE_ACCOUNT
        
        # Инициализируем выбранные аккаунты если нужно
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
        
        action_buttons.append(InlineKeyboardButton("◀️ Назад", callback_data="post_back_to_source"))
        keyboard.append(action_buttons)
        
        # Формируем текст
        folder_text = f"📁 {folder_name}" if folder_name != "all" else "📁 Все аккаунты"
        selected_count = len(context.user_data['selected_post_accounts'])
        
        text = f"🎯 Выбор аккаунтов для публикации поста\n\n"
        text += f"{folder_text}\n"
        text += f"Выбрано: {selected_count} из {len(accounts)}\n\n"
        
        if selected_count > 1:
            text += "⚠️ При выборе нескольких аккаунтов контент будет автоматически уникализирован\n\n"
        
        text += "📋 Выберите аккаунты:"
        
        # Сохраняем данные для пагинации
        context.user_data['post_folder_name'] = folder_name
        context.user_data['post_current_page'] = page
        context.user_data['post_total_accounts'] = len(accounts)
        context.user_data['post_all_accounts'] = [acc.id for acc in accounts]
        
        query = update.callback_query
        query.edit_message_text(
            text,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        
        return CHOOSE_ACCOUNT
        
    except Exception as e:
        logger.error(f"Ошибка при показе списка аккаунтов: {e}")
        query = update.callback_query
        query.edit_message_text(
            f"❌ Ошибка при загрузке аккаунтов: {e}",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("◀️ Назад", callback_data="post_back_to_source")
            ]])
        )
        return CHOOSE_ACCOUNT

def handle_post_account_toggle(update, context):
    """Обрабатывает выбор/отмену выбора аккаунта"""
    query = update.callback_query
    query.answer()
    
    try:
        if query.data.startswith("post_toggle_"):
            # Извлекаем ID аккаунта
            account_id = int(query.data.replace("post_toggle_", ""))
            
            # Инициализируем список если нужно
            if 'selected_post_accounts' not in context.user_data:
                context.user_data['selected_post_accounts'] = []
            
            selected = context.user_data['selected_post_accounts']
            
            # Переключаем выбор
            if account_id in selected:
                selected.remove(account_id)
            else:
                selected.append(account_id)
            
            context.user_data['selected_post_accounts'] = selected
            
            # Обновляем интерфейс
            folder_name = context.user_data.get('post_folder_name', 'all')
            current_page = context.user_data.get('post_current_page', 0)
            
            return show_post_accounts_list(update, context, folder_name, current_page)
            
        elif query.data == "post_select_all":
            # Выбираем все аккаунты
            all_account_ids = context.user_data.get('post_all_accounts', [])
            context.user_data['selected_post_accounts'] = all_account_ids.copy()
            
            folder_name = context.user_data.get('post_folder_name', 'all')
            current_page = context.user_data.get('post_current_page', 0)
            
            return show_post_accounts_list(update, context, folder_name, current_page)
            
        elif query.data == "post_deselect_all":
            # Сбрасываем все выборы
            context.user_data['selected_post_accounts'] = []
            
            folder_name = context.user_data.get('post_folder_name', 'all')
            current_page = context.user_data.get('post_current_page', 0)
            
            return show_post_accounts_list(update, context, folder_name, current_page)
            
        elif query.data.startswith("post_page_"):
            # Навигация по страницам
            page_part = query.data.replace("post_page_", "")
            
            # Проверяем, что это не информационная кнопка
            if page_part == "info":
                return CHOOSE_ACCOUNT
            
            try:
                page_num = int(page_part)
                folder_name = context.user_data.get('post_folder_name', 'all')
                return show_post_accounts_list(update, context, folder_name, page_num)
            except ValueError:
                # Если не удалось преобразовать в число, игнорируем
                return CHOOSE_ACCOUNT
            
    except Exception as e:
        logger.error(f"Ошибка при переключении аккаунта: {e}")
        query.edit_message_text(
            f"❌ Ошибка: {e}",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("◀️ Назад", callback_data="post_back_to_source")
            ]])
        )
        return CHOOSE_ACCOUNT

def handle_post_confirm_selection(update, context):
    """Подтверждает выбор аккаунтов для постов"""
    query = update.callback_query
    query.answer()
    
    selected = context.user_data.get('selected_post_accounts', [])
    
    if not selected:
        query.answer("Выберите хотя бы один аккаунт", show_alert=True)
        return CHOOSE_ACCOUNT
    
    # Сохраняем выбранные аккаунты
    context.user_data['selected_accounts'] = selected
    context.user_data['publish_account_ids'] = selected
    context.user_data['publish_to_all_accounts'] = len(selected) > 1
    
    # Получаем информацию об аккаунтах
    from database.db_manager import get_instagram_account
    accounts = [get_instagram_account(acc_id) for acc_id in selected]
    usernames = [acc.username for acc in accounts if acc]
    context.user_data['publish_account_usernames'] = usernames
    
    if len(selected) == 1:
        text = f"📸 Выбран аккаунт: @{usernames[0]}\n\n"
        context.user_data['publish_account_id'] = selected[0]
        context.user_data['publish_account_username'] = usernames[0]
    else:
        text = f"📸 Выбрано аккаунтов: {len(selected)}\n"
        text += f"Аккаунты: {', '.join(usernames[:5])}"
        if len(usernames) > 5:
            text += f" и ещё {len(usernames) - 5}..."
        text += "\n\n"
    
    text += "📎 Отправьте медиа файлы для публикации:\n"
    text += "• Фото (JPG, PNG)\n"
    text += "• Видео (MP4, MOV)\n"
    text += "• Несколько фото для карусели (до 10 штук)\n\n"
    
    if len(selected) > 1:
        text += "🎨 Контент будет автоматически уникализирован для каждого аккаунта"
    
    query.edit_message_text(text)
    
    # Очищаем временные данные
    if 'selected_post_accounts' in context.user_data:
        del context.user_data['selected_post_accounts']
    if 'available_post_accounts' in context.user_data:
        del context.user_data['available_post_accounts']
    
    return UPLOAD_MEDIA

# Вспомогательные функции для нового интерфейса публикации

def show_post_folders(update, context):
    """Показать список папок для выбора"""
    try:
        from database.db_manager import get_account_groups
        folders = get_account_groups()
        
        if not folders:
            query = update.callback_query
            query.edit_message_text(
                "📂 Папки не найдены",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("◀️ Назад", callback_data="post_back_to_source")
                ]])
            )
            return CHOOSE_ACCOUNT
        
        keyboard = []
        for folder in folders:
            from database.db_manager import get_accounts_in_group
            accounts_count = len(get_accounts_in_group(folder.id))
            button_text = f"{folder.icon} {folder.name} ({accounts_count} акк.)"
            keyboard.append([InlineKeyboardButton(button_text, callback_data=f"post_folder_{folder.id}")])
        
        keyboard.append([InlineKeyboardButton("◀️ Назад", callback_data="post_back_to_source")])
        
        query = update.callback_query
        query.edit_message_text(
            "📁 Выберите папку:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        
        return CHOOSE_ACCOUNT
        
    except Exception as e:
        logger.error(f"Ошибка при показе папок: {e}")
        query = update.callback_query
        query.edit_message_text(f"❌ Ошибка: {e}")
        return CHOOSE_ACCOUNT

def get_accounts_by_folder(folder_name):
    """Получить аккаунты по имени папки"""
    try:
        if folder_name == "all":
            return get_instagram_accounts()
        
        # Если это ID папки
        if folder_name.isdigit():
            from database.db_manager import get_accounts_in_group
            return get_accounts_in_group(int(folder_name))
        
        # Если это имя папки
        from database.db_manager import get_account_groups, get_accounts_in_group
        folders = get_account_groups()
        for folder in folders:
            if folder.name == folder_name:
                return get_accounts_in_group(folder.id)
        
        return []
        
    except Exception as e:
        logger.error(f"Ошибка при получении аккаунтов папки {folder_name}: {e}")
        return []

# Новые функции для обработки медиа и описаний

def handle_media_upload(update, context):
    """Обработка загрузки медиа файлов для нового интерфейса"""
    try:
        # Инициализируем список медиа файлов если нужно
        if 'media_files' not in context.user_data:
            context.user_data['media_files'] = []
        
        media_files = context.user_data['media_files']
        
        # Проверяем тип сообщения
        if update.message.photo:
            # Фото
            photo = update.message.photo[-1]  # Берем самое большое разрешение
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
            
            logger.info(f"Загружено фото: {file_path}")
            
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
            
            logger.info(f"Загружено видео: {file_path}")
            
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
                
                logger.info(f"Загружен медиа файл: {file_path}")
            else:
                update.message.reply_text("❌ Неподдерживаемый формат файла. Поддерживаются: JPG, PNG, MP4, MOV")
                return UPLOAD_MEDIA
        else:
            update.message.reply_text("❌ Отправьте фото, видео или медиа файл")
            return UPLOAD_MEDIA
        
        # Обновляем список медиа файлов
        context.user_data['media_files'] = media_files
        
        # Формируем ответ
        total_files = len(media_files)
        current_file = media_files[-1]
        
        text = f"✅ Загружено файлов: {total_files}\n\n"
        
        # Показываем информацию о файлах
        for i, file_info in enumerate(media_files, 1):
            file_type = "📷 Фото" if file_info['type'] == 'photo' else "🎥 Видео"
            # Просто используем имя файла без экранирования
            text += f"{i}. {file_type} - {file_info['original_filename']}\n"
        
        text += "\n"
        
        # Определяем тип публикации
        if total_files == 1:
            if current_file['type'] == 'photo':
                text += "📤 Тип публикации: Обычное фото\n"
            else:
                text += "📤 Тип публикации: Видео\n"
        else:
            # Проверяем, что все файлы - фото
            all_photos = all(f['type'] == 'photo' for f in media_files)
            if all_photos and total_files <= 10:
                text += f"🎠 Тип публикации: Карусель ({total_files} фото)\n"
            elif not all_photos:
                text += "❌ Ошибка: Для карусели можно использовать только фото\n"
                text += "Удалите видео файлы или начните заново\n"
            else:
                text += f"❌ Ошибка: Максимум 10 фото в карусели (загружено {total_files})\n"
        
        text += "\n📎 Отправьте еще файлы или нажмите 'Продолжить'"
        
        # Клавиатура
        keyboard = [
            [InlineKeyboardButton("📝 Продолжить", callback_data="continue_to_caption")],
            [InlineKeyboardButton("🗑 Очистить все", callback_data="clear_media_files")],
            [InlineKeyboardButton("❌ Отмена", callback_data="cancel_publish")]
        ]
        
        update.message.reply_text(
            text,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        
        return UPLOAD_MEDIA
        
    except Exception as e:
        logger.error(f"Ошибка при загрузке медиа: {e}")
        update.message.reply_text(f"❌ Ошибка при загрузке файла: {e}")
        return UPLOAD_MEDIA

def handle_media_actions(update, context):
    """Обработка действий с медиа файлами"""
    query = update.callback_query
    query.answer()
    
    try:
        if query.data == "continue_to_caption":
            # Проверяем медиа файлы
            media_files = context.user_data.get('media_files', [])
            
            if not media_files:
                query.edit_message_text(
                    "❌ Не загружено ни одного медиа файла",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("◀️ Назад", callback_data="back_to_accounts")
                    ]])
                )
                return UPLOAD_MEDIA
            
            # Проверяем корректность для карусели
            if len(media_files) > 1:
                all_photos = all(f['type'] == 'photo' for f in media_files)
                if not all_photos:
                    query.edit_message_text(
                        "❌ Для карусели можно использовать только фото",
                        reply_markup=InlineKeyboardMarkup([[
                            InlineKeyboardButton("🗑 Очистить все", callback_data="clear_media_files"),
                            InlineKeyboardButton("❌ Отмена", callback_data="cancel_publish")
                        ]])
                    )
                    return UPLOAD_MEDIA
                
                if len(media_files) > 10:
                    query.edit_message_text(
                        f"❌ Максимум 10 фото в карусели (загружено {len(media_files)})",
                        reply_markup=InlineKeyboardMarkup([[
                            InlineKeyboardButton("🗑 Очистить все", callback_data="clear_media_files"),
                            InlineKeyboardButton("❌ Отмена", callback_data="cancel_publish")
                        ]])
                    )
                    return UPLOAD_MEDIA
            
            # Переходим к вводу описания
            selected_accounts = context.user_data.get('selected_accounts', [])
            total_files = len(media_files)
            
            text = f"📝 Введите описание для публикации\n\n"
            text += f"📤 Аккаунтов: {len(selected_accounts)}\n"
            text += f"📁 Файлов: {total_files}\n"
            
            if total_files > 1:
                text += f"🎠 Тип: Карусель\n"
            elif media_files[0]['type'] == 'photo':
                text += f"📷 Тип: Фото\n"
            else:
                text += f"🎥 Тип: Видео\n"
            
            if len(selected_accounts) > 1:
                text += "\n🎨 Контент будет уникализирован для каждого аккаунта\n"
            
            text += "\n✍️ Отправьте текст описания или нажмите 'Без описания':"
            
            keyboard = [
                [InlineKeyboardButton("📝 Без описания", callback_data="no_caption")],
                [InlineKeyboardButton("◀️ Назад к медиа", callback_data="back_to_media")]
            ]
            
            query.edit_message_text(
                text,
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            
            return ENTER_CAPTION
            
        elif query.data == "clear_media_files":
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
            
            query.edit_message_text(
                "🗑 Все файлы очищены\n\n📎 Отправьте медиа файлы для публикации:"
            )
            
            return UPLOAD_MEDIA
            
        elif query.data == "cancel_publish":
            # Отменяем публикацию
            media_files = context.user_data.get('media_files', [])
            
            # Удаляем файлы с диска
            for file_info in media_files:
                try:
                    if os.path.exists(file_info['path']):
                        os.remove(file_info['path'])
                except:
                    pass
            
            # Очищаем контекст
            context.user_data.clear()
            
            query.edit_message_text("❌ Публикация отменена")
            return ConversationHandler.END
            
        elif query.data == "back_to_accounts":
            # Возвращаемся к выбору аккаунтов
            return handle_post_source_selection(update, context)
            
    except Exception as e:
        logger.error(f"Ошибка при обработке действий с медиа: {e}")
        query.edit_message_text(f"❌ Ошибка: {e}")
        return UPLOAD_MEDIA

def handle_caption_input(update, context):
    """Обработка ввода описания"""
    try:
        caption = update.message.text
        
        # Сохраняем описание
        context.user_data['caption'] = caption
        
        # Переходим к вводу хештегов
        return show_hashtags_input(update, context)
        
    except Exception as e:
        logger.error(f"Ошибка при обработке описания: {e}")
        update.message.reply_text(f"❌ Ошибка: {e}")
        return ENTER_CAPTION

def handle_caption_actions(update, context):
    """Обработка действий с описанием"""
    query = update.callback_query
    query.answer()
    
    try:
        if query.data == "no_caption":
            # Без описания
            context.user_data['caption'] = ""
            return show_hashtags_input(update, context)
            
        elif query.data == "back_to_media":
            # Возвращаемся к медиа
            media_files = context.user_data.get('media_files', [])
            
            if not media_files:
                query.edit_message_text("📎 Отправьте медиа файлы для публикации:")
                return UPLOAD_MEDIA
            
            # Показываем текущие медиа файлы
            text = f"📁 Загружено файлов: {len(media_files)}\n\n"
            
            for i, file_info in enumerate(media_files, 1):
                file_type = "📷 Фото" if file_info['type'] == 'photo' else "🎥 Видео"
                text += f"{i}. {file_type} - {file_info['original_filename']}\n"
            
            text += "\n📎 Отправьте еще файлы или нажмите 'Продолжить'"
            
            keyboard = [
                [InlineKeyboardButton("📝 Продолжить", callback_data="continue_to_caption")],
                [InlineKeyboardButton("🗑 Очистить все", callback_data="clear_media_files")],
                [InlineKeyboardButton("❌ Отмена", callback_data="cancel_publish")]
            ]
            
            query.edit_message_text(
                text,
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            
            return UPLOAD_MEDIA
            
    except Exception as e:
        logger.error(f"Ошибка при обработке действий с описанием: {e}")
        query.edit_message_text(f"❌ Ошибка: {e}")
        return ENTER_CAPTION

def show_hashtags_input(update, context):
    """Показать экран ввода хештегов"""
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
            [InlineKeyboardButton("🏷 Без хештегов", callback_data="no_hashtags")],
            [InlineKeyboardButton("◀️ Назад к описанию", callback_data="back_to_caption_from_hashtags")]
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
        
        return ENTER_HASHTAGS
        
    except Exception as e:
        logger.error(f"Ошибка при показе ввода хештегов: {e}")
        if hasattr(update, 'callback_query') and update.callback_query:
            update.callback_query.edit_message_text(f"❌ Ошибка: {e}")
        else:
            update.message.reply_text(f"❌ Ошибка: {e}")
        return ENTER_HASHTAGS

def handle_hashtags_input(update, context):
    """Обработка ввода хештегов"""
    try:
        hashtags = update.message.text
        
        # Сохраняем хештеги
        context.user_data['hashtags'] = hashtags
        
        # Переходим к финальному подтверждению
        return show_final_confirmation(update, context)
        
    except Exception as e:
        logger.error(f"Ошибка при обработке хештегов: {e}")
        update.message.reply_text(f"❌ Ошибка: {e}")
        return ENTER_HASHTAGS

def handle_hashtags_actions(update, context):
    """Обработка действий с хештегами"""
    query = update.callback_query
    query.answer()
    
    try:
        if query.data == "no_hashtags":
            # Без хештегов
            context.user_data['hashtags'] = ""
            return show_final_confirmation(update, context)
            
        elif query.data == "back_to_caption_from_hashtags":
            # Возвращаемся к описанию
            text = f"📝 Введите описание для публикации\n\n"
            text += f"✍️ Отправьте текст описания или нажмите 'Без описания':"
            
            keyboard = [
                [InlineKeyboardButton("📝 Без описания", callback_data="no_caption")],
                [InlineKeyboardButton("◀️ Назад к медиа", callback_data="back_to_media")]
            ]
            
            query.edit_message_text(
                text,
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            
            return ENTER_CAPTION
            
    except Exception as e:
        logger.error(f"Ошибка при обработке действий с хештегами: {e}")
        query.edit_message_text(f"❌ Ошибка: {e}")
        return ENTER_HASHTAGS

def show_final_confirmation(update, context):
    """Показать финальное подтверждение публикации"""
    try:
        selected_accounts = context.user_data.get('selected_accounts', [])
        media_files = context.user_data.get('media_files', [])
        caption = context.user_data.get('caption', "")
        hashtags = context.user_data.get('hashtags', "")
        
        # Получаем информацию о аккаунтах
        accounts = []
        for account_id in selected_accounts:
            account = get_instagram_account(account_id)
            if account:
                accounts.append(account)
        
        # Формируем текст подтверждения
        text = f"🎯 Подтверждение публикации\n\n"
        
        # Информация о медиа
        if len(media_files) == 1:
            if media_files[0]['type'] == 'photo':
                text += f"📷 Тип: Фото\n"
            else:
                text += f"🎥 Тип: Видео\n"
        else:
            text += f"🎠 Тип: Карусель ({len(media_files)} фото)\n"
        
        # Информация об аккаунтах
        text += f"📤 Аккаунтов: {len(accounts)}\n"
        
        # Показываем первые 5 аккаунтов
        text += f"👥 Аккаунты:\n"
        for i, account in enumerate(accounts[:5]):
            status = "✅" if account.is_active else "❌"
            text += f"   {i+1}. {status} @{account.username}\n"
        
        if len(accounts) > 5:
            text += f"   ... и еще {len(accounts) - 5} аккаунтов\n"
        
        # Информация об описании
        if caption:
            preview = caption[:100] + "..." if len(caption) > 100 else caption
            text += f"\n📝 Описание: {preview}\n"
        else:
            text += f"\n📝 Описание: Без описания\n"
        
        # Информация о хештегах
        if hashtags:
            hashtags_preview = hashtags[:100] + "..." if len(hashtags) > 100 else hashtags
            text += f"🏷 Хештеги: {hashtags_preview}\n"
        else:
            text += f"🏷 Хештеги: Без хештегов\n"
        
        # Уникализация
        if len(accounts) > 1:
            text += f"\n🎨 Уникализация: Включена (контент будет изменен для каждого аккаунта)\n"
        
        text += f"\n✅ Все готово к публикации!"
        
        # Проверяем, это запланированная публикация или нет
        is_scheduled = context.user_data.get('is_scheduled_post', False)
        
        # Клавиатура
        if is_scheduled:
            # Для запланированных постов показываем только кнопку планирования
            keyboard = [
                [InlineKeyboardButton("🗓️ Выбрать время публикации", callback_data="schedule_publish")],
                [InlineKeyboardButton("📝 Изменить описание", callback_data="back_to_caption")],
                [InlineKeyboardButton("🏷 Изменить хештеги", callback_data="back_to_hashtags")],
                [InlineKeyboardButton("📁 Изменить медиа", callback_data="back_to_media")],
                [InlineKeyboardButton("❌ Отмена", callback_data="cancel_publish")]
            ]
        else:
            keyboard = [
                [InlineKeyboardButton("🚀 Опубликовать", callback_data="confirm_publish")],
                [InlineKeyboardButton("📝 Изменить описание", callback_data="back_to_caption")],
                [InlineKeyboardButton("🏷 Изменить хештеги", callback_data="back_to_hashtags")],
                [InlineKeyboardButton("📁 Изменить медиа", callback_data="back_to_media")],
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
        
        return CONFIRM_PUBLISH
        
    except Exception as e:
        logger.error(f"Ошибка при показе подтверждения: {e}")
        if hasattr(update, 'callback_query') and update.callback_query:
            update.callback_query.edit_message_text(f"❌ Ошибка: {e}")
        else:
            update.message.reply_text(f"❌ Ошибка: {e}")
        return CONFIRM_PUBLISH

def handle_final_confirmation(update, context):
    """Обработка финального подтверждения"""
    query = update.callback_query
    query.answer()
    
    try:
        if query.data == "confirm_publish":
            # Подтверждаем публикацию
            return execute_publish_task(update, context)
        
        elif query.data == "schedule_publish":
            # Запланированная публикация - конвертируем данные из старого формата в новый
            selected_accounts = context.user_data.get('selected_accounts', [])
            media_files = context.user_data.get('media_files', [])
            caption = context.user_data.get('caption', "")
            hashtags = context.user_data.get('hashtags', "")
            
            # Объединяем описание и хештеги
            full_caption = caption
            if hashtags:
                if full_caption:
                    full_caption += "\n\n" + hashtags
                else:
                    full_caption = hashtags
            
            # Конвертируем в новый формат
            context.user_data['publish_account_ids'] = selected_accounts
            context.user_data['publish_caption'] = full_caption
            context.user_data['publish_type'] = 'post'
            
            # Определяем медиа данные
            if media_files:
                if len(media_files) == 1:
                    context.user_data['publish_media_path'] = media_files[0]['path']
                    context.user_data['publish_media_type'] = 'PHOTO' if media_files[0]['type'] == 'photo' else 'VIDEO'
                else:
                    # Карусель - используем первый файл как основной
                    context.user_data['publish_media_path'] = json.dumps([f['path'] for f in media_files])
                    context.user_data['publish_media_type'] = 'CAROUSEL'
            
            # Вызываем обработчик планирования
            return schedule_publish_callback(update, context)
            
        elif query.data == "back_to_caption":
            # Возвращаемся к описанию
            text = f"📝 Введите описание для публикации\n\n"
            text += f"✍️ Отправьте текст описания или нажмите 'Без описания':"
            
            keyboard = [
                [InlineKeyboardButton("📝 Без описания", callback_data="no_caption")],
                [InlineKeyboardButton("◀️ Назад к медиа", callback_data="back_to_media")]
            ]
            
            query.edit_message_text(
                text,
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            
            return ENTER_CAPTION
            
        elif query.data == "back_to_hashtags":
            # Возвращаемся к хештегам
            return show_hashtags_input(update, context)
            
        elif query.data == "back_to_media":
            # Возвращаемся к медиа
            return handle_caption_actions(update, context)
            
        elif query.data == "cancel_publish":
            # Отменяем публикацию
            return handle_media_actions(update, context)
            
    except Exception as e:
        logger.error(f"Ошибка при обработке подтверждения: {e}")
        query.edit_message_text(f"❌ Ошибка: {e}")
        return CONFIRM_PUBLISH

def execute_publish_task(update, context):
    """Выполнение задачи публикации"""
    query = update.callback_query
    
    try:
        selected_accounts = context.user_data.get('selected_accounts', [])
        media_files = context.user_data.get('media_files', [])
        caption = context.user_data.get('caption', "")
        hashtags = context.user_data.get('hashtags', "")
        
        # Объединяем описание и хештеги
        full_caption = caption
        if hashtags:
            if full_caption:
                full_caption += "\n\n" + hashtags
            else:
                full_caption = hashtags
        
        if not selected_accounts or not media_files:
            query.edit_message_text("❌ Недостаточно данных для публикации")
            return ConversationHandler.END
        
        # Показываем прогресс
        query.edit_message_text("🚀 Начинаю публикацию...\n\n⏳ Подготовка задач...")
        
        # Определяем тип задачи
        if len(media_files) == 1:
            if media_files[0]['type'] == 'photo':
                task_type = TaskType.PHOTO
            else:
                task_type = TaskType.VIDEO
        else:
            task_type = TaskType.CAROUSEL
        
        # Подготавливаем медиа пути
        if task_type == TaskType.CAROUSEL:
            media_paths = [f['path'] for f in media_files]
            media_path_json = json.dumps(media_paths)
        else:
            media_path_json = media_files[0]['path']
        
        # Создаем задачи для каждого аккаунта
        task_ids = []
        need_uniquification = len(selected_accounts) > 1
        
        for account_id in selected_accounts:
            account = get_instagram_account(account_id)
            if not account:
                continue
            
            # Создаем задачу публикации
            success, task_id = create_publish_task(
                account_id=account_id,
                task_type=task_type,
                media_path=media_path_json,
                caption=full_caption,
                additional_data=json.dumps({
                    'uniquify_content': need_uniquification,
                    'is_carousel': task_type == TaskType.CAROUSEL,
                    'account_username': account.username,
                    'account_email': account.email,
                    'account_email_password': account.email_password
                }),
                user_id=query.from_user.id
            )
            
            if success and task_id:
                task_ids.append(task_id)
                logger.info(f"Создана задача #{task_id} для аккаунта @{account.username}")
                
                # Добавляем задачу в очередь для выполнения
                from utils.task_queue import add_task_to_queue
                add_task_to_queue(task_id, query.message.chat_id, context.bot)
                logger.info(f"Задача #{task_id} добавлена в очередь обработки")
        
        # Регистрируем пакет задач для итогового отчета
        if task_ids:
            from utils.task_queue import register_task_batch
            register_task_batch(task_ids, query.message.chat_id, context.bot)
            logger.info(f"📦 Зарегистрирован пакет из {len(task_ids)} задач для итогового отчета")
        
        # Обновляем сообщение с результатом
        if task_ids:
            text = f"✅ Публикация запущена!\n\n"
            text += f"🎯 Создано задач: {len(task_ids)}\n"
            text += f"📁 Тип публикации: "
            
            if task_type == TaskType.CAROUSEL:
                text += f"Карусель ({len(media_files)} фото)\n"
            elif task_type == TaskType.PHOTO:
                text += "Фото\n"
            else:
                text += "Видео\n"
            
            if need_uniquification:
                text += f"🎨 Уникализация: Включена\n"
            
            text += f"\n📋 Задачи: {', '.join([f'#{tid}' for tid in task_ids])}\n"
            
            if len(task_ids) > 1:
                text += f"\n⏳ Публикация будет выполнена в ближайшее время...\n"
                text += f"📊 Вы получите итоговый отчет после завершения всех задач."
            else:
                text += f"\n⏳ Публикация будет выполнена в ближайшее время..."
            
            query.edit_message_text(text)
        else:
            query.edit_message_text("❌ Не удалось создать задачи публикации")
        
        # Очищаем временные файлы и данные
        for file_info in media_files:
            try:
                if os.path.exists(file_info['path']):
                    # Не удаляем файлы сразу - они нужны для публикации
                    pass
            except:
                pass
        
        # Очищаем контекст
        context.user_data.clear()
        
        return ConversationHandler.END
        
    except Exception as e:
        logger.error(f"Ошибка при выполнении публикации: {e}")
        query.edit_message_text(f"❌ Ошибка при создании задач: {e}")
        return ConversationHandler.END

def handle_story_media_upload(update, context):
    """Обработчик загрузки медиа для Stories"""
    user_id = update.effective_user.id

    if not is_admin(user_id):
        update.message.reply_text("У вас нет прав для выполнения этой команды.")
        return ConversationHandler.END

    # Добавляем отладочную информацию
    publish_type = context.user_data.get('publish_type')
    account_ids = context.user_data.get('publish_account_ids', [])
    
    logger.info(f"📱 STORY: handle_story_media_upload вызван")
    logger.info(f"📱 STORY: publish_type={publish_type}")
    logger.info(f"📱 STORY: account_ids={account_ids}")
    logger.info(f"📱 STORY: user_data keys={list(context.user_data.keys())}")
    
    # Проверяем, что это для Stories и аккаунты выбраны
    if publish_type != 'story' or not account_ids:
        # Если это не для Stories или аккаунты не выбраны, НЕ обрабатываем
        logger.info(f"📱 STORY: Игнорируем - publish_type={publish_type}, account_ids={len(account_ids)}")
        # Возвращаем None вместо ConversationHandler.END, чтобы не блокировать другие обработчики
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
        if media_file.mime_type and media_file.mime_type.startswith('video/'):
            media_type = 'VIDEO'
            file_extension = '.mp4'
        else:
            media_type = 'PHOTO'
            file_extension = '.jpg'
    else:
        update.message.reply_text("❌ Пожалуйста, отправьте фото или видео для истории.")
        return ConversationHandler.END

    # Скачиваем медиафайл
    import tempfile
    with tempfile.NamedTemporaryFile(delete=False, suffix=file_extension) as temp_file:
        media_path = temp_file.name

    # Получаем файл и скачиваем его
    file = media_file.get_file()
    file.download(media_path)

    # Сохраняем данные
    context.user_data['publish_media_path'] = media_path
    context.user_data['publish_media_type'] = media_type

    # Показываем меню дополнительных возможностей (упрощенный интерфейс)
    is_scheduled = context.user_data.get('is_scheduled_post', False)
    
    if is_scheduled:
        # Для запланированных публикаций показываем только кнопку планирования
        keyboard = [
            [
                InlineKeyboardButton("👥 Упомянуть пользователей", callback_data='story_add_mentions'),
                InlineKeyboardButton("🔗 Добавить ссылку", callback_data='story_add_link')
            ],
            [
                InlineKeyboardButton("🗓️ Выбрать время публикации", callback_data='story_schedule_publish')
            ],
            [
                InlineKeyboardButton("❌ Отмена", callback_data='cancel_publish')
            ]
        ]
        title_prefix = "📅 Запланированная "
    else:
        # Для обычных публикаций показываем обе кнопки
        keyboard = [
            [
                InlineKeyboardButton("👥 Упомянуть пользователей", callback_data='story_add_mentions'),
                InlineKeyboardButton("🔗 Добавить ссылку", callback_data='story_add_link')
            ],
            [
                InlineKeyboardButton("✅ Опубликовать", callback_data='story_confirm_publish'),
                InlineKeyboardButton("⏰ Запланировать", callback_data='story_schedule_publish')
            ],
            [
                InlineKeyboardButton("❌ Отмена", callback_data='cancel_publish')
            ]
        ]
        title_prefix = ""
    
    reply_markup = InlineKeyboardMarkup(keyboard)

    account_info = ""
    if context.user_data.get('publish_to_all_accounts'):
        account_count = len(context.user_data.get('publish_account_ids', []))
        account_info = f"📱 Аккаунтов: {account_count}"
    else:
        username = context.user_data.get('publish_account_username', 'Неизвестно')
        account_info = f"📱 Аккаунт: @{username}"

    # Формируем текст с текущими настройками
    text = f"📱 {title_prefix}Медиа для истории загружено!\n\n{account_info}\n"
    text += f"🎬 Тип: {media_type}\n\n"
    
    # Показываем текущие настройки
    mentions = context.user_data.get('story_mentions', [])
    if mentions:
        text += f"👥 Упоминания: {len(mentions)} пользователей\n"
    
    link = context.user_data.get('story_link', '')
    if link:
        text += f"🔗 Ссылка: {link[:30]}{'...' if len(link) > 30 else ''}\n"
    
    if is_scheduled:
        text += "\nВыберите дополнительные возможности или выберите время публикации:"
    else:
        text += "\nВыберите дополнительные возможности или опубликуйте:"

    update.message.reply_text(
        text,
        reply_markup=reply_markup,
        
    )

    return STORY_ADD_FEATURES

def story_add_caption_handler(update, context):
    """Обработчик добавления подписи к истории"""
    query = update.callback_query
    query.answer()

    query.edit_message_text(
        "📝 *Добавление подписи к истории*\n\n"
        "Введите подпись для истории (или отправьте /skip для пропуска):",
        
    )

    return ENTER_CAPTION

def handle_story_caption_input(update, context):
    """Обработчик ввода подписи для истории"""
    if update.message.text == '/skip':
        context.user_data.pop('publish_caption', None)
        update.message.reply_text("⏭️ Подпись пропущена.")
    else:
        caption = update.message.text.strip()
        context.user_data['publish_caption'] = caption
        update.message.reply_text(f"✅ Подпись добавлена: {caption[:50]}{'...' if len(caption) > 50 else ''}")

    return back_to_story_features(update, context)

def story_add_mentions_handler(update, context):
    """Обработчик добавления упоминаний"""
    query = update.callback_query
    query.answer()

    query.edit_message_text(
        "👥 *Добавление упоминаний*\n\n"
        "Введите username пользователей для упоминания через запятую:\n"
        "Пример: `username1, username2, username3`\n\n"
        "Или отправьте /skip для пропуска.",
        
    )

    return STORY_ADD_MENTIONS

def story_add_link_handler(update, context):
    """Обработчик добавления ссылки"""
    query = update.callback_query
    query.answer()

    query.edit_message_text(
        "🔗 Добавление ссылки в Stories\n\n"
        "⚠️ ВАЖНО: Ссылки в Stories доступны только для:\n"
        "• 🎯 Аккаунтов с 10,000+ подписчиков\n"
        "• ✅ Верифицированных аккаунтов (синяя галочка)\n"
        "• 🏢 Business/Creator аккаунтов\n\n"
        "📝 Введите ссылку для swipe up:\n"
        "Пример: https://example.com\n\n"
        "Или отправьте /skip для пропуска."
    )

    return STORY_ADD_LINK

def story_add_text_handler(update, context):
    """Обработчик добавления текста поверх фото"""
    query = update.callback_query
    query.answer()

    query.edit_message_text(
        "💬 *Добавление текста поверх фото/видео*\n\n"
        "Введите текст, который будет отображаться поверх истории:\n\n"
        "Или отправьте /skip для пропуска.",
        
    )

    return STORY_ADD_TEXT

def handle_story_mentions_input(update, context):
    """Обработчик ввода упоминаний"""
    if update.message.text == '/skip':
        context.user_data.pop('story_mentions', None)
    else:
        # Парсим usernames
        usernames = [username.strip().replace('@', '') for username in update.message.text.split(',')]
        usernames = [u for u in usernames if u]  # Убираем пустые
        
        if usernames:
            # Создаем список упоминаний
            mentions = []
            for i, username in enumerate(usernames[:5]):  # Максимум 5 упоминаний
                mentions.append({
                    'username': username,
                    'x': 0.5,
                    'y': 0.2 + (i * 0.15),  # Располагаем вертикально
                    'width': 0.6,
                    'height': 0.1
                })
            
            context.user_data['story_mentions'] = mentions
            usernames_str = ', '.join([f"@{m['username']}" for m in mentions])
            update.message.reply_text(f"✅ Добавлено {len(mentions)} упоминаний: {usernames_str}")
        else:
            update.message.reply_text("❌ Не удалось распознать usernames. Попробуйте ещё раз.")
            return STORY_ADD_MENTIONS

    return back_to_story_features(update, context)

def handle_story_link_input(update, context):
    """Обработчик ввода ссылки"""
    if update.message.text == '/skip':
        context.user_data.pop('story_link', None)
    else:
        link = update.message.text.strip()
        if link.startswith('http://') or link.startswith('https://'):
            context.user_data['story_link'] = link
            update.message.reply_text(f"✅ Ссылка добавлена: {link}")
        else:
            update.message.reply_text("❌ Ссылка должна начинаться с http:// или https://. Попробуйте ещё раз.")
            return STORY_ADD_LINK

    return back_to_story_features(update, context)

def handle_story_text_input(update, context):
    """Обработчик ввода текста поверх фото"""
    if update.message.text == '/skip':
        context.user_data.pop('story_text', None)
        context.user_data.pop('story_text_color', None)
        context.user_data.pop('story_text_position', None)
    else:
        story_text = update.message.text.strip()
        if len(story_text) > 100:
            update.message.reply_text("❌ Текст слишком длинный. Максимум 100 символов.")
            return STORY_ADD_TEXT
        
        context.user_data['story_text'] = story_text
        context.user_data['story_text_color'] = '#ffffff'  # Белый по умолчанию
        context.user_data['story_text_position'] = {
            'x': 0.5, 'y': 0.5, 'width': 0.8, 'height': 0.1
        }
        
        update.message.reply_text(f"✅ Текст добавлен: {story_text}")

    return back_to_story_features(update, context)

def back_to_story_features(update, context):
    """Возвращает к меню дополнительных возможностей Stories"""
    is_scheduled = context.user_data.get('is_scheduled_post', False)
    
    if is_scheduled:
        # Для запланированных публикаций показываем только кнопку планирования
        keyboard = [
            [
                InlineKeyboardButton("👥 Упомянуть пользователей", callback_data='story_add_mentions'),
                InlineKeyboardButton("🔗 Добавить ссылку", callback_data='story_add_link')
            ],
            [
                InlineKeyboardButton("🗓️ Выбрать время публикации", callback_data='story_schedule_publish')
            ],
            [
                InlineKeyboardButton("❌ Отмена", callback_data='cancel_publish')
            ]
        ]
        title_prefix = "📅 Запланированная "
    else:
        # Для обычных публикаций показываем обе кнопки
        keyboard = [
            [
                InlineKeyboardButton("👥 Упомянуть пользователей", callback_data='story_add_mentions'),
                InlineKeyboardButton("🔗 Добавить ссылку", callback_data='story_add_link')
            ],
            [
                InlineKeyboardButton("✅ Опубликовать", callback_data='story_confirm_publish'),
                InlineKeyboardButton("⏰ Запланировать", callback_data='story_schedule_publish')
            ],
            [
                InlineKeyboardButton("❌ Отмена", callback_data='cancel_publish')
            ]
        ]
        title_prefix = ""
    
    reply_markup = InlineKeyboardMarkup(keyboard)

    account_info = ""
    if context.user_data.get('publish_to_all_accounts'):
        account_count = len(context.user_data.get('publish_account_ids', []))
        account_info = f"📱 Аккаунтов: {account_count}"
    else:
        username = context.user_data.get('publish_account_username', 'Неизвестно')
        account_info = f"📱 Аккаунт: @{username}"

    # Формируем текст с текущими настройками
    media_type = context.user_data.get('publish_media_type', 'UNKNOWN')
    text = f"📱 {title_prefix}Настройка истории\n\n{account_info}\n"
    text += f"🎬 Тип: {media_type}\n\n"
    
    # Показываем текущие настройки
    mentions = context.user_data.get('story_mentions', [])
    if mentions:
        text += f"👥 Упоминания: {len(mentions)} пользователей\n"
    
    link = context.user_data.get('story_link', '')
    if link:
        text += f"🔗 Ссылка: {link[:30]}{'...' if len(link) > 30 else ''}\n"
    
    if is_scheduled:
        text += "\nВыберите дополнительные возможности или выберите время публикации:"
    else:
        text += "\nВыберите дополнительные возможности или опубликуйте:"

    update.message.reply_text(
        text,
        reply_markup=reply_markup,
        
    )

    return STORY_ADD_FEATURES

def story_add_location_handler(update, context):
    """Обработчик добавления геолокации"""
    query = update.callback_query
    query.answer()

    query.edit_message_text(
        "📍 Добавление геолокации\n\n"
        "Введите название места или адрес:\n"
        "Пример: Москва, Красная площадь\n"
        "Пример: New York, Central Park\n\n"
        "Или отправьте /skip для пропуска."
    )

    return STORY_ADD_LOCATION

def handle_story_location_input(update, context):
    """Обработчик ввода геолокации"""
    if update.message.text == '/skip':
        context.user_data.pop('story_location', None)
    else:
        location_name = update.message.text.strip()
        if len(location_name) > 100:
            update.message.reply_text("❌ Название места слишком длинное. Максимум 100 символов.")
            return STORY_ADD_LOCATION
        
        context.user_data['story_location'] = location_name
        update.message.reply_text(f"✅ Геолокация добавлена: {location_name}")

    return back_to_story_features(update, context)

def story_add_hashtags_handler(update, context):
    """Обработчик добавления хештегов"""
    query = update.callback_query
    query.answer()

    query.edit_message_text(
        "🏷️ Добавление хештегов\n\n"
        "Введите хештеги через пробел или запятую:\n"
        "Пример: #travel #nature #beautiful\n"
        "Пример: travel, nature, beautiful\n\n"
        "Максимум 10 хештегов.\n"
        "Или отправьте /skip для пропуска."
    )

    return STORY_ADD_HASHTAGS

def handle_story_hashtags_input(update, context):
    """Обработчик ввода хештегов"""
    if update.message.text == '/skip':
        context.user_data.pop('story_hashtags', None)
    else:
        hashtags_text = update.message.text.strip()
        
        # Парсим хештеги (разделители: пробел, запятая)
        import re
        hashtags = re.split(r'[,\s]+', hashtags_text)
        hashtags = [tag.strip().replace('#', '') for tag in hashtags if tag.strip()]
        hashtags = [tag for tag in hashtags if tag]  # Убираем пустые
        
        if len(hashtags) > 10:
            update.message.reply_text("❌ Слишком много хештегов. Максимум 10. Попробуйте ещё раз.")
            return STORY_ADD_HASHTAGS
        
        if hashtags:
            context.user_data['story_hashtags'] = hashtags
            hashtags_str = ', '.join([f"#{tag}" for tag in hashtags])
            update.message.reply_text(f"✅ Добавлено {len(hashtags)} хештегов: {hashtags_str}")
        else:
            update.message.reply_text("❌ Не удалось распознать хештеги. Попробуйте ещё раз.")
            return STORY_ADD_HASHTAGS

    return back_to_story_features(update, context)

def story_confirm_publish_handler(update, context):
    """Обработчик подтверждения публикации истории"""
    query = update.callback_query
    query.answer()
    
    # Получаем данные для публикации
    media_path = context.user_data.get('publish_media_path')
    media_type = context.user_data.get('publish_media_type')
    caption = context.user_data.get('publish_caption', '')
    mentions = context.user_data.get('story_mentions', [])
    link = context.user_data.get('story_link', '')
    story_text = context.user_data.get('story_text', '')
    story_text_color = context.user_data.get('story_text_color', '#ffffff')
    story_text_position = context.user_data.get('story_text_position', {})

    # Отправляем сообщение о начале публикации
    status_message = query.edit_message_text(
        "⏳ Начинаем публикацию истории... Это может занять некоторое время."
    )

    # Проверяем, публикуем на один аккаунт или на несколько
    if context.user_data.get('publish_to_all_accounts'):
        # Публикация на несколько аккаунтов
        account_ids = context.user_data.get('publish_account_ids', [])
        
        # Создаем уникальный batch_id для группировки задач
        import time
        batch_id = f"{query.from_user.id}_{int(time.time())}"
        
        # Подготавливаем дополнительные данные для Stories
        additional_data = {
            'publish_type': 'story',
            'mentions': mentions,
            'link': link,
            'story_text': story_text,
            'story_text_color': story_text_color,
            'story_text_position': story_text_position
        }

        successful_tasks = 0
        failed_tasks = 0
        task_ids = []
        
        for account_id in account_ids:
            # Создаем задачу на публикацию
            success, task_id = create_publish_task(
                account_id=account_id,
                task_type=TaskType.STORY,
                media_path=media_path,
                caption=caption,
                additional_data=json.dumps(additional_data),
                user_id=query.from_user.id  # Добавляем user_id
            )
            
            if success:
                # Добавляем задачу в очередь
                from utils.task_queue import add_task_to_queue
                add_task_to_queue(task_id, query.message.chat_id, context.bot)
                task_ids.append(task_id)
                successful_tasks += 1
            else:
                failed_tasks += 1

        # Регистрируем пакет задач для итогового отчета
        if task_ids:
            from utils.task_queue import register_task_batch
            register_task_batch(task_ids, query.message.chat_id, context.bot)

        # Отправляем сообщение о результате
        if successful_tasks > 0:
            context.bot.edit_message_text(
                f"✅ Создано {successful_tasks} задач на публикацию истории!\n"
                f"{'❌ Не удалось создать ' + str(failed_tasks) + ' задач.' if failed_tasks > 0 else ''}\n\n"
                f"📊 Следите за прогрессом в разделе 'Статус задач'.",
                chat_id=status_message.chat_id,
                message_id=status_message.message_id
            )
        else:
            context.bot.edit_message_text(
                f"❌ Не удалось создать ни одной задачи на публикацию.",
                chat_id=status_message.chat_id,
                message_id=status_message.message_id
            )
    else:
        # Публикация на один аккаунт
        account_id = context.user_data.get('publish_account_id')

        # Подготавливаем дополнительные данные для Stories
        additional_data = {
            'publish_type': 'story',
            'mentions': mentions,
            'link': link,
            'story_text': story_text,
            'story_text_color': story_text_color,
            'story_text_position': story_text_position
        }

        # Создаем задачу на публикацию
        success, task_id = create_publish_task(
            account_id=account_id,
            task_type=TaskType.STORY,
            media_path=media_path,
            caption=caption,
            additional_data=json.dumps(additional_data),
            user_id=query.from_user.id  # Добавляем user_id
        )

        if not success:
            context.bot.edit_message_text(
                f"❌ Ошибка при создании задачи: {task_id}",
                chat_id=status_message.chat_id,
                message_id=status_message.message_id
            )
            return ConversationHandler.END

        # Добавляем задачу в очередь
        from utils.task_queue import add_task_to_queue
        add_task_to_queue(task_id)

        # Создаем batch для одной задачи
        from utils.task_queue import register_task_batch
        register_task_batch([task_id], query.message.chat_id, context.bot)

        # Отправляем сообщение об успехе
        context.bot.edit_message_text(
            f"✅ Задача #{task_id} на публикацию истории создана!\n\n"
            f"📊 Следите за прогрессом в разделе 'Статус задач'.",
            chat_id=status_message.chat_id,
            message_id=status_message.message_id
        )

    # Очищаем данные пользователя
    context.user_data.clear()
    return ConversationHandler.END

def handle_reels_media_upload(update, context):
    """Обработчик загрузки видео для Reels"""
    user_id = update.effective_user.id

    if not is_admin(user_id):
        update.message.reply_text("У вас нет прав для выполнения этой команды.")
        return ConversationHandler.END

    # Добавляем отладочную информацию
    publish_type = context.user_data.get('publish_type')
    account_ids = context.user_data.get('publish_account_ids', [])
    
    logger.info(f"🎥 REELS: handle_reels_media_upload вызван")
    logger.info(f"🎥 REELS: publish_type={publish_type}")
    logger.info(f"🎥 REELS: account_ids={account_ids}")
    logger.info(f"🎥 REELS: user_data keys={list(context.user_data.keys())}")
    
    # Проверяем наличие видео
    video_file = None
    if update.message.video:
        video_file = update.message.video
        logger.info(f"🎥 REELS: Получено видео: {video_file.file_id}")
    elif update.message.document and update.message.document.mime_type and update.message.document.mime_type.startswith('video/'):
        video_file = update.message.document
        logger.info(f"🎥 REELS: Получен видео документ: {video_file.file_id}")
    
    if not video_file:
        logger.warning(f"🎥 REELS: Видео не найдено")
        update.message.reply_text("❌ Пожалуйста, отправьте видео файл для Reels.")
        return ConversationHandler.END
    
    # Если аккаунты не выбраны, но есть видео - показываем сообщение
    if not account_ids:
        logger.warning(f"🎥 REELS: Аккаунты не выбраны")
        update.message.reply_text("❌ Сначала выберите аккаунты для публикации Reels через меню.")
        return ConversationHandler.END
    
    # Если publish_type не установлен, устанавливаем его
    if publish_type != 'reels':
        logger.info(f"🎥 REELS: Устанавливаем publish_type = 'reels'")
        context.user_data['publish_type'] = 'reels'

    # Проверяем длительность видео (должно быть до 90 секунд)
    if hasattr(video_file, 'duration') and video_file.duration > 90:
        update.message.reply_text("❌ Видео слишком длинное. Максимальная длительность для Reels - 90 секунд.")
        return ConversationHandler.END

    try:
        # Скачиваем видео
        file_id = video_file.file_id
        media = context.bot.get_file(file_id)

        # Создаем временный файл для сохранения видео
        import tempfile
        with tempfile.NamedTemporaryFile(delete=False, suffix='.mp4') as temp_file:
            video_path = temp_file.name

        # Скачиваем видео во временный файл
        media.download(video_path)

        # Сохраняем путь к видео
        context.user_data['reels_video_path'] = video_path
        
        # Инициализируем reels_options если не существует
        if 'reels_options' not in context.user_data:
            context.user_data['reels_options'] = {}

        # Переходим к вводу описания (как в постах)
        return show_reels_caption_input(update, context)

    except Exception as e:
        logger.error(f"Ошибка при загрузке видео: {e}")
        update.message.reply_text("❌ Ошибка при загрузке видео. Попробуйте еще раз.")
        return ConversationHandler.END

def show_reels_caption_input(update, context):
    """Показать экран ввода описания для Reels"""
    try:
        selected_accounts = context.user_data.get('publish_account_ids', [])
        
        text = f"📝 Введите описание для Reels\n\n"
        text += f"🎥 Видео: Загружено\n"
        text += f"📤 Аккаунтов: {len(selected_accounts)}\n"
        
        if len(selected_accounts) > 1:
            text += "\n🎨 Контент будет уникализирован для каждого аккаунта\n"
        
        text += "\n✍️ Отправьте текст описания или нажмите 'Без описания':"
        
        keyboard = [
            [InlineKeyboardButton("📝 Без описания", callback_data="reels_no_caption")],
            [InlineKeyboardButton("◀️ Назад к видео", callback_data="reels_back_to_video")]
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
        
        from telegram_bot.states import REELS_ADD_CAPTION
        return REELS_ADD_CAPTION
        
    except Exception as e:
        logger.error(f"Ошибка при показе ввода описания: {e}")
        if hasattr(update, 'callback_query') and update.callback_query:
            update.callback_query.edit_message_text(f"❌ Ошибка: {e}")
        else:
            update.message.reply_text(f"❌ Ошибка: {e}")
        from telegram_bot.states import REELS_ADD_CAPTION
        return REELS_ADD_CAPTION

def handle_reels_caption_input(update, context):
    """Обработчик ввода описания для Reels"""
    try:
        caption = update.message.text
        
        # Сохраняем описание
        context.user_data['reels_caption'] = caption
        
        # Переходим к вводу хештегов
        return show_reels_hashtags_input(update, context)
        
    except Exception as e:
        logger.error(f"Ошибка при обработке описания: {e}")
        update.message.reply_text(f"❌ Ошибка: {e}")
        from telegram_bot.states import REELS_ADD_CAPTION
        return REELS_ADD_CAPTION

def handle_reels_caption_actions(update, context):
    """Обработка действий с описанием для Reels"""
    query = update.callback_query
    query.answer()
    
    try:
        if query.data == "reels_no_caption":
            # Без описания
            context.user_data['reels_caption'] = ""
            return show_reels_hashtags_input(update, context)
            
        elif query.data == "reels_back_to_video":
            # Возвращаемся к загрузке видео
            query.edit_message_text("🎥 Отправьте видео для Reels (до 90 секунд):")
            from telegram_bot.states import REELS_UPLOAD_MEDIA
            return REELS_UPLOAD_MEDIA
            
    except Exception as e:
        logger.error(f"Ошибка при обработке действий с описанием: {e}")
        query.edit_message_text(f"❌ Ошибка: {e}")
        from telegram_bot.states import REELS_ADD_CAPTION
        return REELS_ADD_CAPTION

def show_reels_hashtags_input(update, context):
    """Показать экран ввода хештегов для Reels"""
    try:
        selected_accounts = context.user_data.get('publish_account_ids', [])
        caption = context.user_data.get('reels_caption', "")
        
        text = f"🏷 Введите хештеги для Reels\n\n"
        text += f"🎥 Видео: Загружено\n"
        text += f"📤 Аккаунтов: {len(selected_accounts)}\n"
        
        if caption:
            preview = caption[:50] + "..." if len(caption) > 50 else caption
            text += f"📝 Описание: {preview}\n"
        else:
            text += f"📝 Описание: Без описания\n"
        
        text += "\n🏷 Введите хештеги (например: #reels #video #instagram)\n"
        text += "или нажмите 'Без хештегов':"
        
        keyboard = [
            [InlineKeyboardButton("🏷 Без хештегов", callback_data="reels_no_hashtags")],
            [InlineKeyboardButton("◀️ Назад к описанию", callback_data="reels_back_to_caption")]
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
        
        from telegram_bot.states import REELS_ADD_HASHTAGS
        return REELS_ADD_HASHTAGS
        
    except Exception as e:
        logger.error(f"Ошибка при показе ввода хештегов: {e}")
        if hasattr(update, 'callback_query') and update.callback_query:
            update.callback_query.edit_message_text(f"❌ Ошибка: {e}")
        else:
            update.message.reply_text(f"❌ Ошибка: {e}")
        from telegram_bot.states import REELS_ADD_HASHTAGS
        return REELS_ADD_HASHTAGS

def handle_reels_hashtags_input(update, context):
    """Обработчик ввода хештегов для Reels"""
    try:
        hashtags_text = update.message.text
        
        # Парсим хештеги
        hashtags = []
        # Убираем # если есть, разбиваем по пробелам
        text = hashtags_text.replace('#', '')
        tags = text.split()
        
        for tag in tags:
            if tag.strip():
                hashtags.append(tag.strip())
        
        # Сохраняем хештеги
        if 'reels_options' not in context.user_data:
            context.user_data['reels_options'] = {}
        context.user_data['reels_options']['hashtags'] = hashtags
        
        # Переходим к дополнительным функциям
        return show_reels_features_menu(update, context)
        
    except Exception as e:
        logger.error(f"Ошибка при обработке хештегов: {e}")
        update.message.reply_text(f"❌ Ошибка: {e}")
        from telegram_bot.states import REELS_ADD_HASHTAGS
        return REELS_ADD_HASHTAGS

def handle_reels_hashtags_actions(update, context):
    """Обработка действий с хештегами для Reels"""
    query = update.callback_query
    query.answer()
    
    try:
        if query.data == "reels_no_hashtags":
            # Без хештегов
            if 'reels_options' not in context.user_data:
                context.user_data['reels_options'] = {}
            context.user_data['reels_options']['hashtags'] = []
            return show_reels_features_menu(update, context)
            
        elif query.data == "reels_back_to_caption":
            # Возвращаемся к описанию
            return show_reels_caption_input(update, context)
            
    except Exception as e:
        logger.error(f"Ошибка при обработке действий с хештегами: {e}")
        query.edit_message_text(f"❌ Ошибка: {e}")
        from telegram_bot.states import REELS_ADD_HASHTAGS
        return REELS_ADD_HASHTAGS

def show_reels_features_menu(update, context):
    """Показывает меню дополнительных функций для Reels"""
    from telegram_bot.states import REELS_ADD_FEATURES
    
    # Получаем текущие настройки
    options = context.user_data.get('reels_options', {})
    caption = context.user_data.get('reels_caption', '')
    selected_accounts = context.user_data.get('publish_account_ids', [])
    
    # Показываем текущие настройки
    is_scheduled = context.user_data.get('is_scheduled_post', False)
    title_prefix = "📅 Запланированные " if is_scheduled else ""
    
    status_text = f"🎥 *{title_prefix}Настройки Reels*\n\n"
    status_text += f"📤 Аккаунтов: {len(selected_accounts)}\n"
    
    if caption:
        preview = caption[:50] + "..." if len(caption) > 50 else caption
        status_text += f"📝 Описание: {preview}\n"
    else:
        status_text += f"📝 Описание: Без описания\n"
    
    hashtags = options.get('hashtags', [])
    if hashtags:
        status_text += f"🏷️ Хештеги: {len(hashtags)} шт.\n"
    else:
        status_text += f"🏷️ Хештеги: Без хештегов\n"
    
    # Дополнительные функции
    usertags = options.get('usertags', [])
    distributed_usertags = options.get('distributed_usertags', [])
    
    if distributed_usertags:
        total_tags = sum(len(item['tags']) for item in distributed_usertags)
        status_text += f"👥 Теги пользователей: ✅ ({total_tags} распределено)\n"
    elif usertags:
        status_text += f"👥 Теги пользователей: ✅ ({len(usertags)})\n"
    else:
        status_text += f"👥 Теги пользователей: ❌ (0)\n"
    
    location = options.get('location')
    status_text += f"📍 Локация: {'✅' if location else '❌'}\n"
    
    # Обложка
    thumbnail_path = options.get('thumbnail_path')
    cover_time = options.get('cover_time', 0)
    
    if thumbnail_path:
        status_text += f"🖼️ Обложка: ✅ (фото)\n"
    elif cover_time > 0:
        status_text += f"🖼️ Обложка: ✅ ({cover_time}с)\n"
    else:
        status_text += f"🖼️ Обложка: ❌\n"
    
    if len(selected_accounts) > 1:
        status_text += f"\n🎨 Уникализация: Включена\n"
    
    status_text += "\n*Дополнительные настройки:*"
    
    # Формируем клавиатуру
    keyboard = []
    
    # Дополнительные функции
    if len(selected_accounts) > 1:
        keyboard.append([
            InlineKeyboardButton("👥 Массовые теги", callback_data="reels_bulk_usertags"),
            InlineKeyboardButton("📍 Добавить локацию", callback_data="reels_add_location")
        ])
    else:
        keyboard.append([
            InlineKeyboardButton("👥 Отметить пользователей", callback_data="reels_add_usertags"),
            InlineKeyboardButton("📍 Добавить локацию", callback_data="reels_add_location")
        ])
    
    keyboard.append([
        InlineKeyboardButton("🖼️ Выбрать обложку", callback_data="reels_choose_cover")
    ])
    
    # Навигация
    keyboard.append([
        InlineKeyboardButton("✏️ Изменить описание", callback_data="reels_edit_caption"),
        InlineKeyboardButton("🏷️ Изменить хештеги", callback_data="reels_edit_hashtags")
    ])
    
    # Кнопки действий
    is_scheduled = context.user_data.get('is_scheduled_post', False)
    
    if is_scheduled:
        # Для запланированных публикаций показываем только кнопку планирования
        keyboard.append([
            InlineKeyboardButton("🗓️ Выбрать время публикации", callback_data="reels_schedule_publish")
        ])
    else:
        # Для обычных публикаций показываем обе кнопки
        keyboard.append([
            InlineKeyboardButton("✅ Опубликовать", callback_data="reels_confirm_publish"),
            InlineKeyboardButton("⏰ Запланировать", callback_data="reels_schedule_publish")
        ])
    
    keyboard.append([InlineKeyboardButton("❌ Отмена", callback_data="cancel_reels")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    if hasattr(update, 'callback_query') and update.callback_query:
        update.callback_query.edit_message_text(status_text, reply_markup=reply_markup)
    else:
        update.message.reply_text(status_text, reply_markup=reply_markup)
    
    return REELS_ADD_FEATURES

def reels_choose_cover_handler(update, context):
    """Обработчик выбора обложки для Reels"""
    query = update.callback_query
    query.answer()
    
    keyboard = [
        [InlineKeyboardButton("🖼️ Загрузить фото обложки", callback_data="reels_upload_cover")],
        [InlineKeyboardButton("⏰ Выбрать время в видео", callback_data="reels_time_cover")],
        [InlineKeyboardButton("🔙 Назад", callback_data="reels_back_to_features")]
    ]
    
    query.edit_message_text(
        "🖼️ *Выберите способ создания обложки:*\n\n"
        "🖼️ *Загрузить фото* - используйте собственное изображение\n"
        "⏰ *Выбрать время* - кадр из видео на определенной секунде",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )
    
    from telegram_bot.states import REELS_CHOOSE_COVER
    return REELS_CHOOSE_COVER

def handle_reels_cover_input(update, context):
    """Обработчик ввода времени обложки для Reels"""
    if update.message.text == '/skip':
        if 'reels_options' not in context.user_data:
            context.user_data['reels_options'] = {}
        context.user_data['reels_options']['cover_time'] = 0
        context.user_data['reels_options']['thumbnail_path'] = None
        update.message.reply_text("✅ Обложка будет выбрана автоматически.")
    else:
        try:
            cover_time = float(update.message.text.strip())
            if 0 <= cover_time <= 90:
                if 'reels_options' not in context.user_data:
                    context.user_data['reels_options'] = {}
                context.user_data['reels_options']['cover_time'] = cover_time
                context.user_data['reels_options']['thumbnail_path'] = None  # Сбрасываем загруженную обложку
                update.message.reply_text(f"✅ Обложка установлена на {cover_time} секунд.")
            else:
                update.message.reply_text("❌ Время должно быть от 0 до 90 секунд.")
                from telegram_bot.states import REELS_TIME_COVER
                return REELS_TIME_COVER
        except ValueError:
            update.message.reply_text("❌ Пожалуйста, введите число.")
            from telegram_bot.states import REELS_TIME_COVER
            return REELS_TIME_COVER
    
    return show_reels_features_menu(update, context)

def handle_reels_cover_upload(update, context):
    """Обработчик загрузки фото обложки для Reels"""
    if not update.message.photo:
        update.message.reply_text("❌ Пожалуйста, отправьте фото для обложки.")
        from telegram_bot.states import REELS_UPLOAD_COVER
        return REELS_UPLOAD_COVER
    
    try:
        # Получаем фото с наибольшим разрешением
        photo_file = update.message.photo[-1]
        file_id = photo_file.file_id
        media = context.bot.get_file(file_id)
        
        # Создаем временный файл для сохранения обложки
        import tempfile
        with tempfile.NamedTemporaryFile(delete=False, suffix='.jpg') as temp_file:
            thumbnail_path = temp_file.name
        
        # Скачиваем фото во временный файл
        media.download(thumbnail_path)
        
        # Сохраняем путь к обложке
        if 'reels_options' not in context.user_data:
            context.user_data['reels_options'] = {}
        context.user_data['reels_options']['thumbnail_path'] = thumbnail_path
        context.user_data['reels_options']['cover_time'] = 0  # Сбрасываем время
        
        update.message.reply_text("✅ Фото обложки сохранено.")
        
        return show_reels_features_menu(update, context)
        
    except Exception as e:
        logger.error(f"Ошибка при загрузке обложки: {e}")
        update.message.reply_text("❌ Ошибка при загрузке обложки. Попробуйте еще раз.")
        from telegram_bot.states import REELS_UPLOAD_COVER
        return REELS_UPLOAD_COVER

def handle_reels_location_input(update, context):
    """Обработчик ввода локации для Reels"""
    if update.message.text == '/skip':
        if 'reels_options' not in context.user_data:
            context.user_data['reels_options'] = {}
        context.user_data['reels_options']['location'] = None
        update.message.reply_text("✅ Локация пропущена.")
    else:
        location_name = update.message.text.strip()
        if 'reels_options' not in context.user_data:
            context.user_data['reels_options'] = {}
        context.user_data['reels_options']['location'] = location_name
        update.message.reply_text(f"✅ Локация сохранена: {location_name}")
    
    # Показываем меню функций через новое сообщение
    return show_reels_features_menu_message(update, context)

def reels_add_music_handler(update, context):
    """Обработчик добавления музыки к Reels"""
    query = update.callback_query
    query.answer()
    
    query.edit_message_text(
        "🎵 *Добавление музыки к Reels*\n\n"
        "Отправьте название песни или исполнителя для поиска или /skip для пропуска:\n\n"
        "Пример: Shape of You Ed Sheeran"
    )
    
    from telegram_bot.states import REELS_ADD_MUSIC
    return REELS_ADD_MUSIC

def handle_reels_music_input(update, context):
    """Обработчик ввода музыки для Reels"""
    if update.message.text == '/skip':
        if 'reels_options' not in context.user_data:
            context.user_data['reels_options'] = {}
        context.user_data['reels_options']['music_track'] = None
        update.message.reply_text("✅ Музыка пропущена.")
        return show_reels_features_menu_message(update, context)
    
    # Поиск музыки
    query = update.message.text.strip()
    
    try:
        # Используем первый аккаунт для поиска музыки
        account_ids = context.user_data.get('publish_account_ids', [])
        if not account_ids:
            update.message.reply_text("❌ Ошибка: аккаунты не выбраны.")
            return show_reels_features_menu(update, context)
        
        from instagram.reels_manager import ReelsManager
        manager = ReelsManager(account_ids[0])
        tracks = manager.search_music(query, limit=5)
        
        if not tracks:
            update.message.reply_text("❌ Музыка не найдена. Попробуйте другой запрос.")
            from telegram_bot.states import REELS_ADD_MUSIC
            return REELS_ADD_MUSIC
        
        # Показываем найденные треки
        keyboard = []
        for i, track in enumerate(tracks):
            title = track.title if hasattr(track, 'title') else 'Unknown'
            artist = track.artist if hasattr(track, 'artist') else 'Unknown'
            keyboard.append([InlineKeyboardButton(
                f"🎵 {title} - {artist}",
                callback_data=f"reels_select_music_{i}"
            )])
        
        keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="reels_back_to_features")])
        
        # Сохраняем треки для выбора
        context.user_data['found_tracks'] = tracks
        
        update.message.reply_text(
            f"🎵 *Найдено треков: {len(tracks)}*\n\n"
            "Выберите трек:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        
        from telegram_bot.states import REELS_ADD_MUSIC
        return REELS_ADD_MUSIC
        
    except Exception as e:
        logger.error(f"Ошибка при поиске музыки: {e}")
        update.message.reply_text("❌ Ошибка при поиске музыки. Попробуйте еще раз.")
        from telegram_bot.states import REELS_ADD_MUSIC
        return REELS_ADD_MUSIC

def reels_choose_visibility_handler(update, context):
    """Обработчик выбора видимости Reels"""
    query = update.callback_query
    query.answer()
    
    keyboard = [
        [InlineKeyboardButton("📱 Только в Reels", callback_data="reels_hide_from_feed")],
        [InlineKeyboardButton("📱📸 В Reels + Ленте", callback_data="reels_show_in_feed")],
        [InlineKeyboardButton("🔙 Назад", callback_data="reels_back_to_features")]
    ]
    
    query.edit_message_text(
        "🔍 *Настройки видимости Reels*\n\n"
        "Выберите где будет отображаться ваш Reels:\n\n"
        "📱 *Только в Reels* - видео будет только в разделе Reels\n"
        "📱📸 *В Reels + Ленте* - видео будет в Reels и в основной ленте профиля",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    
    from telegram_bot.states import REELS_CHOOSE_VISIBILITY
    return REELS_CHOOSE_VISIBILITY

def reels_confirm_publish_handler(update, context):
    """Обработчик подтверждения публикации Reels"""
    query = update.callback_query
    query.answer()
    
    # Получаем данные для публикации
    video_path = context.user_data.get('reels_video_path')
    caption = context.user_data.get('reels_caption', '')
    options = context.user_data.get('reels_options', {})
    selected_accounts = context.user_data.get('publish_account_ids', [])

    if not video_path or not selected_accounts:
        query.edit_message_text("❌ Недостаточно данных для публикации Reels.")
        return ConversationHandler.END

    # Отправляем сообщение о начале публикации
    status_message = query.edit_message_text(
        "⏳ Начинаем публикацию Reels... Это может занять некоторое время."
    )

    # Создаем задачи для каждого аккаунта
    from database.db_manager import create_publish_task
    from database.models import TaskType
    from utils.task_queue import add_task_to_queue, register_task_batch
    
    task_ids = []
    
    for account_id in selected_accounts:
        # Подготавливаем данные для задачи
        music_track = options.get('music_track')
        # Преобразуем объект Track в словарь для JSON сериализации
        if music_track and hasattr(music_track, '__dict__'):
            music_track_dict = {
                'id': getattr(music_track, 'id', ''),
                'title': getattr(music_track, 'title', ''),
                'subtitle': getattr(music_track, 'subtitle', ''),
                'display_artist': getattr(music_track, 'display_artist', getattr(music_track, 'artist', '')),
                'audio_asset_id': getattr(music_track, 'audio_asset_id', 0),
                'audio_cluster_id': getattr(music_track, 'audio_cluster_id', 0),
                'highlight_start_times_in_ms': getattr(music_track, 'highlight_start_times_in_ms', []),
                'is_explicit': getattr(music_track, 'is_explicit', False),
                'dash_manifest': getattr(music_track, 'dash_manifest', ''),
                'has_lyrics': getattr(music_track, 'has_lyrics', False),
                'duration_in_ms': getattr(music_track, 'duration_in_ms', 0),
                'allows_saving': getattr(music_track, 'allows_saving', True),
                'territory_validity_periods': getattr(music_track, 'territory_validity_periods', {}),
                # Добавляем поля для обратной совместимости
                'artist': getattr(music_track, 'display_artist', getattr(music_track, 'artist', '')),
                'duration': getattr(music_track, 'duration_in_ms', 0) // 1000 if getattr(music_track, 'duration_in_ms', 0) else 0
            }
        else:
            music_track_dict = music_track
        
        task_data = {
            'hashtags': options.get('hashtags', []),
            'usertags': options.get('usertags', []),
            'distributed_usertags': options.get('distributed_usertags', []),
            'location': options.get('location'),
            'music_track': music_track_dict,
            'cover_time': options.get('cover_time', 0),
            'thumbnail_path': options.get('thumbnail_path'),
            'uniquify_content': len(selected_accounts) > 1  # Уникализируем только для множественной публикации
        }
        
        # Создаем задачу
        success, task_id = create_publish_task(
            account_id=account_id,
            task_type=TaskType.VIDEO,  # Reels используют VIDEO тип
            media_path=video_path,
            caption=caption,
            additional_data=json.dumps(task_data),
            user_id=query.from_user.id  # Добавляем user_id
        )
        
        if success:
            task_ids.append(task_id)
            # Добавляем в очередь с уведомлениями
            add_task_to_queue(task_id, query.message.chat_id, context.bot)
        else:
            raise Exception(f"Не удалось создать задачу для аккаунта {account_id}: {task_id}")
    
    # Регистрируем пакет задач для итогового отчета
    if task_ids:
        register_task_batch(task_ids, query.message.chat_id, context.bot)
    
    # Сообщаем об успешном создании задач
    if len(selected_accounts) == 1:
        username = context.user_data.get('publish_account_username', 'Unknown')
        message = f"✅ *Reels поставлен в очередь на публикацию*\n\n"
        message += f"👤 Аккаунт: @{username}\n"
        message += f"📋 ID задачи: {task_ids[0]}\n\n"
        message += "Вы получите уведомление о завершении публикации."
    else:
        message = f"✅ *Reels поставлены в очередь на публикацию*\n\n"
        message += f"👥 Аккаунтов: {len(selected_accounts)}\n"
        message += f"📋 Задач создано: {len(task_ids)}\n\n"
        message += "Вы получите уведомления о завершении каждой публикации."
    
    # Кнопка для проверки статуса
    keyboard = [
        [InlineKeyboardButton("📊 Проверить статус", callback_data=f"check_task_status_{task_ids[0]}")],
        [InlineKeyboardButton("🔙 В главное меню", callback_data="main_menu")]
    ]
    
    status_message.edit_text(
        message,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )
    
    # Очищаем данные пользователя
    cleanup_reels_data(context)
    
    return ConversationHandler.END

def execute_reels_publish(update, context):
    """Выполнение публикации Reels"""
    query = update.callback_query
    query.answer()
    
    # Получаем все данные
    video_path = context.user_data.get('reels_video_path')
    caption = context.user_data.get('reels_caption', '')
    options = context.user_data.get('reels_options', {})
    account_ids = context.user_data.get('publish_account_ids', [])
    
    try:
        # Отправляем сообщение о начале публикации
        query.edit_message_text("⏳ Начинаем публикацию Reels... Это может занять некоторое время.")
        
        # Создаем задачи для каждого аккаунта
        from database.db_manager import create_publish_task
        from database.models import TaskType
        from utils.task_queue import add_task_to_queue, register_task_batch
        
        task_ids = []
        
        for account_id in account_ids:
            # Подготавливаем данные для задачи
            music_track = options.get('music_track')
            # Преобразуем объект Track в словарь для JSON сериализации
            if music_track and hasattr(music_track, '__dict__'):
                music_track_dict = {
                    'id': getattr(music_track, 'id', ''),
                    'title': getattr(music_track, 'title', ''),
                    'subtitle': getattr(music_track, 'subtitle', ''),
                    'display_artist': getattr(music_track, 'display_artist', getattr(music_track, 'artist', '')),
                    'audio_asset_id': getattr(music_track, 'audio_asset_id', 0),
                    'audio_cluster_id': getattr(music_track, 'audio_cluster_id', 0),
                    'highlight_start_times_in_ms': getattr(music_track, 'highlight_start_times_in_ms', []),
                    'is_explicit': getattr(music_track, 'is_explicit', False),
                    'dash_manifest': getattr(music_track, 'dash_manifest', ''),
                    'has_lyrics': getattr(music_track, 'has_lyrics', False),
                    'duration_in_ms': getattr(music_track, 'duration_in_ms', 0),
                    'allows_saving': getattr(music_track, 'allows_saving', True),
                    'territory_validity_periods': getattr(music_track, 'territory_validity_periods', {}),
                    # Добавляем поля для обратной совместимости
                    'artist': getattr(music_track, 'display_artist', getattr(music_track, 'artist', '')),
                    'duration': getattr(music_track, 'duration_in_ms', 0) // 1000 if getattr(music_track, 'duration_in_ms', 0) else 0
                }
            else:
                music_track_dict = music_track
            
            task_data = {
                'hashtags': options.get('hashtags', []),
                'usertags': options.get('usertags', []),
                'distributed_usertags': options.get('distributed_usertags', []),
                'location': options.get('location'),
                'music_track': music_track_dict,
                'cover_time': options.get('cover_time', 0),
                'thumbnail_path': options.get('thumbnail_path'),
                'uniquify_content': len(account_ids) > 1  # Уникализируем только для множественной публикации
            }
            
            # Создаем задачу
            success, task_id = create_publish_task(
                account_id=account_id,
                task_type=TaskType.VIDEO,  # Reels используют VIDEO тип
                media_path=video_path,
                caption=caption,
                additional_data=json.dumps(task_data),
                user_id=query.from_user.id  # Добавляем user_id
            )
            
            if success:
                task_ids.append(task_id)
                # Добавляем в очередь с уведомлениями
                add_task_to_queue(task_id, query.message.chat_id, context.bot)
            else:
                raise Exception(f"Не удалось создать задачу для аккаунта {account_id}: {task_id}")
        
        # Регистрируем пакет задач для итогового отчета
        if task_ids:
            register_task_batch(task_ids, query.message.chat_id, context.bot)
        
        # Сообщаем об успешном создании задач
        if len(account_ids) == 1:
            username = context.user_data.get('publish_account_username', 'Unknown')
            message = f"✅ *Reels поставлен в очередь на публикацию*\n\n"
            message += f"👤 Аккаунт: @{username}\n"
            message += f"📋 ID задачи: {task_ids[0]}\n\n"
            message += "Вы получите уведомление о завершении публикации."
        else:
            message = f"✅ *Reels поставлены в очередь на публикацию*\n\n"
            message += f"👥 Аккаунтов: {len(account_ids)}\n"
            message += f"📋 Задач создано: {len(task_ids)}\n\n"
            message += "Вы получите уведомления о завершении каждой публикации."
        
        # Кнопка для проверки статуса
        keyboard = [
            [InlineKeyboardButton("📊 Проверить статус", callback_data=f"check_task_status_{task_ids[0]}")],
            [InlineKeyboardButton("🔙 В главное меню", callback_data="main_menu")]
        ]
        
        query.edit_message_text(
            message,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        
        # Очищаем данные пользователя
        cleanup_reels_data(context)
        
        return ConversationHandler.END
        
    except Exception as e:
        logger.error(f"Ошибка при создании задач публикации Reels: {e}")
        query.edit_message_text(
            f"❌ Ошибка при создании задач публикации: {e}",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Назад", callback_data="reels_back_to_features")]
            ])
        )
        from telegram_bot.states import REELS_CONFIRM_PUBLISH
        return REELS_CONFIRM_PUBLISH

def cleanup_reels_data(context):
    """Очистка данных Reels из контекста"""
    keys_to_remove = [
        'reels_video_path', 'reels_caption', 'reels_options',
        'found_tracks', 'publish_account_ids', 'publish_account_usernames',
        'publish_account_id', 'publish_account_username', 'publish_type',
        'publish_to_all_accounts'
    ]
    
    for key in keys_to_remove:
        if key in context.user_data:
            del context.user_data[key]

# Обработчики callback'ов для Reels
def handle_reels_callbacks(update, context):
    """Обработчик callback'ов для Reels"""
    query = update.callback_query
    data = query.data
    
    # Основные функции
    if data == "reels_no_caption":
        return handle_reels_caption_actions(update, context)
    elif data == "reels_back_to_video":
        return handle_reels_caption_actions(update, context)
    elif data == "reels_no_hashtags":
        return handle_reels_hashtags_actions(update, context)
    elif data == "reels_back_to_caption":
        return handle_reels_hashtags_actions(update, context)
    elif data == "reels_edit_caption":
        return show_reels_caption_input(update, context)
    elif data == "reels_edit_hashtags":
        return show_reels_hashtags_input(update, context)
    
    # Дополнительные функции
    elif data == "reels_add_usertags":
        return reels_add_usertags_handler(update, context)
    elif data == "reels_add_location":
        return reels_add_location_handler(update, context)
    elif data == "reels_add_music":
        return reels_add_music_handler(update, context)
    elif data == "reels_bulk_usertags":
        # Обработчик массовых тегов
        query.answer()
        query.edit_message_text(
            "👥 *Массовые теги пользователей*\n\n"
            "Отправьте список пользователей для тегирования:\n\n"
            "📝 *Текстом* - каждый пользователь с новой строки\n"
            "📁 *Файлом* - .txt файл с пользователями\n\n"
            "Пример:\n"
            "excc\n"
            "user123\n"
            "@another_user\n\n"
            "Или отправьте /skip для пропуска",
            parse_mode='Markdown'
        )
        from telegram_bot.states import REELS_BULK_USERTAGS
        return REELS_BULK_USERTAGS
    elif data == "reels_upload_cover":
        # Обработчик загрузки обложки
        query.answer()
        query.edit_message_text(
            "🖼️ *Загрузка обложки*\n\n"
            "Отправьте фото которое будет использоваться как обложка для Reels:",
            parse_mode='Markdown'
        )
        from telegram_bot.states import REELS_UPLOAD_COVER
        return REELS_UPLOAD_COVER
    elif data == "reels_time_cover":
        # Обработчик выбора времени обложки
        query.answer()
        query.edit_message_text(
            "⏰ *Выбор времени обложки*\n\n"
            "Введите время в секундах для выбора кадра обложки (0-90):\n\n"
            "Например: 5 (для кадра на 5-й секунде)\n\n"
            "Или отправьте /skip для автоматического выбора:",
            parse_mode='Markdown'
        )
        from telegram_bot.states import REELS_TIME_COVER
        return REELS_TIME_COVER
    # Настройки видимости удалены
    elif data == "reels_choose_cover":
        return reels_choose_cover_handler(update, context)
    elif data == "reels_confirm_publish":
        return reels_confirm_publish_handler(update, context)
    elif data == "reels_schedule_publish":
        return reels_schedule_publish_handler(update, context)
    elif data == "reels_execute_publish":
        return execute_reels_publish(update, context)
    elif data == "reels_back_to_features":
        return show_reels_features_menu(update, context)
    # Обработчики видимости удалены
    elif data.startswith("reels_select_music_"):
        # Выбор музыки
        try:
            track_index = int(data.split("_")[-1])
            tracks = context.user_data.get('found_tracks', [])
            if 0 <= track_index < len(tracks):
                selected_track = tracks[track_index]
                if 'reels_options' not in context.user_data:
                    context.user_data['reels_options'] = {}
                context.user_data['reels_options']['music_track'] = selected_track
                query.answer("✅ Музыка выбрана")
                return show_reels_features_menu(update, context)
        except (ValueError, IndexError):
            query.answer("❌ Ошибка выбора музыки")
        return show_reels_features_menu(update, context)
    elif data == "cancel_reels":
        query.answer()
        cleanup_reels_data(context)
        query.edit_message_text("❌ Публикация Reels отменена.")
        return ConversationHandler.END
    
    return ConversationHandler.END

def reels_add_usertags_handler(update, context):
    """Обработчик добавления пользовательских тегов к Reels"""
    query = update.callback_query
    query.answer()
    
    query.edit_message_text(
        "👥 *Отметка пользователей в Reels*\n\n"
        "Отправьте username пользователей через пробел (без @) или /skip для пропуска:\n\n"
        "Пример: username1 username2 username3\n\n"
        "⚠️ Максимум 5 пользователей"
    )
    
    from telegram_bot.states import REELS_ADD_USERTAGS
    return REELS_ADD_USERTAGS

def handle_reels_usertags_input(update, context):
    """Обработчик ввода пользовательских тегов для Reels"""
    if update.message.text == '/skip':
        if 'reels_options' not in context.user_data:
            context.user_data['reels_options'] = {}
        context.user_data['reels_options']['usertags'] = []
        update.message.reply_text("✅ Теги пользователей пропущены.")
    else:
        # Парсим username
        usertags = []
        usernames = update.message.text.replace('@', '').split()
        
        for i, username in enumerate(usernames[:5]):  # Максимум 5 тегов
            if username.strip():
                # Создаем тег с позицией (распределяем по вертикали)
                y_position = 0.2 + (i * 0.15)  # Начинаем с 20% и добавляем по 15%
                usertags.append({
                    'username': username.strip(),
                    'x': 0.5,  # По центру по X
                    'y': min(y_position, 0.8)  # Не выше 80%
                })
        
        if 'reels_options' not in context.user_data:
            context.user_data['reels_options'] = {}
        context.user_data['reels_options']['usertags'] = usertags
        update.message.reply_text(f"✅ Добавлено тегов пользователей: {len(usertags)}")
    
    return show_reels_features_menu_message(update, context)

def reels_add_location_handler(update, context):
    """Обработчик добавления локации к Reels"""
    query = update.callback_query
    query.answer()
    
    query.edit_message_text(
        "📍 *Добавление локации к Reels*\n\n"
        "Отправьте название места или координаты в формате \"широта,долгота\" или /skip для пропуска:\n\n"
        "Примеры:\n"
        "• Kiev\n"
        "• 50.4501,30.5234\n"
        "• Central Park"
    )
    
    from telegram_bot.states import REELS_ADD_LOCATION
    return REELS_ADD_LOCATION

def reels_add_music_handler(update, context):
    """Обработчик добавления музыки к Reels"""
    query = update.callback_query
    query.answer()
    
    query.edit_message_text(
        "🎵 *Добавление музыки к Reels*\n\n"
        "Отправьте название песни или исполнителя для поиска или /skip для пропуска:\n\n"
        "Пример: Shape of You Ed Sheeran"
    )
    
    from telegram_bot.states import REELS_ADD_MUSIC
    return REELS_ADD_MUSIC

def handle_reels_music_input(update, context):
    """Обработчик ввода музыки для Reels"""
    if update.message.text == '/skip':
        if 'reels_options' not in context.user_data:
            context.user_data['reels_options'] = {}
        context.user_data['reels_options']['music_track'] = None
        update.message.reply_text("✅ Музыка пропущена.")
        return show_reels_features_menu_message(update, context)
    
    # Поиск музыки
    query = update.message.text.strip()
    
    try:
        # Используем первый аккаунт для поиска музыки
        account_ids = context.user_data.get('publish_account_ids', [])
        if not account_ids:
            update.message.reply_text("❌ Ошибка: аккаунты не выбраны.")
            return show_reels_features_menu(update, context)
        
        from instagram.reels_manager import ReelsManager
        manager = ReelsManager(account_ids[0])
        tracks = manager.search_music(query, limit=5)
        
        if not tracks:
            update.message.reply_text("❌ Музыка не найдена. Попробуйте другой запрос.")
            from telegram_bot.states import REELS_ADD_MUSIC
            return REELS_ADD_MUSIC
        
        # Показываем найденные треки
        keyboard = []
        for i, track in enumerate(tracks):
            title = track.title if hasattr(track, 'title') else 'Unknown'
            artist = track.artist if hasattr(track, 'artist') else 'Unknown'
            keyboard.append([InlineKeyboardButton(
                f"🎵 {title} - {artist}",
                callback_data=f"reels_select_music_{i}"
            )])
        
        keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="reels_back_to_features")])
        
        # Сохраняем треки для выбора
        context.user_data['found_tracks'] = tracks
        
        update.message.reply_text(
            f"🎵 *Найдено треков: {len(tracks)}*\n\n"
            "Выберите трек:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        
        from telegram_bot.states import REELS_ADD_MUSIC
        return REELS_ADD_MUSIC
        
    except Exception as e:
        logger.error(f"Ошибка при поиске музыки: {e}")
        update.message.reply_text("❌ Ошибка при поиске музыки. Попробуйте еще раз.")
        from telegram_bot.states import REELS_ADD_MUSIC
        return REELS_ADD_MUSIC

def reels_choose_visibility_handler(update, context):
    """Обработчик выбора видимости Reels"""
    query = update.callback_query
    query.answer()
    
    keyboard = [
        [InlineKeyboardButton("📱 Только в Reels", callback_data="reels_hide_from_feed")],
        [InlineKeyboardButton("📱📸 В Reels + Ленте", callback_data="reels_show_in_feed")],
        [InlineKeyboardButton("🔙 Назад", callback_data="reels_back_to_features")]
    ]
    
    query.edit_message_text(
        "🔍 *Настройки видимости Reels*\n\n"
        "Выберите где будет отображаться ваш Reels:\n\n"
        "📱 *Только в Reels* - видео будет только в разделе Reels\n"
        "📱📸 *В Reels + Ленте* - видео будет в Reels и в основной ленте профиля",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    
    from telegram_bot.states import REELS_CHOOSE_VISIBILITY
    return REELS_CHOOSE_VISIBILITY

# Функция handle_reels_accounts_selected удалена - больше не нужна

# Добавляем функцию для показа меню через сообщение
def show_reels_features_menu_message(update, context):
    """Показывает меню дополнительных функций Reels через новое сообщение"""
    # Получаем данные
    account_ids = context.user_data.get('publish_account_ids', [])
    usernames = context.user_data.get('publish_account_usernames', [])
    caption = context.user_data.get('reels_caption', '')
    options = context.user_data.get('reels_options', {})
    
    # Формируем текст
    text = "�� *Настройки Reels*\n\n"
    
    # Аккаунты
    if len(account_ids) == 1:
        text += f"👤 Аккаунт: @{usernames[0]}\n"
    else:
        text += f"👥 Аккаунтов: {len(account_ids)}\n"
    
    # Описание
    text += f"📝 Описание: {caption[:30]}{'...' if len(caption) > 30 else caption}\n"
    
    # Хештеги
    hashtags = options.get('hashtags', [])
    text += f"🏷️ Хештеги: {len(hashtags)} шт.\n"
    
    # Теги пользователей
    usertags = options.get('usertags', [])
    if usertags:
        text += f"👥 Теги пользователей: ✅ ({len(usertags)})\n"
    else:
        text += f"👥 Теги пользователей: ❌ (0)\n"
    
    # Локация
    location = options.get('location')
    if location:
        text += f"📍 Локация: ✅\n"
    else:
        text += f"📍 Локация: ❌\n"
    
    # Музыка
    music_track = options.get('music_track')
    if music_track:
        text += f"🎵 Музыка: ✅\n"
    else:
        text += f"🎵 Музыка: ❌\n"
    
    # Обложка
    cover_time = options.get('cover_time', 0)
    if cover_time > 0:
        text += f"🖼️ Обложка: ✅\n"
    else:
        text += f"🖼️ Обложка: ❌\n"
    
    # Уникализация
    if len(account_ids) > 1:
        text += f"🎨 Уникализация: Включена\n"
    
    text += "\n*Дополнительные настройки:*"
    
    # Создаем клавиатуру
    keyboard = []
    
    # Первый ряд
    keyboard.append([
        InlineKeyboardButton("👥 Отметить пользователей", callback_data="reels_add_usertags"),
        InlineKeyboardButton("📍 Добавить локацию", callback_data="reels_add_location")
    ])
    
    # Второй ряд
    keyboard.append([
        InlineKeyboardButton("🖼️ Выбрать обложку", callback_data="reels_choose_cover")
    ])
    
    # Третий ряд удален - обложка теперь во втором ряду
    
    # Четвертый ряд - редактирование
    keyboard.append([
        InlineKeyboardButton("✏️ Изменить описание", callback_data="reels_edit_caption"),
        InlineKeyboardButton("🏷️ Изменить хештеги", callback_data="reels_edit_hashtags")
    ])
    
    # Пятый ряд - действия
    keyboard.append([
        InlineKeyboardButton("✅ Опубликовать", callback_data="reels_confirm_publish"),
        InlineKeyboardButton("⏰ Запланировать", callback_data="reels_schedule_publish")
    ])
    
    keyboard.append([InlineKeyboardButton("❌ Отмена", callback_data="cancel_reels")])
    
    from telegram import InlineKeyboardMarkup
    update.message.reply_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )
    
    from telegram_bot.states import REELS_ADD_FEATURES
    return REELS_ADD_FEATURES

def handle_reels_bulk_usertags(update, context):
    """Обработчик массовых пользовательских тегов"""
    if update.message.text == '/skip':
        if 'reels_options' not in context.user_data:
            context.user_data['reels_options'] = {}
        context.user_data['reels_options']['usertags'] = []
        update.message.reply_text("✅ Теги пользователей пропущены.")
        return show_reels_features_menu(update, context)
    
    # Проверяем, это файл или текст
    if update.message.document:
        # Обрабатываем файл
        try:
            file_id = update.message.document.file_id
            media = context.bot.get_file(file_id)
            
            # Создаем временный файл
            import tempfile
            with tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix='.txt') as temp_file:
                temp_path = temp_file.name
            
            # Скачиваем файл
            media.download(temp_path)
            
            # Читаем теги из файла
            with open(temp_path, 'r', encoding='utf-8') as f:
                usernames = [line.strip().replace('@', '') for line in f.readlines() if line.strip()]
            
            # Удаляем временный файл
            import os
            os.unlink(temp_path)
            
        except Exception as e:
            logger.error(f"Ошибка при обработке файла тегов: {e}")
            update.message.reply_text("❌ Ошибка при обработке файла. Попробуйте еще раз.")
            from telegram_bot.states import REELS_BULK_USERTAGS
            return REELS_BULK_USERTAGS
    else:
        # Обрабатываем текст
        text = update.message.text.strip()
        usernames = [username.strip().replace('@', '') for username in text.split('\n') if username.strip()]
    
    if not usernames:
        update.message.reply_text("❌ Не найдено ни одного пользователя. Попробуйте еще раз.")
        from telegram_bot.states import REELS_BULK_USERTAGS
        return REELS_BULK_USERTAGS
    
    # Получаем выбранные аккаунты
    account_ids = context.user_data.get('publish_account_ids', [])
    if not account_ids:
        update.message.reply_text("❌ Ошибка: аккаунты не выбраны.")
        return show_reels_features_menu(update, context)
    
    # Распределяем теги между аккаунтами
    max_tags_per_account = 5
    total_slots = len(account_ids) * max_tags_per_account
    
    if len(usernames) > total_slots:
        # Если тегов больше чем слотов, берем только первые и сообщаем об излишках
        used_usernames = usernames[:total_slots]
        excess_usernames = usernames[total_slots:]
        
        # Формируем список излишков
        excess_text = "\n".join([f"@{username}" for username in excess_usernames])
        
        update.message.reply_text(
            f"⚠️ Слишком много тегов!\n\n"
            f"📊 Максимум: {len(account_ids)} аккаунтов × {max_tags_per_account} тегов = {total_slots} тегов\n"
            f"📥 Получено: {len(usernames)} тегов\n"
            f"✅ Использовано: {len(used_usernames)} тегов\n\n"
            f"❌ Не использованы ({len(excess_usernames)}):\n{excess_text}"
        )
        
        usernames = used_usernames
    else:
        update.message.reply_text(
            f"✅ Получено {len(usernames)} тегов для {len(account_ids)} аккаунтов"
        )
    
    # Распределяем теги равномерно между аккаунтами
    distributed_tags = []
    for i, account_id in enumerate(account_ids):
        # Вычисляем диапазон тегов для каждого аккаунта
        start_idx = i * max_tags_per_account
        end_idx = min(start_idx + max_tags_per_account, len(usernames))
        
        account_tags = usernames[start_idx:end_idx]
        
        # Создаем теги с позициями (равномерно распределенными)
        account_usertags = []
        for j, username in enumerate(account_tags):
            # Распределяем позиции равномерно по экрану
            x = 0.2 + (j % 3) * 0.3  # 0.2, 0.5, 0.8
            y = 0.2 + (j // 3) * 0.3  # 0.2, 0.5, 0.8
            
            account_usertags.append({
                'username': username,
                'x': x,
                'y': y
            })
        
        distributed_tags.append({
            'account_id': account_id,
            'tags': account_usertags
        })
    
    # Сохраняем распределенные теги
    if 'reels_options' not in context.user_data:
        context.user_data['reels_options'] = {}
    context.user_data['reels_options']['distributed_usertags'] = distributed_tags
    
    # Показываем распределение
    distribution_text = "📊 *Распределение тегов по аккаунтам:*\n\n"
    
    from database.db_manager import get_instagram_account
    for item in distributed_tags:
        account = get_instagram_account(item['account_id'])
        if account:
            distribution_text += f"👤 *@{account.username}*: {len(item['tags'])} тегов\n"
            for tag in item['tags']:
                distribution_text += f"   • @{tag['username']}\n"
            distribution_text += "\n"
    
    update.message.reply_text(distribution_text, parse_mode='Markdown')
    
    return show_reels_features_menu(update, context)

def start_schedule_post(update, context):
    """Начинает процесс планирования поста используя существующий механизм публикации"""
    # Устанавливаем флаг что это запланированный пост
    context.user_data['is_scheduled_post'] = True
    context.user_data['publish_type'] = 'post'  # Используем обычный тип поста
    context.user_data['publish_media_type'] = 'PHOTO'
    
    # Используем существующий механизм выбора аккаунтов для постов
    return start_post_publish(update, context)

# Добавляем функцию планирования для Stories
def story_schedule_publish_handler(update, context):
    """Обработчик планирования публикации истории"""
    query = update.callback_query
    query.answer()
    
    # Проверяем, что аккаунты уже выбраны
    account_ids = context.user_data.get('publish_account_ids', [])
    if not account_ids:
        query.edit_message_text("❌ Ошибка: аккаунты не выбраны.")
        return ConversationHandler.END
    
    # Устанавливаем флаг что это запланированная публикация
    context.user_data['is_scheduled_post'] = True
    context.user_data['publish_type'] = 'story'
    
    # Переходим сразу к выбору времени
    query.edit_message_text(
        "🗓️ *Планирование публикации истории*\n\n"
        "Введите дату и время публикации в формате:\n"
        "`ДД.ММ.ГГГГ ЧЧ:ММ`\n\n"
        "Примеры:\n"
        "• `10.07.2025 14:30`\n"
        "• `15.07.2025 09:00`\n\n"
        "Время указывается в московском часовом поясе.",
        parse_mode='Markdown'
    )
    
    return CHOOSE_SCHEDULE

# Добавляем функцию планирования для Reels  
def reels_schedule_publish_handler(update, context):
    """Обработчик планирования публикации Reels"""
    query = update.callback_query
    query.answer()
    
    # Проверяем, что аккаунты уже выбраны
    account_ids = context.user_data.get('publish_account_ids', [])
    if not account_ids:
        query.edit_message_text("❌ Ошибка: аккаунты не выбраны.")
        return ConversationHandler.END
    
    # Устанавливаем флаг что это запланированная публикация
    context.user_data['is_scheduled_post'] = True
    context.user_data['publish_type'] = 'reels'
    
    # Переходим сразу к выбору времени
    query.edit_message_text(
        "🗓️ *Планирование публикации Reels*\n\n"
        "Введите дату и время публикации в формате:\n"
        "`ДД.ММ.ГГГГ ЧЧ:ММ`\n\n"
        "Примеры:\n"
        "• `10.07.2025 14:30`\n"
        "• `15.07.2025 09:00`\n\n"
        "Время указывается в московском часовом поясе.",
        parse_mode='Markdown'
    )
    
    return CHOOSE_SCHEDULE
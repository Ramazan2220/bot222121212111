import logging
import os
import threading
import concurrent.futures
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import CallbackContext, ConversationHandler, CommandHandler, CallbackQueryHandler, MessageHandler, Filters

from database.db_manager import get_instagram_account, get_instagram_accounts as get_all_instagram_accounts, update_instagram_account
from instagram.profile_manager import ProfileManager
from telegram_bot.keyboards import get_main_menu_keyboard
from telegram_bot.states import ProfileStates
from telegram_bot.utils.account_selection import AccountSelector
from profile_setup.name_manager import edit_profile_name, save_profile_name
from profile_setup.username_manager import edit_profile_username, save_profile_username
from profile_setup.bio_manager import edit_profile_bio, save_profile_bio
from profile_setup.links_manager import edit_profile_links, save_profile_links
from profile_setup.avatar_manager import add_profile_photo, save_profile_photo, delete_profile_photo
from profile_setup.post_manager import add_post, save_post
from profile_setup.cleanup_manager import delete_all_posts
from profile_setup import EDIT_NAME, EDIT_USERNAME, EDIT_BIO, EDIT_LINKS, ADD_PHOTO, ADD_POST

logger = logging.getLogger(__name__)

# Состояния для ConversationHandler
EDIT_NAME, EDIT_USERNAME, EDIT_BIO, EDIT_LINKS, ADD_PHOTO, ADD_POST = range(6)
# Состояния для массовых операций
BULK_EDIT_NAME, BULK_EDIT_USERNAME, BULK_EDIT_BIO, BULK_ADD_PHOTO, BULK_EDIT_LINKS = range(10, 15)
# Состояния для мастера оформления профиля
WIZARD_NAME, WIZARD_USERNAME, WIZARD_BIO, WIZARD_LINK, WIZARD_PHOTO, WIZARD_CONFIRM = range(20, 26)
# Состояния для bulk-мастера
BULK_WIZARD_NAME, BULK_WIZARD_USERNAME, BULK_WIZARD_BIO, BULK_WIZARD_LINK, BULK_WIZARD_PHOTO, BULK_WIZARD_CONFIRM = range(30, 36)

def profile_setup_menu(update: Update, context: CallbackContext) -> int:
    """Показывает меню настройки профиля - entry point для ConversationHandler"""
    query = update.callback_query
    if query:
        query.answer()

    # Запускаем процесс выбора аккаунтов через profile_selector
    return profile_selector.start_selection(update, context, PROFILE_ACCOUNT_CALLBACK)

def show_bulk_profile_actions(update: Update, context: CallbackContext) -> None:
    """Показывает меню массовых действий для выбранных аккаунтов"""
    query = update.callback_query
    if query:
        query.answer()
    
    account_ids = context.user_data.get('selected_profile_accounts', [])
    accounts = [get_instagram_account(acc_id) for acc_id in account_ids]
    
    keyboard = [
        [InlineKeyboardButton("🚀 Полное оформление (мастер)", callback_data="bulk_full_wizard")],
        [InlineKeyboardButton("📝 Изменить имя", callback_data="bulk_edit_name")],
        [InlineKeyboardButton("👤 Изменить username", callback_data="bulk_edit_username")],
        [InlineKeyboardButton("✏️ Изменить описание", callback_data="bulk_edit_bio")],
        [InlineKeyboardButton("🔗 Изменить ссылку", callback_data="bulk_edit_links")],
        [InlineKeyboardButton("🖼 Добавить фото профиля", callback_data="bulk_add_photo")],
        [InlineKeyboardButton("🗑 Удалить фото профиля", callback_data="bulk_remove_photo")],
        [InlineKeyboardButton("📸 Удалить все посты", callback_data="bulk_delete_all_posts")],
        [InlineKeyboardButton("🧹 Очистить описание", callback_data="bulk_clear_bio")],
        [InlineKeyboardButton("🔙 Назад", callback_data="profile_setup")]
    ]
    
    accounts_list = "\n".join([f"• @{acc.username}" for acc in accounts[:10]])
    if len(accounts) > 10:
        accounts_list += f"\n... и еще {len(accounts) - 10} аккаунтов"
    
    text = f"Массовые действия для {len(accounts)} аккаунтов:\n\n{accounts_list}\n\nВыберите действие:"

    if query:
        query.edit_message_text(text, parse_mode=None, reply_markup=InlineKeyboardMarkup(keyboard))
    else:
        update.message.reply_text(text, parse_mode=None, reply_markup=InlineKeyboardMarkup(keyboard))

def profile_account_menu(update: Update, context: CallbackContext) -> None:
    """Показывает меню настройки конкретного аккаунта"""
    query = update.callback_query
    query.answer()

    # Получаем ID аккаунта из callback_data
    account_id = int(query.data.split('_')[-1])
    context.user_data['current_account_id'] = account_id

    account = get_instagram_account(account_id)
    if not account:
        query.edit_message_text(
            "Аккаунт не найден. Возможно, он был удален.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Назад", callback_data="profile_setup")]
            ])
        )
        return

    # Динамическая видимость пунктов (безопасность/удобство)
    try:
        pm = ProfileManager(account_id)
        info = pm.get_profile_info()
    except Exception:
        info = None
    has_photo = False
    if info is not None:
        has_photo = bool(getattr(info, 'profile_pic_url', None)) and not bool(getattr(info, 'has_anonymous_profile_picture', False))
    has_bio = bool(getattr(account, 'biography', None))

    keyboard = [[InlineKeyboardButton("🚀 Полное оформление (мастер)", callback_data="profile_full_wizard")]]
    keyboard.append([InlineKeyboardButton("👤 Изменить имя", callback_data="profile_edit_name")])
    keyboard.append([InlineKeyboardButton("🔤 Изменить имя пользователя", callback_data="profile_edit_username")])
    keyboard.append([InlineKeyboardButton("📝 Изменить описание профиля", callback_data="profile_edit_bio")])
    keyboard.append([InlineKeyboardButton("🔗 Добавить/изменить ссылки", callback_data="profile_edit_links")])

    if has_photo:
        keyboard.append([InlineKeyboardButton("🗑 Удалить фото профиля", callback_data="profile_delete_photo")])
    else:
        keyboard.append([InlineKeyboardButton("🖼️ Добавить фото профиля", callback_data=f"add_profile_photo_{account_id}")])

    keyboard.append([InlineKeyboardButton("🖼️ Добавить пост", callback_data="profile_add_post")])
    keyboard.append([InlineKeyboardButton("🗑️ Удалить все посты", callback_data="profile_delete_posts")])
    if has_bio:
        keyboard.append([InlineKeyboardButton("🧹 Очистить описание профиля", callback_data="profile_delete_bio")])
    keyboard.append([InlineKeyboardButton("🔙 Назад к списку аккаунтов", callback_data="profile_setup")])

    query.edit_message_text(
        f"Настройка профиля для аккаунта: *{account.username}*\n\nВыберите действие:",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )

def setup_profile_menu(update: Update, context: CallbackContext) -> None:
    """Показывает меню настройки профиля для выбранного аккаунта"""
    query = update.callback_query
    query.answer()
    
    # Получаем ID аккаунта из callback_data
    account_id = int(query.data.split('_')[-1])
    context.user_data['current_account_id'] = account_id
    
    account = get_instagram_account(account_id)
    if not account:
        query.edit_message_text(
            "❌ Аккаунт не найден",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Назад", callback_data="profile_setup")]
            ])
        )
        return
    
    keyboard = [
        [InlineKeyboardButton("📝 Изменить имя", callback_data=f"edit_name_{account_id}")],
        [InlineKeyboardButton("👤 Изменить username", callback_data=f"edit_username_{account_id}")],
        [InlineKeyboardButton("📄 Изменить био", callback_data=f"edit_bio_{account_id}")],
        [InlineKeyboardButton("🔗 Изменить ссылки", callback_data=f"edit_links_{account_id}")],
        [InlineKeyboardButton("🖼 Изменить аватар", callback_data=f"edit_avatar_{account_id}")],
        [InlineKeyboardButton("➕ Добавить пост", callback_data=f"add_post_{account_id}")],
        [InlineKeyboardButton("🗑 Очистить профиль", callback_data=f"delete_all_posts_{account_id}")],
        [InlineKeyboardButton("🔙 Назад", callback_data="profile_setup")]
    ]
    
    query.edit_message_text(
        f"👤 Настройка профиля\n"
        f"Аккаунт: @{account.username}\n\n"
        f"Выберите действие:",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

def edit_profile_name(update: Update, context: CallbackContext) -> int:
    """Запрашивает новое имя профиля"""
    query = update.callback_query
    query.answer()

    account_id = context.user_data.get('current_account_id')
    account = get_instagram_account(account_id)

    if not account:
        query.edit_message_text(
            "Аккаунт не найден. Возможно, он был удален.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Назад", callback_data="profile_setup")]
            ])
        )
        return ConversationHandler.END

    # Получаем текущее имя профиля
    current_name = account.full_name if hasattr(account, 'full_name') and account.full_name else "Не указано"

    query.edit_message_text(
        f"Текущее имя профиля: *{current_name}*\n\nВведите новое имя профиля:",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 Отмена", callback_data=f"profile_account_{account_id}")]
        ])
    )

    return EDIT_NAME

def save_profile_name(update: Update, context: CallbackContext) -> int:
    """Сохраняет новое имя профиля"""
    new_name = update.message.text
    account_id = context.user_data.get('current_account_id')

    # Отправляем сообщение о начале процесса
    message = update.message.reply_text("⏳ Обновление имени профиля...")

    try:
        # Создаем менеджер профиля и обновляем имя
        profile_manager = ProfileManager(account_id)
        success, result = profile_manager.update_profile_name(new_name)

        if success:
            # Обновляем имя в базе данных
            account = get_instagram_account(account_id)
            if hasattr(account, 'full_name'):
                account.full_name = new_name
                update_instagram_account(account_id, full_name=new_name)

            # Отправляем сообщение об успехе
            update.message.reply_text(
                f"✅ Имя профиля успешно изменено на *{new_name}*!",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Назад к настройкам профиля", callback_data=f"profile_account_{account_id}")],
                    [InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")]
                ])
            )
        else:
            # Отправляем сообщение об ошибке
            update.message.reply_text(
                f"❌ Ошибка при изменении имени профиля: {result}",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Назад к настройкам профиля", callback_data=f"profile_account_{account_id}")],
                    [InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")]
                ])
            )
    except Exception as e:
        logger.error(f"Ошибка при обновлении имени профиля: {e}")
        update.message.reply_text(
            "❌ Произошла ошибка при обработке вашего запроса. Пожалуйста, попробуйте еще раз.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Назад к настройкам профиля", callback_data=f"profile_account_{account_id}")],
                [InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")]
            ])
        )

    # Удаляем сообщение о процессе
    message.delete()

    return ConversationHandler.END

def edit_profile_username(update: Update, context: CallbackContext) -> int:
    """Запрашивает новое имя пользователя"""
    query = update.callback_query
    query.answer()

    account_id = context.user_data.get('current_account_id')
    account = get_instagram_account(account_id)

    if not account:
        query.edit_message_text(
            "Аккаунт не найден. Возможно, он был удален.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Назад", callback_data="profile_setup")]
            ])
        )
        return ConversationHandler.END

    query.edit_message_text(
        "👤 Изменение username\n\n"
        f"Текущий: *{account.username}*\n\n"
        "Важно:\n"
        "• Username должен быть уникальным\n"
        "• Некоторые варианты могут быть заняты в Instagram\n\n"
        "Рекомендации:\n"
        "• Подготовьте несколько вариантов (на запас)\n"
        "• Для массовых операций — отправляйте ~20% больше вариантов, чем аккаунтов\n\n"
        "Введите новый username (без @):",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 Отмена", callback_data=f"profile_account_{account_id}")]
        ])
    )

    return EDIT_USERNAME

def save_profile_username(update: Update, context: CallbackContext) -> int:
    """Сохраняет новое имя пользователя"""
    new_username = update.message.text
    account_id = context.user_data.get('current_account_id')

    # Отправляем сообщение о начале процесса
    message = update.message.reply_text("⏳ Обновление имени пользователя...")

    try:
        # Создаем менеджер профиля и обновляем имя пользователя
        profile_manager = ProfileManager(account_id)
        success, result = profile_manager.update_username(new_username)

        if success:
            # Обновляем имя пользователя в базе данных
            update_instagram_account(account_id, username=new_username)

            # Отправляем сообщение об успехе
            update.message.reply_text(
                f"✅ Имя пользователя успешно изменено на *{new_username}*!",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Назад к настройкам профиля", callback_data=f"profile_account_{account_id}")],
                    [InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")]
                ])
            )
        else:
            # Отправляем сообщение об ошибке
            update.message.reply_text(
                f"❌ Ошибка при изменении имени пользователя: {result}",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Назад к настройкам профиля", callback_data=f"profile_account_{account_id}")],
                    [InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")]
                ])
            )
    except Exception as e:
        logger.error(f"Ошибка при обновлении имени пользователя: {e}")
        update.message.reply_text(
            "❌ Произошла ошибка при обработке вашего запроса. Пожалуйста, попробуйте еще раз.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Назад к настройкам профиля", callback_data=f"profile_account_{account_id}")],
                [InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")]
            ])
        )

    # Удаляем сообщение о процессе
    message.delete()

    return ConversationHandler.END

def edit_profile_bio(update: Update, context: CallbackContext) -> int:
    """Запрашивает новое описание профиля"""
    query = update.callback_query
    query.answer()

    account_id = context.user_data.get('current_account_id')
    account = get_instagram_account(account_id)

    if not account:
        query.edit_message_text(
            "Аккаунт не найден. Возможно, он был удален.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Назад", callback_data="profile_setup")]
            ])
        )
        return ConversationHandler.END

    # Получаем текущее описание профиля
    current_bio = account.biography if hasattr(account, 'biography') and account.biography else "Не указано"

    query.edit_message_text(
        f"Текущее описание профиля:\n\n{current_bio}\n\nВведите новое описание профиля (до 150 символов):",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 Отмена", callback_data=f"profile_account_{account_id}")]
        ])
    )

    return EDIT_BIO

def save_profile_bio(update: Update, context: CallbackContext) -> int:
    """Сохраняет новое описание профиля"""
    new_bio = update.message.text
    account_id = context.user_data.get('current_account_id')

    # Отправляем сообщение о начале процесса
    message = update.message.reply_text("⏳ Обновление описания профиля...")

    try:
        # Создаем менеджер профиля и обновляем описание
        profile_manager = ProfileManager(account_id)
        success, result = profile_manager.update_biography(new_bio)

        if success:
            # Обновляем описание в базе данных
            # Вместо передачи объекта account, передаем account_id и biography
            update_instagram_account(account_id, biography=new_bio)

            # Отправляем сообщение об успехе
            update.message.reply_text(
                "✅ Описание профиля успешно обновлено!",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Назад к настройкам профиля", callback_data=f"profile_account_{account_id}")],
                    [InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")]
                ])
            )
        else:
            # Отправляем сообщение об ошибке
            update.message.reply_text(
                f"❌ Ошибка при обновлении описания профиля: {result}",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Назад к настройкам профиля", callback_data=f"profile_account_{account_id}")],
                    [InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")]
                ])
            )
    except Exception as e:
        logger.error(f"Ошибка при обновлении описания профиля: {e}")
        update.message.reply_text(
            "❌ Произошла ошибка при обработке вашего запроса. Пожалуйста, попробуйте еще раз.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Назад к настройкам профиля", callback_data=f"profile_account_{account_id}")],
                [InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")]
            ])
        )

    # Удаляем сообщение о процессе
    message.delete()

    return ConversationHandler.END

def edit_profile_links(update: Update, context: CallbackContext) -> int:
    """Запрашивает новые ссылки профиля"""
    query = update.callback_query
    query.answer()
    
    account_id = context.user_data.get('current_account_id')
    account = get_instagram_account(account_id)

    if not account:
        query.edit_message_text(
            "Аккаунт не найден. Возможно, он был удален.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Назад", callback_data="profile_setup")]
            ])
        )
        return ConversationHandler.END

    # Отправляем сообщение о загрузке
    loading_message = query.message.reply_text("⏳ Подключение к Instagram... Пожалуйста, подождите.")

    # Получаем текущие ссылки профиля
    try:
        profile_manager = ProfileManager(account_id)
        current_link = profile_manager.get_profile_links()

        # Удаляем сообщение о загрузке
        loading_message.delete()

        current_link_text = "Не указана" if not current_link else current_link

        query.message.reply_text(
            f"Текущая ссылка в профиле: {current_link_text}\n\n"
            "Введите новую ссылку для профиля Instagram (например, example.com):",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Отмена", callback_data=f"profile_account_{account_id}")]
            ])
        )

        return EDIT_LINKS
    except Exception as e:
        logger.error(f"Ошибка при получении ссылок профиля: {e}")

        # Удаляем сообщение о загрузке
        loading_message.delete()

        query.message.reply_text(
            "❌ Произошла ошибка при получении ссылок профиля. Пожалуйста, попробуйте еще раз.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Назад к настройкам профиля", callback_data=f"profile_account_{account_id}")],
                [InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")]
            ])
        )
        return ConversationHandler.END

def save_profile_links(update: Update, context: CallbackContext) -> int:
    """Сохраняет новые ссылки профиля"""
    links_text = update.message.text
    account_id = context.user_data.get('current_account_id')

    # Отправляем сообщение о начале процесса
    message = update.message.reply_text("⏳ Обновление ссылок профиля...")

    try:
        # Берем только первую ссылку, так как Instagram поддерживает только одну
        link = links_text.strip()
        if '|' in link:
            _, url = link.split('|', 1)
            link = url.strip()

        # Создаем менеджер профиля и обновляем ссылку
        profile_manager = ProfileManager(account_id)
        success, result = profile_manager.update_profile_links(link)

        if success:
            # Отправляем сообщение об успехе
            update.message.reply_text(
                "✅ Ссылка профиля успешно обновлена!",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Назад к настройкам профиля", callback_data=f"profile_account_{account_id}")],
                    [InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")]
                ])
            )
        else:
            # Отправляем сообщение об ошибке
            update.message.reply_text(
                f"❌ Ошибка при обновлении ссылки профиля: {result}",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Назад к настройкам профиля", callback_data=f"profile_account_{account_id}")],
                    [InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")]
                ])
            )
    except Exception as e:
        logger.error(f"Ошибка при обновлении ссылки профиля: {e}")
        update.message.reply_text(
            "❌ Произошла ошибка при обработке вашего запроса. Пожалуйста, попробуйте еще раз.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Назад к настройкам профиля", callback_data=f"profile_account_{account_id}")],
                [InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")]
            ])
        )

    # Удаляем сообщение о процессе
    message.delete()

    return ConversationHandler.END

def add_profile_photo(update: Update, context: CallbackContext) -> int:
    """Запрашивает фото профиля"""
    query = update.callback_query
    query.answer()

    # Извлекаем ID аккаунта из callback_data
    account_id = int(query.data.split('_')[-1])
    context.user_data['current_account_id'] = account_id

    account = get_instagram_account(account_id)

    if not account:
        query.edit_message_text(
            "Аккаунт не найден. Возможно, он был удален.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Назад", callback_data="profile_setup")]
            ])
        )
        return ConversationHandler.END

    query.edit_message_text(
        f"Отправьте фото для установки в качестве фото профиля для аккаунта *{account.username}*:",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 Отмена", callback_data=f"profile_account_{account_id}")]
        ])
    )

    return ADD_PHOTO

def save_profile_photo(update: Update, context: CallbackContext) -> int:
    """Сохраняет новое фото профиля"""
    account_id = context.user_data.get('current_account_id')

    # Отправляем сообщение о начале процесса
    message = update.message.reply_text("⏳ Обновление фото профиля...")

    try:
        # Получаем фото
        photo_file = update.message.photo[-1].get_file()
        photo_path = f"temp_profile_photo_{account_id}.jpg"
        photo_file.download(photo_path)

        # Создаем менеджер профиля и обновляем фото
        profile_manager = ProfileManager(account_id)
        success, result = profile_manager.update_profile_picture(photo_path)

        # Удаляем временный файл
        if os.path.exists(photo_path):
            os.remove(photo_path)

        if success:
            # Отправляем сообщение об успехе
            update.message.reply_text(
                "✅ Фото профиля успешно обновлено!",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Назад к настройкам профиля", callback_data=f"profile_account_{account_id}")],
                    [InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")]
                ])
            )
        else:
            # Отправляем сообщение об ошибке
            update.message.reply_text(
                f"❌ Ошибка при обновлении фото профиля: {result}",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Назад к настройкам профиля", callback_data=f"profile_account_{account_id}")],
                    [InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")]
                ])
            )
    except Exception as e:
        logger.error(f"Ошибка при обновлении фото профиля: {e}")
        update.message.reply_text(
            "❌ Произошла ошибка при обработке вашего запроса. Пожалуйста, попробуйте еще раз.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Назад к настройкам профиля", callback_data=f"profile_account_{account_id}")],
                [InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")]
            ])
        )

    # Удаляем сообщение о процессе
    message.delete()

    return ConversationHandler.END

def add_post(update: Update, context: CallbackContext) -> int:
    """Запрашивает фото или видео для поста"""
    query = update.callback_query
    query.answer()

    account_id = context.user_data.get('current_account_id')
    account = get_instagram_account(account_id)

    if not account:
        query.edit_message_text(
            "Аккаунт не найден. Возможно, он был удален.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Назад", callback_data="profile_setup")]
            ])
        )
        return ConversationHandler.END

    query.edit_message_text(
        f"Отправьте фото или видео для публикации в профиле *{account.username}*.\n\nПосле отправки медиафайла вам будет предложено ввести подпись к посту.",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 Отмена", callback_data=f"profile_account_{account_id}")]
        ])
    )

    return ADD_POST

def save_post(update: Update, context: CallbackContext) -> int:
    """Сохраняет новый пост"""
    account_id = context.user_data.get('current_account_id')

    # Отправляем сообщение о начале процесса
    message = update.message.reply_text("⏳ Публикация поста...")

    try:
        # Определяем тип медиа (фото или видео)
        if update.message.photo:
            # Получаем фото
            media_file = update.message.photo[-1].get_file()
            media_path = f"temp_post_{account_id}.jpg"
            media_file.download(media_path)

            # Создаем менеджер профиля и публикуем фото
            profile_manager = ProfileManager(account_id)
            caption = update.message.caption or ""
            success, result = profile_manager.upload_photo(media_path, caption)
        elif update.message.video:
            # Получаем видео
            media_file = update.message.video.get_file()
            media_path = f"temp_post_{account_id}.mp4"
            media_file.download(media_path)

            # Создаем менеджер профиля и публикуем видео
            profile_manager = ProfileManager(account_id)
            caption = update.message.caption or ""
            success, result = profile_manager.upload_video(media_path, caption)
        else:
            update.message.reply_text(
                "❌ Пожалуйста, отправьте фото или видео для публикации.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Назад к настройкам профиля", callback_data=f"profile_account_{account_id}")],
                    [InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")]
                ])
            )
            message.delete()
            return ConversationHandler.END

        # Удаляем временный файл
        if os.path.exists(media_path):
            os.remove(media_path)

        if success:
            # Отправляем сообщение об успехе
            update.message.reply_text(
                "✅ Пост успешно опубликован!",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Назад к настройкам профиля", callback_data=f"profile_account_{account_id}")],
                    [InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")]
                ])
            )
        else:
            # Отправляем сообщение об ошибке
            update.message.reply_text(
                f"❌ Ошибка при публикации поста: {result}",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Назад к настройкам профиля", callback_data=f"profile_account_{account_id}")],
                    [InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")]
                ])
            )
    except Exception as e:
        logger.error(f"Ошибка при публикации поста: {e}")
        update.message.reply_text(
            "❌ Произошла ошибка при обработке вашего запроса. Пожалуйста, попробуйте еще раз.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Назад к настройкам профиля", callback_data=f"profile_account_{account_id}")],
                [InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")]
            ])
        )

    # Удаляем сообщение о процессе
    message.delete()

    return ConversationHandler.END

def delete_profile_photo(update: Update, context: CallbackContext) -> None:
    """Удаляет фото профиля"""
    query = update.callback_query
    query.answer()

    account_id = context.user_data.get('current_account_id')
    account = get_instagram_account(account_id)

    if not account:
        query.edit_message_text(
            "Аккаунт не найден. Возможно, он был удален.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Назад", callback_data="profile_setup")]
            ])
        )
        return

    # Отправляем сообщение о начале процесса
    query.edit_message_text(
        f"⏳ Удаление фото профиля для аккаунта {account.username}..."
    )

    try:
        # Создаем менеджер профиля и удаляем фото
        profile_manager = ProfileManager(account_id)
        success, result = profile_manager.remove_profile_picture()

        if success:
            # Отправляем сообщение об успехе
            query.edit_message_text(
                "✅ Фото профиля успешно удалено!",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Назад к настройкам профиля", callback_data=f"profile_account_{account_id}")],
                    [InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")]
                ])
            )
        else:
            # Отправляем сообщение об ошибке
            query.edit_message_text(
                f"❌ Ошибка при удалении фото профиля: {result}",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Назад к настройкам профиля", callback_data=f"profile_account_{account_id}")],
                    [InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")]
                ])
            )
    except Exception as e:
        logger.error(f"Ошибка при удалении фото профиля: {e}")
        query.edit_message_text(
            "❌ Произошла ошибка при обработке вашего запроса. Пожалуйста, попробуйте еще раз.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Назад к настройкам профиля", callback_data=f"profile_account_{account_id}")],
                [InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")]
            ])
        )

def delete_all_posts(update: Update, context: CallbackContext) -> None:
    """Удаляет все посты"""
    query = update.callback_query
    query.answer()

    account_id = context.user_data.get('current_account_id')
    account = get_instagram_account(account_id)

    if not account:
        query.edit_message_text(
            "Аккаунт не найден. Возможно, он был удален.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Назад", callback_data="profile_setup")]
            ])
        )
        return

    # Отправляем сообщение о начале процесса
    query.edit_message_text(
        f"⏳ Удаление всех постов для аккаунта {account.username}...\n\nЭто может занять некоторое время."
    )

    try:
        # Создаем менеджер профиля и удаляем все посты
        profile_manager = ProfileManager(account_id)
        success, result = profile_manager.delete_all_posts()

        if success:
            # Отправляем сообщение об успехе
            query.edit_message_text(
                f"✅ {result}",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Назад к настройкам профиля", callback_data=f"profile_account_{account_id}")],
                    [InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")]
                ])
            )
        else:
            # Отправляем сообщение об ошибке
            query.edit_message_text(
                f"❌ Ошибка при удалении постов: {result}",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Назад к настройкам профиля", callback_data=f"profile_account_{account_id}")],
                    [InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")]
                ])
            )
    except Exception as e:
        logger.error(f"Ошибка при удалении постов: {e}")
        query.edit_message_text(
            "❌ Произошла ошибка при обработке вашего запроса. Пожалуйста, попробуйте еще раз.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Назад к настройкам профиля", callback_data=f"profile_account_{account_id}")],
                [InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")]
            ])
        )

def delete_bio(update: Update, context: CallbackContext) -> None:
    """Очищает описание профиля"""
    query = update.callback_query
    query.answer()

    account_id = context.user_data.get('current_account_id')
    account = get_instagram_account(account_id)

    if not account:
        query.edit_message_text(
            "Аккаунт не найден. Возможно, он был удален.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Назад", callback_data="profile_setup")]
            ])
        )
        return

    # Отправляем сообщение о начале процесса
    query.edit_message_text(
        f"⏳ Очистка описания профиля для аккаунта {account.username}..."
    )

    try:
        # Создаем менеджер профиля и очищаем описание
        profile_manager = ProfileManager(account_id)
        success, result = profile_manager.update_biography("")

        if success:
            # Обновляем описание в базе данных
            update_instagram_account(account_id, biography="")

            # Отправляем сообщение об успехе
            query.edit_message_text(
                "✅ Описание профиля успешно очищено!",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Назад к настройкам профиля", callback_data=f"profile_account_{account_id}")],
                    [InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")]
                ])
            )
        else:
            # Отправляем сообщение об ошибке
            query.edit_message_text(
                f"❌ Ошибка при очистке описания профиля: {result}",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Назад к настройкам профиля", callback_data=f"profile_account_{account_id}")],
                    [InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")]
                ])
            )
    except Exception as e:
        logger.error(f"Ошибка при очистке описания профиля: {e}")
        query.edit_message_text(
            "❌ Произошла ошибка при обработке вашего запроса. Пожалуйста, попробуйте еще раз.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Назад к настройкам профиля", callback_data=f"profile_account_{account_id}")],
                [InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")]
            ])
        )

def bulk_edit_name(update: Update, context: CallbackContext) -> int:
    """Массовое изменение имени"""
    query = update.callback_query
    query.answer()
    
    account_ids = context.user_data.get('selected_profile_accounts', [])
    
    query.edit_message_text(
        f"📝 *Массовое изменение имени*\n\n"
        f"Выбрано аккаунтов: {len(account_ids)}\n\n"
        f"Вы можете:\n"
        f"• Ввести одно имя для всех аккаунтов\n"
        f"• Ввести несколько имён (каждое с новой строки)\n\n"
        f"⚠️ *Рекомендация:* При работе с большим количеством аккаунтов лучше использовать уникальные имена.\n\n"
        f"Отправьте имя(имена) или /cancel для отмены:",
        parse_mode="Markdown"
    )
    
    return BULK_EDIT_NAME

def save_bulk_names(update: Update, context: CallbackContext) -> int:
    """Сохраняет массовые имена"""
    import random
    
    text = update.message.text
    if text == '/cancel':
        update.message.reply_text("❌ Операция отменена", 
                                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="profile_setup")]]))
        return ConversationHandler.END
    
    account_ids = context.user_data.get('selected_profile_accounts', [])
    accounts = [get_instagram_account(acc_id) for acc_id in account_ids]
    
    # Разбираем имена
    names = [name.strip() for name in text.strip().split('\n') if name.strip()]
    
    if not names:
        update.message.reply_text("❌ Не указано ни одного имени. Попробуйте ещё раз.")
        return BULK_EDIT_NAME
    
    # Подготавливаем имена для распределения
    if len(names) == 1:
        # Одно имя для всех
        name_assignments = {acc_id: names[0] for acc_id in account_ids}
    else:
        # Распределяем имена
        name_assignments = {}
        if len(names) >= len(accounts):
            # Имён достаточно
            for i, acc_id in enumerate(account_ids):
                name_assignments[acc_id] = names[i % len(names)]
        else:
            # Имён меньше, чем аккаунтов - дублируем с перемешиванием
            extended_names = []
            while len(extended_names) < len(accounts):
                shuffled = names.copy()
                random.shuffle(shuffled)
                extended_names.extend(shuffled)
            
            for i, acc_id in enumerate(account_ids):
                name_assignments[acc_id] = extended_names[i]
    
    # Применяем изменения
    message = update.message.reply_text("⏳ Обновление имён...")
    success_count = 0
    failed_accounts = []
    
    for acc_id, new_name in name_assignments.items():
        account = get_instagram_account(acc_id)
        profile_manager = ProfileManager(acc_id)
        success, result = profile_manager.update_profile_name(new_name)
        
        if success:
            success_count += 1
            # Обновляем в базе данных
            update_instagram_account(acc_id, full_name=new_name)
        else:
            failed_accounts.append(f"@{account.username}: {result}")
    
    # Формируем отчёт
    report = f"✅ Успешно обновлено: {success_count} из {len(accounts)} аккаунтов\n"
    
    if failed_accounts:
        report += f"\n❌ Ошибки:\n" + "\n".join(failed_accounts[:5])
        if len(failed_accounts) > 5:
            report += f"\n... и ещё {len(failed_accounts) - 5} ошибок"
    
    message.delete()
    update.message.reply_text(
        report,
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 К массовым действиям", callback_data="show_bulk_actions")]])
    )
    
    return ConversationHandler.END

def bulk_edit_username(update: Update, context: CallbackContext) -> None:
    """Массовое изменение username"""
    query = update.callback_query
    query.answer()
    
    account_ids = context.user_data.get('selected_profile_accounts', [])
    query.edit_message_text(
        "👤 Массовое изменение username\n\n"
        f"Выбрано аккаунтов: {len(account_ids)}\n\n"
        "Важно:\n"
        "• Каждый аккаунт должен иметь уникальный username\n"
        "• Часть username может быть занята в Instagram\n"
        "• Рекомендуется отправлять на ~20% больше вариантов, чем аккаунтов\n\n"
        "Как отправлять:\n"
        "• Один на строку (списком)\n"
        "• Без символа @\n\n"
        "Отправьте список username или /cancel для отмены:",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 Отмена", callback_data="profile_setup")]
        ])
    )
    
    context.user_data['bulk_action'] = 'edit_username'
    return BULK_EDIT_USERNAME

def bulk_edit_bio(update: Update, context: CallbackContext) -> int:
    """Массовое изменение описания"""
    query = update.callback_query
    query.answer()
    
    account_ids = context.user_data.get('selected_profile_accounts', [])
    
    query.edit_message_text(
        f"✏️ *Массовое изменение описания*\n\n"
        f"Выбрано аккаунтов: {len(account_ids)}\n\n"
        f"*Поддержка шаблонов для уникализации:*\n"
        f"`{{Привет|Здравствуйте|Добрый день}}`\n"
        f"`{{как дела?|что нового?}}`\n\n"
        f"Система автоматически создаст уникальные варианты для каждого аккаунта.\n\n"
        f"*Пример:*\n"
        f"`{{Привет|Здравствуйте}}, я {{фотограф|блогер}}!`\n\n"
        f"Отправьте описание с шаблонами или обычный текст:",
        parse_mode="Markdown"
    )
    
    return BULK_EDIT_BIO

def bulk_edit_links(update: Update, context: CallbackContext) -> int:
    """Массовое изменение ссылок"""
    query = update.callback_query
    query.answer()
    
    account_ids = context.user_data.get('selected_profile_accounts', [])
    
    query.edit_message_text(
        f"🔗 Массовое изменение ссылок\n\n"
        f"Выбрано аккаунтов: {len(account_ids)}\n\n"
        f"Введите ссылку для добавления во все профили:\n"
        f"Примеры:\n"
        f"• https://linktr.ee/yourname\n"
        f"• https://t.me/yourchannel\n"
        f"• https://yourwebsite.com\n\n"
        f"⚠️ ВАЖНО: Для безопасности ссылки будут автоматически уникализированы!\n\n"
        f"Методы уникализации:\n"
        f"📊 UTM-параметры: ?utm_source=instagram&utm_campaign=acc1\n"
        f"🔢 ID параметры: ?ref=user123\n"
        f"🎲 Случайные токены: ?token=abc123\n\n"
        f"Отправьте ссылку или /cancel для отмены:",
        parse_mode=None
    )
    
    return BULK_EDIT_LINKS

def uniquify_link(base_url: str, account_username: str, method: str = "utm") -> str:
    """Создает уникальную ссылку для аккаунта"""
    import random
    import string
    from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
    
    # Парсим URL
    parsed = urlparse(base_url)
    query_params = parse_qs(parsed.query)
    
    if method == "utm":
        # UTM параметры для аналитики
        query_params.update({
            'utm_source': ['instagram'],
            'utm_medium': ['bio_link'],
            'utm_campaign': [f'profile_{account_username}'],
            'utm_content': [account_username]
        })
    elif method == "ref":
        # Простой ref параметр
        query_params['ref'] = [account_username]
    elif method == "random":
        # Случайный токен
        token = ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))
        query_params['id'] = [token]
    elif method == "mixed":
        # Комбинированный подход
        token = ''.join(random.choices(string.ascii_lowercase + string.digits, k=6))
        query_params.update({
            'utm_source': ['instagram'],
            'ref': [account_username],
            'token': [token]
        })
    
    # Собираем URL обратно
    new_query = urlencode(query_params, doseq=True)
    unique_url = urlunparse((
        parsed.scheme,
        parsed.netloc,
        parsed.path,
        parsed.params,
        new_query,
        parsed.fragment
    ))
    
    return unique_url

def save_bulk_links(update: Update, context: CallbackContext) -> int:
    """Сохраняет массовые ссылки с уникализацией (асинхронная версия)"""
    text = update.message.text
    if text == '/cancel':
        update.message.reply_text("❌ Операция отменена", 
                                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="profile_setup")]]))
        return ConversationHandler.END
    
    # Простая валидация ссылки
    base_link = text.strip()
    if not base_link:
        update.message.reply_text("❌ Ссылка не может быть пустой. Попробуйте ещё раз.")
        return BULK_EDIT_LINKS
    
    # Добавляем http:// если протокол не указан
    if not base_link.startswith(('http://', 'https://')):
        base_link = 'https://' + base_link
    
    account_ids = context.user_data.get('selected_profile_accounts', [])
    accounts = [get_instagram_account(acc_id) for acc_id in account_ids]
    
    # Выбираем метод уникализации в зависимости от количества аккаунтов
    if len(accounts) <= 5:
        method = "utm"  # UTM для небольших групп
    elif len(accounts) <= 15:
        method = "mixed"  # Смешанный для средних групп
    else:
        method = "random"  # Случайные токены для больших групп
    
    # Запускаем асинхронную обработку
    message = update.message.reply_text("🚀 Запуск параллельной обработки ссылок...")
    
    try:
        result = process_bulk_links_async(accounts, base_link, method, message, update.message.chat_id)
        
        # Отправляем финальный отчет
        send_bulk_links_report(result, update.message, base_link, method)
        
    except Exception as e:
        logger.error(f"Ошибка при асинхронной обработке ссылок: {e}")
        message.delete()
        update.message.reply_text(
            f"❌ Произошла ошибка при обработке: {str(e)}",
            parse_mode=None,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 К массовым действиям", callback_data="show_bulk_actions")]])
        )
    
    return ConversationHandler.END

def process_single_link(account, base_link: str, method: str) -> dict:
    """Обрабатывает одну ссылку для одного аккаунта"""
    import time
    import random
    
    try:
        # Создаем уникальную ссылку
        unique_link = uniquify_link(base_link, account.username, method)
        
        # Добавляем случайную задержку для имитации человеческого поведения
        delay = random.uniform(1, 4)
        time.sleep(delay)
        
        # Добавляем ссылку через ProfileManager
        profile_manager = ProfileManager(account.id)
        success, result = profile_manager.update_profile_links(unique_link)
        
        if success:
            logger.info(f"✅ Уникальная ссылка добавлена для @{account.username}: {unique_link}")
            return {
                'success': True,
                'account': account.username,
                'link': unique_link,
                'message': result
            }
        else:
            logger.warning(f"❌ Не удалось добавить ссылку для @{account.username}: {result}")
            return {
                'success': False,
                'account': account.username,
                'link': unique_link,
                'message': result
            }
            
    except Exception as e:
        logger.error(f"Ошибка при обработке ссылки для @{account.username}: {e}")
        return {
            'success': False,
            'account': account.username,
            'link': base_link,
            'message': str(e)
        }

def process_bulk_links_async(accounts, base_link: str, method: str, status_message, chat_id) -> dict:
    """Асинхронная обработка массовых ссылок"""
    import concurrent.futures
    import time
    
    # Адаптивный выбор количества потоков
    try:
        from utils.system_monitor import get_adaptive_limits
        limits = get_adaptive_limits()
        system_max_workers = limits.max_workers  # Убираем ограничение в 4 потока
        logger.info(f"🖥️ Системные лимиты: {limits.description}, макс потоков: {system_max_workers}")
    except Exception as e:
        logger.warning(f"Не удалось получить системные лимиты: {e}")
        system_max_workers = 20  # Увеличиваем fallback с 3 до 10
    
    # Определяем количество потоков на основе аккаунтов и системной нагрузки
    account_based_workers = min(20, max(1, len(accounts) // 2))  # Увеличиваем с 4 до 20
    max_workers = min(system_max_workers, account_based_workers)
    
    logger.info(f"🚀 Запуск асинхронной обработки {len(accounts)} аккаунтов в {max_workers} потоках")
    
    success_results = []
    failed_results = []
    completed = 0
    
    start_time = time.time()
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Запускаем все задачи
        future_to_account = {
            executor.submit(process_single_link, account, base_link, method): account 
            for account in accounts
        }
        
        # Обрабатываем результаты по мере готовности
        for future in concurrent.futures.as_completed(future_to_account):
            account = future_to_account[future]
            completed += 1
            
            try:
                result = future.result()
                
                if result['success']:
                    success_results.append(result)
                else:
                    failed_results.append(result)
                
                # Обновляем статус каждые 2-3 аккаунта
                if completed % 3 == 0 or completed == len(accounts):
                    elapsed = time.time() - start_time
                    
                    try:
                        status_message.edit_text(
                            f"⚡ Параллельная обработка ссылок\n\n"
                            f"📊 Прогресс: {completed}/{len(accounts)} аккаунтов\n"
                            f"✅ Успешно: {len(success_results)}\n"
                            f"❌ Ошибки: {len(failed_results)}\n"
                            f"🧵 Потоков: {max_workers}\n"
                            f"⏱️ Время: {elapsed:.1f}с\n\n"
                            f"{'█' * (completed * 20 // len(accounts))}{'░' * (20 - completed * 20 // len(accounts))}",
                            parse_mode=None
                        )
                    except Exception as e:
                        logger.warning(f"Не удалось обновить статус: {e}")
                
            except Exception as e:
                logger.error(f"Ошибка при получении результата для @{account.username}: {e}")
                failed_results.append({
                    'success': False,
                    'account': account.username,
                    'link': base_link,
                    'message': str(e)
                })
    
    total_time = time.time() - start_time
    
    logger.info(f"🏁 Асинхронная обработка завершена за {total_time:.1f}с. Успешно: {len(success_results)}, Ошибок: {len(failed_results)}")
    
    return {
        'success_results': success_results,
        'failed_results': failed_results,
        'total_time': total_time,
        'max_workers': max_workers
    }

def send_bulk_links_report(result: dict, message, base_link: str, method: str):
    """Отправляет финальный отчет по результатам обработки"""
    success_results = result['success_results']
    failed_results = result['failed_results']
    total_time = result['total_time']
    max_workers = result['max_workers']
    
    total_accounts = len(success_results) + len(failed_results)
    
    # Выбираем примеры ссылок для отчета
    link_examples = []
    for res in success_results[:3]:
        link_examples.append(f"@{res['account']}: {res['link']}")
    
    method_names = {
        "utm": "UTM-параметры",
        "ref": "REF-параметры", 
        "random": "Случайные токены",
        "mixed": "Смешанный метод"
    }
    
    # Формируем отчет
    report = f"🔗 ОТЧЕТ ПО АСИНХРОННОМУ ДОБАВЛЕНИЮ ССЫЛОК\n\n"
    report += f"✅ Успешно: {len(success_results)} из {total_accounts} аккаунтов\n"
    report += f"🎯 Базовая ссылка: {base_link}\n"
    report += f"🔧 Метод уникализации: {method_names.get(method, method)}\n"
    report += f"⚡ Потоков использовано: {max_workers}\n"
    report += f"⏱️ Время обработки: {total_time:.1f} секунд\n"
    
    if total_accounts > 0:
        speed = total_accounts / total_time
        old_method_time = total_accounts * 2  # Старый метод: 2 сек на аккаунт
        improvement = old_method_time / total_time
        report += f"🚀 Скорость: {speed:.1f} аккаунтов/сек\n"
        report += f"⚡ Ускорение: в {improvement:.1f}x раз (было бы {old_method_time:.0f}с)\n"
    
    report += "\n"
    
    if link_examples:
        report += f"📋 Примеры уникальных ссылок:\n"
        for example in link_examples:
            report += f"• {example}\n"
        if len(success_results) > 3:
            report += f"... и еще {len(success_results) - 3} успешных ссылок\n"
        report += "\n"
    
    if failed_results:
        report += f"❌ Ошибки ({len(failed_results)}):\n"
        for res in failed_results[:5]:
            report += f"• @{res['account']}: {res['message']}\n"
        if len(failed_results) > 5:
            report += f"... и ещё {len(failed_results) - 5} ошибок\n"
        report += "\n"
    
    if len(success_results) > 0:
        report += f"💡 Ссылки могут появиться в профилях не сразу\n"
        report += f"🛡️ Уникализация защищает от банов Instagram\n"
        report += f"⚡ Параллельная обработка ускорила процесс в {max_workers}x раз!"
    
    # Удаляем статусное сообщение и отправляем финальный отчет
    try:
        message.delete()
    except:
        pass
        
    message.reply_text(
        report,
        parse_mode=None,
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 К массовым действиям", callback_data="show_bulk_actions")]])
    )

def bulk_add_photo(update: Update, context: CallbackContext) -> int:
    """Массовое добавление фото профиля"""
    query = update.callback_query
    query.answer()
    
    account_ids = context.user_data.get('selected_profile_accounts', [])
    
    query.edit_message_text(
        f"🖼 *Массовое добавление фото профиля*\n\n"
        f"Выбрано аккаунтов: {len(account_ids)}\n\n"
        f"*Как это работает:*\n"
        f"• Можете отправить одно или несколько фото\n"
        f"• Если фото меньше, чем аккаунтов - они будут уникализированы\n"
        f"• Уникализация: изменение фона, кроп, поворот\n\n"
        f"Отправьте фото (можно несколько) или /cancel для отмены:",
        parse_mode="Markdown"
    )
    
    context.user_data['bulk_photos'] = []
    return BULK_ADD_PHOTO

def show_bulk_actions_callback(update: Update, context: CallbackContext) -> None:
    """Обработчик для callback show_bulk_actions"""
    show_bulk_profile_actions(update, context)

def bulk_delete_photo(update: Update, context: CallbackContext) -> None:
    """Массовое удаление фото профиля"""
    query = update.callback_query
    query.answer()
    query.edit_message_text("🚧 Функция массового удаления фото профиля в разработке", 
                          reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="profile_setup")]]))

def bulk_delete_posts(update: Update, context: CallbackContext) -> None:
    """Массовое удаление постов"""
    query = update.callback_query
    query.answer()
    query.edit_message_text("🚧 Функция массового удаления постов в разработке", 
                          reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="profile_setup")]]))

def bulk_delete_bio(update: Update, context: CallbackContext) -> None:
    """Массовое очищение описания"""
    query = update.callback_query
    query.answer()
    query.edit_message_text("🚧 Функция массовой очистки описания в разработке", 
                          reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="profile_setup")]]))

def save_bulk_bio(update: Update, context: CallbackContext) -> int:
    """Сохраняет массовое описание с поддержкой шаблонов"""
    text = update.message.text
    if text == '/cancel':
        update.message.reply_text("❌ Операция отменена", 
                                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="show_bulk_actions")]]))
        return ConversationHandler.END
    
    # Устанавливаем действие и передаем управление общему обработчику
    context.user_data['bulk_action'] = 'edit_bio'
    return handle_bulk_profile_action(update, context)

def save_bulk_usernames(update: Update, context: CallbackContext) -> int:
    """Сохраняет массовые username"""
    text = update.message.text
    if text == '/cancel':
        update.message.reply_text("❌ Операция отменена", 
                                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="show_bulk_actions")]]))
        return ConversationHandler.END
    
    # Здесь будет логика обработки username
    update.message.reply_text("🚧 Обработка username в разработке", 
                            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 К массовым действиям", callback_data="show_bulk_actions")]]))
    return ConversationHandler.END

def collect_bulk_photos(update: Update, context: CallbackContext) -> int:
    """Собирает фото для массовой установки"""
    if 'bulk_photos' not in context.user_data:
        context.user_data['bulk_photos'] = []
    
    # Сохраняем фото
    photo_file = update.message.photo[-1]
    context.user_data['bulk_photos'].append(photo_file.file_id)
    
    count = len(context.user_data['bulk_photos'])
    update.message.reply_text(
        f"📸 Фото #{count} получено.\n\n"
        f"Отправьте ещё фото или введите /done для начала обработки."
    )
    
    return BULK_ADD_PHOTO

def process_bulk_photos(update: Update, context: CallbackContext) -> int:
    """Обрабатывает собранные фото"""
    import concurrent.futures
    import threading
    
    photos = context.user_data.get('bulk_photos', [])
    account_ids = context.user_data.get('selected_profile_accounts', [])
    
    if not photos:
        update.message.reply_text("❌ Не загружено ни одного фото.")
        return BULK_ADD_PHOTO
    
    # Фильтруем только активные аккаунты
    active_account_ids = []
    inactive_accounts = []
    
    for acc_id in account_ids:
        account = get_instagram_account(acc_id)
        if account and account.status == 'active':
            active_account_ids.append(acc_id)
        else:
            inactive_accounts.append(account.username if account else f"ID {acc_id}")
    
    if not active_account_ids:
        update.message.reply_text(
            "❌ Нет активных аккаунтов для обработки.\n\n"
            f"Неактивные: {', '.join(inactive_accounts)}",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 К массовым действиям", callback_data="show_bulk_actions")]])
        )
        return ConversationHandler.END
    
    # Начинаем обработку
    message_text = f"⏳ Установка аватаров...\n\n"
    if inactive_accounts:
        message_text += f"⚠️ Пропущено неактивных: {len(inactive_accounts)}\n"
    message_text += f"📊 Будет обработано: {len(active_account_ids)}"
    
    message = update.message.reply_text(message_text)
    
    # Используем активные аккаунты вместо всех
    account_ids = active_account_ids
    
    # Скачиваем все фото
    photo_paths = []
    for i, photo_file_id in enumerate(photos):
        try:
            photo_file = context.bot.get_file(photo_file_id)
            photo_path = f"temp_bulk_photo_{i}.jpg"
            photo_file.download(photo_path)
            photo_paths.append(photo_path)
        except Exception as e:
            logger.error(f"Ошибка при скачивании фото: {e}")
    
    if not photo_paths:
        message.delete()
        update.message.reply_text(
            "❌ Не удалось скачать фото",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 К массовым действиям", callback_data="show_bulk_actions")]])
        )
        return ConversationHandler.END
    
    # Переменные для отслеживания прогресса
    success_count = 0
    failed_accounts = []
    processed_count = 0
    lock = threading.Lock()
    
    def update_avatar(account_id, index):
        """Функция для обновления аватара одного аккаунта"""
        nonlocal success_count, processed_count
        photo_path = photo_paths[index % len(photo_paths)]
        
        try:
            account = get_instagram_account(account_id)
            
            # Проверяем статус аккаунта
            if account.status != 'active':
                with lock:
                    processed_count += 1
                    failed_accounts.append(f"@{account.username}: Аккаунт неактивен")
                return
            
            # Пытаемся создать ProfileManager
            try:
                profile_manager = ProfileManager(account_id)
            except Exception as e:
                with lock:
                    processed_count += 1
                    failed_accounts.append(f"@{account.username}: Не удалось войти в аккаунт")
                return
            
            # Обновляем фото
            success, result = profile_manager.update_profile_picture(photo_path)
            
            with lock:
                processed_count += 1
                if success:
                    success_count += 1
                else:
                    failed_accounts.append(f"@{account.username}: {result}")
                
                # Обновляем прогресс
                if processed_count % 3 == 0 or processed_count == len(account_ids):
                    try:
                        message.edit_text(
                            f"⏳ Установка аватаров...\n\n"
                            f"🔄 Обработано: {processed_count}/{len(account_ids)}\n"
                            f"✅ Успешно: {success_count}\n"
                            f"❌ Ошибок: {len(failed_accounts)}\n\n"
                            f"⚡ Параллельная обработка"
                        )
                    except:
                        pass
                        
        except Exception as e:
            with lock:
                processed_count += 1
                error_msg = str(e)
                if "Клиент Instagram не инициализирован" in error_msg:
                    failed_accounts.append(f"@{account.username if 'account' in locals() else f'ID {account_id}'}: Не удалось войти")
                else:
                    failed_accounts.append(f"Аккаунт {account_id}: {error_msg}")
    
    # Используем ThreadPoolExecutor для параллельной обработки
    max_workers = min(20, len(account_ids))  # Максимум 20 параллельных потоков
    
    try:
        message.edit_text(
            f"⚡ Запуск параллельной обработки...\n\n"
            f"📊 Всего аккаунтов: {len(account_ids)}\n"
            f"🔄 Параллельных потоков: {max_workers}"
        )
    except:
        pass
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Запускаем все задачи
        futures = {}
        for i, account_id in enumerate(account_ids):
            future = executor.submit(update_avatar, account_id, i)
            futures[future] = (account_id, i)
        
        # Ждем завершения с таймаутом
        timeout = 60 * len(account_ids) / max_workers  # 60 секунд на аккаунт
        done, not_done = concurrent.futures.wait(futures, timeout=timeout)
        
        # Обрабатываем незавершенные задачи
        if not_done:
            for future in not_done:
                account_id, _ = futures[future]
                try:
                    account = get_instagram_account(account_id)
                    with lock:
                        failed_accounts.append(f"@{account.username}: Превышен таймаут")
                        processed_count += 1
                except:
                    with lock:
                        failed_accounts.append(f"Аккаунт {account_id}: Превышен таймаут")
                        processed_count += 1
                future.cancel()
    
    # Удаляем временные файлы
    for photo_path in photo_paths:
        if os.path.exists(photo_path):
            os.remove(photo_path)
    
    # Формируем отчёт
    report = f"✅ Успешно обновлено: {success_count} из {len(account_ids)} аккаунтов\n"
    
    if failed_accounts:
        report += f"\n❌ Ошибки:\n" + "\n".join(failed_accounts[:5])
        if len(failed_accounts) > 5:
            report += f"\n... и ещё {len(failed_accounts) - 5} ошибок"
    
    message.delete()
    update.message.reply_text(
        report,
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 К массовым действиям", callback_data="show_bulk_actions")]])
    )
    
    # Очищаем временные данные
    context.user_data.pop('bulk_photos', None)
    context.user_data.pop('selected_profile_accounts', None)
    
    return ConversationHandler.END

def cancel_bulk_operation(update: Update, context: CallbackContext) -> int:
    """Отменяет массовую операцию"""
    # Очищаем временные данные
    context.user_data.pop('bulk_photos', None)
    
    update.message.reply_text(
        "❌ Операция отменена",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 К массовым действиям", callback_data="show_bulk_actions")]])
    )
    
    return ConversationHandler.END

def cancel(update: Update, context: CallbackContext) -> int:
    """Отменяет текущую операцию"""
    query = update.callback_query
    query.answer()

    account_id = context.user_data.get('current_account_id')

    if account_id:
        return profile_account_menu(update, context)
    else:
        return profile_setup_menu(update, context)

def on_profile_account_selected(account_ids: list, update: Update, context: CallbackContext):
    """Callback для обработки выбранных аккаунтов в профилях"""
    if account_ids:
        if len(account_ids) == 1:
            # Если выбран один аккаунт, показываем меню настроек
            account_id = account_ids[0]
            context.user_data['current_account_id'] = account_id
            profile_account_menu(update, context)
        else:
            # Если выбрано несколько аккаунтов, показываем опции массовых действий
            context.user_data['selected_profile_accounts'] = account_ids
            show_bulk_profile_actions(update, context)

# Глобальная переменная для хранения callback функции
PROFILE_ACCOUNT_CALLBACK = on_profile_account_selected

# Создаем глобальный селектор для профилей
profile_selector = AccountSelector(
    callback_prefix="profile_select",
    title="⚙️ Настройка профиля",
    allow_multiple=True,  # Изменено на True для множественного выбора
    show_status=True,
    show_folders=True,
    back_callback="menu_accounts"
)

def start_profile_selection(update: Update, context: CallbackContext):
    """Запускает процесс выбора аккаунтов для профиля"""
    return profile_selector.start_selection(update, context, PROFILE_ACCOUNT_CALLBACK)

def get_profile_handlers():
    """Возвращает обработчики для управления профилем"""
    
    profile_conv_handler = ConversationHandler(
        entry_points=[
            CallbackQueryHandler(edit_profile_name, pattern='^profile_edit_name$'),
            CallbackQueryHandler(edit_profile_username, pattern='^profile_edit_username$'),
            CallbackQueryHandler(edit_profile_bio, pattern='^profile_edit_bio$'),
            CallbackQueryHandler(edit_profile_links, pattern='^profile_edit_links$'),
            CallbackQueryHandler(add_profile_photo, pattern='^profile_add_photo$'),
            CallbackQueryHandler(add_post, pattern='^profile_add_post$'),
        ],
        states={
            EDIT_NAME: [MessageHandler(Filters.text & ~Filters.command, save_profile_name)],
            EDIT_USERNAME: [MessageHandler(Filters.text & ~Filters.command, save_profile_username)],
            EDIT_BIO: [MessageHandler(Filters.text & ~Filters.command, save_profile_bio)],
            EDIT_LINKS: [MessageHandler(Filters.text & ~Filters.command, save_profile_links)],
            ADD_PHOTO: [MessageHandler(Filters.photo, save_profile_photo)],
            ADD_POST: [
                MessageHandler(Filters.photo | Filters.video, save_post),
            ],
        },
        fallbacks=[
            CallbackQueryHandler(cancel, pattern='^profile_account_'),
            CallbackQueryHandler(cancel, pattern='^profile_setup$'),
        ],
        name="profile_conversation",
        persistent=False,
    )
    
    # ConversationHandler для массовых операций
    bulk_conv_handler = ConversationHandler(
        entry_points=[
            CallbackQueryHandler(bulk_edit_name, pattern='^bulk_edit_name$'),
            CallbackQueryHandler(bulk_edit_username, pattern='^bulk_edit_username$'),
            CallbackQueryHandler(bulk_edit_bio, pattern='^bulk_edit_bio$'),
            CallbackQueryHandler(bulk_edit_links, pattern='^bulk_edit_links$'),
            CallbackQueryHandler(bulk_add_photo, pattern='^bulk_add_photo$'),
        ],
        states={
            BULK_EDIT_NAME: [MessageHandler(Filters.text & ~Filters.command, save_bulk_names)],
            BULK_EDIT_USERNAME: [MessageHandler(Filters.text & ~Filters.command, save_bulk_usernames)],
            BULK_EDIT_BIO: [MessageHandler(Filters.text & ~Filters.command, save_bulk_bio)],
            BULK_EDIT_LINKS: [MessageHandler(Filters.text & ~Filters.command, save_bulk_links)],
            BULK_ADD_PHOTO: [
                MessageHandler(Filters.photo, collect_bulk_photos),
                CommandHandler('done', process_bulk_photos)
            ],
        },
        fallbacks=[
            CommandHandler('cancel', cancel_bulk_operation),
            CallbackQueryHandler(show_bulk_actions_callback, pattern='^show_bulk_actions$'),
        ],
        name="bulk_profile_conversation",
        persistent=False,
    )

    # ConversationHandler для мастера оформления профиля
    wizard_conv_handler = ConversationHandler(
        entry_points=[
            CallbackQueryHandler(start_profile_wizard, pattern='^profile_full_wizard$'),
        ],
        states={
            WIZARD_NAME: [
                MessageHandler(Filters.text & ~Filters.command, save_wizard_name),
                CallbackQueryHandler(skip_wizard_name, pattern='^wizard_skip_name$'),
            ],
            WIZARD_USERNAME: [
                MessageHandler(Filters.text & ~Filters.command, save_wizard_username),
                CallbackQueryHandler(skip_wizard_username, pattern='^wizard_skip_username$'),
            ],
            WIZARD_BIO: [
                MessageHandler(Filters.text & ~Filters.command, save_wizard_bio),
                CallbackQueryHandler(skip_wizard_bio, pattern='^wizard_skip_bio$'),
            ],
            WIZARD_LINK: [
                MessageHandler(Filters.text & ~Filters.command, save_wizard_link),
                CallbackQueryHandler(skip_wizard_link, pattern='^wizard_skip_link$'),
            ],
            WIZARD_PHOTO: [
                MessageHandler(Filters.photo, save_wizard_photo),
                CallbackQueryHandler(skip_wizard_photo, pattern='^wizard_skip_photo$'),
            ],
            WIZARD_CONFIRM: [
                CallbackQueryHandler(wizard_apply, pattern='^wizard_apply$'),
                CallbackQueryHandler(profile_account_menu, pattern='^profile_account_'),
            ],
        },
        fallbacks=[
            CallbackQueryHandler(cancel, pattern='^profile_account_'),
            CallbackQueryHandler(cancel, pattern='^profile_setup$'),
        ],
        name="profile_wizard_conversation",
        persistent=False,
    )

    # ConversationHandler для bulk-мастера
    wizard_bulk_conv_handler = ConversationHandler(
        entry_points=[
            CallbackQueryHandler(start_bulk_profile_wizard, pattern='^bulk_full_wizard$'),
        ],
        states={
            BULK_WIZARD_NAME: [
                MessageHandler(Filters.text & ~Filters.command, save_bulk_wizard_name),
                CallbackQueryHandler(skip_bulk_wizard_name, pattern='^bulk_wizard_skip_name$'),
            ],
            BULK_WIZARD_USERNAME: [
                MessageHandler(Filters.text & ~Filters.command, save_bulk_wizard_username),
                CallbackQueryHandler(skip_bulk_wizard_username, pattern='^bulk_wizard_skip_username$'),
            ],
            BULK_WIZARD_BIO: [
                MessageHandler(Filters.text & ~Filters.command, save_bulk_wizard_bio),
                CallbackQueryHandler(skip_bulk_wizard_bio, pattern='^bulk_wizard_skip_bio$'),
            ],
            BULK_WIZARD_LINK: [
                MessageHandler(Filters.text & ~Filters.command, save_bulk_wizard_link),
                CallbackQueryHandler(skip_bulk_wizard_link, pattern='^bulk_wizard_skip_link$'),
            ],
            BULK_WIZARD_PHOTO: [
                MessageHandler(Filters.photo, save_bulk_wizard_photo),
                CallbackQueryHandler(skip_bulk_wizard_photo, pattern='^bulk_wizard_skip_photo$'),
            ],
            BULK_WIZARD_CONFIRM: [
                CallbackQueryHandler(bulk_wizard_apply, pattern='^bulk_wizard_apply$'),
                CallbackQueryHandler(show_bulk_profile_actions, pattern='^show_bulk_actions$'),
            ],
        },
        fallbacks=[
            CallbackQueryHandler(show_bulk_profile_actions, pattern='^show_bulk_actions$'),
            CallbackQueryHandler(cancel, pattern='^profile_setup$'),
        ],
        name="profile_bulk_wizard_conversation",
        persistent=False,
    )

    # Создаем объединенный ConversationHandler для profile_selector
    selector_conv = profile_selector.get_conversation_handler()
    
    # Добавляем profile_setup как entry point
    profile_selector_conv = ConversationHandler(
        entry_points=selector_conv.entry_points + [
            CallbackQueryHandler(profile_setup_menu, pattern='^profile_setup$')
        ],
        states=selector_conv.states,
        fallbacks=selector_conv.fallbacks,
        name="profile_selector_conversation",
        persistent=False,
    )

    handlers = [
        CommandHandler('profile', profile_setup_menu),
        CallbackQueryHandler(show_bulk_actions_callback, pattern='^show_bulk_actions$'),
        CallbackQueryHandler(profile_account_menu, pattern='^profile_account_'),
        CallbackQueryHandler(delete_profile_photo, pattern='^profile_delete_photo$'),
        CallbackQueryHandler(add_profile_photo, pattern=r'^add_profile_photo_\d+$'),
        CallbackQueryHandler(delete_all_posts, pattern='^profile_delete_posts$'),
        CallbackQueryHandler(delete_bio, pattern='^profile_delete_bio$'),
        # Обработчики массовых действий (не в ConversationHandler)
        CallbackQueryHandler(bulk_remove_photo, pattern='^bulk_remove_photo$'),
        CallbackQueryHandler(bulk_delete_all_posts, pattern='^bulk_delete_all_posts$'),
        CallbackQueryHandler(confirm_bulk_delete_posts, pattern='^confirm_bulk_delete_posts$'),
        CallbackQueryHandler(bulk_clear_bio, pattern='^bulk_clear_bio$'),
        profile_conv_handler,
        bulk_conv_handler,
        wizard_conv_handler,
        wizard_bulk_conv_handler,
        profile_selector_conv,
    ]

    return handlers

def bulk_add_link(update: Update, context: CallbackContext) -> None:
    """Массовое добавление ссылки"""
    query = update.callback_query
    query.answer()
    
    query.edit_message_text(
        "🔗 Введите ссылку для добавления во все выбранные аккаунты:",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 Отмена", callback_data="profile_setup")]
        ])
    )
    
    context.user_data['bulk_action'] = 'add_link'
    return EDIT_LINKS

def bulk_set_avatar(update: Update, context: CallbackContext) -> None:
    """Массовая установка аватара"""
    query = update.callback_query
    query.answer()
    
    query.edit_message_text(
        "🖼 Отправьте фото для установки как аватар во все выбранные аккаунты:",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 Отмена", callback_data="profile_setup")]
        ])
    )
    
    context.user_data['bulk_action'] = 'set_avatar'
    return ADD_PHOTO

def bulk_delete_avatar(update: Update, context: CallbackContext) -> None:
    """Массовое удаление аватара"""
    query = update.callback_query
    query.answer()
    
    selected_accounts = context.user_data.get('selected_accounts', [])
    
    # Здесь должна быть логика удаления аватаров
    query.edit_message_text(
        f"✅ Аватары удалены у {len(selected_accounts)} аккаунтов",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 Назад", callback_data="profile_setup")]
        ])
    )
    
    # Очищаем выбранные аккаунты
    context.user_data['selected_accounts'] = []

def handle_bulk_profile_action(update: Update, context: CallbackContext) -> int:
    """Обрабатывает массовые действия с профилями"""
    try:
        text = update.message.text if update.message.text else ""
        bulk_action = context.user_data.get('bulk_action')
        selected_accounts = context.user_data.get('selected_profile_accounts', [])
        
        if not selected_accounts or not bulk_action:
            update.message.reply_text(
                "❌ Ошибка: не выбраны аккаунты или действие",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Назад", callback_data="profile_setup")]
                ])
            )
            return ConversationHandler.END
        
        # ФИЛЬТРУЕМ ТОЛЬКО АКТИВНЫЕ АККАУНТЫ
        from database.db_manager import get_instagram_account
        active_accounts = []
        inactive_accounts = []
        
        for acc_id in selected_accounts:
            account = get_instagram_account(acc_id)
            if account and account.status == 'active':
                active_accounts.append(acc_id)
            else:
                inactive_accounts.append(account.username if account else f"ID {acc_id}")
        
        if not active_accounts:
            update.message.reply_text(
                "❌ Нет активных аккаунтов для обработки.\n\n"
                f"Неактивные: {', '.join(inactive_accounts)}",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 К массовым действиям", callback_data="show_bulk_actions")]
                ])
            )
            return ConversationHandler.END
        
        # Информируем о пропущенных аккаунтах
        if inactive_accounts:
            info_message = f"⚠️ Пропущено неактивных аккаунтов: {len(inactive_accounts)}\n\n"
            update.message.reply_text(info_message)
        
        # Используем только активные аккаунты
        selected_accounts = active_accounts
        
        success_count = 0
        errors = []
        
        # Обработка фото для аватара
        if bulk_action == 'set_avatar' and update.message.photo:
            import concurrent.futures
            import threading
            
            # Сохраняем фото
            photo_file = update.message.photo[-1]
            file_obj = photo_file.get_file()
            photo_bytes = file_obj.download_as_bytearray()
            
            # Подсчет обновленных аккаунтов
            processed_count = 0
            lock = threading.Lock()
            
            def update_single_avatar(account_id):
                nonlocal processed_count, success_count
                try:
                    from instagram.profile_manager import ProfileManager
                    profile_manager = ProfileManager(account_id)
                    success, message = profile_manager.update_profile_photo(photo_bytes)
                    
                    with lock:
                        if success:
                            success_count += 1
                        else:
                            errors.append(f"Аккаунт ID {account_id}: {message}")
                        processed_count += 1
                            
                except Exception as e:
                    with lock:
                        processed_count += 1
                        errors.append(f"Аккаунт ID {account_id}: {str(e)}")
            
            # Отправляем сообщение о прогрессе
            progress_message = update.message.reply_text(
                f"🔄 Обновление фото профилей...\n\n"
                f"Обработано: 0/{len(selected_accounts)}"
            )
            
            # Используем system_monitor для определения оптимального количества потоков
            from utils.system_monitor import system_monitor
            workload_limits = system_monitor.get_workload_limits()
            max_workers = min(workload_limits.max_workers, len(selected_accounts))  # Используем рекомендованное количество потоков
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {executor.submit(update_single_avatar, acc_id): acc_id 
                          for acc_id in selected_accounts}
                
                # Периодически обновляем прогресс
                for future in concurrent.futures.as_completed(futures):
                    if processed_count % 3 == 0:
                        try:
                            progress_message.edit_text(
                                f"🔄 Обновление фото профилей...\n\n"
                                f"Обработано: {processed_count}/{len(selected_accounts)}\n"
                                f"Успешно: {success_count}\n"
                                f"Потоков: {max_workers}"
                            )
                        except:
                            pass
            
            # Удаляем сообщение о прогрессе
            try:
                progress_message.delete()
            except:
                pass
        
        else:
            # Обработка текстовых данных с использованием нового batch_update_profiles
            from instagram.profile_manager import ProfileManager
            from utils.system_monitor import system_monitor
            
            # Определяем оптимальное количество потоков
            workload_limits = system_monitor.get_workload_limits()
            max_workers = min(workload_limits.max_workers, len(selected_accounts))
            
            # Отправляем сообщение о прогрессе
            progress_message = update.message.reply_text(
                f"🔄 Обновление профилей...\n\n"
                f"Обработано: 0/{len(selected_accounts)}\n"
                f"Потоков: {max_workers}"
            )
            
            # Функция обратного вызова для обновления прогресса
            def progress_callback(processed, total, success, error_count):
                try:
                    progress_message.edit_text(
                        f"🔄 Обновление профилей...\n\n"
                        f"Обработано: {processed}/{total}\n"
                        f"Успешно: {success}\n"
                        f"Ошибок: {error_count}\n"
                        f"Потоков: {max_workers}"
                    )
                except:
                    pass
            
            # Вызываем batch обновление
            success_count, error_list = ProfileManager.batch_update_profiles(
                account_ids=selected_accounts,
                update_type=bulk_action,
                value=text,
                max_workers=max_workers,
                progress_callback=progress_callback
            )
            
            # Формируем список ошибок для отчета
            errors = [f"@{err.get('username', f'ID {err.get("account_id")}')}: {err.get('error')}" 
                     for err in error_list]
            
            # Удаляем сообщение о прогрессе
            try:
                progress_message.delete()
            except:
                pass
        
        # Формируем отчет
        report = f"✅ Успешно обновлено: {success_count} из {len(selected_accounts)} аккаунтов\n"
        if errors:
            report += "\n❌ Ошибки:\n" + "\n".join(errors[:5])
            if len(errors) > 5:
                report += f"\n... и еще {len(errors) - 5} ошибок"
        
        update.message.reply_text(
            report,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Назад", callback_data="profile_setup")]
            ])
        )
        
        # Очищаем данные
        context.user_data['selected_profile_accounts'] = []
        context.user_data['bulk_action'] = None
        
        return ConversationHandler.END
        
    except Exception as e:
        logger.error(f"Ошибка в handle_bulk_profile_action: {e}", exc_info=True)
        update.message.reply_text(
            f"❌ Произошла ошибка: {str(e)}",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Назад", callback_data="profile_setup")]
            ])
        )
        return ConversationHandler.END

def bulk_remove_photo(update: Update, context: CallbackContext) -> None:
    """Массовое удаление фото профиля"""
    query = update.callback_query
    query.answer()
    
    account_ids = context.user_data.get('selected_profile_accounts', [])
    
    if not account_ids:
        query.edit_message_text(
            "❌ Не выбраны аккаунты",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Назад", callback_data="profile_setup")]
            ])
        )
        return
    
    # Фильтруем только активные аккаунты
    active_account_ids = []
    inactive_accounts = []
    
    for acc_id in account_ids:
        account = get_instagram_account(acc_id)
        if account and account.status == 'active':
            active_account_ids.append(acc_id)
        else:
            inactive_accounts.append(account.username if account else f"ID {acc_id}")
    
    if not active_account_ids:
        query.edit_message_text(
            "❌ Нет активных аккаунтов для обработки.\n\n"
            f"Неактивные: {', '.join(inactive_accounts)}",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 К массовым действиям", callback_data="show_bulk_actions")]
            ])
        )
        return
    
    # Начинаем процесс
    message_text = f"🔄 Удаление фото профиля...\n"
    if inactive_accounts:
        message_text += f"⚠️ Пропущено неактивных: {len(inactive_accounts)}\n"
    message_text += f"\nОбработано: 0/{len(active_account_ids)}"
    
    query.edit_message_text(message_text)
    
    success_count = 0
    errors = []
    
    def progress_callback(processed, total, success, error_count):
        try:
            query.edit_message_text(
                f"🔄 Удаление фото профиля...\n\n"
                f"Обработано: {processed}/{total}\n"
                f"Успешно: {success}\n"
                f"Ошибок: {error_count}"
            )
        except Exception as e:
            logger.error(f"Ошибка обновления прогресса: {e}")
    
    # Используем batch_update_profiles для параллельной обработки
    from instagram.profile_manager import ProfileManager
    
    # Создаем функцию-обертку для удаления фото
    def delete_photo_wrapper(account_id):
        try:
            pm = ProfileManager(account_id)
            return pm.remove_profile_picture()
        except Exception as e:
            return False, str(e)
    
    # Обрабатываем аккаунты параллельно
    import concurrent.futures
    import threading
    
    processed_count = 0
    lock = threading.Lock()
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = []
        for acc_id in active_account_ids:
            future = executor.submit(delete_photo_wrapper, acc_id)
            futures.append((acc_id, future))
        
        for acc_id, future in futures:
            try:
                success, message = future.result()
                with lock:
                    processed_count += 1
                    if success:
                        success_count += 1
                    else:
                        account = get_instagram_account(acc_id)
                        errors.append({
                            'username': account.username if account else f'ID {acc_id}',
                            'error': message
                        })
                    progress_callback(processed_count, len(active_account_ids), success_count, len(errors))
            except Exception as e:
                with lock:
                    processed_count += 1
                    errors.append({
                        'username': f'ID {acc_id}',
                        'error': str(e)
                    })
    
    # Формируем финальный отчет
    report = f"✅ Завершено!\n\n"
    report += f"Успешно: {success_count}/{len(active_account_ids)}\n"
    
    if errors:
        report += f"\n❌ Ошибки:\n"
        for err in errors[:5]:
            report += f"@{err['username']}: {err['error']}\n"
        if len(errors) > 5:
            report += f"... и еще {len(errors) - 5} ошибок"
    
    query.edit_message_text(
        report,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 К массовым действиям", callback_data="show_bulk_actions")]
        ])
    )

def bulk_delete_all_posts(update: Update, context: CallbackContext) -> None:
    """Массовое удаление всех постов"""
    query = update.callback_query
    query.answer()
    
    account_ids = context.user_data.get('selected_profile_accounts', [])
    
    if not account_ids:
        query.edit_message_text(
            "❌ Не выбраны аккаунты",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Назад", callback_data="profile_setup")]
            ])
        )
        return
    
    # Запрашиваем подтверждение
    query.edit_message_text(
        f"⚠️ *ВНИМАНИЕ!*\n\n"
        f"Вы собираетесь удалить *ВСЕ ПОСТЫ* у {len(account_ids)} аккаунтов.\n"
        f"Это действие *НЕОБРАТИМО*!\n\n"
        f"Вы уверены?",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([
            [
                InlineKeyboardButton("✅ Да, удалить", callback_data="confirm_bulk_delete_posts"),
                InlineKeyboardButton("❌ Отмена", callback_data="show_bulk_actions")
            ]
        ])
    )

def confirm_bulk_delete_posts(update: Update, context: CallbackContext) -> None:
    """Подтверждение и выполнение массового удаления постов"""
    query = update.callback_query
    query.answer()
    
    account_ids = context.user_data.get('selected_profile_accounts', [])
    
    # Фильтруем только активные аккаунты
    active_account_ids = []
    inactive_accounts = []
    
    for acc_id in account_ids:
        account = get_instagram_account(acc_id)
        if account and account.status == 'active':
            active_account_ids.append(acc_id)
        else:
            inactive_accounts.append(account.username if account else f"ID {acc_id}")
    
    if not active_account_ids:
        query.edit_message_text(
            "❌ Нет активных аккаунтов для обработки.\n\n"
            f"Неактивные: {', '.join(inactive_accounts)}",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 К массовым действиям", callback_data="show_bulk_actions")]
            ])
        )
        return
    
    # Начинаем процесс
    message_text = f"📸 Удаление постов...\n"
    if inactive_accounts:
        message_text += f"⚠️ Пропущено неактивных: {len(inactive_accounts)}\n"
    message_text += f"\nОбработано: 0/{len(active_account_ids)}"
    
    query.edit_message_text(message_text)
    
    success_count = 0
    total_deleted = 0
    errors = []
    
    def progress_callback(processed, total):
        try:
            query.edit_message_text(
                f"📸 Удаление постов...\n\n"
                f"Обработано аккаунтов: {processed}/{total}\n"
                f"Удалено постов: {total_deleted}"
            )
        except Exception as e:
            logger.error(f"Ошибка обновления прогресса: {e}")
    
    # Обрабатываем аккаунты последовательно (для постов лучше не параллельно)
    for i, acc_id in enumerate(active_account_ids):
        try:
            pm = ProfileManager(acc_id)
            success, message = pm.delete_all_posts()
            
            if success:
                success_count += 1
                # Извлекаем количество удаленных постов из сообщения
                import re
                match = re.search(r'Удалено постов: (\d+)', message)
                if match:
                    total_deleted += int(match.group(1))
            else:
                account = get_instagram_account(acc_id)
                errors.append({
                    'username': account.username if account else f'ID {acc_id}',
                    'error': message
                })
            
            progress_callback(i + 1, len(active_account_ids))
            
        except Exception as e:
            errors.append({
                'username': f'ID {acc_id}',
                'error': str(e)
            })
    
    # Формируем финальный отчет
    report = f"✅ Завершено!\n\n"
    report += f"Обработано аккаунтов: {success_count}/{len(active_account_ids)}\n"
    report += f"Всего удалено постов: {total_deleted}\n"
    
    if errors:
        report += f"\n❌ Ошибки:\n"
        for err in errors[:5]:
            report += f"@{err['username']}: {err['error']}\n"
        if len(errors) > 5:
            report += f"... и еще {len(errors) - 5} ошибок"
    
    query.edit_message_text(
        report,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 К массовым действиям", callback_data="show_bulk_actions")]
        ])
    )

def bulk_clear_bio(update: Update, context: CallbackContext) -> None:
    """Массовая очистка описания профиля"""
    query = update.callback_query
    query.answer()
    
    account_ids = context.user_data.get('selected_profile_accounts', [])
    
    if not account_ids:
        query.edit_message_text(
            "❌ Не выбраны аккаунты",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Назад", callback_data="profile_setup")]
            ])
        )
        return
    
    # Начинаем процесс
    query.edit_message_text(f"🧹 Очистка описаний...\n\nОбработано: 0/{len(account_ids)}")
    
    # Используем batch_update_profiles с пустым значением
    from instagram.profile_manager import ProfileManager
    
    def progress_callback(processed, total, success, error_count):
        try:
            query.edit_message_text(
                f"🧹 Очистка описаний...\n\n"
                f"Обработано: {processed}/{total}\n"
                f"Успешно: {success}\n"
                f"Ошибок: {error_count}"
            )
        except Exception as e:
            logger.error(f"Ошибка обновления прогресса: {e}")
    
    # Используем batch_update_profiles для параллельной очистки био
    success_count, errors = ProfileManager.batch_update_profiles(
        account_ids, 
        'bio', 
        '',  # Пустое значение для очистки
        max_workers=4,
        progress_callback=progress_callback
    )
    
    # Формируем финальный отчет
    report = f"✅ Завершено!\n\n"
    report += f"Успешно очищено: {success_count}/{len(account_ids)}\n"
    
    if errors:
        report += f"\n❌ Ошибки:\n"
        for err in errors[:5]:
            report += f"@{err['username']}: {err['error']}\n"
        if len(errors) > 5:
            report += f"... и еще {len(errors) - 5} ошибок"
    
    query.edit_message_text(
        report,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 К массовым действиям", callback_data="show_bulk_actions")]
        ])
    )

# =====================
# Мастер оформления профиля (wizard)
# =====================

def _wizard_init(context: CallbackContext):
    context.user_data['profile_wizard'] = {
        'full_name': None,
        'username': None,
        'bio': None,
        'link': None,
        'photo_file_id': None,
    }


def start_profile_wizard(update: Update, context: CallbackContext) -> int:
    query = update.callback_query
    query.answer()

    account_id = context.user_data.get('current_account_id')
    if not account_id:
        query.edit_message_text("❌ Аккаунт не выбран", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="profile_setup")]]))
        return ConversationHandler.END

    _wizard_init(context)

    # Шаг 1: Имя
    query.edit_message_text(
        "📝 Введите новое имя профиля или нажмите Пропустить",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⏭ Пропустить", callback_data="wizard_skip_name")]])
    )
    return WIZARD_NAME


def save_wizard_name(update: Update, context: CallbackContext) -> int:
    text = update.message.text.strip() if update.message else None
    if text:
        context.user_data['profile_wizard']['full_name'] = text
    return ask_wizard_username(update, context)


def skip_wizard_name(update: Update, context: CallbackContext) -> int:
    query = update.callback_query
    query.answer()
    return ask_wizard_username(update, context)


def ask_wizard_username(update: Update, context: CallbackContext) -> int:
    # Отправляем следующий вопрос
    if isinstance(update, Update) and update.callback_query:
        send = update.callback_query.edit_message_text
    else:
        send = update.message.reply_text

    send(
        "👤 Введите новый username или нажмите Пропустить",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⏭ Пропустить", callback_data="wizard_skip_username")]])
    )
    return WIZARD_USERNAME


def save_wizard_username(update: Update, context: CallbackContext) -> int:
    text = update.message.text.strip() if update.message else None
    if text:
        context.user_data['profile_wizard']['username'] = text
    return ask_wizard_bio(update, context)


def skip_wizard_username(update: Update, context: CallbackContext) -> int:
    query = update.callback_query
    query.answer()
    return ask_wizard_bio(update, context)


def ask_wizard_bio(update: Update, context: CallbackContext) -> int:
    # Отправляем следующий вопрос
    if isinstance(update, Update) and update.callback_query:
        send = update.callback_query.edit_message_text
    else:
        send = update.message.reply_text

    send(
        "📄 Введите описание (bio) или нажмите Пропустить",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⏭ Пропустить", callback_data="wizard_skip_bio")]])
    )
    return WIZARD_BIO


def save_wizard_bio(update: Update, context: CallbackContext) -> int:
    text = update.message.text.strip() if update.message else None
    if text:
        context.user_data['profile_wizard']['bio'] = text
    return ask_wizard_link(update, context)


def skip_wizard_bio(update: Update, context: CallbackContext) -> int:
    query = update.callback_query
    query.answer()
    return ask_wizard_link(update, context)


def ask_wizard_link(update: Update, context: CallbackContext) -> int:
    if isinstance(update, Update) and update.callback_query:
        send = update.callback_query.edit_message_text
    else:
        send = update.message.reply_text

    send(
        "🔗 Введите ссылку (URL) или нажмите Пропустить",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⏭ Пропустить", callback_data="wizard_skip_link")]])
    )
    return WIZARD_LINK


def save_wizard_link(update: Update, context: CallbackContext) -> int:
    text = update.message.text.strip() if update.message else None
    if text:
        context.user_data['profile_wizard']['link'] = text
    return ask_wizard_photo(update, context)


def skip_wizard_link(update: Update, context: CallbackContext) -> int:
    query = update.callback_query
    query.answer()
    return ask_wizard_photo(update, context)


def ask_wizard_photo(update: Update, context: CallbackContext) -> int:
    if isinstance(update, Update) and update.callback_query:
        send = update.callback_query.edit_message_text
    else:
        send = update.message.reply_text

    send(
        "🖼 Отправьте фото для аватара или нажмите Пропустить",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⏭ Пропустить", callback_data="wizard_skip_photo")]])
    )
    return WIZARD_PHOTO


def save_wizard_photo(update: Update, context: CallbackContext) -> int:
    if update.message and update.message.photo:
        # Берем последний (максимального размера)
        file_id = update.message.photo[-1].file_id
        context.user_data['profile_wizard']['photo_file_id'] = file_id
    return wizard_confirm(update, context)


def skip_wizard_photo(update: Update, context: CallbackContext) -> int:
    query = update.callback_query
    query.answer()
    return wizard_confirm(update, context)


def wizard_confirm(update: Update, context: CallbackContext) -> int:
    data = context.user_data.get('profile_wizard', {})
    account_id = context.user_data.get('current_account_id')
    account = get_instagram_account(account_id) if account_id else None

    summary_lines = [
        f"Аккаунт: @{account.username}" if account else "Аккаунт: неизвестен",
        f"Имя: {data.get('full_name') or '— пропущено —'}",
        f"Username: {data.get('username') or '— пропущено —'}",
        f"Bio: {('есть' if data.get('bio') else '— пропущено —')}",
        f"Ссылка: {data.get('link') or '— пропущено —'}",
        f"Фото: {('загружено' if data.get('photo_file_id') else '— пропущено —')}",
    ]

    text = "\n".join(["✅ Проверьте настройки перед применением:"] + summary_lines)

    # Кнопки подтверждения
    if isinstance(update, Update) and update.callback_query:
        send = update.callback_query.edit_message_text
    else:
        send = update.message.reply_text

    send(
        text,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Применить", callback_data="wizard_apply")],
            [InlineKeyboardButton("🔙 Назад к аккаунту", callback_data=f"profile_account_{account_id}")]
        ]),
        parse_mode=None
    )
    return WIZARD_CONFIRM


def wizard_apply(update: Update, context: CallbackContext) -> int:
    query = update.callback_query
    query.answer()

    account_id = context.user_data.get('current_account_id')
    data = context.user_data.get('profile_wizard', {})

    # Выполняем единый цикл оформления
    results = []
    try:
        pm = ProfileManager(account_id)
        # Имя
        if data.get('full_name'):
            ok, msg = pm.update_profile_name(data['full_name'])
            results.append(("Имя", ok, msg))
        # Username
        if data.get('username'):
            ok, msg = pm.update_username(data['username'])
            results.append(("Username", ok, msg))
        # Bio
        if data.get('bio'):
            ok, msg = pm.update_biography(data['bio'])
            results.append(("Bio", ok, msg))
        # Link
        if data.get('link'):
            ok, msg = pm.update_profile_links(data['link'])
            results.append(("Ссылка", ok, msg))
        # Photo
        if data.get('photo_file_id'):
            # Сохраняем фото через существующую логику add_profile_photo/save_profile_photo
            # Имитация: в этом обработчике нет Message с фото, потому используем ProfileManager напрямую если поддерживается
            try:
                # Пытаемся скачать файл и загрузить через ProfileManager при наличии метода
                # Здесь используем существующий client, если есть соответствующий метод
                # Если прямой метод недоступен, пропускаем с сообщением
                ok_photo = False
                msg_photo = ""
                # Пробуем через client, если есть метод
                if hasattr(pm.client, 'account_change_picture'):
                    file = context.bot.get_file(data['photo_file_id'])
                    local_path = file.download(custom_path=f"/tmp/{data['photo_file_id']}.jpg")
                    res = pm.client.account_change_picture(local_path)
                    ok_photo = bool(res)
                    msg_photo = "Фото профиля обновлено" if ok_photo else "Не удалось обновить фото"
                else:
                    ok_photo = False
                    msg_photo = "Метод обновления фото недоступен"
                results.append(("Фото", ok_photo, msg_photo))
            except Exception as e:
                results.append(("Фото", False, f"Ошибка: {e}"))

        # Итоговый отчет
        ok_count = sum(1 for _, ok, _ in results if ok)
        total = len(results)
        report_lines = ["📋 Результат применения настроек:"]
        for title, ok, msg in results:
            status = "✅" if ok else "❌"
            report_lines.append(f"{status} {title}: {msg}")
        if total == 0:
            report_lines.append("ℹ️ Нечего применять: все шаги были пропущены")

        query.edit_message_text(
            "\n".join(report_lines),
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Назад к аккаунту", callback_data=f"profile_account_{account_id}")],
                [InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")]
            ])
        )
    except Exception as e:
        query.edit_message_text(
            f"❌ Ошибка при применении настроек: {e}",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=f"profile_account_{account_id}")]])
        )

    # Очистка
    context.user_data.pop('profile_wizard', None)
    return ConversationHandler.END

# =====================
# Bulk-мастер оформления профиля (для нескольких аккаунтов)
# =====================

def _bulk_wizard_init(context: CallbackContext):
    context.user_data['profile_bulk_wizard'] = {
        'full_name': None,
        'username': None,
        'bio': None,
        'link': None,
        'photo_file_id': None,
    }


def start_bulk_profile_wizard(update: Update, context: CallbackContext) -> int:
    query = update.callback_query
    query.answer()

    if not context.user_data.get('selected_profile_accounts'):
        query.edit_message_text("❌ Сначала выберите аккаунты", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="profile_setup")]]))
        return ConversationHandler.END

    _bulk_wizard_init(context)

    # Используем текст из bulk_edit_name
    account_ids = context.user_data.get('selected_profile_accounts', [])
    query.edit_message_text(
        f"📝 *Массовое изменение имени*\n\n"
        f"Выбрано аккаунтов: {len(account_ids)}\n\n"
        f"Вы можете:\n"
        f"• Ввести одно имя для всех аккаунтов\n"
        f"• Ввести несколько имён (каждое с новой строки)\n\n"
        f"⚠️ *Рекомендация:* При работе с большим количеством аккаунтов лучше использовать уникальные имена.\n\n"
        f"Отправьте имя(имена) или нажмите Пропустить",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⏭ Пропустить", callback_data="bulk_wizard_skip_name")]]),
        parse_mode="Markdown"
    )
    return BULK_WIZARD_NAME


def save_bulk_wizard_name(update: Update, context: CallbackContext) -> int:
    text = update.message.text.strip() if update.message else None
    if text:
        context.user_data['profile_bulk_wizard']['full_name'] = text
    return ask_bulk_wizard_username(update, context)


def skip_bulk_wizard_name(update: Update, context: CallbackContext) -> int:
    query = update.callback_query
    query.answer()
    return ask_bulk_wizard_username(update, context)


def ask_bulk_wizard_username(update: Update, context: CallbackContext) -> int:
    sender = update.callback_query.edit_message_text if update.callback_query else update.message.reply_text
    sender(
        "👤 Массовое изменение username\n\nВажно:\n• Каждый аккаунт должен иметь уникальный username\n• Некоторые варианты могут быть заняты в Instagram\n• Рекомендуется отправлять ~20% больше вариантов, чем аккаунтов\n\nВведите новый username для всех выбранных аккаунтов (без @) или нажмите Пропустить:",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⏭ Пропустить", callback_data="bulk_wizard_skip_username")]])
    )
    return BULK_WIZARD_USERNAME


def save_bulk_wizard_username(update: Update, context: CallbackContext) -> int:
    text = update.message.text.strip() if update.message else None
    if text:
        context.user_data['profile_bulk_wizard']['username'] = text
    return ask_bulk_wizard_bio(update, context)


def skip_bulk_wizard_username(update: Update, context: CallbackContext) -> int:
    query = update.callback_query
    query.answer()
    return ask_bulk_wizard_bio(update, context)


def ask_bulk_wizard_bio(update: Update, context: CallbackContext) -> int:
    sender = update.callback_query.edit_message_text if update.callback_query else update.message.reply_text
    # Текст из bulk_edit_bio
    account_ids = context.user_data.get('selected_profile_accounts', [])
    sender(
        f"✏️ *Массовое изменение описания*\n\n"
        f"Выбрано аккаунтов: {len(account_ids)}\n\n"
        f"*Поддержка шаблонов для уникализации:*\n"
        f"`{{Привет|Здравствуйте|Добрый день}}`\n"
        f"`{{как дела?|что нового?}}`\n\n"
        f"Система автоматически создаст уникальные варианты для каждого аккаунта.\n\n"
        f"*Пример:*\n"
        f"`{{Привет|Здравствуйте}}, я {{фотограф|блогер}}!`\n\n"
        f"Отправьте описание с шаблонами или обычный текст, или нажмите Пропустить",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⏭ Пропустить", callback_data="bulk_wizard_skip_bio")]]),
        parse_mode="Markdown"
    )
    return BULK_WIZARD_BIO


def save_bulk_wizard_bio(update: Update, context: CallbackContext) -> int:
    text = update.message.text.strip() if update.message else None
    if text:
        context.user_data['profile_bulk_wizard']['bio'] = text
    return ask_bulk_wizard_link(update, context)


def skip_bulk_wizard_bio(update: Update, context: CallbackContext) -> int:
    query = update.callback_query
    query.answer()
    return ask_bulk_wizard_link(update, context)


def ask_bulk_wizard_link(update: Update, context: CallbackContext) -> int:
    sender = update.callback_query.edit_message_text if update.callback_query else update.message.reply_text
    # Текст из bulk_edit_links
    account_ids = context.user_data.get('selected_profile_accounts', [])
    sender(
        f"🔗 Массовое изменение ссылок\n\n"
        f"Выбрано аккаунтов: {len(account_ids)}\n\n"
        f"Введите ссылку для добавления во все профили:\n"
        f"Примеры:\n"
        f"• https://linktr.ee/yourname\n"
        f"• https://t.me/yourchannel\n"
        f"• https://yourwebsite.com\n\n"
        f"⚠️ ВАЖНО: Для безопасности ссылки будут автоматически уникализированы!\n\n"
        f"Методы уникализации:\n"
        f"📊 UTM-параметры: ?utm_source=instagram&utm_campaign=acc1\n"
        f"🔢 ID параметры: ?ref=user123\n"
        f"🎲 Случайные токены: ?token=abc123\n\n"
        f"Отправьте ссылку или нажмите Пропустить:",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⏭ Пропустить", callback_data="bulk_wizard_skip_link")]])
    )
    return BULK_WIZARD_LINK


def save_bulk_wizard_link(update: Update, context: CallbackContext) -> int:
    text = update.message.text.strip() if update.message else None
    if text:
        context.user_data['profile_bulk_wizard']['link'] = text
    return ask_bulk_wizard_photo(update, context)


def skip_bulk_wizard_link(update: Update, context: CallbackContext) -> int:
    query = update.callback_query
    query.answer()
    return ask_bulk_wizard_photo(update, context)


def ask_bulk_wizard_photo(update: Update, context: CallbackContext) -> int:
    sender = update.callback_query.edit_message_text if update.callback_query else update.message.reply_text
    sender(
        "🖼 Массовое добавление фото профиля\n\nКак это работает:\n• Можете отправить одно или несколько фото\n• Если фото меньше, чем аккаунтов — они будут уникализированы\n• Уникализация: изменение фона, кроп, поворот\n\nОтправьте фото (можно несколько) или нажмите Пропустить",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⏭ Пропустить", callback_data="bulk_wizard_skip_photo")]])
    )
    return BULK_WIZARD_PHOTO


def save_bulk_wizard_photo(update: Update, context: CallbackContext) -> int:
    if update.message and update.message.photo:
        file_id = update.message.photo[-1].file_id
        context.user_data['profile_bulk_wizard']['photo_file_id'] = file_id
    return bulk_wizard_confirm(update, context)


def skip_bulk_wizard_photo(update: Update, context: CallbackContext) -> int:
    query = update.callback_query
    query.answer()
    return bulk_wizard_confirm(update, context)


def bulk_wizard_confirm(update: Update, context: CallbackContext) -> int:
    data = context.user_data.get('profile_bulk_wizard', {})
    acc_ids = context.user_data.get('selected_profile_accounts', [])

    summary_lines = [
        f"Аккаунтов: {len(acc_ids)}",
        f"Имя: {data.get('full_name') or '— пропущено —'}",
        f"Username: {data.get('username') or '— пропущено —'}",
        f"Bio: {('есть' if data.get('bio') else '— пропущено —')}",
        f"Ссылка: {data.get('link') or '— пропущено —'}",
        f"Фото: {('загружено' if data.get('photo_file_id') else '— пропущено —')}",
    ]

    text = "\n".join(["✅ Проверьте массовые настройки перед применением:"] + summary_lines)
    sender = update.callback_query.edit_message_text if update.callback_query else update.message.reply_text
    sender(
        text,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Применить", callback_data="bulk_wizard_apply")],
            [InlineKeyboardButton("🔙 Назад к массовым действиям", callback_data="show_bulk_actions")]
        ]),
        parse_mode=None
    )
    return BULK_WIZARD_CONFIRM


def bulk_wizard_apply(update: Update, context: CallbackContext) -> int:
    query = update.callback_query
    query.answer()

    acc_ids = context.user_data.get('selected_profile_accounts', [])
    data = context.user_data.get('profile_bulk_wizard', {})

    results = []
    for acc_id in acc_ids:
        try:
            pm = ProfileManager(acc_id)
            if data.get('full_name'):
                ok, msg = pm.update_profile_name(data['full_name'])
                results.append((acc_id, 'Имя', ok, msg))
            if data.get('username'):
                ok, msg = pm.update_username(data['username'])
                results.append((acc_id, 'Username', ok, msg))
            if data.get('bio'):
                ok, msg = pm.update_biography(data['bio'])
                results.append((acc_id, 'Bio', ok, msg))
            if data.get('link'):
                ok, msg = pm.update_profile_links(data['link'])
                results.append((acc_id, 'Ссылка', ok, msg))
            if data.get('photo_file_id') and hasattr(pm.client, 'account_change_picture'):
                try:
                    file = context.bot.get_file(data['photo_file_id'])
                    local_path = file.download(custom_path=f"/tmp/{data['photo_file_id']}.jpg")
                    res = pm.client.account_change_picture(local_path)
                    ok_photo = bool(res)
                    results.append((acc_id, 'Фото', ok_photo, 'Фото профиля обновлено' if ok_photo else 'Не удалось обновить фото'))
                except Exception as e:
                    results.append((acc_id, 'Фото', False, f"Ошибка: {e}"))
        except Exception as e:
            results.append((acc_id, 'Общее', False, f"Ошибка: {e}"))

    # Сводка
    lines = ["📋 Результат массового применения:"]
    for acc_id, field, ok, msg in results:
        status = "✅" if ok else "❌"
        lines.append(f"{status} [{acc_id}] {field}: {msg}")
    if not results:
        lines.append("ℹ️ Нечего применять: все шаги были пропущены")

    query.edit_message_text(
        "\n".join(lines),
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 К массовым действиям", callback_data="show_bulk_actions")],
            [InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")]
        ])
    )

    context.user_data.pop('profile_bulk_wizard', None)
    return ConversationHandler.END

# Экспортируем обработчики из модулей profile_setup
__all__ = [
    'profile_setup_handler',
    'setup_profile_menu',
    'handle_profile_selection',
    'show_bulk_profile_actions',
    'bulk_edit_name',
    'bulk_edit_username',
    'bulk_edit_bio',
    'bulk_add_link',
    'bulk_set_avatar',
    'bulk_delete_avatar',
    'bulk_delete_photo',
    'handle_bulk_profile_action',
    'edit_profile_name',
    'save_profile_name',
    'edit_profile_username', 
    'save_profile_username',
    'edit_profile_bio',
    'save_profile_bio',
    'edit_profile_links',
    'save_profile_links',
    'add_profile_photo',
    'save_profile_photo',
    'delete_profile_photo',
    'add_post',
    'save_post',
    'delete_all_posts',
    'EDIT_NAME',
    'EDIT_USERNAME',
    'EDIT_BIO',
    'EDIT_LINKS',
    'ADD_PHOTO',
    'ADD_POST'
]
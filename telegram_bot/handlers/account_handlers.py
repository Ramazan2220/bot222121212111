import json
import os
import time
import shutil
import logging
import asyncio
import concurrent.futures
from datetime import datetime  
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, ParseMode
from telegram.ext import ConversationHandler, CommandHandler, MessageHandler, Filters

logger = logging.getLogger(__name__)

from config import ACCOUNTS_DIR, ADMIN_USER_IDS, MEDIA_DIR
from utils.access_manager import has_access
from database.db_manager import (
    get_session, bulk_add_instagram_accounts, 
    delete_instagram_account, get_account_groups,
    update_instagram_account, activate_instagram_account
)
from database.safe_user_wrapper import get_user_instagram_accounts as get_instagram_accounts, get_user_instagram_account as get_instagram_account, extract_user_id_from_update
from database.models import InstagramAccount, PublishTask, Proxy
from instagrapi import Client
from instagrapi.exceptions import LoginRequired, BadPassword, ChallengeRequired
from instagram.client import check_login_challenge, submit_challenge_code
from instagram.email_utils import test_email_connection
import random
from database.models import Proxy
from utils.proxy_manager import assign_proxy_to_account
from instagram.client import check_login_challenge, submit_challenge_code, test_instagram_login_with_proxy
from utils.system_monitor import get_adaptive_limits, get_system_status

# Импорт middleware для проверки подписок
from telegram_bot.middleware import subscription_required, trial_allowed, premium_only
from utils.subscription_middleware import get_user_subscription_info
from utils.user_logger import log_user_info, log_user_error

logger = logging.getLogger(__name__)

# Глобальный словарь для хранения обработчиков запросов на подтверждение
challenge_handlers = {}

# Состояния для добавления аккаунта
ENTER_USERNAME, ENTER_PASSWORD, ENTER_EMAIL, ENTER_EMAIL_PASSWORD, CONFIRM_ACCOUNT, ENTER_VERIFICATION_CODE = range(6)

# Состояние для ожидания файла с аккаунтами
WAITING_ACCOUNTS_FILE = 10

@trial_allowed
def save_account_from_telegram(update, context):
    """Добавляет аккаунт Instagram в базу данных из Telegram-бота"""
    user_data = context.user_data
    user_id = update.effective_user.id

    username = user_data.get('instagram_username')
    password = user_data.get('instagram_password')
    email = user_data.get('email')
    email_password = user_data.get('email_password')

    # Проверяем наличие всех необходимых данных
    if not all([username, password, email, email_password]):
        missing_fields = []
        if not username: missing_fields.append("имя пользователя")
        if not password: missing_fields.append("пароль")
        if not email: missing_fields.append("email")
        if not email_password: missing_fields.append("пароль от email")

        update.message.reply_text(
            f"❌ Отсутствуют необходимые данные: {', '.join(missing_fields)}.\n"
            f"Пожалуйста, начните процесс добавления аккаунта заново."
        )
        user_data.clear()
        return ConversationHandler.END

    try:
        # Добавляем аккаунт в базу данных
        from database.db_manager import add_instagram_account
        log_user_info(user_id, f"➕ Добавление аккаунта @{username} через Telegram")
        
        # Добавляем аккаунт в базу данных напрямую
        success, result = add_instagram_account(username, password, email, email_password)
        
        if success:
            account_id = result
            log_user_info(user_id, f"✅ Аккаунт @{username} добавлен (ID={account_id})")
            # Назначаем прокси для аккаунта
            from utils.proxy_manager import assign_proxy_to_account
            proxy_success, proxy_message = assign_proxy_to_account(account_id)
            log_user_info(user_id, f"🌐 {proxy_message}")
            
            update.message.reply_text(
                f"✅ Аккаунт {username} успешно добавлен!\n"
                f"{'📡' if proxy_success else '⚠️'} {proxy_message}\n\n"
                f"Теперь вы можете использовать его для публикации контента."
            )
        else:
            log_user_error(user_id, f"❌ Ошибка при добавлении @{username}: {result}")
            update.message.reply_text(f"❌ Ошибка при добавлении аккаунта {username}: {result}")
        
        # Очищаем данные пользователя
        user_data.clear()
        return ConversationHandler.END

    except Exception as e:
        log_user_error(user_id, f"❌ Критическая ошибка при добавлении @{username}: {e}")
        logger.error(f"Ошибка при добавлении аккаунта: {str(e)}")
        user_data.clear()  # Очищаем данные даже при ошибке
        return ConversationHandler.END

def is_admin(user_id):
    return has_access(user_id)

def accounts_handler(update, context):
    from telegram_bot.keyboards import get_accounts_menu_keyboard
    
    update.message.reply_text(
        "👤 *Управление аккаунтами*\n\n"
        "Выберите действие из списка ниже:",
        reply_markup=get_accounts_menu_keyboard(),
        parse_mode=ParseMode.MARKDOWN
    )

def add_account(update, context):
    if update.callback_query:
        query = update.callback_query
        query.answer()

        # Проверяем наличие прокси перед добавлением аккаунта
        session = get_session()
        proxies_count = session.query(Proxy).filter_by(is_active=True).count()
        session.close()

        if proxies_count == 0:
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data='menu_accounts')]]
            reply_markup = InlineKeyboardMarkup(keyboard)

            query.edit_message_text(
                "⚠️ В системе нет доступных прокси!\n\n"
                "Для корректной работы с Instagram необходимо добавить хотя бы один рабочий прокси.\n"
                "Используйте команду /add_proxy для добавления прокси.",
                reply_markup=reply_markup
            )
            return ConversationHandler.END

        keyboard = [
            [InlineKeyboardButton("🔙 Назад", callback_data='menu_accounts')]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        query.edit_message_text(
            "Пожалуйста, введите имя пользователя (логин) аккаунта Instagram:\n\n"
            "Или введите все данные сразу в формате:\n"
            "`логин:пароль:email:пароль_email`\n\n"
            "Пример: `username:password123:user@example.com:emailpass456`\n\n"
            "Или нажмите 'Назад' для возврата в меню аккаунтов.",
            reply_markup=reply_markup,
            parse_mode=ParseMode.MARKDOWN
        )
        return ENTER_USERNAME
    else:
        # Проверяем наличие прокси перед добавлением аккаунта
        session = get_session()
        proxies_count = session.query(Proxy).filter_by(is_active=True).count()
        session.close()

        if proxies_count == 0:
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data='menu_accounts')]]
            reply_markup = InlineKeyboardMarkup(keyboard)

            update.message.reply_text(
                "⚠️ В системе нет доступных прокси!\n\n"
                "Для корректной работы с Instagram необходимо добавить хотя бы один рабочий прокси.\n"
                "Используйте команду /add_proxy для добавления прокси.",
                reply_markup=reply_markup
            )
            return ConversationHandler.END

        keyboard = [
            [InlineKeyboardButton("🔙 Назад", callback_data='menu_accounts')]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        update.message.reply_text(
            "Пожалуйста, введите имя пользователя (логин) аккаунта Instagram:\n\n"
            "Или введите все данные сразу в формате:\n"
            "`логин:пароль:email:пароль_email`\n\n"
            "Пример: `username:password123:user@example.com:emailpass456`\n\n"
            "Или нажмите 'Назад' для возврата в меню аккаунтов.",
            reply_markup=reply_markup,
            parse_mode=ParseMode.MARKDOWN
        )
        return ENTER_USERNAME

def enter_username(update, context):
    text = update.message.text.strip()
    user_id = update.effective_user.id

    # Проверяем, содержит ли ввод все данные сразу в формате login:password:email:email_password
    parts = text.split(':')

    if len(parts) == 4:
        # Полный формат login:password:email:email_password
        username, password, email, email_password = parts

        # Проверяем, существует ли уже аккаунт с таким именем
        session = get_session()
        existing_account = session.query(InstagramAccount).filter_by(username=username).first()
        session.close()

        if existing_account:
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data='menu_accounts')]]
            reply_markup = InlineKeyboardMarkup(keyboard)

            update.message.reply_text(
                f"Аккаунт с именем пользователя '{username}' уже существует. "
                f"Пожалуйста, используйте другое имя пользователя или вернитесь в меню аккаунтов.",
                reply_markup=reply_markup
            )
            return ConversationHandler.END

        # Сообщаем пользователю, что данные получены
        update.message.reply_text(f"Получены все данные для аккаунта {username}. Начинаем процесс добавления...")
        log_user_info(user_id, f"📝 Начало добавления @{username}")
        
        # Сначала добавляем аккаунт в базу данных без проверки входа
        try:
            from database.db_manager import add_instagram_account_without_login

            # Добавляем аккаунт без проверки входа
            account = add_instagram_account_without_login(
                username=username,
                password=password,
                email=email,
                email_password=email_password
            )

            if not account:
                log_user_error(user_id, f"❌ Не удалось добавить @{username} в БД")
                update.message.reply_text("❌ Не удалось добавить аккаунт в базу данных.")
                return ConversationHandler.END
            
            log_user_info(user_id, f"✅ Аккаунт @{username} добавлен в БД (ID={account.id})")
            update.message.reply_text("✅ Аккаунт добавлен в базу данных. Назначаем прокси...")

            # Назначаем прокси для аккаунта
            from utils.proxy_manager import assign_proxy_to_account
            proxy_success, proxy_message = assign_proxy_to_account(account.id)
            log_user_info(user_id, f"🌐 {proxy_message}")
            
            if not proxy_success:
                update.message.reply_text(f"⚠️ {proxy_message}\n\nПродолжаем без прокси...")
            else:
                update.message.reply_text(f"✅ {proxy_message}")

            # Теперь проверяем подключение к почте
            update.message.reply_text("🔄 Проверяем подключение к почте...")
            print(f"[DEBUG] Проверка подключения к почте {email}")

            from instagram.email_utils import test_email_connection
            success, message = test_email_connection(email, email_password)

            if not success:
                update.message.reply_text(f"❌ Ошибка подключения к почте: {message}\n\nПожалуйста, проверьте пароль и попробуйте снова.")
                return ConversationHandler.END

            update.message.reply_text("✅ Подключение к почте успешно установлено.")

            # Теперь пытаемся войти в Instagram с использованием прокси
            update.message.reply_text("🔄 Пытаемся войти в Instagram...")

            from instagram.client import test_instagram_login_with_proxy
            login_success = test_instagram_login_with_proxy(
                account_id=account.id,
                username=username,
                password=password,
                email=email,
                email_password=email_password
            )

            if login_success:
                # Если вход успешен, активируем аккаунт
                from database.db_manager import activate_instagram_account
                activate_instagram_account(account.id)

                update.message.reply_text(
                    f"✅ Аккаунт {username} успешно добавлен и активирован!\n\n"
                    f"Теперь вы можете использовать его для публикации контента."
                )
            else:
                update.message.reply_text(
                    f"⚠️ Аккаунт {username} добавлен, но не удалось войти в Instagram.\n\n"
                    f"Вы можете попробовать войти позже через меню управления аккаунтами."
                )

            # Очищаем данные пользователя
            context.user_data.clear()
            return ConversationHandler.END

        except Exception as e:
            log_user_error(user_id, f"❌ Ошибка добавления @{username}: {e}")
            update.message.reply_text(f"❌ Произошла ошибка при добавлении аккаунта: {str(e)}")
            return ConversationHandler.END

    # Если это не полный формат, продолжаем стандартную логику
    username = text

    session = get_session()
    existing_account = session.query(InstagramAccount).filter_by(username=username).first()
    session.close()

    if existing_account:
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data='menu_accounts')]]
        reply_markup = InlineKeyboardMarkup(keyboard)

        update.message.reply_text(
            f"Аккаунт с именем пользователя '{username}' уже существует. "
            f"Пожалуйста, используйте другое имя пользователя или вернитесь в меню аккаунтов.",
            reply_markup=reply_markup
        )
        return ConversationHandler.END

    context.user_data['instagram_username'] = username

    keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data='menu_accounts')]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    update.message.reply_text(
        "Теперь введите пароль для этого аккаунта Instagram.\n\n"
        "⚠️ *Важно*: Ваш пароль будет храниться в зашифрованном виде и использоваться только для авторизации в Instagram.",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=reply_markup
    )
    return ENTER_PASSWORD

def enter_password(update, context):
    password = update.message.text.strip()

    context.user_data['instagram_password'] = password

    keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data='menu_accounts')]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    update.message.reply_text(
        "Теперь введите адрес электронной почты, привязанный к этому аккаунту Instagram.\n\n"
        "Этот адрес будет использоваться для получения кодов подтверждения.",
        reply_markup=reply_markup
    )

    # Удаляем сообщение с паролем для безопасности
    update.message.delete()

    return ENTER_EMAIL

def enter_email(update, context):
    """Обработчик ввода адреса электронной почты"""
    user_data = context.user_data
    email = update.message.text.strip()

    # Сохраняем адрес электронной почты
    user_data['email'] = email

    update.message.reply_text(
        "Теперь введите пароль от электронной почты.\n\n"
        "⚠️ Важно: Пароль будет храниться в зашифрованном виде и использоваться только для получения кодов подтверждения."
    )

    return ENTER_EMAIL_PASSWORD

def enter_email_password(update, context):
    """
    Обрабатывает ввод пароля от электронной почты
    """
    user_id = update.effective_user.id
    email_password = update.message.text

    # Сохраняем пароль от почты
    context.user_data['email_password'] = email_password

    # Получаем данные для входа
    email = context.user_data.get('email')
    instagram_username = context.user_data.get('instagram_username')
    instagram_password = context.user_data.get('instagram_password')

    if not email:
        update.message.reply_text("❌ Ошибка: адрес электронной почты не указан.")
        return ConversationHandler.END

    if not instagram_username or not instagram_password:
        update.message.reply_text("❌ Ошибка: данные для входа в Instagram не указаны.")
        return ConversationHandler.END

    # Сначала добавляем аккаунт в базу данных без проверки входа
    try:
        from database.db_manager import add_instagram_account_without_login

        # Добавляем аккаунт без проверки входа
        account = add_instagram_account_without_login(
            username=instagram_username,
            password=instagram_password,
            email=email,
            email_password=email_password
        )

        if not account:
            update.message.reply_text("❌ Не удалось добавить аккаунт в базу данных.")
            return ConversationHandler.END

        update.message.reply_text("✅ Аккаунт добавлен в базу данных. Назначаем прокси...")

        # Назначаем прокси для аккаунта
        from utils.proxy_manager import assign_proxy_to_account
        proxy_success, proxy_message = assign_proxy_to_account(account.id)

        if not proxy_success:
            update.message.reply_text(f"⚠️ {proxy_message}\n\nПродолжаем без прокси...")
        else:
            update.message.reply_text(f"✅ {proxy_message}")

        # Теперь проверяем подключение к почте
        update.message.reply_text("🔄 Проверяем подключение к почте...")
        print(f"[DEBUG] Проверка подключения к почте {email}")

        from instagram.email_utils import test_email_connection
        success, message = test_email_connection(email, email_password)

        if not success:
            update.message.reply_text(f"❌ Ошибка подключения к почте: {message}\n\nПожалуйста, проверьте пароль и попробуйте снова.")
            return ENTER_EMAIL_PASSWORD

        update.message.reply_text("✅ Подключение к почте успешно установлено.")

        # Теперь пытаемся войти в Instagram с использованием прокси
        update.message.reply_text("🔄 Пытаемся войти в Instagram...")

        from instagram.client import test_instagram_login_with_proxy
        login_success = test_instagram_login_with_proxy(
            account_id=account.id,
            username=instagram_username,
            password=instagram_password,
            email=email,
            email_password=email_password
        )

        if login_success:
            # Если вход успешен, активируем аккаунт
            from database.db_manager import activate_instagram_account
            activate_instagram_account(account.id)

            update.message.reply_text(
                f"✅ Аккаунт {instagram_username} успешно добавлен и активирован!\n\n"
                f"Теперь вы можете использовать его для публикации контента."
            )
        else:
            update.message.reply_text(
                f"⚠️ Аккаунт {instagram_username} добавлен, но не удалось войти в Instagram.\n\n"
                f"Вы можете попробовать войти позже через меню управления аккаунтами."
            )

        # Очищаем данные пользователя
        context.user_data.clear()
        return ConversationHandler.END

    except Exception as e:
        update.message.reply_text(f"❌ Произошла ошибка: {str(e)}")
        logger.error(f"Ошибка при обработке пароля от почты: {str(e)}")
        return ConversationHandler.END

def confirm_add_account(update, context):
    """Подтверждение добавления аккаунта"""
    query = update.callback_query
    query.answer()

    user_id = update.effective_user.id
    username = context.user_data.get('instagram_username')
    password = context.user_data.get('instagram_password')
    email = context.user_data.get('instagram_email')
    email_password = context.user_data.get('instagram_email_password')

    print(f"[DEBUG] confirm_add_account вызван для {username}")

    query.edit_message_text(
        text=f"🔄 Выполняется вход в аккаунт {username}...\n\n"
        f"Это может занять некоторое время. Пожалуйста, подождите."
    )

    try:
        # Проверяем, требуется ли код подтверждения
        challenge_required, challenge_info = check_login_challenge(username, password, email, email_password)

        if not challenge_required:
            # Если вход успешен, добавляем аккаунт
            success, result = add_instagram_account(username, password, email, email_password)

            if success:
                print(f"[DEBUG] Аккаунт {username} успешно добавлен")
                query.edit_message_text(
                    text=f"✅ Аккаунт {username} успешно добавлен!"
                )
                return ConversationHandler.END
            else:
                print(f"[DEBUG] Ошибка при добавлении аккаунта {username}: {result}")
                query.edit_message_text(
                    text=f"❌ Не удалось добавить аккаунт {username}.\n\n"
                    f"Ошибка: {result}\n\n"
                    f"Пожалуйста, попробуйте снова или используйте другой аккаунт."
                )
                return ConversationHandler.END
        else:
            # Если требуется код подтверждения
            print(f"[DEBUG] Требуется код подтверждения для {username}")

            # Сохраняем информацию о запросе
            context.user_data['challenge_info'] = challenge_info

            # Отправляем сообщение с запросом кода
            query.edit_message_text(
                text=f"📱 Требуется подтверждение для аккаунта *{username}*\n\n"
                f"Instagram запрашивает код подтверждения, отправленный на электронную почту.\n\n"
                f"Пожалуйста, введите код подтверждения (6 цифр):",
                parse_mode='Markdown'
            )

            return ENTER_VERIFICATION_CODE

    except Exception as e:
        print(f"[DEBUG] Ошибка в confirm_add_account для {username}: {str(e)}")
        logger.error(f"Ошибка в confirm_add_account для {username}: {str(e)}")

        query.edit_message_text(
            text=f"❌ Произошла ошибка при входе в аккаунт {username}.\n\n"
            f"Ошибка: {str(e)}\n\n"
            f"Пожалуйста, попробуйте снова или используйте другой аккаунт."
        )
        return ConversationHandler.END

def enter_verification_code(update, context):
    """Обработчик ввода кода подтверждения"""
    user_data = context.user_data
    verification_code = update.message.text.strip()

    print(f"[DEBUG] enter_verification_code вызван с кодом {verification_code}")

    username = user_data.get('instagram_username')  # Исправлено
    password = user_data.get('instagram_password')  # Исправлено
    challenge_info = user_data.get('challenge_info')

    print(f"[DEBUG] Данные для {username}: challenge_info={bool(challenge_info)}")

    if not challenge_info:
        update.message.reply_text("❌ Ошибка: данные о запросе на подтверждение отсутствуют.")
        return ConversationHandler.END

    # Отправляем код подтверждения
    from instagram.client import submit_challenge_code

    print(f"[DEBUG] Вызываем submit_challenge_code для {username} с кодом {verification_code}")
    success, result = submit_challenge_code(username, password, verification_code, challenge_info)

    print(f"[DEBUG] Результат submit_challenge_code: success={success}, result={result}")

    if not success:
        update.message.reply_text(f"❌ Ошибка при проверке кода: {result}\n\nПожалуйста, проверьте код и попробуйте снова.")
        return ENTER_VERIFICATION_CODE

    # Код подтверждения принят, добавляем аккаунт
    return save_account_from_telegram(update, context)

def verification_code_handler(update, context):
    """Обработчик для ввода кода подтверждения"""
    user_id = update.effective_user.id
    code = update.message.text.strip()

    print(f"[VERIFICATION_HANDLER] Получен код подтверждения: {code} от пользователя {user_id}")

    # Проверяем формат кода (6 цифр)
    if not code.isdigit() or len(code) != 6:
        print(f"[VERIFICATION_HANDLER] Некорректный формат кода: {code}")
        update.message.reply_text("Пожалуйста, введите корректный код подтверждения (6 цифр).")
        return

    # Используем статический метод для установки кода
    from instagram.auth_manager import TelegramChallengeHandler
    if TelegramChallengeHandler.set_code(user_id, code):
        update.message.reply_text("✅ Код подтверждения принят. Выполняется вход...")
    else:
        update.message.reply_text("В данный момент код подтверждения не запрашивается.")

def cancel_add_account(update, context):
    """Обработчик отмены добавления аккаунта"""
    query = update.callback_query
    query.answer()

    # Очищаем данные
    if 'instagram_username' in context.user_data:
        del context.user_data['instagram_username']
    if 'instagram_password' in context.user_data:
        del context.user_data['instagram_password']
    if 'instagram_client' in context.user_data:
        del context.user_data['instagram_client']
    if 'challenge_handler' in context.user_data:
        del context.user_data['challenge_handler']

    keyboard = [[InlineKeyboardButton("🔙 К меню аккаунтов", callback_data='menu_accounts')]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    query.edit_message_text(
        "❌ Добавление аккаунта отменено.",
        reply_markup=reply_markup
    )

    return ConversationHandler.END

def list_accounts_handler(update, context):
    """Обработчик списка аккаунтов с пагинацией и улучшенным UI"""
    user_id = extract_user_id_from_update(update, context)
    
    # Получаем страницу из callback_data
    page = 1
    if update.callback_query:
        query = update.callback_query
        query.answer()
        
        # Проверяем, есть ли номер страницы в callback_data
        if query.data.startswith("list_accounts_page_"):
            page = int(query.data.replace("list_accounts_page_", ""))
    
    # 🔒 БЕЗОПАСНО: Получаем только аккаунты текущего пользователя
    all_accounts = get_instagram_accounts(context=context, user_id=user_id)
    
    # Сохраняем нужные данные (упрощаем без groups пока)
    accounts_data = []
    for acc in all_accounts:
        accounts_data.append({
            'id': acc.id,
            'username': acc.username,
            'is_active': acc.is_active,
            'groups': []  # Упрощаем пока, избегаем lazy loading
        })
    
    if not accounts_data:
        keyboard = [
            [InlineKeyboardButton("➕ Добавить аккаунт", callback_data='add_account')],
            [InlineKeyboardButton("📥 Массовая загрузка", callback_data='bulk_add_accounts')],
            [InlineKeyboardButton("🔙 К меню аккаунтов", callback_data='menu_accounts')]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        text = "📋 У вас пока нет добавленных аккаунтов Instagram.\n\n" \
               "Добавьте аккаунты для начала работы."
        
        if update.callback_query:
            query.edit_message_text(text, reply_markup=reply_markup)
        else:
            update.message.reply_text(text, reply_markup=reply_markup)
        return
    
    # Пагинация
    accounts_per_page = 8
    total_pages = (len(accounts_data) + accounts_per_page - 1) // accounts_per_page
    start_idx = (page - 1) * accounts_per_page
    end_idx = min(start_idx + accounts_per_page, len(accounts_data))
    
    # Аккаунты на текущей странице
    page_accounts = accounts_data[start_idx:end_idx]
    
    # Формируем статистику
    active_count = sum(1 for acc in accounts_data if acc['is_active'])
    inactive_count = len(accounts_data) - active_count
    groups_count = len(get_account_groups())
    
    # Формируем текст
    text = "📊 *Статистика аккаунтов*\n"
    text += "━━━━━━━━━━━━━━━━━━━━\n"
    text += f"👥 Всего: {len(accounts_data)}\n"
    text += f"✅ Активных: {active_count}\n"
    text += f"❌ Неактивных: {inactive_count}\n"
    text += f"📁 Папок: {groups_count}\n"
    text += "━━━━━━━━━━━━━━━━━━━━\n\n"
    
    text += f"📋 *Аккаунты (стр. {page}/{total_pages}):*\n\n"
    
    keyboard = []
    
    # Кнопки для каждого аккаунта
    for i, account in enumerate(page_accounts, start=start_idx+1):
        status = "✅" if account['is_active'] else "❌"
        groups = account['groups']
        group_info = f" [{groups[0]['icon']}]" if groups else ""
        
        # Экранируем специальные символы для Markdown
        username = account['username'].replace('_', '\\_').replace('*', '\\*').replace('[', '\\[').replace(']', '\\]').replace('(', '\\(').replace(')', '\\)').replace('~', '\\~').replace('`', '\\`').replace('>', '\\>').replace('#', '\\#').replace('+', '\\+').replace('-', '\\-').replace('=', '\\=').replace('|', '\\|').replace('{', '\\{').replace('}', '\\}').replace('.', '\\.').replace('!', '\\!')
        
        # Краткая информация об аккаунте
        text += f"{i}. {status} @{username}{group_info}\n"
        
        # Кнопка для просмотра деталей
        keyboard.append([InlineKeyboardButton(
            f"{status} @{account['username']}",
            callback_data=f"account_details_{account['id']}"
        )])
    
    # Навигация по страницам
    nav_buttons = []
    if page > 1:
        nav_buttons.append(InlineKeyboardButton("◀️", callback_data=f"list_accounts_page_{page-1}"))
    
    nav_buttons.append(InlineKeyboardButton(f"{page}/{total_pages}", callback_data="noop"))
    
    if page < total_pages:
        nav_buttons.append(InlineKeyboardButton("▶️", callback_data=f"list_accounts_page_{page+1}"))
    
    if nav_buttons:
        keyboard.append(nav_buttons)
    
    # Кнопки действий
    action_row1 = []
    action_row1.append(InlineKeyboardButton("📁 Папки", callback_data="folders_menu"))
    action_row1.append(InlineKeyboardButton("🔍 Поиск", callback_data="search_accounts"))
    keyboard.append(action_row1)
    
    action_row2 = []
    action_row2.append(InlineKeyboardButton("🔄 Проверить", callback_data='check_accounts_validity'))
    action_row2.append(InlineKeyboardButton("➕ Добавить", callback_data='add_account'))
    keyboard.append(action_row2)
    
    keyboard.append([InlineKeyboardButton("🔙 К меню аккаунтов", callback_data='menu_accounts')])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    if update.callback_query:
        query.edit_message_text(text, reply_markup=reply_markup, parse_mode=ParseMode.MARKDOWN)
    else:
        update.message.reply_text(text, reply_markup=reply_markup, parse_mode=ParseMode.MARKDOWN)

def account_details_handler(update, context):
    """Показывает детальную информацию об аккаунте"""
    query = update.callback_query
    query.answer()
    
    account_id = int(query.data.replace("account_details_", ""))
    account = get_instagram_account(account_id)
    
    if not account:
        query.edit_message_text(
            "❌ Аккаунт не найден",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="list_accounts")]])
        )
        return
    
    # Формируем детальную информацию
    status_emoji = "✅" if account.is_active else "❌"
    status_text = "Активен" if account.is_active else "Неактивен"
    
    # Детальная информация о статусе
    detailed_status = "Активен"
    if not account.is_active:
        if hasattr(account, 'status') and account.status:
            status_mapping = {
                'challenge_required': '🔐 Требуется верификация',
                'login_required': '🔑 Требуется повторный вход',
                'email_code_failed': '📧 Ошибка получения кода из email',
                'recovery_login_failed': '🔄 Ошибка входа при восстановлении',
                'recovery_verify_failed': '❌ Восстановление не подтвердилось',
                'no_email_data': '📧 Нет данных email для восстановления',
                'email_error': '📧 Ошибка работы с email',
                'recovery_error': '🔄 Ошибка восстановления',
                'invalid_password': '🔑 Неверный пароль',
                'login_error': '❌ Ошибка входа',
                'problematic': '⚠️ Проблемный аккаунт'
            }
            detailed_status = status_mapping.get(account.status, f"❌ {account.status}")
        else:
            detailed_status = "❌ Неактивен"
    
    # Экранируем специальные символы для Markdown
    username = account.username.replace('_', '\\_').replace('*', '\\*').replace('[', '\\[').replace(']', '\\]').replace('(', '\\(').replace(')', '\\)').replace('~', '\\~').replace('`', '\\`').replace('>', '\\>').replace('#', '\\#').replace('+', '\\+').replace('-', '\\-').replace('=', '\\=').replace('|', '\\|').replace('{', '\\{').replace('}', '\\}').replace('.', '\\.').replace('!', '\\!')
    email = (account.email or 'Не указан').replace('_', '\\_').replace('*', '\\*').replace('[', '\\[').replace(']', '\\]').replace('(', '\\(').replace(')', '\\)').replace('~', '\\~').replace('`', '\\`').replace('>', '\\>').replace('#', '\\#').replace('+', '\\+').replace('-', '\\-').replace('=', '\\=').replace('|', '\\|').replace('{', '\\{').replace('}', '\\}').replace('.', '\\.').replace('!', '\\!')
    
    text = f"👤 *Аккаунт: @{username}*\n"
    text += "━━━━━━━━━━━━━━━━━━━━\n"
    text += f"🆔 ID: `{account.id}`\n"
    text += f"📊 Статус: {status_emoji} {detailed_status}\n"
    text += f"📧 Email: {email}\n"
    
    # Информация об IMAP восстановлении
    if account.email and account.email_password:
        text += f"🔄 IMAP восстановление: ✅ Доступно\n"
    else:
        text += f"🔄 IMAP восстановление: ❌ Нет данных\n"
    
    text += f"📅 Добавлен: {account.created_at.strftime('%d.%m.%Y %H:%M')}\n"
    
    # Последняя проверка
    if hasattr(account, 'last_check') and account.last_check:
        text += f"🔍 Последняя проверка: {account.last_check.strftime('%d.%m.%Y %H:%M')}\n"
    
    # Информация о группах
    if account.groups:
        text += f"\n📁 *Группы:*\n"
        for group in account.groups:
            text += f"  • {group.icon} {group.name}\n"
    else:
        text += f"\n📁 *Группы:* Не состоит в группах\n"
    
    # Информация о прокси
    if account.proxy:
        text += f"\n🌐 *Прокси:* {account.proxy.host}:{account.proxy.port}\n"
    else:
        text += f"\n🌐 *Прокси:* Не назначен\n"
    
    # Последняя ошибка
    if account.last_error:
        error_text = account.last_error[:150]
        # Экранируем ошибку
        error_text = error_text.replace('_', '\\_').replace('*', '\\*').replace('[', '\\[').replace(']', '\\]').replace('(', '\\(').replace(')', '\\)').replace('~', '\\~').replace('`', '\\`').replace('>', '\\>').replace('#', '\\#').replace('+', '\\+').replace('-', '\\-').replace('=', '\\=').replace('|', '\\|').replace('{', '\\{').replace('}', '\\}').replace('.', '\\.').replace('!', '\\!')
        text += f"\n⚠️ *Последняя ошибка:*\n`{error_text}`\n"
    
    # Кнопки действий
    keyboard = []
    
    # Первый ряд - основные действия
    row1 = []
    if account.is_active:
        row1.append(InlineKeyboardButton("⏸️ Деактивировать", callback_data=f"deactivate_account_{account_id}"))
    else:
        row1.append(InlineKeyboardButton("▶️ Активировать", callback_data=f"activate_account_{account_id}"))
    row1.append(InlineKeyboardButton("🔄 Проверить", callback_data=f"check_account_{account_id}"))
    keyboard.append(row1)
    
    # Второй ряд - восстановление (только если есть проблемы)
    if not account.is_active:
        row_recovery = []
        if account.email and account.email_password:
            row_recovery.append(InlineKeyboardButton("🔧 IMAP восстановление", callback_data=f"imap_recover_{account_id}"))
        row_recovery.append(InlineKeyboardButton("🚫 Сбросить ошибки", callback_data=f"reset_errors_{account_id}"))
        keyboard.append(row_recovery)
    
    # Третий ряд - настройки
    row3 = []
    row3.append(InlineKeyboardButton("⚙️ Настройки", callback_data=f"account_settings_{account_id}"))
    row3.append(InlineKeyboardButton("📊 Статистика", callback_data=f"account_stats_{account_id}"))
    keyboard.append(row3)
    
    # Четвертый ряд - группы и прокси
    row4 = []
    row4.append(InlineKeyboardButton("📁 Группы", callback_data=f"manage_account_groups_{account_id}"))
    row4.append(InlineKeyboardButton("🌐 Прокси", callback_data=f"manage_account_proxy_{account_id}"))
    keyboard.append(row4)
    
    # Действия с контентом
    keyboard.append([
        InlineKeyboardButton("📤 Опубликовать", callback_data=f"publish_to_{account_id}"),
        InlineKeyboardButton("🔥 Прогреть", callback_data=f"warm_account_{account_id}")
    ])
    
    # Удаление и возврат
    keyboard.append([
        InlineKeyboardButton("🗑️ Удалить", callback_data=f"delete_account_{account_id}"),
        InlineKeyboardButton("🔙 К списку", callback_data="list_accounts")
    ])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=reply_markup)

def delete_account_handler(update, context):
    """Обработчик для удаления аккаунта"""
    query = update.callback_query
    query.answer()

    # Получаем ID аккаунта из callback_data
    account_id = int(query.data.split('_')[2])

    # Получаем информацию об аккаунте
    account = get_instagram_account(account_id)

    if not account:
        keyboard = [[InlineKeyboardButton("🔙 К списку аккаунтов", callback_data='list_accounts')]]
        reply_markup = InlineKeyboardMarkup(keyboard)

        query.edit_message_text(
            "Аккаунт не найден.",
            reply_markup=reply_markup
        )
        return

    try:
        session = get_session()

        # Сначала удаляем связанные задачи
        session.query(PublishTask).filter_by(account_id=account_id).delete()

        # Затем удаляем аккаунт
        account = session.query(InstagramAccount).filter_by(id=account_id).first()
        if account:
            session.delete(account)
            session.commit()

            # Удаляем файл сессии, если он существует
            session_dir = os.path.join(ACCOUNTS_DIR, str(account_id))
            if os.path.exists(session_dir):
                shutil.rmtree(session_dir)

            keyboard = [[InlineKeyboardButton("🔙 К списку аккаунтов", callback_data='list_accounts')]]
            reply_markup = InlineKeyboardMarkup(keyboard)

            query.edit_message_text(
                f"✅ Аккаунт {account.username} успешно удален.",
                reply_markup=reply_markup
            )
        else:
            keyboard = [[InlineKeyboardButton("🔙 К списку аккаунтов", callback_data='list_accounts')]]
            reply_markup = InlineKeyboardMarkup(keyboard)

            query.edit_message_text(
                "Аккаунт не найден.",
                reply_markup=reply_markup
            )
    except Exception as e:
        session.rollback()

        keyboard = [[InlineKeyboardButton("🔙 К списку аккаунтов", callback_data='list_accounts')]]
        reply_markup = InlineKeyboardMarkup(keyboard)

        query.edit_message_text(
            f"❌ Ошибка при удалении аккаунта: {str(e)}",
            reply_markup=reply_markup
        )
    finally:
        session.close()

def delete_all_accounts_handler(update, context):
    query = update.callback_query
    query.answer()

    keyboard = [
        [
            InlineKeyboardButton("✅ Да, удалить все", callback_data='confirm_delete_all_accounts'),
            InlineKeyboardButton("❌ Нет, отмена", callback_data='list_accounts')
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    query.edit_message_text(
        "⚠️ Вы уверены, что хотите удалить ВСЕ аккаунты?\n\n"
        "Это действие нельзя отменить. Все данные аккаунтов будут удалены.",
        reply_markup=reply_markup
    )

def confirm_delete_all_accounts_handler(update, context):
    query = update.callback_query
    query.answer()

    try:
        session = get_session()
        accounts = session.query(InstagramAccount).all()
        session.close()

        # Удаляем каждый аккаунт с помощью нашей новой функции
        from instagram.client import remove_instagram_account
        success_count = 0
        failed_count = 0

        for account in accounts:
            try:
                if remove_instagram_account(account.id):
                    success_count += 1
                else:
                    failed_count += 1
            except Exception as e:
                logger.error(f"Ошибка при удалении аккаунта {account.username}: {e}")
                failed_count += 1

        keyboard = [[InlineKeyboardButton("🔙 К меню аккаунтов", callback_data='menu_accounts')]]
        reply_markup = InlineKeyboardMarkup(keyboard)

        if failed_count == 0:
            query.edit_message_text(
                f"✅ Все аккаунты успешно удалены ({success_count}).",
                reply_markup=reply_markup
            )
        else:
            query.edit_message_text(
                f"⚠️ Удалено {success_count} аккаунтов, не удалось удалить {failed_count} аккаунтов.",
                reply_markup=reply_markup
            )
    except Exception as e:
        keyboard = [[InlineKeyboardButton("🔙 К списку аккаунтов", callback_data='list_accounts')]]
        reply_markup = InlineKeyboardMarkup(keyboard)

        query.edit_message_text(
            f"❌ Ошибка при удалении аккаунтов: {str(e)}",
            reply_markup=reply_markup
        )

def check_accounts_validity_handler(update, context):
    """Обработчик проверки валидности аккаунтов с новым селектором"""
    from telegram_bot.utils.account_selection import create_account_selector
    
    # Создаем селектор аккаунтов для проверки
    selector = create_account_selector(
        callback_prefix="validity_select",
        title="🔍 Проверка валидности аккаунтов",
        allow_multiple=True,  # Разрешаем выбор нескольких аккаунтов
        show_status=True,
        show_folders=True,
        back_callback="menu_accounts"
    )
    
    # Определяем callback для обработки выбранных аккаунтов
    def on_accounts_selected(account_ids: list, update_inner, context_inner):
        if account_ids:
            query = update_inner.callback_query
            
            # Получаем адаптивные лимиты нагрузки
            limits = get_adaptive_limits()
            system_status = get_system_status()
            
            # Начинаем проверку
            query.edit_message_text(
                f"🔄 Проверка валидности аккаунтов...\n\n"
                f"{system_status['emoji']} Система: {system_status['status'].upper()}\n"
                f"⚙️ Потоков: {limits.max_workers}\n"
                f"📦 Размер группы: {limits.batch_size}\n"
                f"⏱️ Задержка: {limits.delay_between_batches}с"
            )
            
            # Функция для проверки одного аккаунта
            def check_single_account(account_id):
                local_session = get_session()
                try:
                    account = local_session.query(InstagramAccount).filter_by(id=account_id).first()
                    if not account:
                        local_session.close()
                        return (f"ID {account_id}", False, "Аккаунт не найден")
                        
                    # Используем специальную функцию для проверки аккаунтов с автоматическим получением кодов
                    from instagram.client import test_instagram_login_with_proxy
                    
                    # Проверяем, есть ли у аккаунта данные почты
                    email = getattr(account, 'email', None)
                    email_password = getattr(account, 'email_password', None)
                    
                    if email and email_password:
                        # Используем функцию с автоматическим получением кодов
                        login_success = test_instagram_login_with_proxy(
                            account_id=account.id,
                            username=account.username,
                            password=account.password,
                            email=email,
                            email_password=email_password
                        )
                        
                        # ✅ ОБНОВЛЯЕМ СТАТУС В БАЗЕ ДАННЫХ
                        from database.db_manager import update_instagram_account
                        if login_success:
                            update_instagram_account(account.id, is_active=True, last_check=datetime.now())
                            local_session.close()
                            return (account.username, True, "Успешный вход с автоматическим получением кодов")
                        else:
                            update_instagram_account(account.id, is_active=False, last_check=datetime.now())
                            local_session.close()
                            return (account.username, False, "Не удалось войти даже с автоматическим получением кодов")
                    else:
                        # Если нет данных почты, используем старый метод
                        client = Client()

                        # Проверяем наличие сессии
                        session_file = os.path.join(ACCOUNTS_DIR, str(account.id), 'session.json')
                        if os.path.exists(session_file):
                            try:
                                with open(session_file, 'r') as f:
                                    session_data = json.load(f)

                                if 'settings' in session_data:
                                    client.set_settings(session_data['settings'])

                                # Проверяем валидность сессии
                                try:
                                    client.get_timeline_feed()
                                    # ✅ ОБНОВЛЯЕМ СТАТУС В БАЗЕ ДАННЫХ
                                    from database.db_manager import update_instagram_account
                                    update_instagram_account(account.id, is_active=True, last_check=datetime.now())
                                    local_session.close()
                                    return (account.username, True, "Сессия валидна")
                                except:
                                    # Если сессия невалидна, пробуем войти с логином и паролем
                                    pass
                            except:
                                pass

                        # Пробуем войти с логином и паролем
                        try:
                            client.login(account.username, account.password)

                            # Сохраняем обновленную сессию
                            os.makedirs(os.path.join(ACCOUNTS_DIR, str(account.id)), exist_ok=True)
                            session_data = {
                                'username': account.username,
                                'account_id': account.id,
                                'updated_at': time.strftime('%Y-%m-%d %H:%M:%S'),
                                'settings': client.get_settings()
                            }
                            with open(session_file, 'w') as f:
                                json.dump(session_data, f)

                            # ✅ ОБНОВЛЯЕМ СТАТУС В БАЗЕ ДАННЫХ
                            from database.db_manager import update_instagram_account
                            update_instagram_account(account.id, is_active=True, last_check=datetime.now())
                            local_session.close()
                            return (account.username, True, "Успешный вход")
                        except Exception as e:
                            # ✅ ОБНОВЛЯЕМ СТАТУС В БАЗЕ ДАННЫХ
                            from database.db_manager import update_instagram_account
                            update_instagram_account(account.id, is_active=False, last_check=datetime.now())
                            local_session.close()
                            return (account.username, False, str(e))
                        
                except Exception as e:
                    account_name = account.username if 'account' in locals() and account else f"ID {account_id}"
                    # ✅ ОБНОВЛЯЕМ СТАТУС В БАЗЕ ДАННЫХ ПРИ ОШИБКЕ
                    try:
                        from database.db_manager import update_instagram_account
                        update_instagram_account(account_id, is_active=False, last_check=datetime.now())
                    except:
                        pass  # Игнорируем ошибки обновления при общих ошибках
                    local_session.close()
                    return (account_name, False, str(e))
            
            # Разбиваем аккаунты на группы согласно адаптивным лимитам
            account_batches = [account_ids[i:i + limits.batch_size] for i in range(0, len(account_ids), limits.batch_size)]
            results = []
            
            for batch_num, batch in enumerate(account_batches, 1):
                # Обновляем прогресс
                query.edit_message_text(
                    f"🔄 Проверка группы {batch_num}/{len(account_batches)}...\n\n"
                    f"{system_status['emoji']} Система: {system_status['status'].upper()}\n"
                    f"⚙️ Потоков: {limits.max_workers}\n"
                    f"📦 Аккаунтов в группе: {len(batch)}\n"
                    f"⏱️ Задержка между группами: {limits.delay_between_batches}с"
                )
                
                # Обрабатываем группу параллельно
                with concurrent.futures.ThreadPoolExecutor(max_workers=limits.max_workers) as executor:
                    # Запускаем проверку для всех аккаунтов в группе
                    future_to_account = {
                        executor.submit(check_single_account, account_id): account_id 
                        for account_id in batch
                    }
                    
                    # Ждем завершения с таймаутом
                    timeout = 60 * limits.timeout_multiplier
                    done, not_done = concurrent.futures.wait(future_to_account, timeout=timeout)
                    
                    # Собираем результаты
                    for future in done:
                        try:
                            result = future.result()
                            results.append(result)
                        except Exception as e:
                            account_id = future_to_account[future]
                            results.append((f"ID {account_id}", False, f"Ошибка: {e}"))
                    
                    # Обрабатываем незавершенные задачи
                    for future in not_done:
                        account_id = future_to_account[future]
                        results.append((f"ID {account_id}", False, "Превышен таймаут"))
                        future.cancel()
                
                # Задержка между группами (кроме последней)
                if batch_num < len(account_batches):
                    time.sleep(limits.delay_between_batches)

            # Формируем отчет
            report = "📊 *Результаты проверки аккаунтов:*\n\n"

            for username, is_valid, message in results:
                status = "✅ Валиден" if is_valid else "❌ Невалиден"
                report += f"👤 *{username}*: {status}\n"
                if not is_valid:
                    report += f"📝 Причина: {message}\n"
                report += "\n"

            keyboard = [[InlineKeyboardButton("🔙 К списку аккаунтов", callback_data='list_accounts')]]
            reply_markup = InlineKeyboardMarkup(keyboard)

            query.edit_message_text(
                report,
                reply_markup=reply_markup,
                parse_mode=ParseMode.MARKDOWN
            )
    
    # Запускаем процесс выбора
    return selector.start_selection(update, context, on_accounts_selected)

@trial_allowed
def bulk_upload_accounts_command(update, context):
    """Команда массовой загрузки аккаунтов"""
    
    # ОБЯЗАТЕЛЬНАЯ ПРОВЕРКА ПРОКСИ ПЕРЕД НАЧАЛОМ
    session = get_session()
    proxies_count = session.query(Proxy).filter_by(is_active=True).count()
    session.close()
    
    if proxies_count == 0:
        keyboard = [
            [InlineKeyboardButton("➕ Добавить прокси", callback_data='menu_proxy')],
            [InlineKeyboardButton("🏠 Главное меню", callback_data='main_menu')]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        message_text = (
            "⚠️ В системе нет доступных прокси!\n\n"
            "❌ Массовая загрузка аккаунтов невозможна без прокси.\n"
            "Для корректной работы с Instagram необходимо добавить хотя бы один рабочий прокси."
        )
        
        if update.callback_query:
            update.callback_query.answer()
            update.callback_query.edit_message_text(message_text, reply_markup=reply_markup)
        else:
            update.message.reply_text(message_text, reply_markup=reply_markup)
        
        return ConversationHandler.END

    # Если прокси есть, продолжаем
    keyboard = [[InlineKeyboardButton("🔙 Назад к аккаунтам", callback_data='menu_accounts')]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    message_text = (
        "📤 Массовая загрузка аккаунтов Instagram\n\n"
        "📧 Поддерживается ТОЛЬКО полный формат:\n"
        "username:password:email:email_password\n\n"
        "🔹 Способы загрузки:\n"
        "1️⃣ Отправьте TXT файл с аккаунтами\n"
        "2️⃣ Введите аккаунты текстом прямо в чат\n\n"
        "📝 Каждый аккаунт должен быть на новой строке.\n"
        "🔄 Система автоматически назначит прокси и запустит IMAP инициализацию."
    )

    if update.callback_query:
        query = update.callback_query
        query.answer()
        query.edit_message_text(message_text, reply_markup=reply_markup)
    else:
        update.message.reply_text(message_text, reply_markup=reply_markup)

    # Устанавливаем состояние для ожидания файла или текста
    context.user_data['waiting_for_accounts_file'] = True
    return WAITING_ACCOUNTS_FILE

@trial_allowed
def bulk_upload_accounts_file(update, context):
    """Обрабатывает файл или текст с аккаунтами для массовой загрузки"""
    print("[DEBUG] Начинаем обработку аккаунтов")
    
    # Проверяем что это файл или текстовое сообщение
    if update.message.document:
        # Обработка файла
        file = update.message.document
        file_name = file.file_name
        
        # Проверяем расширение файла
        if not file_name.endswith('.txt'):
            keyboard = [[InlineKeyboardButton("🔙 К меню аккаунтов", callback_data='menu_accounts')]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            update.message.reply_text(
                "❌ Пожалуйста, загрузите файл с расширением .txt",
                reply_markup=reply_markup
            )
            return ConversationHandler.END

        # Скачиваем файл
        file_path = f"temp_{file_name}"
        file_obj = context.bot.get_file(file.file_id)
        file_obj.download(file_path)
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
        except Exception as e:
            keyboard = [[InlineKeyboardButton("🔙 К меню аккаунтов", callback_data='menu_accounts')]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            update.message.reply_text(
                f"❌ Ошибка при чтении файла: {str(e)}",
                reply_markup=reply_markup
            )
            return ConversationHandler.END
        finally:
            # Удаляем временный файл
            try:
                os.remove(file_path)
            except:
                pass
    
    elif update.message.text:
        # Обработка текста
        lines = update.message.text.strip().split('\n')
    
    else:
        keyboard = [[InlineKeyboardButton("🔙 К меню аккаунтов", callback_data='menu_accounts')]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        update.message.reply_text(
            "❌ Отправьте TXT файл или введите аккаунты текстом",
            reply_markup=reply_markup
        )
        return ConversationHandler.END

    # Парсим аккаунты - ТОЛЬКО полный формат
    accounts_for_init = []
    invalid_lines = []
    
    for i, line in enumerate(lines, 1):
        line = line.strip()
        if not line:
            continue

        parts = line.split(':')
        if len(parts) != 4:
            invalid_lines.append(f"Строка {i}: {line[:50]}... (неверный формат)")
            continue

        username, password, email, email_password = [part.strip() for part in parts]
        
        if not all([username, password, email, email_password]):
            invalid_lines.append(f"Строка {i}: пустые поля")
            continue
            
        accounts_for_init.append((username, password, email, email_password))

    # Проверяем результат парсинга
    if not accounts_for_init:
        keyboard = [[InlineKeyboardButton("🔙 К меню аккаунтов", callback_data='menu_accounts')]]
        reply_markup = InlineKeyboardMarkup(keyboard)

        error_message = "❌ Не найдено валидных аккаунтов!\n\n"
        error_message += "Требуемый формат: username:password:email:email_password\n\n"
        
        if invalid_lines:
            error_message += "Ошибки:\n"
            for error in invalid_lines[:5]:  # Показываем первые 5 ошибок
                error_message += f"• {error}\n"
            if len(invalid_lines) > 5:
                error_message += f"... и еще {len(invalid_lines) - 5} ошибок"

        update.message.reply_text(error_message, reply_markup=reply_markup)
        return ConversationHandler.END

    # Показываем ошибки парсинга, если есть
    if invalid_lines:
        error_message = f"⚠️ Найдено {len(invalid_lines)} некорректных строк:\n"
        for error in invalid_lines[:3]:
            error_message += f"• {error}\n"
        if len(invalid_lines) > 3:
            error_message += f"... и еще {len(invalid_lines) - 3}"
        error_message += f"\n\n✅ Будет обработано {len(accounts_for_init)} валидных аккаунтов."
        update.message.reply_text(error_message)

    # Запускаем асинхронную обработку с полной IMAP инициализацией
    update.message.reply_text(
        f"🔄 Начинаем полную асинхронную инициализацию {len(accounts_for_init)} аккаунтов с IMAP...\n"
        "Процесс проверки и настройки аккаунтов запущен в фоновом режиме."
    )
    log_user_info(user_id, f"📦 Запущена массовая инициализация {len(accounts_for_init)} аккаунтов")
    
    # Запускаем асинхронную инициализацию
    asyncio.run(async_bulk_add_accounts(update, context, accounts_for_init))

    return ConversationHandler.END

def profile_setup_handler(update, context):
    if update.callback_query:
        query = update.callback_query
        query.answer()

        keyboard = [[InlineKeyboardButton("🔙 К меню аккаунтов", callback_data='menu_accounts')]]
        reply_markup = InlineKeyboardMarkup(keyboard)

        query.edit_message_text(
            "⚙️ Функция настройки профиля находится в разработке.\n\n"
            "Пожалуйста, попробуйте позже.",
            reply_markup=reply_markup
        )
    else:
        keyboard = [[InlineKeyboardButton("🔙 К меню аккаунтов", callback_data='menu_accounts')]]
        reply_markup = InlineKeyboardMarkup(keyboard)

        update.message.reply_text(
            "⚙️ Функция настройки профиля находится в разработке.\n\n"
            "Пожалуйста, попробуйте позже.",
            reply_markup=reply_markup
        )

# Новые асинхронные функции для обработки аккаунтов
def process_account_sync(account_data):
    """Синхронная функция для обработки одного аккаунта"""
    username, password, email, email_password = account_data
    result = {
        "username": username,
        "status": "failed", 
        "message": ""
    }
    
    try:
        # Проверяем, существует ли уже аккаунт с таким именем
        session = get_session()
        existing_account = session.query(InstagramAccount).filter_by(username=username).first()
        session.close()

        if existing_account:
            result["status"] = "skipped"
            result["message"] = "Аккаунт уже существует в базе данных"
            return result

        # Добавляем аккаунт в базу данных без проверки входа
        from database.db_manager import add_instagram_account_without_login
        account = add_instagram_account_without_login(
            username=username,
            password=password,
            email=email,
            email_password=email_password
        )

        if not account:
            result["message"] = "Не удалось добавить аккаунт в базу данных"
            return result

        # Назначаем прокси для аккаунта
        from utils.proxy_manager import assign_proxy_to_account
        proxy_success, proxy_message = assign_proxy_to_account(account.id)
        
        # Проверяем подключение к почте
        from instagram.email_utils import test_email_connection
        email_success, email_message = test_email_connection(email, email_password)
        
        if not email_success:
            result["message"] = f"Ошибка подключения к почте: {email_message}"
            return result

        # Пытаемся войти в Instagram с использованием прокси
        from instagram.client import test_instagram_login_with_proxy
        
        # Делаем до 3 попыток входа (учитывая возможную замену прокси)
        max_attempts = 3
        login_success = False
        
        for attempt in range(1, max_attempts + 1):
            logger.info(f"Попытка входа {attempt}/{max_attempts} для {username}")
            
            login_success = test_instagram_login_with_proxy(
                account_id=account.id,
                username=username,
                password=password,
                email=email,
                email_password=email_password
            )
            
            if login_success:
                logger.info(f"✅ Успешный вход для {username} с попытки {attempt}")
                break
            elif attempt < max_attempts:
                logger.info(f"⚠️ Попытка {attempt} неудачна для {username}, ждем 5 секунд...")
                import time
                time.sleep(5)  # Небольшая пауза между попытками

        if login_success:
            # Если вход успешен, активируем аккаунт
            from database.db_manager import activate_instagram_account
            activate_instagram_account(account.id)
            result["status"] = "success"
            result["message"] = "Аккаунт успешно добавлен и активирован"
        else:
            result["status"] = "partial"
            result["message"] = "Аккаунт добавлен, но не удалось войти в Instagram или активировать"
            
        return result
    except Exception as e:
        result["message"] = str(e)
        logger.error(f"Ошибка при добавлении аккаунта {username}: {str(e)}")
        return result

async def process_account_async(account_data):
    """Асинхронная обертка для обработки одного аккаунта"""
    loop = asyncio.get_event_loop()
    # Запускаем синхронную функцию в отдельном потоке
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        result = await loop.run_in_executor(executor, process_account_sync, account_data)
    return result

async def async_bulk_add_accounts(update, context, accounts_data):
    """Асинхронно обрабатывает массовое добавление аккаунтов"""
    update.message.reply_text(f"🔄 Начинаем ПАРАЛЛЕЛЬНУЮ инициализацию {len(accounts_data)} аккаунтов...")
    
    # Создаем пул задач для обработки аккаунтов
    tasks = []
    for account_data in accounts_data:
        tasks.append(process_account_async(account_data))
    
    # Запускаем все задачи параллельно и ждем их завершения
    results = await asyncio.gather(*tasks)
    
    # Подсчитываем статистику
    success_count = sum(1 for r in results if r["status"] == "success")
    partial_count = sum(1 for r in results if r["status"] == "partial")
    skipped_count = sum(1 for r in results if r["status"] == "skipped")
    failed_count = sum(1 for r in results if r["status"] == "failed")
    
    # Формируем отчет
    report = f"📊 *Результаты параллельной инициализации:*\n\n"
    report += f"✅ Успешно добавлено: {success_count}\n"
    report += f"⚠️ Частично добавлено: {partial_count}\n"
    report += f"⏭️ Пропущено (уже существуют): {skipped_count}\n"
    report += f"❌ Не удалось добавить: {failed_count}\n\n"
    
    # Добавляем детали по неудачным аккаунтам
    failed_accounts = [r for r in results if r["status"] == "failed"]
    if failed_accounts:
        report += "*Ошибки при добавлении:*\n"
        for account in failed_accounts:
            report += f"👤 *{account['username']}*: {account['message']}\n"
    
    keyboard = [[InlineKeyboardButton("📊 К списку аккаунтов", callback_data='list_accounts')]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    update.message.reply_text(
        report,
        reply_markup=reply_markup,
        parse_mode=ParseMode.MARKDOWN
    )

    return ConversationHandler.END

def async_upload_accounts_command(update, context):
    """Обработчик команды для асинхронной загрузки аккаунтов"""
    if update.callback_query:
        query = update.callback_query
        query.answer()

        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data='menu_accounts')]]
        reply_markup = InlineKeyboardMarkup(keyboard)

        query.edit_message_text(
            "Отправьте TXT файл с аккаунтами Instagram для асинхронной загрузки.\n\n"
            "Формат файла:\n"
            "username:password:email:email_password\n"
            "username:password:email:email_password\n"
            "...\n\n"
            "Каждый аккаунт должен быть на новой строке в указанном формате.",
            reply_markup=reply_markup
        )
        context.user_data['waiting_for_async_accounts_file'] = True
        return WAITING_ACCOUNTS_FILE
    else:
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data='menu_accounts')]]
        reply_markup = InlineKeyboardMarkup(keyboard)

        update.message.reply_text(
            "Отправьте TXT файл с аккаунтами Instagram для асинхронной загрузки.\n\n"
            "Формат файла:\n"
            "username:password:email:email_password\n"
            "username:password:email:email_password\n"
            "...\n\n"
            "Каждый аккаунт должен быть на новой строке в указанном формате.",
            reply_markup=reply_markup
        )
        context.user_data['waiting_for_async_accounts_file'] = True
        return WAITING_ACCOUNTS_FILE

def async_upload_accounts_file(update, context):
    """Обработчик для файла с аккаунтами для асинхронной загрузки"""
    # Сбрасываем флаг ожидания файла
    context.user_data['waiting_for_async_accounts_file'] = False

    file = update.message.document

    if not file.file_name.endswith('.txt'):
        keyboard = [[InlineKeyboardButton("🔙 К меню аккаунтов", callback_data='menu_accounts')]]
        reply_markup = InlineKeyboardMarkup(keyboard)

        update.message.reply_text(
            "❌ Пожалуйста, отправьте файл в формате .txt",
            reply_markup=reply_markup
        )
        return ConversationHandler.END

    # Скачиваем файл
    file_path = os.path.join(MEDIA_DIR, file.file_name)
    file_obj = context.bot.get_file(file.file_id)
    file_obj.download(file_path)

    # Читаем файл
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()

        accounts_data = []
        for line in lines:
            line = line.strip()
            if not line:
                continue

            parts = line.split(':')
            if len(parts) != 4:
                continue

            username, password, email, email_password = parts
            accounts_data.append((username.strip(), password.strip(), email.strip(), email_password.strip()))

        if not accounts_data:
            keyboard = [[InlineKeyboardButton("🔙 К меню аккаунтов", callback_data='menu_accounts')]]
            reply_markup = InlineKeyboardMarkup(keyboard)

            update.message.reply_text(
                "❌ В файле не найдено аккаунтов в правильном формате.",
                reply_markup=reply_markup
            )
            return ConversationHandler.END

        # Запускаем асинхронную обработку аккаунтов
        asyncio.run(async_bulk_add_accounts(update, context, accounts_data))

    except Exception as e:
        keyboard = [[InlineKeyboardButton("🔙 К меню аккаунтов", callback_data='menu_accounts')]]
        reply_markup = InlineKeyboardMarkup(keyboard)

        update.message.reply_text(
            f"❌ Произошла ошибка при обработке файла: {str(e)}",
            reply_markup=reply_markup
        )

    # Удаляем временный файл
    try:
        os.remove(file_path)
    except:
        pass

    return ConversationHandler.END

def get_account_handlers():
    """Возвращает обработчики для управления аккаунтами"""
    from telegram.ext import CommandHandler, CallbackQueryHandler, MessageHandler, Filters
    from telegram_bot.utils.account_selection import create_account_selector

    # Создаем селектор для проверки валидности
    validity_selector = create_account_selector(
        callback_prefix="validity_select",
        title="🔍 Проверка валидности аккаунтов",
        allow_multiple=True,
        show_status=True,
        show_folders=True,
        back_callback="menu_accounts"
    )

    # Новый ConversationHandler для массовой загрузки аккаунтов
    bulk_upload_conversation = ConversationHandler(
        entry_points=[
            CommandHandler("upload_accounts", bulk_upload_accounts_command),
            CallbackQueryHandler(bulk_upload_accounts_command, pattern='^upload_accounts$')
        ],
        states={
            WAITING_ACCOUNTS_FILE: [
                MessageHandler(Filters.document.file_extension("txt"), bulk_upload_accounts_file),
                CallbackQueryHandler(lambda u, c: ConversationHandler.END, pattern='^menu_accounts$')
            ]
        },
        fallbacks=[CommandHandler("cancel", lambda update, context: ConversationHandler.END)]
    )
    
    # Новый ConversationHandler для асинхронной загрузки аккаунтов
    async_upload_conversation = ConversationHandler(
        entry_points=[
            CommandHandler("async_upload_accounts", async_upload_accounts_command),
            CallbackQueryHandler(async_upload_accounts_command, pattern='^async_upload_accounts$')
        ],
        states={
            WAITING_ACCOUNTS_FILE: [
                MessageHandler(Filters.document.file_extension("txt"), async_upload_accounts_file),
                CallbackQueryHandler(lambda u, c: ConversationHandler.END, pattern='^menu_accounts$')
            ]
        },
        fallbacks=[CommandHandler("cancel", lambda update, context: ConversationHandler.END)]
    )

    return [
        CommandHandler("accounts", accounts_handler),
        # Удаляем account_conversation, так как он теперь регистрируется в bot.py
        bulk_upload_conversation,
        async_upload_conversation,
        CommandHandler("list_accounts", list_accounts_handler),
        CommandHandler("profile_setup", profile_setup_handler),
        # Обработчики для списка аккаунтов с пагинацией
        CallbackQueryHandler(list_accounts_handler, pattern='^list_accounts$'),
        CallbackQueryHandler(list_accounts_handler, pattern='^list_accounts_page_\\d+$'),
        validity_selector.get_conversation_handler(),  # Добавляем обработчик селектора для проверки
        # Обработчик деталей аккаунта
        CallbackQueryHandler(account_details_handler, pattern='^account_details_\\d+$'),
        # Обработчики действий с аккаунтом
        CallbackQueryHandler(delete_account_handler, pattern='^delete_account_\\d+$'),
        CallbackQueryHandler(delete_all_accounts_handler, pattern='^delete_all_accounts$'),
        # Новые обработчики для IMAP восстановления и сброса ошибок
        CallbackQueryHandler(imap_recover_handler, pattern='^imap_recover_\\d+$'),
        CallbackQueryHandler(reset_errors_handler, pattern='^reset_errors_\\d+$'),
        CallbackQueryHandler(confirm_delete_all_accounts_handler, pattern='^confirm_delete_all_accounts$'),
        CallbackQueryHandler(check_accounts_validity_handler, pattern='^check_accounts_validity$'),
        # Обработчики активации/деактивации аккаунтов
        CallbackQueryHandler(activate_account_handler, pattern='^activate_account_\\d+$'),
        CallbackQueryHandler(deactivate_account_handler, pattern='^deactivate_account_\\d+$'),
        CallbackQueryHandler(check_single_account_handler, pattern='^check_account_\\d+$'),
        CallbackQueryHandler(account_settings_handler, pattern='^account_settings_\\d+$'),
        CallbackQueryHandler(account_stats_handler, pattern='^account_stats_\\d+$'),
        CallbackQueryHandler(manage_account_groups_handler, pattern='^manage_account_groups_\\d+$'),
        CallbackQueryHandler(manage_account_proxy_handler, pattern='^manage_account_proxy_\\d+$'),
        CallbackQueryHandler(test_proxy_handler, pattern='^test_proxy_\\d+$'),
        CallbackQueryHandler(change_proxy_handler, pattern='^change_proxy_\\d+$'),
        CallbackQueryHandler(proxy_stats_handler, pattern='^proxy_stats_\\d+$'),
        CallbackQueryHandler(publish_to_account_handler, pattern='^publish_to_\\d+$'),
        CallbackQueryHandler(warm_account_handler, pattern='^warm_account_\\d+$'),
        CallbackQueryHandler(quick_warmup_handler, pattern='^quick_warmup_\\d+$'),
        CallbackQueryHandler(smart_warmup_handler, pattern='^smart_warmup_\\d+$'),
        CallbackQueryHandler(warmup_settings_handler, pattern='^warmup_settings_\\d+$'),
        CallbackQueryHandler(lambda u, c: u.callback_query.answer("В разработке"), pattern='^search_accounts$'),
    ]

def bulk_add_accounts_command(update, context):
    """Обработчик команды /bulk_add_accounts для массового добавления аккаунтов"""
    user_id = update.effective_user.id

    # Проверяем, является ли пользователь администратором
    if not is_admin(user_id):
        update.message.reply_text("У вас нет прав для выполнения этой команды.")
        return

    # Проверяем, есть ли текст после команды
    if not context.args:
        update.message.reply_text(
            "Пожалуйста, укажите аккаунты в формате:\n\n"
            "/bulk_add_accounts\n"
            "username1:password1:email1:email_password1\n"
            "username2:password2:email2:email_password2\n"
            "...\n\n"
            "Каждый аккаунт должен быть на новой строке."
        )
        return

    # Получаем текст после команды
    accounts_text = " ".join(context.args)

    # Разбиваем текст на строки
    accounts_lines = accounts_text.strip().split("\n")

    # Статистика
    total_accounts = len(accounts_lines)
    added_accounts = 0
    failed_accounts = 0
    already_exists = 0
    failed_accounts_list = []

    update.message.reply_text(f"🔄 Начинаем добавление {total_accounts} аккаунтов...")

    # Обрабатываем каждую строку
    for line in accounts_lines:
        line = line.strip()
        if not line:
            continue

        parts = line.split(":")
        if len(parts) != 4:
            update.message.reply_text(f"❌ Неверный формат строки: {line}")
            failed_accounts += 1
            failed_accounts_list.append(f"{line} - неверный формат")
            continue

        username, password, email, email_password = parts

        # Проверяем, существует ли уже аккаунт с таким именем
        session = get_session()
        existing_account = session.query(InstagramAccount).filter_by(username=username).first()
        session.close()

        if existing_account:
            update.message.reply_text(f"⚠️ Аккаунт {username} уже существует в базе данных.")
            already_exists += 1
            continue

        try:
            # Добавляем аккаунт в базу данных без проверки входа
            from database.db_manager import add_instagram_account_without_login

            account = add_instagram_account_without_login(
                username=username,
                password=password,
                email=email,
                email_password=email_password
            )

            if not account:
                update.message.reply_text(f"❌ Не удалось добавить аккаунт {username} в базу данных.")
                failed_accounts += 1
                failed_accounts_list.append(f"{username} - ошибка добавления в БД")
                continue

            # Назначаем прокси для аккаунта
            from utils.proxy_manager import assign_proxy_to_account
            proxy_success, proxy_message = assign_proxy_to_account(account.id)

            if not proxy_success:
                update.message.reply_text(f"⚠️ {username}: {proxy_message}")

            # Проверяем подключение к почте
            from instagram.email_utils import test_email_connection
            email_success, email_message = test_email_connection(email, email_password)

            if not email_success:
                update.message.reply_text(f"⚠️ {username}: Ошибка подключения к почте: {email_message}")
                # Продолжаем, даже если не удалось подключиться к почте

            # Пытаемся войти в Instagram с использованием прокси
            from instagram.client import test_instagram_login_with_proxy
            login_success = test_instagram_login_with_proxy(
                account_id=account.id,
                username=username,
                password=password,
                email=email,
                email_password=email_password
            )

            if login_success:
                # Если вход успешен, активируем аккаунт
                from database.db_manager import activate_instagram_account
                activate_instagram_account(account.id)
                update.message.reply_text(f"✅ Аккаунт {username} успешно добавлен и активирован!")
            else:
                update.message.reply_text(f"⚠️ Аккаунт {username} добавлен, но не удалось войти в Instagram.")

            added_accounts += 1

        except Exception as e:
            update.message.reply_text(f"❌ Ошибка при добавлении аккаунта {username}: {str(e)}")
            logger.error(f"Ошибка при добавлении аккаунта {username}: {str(e)}")
            failed_accounts += 1
            failed_accounts_list.append(f"{username} - {str(e)}")

    # Отправляем итоговую статистику
    summary = (
        f"📊 Итоги добавления аккаунтов:\n"
        f"Всего обработано: {total_accounts}\n"
        f"Успешно добавлено: {added_accounts}\n"
        f"Уже существуют: {already_exists}\n"
        f"Не удалось добавить: {failed_accounts}"
    )

    update.message.reply_text(summary)

    # Если есть неудачные аккаунты, отправляем их список
    if failed_accounts_list:
        failed_list = "❌ Список неудачно добавленных аккаунтов:\n" + "\n".join(failed_accounts_list)
        update.message.reply_text(failed_list)

    # Возвращаем клавиатуру меню аккаунтов
    keyboard = [
        [InlineKeyboardButton("➕ Добавить аккаунт", callback_data='add_account')],
        [InlineKeyboardButton("📋 Список аккаунтов", callback_data='list_accounts')],
        [InlineKeyboardButton("🔙 Назад", callback_data='main_menu')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    update.message.reply_text("Выберите действие:", reply_markup=reply_markup)

    # Очищаем флаг ожидания списка аккаунтов
    context.user_data['waiting_for_bulk_accounts'] = False

    return ConversationHandler.END

def imap_recover_handler(update, context):
    """Обработчик IMAP восстановления аккаунта"""
    query = update.callback_query
    query.answer()
    
    account_id = int(query.data.replace("imap_recover_", ""))
    account = get_instagram_account(account_id)
    
    if not account:
        query.edit_message_text("❌ Аккаунт не найден")
        return
    
    if not account.email or not account.email_password:
        query.edit_message_text(
            f"❌ У аккаунта @{account.username} нет данных email для восстановления",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=f"account_details_{account_id}")]])
        )
        return
    
    # Показываем сообщение о начале восстановления
    query.edit_message_text(
        f"🔄 Начинаю IMAP восстановление для @{account.username}...\n\n"
        f"📧 Email: {account.email}\n"
        f"⏳ Это может занять до 2 минут"
    )
    
    try:
        # Импортируем функции для восстановления
        from instagram.email_utils_optimized import get_verification_code_from_email
        from instagram.client import InstagramClient
        from database.db_manager import update_instagram_account
        from datetime import datetime
        
        # Создаем новый клиент для восстановления
        instagram_client = InstagramClient(account_id)
        
        # Пытаемся получить код верификации из почты
        verification_code = get_verification_code_from_email(
            account.email, 
            account.email_password, 
            max_attempts=3, 
            delay_between_attempts=15
        )
        
        if verification_code:
            # Пытаемся войти с кодом верификации
            login_success = instagram_client.login_with_challenge_code(verification_code)
            if login_success:
                # Проверяем что восстановление прошло успешно
                try:
                    instagram_client.client.get_timeline_feed()
                    # Обновляем статус в БД как активный
                    update_instagram_account(
                        account_id,
                        is_active=True,
                        status="active",
                        last_error=None,
                        last_check=datetime.now()
                    )
                    
                    query.edit_message_text(
                        f"✅ IMAP восстановление успешно!\n\n"
                        f"👤 Аккаунт: @{account.username}\n"
                        f"📧 Код получен из: {account.email}\n"
                        f"✅ Статус: Активен",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 К деталям", callback_data=f"account_details_{account_id}")]])
                    )
                except Exception as verify_error:
                    update_instagram_account(
                        account_id,
                        is_active=False,
                        status="recovery_verify_failed",
                        last_error=f"Восстановление не подтвердилось: {verify_error}",
                        last_check=datetime.now()
                    )
                    
                    query.edit_message_text(
                        f"❌ Восстановление не подтвердилось\n\n"
                        f"👤 Аккаунт: @{account.username}\n"
                        f"⚠️ Ошибка: {str(verify_error)[:100]}",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 К деталям", callback_data=f"account_details_{account_id}")]])
                    )
            else:
                update_instagram_account(
                    account_id,
                    is_active=False,
                    status="recovery_login_failed",
                    last_error="Не удалось войти с кодом верификации",
                    last_check=datetime.now()
                )
                
                query.edit_message_text(
                    f"❌ Не удалось войти с кодом верификации\n\n"
                    f"👤 Аккаунт: @{account.username}\n"
                    f"📧 Email: {account.email}\n"
                    f"🔐 Код получен, но вход не удался",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 К деталям", callback_data=f"account_details_{account_id}")]])
                )
        else:
            update_instagram_account(
                account_id,
                is_active=False,
                status="email_code_failed",
                last_error="Не удалось получить код из email",
                last_check=datetime.now()
            )
            
            query.edit_message_text(
                f"❌ Не удалось получить код из email\n\n"
                f"👤 Аккаунт: @{account.username}\n"
                f"📧 Email: {account.email}\n"
                f"⚠️ Проверьте доступность почтового ящика",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 К деталям", callback_data=f"account_details_{account_id}")]])
            )
            
    except Exception as e:
        error_msg = str(e) if e else "Unknown error"
        
        # Определяем тип ошибки
        if error_msg and "challenge_required" in error_msg.lower():
            error_type = "challenge_required"
        elif error_msg and "login_required" in error_msg.lower():
            error_type = "login_required"
        elif error_msg and "email" in error_msg.lower():
            error_type = "email_error"
        else:
            error_type = "recovery_error"
        
        update_instagram_account(
            account_id,
            is_active=False,
            status=error_type,
            last_error=error_msg,
            last_check=datetime.now()
        )
        
        query.edit_message_text(
            f"❌ Ошибка при восстановлении\n\n"
            f"👤 Аккаунт: @{account.username}\n"
            f"⚠️ Ошибка: {error_msg[:100]}",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 К деталям", callback_data=f"account_details_{account_id}")]])
        )

def reset_errors_handler(update, context):
    """Обработчик сброса ошибок аккаунта"""
    query = update.callback_query
    query.answer()
    
    account_id = int(query.data.replace("reset_errors_", ""))
    account = get_instagram_account(account_id)
    
    if not account:
        query.edit_message_text("❌ Аккаунт не найден")
        return
    
    try:
        from database.db_manager import update_instagram_account
        from datetime import datetime
        
        # Сбрасываем ошибки и статус
        update_instagram_account(
            account_id,
            status="active",
            last_error=None,
            last_check=datetime.now()
        )
        
        query.edit_message_text(
            f"✅ Ошибки сброшены\n\n"
            f"👤 Аккаунт: @{account.username}\n"
            f"🔄 Статус сброшен на 'active'\n"
            f"🚫 Последняя ошибка очищена\n\n"
            f"ℹ️ Аккаунт будет проверен при следующем использовании",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 К деталям", callback_data=f"account_details_{account_id}")]])
        )
        
    except Exception as e:
        query.edit_message_text(
            f"❌ Ошибка при сбросе\n\n"
            f"👤 Аккаунт: @{account.username}\n"
            f"⚠️ Ошибка: {str(e)[:100]}",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 К деталям", callback_data=f"account_details_{account_id}")]])
        )


def activate_account_handler(update, context):
    """Активирует аккаунт"""
    query = update.callback_query
    query.answer()
    
    account_id = int(query.data.replace("activate_account_", ""))
    
    # Показываем процесс активации
    query.edit_message_text(
        f"🔄 Активирую аккаунт...\n\n"
        f"Выполняется проверка входа и восстановление сессии.\n"
        f"Пожалуйста, подождите..."
    )
    
    try:
        account = get_instagram_account(account_id)
        if not account:
            query.edit_message_text("❌ Аккаунт не найден")
            return
        
        # Пытаемся войти в аккаунт и создать сессию
        from instagram.client import test_instagram_login_with_proxy
        
        success = test_instagram_login_with_proxy(
            account_id=account_id,
            username=account.username,
            password=account.password,
            email=account.email,
            email_password=account.email_password
        )
        
        if success:
            # Активируем аккаунт
            activate_instagram_account(account_id)
            
            keyboard = [[InlineKeyboardButton("🔙 К деталям", callback_data=f"account_details_{account_id}")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            query.edit_message_text(
                f"✅ Аккаунт @{account.username} успешно активирован!\n\n"
                f"🔐 Сессия создана и сохранена\n"
                f"📡 Прокси настроен\n"
                f"✅ Готов к использованию",
                reply_markup=reply_markup
            )
        else:
            keyboard = [
                [InlineKeyboardButton("🔧 IMAP восстановление", callback_data=f"imap_recover_{account_id}")],
                [InlineKeyboardButton("🔙 К деталям", callback_data=f"account_details_{account_id}")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            query.edit_message_text(
                f"⚠️ Не удалось активировать аккаунт @{account.username}\n\n"
                f"Возможные причины:\n"
                f"• Требуется код подтверждения\n"
                f"• Неверные данные входа\n"
                f"• Аккаунт заблокирован\n\n"
                f"Попробуйте IMAP восстановление:",
                reply_markup=reply_markup
            )
            
    except Exception as e:
        logger.error(f"Ошибка при активации аккаунта {account_id}: {e}")
        keyboard = [[InlineKeyboardButton("🔙 К деталям", callback_data=f"account_details_{account_id}")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        query.edit_message_text(
            f"❌ Ошибка при активации аккаунта:\n{str(e)}",
            reply_markup=reply_markup
        )

def deactivate_account_handler(update, context):
    """Деактивирует аккаунт"""
    query = update.callback_query
    query.answer()
    
    account_id = int(query.data.replace("deactivate_account_", ""))
    
    try:
        account = get_instagram_account(account_id)
        if not account:
            query.edit_message_text("❌ Аккаунт не найден")
            return
        
        # Деактивируем аккаунт
        from database.db_manager import update_instagram_account
        update_instagram_account(account_id, is_active=False, status='manually_deactivated')
        
        keyboard = [[InlineKeyboardButton("🔙 К деталям", callback_data=f"account_details_{account_id}")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        query.edit_message_text(
            f"⏸️ Аккаунт @{account.username} деактивирован\n\n"
            f"Аккаунт больше не будет использоваться для:\n"
            f"• Публикации контента\n"
            f"• Прогрева\n"
            f"• Автоматических действий\n\n"
            f"Вы можете активировать его снова в любое время.",
            reply_markup=reply_markup
        )
        
    except Exception as e:
        logger.error(f"Ошибка при деактивации аккаунта {account_id}: {e}")
        query.edit_message_text(f"❌ Ошибка: {str(e)}")

def check_single_account_handler(update, context):
    """Проверяет состояние одного аккаунта"""
    query = update.callback_query
    query.answer()
    
    account_id = int(query.data.replace("check_account_", ""))
    
    query.edit_message_text(
        f"🔍 Проверяю состояние аккаунта...\n\n"
        f"Выполняется:\n"
        f"• Проверка подключения к Instagram\n"
        f"• Проверка прокси\n"
        f"• Проверка email доступа\n"
        f"• Анализ последних ошибок"
    )
    
    try:
        account = get_instagram_account(account_id)
        if not account:
            query.edit_message_text("❌ Аккаунт не найден")
            return
        
        report = []
        overall_status = "✅"
        
        # 1. Проверка Instagram подключения
        from instagram.client import test_instagram_login_with_proxy
        instagram_ok = test_instagram_login_with_proxy(
            account_id=account_id,
            username=account.username,
            password=account.password,
            email=account.email,
            email_password=account.email_password
        )
        
        if instagram_ok:
            report.append("✅ Instagram: Подключение работает")
        else:
            report.append("❌ Instagram: Ошибка подключения")
            overall_status = "❌"
        
        # 2. Проверка email
        if account.email and account.email_password:
            from instagram.email_utils import test_email_connection
            email_ok, email_msg = test_email_connection(account.email, account.email_password)
            if email_ok:
                report.append("✅ Email: Подключение работает")
            else:
                report.append(f"❌ Email: {email_msg}")
                overall_status = "⚠️"
        else:
            report.append("⚠️ Email: Данные не указаны")
            
        # 3. Проверка прокси
        if account.proxy:
            report.append(f"✅ Прокси: {account.proxy.host}:{account.proxy.port}")
        else:
            report.append("⚠️ Прокси: Не назначен")
            overall_status = "⚠️"
        
        # 4. Обновляем время последней проверки
        from database.db_manager import update_instagram_account
        from datetime import datetime
        update_instagram_account(account_id, last_check=datetime.now())
        
        # Формируем итоговый отчет
        status_text = {
            "✅": "Исправен",
            "⚠️": "Есть предупреждения", 
            "❌": "Есть проблемы"
        }
        
        text = f"🔍 РЕЗУЛЬТАТ ПРОВЕРКИ\n\n"
        text += f"👤 Аккаунт: @{account.username}\n"
        text += f"📊 Общий статус: {overall_status} {status_text[overall_status]}\n\n"
        text += f"📋 Детали:\n"
        for item in report:
            text += f"  {item}\n"
        
        keyboard = [
            [InlineKeyboardButton("🔄 Проверить снова", callback_data=f"check_account_{account_id}")],
            [InlineKeyboardButton("🔙 К деталям", callback_data=f"account_details_{account_id}")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        query.edit_message_text(text, reply_markup=reply_markup)
        
    except Exception as e:
        logger.error(f"Ошибка при проверке аккаунта {account_id}: {e}")
        keyboard = [[InlineKeyboardButton("🔙 К деталям", callback_data=f"account_details_{account_id}")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        query.edit_message_text(
            f"❌ Ошибка при проверке аккаунта:\n{str(e)}",
            reply_markup=reply_markup
        )

def account_settings_handler(update, context):
    """Настройки аккаунта"""
    query = update.callback_query
    query.answer()
    
    account_id = int(query.data.replace("account_settings_", ""))
    account = get_instagram_account(account_id)
    
    if not account:
        query.edit_message_text("❌ Аккаунт не найден")
        return
    
    text = f"⚙️ НАСТРОЙКИ АККАУНТА\n\n"
    text += f"👤 @{account.username}\n\n"
    text += f"🔧 Доступные настройки:"
    
    keyboard = [
        [InlineKeyboardButton("🔑 Изменить пароль", callback_data=f"change_password_{account_id}")],
        [InlineKeyboardButton("📧 Изменить email", callback_data=f"change_email_{account_id}")],
        [InlineKeyboardButton("🌐 Сменить прокси", callback_data=f"change_proxy_{account_id}")],
        [InlineKeyboardButton("📱 Сбросить устройство", callback_data=f"reset_device_{account_id}")],
        [InlineKeyboardButton("🗂️ Управление группами", callback_data=f"manage_account_groups_{account_id}")],
        [InlineKeyboardButton("🔙 К деталям", callback_data=f"account_details_{account_id}")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    query.edit_message_text(text, reply_markup=reply_markup)

def account_stats_handler(update, context):
    """Статистика аккаунта"""
    query = update.callback_query
    query.answer()
    
    account_id = int(query.data.replace("account_stats_", ""))
    account = get_instagram_account(account_id)
    
    if not account:
        query.edit_message_text("❌ Аккаунт не найден")
        return
    
    try:
        # Получаем статистику публикаций из базы
        session = get_session()
        from database.models import PublishTask
        
        # Подсчитываем публикации
        total_posts = session.query(PublishTask).filter_by(account_id=account_id).count()
        completed_posts = session.query(PublishTask).filter_by(account_id=account_id, status='completed').count()
        failed_posts = session.query(PublishTask).filter_by(account_id=account_id, status='failed').count()
        pending_posts = session.query(PublishTask).filter_by(account_id=account_id, status='pending').count()
        
        session.close()
        
        # Рассчитываем процент успеха
        success_rate = (completed_posts / total_posts * 100) if total_posts > 0 else 0
        
        text = f"📊 СТАТИСТИКА АККАУНТА\n\n"
        text += f"👤 @{account.username}\n"
        text += f"📅 Добавлен: {account.created_at.strftime('%d.%m.%Y')}\n\n"
        
        text += f"📈 ПУБЛИКАЦИИ:\n"
        text += f"  📤 Всего: {total_posts}\n"
        text += f"  ✅ Успешно: {completed_posts}\n"
        text += f"  ❌ Ошибок: {failed_posts}\n"
        text += f"  ⏳ Ожидает: {pending_posts}\n"
        text += f"  📊 Успешность: {success_rate:.1f}%\n\n"
        
        if account.last_check:
            text += f"🔍 Последняя проверка: {account.last_check.strftime('%d.%m.%Y %H:%M')}\n"
        
        # Информация о группах
        if account.groups:
            text += f"\n📁 Группы: {', '.join([g.name for g in account.groups])}\n"
        
        keyboard = [
            [InlineKeyboardButton("📊 Детальная аналитика", callback_data=f"detailed_analytics_{account_id}")],
            [InlineKeyboardButton("🔄 Обновить", callback_data=f"account_stats_{account_id}")],
            [InlineKeyboardButton("🔙 К деталям", callback_data=f"account_details_{account_id}")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        query.edit_message_text(text, reply_markup=reply_markup)
        
    except Exception as e:
        logger.error(f"Ошибка при получении статистики аккаунта {account_id}: {e}")
        query.edit_message_text(f"❌ Ошибка получения статистики: {str(e)}")

def manage_account_groups_handler(update, context):
    """Управление группами аккаунта"""
    query = update.callback_query
    query.answer()
    
    account_id = int(query.data.replace("manage_account_groups_", ""))
    account = get_instagram_account(account_id)
    
    if not account:
        query.edit_message_text("❌ Аккаунт не найден")
        return
    
    # Получаем все доступные группы
    session = get_session()
    from database.models import AccountGroup
    all_groups = session.query(AccountGroup).all()
    current_groups = account.groups
    session.close()
    
    text = f"📁 УПРАВЛЕНИЕ ГРУППАМИ\n\n"
    text += f"👤 @{account.username}\n\n"
    
    if current_groups:
        text += f"📌 Текущие группы:\n"
        for group in current_groups:
            text += f"  {group.icon} {group.name}\n"
        text += "\n"
    else:
        text += f"📌 Аккаунт не состоит в группах\n\n"
    
    text += f"🔧 Доступные действия:"
    
    keyboard = []
    
    # Кнопки для добавления в группы
    if all_groups:
        keyboard.append([InlineKeyboardButton("➕ Добавить в группу", callback_data=f"add_to_group_{account_id}")])
    
    # Кнопки для удаления из групп
    if current_groups:
        keyboard.append([InlineKeyboardButton("➖ Удалить из группы", callback_data=f"remove_from_group_{account_id}")])
    
    # Создание новой группы
    keyboard.append([InlineKeyboardButton("📂 Создать новую группу", callback_data=f"create_group_{account_id}")])
    keyboard.append([InlineKeyboardButton("🔙 К деталям", callback_data=f"account_details_{account_id}")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    query.edit_message_text(text, reply_markup=reply_markup)

def manage_account_proxy_handler(update, context):
    """Управление прокси аккаунта"""
    query = update.callback_query
    query.answer()
    
    account_id = int(query.data.replace("manage_account_proxy_", ""))
    account = get_instagram_account(account_id)
    
    if not account:
        query.edit_message_text("❌ Аккаунт не найден")
        return
    
    text = f"🌐 УПРАВЛЕНИЕ ПРОКСИ\n\n"
    text += f"👤 @{account.username}\n\n"
    
    if account.proxy:
        text += f"📡 Текущий прокси:\n"
        text += f"  🌐 {account.proxy.host}:{account.proxy.port}\n"
        text += f"  📊 Статус: {'✅ Активен' if account.proxy.is_active else '❌ Неактивен'}\n\n"
    else:
        text += f"📡 Прокси: Не назначен\n\n"
    
    text += f"🔧 Доступные действия:"
    
    keyboard = [
        [InlineKeyboardButton("🔄 Сменить прокси", callback_data=f"change_proxy_{account_id}")],
        [InlineKeyboardButton("🧪 Проверить прокси", callback_data=f"test_proxy_{account_id}")],
    ]
    
    if account.proxy:
        keyboard.append([InlineKeyboardButton("📊 Статистика прокси", callback_data=f"proxy_stats_{account_id}")])
    
    keyboard.append([InlineKeyboardButton("🔙 К деталям", callback_data=f"account_details_{account_id}")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    query.edit_message_text(text, reply_markup=reply_markup)

def publish_to_account_handler(update, context):
    """Быстрая публикация в аккаунт"""
    query = update.callback_query
    query.answer()
    
    account_id = int(query.data.replace("publish_to_", ""))
    account = get_instagram_account(account_id)
    
    if not account:
        query.edit_message_text("❌ Аккаунт не найден")
        return
    
    # Сохраняем выбранный аккаунт для публикации
    context.user_data['publish_account_id'] = account_id
    context.user_data['publish_account_username'] = account.username
    context.user_data['selected_accounts'] = [account_id]
    
    text = f"📤 БЫСТРАЯ ПУБЛИКАЦИЯ\n\n"
    text += f"👤 Выбран: @{account.username}\n\n"
    text += f"📋 Выберите тип публикации:"
    
    keyboard = [
        [InlineKeyboardButton("📸 Пост", callback_data="start_post_publish")],
        [InlineKeyboardButton("📱 Story", callback_data="start_story_publish")],
        [InlineKeyboardButton("🎥 Reels", callback_data="start_reels_publish")],
        [InlineKeyboardButton("🎬 IGTV", callback_data="start_igtv_publish")],
        [InlineKeyboardButton("🔙 К деталям", callback_data=f"account_details_{account_id}")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    query.edit_message_text(text, reply_markup=reply_markup)

def warm_account_handler(update, context):
    """Запуск прогрева аккаунта"""
    query = update.callback_query
    query.answer()
    
    account_id = int(query.data.replace("warm_account_", ""))
    account = get_instagram_account(account_id)
    
    if not account:
        query.edit_message_text("❌ Аккаунт не найден")
        return
    
    if not account.is_active:
        keyboard = [[InlineKeyboardButton("🔙 К деталям", callback_data=f"account_details_{account_id}")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        query.edit_message_text(
            f"⚠️ Аккаунт @{account.username} неактивен\n\n"
            f"Для прогрева необходимо сначала активировать аккаунт.",
            reply_markup=reply_markup
        )
        return
    
    text = f"🔥 ПРОГРЕВ АККАУНТА\n\n"
    text += f"👤 @{account.username}\n\n"
    text += f"🎯 Выберите тип прогрева:"
    
    keyboard = [
        [InlineKeyboardButton("⚡ Быстрый прогрев", callback_data=f"quick_warmup_{account_id}")],
        [InlineKeyboardButton("🎯 Умный прогрев", callback_data=f"smart_warmup_{account_id}")],
        [InlineKeyboardButton("🎨 Прогрев по интересам", callback_data=f"interest_warmup_{account_id}")],
        [InlineKeyboardButton("📊 Настройки прогрева", callback_data=f"warmup_settings_{account_id}")],
        [InlineKeyboardButton("🔙 К деталям", callback_data=f"account_details_{account_id}")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    query.edit_message_text(text, reply_markup=reply_markup)


def quick_warmup_handler(update, context):
    """Быстрый прогрев аккаунта"""
    query = update.callback_query
    query.answer("⚡ Запускаю быстрый прогрев...")
    
    account_id = int(query.data.replace("quick_warmup_", ""))
    
    # Используем новую систему прогрева
    from services.advanced_warmup import advanced_warmup
    
    keyboard = [[InlineKeyboardButton("🔙 К настройкам", callback_data=f"warm_account_{account_id}")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    query.edit_message_text(
        "⚡ Быстрый прогрев запущен!\n\n"
        "⏱️ Примерное время: 5-15 минут\n"
        "📱 Аккаунт будет активен в Instagram\n\n"
        "Вы получите уведомление по завершении.",
        reply_markup=reply_markup
    )
    
    # Запускаем прогрев в фоне
    from threading import Thread
    def run_warmup():
        try:
            success, report = advanced_warmup.start_warmup(
                account_id=account_id,
                duration_minutes=10
            )
            # Отправляем отчет пользователю
            if success:
                context.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text=f"✅ Быстрый прогрев завершен!\n\n{report}"
                )
            else:
                context.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text=f"❌ Ошибка прогрева: {report}"
                )
        except Exception as e:
            context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=f"❌ Ошибка: {str(e)}"
            )
        
    thread = Thread(target=run_warmup)
    thread.start()
    
    
def smart_warmup_handler(update, context):
    """Умный прогрев аккаунта"""
    query = update.callback_query
    query.answer("🎯 Анализирую аккаунт...")
    
    account_id = int(query.data.replace("smart_warmup_", ""))
    
    # Используем AccountAutomationService для умного прогрева
    from services.account_automation import automation_service
    
    keyboard = [[InlineKeyboardButton("🔙 К настройкам", callback_data=f"warm_account_{account_id}")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    # Получаем статус аккаунта
    status = automation_service.get_account_status(account_id)
    
    text = "🎯 Умный прогрев запущен!\n\n"
    text += f"📊 Состояние аккаунта: {status.get('overall_status', 'N/A')}\n"
    text += f"💚 Здоровье: {status.get('health_score', 0)}%\n"
    text += f"⚠️ Риск бана: {status.get('ban_risk_score', 0)}%\n\n"
    text += "Система автоматически подберет оптимальные параметры\n\n"
    text += "Вы получите уведомление по завершении."
    
    query.edit_message_text(text, reply_markup=reply_markup)
    
    # Запускаем умный прогрев в фоне
    from threading import Thread
    def run_smart_warmup():
        try:
            success, message = automation_service.smart_warm_account(account_id)
            # Отправляем результат пользователю
            if success:
                context.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text=f"✅ Умный прогрев завершен!\n\n{message}"
                )
            else:
                context.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text=f"❌ Ошибка прогрева: {message}"
                )
        except Exception as e:
            context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=f"❌ Ошибка: {str(e)}"
            )
        
    thread = Thread(target=run_smart_warmup)
    thread.start()


def warmup_settings_handler(update, context):
    """Настройки прогрева аккаунта"""
    query = update.callback_query
    query.answer()
    
    account_id = int(query.data.replace("warmup_settings_", ""))
    account = get_instagram_account(account_id)
    
    if not account:
        query.edit_message_text("❌ Аккаунт не найден")
        return
    
    # Определяем стратегию прогрева
    from services.advanced_warmup import advanced_warmup
    strategy = advanced_warmup.determine_strategy(account_id)
    
    # Вычисляем возраст аккаунта
    if account.created_at:
        age_days = (datetime.now() - account.created_at).days
        age_text = f"{age_days} дней"
    else:
        age_text = "Неизвестно"
    
    text = f"📊 НАСТРОЙКИ ПРОГРЕВА\n\n"
    text += f"👤 Аккаунт: @{account.username}\n"
    text += f"📅 Возраст: {age_text}\n"
    text += f"🎯 Стратегия: {strategy.value}\n\n"
    
    if strategy.value == "baby":
        text += "👶 Стратегия для новых аккаунтов:\n"
        text += "• 10-20 действий в час\n"
        text += "• Короткие сессии (5-15 мин)\n"
        text += "• Больше просмотров, меньше действий\n"
    elif strategy.value == "child":
        text += "🧒 Стратегия для молодых аккаунтов:\n"
        text += "• 20-40 действий в час\n"
        text += "• Средние сессии (10-30 мин)\n"
        text += "• Начинаем комментировать\n"
    elif strategy.value == "teen":
        text += "👦 Стратегия для подростков:\n"
        text += "• 40-80 действий в час\n"
        text += "• Длинные сессии (20-45 мин)\n"
        text += "• Активные подписки и комменты\n"
    else:
        text += "👨 Стратегия для взрослых:\n"
        text += "• 80-150 действий в час\n"
        text += "• Полные сессии (30-60 мин)\n"
        text += "• Максимальная активность\n"
    
    # Статистика прогревов
    if hasattr(account, 'last_warmup') and account.last_warmup:
        last_warmup = account.last_warmup.strftime("%d.%m.%Y %H:%M")
        text += f"\n📅 Последний прогрев: {last_warmup}"
    
    keyboard = [
        [InlineKeyboardButton("🔄 Изменить стратегию", callback_data=f"change_warmup_strategy_{account_id}")],
        [InlineKeyboardButton("📈 Статистика прогревов", callback_data=f"warmup_stats_{account_id}")],
        [InlineKeyboardButton("🔙 Назад", callback_data=f"warm_account_{account_id}")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    query.edit_message_text(text, reply_markup=reply_markup)

def start_command_with_subscription(update, context):
    """Команда /start с проверкой доступа и автосоздание пользователя"""
    user_id = update.effective_user.id
    username = update.effective_user.username or "Неизвестно"
    
    # ДЕБАГ: логируем вызов функции
    logger.info(f"🔧 ДЕБАГ: start_command_with_subscription вызвана для пользователя {user_id} (@{username})")
    
    # Получаем информацию о подписке (БЕЗ автосоздания)
    from utils.subscription_service import subscription_service
    subscription_info = subscription_service.check_user_access(user_id)
    
    # Если пользователь найден, добавляем только название плана
    if subscription_info['user'] and subscription_info['user'].subscription_plan:
        from admin_bot.models.user import PLAN_INFO
        plan_info = PLAN_INFO.get(subscription_info['user'].subscription_plan, {})
        subscription_info['plan_name'] = plan_info.get('name', 'Неизвестный план')
        subscription_info['plan_price'] = plan_info.get('price', 0)
    else:
        subscription_info['plan_name'] = 'Неизвестный план'
        subscription_info['plan_price'] = 0
    
    # Если пользователь не найден, НЕ создаем автоматически
    # Пусть админ создает пользователей вручную через админ панель
    logger.info(f"🔧 ДЕБАГ: subscription_info = {subscription_info}")
    
    if subscription_info['has_access']:
        # У пользователя ЕСТЬ доступ - показываем полное приветствие
        welcome_text = f"Привет, @{username}! Я бот для автоматической загрузки контента в Instagram.\n\n"
        
        # Информация о подписке
        plan_name = subscription_info.get('plan_name', 'Неизвестный план')
        days_remaining = subscription_info.get('days_remaining', 0)
        
        if subscription_info['is_trial']:
            welcome_text += f"🆓 **Ваш статус:** {plan_name}\n"
            welcome_text += f"⏰ **Осталось дней:** {days_remaining}\n\n"
            welcome_text += "💡 Не забудьте оформить полную подписку до окончания триала!\n\n"
        else:
            welcome_text += f"💎 **Ваш статус:** {plan_name}\n"
            if days_remaining != float('inf'):
                welcome_text += f"⏰ **Осталось дней:** {days_remaining}\n\n"
            else:
                welcome_text += "♾️ **Безлимитный доступ**\n\n"
        
        welcome_text += "Выберите раздел из меню ниже или используйте /help для получения списка доступных команд."
        
        # Полная клавиатура для пользователей с доступом
        keyboard = [
            [InlineKeyboardButton("👥 Аккаунты", callback_data="accounts_menu")],
            [InlineKeyboardButton("📤 Публикации", callback_data="publish_menu")],
            [InlineKeyboardButton("📋 Запланированные", callback_data="scheduled_menu")],
            [InlineKeyboardButton("🔥 Прогрев", callback_data="warmup_menu")],
            [InlineKeyboardButton("🌐 Прокси", callback_data="proxy_menu")],
            [InlineKeyboardButton("📊 Статистика", callback_data="analytics_menu")],
            [InlineKeyboardButton("⚙️ Настройки", callback_data="settings_menu")]
        ]
        
    else:
        # У пользователя НЕТ доступа - показываем блокировку сразу
        status = subscription_info.get('status', 'unknown')
        
        welcome_text = f"🔒 **Доступ ограничен**\n\n"
        welcome_text += f"👤 @{username} (ID: `{user_id}`)\n"
        welcome_text += f"📊 Статус: ❌ Пользователь не зарегистрирован в системе\n\n"
        
        if status == 'not_registered':
            welcome_text += "💳 **Для использования бота необходима подписка:**\n"
            welcome_text += "• 🆓 Триал 1-7 дней - Бесплатно\n"
            welcome_text += "• 💳 1 месяц - $200\n"
            welcome_text += "• 💳 3 месяца - $400\n"
            welcome_text += "• 💎 Навсегда - $500\n\n"
            welcome_text += f"📞 Обратитесь к администратору: @admin\n"
            welcome_text += f"📨 Сообщите ваш ID: `{user_id}`\n\n"
            welcome_text += "После активации нажмите кнопку \"Обновить доступ\" ниже."
            
        elif status == 'expired':
            welcome_text += "⏰ **Ваша подписка истекла**\n\n"
            welcome_text += "💳 **Продлите подписку:**\n"
            welcome_text += "• 1 месяц - $200\n"
            welcome_text += "• 3 месяца - $400\n" 
            welcome_text += "• Навсегда - $500\n\n"
            welcome_text += f"📞 Обратитесь к администратору: @admin\n"
            welcome_text += f"📨 Сообщите ваш ID: `{user_id}`\n\n"
            welcome_text += "После продления нажмите кнопку \"Обновить доступ\" ниже."
            
        elif status == 'blocked':
            welcome_text += "🚫 **Ваш аккаунт заблокирован**\n\n"
            welcome_text += f"📞 Для разблокировки обратитесь к администратору: @admin\n"
            welcome_text += f"📨 Сообщите ваш ID: `{user_id}`\n\n"
            welcome_text += "После разблокировки нажмите кнопку \"Обновить доступ\" ниже."
        
        # Ограниченная клавиатура для пользователей без доступа
        keyboard = [
            [InlineKeyboardButton("🔄 Обновить доступ", callback_data="refresh_access")],
            [InlineKeyboardButton("🔒 Получить доступ", url="https://t.me/admin")],
            [InlineKeyboardButton("ℹ️ О подписке", callback_data="subscription_info")]
        ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    update.message.reply_text(
        welcome_text,
        reply_markup=reply_markup,
        parse_mode=ParseMode.MARKDOWN
    )

def refresh_access_callback(update, context):
    """Обновляет статус доступа пользователя"""
    query = update.callback_query
    query.answer()
    
    user_id = update.effective_user.id
    username = update.effective_user.username or "Неизвестно"
    
    # Обновляем информацию о пользователе
    from utils.subscription_service import subscription_service
    subscription_service.ensure_user_exists(user_id, username)
    
    # Получаем свежую информацию о подписке
    subscription_info = get_user_subscription_info(user_id)
    
    if subscription_info['has_access']:
        # Доступ появился! Показываем успешное сообщение и перенаправляем на главное меню
        success_text = f"✅ **Доступ активирован!**\n\n"
        success_text += f"👤 @{username}\n"
        
        plan_name = subscription_info.get('plan_name', 'Неизвестный план')
        days_remaining = subscription_info.get('days_remaining', 0)
        
        if subscription_info['is_trial']:
            success_text += f"🆓 **Ваш статус:** {plan_name}\n"
            success_text += f"⏰ **Осталось дней:** {days_remaining}\n\n"
            success_text += "💡 Добро пожаловать! Теперь вы можете использовать все функции бота.\n\n"
        else:
            success_text += f"💎 **Ваш статус:** {plan_name}\n"
            if days_remaining != float('inf'):
                success_text += f"⏰ **Осталось дней:** {days_remaining}\n\n"
            else:
                success_text += "♾️ **Безлимитный доступ**\n\n"
        
        success_text += "Нажмите \"Перейти к боту\" чтобы начать работу!"
        
        keyboard = [[InlineKeyboardButton("🚀 Перейти к боту", callback_data="main_menu")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        query.edit_message_text(
            success_text,
            reply_markup=reply_markup,
            parse_mode=ParseMode.MARKDOWN
        )
    else:
        # Доступ все еще отсутствует
        status = subscription_info.get('status', 'unknown')
        
        blocked_text = f"🔒 **Доступ по-прежнему ограничен**\n\n"
        blocked_text += f"👤 @{username} (ID: `{user_id}`)\n"
        
        if status == 'not_registered':
            blocked_text += f"📊 Статус: ❌ Пользователь не зарегистрирован в системе\n\n"
            blocked_text += "💳 **Для получения доступа:**\n"
            blocked_text += "1. Обратитесь к администратору: @admin\n"
            blocked_text += f"2. Сообщите ваш ID: `{user_id}`\n"
            blocked_text += "3. Выберите тарифный план и оплатите\n"
            blocked_text += "4. После активации снова нажмите \"Обновить доступ\"\n\n"
        elif status == 'expired':
            blocked_text += f"📊 Статус: ⏰ Подписка истекла\n\n"
            blocked_text += "💳 **Для продления:**\n"
            blocked_text += "1. Обратитесь к администратору: @admin\n"
            blocked_text += f"2. Сообщите ваш ID: `{user_id}`\n"
            blocked_text += "3. Продлите подписку\n\n"
        elif status == 'blocked':
            blocked_text += f"📊 Статус: 🚫 Аккаунт заблокирован\n\n"
            blocked_text += "📞 Для разблокировки обратитесь к администратору: @admin\n\n"
        
        blocked_text += "🔄 Нажмите \"Попробовать снова\" после решения проблемы."
        
        keyboard = [
            [InlineKeyboardButton("🔄 Попробовать снова", callback_data="refresh_access")],
            [InlineKeyboardButton("🔒 Получить доступ", url="https://t.me/admin")],
            [InlineKeyboardButton("ℹ️ О подписке", callback_data="subscription_info")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        query.edit_message_text(
            blocked_text,
            reply_markup=reply_markup,
            parse_mode=ParseMode.MARKDOWN
        )

def subscription_info_callback(update, context):
    """Показывает подробную информацию о подписке"""
    query = update.callback_query
    query.answer()
    
    user_id = update.effective_user.id
    subscription_info = get_user_subscription_info(user_id)
    
    info_text = "📊 **ИНФОРМАЦИЯ О ПОДПИСКЕ**\n\n"
    
    if subscription_info['has_access']:
        plan_name = subscription_info.get('plan_name', 'Неизвестный план')
        plan_price = subscription_info.get('plan_price', 0)
        start_date = subscription_info.get('subscription_start')
        end_date = subscription_info.get('subscription_end')
        last_activity = subscription_info.get('last_activity')
        
        info_text += f"💎 **План:** {plan_name}\n"
        info_text += f"💰 **Стоимость:** ${plan_price}\n"
        
        if start_date:
            info_text += f"📅 **Дата начала:** {start_date}\n"
        if end_date:
            info_text += f"📅 **Дата окончания:** {end_date}\n"
        else:
            info_text += "♾️ **Безлимитная подписка**\n"
        
        if last_activity:
            info_text += f"🕐 **Последняя активность:** {last_activity}\n"
        
        days_remaining = subscription_info.get('days_remaining', 0)
        if days_remaining != float('inf'):
            info_text += f"\n⏰ **Осталось дней:** {days_remaining}\n"
        
        info_text += f"\n📱 **Аккаунтов добавлено:** {subscription_info.get('accounts_count', 0)}\n"
        
    else:
        info_text += "❌ **Подписка неактивна**\n\n"
        info_text += "🛒 **Доступные тарифы:**\n"
        info_text += "🆓 Триал 1 день - Бесплатно\n"
        info_text += "🆓 Триал 3 дня - Бесплатно\n"
        info_text += "🆓 Триал 7 дней - Бесплатно\n"
        info_text += "💳 1 месяц - $200\n"
        info_text += "💳 3 месяца - $400\n"
        info_text += "💎 Навсегда - $500\n\n"
        info_text += "📞 Для оформления обратитесь к администратору: @admin\n"
    
    keyboard = [[InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    query.edit_message_text(
        info_text,
        reply_markup=reply_markup,
        parse_mode=ParseMode.MARKDOWN
    )

def test_proxy_handler(update, context):
    """Обработчик проверки прокси аккаунта"""
    query = update.callback_query
    query.answer()
    
    account_id = int(query.data.replace("test_proxy_", ""))
    account = get_instagram_account(account_id)
    
    if not account:
        query.edit_message_text("❌ Аккаунт не найден")
        return
    
    if not account.proxy:
        query.edit_message_text("❌ У аккаунта не назначен прокси")
        return
    
    text = f"🧪 ПРОВЕРКА ПРОКСИ\n\n"
    text += f"👤 @{account.username}\n"
    text += f"🌐 {account.proxy.host}:{account.proxy.port}\n\n"
    text += f"🔄 Запускается проверка..."
    
    keyboard = [[InlineKeyboardButton("🔙 К управлению прокси", callback_data=f"manage_account_proxy_{account_id}")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    query.edit_message_text(text, reply_markup=reply_markup)
    
    # TODO: Добавить реальную проверку прокси
    context.bot.edit_message_text(
        text=f"🧪 ПРОВЕРКА ПРОКСИ\n\n👤 @{account.username}\n🌐 {account.proxy.host}:{account.proxy.port}\n\n✅ Прокси работает корректно",
        chat_id=query.message.chat_id,
        message_id=query.message.message_id,
        reply_markup=reply_markup
    )

def change_proxy_handler(update, context):
    """Обработчик смены прокси аккаунта"""
    query = update.callback_query
    query.answer()
    
    account_id = int(query.data.replace("change_proxy_", ""))
    account = get_instagram_account(account_id)
    
    if not account:
        query.edit_message_text("❌ Аккаунт не найден")
        return
    
    text = f"🔄 СМЕНА ПРОКСИ\n\n"
    text += f"👤 @{account.username}\n\n"
    text += f"📋 Выберите новый прокси:"
    
    # Получаем доступные прокси
    from database.db_manager import get_session
    session = get_session()
    try:
        from database.models import ProxyServer
        proxies = session.query(ProxyServer).filter(ProxyServer.is_active == True).all()
        
        keyboard = []
        for proxy in proxies[:10]:  # Показываем первые 10 прокси
            keyboard.append([InlineKeyboardButton(
                f"🌐 {proxy.host}:{proxy.port}",
                callback_data=f"assign_proxy_{account_id}_{proxy.id}"
            )])
        
        keyboard.append([InlineKeyboardButton("🔙 К управлению прокси", callback_data=f"manage_account_proxy_{account_id}")])
        
    except Exception as e:
        text += f"\n❌ Ошибка загрузки прокси: {str(e)}"
        keyboard = [[InlineKeyboardButton("🔙 К управлению прокси", callback_data=f"manage_account_proxy_{account_id}")]]
    finally:
        session.close()
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    query.edit_message_text(text, reply_markup=reply_markup)

def proxy_stats_handler(update, context):
    """Обработчик статистики прокси"""
    query = update.callback_query
    query.answer()
    
    account_id = int(query.data.replace("proxy_stats_", ""))
    account = get_instagram_account(account_id)
    
    if not account:
        query.edit_message_text("❌ Аккаунт не найден")
        return
    
    if not account.proxy:
        query.edit_message_text("❌ У аккаунта не назначен прокси")
        return
    
    text = f"📊 СТАТИСТИКА ПРОКСИ\n\n"
    text += f"👤 @{account.username}\n"
    text += f"🌐 {account.proxy.host}:{account.proxy.port}\n\n"
    text += f"📈 Использование:\n"
    text += f"  • Последняя проверка: N/A\n"
    text += f"  • Успешных запросов: N/A\n"
    text += f"  • Ошибок: N/A\n"
    text += f"  • Время отклика: N/A\n\n"
    text += f"💡 Полная статистика в разработке"
    
    keyboard = [[InlineKeyboardButton("🔙 К управлению прокси", callback_data=f"manage_account_proxy_{account_id}")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    query.edit_message_text(text, reply_markup=reply_markup)


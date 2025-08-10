import logging
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters, CallbackQueryHandler, ConversationHandler, TypeHandler
from telegram import ParseMode, InlineKeyboardButton, InlineKeyboardMarkup
from telegram import Update

from config import TELEGRAM_TOKEN, ADMIN_USER_IDS
from utils.access_manager import has_access
from telegram_bot.handlers import get_all_handlers
from telegram_bot.handlers.account_handlers import (
    add_account, enter_username, enter_password, enter_email, enter_email_password,
    confirm_add_account, enter_verification_code, cancel_add_account,
    ENTER_USERNAME, ENTER_PASSWORD, ENTER_EMAIL, ENTER_EMAIL_PASSWORD, CONFIRM_ACCOUNT, ENTER_VERIFICATION_CODE,
    bulk_add_accounts_command, bulk_upload_accounts_file, list_accounts_handler
)
from telegram_bot.states import BULK_ADD_ACCOUNTS, WAITING_ACCOUNTS_FILE
from telegram_bot.handlers.task_handlers import retry_task_callback
from database.models import InstagramAccount
from database.db_manager import add_instagram_account, get_session
from database.safe_user_wrapper import get_user_instagram_account as get_instagram_account, extract_user_id_from_update
from telegram_bot.handlers.profile_handlers import get_profile_handlers, profile_setup_menu
from profile_setup import EDIT_NAME, EDIT_USERNAME, EDIT_BIO, EDIT_LINKS, ADD_PHOTO, ADD_POST


logger = logging.getLogger(__name__)

def is_admin(user_id):
    return has_access(user_id)

def start_handler(update, context):
    """Обработчик команды /start с проверкой доступа"""
    user_id = update.effective_user.id
    username = update.effective_user.username or "Пользователь"
    first_name = update.effective_user.first_name or ""
    
    # Проверяем доступ пользователя через централизованную систему
    if not has_access(user_id):
        # Получаем информацию о пользователе для детального ответа
        from utils.access_manager import get_access_manager
        access_manager = get_access_manager()
        user_info = access_manager.get_user_info(user_id)
        
        if user_info and not user_info.get('is_active'):
            # Пользователь был, но заблокирован/истекла подписка
            text = f"""❌ **ДОСТУП ЗАБЛОКИРОВАН**

👋 Привет, {first_name}!

🚫 **Ваш доступ к боту был ограничен.**

💡 **Возможные причины:**
• Истекла подписка
• Нарушение правил использования
• Административная блокировка

📞 **Для восстановления доступа:**
• Обратитесь к администратору
• Telegram: @your_admin_username
• Укажите ваш ID: `{user_id}`

⏰ Дата блокировки: {user_info.get('added_at', 'Неизвестно')[:10]}"""
        else:
            # Новый пользователь без доступа
            text = f"""🚫 **ДОСТУП ОГРАНИЧЕН**

👋 Привет, {first_name}!

🤖 **Instagram Automation Bot**
Профессиональный инструмент для управления Instagram аккаунтами.

❌ **У вас нет доступа к этому боту.**

🎯 **Для получения доступа:**
• Свяжитесь с администратором
• Telegram: @your_admin_username  
• Приобретите подписку

💎 **Что вы получите:**
• Автоматизация Instagram
• Умный прогрев аккаунтов
• Планирование публикаций
• ML анализ здоровья аккаунтов
• Профессиональная поддержка

🆔 **Ваш ID для регистрации:** `{user_id}`"""
        
        update.message.reply_text(text, parse_mode='Markdown')
        return

    # Пользователь имеет доступ - инициализируем его персональное хранилище
    from utils.user_storage import init_user_storage
    init_user_storage(user_id, username)
    
    # Приветственное сообщение с кнопкой "Продолжить"
    from telegram_bot.keyboards import get_main_menu_keyboard
    
    welcome_text = f"""🎉 **Добро пожаловать в Instagram Automation Bot!**

👋 Привет, {first_name}!

🤖 **Ваш профессиональный помощник для Instagram:**

🚀 **Основные возможности:**
• 📱 Управление аккаунтами
• 🔥 Умный прогрев
• 📅 Планирование публикаций  
• 🤖 ML анализ здоровья
• 📊 Детальная аналитика

🎯 **Улучшенные системы:**
• AI прогрев с анализом интересов
• ML предиктивная аналитика
• Умное планирование контента
• Автоматизация процессов

💎 **Ваш статус:** Активная подписка
🆔 **ID:** `{user_id}`

Нажмите кнопку ниже, чтобы начать работу:"""
    
    update.message.reply_text(
        welcome_text,
        reply_markup=get_main_menu_keyboard(),
        parse_mode='Markdown'
    )

def help_handler(update, context):
    help_text = """
*Доступные команды:*

*Аккаунты:*
/accounts - Меню управления аккаунтами
/add_account - Добавить новый аккаунт Instagram
/upload_accounts - Загрузить несколько аккаунтов из файла
/list_accounts - Показать список аккаунтов
/profile_setup - Настроить профиль аккаунта

*Автоматизация:*
/status - Статус всех аккаунтов (здоровье, риски)
/smart_warm - Умный прогрев аккаунтов
/limits - Просмотр текущих лимитов

*Задачи:*
/tasks - Меню управления задачами
/publish_now - Опубликовать контент сейчас
/schedule_publish - Запланировать публикацию

*Прокси:*
/proxy - Меню управления прокси
/add_proxy - Добавить новый прокси
/distribute_proxies - Распределить прокси по аккаунтам
/list_proxies - Показать список прокси

/cancel - Отменить текущую операцию
    """

    from telegram_bot.keyboards import get_main_menu_keyboard
    reply_markup = get_main_menu_keyboard()

    update.message.reply_text(help_text, parse_mode=ParseMode.MARKDOWN, reply_markup=reply_markup)

def cancel_handler(update, context):
    keyboard = [[InlineKeyboardButton("🔙 Главное меню", callback_data='main_menu')]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    update.message.reply_text(
        "Операция отменена.",
        reply_markup=reply_markup
    )
    return ConversationHandler.END

def handle_bulk_accounts_text(update, context):
    """Обработчик текстового сообщения для массовой загрузки аккаунтов"""
    
    # Проверяем, ожидаем ли мы массовую загрузку
    if not context.user_data.get('waiting_for_bulk_accounts'):
        return ConversationHandler.END
    
    user_id = update.effective_user.id
    
    # Проверяем права администратора (берем функцию из account_handlers)
    from telegram_bot.handlers.account_handlers import is_admin
    if not is_admin(user_id):
        update.message.reply_text("❌ У вас нет прав для выполнения этой команды.")
        return ConversationHandler.END
    
    accounts_text = update.message.text.strip()
    
    if not accounts_text:
        update.message.reply_text("❌ Сообщение пустое. Попробуйте еще раз.")
        return BULK_ADD_ACCOUNTS
    
    # Разбиваем текст на строки
    accounts_lines = accounts_text.split("\n")
    
    # Статистика
    total_accounts = len([line for line in accounts_lines if line.strip()])
    added_accounts = 0
    failed_accounts = 0
    already_exists = 0
    failed_accounts_list = []
    
    progress_message = update.message.reply_text(f"🔄 Начинаем добавление {total_accounts} аккаунтов...")
    
    # Обрабатываем каждую строку
    for line_num, line in enumerate(accounts_lines, 1):
        line = line.strip()
        if not line:
            continue
        
        parts = line.split(":")
        if len(parts) != 4:
            failed_accounts += 1
            failed_accounts_list.append(f"Строка {line_num}: {line} - неверный формат")
            continue
        
        username, password, email, email_password = [part.strip() for part in parts]
        
        # Проверяем, существует ли уже аккаунт с таким именем
        session = get_session()
        existing_account = session.query(InstagramAccount).filter_by(username=username).first()
        session.close()
        
        if existing_account:
            already_exists += 1
            failed_accounts_list.append(f"Строка {line_num}: @{username} - уже существует")
            continue
        
        # Добавляем аккаунт с ПОЛНОЙ ПРОВЕРКОЙ (как при обычном добавлении)
        try:
            # Обновляем прогресс каждые 5 аккаунтов
            if line_num % 5 == 0:
                try:
                    progress_message.edit_text(f"🔄 Обрабатываем аккаунт {line_num}/{total_accounts}: @{username}")
                except:
                    pass
            
            # Шаг 1: Проверяем подключение к почте
            print(f"[BULK] Проверка почты для {username}")
            from instagram.email_utils import test_email_connection
            email_success, email_msg = test_email_connection(email, email_password)
            
            if not email_success:
                failed_accounts += 1
                failed_accounts_list.append(f"Строка {line_num}: @{username} - ошибка почты: {email_msg}")
                continue
            
            # Шаг 2: Добавляем аккаунт в базу (сначала БЕЗ активации)
            from database.db_manager import add_instagram_account_without_login
            account = add_instagram_account_without_login(username, password, email, email_password)
            
            if not account:
                failed_accounts += 1
                failed_accounts_list.append(f"Строка {line_num}: @{username} - ошибка добавления в базу")
                continue
                
            account_id = account.id
            print(f"[BULK] Аккаунт @{username} добавлен в базу (ID: {account_id})")
            
            # Шаг 3: Тестируем полный вход с прокси и сохранением сессии
            print(f"[BULK] Проверка входа с прокси для {username}")
            from instagram.client import test_instagram_login_with_proxy
            
            login_success = test_instagram_login_with_proxy(
                account_id=account_id,
                username=username,
                password=password,
                email=email,
                email_password=email_password
            )
            
            if login_success:
                # Активируем аккаунт если вход успешен
                from database.db_manager import activate_instagram_account
                activate_instagram_account(account_id)
                added_accounts += 1
                print(f"[BULK] ✅ Аккаунт {username} успешно добавлен и активирован")
            else:
                # Аккаунт добавлен в базу, но не активирован
                # Добавляем детали о проблеме
                failed_accounts += 1
                failed_accounts_list.append(f"Строка {line_num}: @{username} - добавлен в базу, но не удалось войти (возможно нужен код подтверждения)")
                print(f"[BULK] ⚠️ Аккаунт {username} в базе но не активирован")
                
        except Exception as e:
            failed_accounts += 1
            failed_accounts_list.append(f"Строка {line_num}: @{username} - {str(e)}")
            print(f"[BULK] ❌ Ошибка для {username}: {e}")
    
    # Отчет о результатах
    result_text = f"📊 РЕЗУЛЬТАТЫ МАССОВОЙ ЗАГРУЗКИ:\n\n"
    result_text += f"✅ Добавлено и активировано: {added_accounts}\n"
    result_text += f"⚠️ Уже существует: {already_exists}\n" 
    result_text += f"❌ Ошибки: {failed_accounts}\n"
    result_text += f"📋 Всего обработано: {total_accounts}\n\n"
    result_text += f"🔐 Примечание:\n"
    result_text += f"• Все аккаунты прошли проверку почты\n"
    result_text += f"• Активированы только аккаунты с успешным входом\n"
    result_text += f"• Неактивные аккаунты можно активировать вручную\n"
    result_text += f"• Каждый аккаунт настроен с прокси\n\n"
    
    if failed_accounts_list:
        result_text += "❌ ОШИБКИ:\n"
        for error in failed_accounts_list[:10]:  # Показываем только первые 10 ошибок
            result_text += f"• {error}\n"
        
        if len(failed_accounts_list) > 10:
            result_text += f"... и еще {len(failed_accounts_list) - 10} ошибок\n"
    
    # Обновляем прогресс и показываем результат
    try:
        progress_message.edit_text(result_text, parse_mode=None)
    except:
        update.message.reply_text(result_text, parse_mode=None)
    
    # Показываем меню аккаунтов
    from telegram_bot.keyboards import get_accounts_menu_keyboard
    keyboard = get_accounts_menu_keyboard()
    
    update.message.reply_text(
        "Операция завершена. Выберите действие:",
        reply_markup=keyboard
    )
    
    # Очищаем состояние
    context.user_data.pop('waiting_for_bulk_accounts', None)
    return ConversationHandler.END


def callback_handler(update, context):
    query = update.callback_query
    
    # 🛡️ ПРОСТАЯ ПРОВЕРКА ДОСТУПА ДЛЯ КНОПОК (по образцу из официальной документации)
    user_id = update.effective_user.id if update.effective_user else None
    if user_id:
        try:
            from telegram_bot.middleware.smart_access_check import SmartAccessManager
            access_manager = SmartAccessManager()
            has_access = access_manager.check_access_fast(user_id)
            
            if not has_access:
                logger.warning(f"🚫 БЛОКИРОВКА КНОПКИ: Пользователь {user_id} без доступа")
                
                # Отвечаем на callback query и показываем alert
                query.answer("🚫 Доступ заблокирован", show_alert=True)
                
                # Отправляем сообщение с предложением перезапустить
                query.message.reply_text(
                    "🚫 Ваш доступ к боту заблокирован.\n\n"
                    "🔄 Для продолжения работы нажмите /start\n"
                    "📞 Если проблема повторяется - обратитесь к администратору"
                )
                
                # ПРОСТОЙ RETURN как в официальной документации (без ApplicationHandlerStop)
                return
                
        except Exception as e:
            logger.error(f"Ошибка проверки доступа в callback: {e}")
            # При ошибке проверки позволяем продолжить работу
    
    # Отвечаем на callback query
    query.answer()
    
    # Парсим данные callback
    data = query.data
    
    if query.data == 'menu_accounts':
        from telegram_bot.keyboards import get_accounts_menu_keyboard
        query.edit_message_text(
            text="👤 *Управление аккаунтами*\n\n"
            "Выберите действие из списка ниже:",
            reply_markup=get_accounts_menu_keyboard(),
            parse_mode=ParseMode.MARKDOWN
        )

    elif query.data == 'menu_tasks':
        # Перенаправляем на меню публикаций
        from telegram_bot.keyboards import get_publications_menu_keyboard
        query.edit_message_text(
            "📤 *Меню публикаций*\n\nВыберите тип публикации:",
            reply_markup=get_publications_menu_keyboard(),
            parse_mode=ParseMode.MARKDOWN
        )

    elif query.data == 'menu_proxy':
        keyboard = [
            [InlineKeyboardButton("➕ Добавить прокси", callback_data='add_proxy')],
            [InlineKeyboardButton("📋 Список прокси", callback_data='list_proxies')],
            [InlineKeyboardButton("🔄 Распределить прокси", callback_data='distribute_proxies')],
            [InlineKeyboardButton("🔙 Назад", callback_data='main_menu')]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        query.edit_message_text(
            text="🔄 *Меню управления прокси*\n\n"
            "Выберите действие из списка ниже:",
            reply_markup=reply_markup,
            parse_mode=ParseMode.MARKDOWN
        )

    elif query.data == 'menu_help':
        # Показываем справку из handlers.py
        from telegram_bot.handlers import help_handler as main_help_handler
        return main_help_handler(update, context)

    elif query.data == 'main_menu':
        # Используем новое меню из keyboards.py
        from telegram_bot.keyboards import get_main_menu_keyboard

        query.edit_message_text(
            text="🏠 Главное меню\n\nВыберите раздел:",
            reply_markup=get_main_menu_keyboard()
        )

    # Обработка деталей аккаунта
    elif query.data.startswith('account_details_'):
        try:
            account_id = int(query.data.replace("account_details_", ""))
            account = get_instagram_account(account_id, context=context, user_id=update.effective_user.id)
            
            if account:
                keyboard = [
                    [InlineKeyboardButton("⚙️ Настроить профиль", callback_data=f"profile_setup_{account_id}")],
                    [InlineKeyboardButton("📤 Опубликовать", callback_data=f"publish_to_{account_id}")],
                    [InlineKeyboardButton("🔑 Сменить пароль", callback_data=f"change_password_{account_id}")],
                    [InlineKeyboardButton("🌐 Назначить прокси", callback_data=f"assign_proxy_{account_id}")],
                    [InlineKeyboardButton("❌ Удалить аккаунт", callback_data=f"delete_account_{account_id}")],
                    [InlineKeyboardButton("🔙 Назад к списку", callback_data="list_accounts")]
                ]
                reply_markup = InlineKeyboardMarkup(keyboard)
                
                status_emoji = "✅" if account.is_active else "❌"
                query.edit_message_text(
                    text=f"*Аккаунт:* {account.username} {status_emoji}\n"
                         f"*Email:* {account.email or 'Не указан'}\n"
                         f"*Статус:* {'Активен' if account.is_active else 'Неактивен'}\n"
                         f"*Создан:* {account.created_at.strftime('%d.%m.%Y %H:%M')}\n\n"
                         "Выберите действие:",
                    reply_markup=reply_markup,
                    parse_mode=ParseMode.MARKDOWN
                )
            else:
                query.edit_message_text(
                    "❌ Аккаунт не найден",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="list_accounts")]])
                )
        except (ValueError, Exception) as e:
            query.edit_message_text(
                "❌ Ошибка: неверный формат данных",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="list_accounts")]])
            )

    # Обработка смены пароля
    elif query.data.startswith('change_password_'):
        try:
            account_id = int(query.data.replace("change_password_", ""))
            query.edit_message_text(
                "🔑 Функция смены пароля находится в разработке.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=f"account_details_{account_id}")]])
            )
        except (ValueError, Exception):
            query.edit_message_text(
                "❌ Ошибка: неверный формат данных",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="list_accounts")]])
            )

    # Обработка назначения прокси
    elif query.data.startswith('assign_proxy_'):
        try:
            account_id = int(query.data.replace("assign_proxy_", ""))
            query.edit_message_text(
                "🌐 Функция назначения прокси находится в разработке.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=f"account_details_{account_id}")]])
            )
        except (ValueError, Exception):
            query.edit_message_text(
                "❌ Ошибка: неверный формат данных",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="list_accounts")]])
            )

    # Обработка удаления аккаунта
    elif query.data.startswith('delete_account_'):
        try:
            account_id = int(query.data.replace("delete_account_", ""))
            account = get_instagram_account(account_id, context=context, user_id=update.effective_user.id)
            
            if account:
                keyboard = [
                    [InlineKeyboardButton("✅ Да, удалить", callback_data=f"confirm_delete_{account_id}")],
                    [InlineKeyboardButton("❌ Нет, отмена", callback_data=f"account_details_{account_id}")]
                ]
                reply_markup = InlineKeyboardMarkup(keyboard)
                
                query.edit_message_text(
                    f"⚠️ *Подтверждение удаления*\n\n"
                    f"Вы действительно хотите удалить аккаунт *{account.username}*?\n\n"
                    f"Это действие нельзя отменить!",
                    reply_markup=reply_markup,
                    parse_mode=ParseMode.MARKDOWN
                )
            else:
                query.edit_message_text(
                    "❌ Аккаунт не найден",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="list_accounts")]])
                )
        except (ValueError, Exception):
            query.edit_message_text(
                "❌ Ошибка: неверный формат данных",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="list_accounts")]])
            )

    # Подтверждение удаления аккаунта
    elif query.data.startswith('confirm_delete_'):
        try:
            account_id = int(query.data.replace("confirm_delete_", ""))
            from database.db_manager import delete_instagram_account
            account = get_instagram_account(account_id, context=context, user_id=update.effective_user.id)
            
            if account:
                success, message = delete_instagram_account(account_id)
                if success:
                    query.edit_message_text(
                        f"✅ Аккаунт *{account.username}* успешно удален!",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 К списку аккаунтов", callback_data="list_accounts")]]),
                        parse_mode=ParseMode.MARKDOWN
                    )
                else:
                    query.edit_message_text(
                        f"❌ Ошибка при удалении аккаунта: {message}",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=f"account_details_{account_id}")]])
                    )
            else:
                query.edit_message_text(
                    "❌ Аккаунт не найден",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="list_accounts")]])
                )
        except (ValueError, Exception):
            query.edit_message_text(
                "❌ Ошибка: неверный формат данных",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="list_accounts")]])
            )

    # Обработка публикации в аккаунт
    elif query.data.startswith('publish_to_'):
        try:
            account_id = int(query.data.replace("publish_to_", ""))
            query.edit_message_text(
                "🌐 Функция публикации находится в разработке.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=f"account_details_{account_id}")]])
            )
        except (ValueError, Exception):
            query.edit_message_text(
                "❌ Ошибка: неверный формат данных",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="list_accounts")]])
            )

    elif query.data == 'upload_accounts':
        # Отправляем новое сообщение вместо редактирования текущего
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data='menu_accounts')]]
        reply_markup = InlineKeyboardMarkup(keyboard)

        query.edit_message_text(
            "Отправьте TXT файл с аккаунтами Instagram.\n\n"
            "Формат файла:\n"
            "username:password\n"
            "username:password\n"
            "...\n\n"
            "Каждый аккаунт должен быть на новой строке в формате username:password",
            reply_markup=reply_markup
        )

        # Устанавливаем состояние для ожидания файла
        context.user_data['waiting_for_accounts_file'] = True
        return WAITING_ACCOUNTS_FILE

    elif query.data == 'list_accounts':
        # Вызываем обработчик списка аккаунтов
        list_accounts_handler(update, context)

    # bulk_add_accounts теперь обрабатывается через ConversationHandler

    elif query.data == 'profile_setup':
        # Запускаем селектор аккаунтов для настройки профиля
        query.data = 'profile_select_start'
        # Передаем управление обработчикам из profile_handlers
        return

    # Обработка новых меню
    elif query.data == 'menu_publications':
        from telegram_bot.keyboards import get_publications_menu_keyboard
        query.edit_message_text(
            text="📤 *Меню публикаций*\n\nВыберите тип публикации:",
            reply_markup=get_publications_menu_keyboard(),
            parse_mode=ParseMode.MARKDOWN
        )
    
    elif query.data == "menu_scheduled":
        from telegram_bot.keyboards import get_scheduled_menu_keyboard
        query.edit_message_text(
            "🗓️ *Меню запланированных публикаций*\n\nВыберите тип публикации для планирования:",
            reply_markup=get_scheduled_menu_keyboard(),
            parse_mode='Markdown'
        )
    
    # Обработчики запланированных публикаций
    elif query.data == "schedule_post":
        from telegram_bot.handlers.publish import start_schedule_post
        return start_schedule_post(update, context)
    
    elif query.data == "schedule_story":
        # Устанавливаем флаг планирования и запускаем выбор аккаунтов
        context.user_data['is_scheduled_post'] = True
        from telegram_bot.handlers.publish import start_story_publish
        return start_story_publish(update, context)
    
    elif query.data == "schedule_reels":
        # Устанавливаем флаг планирования и запускаем выбор аккаунтов
        context.user_data['is_scheduled_post'] = True
        from telegram_bot.handlers.publish import start_reels_publish
        return start_reels_publish(update, context)
    
    elif query.data == "schedule_igtv_blocked":
        from telegram_bot.keyboards import get_scheduled_menu_keyboard
        query.edit_message_text(
            "🔒 *IGTV планирование временно недоступно*\n\n"
            "🚧 Функция находится в разработке и будет доступна в ближайших обновлениях.\n\n"
            "📱 Пока что вы можете использовать другие типы публикаций.\n\n"
            "Спасибо за понимание! 🙏",
            reply_markup=get_scheduled_menu_keyboard(),
            parse_mode='Markdown'
        )
    
    elif query.data == "view_schedule":
        from telegram_bot.keyboards import get_scheduled_menu_keyboard
        query.edit_message_text(
            "🗓️ *Просмотр расписания*\n\n"
            "🚧 Функция находится в разработке и будет доступна в ближайших обновлениях.\n\n"
            "Здесь будет отображаться список всех запланированных публикаций с возможностью редактирования и удаления.\n\n"
            "Спасибо за понимание! 🙏",
            reply_markup=get_scheduled_menu_keyboard(),
            parse_mode='Markdown'
        )
    
    elif query.data == "scheduled_history":
        from telegram_bot.keyboards import get_scheduled_menu_keyboard
        query.edit_message_text(
            "📊 *История запланированных публикаций*\n\n"
            "🚧 Функция находится в разработке и будет доступна в ближайших обновлениях.\n\n"
            "Здесь будет отображаться история всех запланированных публикаций с их статусами.\n\n"
            "Спасибо за понимание! 🙏",
            reply_markup=get_scheduled_menu_keyboard(),
            parse_mode='Markdown'
        )
    
    elif query.data == 'menu_warmup':
        from telegram_bot.keyboards import get_warmup_menu_keyboard
        query.edit_message_text(
            text="🔥 *Меню прогрева аккаунтов*\n\nВыберите действие:",
            reply_markup=get_warmup_menu_keyboard(),
            parse_mode=ParseMode.MARKDOWN
        )
    
    elif query.data == 'menu_statistics':
        from telegram_bot.keyboards import get_statistics_menu_keyboard
        query.edit_message_text(
            text="📊 *Меню статистики*\n\nВыберите раздел:",
            reply_markup=get_statistics_menu_keyboard(),
            parse_mode=ParseMode.MARKDOWN
        )
    
    elif query.data == 'menu_settings':
        from telegram_bot.keyboards import get_settings_menu_keyboard
        query.edit_message_text(
            text="⚙️ *Меню настроек*\n\nВыберите раздел:",
            reply_markup=get_settings_menu_keyboard(),
            parse_mode=ParseMode.MARKDOWN
        )

    elif query.data in ['publication_stats', 'publish_now', 'schedule_publish']:
        query.edit_message_text(
            text=f"Функция '{query.data}' находится в разработке.\n\n"
            "Пожалуйста, попробуйте позже.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data='main_menu')]])
        )

    elif query.data == 'folders_menu':
        from telegram_bot.keyboards import get_folders_menu_keyboard
        query.edit_message_text(
            text="📁 *Управление папками аккаунтов*\n\nВыберите действие:",
            reply_markup=get_folders_menu_keyboard(),
            parse_mode=ParseMode.MARKDOWN
        )
    
    # Обработчики для папок
    elif query.data == 'list_folders':
        from telegram_bot.handlers.group_handlers import list_groups_handler
        return list_groups_handler(update, context)
    
    elif query.data == 'create_folder':
        from telegram_bot.handlers.group_handlers import create_group_handler
        return create_group_handler(update, context)
    
    elif query.data == 'rename_folder':
        query.edit_message_text(
            text="✏️ Функция переименования папок в разработке.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data='folders_menu')]])
        )
    
    elif query.data == 'delete_folder':
        query.edit_message_text(
            text="❌ Функция удаления папок в разработке.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data='folders_menu')]])
        )
    
    elif query.data == 'view_folder_accounts':
        query.edit_message_text(
            text="👁️ Функция просмотра аккаунтов в папке в разработке.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data='folders_menu')]])
        )
    
    # Обработчики для работы с папками
    elif query.data.startswith('view_group_'):
        from telegram_bot.handlers.group_handlers import view_group_handler
        return view_group_handler(update, context)
    
    elif query.data.startswith('list_folders_page_'):
        from telegram_bot.handlers.group_handlers import list_groups_handler
        return list_groups_handler(update, context)
    
    elif query.data.startswith('icon_'):
        # Передаем обработку выбора иконки в group_handlers
        from telegram_bot.handlers.group_handlers import process_group_icon
        return process_group_icon(update, context)
    
    elif query.data == 'skip_group_description':
        from telegram_bot.handlers.group_handlers import process_group_description
        return process_group_description(update, context)
    
    # Обработчики массовых действий с профилями
    elif query.data == 'bulk_edit_name':
        from telegram_bot.handlers.profile_handlers import bulk_edit_name
        return bulk_edit_name(update, context)
    
    elif query.data == 'bulk_edit_username':
        from telegram_bot.handlers.profile_handlers import bulk_edit_username
        return bulk_edit_username(update, context)
    
    elif query.data == 'bulk_edit_bio':
        from telegram_bot.handlers.profile_handlers import bulk_edit_bio
        return bulk_edit_bio(update, context)
    
    elif query.data == 'bulk_add_link':
        from telegram_bot.handlers.profile_handlers import bulk_add_link
        return bulk_add_link(update, context)
    
    elif query.data == 'bulk_set_avatar':
        from telegram_bot.handlers.profile_handlers import bulk_set_avatar
        return bulk_set_avatar(update, context)
    
    elif query.data == 'bulk_delete_avatar':
        from telegram_bot.handlers.profile_handlers import bulk_delete_avatar
        return bulk_delete_avatar(update, context)
    
    elif query.data == 'noop':
        # Для неактивных кнопок просто отвечаем на callback
        query.answer()
    
    # Обработчики типов публикаций
    elif query.data == "publish_post":
        from telegram_bot.handlers.publish import start_post_publish
        return start_post_publish(update, context)
    
    elif query.data == "publish_story":
        from telegram_bot.handlers.publish import start_story_publish
        return start_story_publish(update, context)
    
    elif query.data == "publish_igtv":
        from telegram_bot.handlers.publish import start_igtv_publish
        return start_igtv_publish(update, context)
    
    elif query.data == "publish_igtv_blocked":
        from telegram_bot.keyboards import get_publications_menu_keyboard
        query.edit_message_text(
            "🔒 *IGTV публикация временно недоступна*\n\n"
            "🚧 Функция находится в разработке и будет доступна в ближайших обновлениях.\n\n"
            "📱 Пока что вы можете использовать:\n"
            "• 📸 Публикация постов\n"
            "• 📱 Истории\n"
            "• 🎥 Reels\n\n"
            "Спасибо за понимание! 🙏",
            reply_markup=get_publications_menu_keyboard(),
            parse_mode='Markdown'
        )
    
    elif query.data == "scheduled_posts":
        from telegram_bot.handlers.publish import show_scheduled_posts
        return show_scheduled_posts(update, context)
    
    elif query.data == "publication_history":
        from telegram_bot.handlers.publish import show_publication_history
        return show_publication_history(update, context)
    
    # Обработчики выбора аккаунтов для публикации
    elif query.data.startswith('post_account_'):
        from telegram_bot.handlers.publish import handle_post_account_selection
        return handle_post_account_selection(update, context)
    
    elif query.data.startswith('story_account_'):
        from telegram_bot.handlers.publish import handle_story_account_selection
        return handle_story_account_selection(update, context)
    
    elif query.data.startswith('igtv_account_'):
        from telegram_bot.handlers.publish import handle_igtv_account_selection
        return handle_igtv_account_selection(update, context)
    
    # Обработчики callback'ов для Reels
    elif query.data.startswith('reels_'):
        from telegram_bot.handlers.publish import handle_reels_callbacks
        return handle_reels_callbacks(update, context)
    
    elif query.data == "publish_reels":
        from telegram_bot.handlers.publish import start_reels_publish
        return start_reels_publish(update, context)
    
    # Обработчики аналитики
    elif query.data == "general_stats":
        from telegram_bot.handlers.analytics_handlers import start_general_analytics
        start_general_analytics(update, context)
    
    elif query.data == "accounts_stats":
        from telegram_bot.handlers.analytics_handlers import start_accounts_analytics
        start_accounts_analytics(update, context)
    
    elif query.data == "publications_stats":
        from telegram_bot.handlers.analytics_handlers import start_publications_analytics
        start_publications_analytics(update, context)
    
    elif query.data == "warmup_stats":
        from telegram_bot.keyboards import get_statistics_menu_keyboard
        query.edit_message_text("🔥 Статистика по прогреву в разработке", reply_markup=get_statistics_menu_keyboard())
    
    # Обработчики прогрева
    elif query.data == "quick_warmup":
        query.edit_message_text("⚡ Быстрый прогрев в разработке",
                               reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="menu_warmup")]]))
    
    elif query.data == "smart_warmup":
        # Перенаправляем на новую систему Advanced Warmup 2.0
        from telegram_bot.handlers.automation_handlers import smart_warm_command
        smart_warm_command(update, context)
    
    elif query.data == "warmup_status":
        # Перенаправляем на новую команду /status
        from telegram_bot.handlers.automation_handlers import status_command
        status_command(update, context)
    
    elif query.data == "warmup_settings":
        # Перенаправляем на новую команду /limits
        from telegram_bot.handlers.automation_handlers import limits_command
        limits_command(update, context)
    
    elif query.data == "warmup_analytics":
        query.edit_message_text("📈 Аналитика прогрева в разработке",
                               reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="menu_warmup")]]))
    
    # Новые обработчики Advanced Warmup 2.0  
    elif query.data == "smart_warm_menu":
        from telegram_bot.handlers.automation_handlers import smart_warm_command
        smart_warm_command(update, context)
    
    elif query.data == "status":
        from telegram_bot.handlers.automation_handlers import status_command
        status_command(update, context)
    
    elif query.data == "limits":
        from telegram_bot.handlers.automation_handlers import limits_command
        limits_command(update, context)
    
    elif query.data == "interest_warmup_menu":
        from telegram_bot.handlers.warmup_interest_handlers import show_interest_warmup_menu
        show_interest_warmup_menu(update, context)
    
    elif query.data == "charts":
        from telegram_bot.keyboards import get_statistics_menu_keyboard
        query.edit_message_text("📈 Графики в разработке", reply_markup=get_statistics_menu_keyboard())
    
    # Обработчики действий аналитики публикаций
    elif query.data in ["analytics_recent_posts", "analytics_top_likes", "analytics_top_comments", "analytics_detailed", "analytics_stories"]:
        from telegram_bot.handlers.analytics_handlers import handle_analytics_action
        handle_analytics_action(update, context, query.data)
    
    # Обработчики множественной аналитики
    elif query.data in ["analytics_comparison", "analytics_summary", "analytics_top_all", "analytics_detailed_all"]:
        from telegram_bot.handlers.analytics_handlers import handle_multiple_analytics_action
        handle_multiple_analytics_action(update, context, query.data)
    
    elif query.data == "analytics_menu":
        from telegram_bot.keyboards import get_statistics_menu_keyboard
        query.edit_message_text(
            text="📊 Меню статистики\n\nВыберите раздел:",
            reply_markup=get_statistics_menu_keyboard()
        )
    
    # Обработчик конвертации в бизнес-аккаунт
    elif query.data.startswith('convert_business_'):
        from profile_setup.links_manager import convert_to_business_account
        convert_to_business_account(update, context)
    
    else:
        # Для неизвестных callback_data логируем для отладки
        logger.warning(f"Необработанный callback_data: {query.data}")
        # Не изменяем сообщение, чтобы другие обработчики могли обработать callback
        pass

def text_handler(update, context):
    from telegram_bot.keyboards import get_main_menu_keyboard

    update.message.reply_text(
        "Я понимаю только команды. Используйте /help для получения списка доступных команд или выберите раздел из меню ниже:",
        reply_markup=get_main_menu_keyboard()
    )

def error_handler(update, context):
    """Обрабатывает ошибки"""
    # Проверяем, является ли ошибка "Query is too old"
    if "Query is too old" in str(context.error):
        logger.warning(f"Устаревший запрос: {update}")
        return  # Просто игнорируем эту ошибку
    
    # Проверяем, является ли ошибка связанной с парсингом entities
    if "Can't parse entities" in str(context.error) or "can't find end of the entity" in str(context.error):
        logger.warning(f"Ошибка парсинга entities: {context.error}")
        return  # Игнорируем ошибки парсинга entities, так как мы их исправили

    logger.error(f"Ошибка при обработке обновления {update}: {context.error}")

    if update and update.effective_chat:
        try:
            context.bot.send_message(
                chat_id=update.effective_chat.id,
                text="Произошла ошибка при обработке вашего запроса. Пожалуйста, попробуйте еще раз.",
                parse_mode=None  # Отключаем парсинг чтобы избежать дополнительных ошибок entities
            )
        except Exception as e:
            logger.error(f"Не удалось отправить сообщение об ошибке: {e}")

def setup_bot(updater):
    dp = updater.dispatcher

    # КРИТИЧЕСКИ ВАЖНО: TypeHandler для триггерных сообщений блокировки
    # Добавляем с ВЫСОЧАЙШИМ приоритетом (-2) чтобы перехватывать ПЕРЕД всеми остальными
    try:
        from telegram_bot.trigger_block_handler import handle_trigger_block_message
        from telegram.ext import TypeHandler
        
        trigger_handler = TypeHandler(Update, handle_trigger_block_message)
        dp.add_handler(trigger_handler, group=-2)  # Высочайший приоритет
        logger.info("🚫 Триггерный обработчик блокировки добавлен с приоритетом -2")
        
    except ImportError as e:
        logger.error(f"⚠️ Не удалось загрузить триггерный обработчик блокировки: {e}")
        logger.info("🔄 Продолжаем без триггерной системы блокировки")

    # Основные обработчики
    dp.add_handler(CommandHandler("start", start_handler))
    dp.add_handler(CommandHandler("help", help_handler))
    
    # Добавляем остальные обработчики только если они доступны
    try:
        dp.add_handler(CommandHandler("account", account_handler))
        dp.add_handler(CommandHandler("group", group_handler))
        dp.add_handler(CommandHandler("system", system_handler))
        dp.add_handler(CommandHandler("analytics", analytics_handler))
    except NameError as e:
        logger.warning(f"Некоторые обработчики недоступны: {e}")

    # Регистрируем ConversationHandler для добавления аккаунта
    add_account_conv_handler = ConversationHandler(
        entry_points=[
            CommandHandler("add_account", add_account),
            CallbackQueryHandler(add_account, pattern='^add_account$')
        ],
        states={
            ENTER_USERNAME: [MessageHandler(Filters.text & ~Filters.command, enter_username)],
            ENTER_PASSWORD: [MessageHandler(Filters.text & ~Filters.command, enter_password)],
            ENTER_EMAIL: [MessageHandler(Filters.text & ~Filters.command, enter_email)],
            ENTER_EMAIL_PASSWORD: [MessageHandler(Filters.text & ~Filters.command, enter_email_password)],
            CONFIRM_ACCOUNT: [CallbackQueryHandler(confirm_add_account, pattern='^confirm_add_account$')],
            ENTER_VERIFICATION_CODE: [MessageHandler(Filters.text & ~Filters.command, enter_verification_code)]
        },
        fallbacks=[
            CallbackQueryHandler(cancel_add_account, pattern='^cancel_add_account$'),
            CallbackQueryHandler(lambda u, c: ConversationHandler.END, pattern='^menu_accounts$'),
            CommandHandler("cancel", cancel_handler)
        ]
    )

    dp.add_handler(add_account_conv_handler)

    # Функция для инициализации массовой загрузки
    def start_bulk_add_accounts(update, context):
        query = update.callback_query
        query.answer()
        
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data='menu_accounts')]]
        reply_markup = InlineKeyboardMarkup(keyboard)

        query.edit_message_text(
            "📥 Массовая загрузка аккаунтов Instagram\n\n"
            "Отправьте TXT файл с аккаунтами в формате:\n"
            "username:password\n\n"
            "Или введите аккаунты текстом в формате:\n"
            "username:password:email:email_password\n\n"
            "Каждый аккаунт должен быть на новой строке.",
            reply_markup=reply_markup
        )
        
        # Устанавливаем состояние для ожидания файла или текста
        context.user_data['waiting_for_accounts_file'] = True
        return WAITING_ACCOUNTS_FILE

    # ConversationHandler для массовой загрузки аккаунтов через кнопку
    bulk_add_conv_handler = ConversationHandler(
        entry_points=[CallbackQueryHandler(start_bulk_add_accounts, pattern='^bulk_add_accounts$')],
        states={
            WAITING_ACCOUNTS_FILE: [
                MessageHandler(Filters.document.file_extension("txt"), bulk_upload_accounts_file),
                MessageHandler(Filters.text & ~Filters.command, handle_bulk_accounts_text)
            ]
        },
        fallbacks=[
            CommandHandler("cancel", cancel_handler),
            CallbackQueryHandler(lambda u, c: ConversationHandler.END, pattern='menu_accounts')
        ],
        name="bulk_add_accounts_conversation",
        persistent=False,
    )
    dp.add_handler(bulk_add_conv_handler)

    # Добавляем обработчик для массовой загрузки аккаунтов
    dp.add_handler(CommandHandler("bulk_add_accounts", bulk_add_accounts_command, pass_args=True))

    # Обработчик файлов теперь находится в ConversationHandler в account_handlers.py
    # Убираем дублирующий обработчик чтобы избежать конфликтов

    # ВАЖНО: Сначала регистрируем специфичные обработчики из модулей
    # Добавляем все обработчики из модулей
    try:
        for handler in get_all_handlers():
            dp.add_handler(handler)
    except Exception as e:
        logger.error(f"Ошибка при добавлении обработчиков: {e}")
        import traceback
        traceback.print_exc()

    # Добавляем обработчики для настройки профиля (старые)
    for handler in get_profile_handlers():
        dp.add_handler(handler)
        
    # Обработчик массовых операций уже включен в profile_setup handlers

    # Добавляем обработчики из profile_setup модулей
    from profile_setup import get_profile_handlers as get_profile_setup_handlers_real
    for handler in get_profile_setup_handlers_real():
        dp.add_handler(handler)
    
    # Добавляем обработчики автоматизации
    from telegram_bot.handlers.automation_handlers import register_automation_handlers
    register_automation_handlers(dp)

    # ConversationHandler для Reels теперь добавляется через get_publish_handlers()

    # Обработчик callback-запросов регистрируем ПОСЛЕДНИМ
    # чтобы он не перехватывал callback'и, обрабатываемые в модулях
    dp.add_handler(CallbackQueryHandler(callback_handler))
    
    # Добавляем fallback обработчик для неизвестных callback
    def fallback_callback_handler(update, context):
        query = update.callback_query
        query.answer("❌ Неизвестная команда. Используйте меню ниже.")
        logger.warning(f"Неизвестный callback: {query.data}")

    dp.add_handler(CallbackQueryHandler(fallback_callback_handler))

    # Добавляем обработчик для кодов подтверждения
    from telegram_bot.handlers.account_handlers import verification_code_handler
    dp.add_handler(MessageHandler(
        Filters.regex(r'^\d{6}$') & ~Filters.command,
        verification_code_handler
    ))

    # Добавляем обработчик для повтора задач
    dp.add_handler(CallbackQueryHandler(retry_task_callback, pattern=r'^retry_task_\d+$'))

    # Обработчик текстовых сообщений (должен быть после обработчика кодов)
    dp.add_handler(MessageHandler(
        Filters.text & ~Filters.command,
        text_handler
    ))

    # Обработчик ошибок
    dp.add_error_handler(error_handler)

    logger.info("Бот настроен и готов к работе")

def get_profile_setup_handlers():
    """Возвращает обработчики для настройки профиля из модулей profile_setup"""
    from telegram_bot.handlers.profile_handlers import (
        edit_profile_name, save_profile_name,
        edit_profile_username, save_profile_username,
        edit_profile_bio, save_profile_bio,
        edit_profile_links, save_profile_links,
        add_profile_photo, save_profile_photo,
        add_post, save_post,
        delete_all_posts,
        setup_profile_menu,
        handle_bulk_profile_action
    )
    
    # ConversationHandler для редактирования имени
    name_conv_handler = ConversationHandler(
        entry_points=[CallbackQueryHandler(edit_profile_name, pattern=r'^edit_name_\d+$')],
        states={
            EDIT_NAME: [MessageHandler(Filters.text & ~Filters.command, save_profile_name)],
        },
        fallbacks=[
            CallbackQueryHandler(lambda u, c: ConversationHandler.END, pattern=r'^profile_account_\d+$'),
            CommandHandler("cancel", cancel_handler)
        ]
    )
    
    # ConversationHandler для редактирования username
    username_conv_handler = ConversationHandler(
        entry_points=[CallbackQueryHandler(edit_profile_username, pattern=r'^edit_username_\d+$')],
        states={
            EDIT_USERNAME: [MessageHandler(Filters.text & ~Filters.command, save_profile_username)],
        },
        fallbacks=[
            CallbackQueryHandler(lambda u, c: ConversationHandler.END, pattern=r'^profile_account_\d+$'),
            CommandHandler("cancel", cancel_handler)
        ]
    )
    
    # ConversationHandler для редактирования био
    bio_conv_handler = ConversationHandler(
        entry_points=[CallbackQueryHandler(edit_profile_bio, pattern=r'^edit_bio_\d+$')],
        states={
            EDIT_BIO: [MessageHandler(Filters.text & ~Filters.command, save_profile_bio)],
        },
        fallbacks=[
            CallbackQueryHandler(lambda u, c: ConversationHandler.END, pattern=r'^profile_account_\d+$'),
            CommandHandler("cancel", cancel_handler)
        ]
    )
    
    # ConversationHandler для редактирования ссылок
    links_conv_handler = ConversationHandler(
        entry_points=[CallbackQueryHandler(edit_profile_links, pattern=r'^edit_links_\d+$')],
        states={
            EDIT_LINKS: [MessageHandler(Filters.text & ~Filters.command, save_profile_links)],
        },
        fallbacks=[
            CallbackQueryHandler(lambda u, c: ConversationHandler.END, pattern=r'^profile_account_\d+$'),
            CommandHandler("cancel", cancel_handler)
        ]
    )
    
    # ConversationHandler для изменения аватара
    avatar_conv_handler = ConversationHandler(
        entry_points=[CallbackQueryHandler(add_profile_photo, pattern=r'^edit_avatar_\d+$')],
        states={
            ADD_PHOTO: [MessageHandler(Filters.photo | Filters.document, save_profile_photo)],
        },
        fallbacks=[
            CallbackQueryHandler(lambda u, c: ConversationHandler.END, pattern=r'^profile_account_\d+$'),
            CommandHandler("cancel", cancel_handler)
        ]
    )
    
    # ConversationHandler для добавления поста
    post_conv_handler = ConversationHandler(
        entry_points=[CallbackQueryHandler(add_post, pattern=r'^add_post_\d+$')],
        states={
            ADD_POST: [MessageHandler(Filters.photo | Filters.video | Filters.document, save_post)],
        },
        fallbacks=[
            CallbackQueryHandler(lambda u, c: ConversationHandler.END, pattern=r'^profile_account_\d+$'),
            CommandHandler("cancel", cancel_handler)
        ]
    )
    
    # ConversationHandler для массовых действий
    bulk_actions_handler = ConversationHandler(
        entry_points=[
            CallbackQueryHandler(lambda u, c: EDIT_NAME, pattern='^bulk_edit_name$'),
            CallbackQueryHandler(lambda u, c: EDIT_USERNAME, pattern='^bulk_edit_username$'),
            CallbackQueryHandler(lambda u, c: EDIT_BIO, pattern='^bulk_edit_bio$'),
            CallbackQueryHandler(lambda u, c: EDIT_LINKS, pattern='^bulk_add_link$'),
            CallbackQueryHandler(lambda u, c: ADD_PHOTO, pattern='^bulk_set_avatar$'),
        ],
        states={
            EDIT_NAME: [MessageHandler(Filters.text & ~Filters.command, handle_bulk_profile_action)],
            EDIT_USERNAME: [MessageHandler(Filters.text & ~Filters.command, handle_bulk_profile_action)],
            EDIT_BIO: [MessageHandler(Filters.text & ~Filters.command, handle_bulk_profile_action)],
            EDIT_LINKS: [MessageHandler(Filters.text & ~Filters.command, handle_bulk_profile_action)],
            ADD_PHOTO: [MessageHandler(Filters.photo | Filters.document, handle_bulk_profile_action)],
        },
        fallbacks=[
            CallbackQueryHandler(lambda u, c: ConversationHandler.END, pattern='^profile_setup$'),
            CommandHandler("cancel", cancel_handler)
        ]
    )
    
    return [
        # Обработчик меню настройки профиля для конкретного аккаунта
        CallbackQueryHandler(setup_profile_menu, pattern=r'^profile_account_\d+$'),
        # ConversationHandlers для каждого действия
        name_conv_handler,
        username_conv_handler,
        bio_conv_handler,
        links_conv_handler,
        avatar_conv_handler,
        post_conv_handler,
        bulk_actions_handler,
        # Обработчик очистки профиля
        CallbackQueryHandler(delete_all_posts, pattern=r'^delete_all_posts_\d+$'),
    ]
    
    # Регистрируем обработчики прогрева по интересам
    from telegram_bot.handlers.warmup_interest_handlers import INTEREST_WARMUP_HANDLERS
    handlers.extend(INTEREST_WARMUP_HANDLERS)
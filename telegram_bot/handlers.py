import logging
import os
from datetime import datetime
from pathlib import Path

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ParseMode
from telegram.ext import CallbackContext, ConversationHandler

from config import MEDIA_DIR, ADMIN_USER_IDS
from utils.access_manager import has_access
from database.db_manager import (
    add_instagram_account,
    add_proxy, get_proxies, assign_proxy_to_account,
    create_publish_task, delete_instagram_account
)
from database.safe_user_wrapper import get_user_instagram_accounts as get_instagram_accounts, get_user_instagram_account as get_instagram_account, extract_user_id_from_update
from .keyboards import (
    get_main_menu_keyboard,
    get_accounts_menu_keyboard, get_tasks_menu_keyboard, 
    get_proxy_menu_keyboard, get_accounts_list_keyboard, 
    get_account_actions_keyboard
)
from utils.proxy_manager import distribute_proxies, check_proxy, check_all_proxies
from instagram.post_manager import PostManager
from instagram.reels_manager import ReelsManager, publish_reels_in_parallel

# Импорты продвинутых систем
from instagram.improved_account_warmer import ImprovedAccountWarmer, warm_account_improved
from instagram.health_monitor import AdvancedHealthMonitor
from instagram.activity_limiter import ActivityLimiter
from instagram.advanced_verification import AdvancedVerificationSystem
from instagram.lifecycle_manager import AccountLifecycleManager
from instagram.predictive_monitor import PredictiveMonitor

# Импорты из нового модуля profile_setup
from profile_setup.name_manager import update_profile_name
from profile_setup.username_manager import update_username
from profile_setup.bio_manager import update_biography, clear_biography
from profile_setup.links_manager import update_profile_links
from profile_setup.avatar_manager import update_profile_picture, remove_profile_picture
from profile_setup.post_manager import upload_photo, upload_video, delete_all_posts
from profile_setup.cleanup_manager import clear_profile

# Импорты новых улучшенных систем
from telegram_bot.enhanced_handlers import get_enhanced_handlers, register_enhanced_account_handlers

logger = logging.getLogger(__name__)

# Состояния для ConversationHandler - импорт из states.py
from .states import (
    WAITING_USERNAME, WAITING_PASSWORD, WAITING_ACCOUNT_INFO,
    WAITING_ACCOUNTS_FILE, WAITING_COOKIES_INFO, WAITING_NEW_PASSWORD,
    WAITING_ACCOUNT_SELECTION, WAITING_BIO_OR_AVATAR,
    WAITING_TASK_TYPE, WAITING_MEDIA, WAITING_CAPTION,
    WAITING_SCHEDULE_TIME, WAITING_PROXY_INFO, WAITING_PROFILE_PHOTO
)

# Временное хранилище данных пользователя
user_data_store = {}

def start_handler(update: Update, context: CallbackContext):
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
    from .keyboards import get_start_keyboard
    
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
        reply_markup=get_start_keyboard(),
        parse_mode='Markdown'
    )



def help_handler(update: Update, context: CallbackContext):
    """Обработчик команды /help"""
    user_id = update.effective_user.id
    
    if not has_access(user_id):
        return
    
    help_text = (
        "*Основные команды:*\n"
        "/start - Запустить бота\n"
        "/help - Показать эту справку\n\n"
        
        "*Управление аккаунтами:*\n"
        "/accounts - Меню аккаунтов\n"
        "/add_account - Добавить аккаунт Instagram\n"
        "/list_accounts - Список добавленных аккаунтов\n"
        "/profile_setup - Настройка профиля\n\n"
        
        "*Публикация контента:*\n"
        "/tasks - Меню задач\n"
        "/publish_now - Опубликовать сейчас\n"
        "/schedule_publish - Отложенная публикация\n\n"
        
        "*Управление прокси:*\n"
        "/proxy - Меню прокси\n"
        "/add_proxy - Добавить прокси\n"
        "/distribute_proxies - Распределить прокси\n"
        "/list_proxies - Список добавленных прокси\n\n"
        
        "*Дополнительно:*\n"
        "/cancel - Отменить текущую операцию"
    )
    
    update.message.reply_text(help_text, parse_mode=ParseMode.MARKDOWN)

def accounts_handler(update: Update, context: CallbackContext):
    """Обработчик команды /accounts"""
    user_id = update.effective_user.id
    
    if not has_access(user_id):
        return
    
    update.message.reply_text(
        "Меню управления аккаунтами Instagram:",
        reply_markup=get_accounts_menu_keyboard()
    )

def tasks_handler(update: Update, context: CallbackContext):
    """Обработчик команды /tasks"""
    user_id = update.effective_user.id
    
    if not has_access(user_id):
        return
    
    update.message.reply_text(
        "Меню публикации контента:",
        reply_markup=get_tasks_menu_keyboard()
    )

def proxy_handler(update: Update, context: CallbackContext):
    """Обработчик команды /proxy"""
    user_id = update.effective_user.id
    
    if not has_access(user_id):
        return
    
    update.message.reply_text(
        "Меню управления прокси:",
        reply_markup=get_proxy_menu_keyboard()
    )

# Обработчики для продвинутых систем
def advanced_handler(update: Update, context: CallbackContext):
    """Обработчик команды /advanced"""
    user_id = update.effective_user.id
    
    if not has_access(user_id):
        return
    
    keyboard = [
        [InlineKeyboardButton("🔍 Health Monitor", callback_data="health_monitor")],
        [InlineKeyboardButton("⚡ Activity Limiter", callback_data="activity_limiter")],
        [InlineKeyboardButton("🚀 Improved Warmer", callback_data="improved_warmer")],
        [InlineKeyboardButton("📊 System Status", callback_data="system_status")],
        [InlineKeyboardButton("🔙 Главное меню", callback_data="main_menu")]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    update.message.reply_text(
        "🎛️ *Продвинутые системы управления*\n\n"
        "Выберите систему:",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=reply_markup
    )

def health_monitor_handler(update: Update, context: CallbackContext):
    """Обработчик команды /health_monitor"""
    user_id = extract_user_id_from_update(update, context)
    user_id = update.effective_user.id
    
    if not has_access(user_id):
        return
    
    update.message.reply_text("🔍 Запускаю проверку здоровья всех аккаунтов...")
    
    try:
        health_monitor = AdvancedHealthMonitor()
        accounts = get_instagram_accounts(context=context, user_id=user_id)
        
        if not accounts:
            update.message.reply_text("❌ Аккаунты не найдены.")
            return
        
        report = "📊 *Отчет Health Monitor*\n\n"
        
        for account in accounts:
            score = health_monitor.calculate_comprehensive_health_score(account.id)
            recommendations = health_monitor.get_health_recommendations(account.id)
            
            status = "🟢" if score >= 80 else "🟡" if score >= 60 else "🔴"
            report += f"{status} *{account.username}*: {score}/100\n"
            
            if recommendations:
                report += f"   💡 {recommendations[0]}\n"
            report += "\n"
        
        update.message.reply_text(report, parse_mode=ParseMode.MARKDOWN)
        
    except Exception as e:
        logger.error(f"Ошибка Health Monitor: {e}")
        update.message.reply_text(f"❌ Ошибка при проверке здоровья: {e}")

def improved_warmer_handler(update: Update, context: CallbackContext):
    """Обработчик команды /improved_warmer"""
    user_id = update.effective_user.id
    
    if not has_access(user_id):
        return
    
    # 🧪 ТЕСТ ИЗОЛЯЦИИ: Используем новую безопасную функцию
    accounts = get_instagram_accounts(context=context, user_id=user_id)
    
    if not accounts:
        update.message.reply_text("❌ Аккаунты не найдены.")
        return
    
    keyboard = []
    for account in accounts:
        keyboard.append([InlineKeyboardButton(
            f"🔥 {account.username}", 
            callback_data=f"improved_warm_{account.id}"
        )])
    
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    update.message.reply_text(
        f"🔥 <b>Выберите аккаунт для прогрева:</b>\n\n"
        f"📊 Найдено аккаунтов: {len(accounts)}\n"
        f"🎯 Функция: improved_warmer_handler (ТЕСТ ИЗОЛЯЦИИ)",
        reply_markup=reply_markup,
        parse_mode='HTML'
    )

def system_status_handler(update: Update, context: CallbackContext):
    """Обработчик команды /system_status"""
    user_id = extract_user_id_from_update(update, context)
    user_id = update.effective_user.id
    
    if not has_access(user_id):
        return
    
    try:
        # Проверяем статус всех систем
        health_monitor = AdvancedHealthMonitor()
        activity_limiter = ActivityLimiter()
        lifecycle_manager = AccountLifecycleManager()
        predictive_monitor = PredictiveMonitor()
        
        accounts = get_instagram_accounts(context=context, user_id=user_id)
        total_accounts = len(accounts)
        
        # Health Monitor Status
        healthy_accounts = 0
        for account in accounts:
            score = health_monitor.calculate_comprehensive_health_score(account.id)
            if score >= 80:
                healthy_accounts += 1
        
        # Activity Limiter Status
        restricted_accounts = 0
        for account in accounts:
            restrictions = activity_limiter.check_current_restrictions(account.id)
            if restrictions:
                restricted_accounts += 1
        
        status_report = (
            f"📊 *Статус всех систем*\n\n"
            f"👥 *Общая информация:*\n"
            f"   Всего аккаунтов: {total_accounts}\n\n"
            
            f"🔍 *Health Monitor:*\n"
            f"   ✅ Здоровых аккаунтов: {healthy_accounts}/{total_accounts}\n"
            f"   📈 Процент здоровья: {int(healthy_accounts/total_accounts*100) if total_accounts > 0 else 0}%\n\n"
            
            f"⚡ *Activity Limiter:*\n"
            f"   🚫 Аккаунтов с ограничениями: {restricted_accounts}/{total_accounts}\n"
            f"   ✅ Свободных аккаунтов: {total_accounts - restricted_accounts}\n\n"
            
            f"🔄 *Lifecycle Manager:*\n"
            f"   🆕 Статус: Все аккаунты отслеживаются\n\n"
            
            f"🛡️ *Predictive Monitor:*\n"
            f"   🎯 Система анализа рисков: Активна\n"
            f"   📊 ML модель: Готова к предсказаниям\n\n"
            
            f"⏰ Последнее обновление: {datetime.now().strftime('%H:%M:%S')}"
        )
        
        update.message.reply_text(status_report, parse_mode=ParseMode.MARKDOWN)
        
    except Exception as e:
        logger.error(f"Ошибка System Status: {e}")
        update.message.reply_text(f"❌ Ошибка при получении статуса систем: {e}")

# Обработчики для аккаунтов
def add_account_handler(update: Update, context: CallbackContext):
    """Обработчик для добавления аккаунта Instagram"""
    user_id = update.effective_user.id
    
    if not has_access(user_id):
        return
    
    # Если это первый вызов команды
    if context.args is None or len(context.args) == 0:
        update.message.reply_text(
            "Пожалуйста, введите имя пользователя (логин) аккаунта Instagram:"
        )
        return WAITING_USERNAME
    
    # Если пользователь ввел имя пользователя
    if not user_id in user_data_store:
        user_data_store[user_id] = {}
    
    if 'instagram_username' not in user_data_store[user_id]:
        user_data_store[user_id]['instagram_username'] = update.message.text
        update.message.reply_text(
            "Теперь введите пароль для аккаунта:"
        )
        return WAITING_PASSWORD
    
    # Если пользователь ввел пароль
    if 'instagram_password' not in user_data_store[user_id]:
        user_data_store[user_id]['instagram_password'] = update.message.text
    
        # Добавляем аккаунт в базу данных
        username = user_data_store[user_id]['instagram_username']
        password = user_data_store[user_id]['instagram_password']
    
        success, result = add_instagram_account(username, password)
    
        if success:
            update.message.reply_text(
                f"Аккаунт {username} успешно добавлен!",
                reply_markup=get_accounts_menu_keyboard()
            )
        else:
            update.message.reply_text(
                f"Ошибка при добавлении аккаунта: {result}",
                reply_markup=get_accounts_menu_keyboard()
            )
    
        # Очищаем данные пользователя
        del user_data_store[user_id]
    
        return ConversationHandler.END

def list_accounts_handler(update: Update, context: CallbackContext):
    """Обработчик для отображения списка аккаунтов"""
    user_id = extract_user_id_from_update(update, context)
    user_id = update.effective_user.id
    
    if not has_access(user_id):
        return
    
    accounts = get_instagram_accounts(context=context, user_id=user_id)
    
    if not accounts:
        update.message.reply_text(
            "Список аккаунтов пуст. Добавьте аккаунты с помощью команды /add_account",
            reply_markup=get_accounts_menu_keyboard()
        )
        return
    
    # Создаем клавиатуру со списком аккаунтов
    keyboard = get_accounts_list_keyboard(accounts)
    
    update.message.reply_text(
        "Список добавленных аккаунтов Instagram:",
        reply_markup=keyboard
    )

def profile_setup_handler(update: Update, context: CallbackContext):
    """Обработчик для настройки профиля Instagram"""
    user_id = extract_user_id_from_update(update, context)
    user_id = update.effective_user.id
    
    if not has_access(user_id):
        return
    
    # Если это первый вызов команды
    if context.args is None or len(context.args) == 0:
        accounts = get_instagram_accounts(context=context, user_id=user_id)
    
        if not accounts:
            update.message.reply_text(
                "Список аккаунтов пуст. Добавьте аккаунты с помощью команды /add_account",
                reply_markup=get_accounts_menu_keyboard()
            )
            return ConversationHandler.END
    
        # Создаем клавиатуру для выбора аккаунта
        keyboard = []
        for account in accounts:
            keyboard.append([InlineKeyboardButton(account.username, callback_data=f"profile_setup_{account.id}")])
    
        reply_markup = InlineKeyboardMarkup(keyboard)
    
        update.message.reply_text(
            "Выберите аккаунт для настройки профиля:",
            reply_markup=reply_markup
        )
    
        return WAITING_ACCOUNT_SELECTION
    
    # Если пользователь выбрал аккаунт (через callback_handler)
    if 'selected_account_id' in user_data_store.get(user_id, {}):
        # Если пользователь отправил текст (описание профиля)
        if update.message.text:
            user_data_store[user_id]['profile_bio'] = update.message.text
    
            update.message.reply_text(
                "Отправьте фотографию для аватара профиля или введите 'пропустить', чтобы не менять аватар:"
            )
    
            return WAITING_BIO_OR_AVATAR
    
        # Если пользователь отправил фото (аватар)
        if update.message.photo:
            # Получаем файл с наилучшим качеством
            photo_file = update.message.photo[-1].get_file()
    
            # Создаем директорию для аватаров, если её нет
            avatar_dir = Path(MEDIA_DIR) / "avatars"
            os.makedirs(avatar_dir, exist_ok=True)
    
            # Сохраняем файл
            avatar_path = avatar_dir / f"avatar_{user_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jpg"
            photo_file.download(avatar_path)
    
            user_data_store[user_id]['avatar_path'] = str(avatar_path)
    
            # Создаем задачу на обновление профиля
            account_id = user_data_store[user_id]['selected_account_id']
            bio = user_data_store[user_id].get('profile_bio')
    
            success, task_id = create_publish_task(
                account_id=account_id,
                task_type='profile',
                media_path=str(avatar_path),
                caption=bio
            )
    
            if success:
                # Запускаем обновление профиля
                account = get_instagram_account(account_id, context=context, user_id=update.effective_user.id)
    
                update.message.reply_text(
                    f"Задача на обновление профиля {account.username} создана. Выполняется обновление..."
                )
    
                # Выполняем задачу
                from database.db_manager import get_pending_tasks
                tasks = get_pending_tasks()
                for task in tasks:
                    if task.id == task_id:
                        # Используем новые функции вместо ProfileManager
                        success = True
                        error = None
                        
                        if task.media_path:
                            success, error = update_profile_picture(account_id, task.media_path)
                        
                        if task.caption:
                            bio_success, bio_error = update_biography(account_id, task.caption)
                            if not success:  # Если фото не обновлялось или была ошибка
                                success, error = bio_success, bio_error
    
                        if success:
                            update.message.reply_text(
                                f"Профиль {account.username} успешно обновлен!",
                                reply_markup=get_accounts_menu_keyboard()
                            )
                        else:
                            update.message.reply_text(
                                f"Ошибка при обновлении профиля: {error}",
                                reply_markup=get_accounts_menu_keyboard()
                            )
    
                        break
            else:
                update.message.reply_text(
                    f"Ошибка при создании задачи: {task_id}",
                    reply_markup=get_accounts_menu_keyboard()
                )
    
            # Очищаем данные пользователя
            del user_data_store[user_id]
    
            return ConversationHandler.END
    
        # Если пользователь решил пропустить аватар
        if update.message.text.lower() == 'пропустить':
            # Создаем задачу только на обновление био
            account_id = user_data_store[user_id]['selected_account_id']
            bio = user_data_store[user_id].get('profile_bio')
    
            success, task_id = create_publish_task(
                account_id=account_id,
                task_type='profile',
                caption=bio
            )
    
            if success:
                # Запускаем обновление профиля
                account = get_instagram_account(account_id, context=context, user_id=update.effective_user.id)
    
                update.message.reply_text(
                    f"Задача на обновление профиля {account.username} создана. Выполняется обновление..."
                )
    
                # Выполняем задачу
                from database.db_manager import get_pending_tasks
                tasks = get_pending_tasks()
                for task in tasks:
                    if task.id == task_id:
                        # Используем новую функцию вместо ProfileManager
                        success, error = update_biography(account_id, bio)
    
                        if success:
                            update.message.reply_text(
                                f"Профиль {account.username} успешно обновлен!",
                                reply_markup=get_accounts_menu_keyboard()
                            )
                        else:
                            update.message.reply_text(
                                f"Ошибка при обновлении профиля: {error}",
                                reply_markup=get_accounts_menu_keyboard()
                            )
    
                        break
            else:
                update.message.reply_text(
                    f"Ошибка при создании задачи: {task_id}",
                    reply_markup=get_accounts_menu_keyboard()
                )
    
            # Очищаем данные пользователя
            del user_data_store[user_id]
    
            return ConversationHandler.END

# Обработчики для публикации контента
def publish_now_handler(update: Update, context: CallbackContext):
    """Обработчик для немедленной публикации контента"""
    user_id = extract_user_id_from_update(update, context)
    user_id = update.effective_user.id
    
    if not has_access(user_id):
        return
    
    # Если это первый вызов команды
    if context.args is None or len(context.args) == 0:
        # Запрашиваем тип публикации
        keyboard = [
            [InlineKeyboardButton("Reels (видео)", callback_data="publish_type_reel")],
            [InlineKeyboardButton("Фото", callback_data="publish_type_post")],
            [InlineKeyboardButton("Мозаика (6 частей)", callback_data="publish_type_mosaic")]
        ]
    
        reply_markup = InlineKeyboardMarkup(keyboard)
    
        update.message.reply_text(
            "Выберите тип публикации:",
            reply_markup=reply_markup
        )
    
        return WAITING_TASK_TYPE
    
    # Если пользователь выбрал тип публикации (через callback_handler)
    if 'publish_type' in user_data_store.get(user_id, {}):
        # Если пользователь еще не выбрал аккаунт
        if 'selected_account_id' not in user_data_store[user_id]:
            accounts = get_instagram_accounts(context=context, user_id=user_id)
    
            if not accounts:
                update.message.reply_text(
                    "Список аккаунтов пуст. Добавьте аккаунты с помощью команды /add_account",
                    reply_markup=get_tasks_menu_keyboard()
                )
                return ConversationHandler.END
    
            # Создаем клавиатуру для выбора аккаунта
            keyboard = []
            for account in accounts:
                keyboard.append([InlineKeyboardButton(account.username, callback_data=f"publish_account_{account.id}")])
    
            # Добавляем опцию публикации во все аккаунты для Reels
            if user_data_store[user_id]['publish_type'] == 'reel':
                keyboard.append([InlineKeyboardButton("Опубликовать во все аккаунты", callback_data="publish_account_all")])
    
            reply_markup = InlineKeyboardMarkup(keyboard)
    
            update.message.reply_text(
                "Выберите аккаунт для публикации:",
                reply_markup=reply_markup
            )
    
            return WAITING_ACCOUNT_SELECTION
    
        # Если пользователь еще не отправил медиафайл
        if 'media_path' not in user_data_store[user_id]:
            # Если пользователь отправил фото
            if update.message.photo and user_data_store[user_id]['publish_type'] in ['post', 'mosaic']:
                # Получаем файл с наилучшим качеством
                photo_file = update.message.photo[-1].get_file()
    
                # Создаем директорию для фото, если её нет
                photo_dir = Path(MEDIA_DIR) / "photos"
                os.makedirs(photo_dir, exist_ok=True)
    
                # Сохраняем файл
                photo_path = photo_dir / f"photo_{user_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jpg"
                photo_file.download(photo_path)
    
                user_data_store[user_id]['media_path'] = str(photo_path)
    
                update.message.reply_text(
                    "Введите описание для публикации (или 'пропустить' для публикации без описания):"
                )
    
                return WAITING_CAPTION
    
            # Если пользователь отправил видео
            elif (update.message.video or update.message.document) and user_data_store[user_id]['publish_type'] == 'reel':
                # Получаем файл
                if update.message.video:
                    video_file = update.message.video.get_file()
                    file_name = f"video_{user_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.mp4"
                else:
                    video_file = update.message.document.get_file()
                    file_name = update.message.document.file_name
    
                # Создаем директорию для видео, если её нет
                video_dir = Path(MEDIA_DIR) / "videos"
                os.makedirs(video_dir, exist_ok=True)
    
                # Сохраняем файл
                video_path = video_dir / file_name
                video_file.download(video_path)
    
                user_data_store[user_id]['media_path'] = str(video_path)
    
                update.message.reply_text(
                    "Введите описание для публикации (или 'пропустить' для публикации без описания):"
                )
    
                return WAITING_CAPTION
    
            # Если пользователь отправил неподходящий тип файла
            else:
                update.message.reply_text(
                    f"Пожалуйста, отправьте {'фото' if user_data_store[user_id]['publish_type'] in ['post', 'mosaic'] else 'видео'} для публикации."
                )
                return WAITING_MEDIA
    
        # Если пользователь ввел описание
        if 'caption' not in user_data_store[user_id]:
            if update.message.text.lower() == 'пропустить':
                user_data_store[user_id]['caption'] = ""
            else:
                user_data_store[user_id]['caption'] = update.message.text
    
            # Создаем и выполняем задачу на публикацию
            publish_type = user_data_store[user_id]['publish_type']
            media_path = user_data_store[user_id]['media_path']
            caption = user_data_store[user_id]['caption']
    
            # Если публикация во все аккаунты
            if user_data_store[user_id].get('selected_account_id') == 'all':
                if publish_type == 'reel':
                    update.message.reply_text(
                        "Начинаю публикацию Reels во все аккаунты..."
                    )
    
                    # Получаем все аккаунты
                    accounts = get_instagram_accounts(context=context, user_id=user_id)
                    account_ids = [account.id for account in accounts]
    
                    # Публикуем Reels параллельно
                    results = publish_reels_in_parallel(media_path, caption, account_ids)
    
                    # Формируем отчет
                    report = "Результаты публикации Reels:\n\n"
                    for account_id, result in results.items():
                        account = get_instagram_account(account_id, context=context, user_id=update.effective_user.id)
                        status = "✅ Успешно" if result['success'] else f"❌ Ошибка: {result['result']}"
                        report += f"{account.username}: {status}\n"
    
                    update.message.reply_text(
                        report,
                        reply_markup=get_tasks_menu_keyboard()
                    )
            else:
                # Публикация в один аккаунт
                account_id = user_data_store[user_id]['selected_account_id']
    
                success, task_id = create_publish_task(
                    account_id=account_id,
                    task_type=publish_type,
                    media_path=media_path,
                    caption=caption
                )
    
                if success:
                    # Запускаем публикацию
                    account = get_instagram_account(account_id, context=context, user_id=update.effective_user.id)
    
                    update.message.reply_text(
                        f"Задача на публикацию в аккаунт {account.username} создана. Выполняется публикация..."
                    )
    
                    # Выполняем задачу
                    from database.db_manager import get_pending_tasks
                    tasks = get_pending_tasks()
    
                    for task in tasks:
                        if task.id == task_id:
                            if publish_type == 'reel':
                                manager = ReelsManager(account_id)
                                success, error = manager.execute_reel_task(task)
                            else:  # 'post' или 'mosaic'
                                # Используем новую функцию вместо PostManager
                                if publish_type == 'post':
                                    success, error = upload_photo(account_id, media_path, caption)
                                else:  # 'mosaic'
                                    manager = PostManager(account_id)
                                    success, error = manager.execute_post_task(task)
    
                            if success:
                                update.message.reply_text(
                                    f"Публикация в аккаунт {account.username} успешно выполнена!",
                                    reply_markup=get_tasks_menu_keyboard()
                                )
                            else:
                                update.message.reply_text(
                                    f"Ошибка при публикации: {error}",
                                    reply_markup=get_tasks_menu_keyboard()
                                )
    
                            break
                else:
                    update.message.reply_text(
                        f"Ошибка при создании задачи: {task_id}",
                        reply_markup=get_tasks_menu_keyboard()
                    )
    
            # Очищаем данные пользователя
            del user_data_store[user_id]
    
            return ConversationHandler.END

def schedule_publish_handler(update: Update, context: CallbackContext):
    """Обработчик для отложенной публикации контента"""
    # Аналогично publish_now_handler, но с дополнительным шагом для выбора времени
    # Реализация будет похожа, но с добавлением обработки времени публикации
    pass

# Обработчики для прокси
def add_proxy_handler(update: Update, context: CallbackContext):
    """Обработчик для добавления прокси"""
    user_id = update.effective_user.id
    
    if not has_access(user_id):
        return
    
    # Если это первый вызов команды
    if context.args is None or len(context.args) == 0:
        update.message.reply_text(
            "Введите данные прокси в формате:\n"
            "протокол://логин:пароль@хост:порт\n\n"
            "Например: http://user:pass@1.2.3.4:8080\n"
            "Или без авторизации: http://1.2.3.4:8080"
        )
        return WAITING_PROXY_INFO
    
    # Если пользователь ввел данные прокси
    proxy_info = update.message.text
    
    # Парсим данные прокси
    try:
        # Разбираем протокол
        protocol, rest = proxy_info.split('://', 1)
    
        # Разбираем логин:пароль@хост:порт или хост:порт
        if '@' in rest:
            auth, host_port = rest.split('@', 1)
            username, password = auth.split(':', 1)
        else:
            host_port = rest
            username = None
            password = None
    
        # Разбираем хост:порт
        host, port = host_port.split(':', 1)
        port = int(port)
    
        # Добавляем прокси в базу данных
        success, result = add_proxy(host, port, username, password, protocol)
    
        if success:
            update.message.reply_text(
                f"Прокси {host}:{port} успешно добавлен!",
                reply_markup=get_proxy_menu_keyboard()
            )
        else:
            update.message.reply_text(
                f"Ошибка при добавлении прокси: {result}",
                reply_markup=get_proxy_menu_keyboard()
            )
    except Exception as e:
        update.message.reply_text(
            f"Ошибка при разборе данных прокси: {e}\n"
            "Пожалуйста, проверьте формат и попробуйте снова.",
            reply_markup=get_proxy_menu_keyboard()
        )
    
    return ConversationHandler.END

def distribute_proxies_handler(update: Update, context: CallbackContext):
    """Обработчик для распределения прокси по аккаунтам"""
    user_id = update.effective_user.id
    
    if not has_access(user_id):
        return
    
    update.message.reply_text("Начинаю распределение прокси по аккаунтам...")
    
    success, message = distribute_proxies()
    
    if success:
        update.message.reply_text(
            f"Прокси успешно распределены: {message}",
            reply_markup=get_proxy_menu_keyboard()
        )
    else:
        update.message.reply_text(
            f"Ошибка при распределении прокси: {message}",
            reply_markup=get_proxy_menu_keyboard()
        )

def list_proxies_handler(update: Update, context: CallbackContext):
    """Обработчик для отображения списка прокси"""
    user_id = update.effective_user.id
    
    if not has_access(user_id):
        return
    
    proxies = get_proxies()
    
    if not proxies:
        update.message.reply_text(
            "Список прокси пуст. Добавьте прокси с помощью команды /add_proxy",
            reply_markup=get_proxy_menu_keyboard()
        )
        return
    
    # Формируем список прокси
    proxy_list = "Список добавленных прокси:\n\n"
    
    for proxy in proxies:
        status = "✅ Активен" if proxy.is_active else "❌ Неактивен"
        last_checked = proxy.last_checked.strftime("%d.%m.%Y %H:%M") if proxy.last_checked else "Не проверялся"
    
        proxy_list += f"ID: {proxy.id}\n"
        proxy_list += f"Адрес: {proxy.protocol}://{proxy.host}:{proxy.port}\n"
        if proxy.username and proxy.password:
            proxy_list += f"Авторизация: {proxy.username}:{'*' * len(proxy.password)}\n"
        proxy_list += f"Статус: {status}\n"
        proxy_list += f"Последняя проверка: {last_checked}\n\n"
    
    # Добавляем кнопки для проверки прокси
    keyboard = [
        [InlineKeyboardButton("Проверить все прокси", callback_data="check_all_proxies")]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    update.message.reply_text(
        proxy_list,
        reply_markup=reply_markup
    )

# Обработчики для медиафайлов
def photo_handler(update: Update, context: CallbackContext):
    """Обработчик для фотографий"""
    # Этот обработчик будет вызываться, когда пользователь отправляет фото вне контекста диалога
    user_id = update.effective_user.id

    if not has_access(user_id):
        return

    update.message.reply_text(
        "Вы отправили фотографию. Чтобы опубликовать её, используйте команду /publish_now",
        reply_markup=get_main_menu_keyboard()
    )

def video_handler(update: Update, context: CallbackContext):
    """Обработчик для видео"""
    # Этот обработчик будет вызываться, когда пользователь отправляет видео вне контекста диалога
    user_id = update.effective_user.id

    if not has_access(user_id):
        return

    update.message.reply_text(
        "Вы отправили видео. Чтобы опубликовать его в Reels, используйте команду /publish_now",
        reply_markup=get_main_menu_keyboard()
    )

def text_handler(update: Update, context: CallbackContext):
    """Обработчик для текстовых сообщений"""
    # Этот обработчик будет вызываться для всех текстовых сообщений, не являющихся командами
    user_id = update.effective_user.id

    # КРИТИЧЕСКАЯ ПРОВЕРКА: Используем умную систему доступа для мгновенной блокировки
    try:
        from telegram_bot.middleware.smart_access_check import check_user_access_smart
        if not check_user_access_smart(update, context):
            return  # Умная система уже отправила сообщение о блокировке
    except ImportError:
        # Fallback на обычную проверку
        if not has_access(user_id):
            return

    update.message.reply_text(
        "Используйте кнопки меню или команды для взаимодействия с ботом.",
        reply_markup=get_main_menu_keyboard()
    )

def callback_handler(update: Update, context: CallbackContext):
    """Обработчик для кнопок"""
    user_id = extract_user_id_from_update(update, context)
    query = update.callback_query
    user_id = query.from_user.id

    # КРИТИЧЕСКАЯ ПРОВЕРКА: Используем умную систему доступа для мгновенной блокировки
    try:
        from telegram_bot.middleware.smart_access_check import check_user_access_smart
        if not check_user_access_smart(update, context):
            query.answer("🚫 Доступ запрещен")
            return  # Умная система уже отправила сообщение о блокировке
    except ImportError:
        # Fallback на обычную проверку
        if not has_access(user_id):
            query.answer("У вас нет прав доступа к этому боту.")
            return

    # Получаем данные кнопки
    data = query.data

    # Инициализируем хранилище данных пользователя, если его нет
    if not user_id in user_data_store:
        user_data_store[user_id] = {}

    # Обрабатываем различные типы кнопок

    # Проверяем новые улучшенные системы
    enhanced_handlers = get_enhanced_handlers()
    
    # Добавляем динамические обработчики для аккаунтов
    enhanced_handlers = register_enhanced_account_handlers(enhanced_handlers)
    
    if data in enhanced_handlers:
        try:
            enhanced_handlers[data](update, context)
            return
        except Exception as e:
            logger.error(f"Ошибка в улучшенном обработчике {data}: {e}")
            query.answer("❌ Произошла ошибка при выполнении операции")
            return

    # Основные меню
    if data == "main_menu":
        from .keyboards import get_main_menu_keyboard
        query.edit_message_text(
            "🏠 Главное меню\n\nВыберите раздел:",
            reply_markup=get_main_menu_keyboard()
        )
    
    elif data == "menu_publications":
        from .keyboards import get_publications_menu_keyboard
        query.edit_message_text(
            "📤 *Меню публикаций*\n\nВыберите тип публикации:",
            reply_markup=get_publications_menu_keyboard(),
            parse_mode=ParseMode.MARKDOWN
        )
    
    elif data == "menu_scheduled":
        from .keyboards import get_scheduled_menu_keyboard
        query.edit_message_text(
            "🗓️ *Меню запланированных публикаций*\n\nВыберите тип публикации для планирования:",
            reply_markup=get_scheduled_menu_keyboard(),
            parse_mode=ParseMode.MARKDOWN
        )
    
    elif data == "menu_warmup":
        from .keyboards import get_warmup_menu_keyboard
        query.edit_message_text(
            "🔥 *Меню прогрева аккаунтов*\n\nВыберите действие:",
            reply_markup=get_warmup_menu_keyboard(),
            parse_mode=ParseMode.MARKDOWN
        )
    
    elif data == "menu_statistics":
        from .keyboards import get_statistics_menu_keyboard
        query.edit_message_text(
            "📊 *Меню статистики*\n\nВыберите раздел:",
            reply_markup=get_statistics_menu_keyboard(),
            parse_mode=ParseMode.MARKDOWN
        )
    
    elif data == "menu_settings":
        from .keyboards import get_settings_menu_keyboard
        query.edit_message_text(
            "⚙️ *Меню настроек*\n\nВыберите раздел:",
            reply_markup=get_settings_menu_keyboard(),
            parse_mode=ParseMode.MARKDOWN
        )
    
    elif data == "menu_accounts":
        from .keyboards import get_accounts_menu_keyboard
        query.edit_message_text(
            "👤 *Управление аккаунтами*\n\nВыберите действие:",
            reply_markup=get_accounts_menu_keyboard(),
            parse_mode=ParseMode.MARKDOWN
        )
    
    elif data == "tasks_menu":
        query.edit_message_text(
            "📝 Управление задачами\nВыберите действие:",
            reply_markup=get_tasks_menu_keyboard()
        )

    # Действия с аккаунтами
    elif data == "add_account":
        query.edit_message_text(
            "➕ Добавление аккаунта\n\n"
            "Введите данные аккаунта в формате:\n"
            "username:password:email:email_password\n\n"
            "Например: myaccount:mypass123:my@email.com:emailpass"
        )
        return WAITING_ACCOUNT_INFO

    elif data == "bulk_add_accounts":
        query.edit_message_text(
            "📥 Массовая загрузка аккаунтов\n\n"
            "Отправьте файл с аккаунтами в формате:\n"
            "username:password:email:email_password\n"
            "(по одному аккаунту на строку)"
        )
        return WAITING_ACCOUNTS_FILE

    elif data == "add_account_cookie":
        query.edit_message_text(
            "🍪 Добавление аккаунта по cookies\n\n"
            "Отправьте файл cookies или введите данные cookies"
        )
        return WAITING_COOKIES_INFO

    elif data == "list_accounts":
        accounts = get_instagram_accounts(context=context, user_id=user_id)
        if not accounts:
            query.edit_message_text(
                "📋 Список аккаунтов пуст\n\n"
                "Добавьте аккаунты с помощью кнопки '➕ Добавить аккаунт'",
                reply_markup=get_accounts_menu_keyboard()
            )
        else:
            query.edit_message_text(
                "📋 Выберите аккаунт для управления:",
                reply_markup=get_accounts_list_keyboard(accounts)
            )

    elif data == "upload_accounts":
        query.edit_message_text(
            "📤 Выгрузка аккаунтов\n\n"
            "Функция пока в разработке",
            reply_markup=get_accounts_menu_keyboard()
        )

    elif data == "profile_setup":
        accounts = get_instagram_accounts(context=context, user_id=user_id)
        if not accounts:
            query.edit_message_text(
                "⚙️ Список аккаунтов пуст\n\n"
                "Добавьте аккаунты для настройки профилей",
                reply_markup=get_accounts_menu_keyboard()
            )
        else:
            keyboard = []
            for account in accounts:
                keyboard.append([InlineKeyboardButton(
                    f"{account.username} {'✅' if account.is_active else '❌'}",
                    callback_data=f"profile_setup_{account.id}"
                )])
            keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="menu_accounts")])
            
            query.edit_message_text(
                "⚙️ Выберите аккаунт для настройки профиля:",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
    
    elif data == "folders_menu":
        from .keyboards import get_folders_menu_keyboard
        query.edit_message_text(
            "📁 *Управление папками аккаунтов*\n\nВыберите действие:",
            reply_markup=get_folders_menu_keyboard(),
            parse_mode=ParseMode.MARKDOWN
        )


    
    # Новые обработчики для расширенного меню
    elif data == "scheduled_posts":
        from .keyboards import get_publications_menu_keyboard
        query.edit_message_text("🗓️ Запланированные публикации в разработке", reply_markup=get_publications_menu_keyboard())
    
    elif data == "publication_history":
        from .keyboards import get_publications_menu_keyboard
        query.edit_message_text("📊 История публикаций в разработке", reply_markup=get_publications_menu_keyboard())
    
    elif data == "warmup_analytics":
        from .keyboards import get_warmup_menu_keyboard
        query.edit_message_text("📈 Аналитика прогрева в разработке", reply_markup=get_warmup_menu_keyboard())
    
    # Обработчики статистики
    elif data == "general_stats":
        # Запускаем общую аналитику
        from .handlers.analytics_handlers import start_general_analytics
        start_general_analytics(update, context)
    
    elif data == "accounts_stats":
        # Запускаем аналитику аккаунтов
        from .handlers.analytics_handlers import start_accounts_analytics
        start_accounts_analytics(update, context)
    
    elif data == "publications_stats":
        # Запускаем аналитику публикаций с выбором аккаунта
        from .handlers.analytics_handlers import start_publications_analytics
        start_publications_analytics(update, context)
    
    elif data == "warmup_stats":
        from .keyboards import get_statistics_menu_keyboard
        query.edit_message_text("🔥 Статистика по прогреву в разработке", reply_markup=get_statistics_menu_keyboard())
    
    # Обработчики действий аналитики публикаций
    elif data in ["analytics_recent_posts", "analytics_top_likes", "analytics_top_comments", "analytics_detailed", "analytics_stories"]:
        from .handlers.analytics_handlers import handle_analytics_action
        handle_analytics_action(update, context, data)
    
    # Новые обработчики Advanced Warmup 2.0
    elif data == "smart_warm_menu":
        from telegram_bot.handlers.automation_handlers import smart_warm_command
        smart_warm_command(update, context)
    
    elif data == "status":
        from telegram_bot.handlers.automation_handlers import status_command
        status_command(update, context)
    
    elif data == "limits":
        from telegram_bot.handlers.automation_handlers import limits_command
        limits_command(update, context)
    
    # Обработчики настроек
    elif data == "general_settings":
        from .keyboards import get_settings_menu_keyboard
        query.edit_message_text("🔧 Основные настройки в разработке", reply_markup=get_settings_menu_keyboard())
    
    elif data == "schedule_settings":
        from .keyboards import get_settings_menu_keyboard
        query.edit_message_text("⏰ Настройки расписания в разработке", reply_markup=get_settings_menu_keyboard())
    
    elif data == "notifications_settings":
        from .keyboards import get_settings_menu_keyboard
        query.edit_message_text("🚨 Настройки уведомлений в разработке", reply_markup=get_settings_menu_keyboard())
    
    elif data == "security_settings":
        from .keyboards import get_settings_menu_keyboard
        query.edit_message_text("🔒 Настройки безопасности в разработке", reply_markup=get_settings_menu_keyboard())
    
    elif data == "backup_settings":
        from .keyboards import get_settings_menu_keyboard
        query.edit_message_text("💾 Настройки резервного копирования в разработке", reply_markup=get_settings_menu_keyboard())
    
    # Обработчики типов публикаций
    elif data == "publish_post":
        # Выбираем аккаунт для публикации
        from .keyboards import get_publications_menu_keyboard
        accounts = get_instagram_accounts(context=context, user_id=user_id)
        if not accounts:
            query.edit_message_text(
                "❌ Нет доступных аккаунтов",
                reply_markup=get_publications_menu_keyboard()
            )
        else:
            keyboard = []
            for account in accounts:
                keyboard.append([InlineKeyboardButton(
                    f"{'✅' if account.is_active else '❌'} {account.username}",
                    callback_data=f"post_to_{account.id}"
                )])
            keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="publish_type")])
            query.edit_message_text(
                "📸 Выберите аккаунт для публикации поста:",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
    
    elif data == "publish_story":
        from .keyboards import get_publications_menu_keyboard
        query.edit_message_text("📱 Публикация историй в разработке", reply_markup=get_publications_menu_keyboard())
    
    elif data == "publish_igtv":
        from .keyboards import get_publications_menu_keyboard
        query.edit_message_text("🎬 Публикация IGTV в разработке", reply_markup=get_publications_menu_keyboard())
    
    elif data == "publish_igtv_blocked":
        from .keyboards import get_publications_menu_keyboard
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
    
    elif data == "publish_reels":
        # Выбираем аккаунт для публикации Reels
        from .keyboards import get_publications_menu_keyboard
        accounts = get_instagram_accounts(context=context, user_id=user_id)
        if not accounts:
            query.edit_message_text(
                "❌ Нет доступных аккаунтов",
                reply_markup=get_publications_menu_keyboard()
            )
        else:
            keyboard = []
            for account in accounts:
                keyboard.append([InlineKeyboardButton(
                    f"{'✅' if account.is_active else '❌'} {account.username}",
                    callback_data=f"reel_to_{account.id}"
                )])
            keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="publish_type")])
            query.edit_message_text(
                "🎥 Выберите аккаунт для публикации Reels:",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
    
    elif data == "set_limits":
        from .keyboards import get_publications_menu_keyboard
        query.edit_message_text("🔧 Настройка лимитов в разработке", reply_markup=get_publications_menu_keyboard())
    
    # Обработчики режимов прогрева
    elif data == "quick_warmup":
        from .keyboards import get_warmup_mode_keyboard
        query.edit_message_text("⚡ Быстрый прогрев в разработке", reply_markup=get_warmup_mode_keyboard())
    
    elif data == "smart_warmup":
        # Перенаправляем на новую систему Advanced Warmup 2.0
        from telegram_bot.handlers.automation_handlers import smart_warm_command
        smart_warm_command(update, context)
    
    elif data == "warmup_status":
        # Показываем статус прогрева аккаунтов
        from .keyboards import get_warmup_mode_keyboard
        query.edit_message_text("📊 Загрузка статуса прогрева...")
        # TODO: Реализовать получение статуса прогрева
        query.edit_message_text("📊 Статус прогрева в разработке", reply_markup=get_warmup_mode_keyboard())
    
    elif data == "warmup_settings":
        from .keyboards import get_warmup_mode_keyboard
        query.edit_message_text("⚙️ Настройки прогрева в разработке", reply_markup=get_warmup_mode_keyboard())
    
    # Обработчики задач по статусам
    elif data == "active_tasks":
        from .keyboards import get_tasks_by_status_keyboard
        query.edit_message_text("✅ Активные задачи в разработке", reply_markup=get_tasks_by_status_keyboard())
    
    elif data == "paused_tasks":
        from .keyboards import get_tasks_by_status_keyboard
        query.edit_message_text("⏸️ Приостановленные задачи в разработке", reply_markup=get_tasks_by_status_keyboard())
    
    elif data == "completed_tasks":
        from .keyboards import get_tasks_by_status_keyboard
        query.edit_message_text("✓ Завершенные задачи в разработке", reply_markup=get_tasks_by_status_keyboard())
    
    elif data == "error_tasks":
        from .keyboards import get_tasks_by_status_keyboard
        query.edit_message_text("❌ Задачи с ошибками в разработке", reply_markup=get_tasks_by_status_keyboard())
    
    # Статистика аккаунтов
    elif data == "accounts_statistics":
        accounts = get_instagram_accounts(context=context, user_id=user_id)
        active_count = sum(1 for acc in accounts if acc.is_active)
        inactive_count = len(accounts) - active_count
        
        stats_text = (
            f"📊 *Статистика аккаунтов*\n\n"
            f"👥 Всего аккаунтов: {len(accounts)}\n"
            f"✅ Активных: {active_count}\n"
            f"❌ Неактивных: {inactive_count}\n"
        )
        
        from .keyboards import get_messages_actions_keyboard
        query.edit_message_text(
            stats_text,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=get_messages_actions_keyboard()
            )

    # Задачи публикации
    elif data == "publish_now":
        query.edit_message_text(
            "📤 Выберите тип публикации:",
            reply_markup=get_publish_type_keyboard()
        )

    elif data == "schedule_publish":
        query.edit_message_text(
            "⏰ Отложенная публикация\n\n"
            "Функция пока в разработке",
            reply_markup=get_tasks_menu_keyboard()
        )

    # Прокси
    elif data == "add_proxy":
        query.edit_message_text(
            "➕ Добавление прокси\n\n"
            "Введите данные прокси в формате:\n"
            "протокол://логин:пароль@хост:порт\n\n"
            "Например: http://user:pass@1.2.3.4:8080"
        )
        return WAITING_PROXY_INFO

    elif data == "distribute_proxies":
        query.edit_message_text("🔄 Распределяю прокси по аккаунтам...")
        success, message = distribute_proxies()
        
        if success:
            query.edit_message_text(
                f"✅ Прокси успешно распределены:\n{message}",
                reply_markup=get_proxy_menu_keyboard()
            )
        else:
            query.edit_message_text(
                f"❌ Ошибка при распределении прокси:\n{message}",
                reply_markup=get_proxy_menu_keyboard()
            )

    elif data == "list_proxies" or data.startswith("list_proxies_page_"):
        # Извлекаем номер страницы
        page = 1
        if data.startswith("list_proxies_page_"):
            try:
                page = int(data.split("_")[-1])
            except (ValueError, IndexError):
                page = 1
        
        proxies = get_proxies()
        if not proxies:
            query.edit_message_text(
                "📋 Список прокси пуст\n\n"
                "Добавьте прокси с помощью кнопки '➕ Добавить прокси'",
                reply_markup=get_proxy_menu_keyboard()
            )
        else:
            # Пагинация: показываем по 10 прокси на страницу
            proxies_per_page = 10
            total_pages = (len(proxies) + proxies_per_page - 1) // proxies_per_page
            start_idx = (page - 1) * proxies_per_page
            end_idx = start_idx + proxies_per_page
            page_proxies = proxies[start_idx:end_idx]
            
            proxy_list = f"📋 Список прокси (страница {page}/{total_pages}):\n\n"
            for proxy in page_proxies:
                status = "✅" if proxy.is_active else "❌"
                auth_info = " 🔐" if proxy.username else ""
                proxy_list += f"{status} {proxy.protocol}://{proxy.host}:{proxy.port}{auth_info}\n"
            
            # Создаем кнопки навигации
            keyboard = []
            
            # Кнопки навигации по страницам
            nav_buttons = []
            if page > 1:
                nav_buttons.append(InlineKeyboardButton("⬅️ Пред", callback_data=f"list_proxies_page_{page-1}"))
            if page < total_pages:
                nav_buttons.append(InlineKeyboardButton("След ➡️", callback_data=f"list_proxies_page_{page+1}"))
            
            if nav_buttons:
                keyboard.append(nav_buttons)
            
            # Кнопки действий
            keyboard.append([InlineKeyboardButton("🔍 Проверить все", callback_data="check_all_proxies")])
            keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="proxy_menu")])
            
            query.edit_message_text(
                proxy_list,
                reply_markup=InlineKeyboardMarkup(keyboard)
            )

    # Детали аккаунта
    elif data.startswith("account_"):
        try:
            account_id = int(data.replace("account_", ""))
            account = get_instagram_account(account_id, context=context, user_id=update.effective_user.id)
            
            if account:
                account_info = (
                    f"👤 Аккаунт: {account.username}\n"
                    f"📧 Email: {account.email or 'Не указан'}\n"
                    f"📊 Статус: {'✅ Активен' if account.is_active else '❌ Неактивен'}\n"
                    f"📅 Добавлен: {account.created_at.strftime('%d.%m.%Y %H:%M')}"
                )
                
                query.edit_message_text(
                    account_info,
                    reply_markup=get_account_actions_keyboard(account_id)
                )
            else:
                query.edit_message_text(
                    "❌ Аккаунт не найден",
                    reply_markup=get_accounts_menu_keyboard()
                )
        except ValueError:
            query.edit_message_text(
                "❌ Ошибка: неверный формат ID аккаунта",
                reply_markup=get_accounts_menu_keyboard()
            )

    # Действия с конкретным аккаунтом
    elif data.startswith("publish_to_"):
        try:
            account_id = int(data.replace("publish_to_", ""))
            user_data_store[user_id]['selected_account_id'] = account_id
            
            query.edit_message_text(
                "📤 Выберите тип публикации:",
                reply_markup=get_publish_type_keyboard()
            )
        except ValueError:
            query.edit_message_text(
                "❌ Ошибка: неверный формат ID аккаунта",
                reply_markup=get_accounts_menu_keyboard()
            )

    elif data.startswith("change_password_"):
        try:
            account_id = int(data.replace("change_password_", ""))
            user_data_store[user_id]['selected_account_id'] = account_id
            
            query.edit_message_text(
                "🔑 Введите новый пароль для аккаунта:"
            )
            return WAITING_NEW_PASSWORD
        except ValueError:
            query.edit_message_text(
                "❌ Ошибка: неверный формат ID аккаунта",
                reply_markup=get_accounts_menu_keyboard()
            )

    elif data.startswith("assign_proxy_"):
        try:
            account_id = int(data.replace("assign_proxy_", ""))
            proxies = get_proxies()
            
            if not proxies:
                query.edit_message_text(
                    "🌐 Список прокси пуст\n\n"
                    "Добавьте прокси перед назначением",
                    reply_markup=get_account_actions_keyboard(account_id)
                )
            else:
                keyboard = []
                for proxy in proxies:
                    status = "✅" if proxy.is_active else "❌"
                    keyboard.append([InlineKeyboardButton(
                        f"{status} {proxy.host}:{proxy.port}",
                        callback_data=f"set_proxy_{account_id}_{proxy.id}"
                    )])
                keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data=f"account_{account_id}")])
                
                query.edit_message_text(
                    "🌐 Выберите прокси для аккаунта:",
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
        except ValueError:
            query.edit_message_text(
                "❌ Ошибка: неверный формат ID аккаунта",
                reply_markup=get_accounts_menu_keyboard()
            )

    elif data.startswith("delete_account_"):
        try:
            account_id = int(data.replace("delete_account_", ""))
            account = get_instagram_account(account_id, context=context, user_id=update.effective_user.id)
            
            if account:
                keyboard = [
                    [InlineKeyboardButton("✅ Да, удалить", callback_data=f"confirm_delete_{account_id}")],
                    [InlineKeyboardButton("❌ Отмена", callback_data=f"account_{account_id}")]
                ]
                
                query.edit_message_text(
                    f"⚠️ Вы уверены, что хотите удалить аккаунт {account.username}?\n\n"
                    "Это действие нельзя отменить!",
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
            else:
                query.edit_message_text(
                    "❌ Аккаунт не найден",
                    reply_markup=get_accounts_menu_keyboard()
                )
        except ValueError:
            query.edit_message_text(
                "❌ Ошибка: неверный формат ID аккаунта",
                reply_markup=get_accounts_menu_keyboard()
            )

    # Подтверждение удаления аккаунта
    elif data.startswith("confirm_delete_"):
        try:
            account_id = int(data.replace("confirm_delete_", ""))
            account = get_instagram_account(account_id, context=context, user_id=update.effective_user.id)
            
            if account:
                username = account.username
                success = delete_instagram_account(account_id)
                
                if success:
                    query.edit_message_text(
                        f"✅ Аккаунт {username} успешно удален!",
                        reply_markup=get_accounts_menu_keyboard()
                    )
                else:
                    query.edit_message_text(
                        f"❌ Ошибка при удалении аккаунта {username}",
                        reply_markup=get_accounts_menu_keyboard()
                    )
            else:
                query.edit_message_text(
                    "❌ Аккаунт не найден",
                    reply_markup=get_accounts_menu_keyboard()
                )
        except ValueError:
            query.edit_message_text(
                "❌ Ошибка: неверный формат ID аккаунта",
                reply_markup=get_accounts_menu_keyboard()
            )

    # Установка прокси для аккаунта
    elif data.startswith("set_proxy_"):
        try:
            parts = data.replace("set_proxy_", "").split("_")
            account_id = int(parts[0])
            proxy_id = int(parts[1])
            
            success = assign_proxy_to_account(account_id, proxy_id)
            account = get_instagram_account(account_id, context=context, user_id=update.effective_user.id)
            proxy = next((p for p in get_proxies() if p.id == proxy_id), None)
            
            if success and account and proxy:
                query.edit_message_text(
                    f"✅ Прокси {proxy.host}:{proxy.port} назначен аккаунту {account.username}",
                    reply_markup=get_account_actions_keyboard(account_id)
                )
            else:
                query.edit_message_text(
                    "❌ Ошибка при назначении прокси",
                    reply_markup=get_account_actions_keyboard(account_id)
                )
        except (ValueError, IndexError):
            query.edit_message_text(
                "❌ Ошибка: неверный формат данных",
                reply_markup=get_accounts_menu_keyboard()
            )

    # Меню прокси
    elif data == "proxy_menu":
        query.edit_message_text(
            "🌐 Управление прокси\nВыберите действие:",
            reply_markup=get_proxy_menu_keyboard()
        )

    # Обработчики для новых callback_data из bot.py

    elif data == "menu_accounts":
        query.edit_message_text(
            "🔑 Управление аккаунтами\nВыберите действие:",
            reply_markup=get_accounts_menu_keyboard()
        )

    elif data == "menu_tasks":
        query.edit_message_text(
            "📝 Управление задачами\nВыберите действие:",
            reply_markup=get_tasks_menu_keyboard()
        )

    elif data == "menu_proxy":
        query.edit_message_text(
            "🌐 Управление прокси\nВыберите действие:",
            reply_markup=get_proxy_menu_keyboard()
        )

    elif data == "menu_help":
        help_text = (
            "*Основные команды:*\n"
            "/start - Запустить бота\n"
            "/help - Показать эту справку\n\n"
            
            "*Управление аккаунтами:*\n"
            "• Добавить аккаунт Instagram\n"
            "• Список добавленных аккаунтов\n"
            "• Настройка профиля\n\n"
            
            "*Публикация контента:*\n"
            "• Опубликовать сейчас\n"
            "• Отложенная публикация\n\n"
            
            "*Управление прокси:*\n"
            "• Добавить прокси\n"
            "• Распределить прокси\n"
            "• Список добавленных прокси\n\n"
            
            "*Дополнительно:*\n"
            "/cancel - Отменить текущую операцию"
        )
        
        from telegram import InlineKeyboardMarkup, InlineKeyboardButton
        back_keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 Назад в главное меню", callback_data="main_menu")]
        ])
        
        query.edit_message_text(
            help_text,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=back_keyboard
        )

    # Дополнительные callback_data которые могут встречаться
    elif data == "publication_stats":
        query.edit_message_text(
            "📊 Статистика публикаций\n\n"
            "Функция пока в разработке",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Назад", callback_data="menu_tasks")]
            ])
        )

    elif data == "check_proxies":
        query.edit_message_text("🔍 Проверяю прокси...")
        results = check_all_proxies()
        
        report = "Результаты проверки прокси:\n\n"
        for proxy_id, result in results.items():
            proxy = next((p for p in get_proxies() if p.id == proxy_id), None)
            if proxy:
                status = "✅ Работает" if result['working'] else f"❌ Не работает: {result['error']}"
                report += f"ID: {proxy.id}, {proxy.host}:{proxy.port} - {status}\n"
        
        query.edit_message_text(
            report,
            reply_markup=get_proxy_menu_keyboard()
        )

    elif data == "import_proxies":
        query.edit_message_text(
            "📤 Импорт прокси\n\n"
            "Функция пока в разработке",
            reply_markup=get_proxy_menu_keyboard()
        )

    # Отмена публикации
    elif data == "cancel_publish":
        # Очищаем данные пользователя если есть
        if user_id in user_data_store:
            del user_data_store[user_id]
        
        query.edit_message_text(
            "❌ Публикация отменена",
            reply_markup=get_tasks_menu_keyboard()
        )

    # Детали аккаунта (альтернативный обработчик)
    elif data.startswith("account_details_"):
        try:
            account_id = int(data.replace("account_details_", ""))
            account = get_instagram_account(account_id, context=context, user_id=update.effective_user.id)
            
            if account:
                account_info = (
                    f"👤 Аккаунт: {account.username}\n"
                    f"📧 Email: {account.email or 'Не указан'}\n"
                    f"📊 Статус: {'✅ Активен' if account.is_active else '❌ Неактивен'}\n"
                    f"📅 Добавлен: {account.created_at.strftime('%d.%m.%Y %H:%M')}"
                )
                
                query.edit_message_text(
                    account_info,
                    reply_markup=get_account_actions_keyboard(account_id)
                )
            else:
                query.edit_message_text(
                    "❌ Аккаунт не найден",
                    reply_markup=get_accounts_menu_keyboard()
                )
        except ValueError:
            query.edit_message_text(
                "❌ Ошибка: неверный формат ID аккаунта",
                reply_markup=get_accounts_menu_keyboard()
            )

    # Меню профиля аккаунта
    elif data.startswith("profile_account_"):
        try:
            account_id = int(data.replace("profile_account_", ""))
            account = get_instagram_account(account_id, context=context, user_id=update.effective_user.id)
            
            if account:
                from telegram import InlineKeyboardMarkup, InlineKeyboardButton
                keyboard = [
                    [InlineKeyboardButton("✏️ Изменить имя", callback_data=f"edit_name_{account_id}")],
                    [InlineKeyboardButton("👤 Изменить username", callback_data=f"edit_username_{account_id}")],
                    [InlineKeyboardButton("📝 Изменить описание", callback_data=f"edit_bio_{account_id}")],
                    [InlineKeyboardButton("🖼️ Изменить фото", callback_data=f"edit_photo_{account_id}")],
                    [InlineKeyboardButton("🗑️ Удалить посты", callback_data="profile_delete_posts")],
                    [InlineKeyboardButton("🔙 Назад", callback_data=f"account_{account_id}")]
                ]
                
                query.edit_message_text(
                    f"⚙️ Настройка профиля {account.username}\nВыберите действие:",
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
            else:
                query.edit_message_text(
                    "❌ Аккаунт не найден",
                    reply_markup=get_accounts_menu_keyboard()
                )
        except ValueError:
                         query.edit_message_text(
                 "❌ Ошибка: неверный формат ID аккаунта",
                 reply_markup=get_accounts_menu_keyboard()
             )

    # Редактирование элементов профиля
    elif data.startswith("edit_name_"):
        try:
            account_id = int(data.replace("edit_name_", ""))
            user_data_store[user_id]['editing_profile'] = {'account_id': account_id, 'field': 'name'}
            
            query.edit_message_text(
                "✏️ Введите новое имя профиля:"
            )
            return WAITING_ACCOUNT_INFO
        except ValueError:
            query.edit_message_text(
                "❌ Ошибка: неверный формат ID аккаунта",
                reply_markup=get_accounts_menu_keyboard()
            )

    elif data.startswith("edit_username_"):
        try:
            account_id = int(data.replace("edit_username_", ""))
            user_data_store[user_id]['editing_profile'] = {'account_id': account_id, 'field': 'username'}
            
            query.edit_message_text(
                "👤 Введите новый username (без @):"
            )
            return WAITING_ACCOUNT_INFO
        except ValueError:
            query.edit_message_text(
                "❌ Ошибка: неверный формат ID аккаунта",
                reply_markup=get_accounts_menu_keyboard()
            )

    elif data.startswith("edit_bio_"):
        try:
            account_id = int(data.replace("edit_bio_", ""))
            user_data_store[user_id]['editing_profile'] = {'account_id': account_id, 'field': 'bio'}
            
            query.edit_message_text(
                "📝 Введите новое описание профиля:"
            )
            return WAITING_ACCOUNT_INFO
        except ValueError:
            query.edit_message_text(
                "❌ Ошибка: неверный формат ID аккаунта",
                reply_markup=get_accounts_menu_keyboard()
            )

    elif data.startswith("edit_photo_"):
        try:
            account_id = int(data.replace("edit_photo_", ""))
            user_data_store[user_id]['editing_profile'] = {'account_id': account_id, 'field': 'photo'}
            
            query.edit_message_text(
                "🖼️ Отправьте новое фото профиля:"
            )
            return WAITING_PROFILE_PHOTO
        except ValueError:
            query.edit_message_text(
                "❌ Ошибка: неверный формат ID аккаунта",
                reply_markup=get_accounts_menu_keyboard()
            )

    # Кнопки выбора типа публикации
    elif data.startswith("publish_type_"):
        publish_type = data.replace("publish_type_", "")
        user_data_store[user_id]['publish_type'] = publish_type

        # Запрашиваем выбор аккаунта
        accounts = get_instagram_accounts(context=context, user_id=user_id)

        if not accounts:
            query.edit_message_text(
                "Список аккаунтов пуст. Добавьте аккаунты с помощью команды /add_account"
            )
            return ConversationHandler.END

        # Создаем клавиатуру для выбора аккаунта
        keyboard = []
        for account in accounts:
            keyboard.append([InlineKeyboardButton(account.username, callback_data=f"publish_account_{account.id}")])

        # Добавляем опцию публикации во все аккаунты для Reels
        if publish_type == 'reel':
            keyboard.append([InlineKeyboardButton("Опубликовать во все аккаунты", callback_data="publish_account_all")])

        reply_markup = InlineKeyboardMarkup(keyboard)

        query.edit_message_text(
            text=f"Выбран тип публикации: {publish_type}. Теперь выберите аккаунт:",
            reply_markup=reply_markup
        )

    # Кнопки выбора аккаунта для публикации
    elif data.startswith("publish_account_"):
        account_id = data.replace("publish_account_", "")
        user_data_store[user_id]['selected_account_id'] = account_id

        # Запрашиваем медиафайл
        if user_data_store[user_id]['publish_type'] in ['post', 'mosaic']:
            query.edit_message_text(
                "Отправьте фотографию для публикации:"
            )
        else:  # 'reel'
            query.edit_message_text(
                "Отправьте видео для публикации в Reels:"
            )

    # Кнопки выбора аккаунта для настройки профиля
    elif data.startswith("profile_setup_"):
        account_id = data.replace("profile_setup_", "")
        user_data_store[user_id]['selected_account_id'] = account_id

        # Запрашиваем описание профиля
        query.edit_message_text(
            "Введите новое описание профиля (или 'пропустить', чтобы не менять описание):"
        )

    # Кнопка удаления всех постов
    elif data == "profile_delete_posts":
        account_id = user_data_store[user_id].get('selected_account_id')
        if account_id:
            query.edit_message_text("⏳ Удаление всех постов...")
            success, result = delete_all_posts(account_id)
            if success:
                query.edit_message_text(
                    "✅ Все посты успешно удалены!",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🔙 Назад", callback_data=f"profile_account_{account_id}")]
                    ])
                )
            else:
                query.edit_message_text(
                    f"❌ Ошибка при удалении постов: {result}",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🔙 Назад", callback_data=f"profile_account_{account_id}")]
                    ])
                )

    # Кнопка очистки описания профиля
    elif data == "profile_delete_bio":
        account_id = user_data_store[user_id].get('selected_account_id')
        if account_id:
            query.edit_message_text("⏳ Очистка описания профиля...")
            success, result = clear_biography(account_id)
            if success:
                query.edit_message_text(
                    "✅ Описание профиля успешно очищено!",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🔙 Назад", callback_data=f"profile_account_{account_id}")]
                    ])
                )
            else:
                query.edit_message_text(
                    f"❌ Ошибка при очистке профиля: {result}",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🔙 Назад", callback_data=f"profile_account_{account_id}")]
                    ])
                )

    # Кнопка проверки всех прокси
    elif data == "check_all_proxies":
        query.edit_message_text(
            "Начинаю проверку всех прокси. Это может занять некоторое время..."
        )

        # Запускаем проверку прокси
        results = check_all_proxies()

        # Формируем отчет
        report = "Результаты проверки прокси:\n\n"

        for proxy_id, result in results.items():
            proxy = next((p for p in get_proxies() if p.id == proxy_id), None)
            if proxy:
                status = "✅ Работает" if result['working'] else f"❌ Не работает: {result['error']}"
                report += f"ID: {proxy.id}, {proxy.host}:{proxy.port} - {status}\n"

        query.edit_message_text(
            report,
            reply_markup=get_proxy_menu_keyboard()
        )

    # Callback обработчики для продвинутых систем
    elif data == "advanced_systems":
        keyboard = [
            [InlineKeyboardButton("🔍 Health Monitor", callback_data="health_monitor")],
            [InlineKeyboardButton("⚡ Activity Limiter", callback_data="activity_limiter")],
            [InlineKeyboardButton("🚀 Improved Warmer", callback_data="improved_warmer")],
            [InlineKeyboardButton("📊 System Status", callback_data="system_status")],
            [InlineKeyboardButton("🔙 Главное меню", callback_data="main_menu")]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        query.edit_message_text(
            "🎛️ *Продвинутые системы управления*\n\n"
            "Выберите систему:",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=reply_markup
        )

    elif data == "health_monitor":
        query.edit_message_text("🔍 Запускаю проверку здоровья всех аккаунтов...")
        
        try:
            health_monitor = AdvancedHealthMonitor()
            accounts = get_instagram_accounts(context=context, user_id=user_id)
            
            if not accounts:
                query.edit_message_text("❌ Аккаунты не найдены.")
                return
            
            report = "📊 *Отчет Health Monitor*\n\n"
            
            for account in accounts:
                score = health_monitor.calculate_comprehensive_health_score(account.id)
                recommendations = health_monitor.get_health_recommendations(account.id)
                
                status = "🟢" if score >= 80 else "🟡" if score >= 60 else "🔴"
                report += f"{status} *{account.username}*: {score}/100\n"
                
                if recommendations:
                    report += f"   💡 {recommendations[0]}\n"
                report += "\n"
            
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="advanced_systems")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            query.edit_message_text(
                report, 
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=reply_markup
            )
            
        except Exception as e:
            logger.error(f"Ошибка Health Monitor: {e}")
            query.edit_message_text(f"❌ Ошибка при проверке здоровья: {e}")

    elif data == "activity_limiter":
        query.edit_message_text("⚡ Проверяю лимиты активности...")
        
        try:
            activity_limiter = ActivityLimiter()
            accounts = get_instagram_accounts(context=context, user_id=user_id)
            
            if not accounts:
                query.edit_message_text("❌ Аккаунты не найдены.")
                return
            
            report = "⚡ *Отчет Activity Limiter*\n\n"
            
            for account in accounts:
                restrictions = activity_limiter.check_current_restrictions(account.id)
                limits = activity_limiter.get_dynamic_limits(account.id)
                
                status = "🔴" if restrictions else "🟢"
                report += f"{status} *{account.username}*\n"
                
                if restrictions:
                    report += f"   ⚠️ Ограничения: {', '.join(restrictions)}\n"
                else:
                    report += f"   ✅ Лимиты: follows: {limits.get('follows_per_day', 0)}/день\n"
                
                report += "\n"
            
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="advanced_systems")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            query.edit_message_text(
                report,
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=reply_markup
            )
            
        except Exception as e:
            logger.error(f"Ошибка Activity Limiter: {e}")
            query.edit_message_text(f"❌ Ошибка при проверке лимитов: {e}")

    elif data == "improved_warmer":
        accounts = get_instagram_accounts(context=context, user_id=user_id)
        
        if not accounts:
            query.edit_message_text("❌ Аккаунты не найдены.")
            return
        
        keyboard = []
        for account in accounts:
            keyboard.append([InlineKeyboardButton(
                f"🔥 {account.username}", 
                callback_data=f"improved_warm_{account.id}"
            )])
        
        keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="advanced_systems")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        query.edit_message_text(
            "🚀 *Улучшенный прогрев аккаунтов*\n\n"
            "Выберите аккаунт для прогрева:",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=reply_markup
        )

    elif data.startswith("improved_warm_"):
        try:
            account_id = int(data.replace("improved_warm_", ""))
            account = get_instagram_account(account_id, context=context, user_id=update.effective_user.id)
            
            if not account:
                query.edit_message_text("❌ Аккаунт не найден.")
                return
            
            query.edit_message_text(f"🔥 Запускаю улучшенный прогрев для {account.username}...")
            
            success, result = warm_account_improved(account_id)
            
            if success:
                query.edit_message_text(
                    f"✅ Прогрев аккаунта {account.username} успешно завершен!\n\n"
                    f"📊 Результат: {result}",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🔙 Назад", callback_data="improved_warmer")]
                    ])
                )
            else:
                query.edit_message_text(
                    f"❌ Ошибка при прогреве {account.username}: {result}",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🔙 Назад", callback_data="improved_warmer")]
                    ])
                )
                
        except ValueError:
            query.edit_message_text("❌ Ошибка: неверный формат ID аккаунта")
        except Exception as e:
            logger.error(f"Ошибка улучшенного прогрева: {e}")
            query.edit_message_text(f"❌ Ошибка при прогреве: {e}")

    elif data == "system_status":
        try:
            # Проверяем статус всех систем
            health_monitor = AdvancedHealthMonitor()
            activity_limiter = ActivityLimiter()
            lifecycle_manager = AccountLifecycleManager()
            predictive_monitor = PredictiveMonitor()
            
            accounts = get_instagram_accounts(context=context, user_id=user_id)
            total_accounts = len(accounts)
            
            # Health Monitor Status
            healthy_accounts = 0
            for account in accounts:
                score = health_monitor.calculate_comprehensive_health_score(account.id)
                if score >= 80:
                    healthy_accounts += 1
            
            # Activity Limiter Status
            restricted_accounts = 0
            for account in accounts:
                restrictions = activity_limiter.check_current_restrictions(account.id)
                if restrictions:
                    restricted_accounts += 1
            
            status_report = (
                f"📊 *Статус всех систем*\n\n"
                f"👥 *Общая информация:*\n"
                f"   Всего аккаунтов: {total_accounts}\n\n"
                
                f"🔍 *Health Monitor:*\n"
                f"   ✅ Здоровых аккаунтов: {healthy_accounts}/{total_accounts}\n"
                f"   📈 Процент здоровья: {int(healthy_accounts/total_accounts*100) if total_accounts > 0 else 0}%\n\n"
                
                f"⚡ *Activity Limiter:*\n"
                f"   🚫 Аккаунтов с ограничениями: {restricted_accounts}/{total_accounts}\n"
                f"   ✅ Свободных аккаунтов: {total_accounts - restricted_accounts}\n\n"
                
                f"🔄 *Lifecycle Manager:*\n"
                f"   🆕 Статус: Все аккаунты отслеживаются\n\n"
                
                f"🛡️ *Predictive Monitor:*\n"
                f"   🎯 Система анализа рисков: Активна\n"
                f"   📊 ML модель: Готова к предсказаниям\n\n"
                
                f"⏰ Последнее обновление: {datetime.now().strftime('%H:%M:%S')}"
            )
            
            keyboard = [[InlineKeyboardButton("🔄 Обновить", callback_data="system_status")],
                       [InlineKeyboardButton("🔙 Назад", callback_data="advanced_systems")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            query.edit_message_text(
                status_report, 
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=reply_markup
            )
            
        except Exception as e:
            logger.error(f"Ошибка System Status: {e}")
            query.edit_message_text(f"❌ Ошибка при получении статуса систем: {e}")

    # Обработчик для неизвестных callback_data
    else:
        logger.warning(f"Неизвестный callback_data: {data} от пользователя {user_id}")
        query.edit_message_text(
            f"❌ Неизвестная команда: {data}\n\n"
            "Возвращаю в главное меню",
                                    reply_markup=get_main_menu_keyboard()
        )

    # Подтверждаем обработку callback
    query.answer()

def cancel_handler(update: Update, context: CallbackContext):
    """Обработчик для отмены текущей операции"""
    user_id = update.effective_user.id

    if not has_access(user_id):
        return

    # Очищаем данные пользователя
    if user_id in user_data_store:
        del user_data_store[user_id]

    update.message.reply_text(
        "Операция отменена.",
        reply_markup=get_main_menu_keyboard()
    )

    return ConversationHandler.END

# Обработчик для кнопки "Добавить фото профиля"
def process_add_profile_photo(update: Update, context: CallbackContext):
    """Обработчик для добавления фото профиля"""
    query = update.callback_query
    user_id = query.from_user.id

    if not has_access(user_id):
        return

    # Получаем ID аккаунта из callback_data
    account_id = int(query.data.split('_')[-1])

    # Сохраняем ID аккаунта в данных пользователя
    if not user_id in user_data_store:
        user_data_store[user_id] = {}

    user_data_store[user_id]['selected_account_id'] = account_id

    # Запрашиваем фото профиля
    query.edit_message_text(
        "Отправьте фотографию для установки в качестве фото профиля:"
    )

    return WAITING_PROFILE_PHOTO

# Обработчик для получения фото профиля
def handle_profile_photo(update: Update, context: CallbackContext):
    """Обработчик для получения фото профиля"""
    user_id = update.effective_user.id

    if not has_access(user_id):
        return

    # Получаем ID аккаунта из данных пользователя
    if user_id not in user_data_store or 'selected_account_id' not in user_data_store[user_id]:
        update.message.reply_text(
            "Ошибка: не выбран аккаунт. Пожалуйста, начните процесс заново.",
            reply_markup=get_accounts_menu_keyboard()
        )
        return ConversationHandler.END

    account_id = user_data_store[user_id]['selected_account_id']

    # Получаем файл с наилучшим качеством
    photo_file = update.message.photo[-1].get_file()

    # Создаем директорию для аватаров, если её нет
    avatar_dir = Path(MEDIA_DIR) / "avatars"
    os.makedirs(avatar_dir, exist_ok=True)

    # Сохраняем файл
    avatar_path = avatar_dir / f"avatar_{user_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jpg"
    photo_file.download(avatar_path)

    update.message.reply_text(
        "Фото получено. Устанавливаю фото профиля..."
    )

    try:
        # Используем новую функцию вместо ProfileManager
        success, result = update_profile_picture(account_id, str(avatar_path))

        if success:
            account = get_instagram_account(account_id, context=context, user_id=update.effective_user.id)
            update.message.reply_text(
                f"✅ Фото профиля для аккаунта {account.username} успешно установлено!",
                reply_markup=get_accounts_menu_keyboard()
            )
        else:
            update.message.reply_text(
                f"❌ Ошибка при установке фото профиля: {result}",
                reply_markup=get_accounts_menu_keyboard()
            )
    except Exception as e:
        update.message.reply_text(
            f"❌ Произошла ошибка: {str(e)}",
            reply_markup=get_accounts_menu_keyboard()
        )
    finally:
        # Очищаем данные пользователя
        if user_id in user_data_store:
            del user_data_store[user_id]

    return ConversationHandler.END
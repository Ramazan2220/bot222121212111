from telegram import InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton

def get_start_keyboard():
    """Стартовая клавиатура с кнопкой Продолжить"""
    keyboard = [
        [InlineKeyboardButton("▶️ Начать работу", callback_data="main_menu")]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_main_menu_keyboard():
    """Главное меню с основными секциями"""
    keyboard = [
        [InlineKeyboardButton("👤 Аккаунты", callback_data="menu_accounts")],
        [InlineKeyboardButton("📤 Публикации", callback_data="menu_publications")],
        [InlineKeyboardButton("🗓️ Запланированные", callback_data="menu_scheduled")],
        [InlineKeyboardButton("🔥 Прогрев", callback_data="menu_warmup")],
        [InlineKeyboardButton("🚀 Улучшенные системы", callback_data="enhanced_systems")],
        [InlineKeyboardButton("🌐 Прокси", callback_data="menu_proxy")],
        [InlineKeyboardButton("📊 Статистика", callback_data="menu_statistics")],
        [InlineKeyboardButton("⚙️ Настройки", callback_data="menu_settings")]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_accounts_menu_keyboard():
    """Меню управления аккаунтами"""
    keyboard = [
        [InlineKeyboardButton("➕ Добавить аккаунт", callback_data="add_account")],
        [InlineKeyboardButton("📥 Массовая загрузка", callback_data="bulk_add_accounts")],
        [InlineKeyboardButton("📋 Список аккаунтов", callback_data="list_accounts")],
        [InlineKeyboardButton("📁 Папки аккаунтов", callback_data="folders_menu")],
        [InlineKeyboardButton("✅ Проверить аккаунты", callback_data="check_accounts_validity")],
        [InlineKeyboardButton("⚙️ Настройка профилей", callback_data="profile_setup")],
        [InlineKeyboardButton("🔙 Главное меню", callback_data="main_menu")]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_folders_menu_keyboard():
    """Меню управления папками аккаунтов"""
    keyboard = [
        [InlineKeyboardButton("📁 Список папок", callback_data="list_folders")],
        [InlineKeyboardButton("➕ Создать папку", callback_data="create_folder")],
        [InlineKeyboardButton("✏️ Переименовать папку", callback_data="rename_folder")],
        [InlineKeyboardButton("❌ Удалить папку", callback_data="delete_folder")],
        [InlineKeyboardButton("👁️ Просмотр аккаунтов в папке", callback_data="view_folder_accounts")],
        [InlineKeyboardButton("🔙 Назад", callback_data="menu_accounts")]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_publications_menu_keyboard():
    """Меню публикаций"""
    keyboard = [
        [InlineKeyboardButton("📸 Публикация поста", callback_data="publish_post")],
        [InlineKeyboardButton("📱 История", callback_data="publish_story")],
        [InlineKeyboardButton("🎥 Reels", callback_data="publish_reels")],
        [InlineKeyboardButton("🔒 IGTV (в разработке)", callback_data="publish_igtv_blocked")],
        [InlineKeyboardButton("🗓️ Запланированные", callback_data="scheduled_posts")],
        [InlineKeyboardButton("📊 История публикаций", callback_data="publication_history")],
        [InlineKeyboardButton("🔙 Главное меню", callback_data="main_menu")]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_warmup_menu_keyboard():
    """Меню прогрева аккаунтов"""
    keyboard = [
        [InlineKeyboardButton("⚡ Быстрый прогрев", callback_data="smart_warm_menu")],
        [InlineKeyboardButton("🧠 Умный прогрев", callback_data="smart_warm_menu")],
        [InlineKeyboardButton("🎯 Прогрев по интересам", callback_data="smart_warm_menu")],
        [InlineKeyboardButton("📊 Статус прогрева", callback_data="status")],
        [InlineKeyboardButton("⚙️ Лимиты", callback_data="limits")],
        [InlineKeyboardButton("📈 Аналитика прогрева", callback_data="warmup_analytics")],
        [InlineKeyboardButton("🔙 Главное меню", callback_data="main_menu")]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_proxy_menu_keyboard():
    """Создает клавиатуру меню прокси"""
    keyboard = [
        [InlineKeyboardButton("➕ Добавить прокси", callback_data="add_proxy")],
        [InlineKeyboardButton("📋 Список прокси", callback_data="list_proxies")],
        [InlineKeyboardButton("🔄 Проверить прокси", callback_data="check_proxies")],
        [InlineKeyboardButton("📊 Распределить прокси", callback_data="distribute_proxies")],
        [InlineKeyboardButton("📤 Импорт прокси", callback_data="import_proxies")],
        [InlineKeyboardButton("🔙 Главное меню", callback_data="main_menu")]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_statistics_menu_keyboard():
    """Меню статистики"""
    keyboard = [
        [InlineKeyboardButton("📊 Общая статистика", callback_data="general_stats")],
        [InlineKeyboardButton("👤 По аккаунтам", callback_data="accounts_stats")],
        [InlineKeyboardButton("📤 По публикациям", callback_data="publications_stats")],
        [InlineKeyboardButton("🔥 По прогреву", callback_data="warmup_stats")],
        [InlineKeyboardButton("📈 Графики", callback_data="charts")],
        [InlineKeyboardButton("🔙 Главное меню", callback_data="main_menu")]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_settings_menu_keyboard():
    """Меню настроек"""
    keyboard = [
        [InlineKeyboardButton("🔧 Основные настройки", callback_data="general_settings")],
        [InlineKeyboardButton("⏰ Расписание", callback_data="schedule_settings")],
        [InlineKeyboardButton("🚨 Уведомления", callback_data="notifications_settings")],
        [InlineKeyboardButton("🔒 Безопасность", callback_data="security_settings")],
        [InlineKeyboardButton("💾 Бэкап", callback_data="backup_settings")],
        [InlineKeyboardButton("🔙 Главное меню", callback_data="main_menu")]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_messages_menu_keyboard():
    """Главное меню сообщений"""
    keyboard = [
        [InlineKeyboardButton("📝 Выберите действие", callback_data="messages_actions")],
        [InlineKeyboardButton("📸 Выберите тип публикации", callback_data="publish_type")],
        [InlineKeyboardButton("🔥 Выберите режим прогрева", callback_data="warmup_mode")],
        [InlineKeyboardButton("📊 Просмотр задач по статусам", callback_data="tasks_by_status")],
        [InlineKeyboardButton("👤 Связь с администратором @ramazhan", url="https://t.me/ramazhan")],
        [InlineKeyboardButton("🔙 Главное меню", callback_data="main_menu")]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_messages_actions_keyboard():
    """Подменю действий в сообщениях"""
    keyboard = [
        [InlineKeyboardButton("➕ Добавить аккаунт", callback_data="add_account")],
        [InlineKeyboardButton("📥 Массовая загрузка", callback_data="bulk_add_accounts")],
        [InlineKeyboardButton("📋 Список аккаунтов", callback_data="list_accounts")],
        [InlineKeyboardButton("📊 Статистика аккаунтов", callback_data="accounts_statistics")],
        [InlineKeyboardButton("🔙 Назад", callback_data="messages_menu")]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_publish_type_keyboard():
    """Меню выбора типа публикации"""
    keyboard = [
        [InlineKeyboardButton("📸 Публикация", callback_data="publish_post")],
        [InlineKeyboardButton("📱 История", callback_data="publish_story")],
        [InlineKeyboardButton("🔒 IGTV (в разработке)", callback_data="publish_igtv_blocked")],
        [InlineKeyboardButton("🎥 Reels", callback_data="publish_reels")],
        [InlineKeyboardButton("🔧 Выставить лимиты", callback_data="set_limits")],
        [InlineKeyboardButton("🔙 Назад", callback_data="messages_menu")]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_warmup_mode_keyboard():
    """Меню выбора режима прогрева"""
    keyboard = [
        [InlineKeyboardButton("⚡ Быстрый прогрев", callback_data="smart_warm_menu")],
        [InlineKeyboardButton("🧠 Умный прогрев", callback_data="smart_warm_menu")],
        [InlineKeyboardButton("📊 Статус прогрева", callback_data="status")],
        [InlineKeyboardButton("⚙️ Лимиты", callback_data="limits")],
        [InlineKeyboardButton("🔙 Назад", callback_data="messages_menu")]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_tasks_by_status_keyboard():
    """Меню просмотра задач по статусам"""
    keyboard = [
        [InlineKeyboardButton("✅ Активные задачи", callback_data="active_tasks")],
        [InlineKeyboardButton("⏸️ Приостановленные", callback_data="paused_tasks")],
        [InlineKeyboardButton("✓ Завершенные", callback_data="completed_tasks")],
        [InlineKeyboardButton("❌ С ошибками", callback_data="error_tasks")],
        [InlineKeyboardButton("🔙 Назад", callback_data="messages_menu")]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_tasks_menu_keyboard():
    """Создает клавиатуру меню задач"""
    keyboard = [
        [InlineKeyboardButton("📤 Опубликовать сейчас", callback_data="publish_now")],
        [InlineKeyboardButton("⏰ Отложенная публикация", callback_data="schedule_publish")],
        [InlineKeyboardButton("🔙 Назад в главное меню", callback_data="main_menu")]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_scheduled_menu_keyboard():
    """Меню запланированных публикаций"""
    keyboard = [
        [InlineKeyboardButton("📸 Запланировать пост", callback_data="schedule_post")],
        [InlineKeyboardButton("📱 Запланировать историю", callback_data="schedule_story")],
        [InlineKeyboardButton("🎥 Запланировать Reels", callback_data="schedule_reels")],
        [InlineKeyboardButton("🔒 IGTV (в разработке)", callback_data="schedule_igtv_blocked")],
        [InlineKeyboardButton("🗓️ Просмотр расписания", callback_data="view_schedule")],
        [InlineKeyboardButton("📊 История запланированных", callback_data="scheduled_history")],
        [InlineKeyboardButton("🔙 Главное меню", callback_data="main_menu")]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_accounts_list_keyboard(accounts):
    """Создает клавиатуру со списком аккаунтов"""
    keyboard = []

    for account in accounts:
        # Добавляем кнопку для каждого аккаунта
        keyboard.append([InlineKeyboardButton(
            f"{account.username} {'✅' if account.is_active else '❌'}",
            callback_data=f"account_{account.id}"
        )])

    # Добавляем кнопку "Назад"
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="accounts_menu")])

    return InlineKeyboardMarkup(keyboard)

def get_account_actions_keyboard(account_id):
    """Создает клавиатуру действий для конкретного аккаунта"""
    keyboard = [
        [InlineKeyboardButton("⚙️ Настроить профиль", callback_data=f"profile_setup_{account_id}")],
        [InlineKeyboardButton("📤 Опубликовать", callback_data=f"publish_to_{account_id}")],
        [InlineKeyboardButton("🔑 Сменить пароль", callback_data=f"change_password_{account_id}")],
        [InlineKeyboardButton("🌐 Назначить прокси", callback_data=f"assign_proxy_{account_id}")],
        [InlineKeyboardButton("❌ Удалить аккаунт", callback_data=f"delete_account_{account_id}")],
        [InlineKeyboardButton("🔙 Назад к списку", callback_data="list_accounts")]
    ]
    return InlineKeyboardMarkup(keyboard)





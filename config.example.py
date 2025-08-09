# -*- coding: utf-8 -*-

"""
Пример конфигурационного файла для Instagram Telegram Bot
Скопируйте этот файл как config.py и заполните своими данными
"""

# Telegram Bot настройки
TELEGRAM_BOT_TOKEN = "YOUR_BOT_TOKEN_HERE"  # Получите от @BotFather
ADMIN_USER_IDS = [123456789]  # Ваш Telegram user ID

# База данных
DATABASE_URL = "sqlite:///instagram.db"  # или PostgreSQL URL

# Instagram API настройки (опционально)
INSTAGRAM_USERNAME = "your_instagram_username"
INSTAGRAM_PASSWORD = "your_instagram_password"

# Прокси настройки (опционально)
DEFAULT_PROXY = None  # или "http://user:pass@proxy.com:8080"

# Пути к файлам
ACCOUNTS_PATH = "data/accounts"
DEVICES_PATH = "devices"
MEDIA_PATH = "media"
LOGS_PATH = "data/logs"

# Настройки прогрева аккаунтов
WARMUP_SETTINGS = {
    "min_delay": 30,      # Минимальная задержка между действиями (сек)
    "max_delay": 180,     # Максимальная задержка между действиями (сек)
    "daily_limit": 100,   # Дневной лимит действий
}

# Email настройки для верификации (опционально)
EMAIL_SETTINGS = {
    "imap_server": "imap.gmail.com",
    "imap_port": 993,
    "smtp_server": "smtp.gmail.com", 
    "smtp_port": 587,
}

# API ключи (если используются)
OPENAI_API_KEY = "your_openai_api_key_here"  # Для AI функций
ANTICAPTCHA_API_KEY = "your_anticaptcha_key_here"  # Для решения капчи

# Настройки безопасности
MAX_ACCOUNTS_PER_PROXY = 5
MAX_ACTIONS_PER_HOUR = 60
BAN_PAUSE_HOURS = 48

# Режим отладки
DEBUG = False 

# ============================================================================
# LAZY LOADING CONFIGURATION (ОПТИМИЗАЦИЯ ПАМЯТИ)
# ============================================================================

# Включить Lazy Loading для экономии памяти
USE_LAZY_LOADING = True

# Максимальное количество одновременно активных Instagram клиентов
# Вместо 30,000 клиентов в памяти, будет только указанное количество
LAZY_MAX_ACTIVE_CLIENTS = 1000

# Интервал очистки неактивных клиентов (в секундах)
# 1800 = 30 минут
LAZY_CLEANUP_INTERVAL = 1800

# Fallback на обычные клиенты при ошибках Lazy Loading
LAZY_FALLBACK_TO_NORMAL = True

# Время неактивности после которого клиент считается старым (секунды)
# 3600 = 1 час
LAZY_INACTIVE_THRESHOLD = 3600

# Логирование статистики Lazy Loading
LAZY_ENABLE_STATS_LOGGING = True

# Интервал логирования статистики (секунды)
LAZY_STATS_LOG_INTERVAL = 300  # 5 минут 
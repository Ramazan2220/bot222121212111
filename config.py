import os
from pathlib import Path

# Загружаем переменные окружения из файла .env
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # Если python-dotenv не установлен, продолжаем без него

# Базовые пути
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / 'data'
ACCOUNTS_DIR = DATA_DIR / 'accounts'
MEDIA_DIR = DATA_DIR / 'media'
LOGS_DIR = DATA_DIR / 'logs'

# Создаем директории, если они не существуют
os.makedirs(ACCOUNTS_DIR, exist_ok=True)
os.makedirs(MEDIA_DIR, exist_ok=True)
os.makedirs(LOGS_DIR, exist_ok=True)

# Настройки Telegram бота
# Пытаемся получить токен из переменных окружения, иначе используем значение по умолчанию
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", '8092949155:AAEs6GSSqEU4C_3qNkskqVNAdcoAUHZi0fE')
ADMIN_USER_IDS = [6499246016]  # Замените на ваш Telegram ID

# Токен для нового веб-интерфейс бота
WEB_TELEGRAM_BOT_TOKEN = os.getenv("WEB_TELEGRAM_BOT_TOKEN", '7966714751:AAEXhWtUxU4Hp9nnlEN1EUjK8wiYkwJmMfw')  # Добавьте полный токен!
# Например: WEB_TELEGRAM_BOT_TOKEN = os.getenv("WEB_TELEGRAM_BOT_TOKEN", '1234567890:ABCdefGHIjklMNOpqrsTUVwxyz')

# Настройки базы данных
# DATABASE_URL = f'sqlite:///{DATA_DIR}/database.sqlite'  # Старая SQLite база
# Для ручного фейловера используйте переменную окружения DATABASE_URL
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    'postgresql://postgres.rjjndgmnfebunurtjlmb:wunceN-raxhaf-3vyzve@aws-0-eu-central-1.pooler.supabase.com:5432/postgres'
)

# Параметры Redis (реальный Redis опционален; по умолчанию fallback на FakeRedis)
REDIS_URL = os.getenv("REDIS_URL", "")
REDIS_HOST = os.getenv("REDIS_HOST", "127.0.0.1")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", None)
# Если явно не указали, то используем FakeRedis. Если указали REDIS_URL — то используем реальный
REDIS_USE_FAKE = os.getenv("REDIS_USE_FAKE", "false" if REDIS_URL else "true").lower() == "true"
REDIS_DEFAULT_TTL_SECONDS = int(os.getenv("REDIS_DEFAULT_TTL_SECONDS", "300"))

# Параметры engine/пула
ENGINE_POOL_PRE_PING = os.getenv("ENGINE_POOL_PRE_PING", "true").lower() == "true"
ENGINE_POOL_USE_LIFO = os.getenv("ENGINE_POOL_USE_LIFO", "true").lower() == "true"

# Настройки многопоточности
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "50"))  # Максимальное количество одновременных потоков

# Быстрый пакет оптимизаций (очереди/бэкпрешер/бэкофф)
WARMUP_MAX_PER_USER = int(os.getenv("WARMUP_MAX_PER_USER", "1"))
WARMUP_BACKOFF_MIN_MINUTES = int(os.getenv("WARMUP_BACKOFF_MIN_MINUTES", "30"))
WARMUP_BACKOFF_MAX_MINUTES = int(os.getenv("WARMUP_BACKOFF_MAX_MINUTES", "90"))
TASK_QUEUE_MAXSIZE = int(os.getenv("TASK_QUEUE_MAXSIZE", "1000"))

# Настройки логирования
LOG_LEVEL = 'INFO'
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
LOG_FILE = LOGS_DIR / 'bot.log'

# Настройки Instagram
INSTAGRAM_LOGIN_ATTEMPTS = 3  # Количество попыток входа
INSTAGRAM_DELAY_BETWEEN_REQUESTS = 5  # Задержка между запросами (в секундах)

# Настройки таймаутов для Telegram API
TELEGRAM_READ_TIMEOUT = 60  # Таймаут чтения в секундах
TELEGRAM_CONNECT_TIMEOUT = 60  # Таймаут соединения в секундах

# Настройки бота верификации
VERIFICATION_BOT_TOKEN = "7709908636:AAHB9bH74-w565IApIggZ7L1XwdOufXSnu0"  # Токен бота верификации
VERIFICATION_BOT_ADMIN_ID = 6499246016  # Ваш ID в Telegram

# Добавьте в config.py
TELEGRAM_ERROR_LOG = LOGS_DIR / 'telegram_errors.log'

# EMAIL_SETTINGS для продвинутых систем
EMAIL_SETTINGS = {
    'enabled': True,
    'imap_server': 'imap.firstmail.ltd',
    'imap_port': 993,
    'use_ssl': True,
    'check_interval': 30,
    'max_wait_time': 300,
    'credentials': {
        # Динамически загружаются из базы данных по email аккаунта
    }
}

# Настройки Lazy Loading
USE_LAZY_LOADING = True
LAZY_MAX_ACTIVE_CLIENTS = 1000
LAZY_CLEANUP_INTERVAL = 1800  # 30 минут в секундах
LAZY_FALLBACK_TO_NORMAL = True
LAZY_INACTIVE_THRESHOLD = 600  # 10 минут в секундах
LAZY_ENABLE_STATS_LOGGING = True
LAZY_STATS_LOG_INTERVAL = 300  # 5 минут в секундах
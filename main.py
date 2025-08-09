import logging
import threading
import time
import sys
from datetime import datetime
from telegram.ext import Updater
from instagram.monkey_patch import *
from instagram.client_patch import *  # Импортируем усиленные патчи устройств

# Импортируем наши модули
from config import (
    TELEGRAM_TOKEN, LOG_LEVEL, LOG_FORMAT, LOG_FILE,
    TELEGRAM_READ_TIMEOUT, TELEGRAM_CONNECT_TIMEOUT,
    TELEGRAM_ERROR_LOG,
    # Lazy Loading конфигурация
    USE_LAZY_LOADING, LAZY_MAX_ACTIVE_CLIENTS, LAZY_CLEANUP_INTERVAL,
    LAZY_FALLBACK_TO_NORMAL, LAZY_INACTIVE_THRESHOLD,
    LAZY_ENABLE_STATS_LOGGING, LAZY_STATS_LOG_INTERVAL
)
from database.db_manager import init_db
from telegram_bot.bot import setup_bot
from utils.scheduler import start_scheduler
from utils.task_queue import start_task_queue  # Добавляем импорт
from utils.system_monitor import start_system_monitoring, stop_system_monitoring

print(f"Python version: {sys.version}")
print(f"Python executable: {sys.executable}")
print(f"Python path: {sys.path}")

# Настраиваем логирование
logging.basicConfig(
    format=LOG_FORMAT,
    level=getattr(logging, LOG_LEVEL),
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def error_callback(update, context):
    """Логирование ошибок Telegram"""
    logger.error(f'Update "{update}" caused error "{context.error}"')

    # Записываем ошибку в отдельный файл
    with open(TELEGRAM_ERROR_LOG, 'a') as f:
        f.write(f'{datetime.now()} - Update: {update} - Error: {context.error}\n')

def main():
    # Инициализируем базу данных
    logger.info("Инициализация базы данных...")
    init_db()
    
    # 🔒 АКТИВАЦИЯ НОВОЙ СИСТЕМЫ ИЗОЛЯЦИИ ПОЛЬЗОВАТЕЛЕЙ
    logger.info("🔒 Активация продвинутой системы изоляции пользователей...")
    from database.user_context_manager import create_scoped_session_with_user_isolation
    from sqlalchemy import create_engine
    from config import DATABASE_URL
    engine = create_engine(DATABASE_URL)
    scoped_session = create_scoped_session_with_user_isolation(engine)
    logger.info("🔒 ✅ Продвинутая система изоляции пользователей активирована")
    
    # 🛡️ АКТИВАЦИЯ НЕВИДИМОГО ПАТЧА ИЗОЛЯЦИИ
    logger.info("🛡️ Активация невидимого патча изоляции...")
    from database.invisible_user_patch import activate_invisible_user_isolation
    activate_invisible_user_isolation()
    logger.info("🛡️ ✅ Невидимый патч изоляции активирован")

    # Запускаем планировщик задач в отдельном потоке
    logger.info("Запуск планировщика задач...")
    scheduler_thread = threading.Thread(target=start_scheduler, daemon=True)
    scheduler_thread.start()

    # Запускаем обработчик очереди задач
    logger.info("Запуск обработчика очереди задач...")
    start_task_queue()  # Добавляем запуск очереди задач

    # Запускаем мониторинг системы
    logger.info("Запуск мониторинга системных ресурсов...")
    start_system_monitoring()
    
    # Запуск умного валидатора аккаунтов
    logger.info("Запуск умного валидатора аккаунтов...")
    from utils.smart_validator_service import get_smart_validator
    smart_validator = get_smart_validator()
    smart_validator.start()

    # Инициализация Universal Client Adapter (Lazy Loading + обратная совместимость)
    logger.info("🚀 Инициализация Universal Client Adapter с Lazy Loading...")
    from instagram.client_adapter import init_client_adapter, ClientConfig
    
    # Конфигурация для оптимального использования памяти
    client_config = ClientConfig(
        use_lazy_loading=True,      # Включаем Lazy Loading
        lazy_max_active=1000,       # Максимум активных клиентов (вместо 30,000)
        lazy_cleanup_interval=1800, # Очистка каждые 30 минут
        fallback_to_normal=True     # Fallback на обычные клиенты при ошибках
    )
    init_client_adapter(client_config)
    logger.info("✅ Universal Client Adapter готов (экономия памяти: ~98%)")

    # Инициализация Instagram Client Pool (для fallback режима)
    logger.info("🏊‍♂️ Готовим Instagram Client Pool для fallback режима...")
    from instagram.client_pool import init_client_pool
    init_client_pool(
        initial_max_clients=50,      # Уменьшаем т.к. основное - lazy клиенты
        max_clients_limit=200,       # Уменьшаем лимит для fallback
        adaptive_scaling=True,       # Включаем адаптивное масштабирование
        inactive_threshold=2700,     # 45 минут неактивности
        old_threshold=14400          # 4 часа максимальный возраст
    )
    logger.info("✅ Instagram Client Pool готов к работе")

    # Инициализация Structured Logging с Sampling
    # logger.info("📝 Готовим Structured Logging для оптимизации логов...")
    # from utils.structured_logger import init_structured_logging, SamplingConfig, SamplingStrategy
    
    # # Настройка специализированных логгеров
    # configs = {
    #     "instagram": SamplingConfig(SamplingStrategy.ADAPTIVE, min_level=logging.INFO),
    #     "telegram": SamplingConfig(SamplingStrategy.TIME_WINDOW, time_window=60, max_logs_per_window=100),
    #     "performance": SamplingConfig(SamplingStrategy.FREQUENCY, frequency=10),
    #     "database": SamplingConfig(SamplingStrategy.TIME_WINDOW, time_window=120, max_logs_per_window=50),
    #     "warmup": SamplingConfig(SamplingStrategy.HASH_BASED, hash_percentage=25),
    #     "publish": SamplingConfig(SamplingStrategy.FREQUENCY, frequency=5)
    # }
    
    # init_structured_logging(configs)
    # logger.info("✅ Structured Logging готов к работе")

    # 🚀 Инициализация Notification Receiver
    logger.info("📨 Инициализация Notification Receiver...")
    from telegram_bot.notification_receiver import start_notification_receiver
    notification_receiver = start_notification_receiver(TELEGRAM_TOKEN)
    logger.info("✅ Notification Receiver готов к работе")
    
    # ⏰ Инициализация Subscription Monitor
    logger.info("⏰ Инициализация Subscription Monitor...")
    from utils.subscription_monitor import get_subscription_monitor
    subscription_monitor = get_subscription_monitor()
    logger.info("✅ Subscription Monitor готов к работе")
    
    # 📢 Инициализация Broadcast System
    logger.info("📢 Инициализация Broadcast System...")
    from utils.broadcast_system import get_broadcast_system
    broadcast_system = get_broadcast_system()
    logger.info("✅ Broadcast System готов к работе")

    # Создаем и настраиваем Telegram бота
    logger.info("Создание Telegram бота...")
    updater = Updater(
        TELEGRAM_TOKEN,
        use_context=True,
        request_kwargs={
            'read_timeout': TELEGRAM_READ_TIMEOUT,
            'connect_timeout': TELEGRAM_CONNECT_TIMEOUT
        }
    )

    # Регистрируем обработчики
    setup_bot(updater)

    # Добавляем обработчик ошибок
    updater.dispatcher.add_error_handler(error_callback)

    try:
        logger.info("Запуск бота...")
        updater.start_polling()
        logger.info("Бот запущен и готов к работе!")
        updater.idle()
    except KeyboardInterrupt:
        logger.info("Получен сигнал остановки...")
    except Exception as e:
        logger.error(f"Критическая ошибка в работе бота: {e}")
    finally:
        logger.info("Остановка бота...")
        stop_system_monitoring()
        
        # Останавливаем все системы
        try:
            if 'notification_receiver' in locals():
                notification_receiver.stop_receiving()
                logger.info("📨 Notification Receiver остановлен")
        except:
            pass
        
        try:
            if 'subscription_monitor' in locals():
                subscription_monitor.stop_monitoring()
                logger.info("⏰ Subscription Monitor остановлен")
        except:
            pass
        
        try:
            if 'broadcast_system' in locals():
                broadcast_system.stop_processor()
                logger.info("📢 Broadcast System остановлен")
        except:
            pass

if __name__ == '__main__':
    main()
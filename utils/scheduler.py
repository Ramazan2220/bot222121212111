import logging
import time
import threading
import schedule
import datetime
import random
import os

from database.db_manager import get_pending_tasks, update_publish_task_status, get_all_accounts
from instagram.profile_manager import ProfileManager
from instagram.post_manager import PostManager
from instagram.reels_manager import ReelsManager
from database.db_manager import get_scheduled_tasks
from utils.task_queue import add_task_to_queue
from instagram.client import Client

logger = logging.getLogger(__name__)

def execute_task(task):
    """Выполнение запланированной задачи"""
    try:
        logger.info(f"Выполнение запланированной задачи {task.id} типа {task.task_type}")

        # Выбираем менеджер в зависимости от типа задачи
        if task.task_type == 'profile':
            manager = ProfileManager(task.account_id)
            success, error = manager.execute_profile_task(task)
        elif task.task_type in ['post', 'mosaic']:
            manager = PostManager(task.account_id)
            success, error = manager.execute_post_task(task)
        elif task.task_type == 'reel':
            manager = ReelsManager(task.account_id)
            success, error = manager.execute_reel_task(task)
        else:
            logger.error(f"Неизвестный тип задачи: {task.task_type}")
            update_publish_task_status(task.id, 'failed', error_message=f"Неизвестный тип задачи: {task.task_type}")
            return

        if success:
            logger.info(f"Задача {task.id} выполнена успешно")
        else:
            logger.error(f"Задача {task.id} не выполнена: {error}")
    except Exception as e:
        logger.error(f"Ошибка при выполнении задачи {task.id}: {e}")
        update_publish_task_status(task.id, 'failed', error_message=str(e))

def check_scheduled_tasks(bot=None):
    """Проверка и выполнение запланированных задач"""
    try:
        # Получаем текущее время в локальной временной зоне
        now = datetime.datetime.now()
        
        logger.debug(f"🕐 Проверка запланированных задач. Текущее время: {now}")

        # Получаем все запланированные задачи
        tasks = get_scheduled_tasks()
        
        logger.debug(f"📋 Найдено {len(tasks)} запланированных задач")

        for task in tasks:
            # Если время выполнения задачи наступило или прошло
            if task.scheduled_time and task.scheduled_time <= now:
                logger.info(f"⏰ Время выполнения задачи #{task.id} наступило! Запланировано: {task.scheduled_time}, Сейчас: {now}")
                
                # Используем новую систему очереди задач для всех типов задач
                if bot:
                    logger.info(f"📤 Добавление задачи #{task.id} (тип: {task.task_type}) в очередь задач")
                    # Получаем user_id из задачи для отправки уведомлений
                    user_id = task.user_id if hasattr(task, 'user_id') else None
                    logger.info(f"📧 User ID для уведомлений: {user_id}")
                    add_task_to_queue(task.id, user_id, bot)
                else:
                    # Fallback для старого механизма если нет бота
                    logger.info(f"🔄 Запуск задачи #{task.id} в отдельном потоке (fallback)")
                    threading.Thread(target=execute_task, args=(task,)).start()
            else:
                if task.scheduled_time:
                    time_diff = task.scheduled_time - now
                    logger.debug(f"⏳ Задача #{task.id} запланирована на {task.scheduled_time} (через {time_diff})")
                    
    except Exception as e:
        logger.error(f"❌ Ошибка при проверке запланированных задач: {e}")
        import traceback
        logger.error(traceback.format_exc())

def refresh_account_sessions():
    """Периодическое обновление сессий аккаунтов"""
    try:
        logger.info("Запуск обновления сессий аккаунтов")
        accounts = get_all_accounts()

        for account in accounts:
            try:
                # Проверяем существование файла сессии
                session_path = f"Data/accounts/{account.username}_session.json"
                if not os.path.exists(session_path):
                    logger.warning(f"Файл сессии для аккаунта {account.username} не найден, пропускаем")
                    continue

                # Создаем клиент и обновляем сессию
                client = Client(username=account.username, password=account.password, proxy=account.proxy)

                # Пытаемся загрузить существующую сессию
                if client.load_session_from_file():
                    # Выполняем простое действие для обновления сессии
                    client.get_timeline_feed()
                    # Сохраняем обновленную сессию
                    client.save_session_to_file()
                    logger.info(f"Сессия для аккаунта {account.username} успешно обновлена")
                else:
                    logger.warning(f"Не удалось загрузить сессию для аккаунта {account.username}")

                # Добавляем случайную задержку между обновлениями аккаунтов
                time.sleep(random.uniform(5, 15))

            except Exception as e:
                logger.error(f"Ошибка при обновлении сессии аккаунта {account.username}: {e}")

        logger.info("Обновление сессий аккаунтов завершено")
    except Exception as e:
        logger.error(f"Ошибка в процессе обновления сессий аккаунтов: {e}")

def start_scheduler():
    """Запуск планировщика задач"""
    try:
        # Получаем бота для отправки уведомлений
        from telegram.ext import Updater
        from config import TELEGRAM_TOKEN

        updater = Updater(TELEGRAM_TOKEN, use_context=True)
        bot = updater.bot

        # Проверяем запланированные задачи каждую минуту
        schedule.every(1).minutes.do(check_scheduled_tasks, bot=bot)

        # Обновляем сессии аккаунтов каждые 12 часов
        schedule.every(12).hours.do(refresh_account_sessions)

        logger.info("Планировщик задач запущен")

        # Бесконечный цикл для выполнения запланированных задач
        while True:
            schedule.run_pending()
            time.sleep(10)  # Пауза между проверками
    except Exception as e:
        logger.error(f"Ошибка в планировщике задач: {e}")
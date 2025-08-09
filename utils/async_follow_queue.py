import logging
import threading
import queue
import time
import asyncio
from typing import Optional
from concurrent.futures import ThreadPoolExecutor

from database.db_manager import get_session
from database.models import FollowTask, FollowTaskStatus
from utils.follow_manager import FollowManager

logger = logging.getLogger(__name__)

# Глобальные переменные для управления очередью
follow_queue: Optional[queue.Queue] = None
follow_executor: Optional[ThreadPoolExecutor] = None
follow_thread: Optional[threading.Thread] = None
is_running = False


def process_follow_task(task_id: int):
    """Обработать одну задачу автоподписки"""
    logger.info(f"🔄 Обработка задачи автоподписки #{task_id}")
    
    try:
        # Создаем новый event loop для этого потока
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            with FollowManager(task_id) as manager:
                # Выполняем задачу асинхронно
                loop.run_until_complete(manager.execute_follow_task(manager.task))
        finally:
            loop.close()
            
    except Exception as e:
        logger.error(f"❌ Ошибка при обработке задачи #{task_id}: {e}")
        
        # Обновляем статус задачи на FAILED
        session = get_session()
        try:
            task = session.query(FollowTask).filter_by(id=task_id).first()
            if task and task.status == FollowTaskStatus.RUNNING:
                task.status = FollowTaskStatus.FAILED
                task.error = str(e)
                session.commit()
        finally:
            session.close()


def follow_queue_worker():
    """Рабочий поток для обработки очереди задач"""
    logger.info("🚀 Запущен рабочий поток очереди автоподписок")
    
    while is_running:
        try:
            # Получаем задачу из очереди с таймаутом
            task_id = follow_queue.get(timeout=1)
            
            if task_id is None:  # Сигнал остановки
                break
            
            # Отправляем задачу на выполнение в пул потоков
            follow_executor.submit(process_follow_task, task_id)
            
        except queue.Empty:
            # Проверяем новые задачи в БД
            check_pending_tasks()
        except Exception as e:
            logger.error(f"❌ Ошибка в рабочем потоке очереди: {e}")
            time.sleep(1)
    
    logger.info("🛑 Рабочий поток очереди автоподписок остановлен")


def check_pending_tasks():
    """Проверить наличие новых задач в БД и добавить их в очередь"""
    session = get_session()
    try:
        # Находим все задачи со статусом PENDING
        pending_tasks = session.query(FollowTask).filter_by(
            status=FollowTaskStatus.PENDING
        ).all()
        
        for task in pending_tasks:
            # Обновляем статус на RUNNING
            task.status = FollowTaskStatus.RUNNING
            session.commit()
            
            # Добавляем в очередь
            follow_queue.put(task.id)
            logger.info(f"📋 Добавлена задача #{task.id} в очередь")
        
    except Exception as e:
        logger.error(f"❌ Ошибка при проверке новых задач: {e}")
    finally:
        session.close()


def start_async_follow_queue(max_workers: int = 5):
    """Запустить асинхронную очередь обработки задач"""
    global follow_queue, follow_executor, follow_thread, is_running
    
    if is_running:
        logger.warning("⚠️ Очередь автоподписок уже запущена")
        return
    
    # Определяем максимальное количество потоков из активных задач
    try:
        session = get_session()
        active_tasks = session.query(FollowTask).filter(
            FollowTask.status.in_([FollowTaskStatus.PENDING, FollowTaskStatus.RUNNING])
        ).all()
        
        # Получаем максимальное количество потоков из настроек задач
        max_threads_from_tasks = 1
        for task in active_tasks:
            if task.filters:
                threads = task.filters.get('threads', 1)
                max_threads_from_tasks = max(max_threads_from_tasks, threads)
        
        # Используем максимум из переданного параметра и настроек задач
        max_workers = max(max_workers, max_threads_from_tasks)
        session.close()
    except Exception as e:
        logger.warning(f"Не удалось получить настройки потоков из задач: {e}")
    
    logger.info(f"🚀 Запуск асинхронной очереди автоподписок с {max_workers} потоками")
    
    # Инициализируем компоненты
    follow_queue = queue.Queue()
    follow_executor = ThreadPoolExecutor(max_workers=max_workers)
    is_running = True
    
    # Запускаем рабочий поток
    follow_thread = threading.Thread(target=follow_queue_worker, daemon=True)
    follow_thread.start()
    
    # Проверяем существующие задачи
    check_pending_tasks()
    
    logger.info("✅ Асинхронная очередь автоподписок запущена")


def stop_async_follow_queue():
    """Остановить асинхронную очередь"""
    global is_running
    
    if not is_running:
        logger.warning("⚠️ Очередь автоподписок уже остановлена")
        return
    
    logger.info("🛑 Останавливаем асинхронную очередь автоподписок...")
    
    # Сигнализируем об остановке
    is_running = False
    
    # Добавляем сигнал остановки в очередь
    if follow_queue:
        follow_queue.put(None)
    
    # Ждем завершения рабочего потока
    if follow_thread:
        follow_thread.join(timeout=5)
    
    # Завершаем пул потоков
    if follow_executor:
        follow_executor.shutdown(wait=True)
    
    logger.info("✅ Асинхронная очередь автоподписок остановлена")


def add_task_to_queue(task_id: int):
    """Добавить задачу в очередь"""
    if not is_running:
        logger.error("❌ Очередь автоподписок не запущена")
        return False
    
    follow_queue.put(task_id)
    logger.info(f"📋 Задача #{task_id} добавлена в очередь")
    return True 
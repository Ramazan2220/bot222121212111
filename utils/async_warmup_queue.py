#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Асинхронный обработчик очереди задач прогрева аккаунтов
"""

import logging
import threading
import queue
import time
import json
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional

from database.db_manager import get_session
from database.models import WarmupStatus, WarmupTask
from instagram.client import InstagramClient
from utils.warmup_manager import WarmupManager
from utils.user_logger import log_user_info, log_user_error
from database.db_manager import get_instagram_account
from instagram.health_monitor import AdvancedHealthMonitor
from instagram.predictive_monitor import PredictiveMonitor
from config import WARMUP_MAX_PER_USER, WARMUP_BACKOFF_MIN_MINUTES, WARMUP_BACKOFF_MAX_MINUTES, TASK_QUEUE_MAXSIZE

logger = logging.getLogger(__name__)

# Глобальные переменные для управления очередью
warmup_executor = None
warmup_queue_running = False
active_tasks = {}
task_lock = threading.Lock()

# Настройки по умолчанию
DEFAULT_MAX_WORKERS = 3  # Количество одновременных потоков
MAX_CONCURRENT_ACCOUNTS = 5  # Максимум аккаунтов одновременно


class AsyncWarmupQueue:
    """Асинхронная очередь прогрева с поддержкой параллельной обработки"""
    
    def __init__(self, max_workers: int = DEFAULT_MAX_WORKERS):
        self.max_workers = max_workers
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.task_queue = queue.Queue(maxsize=TASK_QUEUE_MAXSIZE)
        self.running = False
        self.active_accounts = set()
        self.queued_tasks = set()  # Добавляем отслеживание задач в очереди
        self.lock = threading.Lock()
        # Новый: лимит параллельных задач на пользователя
        self.active_by_user: Dict[int, int] = {}
        self.max_per_user = WARMUP_MAX_PER_USER
        # Мониторы для risk/health
        self.health_monitor = AdvancedHealthMonitor()
        self.predict_monitor = PredictiveMonitor()
        
    def start(self):
        """Запустить асинхронную очередь"""
        self.running = True
        logger.info(f"🚀 Запуск асинхронной очереди прогрева с {self.max_workers} потоками")
        
        # Запускаем обработчик очереди
        threading.Thread(target=self._process_queue, daemon=True).start()
        
    def stop(self):
        """Остановить очередь"""
        logger.info("🛑 Остановка асинхронной очереди прогрева...")
        self.running = False
        self.executor.shutdown(wait=True)
        
    def add_task(self, task):
        """Добавить задачу в очередь"""
        with self.lock:
            if task.id not in self.queued_tasks:
                self.task_queue.put(task)
                self.queued_tasks.add(task.id)
                logger.info(f"➕ Задача #{task.id} добавлена в очередь")
            
    def _process_queue(self):
        """Основной цикл обработки очереди"""
        futures = {}
        
        while self.running:
            try:
                # Проверяем завершенные задачи
                completed_futures = []
                for future in list(futures.keys()):
                    if future.done():
                        completed_futures.append(future)
                        
                # Обрабатываем завершенные задачи
                for future in completed_futures:
                    task = futures.pop(future)
                    try:
                        result = future.result()
                        logger.info(f"✅ Задача #{task.id} завершена")
                        with self.lock:
                            self.active_accounts.discard(task.account_id)
                            # уменьшаем счётчик по пользователю
                            try:
                                s = json.loads(task.settings) if task.settings else {}
                                uid = s.get('user_id')
                            except Exception:
                                uid = None
                            if uid is None:
                                acc = get_instagram_account(task.account_id)
                                uid = getattr(acc, 'user_id', None)
                            if uid in self.active_by_user:
                                self.active_by_user[uid] = max(0, self.active_by_user[uid] - 1)
                            self.queued_tasks.discard(task.id)
                    except Exception as e:
                        logger.error(f"❌ Ошибка в задаче #{task.id}: {e}")
                        with self.lock:
                            self.active_accounts.discard(task.account_id)
                            try:
                                s = json.loads(task.settings) if task.settings else {}
                                uid = s.get('user_id')
                            except Exception:
                                uid = None
                            if uid is None:
                                acc = get_instagram_account(task.account_id)
                                uid = getattr(acc, 'user_id', None)
                            if uid in self.active_by_user:
                                self.active_by_user[uid] = max(0, self.active_by_user[uid] - 1)
                            self.queued_tasks.discard(task.id)
                
                # Добавляем новые задачи если есть свободные слоты
                while len(futures) < self.max_workers and not self.task_queue.empty():
                    try:
                        task = self.task_queue.get_nowait()
                        
                        # Проверяем, не обрабатывается ли уже этот аккаунт
                        with self.lock:
                            if task.account_id in self.active_accounts:
                                logger.info(f"⏳ Аккаунт {task.account_id} уже обрабатывается, откладываем")
                                self.task_queue.put(task)  # Возвращаем в очередь
                                continue
                            
                            # Проверяем лимит на пользователя
                            uid = None
                            try:
                                s = json.loads(task.settings) if task.settings else {}
                                uid = s.get('user_id')
                            except Exception:
                                uid = None
                            if uid is None:
                                acc = get_instagram_account(task.account_id)
                                uid = getattr(acc, 'user_id', None)
                            count = self.active_by_user.get(uid or -1, 0)
                            if count >= self.max_per_user:
                                logger.info(f"⏳ Лимит активных задач для пользователя {uid}, откладываем задачу #{task.id}")
                                self.task_queue.put(task)
                                continue
                            
                            self.active_accounts.add(task.account_id)
                            self.active_by_user[uid or -1] = count + 1
                        
                        # Запускаем задачу в отдельном потоке
                        future = self.executor.submit(self._process_task, task)
                        futures[future] = task
                        logger.info(f"🔄 Запущена обработка задачи #{task.id} для аккаунта {task.account_id}")
                        
                    except queue.Empty:
                        break
                
                # Небольшая пауза
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"❌ Ошибка в цикле обработки очереди: {e}")
                time.sleep(5)
                
    def _process_task(self, task):
        """Обработать одну задачу прогрева"""
        try:
            # Определяем user_id
            user_id = None
            try:
                settings = json.loads(task.settings) if task.settings else {}
                user_id = settings.get('user_id')
            except Exception:
                settings = {}
            if not user_id:
                account = get_instagram_account(task.account_id)
                user_id = getattr(account, 'user_id', None)

            # Backoff: проверяем не пора ли запускать
            progress = task.progress or {}
            next_at = progress.get('next_attempt_at')
            if next_at:
                try:
                    next_ts = datetime.fromisoformat(next_at)
                    if datetime.now() < next_ts:
                        logger.info(f"⏳ Ранний запуск задачи #{task.id}, переназначаем позже")
                        raise Exception("Too early to retry")
                except Exception:
                    pass

            if user_id:
                log_user_info(user_id, f"🔥 Старт сессии прогрева (task #{task.id}) для аккаунта ID={task.account_id}")

            # Обновляем статус задачи
            task.status = WarmupStatus.RUNNING
            task.started_at = datetime.now()
            # Бэкфилл user_id, если отсутствует
            if getattr(task, 'user_id', None) is None:
                try:
                    if settings.get('user_id'):
                        task.user_id = int(settings['user_id'])
                    else:
                        acc = get_instagram_account(task.account_id)
                        task.user_id = getattr(acc, 'user_id', None)
                except Exception:
                    task.user_id = None
            session = get_session()
            session.merge(task)
            session.commit()

            # Риск-aware режим: если риск высокий — пассивный прогрев
            risk = self.predict_monitor.calculate_ban_risk_score(task.account_id)
            health = self.health_monitor.calculate_comprehensive_health_score(task.account_id)
            if risk >= 60 or health < 40:
                settings['force_passive'] = True
                logger.info(f"⚠️ Высокий риск/низкое здоровье (risk={risk}, health={health}) — пассивный режим")

            # Создаем клиент и входим
            client = InstagramClient(task.account_id)
            if not client or not client.check_login():
                raise Exception(f"Не удалось войти в аккаунт {task.account_id}")

            if user_id:
                log_user_info(user_id, f"✅ Вход выполнен, выполняем прогрев аккаунта ID={task.account_id}")

            # Менеджер прогрева
            warmup_manager = WarmupManager(
                account_id=task.account_id,
                client=client,
                warmup_speed=settings.get('warmup_speed', 'NORMAL')
            )
            warmup_manager.set_task_id(task.id)
            warmup_manager.current_phase = settings.get('current_phase', 'phase1')

            # Выполняем сессию
            result = warmup_manager.perform_human_warmup_session(settings)

            # Обновляем прогресс
            progress = task.progress or {}
            progress['sessions_count'] = progress.get('sessions_count', 0) + 1
            progress['current_phase'] = warmup_manager.current_phase
            progress['last_session'] = datetime.now().isoformat()
            progress['last_session_results'] = result
            progress.pop('next_attempt_at', None)
            task.progress = progress
            task.status = WarmupStatus.COMPLETED
            task.completed_at = datetime.now()
            session.merge(task)
            session.commit()

            if user_id:
                actions = result.get('actions_performed', {}) if isinstance(result, dict) else {}
                details = ", ".join([f"{k}={v}" for k, v in actions.items()]) if actions else ""
                log_user_info(user_id, f"🏁 Прогрев завершен (task #{task.id}). Действия: {details}")
            # Обновим updated_at
            task.updated_at = datetime.now()
            session.merge(task)
            session.commit()
        except Exception as e:
            try:
                session.rollback()
            except Exception:
                pass
            logger.error(f"❌ Ошибка при обработке задачи #{task.id}: {e}")
            if user_id:
                log_user_error(user_id, f"❌ Ошибка прогрева (task #{task.id}): {e}")
            # Назначаем backoff 30-90 минут
            try:
                session = get_session()
                task.status = WarmupStatus.FAILED
                progress = task.progress or {}
                delay_min = max(1, WARMUP_BACKOFF_MIN_MINUTES)
                delay_max = max(delay_min, WARMUP_BACKOFF_MAX_MINUTES)
                delay = (delay_min + delay_max) // 2
                next_time = datetime.now() + timedelta(minutes=delay)
                progress['next_attempt_at'] = next_time.isoformat()
                task.progress = progress
                task.error = str(e)
                task.completed_at = datetime.now()
                task.updated_at = datetime.now()
                session.merge(task)
                session.commit()
                session.close()
            except Exception:
                pass
            raise


def start_async_warmup_queue(max_workers: int = DEFAULT_MAX_WORKERS):
    """Запустить асинхронную очередь прогрева"""
    global warmup_executor, warmup_queue_running
    
    if warmup_queue_running:
        logger.info("🔄 Асинхронная очередь прогрева уже запущена")
        return
    
    warmup_executor = AsyncWarmupQueue(max_workers=max_workers)
    warmup_executor.start()
    warmup_queue_running = True
    
    # Запускаем поток для загрузки задач из БД
    threading.Thread(target=_load_tasks_from_db, daemon=True).start()
    

def stop_async_warmup_queue():
    """Остановить асинхронную очередь"""
    global warmup_executor, warmup_queue_running
    
    if warmup_executor:
        warmup_executor.stop()
        warmup_executor = None
    
    warmup_queue_running = False
    

def _load_tasks_from_db():
    """Загружать задачи из БД в очередь"""
    from database.db_manager import get_session
    from database.models import WarmupTask, WarmupStatus
    
    while warmup_queue_running:
        try:
            session = get_session()
            
            # Получаем активные задачи
            tasks = session.query(WarmupTask).filter(
                WarmupTask.status.in_([WarmupStatus.PENDING, WarmupStatus.RUNNING])
            ).all()
            
            # Добавляем в очередь
            for task in tasks:
                if warmup_executor and task.account_id not in warmup_executor.active_accounts:
                    warmup_executor.add_task(task)
            
            session.close()
            
            # Пауза между проверками
            time.sleep(10)
            
        except Exception as e:
            logger.error(f"❌ Ошибка при загрузке задач: {e}")
            time.sleep(30)


def set_max_workers(max_workers: int):
    """Изменить количество потоков"""
    global warmup_executor
    
    if warmup_executor:
        logger.info(f"🔧 Изменение количества потоков с {warmup_executor.max_workers} на {max_workers}")
        # Перезапускаем с новым количеством потоков
        stop_async_warmup_queue()
        time.sleep(2)
        start_async_warmup_queue(max_workers=max_workers)
    else:
        logger.info(f"🔧 Установлено количество потоков: {max_workers}")

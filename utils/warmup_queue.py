#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Обработчик очереди задач прогрева аккаунтов
"""

import logging
import threading
import time
import json
import random
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

# Флаг для отслеживания состояния очереди
warmup_queue_running = False
warmup_thread = None

def start_warmup_queue():
    """Запустить обработчик очереди прогрева"""
    global warmup_queue_running, warmup_thread
    
    if warmup_queue_running:
        logger.info("🔄 Очередь прогрева уже запущена")
        return
    
    logger.info("🚀 Запуск очереди прогрева...")
    warmup_queue_running = True
    
    # Запускаем обработчик в отдельном потоке
    warmup_thread = threading.Thread(target=process_warmup_queue, daemon=True)
    warmup_thread.start()

def stop_warmup_queue():
    """Остановить обработчик очереди прогрева"""
    global warmup_queue_running
    logger.info("🛑 Остановка очереди прогрева...")
    warmup_queue_running = False

def process_warmup_queue():
    """Основной цикл обработки очереди прогрева"""
    from database.db_manager import get_session
    from database.models import WarmupTask, WarmupStatus
    
    while warmup_queue_running:
        try:
            session = get_session()
            
            # Получаем следующую задачу для обработки
            # Берем либо новую (PENDING) либо уже выполняющуюся (RUNNING)
            task = session.query(WarmupTask).filter(
                WarmupTask.status.in_([WarmupStatus.PENDING, WarmupStatus.RUNNING])
            ).order_by(
                # Приоритет: сначала новые, потом давно не обрабатывавшиеся
                WarmupTask.status.desc(),  # RUNNING < PENDING
                WarmupTask.updated_at.asc()  # Старые первые
            ).first()
            
            if task:
                logger.info(f"📋 Обработка задачи прогрева #{task.id} для аккаунта {task.account_id}")
                
                # Обновляем статус на "выполняется"
                task.status = WarmupStatus.RUNNING
                task.started_at = datetime.now()
                session.commit()
                
                try:
                    # Выполняем прогрев
                    process_warmup_task(task, session)
                    
                    # Задача остается в статусе RUNNING для продолжения прогрева
                    # Она будет завершена автоматически когда пройдут все фазы
                    if task.status != WarmupStatus.COMPLETED:
                        logger.info(f"🔄 Задача прогрева #{task.id} продолжается")
                    
                except Exception as e:
                    logger.error(f"❌ Ошибка при выполнении задачи прогрева #{task.id}: {e}")
                    task.status = WarmupStatus.FAILED
                    task.error = str(e)
                    task.completed_at = datetime.now()
                    session.commit()
            
            session.close()
            
            # Пауза между проверками
            time.sleep(10)
            
        except Exception as e:
            logger.error(f"❌ Ошибка в цикле обработки очереди прогрева: {e}")
            time.sleep(30)  # Больше пауза при ошибке
    
    logger.info("🛑 Очередь прогрева остановлена")

def process_warmup_task(task, session):
    """Обработать задачу прогрева"""
    from instagram.client import InstagramClient
    from utils.warmup_manager import WarmupManager
    
    # Загружаем настройки
    settings = json.loads(task.settings)
    
    # Создаем клиент Instagram
    client = InstagramClient(task.account_id)
    
    # Проверяем вход
    if not client.check_login():
        raise Exception(f"Не удалось войти в аккаунт {task.account_id}")
    
    logger.info(f"✅ Успешно вошли в аккаунт для прогрева")
    
    # Получаем скорость прогрева из настроек
    full_settings = settings.get('full_settings', {})
    warmup_speed = full_settings.get('warmup_speed', 'NORMAL')
    
    # Создаем менеджер прогрева
    warmup_manager = WarmupManager(task.account_id, client, warmup_speed)
    
    # Определяем текущую фазу
    start_date = task.created_at or datetime.now()
    current_phase = warmup_manager.get_current_phase(start_date)
    
    if current_phase == 'completed':
        logger.info(f"✨ Прогрев аккаунта завершен!")
        task.status = WarmupStatus.COMPLETED
        task.completed_at = datetime.now()
        session.commit()
        return
    
    logger.info(f"📍 Текущая фаза прогрева: {current_phase}")
    
    # Загружаем прогресс из базы или создаем новый
    progress = task.progress or {
        'current_phase': current_phase,
        'phase_start_date': datetime.now().isoformat(),
        'total_actions': {},
        'daily_actions': {},
        'sessions_count': 0
    }
    
    # Проверяем, не пора ли делать паузу
    current_hour = datetime.now().hour
    if current_hour < 9 or current_hour > 23:
        logger.info(f"😴 Ночное время ({current_hour}:00), откладываем прогрев")
        return
    
    # Выполняем сессию прогрева
    logger.info(f"🚀 Начинаем сессию прогрева #{progress.get('sessions_count', 0) + 1}")
    
    session_results = warmup_manager.perform_human_warmup_session(settings)
    
    # Обновляем прогресс
    progress['sessions_count'] = progress.get('sessions_count', 0) + 1
    progress['current_phase'] = current_phase
    progress['last_session'] = datetime.now().isoformat()
    progress['last_session_results'] = session_results
    
    # Обновляем общую статистику
    for action, count in session_results['actions_performed'].items():
        if action not in progress['total_actions']:
            progress['total_actions'][action] = 0
        progress['total_actions'][action] += count
        
        # Обновляем дневную статистику
        today = datetime.now().strftime('%Y-%m-%d')
        if 'daily_actions' not in progress:
            progress['daily_actions'] = {}
        if today not in progress['daily_actions']:
            progress['daily_actions'][today] = {}
        if action not in progress['daily_actions'][today]:
            progress['daily_actions'][today][action] = 0
        progress['daily_actions'][today][action] += count
    
    # Сохраняем прогресс
    task.progress = progress
    session.commit()
    
    # Логируем результаты сессии
    logger.info(f"📊 Результаты сессии прогрева:")
    logger.info(f"   - Выполнено действий: {session_results['actions_performed']}")
    logger.info(f"   - Длительность сессии: {session_results['session_duration']} сек")
    if session_results['errors']:
        logger.warning(f"   - Ошибки: {session_results['errors']}")
    
    # Планируем следующую сессию
    # Задача останется в статусе RUNNING и будет обработана снова через некоторое время
    logger.info(f"⏰ Следующая сессия прогрева будет выполнена позже") 
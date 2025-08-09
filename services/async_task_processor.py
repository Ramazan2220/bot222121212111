# -*- coding: utf-8 -*-
"""
Асинхронный обработчик задач для параллельной обработки
"""

import asyncio
import logging
import time
from typing import List, Dict, Any, Callable
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

from database.db_manager import get_session, get_publish_task, update_publish_task_status
from database.models import TaskStatus, TaskType
from services.rate_limiter import rate_limiter, ActionType
# from services.instagram_service import instagram_service  # ВРЕМЕННО ОТКЛЮЧЕН

logger = logging.getLogger(__name__)

class AsyncTaskProcessor:
    """Асинхронный обработчик задач"""
    
    def __init__(self, max_workers: int = 5):
        self.max_workers = max_workers
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.running_tasks = {}
        self.task_queue = asyncio.Queue()
        
    async def process_tasks_async(self, task_ids: List[int]) -> Dict[int, bool]:
        """Обработать список задач асинхронно"""
        results = {}
        
        # Добавляем задачи в очередь
        for task_id in task_ids:
            await self.task_queue.put(task_id)
        
        # Создаем воркеры
        workers = [
            asyncio.create_task(self._worker(f"worker-{i}"))
            for i in range(min(len(task_ids), self.max_workers))
        ]
        
        # Ждем завершения всех задач
        await self.task_queue.join()
        
        # Останавливаем воркеры
        for worker in workers:
            worker.cancel()
        
        # Собираем результаты
        for task_id in task_ids:
            results[task_id] = self.running_tasks.get(task_id, {}).get('success', False)
        
        return results
    
    async def _worker(self, name: str):
        """Воркер для обработки задач из очереди"""
        while True:
            try:
                # Получаем задачу из очереди
                task_id = await self.task_queue.get()
                
                logger.info(f"[{name}] Начинаю обработку задачи #{task_id}")
                
                # Обрабатываем задачу в отдельном потоке
                result = await asyncio.get_event_loop().run_in_executor(
                    self.executor,
                    self._process_single_task,
                    task_id
                )
                
                # Сохраняем результат
                self.running_tasks[task_id] = {
                    'success': result,
                    'completed_at': datetime.now()
                }
                
                # Помечаем задачу как выполненную
                self.task_queue.task_done()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[{name}] Ошибка обработки: {e}")
                self.task_queue.task_done()
    
    def _process_single_task(self, task_id: int) -> bool:
        """Обработать одну задачу (синхронно)"""
        try:
            session = get_session()
            task = get_publish_task(task_id)
            
            if not task:
                logger.error(f"Задача #{task_id} не найдена")
                return False
            
            # Проверяем лимиты
            action_type = self._get_action_type(task.task_type)
            can_perform, reason = rate_limiter.can_perform_action(
                task.account_id, 
                action_type
            )
            
            if not can_perform:
                logger.warning(f"Задача #{task_id} отложена: {reason}")
                update_publish_task_status(task_id, TaskStatus.SCHEDULED)
                return False
            
            # Обновляем статус
            update_publish_task_status(task_id, TaskStatus.PROCESSING)
            
            # Имитируем обработку (здесь должна быть реальная публикация)
            time.sleep(2)  # Заменить на реальную публикацию
            
            # Записываем действие
            rate_limiter.record_action(task.account_id, action_type)
            
            # Обновляем статус
            update_publish_task_status(task_id, TaskStatus.COMPLETED)
            
            logger.info(f"✅ Задача #{task_id} выполнена успешно")
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка выполнения задачи #{task_id}: {e}")
            update_publish_task_status(
                task_id, 
                TaskStatus.FAILED,
                error_message=str(e)
            )
            return False
        finally:
            session.close()
    
    def _get_action_type(self, task_type: TaskType) -> ActionType:
        """Преобразовать тип задачи в тип действия"""
        mapping = {
            TaskType.PHOTO: ActionType.POST,
            TaskType.VIDEO: ActionType.POST,
            TaskType.CAROUSEL: ActionType.POST,
            TaskType.STORY: ActionType.STORY,
            TaskType.REEL: ActionType.REEL,
            TaskType.REELS: ActionType.REEL,
            TaskType.IGTV: ActionType.POST,
        }
        return mapping.get(task_type, ActionType.POST)
    
    async def process_batch_with_progress(
        self, 
        task_ids: List[int],
        progress_callback: Callable[[int, int], None] = None
    ) -> Dict[int, bool]:
        """Обработать задачи с отображением прогресса"""
        total = len(task_ids)
        completed = 0
        results = {}
        
        # Создаем задачи
        tasks = []
        for task_id in task_ids:
            task = asyncio.create_task(self._process_with_callback(task_id))
            tasks.append((task_id, task))
        
        # Обрабатываем по мере завершения
        for task_id, task in tasks:
            try:
                result = await task
                results[task_id] = result
                completed += 1
                
                if progress_callback:
                    progress_callback(completed, total)
                    
            except Exception as e:
                logger.error(f"Ошибка задачи {task_id}: {e}")
                results[task_id] = False
        
        return results
    
    async def _process_with_callback(self, task_id: int) -> bool:
        """Обработать задачу асинхронно"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self.executor,
            self._process_single_task,
            task_id
        )

# Глобальный экземпляр
async_processor = AsyncTaskProcessor(max_workers=5)

# Вспомогательная функция для запуска из синхронного кода
def process_tasks_parallel(task_ids: List[int]) -> Dict[int, bool]:
    """Обработать задачи параллельно (для вызова из синхронного кода)"""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(
            async_processor.process_tasks_async(task_ids)
        )
    finally:
        loop.close() 
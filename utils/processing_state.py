#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Система отслеживания состояния обработки пользователей
Обеспечивает восстановление работы после сбоев и перезапусков
"""

import logging
import json
import threading
from datetime import datetime, timedelta
from typing import Dict, Set, Optional, List, Any
from pathlib import Path
from enum import Enum

logger = logging.getLogger(__name__)

class ProcessingStatus(Enum):
    """Статусы обработки пользователя"""
    PENDING = "pending"
    PROCESSING = "processing"  
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"

class ProcessingState:
    """
    Отслеживание прогресса обработки пользователей с возможностью восстановления
    """
    
    def __init__(self, process_name: str = "default", state_file: str = None):
        self.process_name = process_name
        self.state_file = Path(state_file or f"data/processing_state_{process_name}.json")
        
        # Создаем директорию если не существует
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Текущее состояние
        self.current_user_id: Optional[int] = None
        self.processed_users: Set[int] = set()
        self.failed_users: Set[int] = set()
        self.skipped_users: Set[int] = set()
        
        # Детальная статистика
        self.user_processing_times: Dict[int, float] = {}
        self.user_statuses: Dict[int, ProcessingStatus] = {}
        self.user_errors: Dict[int, str] = {}
        
        # Временные метки
        self.cycle_started_at: Optional[datetime] = None
        self.last_full_cycle: Optional[datetime] = None
        self.last_save_time: Optional[datetime] = None
        
        # Блокировка для thread-safety
        self._lock = threading.Lock()
        
        # Загружаем состояние при инициализации
        self.load_state()
    
    def start_cycle(self, total_users: List[int]):
        """
        Начать новый цикл обработки
        
        Args:
            total_users: Список всех пользователей для обработки
        """
        with self._lock:
            self.cycle_started_at = datetime.now()
            
            # Инициализируем статусы для новых пользователей
            for user_id in total_users:
                if user_id not in self.user_statuses:
                    self.user_statuses[user_id] = ProcessingStatus.PENDING
            
            logger.info(f"🔄 {self.process_name}: Начат новый цикл обработки {len(total_users)} пользователей")
            self.save_state()
    
    def start_user_processing(self, user_id: int):
        """
        Отметить начало обработки пользователя
        
        Args:
            user_id: ID пользователя
        """
        with self._lock:
            self.current_user_id = user_id
            self.user_statuses[user_id] = ProcessingStatus.PROCESSING
            self.user_processing_times[user_id] = datetime.now().timestamp()
            
            logger.debug(f"▶️ {self.process_name}: Начинаем обработку пользователя {user_id}")
            self.save_state()
    
    def complete_user_processing(self, user_id: int, success: bool = True, error_message: str = None):
        """
        Отметить завершение обработки пользователя
        
        Args:
            user_id: ID пользователя
            success: Успешность обработки
            error_message: Сообщение об ошибке (если есть)
        """
        with self._lock:
            start_time = self.user_processing_times.get(user_id, datetime.now().timestamp())
            processing_time = datetime.now().timestamp() - start_time
            
            if success:
                self.processed_users.add(user_id)
                self.user_statuses[user_id] = ProcessingStatus.COMPLETED
                logger.debug(f"✅ {self.process_name}: Пользователь {user_id} обработан за {processing_time:.2f}с")
            else:
                self.failed_users.add(user_id)
                self.user_statuses[user_id] = ProcessingStatus.FAILED
                if error_message:
                    self.user_errors[user_id] = error_message
                logger.warning(f"❌ {self.process_name}: Ошибка обработки пользователя {user_id}: {error_message}")
            
            self.user_processing_times[user_id] = processing_time
            
            # Сбрасываем текущего пользователя если это он
            if self.current_user_id == user_id:
                self.current_user_id = None
            
            self.save_state()
    
    def skip_user(self, user_id: int, reason: str = "Системные ограничения"):
        """
        Пропустить обработку пользователя
        
        Args:
            user_id: ID пользователя
            reason: Причина пропуска
        """
        with self._lock:
            self.skipped_users.add(user_id)
            self.user_statuses[user_id] = ProcessingStatus.SKIPPED
            self.user_errors[user_id] = f"Пропущен: {reason}"
            
            logger.info(f"⏭️ {self.process_name}: Пользователь {user_id} пропущен - {reason}")
            self.save_state()
    
    def complete_cycle(self):
        """
        Завершить текущий цикл обработки
        """
        with self._lock:
            self.last_full_cycle = datetime.now()
            self.current_user_id = None
            
            # Статистика цикла
            total_users = len(self.user_statuses)
            completed = len(self.processed_users)
            failed = len(self.failed_users)
            skipped = len(self.skipped_users)
            
            cycle_time = (self.last_full_cycle - self.cycle_started_at).total_seconds() if self.cycle_started_at else 0
            
            logger.info(f"🏁 {self.process_name}: Цикл завершен за {cycle_time:.1f}с. "
                       f"Обработано: {completed}, Ошибки: {failed}, Пропущено: {skipped}, Всего: {total_users}")
            
            self.save_state()
    
    def get_unprocessed_users(self, all_users: List[int]) -> List[int]:
        """
        Получить список необработанных пользователей
        
        Args:
            all_users: Полный список пользователей
            
        Returns:
            List[int]: Список ID пользователей для обработки
        """
        with self._lock:
            unprocessed = []
            
            for user_id in all_users:
                status = self.user_statuses.get(user_id, ProcessingStatus.PENDING)
                
                # Обрабатываем если статус PENDING или FAILED (повторная попытка)
                if status in [ProcessingStatus.PENDING, ProcessingStatus.FAILED]:
                    unprocessed.append(user_id)
                # Также включаем пользователей, которые были в процессе обработки при сбое
                elif status == ProcessingStatus.PROCESSING:
                    logger.warning(f"⚠️ {self.process_name}: Пользователь {user_id} был в процессе обработки при сбое - повторяем")
                    unprocessed.append(user_id)
            
            return unprocessed
    
    def should_retry_failed_users(self, max_retry_hours: int = 24) -> bool:
        """
        Проверить, нужно ли повторять обработку failed пользователей
        
        Args:
            max_retry_hours: Максимальное время для повторных попыток (часы)
            
        Returns:
            bool: True если нужно повторить
        """
        if not self.failed_users:
            return False
        
        if not self.last_full_cycle:
            return True
        
        time_since_last_cycle = datetime.now() - self.last_full_cycle
        return time_since_last_cycle > timedelta(hours=max_retry_hours)
    
    def reset_failed_users(self):
        """
        Сбросить статус failed пользователей для повторной обработки
        """
        with self._lock:
            reset_count = 0
            for user_id in list(self.failed_users):
                self.user_statuses[user_id] = ProcessingStatus.PENDING
                if user_id in self.user_errors:
                    del self.user_errors[user_id]
                reset_count += 1
            
            self.failed_users.clear()
            
            logger.info(f"🔄 {self.process_name}: Сброшен статус {reset_count} пользователей для повторной обработки")
            self.save_state()
    
    def get_progress_stats(self) -> Dict[str, Any]:
        """
        Получить статистику прогресса
        
        Returns:
            Dict: Детальная статистика
        """
        with self._lock:
            total_users = len(self.user_statuses)
            completed = len([u for u, s in self.user_statuses.items() if s == ProcessingStatus.COMPLETED])
            failed = len([u for u, s in self.user_statuses.items() if s == ProcessingStatus.FAILED])
            skipped = len([u for u, s in self.user_statuses.items() if s == ProcessingStatus.SKIPPED])
            processing = len([u for u, s in self.user_statuses.items() if s == ProcessingStatus.PROCESSING])
            pending = len([u for u, s in self.user_statuses.items() if s == ProcessingStatus.PENDING])
            
            # Средняя скорость обработки
            processing_times = [t for t in self.user_processing_times.values() if isinstance(t, float)]
            avg_processing_time = sum(processing_times) / len(processing_times) if processing_times else 0
            
            return {
                "process_name": self.process_name,
                "total_users": total_users,
                "completed": completed,
                "failed": failed,
                "skipped": skipped,
                "processing": processing,
                "pending": pending,
                "current_user_id": self.current_user_id,
                "completion_rate": (completed / total_users * 100) if total_users > 0 else 0,
                "avg_processing_time": avg_processing_time,
                "cycle_started_at": self.cycle_started_at.isoformat() if self.cycle_started_at else None,
                "last_full_cycle": self.last_full_cycle.isoformat() if self.last_full_cycle else None,
                "last_save_time": self.last_save_time.isoformat() if self.last_save_time else None
            }
    
    def save_state(self):
        """
        Сохранить состояние в файл
        """
        try:
            state_data = {
                "process_name": self.process_name,
                "current_user_id": self.current_user_id,
                "processed_users": list(self.processed_users),
                "failed_users": list(self.failed_users),
                "skipped_users": list(self.skipped_users),
                "user_statuses": {str(k): v.value for k, v in self.user_statuses.items()},
                "user_processing_times": {str(k): v for k, v in self.user_processing_times.items()},
                "user_errors": {str(k): v for k, v in self.user_errors.items()},
                "cycle_started_at": self.cycle_started_at.isoformat() if self.cycle_started_at else None,
                "last_full_cycle": self.last_full_cycle.isoformat() if self.last_full_cycle else None,
                "saved_at": datetime.now().isoformat()
            }
            
            with open(self.state_file, 'w', encoding='utf-8') as f:
                json.dump(state_data, f, indent=2, ensure_ascii=False)
            
            self.last_save_time = datetime.now()
            logger.debug(f"💾 {self.process_name}: Состояние сохранено в {self.state_file}")
            
        except Exception as e:
            logger.error(f"❌ {self.process_name}: Ошибка сохранения состояния: {e}")
    
    def load_state(self) -> bool:
        """
        Загрузить состояние из файла
        
        Returns:
            bool: True если состояние успешно загружено
        """
        try:
            if not self.state_file.exists():
                logger.debug(f"📁 {self.process_name}: Файл состояния не существует - первый запуск")
                return False
            
            with open(self.state_file, 'r', encoding='utf-8') as f:
                state_data = json.load(f)
            
            self.current_user_id = state_data.get('current_user_id')
            self.processed_users = set(state_data.get('processed_users', []))
            self.failed_users = set(state_data.get('failed_users', []))
            self.skipped_users = set(state_data.get('skipped_users', []))
            
            # Восстанавливаем статусы
            user_statuses_raw = state_data.get('user_statuses', {})
            self.user_statuses = {int(k): ProcessingStatus(v) for k, v in user_statuses_raw.items()}
            
            # Восстанавливаем времена и ошибки
            user_times_raw = state_data.get('user_processing_times', {})
            self.user_processing_times = {int(k): v for k, v in user_times_raw.items()}
            
            user_errors_raw = state_data.get('user_errors', {})
            self.user_errors = {int(k): v for k, v in user_errors_raw.items()}
            
            # Восстанавливаем временные метки
            cycle_started_str = state_data.get('cycle_started_at')
            if cycle_started_str:
                self.cycle_started_at = datetime.fromisoformat(cycle_started_str)
            
            last_cycle_str = state_data.get('last_full_cycle')
            if last_cycle_str:
                self.last_full_cycle = datetime.fromisoformat(last_cycle_str)
            
            total_users = len(self.user_statuses)
            completed = len(self.processed_users)
            failed = len(self.failed_users)
            
            logger.info(f"📂 {self.process_name}: Состояние загружено. "
                       f"Всего: {total_users}, Завершено: {completed}, Ошибки: {failed}")
            
            # Если было прерывание во время обработки
            if self.current_user_id:
                logger.warning(f"⚠️ {self.process_name}: Обнаружено прерывание обработки пользователя {self.current_user_id}")
                self.user_statuses[self.current_user_id] = ProcessingStatus.PENDING  # Сбрасываем для повторной обработки
                self.current_user_id = None
                self.save_state()
            
            return True
            
        except Exception as e:
            logger.error(f"❌ {self.process_name}: Ошибка загрузки состояния: {e}")
            return False
    
    def cleanup_old_data(self, keep_days: int = 7):
        """
        Очистить старые данные
        
        Args:
            keep_days: Количество дней для хранения данных
        """
        with self._lock:
            cutoff_time = datetime.now() - timedelta(days=keep_days)
            
            # Очищаем старые ошибки
            old_errors = {k: v for k, v in self.user_errors.items() 
                         if self.user_statuses.get(k) == ProcessingStatus.COMPLETED}
            
            for user_id in old_errors:
                del self.user_errors[user_id]
            
            # Очищаем старые времена обработки для завершенных пользователей
            completed_users = [k for k, v in self.user_statuses.items() 
                             if v == ProcessingStatus.COMPLETED]
            
            for user_id in completed_users:
                if user_id in self.user_processing_times:
                    del self.user_processing_times[user_id]
            
            logger.info(f"🧹 {self.process_name}: Очищены старые данные ({len(old_errors)} ошибок, {len(completed_users)} времен)")
            self.save_state()

def health_check_processing_states() -> Dict[str, Any]:
    """
    Проверка состояния всех процессов обработки
    
    Returns:
        Dict: Статус всех процессов
    """
    state_dir = Path("data")
    health_status = {
        "timestamp": datetime.now().isoformat(),
        "processes": {},
        "summary": {
            "total_processes": 0,
            "active_processes": 0,
            "failed_processes": 0
        }
    }
    
    try:
        # Находим все файлы состояний
        if state_dir.exists():
            state_files = list(state_dir.glob("processing_state_*.json"))
            
            for state_file in state_files:
                process_name = state_file.stem.replace("processing_state_", "")
                
                try:
                    processing_state = ProcessingState(process_name)
                    stats = processing_state.get_progress_stats()
                    
                    health_status["processes"][process_name] = stats
                    health_status["summary"]["total_processes"] += 1
                    
                    if stats["processing"] > 0:
                        health_status["summary"]["active_processes"] += 1
                    
                    if stats["failed"] > 0:
                        health_status["summary"]["failed_processes"] += 1
                        
                except Exception as e:
                    health_status["processes"][process_name] = {"error": str(e)}
        
        return health_status
        
    except Exception as e:
        return {
            "timestamp": datetime.now().isoformat(),
            "error": str(e),
            "processes": {}
        } 
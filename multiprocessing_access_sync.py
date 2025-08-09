#!/usr/bin/env python3
"""
СИСТЕМА СИНХРОНИЗАЦИИ НА MULTIPROCESSING
Event-driven синхронизация без внешних зависимостей
"""

import multiprocessing as mp
import threading
import time
import json
import os
from typing import Dict, Optional
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class MultiprocessingAccessSync:
    """
    Система синхронизации через multiprocessing.Manager
    """
    
    def __init__(self):
        # Создаем Manager для shared objects
        self.manager = mp.Manager()
        
        # Shared данные
        self.shared_users = self.manager.dict()  # Общие данные пользователей
        self.user_events = self.manager.dict()   # События для каждого пользователя
        
        # События для синхронизации
        self.user_added_event = mp.Event()
        self.user_removed_event = mp.Event()
        
        # Локальный кеш для быстрого доступа
        self._local_cache = {}
        self._cache_lock = threading.Lock()
        
        # Поток мониторинга событий
        self._monitor_thread = None
        self._stop_monitoring = False
        
    def start_monitoring(self):
        """Запускает мониторинг событий"""
        if self._monitor_thread and self._monitor_thread.is_alive():
            return
            
        self._stop_monitoring = False
        self._monitor_thread = threading.Thread(target=self._monitor_events, daemon=True)
        self._monitor_thread.start()
        
        logger.info("🔄 Multiprocessing monitor запущен")
        
    def stop_monitoring(self):
        """Останавливает мониторинг"""
        self._stop_monitoring = True
        if self._monitor_thread:
            self._monitor_thread.join(timeout=1)
            
    def _monitor_events(self):
        """Мониторит события в отдельном потоке"""
        while not self._stop_monitoring:
            try:
                # Проверяем события добавления
                if self.user_added_event.wait(timeout=0.1):
                    self._sync_from_shared()
                    self.user_added_event.clear()
                
                # Проверяем события удаления
                if self.user_removed_event.wait(timeout=0.1):
                    self._sync_from_shared()
                    self.user_removed_event.clear()
                    
            except Exception as e:
                logger.error(f"Ошибка мониторинга событий: {e}")
                time.sleep(0.1)
                
    def _sync_from_shared(self):
        """Синхронизирует локальный кеш с shared данными"""
        try:
            with self._cache_lock:
                # Копируем все данные из shared в локальный кеш
                self._local_cache.clear()
                for user_id, user_data in self.shared_users.items():
                    self._local_cache[user_id] = user_data
                    
            logger.info(f"🔄 Локальный кеш синхронизирован: {len(self._local_cache)} пользователей")
            
        except Exception as e:
            logger.error(f"Ошибка синхронизации: {e}")
    
    def add_user(self, user_id: int, user_data: dict):
        """Добавляет пользователя"""
        try:
            user_key = str(user_id)
            
            # Добавляем в shared данные
            self.shared_users[user_key] = user_data
            
            # Обновляем локальный кеш
            with self._cache_lock:
                self._local_cache[user_key] = user_data
            
            # Уведомляем о событии
            self.user_added_event.set()
            
            logger.info(f"✅ Пользователь {user_id} добавлен")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка добавления пользователя {user_id}: {e}")
            return False
    
    def remove_user(self, user_id: int):
        """Удаляет пользователя"""
        try:
            user_key = str(user_id)
            
            # Удаляем из shared данных
            if user_key in self.shared_users:
                del self.shared_users[user_key]
            
            # Удаляем из локального кеша
            with self._cache_lock:
                self._local_cache.pop(user_key, None)
            
            # Уведомляем о событии
            self.user_removed_event.set()
            
            logger.info(f"✅ Пользователь {user_id} удален")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка удаления пользователя {user_id}: {e}")
            return False
    
    def has_access(self, user_id: int) -> bool:
        """Проверяет доступ пользователя"""
        user_key = str(user_id)
        
        # Сначала проверяем локальный кеш
        with self._cache_lock:
            if user_key in self._local_cache:
                user_data = self._local_cache[user_key]
                return self._check_user_active(user_data)
        
        # Если нет в локальном кеше, проверяем shared данные
        try:
            if user_key in self.shared_users:
                user_data = self.shared_users[user_key]
                
                # Обновляем локальный кеш
                with self._cache_lock:
                    self._local_cache[user_key] = user_data
                
                return self._check_user_active(user_data)
                
        except Exception as e:
            logger.error(f"Ошибка проверки доступа для {user_id}: {e}")
        
        return False
    
    def _check_user_active(self, user_data: dict) -> bool:
        """Проверяет активность пользователя"""
        if not user_data.get('is_active', False):
            return False
            
        # Проверяем срок подписки
        subscription_end = user_data.get('subscription_end')
        if subscription_end:
            try:
                end_date = datetime.fromisoformat(subscription_end)
                if datetime.now() > end_date:
                    return False
            except:
                pass
                
        return True
    
    def get_stats(self) -> dict:
        """Возвращает статистику"""
        with self._cache_lock:
            local_count = len(self._local_cache)
            
        try:
            shared_count = len(self.shared_users)
        except:
            shared_count = 0
            
        return {
            'local_cache_users': local_count,
            'shared_users': shared_count,
            'monitor_active': self._monitor_thread and self._monitor_thread.is_alive()
        }

# Глобальный экземпляр
_mp_sync = None

def get_mp_sync() -> MultiprocessingAccessSync:
    """Получает глобальный экземпляр multiprocessing синхронизации"""
    global _mp_sync
    if _mp_sync is None:
        _mp_sync = MultiprocessingAccessSync()
        _mp_sync.start_monitoring()
    return _mp_sync

def has_access_mp(user_id: int) -> bool:
    """Проверяет доступ через multiprocessing"""
    return get_mp_sync().has_access(user_id)

def add_user_mp(user_id: int, user_data: dict) -> bool:
    """Добавляет пользователя через multiprocessing"""
    return get_mp_sync().add_user(user_id, user_data)

def remove_user_mp(user_id: int) -> bool:
    """Удаляет пользователя через multiprocessing"""
    return get_mp_sync().remove_user(user_id) 
"""
СИСТЕМА СИНХРОНИЗАЦИИ НА MULTIPROCESSING
Event-driven синхронизация без внешних зависимостей
"""

import multiprocessing as mp
import threading
import time
import json
import os
from typing import Dict, Optional
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class MultiprocessingAccessSync:
    """
    Система синхронизации через multiprocessing.Manager
    """
    
    def __init__(self):
        # Создаем Manager для shared objects
        self.manager = mp.Manager()
        
        # Shared данные
        self.shared_users = self.manager.dict()  # Общие данные пользователей
        self.user_events = self.manager.dict()   # События для каждого пользователя
        
        # События для синхронизации
        self.user_added_event = mp.Event()
        self.user_removed_event = mp.Event()
        
        # Локальный кеш для быстрого доступа
        self._local_cache = {}
        self._cache_lock = threading.Lock()
        
        # Поток мониторинга событий
        self._monitor_thread = None
        self._stop_monitoring = False
        
    def start_monitoring(self):
        """Запускает мониторинг событий"""
        if self._monitor_thread and self._monitor_thread.is_alive():
            return
            
        self._stop_monitoring = False
        self._monitor_thread = threading.Thread(target=self._monitor_events, daemon=True)
        self._monitor_thread.start()
        
        logger.info("🔄 Multiprocessing monitor запущен")
        
    def stop_monitoring(self):
        """Останавливает мониторинг"""
        self._stop_monitoring = True
        if self._monitor_thread:
            self._monitor_thread.join(timeout=1)
            
    def _monitor_events(self):
        """Мониторит события в отдельном потоке"""
        while not self._stop_monitoring:
            try:
                # Проверяем события добавления
                if self.user_added_event.wait(timeout=0.1):
                    self._sync_from_shared()
                    self.user_added_event.clear()
                
                # Проверяем события удаления
                if self.user_removed_event.wait(timeout=0.1):
                    self._sync_from_shared()
                    self.user_removed_event.clear()
                    
            except Exception as e:
                logger.error(f"Ошибка мониторинга событий: {e}")
                time.sleep(0.1)
                
    def _sync_from_shared(self):
        """Синхронизирует локальный кеш с shared данными"""
        try:
            with self._cache_lock:
                # Копируем все данные из shared в локальный кеш
                self._local_cache.clear()
                for user_id, user_data in self.shared_users.items():
                    self._local_cache[user_id] = user_data
                    
            logger.info(f"🔄 Локальный кеш синхронизирован: {len(self._local_cache)} пользователей")
            
        except Exception as e:
            logger.error(f"Ошибка синхронизации: {e}")
    
    def add_user(self, user_id: int, user_data: dict):
        """Добавляет пользователя"""
        try:
            user_key = str(user_id)
            
            # Добавляем в shared данные
            self.shared_users[user_key] = user_data
            
            # Обновляем локальный кеш
            with self._cache_lock:
                self._local_cache[user_key] = user_data
            
            # Уведомляем о событии
            self.user_added_event.set()
            
            logger.info(f"✅ Пользователь {user_id} добавлен")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка добавления пользователя {user_id}: {e}")
            return False
    
    def remove_user(self, user_id: int):
        """Удаляет пользователя"""
        try:
            user_key = str(user_id)
            
            # Удаляем из shared данных
            if user_key in self.shared_users:
                del self.shared_users[user_key]
            
            # Удаляем из локального кеша
            with self._cache_lock:
                self._local_cache.pop(user_key, None)
            
            # Уведомляем о событии
            self.user_removed_event.set()
            
            logger.info(f"✅ Пользователь {user_id} удален")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка удаления пользователя {user_id}: {e}")
            return False
    
    def has_access(self, user_id: int) -> bool:
        """Проверяет доступ пользователя"""
        user_key = str(user_id)
        
        # Сначала проверяем локальный кеш
        with self._cache_lock:
            if user_key in self._local_cache:
                user_data = self._local_cache[user_key]
                return self._check_user_active(user_data)
        
        # Если нет в локальном кеше, проверяем shared данные
        try:
            if user_key in self.shared_users:
                user_data = self.shared_users[user_key]
                
                # Обновляем локальный кеш
                with self._cache_lock:
                    self._local_cache[user_key] = user_data
                
                return self._check_user_active(user_data)
                
        except Exception as e:
            logger.error(f"Ошибка проверки доступа для {user_id}: {e}")
        
        return False
    
    def _check_user_active(self, user_data: dict) -> bool:
        """Проверяет активность пользователя"""
        if not user_data.get('is_active', False):
            return False
            
        # Проверяем срок подписки
        subscription_end = user_data.get('subscription_end')
        if subscription_end:
            try:
                end_date = datetime.fromisoformat(subscription_end)
                if datetime.now() > end_date:
                    return False
            except:
                pass
                
        return True
    
    def get_stats(self) -> dict:
        """Возвращает статистику"""
        with self._cache_lock:
            local_count = len(self._local_cache)
            
        try:
            shared_count = len(self.shared_users)
        except:
            shared_count = 0
            
        return {
            'local_cache_users': local_count,
            'shared_users': shared_count,
            'monitor_active': self._monitor_thread and self._monitor_thread.is_alive()
        }

# Глобальный экземпляр
_mp_sync = None

def get_mp_sync() -> MultiprocessingAccessSync:
    """Получает глобальный экземпляр multiprocessing синхронизации"""
    global _mp_sync
    if _mp_sync is None:
        _mp_sync = MultiprocessingAccessSync()
        _mp_sync.start_monitoring()
    return _mp_sync

def has_access_mp(user_id: int) -> bool:
    """Проверяет доступ через multiprocessing"""
    return get_mp_sync().has_access(user_id)

def add_user_mp(user_id: int, user_data: dict) -> bool:
    """Добавляет пользователя через multiprocessing"""
    return get_mp_sync().add_user(user_id, user_data)

def remove_user_mp(user_id: int) -> bool:
    """Удаляет пользователя через multiprocessing"""
    return get_mp_sync().remove_user(user_id) 
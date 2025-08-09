#!/usr/bin/env python3
"""
ФАЙЛОВАЯ СИСТЕМА СИНХРОНИЗАЦИИ ДОСТУПА
Простая и надежная синхронизация между админ ботом и основным ботом
"""

import json
import time
import threading
import os
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Optional
import logging

logger = logging.getLogger(__name__)

class FileAccessSync:
    def __init__(self, sync_file='data/sync_access.json'):
        self.sync_file = Path(sync_file)
        self.sync_file.parent.mkdir(exist_ok=True)
        self._lock = threading.Lock()
        self._local_cache = {}
        self._last_check = 0
        self.CHECK_INTERVAL = 1  # Проверяем каждую секунду
        
        # Инициализируем файл если его нет
        if not self.sync_file.exists():
            self._save_to_file({})
            
        print("🟢 Файловая система синхронизации инициализирована")
    
    def _load_from_file(self) -> Dict:
        """Загружает данные из файла"""
        try:
            with open(self.sync_file, 'r') as f:
                data = json.load(f)
                return data.get('users', {})
        except (FileNotFoundError, json.JSONDecodeError):
            return {}
    
    def _save_to_file(self, users: Dict):
        """Сохраняет данные в файл"""
        try:
            data = {
                'users': users,
                'last_update': time.time(),
                'updated_by': os.getpid()
            }
            
            # Атомарная запись
            temp_file = self.sync_file.with_suffix('.tmp')
            with open(temp_file, 'w') as f:
                json.dump(data, f, indent=2)
            temp_file.replace(self.sync_file)
            
        except Exception as e:
            logger.error(f"Ошибка сохранения в файл: {e}")
    
    def _refresh_cache(self):
        """Обновляет локальный кеш из файла"""
        current_time = time.time()
        
        # Проверяем только раз в секунду
        if current_time - self._last_check < self.CHECK_INTERVAL:
            return
            
        with self._lock:
            try:
                file_users = self._load_from_file()
                
                # Обновляем кеш только если данные изменились
                if file_users != self._local_cache:
                    self._local_cache = file_users.copy()
                    logger.info(f"🔄 Кеш обновлен: {len(self._local_cache)} пользователей")
                
                self._last_check = current_time
                
            except Exception as e:
                logger.error(f"Ошибка обновления кеша: {e}")
    
    def add_user(self, user_id: int, user_data: dict = None) -> bool:
        """Добавляет пользователя"""
        try:
            with self._lock:
                # Загружаем актуальные данные
                users = self._load_from_file()
                
                # Добавляем пользователя
                if user_data is None:
                    user_data = {
                        'telegram_id': user_id,
                        'is_active': True,
                        'subscription_end': (datetime.now() + timedelta(days=30)).isoformat(),
                        'role': 'trial',
                        'added_at': datetime.now().isoformat()
                    }
                
                users[str(user_id)] = user_data
                
                # Сохраняем в файл
                self._save_to_file(users)
                
                # Обновляем локальный кеш
                self._local_cache = users.copy()
                
                logger.info(f"✅ Пользователь {user_id} добавлен")
                return True
                
        except Exception as e:
            logger.error(f"Ошибка добавления пользователя {user_id}: {e}")
            return False
    
    def remove_user(self, user_id: int) -> bool:
        """Удаляет пользователя"""
        try:
            with self._lock:
                # Загружаем актуальные данные
                users = self._load_from_file()
                
                # Удаляем пользователя
                user_key = str(user_id)
                if user_key in users:
                    del users[user_key]
                    
                    # Сохраняем в файл
                    self._save_to_file(users)
                    
                    # Обновляем локальный кеш
                    self._local_cache = users.copy()
                    
                    logger.info(f"🗑️ Пользователь {user_id} удален")
                    return True
                else:
                    logger.warning(f"⚠️ Пользователь {user_id} не найден для удаления")
                    return False
                
        except Exception as e:
            logger.error(f"Ошибка удаления пользователя {user_id}: {e}")
            return False
    
    def has_access(self, user_id: int) -> bool:
        """Проверяет доступ пользователя"""
        # Обновляем кеш
        self._refresh_cache()
        
        user_key = str(user_id)
        if user_key not in self._local_cache:
            return False
        
        user_data = self._local_cache[user_key]
        
        # Проверяем активность
        if not user_data.get('is_active', False):
            return False
        
        # Проверяем подписку
        subscription_end = user_data.get('subscription_end')
        if subscription_end:
            try:
                end_date = datetime.fromisoformat(subscription_end)
                if datetime.now() > end_date:
                    return False
            except ValueError:
                pass  # Игнорируем ошибки парсинга даты
        
        return True
    
    def get_stats(self) -> Dict:
        """Возвращает статистику"""
        self._refresh_cache()
        
        total_users = len(self._local_cache)
        active_users = sum(1 for user in self._local_cache.values() 
                          if user.get('is_active', False))
        
        return {
            'total_users': total_users,
            'active_users': active_users,
            'sync_file': str(self.sync_file),
            'last_check': self._last_check,
            'cache_size': len(self._local_cache)
        }

# Глобальный экземпляр
_file_sync = None

def get_file_sync():
    """Возвращает глобальный экземпляр"""
    global _file_sync
    if _file_sync is None:
        _file_sync = FileAccessSync()
    return _file_sync

# Простые функции для использования
def has_access_file(user_id: int) -> bool:
    return get_file_sync().has_access(user_id)

def add_user_file(user_id: int, user_data: dict = None) -> bool:
    return get_file_sync().add_user(user_id, user_data)

def remove_user_file(user_id: int) -> bool:
    return get_file_sync().remove_user(user_id)

def get_sync_stats() -> Dict:
    return get_file_sync().get_stats() 
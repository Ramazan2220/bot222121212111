#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Система кеширования пользователей с fallback механизмами
Обеспечивает надежное получение списков пользователей даже при сбоях БД
"""

import logging
import json
import time
import threading
from datetime import datetime, timedelta
from typing import List, Tuple, Optional, Dict, Any
from pathlib import Path

from database.user_management import get_active_users, get_users_by_priority, get_user_info
from utils.system_monitor import SystemResourceMonitor
try:
    from utils.redis_cache import cache_json
    _HAS_REDIS_CACHE = True
except Exception:
    _HAS_REDIS_CACHE = False

logger = logging.getLogger(__name__)

class UserCache:
    """
    Система кеширования пользователей с автоматическим fallback
    """
    
    def __init__(self, cache_ttl: int = 3600, cache_file: str = "data/user_cache.json"):
        self.users_cache: List[int] = []
        self.priority_cache: List[Tuple[int, str]] = []
        self.user_info_cache: Dict[int, Dict] = {}
        
        self.last_update: Optional[datetime] = None
        self.cache_ttl = cache_ttl  # TTL в секундах (по умолчанию 1 час)
        self.cache_file = Path(cache_file)
        
        # Создаем директорию если не существует
        self.cache_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Статистика
        self.stats = {
            "cache_hits": 0,
            "cache_misses": 0,
            "fallback_used": 0,
            "errors": 0
        }
        
        # Блокировка для thread-safety
        self._lock = threading.Lock()
        
        # Загружаем кеш при инициализации
        self.load_cache_from_file()
    
    def get_active_users_safe(self) -> List[int]:
        """
        Безопасное получение активных пользователей с fallback
        
        Returns:
            List[int]: Список user_id активных пользователей
        """
        with self._lock:
            try:
                # Попытка получить из Redis-кэша, если доступен
                if _HAS_REDIS_CACHE:
                    def _producer_db():
                        users = get_active_users()
                        if users:
                            self.users_cache = users
                            self.last_update = datetime.now()
                            self.save_cache_to_file()
                        return users or []
                    users = cache_json("cache:active_users", _producer_db, ttl_seconds=300)
                    if users:
                        self.stats["cache_hits"] += 1
                        return list(map(int, users))

                # Пытаемся получить свежий список если кеш устарел
                if not self.is_cache_valid() or not self.users_cache:
                    logger.debug("🔄 Обновляем кеш пользователей...")
                    fresh_users = get_active_users()
                    
                    if fresh_users:
                        self.users_cache = fresh_users
                        self.last_update = datetime.now()
                        self.save_cache_to_file()
                        self.stats["cache_misses"] += 1
                        logger.info(f"✅ Кеш пользователей обновлен: {len(fresh_users)} пользователей")
                        return fresh_users
                    else:
                        logger.warning("⚠️ Получен пустой список пользователей из БД")
                
                # Используем кеш если он валиден
                if self.users_cache:
                    self.stats["cache_hits"] += 1
                    logger.debug(f"📋 Используем кеш: {len(self.users_cache)} пользователей")
                    return self.users_cache.copy()
                
                # Если кеш пуст, пытаемся загрузить из файла
                if self.load_cache_from_file():
                    self.stats["fallback_used"] += 1
                    logger.warning(f"🔄 Используем fallback кеш из файла: {len(self.users_cache)} пользователей")
                    return self.users_cache.copy()
                
                # Последний резерв - возвращаем пустой список
                logger.error("❌ Не удалось получить список пользователей ни из БД, ни из кеша")
                self.stats["errors"] += 1
                return []
                
            except Exception as e:
                logger.error(f"❌ Критическая ошибка в get_active_users_safe: {e}")
                self.stats["errors"] += 1
                
                # Пытаемся использовать кеш
                if self.users_cache:
                    self.stats["fallback_used"] += 1
                    logger.warning(f"🔄 Используем устаревший кеш из-за ошибки: {len(self.users_cache)} пользователей")
                    return self.users_cache.copy()
                
                return []
    
    def get_users_by_priority_safe(self) -> List[Tuple[int, str]]:
        """
        Безопасное получение пользователей с приоритетами
        
        Returns:
            List[Tuple[int, str]]: Список (user_id, priority)
        """
        with self._lock:
            try:
                # Пытаемся получить свежий список
                if not self.is_cache_valid() or not self.priority_cache:
                    fresh_priority_users = get_users_by_priority()
                    
                    if fresh_priority_users:
                        self.priority_cache = fresh_priority_users
                        self.last_update = datetime.now()
                        self.save_cache_to_file()
                        logger.info(f"✅ Кеш приоритетов обновлен: {len(fresh_priority_users)} пользователей")
                        return fresh_priority_users
                
                # Используем кеш
                if self.priority_cache:
                    return self.priority_cache.copy()
                
                # Fallback - создаем приоритеты из обычного списка
                users = self.get_active_users_safe()
                fallback_priority = [(user_id, "UNKNOWN") for user_id in users]
                logger.warning(f"🔄 Fallback: создаем приоритеты для {len(fallback_priority)} пользователей")
                return fallback_priority
                
            except Exception as e:
                logger.error(f"❌ Ошибка получения пользователей с приоритетами: {e}")
                
                # Fallback на обычный список
                users = self.get_active_users_safe()
                return [(user_id, "ERROR") for user_id in users]
    
    def get_user_info_safe(self, user_id: int) -> Dict[str, Any]:
        """
        Безопасное получение информации о пользователе
        
        Args:
            user_id: ID пользователя
            
        Returns:
            Dict: Информация о пользователе
        """
        with self._lock:
            try:
                # Проверяем кеш
                if user_id in self.user_info_cache:
                    cached_info = self.user_info_cache[user_id]
                    # Проверяем возраст кеша (1 час для информации о пользователе)
                    if 'cached_at' in cached_info:
                        cached_at = datetime.fromisoformat(cached_info['cached_at'])
                        if (datetime.now() - cached_at).seconds < 3600:
                            return cached_info.copy()
                
                # Получаем свежую информацию
                user_info = get_user_info(user_id)
                
                if user_info and 'error' not in user_info:
                    # Добавляем метку времени кеширования
                    user_info['cached_at'] = datetime.now().isoformat()
                    self.user_info_cache[user_id] = user_info
                    self.save_cache_to_file()
                    return user_info
                
                # Возвращаем базовую информацию
                return {
                    "user_id": user_id,
                    "accounts_count": 0,
                    "error": "Информация недоступна"
                }
                
            except Exception as e:
                logger.error(f"❌ Ошибка получения информации о пользователе {user_id}: {e}")
                return {
                    "user_id": user_id,
                    "accounts_count": 0,
                    "error": str(e)
                }
    
    def is_cache_valid(self) -> bool:
        """
        Проверить валидность кеша
        
        Returns:
            bool: True если кеш актуален
        """
        if not self.last_update:
            return False
        
        age_seconds = (datetime.now() - self.last_update).total_seconds()
        return age_seconds < self.cache_ttl
    
    def save_cache_to_file(self):
        """
        Сохранить кеш в файл
        """
        try:
            cache_data = {
                "users_cache": self.users_cache,
                "priority_cache": self.priority_cache,
                "user_info_cache": self.user_info_cache,
                "last_update": self.last_update.isoformat() if self.last_update else None,
                "stats": self.stats,
                "saved_at": datetime.now().isoformat()
            }
            
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, indent=2, ensure_ascii=False, default=str)
            
            logger.debug(f"💾 Кеш сохранен в {self.cache_file}")
            
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения кеша: {e}")
    
    def load_cache_from_file(self) -> bool:
        """
        Загрузить кеш из файла
        
        Returns:
            bool: True если кеш успешно загружен
        """
        try:
            if not self.cache_file.exists():
                logger.debug("📁 Файл кеша не существует - первый запуск")
                return False
            
            with open(self.cache_file, 'r', encoding='utf-8') as f:
                cache_data = json.load(f)
            
            self.users_cache = cache_data.get('users_cache', [])
            self.priority_cache = cache_data.get('priority_cache', [])
            self.user_info_cache = cache_data.get('user_info_cache', {})
            self.stats.update(cache_data.get('stats', {}))
            
            last_update_str = cache_data.get('last_update')
            if last_update_str:
                self.last_update = datetime.fromisoformat(last_update_str)
            
            logger.info(f"📂 Кеш загружен из файла: {len(self.users_cache)} пользователей")
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки кеша из файла: {e}")
            return False
    
    def force_refresh(self) -> bool:
        """
        Принудительное обновление кеша
        
        Returns:
            bool: True если обновление прошло успешно
        """
        with self._lock:
            try:
                logger.info("🔄 Принудительное обновление кеша пользователей...")
                
                # Очищаем кеши
                self.users_cache = []
                self.priority_cache = []
                self.user_info_cache = {}
                self.last_update = None
                
                # Получаем свежие данные напрямую из БД
                fresh_users = get_active_users()
                fresh_priority_users = get_users_by_priority()
                
                if fresh_users:
                    self.users_cache = fresh_users
                    self.priority_cache = fresh_priority_users
                    self.last_update = datetime.now()
                    self.save_cache_to_file()
                    logger.info(f"✅ Кеш принудительно обновлен: {len(fresh_users)} пользователей")
                    return True
                else:
                    logger.warning("⚠️ Принудительное обновление не дало результатов")
                    return False
                    
            except Exception as e:
                logger.error(f"❌ Ошибка принудительного обновления кеша: {e}")
                return False
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """
        Получить статистику кеша
        
        Returns:
            Dict: Статистика использования кеша
        """
        return {
            "stats": self.stats.copy(),
            "cache_size": len(self.users_cache),
            "priority_cache_size": len(self.priority_cache),
            "user_info_cache_size": len(self.user_info_cache),
            "last_update": self.last_update.isoformat() if self.last_update else None,
            "cache_valid": self.is_cache_valid(),
            "cache_age_minutes": (datetime.now() - self.last_update).total_seconds() / 60 if self.last_update else None
        }
    
    def clear_cache(self):
        """
        Очистить весь кеш
        """
        with self._lock:
            self.users_cache = []
            self.priority_cache = []
            self.user_info_cache = {}
            self.last_update = None
            
            # Обнуляем статистику
            self.stats = {
                "cache_hits": 0,
                "cache_misses": 0,
                "fallback_used": 0,
                "errors": 0
            }
            
            logger.info("🗑️ Кеш пользователей очищен")

# Глобальный экземпляр кеша
_global_user_cache = None

def get_user_cache() -> UserCache:
    """
    Получить глобальный экземпляр кеша пользователей
    
    Returns:
        UserCache: Глобальный кеш пользователей
    """
    global _global_user_cache
    
    if _global_user_cache is None:
        _global_user_cache = UserCache()
    
    return _global_user_cache

def process_users_with_limits(processor_func, max_users_per_cycle: int = 10, 
                             respect_system_load: bool = True):
    """
    Обработка пользователей с ограничениями по нагрузке
    
    Args:
        processor_func: Функция для обработки пользователя (принимает user_id)
        max_users_per_cycle: Максимум пользователей за цикл
        respect_system_load: Учитывать системную нагрузку
    """
    user_cache = get_user_cache()
    users = user_cache.get_active_users_safe()
    
    if not users:
        logger.warning("⚠️ Нет пользователей для обработки")
        return
    
    system_monitor = SystemResourceMonitor() if respect_system_load else None
    processed_count = 0
    
    logger.info(f"🔄 Начинаем обработку {len(users)} пользователей (макс. {max_users_per_cycle} за цикл)")
    
    for i in range(0, len(users), max_users_per_cycle):
        batch = users[i:i + max_users_per_cycle]
        
        for user_id in batch:
            try:
                # Проверяем системную нагрузку
                if system_monitor and respect_system_load:
                    load_level = system_monitor.get_load_level()
                    if load_level and hasattr(load_level, 'max_threads') and load_level.max_threads <= 1:
                        logger.warning(f"🛑 Останавливаем обработку на пользователе {user_id} - система перегружена")
                        break
                
                # Обрабатываем пользователя
                processor_func(user_id)
                processed_count += 1
                
                # Пауза между пользователями
                time.sleep(0.1)
                
            except Exception as e:
                logger.error(f"❌ Ошибка обработки пользователя {user_id}: {e}")
                continue
        
        # Пауза между батчами
        if i + max_users_per_cycle < len(users):
            time.sleep(0.5)
    
    logger.info(f"✅ Обработка завершена: {processed_count}/{len(users)} пользователей") 
#!/usr/bin/env python3
"""
СОВРЕМЕННАЯ СИСТЕМА СИНХРОНИЗАЦИИ ДОСТУПОВ
Redis Pub/Sub для real-time синхронизации между админ ботом и основным ботом
"""

import json
import threading
import time
from typing import Dict, Optional
from datetime import datetime
import logging
from config import REDIS_USE_FAKE, REDIS_URL, REDIS_HOST, REDIS_PORT, REDIS_DB, REDIS_PASSWORD

# Используем FakeRedis для разработки по умолчанию, но можно включить реальный Redis через ENV
USE_FAKE_REDIS = REDIS_USE_FAKE

if USE_FAKE_REDIS:
    from fake_redis import get_fake_redis
    print("🟡 FakeRedis активирован (ENV)")
else:
    try:
        import redis as real_redis
        print("🟢 Используется настоящий Redis")
    except ImportError:
        from fake_redis import get_fake_redis
        USE_FAKE_REDIS = True
        print("🟡 Библиотека redis не найдена, fallback на FakeRedis")

logger = logging.getLogger(__name__)

class RedisAccessSync:
    """
    Система синхронизации доступов через Redis Pub/Sub
    """
    
    def __init__(self, redis_host: str = None, redis_port: int = None, redis_db: int = None):
        # Инициализация Redis клиента
        if USE_FAKE_REDIS:
            self.redis_client = get_fake_redis()
        else:
            # Предпочитаем REDIS_URL, иначе host/port
            if REDIS_URL:
                self.redis_client = real_redis.Redis.from_url(REDIS_URL)
            else:
                self.redis_client = real_redis.Redis(
                    host=redis_host or REDIS_HOST,
                    port=redis_port or REDIS_PORT,
                    db=redis_db if redis_db is not None else REDIS_DB,
                    password=REDIS_PASSWORD,
                    decode_responses=False
                )
        
        self.pubsub = self.redis_client.pubsub()
        
        # Каналы для событий
        self.USER_ADDED_CHANNEL = "access:user_added"
        self.USER_REMOVED_CHANNEL = "access:user_removed"
        self.ACCESS_KEY = "access:users"
        
        # Локальный кеш
        self._local_cache = {}
        self._cache_lock = threading.Lock()
        
        # Поток для прослушивания событий
        self._listener_thread = None
        self._stop_listening = False
        
    def start_listener(self):
        """Запускает прослушивание событий Redis"""
        if self._listener_thread and self._listener_thread.is_alive():
            return
            
        self._stop_listening = False
        
        # Подписываемся на каналы
        self.pubsub.subscribe(self.USER_ADDED_CHANNEL, self.USER_REMOVED_CHANNEL)
        
        # Запускаем поток
        self._listener_thread = threading.Thread(target=self._listen_events, daemon=True)
        self._listener_thread.start()
        
        logger.info("🔄 Redis listener запущен")
        
    def stop_listener(self):
        """Останавливает прослушивание событий"""
        self._stop_listening = True
        if self._listener_thread:
            self._listener_thread.join(timeout=1)
        self.pubsub.close()
        
    def _listen_events(self):
        """Прослушивает события Redis в отдельном потоке"""
        try:
            for message in self.pubsub.listen():
                if self._stop_listening:
                    break
                    
                if message['type'] == 'message':
                    self._handle_event(message)
                    
        except Exception as e:
            logger.error(f"Ошибка в Redis listener: {e}")
            
    def _handle_event(self, message):
        """Обрабатывает событие от Redis"""
        try:
            # Исправляем для совместимости с FakeRedis
            channel = message['channel']
            if hasattr(channel, 'decode'):
                channel = channel.decode('utf-8')
            
            data = json.loads(message['data'])
            
            user_id = data['user_id']
            
            with self._cache_lock:
                if channel == self.USER_ADDED_CHANNEL:
                    self._local_cache[str(user_id)] = data
                    logger.info(f"🟢 Пользователь {user_id} добавлен в локальный кеш")
                    
                elif channel == self.USER_REMOVED_CHANNEL:
                    if str(user_id) in self._local_cache:
                        del self._local_cache[str(user_id)]
                        logger.info(f"🔴 Пользователь {user_id} удален из локального кеша")
                        
        except Exception as e:
            logger.error(f"Ошибка обработки события: {e}")
    
    def add_user(self, user_id: int, user_data: dict):
        """Добавляет пользователя и рассылает событие"""
        try:
            user_key = str(user_id)
            
            # Сохраняем в Redis
            self.redis_client.hset(self.ACCESS_KEY, user_key, json.dumps(user_data))
            
            # Обновляем локальный кеш
            with self._cache_lock:
                self._local_cache[user_key] = user_data
            
            # Рассылаем событие
            event_data = {
                'user_id': user_id,
                'timestamp': datetime.now().isoformat(),
                **user_data
            }
            self.redis_client.publish(self.USER_ADDED_CHANNEL, json.dumps(event_data))
            
            logger.info(f"✅ Пользователь {user_id} добавлен и событие отправлено")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка добавления пользователя {user_id}: {e}")
            return False
    
    def remove_user(self, user_id: int):
        """Удаляет пользователя и рассылает событие"""
        try:
            user_key = str(user_id)
            
            # Удаляем из Redis
            self.redis_client.hdel(self.ACCESS_KEY, user_key)
            
            # Удаляем из локального кеша
            with self._cache_lock:
                self._local_cache.pop(user_key, None)
            
            # Рассылаем событие
            event_data = {
                'user_id': user_id,
                'timestamp': datetime.now().isoformat()
            }
            self.redis_client.publish(self.USER_REMOVED_CHANNEL, json.dumps(event_data))
            
            logger.info(f"✅ Пользователь {user_id} удален и событие отправлено")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка удаления пользователя {user_id}: {e}")
            return False
    
    def has_access(self, user_id: int) -> bool:
        """Проверяет доступ пользователя (ВСЕГДА актуальные данные)"""
        user_key = str(user_id)
        
        # ВСЕГДА проверяем Redis/файл для актуальных данных
        try:
            raw_data = self.redis_client.hget(self.ACCESS_KEY, user_key)
            if raw_data:
                user_data = json.loads(raw_data)
                
                # Обновляем локальный кеш
                with self._cache_lock:
                    self._local_cache[user_key] = user_data
                
                result = self._check_user_active(user_data)
                logger.debug(f"🔍 Redis проверка {user_id}: найден = True, активен = {result}")
                return result
            else:
                # Пользователя нет - удаляем из локального кеша
                with self._cache_lock:
                    if user_key in self._local_cache:
                        del self._local_cache[user_key]
                        logger.debug(f"🗑️ Удален из локального кеша: {user_id}")
                
                logger.debug(f"🔍 Redis проверка {user_id}: не найден = False")
                return False
                
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
    
    def load_initial_cache(self):
        """Загружает начальный кеш из Redis"""
        try:
            all_users = self.redis_client.hgetall(self.ACCESS_KEY)
            
            with self._cache_lock:
                self._local_cache.clear()
                for user_key, raw_data in all_users.items():
                    try:
                        user_data = json.loads(raw_data)
                        self._local_cache[user_key.decode('utf-8')] = user_data
                    except:
                        continue
                        
            logger.info(f"📦 Загружен начальный кеш: {len(self._local_cache)} пользователей")
            
        except Exception as e:
            logger.error(f"Ошибка загрузки начального кеша: {e}")
    
    def get_stats(self) -> dict:
        """Возвращает статистику"""
        with self._cache_lock:
            local_count = len(self._local_cache)
            
        try:
            redis_count = self.redis_client.hlen(self.ACCESS_KEY)
        except:
            redis_count = 0
            
        return {
            'local_cache_users': local_count,
            'redis_users': redis_count,
            'listener_active': self._listener_thread and self._listener_thread.is_alive()
        }

# Глобальный экземпляр
_redis_sync = None

def get_redis_sync() -> RedisAccessSync:
    """Получает глобальный экземпляр Redis синхронизации"""
    global _redis_sync
    if _redis_sync is None:
        _redis_sync = RedisAccessSync()
        _redis_sync.load_initial_cache()
        _redis_sync.start_listener()
    return _redis_sync

def has_access_redis(user_id: int) -> bool:
    """Проверяет доступ через Redis (для замены has_access)"""
    return get_redis_sync().has_access(user_id)

def add_user_redis(user_id: int, user_data: dict) -> bool:
    """Добавляет пользователя через Redis"""
    return get_redis_sync().add_user(user_id, user_data)

def remove_user_redis(user_id: int) -> bool:
    """Удаляет пользователя через Redis"""
    return get_redis_sync().remove_user(user_id) 
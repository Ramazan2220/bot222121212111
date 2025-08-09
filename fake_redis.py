#!/usr/bin/env python3
"""
🔥 FakeRedis - Эмулятор Redis для разработки с межпроцессной поддержкой
"""

import json
import os
import time
import threading
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
import fcntl  # Для блокировки файлов

logger = logging.getLogger(__name__)

class FakeRedisFileBased:
    """FakeRedis с полной файловой синхронизацией для межпроцессной связи"""
    
    def __init__(self):
        # Используем абсолютный путь для межпроцессной синхронизации
        project_root = os.path.dirname(os.path.abspath(__file__))
        self.data_dir = os.path.join(project_root, "data", "fake_redis")
        self.sync_file = os.path.join(self.data_dir, "data.json")
        self.messages_dir = os.path.join(self.data_dir, "messages")
        
        # Создаем директории
        os.makedirs(self.data_dir, exist_ok=True)
        os.makedirs(self.messages_dir, exist_ok=True)
        
        self._data = {}
        self._lock = threading.Lock()
        self._subscribers = {}  # Каналы подписок
        
        self._load_from_file()
        logger.info("🔥 FakeRedis с файловой синхронизацией запущен")
    
    def _safe_file_operation(self, filename: str, operation: str, data: Any = None):
        """Безопасная операция с файлом с блокировкой"""
        try:
            with open(filename, 'a+' if operation == 'read' else 'w') as f:
                # Блокируем файл
                fcntl.flock(f.fileno(), fcntl.LOCK_EX)
                
                if operation == 'read':
                    f.seek(0)
                    content = f.read()
                    return json.loads(content) if content.strip() else {}
                elif operation == 'write':
                    json.dump(data, f, indent=2)
                    f.flush()
                    os.fsync(f.fileno())
                    return True
                    
        except (FileNotFoundError, json.JSONDecodeError):
            return {} if operation == 'read' else False
        except Exception as e:
            logger.error(f"⚠️ Ошибка файловой операции {operation}: {e}")
            return {} if operation == 'read' else False
    
    def _load_from_file(self):
        """Загружает данные из файла"""
        data = self._safe_file_operation(self.sync_file, 'read')
        with self._lock:
            self._data = data.get('data', {})
    
    def _save_to_file(self):
        """Сохраняет данные в файл"""
        data_to_save = {
            'data': self._data,
            'timestamp': datetime.now().isoformat()
        }
        self._safe_file_operation(self.sync_file, 'write', data_to_save)
    
    def set(self, key: str, value: str, ex: Optional[int] = None) -> bool:
        """Устанавливает значение ключа"""
        with self._lock:
            self._data[key] = value
            self._save_to_file()
            logger.debug(f"Redis SET: {key} = {value}")
            return True
    
    def get(self, key: str) -> Optional[str]:
        """Получает значение ключа"""
        self._load_from_file()  # Обновляем данные
        return self._data.get(key)
    
    def delete(self, key: str) -> bool:
        """Удаляет ключ"""
        with self._lock:
            if key in self._data:
                del self._data[key]
                self._save_to_file()
                logger.debug(f"Redis DELETE: {key}")
                return True
            return False
    
    def hset(self, key: str, field: str, value: str) -> bool:
        """Устанавливает поле в хеше"""
        with self._lock:
            if key not in self._data:
                self._data[key] = "{}"
            
            try:
                hash_data = json.loads(self._data[key])
            except:
                hash_data = {}
            
            hash_data[field] = value
            self._data[key] = json.dumps(hash_data)
            self._save_to_file()
            logger.debug(f"Redis HSET: {key}.{field} = {value}")
            return True
    
    def hget(self, key: str, field: str) -> Optional[str]:
        """Получает поле из хеша"""
        self._load_from_file()  # Обновляем данные
        
        if key not in self._data:
            return None
        
        try:
            hash_data = json.loads(self._data[key])
            return hash_data.get(field)
        except:
            return None
    
    def hdel(self, key: str, field: str) -> bool:
        """Удаляет поле из хеша"""
        with self._lock:
            if key not in self._data:
                return False
            
            try:
                hash_data = json.loads(self._data[key])
                if field in hash_data:
                    del hash_data[field]
                    self._data[key] = json.dumps(hash_data)
                    self._save_to_file()
                    logger.debug(f"Redis HDEL: {key}.{field}")
                    return True
            except:
                pass
            return False

    def hgetall(self, key: str) -> Dict[str, str]:
        """Получает все поля из хеша"""
        self._load_from_file()  # Обновляем данные
        
        if key not in self._data:
            return {}
        
        try:
            hash_data = json.loads(self._data[key])
            return hash_data
        except:
            return {}

    def exists(self, key: str) -> bool:
        """Проверяет существование ключа"""
        self._load_from_file()  # Обновляем данные
        return key in self._data

    def keys(self, pattern: str = "*") -> List[str]:
        """Получает все ключи по паттерну"""
        self._load_from_file()  # Обновляем данные
        
        if pattern == "*":
            return list(self._data.keys())
        else:
            # Простая поддержка паттернов
            pattern_clean = pattern.replace("*", "")
            return [k for k in self._data.keys() if pattern_clean in k]

    def lpush(self, key: str, value: str) -> int:
        """Добавляет элемент в начало списка"""
        with self._lock:
            if key not in self._data:
                self._data[key] = "[]"  # Храним как JSON строку
            
            try:
                data_list = json.loads(self._data[key])
            except:
                data_list = []
            
            data_list.insert(0, value)
            self._data[key] = json.dumps(data_list)
            self._save_to_file()
            logger.debug(f"Redis LPUSH: {key} <- {value}")
            return len(data_list)

    def rpop(self, key: str) -> Optional[str]:
        """Удаляет и возвращает последний элемент списка"""
        self._load_from_file()  # Обновляем данные
        
        with self._lock:
            if key not in self._data:
                return None
            
            try:
                data_list = json.loads(self._data[key])
            except:
                return None
            
            if len(data_list) == 0:
                return None
            
            value = data_list.pop()
            self._data[key] = json.dumps(data_list)
            self._save_to_file()
            logger.debug(f"Redis RPOP: {key} -> {value}")
            return value

    def llen(self, key: str) -> int:
        """Возвращает длину списка"""
        self._load_from_file()  # Обновляем данные
        
        if key not in self._data:
            return 0
        
        try:
            data_list = json.loads(self._data[key])
            return len(data_list)
        except:
            return 0

    def lrem(self, key: str, count: int, value: str) -> int:
        """Удаляет элементы из списка"""
        with self._lock:
            if key not in self._data:
                return 0
            
            try:
                data_list = json.loads(self._data[key])
            except:
                return 0
            
            removed = 0
            if count == 0:  # Удалить все
                removed = data_list.count(value)
                data_list = [item for item in data_list if item != value]
            elif count > 0:  # Удалить count элементов с начала
                for i in range(len(data_list) - 1, -1, -1):
                    if data_list[i] == value and removed < count:
                        del data_list[i]
                        removed += 1
            else:  # Удалить count элементов с конца
                for i in range(len(data_list)):
                    if data_list[i] == value and removed < abs(count):
                        del data_list[i]
                        removed += 1
                        break
            
            self._data[key] = json.dumps(data_list)
            self._save_to_file()
            logger.debug(f"Redis LREM: {key} count={count} value={value} removed={removed}")
            return removed
    
    def publish(self, channel: str, message: str) -> int:
        """Публикует сообщение в канал"""
        # Создаем файл сообщения с временной меткой
        timestamp = int(time.time() * 1000000)  # микросекунды
        message_file = os.path.join(self.messages_dir, f"{channel}_{timestamp}.json")
        
        message_data = {
            'channel': channel,
            'data': message,
            'timestamp': timestamp,
            'created_at': datetime.now().isoformat()
        }
        
        try:
            with open(message_file, 'w') as f:
                json.dump(message_data, f)
            
            logger.info(f"Redis PUBLISH: {channel} -> {message}")
            return 1  # Количество подписчиков (симулируем)
        except Exception as e:
            logger.error(f"Ошибка публикации: {e}")
            return 0
    
    def pubsub(self):
        """Возвращает объект PubSub"""
        return FakePubSub(self.messages_dir)

class FakePubSub:
    """Эмулятор Redis PubSub с файловой системой"""
    
    def __init__(self, messages_dir: str):
        self.messages_dir = messages_dir
        self.subscribed_channels = set()
        self._last_check = 0
        
    def subscribe(self, *channels):
        """Подписывается на каналы"""
        for channel in channels:
            self.subscribed_channels.add(channel)
            logger.info(f"FakePubSub: подписка на {channel}")
    
    def listen(self):
        """Слушает сообщения из подписанных каналов"""
        logger.info(f"FakePubSub: начинаем слушать каналы: {self.subscribed_channels}")
        processed_files = set()  # Защита от дублирования
        
        while True:
            try:
                # Проверяем новые файлы сообщений
                current_time = int(time.time() * 1000000)
                
                if not os.path.exists(self.messages_dir):
                    time.sleep(0.1)
                    continue
                
                for filename in os.listdir(self.messages_dir):
                    if not filename.endswith('.json'):
                        continue
                    
                    # Защита от дублирования
                    if filename in processed_files:
                        continue
                    
                    filepath = os.path.join(self.messages_dir, filename)
                    
                    # Проверяем, что файл еще существует
                    if not os.path.exists(filepath):
                        continue
                    
                    try:
                        # Проверяем время создания файла
                        file_timestamp = int(filename.split('_')[-1].replace('.json', ''))
                        
                        # Пропускаем старые сообщения
                        if file_timestamp <= self._last_check:
                            processed_files.add(filename)
                            continue
                        
                        # Читаем сообщение
                        with open(filepath, 'r') as f:
                            message_data = json.load(f)
                        
                        channel = message_data['channel']
                        
                        # Проверяем подписку на канал
                        if channel in self.subscribed_channels:
                            yield {
                                'type': 'message',
                                'channel': channel,
                                'data': message_data['data']
                            }
                            
                            logger.info(f"📨 FakePubSub получил: {channel} -> {message_data['data']}")
                        
                        # Помечаем как обработанное
                        processed_files.add(filename)
                        
                        # Безопасно удаляем обработанное сообщение
                        try:
                            os.remove(filepath)
                        except FileNotFoundError:
                            pass  # Файл уже удален другим процессом
                            
                    except (json.JSONDecodeError, ValueError, KeyError) as e:
                        logger.warning(f"Поврежденное сообщение {filename}: {e}")
                        try:
                            os.remove(filepath)
                        except FileNotFoundError:
                            pass
                        processed_files.add(filename)
                        
                    except Exception as e:
                        logger.error(f"Ошибка обработки сообщения {filename}: {e}")
                        processed_files.add(filename)
                
                # Обновляем время последней проверки
                self._last_check = current_time
                
                # Очищаем список обработанных файлов периодически
                if len(processed_files) > 1000:
                    processed_files.clear()
                
                time.sleep(0.1)  # Небольшая пауза для снижения нагрузки
                
            except Exception as e:
                logger.error(f"Критическая ошибка в FakePubSub.listen: {e}")
                time.sleep(1)

# Singleton экземпляр
_fake_redis_instance = None

def get_fake_redis():
    """Возвращает singleton экземпляр FakeRedis"""
    global _fake_redis_instance
    if _fake_redis_instance is None:
        _fake_redis_instance = FakeRedisFileBased()
    return _fake_redis_instance 
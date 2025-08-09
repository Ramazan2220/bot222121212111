"""
Lazy Loading система для Instagram клиентов
Экономит до 99% памяти, создавая реальные клиенты только по требованию
"""

import os
import json
import time
import logging
import threading
from typing import Dict, Any, Optional, Union
from dataclasses import dataclass
from contextlib import contextmanager

from instagrapi import Client as InstagrapiClient
from database.db_manager import get_instagram_account, update_account_session_data


logger = logging.getLogger(__name__)


@dataclass
class LazyClientStats:
    """Статистика для мониторинга Lazy Loading"""
    total_created: int = 0
    total_destroyed: int = 0
    currently_active: int = 0
    memory_saved_mb: float = 0.0
    cache_hits: int = 0
    cache_misses: int = 0
    avg_creation_time: float = 0.0
    avg_operation_time: float = 0.0


class LazyInstagramClient:
    """
    Lazy прокси для instagrapi.Client
    Создает реальный клиент только при первом обращении к методу
    """
    
    # Размер одного реального клиента в MB
    REAL_CLIENT_SIZE_MB = 4.0
    LAZY_CLIENT_SIZE_MB = 0.001  # ~1KB метаданных
    
    def __init__(self, account_id: int, factory: 'LazyClientFactory'):
        self.account_id = account_id
        self.factory = factory
        self._real_client: Optional[InstagrapiClient] = None
        self._is_logged_in = False
        self._creation_time = None
        self._last_access = time.time()
        self._lock = threading.Lock()
        
        # Кешируем данные аккаунта
        self._account = None
        self._session_file = None
        
        # Статистика
        self._operations_count = 0
        self._total_operation_time = 0.0
        
        logger.debug(f"LazyInstagramClient создан для аккаунта {account_id}")
    
    @property
    def memory_footprint_mb(self) -> float:
        """Возвращает текущий размер в памяти"""
        if self._real_client is None:
            return self.LAZY_CLIENT_SIZE_MB
        return self.REAL_CLIENT_SIZE_MB
    
    @property
    def account(self):
        """Ленивая загрузка данных аккаунта"""
        if self._account is None:
            self._account = get_instagram_account(self.account_id)
        return self._account
    
    @property
    def session_file(self):
        """Ленивая загрузка пути к файлу сессии"""
        if self._session_file is None:
            from config import ACCOUNTS_DIR
            account_dir = os.path.join(ACCOUNTS_DIR, str(self.account_id))
            self._session_file = os.path.join(account_dir, "session.json")
        return self._session_file
    
    def _ensure_real_client(self) -> InstagrapiClient:
        """Создает реальный клиент если его еще нет"""
        if self._real_client is not None:
            self._last_access = time.time()
            return self._real_client
        
        with self._lock:
            if self._real_client is not None:
                self._last_access = time.time()
                return self._real_client
            
            start_time = time.time()
            logger.info(f"Создание реального клиента для аккаунта {self.account_id}")
            
            try:
                # Создаем реальный instagrapi.Client
                from device_manager import get_or_create_device_settings
                device_settings = get_or_create_device_settings(self.account_id)
                
                self._real_client = InstagrapiClient(settings=device_settings)
                
                # Применяем патчи если они есть
                try:
                    import instagram.client_patch
                    import instagram.deep_patch
                    logger.debug(f"Патчи применены для аккаунта {self.account_id}")
                except ImportError:
                    logger.debug("Патчи не найдены, используем стандартный клиент")
                
                # Загружаем сессию если есть
                self._load_session()
                
                # Обновляем статистику
                creation_time = time.time() - start_time
                self._creation_time = creation_time
                self.factory._update_stats('client_created', creation_time)
                
                self._last_access = time.time()
                
                logger.info(f"Реальный клиент создан за {creation_time:.2f}с для аккаунта {self.account_id}")
                return self._real_client
                
            except Exception as e:
                logger.error(f"Ошибка создания клиента для аккаунта {self.account_id}: {e}")
                self._real_client = None
                raise
    
    def _load_session(self):
        """Загружает сессию из файла"""
        try:
            if os.path.exists(self.session_file):
                with open(self.session_file, 'r') as f:
                    session_data = json.load(f)
                
                if 'settings' in session_data:
                    self._real_client.set_settings(session_data['settings'])
                    self._is_logged_in = True
                    logger.debug(f"Сессия загружена для аккаунта {self.account_id}")
            
            # Также пробуем загрузить из базы данных
            if self.account.session_data:
                try:
                    db_session = json.loads(self.account.session_data)
                    if 'settings' in db_session:
                        self._real_client.set_settings(db_session['settings'])
                        self._is_logged_in = True
                        logger.debug(f"Сессия из БД загружена для аккаунта {self.account_id}")
                except json.JSONDecodeError:
                    logger.warning(f"Некорректные данные сессии в БД для аккаунта {self.account_id}")
                    
        except Exception as e:
            logger.error(f"Ошибка загрузки сессии для аккаунта {self.account_id}: {e}")
    
    def _save_session(self):
        """Сохраняет сессию в файл и БД"""
        if self._real_client is None:
            return
        
        try:
            # Создаем директорию если нужно
            os.makedirs(os.path.dirname(self.session_file), exist_ok=True)
            
            # Получаем настройки
            settings = self._real_client.get_settings()
            session_data = {
                'username': self.account.username,
                'account_id': self.account_id,
                'last_login': time.strftime('%Y-%m-%d %H:%M:%S'),
                'settings': settings
            }
            
            # Сохраняем в файл
            with open(self.session_file, 'w') as f:
                json.dump(session_data, f)
            
            # Сохраняем в БД
            update_account_session_data(self.account_id, json.dumps(session_data))
            
            logger.debug(f"Сессия сохранена для аккаунта {self.account_id}")
            
        except Exception as e:
            logger.error(f"Ошибка сохранения сессии для аккаунта {self.account_id}: {e}")
    
    def _track_operation(self, operation_name: str, duration: float):
        """Отслеживает статистику операций"""
        self._operations_count += 1
        self._total_operation_time += duration
        self.factory._update_stats('operation_completed', duration)
        
        if self._operations_count % 100 == 0:
            avg_time = self._total_operation_time / self._operations_count
            logger.debug(f"Аккаунт {self.account_id}: {self._operations_count} операций, среднее время {avg_time:.3f}с")
    
    def destroy(self):
        """Принудительно уничтожает реальный клиент"""
        if self._real_client is not None:
            with self._lock:
                if self._real_client is not None:
                    try:
                        self._save_session()
                        self._real_client = None
                        self.factory._update_stats('client_destroyed')
                        logger.debug(f"Реальный клиент уничтожен для аккаунта {self.account_id}")
                    except Exception as e:
                        logger.error(f"Ошибка при уничтожении клиента {self.account_id}: {e}")
    
    @property
    def is_active(self) -> bool:
        """Проверяет активен ли реальный клиент"""
        return self._real_client is not None
    
    @property
    def last_access_time(self) -> float:
        """Время последнего обращения"""
        return self._last_access
    
    def __getattr__(self, name):
        """Проксирует все вызовы методов к реальному клиенту"""
        if name.startswith('_'):
            raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{name}'")
        
        def proxy_method(*args, **kwargs):
            start_time = time.time()
            try:
                real_client = self._ensure_real_client()
                method = getattr(real_client, name)
                
                if callable(method):
                    result = method(*args, **kwargs)
                    
                    # Автосохранение после важных операций
                    if name in ['login', 'login_by_sessionid', 'relogin']:
                        self._is_logged_in = True
                        self._save_session()
                    
                    return result
                else:
                    return method
                    
            except Exception as e:
                logger.error(f"Ошибка в методе {name} для аккаунта {self.account_id}: {e}")
                raise
            finally:
                duration = time.time() - start_time
                self._track_operation(name, duration)
        
        return proxy_method


class LazyClientFactory:
    """
    Фабрика для создания и управления Lazy клиентами
    """
    
    def __init__(self, max_active_clients: int = 1000, cleanup_interval: int = 1800):
        self.max_active_clients = max_active_clients
        self.cleanup_interval = cleanup_interval  # 30 минут
        
        self._clients: Dict[int, LazyInstagramClient] = {}
        self._stats = LazyClientStats()
        self._lock = threading.Lock()
        
        # Запускаем фоновую очистку
        self._cleanup_thread = None
        self._start_cleanup_thread()
        
        logger.info(f"LazyClientFactory инициализирована: max_clients={max_active_clients}")
    
    def get_client(self, account_id: int) -> LazyInstagramClient:
        """Получает lazy клиент для аккаунта"""
        with self._lock:
            if account_id not in self._clients:
                self._clients[account_id] = LazyInstagramClient(account_id, self)
                logger.debug(f"Создан новый lazy клиент для аккаунта {account_id}")
            
            client = self._clients[account_id]
            
            # Проверяем лимиты активных клиентов
            self._enforce_limits()
            
            return client
    
    def _enforce_limits(self):
        """Принудительно очищает клиентов если превышен лимит"""
        active_clients = [c for c in self._clients.values() if c.is_active]
        
        if len(active_clients) > self.max_active_clients:
            # Сортируем по времени последнего доступа
            active_clients.sort(key=lambda c: c.last_access_time)
            
            # Удаляем самых старых
            to_remove = len(active_clients) - self.max_active_clients
            for client in active_clients[:to_remove]:
                client.destroy()
                logger.info(f"Принудительно очищен клиент {client.account_id} (превышен лимит)")
    
    def _start_cleanup_thread(self):
        """Запускает фоновый поток очистки"""
        def cleanup_worker():
            while True:
                try:
                    time.sleep(self.cleanup_interval)
                    self.cleanup_inactive_clients()
                except Exception as e:
                    logger.error(f"Ошибка в потоке очистки: {e}")
        
        self._cleanup_thread = threading.Thread(target=cleanup_worker, daemon=True)
        self._cleanup_thread.start()
        logger.info("Фоновый поток очистки запущен")
    
    def cleanup_inactive_clients(self, max_inactive_time: int = 3600):
        """Очищает неактивные клиенты (по умолчанию старше 1 часа)"""
        current_time = time.time()
        cleaned_count = 0
        
        with self._lock:
            for account_id, client in list(self._clients.items()):
                if (client.is_active and 
                    current_time - client.last_access_time > max_inactive_time):
                    client.destroy()
                    cleaned_count += 1
        
        if cleaned_count > 0:
            logger.info(f"Очищено {cleaned_count} неактивных клиентов")
    
    def _update_stats(self, event_type: str, value: float = 0):
        """Обновляет статистику"""
        if event_type == 'client_created':
            self._stats.total_created += 1
            self._stats.currently_active += 1
            self._stats.memory_saved_mb += LazyInstagramClient.REAL_CLIENT_SIZE_MB - LazyInstagramClient.LAZY_CLIENT_SIZE_MB
            
            # Обновляем среднее время создания
            if self._stats.total_created == 1:
                self._stats.avg_creation_time = value
            else:
                self._stats.avg_creation_time = (
                    (self._stats.avg_creation_time * (self._stats.total_created - 1) + value) / 
                    self._stats.total_created
                )
        
        elif event_type == 'client_destroyed':
            self._stats.total_destroyed += 1
            self._stats.currently_active = max(0, self._stats.currently_active - 1)
        
        elif event_type == 'operation_completed':
            if self._stats.avg_operation_time == 0:
                self._stats.avg_operation_time = value
            else:
                # Экспоненциальное скользящее среднее
                self._stats.avg_operation_time = 0.9 * self._stats.avg_operation_time + 0.1 * value
    
    def get_stats(self) -> LazyClientStats:
        """Возвращает текущую статистику"""
        return self._stats
    
    def get_active_clients_count(self) -> int:
        """Возвращает количество активных клиентов"""
        return len([c for c in self._clients.values() if c.is_active])
    
    def get_total_memory_usage_mb(self) -> float:
        """Возвращает общее использование памяти"""
        return sum(c.memory_footprint_mb for c in self._clients.values())
    
    @contextmanager
    def get_client_context(self, account_id: int):
        """Контекстный менеджер для автоочистки клиента"""
        client = self.get_client(account_id)
        try:
            yield client
        finally:
            # Автоматически уничтожаем после использования для экономии памяти
            client.destroy()
    
    def shutdown(self):
        """Корректное завершение работы фабрики"""
        logger.info("Завершение работы LazyClientFactory...")
        
        with self._lock:
            for client in self._clients.values():
                client.destroy()
            self._clients.clear()
        
        logger.info("LazyClientFactory завершена")


# Глобальная фабрика
_lazy_factory: Optional[LazyClientFactory] = None


def init_lazy_factory(max_active_clients: int = 1000, cleanup_interval: int = 1800):
    """Инициализирует глобальную фабрику lazy клиентов"""
    global _lazy_factory
    _lazy_factory = LazyClientFactory(max_active_clients, cleanup_interval)
    logger.info("Глобальная LazyClientFactory инициализирована")


def get_lazy_client(account_id: int) -> LazyInstagramClient:
    """Получает lazy клиент из глобальной фабрики"""
    if _lazy_factory is None:
        raise RuntimeError("LazyClientFactory не инициализирована. Вызовите init_lazy_factory()")
    return _lazy_factory.get_client(account_id)


def get_lazy_factory_stats() -> LazyClientStats:
    """Получает статистику глобальной фабрики"""
    if _lazy_factory is None:
        return LazyClientStats()
    return _lazy_factory.get_stats()


def cleanup_lazy_clients():
    """Очищает неактивные lazy клиенты"""
    if _lazy_factory is not None:
        _lazy_factory.cleanup_inactive_clients()


def shutdown_lazy_factory():
    """Корректно завершает работу lazy фабрики"""
    global _lazy_factory
    if _lazy_factory is not None:
        _lazy_factory.shutdown()
        _lazy_factory = None 
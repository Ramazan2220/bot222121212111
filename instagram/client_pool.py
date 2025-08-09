# -*- coding: utf-8 -*-
"""
Instagram Client Pool - Пул переиспользуемых клиентов Instagram API

Обеспечивает:
- Переиспользование клиентов для снижения нагрузки на API
- Автоматическую очистку неактивных клиентов  
- Обратную совместимость с существующим кодом
- Метрики и мониторинг производительности
"""

import time
import logging
import threading
from typing import Dict, Optional, Any
from dataclasses import dataclass
from instagrapi import Client
from database.db_manager import get_instagram_account
from utils.encryption import encryption

logger = logging.getLogger(__name__)

@dataclass
class ClientStats:
    """Статистика клиента в пуле"""
    created_at: float
    last_used: float
    use_count: int
    account_id: int
    username: str
    is_active: bool

@dataclass 
class PoolStats:
    """Общая статистика пула"""
    total_clients: int = 0
    active_clients: int = 0
    created_count: int = 0
    reused_count: int = 0
    removed_count: int = 0
    cache_hits: int = 0
    cache_misses: int = 0

class InstagramClientPool:
    """Адаптивный пул клиентов Instagram с автомасштабированием"""
    
    def __init__(self, 
                 initial_max_clients: int = 50,
                 max_clients_limit: int = 300,     # Абсолютный максимум
                 adaptive_scaling: bool = True,    # Включить адаптивное масштабирование
                 inactive_threshold: int = 2700,  # 45 min
                 old_threshold: int = 14400,      # 4 hours
                 sleep_mode_threshold: int = 7200): # 2 hours for sleep mode
        self.initial_max_clients = initial_max_clients
        self.max_clients_limit = max_clients_limit
        self.adaptive_scaling = adaptive_scaling
        self.current_max_clients = initial_max_clients  # Текущий лимит (динамический)
        self.inactive_threshold = inactive_threshold  
        self.old_threshold = old_threshold
        self.sleep_mode_threshold = sleep_mode_threshold
        self.sleeping_clients = {}  # account_id -> minimal client data
        
        self._clients: Dict[int, Client] = {}
        self._stats_map: Dict[int, ClientStats] = {}
        self._lock = threading.RLock()
        self._pool_stats = PoolStats()
        
        # Адаптивное масштабирование
        self._last_scale_check = time.time()
        self._scale_check_interval = 180  # Проверка каждые 3 минуты (было 5)
        self._demand_history = []  # История запросов для анализа нагрузки
        
        if self.adaptive_scaling:
            logger.info(f"🏊‍♂️ Адаптивный Instagram Client Pool инициализирован: start={initial_max_clients}, limit={max_clients_limit}")
        else:
            logger.info(f"🏊‍♂️ Instagram Client Pool инициализирован: max={initial_max_clients}, inactive={inactive_threshold//60}мин, old={old_threshold//3600}ч")
    
    def get_client(self, account_id: int, force_new: bool = False) -> Optional[Client]:
        """
        Получить клиент из пула или создать новый
        
        Args:
            account_id: ID аккаунта
            force_new: Принудительно создать новый клиент
            
        Returns:
            Client или None при ошибке
        """
        with self._lock:
            current_time = time.time()
            
            # Записываем запрос для анализа нагрузки
            if self.adaptive_scaling:
                self._record_demand()
                self._check_and_scale()
            
            # Проверяем существующий клиент
            if not force_new and account_id in self._clients:
                client = self._clients[account_id]
                stats = self._stats_map[account_id]
                
                # Проверяем что клиент еще активен
                if stats.is_active and (current_time - stats.last_used) < self.inactive_threshold:
                    # Обновляем статистику использования
                    stats.last_used = current_time
                    stats.use_count += 1
                    self._pool_stats.reused_count += 1
                    self._pool_stats.cache_hits += 1
                    
                    logger.debug(f"♻️ Переиспользуем клиент для аккаунта {account_id} (использований: {stats.use_count})")
                    return client
                else:
                    # Клиент устарел, удаляем его
                    logger.info(f"🗑️ Удаляем устаревший клиент для аккаунта {account_id}")
                    self._remove_client_unsafe(account_id)
            
            # Создаем новый клиент
            self._pool_stats.cache_misses += 1
            return self._create_new_client(account_id)
    
    def _create_new_client(self, account_id: int) -> Optional[Client]:
        """Создать новый клиент и добавить в пул"""
        try:
            # Получаем данные аккаунта
            account = get_instagram_account(account_id)
            if not account:
                logger.error(f"❌ Аккаунт {account_id} не найден в БД")
                return None
            
            # Дешифруем пароль
            password = account.password
            if encryption and hasattr(encryption, 'decrypt'):
                try:
                    password = encryption.decrypt(account.password)
                except:
                    logger.warning(f"⚠️ Не удалось дешифровать пароль для {account.username}")
            
            # Создаем клиент
            client = Client()
            
            # Применяем настройки если есть
            if hasattr(account, 'device_settings') and account.device_settings:
                try:
                    import json
                    settings = json.loads(account.device_settings)
                    client.set_settings(settings)
                    logger.debug(f"📱 Применены настройки устройства для {account.username}")
                except:
                    logger.warning(f"⚠️ Ошибка применения настроек устройства для {account.username}")
            
            # Логин
            if client.login(account.username, password):
                current_time = time.time()
                
                # Очищаем место если пул переполнен
                self._cleanup_if_needed()
                
                # Добавляем в пул
                self._clients[account_id] = client
                self._stats_map[account_id] = ClientStats(
                    created_at=current_time,
                    last_used=current_time,
                    use_count=1,
                    account_id=account_id,
                    username=account.username,
                    is_active=True
                )
                
                # Обновляем статистику
                self._pool_stats.total_clients = len(self._clients)
                self._pool_stats.active_clients = sum(1 for s in self._stats_map.values() if s.is_active)
                self._pool_stats.created_count += 1
                
                logger.info(f"✅ Создан новый клиент для {account.username} (пул: {len(self._clients)}/{self.max_clients})")
                return client
            else:
                logger.error(f"❌ Ошибка логина для {account.username}")
                return None
                
        except Exception as e:
            logger.error(f"❌ Ошибка создания клиента для аккаунта {account_id}: {e}")
            return None
    
    def release_client(self, account_id: int):
        """Освободить клиент (пометить как неиспользуемый)"""
        with self._lock:
            if account_id in self._stats_map:
                self._stats_map[account_id].last_used = time.time()
                logger.debug(f"🔓 Клиент {account_id} освобожден")
    
    def remove_client(self, account_id: int):
        """Принудительно удалить клиент из пула"""
        with self._lock:
            self._remove_client_unsafe(account_id)
    
    def _remove_client_unsafe(self, account_id: int):
        """Удалить клиент без блокировки (небезопасно)"""
        if account_id in self._clients:
            try:
                client = self._clients[account_id]
                # Пытаемся корректно завершить сессию
                try:
                    client.logout()
                except:
                    pass  # Игнорируем ошибки logout
                    
                del self._clients[account_id]
                logger.debug(f"🗑️ Клиент {account_id} удален из пула")
            except Exception as e:
                logger.warning(f"⚠️ Ошибка при удалении клиента {account_id}: {e}")
        
        if account_id in self._stats_map:
            del self._stats_map[account_id]
            
        # Обновляем статистику
        self._pool_stats.total_clients = len(self._clients)
        self._pool_stats.active_clients = sum(1 for s in self._stats_map.values() if s.is_active)
        self._pool_stats.removed_count += 1
    
    def _cleanup_if_needed(self):
        """Очистка пула если нужно"""
        current_time = time.time()
        
        # Если пул переполнен, удаляем старые клиенты (используем динамический лимит)
        current_limit = self.current_max_clients if self.adaptive_scaling else self.initial_max_clients
        if len(self._clients) >= current_limit:
            # Сортируем по времени последнего использования
            sorted_accounts = sorted(
                self._stats_map.items(),
                key=lambda x: x[1].last_used
            )
            
            # Удаляем 20% самых старых
            to_remove = max(1, len(sorted_accounts) // 5)
            for i in range(to_remove):
                account_id = sorted_accounts[i][0]
                logger.info(f"🧹 Очистка пула: удаляем старый клиент {account_id}")
                self._remove_client_unsafe(account_id)
        
        # Очищаем неактивные клиенты
        to_remove = []
        for account_id, stats in self._stats_map.items():
            # Клиент неактивен более inactive_threshold
            if (current_time - stats.last_used) > self.inactive_threshold:
                to_remove.append(account_id)
            # Клиент слишком старый
            elif (current_time - stats.created_at) > self.old_threshold:
                to_remove.append(account_id)
        
        for account_id in to_remove:
            logger.info(f"🧹 Автоочистка: удаляем неактивный клиент {account_id}")
            self._remove_client_unsafe(account_id)
    
    def cleanup(self):
        """Принудительная очистка всех неактивных клиентов"""
        with self._lock:
            self._cleanup_if_needed()
            logger.info(f"🧹 Очистка завершена. Клиентов в пуле: {len(self._clients)}")
    
    def _record_demand(self):
        """Записывает текущий запрос для анализа нагрузки"""
        current_time = time.time()
        self._demand_history.append(current_time)
        
        # Удаляем старые записи (храним историю за последний час)
        cutoff_time = current_time - 3600
        self._demand_history = [t for t in self._demand_history if t > cutoff_time]
    
    def _check_and_scale(self):
        """Проверяет нагрузку и масштабирует пул при необходимости"""
        current_time = time.time()
        
        # Проверяем только раз в N минут
        if current_time - self._last_scale_check < self._scale_check_interval:
            return
        
        self._last_scale_check = current_time
        
        # Анализируем нагрузку за последние 30 минут
        recent_cutoff = current_time - 1800  # 30 минут
        recent_demands = [t for t in self._demand_history if t > recent_cutoff]
        demand_rate = len(recent_demands) / 30 if recent_demands else 0  # запросов в минуту
        
        # Текущая загрузка пула
        current_usage = len(self._clients)
        usage_percentage = current_usage / self.current_max_clients if self.current_max_clients > 0 else 0
        
        # Решение о масштабировании (РЕАЛИСТИЧНЫЕ ПОРОГИ ДЛЯ INSTAGRAM API)
        old_max = self.current_max_clients
        
        # Расчет ожидаемой нагрузки: ~10 запросов/мин на активный аккаунт
        expected_load_per_client = 10
        current_expected_load = current_usage * expected_load_per_client
        capacity_load = self.current_max_clients * expected_load_per_client
        
        if usage_percentage > 0.75 and demand_rate > (capacity_load * 0.6):  # 60% от теоретической нагрузки
            # Увеличиваем пул на 25% - более плавное масштабирование
            new_max = min(int(self.current_max_clients * 1.25), self.max_clients_limit)
            if new_max > self.current_max_clients:
                self.current_max_clients = new_max
                logger.info(f"📈 МАСШТАБИРОВАНИЕ ВВЕРХ: {old_max} → {new_max} клиентов (нагрузка: {usage_percentage:.1%}, запросов/мин: {demand_rate:.1f}, ожидается: {capacity_load})")
        
        elif usage_percentage < 0.4 and demand_rate < (current_expected_load * 0.3):  # 30% от ожидаемой нагрузки
            # Уменьшаем пул на 20% - более консервативное уменьшение
            new_max = max(int(self.current_max_clients * 0.8), self.initial_max_clients)
            if new_max < self.current_max_clients:
                self.current_max_clients = new_max
                logger.info(f"📉 МАСШТАБИРОВАНИЕ ВНИЗ: {old_max} → {new_max} клиентов (нагрузка: {usage_percentage:.1%}, запросов/мин: {demand_rate:.1f}, ожидается: {current_expected_load})")
                # Принудительно очищаем избыточные клиенты
                self._force_scale_down()
    
    def _force_scale_down(self):
        """Принудительно удаляет клиенты при уменьшении пула"""
        while len(self._clients) > self.current_max_clients:
            # Удаляем самый старый по использованию клиент
            if not self._clients:
                break
                
            oldest_account = min(
                self._stats_map.items(), 
                key=lambda x: x[1].last_used
            )[0]
            
            logger.info(f"🔽 Принудительное удаление клиента {oldest_account} при масштабировании")
            self._remove_client_unsafe(oldest_account)
    
    def get_adaptive_stats(self) -> dict:
        """Получить статистику адаптивного масштабирования"""
        current_time = time.time()
        
        # Нагрузка за разные периоды
        demands_5min = [t for t in self._demand_history if t > current_time - 300]
        demands_30min = [t for t in self._demand_history if t > current_time - 1800]
        demands_1hour = [t for t in self._demand_history if t > current_time - 3600]
        
        return {
            "adaptive_enabled": self.adaptive_scaling,
            "current_max_clients": self.current_max_clients,
            "initial_max_clients": self.initial_max_clients,
            "max_clients_limit": self.max_clients_limit,
            "current_usage": len(self._clients),
            "usage_percentage": len(self._clients) / self.current_max_clients if self.current_max_clients > 0 else 0,
            "demand_rates": {
                "last_5min": len(demands_5min) / 5,
                "last_30min": len(demands_30min) / 30,
                "last_hour": len(demands_1hour) / 60
            },
            "total_demand_history": len(self._demand_history),
            "next_scale_check": self._last_scale_check + self._scale_check_interval - current_time
        }
    
    def get_stats(self) -> dict:
        """Получить статистику пула"""
        with self._lock:
            # Обновляем текущие счетчики
            self._pool_stats.total_clients = len(self._clients)
            self._pool_stats.active_clients = sum(1 for s in self._stats_map.values() if s.is_active)
            
            return {
                'pool_stats': {
                    'total_clients': self._pool_stats.total_clients,
                    'active_clients': self._pool_stats.active_clients,
                    'created_count': self._pool_stats.created_count,
                    'reused_count': self._pool_stats.reused_count,
                    'removed_count': self._pool_stats.removed_count,
                    'cache_hits': self._pool_stats.cache_hits,
                    'cache_misses': self._pool_stats.cache_misses,
                    'hit_ratio': self._pool_stats.cache_hits / max(1, self._pool_stats.cache_hits + self._pool_stats.cache_misses)
                },
                'adaptive_stats': self.get_adaptive_stats() if self.adaptive_scaling else None,
                'client_details': [
                    {
                        'account_id': stats.account_id,
                        'username': stats.username,
                        'created_at': stats.created_at,
                        'last_used': stats.last_used,
                        'use_count': stats.use_count,
                        'is_active': stats.is_active,
                        'age_minutes': (time.time() - stats.created_at) / 60,
                        'idle_minutes': (time.time() - stats.last_used) / 60
                    }
                    for stats in self._stats_map.values()
                ],
                'sleeping_clients': len(self.sleeping_clients),
                'clients_put_to_sleep': getattr(self._pool_stats, 'clients_put_to_sleep', 0),
                'clients_woken_up': getattr(self._pool_stats, 'clients_woken_up', 0),
                'memory_saved_by_sleep_mb': getattr(self._pool_stats, 'memory_saved_by_sleep', 0),
                'total_memory_usage_mb': (len(self._clients) * 4) + (len(self.sleeping_clients) * 0.8),  # 4MB active + 0.8MB sleeping
            }
    
    def shutdown(self):
        """Корректное завершение работы пула"""
        with self._lock:
            logger.info("🛑 Завершение работы Instagram Client Pool...")
            account_ids = list(self._clients.keys())
            for account_id in account_ids:
                self._remove_client_unsafe(account_id)
            logger.info("✅ Instagram Client Pool корректно завершен")

# Глобальный экземпляр пула
_client_pool: Optional[InstagramClientPool] = None

def init_client_pool(initial_max_clients: int = 50,
                    max_clients_limit: int = 300, 
                    adaptive_scaling: bool = True,
                    inactive_threshold: int = 3600,
                    old_threshold: int = 21600):
    """Инициализировать глобальный адаптивный пул клиентов"""
    global _client_pool
    _client_pool = InstagramClientPool(
        initial_max_clients=initial_max_clients,
        max_clients_limit=max_clients_limit,
        adaptive_scaling=adaptive_scaling,
        inactive_threshold=inactive_threshold, 
        old_threshold=old_threshold
    )
    if adaptive_scaling:
        logger.info("🏊‍♂️ Глобальный Адаптивный Instagram Client Pool инициализирован")
    else:
        logger.info("🏊‍♂️ Глобальный Instagram Client Pool инициализирован")

def get_instagram_client(account_id: int, force_new: bool = False) -> Optional[Client]:
    """
    Получить клиент из глобального пула
    
    ОБРАТНАЯ СОВМЕСТИМОСТЬ: Эта функция может использоваться везде
    где раньше создавались клиенты напрямую
    """
    if _client_pool is None:
        # Автоматическая инициализация если пул не создан
        init_client_pool()
    
    return _client_pool.get_client(account_id, force_new)

def release_instagram_client(account_id: int):
    """Освободить клиент в глобальном пуле"""
    if _client_pool:
        _client_pool.release_client(account_id)

def remove_instagram_client(account_id: int):
    """Удалить клиент из глобального пула"""
    if _client_pool:
        _client_pool.remove_client(account_id)

def cleanup_client_pool():
    """Очистить глобальный пул"""
    if _client_pool:
        _client_pool.cleanup()

def get_pool_stats() -> dict:
    """Получить статистику глобального пула"""
    if _client_pool:
        return _client_pool.get_stats()
    return {'error': 'Pool not initialized'}

def shutdown_client_pool():
    """Завершить работу глобального пула"""
    global _client_pool
    if _client_pool:
        _client_pool.shutdown()
        _client_pool = None

# Автоматическая инициализация при импорте
init_client_pool()

logger.info("📦 Instagram Client Pool модуль загружен") 
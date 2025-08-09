"""
Адаптер для обратной совместимости между обычными и Lazy Loading клиентами
Позволяет использовать существующий код без изменений
"""

import os
import logging
from typing import Union, Optional, Dict, Any
from dataclasses import dataclass

# Импорты для обычных клиентов
from instagram.client import InstagramClient, get_instagram_client

# Импорты для lazy клиентов
from instagram.lazy_client_factory import (
    LazyInstagramClient, get_lazy_client, init_lazy_factory,
    get_lazy_factory_stats, cleanup_lazy_clients
)

logger = logging.getLogger(__name__)


@dataclass
class ClientConfig:
    """Конфигурация для выбора типа клиента"""
    use_lazy_loading: bool = True
    lazy_max_active: int = 1000
    lazy_cleanup_interval: int = 1800  # 30 минут
    fallback_to_normal: bool = True  # Если lazy не работает, использовать обычный


class ClientAdapter:
    """
    Универсальный адаптер для работы с Instagram клиентами
    Автоматически выбирает между обычными и lazy клиентами
    """
    
    def __init__(self, config: Optional[ClientConfig] = None):
        self.config = config or ClientConfig()
        self._lazy_initialized = False
        self._fallback_mode = False
        
        # Инициализируем lazy factory если нужно
        if self.config.use_lazy_loading:
            self._init_lazy_factory()
    
    def _init_lazy_factory(self):
        """Инициализирует lazy factory"""
        try:
            if not self._lazy_initialized:
                init_lazy_factory(
                    max_active_clients=self.config.lazy_max_active,
                    cleanup_interval=self.config.lazy_cleanup_interval
                )
                self._lazy_initialized = True
                logger.info("Lazy Loading factory инициализирована")
        except Exception as e:
            logger.warning(f"Не удалось инициализировать Lazy Loading: {e}")
            if self.config.fallback_to_normal:
                self._fallback_mode = True
                logger.info("Переключение на обычные клиенты")
            else:
                raise
    
    def get_client(self, account_id: int) -> Union[InstagramClient, LazyInstagramClient]:
        """
        Получает клиент для аккаунта (lazy или обычный)
        
        Args:
            account_id: ID аккаунта
            
        Returns:
            Instagram клиент (совместимый с существующим API)
        """
        try:
            # Пытаемся использовать lazy клиент
            if self.config.use_lazy_loading and not self._fallback_mode:
                try:
                    client = get_lazy_client(account_id)
                    logger.debug(f"Создан lazy клиент для аккаунта {account_id}")
                    return client
                except Exception as e:
                    logger.warning(f"Ошибка создания lazy клиента для {account_id}: {e}")
                    if not self.config.fallback_to_normal:
                        raise
            
            # Используем обычный клиент
            client = get_instagram_client(account_id)
            logger.debug(f"Создан обычный клиент для аккаунта {account_id}")
            return client
            
        except Exception as e:
            logger.error(f"Ошибка создания клиента для аккаунта {account_id}: {e}")
            raise
    
    def get_stats(self) -> Dict[str, Any]:
        """Получает статистику работы клиентов"""
        stats = {
            'mode': 'lazy' if (self.config.use_lazy_loading and not self._fallback_mode) else 'normal',
            'lazy_initialized': self._lazy_initialized,
            'fallback_mode': self._fallback_mode
        }
        
        # Добавляем статистику lazy factory если доступна
        if self._lazy_initialized and not self._fallback_mode:
            try:
                lazy_stats = get_lazy_factory_stats()
                stats.update({
                    'lazy_total_created': lazy_stats.total_created,
                    'lazy_currently_active': lazy_stats.currently_active,
                    'lazy_memory_saved_mb': lazy_stats.memory_saved_mb,
                    'lazy_avg_creation_time': lazy_stats.avg_creation_time,
                    'lazy_avg_operation_time': lazy_stats.avg_operation_time
                })
            except Exception as e:
                logger.warning(f"Ошибка получения lazy статистики: {e}")
        
        return stats
    
    def cleanup(self):
        """Очищает неактивные клиенты"""
        if self._lazy_initialized and not self._fallback_mode:
            try:
                cleanup_lazy_clients()
                logger.debug("Выполнена очистка lazy клиентов")
            except Exception as e:
                logger.warning(f"Ошибка очистки lazy клиентов: {e}")
    
    def is_lazy_mode(self) -> bool:
        """Проверяет используется ли lazy режим"""
        return self.config.use_lazy_loading and not self._fallback_mode and self._lazy_initialized


# Глобальный адаптер
_global_adapter: Optional[ClientAdapter] = None


def init_client_adapter(config: Optional[ClientConfig] = None):
    """Инициализирует глобальный адаптер клиентов"""
    global _global_adapter
    _global_adapter = ClientAdapter(config)
    logger.info(f"Client Adapter инициализирован (lazy={_global_adapter.is_lazy_mode()})")


def get_universal_client(account_id: int) -> Union[InstagramClient, LazyInstagramClient]:
    """
    Универсальная функция для получения клиента
    ПОЛНОСТЬЮ СОВМЕСТИМА с существующим get_instagram_client()
    """
    if _global_adapter is None:
        # Если адаптер не инициализирован, используем обычный клиент
        logger.warning("Client Adapter не инициализирован, используем обычный клиент")
        return get_instagram_client(account_id)
    
    return _global_adapter.get_client(account_id)


def get_client_stats() -> Dict[str, Any]:
    """Получает статистику работы клиентов"""
    if _global_adapter is None:
        return {'mode': 'normal', 'adapter_initialized': False}
    
    return _global_adapter.get_stats()


def cleanup_clients():
    """Очищает неактивные клиенты"""
    if _global_adapter is not None:
        _global_adapter.cleanup()


def is_lazy_mode() -> bool:
    """Проверяет используется ли lazy режим"""
    if _global_adapter is None:
        return False
    return _global_adapter.is_lazy_mode()


# Функция обратной совместимости - алиас для существующего кода
def get_instagram_client_compatible(account_id: int) -> Union[InstagramClient, LazyInstagramClient]:
    """
    Функция обратной совместимости
    Заменяет существующие вызовы get_instagram_client() без изменения кода
    """
    return get_universal_client(account_id) 
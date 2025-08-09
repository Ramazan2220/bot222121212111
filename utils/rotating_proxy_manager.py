#!/usr/bin/env python3
"""
Модуль для управления rotating прокси с автоматической сменой IP
Поддерживает residential прокси с ротацией сессий
"""

import time
import hashlib
import random
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, Tuple

logger = logging.getLogger(__name__)

class RotatingProxyManager:
    """Менеджер для rotating прокси с автоматической сменой IP"""
    
    def __init__(self):
        self.session_cache = {}  # Кэш сессий для аккаунтов
        self.last_rotation = {}  # Время последней смены IP для аккаунта
        
    def generate_session_id(self, account_id: int, rotation_type: str = "time") -> str:
        """
        Генерирует уникальный session ID для ротации IP
        
        Args:
            account_id: ID аккаунта
            rotation_type: Тип ротации ('time', 'request', 'manual')
            
        Returns:
            Уникальный session ID
        """
        if rotation_type == "request":
            # Новая сессия на каждом запросе
            timestamp = str(time.time())
            random_part = str(random.randint(100000, 999999))
            session_data = f"{account_id}-{timestamp}-{random_part}"
            
        elif rotation_type == "time":
            # Смена IP каждые 5-15 минут
            current_time = datetime.now()
            
            # Проверяем нужна ли ротация
            if account_id in self.last_rotation:
                time_diff = current_time - self.last_rotation[account_id]
                if time_diff < timedelta(minutes=5):
                    # Используем существующую сессию
                    return self.session_cache.get(account_id, f"session-{account_id}-{int(time.time())}")
            
            # Генерируем новую сессию
            rotation_interval = random.randint(5, 15)  # 5-15 минут
            session_timestamp = int(current_time.timestamp() // (rotation_interval * 60))
            session_data = f"{account_id}-{session_timestamp}"
            
            # Обновляем кэш
            self.last_rotation[account_id] = current_time
            
        elif rotation_type == "manual":
            # Ручная ротация по запросу
            session_data = f"{account_id}-manual-{int(time.time())}"
            
        else:
            # Базовая сессия без ротации
            session_data = f"{account_id}-static"
        
        # Создаем хэш для session ID
        session_hash = hashlib.md5(session_data.encode()).hexdigest()[:8]
        session_id = f"user-session-{session_hash}"
        
        # Сохраняем в кэш
        self.session_cache[account_id] = session_id
        
        logger.info(f"🔄 Создана новая сессия для аккаунта {account_id}: {session_id} (тип: {rotation_type})")
        return session_id
    
    def build_rotating_proxy_url(self, proxy_config: Dict, account_id: int, 
                                rotation_type: str = "time") -> str:
        """
        Строит URL прокси с ротацией IP
        
        Args:
            proxy_config: Конфигурация прокси {protocol, host, port, username, password}
            account_id: ID аккаунта Instagram
            rotation_type: Тип ротации IP
            
        Returns:
            Полный URL прокси с сессией
        """
        # Генерируем session ID
        session_id = self.generate_session_id(account_id, rotation_type)
        
        # Строим URL прокси
        protocol = proxy_config.get('protocol', 'http')
        host = proxy_config['host']
        port = proxy_config['port']
        base_username = proxy_config.get('username', 'user')
        password = proxy_config.get('password', '')
        
        # Для residential прокси добавляем session ID к username
        if 'session' in base_username or 'user-' in base_username:
            # Прокси уже поддерживает сессии
            rotating_username = session_id
        else:
            # Добавляем session к базовому username
            rotating_username = f"{base_username}-{session_id}"
        
        # Формируем итоговый URL
        if password:
            proxy_url = f"{protocol}://{rotating_username}:{password}@{host}:{port}"
        else:
            proxy_url = f"{protocol}://{rotating_username}@{host}:{port}"
        
        logger.info(f"🌐 Создан rotating proxy URL для аккаунта {account_id}: {protocol}://{rotating_username}:***@{host}:{port}")
        return proxy_url
    
    def force_rotation(self, account_id: int) -> None:
        """
        Принудительно инициирует смену IP для аккаунта
        
        Args:
            account_id: ID аккаунта
        """
        logger.info(f"🔄 Принудительная ротация IP для аккаунта {account_id}")
        
        # Удаляем из кэша для принудительного обновления
        if account_id in self.session_cache:
            del self.session_cache[account_id]
        if account_id in self.last_rotation:
            del self.last_rotation[account_id]
    
    def get_proxy_stats(self, account_id: int) -> Dict:
        """
        Возвращает статистику использования прокси для аккаунта
        
        Args:
            account_id: ID аккаунта
            
        Returns:
            Словарь со статистикой
        """
        current_session = self.session_cache.get(account_id, "Нет активной сессии")
        last_rotation_time = self.last_rotation.get(account_id, "Никогда")
        
        if isinstance(last_rotation_time, datetime):
            time_since_rotation = datetime.now() - last_rotation_time
            minutes_since_rotation = int(time_since_rotation.total_seconds() / 60)
        else:
            minutes_since_rotation = "N/A"
        
        return {
            "account_id": account_id,
            "current_session": current_session,
            "last_rotation": last_rotation_time,
            "minutes_since_rotation": minutes_since_rotation
        }

# Глобальный экземпляр менеджера
rotating_proxy_manager = RotatingProxyManager()

def get_rotating_proxy_url(proxy_config: Dict, account_id: int, 
                          rotation_type: str = "time") -> str:
    """
    Функция-обертка для получения rotating proxy URL
    
    Args:
        proxy_config: Конфигурация прокси
        account_id: ID аккаунта
        rotation_type: Тип ротации ('time', 'request', 'manual')
    
    Returns:
        URL прокси с ротацией
    """
    return rotating_proxy_manager.build_rotating_proxy_url(
        proxy_config, account_id, rotation_type
    )

def force_proxy_rotation(account_id: int) -> None:
    """
    Принудительная смена IP для аккаунта
    
    Args:
        account_id: ID аккаунта
    """
    rotating_proxy_manager.force_rotation(account_id)

def get_proxy_rotation_stats(account_id: int) -> Dict:
    """
    Статистика ротации прокси для аккаунта
    
    Args:
        account_id: ID аккаунта
    
    Returns:
        Статистика ротации
    """
    return rotating_proxy_manager.get_proxy_stats(account_id) 
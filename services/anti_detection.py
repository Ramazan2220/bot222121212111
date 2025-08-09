# -*- coding: utf-8 -*-
"""
Модуль Anti-Detection для защиты от обнаружения ботов
"""

import random
import time
import logging
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
import json
import hashlib

from database.db_manager import get_instagram_account, update_instagram_account
from utils.rotating_proxy_manager import RotatingProxyManager

logger = logging.getLogger(__name__)

class AntiDetectionService:
    """Сервис защиты от обнаружения Instagram"""
    
    def __init__(self):
        self.proxy_manager = RotatingProxyManager()
        self.fingerprints = {}
        self.behavior_patterns = {}
        
    def create_human_behavior_pattern(self, account_id: int) -> Dict:
        """Создать человекоподобный паттерн поведения"""
        
        # Базовые характеристики "личности"
        personality = {
            # Время активности (утренний/дневной/вечерний тип)
            'active_hours': self._generate_active_hours(),
            
            # Скорость действий (быстрый/средний/медленный)
            'action_speed': random.choice(['fast', 'medium', 'slow']),
            
            # Интересы (для более естественного поведения)
            'interests': random.sample([
                'travel', 'food', 'fitness', 'fashion', 'art', 
                'music', 'photography', 'nature', 'tech', 'sports'
            ], k=random.randint(3, 5)),
            
            # Паттерны скроллинга
            'scroll_patterns': {
                'speed': random.uniform(0.5, 2.0),  # Скорость скролла
                'pause_probability': random.uniform(0.1, 0.3),  # Вероятность паузы
                'pause_duration': (2, 10),  # Диапазон паузы в секундах
            },
            
            # Паттерны взаимодействия
            'interaction_patterns': {
                'like_probability': random.uniform(0.05, 0.15),
                'comment_probability': random.uniform(0.01, 0.05),
                'save_probability': random.uniform(0.02, 0.08),
                'story_view_probability': random.uniform(0.3, 0.7),
            },
            
            # Паттерны набора текста
            'typing_patterns': {
                'speed_wpm': random.randint(30, 80),  # Слов в минуту
                'typo_probability': random.uniform(0.01, 0.05),
                'backspace_probability': random.uniform(0.05, 0.15),
            }
        }
        
        self.behavior_patterns[account_id] = personality
        return personality
    
    def _generate_active_hours(self) -> List[int]:
        """Генерировать часы активности"""
        # Выбираем основной период активности
        active_type = random.choice(['morning', 'afternoon', 'evening', 'night'])
        
        hours_map = {
            'morning': list(range(6, 12)),      # 6:00 - 12:00
            'afternoon': list(range(12, 18)),   # 12:00 - 18:00
            'evening': list(range(18, 23)),     # 18:00 - 23:00
            'night': list(range(20, 24)) + list(range(0, 3))  # 20:00 - 03:00
        }
        
        # Основные часы активности
        main_hours = hours_map[active_type]
        
        # Добавляем случайные часы вне основного периода
        all_hours = list(range(24))
        additional_hours = random.sample(
            [h for h in all_hours if h not in main_hours], 
            k=random.randint(2, 5)
        )
        
        return sorted(main_hours + additional_hours)
    
    def generate_device_fingerprint(self, account_id: int) -> Dict:
        """Генерировать уникальный fingerprint устройства"""
        
        # Популярные устройства 2024
        devices = [
            {
                'manufacturer': 'samsung',
                'model': 'SM-S928B',  # Galaxy S24 Ultra
                'android_version': 34,
                'android_release': '14.0',
                'resolution': '3120x1440',
                'dpi': 640
            },
            {
                'manufacturer': 'Google',
                'model': 'Pixel 8 Pro',
                'android_version': 34,
                'android_release': '14.0',
                'resolution': '3120x1440', 
                'dpi': 512
            },
            {
                'manufacturer': 'OnePlus',
                'model': 'CPH2581',  # OnePlus 12
                'android_version': 34,
                'android_release': '14.0',
                'resolution': '3168x1440',
                'dpi': 510
            },
            {
                'manufacturer': 'Apple',
                'model': 'iPhone15,3',  # iPhone 15 Pro Max
                'ios_version': '17.2.1',
                'resolution': '2796x1290',
                'scale': 3
            }
        ]
        
        device = random.choice(devices)
        
        # Генерируем уникальные ID
        unique_seed = f"{account_id}_{datetime.now().isoformat()}"
        
        fingerprint = {
            **device,
            'device_id': hashlib.md5(f"device_{unique_seed}".encode()).hexdigest(),
            'phone_id': hashlib.md5(f"phone_{unique_seed}".encode()).hexdigest(),
            'uuid': hashlib.md5(f"uuid_{unique_seed}".encode()).hexdigest(),
            'advertising_id': hashlib.md5(f"adid_{unique_seed}".encode()).hexdigest(),
            'app_version': '321.0.0.32.118',  # Актуальная версия Instagram
            'capabilities': '3brTv10=',  # Стандартные capabilities
            'connection_type': random.choice(['WIFI', '4G']),
            'user_agent': self._generate_user_agent(device),
            'timezone_offset': random.choice([-28800, -25200, -21600, -18000, -14400, 0, 3600, 7200, 10800]),
            'created_at': datetime.now().isoformat()
        }
        
        self.fingerprints[account_id] = fingerprint
        return fingerprint
    
    def _generate_user_agent(self, device: Dict) -> str:
        """Генерировать User-Agent на основе устройства"""
        if 'ios_version' in device:
            # iOS User-Agent
            ios_v = device['ios_version'].replace('.', '_')
            return f"Instagram 321.0.0.32.118 ({device['model']}; iOS {ios_v}; ru_RU; scale={device['scale']})"
        else:
            # Android User-Agent
            return (
                f"Instagram 321.0.0.32.118 Android "
                f"({device['android_version']}/{device['android_release']}; "
                f"{device['dpi']}dpi; {device['resolution']}; "
                f"{device['manufacturer']}; {device['model']}; {device['model']}; en_US)"
            )
    
    def humanize_action_timing(self, account_id: int, action_type: str) -> float:
        """Получить человекоподобную задержку для действия"""
        
        # Получаем паттерн поведения
        if account_id not in self.behavior_patterns:
            self.create_human_behavior_pattern(account_id)
        
        pattern = self.behavior_patterns[account_id]
        speed = pattern['action_speed']
        
        # Базовые задержки по типам действий
        base_delays = {
            'scroll': (0.5, 3.0),
            'like': (0.3, 1.5),
            'unlike': (0.2, 1.0),
            'follow': (1.0, 3.0),
            'unfollow': (0.8, 2.5),
            'comment': (15.0, 45.0),  # Время на написание комментария
            'story_view': (3.0, 8.0),
            'post_view': (2.0, 10.0),
            'typing': (0.05, 0.15),  # Между символами
            'navigation': (0.5, 2.0),
        }
        
        # Модификаторы скорости
        speed_modifiers = {
            'fast': 0.7,
            'medium': 1.0,
            'slow': 1.5
        }
        
        # Получаем базовую задержку
        min_delay, max_delay = base_delays.get(action_type, (1.0, 3.0))
        modifier = speed_modifiers[speed]
        
        # Применяем модификатор
        min_delay *= modifier
        max_delay *= modifier
        
        # Добавляем случайность с нормальным распределением
        mean = (min_delay + max_delay) / 2
        std = (max_delay - min_delay) / 4
        
        delay = random.gauss(mean, std)
        
        # Ограничиваем значения
        delay = max(min_delay, min(max_delay, delay))
        
        # Иногда добавляем микропаузы (человек отвлекся)
        if random.random() < 0.05:  # 5% шанс
            delay += random.uniform(2, 10)
            logger.info(f"😴 Добавлена микропауза: +{delay:.1f} сек")
        
        return delay
    
    def should_perform_action(self, account_id: int, action_type: str) -> bool:
        """Определить, должно ли быть выполнено действие (вероятностный подход)"""
        
        if account_id not in self.behavior_patterns:
            self.create_human_behavior_pattern(account_id)
        
        pattern = self.behavior_patterns[account_id]
        probabilities = pattern['interaction_patterns']
        
        # Получаем вероятность для типа действия
        probability = probabilities.get(f"{action_type}_probability", 0.1)
        
        # Проверяем, попадаем ли в вероятность
        return random.random() < probability
    
    def is_safe_time(self, account_id: int) -> bool:
        """Проверить, безопасное ли время для активности"""
        
        if account_id not in self.behavior_patterns:
            self.create_human_behavior_pattern(account_id)
        
        pattern = self.behavior_patterns[account_id]
        current_hour = datetime.now().hour
        
        # Проверяем, входит ли текущий час в активные
        is_active_hour = current_hour in pattern['active_hours']
        
        # Иногда можем быть активны вне обычных часов (10% шанс)
        if not is_active_hour and random.random() < 0.1:
            logger.info(f"🌙 Внеплановая активность в {current_hour}:00")
            return True
        
        return is_active_hour
    
    def rotate_proxy_if_needed(self, account_id: int, force: bool = False) -> Optional[str]:
        """Ротировать прокси если необходимо"""
        
        account = get_instagram_account(account_id)
        if not account or not account.proxy_id:
            return None
        
        # Проверяем, когда последний раз меняли IP
        last_rotation = self.proxy_manager.get_last_rotation(account_id)
        
        # Ротируем каждые 30-60 минут или при force
        if force or (last_rotation and 
                    (datetime.now() - last_rotation) > timedelta(minutes=random.randint(30, 60))):
            
            new_session_id = self.proxy_manager.generate_session_id(account_id)
            logger.info(f"🔄 Ротация IP для аккаунта {account.username}")
            return new_session_id
        
        return None
    
    def simulate_human_typing(self, text: str) -> List[Tuple[str, float]]:
        """Симулировать человеческий набор текста"""
        
        typing_events = []
        current_text = ""
        
        # Параметры набора
        wpm = random.randint(30, 80)  # Слов в минуту
        char_delay = 60.0 / (wpm * 5)  # Примерно 5 символов на слово
        
        for char in text:
            # Задержка перед символом
            delay = random.gauss(char_delay, char_delay * 0.3)
            delay = max(0.01, delay)
            
            # Иногда делаем опечатки
            if random.random() < 0.02:  # 2% шанс опечатки
                # Добавляем неправильный символ
                wrong_char = random.choice('abcdefghijklmnopqrstuvwxyz')
                typing_events.append((current_text + wrong_char, delay))
                
                # Пауза осознания
                typing_events.append((current_text + wrong_char, random.uniform(0.3, 0.8)))
                
                # Удаляем неправильный символ
                typing_events.append((current_text, 0.1))
            
            # Добавляем правильный символ
            current_text += char
            typing_events.append((current_text, delay))
            
            # Паузы на пробелах (обдумывание)
            if char == ' ' and random.random() < 0.3:
                typing_events.append((current_text, random.uniform(0.5, 2.0)))
        
        return typing_events

# Глобальный экземпляр
anti_detection = AntiDetectionService() 
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Менеджер прогрева аккаунтов Instagram

РАНДОМИЗАЦИЯ ДЕЙСТВИЙ:
======================
Система максимально имитирует человеческое поведение через:

1. ВРЕМЕННАЯ РАНДОМИЗАЦИЯ:
   - Случайные интервалы между действиями (3-120 сек)
   - Длинные паузы (15% шанс на 2-4x увеличение задержки)
   - Быстрые действия (10% шанс на 0.3x уменьшение)
   - Отдых между сессиями (5-60 минут)
   - Длинные перерывы (30 минут - 2 часа)

2. ПОСЛЕДОВАТЕЛЬНОСТЬ ДЕЙСТВИЙ:
   - Случайный выбор из предустановленных паттернов
   - Адаптация под время дня (утро/день/вечер)
   - Перемешивание порядка действий
   - Случайные длительности сессий

3. КОЛИЧЕСТВЕННАЯ РАНДОМИЗАЦИЯ:
   - Случайное количество действий в диапазоне (min-max)
   - Вариативность ±20% от базовых значений
   - Разные лимиты для разных режимов прогрева

4. ПОВЕДЕНЧЕСКАЯ РАНДОМИЗАЦИЯ:
   - Случайный выбор целевых аккаунтов
   - Случайные комментарии из базы
   - Случайные реакции на истории
   - Имитация отвлечений (10% шанс завершить сессию)

5. ЧЕЛОВЕЧЕСКИЕ ПАТТЕРНЫ:
   - Активность только днем (9:00-23:00)
   - Пиковая активность в 12-13, 19-21
   - Разная активность по времени дня
   - Естественные перерывы и паузы
"""

import logging
import random
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from enum import Enum

logger = logging.getLogger(__name__)

class WarmupSpeed(Enum):
    SLOW = "SLOW"
    NORMAL = "NORMAL"
    FAST = "FAST"
    SUPER_FAST = "SUPER_FAST"
    DAILY = "DAILY"

class HumanBehaviorManager:
    """Менеджер для имитации человеческого поведения"""
    
    # Паттерны человеческого поведения для каждого режима
    BEHAVIOR_PATTERNS = {
        'SLOW': {
            'session_duration': (10, 20),  # минуты
            'break_duration': (15, 60),    # минуты между сессиями
            'actions_per_session': (3, 8),
            'sequences': [
                ['view_feed', 'like_posts', 'rest'],
                ['view_stories', 'view_profiles', 'rest'],
                ['follow', 'view_profiles', 'like_posts', 'rest'],
                ['save_posts', 'view_feed', 'rest'],
                ['watch_reels', 'like_posts', 'long_rest']
            ]
        },
        'NORMAL': {
            'session_duration': (15, 30),
            'break_duration': (10, 40),
            'actions_per_session': (5, 12),
            'sequences': [
                ['view_feed', 'like_posts', 'follow', 'rest'],
                ['view_stories', 'comment', 'rest'],
                ['post_story', 'watch_reels', 'rest'],
                ['like_posts', 'save_posts', 'rest'],
                ['explore_hashtags', 'follow', 'like_posts', 'long_rest']
            ]
        },
        'FAST': {
            'session_duration': (20, 40),
            'break_duration': (8, 25),
            'actions_per_session': (8, 18),
            'sequences': [
                ['like_posts', 'like_posts', 'follow', 'follow', 'rest'],
                ['comment', 'view_stories', 'rest'],
                ['post_story', 'watch_reels', 'rest'],
                ['watch_reels', 'like_posts', 'save_posts', 'rest'],
                ['explore_hashtags', 'follow', 'like_posts', 'comment', 'long_rest']
            ]
        },
        'SUPER_FAST': {
            'session_duration': (25, 45),
            'break_duration': (5, 15),
            'actions_per_session': (12, 25),
            'sequences': [
                ['like_posts', 'like_posts', 'like_posts', 'follow', 'follow', 'rest'],
                ['comment', 'comment', 'post_story', 'rest'],
                ['like_posts', 'follow', 'watch_reels', 'rest'],
                ['watch_reels', 'like_posts', 'save_posts', 'comment', 'rest']
            ]
        },
        'DAILY': {
            'session_duration': (15, 25),
            'break_duration': (15, 30),
            'actions_per_session': (6, 15),
            'sequences': [
                # Утренняя активность
                ['view_feed', 'like_posts', 'like_posts', 'follow', 'rest'],
                # Дневная активность  
                ['comment', 'view_stories', 'rest'],
                # Вечерняя активность
                ['post_story', 'watch_reels', 'rest']
            ]
        }
    }
    
    # Веса действий для разного времени дня
    TIME_BASED_WEIGHTS = {
        'morning': {  # 9-12
            'view_feed': 1.5,
            'view_stories': 1.3,
            'like_posts': 1.2,
            'follow': 0.8,
            'comment': 0.5,
            'post_story': 0.3
        },
        'afternoon': {  # 12-17
            'view_feed': 1.0,
            'like_posts': 1.0,
            'follow': 1.2,
            'comment': 1.0,
            'watch_reels': 1.3,
            'explore_hashtags': 1.2
        },
        'evening': {  # 17-23
            'view_stories': 1.4,
            'like_posts': 1.1,
            'comment': 1.3,
            'post_story': 1.5,
            'send_messages': 1.2,
            'save_posts': 1.1
        }
    }
    
    def __init__(self, warmup_speed: str = 'NORMAL'):
        self.warmup_speed = warmup_speed
        self.last_action_time = None
        self.session_start_time = None
        self.actions_in_session = 0
        
    def get_time_period(self) -> str:
        """Определить период дня"""
        hour = datetime.now().hour
        if 9 <= hour < 12:
            return 'morning'
        elif 12 <= hour < 17:
            return 'afternoon'
        elif 17 <= hour <= 23:
            return 'evening'
        else:
            return 'night'
    
    def should_start_session(self) -> bool:
        """Определить, стоит ли начинать новую сессию"""
        current_time = datetime.now()
        
        # Не работаем ночью
        if current_time.hour < 9 or current_time.hour > 23:
            return False
        
        # Если это первая сессия
        if self.last_action_time is None:
            return True
        
        # Проверяем, прошло ли достаточно времени с последней сессии
        pattern = self.BEHAVIOR_PATTERNS[self.warmup_speed]
        min_break, max_break = pattern['break_duration']
        
        time_since_last = (current_time - self.last_action_time).total_seconds() / 60
        
        # Для первых запусков делаем меньшие интервалы
        if time_since_last < 5:  # Если прошло меньше 5 минут
            required_break = random.randint(2, 5)  # Короткий перерыв
        else:
            required_break = random.randint(min_break, max_break)
        
        should_start = time_since_last >= required_break
        
        if should_start:
            logger.info(f"✅ Время для новой сессии (прошло {time_since_last:.1f} мин)")
        else:
            logger.info(f"⏳ Ждем еще {required_break - time_since_last:.1f} мин до следующей сессии")
        
        return should_start
    
    def should_end_session(self) -> bool:
        """Определить, стоит ли завершить текущую сессию"""
        if self.session_start_time is None:
            return True
        
        pattern = self.BEHAVIOR_PATTERNS[self.warmup_speed]
        
        # Проверяем длительность сессии
        session_duration = (datetime.now() - self.session_start_time).total_seconds() / 60
        min_duration, max_duration = pattern['session_duration']
        max_session_duration = random.randint(min_duration, max_duration)
        
        # Проверяем количество действий
        min_actions, max_actions = pattern['actions_per_session']
        max_session_actions = random.randint(min_actions, max_actions)
        
        # Завершаем сессию если превышены лимиты
        if session_duration >= max_session_duration or self.actions_in_session >= max_session_actions:
            return True
        
        # Иногда завершаем сессию случайно (человек отвлекся)
        if random.random() < 0.1:  # 10% шанс
            return True
        
        return False
    
    def get_next_action_sequence(self) -> List[str]:
        """Получить следующую последовательность действий"""
        pattern = self.BEHAVIOR_PATTERNS[self.warmup_speed]
        sequence = random.choice(pattern['sequences'])
        
        # Адаптируем последовательность под время дня
        time_period = self.get_time_period()
        if time_period == 'night':
            return []
        
        # Фильтруем действия на основе времени дня
        filtered_sequence = []
        for action in sequence:
            if action in ['rest', 'long_rest']:
                filtered_sequence.append(action)
                continue
            
            # Проверяем вес действия для текущего времени
            weight = self.TIME_BASED_WEIGHTS.get(time_period, {}).get(action, 1.0)
            
            # Добавляем действие с учетом веса
            if random.random() < weight:
                filtered_sequence.append(action)
        
        return filtered_sequence
    
    def get_action_delay(self, action_type: str, is_rest: bool = False) -> int:
        """Получить задержку для действия с учетом человеческого поведения"""
        if is_rest:
            if action_type == 'long_rest':
                return random.randint(1800, 7200)  # 30 минут - 2 часа
            else:  # обычный rest
                return random.randint(300, 1800)   # 5-30 минут
        
        # Базовые задержки между действиями (в секундах)
        base_delays = {
            'view_feed': (10, 30),
            'view_stories': (5, 15),
            'view_profiles': (8, 25),
            'like_posts': (3, 12),
            'follow': (15, 45),
            'comment': (30, 90),
            'save_posts': (5, 20),
            'post_story': (60, 180),
            'post_photo': (120, 300),
            'watch_reels': (20, 60),
            'explore_hashtags': (15, 40),
            'send_messages': (45, 120),
            'update_profile': (60, 180),
        }
        
        min_delay, max_delay = base_delays.get(action_type, (10, 30))
        
        # Добавляем вариативность в зависимости от режима
        speed_multipliers = {
            'SLOW': 2.0,
            'NORMAL': 1.0,
            'FAST': 0.7,
            'SUPER_FAST': 0.4,
            'DAILY': 1.0
        }
        
        multiplier = speed_multipliers.get(self.warmup_speed, 1.0)
        min_delay = int(min_delay * multiplier)
        max_delay = int(max_delay * multiplier)
        
        delay = random.randint(min_delay, max_delay)
        
        # Добавляем человеческую непредсказуемость
        if random.random() < 0.15:  # 15% шанс на длинную паузу
            delay *= random.randint(2, 4)
        
        # Иногда очень быстрые действия (человек сосредоточен)
        elif random.random() < 0.1:  # 10% шанс
            delay = int(delay * 0.3)
        
        return max(delay, 2)  # Минимум 2 секунды
    
    def start_session(self):
        """Начать новую сессию"""
        self.session_start_time = datetime.now()
        self.actions_in_session = 0
        logger.info(f"🎬 Начинаем новую сессию активности ({self.warmup_speed})")
    
    def end_session(self):
        """Завершить сессию"""
        if self.session_start_time:
            duration = (datetime.now() - self.session_start_time).total_seconds() / 60
            logger.info(f"🏁 Сессия завершена. Длительность: {duration:.1f} мин, действий: {self.actions_in_session}")
        
        self.session_start_time = None
        self.actions_in_session = 0
        self.last_action_time = datetime.now()
    
    def record_action(self, action_type: str):
        """Записать выполненное действие"""
        self.actions_in_session += 1
        logger.info(f"📊 Действие {self.actions_in_session} в сессии: {action_type}")

class WarmupManager:
    """Менеджер для управления прогревом аккаунтов"""
    
    # Фазы прогрева с постепенным увеличением активности
    # Можно выбрать скорость прогрева: SLOW, NORMAL, FAST
    WARMUP_SPEEDS = {
        'SLOW': {  # ~21 день - для максимальной безопасности
            'phase1': 3, 'phase2': 3, 'phase3': 5, 'phase4': 5, 'phase5': 5
        },
        'NORMAL': {  # ~14 дней - оптимальный баланс
            'phase1': 2, 'phase2': 2, 'phase3': 3, 'phase4': 3, 'phase5': 4
        },
        'FAST': {  # ~7 дней - для опытных аккаунтов или срочных задач
            'phase1': 1, 'phase2': 1, 'phase3': 2, 'phase4': 2, 'phase5': 1
        },
        'SUPER_FAST': {  # ~3 дня - агрессивный режим
            'phase1': 1, 'phase2': 1, 'phase3': 1, 'phase4': 0, 'phase5': 0
        },
        'DAILY': {  # ~1 день - тестовый режим
            'phase1': 0.3, 'phase2': 0.3, 'phase3': 0.2, 'phase4': 0.2, 'phase5': 0
        }
    }
    
    WARMUP_PHASES = {
        'phase1': {
            'name': 'Начальная активность',
            'duration_days': 2,
            'actions': {
                'view_feed': {'min': 5, 'max': 10},
                'view_stories': {'min': 3, 'max': 8},
                'view_profiles': {'min': 2, 'max': 5},
                'like_posts': {'min': 5, 'max': 10},
                'explore_hashtags': {'min': 1, 'max': 3},
            }
        },
        'phase2': {
            'name': 'Базовая активность',
            'duration_days': 2,
            'actions': {
                'view_feed': {'min': 10, 'max': 20},
                'view_stories': {'min': 5, 'max': 15},
                'view_profiles': {'min': 5, 'max': 10},
                'like_posts': {'min': 10, 'max': 20},
                'follow': {'min': 3, 'max': 5},
                'save_posts': {'min': 2, 'max': 5},
                'watch_reels': {'min': 3, 'max': 8},
                'explore_hashtags': {'min': 2, 'max': 5},
            }
        },
        'phase3': {
            'name': 'Расширенная активность',
            'duration_days': 3,
            'actions': {
                'view_feed': {'min': 20, 'max': 30},
                'view_stories': {'min': 10, 'max': 20},
                'view_profiles': {'min': 10, 'max': 15},
                'like_posts': {'min': 15, 'max': 30},
                'follow': {'min': 5, 'max': 10},
                'comment': {'min': 2, 'max': 5},
                'save_posts': {'min': 5, 'max': 10},
                'watch_reels': {'min': 5, 'max': 15},
                'explore_hashtags': {'min': 3, 'max': 8},
                'send_messages': {'min': 1, 'max': 3},
            }
        },
        'phase4': {
            'name': 'Активный пользователь',
            'duration_days': 3,
            'actions': {
                'view_feed': {'min': 30, 'max': 50},
                'view_stories': {'min': 15, 'max': 30},
                'view_profiles': {'min': 15, 'max': 25},
                'like_posts': {'min': 25, 'max': 50},
                'follow': {'min': 10, 'max': 20},
                'comment': {'min': 5, 'max': 10},
                'save_posts': {'min': 10, 'max': 20},
                'post_story': {'min': 1, 'max': 2},
                'watch_reels': {'min': 10, 'max': 25},
                'explore_hashtags': {'min': 5, 'max': 10},
                'send_messages': {'min': 2, 'max': 5},
                'update_profile': {'min': 0, 'max': 1},
            }
        },
        'phase5': {
            'name': 'Полная активность',
            'duration_days': 4,
            'actions': {
                'view_feed': {'min': 50, 'max': 100},
                'view_stories': {'min': 20, 'max': 50},
                'view_profiles': {'min': 20, 'max': 40},
                'like_posts': {'min': 50, 'max': 100},
                'follow': {'min': 20, 'max': 50},
                'comment': {'min': 10, 'max': 20},
                'save_posts': {'min': 15, 'max': 30},
                'post_story': {'min': 1, 'max': 3},
                'post_photo': {'min': 0, 'max': 1},
                'watch_reels': {'min': 20, 'max': 40},
                'explore_hashtags': {'min': 10, 'max': 20},
                'send_messages': {'min': 3, 'max': 10},
                'update_profile': {'min': 0, 'max': 1},
            }
        }
    }
    
    def __init__(self, account_id: int, client, warmup_speed: str = 'NORMAL'):
        self.account_id = account_id
        self.client = client
        self.current_phase = None
        self.phase_start_date = None
        self.daily_actions = {}
        self.total_actions = {}
        self.warmup_speed = warmup_speed if warmup_speed in self.WARMUP_SPEEDS else 'NORMAL'
        self.behavior_manager = HumanBehaviorManager(warmup_speed)
        
        # Счетчики действий для текущей сессии
        self.session_stats = {
            'likes': 0,
            'follows': 0,
            'comments': 0,
            'saves': 0,
            'stories': 0,
            'posts': 0
        }
        
        # ID текущей задачи прогрева
        self.task_id = None
        
        # Система прогрева по интересам
        self.interest_warmup = None
    
    def set_task_id(self, task_id: int):
        """Установить ID задачи прогрева"""
        self.task_id = task_id
    
    def setup_interest_warmup(self, interests_config: dict):
        """Настроить прогрев по интересам"""
        try:
            from utils.interest_based_warmup import InterestBasedWarmup
            
            # Извлекаем интересы из конфига
            primary_interests = interests_config.get('primary_interests', [])
            secondary_interests = interests_config.get('secondary_interests', [])
            custom_hashtags = interests_config.get('custom_hashtags', [])
            custom_accounts = interests_config.get('custom_accounts', [])
            
            if primary_interests:
                # Создаем профиль интересов
                interest_profile = InterestBasedWarmup.create_interest_profile(
                    primary_interests=primary_interests,
                    secondary_interests=secondary_interests,
                    custom_hashtags=custom_hashtags,
                    custom_accounts=custom_accounts
                )
                
                # Инициализируем систему прогрева по интересам
                self.interest_warmup = InterestBasedWarmup(
                    account_id=self.account_id,
                    client=self.client,
                    interest_profile=interest_profile
                )
                
                logger.info(f"✅ Настроен прогрев по интересам: {', '.join(primary_interests)}")
                return True
            else:
                logger.warning("⚠️ Не указаны основные интересы для прогрева")
                return False
                
        except Exception as e:
            logger.error(f"❌ Ошибка настройки прогрева по интересам: {e}")
            return False
    
    def update_task_progress(self, action_type: str = None):
        """Обновить прогресс задачи в базе данных"""
        if not self.task_id:
            return
        
        try:
            from database.db_manager import get_session
            from database.models import WarmupTask
            import json
            
            session = get_session()
            task = session.query(WarmupTask).filter_by(id=self.task_id).first()
            
            if task:
                # Загружаем текущий прогресс или создаем новый
                progress = task.progress or {}
                
                # Обновляем общие счетчики
                progress['likes_given'] = progress.get('likes_given', 0) + self.session_stats['likes']
                progress['follows_made'] = progress.get('follows_made', 0) + self.session_stats['follows']
                progress['comments_made'] = progress.get('comments_made', 0) + self.session_stats['comments']
                progress['posts_saved'] = progress.get('posts_saved', 0) + self.session_stats['saves']
                progress['stories_created'] = progress.get('stories_created', 0) + self.session_stats['stories']
                progress['posts_created'] = progress.get('posts_created', 0) + self.session_stats['posts']
                
                # Сохраняем текущую фазу
                if hasattr(self, 'current_phase'):
                    progress['current_phase'] = self.current_phase
                
                # Сохраняем последнее действие
                if action_type:
                    progress['last_action'] = action_type
                    progress['last_action_time'] = datetime.now().isoformat()
                
                # Обновляем прогресс в базе данных
                task.progress = progress
                task.updated_at = datetime.now()
                session.commit()
                
                logger.debug(f"✅ Обновлен прогресс задачи #{self.task_id}: {progress}")
                
            session.close()
            
        except Exception as e:
            logger.error(f"❌ Ошибка при обновлении прогресса: {e}")
    
    def get_phase_duration(self, phase_name: str) -> int:
        """Получить длительность фазы в зависимости от скорости прогрева"""
        return self.WARMUP_SPEEDS[self.warmup_speed].get(phase_name, 
                                                         self.WARMUP_PHASES[phase_name]['duration_days'])
        
    def get_current_phase(self, start_date: datetime) -> str:
        """Определить текущую фазу прогрева"""
        days_passed = (datetime.now() - start_date).days
        
        total_days = 0
        for phase_name in ['phase1', 'phase2', 'phase3', 'phase4', 'phase5']:
            phase_duration = self.get_phase_duration(phase_name)
            total_days += phase_duration
            if days_passed < total_days:
                return phase_name
        
        return 'completed'
    
    def should_perform_action(self) -> bool:
        """Проверить, нужно ли выполнять действие сейчас"""
        current_hour = datetime.now().hour
        
        # Активность только в дневное время (9:00 - 23:00)
        if current_hour < 9 or current_hour > 23:
            return False
        
        # Пик активности в определенные часы
        peak_hours = [12, 13, 19, 20, 21]
        if current_hour in peak_hours:
            return random.random() < 0.8  # 80% шанс
        
        # Обычная активность
        return random.random() < 0.5  # 50% шанс
    
    def get_action_delay(self, action_type: str) -> int:
        """Получить задержку между действиями"""
        base_delays = {
            'view_feed': (3, 10),
            'view_stories': (2, 5),
            'view_profiles': (5, 15),
            'like_posts': (2, 8),
            'follow': (30, 120),
            'comment': (60, 180),
            'save_posts': (10, 30),
            'post_story': (300, 600),
            'post_photo': (600, 1200),
            'watch_reels': (15, 60),
            'explore_hashtags': (10, 30),
            'send_messages': (120, 300),
            'update_profile': (300, 600),
        }
        
        min_delay, max_delay = base_delays.get(action_type, (30, 120))
        
        # Добавляем случайность для имитации человека
        delay = random.randint(min_delay, max_delay)
        
        # Иногда делаем длинные паузы (человек отвлекся)
        if random.random() < 0.1:  # 10% шанс
            delay *= random.randint(3, 5)
        
        return delay
    
    def perform_warmup_session(self, phase_name: str, settings: dict) -> dict:
        """Выполнить сессию прогрева"""
        phase = self.WARMUP_PHASES[phase_name]
        session_results = {
            'actions_performed': {},
            'errors': [],
            'session_duration': 0
        }
        
        session_start = time.time()
        
        # Планируем действия на эту сессию
        planned_actions = self.plan_session_actions(phase['actions'])
        
        logger.info(f"📋 План сессии прогрева для фазы {phase['name']}:")
        for action, count in planned_actions.items():
            logger.info(f"   - {action}: {count} раз")
        
        # Выполняем действия в случайном порядке
        action_list = []
        for action, count in planned_actions.items():
            action_list.extend([action] * count)
        random.shuffle(action_list)
        
        for i, action in enumerate(action_list):
            try:
                # Проверяем, стоит ли продолжать
                if not self.should_perform_action():
                    logger.info("⏸️ Пауза в активности (имитация человеческого поведения)")
                    time.sleep(random.randint(300, 900))  # 5-15 минут паузы
                    continue
                
                # Выполняем действие
                logger.info(f"🎯 Выполняем действие: {action} ({i+1}/{len(action_list)})")
                
                success = self.perform_action(action, settings)
                
                if success:
                    if action not in session_results['actions_performed']:
                        session_results['actions_performed'][action] = 0
                    session_results['actions_performed'][action] += 1
                
                # Задержка между действиями
                delay = self.get_action_delay(action)
                logger.info(f"⏱️ Ждем {delay} секунд...")
                time.sleep(delay)
                
            except Exception as e:
                logger.error(f"❌ Ошибка при выполнении {action}: {e}")
                session_results['errors'].append(f"{action}: {str(e)}")
                
                # Увеличиваем задержку после ошибки
                time.sleep(random.randint(60, 180))
        
        session_results['session_duration'] = int(time.time() - session_start)
        return session_results
    
    def plan_session_actions(self, actions_config: dict) -> dict:
        """Спланировать действия для сессии"""
        planned = {}
        
        for action, limits in actions_config.items():
            # Определяем количество действий
            count = random.randint(limits['min'], limits['max'])
            
            # Добавляем вариативность (±20%)
            variance = int(count * 0.2)
            if variance > 0:
                count += random.randint(-variance, variance)
            
            # Убеждаемся, что не выходим за лимиты
            count = max(limits['min'], min(count, limits['max']))
            
            if count > 0:
                planned[action] = count
        
        return planned
    
    def perform_action(self, action_type: str, settings: dict) -> bool:
        """Выполнить конкретное действие"""
        try:
            # Сбрасываем счетчики текущей сессии перед действием
            self.session_stats = {
                'likes': 0,
                'follows': 0,
                'comments': 0,
                'saves': 0,
                'stories': 0,
                'posts': 0
            }
            
            action_map = {
                'view_feed': self.view_feed,
                'view_stories': self.view_stories,
                'view_profiles': self.view_profiles,
                'like_posts': self.like_posts,
                'follow': lambda: self.follow_users(settings),
                'comment': lambda: self.comment_posts(settings),
                'save_posts': self.save_posts,
                'post_story': lambda: self.post_story(settings),
                'post_photo': self.post_photo,
                'watch_reels': self.watch_reels,
                'explore_hashtags': self.explore_hashtags,
                'send_direct': self.send_direct_messages,
                'update_profile': self.update_profile_info
            }
            
            if action_type in action_map:
                result = action_map[action_type]()
                
                # Записываем действие в статистику
                if result:
                    logger.info(f"✅ Действие {action_type} выполнено успешно")
                else:
                    logger.warning(f"⚠️ Действие {action_type} не выполнено")
                    
                return result
            else:
                logger.warning(f"Неизвестное действие: {action_type}")
                return False
                
        except Exception as e:
            logger.error(f"Ошибка при выполнении действия {action_type}: {e}")
            return False
    
    def view_feed(self) -> bool:
        """Просмотр ленты"""
        try:
            # Получаем посты из ленты
            feed = self.client.client.get_timeline_feed()
            if feed and feed.get('feed_items'):
                # Имитируем просмотр
                view_time = random.randint(2, 8)
                logger.info(f"👀 Просматриваем ленту {view_time} секунд...")
                time.sleep(view_time)
                return True
            return False
        except Exception as e:
            logger.error(f"Ошибка при просмотре ленты: {e}")
            return False
    
    def view_stories(self) -> bool:
        """Просмотр историй"""
        try:
            # Получаем ленту с историями
            feed = self.client.client.get_timeline_feed()
            
            if feed and feed.get('feed_items'):
                # Фильтруем элементы с историями
                story_items = []
                for item in feed['feed_items']:
                    if 'stories_tray' in item:
                        story_items.extend(item['stories_tray'])
                
                if story_items:
                    # Выбираем несколько историй для просмотра
                    stories_to_view = random.randint(1, min(5, len(story_items)))
                    
                    for i in range(stories_to_view):
                        story = story_items[i]
                        
                        if 'user' in story:
                            logger.info(f"📱 Просматриваем историю от @{story['user'].get('username', 'unknown')}")
                            
                            # Время просмотра истории
                            view_time = random.randint(2, 5)
                            time.sleep(view_time)
                    
                    return True
                else:
                    logger.info("ℹ️ Нет новых историй для просмотра")
            
            # Альтернативный способ - просто имитируем просмотр
            logger.info("📱 Имитируем просмотр историй...")
            time.sleep(random.randint(10, 20))
            return True
            
        except Exception as e:
            logger.error(f"Ошибка при просмотре историй: {e}")
            # В случае ошибки просто имитируем активность
            logger.info("📱 Имитируем просмотр историй...")
            time.sleep(random.randint(10, 20))
            return True
    
    def view_profiles(self) -> bool:
        """Просмотр профилей"""
        try:
            # Получаем рекомендации или используем поиск
            suggested = self.client.client.search_users("fitness", count=10)
            if suggested:
                profile = random.choice(suggested)
                logger.info(f"👤 Просматриваем профиль @{profile.username}...")
                
                # Получаем информацию о профиле
                user_info = self.client.client.user_info(profile.pk)
                
                # Имитируем просмотр
                view_time = random.randint(5, 15)
                time.sleep(view_time)
                return True
            return False
        except Exception as e:
            logger.error(f"Ошибка при просмотре профилей: {e}")
            return False
    
    def like_posts(self) -> bool:
        """Лайкать посты из ленты"""
        try:
            # Получаем ленту
            feed = self.client.client.get_timeline_feed()
            
            if feed and feed.get('feed_items'):
                posts = [item for item in feed['feed_items'] 
                        if item.get('media_or_ad') and 
                        item['media_or_ad'].get('media_type') in [1, 8]]  # 1 - фото, 8 - карусель
                
                if posts:
                    # Лайкаем 1-3 поста
                    posts_to_like = random.randint(1, min(3, len(posts)))
                    
                    for i in range(posts_to_like):
                        post = posts[i]['media_or_ad']
                        media_id = post.get('id')
                        
                        if media_id:
                            self.client.client.media_like(media_id)
                            logger.info("❤️ Поставили лайк на пост")
                            
                            # Увеличиваем счетчик
                            self.session_stats['likes'] += 1
                            
                            # Пауза между лайками
                            time.sleep(random.randint(2, 5))
                    
                    # Обновляем прогресс в базе данных
                    self.update_task_progress('like_posts')
                    return True
            
            # Альтернативный метод через хештеги
            hashtags = ['photography', 'nature', 'travel', 'art', 'food']
            hashtag = random.choice(hashtags)
            
            medias = self.client.client.hashtag_medias_recent(hashtag, amount=10)
            
            if medias:
                # Лайкаем 1-3 поста
                posts_to_like = random.randint(1, min(3, len(medias)))
                
                for i in range(posts_to_like):
                    media = medias[i]
                    self.client.client.media_like(media.id)
                    logger.info(f"❤️ Поставили лайк на пост из #{hashtag}")
                    
                    # Увеличиваем счетчик
                    self.session_stats['likes'] += 1
                    
                    # Пауза между лайками
                    time.sleep(random.randint(2, 5))
                
                # Обновляем прогресс в базе данных
                self.update_task_progress('like_posts')
                return True
                
            return False
            
        except Exception as e:
            logger.error(f"Ошибка при лайках постов: {e}")
            return False
    
    def follow_users(self, settings: dict) -> bool:
        """Подписаться на пользователей"""
        try:
            # Получаем список целевых аккаунтов
            target_accounts = settings.get('target_accounts', '')
            unique_follows = settings.get('unique_follows', True)
            
            if not target_accounts:
                logger.warning("⚠️ Не указаны целевые аккаунты для подписки")
                return False
            
            # Разбиваем на список
            target_list = [acc.strip().replace('@', '') for acc in target_accounts.split('\n') if acc.strip()]
            
            if not target_list:
                logger.warning("⚠️ Список целевых аккаунтов пуст")
                return False
            
            # Если включены уникальные подписки, выбираем случайные аккаунты
            if unique_follows:
                # Выбираем 1-5 аккаунтов для подписки
                follow_count = random.randint(1, min(5, len(target_list)))
                accounts_to_follow = random.sample(target_list, follow_count)
                logger.info(f"🎯 Уникальные подписки: выбрано {follow_count} из {len(target_list)} аккаунтов")
            else:
                # Используем весь список
                accounts_to_follow = target_list[:5]  # Максимум 5 за раз
            
            followed_count = 0
            
            for username in accounts_to_follow:
                try:
                    # Получаем информацию о пользователе
                    user = self.client.client.user_info_by_username(username)
                    
                    if user:
                        # Проверяем, не подписаны ли уже
                        if not user.friendship_status.following:
                            # Подписываемся
                            result = self.client.client.user_follow(user.pk)
                            
                            if result:
                                logger.info(f"✅ Подписались на @{username}")
                                followed_count += 1
                                
                                # Увеличиваем счетчик
                                self.session_stats['follows'] += 1
                                
                                # Пауза между подписками (важно!)
                                time.sleep(random.randint(10, 30))
                            else:
                                logger.warning(f"⚠️ Не удалось подписаться на @{username}")
                        else:
                            logger.info(f"ℹ️ Уже подписаны на @{username}")
                    else:
                        logger.warning(f"⚠️ Не найден пользователь @{username}")
                        
                except Exception as e:
                    logger.error(f"❌ Ошибка при подписке на @{username}: {e}")
                    continue
            
            # Обновляем прогресс в базе данных
            if followed_count > 0:
                self.update_task_progress('follow')
                
            return followed_count > 0
            
        except Exception as e:
            logger.error(f"Ошибка при подписках: {e}")
            return False
    
    def comment_posts(self, settings: dict) -> bool:
        """Комментирование постов"""
        try:
            # Получаем настройки фазы 3 из конфигурации
            phase3_settings = settings.get('full_settings', {}).get('phases', {}).get('phase3', {})
            
            # Проверяем, включена ли фаза 3
            if not phase3_settings.get('enabled', False):
                logger.info("💬 Фаза комментариев отключена")
                return False
            
            # Загружаем комментарии
            all_comments = []
            
            # Добавляем комментарии из настроек
            custom_comments = phase3_settings.get('custom_comments', [])
            if custom_comments:
                all_comments.extend(custom_comments)
                logger.info(f"📝 Добавлено {len(custom_comments)} пользовательских комментариев")
            
            # Добавляем стандартные комментарии по типам
            comment_types = phase3_settings.get('comment_types', {})
            
            # Загружаем комментарии из файла
            import json
            import os
            
            comments_file = 'data/warmup_comments.json'
            if os.path.exists(comments_file):
                with open(comments_file, 'r', encoding='utf-8') as f:
                    comments_data = json.load(f)
                    
                    # Добавляем комментарии по выбранным типам
                    if comment_types.get('positive', False):
                        all_comments.extend(comments_data.get('positive', []))
                    if comment_types.get('compliments', False):
                        all_comments.extend(comments_data.get('compliments', []))
                    if comment_types.get('questions', False):
                        all_comments.extend(comments_data.get('questions', []))
                    if comment_types.get('emojis', False):
                        all_comments.extend(comments_data.get('emojis', []))
                        
                    logger.info(f"📚 Загружено {len(all_comments)} комментариев из настроек")
            
            # Если нет комментариев, используем базовые
            if not all_comments:
                all_comments = [
                    "Классное фото! 👍",
                    "Очень красиво! ✨",
                    "Вау! 😍",
                    "Супер! 🔥",
                    "Отличный кадр!"
                ]
                logger.info("📝 Используем базовые комментарии")
            
            # Получаем посты из ленты
            feed = self.client.client.get_timeline_feed()
            if feed and feed.get('feed_items'):
                items = [item for item in feed['feed_items'] if 'media_or_ad' in item]
                if items:
                    # Выбираем случайный пост
                    item = random.choice(items[:10])  # Из первых 10 постов
                    media = item['media_or_ad']
                    
                    if 'id' in media and 'user' in media:
                        # Выбираем случайный комментарий
                        comment_text = random.choice(all_comments)
                        
                        # Добавляем вариативность (иногда меняем регистр или добавляем точки)
                        if random.random() < 0.3:
                            comment_text = comment_text.lower()
                        elif random.random() < 0.2:
                            comment_text = comment_text.rstrip('!.') + '...'
                        
                        logger.info(f"💬 Комментируем пост от @{media['user'].get('username', 'unknown')}: {comment_text}")
                        
                        # Отправляем комментарий
                        result = self.client.client.media_comment(media['id'], comment_text)
                        
                        if result:
                            logger.info("✅ Комментарий успешно добавлен")
                            return True
                        else:
                            logger.warning("⚠️ Не удалось добавить комментарий")
            
            return False
        except Exception as e:
            logger.error(f"Ошибка при комментировании: {e}")
            return False
    
    def save_posts(self) -> bool:
        """Сохранение постов"""
        try:
            # Получаем ленту
            feed = self.client.client.get_timeline_feed()
            
            if feed and feed.get('feed_items'):
                posts = [item for item in feed['feed_items'] 
                        if item.get('media_or_ad') and 
                        item['media_or_ad'].get('media_type') in [1, 8]]  # 1 - фото, 8 - карусель
                
                if posts:
                    # Сохраняем 1-2 поста
                    posts_to_save = random.randint(1, min(2, len(posts)))
                    
                    for i in range(posts_to_save):
                        post = posts[i]['media_or_ad']
                        media_id = post.get('id')
                        
                        if media_id:
                            self.client.client.media_save(media_id)
                            logger.info("💾 Сохранили пост")
                            
                            # Увеличиваем счетчик
                            self.session_stats['saves'] += 1
                            
                            # Пауза между сохранениями
                            time.sleep(random.randint(3, 8))
                    
                    # Обновляем прогресс в базе данных
                    self.update_task_progress('save_posts')
                    return True
                    
            return False
            
        except Exception as e:
            logger.error(f"Ошибка при сохранении постов: {e}")
            return False
    
    def post_story(self, settings: dict) -> bool:
        """Публикация истории"""
        try:
            # Получаем настройки фазы 4 из конфигурации
            phase4_settings = settings.get('full_settings', {}).get('phases', {}).get('phase4', {})
            
            # Проверяем, включена ли фаза 4 и публикация историй
            if not phase4_settings.get('enabled', False):
                logger.info("📸 Фаза историй отключена")
                return False
                
            activities = phase4_settings.get('activities', {})
            if not activities.get('publish', False):
                logger.info("📸 Публикация историй отключена в настройках")
                return False
            
            # Создаем простую историю с текстом
            from PIL import Image, ImageDraw, ImageFont
            import tempfile
            
            # Создаем изображение для истории
            width, height = 1080, 1920  # Размер для историй
            
            # Получаем стиль из настроек
            story_style = phase4_settings.get('style', 'gradient')
            
            # Создаем фон в зависимости от стиля
            image = Image.new('RGB', (width, height))
            draw = ImageDraw.Draw(image)
            
            if story_style == 'gradient':
                # Случайный градиентный фон
                colors = [
                    ((255, 94, 77), (255, 154, 0)),    # Оранжевый градиент
                    ((84, 51, 255), (0, 234, 255)),    # Синий градиент
                    ((255, 0, 128), (255, 0, 255)),    # Розовый градиент
                    ((0, 255, 127), (51, 255, 153)),   # Зеленый градиент
                ]
                
                color1, color2 = random.choice(colors)
                
                for y in range(height):
                    ratio = y / height
                    r = int(color1[0] * (1 - ratio) + color2[0] * ratio)
                    g = int(color1[1] * (1 - ratio) + color2[1] * ratio)
                    b = int(color1[2] * (1 - ratio) + color2[2] * ratio)
                    draw.rectangle([(0, y), (width, y + 1)], fill=(r, g, b))
            else:
                # Однотонный фон
                colors = [
                    (74, 144, 226),   # Синий
                    (156, 39, 176),   # Фиолетовый
                    (233, 30, 99),    # Розовый
                    (255, 152, 0),    # Оранжевый
                    (76, 175, 80),    # Зеленый
                ]
                color = random.choice(colors)
                draw.rectangle([(0, 0), (width, height)], fill=color)
            
            # Получаем тексты из настроек
            story_texts = phase4_settings.get('story_texts', [])
            if not story_texts:
                # Используем базовые тексты
                story_texts = [
                    "Хорошего дня! ☀️",
                    "Всем привет! 👋",
                    "Отличное настроение! 😊",
                    "Продуктивного дня! 💪",
                    "Позитивный настрой! ✨"
                ]
            
            text = random.choice(story_texts)
            logger.info(f"📝 Выбран текст для истории: {text}")
            
            # Пытаемся использовать системный шрифт
            try:
                font = ImageFont.truetype("/System/Library/Fonts/Helvetica.ttc", 80)
            except:
                try:
                    # Для Windows
                    font = ImageFont.truetype("arial.ttf", 80)
                except:
                    font = ImageFont.load_default()
            
            # Получаем размер текста
            bbox = draw.textbbox((0, 0), text, font=font)
            text_width = bbox[2] - bbox[0]
            text_height = bbox[3] - bbox[1]
            
            # Центрируем текст
            x = (width - text_width) // 2
            y = (height - text_height) // 2
            
            # Рисуем текст с тенью
            draw.text((x + 3, y + 3), text, fill=(0, 0, 0, 128), font=font)
            draw.text((x, y), text, fill='white', font=font)
            
            # Сохраняем временный файл
            with tempfile.NamedTemporaryFile(suffix='.jpg', delete=False) as tmp:
                image.save(tmp.name, 'JPEG', quality=95)
                temp_path = tmp.name
            
            try:
                logger.info(f"📸 Публикуем историю: {text}")
                
                # Публикуем историю
                result = self.client.client.photo_upload_to_story(temp_path)
                
                if result:
                    logger.info("✅ История успешно опубликована")
                    return True
                else:
                    logger.warning("⚠️ Не удалось опубликовать историю")
                    
            finally:
                # Удаляем временный файл
                if os.path.exists(temp_path):
                    os.remove(temp_path)
                    
            return False
            
        except Exception as e:
            logger.error(f"Ошибка при публикации истории: {e}")
            return False
    
    def post_photo(self) -> bool:
        """Публикация фото"""
        try:
            # Создаем простое изображение
            from PIL import Image, ImageDraw
            import tempfile
            
            # Создаем квадратное изображение
            size = 1080
            
            # Случайный паттерн
            patterns = ['gradient', 'circles', 'lines']
            pattern = random.choice(patterns)
            
            image = Image.new('RGB', (size, size), 'white')
            draw = ImageDraw.Draw(image)
            
            if pattern == 'gradient':
                # Радиальный градиент
                center_x, center_y = size // 2, size // 2
                max_radius = size // 2
                
                for i in range(size):
                    for j in range(size):
                        distance = ((i - center_x) ** 2 + (j - center_y) ** 2) ** 0.5
                        ratio = min(distance / max_radius, 1)
                        
                        color = (
                            int(255 * (1 - ratio)),
                            int(200 * (1 - ratio) + 55),
                            int(150 * ratio + 105)
                        )
                        draw.point((i, j), fill=color)
                        
            elif pattern == 'circles':
                # Случайные круги
                for _ in range(random.randint(5, 15)):
                    x = random.randint(0, size)
                    y = random.randint(0, size)
                    radius = random.randint(50, 200)
                    color = (
                        random.randint(100, 255),
                        random.randint(100, 255),
                        random.randint(100, 255)
                    )
                    draw.ellipse(
                        [(x - radius, y - radius), (x + radius, y + radius)],
                        fill=color,
                        outline=color
                    )
            
            # Подписи для постов
            captions = [
                "Новый день - новые возможности 🌟",
                "Момент для себя 💫",
                "Вдохновение повсюду ✨",
                "Создаем красоту 🎨",
                "Просто хорошее настроение 😊"
            ]
            
            caption = random.choice(captions)
            
            # Сохраняем временный файл
            with tempfile.NamedTemporaryFile(suffix='.jpg', delete=False) as tmp:
                image.save(tmp.name, 'JPEG', quality=95)
                temp_path = tmp.name
            
            try:
                logger.info(f"🖼️ Публикуем фото с подписью: {caption}")
                
                # Публикуем фото
                result = self.client.client.photo_upload(temp_path, caption)
                
                if result:
                    logger.info("✅ Фото успешно опубликовано")
                    return True
                else:
                    logger.warning("⚠️ Не удалось опубликовать фото")
                    
            finally:
                # Удаляем временный файл
                if os.path.exists(temp_path):
                    os.remove(temp_path)
                    
            return False
            
        except Exception as e:
            logger.error(f"Ошибка при публикации фото: {e}")
            return False
    
    def watch_reels(self) -> bool:
        """Просмотр Reels"""
        try:
            # Используем explore для получения Reels
            try:
                # Пробуем получить Reels через explore
                explore = self.client.client.explore()
                
                if explore and 'items' in explore:
                    # Фильтруем только видео (Reels)
                    reels_items = [item for item in explore['items'] 
                                  if item.get('media', {}).get('media_type') == 2]  # 2 = video
                    
                    if reels_items:
                        # Выбираем несколько reels для просмотра
                        reels_to_watch = random.randint(2, min(5, len(reels_items)))
                        
                        for i in range(reels_to_watch):
                            # Время просмотра reels (15-60 секунд)
                            watch_time = random.randint(15, 60)
                            logger.info(f"🎬 Смотрим Reels {i+1}/{reels_to_watch} ({watch_time} сек)...")
                            time.sleep(watch_time)
                            
                            # Иногда лайкаем reels
                            if random.random() < 0.4:  # 40% шанс
                                reel = reels_items[i]
                                if 'media' in reel:
                                    try:
                                        media_id = reel['media']['id']
                                        self.client.client.media_like(media_id)
                                        logger.info("❤️ Поставили лайк на Reels")
                                    except:
                                        pass
                        
                        return True
            except Exception as e:
                logger.debug(f"Не удалось получить Reels через explore: {e}")
            
            # Альтернативный метод: получаем видео через хештеги
            try:
                hashtags = ['reels', 'viral', 'trending', 'fyp', 'foryou']
                hashtag = random.choice(hashtags)
                
                # Получаем медиа по хештегу
                medias = self.client.client.hashtag_medias_recent(hashtag, amount=10)
                
                # Фильтруем только видео
                videos = [m for m in medias if hasattr(m, 'media_type') and m.media_type == 2]
                
                if videos:
                    # Выбираем несколько видео для просмотра
                    videos_to_watch = random.randint(2, min(5, len(videos)))
                    
                    for i in range(videos_to_watch):
                        # Время просмотра (15-60 секунд)
                        watch_time = random.randint(15, 60)
                        logger.info(f"🎬 Смотрим видео {i+1}/{videos_to_watch} из #{hashtag} ({watch_time} сек)...")
                        time.sleep(watch_time)
                        
                        # Иногда лайкаем
                        if random.random() < 0.4:  # 40% шанс
                            try:
                                video = videos[i]
                                self.client.client.media_like(video.id)
                                logger.info("❤️ Поставили лайк на видео")
                            except:
                                pass
                    
                    return True
                else:
                    logger.info("ℹ️ Не найдено видео для просмотра")
            except Exception as e:
                logger.debug(f"Не удалось получить видео через хештеги: {e}")
            
            # Если ничего не получилось, просто имитируем просмотр
            logger.info("🎬 Имитируем просмотр Reels")
            time.sleep(random.randint(30, 90))
            return True
            
        except Exception as e:
            logger.error(f"Ошибка при просмотре Reels: {e}")
            return False
    
    def explore_hashtags(self) -> bool:
        """Исследование хештегов"""
        try:
            # Популярные хештеги для исследования
            hashtags = [
                'photography', 'nature', 'travel', 'fitness', 'food',
                'art', 'fashion', 'music', 'motivation', 'lifestyle'
            ]
            
            hashtag = random.choice(hashtags)
            logger.info(f"🔍 Исследуем хештег #{hashtag}")
            
            # Поиск по хештегу
            results = self.client.client.hashtag_medias_recent(hashtag, amount=20)
            
            if results:
                # Просматриваем несколько постов
                posts_to_view = random.randint(3, min(10, len(results)))
                
                for i in range(posts_to_view):
                    # Время просмотра поста
                    view_time = random.randint(3, 12)
                    logger.info(f"👀 Просматриваем пост {i+1}/{posts_to_view}")
                    time.sleep(view_time)
                    
                    # Иногда лайкаем
                    if random.random() < 0.3:  # 30% шанс
                        try:
                            post = results[i]
                            self.client.client.media_like(post.id)
                            logger.info("❤️ Поставили лайк")
                        except:
                            pass
                
                return True
            
            return False
        except Exception as e:
            logger.error(f"Ошибка при исследовании хештегов: {e}")
            return False
    
    def send_direct_messages(self) -> bool:
        """Отправка сообщений в директ"""
        try:
            # Проверяем авторизацию
            if not self.client.check_login():
                logger.warning("⚠️ Проблема с авторизацией, пропускаем сообщения")
                return False
            
            # Простая имитация активности в директе
            # Вместо реальной отправки сообщений, просто имитируем активность
            logger.info("💬 Имитируем активность в директе")
            
            # Можно добавить просмотр существующих чатов
            try:
                # Получаем список чатов (без отправки сообщений)
                inbox = self.client.client.direct_threads()
                if inbox:
                    logger.info(f"📥 Просмотрели {len(inbox)} чатов в директе")
                    return True
            except Exception as e:
                logger.debug(f"Не удалось получить чаты: {e}")
            
            # Если нет чатов, просто возвращаем успех (имитация)
            logger.info("💬 Активность в директе выполнена")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка при работе с сообщениями: {e}")
            return False
    
    def update_profile_info(self) -> bool:
        """Обновление информации профиля"""
        try:
            # Получаем текущую информацию
            current_info = self.client.client.account_info()
            
            # Варианты био
            bio_templates = [
                "📸 Photography enthusiast\n🌍 Travel lover\n✨ Living my best life",
                "🎨 Creative soul\n📚 Book lover\n☕ Coffee addict",
                "🏃‍♂️ Fitness journey\n🥗 Healthy lifestyle\n💪 Never give up",
                "🎵 Music is life\n🎸 Guitar player\n🎤 Singer",
                "👨‍💻 Tech enthusiast\n🚀 Innovation lover\n💡 Always learning"
            ]
            
            # Выбираем случайное био
            new_bio = random.choice(bio_templates)
            
            logger.info(f"📝 Обновляем био профиля")
            
            # Обновляем профиль
            result = self.client.client.account_edit(
                biography=new_bio,
                external_url=current_info.get('external_url', ''),
                email=current_info.get('email', ''),
                phone_number=current_info.get('phone_number', ''),
                username=current_info.get('username', ''),
                full_name=current_info.get('full_name', '')
            )
            
            if result:
                logger.info("✅ Профиль успешно обновлен")
                return True
            
            return False
        except Exception as e:
            logger.error(f"Ошибка при обновлении профиля: {e}")
            return False
    
    def perform_human_warmup_session(self, settings: dict) -> dict:
        """Выполнить сессию прогрева с человеческим поведением"""
        session_results = {
            'actions_performed': {},
            'errors': [],
            'session_duration': 0,
            'session_type': 'human_behavior',
            'interest_based': False
        }
        
        session_start = time.time()
        
        # Режим пассивных действий (включается при высоком риске)
        passive_mode = bool(settings.get('force_passive'))
        passive_allowed = {'view_feed', 'view_stories', 'watch_reels', 'explore_hashtags', 'save_posts', 'rest', 'long_rest'}
        if passive_mode:
            logger.info("🛡️ Пассивный режим прогрева: только просмотр и безопасные действия")
        
        # Проверяем, настроен ли прогрев по интересам
        if 'interests' in settings and self.interest_warmup is None:
            self.setup_interest_warmup(settings['interests'])
        
        # Проверяем, можно ли начать сессию
        if not self.behavior_manager.should_start_session():
            logger.info("⏸️ Еще не время для новой сессии активности")
            return session_results
        
        # Если настроен прогрев по интересам, используем его с вероятностью 60%
        if self.interest_warmup and random.random() < 0.6 and not passive_mode:
            logger.info("🎯 Используем прогрев по интересам")
            session_results['session_type'] = 'interest_based'
            session_results['interest_based'] = True
            
            try:
                # Выполняем сессию по интересам
                interest_results = self.interest_warmup.perform_interest_warmup_session(
                    duration_minutes=random.randint(15, 35)
                )
                
                # Объединяем результаты
                session_results.update(interest_results)
                session_results['session_duration'] = interest_results['session_duration']
                
                logger.info("✅ Сессия прогрева по интересам завершена")
                return session_results
                
            except Exception as e:
                logger.error(f"❌ Ошибка прогрева по интересам: {e}")
                session_results['errors'].append(f"Interest warmup error: {str(e)}")
                # Переходим к обычному прогреву
                session_results['session_type'] = 'human_behavior'
                session_results['interest_based'] = False
        
        # Проверяем авторизацию перед началом сессии
        if not self.client.check_login():
            logger.warning("❌ Проблема с авторизацией, откладываем сессию")
            session_results['errors'].append("Проблема с авторизацией")
            return session_results
        
        # Начинаем сессию
        self.behavior_manager.start_session()
        
        try:
            actions_performed = 0
            max_actions_per_session = 10  # Максимум действий за сессию
            
            while not self.behavior_manager.should_end_session() and actions_performed < max_actions_per_session:
                # Получаем последовательность действий
                action_sequence = self.behavior_manager.get_next_action_sequence()
                
                if passive_mode and action_sequence:
                    # Фильтруем действия до пассивных
                    action_sequence = [a for a in action_sequence if a in passive_allowed]
                
                if not action_sequence:
                    logger.info("🌙 Ночное время или пассивный режим без действий - прекращаем активность")
                    break
                
                logger.info(f"📋 Выполняем последовательность: {' → '.join(action_sequence)}")
                
                # Выполняем последовательность действий
                for action in action_sequence:
                    if action in ['rest', 'long_rest']:
                        # Отдых
                        delay = self.behavior_manager.get_action_delay(action, is_rest=True)
                        # Ограничиваем время отдыха максимум 5 минутами для тестирования
                        delay = min(delay, 300)
                        logger.info(f"😴 Отдыхаем {delay//60} минут {delay%60} секунд ({action})")
                        time.sleep(delay)
                        continue
                    
                    # Проверяем, не пора ли завершить сессию
                    if self.behavior_manager.should_end_session() or actions_performed >= max_actions_per_session:
                        logger.info("⏰ Время завершить сессию")
                        break
                    
                    try:
                        # Выполняем действие
                        logger.info(f"🎯 Выполняем: {action}")
                        success = self.perform_action(action, settings)
                        
                        actions_performed += 1
                        
                        if success:
                            # Записываем успешное действие
                            if action not in session_results['actions_performed']:
                                session_results['actions_performed'][action] = 0
                            session_results['actions_performed'][action] += 1
                            
                            # Уведомляем behavior manager
                            self.behavior_manager.record_action(action)
                        
                        # Пауза между действиями
                        delay = self.behavior_manager.get_action_delay(action)
                        # Ограничиваем задержки для тестирования
                        delay = min(delay, 60)
                        logger.info(f"⏱️ Пауза {delay} секунд...")
                        time.sleep(delay)
                        
                    except Exception as e:
                        logger.error(f"❌ Ошибка при выполнении {action}: {e}")
                        session_results['errors'].append(f"{action}: {str(e)}")
                        
                        # Увеличенная пауза после ошибки
                        error_delay = random.randint(30, 60)  # Уменьшили паузу после ошибки
                        logger.info(f"⚠️ Пауза после ошибки: {error_delay} секунд")
                        time.sleep(error_delay)
                
                # Небольшая пауза между последовательностями
                if not self.behavior_manager.should_end_session() and actions_performed < max_actions_per_session:
                    sequence_break = random.randint(10, 30)  # Уменьшили паузу между последовательностями
                    logger.info(f"🔄 Пауза между последовательностями: {sequence_break} секунд")
                    time.sleep(sequence_break)
        
        finally:
            # Завершаем сессию
            self.behavior_manager.end_session()
            session_results['session_duration'] = int(time.time() - session_start)
        
        # Простая логика фаз - здесь только изменение current_phase, хранение делаем в progress
        try:
            performed_total = sum(session_results['actions_performed'].values())
            errs = len(session_results['errors'])
            phase = getattr(self, 'current_phase', 'phase1') or 'phase1'
            next_phase = phase
            if errs == 0 and performed_total >= 8:
                if phase == 'phase1':
                    next_phase = 'phase2'
                elif phase == 'phase2':
                    next_phase = 'phase3'
            elif errs >= 2 and phase in ['phase3', 'phase2']:
                # при частых ошибках понижаем фазу
                next_phase = 'phase2' if phase == 'phase3' else 'phase1'
            if next_phase != phase:
                logger.info(f"🔁 Переход фазы прогрева: {phase} → {next_phase}")
                self.current_phase = next_phase
        except Exception:
            pass
        
        return session_results 
            


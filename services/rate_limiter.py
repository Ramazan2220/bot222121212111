# -*- coding: utf-8 -*-
"""
Централизованный Rate Limiter для контроля всех действий
"""

import time
import logging
from datetime import datetime, timedelta
from collections import defaultdict
from typing import Dict, Optional
from enum import Enum
from database.db_manager import get_instagram_account

logger = logging.getLogger(__name__)

class ActionType(Enum):
    """Типы действий для отслеживания"""
    LIKE = "like"
    FOLLOW = "follow"
    UNFOLLOW = "unfollow"
    COMMENT = "comment"
    POST = "post"
    STORY = "story"
    REEL = "reel"
    VIEW_STORY = "view_story"
    VIEW_FEED = "view_feed"
    DIRECT_MESSAGE = "direct_message"

class RateLimiter:
    """Централизованный контроль rate limiting"""
    
    # Лимиты по умолчанию (консервативные для безопасности)
    DEFAULT_LIMITS = {
        # Лимиты в час для новых аккаунтов (< 7 дней)
        "new_hourly": {
            ActionType.LIKE: 10,
            ActionType.FOLLOW: 5,
            ActionType.UNFOLLOW: 5,
            ActionType.COMMENT: 3,
            ActionType.POST: 1,
            ActionType.STORY: 2,
            ActionType.REEL: 1,
            ActionType.DIRECT_MESSAGE: 5
        },
        # Лимиты в день для новых аккаунтов
        "new_daily": {
            ActionType.LIKE: 50,
            ActionType.FOLLOW: 20,
            ActionType.UNFOLLOW: 20,
            ActionType.COMMENT: 10,
            ActionType.POST: 3,
            ActionType.STORY: 5,
            ActionType.REEL: 2,
            ActionType.DIRECT_MESSAGE: 20
        },
        # Лимиты в час для прогретых аккаунтов (> 30 дней)
        "warmed_hourly": {
            ActionType.LIKE: 60,
            ActionType.FOLLOW: 30,
            ActionType.UNFOLLOW: 30,
            ActionType.COMMENT: 20,
            ActionType.POST: 5,
            ActionType.STORY: 10,
            ActionType.REEL: 3,
            ActionType.DIRECT_MESSAGE: 30
        },
        # Лимиты в день для прогретых аккаунтов
        "warmed_daily": {
            ActionType.LIKE: 500,
            ActionType.FOLLOW: 200,
            ActionType.UNFOLLOW: 200,
            ActionType.COMMENT: 100,
            ActionType.POST: 20,
            ActionType.STORY: 30,
            ActionType.REEL: 10,
            ActionType.DIRECT_MESSAGE: 150
        }
    }
    
    def __init__(self):
        # Хранилище действий: account_id -> action_type -> list of timestamps
        self._actions: Dict[int, Dict[ActionType, list]] = defaultdict(lambda: defaultdict(list))
        
        # Временные блокировки: account_id -> action_type -> unlock_time
        self._blocks: Dict[int, Dict[ActionType, datetime]] = defaultdict(dict)
    
    def _get_account_age_days(self, account_id: int) -> int:
        """Получить возраст аккаунта в днях"""
        try:
            account = get_instagram_account(account_id)
            if account and account.created_at:
                age = (datetime.now() - account.created_at).days
                return age
        except:
            pass
        return 0  # По умолчанию считаем новым
    
    def _get_limits(self, account_id: int) -> Dict[str, Dict[ActionType, int]]:
        """Получить лимиты для аккаунта в зависимости от его возраста"""
        age_days = self._get_account_age_days(account_id)
        
        if age_days < 7:
            # Новый аккаунт - самые строгие лимиты
            return {
                "hourly": self.DEFAULT_LIMITS["new_hourly"],
                "daily": self.DEFAULT_LIMITS["new_daily"]
            }
        elif age_days < 30:
            # Промежуточные лимиты
            return {
                "hourly": {k: int(v * 1.5) for k, v in self.DEFAULT_LIMITS["new_hourly"].items()},
                "daily": {k: int(v * 1.5) for k, v in self.DEFAULT_LIMITS["new_daily"].items()}
            }
        else:
            # Прогретый аккаунт
            return {
                "hourly": self.DEFAULT_LIMITS["warmed_hourly"],
                "daily": self.DEFAULT_LIMITS["warmed_daily"]
            }
    
    def _cleanup_old_actions(self, account_id: int, action_type: ActionType):
        """Очистить старые записи действий (старше 24 часов)"""
        cutoff_time = time.time() - 86400  # 24 часа назад
        self._actions[account_id][action_type] = [
            ts for ts in self._actions[account_id][action_type] 
            if ts > cutoff_time
        ]
    
    def can_perform_action(self, account_id: int, action_type: ActionType) -> tuple[bool, Optional[str]]:
        """
        Проверить, можно ли выполнить действие
        Возвращает (можно_ли, причина_отказа)
        """
        # Проверяем временную блокировку
        if account_id in self._blocks and action_type in self._blocks[account_id]:
            unlock_time = self._blocks[account_id][action_type]
            if datetime.now() < unlock_time:
                wait_seconds = (unlock_time - datetime.now()).seconds
                return False, f"Действие {action_type.value} заблокировано на {wait_seconds} секунд"
        
        # Очищаем старые действия
        self._cleanup_old_actions(account_id, action_type)
        
        # Получаем лимиты для аккаунта
        limits = self._get_limits(account_id)
        
        # Текущее время
        now = time.time()
        
        # Получаем список действий
        actions = self._actions[account_id][action_type]
        
        # Проверяем часовой лимит
        hour_ago = now - 3600
        hourly_count = sum(1 for ts in actions if ts > hour_ago)
        hourly_limit = limits["hourly"].get(action_type, 0)
        
        if hourly_count >= hourly_limit:
            return False, f"Достигнут часовой лимит ({hourly_count}/{hourly_limit}) для {action_type.value}"
        
        # Проверяем дневной лимит
        day_ago = now - 86400
        daily_count = sum(1 for ts in actions if ts > day_ago)
        daily_limit = limits["daily"].get(action_type, 0)
        
        if daily_count >= daily_limit:
            return False, f"Достигнут дневной лимит ({daily_count}/{daily_limit}) для {action_type.value}"
        
        # Проверяем скорость действий (не чаще 1 действия в 2 секунды)
        if actions and (now - actions[-1]) < 2:
            return False, "Слишком быстрые действия, подождите 2 секунды"
        
        return True, None
    
    def record_action(self, account_id: int, action_type: ActionType):
        """Записать выполненное действие"""
        self._actions[account_id][action_type].append(time.time())
        logger.info(f"✅ Действие {action_type.value} записано для аккаунта {account_id}")
    
    def block_action(self, account_id: int, action_type: ActionType, duration_seconds: int):
        """Временно заблокировать действие"""
        unlock_time = datetime.now() + timedelta(seconds=duration_seconds)
        self._blocks[account_id][action_type] = unlock_time
        logger.warning(f"🔒 Действие {action_type.value} заблокировано для аккаунта {account_id} на {duration_seconds} секунд")
    
    def get_action_stats(self, account_id: int) -> Dict[str, Dict[str, int]]:
        """Получить статистику действий аккаунта"""
        stats = {"hourly": {}, "daily": {}}
        now = time.time()
        hour_ago = now - 3600
        day_ago = now - 86400
        
        for action_type in ActionType:
            actions = self._actions[account_id][action_type]
            stats["hourly"][action_type.value] = sum(1 for ts in actions if ts > hour_ago)
            stats["daily"][action_type.value] = sum(1 for ts in actions if ts > day_ago)
        
        return stats
    
    def get_wait_time(self, account_id: int, action_type: ActionType) -> int:
        """Получить рекомендуемое время ожидания перед следующим действием"""
        age_days = self._get_account_age_days(account_id)
        
        # Базовые задержки в зависимости от возраста
        if age_days < 7:
            base_delay = 30  # 30 секунд для новых
        elif age_days < 30:
            base_delay = 15  # 15 секунд для прогреваемых
        else:
            base_delay = 5   # 5 секунд для прогретых
        
        # Добавляем случайность ±20%
        import random
        variation = base_delay * 0.2
        delay = base_delay + random.uniform(-variation, variation)
        
        return max(2, int(delay))  # Минимум 2 секунды

# Глобальный экземпляр
rate_limiter = RateLimiter() 
from datetime import datetime, timedelta
from enum import Enum
from typing import Optional

class SubscriptionPlan(Enum):
    """Тарифные планы на основе времени"""
    FREE_TRIAL_1_DAY = "free_trial_1_day"
    FREE_TRIAL_3_DAYS = "free_trial_3_days"
    FREE_TRIAL_7_DAYS = "free_trial_7_days"
    SUBSCRIPTION_30_DAYS = "subscription_30_days"
    SUBSCRIPTION_90_DAYS = "subscription_90_days"
    SUBSCRIPTION_LIFETIME = "subscription_lifetime"

class UserStatus(Enum):
    """Статусы пользователей"""
    ACTIVE = "active"
    EXPIRED = "expired"
    BLOCKED = "blocked"
    TRIAL = "trial"

class User:
    """Модель пользователя с тарифными планами"""
    
    def __init__(self, telegram_id: int, username: str = None, 
                 subscription_plan: SubscriptionPlan = None):
        self.telegram_id = telegram_id
        self.username = username
        self.subscription_plan = subscription_plan
        self.created_at = datetime.now()
        self.subscription_start = None
        self.subscription_end = None
        self.status = UserStatus.TRIAL
        self.accounts_count = 0
        self.last_activity = datetime.now()
        
    @property
    def is_active(self) -> bool:
        """Проверяет активность подписки"""
        if self.subscription_plan == SubscriptionPlan.SUBSCRIPTION_LIFETIME:
            return self.status != UserStatus.BLOCKED
        
        if self.subscription_end is None:
            return False
            
        # Пользователь активен если подписка не истекла И статус не заблокирован
        return datetime.now() < self.subscription_end and self.status != UserStatus.BLOCKED
    
    @property
    def days_remaining(self) -> int:
        """Количество дней до окончания подписки"""
        if self.subscription_plan == SubscriptionPlan.SUBSCRIPTION_LIFETIME:
            return -1  # Безлимитная подписка
            
        if self.subscription_end is None:
            return 0
            
        delta = self.subscription_end - datetime.now()
        return max(0, delta.days)
    
    @property
    def is_trial(self) -> bool:
        """Проверяет, является ли подписка триальной"""
        return self.subscription_plan in [
            SubscriptionPlan.FREE_TRIAL_1_DAY,
            SubscriptionPlan.FREE_TRIAL_3_DAYS,
            SubscriptionPlan.FREE_TRIAL_7_DAYS
        ]
    
    def set_subscription(self, plan: SubscriptionPlan):
        """Устанавливает тарифный план"""
        self.subscription_plan = plan
        self.subscription_start = datetime.now()
        
        # Определяем период подписки
        plan_durations = {
            SubscriptionPlan.FREE_TRIAL_1_DAY: 1,
            SubscriptionPlan.FREE_TRIAL_3_DAYS: 3,
            SubscriptionPlan.FREE_TRIAL_7_DAYS: 7,
            SubscriptionPlan.SUBSCRIPTION_30_DAYS: 30,
            SubscriptionPlan.SUBSCRIPTION_90_DAYS: 90,
            SubscriptionPlan.SUBSCRIPTION_LIFETIME: None
        }
        
        duration = plan_durations.get(plan)
        if duration:
            self.subscription_end = self.subscription_start + timedelta(days=duration)
        else:
            self.subscription_end = None  # Безлимитная подписка
            
        # Устанавливаем статус
        if self.is_trial:
            self.status = UserStatus.TRIAL
        else:
            self.status = UserStatus.ACTIVE
    
    def extend_subscription(self, days: int):
        """Продлевает подписку на указанное количество дней"""
        if self.subscription_end:
            self.subscription_end += timedelta(days=days)
        else:
            self.subscription_end = datetime.now() + timedelta(days=days)
    
    def block_user(self):
        """Блокирует пользователя"""
        self.status = UserStatus.BLOCKED
    
    def unblock_user(self):
        """Разблокирует пользователя"""
        if self.is_active:
            self.status = UserStatus.ACTIVE
        else:
            self.status = UserStatus.EXPIRED
    
    def update_activity(self):
        """Обновляет время последней активности"""
        self.last_activity = datetime.now()
    
    def to_dict(self) -> dict:
        """Преобразует объект в словарь"""
        return {
            'telegram_id': self.telegram_id,
            'username': self.username,
            'subscription_plan': self.subscription_plan.value if self.subscription_plan else None,
            'created_at': self.created_at.isoformat(),
            'subscription_start': self.subscription_start.isoformat() if self.subscription_start else None,
            'subscription_end': self.subscription_end.isoformat() if self.subscription_end else None,
            'status': self.status.value,
            'accounts_count': self.accounts_count,
            'last_activity': self.last_activity.isoformat(),
            'is_active': self.is_active,
            'days_remaining': self.days_remaining,
            'is_trial': self.is_trial
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'User':
        """Создает объект из словаря"""
        user = cls(
            telegram_id=data['telegram_id'],
            username=data.get('username')
        )
        
        if data.get('subscription_plan'):
            user.subscription_plan = SubscriptionPlan(data['subscription_plan'])
        
        user.created_at = datetime.fromisoformat(data['created_at'])
        
        if data.get('subscription_start'):
            user.subscription_start = datetime.fromisoformat(data['subscription_start'])
        
        if data.get('subscription_end'):
            user.subscription_end = datetime.fromisoformat(data['subscription_end'])
        
        user.status = UserStatus(data['status'])
        user.accounts_count = data.get('accounts_count', 0)
        user.last_activity = datetime.fromisoformat(data['last_activity'])
        
        return user

# Константы тарифных планов
PLAN_INFO = {
    SubscriptionPlan.FREE_TRIAL_1_DAY: {
        'name': '🆓 Фри триал 1 день',
        'duration': 1,
        'price': 0,
        'description': 'Бесплатный доступ на 1 день'
    },
    SubscriptionPlan.FREE_TRIAL_3_DAYS: {
        'name': '🆓 Фри триал 3 дня',
        'duration': 3,
        'price': 0,
        'description': 'Бесплатный доступ на 3 дня'
    },
    SubscriptionPlan.FREE_TRIAL_7_DAYS: {
        'name': '🆓 Фри триал 7 дней',
        'duration': 7,
        'price': 0,
        'description': 'Бесплатный доступ на неделю'
    },
    SubscriptionPlan.SUBSCRIPTION_30_DAYS: {
        'name': '💳 Подписка 30 дней',
        'duration': 30,
        'price': 200,
        'description': 'Полный доступ на месяц'
    },
    SubscriptionPlan.SUBSCRIPTION_90_DAYS: {
        'name': '💳 Подписка 3 месяца',
        'duration': 90,
        'price': 400,
        'description': 'Полный доступ на 3 месяца'
    },
    SubscriptionPlan.SUBSCRIPTION_LIFETIME: {
        'name': '💎 Подписка навсегда',
        'duration': None,
        'price': 500,
        'description': 'Безлимитный доступ навсегда'
    }
} 
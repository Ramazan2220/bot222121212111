"""
🔄 МОДУЛЬ ОПТИМИЗАЦИИ АКТИВНОСТИ
Умное распределение нагрузки между аккаунтами для экономии ресурсов
"""

import time
import random
from typing import Dict, List, Optional, Set
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import logging
from threading import Lock
import json

logger = logging.getLogger(__name__)

@dataclass
class AccountActivity:
    """Информация об активности аккаунта"""
    account_id: str
    user_id: int
    last_activity: float = 0
    total_requests_today: int = 0
    priority: int = 1  # 1-низкий, 5-высокий
    max_requests_per_hour: int = 20
    active_hours: List[int] = field(default_factory=lambda: list(range(8, 22)))  # 8:00-22:00
    cooldown_until: float = 0
    is_warming_up: bool = False

@dataclass
class UserQuota:
    """Квоты пользователя"""
    user_id: int
    max_concurrent_accounts: int = 75  # 25% от 300
    max_requests_per_minute: int = 300  # 75 accounts * 4 req/min
    current_active_accounts: int = 0
    priority_boost: float = 1.0  # Мультипликатор для премиум пользователей

class ActivityOptimizer:
    """
    🎯 УМНЫЙ ОПТИМИЗАТОР АКТИВНОСТИ:
    - Ротация аккаунтов для экономии ресурсов
    - Распределение нагрузки по времени
    - Приоритизация задач
    - Адаптивные лимиты
    """
    
    def __init__(self):
        self.accounts: Dict[str, AccountActivity] = {}
        self.user_quotas: Dict[int, UserQuota] = {}
        self.active_accounts: Set[str] = set()
        self.waiting_queue: List[str] = []
        self._lock = Lock()
        
        # Статистика
        self.total_rotations = 0
        self.total_optimizations = 0
        self.memory_saved_mb = 0
        
        logger.info("🔄 ActivityOptimizer инициализирован")

    def register_account(self, account_id: str, user_id: int, priority: int = 1, 
                        max_requests_per_hour: int = 20):
        """Регистрация аккаунта в оптимизаторе"""
        with self._lock:
            self.accounts[account_id] = AccountActivity(
                account_id=account_id,
                user_id=user_id,
                priority=priority,
                max_requests_per_hour=max_requests_per_hour
            )
            
            # Создаем квоту пользователя если нет
            if user_id not in self.user_quotas:
                self.user_quotas[user_id] = UserQuota(user_id=user_id)
                
        logger.debug(f"📝 Аккаунт {account_id} зарегистрирован (user: {user_id}, priority: {priority})")

    def should_activate_account(self, account_id: str) -> Dict[str, any]:
        """
        Определяет, можно ли активировать аккаунт сейчас
        Возвращает: {'allowed': bool, 'reason': str, 'wait_time': int}
        """
        if account_id not in self.accounts:
            return {'allowed': False, 'reason': 'Account not registered', 'wait_time': 0}
            
        account = self.accounts[account_id]
        user_quota = self.user_quotas.get(account.user_id)
        current_time = time.time()
        current_hour = datetime.now().hour
        
        # Проверка кулдауна
        if current_time < account.cooldown_until:
            wait_time = int(account.cooldown_until - current_time)
            return {'allowed': False, 'reason': f'Cooldown for {wait_time}s', 'wait_time': wait_time}
        
        # Проверка рабочих часов
        if current_hour not in account.active_hours:
            wait_time = self._calculate_next_active_hour(current_hour, account.active_hours) * 3600
            return {'allowed': False, 'reason': 'Outside active hours', 'wait_time': wait_time}
        
        # Проверка квот пользователя
        if user_quota and user_quota.current_active_accounts >= user_quota.max_concurrent_accounts:
            # Пытаемся найти менее приоритетный аккаунт для замены
            if account.priority > 3:  # Высокий приоритет
                replaced_account = self._try_replace_low_priority_account(account.user_id, account.priority)
                if replaced_account:
                    logger.info(f"🔄 Заменяем аккаунт {replaced_account} на высокоприоритетный {account_id}")
                    self._deactivate_account(replaced_account)
                    return {'allowed': True, 'reason': 'Replaced low priority account', 'wait_time': 0}
            
            return {'allowed': False, 'reason': 'User quota exceeded', 'wait_time': 300}  # 5 min
        
        return {'allowed': True, 'reason': 'All checks passed', 'wait_time': 0}

    def activate_account(self, account_id: str) -> bool:
        """Активация аккаунта с учетом оптимизации"""
        with self._lock:
            activation_check = self.should_activate_account(account_id)
            
            if not activation_check['allowed']:
                # Добавляем в очередь ожидания если нужно
                if account_id not in self.waiting_queue:
                    self.waiting_queue.append(account_id)
                logger.debug(f"⏳ Аккаунт {account_id} в очереди: {activation_check['reason']}")
                return False
            
            # Активируем аккаунт
            self.active_accounts.add(account_id)
            account = self.accounts[account_id]
            account.last_activity = time.time()
            
            # Обновляем квоты пользователя
            user_quota = self.user_quotas[account.user_id]
            user_quota.current_active_accounts += 1
            
            # Убираем из очереди ожидания
            if account_id in self.waiting_queue:
                self.waiting_queue.remove(account_id)
                
            logger.info(f"✅ Аккаунт {account_id} активирован (активных: {len(self.active_accounts)})")
            return True

    def deactivate_account(self, account_id: str, cooldown_minutes: int = 30):
        """Деактивация аккаунта с кулдауном"""
        self._deactivate_account(account_id, cooldown_minutes)

    def _deactivate_account(self, account_id: str, cooldown_minutes: int = 30):
        """Внутренняя деактивация аккаунта"""
        with self._lock:
            if account_id in self.active_accounts:
                self.active_accounts.remove(account_id)
                
                account = self.accounts[account_id]
                account.cooldown_until = time.time() + (cooldown_minutes * 60)
                
                # Обновляем квоты пользователя
                user_quota = self.user_quotas[account.user_id]
                user_quota.current_active_accounts = max(0, user_quota.current_active_accounts - 1)
                
                self.total_rotations += 1
                self.memory_saved_mb += 4  # ~4MB на каждую деактивацию
                
                logger.info(f"💤 Аккаунт {account_id} деактивирован на {cooldown_minutes} мин")
                
                # Пытаемся активировать следующий в очереди
                self._try_activate_from_queue()

    def _try_activate_from_queue(self):
        """Попытка активировать аккаунт из очереди ожидания"""
        if not self.waiting_queue:
            return
            
        # Сортируем очередь по приоритету
        self.waiting_queue.sort(key=lambda acc_id: self.accounts[acc_id].priority, reverse=True)
        
        for account_id in self.waiting_queue.copy():
            if self.activate_account(account_id):
                break

    def _try_replace_low_priority_account(self, user_id: int, new_priority: int) -> Optional[str]:
        """Поиск низкоприоритетного аккаунта для замены"""
        user_active_accounts = [
            acc_id for acc_id in self.active_accounts 
            if self.accounts[acc_id].user_id == user_id
        ]
        
        # Ищем аккаунт с приоритетом ниже нового
        for account_id in user_active_accounts:
            if self.accounts[account_id].priority < new_priority:
                return account_id
                
        return None

    def _calculate_next_active_hour(self, current_hour: int, active_hours: List[int]) -> int:
        """Расчет времени до следующего активного часа"""
        for hour in active_hours:
            if hour > current_hour:
                return hour - current_hour
        # Если нет активных часов сегодня, берем первый завтрашний
        return (24 - current_hour) + min(active_hours)

    def optimize_all_activities(self):
        """
        Полная оптимизация всех активностей:
        - Ротация перегруженных аккаунтов
        - Балансировка нагрузки
        - Очистка просроченных кулдаунов
        """
        current_time = time.time()
        optimizations_made = 0
        
        with self._lock:
            # 1. Очищаем просроченные кулдауны
            for account in self.accounts.values():
                if account.cooldown_until <= current_time:
                    account.cooldown_until = 0
            
            # 2. Ротируем перегруженные аккаунты
            for account_id in list(self.active_accounts):
                account = self.accounts[account_id]
                inactive_time = current_time - account.last_activity
                
                # Автоматическая ротация неактивных аккаунтов (45 мин)
                if inactive_time > 2700:  # 45 минут
                    self._deactivate_account(account_id, cooldown_minutes=15)
                    optimizations_made += 1
            
            # 3. Пробуем активировать аккаунты из очереди
            self._try_activate_from_queue()
            
            self.total_optimizations += optimizations_made
            
        if optimizations_made > 0:
            logger.info(f"🎯 Выполнено {optimizations_made} оптимизаций активности")

    def get_optimization_stats(self) -> Dict:
        """Статистика оптимизации"""
        with self._lock:
            total_accounts = len(self.accounts)
            active_accounts = len(self.active_accounts)
            waiting_accounts = len(self.waiting_queue)
            
            return {
                'total_accounts': total_accounts,
                'active_accounts': active_accounts,
                'waiting_accounts': waiting_accounts,
                'utilization_percent': (active_accounts / max(total_accounts, 1)) * 100,
                'total_rotations': self.total_rotations,
                'total_optimizations': self.total_optimizations,
                'memory_saved_mb': self.memory_saved_mb,
                'estimated_resource_savings_percent': min(75, (self.total_rotations / max(total_accounts, 1)) * 100)
            }

    def set_user_premium_status(self, user_id: int, is_premium: bool = True):
        """Установка премиум статуса для увеличения лимитов"""
        if user_id not in self.user_quotas:
            self.user_quotas[user_id] = UserQuota(user_id=user_id)
            
        quota = self.user_quotas[user_id]
        if is_premium:
            quota.max_concurrent_accounts = 100  # 30% вместо 25%
            quota.max_requests_per_minute = 400  # Увеличенный лимит
            quota.priority_boost = 1.5
        else:
            quota.max_concurrent_accounts = 75   # Базовые 25%
            quota.max_requests_per_minute = 300
            quota.priority_boost = 1.0
            
        logger.info(f"👑 Пользователь {user_id} {'премиум' if is_premium else 'базовый'} статус")

# Глобальный экземпляр оптимизатора
activity_optimizer = ActivityOptimizer()

def get_activity_optimizer() -> ActivityOptimizer:
    """Получить глобальный оптимизатор активности"""
    return activity_optimizer

def init_activity_optimizer():
    """Инициализация оптимизатора активности"""
    global activity_optimizer
    activity_optimizer = ActivityOptimizer()
    logger.info("🔄 Activity Optimizer инициализирован")
    
def optimize_account_activity():
    """Запуск полной оптимизации (для планировщика)"""
    activity_optimizer.optimize_all_activities() 
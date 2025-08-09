"""
Сервис управления подписками пользователей
Интеграция между админ-ботом и основной системой
"""

import os
import sys
import logging
from datetime import datetime
from typing import Optional, Dict, Any

# Добавляем путь к admin_bot для импорта
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'admin_bot'))

from admin_bot.services.user_service import UserService
from admin_bot.models.user import User, UserStatus, SubscriptionPlan, PLAN_INFO

logger = logging.getLogger(__name__)

class SubscriptionService:
    """Сервис для проверки подписок пользователей в основной системе"""
    
    def __init__(self):
        self.user_service = UserService()
        logger.info("🔐 SubscriptionService инициализирован")
    
    def check_user_access(self, telegram_id: int) -> Dict[str, Any]:
        """
        Проверяет доступ пользователя к системе
        
        Returns:
            dict: {
                'has_access': bool,
                'status': str,
                'plan': str,
                'days_remaining': int,
                'is_trial': bool,
                'user': User or None
            }
        """
        try:
            user = self.user_service.get_user(telegram_id)
            
            if not user:
                # Пользователь не найден - можно предложить триал
                return {
                    'has_access': False,
                    'status': 'not_registered',
                    'plan': None,
                    'days_remaining': 0,
                    'is_trial': False,
                    'user': None,
                    'message': '❌ Пользователь не зарегистрирован в системе'
                }
            
            # Автоматически проверяем и обновляем истекшие подписки
            self._update_expired_subscription(user)
            
            if user.status == UserStatus.BLOCKED:
                return {
                    'has_access': False,
                    'status': 'blocked',
                    'plan': user.subscription_plan.value if user.subscription_plan else None,
                    'days_remaining': 0,
                    'is_trial': user.is_trial,
                    'user': user,
                    'message': '🚫 Пользователь заблокирован'
                }
            
            if user.status == UserStatus.EXPIRED:
                return {
                    'has_access': False,
                    'status': 'expired',
                    'plan': user.subscription_plan.value if user.subscription_plan else None,
                    'days_remaining': 0,
                    'is_trial': user.is_trial,
                    'user': user,
                    'message': '⏰ Подписка истекла'
                }
            
            # Пользователь активен
            if user.is_active:
                return {
                    'has_access': True,
                    'status': 'active',
                    'plan': user.subscription_plan.value if user.subscription_plan else None,
                    'days_remaining': user.days_remaining,
                    'is_trial': user.is_trial,
                    'user': user,
                    'message': f'✅ Доступ разрешен ({user.days_remaining} дней осталось)' if user.days_remaining != float('inf') else '✅ Безлимитный доступ'
                }
            
            return {
                'has_access': False,
                'status': 'inactive',
                'plan': user.subscription_plan.value if user.subscription_plan else None,
                'days_remaining': 0,
                'is_trial': user.is_trial,
                'user': user,
                'message': '❌ Подписка неактивна'
            }
            
        except Exception as e:
            logger.error(f"Ошибка проверки доступа для пользователя {telegram_id}: {e}")
            return {
                'has_access': False,
                'status': 'error',
                'plan': None,
                'days_remaining': 0,
                'is_trial': False,
                'user': None,
                'message': '❌ Ошибка проверки подписки'
            }
    
    def ensure_user_exists(self, telegram_id: int, username: str = None):
        """Автоматически создает пользователя если его не существует"""
        try:
            from admin_bot.services.user_service import UserService
            user_service = UserService()
            
            # Проверяем существует ли пользователь
            existing_user = user_service.get_user(telegram_id)
            if existing_user:
                # Обновляем username если изменился
                if username and existing_user.username != username:
                    existing_user.username = username
                    user_service.update_user(existing_user)
                    logger.info(f"📝 Обновлен username для пользователя {telegram_id}: {username}")
                return existing_user
            
            # Создаем нового пользователя без подписки
            new_user = user_service.create_user(telegram_id, username)
            logger.info(f"👤 Создан новый пользователь: {telegram_id} (@{username})")
            return new_user
            
        except Exception as e:
            logger.error(f"Ошибка создания пользователя {telegram_id}: {e}")
            return None

    def _update_expired_subscription(self, user: User):
        """Автоматически обновляет статус истекших подписок"""
        if (user.subscription_end and 
            user.subscription_end < datetime.now() and 
            user.status == UserStatus.ACTIVE):
            
            user.status = UserStatus.EXPIRED
            self.user_service.update_user(user)
            logger.info(f"Подписка пользователя {user.telegram_id} автоматически помечена как истекшая")
    
    def create_trial_user(self, telegram_id: int, username: str = None, plan: SubscriptionPlan = SubscriptionPlan.FREE_TRIAL_1_DAY) -> User:
        """Создает пользователя с триальной подпиской"""
        try:
            user = self.user_service.create_user(telegram_id, username)
            user.set_subscription(plan)
            self.user_service.update_user(user)
            
            logger.info(f"Создан триальный пользователь {telegram_id} с планом {plan.value}")
            return user
            
        except Exception as e:
            logger.error(f"Ошибка создания триального пользователя {telegram_id}: {e}")
            return None
    
    def get_user_stats(self, telegram_id: int) -> Dict[str, Any]:
        """Получает статистику пользователя для отображения"""
        access_info = self.check_user_access(telegram_id)
        
        if not access_info['user']:
            return access_info
        
        user = access_info['user']
        plan_info = PLAN_INFO.get(user.subscription_plan, {})
        
        return {
            **access_info,
            'plan_name': plan_info.get('name', 'Неизвестный план'),
            'plan_price': plan_info.get('price', 0),
            'subscription_start': user.subscription_start.strftime('%d.%m.%Y') if user.subscription_start else None,
            'subscription_end': user.subscription_end.strftime('%d.%m.%Y') if user.subscription_end else None,
            'accounts_count': user.accounts_count,
            'last_activity': user.last_activity.strftime('%d.%m.%Y %H:%M') if user.last_activity else None
        }
    
    def update_user_activity(self, telegram_id: int):
        """Обновляет время последней активности пользователя"""
        try:
            self.user_service.update_user_activity(telegram_id)
        except Exception as e:
            logger.error(f"Ошибка обновления активности пользователя {telegram_id}: {e}")
    
    def get_available_plans(self) -> list:
        """Возвращает список доступных тарифных планов"""
        return [
            {
                'plan': plan,
                'info': info
            }
            for plan, info in PLAN_INFO.items()
        ]

# Глобальный экземпляр сервиса
subscription_service = SubscriptionService() 
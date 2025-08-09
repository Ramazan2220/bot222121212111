import json
import os
from datetime import datetime, timedelta
from typing import List, Optional, Dict
from ..models.user import User, SubscriptionPlan, UserStatus, PLAN_INFO

class UserService:
    """Сервис для управления пользователями"""
    
    def __init__(self, data_file: str = "admin_bot/data/users.json"):
        self.data_file = data_file
        self.users = {}
        self._ensure_data_dir()
        self.load_users()
    
    def _ensure_data_dir(self):
        """Создает директорию для данных если её нет"""
        os.makedirs(os.path.dirname(self.data_file), exist_ok=True)
    
    def load_users(self):
        """Загружает пользователей из файла"""
        try:
            if os.path.exists(self.data_file):
                with open(self.data_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.users = {
                        int(telegram_id): User.from_dict(user_data)
                        for telegram_id, user_data in data.items()
                    }
            else:
                self.users = {}
        except Exception as e:
            print(f"Ошибка загрузки пользователей: {e}")
            self.users = {}
    
    def save_users(self):
        """Сохраняет пользователей в файл"""
        try:
            data = {
                str(telegram_id): user.to_dict()
                for telegram_id, user in self.users.items()
            }
            with open(self.data_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"Ошибка сохранения пользователей: {e}")
    
    def get_user(self, telegram_id: int) -> Optional[User]:
        """Получает пользователя по Telegram ID"""
        return self.users.get(telegram_id)
    
    def create_user(self, telegram_id: int, username: str = None) -> User:
        """Создает нового пользователя"""
        user = User(telegram_id=telegram_id, username=username)
        self.users[telegram_id] = user
        self.save_users()
        return user
    
    def update_user(self, user: User):
        """Обновляет пользователя"""
        self.users[user.telegram_id] = user
        self.save_users()
    
    def delete_user(self, telegram_id: int) -> bool:
        """Удаляет пользователя"""
        if telegram_id in self.users:
            del self.users[telegram_id]
            self.save_users()
            return True
        return False
    
    def get_all_users(self) -> List[User]:
        """Получает всех пользователей"""
        return list(self.users.values())
    
    def get_users_by_status(self, status: UserStatus) -> List[User]:
        """Получает пользователей по статусу"""
        return [user for user in self.users.values() if user.status == status]
    
    def get_users_by_plan(self, plan: SubscriptionPlan) -> List[User]:
        """Получает пользователей по тарифному плану"""
        return [user for user in self.users.values() if user.subscription_plan == plan]
    
    def get_expiring_users(self, days: int = 3) -> List[User]:
        """Получает пользователей с истекающей подпиской"""
        expiring_date = datetime.now() + timedelta(days=days)
        return [
            user for user in self.users.values()
            if user.subscription_end and user.subscription_end <= expiring_date
            and user.is_active
        ]
    
    def set_user_subscription(self, telegram_id: int, plan: SubscriptionPlan) -> bool:
        """Устанавливает тарифный план пользователю"""
        user = self.get_user(telegram_id)
        if user:
            user.set_subscription(plan)
            self.update_user(user)
            return True
        return False
    
    def extend_user_subscription(self, telegram_id: int, days: int) -> bool:
        """Продлевает подписку пользователя"""
        user = self.get_user(telegram_id)
        if user:
            user.extend_subscription(days)
            self.update_user(user)
            return True
        return False
    
    def block_user(self, telegram_id: int) -> bool:
        """Блокирует пользователя"""
        user = self.get_user(telegram_id)
        if user:
            user.block_user()
            self.update_user(user)
            return True
        return False
    
    def unblock_user(self, telegram_id: int) -> bool:
        """Разблокирует пользователя"""
        user = self.get_user(telegram_id)
        if user:
            user.unblock_user()
            self.update_user(user)
            return True
        return False
    
    def update_user_activity(self, telegram_id: int):
        """Обновляет активность пользователя"""
        user = self.get_user(telegram_id)
        if user:
            user.update_activity()
            self.update_user(user)
    
    def get_statistics(self) -> Dict:
        """Получает статистику по пользователям"""
        users = self.get_all_users()
        
        stats = {
            'total_users': len(users),
            'active_users': len([u for u in users if u.is_active]),
            'trial_users': len([u for u in users if u.is_trial]),
            'blocked_users': len([u for u in users if u.status == UserStatus.BLOCKED]),
            'expired_users': len([u for u in users if u.status == UserStatus.EXPIRED]),
            'total_accounts': sum(u.accounts_count for u in users),
            'plans_distribution': {}
        }
        
        # Распределение по планам
        for plan in SubscriptionPlan:
            count = len(self.get_users_by_plan(plan))
            if count > 0:
                stats['plans_distribution'][plan.value] = count
        
        # Доходы (примерная оценка)
        stats['estimated_revenue'] = sum(
            PLAN_INFO[user.subscription_plan]['price']
            for user in users
            if user.subscription_plan and not user.is_trial
        )
        
        return stats
    
    def cleanup_expired_users(self):
        """Обновляет статусы истекших пользователей"""
        now = datetime.now()
        updated_count = 0
        
        for user in self.users.values():
            if (user.subscription_end and 
                user.subscription_end < now and 
                user.status == UserStatus.ACTIVE):
                user.status = UserStatus.EXPIRED
                updated_count += 1
        
        if updated_count > 0:
            self.save_users()
        
        return updated_count 
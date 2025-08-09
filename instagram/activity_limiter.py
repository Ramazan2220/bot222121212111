import logging
import time
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from database.db_manager import get_instagram_account

logger = logging.getLogger(__name__)

class ActivityLimiter:
    """Интеллектуальное управление лимитами активности"""
    
    def __init__(self):
        self.activity_log = {}
        self.restriction_cache = {}
        self.cache_timeout = 300  # 5 минут
    
    def get_dynamic_limits(self, account_id: int) -> Dict[str, int]:
        """Получение динамических лимитов на основе возраста аккаунта"""
        try:
            account = get_instagram_account(account_id)
            if not account:
                return {}
            
            # Вычисляем возраст аккаунта
            account_age = datetime.now() - account.created_at
            age_days = account_age.days
            
            # Базовые лимиты на основе возраста
            if age_days >= 365:  # Старый аккаунт (1+ год)
                limits = {
                    'follows_per_day': 500,
                    'likes_per_hour': 60,
                    'comments_per_day': 100,
                    'story_views_per_hour': 100,
                    'direct_messages_per_day': 50
                }
            elif age_days >= 90:  # Зрелый аккаунт (3+ месяца)
                limits = {
                    'follows_per_day': 300,
                    'likes_per_hour': 40,
                    'comments_per_day': 60,
                    'story_views_per_hour': 80,
                    'direct_messages_per_day': 30
                }
            elif age_days >= 30:  # Средний аккаунт (1+ месяц)
                limits = {
                    'follows_per_day': 150,
                    'likes_per_hour': 25,
                    'comments_per_day': 30,
                    'story_views_per_hour': 50,
                    'direct_messages_per_day': 20
                }
            elif age_days >= 7:   # Молодой аккаунт (1+ неделя)
                limits = {
                    'follows_per_day': 50,
                    'likes_per_hour': 15,
                    'comments_per_day': 15,
                    'story_views_per_hour': 30,
                    'direct_messages_per_day': 10
                }
            else:  # Новый аккаунт (менее недели)
                limits = {
                    'follows_per_day': 20,
                    'likes_per_hour': 8,
                    'comments_per_day': 5,
                    'story_views_per_hour': 15,
                    'direct_messages_per_day': 5
                }
            
            logger.info(f"Динамические лимиты для аккаунта {account_id} (возраст: {age_days} дней): {limits}")
            return limits
            
        except Exception as e:
            logger.error(f"Ошибка получения лимитов для аккаунта {account_id}: {e}")
            return {}
    
    def check_current_restrictions(self, account_id: int) -> List[str]:
        """Детекция текущих ограничений Instagram"""
        try:
            # Проверяем кэш
            if self._is_restriction_cache_valid(account_id):
                return self.restriction_cache[account_id]['restrictions']
            
            account = get_instagram_account(account_id)
            if not account:
                return ["Аккаунт не найден"]
            
            restrictions = []
            
            # Симуляция проверки ограничений
            # В реальной реализации здесь были бы API запросы для проверки:
            
            # 1. Проверка shadowban
            if not self._check_shadowban_status(account_id):
                restrictions.append("shadowban")
            
            # 2. Проверка лимитов подписок
            if not self._check_follow_limits(account_id):
                restrictions.append("follow_limit")
            
            # 3. Проверка лимитов лайков
            if not self._check_like_limits(account_id):
                restrictions.append("like_limit")
            
            # 4. Проверка временных блокировок
            if not self._check_temporary_blocks(account_id):
                restrictions.append("temporary_block")
            
            # Кэшируем результат
            self.restriction_cache[account_id] = {
                'restrictions': restrictions,
                'timestamp': time.time()
            }
            
            if restrictions:
                logger.warning(f"Обнаружены ограничения для аккаунта {account_id}: {restrictions}")
            else:
                logger.info(f"Ограничения для аккаунта {account_id} не обнаружены")
            
            return restrictions
            
        except Exception as e:
            logger.error(f"Ошибка проверки ограничений для аккаунта {account_id}: {e}")
            return ["error_checking_restrictions"]
    
    def calculate_safe_delay(self, action_type: str, account_id: int) -> int:
        """Расчет безопасных задержек между действиями"""
        try:
            account = get_instagram_account(account_id)
            if not account:
                return 300  # 5 минут по умолчанию
            
            # Базовые задержки по типу действия
            base_delays = {
                'follow': 180,      # 3 минуты
                'like': 60,         # 1 минута
                'comment': 300,     # 5 минут
                'story_view': 30,   # 30 секунд
                'direct_message': 600  # 10 минут
            }
            
            base_delay = base_delays.get(action_type, 180)
            
            # Корректировка на основе возраста аккаунта
            account_age = datetime.now() - account.created_at
            age_days = account_age.days
            
            if age_days < 7:        # Новый аккаунт - увеличиваем задержки
                multiplier = 2.0
            elif age_days < 30:     # Молодой аккаунт
                multiplier = 1.5
            elif age_days < 90:     # Средний аккаунт
                multiplier = 1.2
            else:                   # Старый аккаунт
                multiplier = 1.0
            
            # Проверяем наличие ограничений
            restrictions = self.check_current_restrictions(account_id)
            if restrictions:
                multiplier *= 2.0  # Удваиваем задержки при ограничениях
            
            safe_delay = int(base_delay * multiplier)
            
            # Добавляем случайность ±20%
            import random
            variance = int(safe_delay * 0.2)
            safe_delay = random.randint(safe_delay - variance, safe_delay + variance)
            
            logger.info(f"Безопасная задержка для действия '{action_type}' аккаунта {account_id}: {safe_delay} сек")
            return safe_delay
            
        except Exception as e:
            logger.error(f"Ошибка расчета задержки для аккаунта {account_id}: {e}")
            return 300
    
    def log_activity(self, account_id: int, action_type: str, success: bool = True):
        """Логирование активности"""
        try:
            if account_id not in self.activity_log:
                self.activity_log[account_id] = []
            
            activity_entry = {
                'action': action_type,
                'timestamp': time.time(),
                'success': success
            }
            
            self.activity_log[account_id].append(activity_entry)
            
            # Ограничиваем размер лога (храним только последние 1000 записей)
            if len(self.activity_log[account_id]) > 1000:
                self.activity_log[account_id] = self.activity_log[account_id][-1000:]
            
            logger.info(f"Активность зарегистрирована для аккаунта {account_id}: {action_type} ({'успешно' if success else 'неудачно'})")
            
        except Exception as e:
            logger.error(f"Ошибка логирования активности для аккаунта {account_id}: {e}")
    
    def get_activity_stats(self, account_id: int, hours: int = 24) -> Dict[str, int]:
        """Получение статистики активности за последние часы"""
        try:
            if account_id not in self.activity_log:
                return {}
            
            cutoff_time = time.time() - (hours * 3600)
            recent_activities = [
                activity for activity in self.activity_log[account_id]
                if activity['timestamp'] > cutoff_time
            ]
            
            stats = {}
            for activity in recent_activities:
                action = activity['action']
                if action not in stats:
                    stats[action] = 0
                if activity['success']:
                    stats[action] += 1
            
            logger.info(f"Статистика активности для аккаунта {account_id} за последние {hours} часов: {stats}")
            return stats
            
        except Exception as e:
            logger.error(f"Ошибка получения статистики для аккаунта {account_id}: {e}")
            return {}
    
    def _check_shadowban_status(self, account_id: int) -> bool:
        """Проверка статуса shadowban (внутренний метод)"""
        # Симуляция проверки - в реальности здесь API запрос
        return True  # Предполагаем отсутствие shadowban
    
    def _check_follow_limits(self, account_id: int) -> bool:
        """Проверка лимитов подписок (внутренний метод)"""
        # Симуляция проверки
        return True
    
    def _check_like_limits(self, account_id: int) -> bool:
        """Проверка лимитов лайков (внутренний метод)"""
        # Симуляция проверки
        return True
    
    def _check_temporary_blocks(self, account_id: int) -> bool:
        """Проверка временных блокировок (внутренний метод)"""
        # Симуляция проверки
        return True
    
    def _is_restriction_cache_valid(self, account_id: int) -> bool:
        """Проверка валидности кэша ограничений"""
        if account_id not in self.restriction_cache:
            return False
        
        cache_time = self.restriction_cache[account_id]['timestamp']
        return (time.time() - cache_time) < self.cache_timeout
    
    def clear_cache(self, account_id: Optional[int] = None):
        """Очистка кэша"""
        if account_id:
            if account_id in self.restriction_cache:
                del self.restriction_cache[account_id]
            if account_id in self.activity_log:
                del self.activity_log[account_id]
        else:
            self.restriction_cache.clear()
            self.activity_log.clear()
        
        logger.info(f"Кэш activity limiter очищен для {'всех аккаунтов' if not account_id else f'аккаунта {account_id}'}") 
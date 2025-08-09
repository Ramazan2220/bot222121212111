#!/usr/bin/env python3
"""
⏰ Subscription Monitor - Система мониторинга подписок
Автоматически отправляет напоминания о подписке:
- За 7 дней до истечения
- За 3 дня до истечения
- За 1 день до истечения
- В день истечения
"""

import logging
import time
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import schedule

# Импорты проекта
from utils.notification_manager import get_notification_manager, NotificationType, NotificationPriority
from utils.access_manager import has_access

logger = logging.getLogger(__name__)

class SubscriptionMonitor:
    """Монитор подписок для автоматических напоминаний"""
    
    def __init__(self):
        self.notification_manager = get_notification_manager()
        self._monitor_thread = None
        self._stop_monitoring = False
        
        # Настройки напоминаний
        self.REMINDER_DAYS = [7, 3, 1, 0]  # За сколько дней напоминать
        self.CHECK_INTERVAL = 3600  # Проверка каждый час
        
        logger.info("⏰ SubscriptionMonitor инициализирован")
    
    def start_monitoring(self):
        """Запускает мониторинг подписок"""
        if self._monitor_thread and self._monitor_thread.is_alive():
            logger.warning("Мониторинг уже запущен")
            return
        
        self._stop_monitoring = False
        self._monitor_thread = threading.Thread(target=self._monitoring_loop, daemon=True)
        self._monitor_thread.start()
        
        # Настраиваем расписание
        schedule.every().hour.do(self.check_all_subscriptions)
        schedule.every().day.at("09:00").do(self.check_all_subscriptions)  # Ежедневно в 9 утра
        schedule.every().day.at("18:00").do(self.check_all_subscriptions)  # И в 6 вечера
        
        logger.info("🔄 Мониторинг подписок запущен")
    
    def stop_monitoring(self):
        """Останавливает мониторинг подписок"""
        self._stop_monitoring = True
        if self._monitor_thread:
            self._monitor_thread.join(timeout=5)
        
        schedule.clear()
        logger.info("🛑 Мониторинг подписок остановлен")
    
    def _monitoring_loop(self):
        """Основной цикл мониторинга"""
        while not self._stop_monitoring:
            try:
                schedule.run_pending()
                time.sleep(60)  # Проверяем расписание каждую минуту
            except Exception as e:
                logger.error(f"Ошибка в цикле мониторинга: {e}")
                time.sleep(60)
    
    def check_all_subscriptions(self):
        """Проверяет все подписки и отправляет напоминания"""
        try:
            logger.info("🔍 Начинаем проверку всех подписок...")
            
            # Получаем всех пользователей из admin_bot
            users = self._get_all_users_from_admin_panel()
            
            checked_count = 0
            reminders_sent = 0
            
            for user in users:
                try:
                    if self._check_user_subscription(user):
                        reminders_sent += 1
                    checked_count += 1
                except Exception as e:
                    logger.error(f"Ошибка проверки пользователя {user.get('telegram_id', 'unknown')}: {e}")
            
            logger.info(f"✅ Проверка завершена: {checked_count} пользователей, {reminders_sent} напоминаний отправлено")
            
        except Exception as e:
            logger.error(f"Критическая ошибка проверки подписок: {e}")
    
    def _get_all_users_from_admin_panel(self) -> List[Dict]:
        """Получает всех пользователей из админ панели"""
        try:
            # Импортируем UserService
            import sys
            import os
            sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'admin_bot'))
            
            from admin_bot.services.user_service import UserService
            
            user_service = UserService()
            users = user_service.get_all_users()
            
            # Конвертируем в словари
            user_list = []
            for user in users:
                try:
                    user_dict = {
                        'telegram_id': user.telegram_id,
                        'username': user.username,
                        'subscription_plan': user.subscription_plan.value if user.subscription_plan else 'trial',
                        'subscription_end': user.subscription_end,
                        'is_active': user.is_active,
                        'created_at': user.created_at
                    }
                    user_list.append(user_dict)
                except Exception as e:
                    logger.error(f"Ошибка конвертации пользователя: {e}")
            
            logger.debug(f"📊 Получено {len(user_list)} пользователей из админ панели")
            return user_list
            
        except Exception as e:
            logger.error(f"Ошибка получения пользователей: {e}")
            return []
    
    def _check_user_subscription(self, user: Dict) -> bool:
        """Проверяет подписку конкретного пользователя"""
        try:
            user_id = user['telegram_id']
            subscription_end = user['subscription_end']
            
            if not subscription_end:
                return False  # Нет даты окончания подписки
            
            # Вычисляем дни до окончания
            if isinstance(subscription_end, str):
                subscription_end = datetime.fromisoformat(subscription_end.replace('Z', '+00:00'))
            
            days_left = (subscription_end - datetime.now()).days
            
            # Проверяем, нужно ли отправить напоминание
            should_remind = False
            priority = NotificationPriority.NORMAL
            
            if days_left in self.REMINDER_DAYS:
                should_remind = True
                
                if days_left <= 1:
                    priority = NotificationPriority.CRITICAL
                elif days_left <= 3:
                    priority = NotificationPriority.HIGH
            
            if should_remind:
                # Проверяем, не отправляли ли уже сегодня
                if not self._was_reminder_sent_today(user_id, days_left):
                    self._send_subscription_reminder(user_id, days_left, subscription_end, priority)
                    self._mark_reminder_sent(user_id, days_left)
                    return True
            
            return False
            
        except Exception as e:
            logger.error(f"Ошибка проверки подписки пользователя {user.get('telegram_id', 'unknown')}: {e}")
            return False
    
    def _send_subscription_reminder(self, user_id: int, days_left: int, subscription_end: datetime, priority: NotificationPriority):
        """Отправляет напоминание о подписке"""
        try:
            if days_left <= 0:
                # Подписка истекла
                self.notification_manager.send_subscription_expired(user_id, subscription_end)
                logger.info(f"📧 Отправлено уведомление об истечении подписки для {user_id}")
            else:
                # Предупреждение
                self.notification_manager.send_subscription_warning(user_id, days_left, subscription_end)
                logger.info(f"⏰ Отправлено напоминание о подписке для {user_id} (осталось {days_left} дн.)")
            
        except Exception as e:
            logger.error(f"Ошибка отправки напоминания для {user_id}: {e}")
    
    def _was_reminder_sent_today(self, user_id: int, days_left: int) -> bool:
        """Проверяет, отправлялось ли напоминание сегодня"""
        try:
            today = datetime.now().strftime('%Y-%m-%d')
            key = f"reminders_sent:{today}"
            reminder_key = f"{user_id}_{days_left}"
            
            redis_client = self.notification_manager.redis_client
            return redis_client.hexists(key, reminder_key)
            
        except Exception as e:
            logger.error(f"Ошибка проверки отправленных напоминаний: {e}")
            return False
    
    def _mark_reminder_sent(self, user_id: int, days_left: int):
        """Отмечает, что напоминание отправлено"""
        try:
            today = datetime.now().strftime('%Y-%m-%d')
            key = f"reminders_sent:{today}"
            reminder_key = f"{user_id}_{days_left}"
            
            redis_client = self.notification_manager.redis_client
            redis_client.hset(key, reminder_key, datetime.now().isoformat())
            
            # TTL 48 часов
            redis_client.expire(key, 48 * 3600)
            
        except Exception as e:
            logger.error(f"Ошибка отметки отправленного напоминания: {e}")
    
    def check_user_subscription_manual(self, user_id: int) -> Dict[str, Any]:
        """Ручная проверка подписки конкретного пользователя"""
        try:
            # Получаем пользователя из админ панели
            users = self._get_all_users_from_admin_panel()
            user = next((u for u in users if u['telegram_id'] == user_id), None)
            
            if not user:
                return {'error': 'Пользователь не найден'}
            
            subscription_end = user['subscription_end']
            if not subscription_end:
                return {'error': 'Дата окончания подписки не установлена'}
            
            if isinstance(subscription_end, str):
                subscription_end = datetime.fromisoformat(subscription_end.replace('Z', '+00:00'))
            
            days_left = (subscription_end - datetime.now()).days
            
            return {
                'user_id': user_id,
                'username': user['username'],
                'subscription_plan': user['subscription_plan'],
                'subscription_end': subscription_end.isoformat(),
                'days_left': days_left,
                'is_active': user['is_active'],
                'status': 'expired' if days_left < 0 else 'expiring_soon' if days_left <= 7 else 'active'
            }
            
        except Exception as e:
            logger.error(f"Ошибка ручной проверки подписки {user_id}: {e}")
            return {'error': str(e)}
    
    def get_expiring_subscriptions(self, days_ahead: int = 7) -> List[Dict]:
        """Получает список подписок, истекающих в ближайшие дни"""
        try:
            users = self._get_all_users_from_admin_panel()
            expiring = []
            
            for user in users:
                try:
                    subscription_end = user['subscription_end']
                    if not subscription_end:
                        continue
                    
                    if isinstance(subscription_end, str):
                        subscription_end = datetime.fromisoformat(subscription_end.replace('Z', '+00:00'))
                    
                    days_left = (subscription_end - datetime.now()).days
                    
                    if 0 <= days_left <= days_ahead:
                        expiring.append({
                            'user_id': user['telegram_id'],
                            'username': user['username'],
                            'subscription_plan': user['subscription_plan'],
                            'days_left': days_left,
                            'subscription_end': subscription_end.isoformat()
                        })
                
                except Exception as e:
                    logger.error(f"Ошибка обработки пользователя при получении истекающих подписок: {e}")
            
            # Сортируем по дням до истечения
            expiring.sort(key=lambda x: x['days_left'])
            
            return expiring
            
        except Exception as e:
            logger.error(f"Ошибка получения истекающих подписок: {e}")
            return []
    
    def get_monitor_stats(self) -> Dict[str, Any]:
        """Получает статистику мониторинга"""
        try:
            users = self._get_all_users_from_admin_panel()
            total_users = len(users)
            
            active_subscriptions = 0
            expiring_soon = 0
            expired = 0
            
            for user in users:
                try:
                    subscription_end = user['subscription_end']
                    if not subscription_end:
                        continue
                    
                    if isinstance(subscription_end, str):
                        subscription_end = datetime.fromisoformat(subscription_end.replace('Z', '+00:00'))
                    
                    days_left = (subscription_end - datetime.now()).days
                    
                    if days_left < 0:
                        expired += 1
                    elif days_left <= 7:
                        expiring_soon += 1
                    else:
                        active_subscriptions += 1
                
                except Exception as e:
                    logger.error(f"Ошибка обработки пользователя в статистике: {e}")
            
            return {
                'total_users': total_users,
                'active_subscriptions': active_subscriptions,
                'expiring_soon': expiring_soon,
                'expired': expired,
                'monitoring_active': self._monitor_thread and self._monitor_thread.is_alive(),
                'last_check': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Ошибка получения статистики мониторинга: {e}")
            return {'error': str(e)}

# Глобальный экземпляр
_subscription_monitor = None

def get_subscription_monitor() -> SubscriptionMonitor:
    """Получить глобальный экземпляр subscription monitor"""
    global _subscription_monitor
    if _subscription_monitor is None:
        _subscription_monitor = SubscriptionMonitor()
        _subscription_monitor.start_monitoring()
    return _subscription_monitor

# Удобные функции
def check_all_subscriptions():
    """Быстрая проверка всех подписок"""
    return get_subscription_monitor().check_all_subscriptions()

def get_expiring_subscriptions(days_ahead: int = 7):
    """Быстрое получение истекающих подписок"""
    return get_subscription_monitor().get_expiring_subscriptions(days_ahead)

def check_user_subscription(user_id: int):
    """Быстрая проверка подписки пользователя"""
    return get_subscription_monitor().check_user_subscription_manual(user_id)

if __name__ == "__main__":
    # Тестирование
    monitor = get_subscription_monitor()
    
    print("📊 Статистика мониторинга:")
    stats = monitor.get_monitor_stats()
    for key, value in stats.items():
        print(f"  {key}: {value}")
    
    print("\n⏰ Истекающие подписки:")
    expiring = monitor.get_expiring_subscriptions()
    for sub in expiring:
        print(f"  {sub['username']} ({sub['user_id']}): {sub['days_left']} дн.")
    
    print("\n✅ Subscription Monitor протестирован") 
"""
⏰ Subscription Monitor - Система мониторинга подписок
Автоматически отправляет напоминания о подписке:
- За 7 дней до истечения
- За 3 дня до истечения
- За 1 день до истечения
- В день истечения
"""

import logging
import time
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import schedule

# Импорты проекта
from utils.notification_manager import get_notification_manager, NotificationType, NotificationPriority
from utils.access_manager import has_access

logger = logging.getLogger(__name__)

class SubscriptionMonitor:
    """Монитор подписок для автоматических напоминаний"""
    
    def __init__(self):
        self.notification_manager = get_notification_manager()
        self._monitor_thread = None
        self._stop_monitoring = False
        
        # Настройки напоминаний
        self.REMINDER_DAYS = [7, 3, 1, 0]  # За сколько дней напоминать
        self.CHECK_INTERVAL = 3600  # Проверка каждый час
        
        logger.info("⏰ SubscriptionMonitor инициализирован")
    
    def start_monitoring(self):
        """Запускает мониторинг подписок"""
        if self._monitor_thread and self._monitor_thread.is_alive():
            logger.warning("Мониторинг уже запущен")
            return
        
        self._stop_monitoring = False
        self._monitor_thread = threading.Thread(target=self._monitoring_loop, daemon=True)
        self._monitor_thread.start()
        
        # Настраиваем расписание
        schedule.every().hour.do(self.check_all_subscriptions)
        schedule.every().day.at("09:00").do(self.check_all_subscriptions)  # Ежедневно в 9 утра
        schedule.every().day.at("18:00").do(self.check_all_subscriptions)  # И в 6 вечера
        
        logger.info("🔄 Мониторинг подписок запущен")
    
    def stop_monitoring(self):
        """Останавливает мониторинг подписок"""
        self._stop_monitoring = True
        if self._monitor_thread:
            self._monitor_thread.join(timeout=5)
        
        schedule.clear()
        logger.info("🛑 Мониторинг подписок остановлен")
    
    def _monitoring_loop(self):
        """Основной цикл мониторинга"""
        while not self._stop_monitoring:
            try:
                schedule.run_pending()
                time.sleep(60)  # Проверяем расписание каждую минуту
            except Exception as e:
                logger.error(f"Ошибка в цикле мониторинга: {e}")
                time.sleep(60)
    
    def check_all_subscriptions(self):
        """Проверяет все подписки и отправляет напоминания"""
        try:
            logger.info("🔍 Начинаем проверку всех подписок...")
            
            # Получаем всех пользователей из admin_bot
            users = self._get_all_users_from_admin_panel()
            
            checked_count = 0
            reminders_sent = 0
            
            for user in users:
                try:
                    if self._check_user_subscription(user):
                        reminders_sent += 1
                    checked_count += 1
                except Exception as e:
                    logger.error(f"Ошибка проверки пользователя {user.get('telegram_id', 'unknown')}: {e}")
            
            logger.info(f"✅ Проверка завершена: {checked_count} пользователей, {reminders_sent} напоминаний отправлено")
            
        except Exception as e:
            logger.error(f"Критическая ошибка проверки подписок: {e}")
    
    def _get_all_users_from_admin_panel(self) -> List[Dict]:
        """Получает всех пользователей из админ панели"""
        try:
            # Импортируем UserService
            import sys
            import os
            sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'admin_bot'))
            
            from admin_bot.services.user_service import UserService
            
            user_service = UserService()
            users = user_service.get_all_users()
            
            # Конвертируем в словари
            user_list = []
            for user in users:
                try:
                    user_dict = {
                        'telegram_id': user.telegram_id,
                        'username': user.username,
                        'subscription_plan': user.subscription_plan.value if user.subscription_plan else 'trial',
                        'subscription_end': user.subscription_end,
                        'is_active': user.is_active,
                        'created_at': user.created_at
                    }
                    user_list.append(user_dict)
                except Exception as e:
                    logger.error(f"Ошибка конвертации пользователя: {e}")
            
            logger.debug(f"📊 Получено {len(user_list)} пользователей из админ панели")
            return user_list
            
        except Exception as e:
            logger.error(f"Ошибка получения пользователей: {e}")
            return []
    
    def _check_user_subscription(self, user: Dict) -> bool:
        """Проверяет подписку конкретного пользователя"""
        try:
            user_id = user['telegram_id']
            subscription_end = user['subscription_end']
            
            if not subscription_end:
                return False  # Нет даты окончания подписки
            
            # Вычисляем дни до окончания
            if isinstance(subscription_end, str):
                subscription_end = datetime.fromisoformat(subscription_end.replace('Z', '+00:00'))
            
            days_left = (subscription_end - datetime.now()).days
            
            # Проверяем, нужно ли отправить напоминание
            should_remind = False
            priority = NotificationPriority.NORMAL
            
            if days_left in self.REMINDER_DAYS:
                should_remind = True
                
                if days_left <= 1:
                    priority = NotificationPriority.CRITICAL
                elif days_left <= 3:
                    priority = NotificationPriority.HIGH
            
            if should_remind:
                # Проверяем, не отправляли ли уже сегодня
                if not self._was_reminder_sent_today(user_id, days_left):
                    self._send_subscription_reminder(user_id, days_left, subscription_end, priority)
                    self._mark_reminder_sent(user_id, days_left)
                    return True
            
            return False
            
        except Exception as e:
            logger.error(f"Ошибка проверки подписки пользователя {user.get('telegram_id', 'unknown')}: {e}")
            return False
    
    def _send_subscription_reminder(self, user_id: int, days_left: int, subscription_end: datetime, priority: NotificationPriority):
        """Отправляет напоминание о подписке"""
        try:
            if days_left <= 0:
                # Подписка истекла
                self.notification_manager.send_subscription_expired(user_id, subscription_end)
                logger.info(f"📧 Отправлено уведомление об истечении подписки для {user_id}")
            else:
                # Предупреждение
                self.notification_manager.send_subscription_warning(user_id, days_left, subscription_end)
                logger.info(f"⏰ Отправлено напоминание о подписке для {user_id} (осталось {days_left} дн.)")
            
        except Exception as e:
            logger.error(f"Ошибка отправки напоминания для {user_id}: {e}")
    
    def _was_reminder_sent_today(self, user_id: int, days_left: int) -> bool:
        """Проверяет, отправлялось ли напоминание сегодня"""
        try:
            today = datetime.now().strftime('%Y-%m-%d')
            key = f"reminders_sent:{today}"
            reminder_key = f"{user_id}_{days_left}"
            
            redis_client = self.notification_manager.redis_client
            return redis_client.hexists(key, reminder_key)
            
        except Exception as e:
            logger.error(f"Ошибка проверки отправленных напоминаний: {e}")
            return False
    
    def _mark_reminder_sent(self, user_id: int, days_left: int):
        """Отмечает, что напоминание отправлено"""
        try:
            today = datetime.now().strftime('%Y-%m-%d')
            key = f"reminders_sent:{today}"
            reminder_key = f"{user_id}_{days_left}"
            
            redis_client = self.notification_manager.redis_client
            redis_client.hset(key, reminder_key, datetime.now().isoformat())
            
            # TTL 48 часов
            redis_client.expire(key, 48 * 3600)
            
        except Exception as e:
            logger.error(f"Ошибка отметки отправленного напоминания: {e}")
    
    def check_user_subscription_manual(self, user_id: int) -> Dict[str, Any]:
        """Ручная проверка подписки конкретного пользователя"""
        try:
            # Получаем пользователя из админ панели
            users = self._get_all_users_from_admin_panel()
            user = next((u for u in users if u['telegram_id'] == user_id), None)
            
            if not user:
                return {'error': 'Пользователь не найден'}
            
            subscription_end = user['subscription_end']
            if not subscription_end:
                return {'error': 'Дата окончания подписки не установлена'}
            
            if isinstance(subscription_end, str):
                subscription_end = datetime.fromisoformat(subscription_end.replace('Z', '+00:00'))
            
            days_left = (subscription_end - datetime.now()).days
            
            return {
                'user_id': user_id,
                'username': user['username'],
                'subscription_plan': user['subscription_plan'],
                'subscription_end': subscription_end.isoformat(),
                'days_left': days_left,
                'is_active': user['is_active'],
                'status': 'expired' if days_left < 0 else 'expiring_soon' if days_left <= 7 else 'active'
            }
            
        except Exception as e:
            logger.error(f"Ошибка ручной проверки подписки {user_id}: {e}")
            return {'error': str(e)}
    
    def get_expiring_subscriptions(self, days_ahead: int = 7) -> List[Dict]:
        """Получает список подписок, истекающих в ближайшие дни"""
        try:
            users = self._get_all_users_from_admin_panel()
            expiring = []
            
            for user in users:
                try:
                    subscription_end = user['subscription_end']
                    if not subscription_end:
                        continue
                    
                    if isinstance(subscription_end, str):
                        subscription_end = datetime.fromisoformat(subscription_end.replace('Z', '+00:00'))
                    
                    days_left = (subscription_end - datetime.now()).days
                    
                    if 0 <= days_left <= days_ahead:
                        expiring.append({
                            'user_id': user['telegram_id'],
                            'username': user['username'],
                            'subscription_plan': user['subscription_plan'],
                            'days_left': days_left,
                            'subscription_end': subscription_end.isoformat()
                        })
                
                except Exception as e:
                    logger.error(f"Ошибка обработки пользователя при получении истекающих подписок: {e}")
            
            # Сортируем по дням до истечения
            expiring.sort(key=lambda x: x['days_left'])
            
            return expiring
            
        except Exception as e:
            logger.error(f"Ошибка получения истекающих подписок: {e}")
            return []
    
    def get_monitor_stats(self) -> Dict[str, Any]:
        """Получает статистику мониторинга"""
        try:
            users = self._get_all_users_from_admin_panel()
            total_users = len(users)
            
            active_subscriptions = 0
            expiring_soon = 0
            expired = 0
            
            for user in users:
                try:
                    subscription_end = user['subscription_end']
                    if not subscription_end:
                        continue
                    
                    if isinstance(subscription_end, str):
                        subscription_end = datetime.fromisoformat(subscription_end.replace('Z', '+00:00'))
                    
                    days_left = (subscription_end - datetime.now()).days
                    
                    if days_left < 0:
                        expired += 1
                    elif days_left <= 7:
                        expiring_soon += 1
                    else:
                        active_subscriptions += 1
                
                except Exception as e:
                    logger.error(f"Ошибка обработки пользователя в статистике: {e}")
            
            return {
                'total_users': total_users,
                'active_subscriptions': active_subscriptions,
                'expiring_soon': expiring_soon,
                'expired': expired,
                'monitoring_active': self._monitor_thread and self._monitor_thread.is_alive(),
                'last_check': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Ошибка получения статистики мониторинга: {e}")
            return {'error': str(e)}

# Глобальный экземпляр
_subscription_monitor = None

def get_subscription_monitor() -> SubscriptionMonitor:
    """Получить глобальный экземпляр subscription monitor"""
    global _subscription_monitor
    if _subscription_monitor is None:
        _subscription_monitor = SubscriptionMonitor()
        _subscription_monitor.start_monitoring()
    return _subscription_monitor

# Удобные функции
def check_all_subscriptions():
    """Быстрая проверка всех подписок"""
    return get_subscription_monitor().check_all_subscriptions()

def get_expiring_subscriptions(days_ahead: int = 7):
    """Быстрое получение истекающих подписок"""
    return get_subscription_monitor().get_expiring_subscriptions(days_ahead)

def check_user_subscription(user_id: int):
    """Быстрая проверка подписки пользователя"""
    return get_subscription_monitor().check_user_subscription_manual(user_id)

if __name__ == "__main__":
    # Тестирование
    monitor = get_subscription_monitor()
    
    print("📊 Статистика мониторинга:")
    stats = monitor.get_monitor_stats()
    for key, value in stats.items():
        print(f"  {key}: {value}")
    
    print("\n⏰ Истекающие подписки:")
    expiring = monitor.get_expiring_subscriptions()
    for sub in expiring:
        print(f"  {sub['username']} ({sub['user_id']}): {sub['days_left']} дн.")
    
    print("\n✅ Subscription Monitor протестирован") 
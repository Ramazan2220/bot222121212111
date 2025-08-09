#!/usr/bin/env python3
"""
🚀 Redis Notification Manager - Мощная система уведомлений
Поддерживает:
- Мгновенную блокировку пользователей
- Массовые рассылки всем пользователям
- Персональные уведомления
- Автоматические напоминания о подписке
- Отложенные уведомления
"""

import logging
import time
import json
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass, asdict
from enum import Enum

# Импортируем Redis система
from redis_access_sync import get_redis_sync

logger = logging.getLogger(__name__)

class NotificationType(Enum):
    """Типы уведомлений"""
    ADMIN_BLOCK = "admin_block"          # Блокировка от админа
    ADMIN_UNBLOCK = "admin_unblock"      # Разблокировка от админа
    SUBSCRIPTION_WARNING = "subscription_warning"  # Предупреждение о подписке
    SUBSCRIPTION_EXPIRED = "subscription_expired"  # Подписка истекла
    BROADCAST_ALL = "broadcast_all"      # Рассылка всем
    BROADCAST_GROUP = "broadcast_group"  # Рассылка группе
    PERSONAL = "personal"                # Персональное уведомление
    SYSTEM_UPDATE = "system_update"      # Обновление системы
    PROMO = "promo"                      # Промо уведомление

class NotificationPriority(Enum):
    """Приоритет уведомлений"""
    CRITICAL = "critical"    # Критические (блокировка)
    HIGH = "high"           # Высокий (истечение подписки)
    NORMAL = "normal"       # Обычный (напоминания)
    LOW = "low"             # Низкий (промо)

@dataclass
class Notification:
    """Структура уведомления"""
    id: str
    type: NotificationType
    priority: NotificationPriority
    title: str
    message: str
    user_id: Optional[int] = None
    user_group: Optional[str] = None  # trial, premium, free
    created_at: str = None
    scheduled_at: Optional[str] = None  # Отложенное уведомление
    data: Dict[str, Any] = None  # Дополнительные данные
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now().isoformat()
        if self.data is None:
            self.data = {}

class RedisNotificationManager:
    """Менеджер уведомлений через Redis Pub/Sub"""
    
    def __init__(self):
        self.redis_client = get_redis_sync().redis_client
        self.pubsub = self.redis_client.pubsub()
        
        # Каналы Redis
        self.CHANNELS = {
            'admin_actions': 'notifications:admin_actions',
            'subscription': 'notifications:subscription',
            'broadcast': 'notifications:broadcast',
            'personal': 'notifications:personal',
            'system': 'notifications:system',
            'access_added': 'access:user_added',
            'access_removed': 'access:user_removed'
        }
        
        # Ключи для хранения
        self.KEYS = {
            'pending': 'notifications:pending',
            'sent': 'notifications:sent',
            'scheduled': 'notifications:scheduled',
            'stats': 'notifications:stats'
        }
        
        self._listener_thread = None
        self._stop_listening = False
        self._notification_handlers = {}
        
        logger.info("🔔 RedisNotificationManager инициализирован")
    
    def start_listener(self):
        """Запускает listener для уведомлений"""
        if self._listener_thread and self._listener_thread.is_alive():
            return
        
        # Подписываемся на все каналы
        for channel in self.CHANNELS.values():
            self.pubsub.subscribe(channel)
        
        self._stop_listening = False
        self._listener_thread = threading.Thread(target=self._notification_listener, daemon=True)
        self._listener_thread.start()
        logger.info("🔄 Notification listener запущен")
    
    def stop_listener(self):
        """Останавливает listener"""
        self._stop_listening = True
        if self._listener_thread:
            self._listener_thread.join(timeout=2)
        logger.info("🛑 Notification listener остановлен")
    
    def _notification_listener(self):
        """Обработчик входящих уведомлений"""
        try:
            for message in self.pubsub.listen():
                if self._stop_listening:
                    break
                
                if message['type'] == 'message':
                    try:
                        data = json.loads(message['data'])
                        channel = message['channel']
                        
                        logger.info(f"🔥 ПОЛУЧЕНО СООБЩЕНИЕ: канал={channel}, данные={data}")
                        
                        # Если есть обработчик для этого канала
                        if channel in self._notification_handlers:
                            handler = self._notification_handlers[channel]
                            logger.info(f"📞 Вызываем обработчик для канала {channel}")
                            handler(data)
                        else:
                            logger.warning(f"⚠️ Нет обработчика для канала: {channel}")
                            
                    except json.JSONDecodeError as e:
                        logger.error(f"Ошибка парсинга JSON: {e}")
                    except Exception as e:
                        logger.error(f"Ошибка обработки уведомления: {e}")
        except Exception as e:
            logger.error(f"Критическая ошибка в notification listener: {e}")
    
    def register_handler(self, channel: str, handler_func):
        """Регистрирует обработчик для канала"""
        self._notification_handlers[channel] = handler_func
        logger.info(f"📝 Зарегистрирован обработчик для канала: {channel}")
    
    # ========================
    # ОТПРАВКА УВЕДОМЛЕНИЙ
    # ========================
    
    def send_admin_block_notification(self, user_id: int, admin_id: int, reason: str = ""):
        """Отправляет уведомление о блокировке пользователя"""
        notification = Notification(
            id=f"block_{user_id}_{int(time.time())}",
            type=NotificationType.ADMIN_BLOCK,
            priority=NotificationPriority.CRITICAL,
            title="🚫 ДОСТУП ЗАБЛОКИРОВАН",
            message=f"Ваш доступ к боту заблокирован администратором.\n\nПричина: {reason or 'Не указана'}\n\n📞 Обратитесь в поддержку для разблокировки.",
            user_id=user_id,
            data={'admin_id': admin_id, 'reason': reason}
        )
        
        return self._send_notification(notification, self.CHANNELS['admin_actions'])
    
    def send_admin_unblock_notification(self, user_id: int, admin_id: int):
        """Отправляет уведомление о разблокировке пользователя"""
        notification = Notification(
            id=f"unblock_{user_id}_{int(time.time())}",
            type=NotificationType.ADMIN_UNBLOCK,
            priority=NotificationPriority.HIGH,
            title="✅ ДОСТУП ВОССТАНОВЛЕН",
            message="🎉 Ваш доступ к боту восстановлен!\n\nВы можете продолжить использование всех функций.",
            user_id=user_id,
            data={'admin_id': admin_id}
        )
        
        return self._send_notification(notification, self.CHANNELS['admin_actions'])
    
    def send_subscription_warning(self, user_id: int, days_left: int, subscription_end: datetime):
        """Отправляет предупреждение об истечении подписки"""
        if days_left <= 0:
            return self.send_subscription_expired(user_id, subscription_end)
        
        title = f"⏰ ПОДПИСКА ИСТЕКАЕТ ЧЕРЕЗ {days_left} ДН."
        if days_left == 1:
            title = "🚨 ПОДПИСКА ИСТЕКАЕТ ЗАВТРА!"
        
        message = f"""⚠️ **Ваша подписка истекает!**

📅 Осталось дней: **{days_left}**
🗓️ Дата окончания: **{subscription_end.strftime('%d.%m.%Y')}**

🔄 **Продлите подписку**, чтобы не потерять доступ ко всем функциям!

💎 Обратитесь к администратору для продления."""
        
        notification = Notification(
            id=f"sub_warning_{user_id}_{days_left}",
            type=NotificationType.SUBSCRIPTION_WARNING,
            priority=NotificationPriority.HIGH if days_left <= 3 else NotificationPriority.NORMAL,
            title=title,
            message=message,
            user_id=user_id,
            data={'days_left': days_left, 'subscription_end': subscription_end.isoformat()}
        )
        
        return self._send_notification(notification, self.CHANNELS['subscription'])
    
    def send_subscription_expired(self, user_id: int, subscription_end: datetime):
        """Отправляет уведомление об истечении подписки"""
        notification = Notification(
            id=f"sub_expired_{user_id}_{int(time.time())}",
            type=NotificationType.SUBSCRIPTION_EXPIRED,
            priority=NotificationPriority.CRITICAL,
            title="🚨 ПОДПИСКА ИСТЕКЛА",
            message=f"""❌ **Ваша подписка истекла!**

🗓️ Дата истечения: **{subscription_end.strftime('%d.%m.%Y')}**

🔒 Доступ к функциям бота ограничен.

💰 **Продлите подписку** для восстановления полного доступа!

📞 Обратитесь к администратору.""",
            user_id=user_id,
            data={'subscription_end': subscription_end.isoformat()}
        )
        
        return self._send_notification(notification, self.CHANNELS['subscription'])
    
    def send_broadcast_to_all(self, title: str, message: str, admin_id: int, notification_type: NotificationType = NotificationType.BROADCAST_ALL):
        """Отправляет рассылку всем пользователям"""
        notification = Notification(
            id=f"broadcast_all_{int(time.time())}",
            type=notification_type,
            priority=NotificationPriority.NORMAL,
            title=title,
            message=message,
            data={'admin_id': admin_id, 'broadcast_type': 'all'}
        )
        
        return self._send_notification(notification, self.CHANNELS['broadcast'])
    
    def send_broadcast_to_group(self, title: str, message: str, user_group: str, admin_id: int):
        """Отправляет рассылку определенной группе пользователей"""
        notification = Notification(
            id=f"broadcast_{user_group}_{int(time.time())}",
            type=NotificationType.BROADCAST_GROUP,
            priority=NotificationPriority.NORMAL,
            title=title,
            message=message,
            user_group=user_group,
            data={'admin_id': admin_id, 'broadcast_type': 'group', 'target_group': user_group}
        )
        
        return self._send_notification(notification, self.CHANNELS['broadcast'])
    
    def send_personal_notification(self, user_id: int, title: str, message: str, admin_id: int, priority: NotificationPriority = NotificationPriority.NORMAL):
        """Отправляет персональное уведомление"""
        notification = Notification(
            id=f"personal_{user_id}_{int(time.time())}",
            type=NotificationType.PERSONAL,
            priority=priority,
            title=title,
            message=message,
            user_id=user_id,
            data={'admin_id': admin_id}
        )
        
        return self._send_notification(notification, self.CHANNELS['personal'])
    
    def send_system_update_notification(self, title: str, message: str, version: str = ""):
        """Отправляет уведомление об обновлении системы"""
        notification = Notification(
            id=f"system_update_{int(time.time())}",
            type=NotificationType.SYSTEM_UPDATE,
            priority=NotificationPriority.HIGH,
            title=title,
            message=message,
            data={'version': version, 'update_type': 'system'}
        )
        
        return self._send_notification(notification, self.CHANNELS['system'])
    
    def _send_notification(self, notification: Notification, channel: str) -> bool:
        """Отправляет уведомление в Redis канал"""
        try:
            # Сохраняем в pending
            self.redis_client.hset(
                self.KEYS['pending'], 
                notification.id, 
                json.dumps(asdict(notification))
            )
            
            # Подготавливаем данные для сериализации
            notification_dict = asdict(notification)
            # Конвертируем enum в строку
            notification_dict['type'] = notification_dict['type'].value
            notification_dict['priority'] = notification_dict['priority'].value
            
            # Отправляем в канал
            self.redis_client.publish(channel, json.dumps(notification_dict))
            
            # Обновляем статистику
            self._update_stats('sent')
            
            logger.info(f"📤 Уведомление отправлено: {notification.title} -> {channel}")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка отправки уведомления: {e}")
            return False
    
    # ========================
    # ОТЛОЖЕННЫЕ УВЕДОМЛЕНИЯ
    # ========================
    
    def schedule_notification(self, notification: Notification, send_at: datetime) -> bool:
        """Планирует отложенное уведомление"""
        try:
            notification.scheduled_at = send_at.isoformat()
            
            # Сохраняем в scheduled
            self.redis_client.hset(
                self.KEYS['scheduled'], 
                notification.id, 
                json.dumps(asdict(notification))
            )
            
            logger.info(f"⏰ Уведомление запланировано на {send_at}: {notification.title}")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка планирования уведомления: {e}")
            return False
    
    def process_scheduled_notifications(self):
        """Обрабатывает отложенные уведомления"""
        try:
            scheduled = self.redis_client.hgetall(self.KEYS['scheduled'])
            now = datetime.now()
            
            for notif_id, notif_data in scheduled.items():
                try:
                    data = json.loads(notif_data)
                    scheduled_at = datetime.fromisoformat(data['scheduled_at'])
                    
                    if now >= scheduled_at:
                        # Время пришло - отправляем
                        notification = Notification(**data)
                        
                        # Определяем канал по типу
                        channel = self._get_channel_by_type(notification.type)
                        
                        if self._send_notification(notification, channel):
                            # Удаляем из scheduled
                            self.redis_client.hdel(self.KEYS['scheduled'], notif_id)
                            logger.info(f"⏰✅ Отложенное уведомление отправлено: {notification.title}")
                
                except Exception as e:
                    logger.error(f"Ошибка обработки отложенного уведомления {notif_id}: {e}")
            
        except Exception as e:
            logger.error(f"Ошибка обработки отложенных уведомлений: {e}")
    
    def _get_channel_by_type(self, notification_type: NotificationType) -> str:
        """Определяет канал по типу уведомления"""
        mapping = {
            NotificationType.ADMIN_BLOCK: self.CHANNELS['admin_actions'],
            NotificationType.ADMIN_UNBLOCK: self.CHANNELS['admin_actions'],
            NotificationType.SUBSCRIPTION_WARNING: self.CHANNELS['subscription'],
            NotificationType.SUBSCRIPTION_EXPIRED: self.CHANNELS['subscription'],
            NotificationType.BROADCAST_ALL: self.CHANNELS['broadcast'],
            NotificationType.BROADCAST_GROUP: self.CHANNELS['broadcast'],
            NotificationType.PERSONAL: self.CHANNELS['personal'],
            NotificationType.SYSTEM_UPDATE: self.CHANNELS['system'],
            NotificationType.PROMO: self.CHANNELS['broadcast']
        }
        return mapping.get(notification_type, self.CHANNELS['personal'])
    
    # ========================
    # СТАТИСТИКА
    # ========================
    
    def _update_stats(self, action: str):
        """Обновляет статистику уведомлений"""
        try:
            today = datetime.now().strftime('%Y-%m-%d')
            key = f"{self.KEYS['stats']}:{today}"
            
            self.redis_client.hincrby(key, action, 1)
            self.redis_client.hincrby(key, 'total', 1)
            
            # TTL 30 дней
            self.redis_client.expire(key, 30 * 24 * 3600)
            
        except Exception as e:
            logger.error(f"Ошибка обновления статистики: {e}")
    
    def get_stats(self, days: int = 7) -> Dict[str, Any]:
        """Получает статистику уведомлений"""
        try:
            stats = {}
            total_sent = 0
            
            for i in range(days):
                date = (datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d')
                key = f"{self.KEYS['stats']}:{date}"
                
                day_stats = self.redis_client.hgetall(key)
                if day_stats:
                    stats[date] = {k: int(v) for k, v in day_stats.items()}
                    total_sent += int(day_stats.get('sent', 0))
                else:
                    stats[date] = {'sent': 0, 'total': 0}
            
            return {
                'daily_stats': stats,
                'total_sent_period': total_sent,
                'pending_count': len(self.redis_client.hgetall(self.KEYS['pending'])),
                'scheduled_count': len(self.redis_client.hgetall(self.KEYS['scheduled']))
            }
            
        except Exception as e:
            logger.error(f"Ошибка получения статистики: {e}")
            return {}
    
    def mark_notification_delivered(self, notification_id: str, user_id: int):
        """Отмечает уведомление как доставленное"""
        try:
            # Перемещаем из pending в sent
            notif_data = self.redis_client.hget(self.KEYS['pending'], notification_id)
            if notif_data:
                data = json.loads(notif_data)
                data['delivered_at'] = datetime.now().isoformat()
                data['delivered_to'] = user_id
                
                self.redis_client.hset(self.KEYS['sent'], notification_id, json.dumps(data))
                self.redis_client.hdel(self.KEYS['pending'], notification_id)
                
                self._update_stats('delivered')
                logger.debug(f"✅ Уведомление {notification_id} доставлено пользователю {user_id}")
        
        except Exception as e:
            logger.error(f"Ошибка отметки доставки: {e}")

# Глобальный экземпляр
_notification_manager = None

def get_notification_manager() -> RedisNotificationManager:
    """Получить глобальный экземпляр notification manager"""
    global _notification_manager
    if _notification_manager is None:
        _notification_manager = RedisNotificationManager()
        _notification_manager.start_listener()
    return _notification_manager

# Удобные функции
def send_block_notification(user_id: int, admin_id: int, reason: str = ""):
    """Быстрая отправка уведомления о блокировке"""
    return get_notification_manager().send_admin_block_notification(user_id, admin_id, reason)

def send_unblock_notification(user_id: int, admin_id: int):
    """Быстрая отправка уведомления о разблокировке"""
    return get_notification_manager().send_admin_unblock_notification(user_id, admin_id)

def send_broadcast_all(title: str, message: str, admin_id: int):
    """Быстрая рассылка всем пользователям"""
    return get_notification_manager().send_broadcast_to_all(title, message, admin_id)

def send_subscription_reminder(user_id: int, days_left: int, subscription_end: datetime):
    """Быстрое напоминание о подписке"""
    return get_notification_manager().send_subscription_warning(user_id, days_left, subscription_end)

if __name__ == "__main__":
    # Тестирование
    nm = get_notification_manager()
    
    # Тест уведомления
    nm.send_broadcast_to_all(
        "🚀 Тестовое уведомление", 
        "Система уведомлений работает!", 
        123456
    )
    
    print("✅ Notification Manager протестирован") 
"""
🚀 Redis Notification Manager - Мощная система уведомлений
Поддерживает:
- Мгновенную блокировку пользователей
- Массовые рассылки всем пользователям
- Персональные уведомления
- Автоматические напоминания о подписке
- Отложенные уведомления
"""

import logging
import time
import json
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass, asdict
from enum import Enum

# Импортируем Redis система
from redis_access_sync import get_redis_sync

logger = logging.getLogger(__name__)

class NotificationType(Enum):
    """Типы уведомлений"""
    ADMIN_BLOCK = "admin_block"          # Блокировка от админа
    ADMIN_UNBLOCK = "admin_unblock"      # Разблокировка от админа
    SUBSCRIPTION_WARNING = "subscription_warning"  # Предупреждение о подписке
    SUBSCRIPTION_EXPIRED = "subscription_expired"  # Подписка истекла
    BROADCAST_ALL = "broadcast_all"      # Рассылка всем
    BROADCAST_GROUP = "broadcast_group"  # Рассылка группе
    PERSONAL = "personal"                # Персональное уведомление
    SYSTEM_UPDATE = "system_update"      # Обновление системы
    PROMO = "promo"                      # Промо уведомление

class NotificationPriority(Enum):
    """Приоритет уведомлений"""
    CRITICAL = "critical"    # Критические (блокировка)
    HIGH = "high"           # Высокий (истечение подписки)
    NORMAL = "normal"       # Обычный (напоминания)
    LOW = "low"             # Низкий (промо)

@dataclass
class Notification:
    """Структура уведомления"""
    id: str
    type: NotificationType
    priority: NotificationPriority
    title: str
    message: str
    user_id: Optional[int] = None
    user_group: Optional[str] = None  # trial, premium, free
    created_at: str = None
    scheduled_at: Optional[str] = None  # Отложенное уведомление
    data: Dict[str, Any] = None  # Дополнительные данные
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now().isoformat()
        if self.data is None:
            self.data = {}

class RedisNotificationManager:
    """Менеджер уведомлений через Redis Pub/Sub"""
    
    def __init__(self):
        self.redis_client = get_redis_sync().redis_client
        self.pubsub = self.redis_client.pubsub()
        
        # Каналы Redis
        self.CHANNELS = {
            'admin_actions': 'notifications:admin_actions',
            'subscription': 'notifications:subscription',
            'broadcast': 'notifications:broadcast',
            'personal': 'notifications:personal',
            'system': 'notifications:system',
            'access_added': 'access:user_added',
            'access_removed': 'access:user_removed'
        }
        
        # Ключи для хранения
        self.KEYS = {
            'pending': 'notifications:pending',
            'sent': 'notifications:sent',
            'scheduled': 'notifications:scheduled',
            'stats': 'notifications:stats'
        }
        
        self._listener_thread = None
        self._stop_listening = False
        self._notification_handlers = {}
        
        logger.info("🔔 RedisNotificationManager инициализирован")
    
    def start_listener(self):
        """Запускает listener для уведомлений"""
        if self._listener_thread and self._listener_thread.is_alive():
            return
        
        # Подписываемся на все каналы
        for channel in self.CHANNELS.values():
            self.pubsub.subscribe(channel)
        
        self._stop_listening = False
        self._listener_thread = threading.Thread(target=self._notification_listener, daemon=True)
        self._listener_thread.start()
        logger.info("🔄 Notification listener запущен")
    
    def stop_listener(self):
        """Останавливает listener"""
        self._stop_listening = True
        if self._listener_thread:
            self._listener_thread.join(timeout=2)
        logger.info("🛑 Notification listener остановлен")
    
    def _notification_listener(self):
        """Обработчик входящих уведомлений"""
        try:
            for message in self.pubsub.listen():
                if self._stop_listening:
                    break
                
                if message['type'] == 'message':
                    try:
                        data = json.loads(message['data'])
                        channel = message['channel']
                        
                        logger.info(f"🔥 ПОЛУЧЕНО СООБЩЕНИЕ: канал={channel}, данные={data}")
                        
                        # Если есть обработчик для этого канала
                        if channel in self._notification_handlers:
                            handler = self._notification_handlers[channel]
                            logger.info(f"📞 Вызываем обработчик для канала {channel}")
                            handler(data)
                        else:
                            logger.warning(f"⚠️ Нет обработчика для канала: {channel}")
                            
                    except json.JSONDecodeError as e:
                        logger.error(f"Ошибка парсинга JSON: {e}")
                    except Exception as e:
                        logger.error(f"Ошибка обработки уведомления: {e}")
        except Exception as e:
            logger.error(f"Критическая ошибка в notification listener: {e}")
    
    def register_handler(self, channel: str, handler_func):
        """Регистрирует обработчик для канала"""
        self._notification_handlers[channel] = handler_func
        logger.info(f"📝 Зарегистрирован обработчик для канала: {channel}")
    
    # ========================
    # ОТПРАВКА УВЕДОМЛЕНИЙ
    # ========================
    
    def send_admin_block_notification(self, user_id: int, admin_id: int, reason: str = ""):
        """Отправляет уведомление о блокировке пользователя"""
        notification = Notification(
            id=f"block_{user_id}_{int(time.time())}",
            type=NotificationType.ADMIN_BLOCK,
            priority=NotificationPriority.CRITICAL,
            title="🚫 ДОСТУП ЗАБЛОКИРОВАН",
            message=f"Ваш доступ к боту заблокирован администратором.\n\nПричина: {reason or 'Не указана'}\n\n📞 Обратитесь в поддержку для разблокировки.",
            user_id=user_id,
            data={'admin_id': admin_id, 'reason': reason}
        )
        
        return self._send_notification(notification, self.CHANNELS['admin_actions'])
    
    def send_admin_unblock_notification(self, user_id: int, admin_id: int):
        """Отправляет уведомление о разблокировке пользователя"""
        notification = Notification(
            id=f"unblock_{user_id}_{int(time.time())}",
            type=NotificationType.ADMIN_UNBLOCK,
            priority=NotificationPriority.HIGH,
            title="✅ ДОСТУП ВОССТАНОВЛЕН",
            message="🎉 Ваш доступ к боту восстановлен!\n\nВы можете продолжить использование всех функций.",
            user_id=user_id,
            data={'admin_id': admin_id}
        )
        
        return self._send_notification(notification, self.CHANNELS['admin_actions'])
    
    def send_subscription_warning(self, user_id: int, days_left: int, subscription_end: datetime):
        """Отправляет предупреждение об истечении подписки"""
        if days_left <= 0:
            return self.send_subscription_expired(user_id, subscription_end)
        
        title = f"⏰ ПОДПИСКА ИСТЕКАЕТ ЧЕРЕЗ {days_left} ДН."
        if days_left == 1:
            title = "🚨 ПОДПИСКА ИСТЕКАЕТ ЗАВТРА!"
        
        message = f"""⚠️ **Ваша подписка истекает!**

📅 Осталось дней: **{days_left}**
🗓️ Дата окончания: **{subscription_end.strftime('%d.%m.%Y')}**

🔄 **Продлите подписку**, чтобы не потерять доступ ко всем функциям!

💎 Обратитесь к администратору для продления."""
        
        notification = Notification(
            id=f"sub_warning_{user_id}_{days_left}",
            type=NotificationType.SUBSCRIPTION_WARNING,
            priority=NotificationPriority.HIGH if days_left <= 3 else NotificationPriority.NORMAL,
            title=title,
            message=message,
            user_id=user_id,
            data={'days_left': days_left, 'subscription_end': subscription_end.isoformat()}
        )
        
        return self._send_notification(notification, self.CHANNELS['subscription'])
    
    def send_subscription_expired(self, user_id: int, subscription_end: datetime):
        """Отправляет уведомление об истечении подписки"""
        notification = Notification(
            id=f"sub_expired_{user_id}_{int(time.time())}",
            type=NotificationType.SUBSCRIPTION_EXPIRED,
            priority=NotificationPriority.CRITICAL,
            title="🚨 ПОДПИСКА ИСТЕКЛА",
            message=f"""❌ **Ваша подписка истекла!**

🗓️ Дата истечения: **{subscription_end.strftime('%d.%m.%Y')}**

🔒 Доступ к функциям бота ограничен.

💰 **Продлите подписку** для восстановления полного доступа!

📞 Обратитесь к администратору.""",
            user_id=user_id,
            data={'subscription_end': subscription_end.isoformat()}
        )
        
        return self._send_notification(notification, self.CHANNELS['subscription'])
    
    def send_broadcast_to_all(self, title: str, message: str, admin_id: int, notification_type: NotificationType = NotificationType.BROADCAST_ALL):
        """Отправляет рассылку всем пользователям"""
        notification = Notification(
            id=f"broadcast_all_{int(time.time())}",
            type=notification_type,
            priority=NotificationPriority.NORMAL,
            title=title,
            message=message,
            data={'admin_id': admin_id, 'broadcast_type': 'all'}
        )
        
        return self._send_notification(notification, self.CHANNELS['broadcast'])
    
    def send_broadcast_to_group(self, title: str, message: str, user_group: str, admin_id: int):
        """Отправляет рассылку определенной группе пользователей"""
        notification = Notification(
            id=f"broadcast_{user_group}_{int(time.time())}",
            type=NotificationType.BROADCAST_GROUP,
            priority=NotificationPriority.NORMAL,
            title=title,
            message=message,
            user_group=user_group,
            data={'admin_id': admin_id, 'broadcast_type': 'group', 'target_group': user_group}
        )
        
        return self._send_notification(notification, self.CHANNELS['broadcast'])
    
    def send_personal_notification(self, user_id: int, title: str, message: str, admin_id: int, priority: NotificationPriority = NotificationPriority.NORMAL):
        """Отправляет персональное уведомление"""
        notification = Notification(
            id=f"personal_{user_id}_{int(time.time())}",
            type=NotificationType.PERSONAL,
            priority=priority,
            title=title,
            message=message,
            user_id=user_id,
            data={'admin_id': admin_id}
        )
        
        return self._send_notification(notification, self.CHANNELS['personal'])
    
    def send_system_update_notification(self, title: str, message: str, version: str = ""):
        """Отправляет уведомление об обновлении системы"""
        notification = Notification(
            id=f"system_update_{int(time.time())}",
            type=NotificationType.SYSTEM_UPDATE,
            priority=NotificationPriority.HIGH,
            title=title,
            message=message,
            data={'version': version, 'update_type': 'system'}
        )
        
        return self._send_notification(notification, self.CHANNELS['system'])
    
    def _send_notification(self, notification: Notification, channel: str) -> bool:
        """Отправляет уведомление в Redis канал"""
        try:
            # Сохраняем в pending
            self.redis_client.hset(
                self.KEYS['pending'], 
                notification.id, 
                json.dumps(asdict(notification))
            )
            
            # Подготавливаем данные для сериализации
            notification_dict = asdict(notification)
            # Конвертируем enum в строку
            notification_dict['type'] = notification_dict['type'].value
            notification_dict['priority'] = notification_dict['priority'].value
            
            # Отправляем в канал
            self.redis_client.publish(channel, json.dumps(notification_dict))
            
            # Обновляем статистику
            self._update_stats('sent')
            
            logger.info(f"📤 Уведомление отправлено: {notification.title} -> {channel}")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка отправки уведомления: {e}")
            return False
    
    # ========================
    # ОТЛОЖЕННЫЕ УВЕДОМЛЕНИЯ
    # ========================
    
    def schedule_notification(self, notification: Notification, send_at: datetime) -> bool:
        """Планирует отложенное уведомление"""
        try:
            notification.scheduled_at = send_at.isoformat()
            
            # Сохраняем в scheduled
            self.redis_client.hset(
                self.KEYS['scheduled'], 
                notification.id, 
                json.dumps(asdict(notification))
            )
            
            logger.info(f"⏰ Уведомление запланировано на {send_at}: {notification.title}")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка планирования уведомления: {e}")
            return False
    
    def process_scheduled_notifications(self):
        """Обрабатывает отложенные уведомления"""
        try:
            scheduled = self.redis_client.hgetall(self.KEYS['scheduled'])
            now = datetime.now()
            
            for notif_id, notif_data in scheduled.items():
                try:
                    data = json.loads(notif_data)
                    scheduled_at = datetime.fromisoformat(data['scheduled_at'])
                    
                    if now >= scheduled_at:
                        # Время пришло - отправляем
                        notification = Notification(**data)
                        
                        # Определяем канал по типу
                        channel = self._get_channel_by_type(notification.type)
                        
                        if self._send_notification(notification, channel):
                            # Удаляем из scheduled
                            self.redis_client.hdel(self.KEYS['scheduled'], notif_id)
                            logger.info(f"⏰✅ Отложенное уведомление отправлено: {notification.title}")
                
                except Exception as e:
                    logger.error(f"Ошибка обработки отложенного уведомления {notif_id}: {e}")
            
        except Exception as e:
            logger.error(f"Ошибка обработки отложенных уведомлений: {e}")
    
    def _get_channel_by_type(self, notification_type: NotificationType) -> str:
        """Определяет канал по типу уведомления"""
        mapping = {
            NotificationType.ADMIN_BLOCK: self.CHANNELS['admin_actions'],
            NotificationType.ADMIN_UNBLOCK: self.CHANNELS['admin_actions'],
            NotificationType.SUBSCRIPTION_WARNING: self.CHANNELS['subscription'],
            NotificationType.SUBSCRIPTION_EXPIRED: self.CHANNELS['subscription'],
            NotificationType.BROADCAST_ALL: self.CHANNELS['broadcast'],
            NotificationType.BROADCAST_GROUP: self.CHANNELS['broadcast'],
            NotificationType.PERSONAL: self.CHANNELS['personal'],
            NotificationType.SYSTEM_UPDATE: self.CHANNELS['system'],
            NotificationType.PROMO: self.CHANNELS['broadcast']
        }
        return mapping.get(notification_type, self.CHANNELS['personal'])
    
    # ========================
    # СТАТИСТИКА
    # ========================
    
    def _update_stats(self, action: str):
        """Обновляет статистику уведомлений"""
        try:
            today = datetime.now().strftime('%Y-%m-%d')
            key = f"{self.KEYS['stats']}:{today}"
            
            self.redis_client.hincrby(key, action, 1)
            self.redis_client.hincrby(key, 'total', 1)
            
            # TTL 30 дней
            self.redis_client.expire(key, 30 * 24 * 3600)
            
        except Exception as e:
            logger.error(f"Ошибка обновления статистики: {e}")
    
    def get_stats(self, days: int = 7) -> Dict[str, Any]:
        """Получает статистику уведомлений"""
        try:
            stats = {}
            total_sent = 0
            
            for i in range(days):
                date = (datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d')
                key = f"{self.KEYS['stats']}:{date}"
                
                day_stats = self.redis_client.hgetall(key)
                if day_stats:
                    stats[date] = {k: int(v) for k, v in day_stats.items()}
                    total_sent += int(day_stats.get('sent', 0))
                else:
                    stats[date] = {'sent': 0, 'total': 0}
            
            return {
                'daily_stats': stats,
                'total_sent_period': total_sent,
                'pending_count': len(self.redis_client.hgetall(self.KEYS['pending'])),
                'scheduled_count': len(self.redis_client.hgetall(self.KEYS['scheduled']))
            }
            
        except Exception as e:
            logger.error(f"Ошибка получения статистики: {e}")
            return {}
    
    def mark_notification_delivered(self, notification_id: str, user_id: int):
        """Отмечает уведомление как доставленное"""
        try:
            # Перемещаем из pending в sent
            notif_data = self.redis_client.hget(self.KEYS['pending'], notification_id)
            if notif_data:
                data = json.loads(notif_data)
                data['delivered_at'] = datetime.now().isoformat()
                data['delivered_to'] = user_id
                
                self.redis_client.hset(self.KEYS['sent'], notification_id, json.dumps(data))
                self.redis_client.hdel(self.KEYS['pending'], notification_id)
                
                self._update_stats('delivered')
                logger.debug(f"✅ Уведомление {notification_id} доставлено пользователю {user_id}")
        
        except Exception as e:
            logger.error(f"Ошибка отметки доставки: {e}")

# Глобальный экземпляр
_notification_manager = None

def get_notification_manager() -> RedisNotificationManager:
    """Получить глобальный экземпляр notification manager"""
    global _notification_manager
    if _notification_manager is None:
        _notification_manager = RedisNotificationManager()
        _notification_manager.start_listener()
    return _notification_manager

# Удобные функции
def send_block_notification(user_id: int, admin_id: int, reason: str = ""):
    """Быстрая отправка уведомления о блокировке"""
    return get_notification_manager().send_admin_block_notification(user_id, admin_id, reason)

def send_unblock_notification(user_id: int, admin_id: int):
    """Быстрая отправка уведомления о разблокировке"""
    return get_notification_manager().send_admin_unblock_notification(user_id, admin_id)

def send_broadcast_all(title: str, message: str, admin_id: int):
    """Быстрая рассылка всем пользователям"""
    return get_notification_manager().send_broadcast_to_all(title, message, admin_id)

def send_subscription_reminder(user_id: int, days_left: int, subscription_end: datetime):
    """Быстрое напоминание о подписке"""
    return get_notification_manager().send_subscription_warning(user_id, days_left, subscription_end)

if __name__ == "__main__":
    # Тестирование
    nm = get_notification_manager()
    
    # Тест уведомления
    nm.send_broadcast_to_all(
        "🚀 Тестовое уведомление", 
        "Система уведомлений работает!", 
        123456
    )
    
    print("✅ Notification Manager протестирован") 
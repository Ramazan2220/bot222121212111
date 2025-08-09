#!/usr/bin/env python3
"""
📢 Broadcast System - Система массовых рассылок
Поддерживает:
- Рассылка всем пользователям
- Рассылка по группам (trial, premium, free)
- Отложенные рассылки
- Персональные рассылки
- Статистика доставки
"""

import logging
import time
import json
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Set
from dataclasses import dataclass, asdict
from enum import Enum

# Импорты проекта
from utils.notification_manager import get_notification_manager, NotificationType, NotificationPriority
from utils.access_manager import has_access

logger = logging.getLogger(__name__)

class BroadcastType(Enum):
    """Типы рассылок"""
    ALL_USERS = "all_users"           # Всем пользователям
    TRIAL_USERS = "trial_users"       # Только trial пользователи
    PREMIUM_USERS = "premium_users"   # Только premium пользователи
    FREE_USERS = "free_users"         # Только free пользователи
    ACTIVE_USERS = "active_users"     # Только активные пользователи
    EXPIRING_USERS = "expiring_users" # Пользователи с истекающей подпиской
    SPECIFIC_USERS = "specific_users" # Конкретный список пользователей

class BroadcastStatus(Enum):
    """Статусы рассылки"""
    PENDING = "pending"       # Ожидает отправки
    IN_PROGRESS = "in_progress"  # В процессе отправки
    COMPLETED = "completed"   # Завершена
    FAILED = "failed"         # Провалена
    CANCELLED = "cancelled"   # Отменена

@dataclass
class BroadcastMessage:
    """Структура рассылки"""
    id: str
    title: str
    message: str
    broadcast_type: BroadcastType
    priority: NotificationPriority
    admin_id: int
    created_at: str = None
    scheduled_at: Optional[str] = None
    status: BroadcastStatus = BroadcastStatus.PENDING
    target_users: Optional[List[int]] = None  # Для SPECIFIC_USERS
    total_recipients: int = 0
    sent_count: int = 0
    failed_count: int = 0
    delivery_stats: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now().isoformat()
        if self.delivery_stats is None:
            self.delivery_stats = {}

class RedisBroadcastSystem:
    """Система массовых рассылок через Redis"""
    
    def __init__(self):
        self.notification_manager = get_notification_manager()
        self.redis_client = self.notification_manager.redis_client
        
        # Ключи Redis
        self.KEYS = {
            'broadcasts': 'broadcasts:messages',
            'queue': 'broadcasts:queue',
            'stats': 'broadcasts:stats',
            'delivery': 'broadcasts:delivery'
        }
        
        self._processor_thread = None
        self._stop_processing = False
        
        logger.info("📢 RedisBroadcastSystem инициализирован")
    
    def start_processor(self):
        """Запускает обработчик очереди рассылок"""
        if self._processor_thread and self._processor_thread.is_alive():
            return
        
        self._stop_processing = False
        self._processor_thread = threading.Thread(target=self._processing_loop, daemon=True)
        self._processor_thread.start()
        logger.info("🔄 Broadcast processor запущен")
    
    def stop_processor(self):
        """Останавливает обработчик очереди"""
        self._stop_processing = True
        if self._processor_thread:
            self._processor_thread.join(timeout=5)
        logger.info("🛑 Broadcast processor остановлен")
    
    def _processing_loop(self):
        """Основной цикл обработки рассылок"""
        while not self._stop_processing:
            try:
                # Проверяем очередь рассылок
                self._process_pending_broadcasts()
                
                # Проверяем отложенные рассылки
                self._process_scheduled_broadcasts()
                
                time.sleep(10)  # Проверяем каждые 10 секунд
                
            except Exception as e:
                logger.error(f"Ошибка в цикле обработки рассылок: {e}")
                time.sleep(30)
    
    def create_broadcast(self, 
                        title: str, 
                        message: str, 
                        broadcast_type: BroadcastType, 
                        admin_id: int,
                        priority: NotificationPriority = NotificationPriority.NORMAL,
                        target_users: Optional[List[int]] = None,
                        scheduled_at: Optional[datetime] = None) -> str:
        """Создает новую рассылку"""
        try:
            broadcast_id = f"broadcast_{int(time.time())}_{admin_id}"
            
            broadcast = BroadcastMessage(
                id=broadcast_id,
                title=title,
                message=message,
                broadcast_type=broadcast_type,
                priority=priority,
                admin_id=admin_id,
                target_users=target_users,
                scheduled_at=scheduled_at.isoformat() if scheduled_at else None
            )
            
            # Подготавливаем данные для сериализации
            broadcast_dict = asdict(broadcast)
            # Конвертируем enum в строку (если это enum)
            if hasattr(broadcast_dict['broadcast_type'], 'value'):
                broadcast_dict['broadcast_type'] = broadcast_dict['broadcast_type'].value
            if hasattr(broadcast_dict['priority'], 'value'):
                broadcast_dict['priority'] = broadcast_dict['priority'].value
            if hasattr(broadcast_dict['status'], 'value'):
                broadcast_dict['status'] = broadcast_dict['status'].value
            
            # Сохраняем рассылку
            self.redis_client.hset(
                self.KEYS['broadcasts'], 
                broadcast_id, 
                json.dumps(broadcast_dict)
            )
            
            if scheduled_at:
                logger.info(f"📅 Рассылка запланирована на {scheduled_at}: {title}")
            else:
                # Добавляем в очередь для немедленной отправки
                self.redis_client.lpush(self.KEYS['queue'], broadcast_id)
                logger.info(f"📢 Рассылка добавлена в очередь: {title}")
            
            return broadcast_id
            
        except Exception as e:
            logger.error(f"Ошибка создания рассылки: {e}")
            return ""
    
    def _process_pending_broadcasts(self):
        """Обрабатывает рассылки в очереди"""
        try:
            # Получаем рассылку из очереди
            broadcast_id = self.redis_client.rpop(self.KEYS['queue'])
            if not broadcast_id:
                return
            
            # Декодируем если нужно
            if isinstance(broadcast_id, bytes):
                broadcast_id = broadcast_id.decode('utf-8')
            
            # Получаем данные рассылки
            broadcast_data = self.redis_client.hget(self.KEYS['broadcasts'], broadcast_id)
            if not broadcast_data:
                logger.warning(f"Рассылка {broadcast_id} не найдена")
                return
            
            broadcast = BroadcastMessage(**json.loads(broadcast_data))
            
            # Обрабатываем рассылку
            self._execute_broadcast(broadcast)
            
        except Exception as e:
            logger.error(f"Ошибка обработки рассылки: {e}")
    
    def _process_scheduled_broadcasts(self):
        """Обрабатывает отложенные рассылки"""
        try:
            # Получаем все рассылки
            broadcasts = self.redis_client.hgetall(self.KEYS['broadcasts'])
            now = datetime.now()
            
            for broadcast_id, broadcast_data in broadcasts.items():
                try:
                    if isinstance(broadcast_id, bytes):
                        broadcast_id = broadcast_id.decode('utf-8')
                    if isinstance(broadcast_data, bytes):
                        broadcast_data = broadcast_data.decode('utf-8')
                    
                    broadcast = BroadcastMessage(**json.loads(broadcast_data))
                    
                    # Проверяем отложенные рассылки
                    if (broadcast.scheduled_at and 
                        broadcast.status == BroadcastStatus.PENDING):
                        
                        scheduled_time = datetime.fromisoformat(broadcast.scheduled_at)
                        
                        if now >= scheduled_time:
                            # Время пришло - добавляем в очередь
                            self.redis_client.lpush(self.KEYS['queue'], broadcast_id)
                            logger.info(f"⏰ Отложенная рассылка добавлена в очередь: {broadcast.title}")
                
                except Exception as e:
                    logger.error(f"Ошибка обработки отложенной рассылки {broadcast_id}: {e}")
            
        except Exception as e:
            logger.error(f"Ошибка обработки отложенных рассылок: {e}")
    
    def _execute_broadcast(self, broadcast: BroadcastMessage):
        """Выполняет рассылку"""
        try:
            logger.info(f"🚀 Начинаем рассылку: {broadcast.title}")
            
            # Обновляем статус
            broadcast.status = BroadcastStatus.IN_PROGRESS
            self._save_broadcast(broadcast)
            
            # Получаем список получателей
            recipients = self._get_recipients(broadcast)
            broadcast.total_recipients = len(recipients)
            
            if not recipients:
                logger.warning(f"Нет получателей для рассылки {broadcast.id}")
                broadcast.status = BroadcastStatus.FAILED
                self._save_broadcast(broadcast)
                return
            
            logger.info(f"📊 Найдено {len(recipients)} получателей")
            
            # Отправляем уведомления
            sent_count = 0
            failed_count = 0
            
            for user_id in recipients:
                try:
                    success = self._send_to_user(broadcast, user_id)
                    if success:
                        sent_count += 1
                    else:
                        failed_count += 1
                    
                    # Небольшая задержка между отправками
                    time.sleep(0.1)
                    
                except Exception as e:
                    logger.error(f"Ошибка отправки пользователю {user_id}: {e}")
                    failed_count += 1
            
            # Обновляем статистику
            broadcast.sent_count = sent_count
            broadcast.failed_count = failed_count
            broadcast.status = BroadcastStatus.COMPLETED
            broadcast.delivery_stats = {
                'completed_at': datetime.now().isoformat(),
                'success_rate': round((sent_count / len(recipients)) * 100, 2) if recipients else 0,
                'total_time': time.time() - time.mktime(datetime.fromisoformat(broadcast.created_at).timetuple())
            }
            
            self._save_broadcast(broadcast)
            self._update_global_stats(broadcast)
            
            logger.info(f"✅ Рассылка завершена: {sent_count}/{len(recipients)} успешно отправлено")
            
        except Exception as e:
            logger.error(f"Критическая ошибка выполнения рассылки: {e}")
            broadcast.status = BroadcastStatus.FAILED
            self._save_broadcast(broadcast)
    
    def _get_recipients(self, broadcast: BroadcastMessage) -> List[int]:
        """Получает список получателей для рассылки"""
        try:
            if broadcast.broadcast_type == BroadcastType.SPECIFIC_USERS:
                return broadcast.target_users or []
            
            # Получаем всех пользователей из админ панели
            users = self._get_all_users_from_admin_panel()
            recipients = []
            
            for user in users:
                user_id = user['telegram_id']
                
                # Фильтруем по типу рассылки
                if self._should_include_user(user, broadcast.broadcast_type):
                    recipients.append(user_id)
            
            return recipients
            
        except Exception as e:
            logger.error(f"Ошибка получения получателей: {e}")
            return []
    
    def _should_include_user(self, user: Dict, broadcast_type: BroadcastType) -> bool:
        """Проверяет, должен ли пользователь получить рассылку"""
        try:
            if broadcast_type == BroadcastType.ALL_USERS:
                return True
            
            subscription_plan = user.get('subscription_plan', 'trial')
            is_active = user.get('is_active', False)
            subscription_end = user.get('subscription_end')
            
            if broadcast_type == BroadcastType.ACTIVE_USERS:
                return is_active
            
            if broadcast_type == BroadcastType.TRIAL_USERS:
                return 'trial' in subscription_plan.lower()
            
            if broadcast_type == BroadcastType.PREMIUM_USERS:
                return 'premium' in subscription_plan.lower()
            
            if broadcast_type == BroadcastType.FREE_USERS:
                return 'free' in subscription_plan.lower()
            
            if broadcast_type == BroadcastType.EXPIRING_USERS:
                if subscription_end:
                    try:
                        if isinstance(subscription_end, str):
                            subscription_end = datetime.fromisoformat(subscription_end.replace('Z', '+00:00'))
                        
                        days_left = (subscription_end - datetime.now()).days
                        return 0 <= days_left <= 7  # Истекает в течение недели
                    except:
                        pass
            
            return False
            
        except Exception as e:
            logger.error(f"Ошибка фильтрации пользователя: {e}")
            return False
    
    def _get_all_users_from_admin_panel(self) -> List[Dict]:
        """Получает всех пользователей из админ панели"""
        try:
            import sys
            import os
            sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'admin_bot'))
            
            from admin_bot.services.user_service import UserService
            
            user_service = UserService()
            users = user_service.get_all_users()
            
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
            
            return user_list
            
        except Exception as e:
            logger.error(f"Ошибка получения пользователей: {e}")
            return []
    
    def _send_to_user(self, broadcast: BroadcastMessage, user_id: int) -> bool:
        """Отправляет рассылку конкретному пользователю"""
        try:
            # Создаем персональное уведомление
            success = self.notification_manager.send_personal_notification(
                user_id=user_id,
                title=broadcast.title,
                message=broadcast.message,
                admin_id=broadcast.admin_id,
                priority=broadcast.priority
            )
            
            # Сохраняем результат доставки
            delivery_key = f"{self.KEYS['delivery']}:{broadcast.id}"
            self.redis_client.hset(
                delivery_key,
                str(user_id),
                json.dumps({
                    'sent_at': datetime.now().isoformat(),
                    'success': success
                })
            )
            
            # TTL 30 дней
            self.redis_client.expire(delivery_key, 30 * 24 * 3600)
            
            return success
            
        except Exception as e:
            logger.error(f"Ошибка отправки пользователю {user_id}: {e}")
            return False
    
    def _save_broadcast(self, broadcast: BroadcastMessage):
        """Сохраняет рассылку в Redis"""
        try:
            # Подготавливаем данные для сериализации
            broadcast_dict = asdict(broadcast)
            # Конвертируем enum в строку
            if hasattr(broadcast_dict['broadcast_type'], 'value'):
                broadcast_dict['broadcast_type'] = broadcast_dict['broadcast_type'].value
            if hasattr(broadcast_dict['priority'], 'value'):
                broadcast_dict['priority'] = broadcast_dict['priority'].value
            if hasattr(broadcast_dict['status'], 'value'):
                broadcast_dict['status'] = broadcast_dict['status'].value
            
            self.redis_client.hset(
                self.KEYS['broadcasts'], 
                broadcast.id, 
                json.dumps(broadcast_dict)
            )
        except Exception as e:
            logger.error(f"Ошибка сохранения рассылки: {e}")
    
    def _update_global_stats(self, broadcast: BroadcastMessage):
        """Обновляет глобальную статистику"""
        try:
            today = datetime.now().strftime('%Y-%m-%d')
            stats_key = f"{self.KEYS['stats']}:{today}"
            
            self.redis_client.hincrby(stats_key, 'broadcasts_sent', 1)
            self.redis_client.hincrby(stats_key, 'messages_sent', broadcast.sent_count)
            self.redis_client.hincrby(stats_key, 'messages_failed', broadcast.failed_count)
            
            # TTL 90 дней
            self.redis_client.expire(stats_key, 90 * 24 * 3600)
            
        except Exception as e:
            logger.error(f"Ошибка обновления статистики: {e}")
    
    # ========================
    # ПУБЛИЧНЫЕ МЕТОДЫ
    # ========================
    
    def broadcast_to_all(self, title: str, message: str, admin_id: int, 
                        priority: NotificationPriority = NotificationPriority.NORMAL,
                        scheduled_at: Optional[datetime] = None) -> str:
        """Рассылка всем пользователям"""
        return self.create_broadcast(
            title=title,
            message=message,
            broadcast_type=BroadcastType.ALL_USERS,
            admin_id=admin_id,
            priority=priority,
            scheduled_at=scheduled_at
        )
    
    def broadcast_to_group(self, title: str, message: str, group: str, admin_id: int,
                          priority: NotificationPriority = NotificationPriority.NORMAL,
                          scheduled_at: Optional[datetime] = None) -> str:
        """Рассылка конкретной группе"""
        group_mapping = {
            'trial': BroadcastType.TRIAL_USERS,
            'premium': BroadcastType.PREMIUM_USERS,
            'free': BroadcastType.FREE_USERS,
            'active': BroadcastType.ACTIVE_USERS,
            'expiring': BroadcastType.EXPIRING_USERS
        }
        
        broadcast_type = group_mapping.get(group.lower(), BroadcastType.ALL_USERS)
        
        return self.create_broadcast(
            title=title,
            message=message,
            broadcast_type=broadcast_type,
            admin_id=admin_id,
            priority=priority,
            scheduled_at=scheduled_at
        )
    
    def broadcast_to_users(self, title: str, message: str, user_ids: List[int], admin_id: int,
                          priority: NotificationPriority = NotificationPriority.NORMAL,
                          scheduled_at: Optional[datetime] = None) -> str:
        """Рассылка конкретным пользователям"""
        return self.create_broadcast(
            title=title,
            message=message,
            broadcast_type=BroadcastType.SPECIFIC_USERS,
            admin_id=admin_id,
            priority=priority,
            target_users=user_ids,
            scheduled_at=scheduled_at
        )
    
    def get_broadcast_status(self, broadcast_id: str) -> Optional[Dict]:
        """Получает статус рассылки"""
        try:
            broadcast_data = self.redis_client.hget(self.KEYS['broadcasts'], broadcast_id)
            if not broadcast_data:
                return None
            
            return json.loads(broadcast_data)
            
        except Exception as e:
            logger.error(f"Ошибка получения статуса рассылки: {e}")
            return None
    
    def get_recent_broadcasts(self, limit: int = 10) -> List[Dict]:
        """Получает последние рассылки"""
        try:
            broadcasts = self.redis_client.hgetall(self.KEYS['broadcasts'])
            
            broadcast_list = []
            for broadcast_id, broadcast_data in broadcasts.items():
                try:
                    if isinstance(broadcast_data, bytes):
                        broadcast_data = broadcast_data.decode('utf-8')
                    
                    data = json.loads(broadcast_data)
                    broadcast_list.append(data)
                except Exception as e:
                    logger.error(f"Ошибка обработки рассылки: {e}")
            
            # Сортируем по дате создания
            broadcast_list.sort(key=lambda x: x['created_at'], reverse=True)
            
            return broadcast_list[:limit]
            
        except Exception as e:
            logger.error(f"Ошибка получения рассылок: {e}")
            return []
    
    def get_broadcast_stats(self, days: int = 7) -> Dict[str, Any]:
        """Получает статистику рассылок"""
        try:
            stats = {}
            total_broadcasts = 0
            total_messages = 0
            
            for i in range(days):
                date = (datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d')
                stats_key = f"{self.KEYS['stats']}:{date}"
                
                day_stats = self.redis_client.hgetall(stats_key)
                if day_stats:
                    day_data = {k.decode() if isinstance(k, bytes) else k: 
                               int(v.decode() if isinstance(v, bytes) else v) 
                               for k, v in day_stats.items()}
                    stats[date] = day_data
                    total_broadcasts += day_data.get('broadcasts_sent', 0)
                    total_messages += day_data.get('messages_sent', 0)
                else:
                    stats[date] = {'broadcasts_sent': 0, 'messages_sent': 0, 'messages_failed': 0}
            
            return {
                'daily_stats': stats,
                'total_broadcasts_period': total_broadcasts,
                'total_messages_period': total_messages,
                'processing_active': self._processor_thread and self._processor_thread.is_alive(),
                'queue_size': self.redis_client.llen(self.KEYS['queue'])
            }
            
        except Exception as e:
            logger.error(f"Ошибка получения статистики: {e}")
            return {}
    
    def cancel_broadcast(self, broadcast_id: str) -> bool:
        """Отменяет рассылку"""
        try:
            broadcast_data = self.redis_client.hget(self.KEYS['broadcasts'], broadcast_id)
            if not broadcast_data:
                return False
            
            broadcast = BroadcastMessage(**json.loads(broadcast_data))
            
            if broadcast.status == BroadcastStatus.PENDING:
                broadcast.status = BroadcastStatus.CANCELLED
                self._save_broadcast(broadcast)
                
                # Удаляем из очереди если есть
                self.redis_client.lrem(self.KEYS['queue'], 0, broadcast_id)
                
                logger.info(f"❌ Рассылка отменена: {broadcast.title}")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Ошибка отмены рассылки: {e}")
            return False

# Глобальный экземпляр
_broadcast_system = None

def get_broadcast_system() -> RedisBroadcastSystem:
    """Получить глобальный экземпляр broadcast system"""
    global _broadcast_system
    if _broadcast_system is None:
        _broadcast_system = RedisBroadcastSystem()
        _broadcast_system.start_processor()
    return _broadcast_system

# Удобные функции
def broadcast_to_all(title: str, message: str, admin_id: int):
    """Быстрая рассылка всем"""
    return get_broadcast_system().broadcast_to_all(title, message, admin_id)

def broadcast_to_group(title: str, message: str, group: str, admin_id: int):
    """Быстрая рассылка группе"""
    return get_broadcast_system().broadcast_to_group(title, message, group, admin_id)

def get_broadcast_stats():
    """Быстрое получение статистики"""
    return get_broadcast_system().get_broadcast_stats()

if __name__ == "__main__":
    # Тестирование
    bs = get_broadcast_system()
    
    # Тест рассылки
    broadcast_id = bs.broadcast_to_all(
        "🚀 Тестовая рассылка",
        "Система массовых рассылок работает!",
        123456
    )
    
    print(f"✅ Рассылка создана: {broadcast_id}")
    
    # Статистика
    stats = bs.get_broadcast_stats()
    print("📊 Статистика рассылок:")
    for key, value in stats.items():
        print(f"  {key}: {value}")
    
    print("\n✅ Broadcast System протестирован") 
"""
📢 Broadcast System - Система массовых рассылок
Поддерживает:
- Рассылка всем пользователям
- Рассылка по группам (trial, premium, free)
- Отложенные рассылки
- Персональные рассылки
- Статистика доставки
"""

import logging
import time
import json
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Set
from dataclasses import dataclass, asdict
from enum import Enum

# Импорты проекта
from utils.notification_manager import get_notification_manager, NotificationType, NotificationPriority
from utils.access_manager import has_access

logger = logging.getLogger(__name__)

class BroadcastType(Enum):
    """Типы рассылок"""
    ALL_USERS = "all_users"           # Всем пользователям
    TRIAL_USERS = "trial_users"       # Только trial пользователи
    PREMIUM_USERS = "premium_users"   # Только premium пользователи
    FREE_USERS = "free_users"         # Только free пользователи
    ACTIVE_USERS = "active_users"     # Только активные пользователи
    EXPIRING_USERS = "expiring_users" # Пользователи с истекающей подпиской
    SPECIFIC_USERS = "specific_users" # Конкретный список пользователей

class BroadcastStatus(Enum):
    """Статусы рассылки"""
    PENDING = "pending"       # Ожидает отправки
    IN_PROGRESS = "in_progress"  # В процессе отправки
    COMPLETED = "completed"   # Завершена
    FAILED = "failed"         # Провалена
    CANCELLED = "cancelled"   # Отменена

@dataclass
class BroadcastMessage:
    """Структура рассылки"""
    id: str
    title: str
    message: str
    broadcast_type: BroadcastType
    priority: NotificationPriority
    admin_id: int
    created_at: str = None
    scheduled_at: Optional[str] = None
    status: BroadcastStatus = BroadcastStatus.PENDING
    target_users: Optional[List[int]] = None  # Для SPECIFIC_USERS
    total_recipients: int = 0
    sent_count: int = 0
    failed_count: int = 0
    delivery_stats: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now().isoformat()
        if self.delivery_stats is None:
            self.delivery_stats = {}

class RedisBroadcastSystem:
    """Система массовых рассылок через Redis"""
    
    def __init__(self):
        self.notification_manager = get_notification_manager()
        self.redis_client = self.notification_manager.redis_client
        
        # Ключи Redis
        self.KEYS = {
            'broadcasts': 'broadcasts:messages',
            'queue': 'broadcasts:queue',
            'stats': 'broadcasts:stats',
            'delivery': 'broadcasts:delivery'
        }
        
        self._processor_thread = None
        self._stop_processing = False
        
        logger.info("📢 RedisBroadcastSystem инициализирован")
    
    def start_processor(self):
        """Запускает обработчик очереди рассылок"""
        if self._processor_thread and self._processor_thread.is_alive():
            return
        
        self._stop_processing = False
        self._processor_thread = threading.Thread(target=self._processing_loop, daemon=True)
        self._processor_thread.start()
        logger.info("🔄 Broadcast processor запущен")
    
    def stop_processor(self):
        """Останавливает обработчик очереди"""
        self._stop_processing = True
        if self._processor_thread:
            self._processor_thread.join(timeout=5)
        logger.info("🛑 Broadcast processor остановлен")
    
    def _processing_loop(self):
        """Основной цикл обработки рассылок"""
        while not self._stop_processing:
            try:
                # Проверяем очередь рассылок
                self._process_pending_broadcasts()
                
                # Проверяем отложенные рассылки
                self._process_scheduled_broadcasts()
                
                time.sleep(10)  # Проверяем каждые 10 секунд
                
            except Exception as e:
                logger.error(f"Ошибка в цикле обработки рассылок: {e}")
                time.sleep(30)
    
    def create_broadcast(self, 
                        title: str, 
                        message: str, 
                        broadcast_type: BroadcastType, 
                        admin_id: int,
                        priority: NotificationPriority = NotificationPriority.NORMAL,
                        target_users: Optional[List[int]] = None,
                        scheduled_at: Optional[datetime] = None) -> str:
        """Создает новую рассылку"""
        try:
            broadcast_id = f"broadcast_{int(time.time())}_{admin_id}"
            
            broadcast = BroadcastMessage(
                id=broadcast_id,
                title=title,
                message=message,
                broadcast_type=broadcast_type,
                priority=priority,
                admin_id=admin_id,
                target_users=target_users,
                scheduled_at=scheduled_at.isoformat() if scheduled_at else None
            )
            
            # Подготавливаем данные для сериализации
            broadcast_dict = asdict(broadcast)
            # Конвертируем enum в строку (если это enum)
            if hasattr(broadcast_dict['broadcast_type'], 'value'):
                broadcast_dict['broadcast_type'] = broadcast_dict['broadcast_type'].value
            if hasattr(broadcast_dict['priority'], 'value'):
                broadcast_dict['priority'] = broadcast_dict['priority'].value
            if hasattr(broadcast_dict['status'], 'value'):
                broadcast_dict['status'] = broadcast_dict['status'].value
            
            # Сохраняем рассылку
            self.redis_client.hset(
                self.KEYS['broadcasts'], 
                broadcast_id, 
                json.dumps(broadcast_dict)
            )
            
            if scheduled_at:
                logger.info(f"📅 Рассылка запланирована на {scheduled_at}: {title}")
            else:
                # Добавляем в очередь для немедленной отправки
                self.redis_client.lpush(self.KEYS['queue'], broadcast_id)
                logger.info(f"📢 Рассылка добавлена в очередь: {title}")
            
            return broadcast_id
            
        except Exception as e:
            logger.error(f"Ошибка создания рассылки: {e}")
            return ""
    
    def _process_pending_broadcasts(self):
        """Обрабатывает рассылки в очереди"""
        try:
            # Получаем рассылку из очереди
            broadcast_id = self.redis_client.rpop(self.KEYS['queue'])
            if not broadcast_id:
                return
            
            # Декодируем если нужно
            if isinstance(broadcast_id, bytes):
                broadcast_id = broadcast_id.decode('utf-8')
            
            # Получаем данные рассылки
            broadcast_data = self.redis_client.hget(self.KEYS['broadcasts'], broadcast_id)
            if not broadcast_data:
                logger.warning(f"Рассылка {broadcast_id} не найдена")
                return
            
            broadcast = BroadcastMessage(**json.loads(broadcast_data))
            
            # Обрабатываем рассылку
            self._execute_broadcast(broadcast)
            
        except Exception as e:
            logger.error(f"Ошибка обработки рассылки: {e}")
    
    def _process_scheduled_broadcasts(self):
        """Обрабатывает отложенные рассылки"""
        try:
            # Получаем все рассылки
            broadcasts = self.redis_client.hgetall(self.KEYS['broadcasts'])
            now = datetime.now()
            
            for broadcast_id, broadcast_data in broadcasts.items():
                try:
                    if isinstance(broadcast_id, bytes):
                        broadcast_id = broadcast_id.decode('utf-8')
                    if isinstance(broadcast_data, bytes):
                        broadcast_data = broadcast_data.decode('utf-8')
                    
                    broadcast = BroadcastMessage(**json.loads(broadcast_data))
                    
                    # Проверяем отложенные рассылки
                    if (broadcast.scheduled_at and 
                        broadcast.status == BroadcastStatus.PENDING):
                        
                        scheduled_time = datetime.fromisoformat(broadcast.scheduled_at)
                        
                        if now >= scheduled_time:
                            # Время пришло - добавляем в очередь
                            self.redis_client.lpush(self.KEYS['queue'], broadcast_id)
                            logger.info(f"⏰ Отложенная рассылка добавлена в очередь: {broadcast.title}")
                
                except Exception as e:
                    logger.error(f"Ошибка обработки отложенной рассылки {broadcast_id}: {e}")
            
        except Exception as e:
            logger.error(f"Ошибка обработки отложенных рассылок: {e}")
    
    def _execute_broadcast(self, broadcast: BroadcastMessage):
        """Выполняет рассылку"""
        try:
            logger.info(f"🚀 Начинаем рассылку: {broadcast.title}")
            
            # Обновляем статус
            broadcast.status = BroadcastStatus.IN_PROGRESS
            self._save_broadcast(broadcast)
            
            # Получаем список получателей
            recipients = self._get_recipients(broadcast)
            broadcast.total_recipients = len(recipients)
            
            if not recipients:
                logger.warning(f"Нет получателей для рассылки {broadcast.id}")
                broadcast.status = BroadcastStatus.FAILED
                self._save_broadcast(broadcast)
                return
            
            logger.info(f"📊 Найдено {len(recipients)} получателей")
            
            # Отправляем уведомления
            sent_count = 0
            failed_count = 0
            
            for user_id in recipients:
                try:
                    success = self._send_to_user(broadcast, user_id)
                    if success:
                        sent_count += 1
                    else:
                        failed_count += 1
                    
                    # Небольшая задержка между отправками
                    time.sleep(0.1)
                    
                except Exception as e:
                    logger.error(f"Ошибка отправки пользователю {user_id}: {e}")
                    failed_count += 1
            
            # Обновляем статистику
            broadcast.sent_count = sent_count
            broadcast.failed_count = failed_count
            broadcast.status = BroadcastStatus.COMPLETED
            broadcast.delivery_stats = {
                'completed_at': datetime.now().isoformat(),
                'success_rate': round((sent_count / len(recipients)) * 100, 2) if recipients else 0,
                'total_time': time.time() - time.mktime(datetime.fromisoformat(broadcast.created_at).timetuple())
            }
            
            self._save_broadcast(broadcast)
            self._update_global_stats(broadcast)
            
            logger.info(f"✅ Рассылка завершена: {sent_count}/{len(recipients)} успешно отправлено")
            
        except Exception as e:
            logger.error(f"Критическая ошибка выполнения рассылки: {e}")
            broadcast.status = BroadcastStatus.FAILED
            self._save_broadcast(broadcast)
    
    def _get_recipients(self, broadcast: BroadcastMessage) -> List[int]:
        """Получает список получателей для рассылки"""
        try:
            if broadcast.broadcast_type == BroadcastType.SPECIFIC_USERS:
                return broadcast.target_users or []
            
            # Получаем всех пользователей из админ панели
            users = self._get_all_users_from_admin_panel()
            recipients = []
            
            for user in users:
                user_id = user['telegram_id']
                
                # Фильтруем по типу рассылки
                if self._should_include_user(user, broadcast.broadcast_type):
                    recipients.append(user_id)
            
            return recipients
            
        except Exception as e:
            logger.error(f"Ошибка получения получателей: {e}")
            return []
    
    def _should_include_user(self, user: Dict, broadcast_type: BroadcastType) -> bool:
        """Проверяет, должен ли пользователь получить рассылку"""
        try:
            if broadcast_type == BroadcastType.ALL_USERS:
                return True
            
            subscription_plan = user.get('subscription_plan', 'trial')
            is_active = user.get('is_active', False)
            subscription_end = user.get('subscription_end')
            
            if broadcast_type == BroadcastType.ACTIVE_USERS:
                return is_active
            
            if broadcast_type == BroadcastType.TRIAL_USERS:
                return 'trial' in subscription_plan.lower()
            
            if broadcast_type == BroadcastType.PREMIUM_USERS:
                return 'premium' in subscription_plan.lower()
            
            if broadcast_type == BroadcastType.FREE_USERS:
                return 'free' in subscription_plan.lower()
            
            if broadcast_type == BroadcastType.EXPIRING_USERS:
                if subscription_end:
                    try:
                        if isinstance(subscription_end, str):
                            subscription_end = datetime.fromisoformat(subscription_end.replace('Z', '+00:00'))
                        
                        days_left = (subscription_end - datetime.now()).days
                        return 0 <= days_left <= 7  # Истекает в течение недели
                    except:
                        pass
            
            return False
            
        except Exception as e:
            logger.error(f"Ошибка фильтрации пользователя: {e}")
            return False
    
    def _get_all_users_from_admin_panel(self) -> List[Dict]:
        """Получает всех пользователей из админ панели"""
        try:
            import sys
            import os
            sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'admin_bot'))
            
            from admin_bot.services.user_service import UserService
            
            user_service = UserService()
            users = user_service.get_all_users()
            
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
            
            return user_list
            
        except Exception as e:
            logger.error(f"Ошибка получения пользователей: {e}")
            return []
    
    def _send_to_user(self, broadcast: BroadcastMessage, user_id: int) -> bool:
        """Отправляет рассылку конкретному пользователю"""
        try:
            # Создаем персональное уведомление
            success = self.notification_manager.send_personal_notification(
                user_id=user_id,
                title=broadcast.title,
                message=broadcast.message,
                admin_id=broadcast.admin_id,
                priority=broadcast.priority
            )
            
            # Сохраняем результат доставки
            delivery_key = f"{self.KEYS['delivery']}:{broadcast.id}"
            self.redis_client.hset(
                delivery_key,
                str(user_id),
                json.dumps({
                    'sent_at': datetime.now().isoformat(),
                    'success': success
                })
            )
            
            # TTL 30 дней
            self.redis_client.expire(delivery_key, 30 * 24 * 3600)
            
            return success
            
        except Exception as e:
            logger.error(f"Ошибка отправки пользователю {user_id}: {e}")
            return False
    
    def _save_broadcast(self, broadcast: BroadcastMessage):
        """Сохраняет рассылку в Redis"""
        try:
            # Подготавливаем данные для сериализации
            broadcast_dict = asdict(broadcast)
            # Конвертируем enum в строку
            if hasattr(broadcast_dict['broadcast_type'], 'value'):
                broadcast_dict['broadcast_type'] = broadcast_dict['broadcast_type'].value
            if hasattr(broadcast_dict['priority'], 'value'):
                broadcast_dict['priority'] = broadcast_dict['priority'].value
            if hasattr(broadcast_dict['status'], 'value'):
                broadcast_dict['status'] = broadcast_dict['status'].value
            
            self.redis_client.hset(
                self.KEYS['broadcasts'], 
                broadcast.id, 
                json.dumps(broadcast_dict)
            )
        except Exception as e:
            logger.error(f"Ошибка сохранения рассылки: {e}")
    
    def _update_global_stats(self, broadcast: BroadcastMessage):
        """Обновляет глобальную статистику"""
        try:
            today = datetime.now().strftime('%Y-%m-%d')
            stats_key = f"{self.KEYS['stats']}:{today}"
            
            self.redis_client.hincrby(stats_key, 'broadcasts_sent', 1)
            self.redis_client.hincrby(stats_key, 'messages_sent', broadcast.sent_count)
            self.redis_client.hincrby(stats_key, 'messages_failed', broadcast.failed_count)
            
            # TTL 90 дней
            self.redis_client.expire(stats_key, 90 * 24 * 3600)
            
        except Exception as e:
            logger.error(f"Ошибка обновления статистики: {e}")
    
    # ========================
    # ПУБЛИЧНЫЕ МЕТОДЫ
    # ========================
    
    def broadcast_to_all(self, title: str, message: str, admin_id: int, 
                        priority: NotificationPriority = NotificationPriority.NORMAL,
                        scheduled_at: Optional[datetime] = None) -> str:
        """Рассылка всем пользователям"""
        return self.create_broadcast(
            title=title,
            message=message,
            broadcast_type=BroadcastType.ALL_USERS,
            admin_id=admin_id,
            priority=priority,
            scheduled_at=scheduled_at
        )
    
    def broadcast_to_group(self, title: str, message: str, group: str, admin_id: int,
                          priority: NotificationPriority = NotificationPriority.NORMAL,
                          scheduled_at: Optional[datetime] = None) -> str:
        """Рассылка конкретной группе"""
        group_mapping = {
            'trial': BroadcastType.TRIAL_USERS,
            'premium': BroadcastType.PREMIUM_USERS,
            'free': BroadcastType.FREE_USERS,
            'active': BroadcastType.ACTIVE_USERS,
            'expiring': BroadcastType.EXPIRING_USERS
        }
        
        broadcast_type = group_mapping.get(group.lower(), BroadcastType.ALL_USERS)
        
        return self.create_broadcast(
            title=title,
            message=message,
            broadcast_type=broadcast_type,
            admin_id=admin_id,
            priority=priority,
            scheduled_at=scheduled_at
        )
    
    def broadcast_to_users(self, title: str, message: str, user_ids: List[int], admin_id: int,
                          priority: NotificationPriority = NotificationPriority.NORMAL,
                          scheduled_at: Optional[datetime] = None) -> str:
        """Рассылка конкретным пользователям"""
        return self.create_broadcast(
            title=title,
            message=message,
            broadcast_type=BroadcastType.SPECIFIC_USERS,
            admin_id=admin_id,
            priority=priority,
            target_users=user_ids,
            scheduled_at=scheduled_at
        )
    
    def get_broadcast_status(self, broadcast_id: str) -> Optional[Dict]:
        """Получает статус рассылки"""
        try:
            broadcast_data = self.redis_client.hget(self.KEYS['broadcasts'], broadcast_id)
            if not broadcast_data:
                return None
            
            return json.loads(broadcast_data)
            
        except Exception as e:
            logger.error(f"Ошибка получения статуса рассылки: {e}")
            return None
    
    def get_recent_broadcasts(self, limit: int = 10) -> List[Dict]:
        """Получает последние рассылки"""
        try:
            broadcasts = self.redis_client.hgetall(self.KEYS['broadcasts'])
            
            broadcast_list = []
            for broadcast_id, broadcast_data in broadcasts.items():
                try:
                    if isinstance(broadcast_data, bytes):
                        broadcast_data = broadcast_data.decode('utf-8')
                    
                    data = json.loads(broadcast_data)
                    broadcast_list.append(data)
                except Exception as e:
                    logger.error(f"Ошибка обработки рассылки: {e}")
            
            # Сортируем по дате создания
            broadcast_list.sort(key=lambda x: x['created_at'], reverse=True)
            
            return broadcast_list[:limit]
            
        except Exception as e:
            logger.error(f"Ошибка получения рассылок: {e}")
            return []
    
    def get_broadcast_stats(self, days: int = 7) -> Dict[str, Any]:
        """Получает статистику рассылок"""
        try:
            stats = {}
            total_broadcasts = 0
            total_messages = 0
            
            for i in range(days):
                date = (datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d')
                stats_key = f"{self.KEYS['stats']}:{date}"
                
                day_stats = self.redis_client.hgetall(stats_key)
                if day_stats:
                    day_data = {k.decode() if isinstance(k, bytes) else k: 
                               int(v.decode() if isinstance(v, bytes) else v) 
                               for k, v in day_stats.items()}
                    stats[date] = day_data
                    total_broadcasts += day_data.get('broadcasts_sent', 0)
                    total_messages += day_data.get('messages_sent', 0)
                else:
                    stats[date] = {'broadcasts_sent': 0, 'messages_sent': 0, 'messages_failed': 0}
            
            return {
                'daily_stats': stats,
                'total_broadcasts_period': total_broadcasts,
                'total_messages_period': total_messages,
                'processing_active': self._processor_thread and self._processor_thread.is_alive(),
                'queue_size': self.redis_client.llen(self.KEYS['queue'])
            }
            
        except Exception as e:
            logger.error(f"Ошибка получения статистики: {e}")
            return {}
    
    def cancel_broadcast(self, broadcast_id: str) -> bool:
        """Отменяет рассылку"""
        try:
            broadcast_data = self.redis_client.hget(self.KEYS['broadcasts'], broadcast_id)
            if not broadcast_data:
                return False
            
            broadcast = BroadcastMessage(**json.loads(broadcast_data))
            
            if broadcast.status == BroadcastStatus.PENDING:
                broadcast.status = BroadcastStatus.CANCELLED
                self._save_broadcast(broadcast)
                
                # Удаляем из очереди если есть
                self.redis_client.lrem(self.KEYS['queue'], 0, broadcast_id)
                
                logger.info(f"❌ Рассылка отменена: {broadcast.title}")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Ошибка отмены рассылки: {e}")
            return False

# Глобальный экземпляр
_broadcast_system = None

def get_broadcast_system() -> RedisBroadcastSystem:
    """Получить глобальный экземпляр broadcast system"""
    global _broadcast_system
    if _broadcast_system is None:
        _broadcast_system = RedisBroadcastSystem()
        _broadcast_system.start_processor()
    return _broadcast_system

# Удобные функции
def broadcast_to_all(title: str, message: str, admin_id: int):
    """Быстрая рассылка всем"""
    return get_broadcast_system().broadcast_to_all(title, message, admin_id)

def broadcast_to_group(title: str, message: str, group: str, admin_id: int):
    """Быстрая рассылка группе"""
    return get_broadcast_system().broadcast_to_group(title, message, group, admin_id)

def get_broadcast_stats():
    """Быстрое получение статистики"""
    return get_broadcast_system().get_broadcast_stats()

if __name__ == "__main__":
    # Тестирование
    bs = get_broadcast_system()
    
    # Тест рассылки
    broadcast_id = bs.broadcast_to_all(
        "🚀 Тестовая рассылка",
        "Система массовых рассылок работает!",
        123456
    )
    
    print(f"✅ Рассылка создана: {broadcast_id}")
    
    # Статистика
    stats = bs.get_broadcast_stats()
    print("📊 Статистика рассылок:")
    for key, value in stats.items():
        print(f"  {key}: {value}")
    
    print("\n✅ Broadcast System протестирован") 
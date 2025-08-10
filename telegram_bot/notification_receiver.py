#!/usr/bin/env python3
"""
📨 Notification Receiver - Получатель уведомлений для основного бота
Слушает Redis каналы и отправляет уведомления пользователям
"""

import logging
import json
import asyncio
import threading
from datetime import datetime
from typing import Dict, Any

from telegram import Bot, ParseMode
from telegram.error import TelegramError

# Импорты систем уведомлений
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.notification_manager import get_notification_manager, NotificationType
from utils.access_manager import has_access

logger = logging.getLogger(__name__)

class TelegramNotificationReceiver:
    """Получатель уведомлений для Telegram бота"""
    
    def __init__(self, bot_token: str):
        self.bot = Bot(token=bot_token)
        self.notification_manager = get_notification_manager()
        self._running = False
        self._receiver_thread = None
        
        # Регистрируем обработчики для каждого канала
        self._setup_handlers()
        
        logger.info("📨 TelegramNotificationReceiver инициализирован")
    
    def _setup_handlers(self):
        """Настраивает обработчики для каналов уведомлений"""
        channels = self.notification_manager.CHANNELS
        
        # Админские действия (блокировка/разблокировка)
        self.notification_manager.register_handler(
            channels['admin_actions'], 
            self._handle_admin_actions
        )
        
        # КРИТИЧЕСКИ ВАЖНО: Обработчики для Redis access событий
        self.notification_manager.register_handler(
            'access:user_added', 
            self._handle_access_added
        )
        self.notification_manager.register_handler(
            'access:user_removed', 
            self._handle_access_removed
        )
        
        # Подписки
        self.notification_manager.register_handler(
            channels['subscription'], 
            self._handle_subscription_notifications
        )
        
        # Массовые рассылки
        self.notification_manager.register_handler(
            channels['broadcast'], 
            self._handle_broadcast_notifications
        )
        
        # Персональные уведомления
        self.notification_manager.register_handler(
            channels['personal'], 
            self._handle_personal_notifications
        )
        
        # Системные уведомления
        self.notification_manager.register_handler(
            channels['system'], 
            self._handle_system_notifications
        )
        
        logger.info("📝 Все обработчики уведомлений зарегистрированы")
    
    def start_receiving(self):
        """Запускает получение уведомлений"""
        if self._running:
            logger.warning("Receiver уже запущен")
            return
        
        self._running = True
        self._receiver_thread = threading.Thread(target=self._run_receiver, daemon=True)
        self._receiver_thread.start()
        
        logger.info("🔄 Notification receiver запущен")
    
    def stop_receiving(self):
        """Останавливает получение уведомлений"""
        self._running = False
        if self._receiver_thread:
            self._receiver_thread.join(timeout=5)
        
        logger.info("🛑 Notification receiver остановлен")
    
    def _run_receiver(self):
        """Основной цикл получения уведомлений"""
        try:
            # Notification manager уже запущен, просто ждем
            while self._running:
                threading.Event().wait(1)  # Проверяем каждую секунду
        except Exception as e:
            logger.error(f"Критическая ошибка в notification receiver: {e}")
    
    def _handle_admin_actions(self, notification_data: Dict[str, Any]):
        """Обрабатывает админские действия (блокировка/разблокировка)"""
        try:
            notification_type = notification_data.get('type')
            user_id = notification_data.get('user_id')
            title = notification_data.get('title', '')
            message = notification_data.get('message', '')
            
            if not user_id:
                logger.warning("Получено уведомление без user_id")
                return
            
            if notification_type == 'admin_block':
                # Блокировка - отправляем уведомление и принудительно блокируем
                self._send_notification_to_user(user_id, title, message, urgent=True)
                
                # Принудительно блокируем в smart cache
                try:
                    from telegram_bot.middleware.smart_access_check import force_block_user
                    force_block_user(user_id)
                    logger.info(f"🚫 Пользователь {user_id} принудительно заблокирован")
                except ImportError:
                    logger.warning("Smart access check не доступен")
                
            elif notification_type == 'admin_unblock':
                # Разблокировка - отправляем уведомление и принудительно разблокируем
                self._send_notification_to_user(user_id, title, message, urgent=True)
                
                # Принудительно разблокируем в smart cache
                try:
                    from telegram_bot.middleware.smart_access_check import force_unblock_user
                    force_unblock_user(user_id)
                    logger.info(f"🔓 Пользователь {user_id} принудительно разблокирован")
                except ImportError:
                    logger.warning("Smart access check не доступен")
            
            # Отмечаем как доставленное
            self.notification_manager.mark_notification_delivered(
                notification_data.get('id', ''), user_id
            )
            
        except Exception as e:
            logger.error(f"Ошибка обработки админского действия: {e}")
    
    def _handle_access_added(self, notification_data: Dict[str, Any]):
        """Обрабатывает события добавления доступа"""
        try:
            user_id = notification_data.get('user_id')
            username = notification_data.get('username', 'неизвестно')
            
            if not user_id:
                logger.warning("Получено событие добавления доступа без user_id")
                return
            
            logger.info(f"🔓 Пользователь {username} ({user_id}) получил доступ")
            
            # Обновляем умный кеш доступа для мгновенного разблокирования
            try:
                from telegram_bot.middleware.smart_access_check import force_unblock_user
                force_unblock_user(user_id)
                logger.info(f"🧠 Пользователь {user_id} разблокирован в умном кеше")
            except ImportError:
                logger.warning("Smart access check не доступен")
            
        except Exception as e:
            logger.error(f"Ошибка обработки добавления доступа: {e}")
    
    def _handle_access_removed(self, notification_data: Dict[str, Any]):
        """Обрабатывает события удаления доступа"""
        try:
            user_id = notification_data.get('user_id')
            username = notification_data.get('username', 'неизвестно')
            
            logger.info(f"🔥 ПОЛУЧЕНО СОБЫТИЕ УДАЛЕНИЯ: {notification_data}")
            
            if not user_id:
                logger.warning("Получено событие удаления доступа без user_id")
                return
            
            logger.info(f"🚫 Пользователь {username} ({user_id}) лишен доступа")
            
            # КРИТИЧЕСКИ ВАЖНО: Отправляем триггерное сообщение для мгновенной блокировки
            try:
                self._send_block_trigger_message(user_id)
                logger.info(f"📨 Триггерное сообщение о блокировке отправлено пользователю {user_id}")
            except Exception as e:
                logger.error(f"Ошибка отправки триггерного сообщения: {e}")
            
            # Обновляем умный кеш доступа для мгновенной блокировки
            try:
                from telegram_bot.middleware.smart_access_check import force_block_user
                force_block_user(user_id)
                logger.info(f"🧠 Пользователь {user_id} заблокирован в умном кеше")
                
            except ImportError:
                logger.warning("Smart access check не доступен")
                
        except Exception as e:
            logger.error(f"Ошибка обработки события удаления доступа: {e}")
    
    def _send_block_trigger_message(self, user_id: int):
        """Отправляет триггерное сообщение для мгновенной блокировки"""
        try:
            # ТРИГГЕРНЫЙ ТЕКСТ - НЕ ИЗМЕНЯТЬ!
            trigger_message = "🚫 Ваш доступ к боту заблокирован администратором"
            
            # Используем синхронный вызов для совместимости
            try:
                self.bot.send_message(
                    chat_id=user_id,
                    text=trigger_message
                )
                logger.info(f"📤 Триггерное сообщение отправлено пользователю {user_id}")
            except Exception as send_error:
                logger.error(f"Ошибка отправки через bot.send_message: {send_error}")
                
                # Fallback: пытаемся через requests
                try:
                    import requests
                    import os
                    
                    token = os.getenv('TELEGRAM_TOKEN', 'UNDEFINED')
                    url = f"https://api.telegram.org/bot{token}/sendMessage"
                    data = {
                        'chat_id': user_id,
                        'text': trigger_message
                    }
                    
                    response = requests.post(url, data=data, timeout=10)
                    if response.status_code == 200:
                        logger.info(f"📤 Триггерное сообщение отправлено через requests пользователю {user_id}")
                    else:
                        logger.error(f"Ошибка отправки через requests: {response.status_code}")
                        
                except Exception as fallback_error:
                    logger.error(f"Ошибка fallback отправки: {fallback_error}")
            
        except Exception as e:
            logger.error(f"Критическая ошибка отправки триггерного сообщения: {e}")
    
    def _handle_subscription_notifications(self, notification_data: Dict[str, Any]):
        """Обрабатывает уведомления о подписках"""
        try:
            user_id = notification_data.get('user_id')
            title = notification_data.get('title', '')
            message = notification_data.get('message', '')
            
            if not user_id:
                logger.warning("Получено уведомление о подписке без user_id")
                return
            
            # Проверяем, что пользователь все еще имеет доступ
            if has_access(user_id):
                self._send_notification_to_user(user_id, title, message, urgent=True)
                
                # Отмечаем как доставленное
                self.notification_manager.mark_notification_delivered(
                    notification_data.get('id', ''), user_id
                )
            else:
                logger.debug(f"Пропускаем уведомление о подписке для заблокированного пользователя {user_id}")
            
        except Exception as e:
            logger.error(f"Ошибка обработки уведомления о подписке: {e}")
    
    def _handle_broadcast_notifications(self, notification_data: Dict[str, Any]):
        """Обрабатывает массовые рассылки"""
        try:
            title = notification_data.get('title', '')
            message = notification_data.get('message', '')
            broadcast_type = notification_data.get('data', {}).get('broadcast_type', 'all')
            target_group = notification_data.get('user_group')
            
            # Получаем список получателей
            recipients = self._get_broadcast_recipients(broadcast_type, target_group)
            
            logger.info(f"📢 Отправляем рассылку '{title}' для {len(recipients)} получателей")
            
            # Отправляем всем получателям
            sent_count = 0
            for user_id in recipients:
                try:
                    if self._send_notification_to_user(user_id, title, message):
                        sent_count += 1
                except Exception as e:
                    logger.error(f"Ошибка отправки рассылки пользователю {user_id}: {e}")
            
            logger.info(f"✅ Рассылка завершена: {sent_count}/{len(recipients)} успешно")
            
        except Exception as e:
            logger.error(f"Ошибка обработки массовой рассылки: {e}")
    
    def _handle_personal_notifications(self, notification_data: Dict[str, Any]):
        """Обрабатывает персональные уведомления"""
        try:
            user_id = notification_data.get('user_id')
            title = notification_data.get('title', '')
            message = notification_data.get('message', '')
            priority = notification_data.get('priority', 'normal')
            
            if not user_id:
                logger.warning("Получено персональное уведомление без user_id")
                return
            
            # Проверяем доступ
            if has_access(user_id):
                urgent = priority in ['critical', 'high']
                self._send_notification_to_user(user_id, title, message, urgent=urgent)
                
                # Отмечаем как доставленное
                self.notification_manager.mark_notification_delivered(
                    notification_data.get('id', ''), user_id
                )
            else:
                logger.debug(f"Пропускаем персональное уведомление для заблокированного пользователя {user_id}")
            
        except Exception as e:
            logger.error(f"Ошибка обработки персонального уведомления: {e}")
    
    def _handle_system_notifications(self, notification_data: Dict[str, Any]):
        """Обрабатывает системные уведомления"""
        try:
            title = notification_data.get('title', '')
            message = notification_data.get('message', '')
            
            # Системные уведомления отправляем всем активным пользователям
            recipients = self._get_all_active_users()
            
            logger.info(f"🔧 Отправляем системное уведомление '{title}' для {len(recipients)} пользователей")
            
            sent_count = 0
            for user_id in recipients:
                try:
                    if self._send_notification_to_user(user_id, title, message, urgent=True):
                        sent_count += 1
                except Exception as e:
                    logger.error(f"Ошибка отправки системного уведомления пользователю {user_id}: {e}")
            
            logger.info(f"✅ Системное уведомление отправлено: {sent_count}/{len(recipients)} успешно")
            
        except Exception as e:
            logger.error(f"Ошибка обработки системного уведомления: {e}")
    
    def _send_notification_to_user(self, user_id: int, title: str, message: str, urgent: bool = False) -> bool:
        """Отправляет уведомление конкретному пользователю"""
        try:
            # Форматируем сообщение
            if urgent:
                formatted_message = f"🚨 **{title}**\n\n{message}"
            else:
                formatted_message = f"🔔 **{title}**\n\n{message}"
            
            # Отправляем через Telegram API
            self.bot.send_message(
                chat_id=user_id,
                text=formatted_message,
                parse_mode=ParseMode.MARKDOWN,
                disable_web_page_preview=True
            )
            
            logger.debug(f"📨 Уведомление отправлено пользователю {user_id}: {title}")
            return True
            
        except TelegramError as e:
            if "blocked by the user" in str(e) or "user not found" in str(e):
                logger.debug(f"Пользователь {user_id} заблокировал бота или не найден")
            else:
                logger.error(f"Telegram ошибка при отправке уведомления пользователю {user_id}: {e}")
            return False
        except Exception as e:
            logger.error(f"Ошибка отправки уведомления пользователю {user_id}: {e}")
            return False
    
    def _get_broadcast_recipients(self, broadcast_type: str, target_group: str = None) -> list:
        """Получает список получателей для рассылки"""
        try:
            if broadcast_type == 'specific' and target_group:
                # Конкретная группа пользователей
                return self._get_users_by_group(target_group)
            else:
                # Все активные пользователи
                return self._get_all_active_users()
        except Exception as e:
            logger.error(f"Ошибка получения получателей рассылки: {e}")
            return []
    
    def _get_all_active_users(self) -> list:
        """Получает всех активных пользователей"""
        try:
            # Получаем всех пользователей из админ панели
            import sys
            import os
            sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'admin_bot'))
            
            from admin_bot.services.user_service import UserService
            
            user_service = UserService()
            users = user_service.get_all_users()
            
            active_users = []
            for user in users:
                if user.is_active and has_access(user.telegram_id):
                    active_users.append(user.telegram_id)
            
            return active_users
            
        except Exception as e:
            logger.error(f"Ошибка получения активных пользователей: {e}")
            return []
    
    def _get_users_by_group(self, group: str) -> list:
        """Получает пользователей конкретной группы"""
        try:
            # Аналогично _get_all_active_users, но с фильтрацией по группе
            import sys
            import os
            sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'admin_bot'))
            
            from admin_bot.services.user_service import UserService
            
            user_service = UserService()
            users = user_service.get_all_users()
            
            group_users = []
            for user in users:
                if (user.is_active and has_access(user.telegram_id) and
                    self._user_matches_group(user, group)):
                    group_users.append(user.telegram_id)
            
            return group_users
            
        except Exception as e:
            logger.error(f"Ошибка получения пользователей группы {group}: {e}")
            return []
    
    def _user_matches_group(self, user, group: str) -> bool:
        """Проверяет, соответствует ли пользователь группе"""
        try:
            plan = user.subscription_plan.value if user.subscription_plan else 'trial'
            
            if group == 'trial':
                return 'trial' in plan.lower()
            elif group == 'premium':
                return 'premium' in plan.lower()
            elif group == 'free':
                return 'free' in plan.lower()
            elif group == 'active':
                return user.is_active
            elif group == 'expiring':
                # Логика для истекающих подписок
                if user.subscription_end:
                    try:
                        from datetime import datetime
                        if isinstance(user.subscription_end, str):
                            subscription_end = datetime.fromisoformat(user.subscription_end.replace('Z', '+00:00'))
                        else:
                            subscription_end = user.subscription_end
                        
                        days_left = (subscription_end - datetime.now()).days
                        return 0 <= days_left <= 7
                    except:
                        pass
            
            return False
            
        except Exception as e:
            logger.error(f"Ошибка проверки группы пользователя: {e}")
            return False

# Глобальный экземпляр
_notification_receiver = None

def get_notification_receiver(bot_token: str = None) -> TelegramNotificationReceiver:
    """Получить глобальный экземпляр notification receiver"""
    global _notification_receiver
    if _notification_receiver is None and bot_token:
        _notification_receiver = TelegramNotificationReceiver(bot_token)
        _notification_receiver.start_receiving()
    return _notification_receiver

def start_notification_receiver(bot_token: str):
    """Запускает notification receiver"""
    receiver = get_notification_receiver(bot_token)
    return receiver

if __name__ == "__main__":
    # Тестирование (нужен токен бота)
    import os
    
    bot_token = os.getenv('TELEGRAM_TOKEN')
    if bot_token:
        receiver = start_notification_receiver(bot_token)
        print("✅ Notification Receiver запущен для тестирования")
        
        import time
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            receiver.stop_receiving()
            print("🛑 Notification Receiver остановлен")
    else:
        print("❌ Токен бота не найден") 
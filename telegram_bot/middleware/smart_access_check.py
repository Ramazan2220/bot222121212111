#!/usr/bin/env python3
"""
Умная система проверки доступа с минимальной нагрузкой
Использует кеширование и периодические проверки
"""

import logging
import time
import threading
from typing import Set, Dict
from telegram import Update
from telegram.ext import CallbackContext
from utils.access_manager import has_access
from database.user_context_manager import UserContextManager

# Совместимость с разными версиями python-telegram-bot
try:
    from telegram.ext import ApplicationHandlerStop
except ImportError:
    # Для старых версий, где ApplicationHandlerStop может быть не напрямую доступен
    class ApplicationHandlerStop(Exception):
        pass

logger = logging.getLogger(__name__)

class SmartAccessManager:
    """Умная система управления доступом с минимальной нагрузкой"""
    
    def __init__(self, cache_ttl: int = 300):  # 5 минут кеш
        self.blocked_users: Set[int] = set()  # Заблокированные пользователи
        self.verified_users: Dict[int, float] = {}  # {user_id: timestamp_last_check}
        self.cache_ttl = cache_ttl  # Время жизни кеша в секундах
        self.last_cleanup = time.time()
        self.cleanup_interval = 300  # Очистка каждые 5 минут
        
        # Запускаем фоновую очистку
        self._start_background_cleanup()
        
        logger.info(f"🧠 SmartAccessManager инициализирован (TTL: {cache_ttl}s)")
    
    def _start_background_cleanup(self):
        """Запускает фоновый поток очистки кеша"""
        def cleanup_worker():
            while True:
                try:
                    time.sleep(self.cleanup_interval)
                    self._cleanup_cache()
                except Exception as e:
                    logger.error(f"Ошибка в фоновой очистке: {e}")
        
        cleanup_thread = threading.Thread(target=cleanup_worker, daemon=True)
        cleanup_thread.start()
        logger.info("🧹 Фоновая очистка кеша запущена")
    
    def _cleanup_cache(self):
        """Очищает устаревший кеш"""
        current_time = time.time()
        expired_users = []
        
        for user_id, last_check in self.verified_users.items():
            if current_time - last_check > self.cache_ttl:
                expired_users.append(user_id)
        
        for user_id in expired_users:
            del self.verified_users[user_id]
        
        if expired_users:
            logger.debug(f"🧹 Очищен кеш для {len(expired_users)} пользователей")
    
    def check_access_fast(self, user_id: int) -> bool:
        """
        Быстрая проверка доступа с кешированием
        True = доступ разрешен, False = заблокирован
        """
        current_time = time.time()
        
        # 1. Быстрая проверка заблокированных
        if user_id in self.blocked_users:
            return False
        
        # 2. Проверка кеша верифицированных
        if user_id in self.verified_users:
            last_check = self.verified_users[user_id]
            if current_time - last_check < self.cache_ttl:
                return True  # Кеш актуален
        
        # 3. Полная проверка (только если кеш устарел)
        try:
            access_granted = has_access(user_id)
            
            if access_granted:
                # Добавляем в кеш верифицированных
                self.verified_users[user_id] = current_time
                # Удаляем из заблокированных (если был)
                self.blocked_users.discard(user_id)
                return True
            else:
                # Добавляем в заблокированные
                self.blocked_users.add(user_id)
                # Удаляем из верифицированных
                self.verified_users.pop(user_id, None)
                return False
                
        except Exception as e:
            logger.error(f"Ошибка проверки доступа для {user_id}: {e}")
            return False
    
    def force_block_user(self, user_id: int):
        """Принудительно блокирует пользователя (для админ команд)"""
        self.blocked_users.add(user_id)
        self.verified_users.pop(user_id, None)
        logger.info(f"🚫 Пользователь {user_id} принудительно заблокирован")
    
    def force_unblock_user(self, user_id: int):
        """Принудительно разблокирует пользователя (для админ команд)"""
        self.blocked_users.discard(user_id)
        self.verified_users[user_id] = time.time()
        logger.info(f"🔓 Пользователь {user_id} принудительно разблокирован")
    
    def get_cache_stats(self) -> Dict:
        """Возвращает статистику кеша"""
        return {
            'blocked_users': len(self.blocked_users),
            'verified_users': len(self.verified_users),
            'cache_ttl': self.cache_ttl,
            'last_cleanup': self.last_cleanup
        }

# Глобальный экземпляр
smart_access = SmartAccessManager()

def send_access_denied_message(update: Update, context: CallbackContext, user_id: int):
    """Отправляет сообщение о запрещенном доступе"""
    try:
        message = """🚫 **ДОСТУП ЗАПРЕЩЕН**

❌ У вас нет доступа к боту
🔑 Обратитесь к администратору для получения доступа

👨‍💼 **Контакты:**
📧 Email: support@yourbot.com
💬 Telegram: @admin

⚠️ Все сообщения будут игнорироваться до получения доступа."""

        if update.message:
            update.message.reply_text(message, parse_mode='Markdown')
        elif update.callback_query:
            update.callback_query.answer("🚫 Доступ запрещен")
            if update.callback_query.message:
                update.callback_query.message.reply_text(message, parse_mode='Markdown')
                
    except Exception as e:
        logger.error(f"Ошибка отправки сообщения о блокировке: {e}")

def check_user_access_smart(update: Update, context: CallbackContext) -> bool:
    """
    Умная проверка доступа пользователя
    Использует кеширование для минимизации нагрузки
    """
    if not update.effective_user:
        return False
    
    user_id = update.effective_user.id
    
    # Быстрая проверка с кешированием
    access_granted = smart_access.check_access_fast(user_id)
    
    if not access_granted:
        # Отправляем сообщение о блокировке
        send_access_denied_message(update, context, user_id)
        logger.warning(f"🚫 Доступ запрещен для пользователя {user_id}")
        return False
    
    return True

# Функции для админ панели
def force_block_user(user_id: int):
    """Принудительная блокировка для админ панели"""
    smart_access.force_block_user(user_id)

def force_unblock_user(user_id: int):
    """Принудительная разблокировка для админ панели"""
    smart_access.force_unblock_user(user_id) 

def send_access_denied_message(self, user_id: int):
    """Отправляет сообщение об отказе в доступе"""
    try:
        from telegram_bot.bot import get_bot
        bot = get_bot()
        if bot:
            bot.send_message(
                chat_id=user_id,
                text="🚫 Доступ к боту заблокирован.\n\nОбратитесь к администратору для получения доступа."
            )
            logger.info(f"📨 Отправлено сообщение об отказе пользователю {user_id}")
    except Exception as e:
        logger.error(f"Ошибка отправки сообщения об отказе: {e}")

def middleware_access_check(func):
    """Декоратор для проверки доступа в middleware"""
    def wrapper(update, context):
        user_id = update.effective_user.id if update.effective_user else None
        
        if user_id and not check_user_access_smart(update, context):
            # Пользователь заблокирован - останавливаем выполнение
            logger.info(f"🚫 Заблокирован middleware доступ для пользователя {user_id}")
            return None
        
        return func(update, context)
    return wrapper

def force_disconnect_user(user_id: int):
    """Принудительно отключает пользователя (завершает активные операции)"""
    try:
        from telegram_bot.bot import get_bot
        bot = get_bot()
        if bot:
            # Отправляем сообщение о блокировке
            bot.send_message(
                chat_id=user_id,
                text="🚫 ВНИМАНИЕ!\n\nВаш доступ к боту был заблокирован администратором.\n\nВсе активные операции завершены.",
                parse_mode='Markdown'
            )
            
            # Принудительно блокируем в кеше
            force_block_user(user_id)
            
            logger.info(f"🔌 Пользователь {user_id} принудительно отключен")
            return True
            
    except Exception as e:
        logger.error(f"Ошибка принудительного отключения пользователя {user_id}: {e}")
        return False

# Глобальная функция для быстрого доступа
def disconnect_user_immediately(user_id: int):
    """Немедленно отключает пользователя"""
    return force_disconnect_user(user_id) 

def universal_access_middleware(update: Update, context: CallbackContext):
    """
    🛡️ УНИВЕРСАЛЬНЫЙ MIDDLEWARE ДЛЯ ПРОВЕРКИ ДОСТУПА
    
    Перехватывает ВСЕ взаимодействия с ботом:
    - Текстовые сообщения
    - Callback queries (кнопки)
    - Команды
    - Любые другие типы обновлений
    
    Блокирует пользователей БЕЗ доступа на самом раннем этапе
    """
    try:
        # Получаем ID пользователя из любого типа обновления
        user_id = None
        if update.effective_user:
            user_id = update.effective_user.id
        elif update.message and update.message.from_user:
            user_id = update.message.from_user.id
        elif update.callback_query and update.callback_query.from_user:
            user_id = update.callback_query.from_user.id
        
        if not user_id:
            logger.warning("⚠️ Получено обновление без пользователя")
            return
        
        # 🔒 УСТАНАВЛИВАЕМ КОНТЕКСТ ПОЛЬЗОВАТЕЛЯ ДЛЯ ИЗОЛЯЦИИ ДАННЫХ
        UserContextManager.set_current_user(user_id)
        
        # Проверяем доступ
        access_manager = SmartAccessManager()
        has_access = access_manager.check_access_fast(user_id)
        
        if not has_access:
            logger.warning(f"🚫 БЛОКИРОВКА: Пользователь {user_id} пытается взаимодействовать без доступа")
            
            # Отправляем сообщение об отказе
            access_manager.send_access_denied_message(user_id)
            
            # Просто возвращаемся без обработки
            return
        
        logger.debug(f"✅ Доступ разрешен для пользователя {user_id}")
        
    except Exception as e:
        logger.error(f"❌ Ошибка в universal_access_middleware: {e}")
        # В случае ошибки middleware НЕ блокируем запрос 
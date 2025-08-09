#!/usr/bin/env python3
"""
🔔 Notification Handlers - Обработчики уведомлений для админ панели
"""

import logging
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import CallbackContext, ConversationHandler

# Импорты систем уведомлений
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from utils.notification_manager import get_notification_manager, NotificationType, NotificationPriority
from utils.subscription_monitor import get_subscription_monitor
from utils.broadcast_system import get_broadcast_system, BroadcastType

logger = logging.getLogger(__name__)

# Состояния конversation handler'а
(WAITING_BROADCAST_TITLE, WAITING_BROADCAST_MESSAGE, WAITING_BROADCAST_TYPE,
 WAITING_PERSONAL_USER, WAITING_PERSONAL_TITLE, WAITING_PERSONAL_MESSAGE,
 WAITING_SCHEDULE_TIME) = range(7)

def notifications_menu(update: Update, context: CallbackContext):
    """Главное меню уведомлений"""
    try:
        keyboard = [
            [
                InlineKeyboardButton("📢 Массовая рассылка", callback_data="notif_broadcast"),
                InlineKeyboardButton("👤 Персональное уведомление", callback_data="notif_personal")
            ],
            [
                InlineKeyboardButton("⏰ Мониторинг подписок", callback_data="notif_subscriptions"),
                InlineKeyboardButton("📊 Статистика уведомлений", callback_data="notif_stats")
            ],
            [
                InlineKeyboardButton("📋 История рассылок", callback_data="notif_history"),
                InlineKeyboardButton("⚙️ Настройки уведомлений", callback_data="notif_settings")
            ],
            [
                InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")
            ]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        text = """🔔 **СИСТЕМА УВЕДОМЛЕНИЙ**

**Доступные функции:**

📢 **Массовая рассылка** - Отправка всем пользователям или группам
👤 **Персональное уведомление** - Уведомление конкретному пользователю
⏰ **Мониторинг подписок** - Автоматические напоминания о подписке
📊 **Статистика** - Аналитика по уведомлениям
📋 **История** - Последние рассылки
⚙️ **Настройки** - Конфигурация системы

Выберите нужную функцию:"""
        
        if update.callback_query:
            update.callback_query.answer()
            update.callback_query.edit_message_text(text, reply_markup=reply_markup, parse_mode='Markdown')
        else:
            update.message.reply_text(text, reply_markup=reply_markup, parse_mode='Markdown')
        
    except Exception as e:
        logger.error(f"Ошибка в notifications_menu: {e}")

def handle_broadcast_start(update: Update, context: CallbackContext):
    """Начинает процесс создания рассылки"""
    update.callback_query.answer()
    
    text = """📢 **СОЗДАНИЕ МАССОВОЙ РАССЫЛКИ**

Введите **заголовок** рассылки:

💡 Примеры:
• 🚀 Обновление системы v2.0
• 🎉 Новая функция - автопостинг
• ⚠️ Техническое обслуживание

Заголовок должен быть кратким и информативным."""
    
    update.callback_query.edit_message_text(text, parse_mode='Markdown')
    
    return WAITING_BROADCAST_TITLE

def handle_broadcast_title(update: Update, context: CallbackContext):
    """Обрабатывает заголовок рассылки"""
    title = update.message.text.strip()
    
    if len(title) < 3:
        update.message.reply_text("❌ Заголовок слишком короткий. Минимум 3 символа.")
        return WAITING_BROADCAST_TITLE
    
    if len(title) > 100:
        update.message.reply_text("❌ Заголовок слишком длинный. Максимум 100 символов.")
        return WAITING_BROADCAST_TITLE
    
    context.user_data['broadcast_title'] = title
    
    text = f"""✅ Заголовок сохранен: **{title}**

Теперь введите **текст сообщения**:

💡 Советы:
• Пишите понятно и кратко
• Используйте эмодзи для визуального оформления
• Укажите важную информацию в начале
• Добавьте призыв к действию если нужно

📝 Пример:
Привет! 🎉
Мы запустили новую функцию автоматического постинга!
Теперь вы можете планировать посты на неделю вперед.
👉 Попробуйте в разделе "Публикации"
"""
    
    update.message.reply_text(text, parse_mode='Markdown')
    
    return WAITING_BROADCAST_MESSAGE

def handle_broadcast_message(update: Update, context: CallbackContext):
    """Обрабатывает текст рассылки"""
    message = update.message.text.strip()
    
    if len(message) < 10:
        update.message.reply_text("❌ Сообщение слишком короткое. Минимум 10 символов.")
        return WAITING_BROADCAST_MESSAGE
    
    if len(message) > 2000:
        update.message.reply_text("❌ Сообщение слишком длинное. Максимум 2000 символов.")
        return WAITING_BROADCAST_MESSAGE
    
    context.user_data['broadcast_message'] = message
    
    # Показываем типы рассылки
    keyboard = [
        [
            InlineKeyboardButton("👥 Всем пользователям", callback_data="broadcast_type_all"),
            InlineKeyboardButton("🆓 Trial пользователям", callback_data="broadcast_type_trial")
        ],
        [
            InlineKeyboardButton("💎 Premium пользователям", callback_data="broadcast_type_premium"),
            InlineKeyboardButton("🔓 Free пользователям", callback_data="broadcast_type_free")
        ],
        [
            InlineKeyboardButton("✅ Активным пользователям", callback_data="broadcast_type_active"),
            InlineKeyboardButton("⏰ С истекающей подпиской", callback_data="broadcast_type_expiring")
        ],
        [
            InlineKeyboardButton("❌ Отмена", callback_data="notif_menu")
        ]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    text = f"""✅ Сообщение сохранено!

**Заголовок:** {context.user_data['broadcast_title']}
**Сообщение:** {message[:100]}{'...' if len(message) > 100 else ''}

Выберите **тип получателей**:"""
    
    update.message.reply_text(text, reply_markup=reply_markup, parse_mode='Markdown')
    
    return WAITING_BROADCAST_TYPE

def handle_broadcast_type(update: Update, context: CallbackContext):
    """Обрабатывает выбор типа рассылки"""
    query = update.callback_query
    query.answer()
    
    type_mapping = {
        'broadcast_type_all': ('all', 'Всем пользователям'),
        'broadcast_type_trial': ('trial', 'Trial пользователям'),
        'broadcast_type_premium': ('premium', 'Premium пользователям'), 
        'broadcast_type_free': ('free', 'Free пользователям'),
        'broadcast_type_active': ('active', 'Активным пользователям'),
        'broadcast_type_expiring': ('expiring', 'С истекающей подпиской')
    }
    
    if query.data not in type_mapping:
        return notifications_menu(update, context)
    
    broadcast_type, type_name = type_mapping[query.data]
    context.user_data['broadcast_type'] = broadcast_type
    
    # Показываем финальное подтверждение
    keyboard = [
        [
            InlineKeyboardButton("🚀 Отправить сейчас", callback_data="broadcast_send_now"),
            InlineKeyboardButton("⏰ Запланировать", callback_data="broadcast_schedule")
        ],
        [
            InlineKeyboardButton("✏️ Редактировать", callback_data="notif_broadcast"),
            InlineKeyboardButton("❌ Отмена", callback_data="notif_menu")
        ]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    title = context.user_data['broadcast_title']
    message = context.user_data['broadcast_message']
    
    text = f"""📋 **ПОДТВЕРЖДЕНИЕ РАССЫЛКИ**

**Заголовок:** {title}
**Получатели:** {type_name}
**Сообщение:**
{message}

Когда отправить рассылку?"""
    
    query.edit_message_text(text, reply_markup=reply_markup, parse_mode='Markdown')
    
    return ConversationHandler.END

def handle_broadcast_send(update: Update, context: CallbackContext):
    """Отправляет рассылку"""
    query = update.callback_query
    query.answer()
    
    try:
        admin_id = update.effective_user.id
        title = context.user_data.get('broadcast_title', '')
        message = context.user_data.get('broadcast_message', '')
        broadcast_type = context.user_data.get('broadcast_type', 'all')
        
        broadcast_system = get_broadcast_system()
        
        if query.data == "broadcast_send_now":
            # Отправляем сейчас
            broadcast_id = broadcast_system.broadcast_to_group(
                title=title,
                message=message,
                group=broadcast_type,
                admin_id=admin_id
            )
            
            text = f"""✅ **РАССЫЛКА ЗАПУЩЕНА!**

🆔 ID рассылки: `{broadcast_id}`
📊 Статус: Добавлена в очередь

Рассылка будет обработана в течение нескольких минут.
Вы можете отслеживать прогресс в разделе "📋 История рассылок"."""
            
        else:
            # Планируем на потом
            text = """⏰ **ПЛАНИРОВАНИЕ РАССЫЛКИ**

Введите время отправки в формате:
`ГГГГ-ММ-ДД ЧЧ:ММ`

💡 Примеры:
• `2024-12-25 10:00` - 25 декабря в 10:00
• `2024-12-20 18:30` - 20 декабря в 18:30

Или используйте относительное время:
• `+1h` - через 1 час
• `+30m` - через 30 минут
• `+1d` - через 1 день"""
            
            query.edit_message_text(text, parse_mode='Markdown')
            return WAITING_SCHEDULE_TIME
        
        # Очищаем данные
        context.user_data.pop('broadcast_title', None)
        context.user_data.pop('broadcast_message', None) 
        context.user_data.pop('broadcast_type', None)
        
        query.edit_message_text(text, parse_mode='Markdown')
        
    except Exception as e:
        logger.error(f"Ошибка отправки рассылки: {e}")
        query.edit_message_text(f"❌ Ошибка отправки рассылки: {str(e)}")

def handle_notifications_stats(update: Update, context: CallbackContext):
    """Показывает статистику уведомлений"""
    query = update.callback_query
    query.answer()
    
    try:
        # Получаем статистику
        notification_manager = get_notification_manager()
        broadcast_system = get_broadcast_system()
        subscription_monitor = get_subscription_monitor()
        
        notif_stats = notification_manager.get_stats(7)
        broadcast_stats = broadcast_system.get_broadcast_stats(7)
        monitor_stats = subscription_monitor.get_monitor_stats()
        
        text = f"""📊 **СТАТИСТИКА УВЕДОМЛЕНИЙ**

**🔔 Уведомления (7 дней):**
• Отправлено: {notif_stats.get('total_sent_period', 0)}
• В очереди: {notif_stats.get('pending_count', 0)}
• Запланировано: {notif_stats.get('scheduled_count', 0)}

**📢 Рассылки (7 дней):**
• Рассылок: {broadcast_stats.get('total_broadcasts_period', 0)}
• Сообщений: {broadcast_stats.get('total_messages_period', 0)}
• В очереди: {broadcast_stats.get('queue_size', 0)}

**⏰ Мониторинг подписок:**
• Всего пользователей: {monitor_stats.get('total_users', 0)}
• Активных подписок: {monitor_stats.get('active_subscriptions', 0)}
• Истекает скоро: {monitor_stats.get('expiring_soon', 0)}
• Истекших: {monitor_stats.get('expired', 0)}
• Мониторинг: {'🟢 Активен' if monitor_stats.get('monitoring_active') else '🔴 Неактивен'}

**Системы:**
• Notification Manager: 🟢 Активен
• Broadcast System: {'🟢 Активен' if broadcast_stats.get('processing_active') else '🔴 Неактивен'}
• Redis: 🟢 Подключен"""
        
        keyboard = [
            [InlineKeyboardButton("🔄 Обновить", callback_data="notif_stats")],
            [InlineKeyboardButton("🏠 Назад к уведомлениям", callback_data="notif_menu")]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        query.edit_message_text(text, reply_markup=reply_markup, parse_mode='Markdown')
        
    except Exception as e:
        logger.error(f"Ошибка получения статистики: {e}")
        query.edit_message_text(f"❌ Ошибка получения статистики: {str(e)}")

def handle_subscriptions_monitor(update: Update, context: CallbackContext):
    """Показывает мониторинг подписок"""
    query = update.callback_query
    query.answer()
    
    try:
        subscription_monitor = get_subscription_monitor()
        
        # Получаем истекающие подписки
        expiring = subscription_monitor.get_expiring_subscriptions(7)
        stats = subscription_monitor.get_monitor_stats()
        
        text = f"""⏰ **МОНИТОРИНГ ПОДПИСОК**

**📊 Общая статистика:**
• Всего пользователей: {stats.get('total_users', 0)}
• Активных подписок: {stats.get('active_subscriptions', 0)}
• Истекает в течение 7 дней: {stats.get('expiring_soon', 0)}
• Уже истекших: {stats.get('expired', 0)}

**🚨 Истекающие подписки:**"""
        
        if expiring:
            for sub in expiring[:10]:  # Показываем первые 10
                username = sub.get('username', 'Неизвестен')
                days_left = sub.get('days_left', 0)
                plan = sub.get('subscription_plan', 'trial')
                
                if days_left == 0:
                    status = "🔴 Истекает сегодня"
                elif days_left == 1:
                    status = "🟡 Истекает завтра"
                else:
                    status = f"⏰ Через {days_left} дн."
                
                text += f"\n• @{username} ({plan}) - {status}"
            
            if len(expiring) > 10:
                text += f"\n... и еще {len(expiring) - 10} пользователей"
        else:
            text += "\n✅ Нет подписок, истекающих в ближайшее время"
        
        keyboard = [
            [
                InlineKeyboardButton("🔍 Проверить все подписки", callback_data="check_all_subscriptions"),
                InlineKeyboardButton("📊 Подробная статистика", callback_data="subscription_detailed_stats")
            ],
            [
                InlineKeyboardButton("🔄 Обновить", callback_data="notif_subscriptions"),
                InlineKeyboardButton("🏠 Назад", callback_data="notif_menu")
            ]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        query.edit_message_text(text, reply_markup=reply_markup, parse_mode='Markdown')
        
    except Exception as e:
        logger.error(f"Ошибка мониторинга подписок: {e}")
        query.edit_message_text(f"❌ Ошибка мониторинга подписок: {str(e)}")

def handle_broadcast_history(update: Update, context: CallbackContext):
    """Показывает историю рассылок"""
    query = update.callback_query
    query.answer()
    
    try:
        broadcast_system = get_broadcast_system()
        recent_broadcasts = broadcast_system.get_recent_broadcasts(10)
        
        text = "📋 **ИСТОРИЯ РАССЫЛОК**\n\n"
        
        if recent_broadcasts:
            for broadcast in recent_broadcasts:
                title = broadcast.get('title', 'Без названия')
                status = broadcast.get('status', 'unknown')
                created_at = broadcast.get('created_at', '')
                sent_count = broadcast.get('sent_count', 0)
                total_recipients = broadcast.get('total_recipients', 0)
                
                try:
                    created_date = datetime.fromisoformat(created_at).strftime('%d.%m %H:%M')
                except:
                    created_date = 'Неизвестно'
                
                status_emoji = {
                    'pending': '⏳',
                    'in_progress': '🔄',
                    'completed': '✅',
                    'failed': '❌',
                    'cancelled': '🚫'
                }.get(status, '❓')
                
                delivery_info = ""
                if total_recipients > 0:
                    delivery_info = f" ({sent_count}/{total_recipients})"
                
                text += f"{status_emoji} **{title}**\n"
                text += f"   📅 {created_date}{delivery_info}\n\n"
        else:
            text += "📭 История рассылок пуста"
        
        keyboard = [
            [
                InlineKeyboardButton("🔄 Обновить", callback_data="notif_history"),
                InlineKeyboardButton("📊 Статистика", callback_data="notif_stats")
            ],
            [
                InlineKeyboardButton("🏠 Назад", callback_data="notif_menu")
            ]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        query.edit_message_text(text, reply_markup=reply_markup, parse_mode='Markdown')
        
    except Exception as e:
        logger.error(f"Ошибка получения истории рассылок: {e}")
        query.edit_message_text(f"❌ Ошибка получения истории: {str(e)}")

def handle_check_all_subscriptions(update: Update, context: CallbackContext):
    """Запускает проверку всех подписок вручную"""
    query = update.callback_query
    query.answer("🔍 Запускаем проверку всех подписок...")
    
    try:
        subscription_monitor = get_subscription_monitor()
        subscription_monitor.check_all_subscriptions()
        
        query.edit_message_text(
            "✅ **ПРОВЕРКА ЗАВЕРШЕНА**\n\n"
            "Все подписки проверены, напоминания отправлены нуждающимся пользователям.\n\n"
            "Результаты можно посмотреть в статистике уведомлений.",
            parse_mode='Markdown'
        )
        
    except Exception as e:
        logger.error(f"Ошибка проверки подписок: {e}")
        query.edit_message_text(f"❌ Ошибка проверки подписок: {str(e)}")

# Функция для отмены conversation
def cancel_conversation(update: Update, context: CallbackContext):
    """Отменяет conversation"""
    context.user_data.clear()
    return notifications_menu(update, context)

# Conversation handler для создания рассылки
def get_broadcast_conversation_handler():
    """Создает conversation handler для рассылок"""
    from telegram.ext import ConversationHandler, CommandHandler, MessageHandler, Filters, CallbackQueryHandler
    
    return ConversationHandler(
        entry_points=[CallbackQueryHandler(handle_broadcast_start, pattern="notif_broadcast")],
        states={
            WAITING_BROADCAST_TITLE: [MessageHandler(Filters.text & ~Filters.command, handle_broadcast_title)],
            WAITING_BROADCAST_MESSAGE: [MessageHandler(Filters.text & ~Filters.command, handle_broadcast_message)],
            WAITING_BROADCAST_TYPE: [CallbackQueryHandler(handle_broadcast_type, pattern="broadcast_type_.*")],
        },
        fallbacks=[
            CallbackQueryHandler(cancel_conversation, pattern="notif_menu"),
            CommandHandler('cancel', cancel_conversation)
        ],
        map_to_parent={
            ConversationHandler.END: ConversationHandler.END,
        }
    ) 
"""
🔔 Notification Handlers - Обработчики уведомлений для админ панели
"""

import logging
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import CallbackContext, ConversationHandler

# Импорты систем уведомлений
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from utils.notification_manager import get_notification_manager, NotificationType, NotificationPriority
from utils.subscription_monitor import get_subscription_monitor
from utils.broadcast_system import get_broadcast_system, BroadcastType

logger = logging.getLogger(__name__)

# Состояния конversation handler'а
(WAITING_BROADCAST_TITLE, WAITING_BROADCAST_MESSAGE, WAITING_BROADCAST_TYPE,
 WAITING_PERSONAL_USER, WAITING_PERSONAL_TITLE, WAITING_PERSONAL_MESSAGE,
 WAITING_SCHEDULE_TIME) = range(7)

def notifications_menu(update: Update, context: CallbackContext):
    """Главное меню уведомлений"""
    try:
        keyboard = [
            [
                InlineKeyboardButton("📢 Массовая рассылка", callback_data="notif_broadcast"),
                InlineKeyboardButton("👤 Персональное уведомление", callback_data="notif_personal")
            ],
            [
                InlineKeyboardButton("⏰ Мониторинг подписок", callback_data="notif_subscriptions"),
                InlineKeyboardButton("📊 Статистика уведомлений", callback_data="notif_stats")
            ],
            [
                InlineKeyboardButton("📋 История рассылок", callback_data="notif_history"),
                InlineKeyboardButton("⚙️ Настройки уведомлений", callback_data="notif_settings")
            ],
            [
                InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")
            ]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        text = """🔔 **СИСТЕМА УВЕДОМЛЕНИЙ**

**Доступные функции:**

📢 **Массовая рассылка** - Отправка всем пользователям или группам
👤 **Персональное уведомление** - Уведомление конкретному пользователю
⏰ **Мониторинг подписок** - Автоматические напоминания о подписке
📊 **Статистика** - Аналитика по уведомлениям
📋 **История** - Последние рассылки
⚙️ **Настройки** - Конфигурация системы

Выберите нужную функцию:"""
        
        if update.callback_query:
            update.callback_query.answer()
            update.callback_query.edit_message_text(text, reply_markup=reply_markup, parse_mode='Markdown')
        else:
            update.message.reply_text(text, reply_markup=reply_markup, parse_mode='Markdown')
        
    except Exception as e:
        logger.error(f"Ошибка в notifications_menu: {e}")

def handle_broadcast_start(update: Update, context: CallbackContext):
    """Начинает процесс создания рассылки"""
    update.callback_query.answer()
    
    text = """📢 **СОЗДАНИЕ МАССОВОЙ РАССЫЛКИ**

Введите **заголовок** рассылки:

💡 Примеры:
• 🚀 Обновление системы v2.0
• 🎉 Новая функция - автопостинг
• ⚠️ Техническое обслуживание

Заголовок должен быть кратким и информативным."""
    
    update.callback_query.edit_message_text(text, parse_mode='Markdown')
    
    return WAITING_BROADCAST_TITLE

def handle_broadcast_title(update: Update, context: CallbackContext):
    """Обрабатывает заголовок рассылки"""
    title = update.message.text.strip()
    
    if len(title) < 3:
        update.message.reply_text("❌ Заголовок слишком короткий. Минимум 3 символа.")
        return WAITING_BROADCAST_TITLE
    
    if len(title) > 100:
        update.message.reply_text("❌ Заголовок слишком длинный. Максимум 100 символов.")
        return WAITING_BROADCAST_TITLE
    
    context.user_data['broadcast_title'] = title
    
    text = f"""✅ Заголовок сохранен: **{title}**

Теперь введите **текст сообщения**:

💡 Советы:
• Пишите понятно и кратко
• Используйте эмодзи для визуального оформления
• Укажите важную информацию в начале
• Добавьте призыв к действию если нужно

📝 Пример:
Привет! 🎉
Мы запустили новую функцию автоматического постинга!
Теперь вы можете планировать посты на неделю вперед.
👉 Попробуйте в разделе "Публикации"
"""
    
    update.message.reply_text(text, parse_mode='Markdown')
    
    return WAITING_BROADCAST_MESSAGE

def handle_broadcast_message(update: Update, context: CallbackContext):
    """Обрабатывает текст рассылки"""
    message = update.message.text.strip()
    
    if len(message) < 10:
        update.message.reply_text("❌ Сообщение слишком короткое. Минимум 10 символов.")
        return WAITING_BROADCAST_MESSAGE
    
    if len(message) > 2000:
        update.message.reply_text("❌ Сообщение слишком длинное. Максимум 2000 символов.")
        return WAITING_BROADCAST_MESSAGE
    
    context.user_data['broadcast_message'] = message
    
    # Показываем типы рассылки
    keyboard = [
        [
            InlineKeyboardButton("👥 Всем пользователям", callback_data="broadcast_type_all"),
            InlineKeyboardButton("🆓 Trial пользователям", callback_data="broadcast_type_trial")
        ],
        [
            InlineKeyboardButton("💎 Premium пользователям", callback_data="broadcast_type_premium"),
            InlineKeyboardButton("🔓 Free пользователям", callback_data="broadcast_type_free")
        ],
        [
            InlineKeyboardButton("✅ Активным пользователям", callback_data="broadcast_type_active"),
            InlineKeyboardButton("⏰ С истекающей подпиской", callback_data="broadcast_type_expiring")
        ],
        [
            InlineKeyboardButton("❌ Отмена", callback_data="notif_menu")
        ]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    text = f"""✅ Сообщение сохранено!

**Заголовок:** {context.user_data['broadcast_title']}
**Сообщение:** {message[:100]}{'...' if len(message) > 100 else ''}

Выберите **тип получателей**:"""
    
    update.message.reply_text(text, reply_markup=reply_markup, parse_mode='Markdown')
    
    return WAITING_BROADCAST_TYPE

def handle_broadcast_type(update: Update, context: CallbackContext):
    """Обрабатывает выбор типа рассылки"""
    query = update.callback_query
    query.answer()
    
    type_mapping = {
        'broadcast_type_all': ('all', 'Всем пользователям'),
        'broadcast_type_trial': ('trial', 'Trial пользователям'),
        'broadcast_type_premium': ('premium', 'Premium пользователям'), 
        'broadcast_type_free': ('free', 'Free пользователям'),
        'broadcast_type_active': ('active', 'Активным пользователям'),
        'broadcast_type_expiring': ('expiring', 'С истекающей подпиской')
    }
    
    if query.data not in type_mapping:
        return notifications_menu(update, context)
    
    broadcast_type, type_name = type_mapping[query.data]
    context.user_data['broadcast_type'] = broadcast_type
    
    # Показываем финальное подтверждение
    keyboard = [
        [
            InlineKeyboardButton("🚀 Отправить сейчас", callback_data="broadcast_send_now"),
            InlineKeyboardButton("⏰ Запланировать", callback_data="broadcast_schedule")
        ],
        [
            InlineKeyboardButton("✏️ Редактировать", callback_data="notif_broadcast"),
            InlineKeyboardButton("❌ Отмена", callback_data="notif_menu")
        ]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    title = context.user_data['broadcast_title']
    message = context.user_data['broadcast_message']
    
    text = f"""📋 **ПОДТВЕРЖДЕНИЕ РАССЫЛКИ**

**Заголовок:** {title}
**Получатели:** {type_name}
**Сообщение:**
{message}

Когда отправить рассылку?"""
    
    query.edit_message_text(text, reply_markup=reply_markup, parse_mode='Markdown')
    
    return ConversationHandler.END

def handle_broadcast_send(update: Update, context: CallbackContext):
    """Отправляет рассылку"""
    query = update.callback_query
    query.answer()
    
    try:
        admin_id = update.effective_user.id
        title = context.user_data.get('broadcast_title', '')
        message = context.user_data.get('broadcast_message', '')
        broadcast_type = context.user_data.get('broadcast_type', 'all')
        
        broadcast_system = get_broadcast_system()
        
        if query.data == "broadcast_send_now":
            # Отправляем сейчас
            broadcast_id = broadcast_system.broadcast_to_group(
                title=title,
                message=message,
                group=broadcast_type,
                admin_id=admin_id
            )
            
            text = f"""✅ **РАССЫЛКА ЗАПУЩЕНА!**

🆔 ID рассылки: `{broadcast_id}`
📊 Статус: Добавлена в очередь

Рассылка будет обработана в течение нескольких минут.
Вы можете отслеживать прогресс в разделе "📋 История рассылок"."""
            
        else:
            # Планируем на потом
            text = """⏰ **ПЛАНИРОВАНИЕ РАССЫЛКИ**

Введите время отправки в формате:
`ГГГГ-ММ-ДД ЧЧ:ММ`

💡 Примеры:
• `2024-12-25 10:00` - 25 декабря в 10:00
• `2024-12-20 18:30` - 20 декабря в 18:30

Или используйте относительное время:
• `+1h` - через 1 час
• `+30m` - через 30 минут
• `+1d` - через 1 день"""
            
            query.edit_message_text(text, parse_mode='Markdown')
            return WAITING_SCHEDULE_TIME
        
        # Очищаем данные
        context.user_data.pop('broadcast_title', None)
        context.user_data.pop('broadcast_message', None) 
        context.user_data.pop('broadcast_type', None)
        
        query.edit_message_text(text, parse_mode='Markdown')
        
    except Exception as e:
        logger.error(f"Ошибка отправки рассылки: {e}")
        query.edit_message_text(f"❌ Ошибка отправки рассылки: {str(e)}")

def handle_notifications_stats(update: Update, context: CallbackContext):
    """Показывает статистику уведомлений"""
    query = update.callback_query
    query.answer()
    
    try:
        # Получаем статистику
        notification_manager = get_notification_manager()
        broadcast_system = get_broadcast_system()
        subscription_monitor = get_subscription_monitor()
        
        notif_stats = notification_manager.get_stats(7)
        broadcast_stats = broadcast_system.get_broadcast_stats(7)
        monitor_stats = subscription_monitor.get_monitor_stats()
        
        text = f"""📊 **СТАТИСТИКА УВЕДОМЛЕНИЙ**

**🔔 Уведомления (7 дней):**
• Отправлено: {notif_stats.get('total_sent_period', 0)}
• В очереди: {notif_stats.get('pending_count', 0)}
• Запланировано: {notif_stats.get('scheduled_count', 0)}

**📢 Рассылки (7 дней):**
• Рассылок: {broadcast_stats.get('total_broadcasts_period', 0)}
• Сообщений: {broadcast_stats.get('total_messages_period', 0)}
• В очереди: {broadcast_stats.get('queue_size', 0)}

**⏰ Мониторинг подписок:**
• Всего пользователей: {monitor_stats.get('total_users', 0)}
• Активных подписок: {monitor_stats.get('active_subscriptions', 0)}
• Истекает скоро: {monitor_stats.get('expiring_soon', 0)}
• Истекших: {monitor_stats.get('expired', 0)}
• Мониторинг: {'🟢 Активен' if monitor_stats.get('monitoring_active') else '🔴 Неактивен'}

**Системы:**
• Notification Manager: 🟢 Активен
• Broadcast System: {'🟢 Активен' if broadcast_stats.get('processing_active') else '🔴 Неактивен'}
• Redis: 🟢 Подключен"""
        
        keyboard = [
            [InlineKeyboardButton("🔄 Обновить", callback_data="notif_stats")],
            [InlineKeyboardButton("🏠 Назад к уведомлениям", callback_data="notif_menu")]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        query.edit_message_text(text, reply_markup=reply_markup, parse_mode='Markdown')
        
    except Exception as e:
        logger.error(f"Ошибка получения статистики: {e}")
        query.edit_message_text(f"❌ Ошибка получения статистики: {str(e)}")

def handle_subscriptions_monitor(update: Update, context: CallbackContext):
    """Показывает мониторинг подписок"""
    query = update.callback_query
    query.answer()
    
    try:
        subscription_monitor = get_subscription_monitor()
        
        # Получаем истекающие подписки
        expiring = subscription_monitor.get_expiring_subscriptions(7)
        stats = subscription_monitor.get_monitor_stats()
        
        text = f"""⏰ **МОНИТОРИНГ ПОДПИСОК**

**📊 Общая статистика:**
• Всего пользователей: {stats.get('total_users', 0)}
• Активных подписок: {stats.get('active_subscriptions', 0)}
• Истекает в течение 7 дней: {stats.get('expiring_soon', 0)}
• Уже истекших: {stats.get('expired', 0)}

**🚨 Истекающие подписки:**"""
        
        if expiring:
            for sub in expiring[:10]:  # Показываем первые 10
                username = sub.get('username', 'Неизвестен')
                days_left = sub.get('days_left', 0)
                plan = sub.get('subscription_plan', 'trial')
                
                if days_left == 0:
                    status = "🔴 Истекает сегодня"
                elif days_left == 1:
                    status = "🟡 Истекает завтра"
                else:
                    status = f"⏰ Через {days_left} дн."
                
                text += f"\n• @{username} ({plan}) - {status}"
            
            if len(expiring) > 10:
                text += f"\n... и еще {len(expiring) - 10} пользователей"
        else:
            text += "\n✅ Нет подписок, истекающих в ближайшее время"
        
        keyboard = [
            [
                InlineKeyboardButton("🔍 Проверить все подписки", callback_data="check_all_subscriptions"),
                InlineKeyboardButton("📊 Подробная статистика", callback_data="subscription_detailed_stats")
            ],
            [
                InlineKeyboardButton("🔄 Обновить", callback_data="notif_subscriptions"),
                InlineKeyboardButton("🏠 Назад", callback_data="notif_menu")
            ]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        query.edit_message_text(text, reply_markup=reply_markup, parse_mode='Markdown')
        
    except Exception as e:
        logger.error(f"Ошибка мониторинга подписок: {e}")
        query.edit_message_text(f"❌ Ошибка мониторинга подписок: {str(e)}")

def handle_broadcast_history(update: Update, context: CallbackContext):
    """Показывает историю рассылок"""
    query = update.callback_query
    query.answer()
    
    try:
        broadcast_system = get_broadcast_system()
        recent_broadcasts = broadcast_system.get_recent_broadcasts(10)
        
        text = "📋 **ИСТОРИЯ РАССЫЛОК**\n\n"
        
        if recent_broadcasts:
            for broadcast in recent_broadcasts:
                title = broadcast.get('title', 'Без названия')
                status = broadcast.get('status', 'unknown')
                created_at = broadcast.get('created_at', '')
                sent_count = broadcast.get('sent_count', 0)
                total_recipients = broadcast.get('total_recipients', 0)
                
                try:
                    created_date = datetime.fromisoformat(created_at).strftime('%d.%m %H:%M')
                except:
                    created_date = 'Неизвестно'
                
                status_emoji = {
                    'pending': '⏳',
                    'in_progress': '🔄',
                    'completed': '✅',
                    'failed': '❌',
                    'cancelled': '🚫'
                }.get(status, '❓')
                
                delivery_info = ""
                if total_recipients > 0:
                    delivery_info = f" ({sent_count}/{total_recipients})"
                
                text += f"{status_emoji} **{title}**\n"
                text += f"   📅 {created_date}{delivery_info}\n\n"
        else:
            text += "📭 История рассылок пуста"
        
        keyboard = [
            [
                InlineKeyboardButton("🔄 Обновить", callback_data="notif_history"),
                InlineKeyboardButton("📊 Статистика", callback_data="notif_stats")
            ],
            [
                InlineKeyboardButton("🏠 Назад", callback_data="notif_menu")
            ]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        query.edit_message_text(text, reply_markup=reply_markup, parse_mode='Markdown')
        
    except Exception as e:
        logger.error(f"Ошибка получения истории рассылок: {e}")
        query.edit_message_text(f"❌ Ошибка получения истории: {str(e)}")

def handle_check_all_subscriptions(update: Update, context: CallbackContext):
    """Запускает проверку всех подписок вручную"""
    query = update.callback_query
    query.answer("🔍 Запускаем проверку всех подписок...")
    
    try:
        subscription_monitor = get_subscription_monitor()
        subscription_monitor.check_all_subscriptions()
        
        query.edit_message_text(
            "✅ **ПРОВЕРКА ЗАВЕРШЕНА**\n\n"
            "Все подписки проверены, напоминания отправлены нуждающимся пользователям.\n\n"
            "Результаты можно посмотреть в статистике уведомлений.",
            parse_mode='Markdown'
        )
        
    except Exception as e:
        logger.error(f"Ошибка проверки подписок: {e}")
        query.edit_message_text(f"❌ Ошибка проверки подписок: {str(e)}")

# Функция для отмены conversation
def cancel_conversation(update: Update, context: CallbackContext):
    """Отменяет conversation"""
    context.user_data.clear()
    return notifications_menu(update, context)

# Conversation handler для создания рассылки
def get_broadcast_conversation_handler():
    """Создает conversation handler для рассылок"""
    from telegram.ext import ConversationHandler, CommandHandler, MessageHandler, Filters, CallbackQueryHandler
    
    return ConversationHandler(
        entry_points=[CallbackQueryHandler(handle_broadcast_start, pattern="notif_broadcast")],
        states={
            WAITING_BROADCAST_TITLE: [MessageHandler(Filters.text & ~Filters.command, handle_broadcast_title)],
            WAITING_BROADCAST_MESSAGE: [MessageHandler(Filters.text & ~Filters.command, handle_broadcast_message)],
            WAITING_BROADCAST_TYPE: [CallbackQueryHandler(handle_broadcast_type, pattern="broadcast_type_.*")],
        },
        fallbacks=[
            CallbackQueryHandler(cancel_conversation, pattern="notif_menu"),
            CommandHandler('cancel', cancel_conversation)
        ],
        map_to_parent={
            ConversationHandler.END: ConversationHandler.END,
        }
    ) 
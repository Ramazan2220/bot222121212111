#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import os
import sys
from datetime import datetime
from telegram import Update, BotCommand
from telegram.ext import Updater, Dispatcher, CommandHandler, CallbackQueryHandler, MessageHandler, Filters

# Добавляем путь к проекту
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Импорты из проекта
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database.db_manager import get_total_accounts, init_db
from utils.subscription_service import subscription_service

# Импорты админ бота
from admin_bot.config.settings import ADMIN_BOT_TOKEN, MESSAGES, BOT_COMMANDS
from admin_bot.config.admin_list import is_admin, get_user_role, AdminRole
from admin_bot.keyboards.main_keyboard import get_main_keyboard, get_back_to_main_keyboard
from admin_bot.handlers.user_handlers import (
    users_menu, users_list, user_detail,
    handle_user_input, handle_add_user_data,
    handle_delete_user_execute, handle_block_user,
    handle_unblock_user, handle_users_callbacks
)
try:
    from admin_bot.handlers.financial_handlers import get_financial_handlers
    FINANCIAL_AVAILABLE = True
except ImportError:
    print("⚠️ Financial handlers не найдены")
    FINANCIAL_AVAILABLE = False
    def get_financial_handlers(): return {}
try:
    from admin_bot.handlers.system_handlers import get_system_handlers
    SYSTEM_AVAILABLE = True
except ImportError:
    print("⚠️ System handlers не найдены")
    SYSTEM_AVAILABLE = False
    def get_system_handlers(): return {}
try:
    from admin_bot.handlers.analytics_handlers import get_analytics_handlers
    ANALYTICS_AVAILABLE = True
except ImportError:
    print("⚠️ Analytics handlers не найдены")
    ANALYTICS_AVAILABLE = False
    def get_analytics_handlers(): return {}
try:
    from admin_bot.handlers.notification_handlers import (
        notifications_menu, handle_broadcast_start, handle_broadcast_title,
        handle_broadcast_message, handle_broadcast_type, handle_broadcast_send,
        handle_notifications_stats, handle_subscriptions_monitor,
        handle_broadcast_history, handle_check_all_subscriptions,
        cancel_conversation, get_broadcast_conversation_handler
    )
    NOTIFICATIONS_AVAILABLE = True
except ImportError as e:
    print(f"⚠️ Notification handlers не найдены: {e}")
    NOTIFICATIONS_AVAILABLE = False
    # Заглушки
    def notifications_menu(update, context): 
        update.callback_query.edit_message_text("🔔 Уведомления временно недоступны")
    def handle_notifications_stats(update, context): pass
    def handle_subscriptions_monitor(update, context): pass
    def handle_broadcast_history(update, context): pass
    def handle_check_all_subscriptions(update, context): pass
    def handle_broadcast_send(update, context): pass
    def get_broadcast_conversation_handler(): return None

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

class AdminBot:
    """Главный класс администраторского бота"""
    
    def __init__(self):
        if not ADMIN_BOT_TOKEN:
            raise ValueError("ADMIN_BOT_TOKEN не установлен!")
        
        self.logger = logging.getLogger(__name__)
        self.updater = Updater(token=ADMIN_BOT_TOKEN, use_context=True)
        self.dispatcher = self.updater.dispatcher
        self.setup_handlers()
    
    def setup_handlers(self):
        """Настройка обработчиков"""
        
        # Команды
        self.dispatcher.add_handler(CommandHandler("start", self.start_command))
        self.dispatcher.add_handler(CommandHandler("stats", self.stats_command))
        self.dispatcher.add_handler(CommandHandler("help", self.help_command))
        
        # Conversation handler для рассылок (должен быть ПЕРЕД обычными callback обработчиками)
        if NOTIFICATIONS_AVAILABLE:
            broadcast_conv_handler = get_broadcast_conversation_handler()
            if broadcast_conv_handler:
                self.dispatcher.add_handler(broadcast_conv_handler)
        
        # Callback обработчики
        self.dispatcher.add_handler(CallbackQueryHandler(self.callback_handler))
        
        # Обработчик текстовых сообщений
        from admin_bot.handlers.user_handlers import handle_user_input
        self.dispatcher.add_handler(MessageHandler(Filters.text & ~Filters.command, handle_user_input))
        
        # Обработчик ошибок
        self.dispatcher.add_error_handler(self.error_handler)
    
    def start_command(self, update, context):
        """Команда /start"""
        user_id = update.effective_user.id
        username = update.effective_user.username
        
        if not is_admin(user_id):
            update.message.reply_text(
                "❌ У вас нет доступа к админ панели.\n"
                "Обратитесь к администратору для получения прав доступа."
            )
            return
        
        role = get_user_role(user_id)
        role_names = {
            AdminRole.SUPER_ADMIN: "Супер-администратор",
            AdminRole.ADMIN: "Администратор",
            AdminRole.MODERATOR: "Модератор"
        }
        
        welcome_text = f"""🎛️ **АДМИН ПАНЕЛЬ INSTAGRAM BOT**

👋 Добро пожаловать, @{username or 'Администратор'}!

🔑 **Ваша роль:** {role_names.get(role, 'Неизвестно')}
⏰ **Время входа:** {datetime.now().strftime('%d.%m.%Y %H:%M')}

📋 **Доступные функции:**
• Управление пользователями и подписками
• Просмотр статистики и аналитики
• Финансовые отчеты
• Системный мониторинг

Выберите раздел для работы:"""
        
        keyboard = get_main_keyboard(user_id)
        update.message.reply_text(welcome_text, reply_markup=keyboard, parse_mode='Markdown')
    
    def stats_command(self, update, context):
        """Расширенная команда статистики"""
        user_id = update.effective_user.id
        
        if not is_admin(user_id):
            update.message.reply_text("❌ У вас нет доступа к этой команде")
            return
        
        try:
            # Статистика основного бота
            total_accounts = get_total_accounts()
            
            # Статистика пользователей админ панели
            user_stats = user_service.get_statistics()
            
            role = get_user_role(user_id)
            role_name = {
                AdminRole.SUPER_ADMIN: "Супер-администратор",
                AdminRole.ADMIN: "Администратор", 
                AdminRole.MODERATOR: "Модератор"
            }.get(role, "Неизвестно")
            
            stats_text = f"""📊 **СТАТИСТИКА СИСТЕМЫ**

👤 **Ваша роль:** {role_name}

🤖 **Основной бот:**
• Всего аккаунтов: {total_accounts}

👥 **Система пользователей:**
• Всего пользователей: {user_stats['total_users']}
• Активных: {user_stats['active_users']}
• На триале: {user_stats['trial_users']}
• Заблокированных: {user_stats['blocked_users']}
• Истекших: {user_stats['expired_users']}

💰 **Финансы:**
• Оценочный доход: ${user_stats['estimated_revenue']:.2f}
• Средний чек: ${user_stats['estimated_revenue'] / max(user_stats['total_users'], 1):.2f}

📊 **Распределение по планам:**"""
            
            # Добавляем информацию о планах
            from admin_bot.models.user import PLAN_INFO
            for plan_key, count in user_stats['plans_distribution'].items():
                plan_info = None
                # Находим план по ключу
                for plan_enum, info in PLAN_INFO.items():
                    if plan_enum.value == plan_key:
                        plan_info = info
                        break
                
                if plan_info and count > 0:
                    stats_text += f"\n• {plan_info['name']}: {count}"
            
            stats_text += f"\n\n⏰ **Время:** {datetime.now().strftime('%d.%m.%Y %H:%M')}"
            
            update.message.reply_text(stats_text, parse_mode='Markdown')
            
        except Exception as e:
            self.logger.error(f"Ошибка получения статистики: {e}")
            update.message.reply_text("❌ Ошибка получения статистики")
    
    def help_command(self, update, context):
        """Команда помощи"""
        user_id = update.effective_user.id
        
        if not is_admin(user_id):
            update.message.reply_text("❌ У вас нет доступа к этой команде")
            return
        
        help_text = """❓ **ПОМОЩЬ ПО АДМИН ПАНЕЛИ**

🔧 **Команды:**
/start - Главное меню
/stats - Быстрая статистика
/help - Эта справка

📋 **Разделы:**
👥 **Пользователи** - управление пользователями и подписками
📊 **Статистика** - аналитика и отчеты
💰 **Финансы** - доходы и платежи
⚙️ **Система** - мониторинг и настройки

🚨 **Поддержка:**
При возникновении проблем обратитесь к супер-администратору."""
        
        keyboard = get_back_to_main_keyboard()
        update.message.reply_text(help_text, reply_markup=keyboard, parse_mode='Markdown')
    
    def main_menu_callback(self, update, context):
        """Возврат в главное меню"""
        query = update.callback_query
        user_id = update.effective_user.id
        
        if not is_admin(user_id):
            query.answer("❌ У вас нет доступа")
            return
        
        role = get_user_role(user_id)
        role_names = {
            AdminRole.SUPER_ADMIN: "Супер-администратор",
            AdminRole.ADMIN: "Администратор",
            AdminRole.MODERATOR: "Модератор"
        }
        
        main_text = f"""🎛️ **АДМИН ПАНЕЛЬ INSTAGRAM BOT**

🔑 **Ваша роль:** {role_names.get(role, 'Неизвестно')}
⏰ **Время:** {datetime.now().strftime('%d.%m.%Y %H:%M')}

Выберите раздел для работы:"""
        
        keyboard = get_main_keyboard(user_id)
        query.edit_message_text(main_text, reply_markup=keyboard, parse_mode='Markdown')
    
    def refresh_main_callback(self, update, context):
        """Обновление главного меню"""
        self.main_menu_callback(update, context)
    
    def callback_handler(self, update, context):
        """Обработчик всех callback запросов"""
        query = update.callback_query
        data = query.data
        user_id = update.effective_user.id
        
        if not is_admin(user_id):
            query.answer("❌ У вас нет доступа")
            return
        
        try:
            # Обработчики пользователей
            if (data.startswith("users") or data.startswith("user_") or 
                data.startswith("confirm_delete_") or data.startswith("cancel_delete_")):
                handle_users_callbacks(update, context)
            # Финансовые обработчики
            elif data.startswith("financial"):
                financial_handlers = get_financial_handlers()
                if data in financial_handlers:
                    financial_handlers[data](update, context)
                else:
                    query.answer("🚧 Функция в разработке")
            # Системные обработчики
            elif data.startswith("system"):
                system_handlers = get_system_handlers()
                if data in system_handlers:
                    system_handlers[data](update, context)
                else:
                    query.answer("🚧 Функция в разработке")
            # Аналитические обработчики
            elif data.startswith("analytics") or data == "export":
                analytics_handlers = get_analytics_handlers()
                if data in analytics_handlers:
                    analytics_handlers[data](update, context)
                else:
                    query.answer("🚧 Функция в разработке")
            # Уведомления
            elif data == "notif_menu" or data == "notifications":
                notifications_menu(update, context)
            elif data == "notif_stats":
                handle_notifications_stats(update, context)
            elif data == "notif_subscriptions":
                handle_subscriptions_monitor(update, context)
            elif data == "notif_history":
                handle_broadcast_history(update, context)
            elif data == "check_all_subscriptions":
                handle_check_all_subscriptions(update, context)
            elif data.startswith("broadcast_send"):
                handle_broadcast_send(update, context)
            # Обработчики доступов
            elif data.startswith("users_") or data.startswith("access_") or data.startswith("confirm_remove_access_"):
                from admin_bot.handlers.user_handlers import get_user_handlers
                user_handlers = get_user_handlers()
                if data in user_handlers:
                    user_handlers[data](update, context)
                else:
                    query.answer("🚧 Функция в разработке")
            # Главное меню и основные callback'и
            elif data == "main_menu":
                self.main_menu_callback(update, context)
            elif data == "refresh_main":
                self.refresh_main_callback(update, context)
            elif data == "help":
                query.answer()
                self.help_command(update, context)
            elif data == "stats":
                query.answer()
                self.stats_command(update, context)
            else:
                self.handle_menu_callbacks(update, context)
            
        except Exception as e:
            self.logger.error(f"Ошибка в callback_handler: {e}")
            query.answer("❌ Произошла ошибка")
    
    def handle_menu_callbacks(self, update, context):
        """Обработчик callback'ов главного меню"""
        query = update.callback_query
        data = query.data
        
        if data == "analytics":
            query.edit_message_text(
                "📈 **АНАЛИТИКА**\n\n🚧 Раздел в разработке\n\nСкоро здесь будет:\n• Графики активности\n• Отчеты по доходам\n• Детальная статистика",
                reply_markup=get_main_keyboard(update.effective_user.id),
                parse_mode='Markdown'
            )
        elif data == "financial":
            financial_handlers = get_financial_handlers()
            financial_handlers['financial'](update, context)
        elif data == "system":
            system_handlers = get_system_handlers()
            system_handlers['system'](update, context)
        elif data == "analytics":
            analytics_handlers = get_analytics_handlers()
            analytics_handlers['analytics'](update, context)
        elif data == "notifications":
            notifications_menu(update, context)
        elif data == "export":
            analytics_handlers = get_analytics_handlers()
            analytics_handlers['export'](update, context)

        elif data == "export":
            query.edit_message_text(
                "📊 **ЭКСПОРТ ДАННЫХ**\n\n🚧 Раздел в разработке\n\nСкоро здесь будет:\n• Экспорт в CSV/JSON\n• Автоматические отчеты\n• Архивы данных",
                reply_markup=get_main_keyboard(update.effective_user.id),
                parse_mode='Markdown'
            )
        else:
            query.answer("🚧 Функция в разработке")
    
    def error_handler(self, update, context):
        """Обработчик ошибок"""
        self.logger.error(f'Update "{update}" caused error "{context.error}"')
        
        if update and update.effective_message:
            try:
                update.effective_message.reply_text(
                    "❌ Произошла ошибка. Попробуйте еще раз или обратитесь к администратору."
                )
            except Exception:
                pass
    
    def setup_commands(self):
        """Настройка команд бота"""
        commands = [
            BotCommand("start", "Главное меню"),
            BotCommand("stats", "Статистика"),
            BotCommand("help", "Помощь")
        ]
        
        try:
            self.updater.bot.set_my_commands(commands)
            self.logger.info("Команды бота настроены")
        except Exception as e:
            self.logger.error(f"Ошибка настройки команд: {e}")
    
    def run(self):
        """Запуск бота"""
        try:
            self.logger.info("🤖 Запуск админ бота...")
            
            # Инициализируем основную базу данных
            self.logger.info("📊 Инициализация основной базы данных...")
            init_db()
            self.logger.info("✅ База данных инициализирована")
            
            # Настраиваем команды
            self.setup_commands()
            
            # Проверяем токен
            bot_info = self.updater.bot.get_me()
            self.logger.info(f"✅ Бот запущен: @{bot_info.username}")
            
            # Запускаем polling
            self.updater.start_polling(drop_pending_updates=True)
            self.logger.info("🟢 Админ бот готов к работе!")
            
            # Ждем сигнала остановки
            self.updater.idle()
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка запуска админ бота: {e}")
            raise

if __name__ == "__main__":
    try:
        admin_bot = AdminBot()
        admin_bot.run()
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        sys.exit(1) 
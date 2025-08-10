# -*- coding: utf-8 -*-
"""
Handlers для команд автоматизации и мониторинга
"""

import logging
import threading
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import CallbackContext, CommandHandler, CallbackQueryHandler, ConversationHandler

from services.account_automation import automation_service
from services.rate_limiter import rate_limiter
from telegram_bot.utils.account_selection import AccountSelector
from database.db_manager import get_instagram_account
from telegram_bot.utils.async_handlers import async_handler, LoadingContext, answer_callback_async
from datetime import datetime
from telegram import ParseMode

logger = logging.getLogger(__name__)

# Создаем глобальный селектор аккаунтов для прогрева
warmup_selector = AccountSelector(
    callback_prefix="warmup_acc",
    title="Выберите аккаунты для прогрева",
    allow_multiple=True,
    show_status=True,
    show_folders=True
)

# Добавляем в начало файла после импортов
# Глобальный словарь для отслеживания активных прогревов
active_warmups = {}

def handle_multi_warmup(update: Update, context: CallbackContext, account_ids_str: str):
    """Обработка множественного прогрева"""
    query = update.callback_query
    user_id = query.from_user.id
    
    # Парсим ID аккаунтов
    account_ids = [int(id_) for id_ in account_ids_str.split(',')]
    
    logger.info(f"Запуск параллельного прогрева для {len(account_ids)} аккаунтов: {account_ids}")
    
    # Обновляем сообщение
    # Получаем информацию об аккаунтах
    accounts_info = []
    for acc_id in account_ids:
        account = get_instagram_account(acc_id)
        if account:
            accounts_info.append(f"• @{account.username} (ID: {acc_id})")
    
    query.edit_message_text(
        f"🚀 Запускаю параллельный прогрев {len(account_ids)} аккаунтов\n\n"
        f"📋 Аккаунты:\n" + "\n".join(accounts_info) + "\n\n"
        f"⏱ Длительность: 15 минут на каждый\n"
        f"💡 Прогрев выполняется параллельно\n\n"
        f"⏳ Статус: Инициализация...",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("⏹ Остановить все", callback_data=f"stop_all_warm_{','.join(map(str, account_ids))}")],
            [InlineKeyboardButton("📊 Обновить статус", callback_data=f"update_warm_status_{','.join(map(str, account_ids))}")],
            [InlineKeyboardButton("📱 Главное меню", callback_data="main_menu")]
        ])
    )
    
    # Запускаем прогревы в отдельном потоке
    def run_multi_warmup():
        import asyncio
        from services.advanced_warmup import AdvancedWarmupService
        
        # Создаем новый event loop для этого потока
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        async def warmup_account(account_id):
            # Добавляем в активные прогревы
            warmup_key = f"{user_id}_{account_id}"
            active_warmups[warmup_key] = True
            
            try:
                logger.info(f"🚀 Начинаю прогрев аккаунта {account_id}")
                
                # Создаем экземпляр с callback для проверки остановки
                advanced_warmup = AdvancedWarmupService()
                
                def check_stop():
                    return not active_warmups.get(warmup_key, False)
                
                advanced_warmup.stop_callback = check_stop
                
                # Запускаем прогрев синхронно через run_in_executor
                success, report = await loop.run_in_executor(
                    None,
                    advanced_warmup.start_warmup,
                    account_id,       # account_id
                    15,              # duration_minutes
                    []               # interests
                )
                
                logger.info(f"{'✅' if success else '⚠️'} Прогрев аккаунта {account_id} завершен")
                return account_id, success, report
                
            except Exception as e:
                logger.error(f"Ошибка прогрева аккаунта {account_id}: {e}")
                return account_id, False, f"Ошибка: {str(e)}"
            finally:
                # Убираем из активных
                active_warmups.pop(warmup_key, None)
        
        async def run_all_warmups():
            # Запускаем все прогревы параллельно
            tasks = [warmup_account(acc_id) for acc_id in account_ids]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Обрабатываем результаты
            successful = []
            failed = []
            detailed_reports = {}
            
            for result in results:
                if isinstance(result, Exception):
                    failed.append(("Системная ошибка", str(result), None))
                else:
                    account_id, success, report = result
                    account = get_instagram_account(account_id)
                    username = account.username if account else f"ID:{account_id}"
                    
                    if success:
                        successful.append((username, account_id))
                    else:
                        # Определяем тип ошибки
                        error_type = "Неизвестная ошибка"
                        if "не найден в Instagram" in report or "We can't find an account" in report:
                            error_type = "❌ Аккаунт не существует"
                        elif "Требуется повторный вход" in report or "login_required" in report:
                            error_type = "🔐 Требуется верификация"
                        elif "565 Server Error" in report or "challenge" in report:
                            error_type = "📧 Ошибка верификации"
                        elif "прокси" in report.lower() or "proxy" in report.lower():
                            error_type = "🌐 Проблема с прокси"
                        elif "Не удалось войти" in report:
                            error_type = "🚫 Ошибка входа"
                        
                        failed.append((username, error_type, account_id))
            
            # Формируем итоговый отчет
            final_report = f"📊 Итоговый отчет о прогреве\n\n"
            final_report += f"Всего аккаунтов: {len(account_ids)}\n"
            final_report += f"✅ Успешно: {len(successful)}\n"
            final_report += f"❌ С ошибками: {len(failed)}\n\n"
            
            if successful:
                final_report += "✅ Успешно прогретые:\n"
                for username, acc_id in successful[:10]:
                    final_report += f"• @{username}\n"
                if len(successful) > 10:
                    final_report += f"...и еще {len(successful) - 10} аккаунтов\n"
                final_report += "\n"
            
            if failed:
                final_report += "❌ Проблемные аккаунты:\n"
                # Группируем по типу ошибки
                error_groups = {}
                for username, error_type, acc_id in failed:
                    if error_type not in error_groups:
                        error_groups[error_type] = []
                    error_groups[error_type].append(f"@{username}")
                
                for error_type, accounts in error_groups.items():
                    final_report += f"\n{error_type}:\n"
                    for acc in accounts[:5]:
                        final_report += f"• {acc}\n"
                    if len(accounts) > 5:
                        final_report += f"...и еще {len(accounts) - 5}\n"
                
                final_report += "\n💡 Рекомендации:\n"
                if "❌ Аккаунт не существует" in error_groups:
                    final_report += "• Удалите несуществующие аккаунты из базы\n"
                if "🔐 Требуется верификация" in error_groups or "📧 Ошибка верификации" in error_groups:
                    final_report += "• Пройдите верификацию для проблемных аккаунтов\n"
                if "🌐 Проблема с прокси" in error_groups:
                    final_report += "• Проверьте работоспособность прокси\n"
                final_report += "• Попробуйте прогреть проблемные аккаунты по одному"
            
            return final_report
        
        # Запускаем и получаем результат
        final_report = loop.run_until_complete(run_all_warmups())
        
        # Отправляем финальный отчет
        try:
            context.bot.edit_message_text(
                chat_id=query.message.chat_id,
                message_id=query.message.message_id,
                text=final_report,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔥 Новый прогрев", callback_data="smart_warm_menu")],
                    [InlineKeyboardButton("📱 Главное меню", callback_data="main_menu")]
                ])
            )
        except Exception as e:
            logger.error(f"Ошибка обновления сообщения: {e}")
    
    # Запускаем в отдельном потоке
    import threading
    thread = threading.Thread(target=run_multi_warmup)
    thread.start()
    
    return ConversationHandler.END

def handle_warmup_confirmation(update: Update, context: CallbackContext):
    """Обработчик подтверждения выбора аккаунтов для прогрева"""
    selection_data = context.user_data.get('account_selection', {})
    action_type = selection_data.get('action_type')
    selected_accounts = selection_data.get('selected_accounts', [])
    
    if not selected_accounts:
        update.callback_query.edit_message_text("❌ Не выбрано ни одного аккаунта")
        return ConversationHandler.END
    
    # Направляем на правильный handler в зависимости от действия
    if action_type == "smart_warm":
        return handle_smart_warm_action(update, context, selected_accounts)
    elif action_type == "show_limits":
        return handle_show_limits_action(update, context, selected_accounts)
    
    update.callback_query.edit_message_text("❌ Неизвестное действие")
    return ConversationHandler.END

def handle_smart_warm_action(update: Update, context: CallbackContext, account_ids):
    """Обработать прогрев выбранных аккаунтов"""
    if len(account_ids) == 1:
        # Один аккаунт - показываем детали как раньше
        account_id = account_ids[0]
        account = get_instagram_account(account_id)
        
        if not account:
            update.callback_query.edit_message_text("❌ Аккаунт не найден")
            return ConversationHandler.END
        
        # Проверяем статус аккаунта
        status = automation_service.get_account_status(account_id)
        
        # Определяем текущий временной паттерн
        from services.advanced_warmup import advanced_warmup
        current_pattern = advanced_warmup.determine_time_pattern()
        
        message = f"🔥 *Прогрев аккаунта @{account.username}*\n\n"
        message += f"📊 Текущее состояние:\n"
        message += f"├ Здоровье: {status['health_score']}/100\n"
        message += f"├ Риск бана: {status['ban_risk_score']}/100\n"
        message += f"└ Статус: {status['status']}\n\n"
        
        # Информация о временном паттерне
        if current_pattern:
            intensity_percent = int(current_pattern['intensity'] * 100)
            message += f"⏰ Текущий паттерн: {current_pattern['name']}\n"
            message += f"├ Интенсивность: {intensity_percent}%\n"
            message += f"└ Длительность: {current_pattern['duration']}\n\n"
        
        message += "🚀 Новые возможности Advanced Warmup 2.0:\n"
        message += "• 💾 Сохранение постов в коллекции\n"
        message += "• 📱 Просмотр Reels с зацикливанием\n"
        message += "• 🔔 Проверка уведомлений\n"
        message += "• 📍 Поиск локаций\n"
        message += "• 👆 UI взаимодействия (долгое нажатие)\n"
        message += "• 💫 Случайные человеческие ошибки\n\n"
        
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("⚡ Быстрый прогрев (15 мин)", callback_data=f"start_warm_quick_{account_id}")],
            [InlineKeyboardButton("🧠 Умный прогрев (30 мин)", callback_data=f"start_warm_smart_{account_id}")],
            [InlineKeyboardButton("🎯 Прогрев по интересам", callback_data=f"warm_interests_{account_id}")],
            [InlineKeyboardButton("❌ Отмена", callback_data="cancel_warm")]
        ])
        
        update.callback_query.edit_message_text(message, reply_markup=keyboard)
    else:
        # Множественный выбор - показываем краткую информацию
        message = f"🔥 Выбрано {len(account_ids)} аккаунтов для прогрева\n\n"
        message += f"⚡ Будет запущен быстрый прогрев для {len(account_ids)} выбранных аккаунтов\n\n"
        message += "📝 Детали:\n"
        message += "• Длительность: 15 минут на каждый аккаунт\n"
        message += "• Режим: одновременный прогрев\n"
        message += "• Безопасные параметры для всех возрастов аккаунтов"
        
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton(f"🚀 Запустить прогрев {len(account_ids)} аккаунтов", callback_data=f"start_warm_multi_{','.join(map(str, account_ids))}")],
            [InlineKeyboardButton("❌ Отмена", callback_data="cancel_warm")]
        ])
        
        update.callback_query.edit_message_text(message, reply_markup=keyboard)
    
    return ConversationHandler.END

def handle_show_limits_action(update: Update, context: CallbackContext, account_ids):
    """Показать лимиты для выбранных аккаунтов"""
    if len(account_ids) == 1:
        account_id = account_ids[0]
        account = get_instagram_account(account_id)
        
        if not account:
            update.callback_query.edit_message_text("❌ Аккаунт не найден")
            return ConversationHandler.END
        
        # Получаем лимиты аккаунта
        from services.rate_limiter import ActionType
        
        message = f"📊 *Лимиты для @{account.username}*\n\n"
        
        # Показываем текущие лимиты по действиям
        actions = [
            (ActionType.LIKE, "👍 Лайки"),
            (ActionType.FOLLOW, "➕ Подписки"),
            (ActionType.UNFOLLOW, "➖ Отписки"),
            (ActionType.COMMENT, "💬 Комментарии"),
            (ActionType.STORY_VIEW, "👁 Просм. историй"),
            (ActionType.DM_SEND, "📩 Сообщения")
        ]
        
        for action_type, name in actions:
            can_perform = rate_limiter.can_perform_action(account_id, action_type)
            if can_perform:
                message += f"✅ {name}: Доступно\n"
            else:
                wait_time = rate_limiter.get_wait_time(account_id, action_type)
                message += f"⏳ {name}: Ждать {wait_time//60} мин\n"
        
        message += f"\n💡 Аккаунт создан: {account.created_at.strftime('%d.%m.%Y') if account.created_at else 'Неизвестно'}"
        
        update.callback_query.edit_message_text(message, parse_mode='Markdown')
    else:
        # Множественный выбор - показываем общую статистику
        message = f"📊 *Лимиты для {len(account_ids)} аккаунтов*\n\n"
        message += "Показана краткая сводка по всем выбранным аккаунтам."
        
        update.callback_query.edit_message_text(message, parse_mode='Markdown')
    
    return ConversationHandler.END

def status_command(update: Update, context: CallbackContext):
    """Команда /status - показать статус всех аккаунтов"""
    user_id = update.effective_user.id
    
    # Отправляем сообщение о загрузке
    loading_msg = update.message.reply_text("⏳ Анализирую аккаунты...")
    
    try:
        recommendations = automation_service.get_daily_recommendations()
        
        if not recommendations:
            loading_msg.edit_text("📭 Нет активных аккаунтов для отображения статуса.")
            return
        
        # Формируем сообщение
        message = "📊 *СТАТУС АККАУНТОВ*\n\n"
        
        for account_id, data in recommendations.items():
            status_emoji = {
                "EXCELLENT": "🟢",
                "GOOD": "🟡", 
                "NEEDS_ATTENTION": "🟠",
                "UNHEALTHY": "🔴",
                "HIGH_RISK": "⚠️",
                "CRITICAL_RISK": "🚨"
            }.get(data['status'], "❓")
            
            message += f"{status_emoji} *@{data['username']}*\n"
            message += f"├ Здоровье: {data['health_score']}/100\n"
            message += f"├ Риск бана: {data['ban_risk']}/100\n"
            message += f"└ Статус: {data['status']}\n\n"
            
            # Добавляем рекомендации
            if data['actions']:
                message += "💡 *Рекомендации:*\n"
                for action in data['actions'][:3]:  # Максимум 3 рекомендации
                    message += f"  • {action}\n"
                message += "\n"
            
            message += "─" * 30 + "\n\n"
        
        # Добавляем кнопки действий
        keyboard = [
            [InlineKeyboardButton("🔥 Прогреть аккаунты", callback_data="smart_warm_menu")],
            [InlineKeyboardButton("📈 Детальная аналитика", callback_data="detailed_analytics")],
            [InlineKeyboardButton("🔄 Обновить", callback_data="refresh_status")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        loading_msg.edit_text(
            message[:4096],  # Telegram limit
            parse_mode='Markdown',
            reply_markup=reply_markup
        )
        
    except Exception as e:
        logger.error(f"Ошибка в status_command: {e}")
        loading_msg.edit_text(f"❌ Ошибка получения статуса: {str(e)}")

def smart_warm_command(update: Update, context: CallbackContext):
    """Команда /smart_warm - умный прогрев аккаунтов"""
    user_id = update.effective_user.id
    
    def on_accounts_selected(account_ids, update, context):
        return handle_smart_warm_action(update, context, account_ids)
    
    # Используем AccountSelector для выбора аккаунта
    return warmup_selector.start_selection(update, context, on_accounts_selected)

@async_handler(loading_text="⏳ Проверяю состояние аккаунта...")
def warm_account_callback(update: Update, context: CallbackContext):
    """Обработчик выбора аккаунта для прогрева"""
    query = update.callback_query
    
    # Получаем ID аккаунта из callback_data
    account_id = int(query.data.replace("warm_account_", ""))
    account = get_instagram_account(account_id)
    
    if not account:
        query.edit_message_text("❌ Аккаунт не найден")
        return
    
    # Проверяем статус аккаунта
    status = automation_service.get_account_status(account_id)
    
    # Определяем текущий временной паттерн
    from services.advanced_warmup import advanced_warmup
    current_pattern = advanced_warmup.determine_time_pattern()
    
    message = f"🔥 *Прогрев аккаунта @{account.username}*\n\n"
    message += f"📊 Текущее состояние:\n"
    message += f"├ Здоровье: {status['health_score']}/100\n"
    message += f"├ Риск бана: {status['ban_risk_score']}/100\n"
    message += f"└ Статус: {status['status']}\n\n"
    
    # Информация о временном паттерне
    if current_pattern:
        intensity_percent = int(current_pattern['intensity'] * 100)
        message += f"⏰ *Временной паттерн:*\n"
        message += f"├ Время суток: {'Утро' if 6 <= datetime.now().hour < 9 else 'День' if 9 <= datetime.now().hour < 18 else 'Вечер' if 18 <= datetime.now().hour < 22 else 'Ночь'}\n"
        message += f"├ Интенсивность: {intensity_percent}%\n"
        if datetime.now().weekday() in [5, 6]:
            message += f"└ Выходной день (+30%)\n"
        message += "\n"
    
    if not status['can_warm']:
        message += "❌ *Прогрев невозможен!*\n"
        message += "Причина: " + status['status']
        query.edit_message_text(message, parse_mode='Markdown')
        return
    
    message += "✨ *Новые функции прогрева:*\n"
    message += "• Автоадаптация по времени суток\n"
    message += "• Просмотр Reels с зацикливанием\n"
    message += "• Сохранение в коллекции\n"
    message += "• Проверка уведомлений\n"
    message += "• Имитация ошибок пользователя\n\n"
    
    # Предлагаем варианты прогрева
    keyboard = [
        [InlineKeyboardButton("⚡ Быстрый (15 мин)", callback_data=f"start_warm_{account_id}_15")],
        [InlineKeyboardButton("🔥 Стандартный (30 мин)", callback_data=f"start_warm_{account_id}_30")],
        [InlineKeyboardButton("💪 Интенсивный (60 мин)", callback_data=f"start_warm_{account_id}_60")],
        [InlineKeyboardButton("🎯 С интересами", callback_data=f"warm_interests_{account_id}")],
        [InlineKeyboardButton("❌ Отмена", callback_data="cancel_warm")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    query.edit_message_text(message, parse_mode='Markdown', reply_markup=reply_markup)

def start_warm_callback(update: Update, context: CallbackContext) -> None:
    """Запуск прогрева с выбранными параметрами"""
    query = update.callback_query
    user_id = query.from_user.id
    
    # Парсим callback_data
    parts = query.data.split("_")
    
    # Проверяем тип прогрева
    if parts[2] == "multi":
        # Множественный прогрев: start_warm_multi_1,2,3
        return handle_multi_warmup(update, context, parts[3])
    elif parts[2] in ["quick", "smart"]:
        # Новый формат: start_warm_quick_123 или start_warm_smart_123
        account_id = int(parts[3])
        duration = 15 if parts[2] == "quick" else 30
    else:
        # Старый формат: start_warm_123_15
        account_id = int(parts[2])
        duration = int(parts[3])
    
    # Получаем интересы из контекста (если были выбраны)
    interests = context.user_data.get(f'warm_interests_{account_id}', [])
    
    # Обновляем сообщение с начальным статусом
    query.edit_message_text(
        text=f"🚀 *Прогрев запущен!*\n\n"
             f"�� Аккаунт: @{account.username}\n"
             f"⏱ Длительность: {duration} минут\n\n"
             f"_Прогрев выполняется..._",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("⏹ Остановить прогрев", callback_data=f"stop_warm_{account_id}")],
            [InlineKeyboardButton("📱 Главное меню", callback_data="main_menu")]
        ])
    )
    
    # Запускаем прогрев в отдельном потоке
    def run_warmup():
        # Добавляем в активные прогревы
        active_warmups[f"{user_id}_{account_id}"] = True
        
        try:
            # Создаем специальную версию advanced_warmup с проверкой флага
            from services.advanced_warmup import AdvancedWarmupService
            
            # Создаем экземпляр с callback для проверки остановки
            def check_stop():
                return not active_warmups.get(f"{user_id}_{account_id}", False)
            
            advanced_warmup = AdvancedWarmupService()
            advanced_warmup.stop_callback = check_stop
            
            import asyncio
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            success, report = loop.run_until_complete(
                advanced_warmup.start_warmup(
                    account_id=account_id,
                    duration_minutes=duration,
                    interests=interests
                )
            )
            
            # Убираем из активных
            active_warmups.pop(f"{user_id}_{account_id}", None)
            
            # Формируем финальное сообщение
            if success:
                final_text = f"✅ *Прогрев завершен успешно!*\n\n{report}"
            else:
                final_text = f"⚠️ *Прогрев завершен с ошибками*\n\n{report}"
            
            # Обновляем сообщение
            context.bot.edit_message_text(
                chat_id=query.message.chat_id,
                message_id=query.message.message_id,
                text=final_text,
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔥 Новый прогрев", callback_data="smart_warm_menu")],
                    [InlineKeyboardButton("📱 Главное меню", callback_data="main_menu")]
                ])
            )
        except Exception as e:
            logger.error(f"Ошибка прогрева: {e}")
            active_warmups.pop(f"{user_id}_{account_id}", None)
            
            context.bot.edit_message_text(
                chat_id=query.message.chat_id,
                message_id=query.message.message_id,
                text=f"❌ *Ошибка прогрева*\n\n{str(e)}",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔥 Попробовать снова", callback_data="smart_warm_menu")],
                    [InlineKeyboardButton("📱 Главное меню", callback_data="main_menu")]
                ])
            )
    
    # Запускаем в отдельном потоке
    import threading
    warmup_thread = threading.Thread(target=run_warmup)
    warmup_thread.start()
    
    return ConversationHandler.END

def warm_interests_callback(update: Update, context: CallbackContext):
    """Выбор интересов для прогрева"""
    query = update.callback_query
    account_id = int(query.data.split("_")[2])
    
    # Популярные интересы
    interests = [
        "travel", "food", "fitness", "fashion", "art",
        "photography", "nature", "technology", "music", "sports",
        "beauty", "lifestyle", "business", "motivation", "cars"
    ]
    
    message = "🎯 *Выберите интересы для прогрева:*\n\n"
    message += "Выберите 3-5 интересов, которые соответствуют тематике аккаунта:\n"
    
    keyboard = []
    for i in range(0, len(interests), 3):
        row = []
        for j in range(3):
            if i + j < len(interests):
                interest = interests[i + j]
                row.append(InlineKeyboardButton(interest.title(), callback_data=f"add_interest_{account_id}_{interest}"))
        keyboard.append(row)
    
    keyboard.append([InlineKeyboardButton("✅ Готово", callback_data=f"done_interests_{account_id}")])
    keyboard.append([InlineKeyboardButton("❌ Отмена", callback_data="cancel_warm")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    query.edit_message_text(message, parse_mode='Markdown', reply_markup=reply_markup)

def add_interest_callback(update: Update, context: CallbackContext):
    """Добавление интереса"""
    query = update.callback_query
    parts = query.data.split("_")
    account_id = int(parts[2])
    interest = parts[3]
    
    # Инициализируем список интересов
    key = f'warm_interests_{account_id}'
    if key not in context.user_data:
        context.user_data[key] = []
    
    # Добавляем/удаляем интерес
    if interest in context.user_data[key]:
        context.user_data[key].remove(interest)
    else:
        if len(context.user_data[key]) < 5:
            context.user_data[key].append(interest)
    
    # Обновляем сообщение
    warm_interests_callback(update, context)
    query.answer(f"Выбрано: {len(context.user_data[key])}/5")

def limits_command(update: Update, context: CallbackContext):
    """Команда /limits - показать текущие лимиты"""
    user_id = update.effective_user.id
    
    def on_accounts_selected(account_ids, update, context):
        return handle_show_limits_action(update, context, account_ids)
    
    # Используем AccountSelector для выбора аккаунта
    return warmup_selector.start_selection(update, context, on_accounts_selected)

@async_handler(loading_text="⏳ Загружаю статистику лимитов...")
def show_limits_callback(update: Update, context: CallbackContext):
    """Показать лимиты для аккаунта"""
    query = update.callback_query
    
    account_id = int(query.data.replace("show_limits_", ""))
    account = get_instagram_account(account_id)
    
    if not account:
        query.edit_message_text("❌ Аккаунт не найден")
        return
    
    # Получаем статистику действий
    stats = rate_limiter.get_action_stats(account_id)
    
    message = f"📊 *Лимиты для @{account.username}*\n\n"
    
    # Определяем возраст аккаунта
    age_days = rate_limiter._get_account_age_days(account_id)
    if age_days < 7:
        message += "🆕 *Статус: Новый аккаунт*\n"
        limits_type = "new"
    elif age_days < 30:
        message += "📈 *Статус: Прогреваемый аккаунт*\n"
        limits_type = "warming"
    else:
        message += "✅ *Статус: Прогретый аккаунт*\n"
        limits_type = "warmed"
    
    message += f"📅 Возраст: {age_days} дней\n\n"
    
    # Показываем использованные лимиты
    message += "*Использовано за час:*\n"
    for action, count in stats['hourly'].items():
        message += f"• {action}: {count}\n"
    
    message += "\n*Использовано за день:*\n"
    for action, count in stats['daily'].items():
        message += f"• {action}: {count}\n"
    
    query.edit_message_text(message, parse_mode='Markdown')

def done_interests_callback(update: Update, context: CallbackContext):
    """Завершение выбора интересов"""
    query = update.callback_query
    account_id = int(query.data.split("_")[2])
    
    interests = context.user_data.get(f'warm_interests_{account_id}', [])
    
    if not interests:
        query.answer("Выберите хотя бы один интерес!")
        return
    
    # Показываем варианты прогрева с выбранными интересами
    message = f"🎯 *Выбранные интересы:* {', '.join(interests)}\n\n"
    message += "Выберите длительность прогрева:"
    
    keyboard = [
        [InlineKeyboardButton("⚡ Быстрый (15 мин)", callback_data=f"start_warm_{account_id}_15")],
        [InlineKeyboardButton("🔥 Стандартный (30 мин)", callback_data=f"start_warm_{account_id}_30")],
        [InlineKeyboardButton("💪 Интенсивный (60 мин)", callback_data=f"start_warm_{account_id}_60")],
        [InlineKeyboardButton("❌ Отмена", callback_data="cancel_warm")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    query.edit_message_text(message, parse_mode='Markdown', reply_markup=reply_markup)

def stop_warm_callback(update: Update, context: CallbackContext) -> None:
    """Остановка активного прогрева"""
    query = update.callback_query
    user_id = query.from_user.id
    
    # Парсим account_id из callback_data
    account_id = int(query.data.split('_')[2])
    
    # Устанавливаем флаг остановки
    warmup_key = f"{user_id}_{account_id}"
    if warmup_key in active_warmups:
        active_warmups[warmup_key] = False
        
        query.edit_message_text(
            text="⏹ *Прогрев остановлен*\n\n"
                 "Процесс прогрева был остановлен пользователем.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔥 Новый прогрев", callback_data="smart_warm_menu")],
                [InlineKeyboardButton("📱 Главное меню", callback_data="main_menu")]
            ])
        )
    else:
        query.edit_message_text(
            text="ℹ️ *Прогрев уже завершен*\n\n"
                 "Этот прогрев уже был завершен или остановлен.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔥 Новый прогрев", callback_data="smart_warm_menu")],
                [InlineKeyboardButton("📱 Главное меню", callback_data="main_menu")]
            ])
        )

def stop_all_warm_callback(update: Update, context: CallbackContext) -> None:
    """Остановка всех активных прогревов"""
    query = update.callback_query
    user_id = query.from_user.id
    
    # Парсим account_ids из callback_data
    account_ids_str = query.data.split('_')[3]
    account_ids = [int(id_) for id_ in account_ids_str.split(',')]
    
    # Останавливаем все прогревы
    stopped_count = 0
    for account_id in account_ids:
        warmup_key = f"{user_id}_{account_id}"
        if warmup_key in active_warmups:
            active_warmups[warmup_key] = False
            stopped_count += 1
    
    query.edit_message_text(
        text=f"⏹ *Остановлено {stopped_count} прогревов*\n\n"
             f"Все активные процессы прогрева были остановлены.",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔥 Новый прогрев", callback_data="smart_warm_menu")],
            [InlineKeyboardButton("📱 Главное меню", callback_data="main_menu")]
        ])
    )

def register_automation_handlers(dispatcher):
    """Регистрация обработчиков автоматизации"""
    # Команды
    dispatcher.add_handler(CommandHandler("status", status_command))
    
    # ConversationHandler для селектора аккаунтов прогрева
    warmup_conversation = warmup_selector.get_conversation_handler()
    
    # Добавляем дополнительные entry points для новых команд
    original_entry_points = warmup_conversation.entry_points
    additional_entry_points = [
        CallbackQueryHandler(smart_warm_command, pattern="^smart_warm_menu$"),
        CallbackQueryHandler(limits_command, pattern="^limits$")
    ]
    warmup_conversation.entry_points.extend(additional_entry_points)
    
    dispatcher.add_handler(warmup_conversation)
    
    # Callback handlers для действий после выбора аккаунтов  
    dispatcher.add_handler(CallbackQueryHandler(start_warm_callback, pattern="^start_warm_"))
    dispatcher.add_handler(CallbackQueryHandler(warm_interests_callback, pattern="^warm_interests_"))
    dispatcher.add_handler(CallbackQueryHandler(add_interest_callback, pattern="^add_interest_"))
    dispatcher.add_handler(CallbackQueryHandler(done_interests_callback, pattern="^done_interests_"))
    dispatcher.add_handler(CallbackQueryHandler(lambda u, c: u.callback_query.edit_message_text("❌ Отменено"), pattern="^cancel_warm$")) 
    dispatcher.add_handler(CallbackQueryHandler(stop_warm_callback, pattern=r'^stop_warm_')) 
    dispatcher.add_handler(CallbackQueryHandler(stop_all_warm_callback, pattern=r'^stop_all_warm_')) 
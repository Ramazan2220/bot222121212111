"""
Универсальный модуль для выбора аккаунтов с поддержкой папок
"""
import logging
from typing import List, Optional, Callable, Dict, Any
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ParseMode
from telegram.ext import CallbackContext, ConversationHandler, CallbackQueryHandler

# Импортируем наши асинхронные утилиты
from telegram_bot.utils.async_handlers import async_handler, answer_callback_async

from database.db_manager import (
    get_instagram_accounts, get_account_groups, 
    get_accounts_in_group, get_accounts_without_group
)
from database.safe_user_wrapper import get_user_instagram_accounts, extract_user_id_from_update

logger = logging.getLogger(__name__)

# Состояния для выбора
SELECTING_SOURCE = 1
SELECTING_FOLDER = 2
SELECTING_ACCOUNTS = 3

# Временное хранилище для процесса выбора
selection_data: Dict[int, Dict[str, Any]] = {}


class AccountSelector:
    """Класс для управления выбором аккаунтов"""
    
    def __init__(self, 
                 callback_prefix: str,
                 title: str = "Выберите аккаунты",
                 allow_multiple: bool = True,
                 show_status: bool = True,
                 show_folders: bool = True,
                 back_callback: str = "main_menu"):
        """
        Инициализация селектора аккаунтов
        
        Args:
            callback_prefix: Префикс для callback_data
            title: Заголовок для выбора
            allow_multiple: Разрешить множественный выбор
            show_status: Показывать статус аккаунтов (активен/неактивен)
            show_folders: Показывать опцию выбора по папкам
            back_callback: Callback для кнопки "Назад"
        """
        self.callback_prefix = callback_prefix
        self.title = title
        self.allow_multiple = allow_multiple
        self.show_status = show_status
        self.show_folders = show_folders
        self.back_callback = back_callback
    
    def start_selection(self, update: Update, context: CallbackContext, 
                       on_complete: Callable[[List[int], Update, CallbackContext], None]) -> int:
        """Начинает процесс выбора аккаунтов"""
        logger.info(f"AccountSelector.start_selection called with prefix: {self.callback_prefix}")
        
        query = update.callback_query
        if query:
            query.answer()
            logger.info(f"Callback query data: {query.data}")
        
        user_id = update.effective_user.id
        
        # Инициализируем данные выбора
        selection_data[user_id] = {
            'prefix': self.callback_prefix,
            'selected_accounts': [],
            'on_complete': on_complete,
            'allow_multiple': self.allow_multiple,
            'show_status': self.show_status,
            'title': self.title,
            'back_callback': self.back_callback
        }
        
        logger.info(f"Selection data initialized for user {user_id}")
        
        # Показываем меню выбора источника
        keyboard = []
        
        if self.show_folders:
            folders = get_account_groups()
            if folders:
                keyboard.append([InlineKeyboardButton("📁 Выбрать из папки", 
                                                    callback_data=f"{self.callback_prefix}_source_folder")])
        
        keyboard.append([InlineKeyboardButton("📋 Все аккаунты", 
                                            callback_data=f"{self.callback_prefix}_source_all")])
        keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data=self.back_callback)])
        
        text = f"{self.title}\n\nВыберите источник аккаунтов:"
        
        if query:
            try:
                query.edit_message_text(text, 
                                      reply_markup=InlineKeyboardMarkup(keyboard),
                                      parse_mode=None)
            except Exception as e:
                logger.error(f"Error editing message: {e}")
                # Пробуем отправить новое сообщение
                query.message.reply_text(text, 
                                       reply_markup=InlineKeyboardMarkup(keyboard),
                                       parse_mode=None)
        else:
            update.message.reply_text(text, 
                                    reply_markup=InlineKeyboardMarkup(keyboard),
                                    parse_mode=None)
        
        return SELECTING_SOURCE
    
    def handle_source_selection(self, update: Update, context: CallbackContext) -> int:
        """Обрабатывает выбор источника аккаунтов"""
        query = update.callback_query
        
        user_id = query.from_user.id
        
        # Если данные не инициализированы, инициализируем их
        if user_id not in selection_data:
            selection_data[user_id] = {
                'prefix': self.callback_prefix,
                'selected_accounts': [],
                'on_complete': None,  # Будет установлено позже
                'allow_multiple': self.allow_multiple,
                'show_status': self.show_status,
                'title': self.title,
                'back_callback': self.back_callback
            }
        else:
            # Обновляем только недостающие поля, сохраняя существующие данные
            data = selection_data[user_id]
            if 'allow_multiple' not in data:
                data['allow_multiple'] = self.allow_multiple
            if 'show_status' not in data:
                data['show_status'] = self.show_status
            if 'title' not in data:
                data['title'] = self.title
            if 'back_callback' not in data:
                data['back_callback'] = self.back_callback
        
        data = selection_data[user_id]
        
        if query.data.endswith("_source_folder"):
            # Показываем список папок
            folders = get_account_groups()
            
            if not folders:
                try:
                    query.edit_message_text(
                        "📂 Папки не найдены. Создайте папки в разделе управления аккаунтами.",
                        reply_markup=InlineKeyboardMarkup([
                            [InlineKeyboardButton("🔙 Назад", 
                                                callback_data=f"{data['prefix']}_start")]
                        ]),
                        parse_mode=None
                    )
                except Exception as e:
                    logger.error(f"Error editing message: {e}")
                    query.message.reply_text(
                        "📂 Папки не найдены. Создайте папки в разделе управления аккаунтами.",
                        reply_markup=InlineKeyboardMarkup([
                            [InlineKeyboardButton("🔙 Назад", 
                                                callback_data=f"{data['prefix']}_start")]
                        ]),
                        parse_mode=None
                    )
                return SELECTING_SOURCE
            
            keyboard = []
            for folder in folders:
                accounts_count = len(get_accounts_in_group(folder.id))
                button_text = f"{folder.icon} {folder.name} ({accounts_count} акк.)"
                keyboard.append([InlineKeyboardButton(button_text, 
                                                    callback_data=f"{data['prefix']}_folder_{folder.id}")])
            
            keyboard.append([InlineKeyboardButton("🔙 Назад", 
                                                callback_data=f"{data['prefix']}_start")])
            
            try:
                query.edit_message_text(
                    f"{data['title']}\n\nВыберите папку:",
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode=None
                )
            except Exception as e:
                logger.error(f"Error editing message: {e}")
                query.message.reply_text(
                    f"{data['title']}\n\nВыберите папку:",
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode=None
                )
            
            return SELECTING_FOLDER
        
        elif query.data.endswith("_source_all"):
            # Показываем все аккаунты
            user_id = query.from_user.id
            return self._show_accounts_list(update, context, get_user_instagram_accounts(context=context, user_id=user_id))
    
    def handle_folder_selection(self, update: Update, context: CallbackContext) -> int:
        """Обрабатывает выбор папки"""
        query = update.callback_query
        
        user_id = query.from_user.id
        data = selection_data.get(user_id, {})
        
        # Извлекаем ID папки
        folder_id = int(query.data.split("_")[-1])
        accounts = get_accounts_in_group(folder_id)
        
        if not accounts:
            try:
                query.edit_message_text(
                    "📂 В этой папке нет аккаунтов.",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🔙 Назад", 
                                            callback_data=f"{data['prefix']}_source_folder")]
                    ]),
                    parse_mode=None
                )
            except Exception as e:
                logger.error(f"Error editing message: {e}")
                query.message.reply_text(
                    "📂 В этой папке нет аккаунтов.",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🔙 Назад", 
                                            callback_data=f"{data['prefix']}_source_folder")]
                    ]),
                    parse_mode=None
                )
            return SELECTING_FOLDER
        
        return self._show_accounts_list(update, context, accounts)
    
    def _show_accounts_list(self, update: Update, context: CallbackContext, 
                           accounts: List[Any], page: int = 1) -> int:
        """Показывает список аккаунтов для выбора"""
        query = update.callback_query
        user_id = query.from_user.id
        data = selection_data.get(user_id, {})
        
        # Сохраняем список аккаунтов для последующего использования
        data['current_accounts'] = accounts
        data['current_page'] = page
        
        if not accounts:
            try:
                query.edit_message_text(
                    "❌ Нет доступных аккаунтов.",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🔙 Назад", callback_data=data['back_callback'])]
                    ]),
                    parse_mode=None
                )
            except Exception as e:
                logger.error(f"Error editing message: {e}")
                query.message.reply_text(
                    "❌ Нет доступных аккаунтов.",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🔙 Назад", callback_data=data['back_callback'])]
                    ]),
                    parse_mode=None
                )
            return ConversationHandler.END
        
        # Пагинация
        accounts_per_page = 8
        total_pages = (len(accounts) + accounts_per_page - 1) // accounts_per_page
        start_idx = (page - 1) * accounts_per_page
        end_idx = min(start_idx + accounts_per_page, len(accounts))
        
        keyboard = []
        selected = data.get('selected_accounts', [])
        
        # Кнопки для множественного выбора
        if data['allow_multiple']:
            logger.info(f"Showing multiple selection buttons for prefix: {data['prefix']}")
            select_buttons = []
            # Проверяем, все ли аккаунты на текущей странице выбраны
            current_page_accounts = accounts[start_idx:end_idx]
            all_selected = all(acc.id in selected for acc in current_page_accounts)
            
            if all_selected:
                select_buttons.append(InlineKeyboardButton("☐ Снять выбор со всех", 
                                                         callback_data=f"{data['prefix']}_deselect_all"))
            else:
                select_buttons.append(InlineKeyboardButton("☑️ Выбрать все", 
                                                         callback_data=f"{data['prefix']}_select_all"))
            
            keyboard.append(select_buttons)
        else:
            logger.info(f"Single selection mode for prefix: {data['prefix']}")
        
        # Аккаунты на текущей странице
        for account in accounts[start_idx:end_idx]:
            if data['show_status']:
                status = "✅" if account.is_active else "❌"
            else:
                status = ""
            
            if data['allow_multiple']:
                check = "☑️" if account.id in selected else "☐"
                button_text = f"{check} {status} {account.username}"
            else:
                button_text = f"{status} {account.username}"
            
            keyboard.append([InlineKeyboardButton(button_text, 
                                                callback_data=f"{data['prefix']}_acc_{account.id}")])
        
        # Навигация по страницам
        nav_buttons = []
        if page > 1:
            nav_buttons.append(InlineKeyboardButton("◀️", 
                                                   callback_data=f"{data['prefix']}_page_{page-1}"))
        
        nav_buttons.append(InlineKeyboardButton(f"{page}/{total_pages}", callback_data="noop"))
        
        if page < total_pages:
            nav_buttons.append(InlineKeyboardButton("▶️", 
                                                   callback_data=f"{data['prefix']}_page_{page+1}"))
        
        if nav_buttons:
            keyboard.append(nav_buttons)
        
        # Кнопки действий
        action_buttons = []
        if data['allow_multiple'] and selected:
            action_buttons.append(InlineKeyboardButton(f"✅ Подтвердить ({len(selected)})", 
                                                     callback_data=f"{data['prefix']}_confirm"))
        
        action_buttons.append(InlineKeyboardButton("🔙 Назад", 
                                                 callback_data=f"{data['prefix']}_start"))
        keyboard.append(action_buttons)
        
        text = f"{data['title']}\n\n"
        if data['allow_multiple']:
            text += f"Выберите аккаунты (выбрано: {len(selected)}):"
        else:
            text += "Выберите аккаунт:"
        
        try:
            query.edit_message_text(text, 
                                  reply_markup=InlineKeyboardMarkup(keyboard),
                                  parse_mode=None)
        except Exception as e:
            logger.error(f"Error editing message: {e}")
            # Пробуем отправить новое сообщение
            query.message.reply_text(text, 
                                   reply_markup=InlineKeyboardMarkup(keyboard),
                                   parse_mode=None)
        
        return SELECTING_ACCOUNTS
    
    def handle_account_toggle(self, update: Update, context: CallbackContext) -> int:
        """Обрабатывает выбор/отмену выбора аккаунта"""
        query = update.callback_query
        
        user_id = query.from_user.id
        data = selection_data.get(user_id, {})
        
        # Извлекаем ID аккаунта
        account_id = int(query.data.split("_")[-1])
        
        if data['allow_multiple']:
            # Множественный выбор
            selected = data.get('selected_accounts', [])
            if account_id in selected:
                selected.remove(account_id)
            else:
                selected.append(account_id)
            data['selected_accounts'] = selected
            
            # Обновляем список с текущими аккаунтами и страницей
            accounts = data.get('current_accounts', get_user_instagram_accounts(context=context, user_id=user_id))
            page = data.get('current_page', 1)
            return self._show_accounts_list(update, context, accounts, page)
        else:
            # Одиночный выбор - сразу завершаем
            on_complete = data.get('on_complete')
            
            # Для профилей используем специальный callback
            if self.callback_prefix == "profile_select" and not on_complete:
                from telegram_bot.handlers.profile_handlers import PROFILE_ACCOUNT_CALLBACK
                on_complete = PROFILE_ACCOUNT_CALLBACK
            
            if on_complete:
                # Очищаем данные
                if user_id in selection_data:
                    del selection_data[user_id]
                
                # Вызываем callback
                on_complete([account_id], update, context)
                return ConversationHandler.END
    
    def handle_select_all(self, update: Update, context: CallbackContext) -> int:
        """Выбирает все аккаунты на текущей странице"""
        query = update.callback_query
        query.answer("Все аккаунты выбраны")
        
        user_id = query.from_user.id
        data = selection_data.get(user_id, {})
        
        # Получаем текущие аккаунты и страницу
        accounts = data.get('current_accounts', [])
        page = data.get('current_page', 1)
        
        # Выбираем все аккаунты
        selected = data.get('selected_accounts', [])
        for account in accounts:
            if account.id not in selected:
                selected.append(account.id)
        data['selected_accounts'] = selected
        
        # Обновляем список
        return self._show_accounts_list(update, context, accounts, page)
    
    def handle_deselect_all(self, update: Update, context: CallbackContext) -> int:
        """Снимает выбор со всех аккаунтов"""
        query = update.callback_query
        answer_callback_async(query, "Выбор снят со всех аккаунтов")
        
        user_id = query.from_user.id
        data = selection_data.get(user_id, {})
        
        # Очищаем выбор
        data['selected_accounts'] = []
        
        # Получаем текущие аккаунты и страницу
        accounts = data.get('current_accounts', [])
        page = data.get('current_page', 1)
        
        # Обновляем список
        return self._show_accounts_list(update, context, accounts, page)
    
    def handle_page_navigation(self, update: Update, context: CallbackContext) -> int:
        """Обрабатывает навигацию по страницам"""
        query = update.callback_query
        user_id = query.from_user.id
        data = selection_data.get(user_id, {})
        
        # Извлекаем номер страницы
        try:
            page = int(query.data.split("_")[-1])
        except ValueError:
            answer_callback_async(query, "Ошибка навигации", show_alert=True)
            return SELECTING_ACCOUNTS
        
        accounts = data.get('current_accounts', [])
        return self._show_accounts_list(update, context, accounts, page)
    
    def handle_confirmation(self, update: Update, context: CallbackContext) -> int:
        """Подтверждает выбор аккаунтов"""
        query = update.callback_query
        
        user_id = query.from_user.id
        data = selection_data.get(user_id, {})
        
        selected = data.get('selected_accounts', [])
        on_complete = data.get('on_complete')
        
        # Для профилей используем специальный callback
        if self.callback_prefix == "profile_select" and not on_complete:
            from telegram_bot.handlers.profile_handlers import PROFILE_ACCOUNT_CALLBACK
            on_complete = PROFILE_ACCOUNT_CALLBACK
        
        if selected:
            # Очищаем данные
            if user_id in selection_data:
                del selection_data[user_id]
            
            # Вызываем callback
            if on_complete:
                on_complete(selected, update, context)
            return ConversationHandler.END
        else:
            query.answer("Выберите хотя бы один аккаунт", show_alert=True)
            return SELECTING_ACCOUNTS
    
    def get_conversation_handler(self) -> ConversationHandler:
        """Возвращает ConversationHandler для процесса выбора"""
        
        # Создаем обертку для start_selection с фиксированным on_complete
        def start_selection_wrapper(update, context):
            # Для профилей используем специальный callback
            if self.callback_prefix == "profile_select":
                from telegram_bot.handlers.profile_handlers import PROFILE_ACCOUNT_CALLBACK
                return self.start_selection(update, context, PROFILE_ACCOUNT_CALLBACK)
            else:
                # Для других случаев должен быть установлен on_complete в selection_data
                user_id = update.effective_user.id
                data = selection_data.get(user_id, {})
                on_complete = data.get('on_complete')
                if on_complete:
                    return self.start_selection(update, context, on_complete)
                else:
                    # Если нет callback, просто показываем меню
                    return SELECTING_SOURCE
        
        return ConversationHandler(
            entry_points=[
                CallbackQueryHandler(start_selection_wrapper, 
                                   pattern=f"^{self.callback_prefix}_start$"),
                # Добавляем прямые entry points для source selection
                CallbackQueryHandler(self.handle_source_selection, 
                                   pattern=f"^{self.callback_prefix}_source_")
            ],
            states={
                SELECTING_SOURCE: [
                    CallbackQueryHandler(self.handle_source_selection, 
                                       pattern=f"^{self.callback_prefix}_source_"),
                    CallbackQueryHandler(start_selection_wrapper, 
                                       pattern=f"^{self.callback_prefix}_start$")
                ],
                SELECTING_FOLDER: [
                    CallbackQueryHandler(self.handle_folder_selection, 
                                       pattern=f"^{self.callback_prefix}_folder_"),
                    CallbackQueryHandler(start_selection_wrapper, 
                                       pattern=f"^{self.callback_prefix}_start$")
                ],
                SELECTING_ACCOUNTS: [
                    CallbackQueryHandler(self.handle_account_toggle, 
                                       pattern=f"^{self.callback_prefix}_acc_"),
                    CallbackQueryHandler(self.handle_select_all, 
                                       pattern=f"^{self.callback_prefix}_select_all$"),
                    CallbackQueryHandler(self.handle_deselect_all, 
                                       pattern=f"^{self.callback_prefix}_deselect_all$"),
                    CallbackQueryHandler(self.handle_confirmation, 
                                       pattern=f"^{self.callback_prefix}_confirm$"),
                    CallbackQueryHandler(self.handle_page_navigation, 
                                       pattern=f"^{self.callback_prefix}_page_"),
                    CallbackQueryHandler(start_selection_wrapper, 
                                       pattern=f"^{self.callback_prefix}_start$")
                ]
            },
            fallbacks=[
                CallbackQueryHandler(lambda u, c: ConversationHandler.END, 
                                   pattern=f"^{self.back_callback}$")
            ],
            per_message=False
        )


def create_account_selector(callback_prefix: str, **kwargs) -> AccountSelector:
    """Создает новый экземпляр селектора аккаунтов"""
    return AccountSelector(callback_prefix, **kwargs) 
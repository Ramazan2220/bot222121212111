"""
Главная клавиатура админ бота
"""

from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from typing import List

from ..config.admin_list import has_permission, Permission

def get_main_keyboard(user_id: int):
    """Создает главную клавиатуру с кнопками в зависимости от прав пользователя"""
    keyboard = []
    
    # Основные разделы доступные всем админам
    keyboard.append([
        InlineKeyboardButton("📊 Статистика", callback_data="stats"),
        InlineKeyboardButton("👥 Пользователи", callback_data="users_menu")
    ])
    
    # Аналитика и финансы
    if has_permission(user_id, Permission.VIEW_ANALYTICS):
        keyboard.append([
            InlineKeyboardButton("📈 Аналитика", callback_data="analytics"),
            InlineKeyboardButton("💰 Финансы", callback_data="financial")
        ])
    
    # Системные функции
    if has_permission(user_id, Permission.MANAGE_SYSTEM):
        keyboard.append([
            InlineKeyboardButton("⚙️ Система", callback_data="system"),
            InlineKeyboardButton("🔔 Уведомления", callback_data="notifications")
        ])
    
    # Экспорт данных
    if has_permission(user_id, Permission.EXPORT_DATA):
        keyboard.append([
            InlineKeyboardButton("📊 Экспорт", callback_data="export")
        ])
    
    # Быстрые действия
    keyboard.append([
        InlineKeyboardButton("🔄 Обновить", callback_data="refresh_main"),
        InlineKeyboardButton("❓ Помощь", callback_data="help")
    ])
    
    return InlineKeyboardMarkup(keyboard)

def get_back_to_main_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура с кнопкой возврата в главное меню"""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")]
    ])

def get_confirmation_keyboard(action: str, item_id: str = "") -> InlineKeyboardMarkup:
    """Клавиатура подтверждения действия"""
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Да", callback_data=f"confirm_{action}_{item_id}"),
            InlineKeyboardButton("❌ Нет", callback_data=f"cancel_{action}")
        ],
        [InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")]
    ])

def get_quick_actions_keyboard(user_id: int) -> InlineKeyboardMarkup:
    """Клавиатура быстрых действий"""
    buttons = []
    
    # Быстрый просмотр статистики
    if has_permission(user_id, Permission.VIEW_STATS):
        buttons.append([
            InlineKeyboardButton("📈 Сегодня", callback_data="quick_stats_today"),
            InlineKeyboardButton("📊 Сейчас", callback_data="quick_stats_now")
        ])
    
    # Быстрые действия с пользователями  
    if has_permission(user_id, Permission.VIEW_USERS):
        buttons.append([
            InlineKeyboardButton("👥 Активные", callback_data="quick_users_active"),
            InlineKeyboardButton("⚠️ Проблемы", callback_data="quick_users_problems")
        ])
    
    # Системные действия
    if has_permission(user_id, Permission.VIEW_SYSTEM):
        buttons.append([
            InlineKeyboardButton("🖥️ Серверы", callback_data="quick_system_status"),
            InlineKeyboardButton("🔗 Прокси", callback_data="quick_proxy_status")
        ])
    
    # Экспорт данных
    if has_permission(user_id, Permission.EXPORT_DATA):
        buttons.append([
            InlineKeyboardButton("📄 Экспорт", callback_data="quick_export")
        ])
    
    buttons.append([
        InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")
    ])
    
    return InlineKeyboardMarkup(buttons)

def get_navigation_keyboard(current_page: int, total_pages: int, 
                          callback_prefix: str) -> InlineKeyboardMarkup:
    """Клавиатура навигации по страницам"""
    buttons = []
    
    navigation_row = []
    
    # Кнопка "Предыдущая страница"
    if current_page > 1:
        navigation_row.append(
            InlineKeyboardButton(
                "◀️ Пред", 
                callback_data=f"{callback_prefix}_page_{current_page - 1}"
            )
        )
    
    # Информация о текущей странице
    navigation_row.append(
        InlineKeyboardButton(
            f"📄 {current_page}/{total_pages}",
            callback_data="page_info"
        )
    )
    
    # Кнопка "Следующая страница"
    if current_page < total_pages:
        navigation_row.append(
            InlineKeyboardButton(
                "След ▶️", 
                callback_data=f"{callback_prefix}_page_{current_page + 1}"
            )
        )
    
    if navigation_row:
        buttons.append(navigation_row)
    
    # Кнопки быстрого перехода (если страниц много)
    if total_pages > 5:
        quick_nav = []
        if current_page > 3:
            quick_nav.append(
                InlineKeyboardButton("1", callback_data=f"{callback_prefix}_page_1")
            )
        if current_page < total_pages - 2:
            quick_nav.append(
                InlineKeyboardButton(str(total_pages), callback_data=f"{callback_prefix}_page_{total_pages}")
            )
        if quick_nav:
            buttons.append(quick_nav)
    
    # Кнопка возврата
    buttons.append([
        InlineKeyboardButton("🔙 Назад", callback_data="back"),
        InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")
    ])
    
    return InlineKeyboardMarkup(buttons) 
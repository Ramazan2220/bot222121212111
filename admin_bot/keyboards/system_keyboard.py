"""
Системные клавиатуры
"""

from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from typing import List

def get_system_keyboard(user_id: int) -> InlineKeyboardMarkup:
    """Клавиатура системного управления"""
    buttons = [
        [
            InlineKeyboardButton("🖥️ Серверы", callback_data="system_servers"),
            InlineKeyboardButton("📋 Логи", callback_data="system_logs")
        ],
        [
            InlineKeyboardButton("🔗 Прокси", callback_data="system_proxies"),
            InlineKeyboardButton("🔄 Перезапуск", callback_data="system_restart")
        ],
        [
            InlineKeyboardButton("💾 Бэкап", callback_data="system_backup"),
            InlineKeyboardButton("⚠️ Алерты", callback_data="system_alerts")
        ],
        [
            InlineKeyboardButton("🏠 Главная", callback_data="main_menu")
        ]
    ]
    
    return InlineKeyboardMarkup(buttons)

def get_pagination_keyboard(current_page: int, total_pages: int, 
                           callback_prefix: str) -> InlineKeyboardMarkup:
    """Универсальная клавиатура пагинации"""
    buttons = []
    
    # Навигация по страницам
    nav_buttons = []
    
    if current_page > 1:
        nav_buttons.append(
            InlineKeyboardButton("◀️", callback_data=f"{callback_prefix}_page_{current_page - 1}")
        )
    
    nav_buttons.append(
        InlineKeyboardButton(f"{current_page}/{total_pages}", callback_data="page_info")
    )
    
    if current_page < total_pages:
        nav_buttons.append(
            InlineKeyboardButton("▶️", callback_data=f"{callback_prefix}_page_{current_page + 1}")
        )
    
    if nav_buttons:
        buttons.append(nav_buttons)
    
    # Быстрые переходы
    if total_pages > 3:
        quick_buttons = []
        if current_page > 2:
            quick_buttons.append(
                InlineKeyboardButton("1", callback_data=f"{callback_prefix}_page_1")
            )
        if current_page < total_pages - 1:
            quick_buttons.append(
                InlineKeyboardButton(str(total_pages), callback_data=f"{callback_prefix}_page_{total_pages}")
            )
        if quick_buttons:
            buttons.append(quick_buttons)
    
    return InlineKeyboardMarkup(buttons) 
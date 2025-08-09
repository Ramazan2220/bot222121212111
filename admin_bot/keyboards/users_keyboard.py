"""
Клавиатуры для управления пользователями
"""

from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from typing import List, Dict, Any

def get_users_keyboard(user_id: int) -> InlineKeyboardMarkup:
    """Клавиатура управления пользователями"""
    buttons = [
        [
            InlineKeyboardButton("📋 Список", callback_data="users_list"),
            InlineKeyboardButton("🔍 Поиск", callback_data="users_search")
        ],
        [
            InlineKeyboardButton("➕ Добавить", callback_data="users_add"),
            InlineKeyboardButton("📊 Топ", callback_data="users_top")
        ],
        [
            InlineKeyboardButton("✅ Активные", callback_data="users_active"),
            InlineKeyboardButton("⚠️ Проблемы", callback_data="users_problems")
        ],
        [
            InlineKeyboardButton("🏠 Главная", callback_data="main_menu")
        ]
    ]
    
    return InlineKeyboardMarkup(buttons)

def get_user_actions_keyboard(user_data: Dict[str, Any]) -> InlineKeyboardMarkup:
    """Клавиатура действий с пользователем"""
    user_id = user_data.get('id', 0)
    is_active = user_data.get('is_active', False)
    
    buttons = []
    
    # Основные действия
    buttons.append([
        InlineKeyboardButton("👀 Детали", callback_data=f"user_details_{user_id}"),
        InlineKeyboardButton("📱 Аккаунты", callback_data=f"user_accounts_{user_id}")
    ])
    
    # Действия с аккаунтом
    if is_active:
        buttons.append([
            InlineKeyboardButton("🔒 Заблокировать", callback_data=f"user_block_{user_id}"),
            InlineKeyboardButton("✏️ Редактировать", callback_data=f"user_edit_{user_id}")
        ])
    else:
        buttons.append([
            InlineKeyboardButton("✅ Разблокировать", callback_data=f"user_unblock_{user_id}"),
            InlineKeyboardButton("✏️ Редактировать", callback_data=f"user_edit_{user_id}")
        ])
    
    # Финансы и статистика
    buttons.append([
        InlineKeyboardButton("💰 Платежи", callback_data=f"user_payments_{user_id}"),
        InlineKeyboardButton("📊 Статистика", callback_data=f"user_stats_{user_id}")
    ])
    
    # Опасные действия
    buttons.append([
        InlineKeyboardButton("🗑️ Удалить", callback_data=f"user_delete_confirm_{user_id}")
    ])
    
    # Навигация
    buttons.append([
        InlineKeyboardButton("🔙 К списку", callback_data="users_list"),
        InlineKeyboardButton("🏠 Главная", callback_data="main_menu")
    ])
    
    return InlineKeyboardMarkup(buttons) 
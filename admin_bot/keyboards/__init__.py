"""
Клавиатуры для админ бота
"""

from .main_keyboard import get_main_keyboard, get_back_to_main_keyboard
from .users_keyboard import get_users_keyboard, get_user_actions_keyboard
from .system_keyboard import get_system_keyboard, get_pagination_keyboard

__all__ = [
    'get_main_keyboard', 'get_back_to_main_keyboard',
    'get_users_keyboard', 'get_user_actions_keyboard', 
    'get_system_keyboard', 'get_pagination_keyboard'
] 
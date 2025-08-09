"""
Управление списком админов и их правами доступа
"""

from typing import List, Dict, Set
from enum import Enum

class AdminRole(Enum):
    """Роли админов"""
    SUPER_ADMIN = "super_admin"      # Полные права
    ADMIN = "admin"                  # Управление пользователями
    MODERATOR = "moderator"          # Только просмотр

class Permission(Enum):
    """Права доступа"""
    # Пользователи
    VIEW_USERS = "view_users"
    CREATE_USERS = "create_users"
    EDIT_USERS = "edit_users"
    DELETE_USERS = "delete_users"
    
    # Статистика
    VIEW_STATS = "view_stats"
    EXPORT_DATA = "export_data"
    
    # Система
    VIEW_SYSTEM = "view_system"
    MANAGE_SYSTEM = "manage_system"
    RESTART_SERVICES = "restart_services"
    
    # Финансы
    VIEW_FINANCE = "view_finance"
    MANAGE_FINANCE = "manage_finance"
    
    # Аналитика
    VIEW_ANALYTICS = "view_analytics"
    
    # Управление пользователями
    MANAGE_USERS = "manage_users"
    
    # Админы
    MANAGE_ADMINS = "manage_admins"

# Ваш Telegram ID (замените на свой!)
YOUR_TELEGRAM_ID = 6499246016  # ❗ ВАЖНО: Замените на ваш реальный Telegram ID

# Список супер-админов (полные права)
SUPER_ADMIN_IDS: List[int] = [
    YOUR_TELEGRAM_ID,  # Ваш ID
    # Добавьте другие ID супер-админов здесь
]

# Список обычных админов
ADMIN_IDS: List[int] = [
    # Добавьте ID обычных админов здесь
    # 987654321,
]

# Список модераторов (только просмотр)
MODERATOR_IDS: List[int] = [
    # Добавьте ID модераторов здесь
    # 555666777,
]

# Все админы
ALL_ADMIN_IDS = SUPER_ADMIN_IDS + ADMIN_IDS + MODERATOR_IDS

# Права доступа по ролям
ROLE_PERMISSIONS: Dict[AdminRole, Set[Permission]] = {
    AdminRole.SUPER_ADMIN: {
        # Полные права на все
        Permission.VIEW_USERS, Permission.CREATE_USERS, Permission.EDIT_USERS, Permission.DELETE_USERS,
        Permission.VIEW_STATS, Permission.EXPORT_DATA,
        Permission.VIEW_SYSTEM, Permission.MANAGE_SYSTEM, Permission.RESTART_SERVICES,
        Permission.VIEW_FINANCE, Permission.MANAGE_FINANCE, Permission.VIEW_ANALYTICS,
        Permission.MANAGE_USERS, Permission.MANAGE_ADMINS
    },
    
    AdminRole.ADMIN: {
        # Права на управление пользователями и просмотр статистики
        Permission.VIEW_USERS, Permission.CREATE_USERS, Permission.EDIT_USERS, Permission.MANAGE_USERS,
        Permission.VIEW_STATS, Permission.EXPORT_DATA,
        Permission.VIEW_SYSTEM, Permission.VIEW_FINANCE, Permission.VIEW_ANALYTICS
    },
    
    AdminRole.MODERATOR: {
        # Только просмотр
        Permission.VIEW_USERS, Permission.VIEW_STATS, Permission.VIEW_SYSTEM, Permission.VIEW_FINANCE, Permission.VIEW_ANALYTICS
    }
}

def get_user_role(user_id: int) -> AdminRole:
    """Получить роль пользователя"""
    if user_id in SUPER_ADMIN_IDS:
        return AdminRole.SUPER_ADMIN
    elif user_id in ADMIN_IDS:
        return AdminRole.ADMIN
    elif user_id in MODERATOR_IDS:
        return AdminRole.MODERATOR
    else:
        raise ValueError(f"User {user_id} is not an admin")

def has_permission(user_id: int, permission: Permission) -> bool:
    """Проверить, есть ли у пользователя определенное право"""
    try:
        role = get_user_role(user_id)
        return permission in ROLE_PERMISSIONS[role]
    except ValueError:
        return False

def is_admin(user_id: int) -> bool:
    """Проверить, является ли пользователь админом"""
    return user_id in ALL_ADMIN_IDS

def get_user_permissions(user_id: int) -> Set[Permission]:
    """Получить все права пользователя"""
    try:
        role = get_user_role(user_id)
        return ROLE_PERMISSIONS[role]
    except ValueError:
        return set()

def add_admin(user_id: int, role: AdminRole) -> bool:
    """Добавить нового админа (только для супер-админов)"""
    global ADMIN_IDS, MODERATOR_IDS, ALL_ADMIN_IDS
    
    if user_id in ALL_ADMIN_IDS:
        return False  # Уже админ
    
    if role == AdminRole.ADMIN:
        ADMIN_IDS.append(user_id)
    elif role == AdminRole.MODERATOR:
        MODERATOR_IDS.append(user_id)
    else:
        return False  # Нельзя добавить супер-админа
    
    ALL_ADMIN_IDS.append(user_id)
    return True

def remove_admin(user_id: int) -> bool:
    """Удалить админа (только для супер-админов)"""
    global ADMIN_IDS, MODERATOR_IDS, ALL_ADMIN_IDS
    
    if user_id in SUPER_ADMIN_IDS:
        return False  # Нельзя удалить супер-админа
    
    removed = False
    if user_id in ADMIN_IDS:
        ADMIN_IDS.remove(user_id)
        removed = True
    elif user_id in MODERATOR_IDS:
        MODERATOR_IDS.remove(user_id)
        removed = True
    
    if removed and user_id in ALL_ADMIN_IDS:
        ALL_ADMIN_IDS.remove(user_id)
    
    return removed

# Функции для работы с админами
def get_admin_info(user_id: int) -> Dict:
    """Получить информацию об админе"""
    if not is_admin(user_id):
        return {}
    
    role = get_user_role(user_id)
    permissions = get_user_permissions(user_id)
    
    return {
        'user_id': user_id,
        'role': role.value,
        'permissions': [p.value for p in permissions],
        'is_super_admin': user_id in SUPER_ADMIN_IDS
    }

def list_all_admins() -> List[Dict]:
    """Получить список всех админов"""
    admins = []
    for user_id in ALL_ADMIN_IDS:
        admins.append(get_admin_info(user_id))
    return admins 
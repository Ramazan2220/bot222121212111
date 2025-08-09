#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Система управления пользователями для изоляции данных
Обеспечивает безопасное получение списков пользователей для системных процессов
"""

import logging
import json
import time
from datetime import datetime, timedelta
from typing import List, Tuple, Optional, Dict, Any
from database.db_manager import get_session
from database.models import InstagramAccount, TelegramUser

logger = logging.getLogger(__name__)

def get_active_users() -> List[int]:
    """
    Получить список активных пользователей для системных задач
    
    Returns:
        List[int]: Список user_id пользователей, у которых есть аккаунты Instagram
    """
    try:
        session = get_session()
        
        # Получаем уникальных пользователей с аккаунтами Instagram
        users = session.query(InstagramAccount.user_id).distinct().all()
        session.close()
        
        user_ids = [user[0] for user in users if user[0] is not None]
        logger.debug(f"📋 Найдено {len(user_ids)} активных пользователей")
        
        return user_ids
        
    except Exception as e:
        logger.error(f"❌ Ошибка получения активных пользователей: {e}")
        return []

def get_users_by_priority() -> List[Tuple[int, str]]:
    """
    Получить пользователей с приоритетами для оптимизированной обработки
    
    Returns:
        List[Tuple[int, str]]: Список (user_id, priority) отсортированный по приоритету
    """
    try:
        session = get_session()
        
        # VIP пользователи (админы)
        vip_users = []
        admin_users = session.query(TelegramUser.telegram_id).filter(
            TelegramUser.is_admin == True,
            TelegramUser.is_active == True
        ).all()
        for admin in admin_users:
            vip_users.append((admin[0], "VIP"))
        
        # Активные пользователи (заходили за последние 7 дней)
        regular_users = []
        week_ago = datetime.now() - timedelta(days=7)
        active_users = session.query(TelegramUser.telegram_id).filter(
            TelegramUser.is_active == True,
            TelegramUser.last_activity >= week_ago,
            TelegramUser.is_admin == False
        ).all()
        for user in active_users:
            regular_users.append((user[0], "ACTIVE"))
        
        # Неактивные пользователи
        inactive_users = []
        inactive_user_list = session.query(TelegramUser.telegram_id).filter(
            TelegramUser.is_active == True,
            TelegramUser.last_activity < week_ago,
            TelegramUser.is_admin == False
        ).all()
        for user in inactive_user_list:
            inactive_users.append((user[0], "INACTIVE"))
        
        # Пользователи без записи в TelegramUser (только с аккаунтами)
        orphan_users = []
        telegram_user_ids = session.query(TelegramUser.telegram_id).all()
        existing_telegram_ids = [u[0] for u in telegram_user_ids]
        
        account_user_ids = session.query(InstagramAccount.user_id).distinct().all()
        for account_user in account_user_ids:
            if account_user[0] not in existing_telegram_ids:
                orphan_users.append((account_user[0], "ORPHAN"))
        
        session.close()
        
        # Возвращаем в порядке приоритета
        all_users = vip_users + regular_users + inactive_users + orphan_users
        
        logger.info(f"👥 Распределение пользователей: VIP={len(vip_users)}, Active={len(regular_users)}, "
                   f"Inactive={len(inactive_users)}, Orphan={len(orphan_users)}")
        
        return all_users
        
    except Exception as e:
        logger.error(f"❌ Ошибка получения пользователей по приоритету: {e}")
        # Fallback на простой список
        return [(user_id, "UNKNOWN") for user_id in get_active_users()]

def get_user_accounts_count(user_id: int) -> int:
    """
    Получить количество аккаунтов у пользователя
    
    Args:
        user_id: ID пользователя
        
    Returns:
        int: Количество аккаунтов
    """
    try:
        session = get_session()
        count = session.query(InstagramAccount).filter(
            InstagramAccount.user_id == user_id,
            InstagramAccount.is_active == True
        ).count()
        session.close()
        return count
    except Exception as e:
        logger.error(f"❌ Ошибка подсчета аккаунтов для пользователя {user_id}: {e}")
        return 0

def get_user_info(user_id: int) -> Dict[str, Any]:
    """
    Получить информацию о пользователе
    
    Args:
        user_id: ID пользователя
        
    Returns:
        Dict: Информация о пользователе
    """
    try:
        session = get_session()
        
        # Информация из TelegramUser
        telegram_user = session.query(TelegramUser).filter(
            TelegramUser.telegram_id == user_id
        ).first()
        
        # Количество аккаунтов
        accounts_count = session.query(InstagramAccount).filter(
            InstagramAccount.user_id == user_id,
            InstagramAccount.is_active == True
        ).count()
        
        # Последняя активность аккаунтов
        last_account_activity = session.query(InstagramAccount.updated_at).filter(
            InstagramAccount.user_id == user_id
        ).order_by(InstagramAccount.updated_at.desc()).first()
        
        session.close()
        
        user_info = {
            "user_id": user_id,
            "accounts_count": accounts_count,
            "last_account_activity": last_account_activity[0] if last_account_activity else None,
            "username": None,
            "first_name": None,
            "is_admin": False,
            "last_activity": None,
            "is_active": True  # По умолчанию активен если есть аккаунты
        }
        
        if telegram_user:
            user_info.update({
                "username": telegram_user.username,
                "first_name": telegram_user.first_name,
                "is_admin": telegram_user.is_admin,
                "last_activity": telegram_user.last_activity,
                "is_active": telegram_user.is_active
            })
        
        return user_info
        
    except Exception as e:
        logger.error(f"❌ Ошибка получения информации о пользователе {user_id}: {e}")
        return {
            "user_id": user_id,
            "accounts_count": 0,
            "error": str(e)
        }

def validate_user_exists(user_id: int) -> bool:
    """
    Проверить существование пользователя в системе
    
    Args:
        user_id: ID пользователя для проверки
        
    Returns:
        bool: True если пользователь существует
    """
    try:
        session = get_session()
        
        # Проверяем наличие аккаунтов у пользователя
        accounts_count = session.query(InstagramAccount).filter(
            InstagramAccount.user_id == user_id
        ).count()
        
        session.close()
        return accounts_count > 0
        
    except Exception as e:
        logger.error(f"❌ Ошибка проверки существования пользователя {user_id}: {e}")
        return False

def cleanup_orphaned_data():
    """
    Очистка данных без привязки к пользователям
    ВНИМАНИЕ: Потенциально опасная операция!
    """
    try:
        session = get_session()
        
        # Находим аккаунты без user_id или с user_id = None
        orphaned_accounts = session.query(InstagramAccount).filter(
            InstagramAccount.user_id.is_(None)
        ).all()
        
        logger.warning(f"⚠️ Найдено {len(orphaned_accounts)} аккаунтов без привязки к пользователю")
        
        # НЕ УДАЛЯЕМ автоматически - только логируем!
        for account in orphaned_accounts:
            logger.warning(f"⚠️ Аккаунт без пользователя: {account.username} (ID: {account.id})")
        
        session.close()
        
        return len(orphaned_accounts)
        
    except Exception as e:
        logger.error(f"❌ Ошибка очистки данных: {e}")
        return -1 
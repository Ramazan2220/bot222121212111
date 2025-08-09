#!/usr/bin/env python3
"""
🔒 Правильное решение изоляции пользователей на основе SQLAlchemy best practices
Использует with_loader_criteria и scoped_session с custom scopefunc
"""

import logging
from contextvars import ContextVar
from typing import Optional, Any
from functools import wraps

from sqlalchemy.orm import scoped_session, sessionmaker, with_loader_criteria
from sqlalchemy import event

from database.models import InstagramAccount

logger = logging.getLogger(__name__)

# Context variable для хранения текущего пользователя (работает с async/await)
_current_user: ContextVar[Optional[int]] = ContextVar('current_user', default=None)

class UserContextManager:
    """🔒 Менеджер контекста пользователя для изоляции данных"""
    
    @staticmethod
    def set_current_user(user_id: int):
        """Устанавливает текущего пользователя"""
        _current_user.set(user_id)
        logger.info(f"🔒 УСТАНОВЛЕН КОНТЕКСТ ПОЛЬЗОВАТЕЛЯ: {user_id}")
    
    @staticmethod
    def get_current_user() -> Optional[int]:
        """Получает текущего пользователя"""
        return _current_user.get()
    
    @staticmethod
    def clear_current_user():
        """Очищает контекст пользователя"""
        _current_user.set(None)

def create_scoped_session_with_user_isolation(engine):
    """
    🔒 Создает scoped_session с автоматической изоляцией пользователей
    
    Args:
        engine: SQLAlchemy engine
    
    Returns:
        scoped_session с изоляцией пользователей
    """
    
    def user_scopefunc():
        """Функция области видимости на основе текущего пользователя"""
        user_id = UserContextManager.get_current_user()
        # Возвращаем кортеж (thread_id, user_id) для уникальности
        import threading
        return (threading.get_ident(), user_id)
    
    # Создаем sessionmaker
    session_factory = sessionmaker(bind=engine)
    
    # Создаем scoped_session с пользовательской функцией области видимости
    Session = scoped_session(session_factory, scopefunc=user_scopefunc)
    
    # Устанавливаем автоматические фильтры для InstagramAccount
    @event.listens_for(Session, "do_orm_execute")
    def _add_user_filtering_criteria(execute_state):
        """Автоматически добавляет фильтрацию по user_id для InstagramAccount"""
        
        # Пропускаем если это не SELECT запрос
        if not execute_state.is_select:
            return
            
        # Пропускаем если установлен флаг игнорирования фильтров
        if execute_state.execution_options.get("skip_user_filter", False):
            return
        
        # Получаем текущего пользователя
        current_user_id = UserContextManager.get_current_user()
        
        if current_user_id is None:
            logger.warning("⚠️ Запрос без установленного пользователя - системный запрос разрешен")
            return
        
        logger.info(f"🔒 ПРИМЕНЯЕМ ФИЛЬТР ПОЛЬЗОВАТЕЛЯ: {current_user_id}")
        
        # Применяем фильтр для InstagramAccount
        execute_state.statement = execute_state.statement.options(
            with_loader_criteria(
                InstagramAccount,
                lambda cls: cls.user_id == current_user_id,
                include_aliases=True
            )
        )
    
    logger.info("🔒 ✅ Scoped session с изоляцией пользователей создан")
    return Session

def require_user_context(func):
    """
    🔒 Декоратор для обеспечения установленного контекста пользователя
    
    Usage:
        @require_user_context
        def some_function():
            # функция требует установленного пользователя
            pass
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        if UserContextManager.get_current_user() is None:
            raise ValueError("🚫 Функция требует установленного контекста пользователя")
        return func(*args, **kwargs)
    return wrapper

def with_user_context(user_id: int):
    """
    🔒 Context manager для временной установки пользователя
    
    Usage:
        with with_user_context(123):
            # код выполняется в контексте пользователя 123
            accounts = session.query(InstagramAccount).all()
    """
    class UserContextManager:
        def __init__(self, user_id: int):
            self.user_id = user_id
            self.previous_user = None
            
        def __enter__(self):
            self.previous_user = _current_user.get()
            UserContextManager.set_current_user(self.user_id)
            return self
            
        def __exit__(self, exc_type, exc_val, exc_tb):
            if self.previous_user is not None:
                UserContextManager.set_current_user(self.previous_user)
            else:
                UserContextManager.clear_current_user()
    
    return UserContextManager(user_id) 
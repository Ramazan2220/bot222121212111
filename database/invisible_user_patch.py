#!/usr/bin/env python3
"""
🛡️ НЕВИДИМЫЙ ПАТЧ ИЗОЛЯЦИИ ПОЛЬЗОВАТЕЛЕЙ
Автоматически подставляет user_id в get_instagram_accounts() и get_instagram_account()
БЕЗ изменения основного кода системы!
"""

import logging
import functools
from typing import Optional, List, Any
from database.user_context_manager import UserContextManager

logger = logging.getLogger(__name__)

class InvisibleUserPatch:
    """🛡️ Невидимый патч для автоматической изоляции пользователей"""
    
    def __init__(self):
        self.original_functions = {}
        self.patched = False
        
    def apply_patch(self):
        """Применяет невидимый патч к функциям базы данных"""
        if self.patched:
            logger.warning("🔧 Патч уже применен")
            return
            
        try:
            # Импортируем модуль только при патчинге
            import database.db_manager as db_manager
            
            # Сохраняем оригинальные функции
            self.original_functions['get_instagram_accounts'] = db_manager.get_instagram_accounts
            self.original_functions['get_instagram_account'] = db_manager.get_instagram_account
            
            # Применяем патчи
            db_manager.get_instagram_accounts = self._patch_get_accounts(
                db_manager.get_instagram_accounts
            )
            db_manager.get_instagram_account = self._patch_get_account(
                db_manager.get_instagram_account
            )
            
            self.patched = True
            logger.info("🛡️ ✅ Невидимый патч изоляции пользователей активирован")
            
        except Exception as e:
            logger.error(f"❌ Ошибка применения невидимого патча: {e}")
            
    def _patch_get_accounts(self, original_func):
        """Патч для get_instagram_accounts"""
        @functools.wraps(original_func)
        def patched_function(user_id: Optional[int] = None):
            # Если user_id не передан, получаем из контекста
            if user_id is None:
                current_user = UserContextManager.get_current_user()
                if current_user is not None:
                    user_id = current_user
                    logger.debug(f"🔧 Автоматически подставлен user_id: {user_id}")
                else:
                    logger.warning("⚠️ Вызов get_instagram_accounts без пользователя - системный запрос")
            
            return original_func(user_id)
        
        return patched_function
    
    def _patch_get_account(self, original_func):
        """Патч для get_instagram_account"""
        @functools.wraps(original_func)
        def patched_function(account_id: int, user_id: Optional[int] = None):
            # Если user_id не передан, получаем из контекста
            if user_id is None:
                current_user = UserContextManager.get_current_user()
                if current_user is not None:
                    user_id = current_user
                    logger.debug(f"🔧 Автоматически подставлен user_id: {user_id} для аккаунта {account_id}")
                else:
                    logger.warning(f"⚠️ Вызов get_instagram_account({account_id}) без пользователя - системный запрос")
            
            return original_func(account_id, user_id)
        
        return patched_function
    
    def remove_patch(self):
        """Удаляет патч и восстанавливает оригинальные функции"""
        if not self.patched:
            logger.warning("🔧 Патч не был применен")
            return
            
        try:
            import database.db_manager as db_manager
            
            # Восстанавливаем оригинальные функции
            db_manager.get_instagram_accounts = self.original_functions['get_instagram_accounts']
            db_manager.get_instagram_account = self.original_functions['get_instagram_account']
            
            self.patched = False
            logger.info("🛡️ Невидимый патч изоляции пользователей удален")
            
        except Exception as e:
            logger.error(f"❌ Ошибка удаления невидимого патча: {e}")

# Глобальный экземпляр патча
_global_patch = InvisibleUserPatch()

def activate_invisible_user_isolation():
    """🛡️ Активирует невидимую изоляцию пользователей"""
    _global_patch.apply_patch()

def deactivate_invisible_user_isolation():
    """🛡️ Деактивирует невидимую изоляцию пользователей"""
    _global_patch.remove_patch() 
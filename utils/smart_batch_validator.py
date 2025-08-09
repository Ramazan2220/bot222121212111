#!/usr/bin/env python3
"""
🚀 УМНЫЙ БАТЧЕВЫЙ ВАЛИДАТОР
Обрабатывает аккаунты по пользователям, избегая перегрузки системы
"""

import time
import logging
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from database.db_manager import get_session, get_instagram_accounts, update_instagram_account
from database.models import InstagramAccount
from sqlalchemy import distinct

logger = logging.getLogger(__name__)

class SmartBatchValidator:
    """Умный валидатор с батчевой обработкой по пользователям"""
    
    def __init__(self):
        self.is_running = False
        self.check_interval = 300  # 5 минут между циклами
        self.batch_size = 5  # Обрабатывать по 5 пользователей за раз
        self.accounts_per_user_limit = 50  # Максимум аккаунтов на пользователя за раз
        self.validation_thread = None
        
        # Ротация пользователей
        self.current_user_offset = 0
        self.last_full_cycle = datetime.now()
        
    def start(self):
        """Запуск батчевого валидатора"""
        if self.is_running:
            logger.warning("⚠️ Батчевый валидатор уже запущен")
            return
            
        self.is_running = True
        self.validation_thread = threading.Thread(target=self._batch_validation_loop, daemon=True)
        self.validation_thread.start()
        logger.info("🚀 Умный батчевый валидатор запущен")
    
    def stop(self):
        """Остановка валидатора"""
        self.is_running = False
        if self.validation_thread:
            self.validation_thread.join(timeout=10)
        logger.info("🛑 Умный батчевый валидатор остановлен")
    
    def _batch_validation_loop(self):
        """Основной цикл батчевой валидации"""
        while self.is_running:
            try:
                time.sleep(self.check_interval)
                
                # 🎯 Получаем список всех пользователей
                user_ids = self._get_active_user_ids()
                if not user_ids:
                    logger.info("📊 Нет активных пользователей для проверки")
                    continue
                
                total_users = len(user_ids)
                logger.info(f"👥 Найдено {total_users} активных пользователей")
                
                # 🔄 Ротация: берем следующий батч пользователей
                end_offset = min(self.current_user_offset + self.batch_size, total_users)
                batch_users = user_ids[self.current_user_offset:end_offset]
                
                logger.info(f"🎯 Обрабатываем пользователей {self.current_user_offset+1}-{end_offset} из {total_users}")
                
                # 🔧 Обрабатываем каждого пользователя в батче
                for user_id in batch_users:
                    try:
                        self._validate_user_accounts(user_id)
                        time.sleep(1)  # Пауза между пользователями
                    except Exception as e:
                        logger.error(f"❌ Ошибка валидации пользователя {user_id}: {e}")
                
                # 📈 Обновляем offset для следующего цикла
                self.current_user_offset = end_offset
                
                # 🔄 Если прошли всех пользователей - начинаем сначала
                if self.current_user_offset >= total_users:
                    self.current_user_offset = 0
                    self.last_full_cycle = datetime.now()
                    logger.info(f"✅ Полный цикл валидации завершен! Следующий цикл через {self.check_interval}с")
                    
            except Exception as e:
                logger.error(f"❌ Критическая ошибка в батчевой валидации: {e}")
    
    def _get_active_user_ids(self) -> List[int]:
        """Получает список ID активных пользователей"""
        try:
            session = get_session()
            # Получаем уникальные user_id из аккаунтов
            user_ids = session.query(distinct(InstagramAccount.user_id)).filter(
                InstagramAccount.user_id.isnot(None)
            ).all()
            session.close()
            
            return [uid[0] for uid in user_ids if uid[0] is not None]
        except Exception as e:
            logger.error(f"❌ Ошибка получения списка пользователей: {e}")
            return []
    
    def _validate_user_accounts(self, user_id: int):
        """Валидирует аккаунты конкретного пользователя"""
        try:
            # 🔒 Получаем аккаунты только этого пользователя
            user_accounts = get_instagram_accounts(user_id)
            
            if not user_accounts:
                logger.debug(f"📊 У пользователя {user_id} нет аккаунтов")
                return
            
            # 🎯 Ограничиваем количество для избежания перегрузки
            accounts_to_check = user_accounts[:self.accounts_per_user_limit]
            
            logger.info(f"🔍 Проверяем {len(accounts_to_check)} аккаунтов пользователя {user_id}")
            
            # 📊 Простая проверка статуса
            valid_count = 0
            invalid_count = 0
            
            for account in accounts_to_check:
                try:
                    # 🔍 Базовая проверка без подключения к Instagram
                    if self._basic_account_check(account):
                        valid_count += 1
                    else:
                        invalid_count += 1
                        
                except Exception as e:
                    logger.error(f"❌ Ошибка проверки аккаунта {account.id}: {e}")
                    invalid_count += 1
            
            logger.info(f"✅ Пользователь {user_id}: {valid_count} валидных, {invalid_count} проблемных")
            
        except Exception as e:
            logger.error(f"❌ Ошибка валидации аккаунтов пользователя {user_id}: {e}")
    
    def _basic_account_check(self, account: InstagramAccount) -> bool:
        """Базовая проверка аккаунта без подключения к Instagram"""
        try:
            # 🔍 Проверяем базовые поля
            if not account.username or not account.password:
                return False
            
            # 🔍 Проверяем не слишком ли старая последняя активность
            if account.last_activity:
                days_since_activity = (datetime.now() - account.last_activity).days
                if days_since_activity > 30:  # Более 30 дней неактивности
                    return False
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка базовой проверки аккаунта {account.id}: {e}")
            return False
    
    def get_stats(self) -> Dict:
        """Статистика работы валидатора"""
        return {
            'is_running': self.is_running,
            'current_user_offset': self.current_user_offset,
            'last_full_cycle': self.last_full_cycle.isoformat() if self.last_full_cycle else None,
            'batch_size': self.batch_size,
            'accounts_per_user_limit': self.accounts_per_user_limit,
            'check_interval': self.check_interval
        }

# 🚀 Глобальный экземпляр
smart_batch_validator = SmartBatchValidator() 
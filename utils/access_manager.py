import json
import os
import time
import logging
from typing import List, Set, Dict, Optional
from datetime import datetime
from admin_bot.models.user import User, UserStatus
from admin_bot.services.user_service import UserService

logger = logging.getLogger(__name__)

class AccessManager:
    """
    Централизованная система управления доступами
    Синхронизирует доступы между админ панелью и основным ботом
    """
    
    def __init__(self, config_path: str = "config.py", cache_file: str = "data/access_cache.json"):
        self.config_path = config_path
        self.cache_file = cache_file
        self.user_service = UserService()
        self._ensure_data_dir()
        
        # Кэш для быстрого доступа
        self._access_cache = {}
        self._last_update = None
        
        # Загружаем начальные данные
        self._load_cache()
        self._sync_access_lists()
        
    def _ensure_data_dir(self):
        """Создает директорию для данных если её нет"""
        os.makedirs(os.path.dirname(self.cache_file), exist_ok=True)
    
    def _load_cache(self):
        """Загружает кэш доступов"""
        try:
            if os.path.exists(self.cache_file):
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self._access_cache = data.get('access_list', {})
                    self._last_update = data.get('last_update')
            else:
                self._access_cache = {}
        except Exception as e:
            logger.error(f"Ошибка загрузки кэша доступов: {e}")
            self._access_cache = {}
    
    def _save_cache(self):
        """Сохраняет кэш доступов"""
        try:
            data = {
                'access_list': self._access_cache,
                'last_update': datetime.now().isoformat()
            }
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            self._last_update = data['last_update']
        except Exception as e:
            logger.error(f"Ошибка сохранения кэша доступов: {e}")
    
    def _get_config_admin_ids(self) -> List[int]:
        """Получает список админов из config.py"""
        try:
            if os.path.exists(self.config_path):
                # Импортируем config модуль
                import importlib.util
                spec = importlib.util.spec_from_file_location("config", self.config_path)
                config = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(config)
                
                if hasattr(config, 'ADMIN_USER_IDS'):
                    return config.ADMIN_USER_IDS
            return []
        except Exception as e:
            logger.error(f"Ошибка чтения config.py: {e}")
            return []
    
    def _get_admin_panel_users(self) -> Dict[int, User]:
        """Получает активных пользователей из админ панели"""
        try:
            all_users = self.user_service.get_all_users()
            return {
                user.telegram_id: user 
                for user in all_users 
                if user.status in [UserStatus.ACTIVE, UserStatus.TRIAL] and user.status != UserStatus.BLOCKED
            }
        except Exception as e:
            logger.error(f"Ошибка получения пользователей из админ панели: {e}")
            return {}
    
    def _sync_access_lists(self):
        """Синхронизирует списки доступов между всеми источниками"""
        try:
            # Получаем данные из всех источников
            config_admins = set(self._get_config_admin_ids())
            panel_users = self._get_admin_panel_users()
            
            # Объединяем все доступы
            all_access = {}
            
            # Добавляем админов из config.py (высший приоритет)
            for admin_id in config_admins:
                all_access[str(admin_id)] = {
                    'telegram_id': admin_id,
                    'source': 'config',
                    'role': 'super_admin',
                    'is_active': True,
                    'added_at': datetime.now().isoformat()
                }
            
            # Добавляем пользователей из админ панели
            for telegram_id, user in panel_users.items():
                user_key = str(telegram_id)
                if user_key not in all_access:  # Не перезаписываем супер-админов
                    all_access[user_key] = {
                        'telegram_id': telegram_id,
                        'source': 'admin_panel',
                        'role': 'admin' if not user.is_trial else 'trial',
                        'is_active': user.is_active,
                        'subscription_plan': user.subscription_plan.value if user.subscription_plan else None,
                        'subscription_end': user.subscription_end.isoformat() if user.subscription_end else None,
                        'added_at': user.created_at.isoformat()
                    }
            
            # Обновляем кэш
            self._access_cache = all_access
            self._save_cache()
            
            logger.info(f"Синхронизированы доступы: {len(all_access)} пользователей")
            
        except Exception as e:
            logger.error(f"Ошибка синхронизации доступов: {e}")
    
    def has_access(self, telegram_id: int) -> bool:
        """Проверяет доступ пользователя (основная функция для проверки)"""
        try:
            # Обновляем кэш если нужно
            self._update_cache_if_needed()
            
            user_key = str(telegram_id)
            if user_key in self._access_cache:
                user_data = self._access_cache[user_key]
                
                # Проверяем активность
                if not user_data.get('is_active', False):
                    return False
                
                # Проверяем срок действия для пользователей из админ панели
                if user_data.get('source') == 'admin_panel':
                    subscription_end = user_data.get('subscription_end')
                    if subscription_end:
                        try:
                            end_date = datetime.fromisoformat(subscription_end)
                            if datetime.now() > end_date:
                                # Подписка истекла - обновляем статус
                                self._deactivate_user(telegram_id)
                                return False
                        except:
                            pass
                
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Ошибка проверки доступа для {telegram_id}: {e}")
            return False
    
    def add_user(self, telegram_id: int, source: str = 'manual', role: str = 'admin') -> bool:
        """Добавляет пользователя в систему доступов"""
        try:
            user_key = str(telegram_id)
            
            # Если пользователь уже есть в config.py, не добавляем его в панель
            config_admins = set(self._get_config_admin_ids())
            if telegram_id in config_admins:
                logger.warning(f"Пользователь {telegram_id} уже супер-админ в config.py")
                return True
            
            # Добавляем в админ панель или реактивируем существующего
            user = self.user_service.get_user(telegram_id)
            if not user:
                user = self.user_service.create_user(telegram_id)
                logger.info(f"Создан новый пользователь {telegram_id}")
            else:
                logger.info(f"Реактивируется существующий пользователь {telegram_id}")
            
            # Устанавливаем активный статус и подписку (для всех пользователей)
            from admin_bot.models.user import SubscriptionPlan
            user.status = UserStatus.ACTIVE
            user.set_subscription(SubscriptionPlan.SUBSCRIPTION_30_DAYS)  # Даем месячную подписку по умолчанию
            self.user_service.update_user(user)
            
            # Обновляем кэш
            self._sync_access_lists()
            
            logger.info(f"Добавлен пользователь {telegram_id} с ролью {role}")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка добавления пользователя {telegram_id}: {e}")
            return False
    
    def remove_user(self, telegram_id: int) -> bool:
        """Удаляет пользователя из системы доступов"""
        try:
            # Нельзя удалить супер-админов из config.py
            config_admins = set(self._get_config_admin_ids())
            if telegram_id in config_admins:
                logger.warning(f"Нельзя удалить супер-админа {telegram_id} из config.py")
                return False
            
            # Удаляем из админ панели
            user = self.user_service.get_user(telegram_id)
            if user:
                user.status = UserStatus.BLOCKED
                self.user_service.update_user(user)
            
            # Обновляем кэш
            self._sync_access_lists()
            
            logger.info(f"Удален пользователь {telegram_id}")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка удаления пользователя {telegram_id}: {e}")
            return False
    
    def _deactivate_user(self, telegram_id: int):
        """Деактивирует пользователя с истекшей подпиской"""
        try:
            user = self.user_service.get_user(telegram_id)
            if user:
                user.status = UserStatus.EXPIRED
                self.user_service.update_user(user)
                
                # Удаляем из кэша
                user_key = str(telegram_id)
                if user_key in self._access_cache:
                    self._access_cache[user_key]['is_active'] = False
                    self._save_cache()
                
                logger.info(f"Деактивирован пользователь {telegram_id} (истекла подписка)")
                
        except Exception as e:
            logger.error(f"Ошибка деактивации пользователя {telegram_id}: {e}")
    
    def _update_cache_if_needed(self):
        """Обновляет кэш если прошло много времени"""
        try:
            # Обновляем кэш каждые 30 секунд (для быстрой синхронизации)
            if self._last_update:
                last_update = datetime.fromisoformat(self._last_update)
                if (datetime.now() - last_update).total_seconds() > 30:  # 30 секунд
                    # ВАЖНО: Сначала синхронизируемся с shared cache
                    self._sync_with_shared_cache()
                    # Затем обновляем из локальных источников
                    self._sync_access_lists()
            else:
                # При первом запуске
                self._sync_with_shared_cache()
                self._sync_access_lists()
        except Exception as e:
            logger.error(f"Ошибка обновления кэша: {e}")
    
    def get_all_users(self) -> Dict[str, dict]:
        """Получает всех пользователей с доступом"""
        self._update_cache_if_needed()
        return self._access_cache.copy()
    
    def get_user_info(self, telegram_id: int) -> Optional[dict]:
        """Получает информацию о пользователе"""
        self._update_cache_if_needed()
        return self._access_cache.get(str(telegram_id))
    
    def force_sync(self):
        """Принудительная синхронизация с общим кешем"""
        # 1. Сначала обновляем локальный кеш из всех источников
        self._sync_access_lists()
        
        # 2. Затем синхронизируемся с shared cache (объединяем данные)
        self._sync_with_shared_cache()
        
        # 3. Сохраняем объединенный кеш в файл
        self._save_shared_cache()
        
        logger.info("Выполнена принудительная синхронизация доступов")


    def _load_shared_cache(self):
        """Загружает общий кеш из файла"""
        try:
            if os.path.exists("data/shared_access_cache.json"):
                with open("data/shared_access_cache.json", "r", encoding="utf-8") as f:
                    data = json.load(f)
                    # Проверяем время последнего обновления
                    if time.time() - data.get("last_update", 0) < 30:  # 30 секунд
                        return data.get("cache", {})
        except Exception as e:
            logger.error(f"Ошибка загрузки общего кеша: {e}")
        return {}
    
    def _save_shared_cache(self):
        """Сохраняет общий кеш в файл"""
        try:
            cache_data = {
                "cache": self._access_cache,
                "last_update": time.time(),
                "updated_by": "AccessManager"
            }
            with open("data/shared_access_cache.json", "w", encoding="utf-8") as f:
                json.dump(cache_data, f, ensure_ascii=False, indent=2, default=str)
        except Exception as e:
            logger.error(f"Ошибка сохранения общего кеша: {e}")
    
    def _sync_with_shared_cache(self):
        """Синхронизирует с общим кешем"""
        shared_cache = self._load_shared_cache()
        if shared_cache:
            self._access_cache.update(shared_cache)
            logger.info(f"Синхронизирован с общим кешем: {len(shared_cache)} пользователей")
    
# Глобальный экземпляр менеджера доступов
_access_manager = None

def get_access_manager() -> AccessManager:
    """Получает глобальный экземпляр менеджера доступов"""
    global _access_manager
    if _access_manager is None:
        _access_manager = AccessManager()
    return _access_manager

def has_access(telegram_id: int) -> bool:
    """Проверяет доступ пользователя (главная функция для использования)"""
    return get_access_manager().has_access(telegram_id)

def add_user_access(telegram_id: int) -> bool:
    """Добавляет доступ пользователю"""
    return get_access_manager().add_user(telegram_id)

def remove_user_access(telegram_id: int) -> bool:
    """Удаляет доступ пользователя"""
    return get_access_manager().remove_user(telegram_id)

def delete_user_completely(telegram_id: int) -> bool:
    """Полностью удаляет пользователя из системы"""
    try:
        access_manager = get_access_manager()
        
        # Нельзя удалить супер-админов из config.py
        config_admins = set(access_manager._get_config_admin_ids())
        if telegram_id in config_admins:
            logger.warning(f"Нельзя удалить супер-админа {telegram_id} из config.py")
            return False
        
        # Удаляем полностью из админ панели
        user = access_manager.user_service.get_user(telegram_id)
        if user:
            access_manager.user_service.delete_user(telegram_id)
            access_manager.user_service.save_users()  # Важно сохранить изменения
            logger.info(f"Пользователь {telegram_id} полностью удален из базы данных")
        
        # ДОПОЛНИТЕЛЬНО: Удаляем из файла admin_bot/data/users.json напрямую
        try:
            users_file = "admin_bot/data/users.json"
            if os.path.exists(users_file):
                import json
                with open(users_file, 'r', encoding='utf-8') as f:
                    users_data = json.load(f)
                
                user_key = str(telegram_id)
                if user_key in users_data:
                    del users_data[user_key]
                    
                    with open(users_file, 'w', encoding='utf-8') as f:
                        json.dump(users_data, f, ensure_ascii=False, indent=2)
                    
                    logger.info(f"Пользователь {telegram_id} удален из файла users.json")
        except Exception as e:
            logger.error(f"Ошибка удаления из файла users.json: {e}")
        
        # Дополнительно удаляем из shared cache напрямую ДО синхронизации
        try:
            cache_file = "data/shared_access_cache.json"
            if os.path.exists(cache_file):
                import json
                with open(cache_file, 'r', encoding='utf-8') as f:
                    cache_data = json.load(f)
                
                cache = cache_data.get('cache', {})
                user_key = str(telegram_id)
                
                if user_key in cache:
                    del cache[user_key]
                    cache_data['cache'] = cache
                    cache_data['last_update'] = time.time()
                    
                    with open(cache_file, 'w', encoding='utf-8') as f:
                        json.dump(cache_data, f, ensure_ascii=False, indent=2, default=str)
                    
                    logger.info(f"Пользователь {telegram_id} удален из shared cache файла")
        except Exception as e:
            logger.error(f"Ошибка удаления из shared cache: {e}")
        
        # Принудительная синхронизация (обновляет кеш из источников)
        access_manager.force_sync()
        
        # Окончательно удаляем из локального кеша если он снова появился
        user_key = str(telegram_id)
        if user_key in access_manager._access_cache:
            del access_manager._access_cache[user_key]
            logger.info(f"Пользователь {telegram_id} окончательно удален из локального кеша")
        
        logger.info(f"Пользователь {telegram_id} полностью удален из системы")
        return True
        
    except Exception as e:
        logger.error(f"Ошибка полного удаления пользователя {telegram_id}: {e}")
        return False

def force_sync_access():
    """Принудительная синхронизация доступов"""
    get_access_manager().force_sync() 

# === НОВАЯ СИСТЕМА СИНХРОНИЗАЦИИ ===

# REDIS СИСТЕМА СИНХРОНИЗАЦИИ
# Импортируем Redis синхронизацию
try:
    from redis_access_sync import has_access_redis, add_user_redis, remove_user_redis, get_redis_sync
    USE_REDIS_SYNC = True
    print("🔥 Используется Redis система синхронизации")
except ImportError:
    USE_REDIS_SYNC = False
    # Fallback на файловую систему
    try:
        from file_access_sync import has_access_file, add_user_file, remove_user_file, get_sync_stats
        USE_FILE_SYNC = True
        print("🟡 Fallback на файловую систему синхронизации")
    except ImportError:
        USE_FILE_SYNC = False
        print("🔴 Fallback на старую систему синхронизации")

def has_access(telegram_id: int) -> bool:
    """Проверяет доступ пользователя (ТОЛЬКО Redis система)"""
    if USE_REDIS_SYNC:
        result = has_access_redis(telegram_id)
        print(f"🔍 Redis проверка доступа {telegram_id}: {result}")
        return result
    else:
        print(f"❌ Redis не доступен! Отказываем в доступе для {telegram_id}")
        return False

def add_user_access(telegram_id: int, user_data: dict = None) -> bool:
    """Добавляет доступ пользователю (Redis система)"""
    if USE_REDIS_SYNC:
        if user_data is None:
            from datetime import datetime, timedelta
            user_data = {
                'telegram_id': telegram_id,
                'is_active': True,
                'subscription_end': (datetime.now() + timedelta(days=30)).isoformat(),
                'role': 'trial'
            }
        return add_user_redis(telegram_id, user_data)
    elif USE_FILE_SYNC:
        if user_data is None:
            from datetime import datetime, timedelta
            user_data = {
                'telegram_id': telegram_id,
                'is_active': True,
                'subscription_end': (datetime.now() + timedelta(days=30)).isoformat(),
                'role': 'trial'
            }
        return add_user_file(telegram_id, user_data)
    else:
        # Fallback на старую систему
        return get_access_manager().add_user(telegram_id)

def remove_user_access(telegram_id: int) -> bool:
    """Удаляет доступ пользователя (Redis система)"""
    if USE_REDIS_SYNC:
        return remove_user_redis(telegram_id)
    elif USE_FILE_SYNC:
        return remove_user_file(telegram_id)
    else:
        # Fallback на старую систему
        return get_access_manager().remove_user(telegram_id)

def delete_user_completely(telegram_id: int) -> bool:
    """Полностью удаляет пользователя (Redis система)"""
    if USE_REDIS_SYNC:
        return remove_user_redis(telegram_id)
    elif USE_FILE_SYNC:
        return remove_user_file(telegram_id)
    else:
        # Fallback на старую систему
        try:
            access_manager = get_access_manager()
            
            # Удаляем из админ панели
            user = access_manager.user_service.get_user(telegram_id)
            if user:
                access_manager.user_service.delete_user(telegram_id)
                access_manager.user_service.save_users()
            
            # Принудительная синхронизация
            access_manager.force_sync()
            
            return True
        except Exception as e:
            print(f"Ошибка удаления пользователя {telegram_id}: {e}")
            return False

def force_sync_access():
    """Принудительная синхронизация (Redis система)"""
    if USE_REDIS_SYNC:
        # В Redis показываем статистику
        redis_sync = get_redis_sync()
        stats = redis_sync.get_stats()
        print(f"🔄 Redis статистика: {stats}")
        return True
    elif USE_FILE_SYNC:
        # В файловой системе показываем статистику
        stats = get_sync_stats()
        print(f"🔄 Статистика синхронизации: {stats}")
        return True
    else:
        # Fallback на старую систему
        try:
            get_access_manager().force_sync()
            return True
        except Exception as e:
            print(f"Ошибка синхронизации: {e}")
            return False

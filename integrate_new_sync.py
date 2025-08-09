#!/usr/bin/env python3
"""
ИНТЕГРАЦИЯ НОВОЙ СИСТЕМЫ СИНХРОНИЗАЦИИ
Заменяет старую файловую систему на modern event-driven multiprocessing
"""

import os
import shutil
import sys

def backup_old_system():
    """Создает бэкап старой системы"""
    print("📦 Создаем бэкап старой системы...")
    
    files_to_backup = [
        "utils/access_manager.py",
        "telegram_bot/handlers.py",
        "admin_bot/handlers/user_handlers.py"
    ]
    
    backup_dir = "backup_old_sync"
    os.makedirs(backup_dir, exist_ok=True)
    
    for file_path in files_to_backup:
        if os.path.exists(file_path):
            backup_path = os.path.join(backup_dir, os.path.basename(file_path))
            shutil.copy2(file_path, backup_path)
            print(f"   ✅ {file_path} → {backup_path}")
    
    print(f"📦 Бэкап создан в {backup_dir}/")

def integrate_multiprocessing_sync():
    """Интегрирует multiprocessing синхронизацию"""
    print("🔧 Интегрируем multiprocessing синхронизацию...")
    
    # 1. Обновляем utils/access_manager.py
    access_manager_patch = '''
# НОВАЯ СИСТЕМА СИНХРОНИЗАЦИИ
# Импортируем multiprocessing синхронизацию
try:
    from multiprocessing_access_sync import has_access_mp, add_user_mp, remove_user_mp, get_mp_sync
    USE_NEW_SYNC = True
    print("🟢 Используется новая multiprocessing синхронизация")
except ImportError:
    USE_NEW_SYNC = False
    print("🔴 Fallback на старую систему синхронизации")

def has_access(telegram_id: int) -> bool:
    """Проверяет доступ пользователя (новая система)"""
    if USE_NEW_SYNC:
        return has_access_mp(telegram_id)
    else:
        # Fallback на старую систему
        return get_access_manager().has_access(telegram_id)

def add_user_access(telegram_id: int, user_data: dict = None) -> bool:
    """Добавляет доступ пользователю (новая система)"""
    if USE_NEW_SYNC:
        if user_data is None:
            from datetime import datetime, timedelta
            user_data = {
                'telegram_id': telegram_id,
                'is_active': True,
                'subscription_end': (datetime.now() + timedelta(days=30)).isoformat(),
                'role': 'trial'
            }
        return add_user_mp(telegram_id, user_data)
    else:
        # Fallback на старую систему
        return get_access_manager().add_user(telegram_id)

def remove_user_access(telegram_id: int) -> bool:
    """Удаляет доступ пользователя (новая система)"""
    if USE_NEW_SYNC:
        return remove_user_mp(telegram_id)
    else:
        # Fallback на старую систему
        return get_access_manager().remove_user(telegram_id)

def delete_user_completely(telegram_id: int) -> bool:
    """Полностью удаляет пользователя (новая система)"""
    if USE_NEW_SYNC:
        return remove_user_mp(telegram_id)
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
    """Принудительная синхронизация (новая система)"""
    if USE_NEW_SYNC:
        # В новой системе синхронизация автоматическая
        sync = get_mp_sync()
        stats = sync.get_stats()
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
'''
    
    # Добавляем патч в конец файла access_manager.py
    with open("utils/access_manager.py", "a", encoding="utf-8") as f:
        f.write("\n\n# === НОВАЯ СИСТЕМА СИНХРОНИЗАЦИИ ===\n")
        f.write(access_manager_patch)
    
    print("   ✅ utils/access_manager.py обновлен")
    
    # 2. Создаем requirements для multiprocessing (уже есть в Python)
    print("   ✅ multiprocessing уже встроен в Python")
    
    print("🔧 Интеграция завершена!")

def create_migration_guide():
    """Создает руководство по миграции"""
    guide_content = '''# РУКОВОДСТВО ПО МИГРАЦИИ НА НОВУЮ СИСТЕМУ СИНХРОНИЗАЦИИ

## Что изменилось

Старая система использовала:
- Файловый кеш (data/shared_access_cache.json)
- Обновление каждые 30 секунд
- Возможные задержки синхронизации

Новая система использует:
- multiprocessing.Manager для shared state
- Event-driven архитектуру
- МГНОВЕННУЮ синхронизацию

## Как использовать

Все функции остались теми же:
- has_access(user_id) - проверка доступа
- add_user_access(user_id, user_data) - добавление пользователя
- remove_user_access(user_id) - удаление пользователя
- delete_user_completely(user_id) - полное удаление

## Преимущества

✅ Мгновенная синхронизация между процессами
✅ Нет файловых операций
✅ Event-driven архитектура
✅ Автоматический fallback на старую систему
✅ Thread-safe операции

## Тестирование

Запустите тест:
```
python test_new_sync_systems.py
```

## Мониторинг

Проверить статистику:
```python
from multiprocessing_access_sync import get_mp_sync
sync = get_mp_sync()
print(sync.get_stats())
```

## Rollback

Если что-то не работает, восстановите файлы из backup_old_sync/
'''
    
    with open("MIGRATION_GUIDE.md", "w", encoding="utf-8") as f:
        f.write(guide_content)
    
    print("📖 Создано руководство: MIGRATION_GUIDE.md")

def main():
    print("🚀 ИНТЕГРАЦИЯ НОВОЙ СИСТЕМЫ СИНХРОНИЗАЦИИ")
    print("=" * 80)
    
    # Проверяем что файлы на месте
    required_files = ["multiprocessing_access_sync.py", "utils/access_manager.py"]
    for file_path in required_files:
        if not os.path.exists(file_path):
            print(f"❌ Отсутствует файл: {file_path}")
            return
    
    # Создаем бэкап
    backup_old_system()
    
    # Интегрируем новую систему
    integrate_multiprocessing_sync()
    
    # Создаем руководство
    create_migration_guide()
    
    print(f"\n🎉 ИНТЕГРАЦИЯ ЗАВЕРШЕНА!")
    print(f"📖 Читайте MIGRATION_GUIDE.md для деталей")
    print(f"🧪 Протестируйте: python test_new_sync_systems.py")
    print(f"🔄 Перезапустите боты для применения изменений")

if __name__ == "__main__":
    main() 
"""
ИНТЕГРАЦИЯ НОВОЙ СИСТЕМЫ СИНХРОНИЗАЦИИ
Заменяет старую файловую систему на modern event-driven multiprocessing
"""

import os
import shutil
import sys

def backup_old_system():
    """Создает бэкап старой системы"""
    print("📦 Создаем бэкап старой системы...")
    
    files_to_backup = [
        "utils/access_manager.py",
        "telegram_bot/handlers.py",
        "admin_bot/handlers/user_handlers.py"
    ]
    
    backup_dir = "backup_old_sync"
    os.makedirs(backup_dir, exist_ok=True)
    
    for file_path in files_to_backup:
        if os.path.exists(file_path):
            backup_path = os.path.join(backup_dir, os.path.basename(file_path))
            shutil.copy2(file_path, backup_path)
            print(f"   ✅ {file_path} → {backup_path}")
    
    print(f"📦 Бэкап создан в {backup_dir}/")

def integrate_multiprocessing_sync():
    """Интегрирует multiprocessing синхронизацию"""
    print("🔧 Интегрируем multiprocessing синхронизацию...")
    
    # 1. Обновляем utils/access_manager.py
    access_manager_patch = '''
# НОВАЯ СИСТЕМА СИНХРОНИЗАЦИИ
# Импортируем multiprocessing синхронизацию
try:
    from multiprocessing_access_sync import has_access_mp, add_user_mp, remove_user_mp, get_mp_sync
    USE_NEW_SYNC = True
    print("🟢 Используется новая multiprocessing синхронизация")
except ImportError:
    USE_NEW_SYNC = False
    print("🔴 Fallback на старую систему синхронизации")

def has_access(telegram_id: int) -> bool:
    """Проверяет доступ пользователя (новая система)"""
    if USE_NEW_SYNC:
        return has_access_mp(telegram_id)
    else:
        # Fallback на старую систему
        return get_access_manager().has_access(telegram_id)

def add_user_access(telegram_id: int, user_data: dict = None) -> bool:
    """Добавляет доступ пользователю (новая система)"""
    if USE_NEW_SYNC:
        if user_data is None:
            from datetime import datetime, timedelta
            user_data = {
                'telegram_id': telegram_id,
                'is_active': True,
                'subscription_end': (datetime.now() + timedelta(days=30)).isoformat(),
                'role': 'trial'
            }
        return add_user_mp(telegram_id, user_data)
    else:
        # Fallback на старую систему
        return get_access_manager().add_user(telegram_id)

def remove_user_access(telegram_id: int) -> bool:
    """Удаляет доступ пользователя (новая система)"""
    if USE_NEW_SYNC:
        return remove_user_mp(telegram_id)
    else:
        # Fallback на старую систему
        return get_access_manager().remove_user(telegram_id)

def delete_user_completely(telegram_id: int) -> bool:
    """Полностью удаляет пользователя (новая система)"""
    if USE_NEW_SYNC:
        return remove_user_mp(telegram_id)
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
    """Принудительная синхронизация (новая система)"""
    if USE_NEW_SYNC:
        # В новой системе синхронизация автоматическая
        sync = get_mp_sync()
        stats = sync.get_stats()
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
'''
    
    # Добавляем патч в конец файла access_manager.py
    with open("utils/access_manager.py", "a", encoding="utf-8") as f:
        f.write("\n\n# === НОВАЯ СИСТЕМА СИНХРОНИЗАЦИИ ===\n")
        f.write(access_manager_patch)
    
    print("   ✅ utils/access_manager.py обновлен")
    
    # 2. Создаем requirements для multiprocessing (уже есть в Python)
    print("   ✅ multiprocessing уже встроен в Python")
    
    print("🔧 Интеграция завершена!")

def create_migration_guide():
    """Создает руководство по миграции"""
    guide_content = '''# РУКОВОДСТВО ПО МИГРАЦИИ НА НОВУЮ СИСТЕМУ СИНХРОНИЗАЦИИ

## Что изменилось

Старая система использовала:
- Файловый кеш (data/shared_access_cache.json)
- Обновление каждые 30 секунд
- Возможные задержки синхронизации

Новая система использует:
- multiprocessing.Manager для shared state
- Event-driven архитектуру
- МГНОВЕННУЮ синхронизацию

## Как использовать

Все функции остались теми же:
- has_access(user_id) - проверка доступа
- add_user_access(user_id, user_data) - добавление пользователя
- remove_user_access(user_id) - удаление пользователя
- delete_user_completely(user_id) - полное удаление

## Преимущества

✅ Мгновенная синхронизация между процессами
✅ Нет файловых операций
✅ Event-driven архитектура
✅ Автоматический fallback на старую систему
✅ Thread-safe операции

## Тестирование

Запустите тест:
```
python test_new_sync_systems.py
```

## Мониторинг

Проверить статистику:
```python
from multiprocessing_access_sync import get_mp_sync
sync = get_mp_sync()
print(sync.get_stats())
```

## Rollback

Если что-то не работает, восстановите файлы из backup_old_sync/
'''
    
    with open("MIGRATION_GUIDE.md", "w", encoding="utf-8") as f:
        f.write(guide_content)
    
    print("📖 Создано руководство: MIGRATION_GUIDE.md")

def main():
    print("🚀 ИНТЕГРАЦИЯ НОВОЙ СИСТЕМЫ СИНХРОНИЗАЦИИ")
    print("=" * 80)
    
    # Проверяем что файлы на месте
    required_files = ["multiprocessing_access_sync.py", "utils/access_manager.py"]
    for file_path in required_files:
        if not os.path.exists(file_path):
            print(f"❌ Отсутствует файл: {file_path}")
            return
    
    # Создаем бэкап
    backup_old_system()
    
    # Интегрируем новую систему
    integrate_multiprocessing_sync()
    
    # Создаем руководство
    create_migration_guide()
    
    print(f"\n🎉 ИНТЕГРАЦИЯ ЗАВЕРШЕНА!")
    print(f"📖 Читайте MIGRATION_GUIDE.md для деталей")
    print(f"🧪 Протестируйте: python test_new_sync_systems.py")
    print(f"🔄 Перезапустите боты для применения изменений")

if __name__ == "__main__":
    main() 
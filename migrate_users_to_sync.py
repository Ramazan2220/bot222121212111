#!/usr/bin/env python3
"""
Скрипт для миграции пользователей из админ панели в файловую систему синхронизации
"""

import sys
import os
import json
from datetime import datetime, timedelta

# Добавляем пути
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), 'admin_bot'))

def main():
    print("🔄 МИГРАЦИЯ ПОЛЬЗОВАТЕЛЕЙ В ФАЙЛОВУЮ СИНХРОНИЗАЦИЮ")
    print("=" * 60)
    
    # Импортируем админ панель
    try:
        from admin_bot.services.user_service import UserService
        from admin_bot.models.user import UserStatus
        print("✅ Импорт админ панели успешен")
    except Exception as e:
        print(f"❌ Ошибка импорта админ панели: {e}")
        return False
    
    # Импортируем файловую синхронизацию
    try:
        from file_access_sync import get_file_sync
        sync = get_file_sync()
        print("✅ Импорт файловой синхронизации успешен")
    except Exception as e:
        print(f"❌ Ошибка импорта файловой синхронизации: {e}")
        return False
    
    # Получаем пользователей из админ панели
    user_service = UserService()
    all_users = user_service.get_all_users()
    
    print(f"📊 Найдено пользователей в админ панели: {len(all_users)}")
    
    if not all_users:
        print("⚠️ Пользователи в админ панели не найдены")
        return True
    
    # Мигрируем каждого пользователя
    migrated_count = 0
    for user in all_users:
        try:
            # Подготавливаем данные пользователя
            user_data = {
                'telegram_id': user.telegram_id,
                'username': user.username or '',
                'is_active': user.status in [UserStatus.ACTIVE, UserStatus.TRIAL],
                'subscription_end': user.subscription_end.isoformat() if user.subscription_end else (datetime.now() + timedelta(days=30)).isoformat(),
                'role': user.subscription_plan.value if user.subscription_plan else 'trial',
                'migrated_at': datetime.now().isoformat(),
                'migrated_from': 'admin_panel'
            }
            
            # Добавляем в файловую синхронизацию
            success = sync.add_user(user.telegram_id, user_data)
            
            if success:
                print(f"✅ Мигрирован: {user.telegram_id} ({user.username}) - {user.status.value}")
                migrated_count += 1
            else:
                print(f"❌ Ошибка миграции: {user.telegram_id} ({user.username})")
                
        except Exception as e:
            print(f"❌ Ошибка обработки пользователя {user.telegram_id}: {e}")
    
    print("=" * 60)
    print(f"🎉 МИГРАЦИЯ ЗАВЕРШЕНА: {migrated_count}/{len(all_users)} пользователей")
    
    # Проверяем результат
    stats = sync.get_stats()
    print(f"📊 Статистика файловой синхронизации: {stats}")
    
    return True

if __name__ == "__main__":
    success = main()
    if success:
        print("✅ Миграция успешно завершена!")
    else:
        print("❌ Миграция завершена с ошибками!")
    
    sys.exit(0 if success else 1) 
"""
Скрипт для миграции пользователей из админ панели в файловую систему синхронизации
"""

import sys
import os
import json
from datetime import datetime, timedelta

# Добавляем пути
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), 'admin_bot'))

def main():
    print("🔄 МИГРАЦИЯ ПОЛЬЗОВАТЕЛЕЙ В ФАЙЛОВУЮ СИНХРОНИЗАЦИЮ")
    print("=" * 60)
    
    # Импортируем админ панель
    try:
        from admin_bot.services.user_service import UserService
        from admin_bot.models.user import UserStatus
        print("✅ Импорт админ панели успешен")
    except Exception as e:
        print(f"❌ Ошибка импорта админ панели: {e}")
        return False
    
    # Импортируем файловую синхронизацию
    try:
        from file_access_sync import get_file_sync
        sync = get_file_sync()
        print("✅ Импорт файловой синхронизации успешен")
    except Exception as e:
        print(f"❌ Ошибка импорта файловой синхронизации: {e}")
        return False
    
    # Получаем пользователей из админ панели
    user_service = UserService()
    all_users = user_service.get_all_users()
    
    print(f"📊 Найдено пользователей в админ панели: {len(all_users)}")
    
    if not all_users:
        print("⚠️ Пользователи в админ панели не найдены")
        return True
    
    # Мигрируем каждого пользователя
    migrated_count = 0
    for user in all_users:
        try:
            # Подготавливаем данные пользователя
            user_data = {
                'telegram_id': user.telegram_id,
                'username': user.username or '',
                'is_active': user.status in [UserStatus.ACTIVE, UserStatus.TRIAL],
                'subscription_end': user.subscription_end.isoformat() if user.subscription_end else (datetime.now() + timedelta(days=30)).isoformat(),
                'role': user.subscription_plan.value if user.subscription_plan else 'trial',
                'migrated_at': datetime.now().isoformat(),
                'migrated_from': 'admin_panel'
            }
            
            # Добавляем в файловую синхронизацию
            success = sync.add_user(user.telegram_id, user_data)
            
            if success:
                print(f"✅ Мигрирован: {user.telegram_id} ({user.username}) - {user.status.value}")
                migrated_count += 1
            else:
                print(f"❌ Ошибка миграции: {user.telegram_id} ({user.username})")
                
        except Exception as e:
            print(f"❌ Ошибка обработки пользователя {user.telegram_id}: {e}")
    
    print("=" * 60)
    print(f"🎉 МИГРАЦИЯ ЗАВЕРШЕНА: {migrated_count}/{len(all_users)} пользователей")
    
    # Проверяем результат
    stats = sync.get_stats()
    print(f"📊 Статистика файловой синхронизации: {stats}")
    
    return True

if __name__ == "__main__":
    success = main()
    if success:
        print("✅ Миграция успешно завершена!")
    else:
        print("❌ Миграция завершена с ошибками!")
    
    sys.exit(0 if success else 1) 
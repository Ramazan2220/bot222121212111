#!/usr/bin/env python3
"""
Утилита для сброса статусов проблемных аккаунтов
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database.db_manager import init_db, get_session, update_instagram_account
from database.models import InstagramAccount
from datetime import datetime

def reset_problematic_accounts():
    """Сбрасывает статусы всех проблемных аккаунтов"""
    
    print("🔄 СБРОС СТАТУСОВ ПРОБЛЕМНЫХ АККАУНТОВ")
    print("=" * 50)
    
    # Инициализируем базу данных
    init_db()
    
    session = get_session()
    
    try:
        # Находим все проблемные аккаунты
        problematic_accounts = session.query(InstagramAccount).filter(
            InstagramAccount.status == 'problematic'
        ).all()
        
        if not problematic_accounts:
            print("✅ Проблемных аккаунтов не найдено!")
            return
        
        print(f"📋 Найдено {len(problematic_accounts)} проблемных аккаунтов:")
        
        for account in problematic_accounts:
            print(f"  - @{account.username} (ID: {account.id})")
        
        # Запрашиваем подтверждение
        response = input(f"\n❓ Сбросить статус для {len(problematic_accounts)} аккаунтов? (y/N): ")
        
        if response.lower() != 'y':
            print("❌ Операция отменена")
            return
        
        # Сбрасываем статусы
        reset_count = 0
        for account in problematic_accounts:
            try:
                update_instagram_account(
                    account.id,
                    status='active',
                    is_active=True,
                    last_error=None,
                    last_check=datetime.now()
                )
                print(f"✅ Сброшен статус для @{account.username}")
                reset_count += 1
            except Exception as e:
                print(f"❌ Ошибка при сбросе @{account.username}: {e}")
        
        print(f"\n🎉 Успешно сброшено статусов: {reset_count}/{len(problematic_accounts)}")
        
    except Exception as e:
        print(f"❌ Ошибка при работе с базой данных: {e}")
    finally:
        session.close()

def list_problematic_accounts():
    """Показывает список проблемных аккаунтов"""
    
    print("📋 СПИСОК ПРОБЛЕМНЫХ АККАУНТОВ")
    print("=" * 50)
    
    # Инициализируем базу данных
    init_db()
    
    session = get_session()
    
    try:
        # Находим все проблемные аккаунты
        problematic_accounts = session.query(InstagramAccount).filter(
            InstagramAccount.status == 'problematic'
        ).all()
        
        if not problematic_accounts:
            print("✅ Проблемных аккаунтов не найдено!")
            return
        
        print(f"Найдено {len(problematic_accounts)} проблемных аккаунтов:\n")
        
        for i, account in enumerate(problematic_accounts, 1):
            print(f"{i:2d}. @{account.username}")
            print(f"     ID: {account.id}")
            print(f"     Email: {account.email}")
            print(f"     Последняя ошибка: {account.last_error or 'Не указана'}")
            print(f"     Последняя проверка: {account.last_check or 'Никогда'}")
            print()
        
    except Exception as e:
        print(f"❌ Ошибка при работе с базой данных: {e}")
    finally:
        session.close()

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--list":
        list_problematic_accounts()
    else:
        reset_problematic_accounts() 
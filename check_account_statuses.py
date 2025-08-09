#!/usr/bin/env python3
"""
Скрипт для проверки статусов аккаунтов Instagram
Показывает какие аккаунты имеют проблемы с почтой или другие проблемы
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database.db_manager import init_db, get_instagram_accounts
from datetime import datetime

def check_account_statuses():
    """Проверяет и отображает статусы всех аккаунтов"""
    
    print("🔍 ПРОВЕРКА СТАТУСОВ АККАУНТОВ")
    print("=" * 50)
    
    # Инициализируем базу данных
    init_db()
    
    # Получаем все аккаунты
    accounts = get_instagram_accounts()
    
    if not accounts:
        print("❌ В базе данных нет аккаунтов!")
        return
    
    print(f"📊 Найдено аккаунтов: {len(accounts)}")
    print()
    
    # Группируем аккаунты по статусам
    status_groups = {}
    
    for account in accounts:
        status = getattr(account, 'status', 'active')
        is_active = getattr(account, 'is_active', True)
        last_error = getattr(account, 'last_error', None)
        last_check = getattr(account, 'last_check', None)
        
        # Определяем реальный статус
        if not is_active:
            if status in ['email_timeout', 'email_failed', 'imap_auth_failed', 'email_auth_failed']:
                real_status = f"❌ {status}"
            else:
                real_status = "❌ inactive"
        elif status == 'active':
            real_status = "✅ active"
        else:
            real_status = f"⚠️ {status}"
        
        if real_status not in status_groups:
            status_groups[real_status] = []
        
        status_groups[real_status].append({
            'account': account,
            'last_error': last_error,
            'last_check': last_check
        })
    
    # Отображаем статистику
    print("📈 СТАТИСТИКА ПО СТАТУСАМ:")
    print("-" * 30)
    
    for status, accounts_list in status_groups.items():
        print(f"{status}: {len(accounts_list)} аккаунтов")
    
    print()
    
    # Показываем детальную информацию по проблемным аккаунтам
    problematic_statuses = [s for s in status_groups.keys() if s.startswith("❌") or s.startswith("⚠️")]
    
    if problematic_statuses:
        print("🚨 ПРОБЛЕМНЫЕ АККАУНТЫ:")
        print("-" * 30)
        
        for status in problematic_statuses:
            accounts_list = status_groups[status]
            print(f"\n{status} ({len(accounts_list)} аккаунтов):")
            
            for item in accounts_list:
                account = item['account']
                last_error = item['last_error']
                last_check = item['last_check']
                
                print(f"  • {account.username} (ID: {account.id})")
                print(f"    Email: {account.email}")
                
                if last_error:
                    error_preview = last_error[:100] + "..." if len(last_error) > 100 else last_error
                    print(f"    Ошибка: {error_preview}")
                
                if last_check:
                    print(f"    Последняя проверка: {last_check}")
                
                print()
    
    # Показываем активные аккаунты
    active_accounts = status_groups.get("✅ active", [])
    if active_accounts:
        print(f"✅ АКТИВНЫЕ АККАУНТЫ ({len(active_accounts)}):")
        print("-" * 30)
        
        for item in active_accounts[:10]:  # Показываем первые 10
            account = item['account']
            print(f"  • {account.username} - {account.email}")
        
        if len(active_accounts) > 10:
            print(f"  ... и еще {len(active_accounts) - 10} аккаунтов")
    
    print()
    print("💡 РЕКОМЕНДАЦИИ:")
    print("-" * 30)
    
    email_problem_accounts = []
    for status in status_groups:
        if 'email' in status.lower():
            email_problem_accounts.extend(status_groups[status])
    
    if email_problem_accounts:
        print(f"📧 {len(email_problem_accounts)} аккаунтов имеют проблемы с почтой")
        print("   Рекомендуется проверить настройки email или заменить email-адреса")
    
    inactive_accounts = []
    for status in status_groups:
        if status.startswith("❌"):
            inactive_accounts.extend(status_groups[status])
    
    if inactive_accounts:
        print(f"🔄 {len(inactive_accounts)} аккаунтов неактивны")
        print("   Рекомендуется провести повторную валидацию или удалить проблемные аккаунты")

if __name__ == "__main__":
    check_account_statuses() 
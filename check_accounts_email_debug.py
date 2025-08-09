#!/usr/bin/env python3
"""
Проверка email настроек для проблемных аккаунтов
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database.db_manager import get_instagram_account

def check_accounts_email():
    """Проверяем email настройки для проблемных аккаунтов"""
    
    # Проблемные аккаунты из логов
    problem_accounts = [
        {"id": 6, "username": "pagehank302073"},
        {"id": 7, "username": "fischercarmen3096194"}, 
        {"id": 22, "username": "meanthony_21260"}  # Этот работает
    ]
    
    print("🔍 ПРОВЕРКА EMAIL НАСТРОЕК:")
    print("=" * 50)
    
    for acc_info in problem_accounts:
        account_id = acc_info["id"]
        expected_username = acc_info["username"]
        
        account = get_instagram_account(account_id)
        
        if not account:
            print(f"❌ ID {account_id}: Аккаунт не найден")
            continue
            
        print(f"\n📱 ID {account_id}: @{account.username}")
        
        if account.username != expected_username:
            print(f"   ⚠️ Внимание: ожидался @{expected_username}")
        
        # Проверяем email данные
        has_email = account.email and account.email.strip()
        has_email_pass = account.email_password and account.email_password.strip()
        
        if has_email:
            print(f"   ✅ Email: {account.email}")
        else:
            print(f"   ❌ Email: НЕТ")
            
        if has_email_pass:
            print(f"   ✅ Email пароль: ЕСТЬ ({len(account.email_password)} символов)")
        else:
            print(f"   ❌ Email пароль: НЕТ")
            
        # Статус для IMAP восстановления
        if has_email and has_email_pass:
            print(f"   🔐 IMAP восстановление: ВКЛЮЧЕНО")
        else:
            print(f"   🚫 IMAP восстановление: ОТКЛЮЧЕНО")
    
    print("\n" + "=" * 50)
    print("💡 ВЫВОД:")
    print("   Аккаунты БЕЗ email данных не смогут автоматически")
    print("   восстанавливаться при challenge запросах!")

if __name__ == "__main__":
    check_accounts_email() 
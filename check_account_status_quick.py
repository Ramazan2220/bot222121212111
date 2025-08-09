import sys
from database.db_manager import get_instagram_account
from services.instagram_service import instagram_service
from instagram.client import test_instagram_login_with_proxy

def check_account_quick(account_id):
    """Быстрая проверка статуса аккаунта"""
    account = get_instagram_account(account_id)
    if not account:
        return False, "Аккаунт не найден в базе"
    
    print(f"\n🔍 Проверка аккаунта {account.username} (ID: {account_id})")
    print(f"📊 Статус в базе: {account.status}")
    
    # Проверяем логин
    print("🔐 Проверка входа...")
    password = instagram_service.get_decrypted_password(account_id)
    if not password:
        return False, "Не удалось получить пароль"
        
    success = test_instagram_login_with_proxy(account_id, account.username, password)
    
    if success:
        print(f"✅ Успешный вход")
        return True, "Аккаунт работает"
    else:
        print(f"❌ Ошибка входа")
        return False, "Ошибка входа"

if __name__ == "__main__":
    if len(sys.argv) > 1:
        account_id = int(sys.argv[1])
        check_account_quick(account_id)
    else:
        # Проверяем проблемные аккаунты из логов
        problem_accounts = [23, 25, 28, 29, 37, 42]
        
        print("Проверка проблемных аккаунтов из последнего прогрева:")
        for acc_id in problem_accounts:
            check_account_quick(acc_id)
            print("-" * 50) 
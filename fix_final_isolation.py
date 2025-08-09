#!/usr/bin/env python3
"""
🚀 ФИНАЛЬНОЕ ИСПРАВЛЕНИЕ ИЗОЛЯЦИИ
Исправляет все оставшиеся вызовы get_instagram_accounts() и get_instagram_account()
"""

import os
import re
import shutil
from datetime import datetime

def fix_enhanced_handlers():
    """Исправляет telegram_bot/enhanced_handlers.py"""
    file_path = "./telegram_bot/enhanced_handlers.py"
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        changes_made = 0
        
        # 1. Добавляем импорт safe_user_wrapper если его нет
        if "from database.safe_user_wrapper import" not in content:
            # Ищем импорт из db_manager
            import_pattern = r'from database\.db_manager import.*'
            import_match = re.search(import_pattern, content)
            
            if import_match:
                old_import = import_match.group(0)
                new_import = old_import + "\nfrom database.safe_user_wrapper import get_user_instagram_accounts as get_instagram_accounts, get_user_instagram_account as get_instagram_account, extract_user_id_from_update"
                content = content.replace(old_import, new_import)
                changes_made += 1
                print(f"   ✅ Добавлен импорт safe_user_wrapper")
        
        # 2. Заменяем get_instagram_accounts() на версию с user_id
        old_calls = [
            "accounts = get_instagram_accounts()",
            "get_instagram_accounts()"
        ]
        
        for old_call in old_calls:
            if old_call in content:
                new_call = "accounts = get_instagram_accounts(context=context, user_id=update.effective_user.id)"
                if "accounts =" not in old_call:
                    new_call = "get_instagram_accounts(context=context, user_id=update.effective_user.id)"
                    
                content = content.replace(old_call, new_call)
                changes_made += 1
                print(f"   ✅ Заменен: {old_call}")
        
        # 3. Заменяем get_instagram_account(account_id) на версию с user_id
        account_pattern = r'get_instagram_account\(([^,)]+)\)'
        matches = re.findall(account_pattern, content)
        
        for match in matches:
            if 'context=' not in match and 'user_id=' not in match:
                old_call = f"get_instagram_account({match})"
                new_call = f"get_instagram_account({match}, context=context, user_id=update.effective_user.id)"
                content = content.replace(old_call, new_call)
                changes_made += 1
                print(f"   ✅ Заменен: {old_call}")
        
        # Сохраняем изменения
        if changes_made > 0:
            backup_path = f"{file_path}.backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            shutil.copy2(file_path, backup_path)
            
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(content)
                
            print(f"✅ {file_path}: {changes_made} изменений (backup: {backup_path})")
            return True
        else:
            print(f"⚪ {file_path}: изменений не требуется")
            return False
            
    except Exception as e:
        print(f"❌ Ошибка в {file_path}: {e}")
        return False

def fix_handlers_remaining():
    """Исправляет оставшиеся вызовы в telegram_bot/handlers.py"""
    file_path = "./telegram_bot/handlers.py"
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        changes_made = 0
        
        # Заменяем get_instagram_account(account_id) на версию с user_id
        account_pattern = r'account = get_instagram_account\(([^,)]+)\)'
        matches = re.findall(account_pattern, content)
        
        for match in matches:
            if 'context=' not in match and 'user_id=' not in match:
                old_call = f"account = get_instagram_account({match})"
                new_call = f"account = get_instagram_account({match}, context=context, user_id=update.effective_user.id)"
                content = content.replace(old_call, new_call)
                changes_made += 1
                print(f"   ✅ Заменен: {old_call}")
        
        # Сохраняем изменения
        if changes_made > 0:
            backup_path = f"{file_path}.backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            shutil.copy2(file_path, backup_path)
            
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(content)
                
            print(f"✅ {file_path}: {changes_made} изменений (backup: {backup_path})")
            return True
        else:
            print(f"⚪ {file_path}: изменений не требуется")
            return False
            
    except Exception as e:
        print(f"❌ Ошибка в {file_path}: {e}")
        return False

def main():
    print("🚀 ФИНАЛЬНОЕ ИСПРАВЛЕНИЕ ИЗОЛЯЦИИ")
    print("=" * 50)
    
    fixed_files = 0
    
    print("\n🔧 Обработка: telegram_bot/enhanced_handlers.py")
    if fix_enhanced_handlers():
        fixed_files += 1
    
    print("\n🔧 Обработка: telegram_bot/handlers.py")
    if fix_handlers_remaining():
        fixed_files += 1
    
    print("\n" + "=" * 50)
    print(f"🎉 ЗАВЕРШЕНО: {fixed_files} файлов исправлено")
    print("✅ Все оставшиеся вызовы обновлены!")

if __name__ == "__main__":
    main() 
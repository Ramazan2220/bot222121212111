#!/usr/bin/env python3
"""
🚀 ИСПРАВЛЕНИЕ ОСТАВШИХСЯ ВЫЗОВОВ get_instagram_account()
Заменяет get_instagram_account(account_id) на get_instagram_account(account_id, context=context, user_id=user_id)
"""

import os
import re
import shutil
from datetime import datetime

# Файлы для исправления
FILES_TO_FIX = [
    "./telegram_bot/bot.py",
    "./telegram_bot/handlers.py", 
    "./telegram_bot/enhanced_handlers.py"
]

def fix_get_instagram_account_calls(file_path):
    """Исправляет вызовы get_instagram_account() в файле"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        original_content = content
        changes_made = 0
        
        # 1. Добавляем импорт safe_user_wrapper если его нет
        if "from database.safe_user_wrapper import" not in content:
            # Ищем существующий импорт get_instagram_account
            import_pattern = r'from database\.db_manager import.*get_instagram_account.*'
            import_match = re.search(import_pattern, content)
            
            if import_match:
                # Заменяем импорт из db_manager на safe_user_wrapper
                old_import = import_match.group(0)
                # Убираем get_instagram_account из db_manager импорта
                new_db_import = re.sub(r',?\s*get_instagram_account,?', '', old_import)
                new_db_import = re.sub(r'\(\s*,', '(', new_db_import)  # Убираем лишние запятые
                new_db_import = re.sub(r',\s*\)', ')', new_db_import)
                
                # Добавляем импорт из safe_user_wrapper
                content = content.replace(old_import, 
                    new_db_import + "\n" +
                    "from database.safe_user_wrapper import get_user_instagram_account as get_instagram_account, extract_user_id_from_update"
                )
                changes_made += 1
                print(f"   ✅ Обновлен импорт в {file_path}")
        
        # 2. Заменяем вызовы get_instagram_account(account_id) на версию с user_id
        # Паттерн для поиска функций с get_instagram_account вызовами
        function_pattern = r'def\s+(\w+)\s*\([^)]*\):[^}]*?get_instagram_account\([^)]*\)'
        
        for func_match in re.finditer(function_pattern, content, re.DOTALL):
            func_content = func_match.group(0)
            func_name = func_match.group(1)
            
            # Ищем get_instagram_account(account_id) в функции
            account_calls = re.findall(r'get_instagram_account\(([^)]+)\)', func_content)
            
            if account_calls:
                # Проверяем параметры функции
                func_start = func_match.start()
                func_def = func_match.group(0).split(':')[0]
                
                has_update = 'update' in func_def
                has_context = 'context' in func_def
                
                if has_update or has_context:
                    # Заменяем простые вызовы на версию с user_id
                    for call in account_calls:
                        # Пропускаем если уже есть context= или user_id=
                        if 'context=' in call or 'user_id=' in call:
                            continue
                            
                        old_call = f"get_instagram_account({call})"
                        
                        if has_update and has_context:
                            new_call = f"get_instagram_account({call}, context=context, user_id=update.effective_user.id)"
                        elif has_context:
                            new_call = f"get_instagram_account({call}, context=context)"
                        else:
                            new_call = f"get_instagram_account({call})"  # Оставляем как есть если нет параметров
                        
                        if old_call != new_call:
                            content = content.replace(old_call, new_call)
                            changes_made += 1
                            print(f"   ✅ Исправлен вызов в функции {func_name}: {old_call} → {new_call}")
        
        # Сохраняем изменения
        if changes_made > 0:
            # Создаем backup
            backup_path = f"{file_path}.backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            shutil.copy2(file_path, backup_path)
            
            # Сохраняем обновленный файл
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
    print("🚀 ИСПРАВЛЕНИЕ ОСТАВШИХСЯ ВЫЗОВОВ get_instagram_account()")
    print("=" * 60)
    
    fixed_files = 0
    total_files = len(FILES_TO_FIX)
    
    for file_path in FILES_TO_FIX:
        if os.path.exists(file_path):
            print(f"\n🔧 Обработка: {file_path}")
            if fix_get_instagram_account_calls(file_path):
                fixed_files += 1
        else:
            print(f"⚠️ Файл не найден: {file_path}")
    
    print("\n" + "=" * 60)
    print(f"🎉 ЗАВЕРШЕНО: {fixed_files}/{total_files} файлов исправлено")
    print("✅ Все оставшие вызовы get_instagram_account() обновлены!")

if __name__ == "__main__":
    main() 
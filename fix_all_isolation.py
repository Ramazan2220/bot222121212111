#!/usr/bin/env python3
"""
🚀 АВТОМАТИЧЕСКОЕ ИСПРАВЛЕНИЕ ИЗОЛЯЦИИ ПОЛЬЗОВАТЕЛЕЙ
Заменяет все вызовы get_instagram_accounts() и get_instagram_account() 
на безопасные версии с user_id
"""

import os
import re
import shutil
from datetime import datetime

# Список файлов для исправления (только ключевые telegram_bot файлы)
FILES_TO_FIX = [
    "./telegram_bot/handlers.py",
    "./telegram_bot/handlers/group_handlers.py", 
    "./telegram_bot/handlers/publish/posts/handlers.py",
    "./telegram_bot/handlers/publish/posts.py",
    "./telegram_bot/handlers/publish/reels/handlers.py",
    "./telegram_bot/handlers/publish/stories/handlers.py",
    "./telegram_bot/handlers/publish/common.py",
    "./telegram_bot/handlers/analytics_handlers.py",
    "./telegram_bot/handlers/publish_handlers.py",
]

def backup_file(filepath):
    """Создает резервную копию файла"""
    backup_path = f"{filepath}.backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    shutil.copy2(filepath, backup_path)
    print(f"✅ Backup: {backup_path}")
    return backup_path

def add_import_if_needed(content):
    """Добавляет импорт безопасных функций если его нет"""
    if "from database.safe_user_wrapper import" in content:
        return content
    
    # Ищем существующий импорт db_manager
    db_import_pattern = r'from database\.db_manager import \([^)]+\)'
    match = re.search(db_import_pattern, content, re.DOTALL)
    
    if match:
        # Заменяем импорт, убирая get_instagram_accounts и get_instagram_account
        old_import = match.group(0)
        new_import = old_import.replace(', get_instagram_accounts', '').replace('get_instagram_accounts, ', '').replace('get_instagram_accounts', '')
        new_import = new_import.replace(', get_instagram_account', '').replace('get_instagram_account, ', '').replace('get_instagram_account', '')
        
        # Добавляем новый импорт
        safe_import = "\nfrom database.safe_user_wrapper import get_user_instagram_accounts as get_instagram_accounts, get_user_instagram_account as get_instagram_account, extract_user_id_from_update"
        
        content = content.replace(old_import, new_import + safe_import)
        print("📦 Добавлен безопасный импорт")
    
    return content

def fix_function_calls(content):
    """Исправляет вызовы функций"""
    changes = 0
    
    # Паттерн для поиска функций-обработчиков
    handler_pattern = r'def\s+(\w+)\s*\([^)]*update[^)]*context[^)]*\):'
    handlers = re.findall(handler_pattern, content)
    
    for handler_name in handlers:
        # Ищем вызовы get_instagram_accounts() в этой функции
        func_start = content.find(f"def {handler_name}")
        if func_start == -1:
            continue
            
        # Находим конец функции (следующий def или конец файла)
        next_def = content.find("\ndef ", func_start + 1)
        func_end = next_def if next_def != -1 else len(content)
        
        func_content = content[func_start:func_end]
        
        # Проверяем есть ли уже user_id = extract_user_id_from_update
        if "user_id = extract_user_id_from_update" in func_content:
            continue
            
        # Ищем get_instagram_accounts() в функции
        if "get_instagram_accounts()" in func_content:
            # Добавляем извлечение user_id после определения функции
            func_def_end = func_content.find('"""', func_content.find('"""') + 3)
            if func_def_end == -1:
                func_def_end = func_content.find('\n', func_content.find('):')) + 1
            else:
                func_def_end = func_content.find('\n', func_def_end) + 1
            
            user_id_code = "    user_id = extract_user_id_from_update(update, context)\n"
            new_func_content = func_content[:func_def_end] + user_id_code + func_content[func_def_end:]
            
            # Заменяем get_instagram_accounts() на версию с параметрами
            new_func_content = new_func_content.replace(
                "get_instagram_accounts()",
                "get_instagram_accounts(context=context, user_id=user_id)"
            )
            
            # Заменяем get_instagram_account( на версию с user_id
            new_func_content = re.sub(
                r'get_instagram_account\((\d+)\)',
                r'get_instagram_account(\1, context=context, user_id=user_id)',
                new_func_content
            )
            
            content = content.replace(func_content, new_func_content)
            changes += 1
            print(f"🔧 Исправлен обработчик: {handler_name}")
    
    return content, changes

def fix_file(filepath):
    """Исправляет один файл"""
    print(f"\n🔧 Обрабатываю: {filepath}")
    
    if not os.path.exists(filepath):
        print(f"⚠️ Файл не найден: {filepath}")
        return False
    
    # Читаем файл
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Создаем backup
    backup_path = backup_file(filepath)
    
    # Исправляем
    original_content = content
    content = add_import_if_needed(content)
    content, changes = fix_function_calls(content)
    
    if content != original_content:
        # Сохраняем исправленный файл
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"✅ Исправлено {changes} функций в {filepath}")
        return True
    else:
        print(f"ℹ️ Нет изменений в {filepath}")
        # Удаляем ненужный backup
        os.remove(backup_path)
        return False

def main():
    """Основная функция"""
    print("🚀 АВТОМАТИЧЕСКОЕ ИСПРАВЛЕНИЕ ИЗОЛЯЦИИ ПОЛЬЗОВАТЕЛЕЙ")
    print("=" * 60)
    
    fixed_files = 0
    total_files = len(FILES_TO_FIX)
    
    for filepath in FILES_TO_FIX:
        if fix_file(filepath):
            fixed_files += 1
    
    print("\n" + "=" * 60)
    print(f"📊 РЕЗУЛЬТАТ: {fixed_files}/{total_files} файлов исправлено")
    print("🎉 Изоляция пользователей настроена автоматически!")
    
    if fixed_files > 0:
        print("\n⚠️ ВАЖНО: Перезапустите бота для применения изменений!")

if __name__ == "__main__":
    main() 
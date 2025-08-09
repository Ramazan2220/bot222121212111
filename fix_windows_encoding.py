#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Windows Encoding Fix - заменяет эмодзи на текст для совместимости с Windows cmd
"""

import os
import re
from pathlib import Path

# Словарь замен эмодзи на текст
EMOJI_REPLACEMENTS = {
    '🗄️': '[DB]',
    '🚀': '[START]', 
    '✅': '[OK]',
    '📝': '[LOG]',
    '🏊‍♂️': '[POOL]',
    '📦': '[PKG]',
    '🖥️': '[SYS]',
    '❌': '[ERR]',
    '⏰': '[WAIT]',
    '🔗': '[LINK]',
    '🛡️': '[SEC]',
    '💾': '[SAVE]',
    '⚡': '[FAST]',
    '💿': '[DISK]',
    '📊': '[STATS]',
    '🔥': '[HOT]',
    '💤': '[SLEEP]',
    '👥': '[USERS]',
    '🔄': '[RETRY]',
    '⚙️': '[SYS]',
    '📈': '[GROW]',
    '📤': '[OUT]',
    '🎨': '[DESIGN]',
    '📋': '[LIST]',
    '🔍': '[SEARCH]',
    '🔧': '[TOOL]',
    '🐧': '[LINUX]',
    '💻': '[WIN]',
    '🔒': '[LOCK]',
}

def replace_emoji_in_file(file_path):
    """Заменяет эмодзи в файле на текстовые аналоги"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        original_content = content
        
        # Применяем замены
        for emoji, replacement in EMOJI_REPLACEMENTS.items():
            content = content.replace(emoji, replacement)
        
        # Сохраняем только если были изменения
        if content != original_content:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(content)
            print(f"[OK] Исправлен: {file_path}")
            return True
        
        return False
        
    except Exception as e:
        print(f"[ERR] Ошибка в {file_path}: {e}")
        return False

def fix_main_files():
    """Исправляет основные файлы с эмодзи"""
    files_to_fix = [
        'main.py',
        'database/connection_pool.py',
        'instagram/client_pool.py',
        'utils/system_monitor.py',
        'utils/task_queue.py',
        'instagram/lazy_client_factory.py',
        'instagram/client_adapter.py',
        'utils/smart_validator_service.py'
    ]
    
    fixed_count = 0
    for file_path in files_to_fix:
        if os.path.exists(file_path):
            if replace_emoji_in_file(file_path):
                fixed_count += 1
        else:
            print(f"[SKIP] Файл не найден: {file_path}")
    
    print(f"\n[SUMMARY] Исправлено файлов: {fixed_count}")
    return fixed_count

if __name__ == "__main__":
    print("[START] Исправление кодировки для Windows...")
    print("[LOG] Заменяем эмодзи на текстовые аналоги...")
    
    fixed = fix_main_files()
    
    if fixed > 0:
        print(f"[OK] Готово! Исправлено {fixed} файлов.")
        print("[LOG] Теперь логи не будут вызывать UnicodeEncodeError в Windows")
    else:
        print("[LOG] Изменений не требуется") 
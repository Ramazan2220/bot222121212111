#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для очистки DEBUG логов из кода
"""

import os
import re
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def clean_debug_logs(directory="."):
    """Очистить DEBUG логи из всех Python файлов"""
    
    patterns_to_remove = [
        r'print\(f?\[?"?\[?DEBUG\]?.*?\)',  # print(f"[DEBUG]...")
        r'logger\.debug\(.*?\)',  # logger.debug(...)
        r'logging\.debug\(.*?\)',  # logging.debug(...)
    ]
    
    files_processed = 0
    lines_removed = 0
    
    for root, dirs, files in os.walk(directory):
        # Пропускаем виртуальные окружения и системные папки
        dirs[:] = [d for d in dirs if d not in ['.venv', 'venv', '__pycache__', '.git', 'bot_env_working']]
        
        for file in files:
            if file.endswith('.py'):
                filepath = os.path.join(root, file)
                
                try:
                    with open(filepath, 'r', encoding='utf-8') as f:
                        content = f.read()
                    
                    original_content = content
                    
                    # Удаляем DEBUG строки
                    for pattern in patterns_to_remove:
                        matches = len(re.findall(pattern, content, re.MULTILINE | re.DOTALL))
                        if matches > 0:
                            content = re.sub(pattern + r'\s*\n?', '', content, flags=re.MULTILINE | re.DOTALL)
                            lines_removed += matches
                    
                    # Сохраняем только если были изменения
                    if content != original_content:
                        with open(filepath, 'w', encoding='utf-8') as f:
                            f.write(content)
                        files_processed += 1
                        logger.info(f"✅ Очищен файл: {filepath}")
                        
                except Exception as e:
                    logger.error(f"❌ Ошибка обработки {filepath}: {e}")
    
    logger.info(f"\n📊 Результаты:")
    logger.info(f"   Обработано файлов: {files_processed}")
    logger.info(f"   Удалено DEBUG строк: {lines_removed}")

def convert_critical_debugs():
    """Конвертировать критически важные DEBUG в INFO"""
    
    # Список важных debug сообщений, которые нужно сохранить как INFO
    important_patterns = [
        (r'logger\.debug\((.*?[Ss]uccessful.*?)\)', r'logger.info(\1)'),
        (r'logger\.debug\((.*?[Ee]rror.*?)\)', r'logger.warning(\1)'),
        (r'logger\.debug\((.*?[Ff]ailed.*?)\)', r'logger.warning(\1)'),
    ]
    
    converted = 0
    
    for root, dirs, files in os.walk("."):
        dirs[:] = [d for d in dirs if d not in ['.venv', 'venv', '__pycache__', '.git', 'bot_env_working']]
        
        for file in files:
            if file.endswith('.py'):
                filepath = os.path.join(root, file)
                
                try:
                    with open(filepath, 'r', encoding='utf-8') as f:
                        content = f.read()
                    
                    original_content = content
                    
                    for pattern, replacement in important_patterns:
                        matches = len(re.findall(pattern, content))
                        if matches > 0:
                            content = re.sub(pattern, replacement, content)
                            converted += matches
                    
                    if content != original_content:
                        with open(filepath, 'w', encoding='utf-8') as f:
                            f.write(content)
                        
                except Exception as e:
                    logger.error(f"❌ Ошибка конвертации {filepath}: {e}")
    
    logger.info(f"   Конвертировано важных DEBUG в INFO: {converted}")

if __name__ == "__main__":
    print("🧹 Очистка DEBUG логов из проекта")
    print("⚠️  Это удалит все debug сообщения!")
    
    response = input("\nПродолжить? (yes/no): ")
    
    if response.lower() == 'yes':
        # Сначала конвертируем важные
        convert_critical_debugs()
        # Затем удаляем остальные
        clean_debug_logs()
    else:
        print("❌ Операция отменена") 
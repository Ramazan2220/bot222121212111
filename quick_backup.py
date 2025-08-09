#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Быстрый бэкап Instagram Bot проекта
Создает один ZIP архив со всем проектом
"""

import os
import zipfile
from datetime import datetime
from pathlib import Path
import sys

def quick_backup():
    """Создает быстрый бэкап всего проекта"""
    # Текущая директория
    project_dir = Path.cwd()
    
    # Директория для бэкапов
    backup_dir = project_dir.parent / 'instagram_bot_backups'
    backup_dir.mkdir(exist_ok=True)
    
    # Имя архива
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_file = backup_dir / f'instagram_bot_full_backup_{timestamp}.zip'
    
    print(f"🔄 Создание полного бэкапа проекта...")
    print(f"📁 Проект: {project_dir}")
    print(f"📦 Бэкап: {backup_file}\n")
    
    # Исключаемые директории
    exclude_dirs = {
        '.git', '__pycache__', 'venv', 'web_env', 'web_env_new',
        '.pytest_cache', 'node_modules', '.next', 'out'
    }
    
    file_count = 0
    
    # Создаем архив
    with zipfile.ZipFile(backup_file, 'w', zipfile.ZIP_DEFLATED) as zf:
        for root, dirs, files in os.walk(project_dir):
            # Исключаем ненужные директории
            dirs[:] = [d for d in dirs if d not in exclude_dirs]
            
            for file in files:
                # Пропускаем временные файлы
                if file.endswith(('.pyc', '.pyo', '.log', '.DS_Store')):
                    continue
                
                file_path = os.path.join(root, file)
                arcname = os.path.relpath(file_path, project_dir)
                
                try:
                    zf.write(file_path, arcname)
                    file_count += 1
                    
                    # Показываем прогресс
                    if file_count % 50 == 0:
                        print(f"  📄 Обработано файлов: {file_count}")
                        
                except Exception as e:
                    print(f"  ⚠️  Ошибка: {file}: {e}")
    
    # Размер архива
    size_mb = backup_file.stat().st_size / 1024 / 1024
    
    print(f"\n✅ Бэкап создан успешно!")
    print(f"📊 Статистика:")
    print(f"  - Файлов: {file_count}")
    print(f"  - Размер: {size_mb:.2f} MB")
    print(f"  - Путь: {backup_file}")
    
    # Создаем файл с информацией
    info_file = backup_dir / f'backup_info_{timestamp}.txt'
    with open(info_file, 'w', encoding='utf-8') as f:
        f.write(f"Instagram Bot Backup\n")
        f.write(f"{'='*30}\n")
        f.write(f"Дата: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Проект: {project_dir}\n")
        f.write(f"Файлов: {file_count}\n")
        f.write(f"Размер: {size_mb:.2f} MB\n")
        f.write(f"Архив: {backup_file.name}\n")

if __name__ == "__main__":
    try:
        quick_backup()
    except KeyboardInterrupt:
        print("\n❌ Прервано пользователем")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Ошибка: {e}")
        sys.exit(1) 
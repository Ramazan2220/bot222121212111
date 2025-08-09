#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Скрипт для создания резервной копии Instagram Bot проекта
Создает ZIP архив со всеми важными файлами проекта
"""

import os
import zipfile
import shutil
from datetime import datetime
from pathlib import Path

# Директории и файлы для исключения из бэкапа
EXCLUDE_DIRS = {
    '__pycache__',
    '.git',
    'venv',
    'web_env',
    'web_env_new',
    '.pytest_cache',
    '.idea',
    '.vscode',
    'node_modules',
    'instagram-automation-dashboard/node_modules',
    'instagram-automation-dashboard/.next',
    'instagram-automation-dashboard/out',
    'email_logs',  # Может содержать чувствительные данные
    'media',  # Медиа файлы могут быть большими
    'test_content',  # Тестовый контент
}

EXCLUDE_FILES = {
    '.DS_Store',
    'Thumbs.db',
    '*.pyc',
    '*.pyo',
    '*.log',
    'nul',
}

# Файлы с чувствительными данными для отдельного бэкапа
SENSITIVE_FILES = {
    'config.py',
    'warmup_settings.json',
    'data/accounts/',
    'data/user_agents.json',
    'devices/',
    'working_accounts/',
}

def should_exclude(path):
    """Проверяет, нужно ли исключить файл или директорию"""
    path = Path(path)
    
    # Проверка директорий
    for part in path.parts:
        if part in EXCLUDE_DIRS:
            return True
    
    # Проверка файлов
    if path.is_file():
        if path.name in EXCLUDE_FILES:
            return True
        for pattern in EXCLUDE_FILES:
            if '*' in pattern and path.match(pattern):
                return True
    
    return False

def create_backup():
    """Создает резервную копию проекта"""
    # Текущая директория проекта
    project_dir = Path.cwd()
    
    # Создаем директорию для бэкапов если её нет
    backup_dir = project_dir.parent / 'instagram_bot_backups'
    backup_dir.mkdir(exist_ok=True)
    
    # Имя архива с датой и временем
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_name = f'instagram_bot_backup_{timestamp}'
    
    # Создаем основной архив проекта
    main_archive = backup_dir / f'{backup_name}_main.zip'
    sensitive_archive = backup_dir / f'{backup_name}_sensitive.zip'
    
    print(f"🔄 Создание резервной копии Instagram Bot проекта...")
    print(f"📁 Директория проекта: {project_dir}")
    print(f"📦 Директория бэкапов: {backup_dir}")
    
    # Счетчики файлов
    main_files = 0
    sensitive_files = 0
    excluded_items = 0
    
    # Создаем основной архив
    with zipfile.ZipFile(main_archive, 'w', zipfile.ZIP_DEFLATED) as main_zip:
        with zipfile.ZipFile(sensitive_archive, 'w', zipfile.ZIP_DEFLATED) as sensitive_zip:
            
            for root, dirs, files in os.walk(project_dir):
                # Пропускаем исключенные директории
                dirs[:] = [d for d in dirs if not should_exclude(os.path.join(root, d))]
                
                for file in files:
                    file_path = os.path.join(root, file)
                    
                    # Пропускаем исключенные файлы
                    if should_exclude(file_path):
                        excluded_items += 1
                        continue
                    
                    # Относительный путь для архива
                    arcname = os.path.relpath(file_path, project_dir)
                    
                    # Проверяем, является ли файл чувствительным
                    is_sensitive = False
                    for sensitive_pattern in SENSITIVE_FILES:
                        if sensitive_pattern in arcname or arcname.startswith(sensitive_pattern):
                            is_sensitive = True
                            break
                    
                    try:
                        if is_sensitive:
                            sensitive_zip.write(file_path, arcname)
                            sensitive_files += 1
                            print(f"  🔐 {arcname}")
                        else:
                            main_zip.write(file_path, arcname)
                            main_files += 1
                            if main_files % 100 == 0:
                                print(f"  📄 Обработано {main_files} файлов...")
                    except Exception as e:
                        print(f"  ⚠️  Ошибка при добавлении {arcname}: {e}")
    
    # Создаем файл с информацией о бэкапе
    info_file = backup_dir / f'{backup_name}_info.txt'
    with open(info_file, 'w', encoding='utf-8') as f:
        f.write(f"Instagram Bot Backup Information\n")
        f.write(f"{'='*50}\n")
        f.write(f"Дата создания: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Директория проекта: {project_dir}\n")
        f.write(f"\nСтатистика:\n")
        f.write(f"- Основных файлов: {main_files}\n")
        f.write(f"- Чувствительных файлов: {sensitive_files}\n")
        f.write(f"- Исключено элементов: {excluded_items}\n")
        f.write(f"\nРазмер архивов:\n")
        f.write(f"- Основной: {main_archive.stat().st_size / 1024 / 1024:.2f} MB\n")
        f.write(f"- Чувствительный: {sensitive_archive.stat().st_size / 1024 / 1024:.2f} MB\n")
        f.write(f"\nИсключенные директории:\n")
        for d in sorted(EXCLUDE_DIRS):
            f.write(f"  - {d}\n")
    
    # Создаем README для бэкапа
    readme_file = backup_dir / f'{backup_name}_README.md'
    with open(readme_file, 'w', encoding='utf-8') as f:
        f.write(f"# Instagram Bot Backup - {timestamp}\n\n")
        f.write(f"## 📦 Содержимое архивов\n\n")
        f.write(f"### Основной архив (`{backup_name}_main.zip`)\n")
        f.write(f"Содержит весь исходный код и документацию проекта:\n")
        f.write(f"- Python модули\n")
        f.write(f"- Web интерфейсы\n") 
        f.write(f"- Документация\n")
        f.write(f"- Тесты\n\n")
        f.write(f"### Архив с чувствительными данными (`{backup_name}_sensitive.zip`)\n")
        f.write(f"⚠️ **ВАЖНО**: Содержит конфиденциальные данные:\n")
        f.write(f"- Конфигурационные файлы\n")
        f.write(f"- Данные аккаунтов\n")
        f.write(f"- Device fingerprints\n")
        f.write(f"- Рабочие аккаунты\n\n")
        f.write(f"## 🔧 Восстановление из бэкапа\n\n")
        f.write(f"1. Создайте новую директорию для проекта\n")
        f.write(f"2. Распакуйте основной архив\n")
        f.write(f"3. Распакуйте архив с чувствительными данными (если нужно)\n")
        f.write(f"4. Создайте виртуальное окружение:\n")
        f.write(f"   ```bash\n")
        f.write(f"   python -m venv venv\n")
        f.write(f"   source venv/bin/activate  # Linux/Mac\n")
        f.write(f"   # или\n")
        f.write(f"   venv\\Scripts\\activate  # Windows\n")
        f.write(f"   ```\n")
        f.write(f"5. Установите зависимости:\n")
        f.write(f"   ```bash\n")
        f.write(f"   pip install -r requirements.txt\n")
        f.write(f"   ```\n\n")
        f.write(f"## 📊 Статистика бэкапа\n\n")
        f.write(f"- **Дата создания:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"- **Основных файлов:** {main_files}\n")
        f.write(f"- **Чувствительных файлов:** {sensitive_files}\n")
        f.write(f"- **Размер основного архива:** {main_archive.stat().st_size / 1024 / 1024:.2f} MB\n")
        f.write(f"- **Размер архива с чувствительными данными:** {sensitive_archive.stat().st_size / 1024 / 1024:.2f} MB\n")
    
    print(f"\n✅ Резервное копирование завершено!")
    print(f"\n📊 Статистика:")
    print(f"  - Основных файлов: {main_files}")
    print(f"  - Чувствительных файлов: {sensitive_files}")
    print(f"  - Исключено элементов: {excluded_items}")
    print(f"\n💾 Созданные файлы:")
    print(f"  - {main_archive}")
    print(f"  - {sensitive_archive}")
    print(f"  - {info_file}")
    print(f"  - {readme_file}")
    print(f"\n🔐 ВАЖНО: Архив '{backup_name}_sensitive.zip' содержит конфиденциальные данные!")
    print(f"           Храните его в безопасном месте!")
    
    # Опционально: создание зашифрованного архива для чувствительных данных
    create_encrypted = input("\n🔒 Создать зашифрованный архив для чувствительных данных? (y/n): ")
    if create_encrypted.lower() == 'y':
        try:
            import pyzipper
            password = input("Введите пароль для шифрования: ")
            
            encrypted_archive = backup_dir / f'{backup_name}_sensitive_encrypted.zip'
            with pyzipper.AESZipFile(encrypted_archive, 'w', compression=pyzipper.ZIP_DEFLATED, encryption=pyzipper.WZ_AES) as zf:
                zf.setpassword(password.encode())
                zf.setencryption(pyzipper.WZ_AES, nbits=256)
                
                # Добавляем файлы из обычного архива
                with zipfile.ZipFile(sensitive_archive, 'r') as source_zip:
                    for file_info in source_zip.filelist:
                        zf.writestr(file_info.filename, source_zip.read(file_info.filename))
            
            print(f"\n✅ Создан зашифрованный архив: {encrypted_archive}")
            print(f"⚠️  НЕ ЗАБУДЬТЕ ПАРОЛЬ! Без него восстановление будет невозможно!")
            
            # Удаляем незашифрованный архив
            os.remove(sensitive_archive)
            print(f"🗑️  Незашифрованный архив удален")
            
        except ImportError:
            print("\n⚠️  Для создания зашифрованного архива установите pyzipper:")
            print("    pip install pyzipper")

if __name__ == "__main__":
    try:
        create_backup()
    except KeyboardInterrupt:
        print("\n\n❌ Резервное копирование прервано пользователем")
    except Exception as e:
        print(f"\n\n❌ Ошибка при создании резервной копии: {e}")
        import traceback
        traceback.print_exc() 
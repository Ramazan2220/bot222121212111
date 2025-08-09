#!/usr/bin/env python3
"""
Скрипт для подготовки проекта к переносу на VDS
"""

import os
import shutil
import json
import tarfile
from pathlib import Path

def create_deployment_package():
    """Создает пакет для развертывания на VDS"""
    
    print("🚀 ПОДГОТОВКА К ПЕРЕНОСУ НА VDS")
    print("=" * 50)
    
    # Список файлов и папок для переноса
    include_files = [
        # Основные файлы
        "main.py",
        "requirements.txt",
        
        # Папки с кодом
        "telegram_bot/",
        "instagram/", 
        "database/",
        "utils/",
        "profile_setup/",
        "instagram_api/",
        
        # Конфигурация
        ".env",
        "config.py",
        
        # Данные (БЕЗ аккаунтов)
        "data/database.db",
        "data/media/",
        
        # Устройства
        "devices/",
        
        # Документация
        "docs/",
    ]
    
    # Файлы которые НЕ переносим
    exclude_patterns = [
        "bot_env*",
        "venv*", 
        "web_env*",
        "__pycache__",
        "*.pyc",
        "*.log",
        "data/accounts/*/session.json",  # Сессии придется пересоздать
        "email_logs/",
        "working_accounts/",
        ".DS_Store",
        "test_*.py",
        "fix_*.py",
    ]
    
    # Создаем папку для развертывания
    deploy_dir = "vds_deployment"
    if os.path.exists(deploy_dir):
        shutil.rmtree(deploy_dir)
    os.makedirs(deploy_dir)
    
    print(f"📁 Создана папка развертывания: {deploy_dir}")
    
    # Копируем файлы
    copied_count = 0
    for item in include_files:
        src = item
        if os.path.exists(src):
            dst = os.path.join(deploy_dir, item)
            
            # Создаем директории если нужно
            os.makedirs(os.path.dirname(dst), exist_ok=True)
            
            if os.path.isdir(src):
                # Копируем папку с исключениями
                shutil.copytree(src, dst, ignore=shutil.ignore_patterns(*exclude_patterns), dirs_exist_ok=True)
                print(f"📂 Скопирована папка: {item}")
            else:
                # Копируем файл
                shutil.copy2(src, dst)
                print(f"📄 Скопирован файл: {item}")
            copied_count += 1
        else:
            print(f"⚠️  Файл не найден: {item}")
    
    # Создаем файл инструкций
    create_deployment_instructions(deploy_dir)
    
    # Создаем архив
    archive_name = "instagram_bot_vds.tar.gz"
    with tarfile.open(archive_name, "w:gz") as tar:
        tar.add(deploy_dir, arcname="instagram_bot")
    
    print(f"\n✅ Создан архив: {archive_name}")
    print(f"📊 Скопировано элементов: {copied_count}")
    
    # Показываем размер
    size_mb = os.path.getsize(archive_name) / 1024 / 1024
    print(f"📏 Размер архива: {size_mb:.1f} MB")
    
    return archive_name

def create_deployment_instructions(deploy_dir):
    """Создает файл с инструкциями по развертыванию"""
    
    instructions = """# 🚀 ИНСТРУКЦИЯ ПО РАЗВЕРТЫВАНИЮ НА VDS

## 1. СИСТЕМНЫЕ ТРЕБОВАНИЯ
- Ubuntu 20.04+ / CentOS 8+ / Debian 11+
- Python 3.9+
- 2GB RAM минимум
- 10GB свободного места

## 2. УСТАНОВКА ЗАВИСИМОСТЕЙ

### Ubuntu/Debian:
```bash
sudo apt update
sudo apt install -y python3 python3-pip python3-venv git sqlite3
sudo apt install -y python3-dev build-essential
```

### CentOS/RHEL:
```bash
sudo yum update -y
sudo yum install -y python3 python3-pip git sqlite
sudo yum groupinstall -y "Development Tools"
```

## 3. РАЗВЕРТЫВАНИЕ

### Распаковка:
```bash
tar -xzf instagram_bot_vds.tar.gz
cd instagram_bot
```

### Создание виртуального окружения:
```bash
python3 -m venv bot_env
source bot_env/bin/activate
```

### Установка зависимостей:
```bash
pip install --upgrade pip
pip install -r requirements.txt
```

## 4. НАСТРОЙКА

### Настройка .env файла:
```bash
nano .env
```

### Заполните переменные:
- TELEGRAM_BOT_TOKEN=ваш_токен
- ENCRYPTION_KEY=ваш_ключ_шифрования
- DATABASE_URL=sqlite:///data/database.db

### Инициализация базы данных:
```bash
python -c "from database.db_manager import init_database; init_database()"
```

## 5. ЗАПУСК

### Тестовый запуск:
```bash
python main.py
```

### Запуск как сервис (systemd):
```bash
sudo cp deployment/instagram_bot.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable instagram_bot
sudo systemctl start instagram_bot
```

### Проверка статуса:
```bash
sudo systemctl status instagram_bot
sudo journalctl -u instagram_bot -f
```

## 6. НАСТРОЙКА FIREWALL

```bash
# Открываем нужные порты
sudo ufw allow 22/tcp    # SSH
sudo ufw allow 80/tcp    # HTTP (если есть веб-интерфейс)
sudo ufw allow 443/tcp   # HTTPS
sudo ufw enable
```

## 7. МОНИТОРИНГ

### Логи:
```bash
tail -f data/logs/bot.log
tail -f data/logs/telegram_errors.log
```

### Системные ресурсы:
```bash
htop
df -h
free -h
```

## 8. BACKUP

### Создание бэкапа:
```bash
tar -czf backup_$(date +%Y%m%d_%H%M%S).tar.gz data/ .env
```

### Автоматический бэкап (cron):
```bash
# Добавить в crontab:
0 2 * * * cd /path/to/bot && tar -czf backup_$(date +\\%Y\\%m\\%d).tar.gz data/ .env
```

## 9. ОБНОВЛЕНИЕ

```bash
# Остановка
sudo systemctl stop instagram_bot

# Бэкап
tar -czf backup_before_update.tar.gz data/ .env

# Обновление кода
git pull  # или загрузка нового архива

# Обновление зависимостей
source bot_env/bin/activate
pip install -r requirements.txt

# Запуск
sudo systemctl start instagram_bot
```

## 10. РЕШЕНИЕ ПРОБЛЕМ

### Проблемы с правами:
```bash
sudo chown -R $USER:$USER /path/to/bot
chmod +x main.py
```

### Проблемы с зависимостями:
```bash
pip install --force-reinstall -r requirements.txt
```

### Очистка логов:
```bash
find data/logs/ -name "*.log" -mtime +7 -delete
```
"""

    with open(os.path.join(deploy_dir, "DEPLOYMENT_GUIDE.md"), "w", encoding="utf-8") as f:
        f.write(instructions)
    
    # Создаем systemd service файл
    service_content = """[Unit]
Description=Instagram Telegram Bot
After=network.target

[Service]
Type=simple
User=ubuntu
WorkingDirectory=/home/ubuntu/instagram_bot
Environment=PATH=/home/ubuntu/instagram_bot/bot_env/bin
ExecStart=/home/ubuntu/instagram_bot/bot_env/bin/python main.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
"""
    
    service_dir = os.path.join(deploy_dir, "deployment")
    os.makedirs(service_dir, exist_ok=True)
    
    with open(os.path.join(service_dir, "instagram_bot.service"), "w") as f:
        f.write(service_content)
    
    # Создаем скрипт автозапуска
    startup_script = """#!/bin/bash
# Скрипт автозапуска бота

BOT_DIR="/home/ubuntu/instagram_bot"
cd $BOT_DIR

# Активируем виртуальное окружение
source bot_env/bin/activate

# Запускаем бота
python main.py
"""
    
    with open(os.path.join(service_dir, "start_bot.sh"), "w") as f:
        f.write(startup_script)
    
    # Делаем скрипт исполняемым
    os.chmod(os.path.join(service_dir, "start_bot.sh"), 0o755)

def show_vds_recommendations():
    """Показывает рекомендации по выбору VDS"""
    
    print("\n" + "="*60)
    print("💡 РЕКОМЕНДАЦИИ ПО VDS")
    print("="*60)
    
    print("\n🖥️  МИНИМАЛЬНЫЕ ТРЕБОВАНИЯ:")
    print("• CPU: 2 ядра")
    print("• RAM: 2GB")
    print("• Диск: 20GB SSD")
    print("• ОС: Ubuntu 20.04 LTS")
    
    print("\n🔥 РЕКОМЕНДУЕМЫЕ КОНФИГУРАЦИИ:")
    print("• CPU: 4 ядра")
    print("• RAM: 4GB")
    print("• Диск: 40GB SSD")
    print("• Сеть: 100 Мбит/с")
    
    print("\n🌍 ПРОВАЙДЕРЫ VDS (для России):")
    print("• Selectel - хорошая производительность")
    print("• Timeweb - доступные цены")  
    print("• REG.RU - стабильность")
    print("• FirstVDS - быстрые диски")
    
    print("\n🌎 МЕЖДУНАРОДНЫЕ ПРОВАЙДЕРЫ:")
    print("• DigitalOcean - простота настройки")
    print("• Vultr - хорошее соотношение цена/качество")
    print("• Linode - стабильность")
    print("• AWS/GCP - для профессионального использования")
    
    print("\n📍 ЛОКАЦИЯ СЕРВЕРОВ:")
    print("• Москва/СПб - минимальная задержка для России")
    print("• Европа - компромисс скорость/стабильность")
    print("• США - если нужна стабильность Instagram API")

if __name__ == "__main__":
    # Создаем пакет развертывания
    archive_name = create_deployment_package()
    
    # Показываем рекомендации
    show_vds_recommendations()
    
    print(f"\n🎉 ГОТОВО!")
    print(f"📦 Архив для VDS: {archive_name}")
    print(f"📋 Инструкция: vds_deployment/DEPLOYMENT_GUIDE.md")
    print(f"\n🚀 СЛЕДУЮЩИЕ ШАГИ:")
    print(f"1. Загрузите {archive_name} на ваш VDS")
    print(f"2. Следуйте инструкции в DEPLOYMENT_GUIDE.md")
    print(f"3. Не забудьте настроить .env файл!")
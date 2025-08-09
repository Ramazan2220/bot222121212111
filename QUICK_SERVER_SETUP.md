# 🚀 Быстрый запуск на сервере

## 📋 Команды для установки (выполнить по порядку):

### 1. Создание виртуального окружения:
```bash
python3 -m venv bot_env
```

### 2. Активация окружения:
```bash
source bot_env/bin/activate
```

### 3. Обновление pip:
```bash
pip install --upgrade pip
```

### 4. Установка зависимостей:
```bash
pip install -r requirements.txt
```

### 5. Настройка конфигурации:
```bash
cp config.example.py config.py
nano config.py
```
**В config.py обязательно укажите:**
- `TELEGRAM_TOKEN = "ваш_токен_основного_бота"`
- `ADMIN_BOT_TOKEN = "ваш_токен_админ_бота"`

### 6. Создание директорий:
```bash
mkdir -p data/logs data/accounts data/media devices email_logs
```

### 7. Инициализация базы данных:
```bash
python -c "from database.db_manager import init_db; init_db()"
```

### 8. Запуск бота:
```bash
python main.py
```

---

## 🔧 Полная команда одной строкой:

```bash
python3 -m venv bot_env && \
source bot_env/bin/activate && \
pip install --upgrade pip && \
pip install -r requirements.txt && \
cp config.example.py config.py && \
mkdir -p data/logs data/accounts data/media devices email_logs && \
echo "✅ Установка завершена! Отредактируйте config.py и запустите: python main.py"
```

---

## 🛠️ Для постоянного запуска (systemd):

### Создать сервис:
```bash
sudo nano /etc/systemd/system/instagram-bot.service
```

### Содержимое файла:
```ini
[Unit]
Description=Instagram Telegram Bot
After=network.target

[Service]
Type=simple
User=your_username
WorkingDirectory=/path/to/your/project
Environment=PYTHONPATH=/path/to/your/project
ExecStart=/path/to/your/project/bot_env/bin/python main.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

### Активация сервиса:
```bash
sudo systemctl daemon-reload
sudo systemctl enable instagram-bot
sudo systemctl start instagram-bot
```

### Управление сервисом:
```bash
# Статус
sudo systemctl status instagram-bot

# Логи
sudo journalctl -u instagram-bot -f

# Перезапуск
sudo systemctl restart instagram-bot

# Остановка
sudo systemctl stop instagram-bot
```

---

## ✅ Проверка работы:

1. **Лог должен показать инициализацию:**
   - ✅ Database Connection Pool готов
   - ✅ Instagram Client Pool инициализирован  
   - ✅ Lazy Loading активирован
   - ✅ Telegram Bot запущен

2. **В Telegram боте должно работать:**
   - `/start` - приветствие и проверка доступа
   - Админ бот - управление пользователями

🎯 **Готово! Ваш бот работает на сервере с оптимизацией Lazy Loading!** 
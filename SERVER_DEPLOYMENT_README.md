# 🚀 Instagram Telegram Bot - Развертывание на сервере

## 📋 Требования к серверу

### Минимальные характеристики:
- **CPU:** 2 cores (рекомендуется 4+ cores)
- **RAM:** 4GB (рекомендуется 8GB+ для Lazy Loading)
- **Диск:** 50GB SSD
- **ОС:** Ubuntu 20.04+ / CentOS 8+ / Debian 11+

### Для масштабирования (100 пользователей):
- **CPU:** 8-16 cores
- **RAM:** 16-32GB
- **Диск:** 200GB SSD
- **Сеть:** 1Gbps

## 🔧 Быстрая установка

### 1. Подготовка файлов

```bash
# Загрузите архив проекта на сервер
scp instagram_telegram_bot_archive.tar.gz user@your-server:/tmp/

# Подключитесь к серверу
ssh user@your-server

# Распакуйте проект
cd /tmp
tar -xzf instagram_telegram_bot_archive.tar.gz
cd instagram_telegram_bot_reserv
```

### 2. Запуск установки

```bash
# Дайте права на выполнение
chmod +x deploy_to_server.sh

# Запустите установку
./deploy_to_server.sh
```

### 3. Настройка конфигурации

```bash
# Отредактируйте конфигурацию
sudo -u instagram_bot nano /home/instagram_bot/instagram_telegram_bot/config.py

# Обязательно укажите:
# - TELEGRAM_TOKEN = "ваш_токен_бота"
# - База данных (по умолчанию SQLite)
# - Настройки Lazy Loading
```

### 4. Запуск бота

```bash
# Запустите сервис
sudo systemctl start instagram-bot

# Проверьте статус
sudo systemctl status instagram-bot

# Посмотрите логи
sudo journalctl -u instagram-bot -f
```

## ⚙️ Детальная настройка

### Конфигурация Lazy Loading

В `config.py` настройте оптимизацию памяти:

```python
# Lazy Loading - экономия до 98% памяти
USE_LAZY_LOADING = True
LAZY_MAX_ACTIVE_CLIENTS = 1000  # Для 100 пользователей
LAZY_CLEANUP_INTERVAL = 1800    # 30 минут
LAZY_FALLBACK_TO_NORMAL = True  # Безопасность
```

### База данных

По умолчанию используется SQLite. Для production рекомендуется PostgreSQL:

```bash
# Установка PostgreSQL
sudo apt-get install postgresql postgresql-contrib

# Создание базы данных
sudo -u postgres createdb instagram_bot
sudo -u postgres createuser instagram_bot

# В config.py:
# DATABASE_URL = "postgresql://instagram_bot:password@localhost/instagram_bot"
```

### Мониторинг

```bash
# Статус сервиса
sudo systemctl status instagram-bot

# Логи в реальном времени
sudo journalctl -u instagram-bot -f

# Использование ресурсов
htop
sudo iotop

# Статистика Lazy Loading (в логах)
sudo journalctl -u instagram-bot | grep "Lazy Loading"
```

## 🛡️ Безопасность

### Firewall

```bash
# UFW (Ubuntu/Debian)
sudo ufw enable
sudo ufw allow ssh
sudo ufw allow 80/tcp   # если используете веб-интерфейс
sudo ufw allow 443/tcp  # HTTPS

# Проверка статуса
sudo ufw status
```

### SSL сертификат (если нужен веб-интерфейс)

```bash
# Установка Certbot
sudo apt-get install certbot

# Получение сертификата
sudo certbot certonly --standalone -d your-domain.com
```

### Обновления безопасности

```bash
# Автоматические обновления
sudo apt-get install unattended-upgrades
sudo dpkg-reconfigure unattended-upgrades
```

## 📊 Мониторинг производительности

### Установка мониторинга

```bash
# Установка htop, iotop, netstat
sudo apt-get install htop iotop net-tools

# Мониторинг памяти
watch -n 5 'free -h'

# Мониторинг дискового пространства
df -h
```

### Telegram уведомления о статусе

Бот может отправлять уведомления о своем состоянии:

```python
# В config.py добавьте:
ADMIN_CHAT_ID = "ваш_chat_id"
MONITORING_ENABLED = True
MONITORING_INTERVAL = 3600  # Каждый час
```

## 💾 Backup и восстановление

### Автоматический backup

Скрипт backup настраивается автоматически:

```bash
# Ручной backup
sudo -u instagram_bot /home/instagram_bot/backup.sh

# Просмотр backup файлов
ls -la /home/instagram_bot/backups/

# Автоматический backup (cron)
sudo -u instagram_bot crontab -l
```

### Восстановление из backup

```bash
# Остановка сервиса
sudo systemctl stop instagram-bot

# Восстановление
cd /home/instagram_bot/instagram_telegram_bot
sudo -u instagram_bot tar -xzf /home/instagram_bot/backups/bot_backup_YYYYMMDD_HHMMSS.tar.gz

# Запуск
sudo systemctl start instagram-bot
```

## 🔄 Обновление проекта

### Обновление кода

```bash
# Остановка
sudo systemctl stop instagram-bot

# Backup текущей версии
sudo -u instagram_bot /home/instagram_bot/backup.sh

# Загрузка новой версии
# ... загрузите новые файлы ...

# Обновление зависимостей
sudo -u instagram_bot bash -c "
    cd /home/instagram_bot/instagram_telegram_bot
    source bot_env/bin/activate
    pip install -r requirements.txt --upgrade
"

# Запуск
sudo systemctl start instagram-bot
```

## 🚨 Устранение проблем

### Проблемы с запуском

```bash
# Проверка логов
sudo journalctl -u instagram-bot --no-pager

# Проверка конфигурации
sudo -u instagram_bot python3 -c "
    import sys
    sys.path.append('/home/instagram_bot/instagram_telegram_bot')
    import config
    print('Config загружен успешно')
"

# Проверка зависимостей
sudo -u instagram_bot bash -c "
    cd /home/instagram_bot/instagram_telegram_bot
    source bot_env/bin/activate
    pip check
"
```

### Проблемы с памятью

```bash
# Проверка использования памяти
free -h
ps aux | grep python

# Настройка Lazy Loading
# Уменьшите LAZY_MAX_ACTIVE_CLIENTS в config.py
```

### Проблемы с сетью

```bash
# Проверка соединения
curl -I https://api.telegram.org
ping instagram.com

# Проверка портов
netstat -tlnp | grep python
```

## 📈 Масштабирование

### Для большого количества пользователей

1. **Увеличьте ресурсы сервера**
2. **Настройте базу данных PostgreSQL**
3. **Используйте Redis для кеширования**
4. **Настройте load balancer**

### Кластеризация

Для очень больших нагрузок рассмотрите microservices архитектуру:

- Отдельный сервер для базы данных
- Отдельные сервера для обработки аккаунтов
- Load balancer для распределения нагрузки

## 📞 Поддержка

### Логи для диагностики

```bash
# Полные логи
sudo journalctl -u instagram-bot --no-pager > bot_logs.txt

# Логи за последний час
sudo journalctl -u instagram-bot --since "1 hour ago"

# Ошибки
sudo journalctl -u instagram-bot -p err
```

### Полезные команды

```bash
# Перезапуск
sudo systemctl restart instagram-bot

# Отключение автозапуска
sudo systemctl disable instagram-bot

# Включение автозапуска
sudo systemctl enable instagram-bot

# Полная остановка
sudo systemctl stop instagram-bot
sudo systemctl disable instagram-bot
```

## 🎯 Чек-лист после установки

- [ ] ✅ Сервис запускается: `sudo systemctl status instagram-bot`
- [ ] ✅ Конфигурация правильная: токен бота, база данных
- [ ] ✅ Lazy Loading работает: проверить логи экономии памяти
- [ ] ✅ Firewall настроен: `sudo ufw status`
- [ ] ✅ Backup работает: `sudo -u instagram_bot /home/instagram_bot/backup.sh`
- [ ] ✅ Мониторинг настроен: `htop`, логи
- [ ] ✅ Автозапуск включен: `sudo systemctl is-enabled instagram-bot`

---

## 💡 Оптимизация для production

### Рекомендуемые настройки для 100 пользователей:

```python
# config.py для production
USE_LAZY_LOADING = True
LAZY_MAX_ACTIVE_CLIENTS = 1000
LAZY_CLEANUP_INTERVAL = 1800

# Логирование
LOG_LEVEL = "INFO"
LAZY_ENABLE_STATS_LOGGING = True
LAZY_STATS_LOG_INTERVAL = 300

# Производительность
DATABASE_POOL_SIZE = 20
DATABASE_MAX_OVERFLOW = 30
```

Эти настройки обеспечат оптимальную работу с экономией до 98% памяти!

🚀 **Ваш Instagram Telegram Bot готов к работе на сервере!** 
#!/bin/bash

# Instagram Telegram Bot - Deployment Script
# Скрипт для развертывания на сервере

echo "🚀 РАЗВЕРТЫВАНИЕ INSTAGRAM TELEGRAM BOT НА СЕРВЕРЕ"
echo "=================================================="

# Цвета для вывода
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Функция логирования
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Проверка что скрипт запущен с правами
check_permissions() {
    log_info "Проверка прав доступа..."
    
    if [[ $EUID -eq 0 ]]; then
        log_warning "Скрипт запущен под root. Рекомендуется использовать обычного пользователя."
    fi
}

# Установка системных зависимостей
install_system_dependencies() {
    log_info "Установка системных зависимостей..."
    
    # Определяем тип системы
    if command -v apt-get &> /dev/null; then
        # Ubuntu/Debian
        sudo apt-get update
        sudo apt-get install -y python3 python3-pip python3-venv python3-dev
        sudo apt-get install -y build-essential libssl-dev libffi-dev
        sudo apt-get install -y sqlite3 libsqlite3-dev
        sudo apt-get install -y curl wget git
    elif command -v yum &> /dev/null; then
        # CentOS/RHEL
        sudo yum update -y
        sudo yum install -y python3 python3-pip python3-devel
        sudo yum install -y gcc openssl-devel libffi-devel
        sudo yum install -y sqlite sqlite-devel
        sudo yum install -y curl wget git
    else
        log_error "Неподдерживаемая операционная система"
        exit 1
    fi
    
    log_success "Системные зависимости установлены"
}

# Создание пользователя для бота
create_bot_user() {
    log_info "Создание пользователя для бота..."
    
    BOT_USER="instagram_bot"
    
    if id "$BOT_USER" &>/dev/null; then
        log_info "Пользователь $BOT_USER уже существует"
    else
        sudo useradd -m -s /bin/bash $BOT_USER
        log_success "Пользователь $BOT_USER создан"
    fi
    
    # Создаем домашнюю директорию
    BOT_HOME="/home/$BOT_USER"
    sudo mkdir -p $BOT_HOME
    sudo chown $BOT_USER:$BOT_USER $BOT_HOME
}

# Настройка проекта
setup_project() {
    log_info "Настройка проекта..."
    
    PROJECT_DIR="/home/instagram_bot/instagram_telegram_bot"
    
    # Создаем директорию проекта
    sudo -u instagram_bot mkdir -p $PROJECT_DIR
    
    # Копируем файлы проекта (предполагается что они уже загружены)
    if [ -d "./instagram_telegram_bot_reserv" ]; then
        log_info "Копирование файлов проекта..."
        sudo cp -r ./instagram_telegram_bot_reserv/* $PROJECT_DIR/
        sudo chown -R instagram_bot:instagram_bot $PROJECT_DIR
        log_success "Файлы проекта скопированы"
    else
        log_warning "Файлы проекта не найдены. Скопируйте их вручную в $PROJECT_DIR"
    fi
}

# Создание виртуального окружения
setup_virtual_environment() {
    log_info "Создание виртуального окружения..."
    
    PROJECT_DIR="/home/instagram_bot/instagram_telegram_bot"
    
    sudo -u instagram_bot bash -c "
        cd $PROJECT_DIR
        python3 -m venv bot_env
        source bot_env/bin/activate
        pip install --upgrade pip
        pip install -r requirements.txt
    "
    
    log_success "Виртуальное окружение создано и зависимости установлены"
}

# Настройка конфигурации
setup_configuration() {
    log_info "Настройка конфигурации..."
    
    PROJECT_DIR="/home/instagram_bot/instagram_telegram_bot"
    
    # Создаем config.py из example
    sudo -u instagram_bot bash -c "
        cd $PROJECT_DIR
        if [ ! -f config.py ] && [ -f config.example.py ]; then
            cp config.example.py config.py
            echo '⚠️  ВНИМАНИЕ: Отредактируйте config.py с вашими настройками'
        fi
    "
    
    # Создаем директории для данных
    sudo -u instagram_bot bash -c "
        cd $PROJECT_DIR
        mkdir -p data/logs data/accounts data/media
        mkdir -p devices email_logs
    "
    
    log_success "Конфигурация настроена"
}

# Создание systemd сервиса
create_systemd_service() {
    log_info "Создание systemd сервиса..."
    
    sudo tee /etc/systemd/system/instagram-bot.service > /dev/null <<EOF
[Unit]
Description=Instagram Telegram Bot
After=network.target

[Service]
Type=simple
User=instagram_bot
Group=instagram_bot
WorkingDirectory=/home/instagram_bot/instagram_telegram_bot
Environment=PYTHONPATH=/home/instagram_bot/instagram_telegram_bot
ExecStart=/home/instagram_bot/instagram_telegram_bot/bot_env/bin/python main.py
Restart=always
RestartSec=10

# Логирование
StandardOutput=journal
StandardError=journal
SyslogIdentifier=instagram-bot

# Ограничения безопасности
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/home/instagram_bot/instagram_telegram_bot

[Install]
WantedBy=multi-user.target
EOF

    sudo systemctl daemon-reload
    sudo systemctl enable instagram-bot
    
    log_success "Systemd сервис создан"
}

# Настройка firewall
setup_firewall() {
    log_info "Настройка firewall..."
    
    if command -v ufw &> /dev/null; then
        # Базовые правила UFW
        sudo ufw --force enable
        sudo ufw default deny incoming
        sudo ufw default allow outgoing
        sudo ufw allow ssh
        sudo ufw allow 80/tcp
        sudo ufw allow 443/tcp
        log_success "UFW firewall настроен"
    elif command -v firewall-cmd &> /dev/null; then
        # Настройка firewalld
        sudo systemctl enable firewalld
        sudo systemctl start firewalld
        sudo firewall-cmd --permanent --add-service=ssh
        sudo firewall-cmd --permanent --add-service=http
        sudo firewall-cmd --permanent --add-service=https
        sudo firewall-cmd --reload
        log_success "Firewalld настроен"
    else
        log_warning "Firewall не настроен автоматически"
    fi
}

# Создание backup скрипта
create_backup_script() {
    log_info "Создание скрипта backup..."
    
    sudo tee /home/instagram_bot/backup.sh > /dev/null <<'EOF'
#!/bin/bash

# Backup скрипт для Instagram Telegram Bot
BACKUP_DIR="/home/instagram_bot/backups"
PROJECT_DIR="/home/instagram_bot/instagram_telegram_bot"
DATE=$(date +%Y%m%d_%H%M%S)

mkdir -p $BACKUP_DIR

# Создаем архив
tar -czf $BACKUP_DIR/bot_backup_$DATE.tar.gz \
    -C $PROJECT_DIR \
    --exclude='bot_env' \
    --exclude='data/logs' \
    --exclude='__pycache__' \
    .

# Удаляем старые backup (старше 7 дней)
find $BACKUP_DIR -name "bot_backup_*.tar.gz" -mtime +7 -delete

echo "Backup создан: $BACKUP_DIR/bot_backup_$DATE.tar.gz"
EOF

    sudo chown instagram_bot:instagram_bot /home/instagram_bot/backup.sh
    sudo chmod +x /home/instagram_bot/backup.sh
    
    # Добавляем в crontab
    (sudo -u instagram_bot crontab -l 2>/dev/null; echo "0 2 * * * /home/instagram_bot/backup.sh") | sudo -u instagram_bot crontab -
    
    log_success "Backup скрипт создан"
}

# Финальная проверка
final_check() {
    log_info "Финальная проверка установки..."
    
    PROJECT_DIR="/home/instagram_bot/instagram_telegram_bot"
    
    # Проверяем файлы
    if [ -f "$PROJECT_DIR/main.py" ]; then
        log_success "✅ Главный файл найден"
    else
        log_error "❌ main.py не найден"
    fi
    
    if [ -f "$PROJECT_DIR/config.py" ]; then
        log_success "✅ Конфигурация найдена"
    else
        log_warning "⚠️  config.py не найден - создайте из config.example.py"
    fi
    
    if [ -d "$PROJECT_DIR/bot_env" ]; then
        log_success "✅ Виртуальное окружение создано"
    else
        log_error "❌ Виртуальное окружение не найдено"
    fi
    
    # Проверяем сервис
    if systemctl is-enabled instagram-bot &>/dev/null; then
        log_success "✅ Systemd сервис настроен"
    else
        log_error "❌ Systemd сервис не настроен"
    fi
}

# Показ инструкций
show_instructions() {
    echo ""
    echo "🎉 УСТАНОВКА ЗАВЕРШЕНА!"
    echo "======================="
    echo ""
    echo "📋 СЛЕДУЮЩИЕ ШАГИ:"
    echo ""
    echo "1️⃣ Отредактируйте конфигурацию:"
    echo "   sudo -u instagram_bot nano /home/instagram_bot/instagram_telegram_bot/config.py"
    echo ""
    echo "2️⃣ Запустите бота:"
    echo "   sudo systemctl start instagram-bot"
    echo ""
    echo "3️⃣ Проверьте статус:"
    echo "   sudo systemctl status instagram-bot"
    echo ""
    echo "4️⃣ Просмотр логов:"
    echo "   sudo journalctl -u instagram-bot -f"
    echo ""
    echo "5️⃣ Автозапуск (уже включен):"
    echo "   sudo systemctl enable instagram-bot"
    echo ""
    echo "🔧 УПРАВЛЕНИЕ:"
    echo "   Старт:     sudo systemctl start instagram-bot"
    echo "   Стоп:      sudo systemctl stop instagram-bot"
    echo "   Рестарт:   sudo systemctl restart instagram-bot"
    echo "   Статус:    sudo systemctl status instagram-bot"
    echo ""
    echo "💾 BACKUP:"
    echo "   Ручной:    sudo -u instagram_bot /home/instagram_bot/backup.sh"
    echo "   Авто:      Каждый день в 2:00"
    echo ""
    echo "📁 ФАЙЛЫ ПРОЕКТА:"
    echo "   /home/instagram_bot/instagram_telegram_bot/"
    echo ""
}

# Главная функция
main() {
    echo ""
    log_info "Начинаем развертывание Instagram Telegram Bot..."
    echo ""
    
    check_permissions
    install_system_dependencies
    create_bot_user
    setup_project
    setup_virtual_environment
    setup_configuration
    create_systemd_service
    setup_firewall
    create_backup_script
    final_check
    show_instructions
    
    echo ""
    log_success "🚀 Развертывание завершено успешно!"
}

# Запуск скрипта
main "$@" 
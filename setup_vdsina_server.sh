#!/bin/bash

# 🚀 АВТОМАТИЧЕСКАЯ НАСТРОЙКА PostgreSQL НА VDSina
# Для нагрузки: 100 пользователей, 50,000 аккаунтов
# Сервер: VDSina PRO (8 CPU, 16GB RAM, 240GB SSD)

set -e

echo "🚀 Начинаем настройку PostgreSQL на VDSina..."
echo "📋 Сервер: VDSina PRO (8 CPU, 16GB RAM)"
echo "🎯 Нагрузка: 100 пользователей, 50,000 аккаунтов"
echo "=" * 60

# =============================================================================
# 1. ОБНОВЛЕНИЕ СИСТЕМЫ
# =============================================================================

echo "📦 Обновляем систему..."
sudo apt update && sudo apt upgrade -y

echo "🔧 Устанавливаем необходимые пакеты..."
sudo apt install -y curl wget gnupg2 software-properties-common apt-transport-https lsb-release ca-certificates

# =============================================================================
# 2. УСТАНОВКА PostgreSQL 15
# =============================================================================

echo "🐘 Устанавливаем PostgreSQL 15..."

# Добавляем официальный репозиторий PostgreSQL
curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo gpg --dearmor -o /etc/apt/trusted.gpg.d/postgresql.gpg
echo "deb http://apt.postgresql.org/pub/repos/apt/ $(lsb_release -cs)-pgdg main" | sudo tee /etc/apt/sources.list.d/pgdg.list

sudo apt update
sudo apt install -y postgresql-15 postgresql-contrib-15 postgresql-client-15

# =============================================================================
# 3. НАСТРОЙКА PostgreSQL
# =============================================================================

echo "⚙️ Настраиваем PostgreSQL для высокой нагрузки..."

# Останавливаем PostgreSQL для настройки
sudo systemctl stop postgresql

# Создаем бэкап оригинального конфига
sudo cp /etc/postgresql/15/main/postgresql.conf /etc/postgresql/15/main/postgresql.conf.backup

# Применяем нашу конфигурацию
sudo cat > /etc/postgresql/15/main/postgresql.conf << 'EOF'
# 🚀 КОНФИГУРАЦИЯ PostgreSQL ДЛЯ VDSina PRO
# Сервер: 8 CPU, 16GB RAM, 240GB SSD
# Нагрузка: 100 пользователей, 50,000 аккаунтов

# ПОДКЛЮЧЕНИЯ И ПАМЯТЬ
max_connections = 500
shared_buffers = 4GB
effective_cache_size = 12GB
maintenance_work_mem = 1GB
work_mem = 32MB

# ПРОИЗВОДИТЕЛЬНОСТЬ ЗАПИСИ
wal_buffers = 64MB
checkpoint_completion_target = 0.9
max_wal_size = 4GB
min_wal_size = 1GB

# ОПТИМИЗАЦИЯ ДЛЯ SSD
random_page_cost = 1.1
seq_page_cost = 1.0
default_statistics_target = 100

# ПАРАЛЛЕЛЬНАЯ ОБРАБОТКА
max_worker_processes = 8
max_parallel_workers = 8
max_parallel_workers_per_gather = 4
max_parallel_maintenance_workers = 4

# ЛОГИРОВАНИЕ
log_min_duration_statement = 1000
log_line_prefix = '%t [%p]: user=%u,db=%d,app=%a,client=%h '
log_checkpoints = on
log_connections = on
log_disconnections = on

# АВТОВАКУУМ
autovacuum = on
autovacuum_max_workers = 4
autovacuum_naptime = 30s

# СТАТИСТИКА
track_activities = on
track_counts = on
track_io_timing = on

# ПОДКЛЮЧЕНИЯ
listen_addresses = '*'
port = 5432

# ДОПОЛНИТЕЛЬНЫЕ ОПТИМИЗАЦИИ
huge_pages = try
jit = on
jit_above_cost = 100000

tcp_keepalives_idle = 600
tcp_keepalives_interval = 30
tcp_keepalives_count = 3
EOF

# =============================================================================
# 4. НАСТРОЙКА ДОСТУПА
# =============================================================================

echo "🔐 Настраиваем доступ к БД..."

# Настраиваем pg_hba.conf для удаленного доступа
sudo cat > /etc/postgresql/15/main/pg_hba.conf << 'EOF'
# TYPE  DATABASE        USER            ADDRESS                 METHOD

# "local" is for Unix domain socket connections only
local   all             postgres                                peer
local   all             all                                     peer

# IPv4 local connections:
host    all             all             127.0.0.1/32            scram-sha-256
host    all             all             0.0.0.0/0               scram-sha-256

# IPv6 local connections:
host    all             all             ::1/128                 scram-sha-256
EOF

# =============================================================================
# 5. СОЗДАНИЕ БД И ПОЛЬЗОВАТЕЛЯ
# =============================================================================

echo "👤 Создаем БД и пользователя..."

# Запускаем PostgreSQL
sudo systemctl start postgresql
sudo systemctl enable postgresql

# Ждем запуска
sleep 5

# Генерируем сильный пароль
DB_PASSWORD=$(openssl rand -base64 32)

# Создаем БД и пользователя
sudo -u postgres psql << EOF
-- Устанавливаем пароль для postgres
ALTER USER postgres PASSWORD '${DB_PASSWORD}';

-- Создаем БД для Instagram бота
CREATE DATABASE instagram_bot;

-- Создаем пользователя для приложения
CREATE USER instagram_user WITH PASSWORD '${DB_PASSWORD}';

-- Даем права на БД
GRANT ALL PRIVILEGES ON DATABASE instagram_bot TO instagram_user;

-- Подключаемся к БД и даем права на схему
\c instagram_bot
GRANT ALL ON SCHEMA public TO instagram_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO instagram_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO instagram_user;

-- Включаем расширения
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";

\q
EOF

# =============================================================================
# 6. ОПТИМИЗАЦИЯ СИСТЕМЫ
# =============================================================================

echo "🔧 Оптимизируем систему для PostgreSQL..."

# Увеличиваем лимиты файлов
echo "postgres soft nofile 65536" | sudo tee -a /etc/security/limits.conf
echo "postgres hard nofile 65536" | sudo tee -a /etc/security/limits.conf

# Оптимизируем память
sudo sysctl -w vm.swappiness=1
sudo sysctl -w vm.overcommit_memory=2
sudo sysctl -w vm.overcommit_ratio=95

# Сохраняем настройки
echo "vm.swappiness = 1" | sudo tee -a /etc/sysctl.conf
echo "vm.overcommit_memory = 2" | sudo tee -a /etc/sysctl.conf
echo "vm.overcommit_ratio = 95" | sudo tee -a /etc/sysctl.conf

# =============================================================================
# 7. НАСТРОЙКА ФАЙРВОЛА
# =============================================================================

echo "🔥 Настраиваем файрвол..."

sudo ufw allow 22/tcp
sudo ufw allow 5432/tcp
sudo ufw --force enable

# =============================================================================
# 8. СОЗДАНИЕ ИНДЕКСОВ
# =============================================================================

echo "📊 Создаем оптимальные индексы..."

sudo -u postgres psql -d instagram_bot << 'EOF'
-- Индексы для пользовательской изоляции
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_accounts_user_id ON instagram_accounts(user_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_accounts_user_active ON instagram_accounts(user_id, is_active);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_publish_tasks_user_id ON publish_tasks(user_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_publish_tasks_user_status ON publish_tasks(user_id, status);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_publish_tasks_scheduled ON publish_tasks(scheduled_time) WHERE status = 'PENDING';

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_warmup_tasks_account_id ON warmup_tasks(account_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_warmup_tasks_status ON warmup_tasks(status);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_telegram_users_telegram_id ON telegram_users(telegram_id);

-- Частичные индексы для активных записей
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_accounts_active ON instagram_accounts(id) WHERE is_active = true;
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_tasks_pending ON publish_tasks(account_id, scheduled_time) WHERE status = 'PENDING';

\q
EOF

# =============================================================================
# 9. МОНИТОРИНГ И ЛОГИ
# =============================================================================

echo "📊 Настраиваем мониторинг..."

# Создаем директории для логов
sudo mkdir -p /var/log/postgresql-custom
sudo chown postgres:postgres /var/log/postgresql-custom

# Ротация логов
sudo cat > /etc/logrotate.d/postgresql-custom << 'EOF'
/var/log/postgresql-custom/*.log {
    daily
    missingok
    rotate 7
    compress
    notifempty
    create 640 postgres postgres
    postrotate
        /usr/bin/killall -HUP rsyslog 2> /dev/null || true
    endscript
}
EOF

# =============================================================================
# 10. ПЕРЕЗАПУСК И ПРОВЕРКА
# =============================================================================

echo "🔄 Перезапускаем PostgreSQL..."
sudo systemctl restart postgresql

# Ждем запуска
sleep 10

echo "✅ Проверяем статус..."
sudo systemctl status postgresql --no-pager

# Проверяем подключение
sudo -u postgres psql -c "SELECT version();"
sudo -u postgres psql -c "SHOW max_connections;"
sudo -u postgres psql -c "SHOW shared_buffers;"

# =============================================================================
# ИТОГИ
# =============================================================================

echo ""
echo "🎉" * 60
echo "🎉 НАСТРОЙКА ЗАВЕРШЕНА УСПЕШНО!"
echo "🎉" * 60
echo ""
echo "📊 ИНФОРМАЦИЯ О СЕРВЕРЕ:"
echo "🖥️  Сервер: VDSina PRO (8 CPU, 16GB RAM)"
echo "🐘 PostgreSQL: 15.x"
echo "🔌 Максимум подключений: 500"
echo "💾 Shared buffers: 4GB"
echo "⚡ Effective cache: 12GB"
echo ""
echo "🔐 ДАННЫЕ ДЛЯ ПОДКЛЮЧЕНИЯ:"
echo "🌐 Хост: $(curl -s ifconfig.me)"
echo "🔌 Порт: 5432"
echo "🗄️  База данных: instagram_bot"
echo "👤 Пользователь: instagram_user"
echo "🔑 Пароль: ${DB_PASSWORD}"
echo ""
echo "🔗 CONNECTION STRING:"
echo "postgresql://instagram_user:${DB_PASSWORD}@$(curl -s ifconfig.me):5432/instagram_bot"
echo ""
echo "📋 СЛЕДУЮЩИЕ ШАГИ:"
echo "1. Сохраните данные подключения"
echo "2. Обновите DATABASE_URL в config.py"
echo "3. Мигрируйте данные с Supabase"
echo "4. Протестируйте нагрузку"
echo ""
echo "🚀 ВАША БАЗА ГОТОВА К 100 ПОЛЬЗОВАТЕЛЯМ И 50,000 АККАУНТАМ!" 
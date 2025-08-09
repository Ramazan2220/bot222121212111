# 🛡️ HIGH AVAILABILITY DATABASE DESIGN

## 🎯 ЦЕЛЬ
Создать отказоустойчивую систему базы данных для Instagram-бота с автоматическим переключением при сбоях.

---

## 🏗️ РЕКОМЕНДУЕМАЯ АРХИТЕКТУРА

### 🔥 ВАРИАНТ 1: MASTER-SLAVE REPLICATION (ОПТИМАЛЬНЫЙ)

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   MASTER DB     │───▶│   SLAVE DB      │───▶│   SLAVE DB 2    │
│   (Write/Read)  │    │   (Read Only)   │    │   (Read Only)   │
│                 │    │                 │    │                 │
│ Hetzner AX61    │    │ Hetzner CX51    │    │ VDSina VPS      │
│ €45/месяц       │    │ €23/месяц       │    │ €15/месяц       │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         │              ┌────────▼──────────┐           │
         │              │   HAProxy         │           │
         │              │ Load Balancer     │           │
         │              └───────────────────┘           │
         │                                               │
    ┌────▼────┐                                    ┌────▼────┐
    │ App     │                                    │ Analytics│
    │ Writes  │                                    │ Reports  │
    └─────────┘                                    └─────────┘
```

### 📊 СТОИМОСТЬ:
```
Master DB (Hetzner AX61): €45/месяц
Slave DB 1 (Hetzner CX51): €23/месяц  
Slave DB 2 (VDSina): €15/месяц
HAProxy (малый VPS): €5/месяц
ИТОГО: €88/месяц

100 пользователей = €0.88/пользователь = $0.95/пользователь
```

---

## ⚙️ НАСТРОЙКА POSTGRESQL REPLICATION

### 1️⃣ MASTER КОНФИГУРАЦИЯ:

```bash
# /etc/postgresql/15/main/postgresql.conf
wal_level = replica
max_wal_senders = 3
wal_keep_size = 64
archive_mode = on
archive_command = 'cp %p /var/lib/postgresql/15/main/pg_wal_archive/%f'

# Разрешить подключения от slaves
listen_addresses = '*'
```

```bash
# /etc/postgresql/15/main/pg_hba.conf
# Разрешить репликацию
host replication replicator 192.168.1.0/24 md5
```

### 2️⃣ SLAVE КОНФИГУРАЦИЯ:

```bash
# Создаем базовый backup с Master
pg_basebackup -h master_ip -D /var/lib/postgresql/15/main -U replicator -P -W

# /var/lib/postgresql/15/main/postgresql.conf
hot_standby = on
```

```bash
# /var/lib/postgresql/15/main/standby.signal (пустой файл)
```

---

## 🔄 АВТОМАТИЧЕСКОЕ ПЕРЕКЛЮЧЕНИЕ (FAILOVER)

### 📋 СЦЕНАРИИ СБОЕВ:

#### 🚨 СЦЕНАРИЙ 1: Master недоступен
```python
1. HAProxy обнаруживает сбой Master (health check)
2. Останавливает направление запросов на Master
3. Промотирует Slave 1 в Master
4. Переключает приложение на новый Master
5. Время: 30-60 секунд
```

#### 🔧 АВТОМАТИЗАЦИЯ:

```bash
#!/bin/bash
# failover_script.sh

MASTER_HOST="master.db.local"
SLAVE_HOST="slave1.db.local"

# Проверяем доступность Master
if ! pg_isready -h $MASTER_HOST; then
    echo "Master недоступен! Запускаем failover..."
    
    # Промотируем Slave в Master
    ssh $SLAVE_HOST "sudo -u postgres pg_promote /var/lib/postgresql/15/main"
    
    # Обновляем конфигурацию приложения
    sed -i "s/$MASTER_HOST/$SLAVE_HOST/g" /app/config.py
    
    # Перезапускаем приложение
    systemctl restart instagram_bot
    
    echo "Failover завершен!"
fi
```

---

## 📱 ИНТЕГРАЦИЯ С ВАШИМ КОДОМ

### 🔧 ОБНОВЛЯЕМ DATABASE/DB_MANAGER.PY:

```python
import os
from sqlalchemy import create_engine
from sqlalchemy.pool import QueuePool

class DatabaseManager:
    def __init__(self):
        self.master_url = os.getenv('DATABASE_MASTER_URL')
        self.read_replica_urls = [
            os.getenv('DATABASE_SLAVE1_URL'),
            os.getenv('DATABASE_SLAVE2_URL')
        ]
        
        # Master для записи
        self.master_engine = create_engine(
            self.master_url,
            poolclass=QueuePool,
            pool_size=50,
            max_overflow=100,
            pool_timeout=60
        )
        
        # Read replicas для чтения
        self.read_engines = []
        for url in self.read_replica_urls:
            if url:
                engine = create_engine(url, pool_size=20, max_overflow=40)
                self.read_engines.append(engine)
    
    def get_write_session(self):
        """Сессия для записи (всегда Master)"""
        return sessionmaker(bind=self.master_engine)()
    
    def get_read_session(self):
        """Сессия для чтения (Round-robin по Slaves)"""
        import random
        if self.read_engines:
            engine = random.choice(self.read_engines)
            return sessionmaker(bind=engine)()
        else:
            # Fallback на Master если Slaves недоступны
            return self.get_write_session()
```

### 🔄 ОБНОВЛЯЕМ SAFE_USER_WRAPPER.PY:

```python
def get_user_instagram_accounts(user_id: int, session=None):
    """Получение аккаунтов пользователя (READ операция)"""
    if session is None:
        session = db_manager.get_read_session()  # Читаем со Slave
    
    try:
        accounts = session.query(InstagramAccount).filter_by(user_id=user_id).all()
        return accounts
    finally:
        session.close()

def create_instagram_account(user_id: int, **kwargs):
    """Создание аккаунта (WRITE операция)"""
    session = db_manager.get_write_session()  # Пишем в Master
    
    try:
        account = InstagramAccount(user_id=user_id, **kwargs)
        session.add(account)
        session.commit()
        return account
    except Exception as e:
        session.rollback()
        raise
    finally:
        session.close()
```

---

## 📊 МОНИТОРИНГ РЕПЛИКАЦИИ

### 🎯 КЛЮЧЕВЫЕ МЕТРИКИ:

```sql
-- На Master: проверяем lag репликации
SELECT client_addr, 
       state, 
       pg_wal_lsn_diff(pg_current_wal_lsn(), sent_lsn) as send_lag,
       pg_wal_lsn_diff(sent_lsn, flush_lsn) as flush_lag
FROM pg_stat_replication;

-- На Slave: проверяем delay
SELECT pg_last_wal_receive_lsn(), 
       pg_last_wal_replay_lsn(),
       pg_last_xact_replay_timestamp();
```

### 📱 АЛЕРТЫ:

```python
# monitoring/replication_check.py
import psycopg2
import telegram

def check_replication_lag():
    """Проверяем lag репликации"""
    try:
        conn = psycopg2.connect(MASTER_DB_URL)
        cur = conn.cursor()
        
        cur.execute("""
            SELECT max(pg_wal_lsn_diff(pg_current_wal_lsn(), sent_lsn)) 
            FROM pg_stat_replication
        """)
        
        lag_bytes = cur.fetchone()[0] or 0
        lag_mb = lag_bytes / 1024 / 1024
        
        if lag_mb > 100:  # Более 100MB lag
            send_alert(f"🚨 Репликация отстает на {lag_mb:.1f} MB!")
            
    except Exception as e:
        send_alert(f"❌ Ошибка проверки репликации: {e}")

def send_alert(message):
    bot = telegram.Bot(token=TELEGRAM_TOKEN)
    bot.send_message(chat_id=ADMIN_CHAT_ID, text=message)
```

---

## 💾 СТРАТЕГИЯ BACKUP

### 🔄 ЕЖЕДНЕВНЫЕ БЭКАПЫ:

```bash
#!/bin/bash
# daily_backup.sh

DATE=$(date +%Y%m%d)
BACKUP_DIR="/backups/postgresql"

# Полный backup Master DB
pg_dump -h master.db.local -U postgres instagram_bot > "$BACKUP_DIR/full_backup_$DATE.sql"

# Архивируем и сжимаем
gzip "$BACKUP_DIR/full_backup_$DATE.sql"

# Загружаем в облако (например, AWS S3)
aws s3 cp "$BACKUP_DIR/full_backup_$DATE.sql.gz" s3://my-backups/postgresql/

# Удаляем локальные бэкапы старше 7 дней
find $BACKUP_DIR -name "*.sql.gz" -mtime +7 -delete
```

### ⚡ POINT-IN-TIME RECOVERY:

```bash
# Настройка continuous archiving
archive_mode = on
archive_command = 'aws s3 cp %p s3://my-wal-archive/%f'

# Восстановление на определенное время
pg_basebackup -h master -D /tmp/recovery
echo "restore_command = 'aws s3 cp s3://my-wal-archive/%f %p'" >> /tmp/recovery/postgresql.conf
echo "recovery_target_time = '2025-08-07 14:30:00'" >> /tmp/recovery/postgresql.conf
```

---

## 🎯 ПЛАН ВНЕДРЕНИЯ

### 📅 ЭТАПЫ:

1. **Этап 1 (1 день)**: Настройка Slave сервера
2. **Этап 2 (1 день)**: Настройка репликации  
3. **Этап 3 (1 день)**: Интеграция в код (read/write split)
4. **Этап 4 (1 день)**: Настройка мониторинга
5. **Этап 5 (1 день)**: Тестирование failover
6. **Этап 6 (1 день)**: Настройка автоматизации

### 🧪 ТЕСТИРОВАНИЕ:

```bash
# Симуляция сбоя Master
sudo systemctl stop postgresql

# Проверяем автоматическое переключение
tail -f /var/log/instagram_bot/failover.log

# Восстанавливаем Master и проверяем синхронизацию
```

---

## 💰 ЭКОНОМИЧЕСКОЕ ОБОСНОВАНИЕ

### 📊 СТОИМОСТЬ vs ВЫГОДА:

```
Дополнительные расходы: €43/месяц
├── Slave сервер: €23/месяц
├── Второй Slave: €15/месяч  
└── Мониторинг: €5/месяц

Стоимость простоя (1 час):
├── Потеря выручки: $20,000/30/24 = $28/час
├── Восстановление репутации: $500-1000
└── Время на восстановление: 4-8 часов = $100-200

ROI: окупается за 2-3 часа простоя в год!
```

### ✅ ВЫВОДЫ:

1. **Репликация критично важна** для продакшна
2. **Стоимость минимальна** (~€0.4/пользователь)
3. **Автоматизация обязательна** (человек не должен участвовать)
4. **Мониторинг 24/7** предотвращает проблемы

---

## 🚀 ГОТОВЫЕ СКРИПТЫ

Хотите готовые скрипты для автоматической настройки? Могу создать:

1. **setup_master_replication.sh** - настройка Master
2. **setup_slave_replication.sh** - настройка Slave  
3. **failover_automation.py** - автоматическое переключение
4. **health_monitoring.py** - мониторинг состояния

**Создаем?** 🔧 
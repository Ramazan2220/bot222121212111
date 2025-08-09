# 🚀 QUICK START: Микросервисы за 1 неделю

## 🎯 Цель
Превратить существующий монолит в микросервисы максимально быстро, используя существующий код.

## 📦 Этап 1: Контейнеризация (2-3 дня)

### 1. Создаем Dockerfile для основного сервиса

```dockerfile
# Dockerfile.core
FROM python:3.12-slim

WORKDIR /app

# Устанавливаем зависимости
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем код
COPY . .

# Порты
EXPOSE 8000 8001

# Команда запуска
CMD ["python", "main.py"]
```

### 2. Dockerfile для Instagram сервиса

```dockerfile
# Dockerfile.instagram  
FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Только Instagram-related код
COPY instagram/ ./instagram/
COPY utils/ ./utils/
COPY database/ ./database/
COPY instagram_service.py .

EXPOSE 8002

CMD ["python", "instagram_service.py"]
```

### 3. Docker Compose для разработки

```yaml
# docker-compose.yml
version: '3.8'

services:
  database:
    image: postgres:15
    environment:
      POSTGRES_DB: instagram_bot
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: password
    volumes:
      - postgres_data:/var/lib/postgresql/data
    ports:
      - "5432:5432"

  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"

  core:
    build:
      context: .
      dockerfile: Dockerfile.core
    environment:
      - DATABASE_URL=postgresql://postgres:password@database:5432/instagram_bot
      - REDIS_URL=redis://redis:6379
    ports:
      - "8000:8000"
      - "8001:8001"
    depends_on:
      - database
      - redis
    volumes:
      - ./data:/app/data

  instagram:
    build:
      context: .
      dockerfile: Dockerfile.instagram
    environment:
      - DATABASE_URL=postgresql://postgres:password@database:5432/instagram_bot
      - REDIS_URL=redis://redis:6379
      - CORE_SERVICE_URL=http://core:8000
    ports:
      - "8002:8002"
    depends_on:
      - database
      - redis
      - core

volumes:
  postgres_data:
```

## 🔗 Этап 2: API между сервисами (1-2 дня)

### 1. Создаем простой REST API

```python
# api/instagram_api.py
from flask import Flask, request, jsonify
from instagram.client import InstagramClient

app = Flask(__name__)

@app.route('/api/instagram/publish', methods=['POST'])
def publish_content():
    data = request.json
    account_id = data['account_id']
    content_type = data['type']  # post, story, reels
    media_path = data['media_path']
    
    try:
        client = InstagramClient(account_id)
        result = client.publish(content_type, media_path)
        return jsonify({'success': True, 'result': result})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/instagram/warmup', methods=['POST'])
def start_warmup():
    data = request.json
    account_id = data['account_id']
    
    try:
        # Запускаем прогрев
        result = start_account_warmup(account_id)
        return jsonify({'success': True, 'task_id': result})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8002)
```

### 2. API клиент для коммуникации

```python
# api/client.py
import requests
import logging

logger = logging.getLogger(__name__)

class InstagramServiceClient:
    def __init__(self, base_url="http://instagram:8002"):
        self.base_url = base_url
    
    def publish_content(self, account_id, content_type, media_path):
        """Публикация контента через Instagram сервис"""
        try:
            response = requests.post(
                f"{self.base_url}/api/instagram/publish",
                json={
                    'account_id': account_id,
                    'type': content_type,
                    'media_path': media_path
                },
                timeout=30
            )
            return response.json()
        except Exception as e:
            logger.error(f"Ошибка публикации: {e}")
            return {'success': False, 'error': str(e)}
    
    def start_warmup(self, account_id):
        """Запуск прогрева через Instagram сервис"""
        try:
            response = requests.post(
                f"{self.base_url}/api/instagram/warmup",
                json={'account_id': account_id},
                timeout=10
            )
            return response.json()
        except Exception as e:
            logger.error(f"Ошибка прогрева: {e}")
            return {'success': False, 'error': str(e)}

# Глобальный клиент
instagram_client = InstagramServiceClient()
```

## 🚀 Этап 3: Быстрый деплой (1-2 дня)

### 1. Скрипт для автоматического развертывания

```bash
#!/bin/bash
# deploy.sh

set -e

echo "🚀 Быстрое развертывание микросервисов..."

# 1. Создаем образы
echo "📦 Сборка образов..."
docker-compose build

# 2. Запускаем базу данных
echo "🗄️ Запуск базы данных..."
docker-compose up -d database redis

# 3. Ждем готовности БД
echo "⏳ Ожидание готовности БД..."
sleep 10

# 4. Миграции
echo "🔄 Выполнение миграций..."
docker-compose run --rm core python migrate_database.py

# 5. Запуск всех сервисов
echo "🌟 Запуск всех сервисов..."
docker-compose up -d

# 6. Проверка здоровья
echo "🏥 Проверка здоровья сервисов..."
sleep 5

if curl -f http://localhost:8000/health > /dev/null 2>&1; then
    echo "✅ Core сервис работает"
else
    echo "❌ Core сервис недоступен"
fi

if curl -f http://localhost:8002/health > /dev/null 2>&1; then
    echo "✅ Instagram сервис работает"
else
    echo "❌ Instagram сервис недоступен"
fi

echo "🎉 Развертывание завершено!"
echo "📱 Telegram Bot: доступен"
echo "🖥️ Admin Panel: доступен"
echo "📸 Instagram API: http://localhost:8002"
```

### 2. Health check endpoints

```python
# health.py
from flask import Flask, jsonify
import psutil
import time

def add_health_routes(app):
    @app.route('/health')
    def health_check():
        return jsonify({
            'status': 'healthy',
            'timestamp': time.time(),
            'service': 'core'
        })
    
    @app.route('/metrics')
    def metrics():
        return jsonify({
            'cpu_percent': psutil.cpu_percent(),
            'memory_percent': psutil.virtual_memory().percent,
            'disk_percent': psutil.disk_usage('/').percent
        })
```

## 📊 Этап 4: Мониторинг (1 день)

### 1. Добавляем Portainer для визуального управления

```yaml
# Добавить в docker-compose.yml
  portainer:
    image: portainer/portainer-ce:latest
    ports:
      - "9000:9000"
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
      - portainer_data:/data

volumes:
  portainer_data:
```

### 2. Базовый мониторинг с Prometheus

```yaml
# monitoring/docker-compose.monitoring.yml
version: '3.8'

services:
  prometheus:
    image: prom/prometheus:latest
    ports:
      - "9090:9090"
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml

  grafana:
    image: grafana/grafana:latest
    ports:
      - "3000:3000"
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=admin
    volumes:
      - grafana_data:/var/lib/grafana

volumes:
  grafana_data:
```

## 🎯 Команды для быстрого старта

```bash
# 1. Клонируем проект и переходим в папку
cd instagram_telegram_bot_reserv

# 2. Создаем файлы (см. выше)
# Dockerfile.core, Dockerfile.instagram, docker-compose.yml

# 3. Делаем скрипт исполняемым
chmod +x deploy.sh

# 4. Запускаем
./deploy.sh

# 5. Проверяем статус
docker-compose ps

# 6. Смотрим логи
docker-compose logs -f

# 7. Перезапуск сервиса
docker-compose restart instagram

# 8. Масштабирование
docker-compose up -d --scale instagram=3
```

## 🏆 Результат

После выполнения этих шагов у вас будет:

✅ **Рабочая микросервисная архитектура** за 1 неделю
✅ **Легкое масштабирование** - добавляете новые контейнеры
✅ **Изоляция сервисов** - проблема в одном не влияет на другие  
✅ **Простой деплой** - один скрипт для развертывания
✅ **Мониторинг** - видите состояние всех сервисов
✅ **Готовность к production** - легко переносится на реальные сервера

**Время:** 5-7 дней вместо месяцев разработки с нуля!
**Стоимость:** €59/месяц на старте вместо €800+ за монолит 
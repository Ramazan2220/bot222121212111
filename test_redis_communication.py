#!/usr/bin/env python3
"""
🧪 Тест межпроцессной связи FakeRedis
"""

import sys
import os
import time
import json
import threading
from datetime import datetime

# Добавляем путь к проекту
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from fake_redis import get_fake_redis

def test_publish():
    """Тест отправки сообщений"""
    print("🔥 ТЕСТ ОТПРАВИТЕЛЬ: Запуск...")
    
    redis_client = get_fake_redis()
    
    for i in range(5):
        message = {
            "user_id": 6499246016,
            "timestamp": datetime.now().isoformat(),
            "test_number": i + 1
        }
        
        result = redis_client.publish("access:user_removed", json.dumps(message))
        print(f"📤 ОТПРАВЛЕНО #{i+1}: {message}")
        print(f"📊 Результат publish: {result}")
        
        time.sleep(2)

def test_subscribe():
    """Тест получения сообщений"""
    print("🔥 ТЕСТ ПОЛУЧАТЕЛЬ: Запуск...")
    
    redis_client = get_fake_redis()
    pubsub = redis_client.pubsub()
    
    # Подписываемся
    pubsub.subscribe("access:user_removed")
    print("📡 Подписался на access:user_removed")
    
    # Слушаем
    for message in pubsub.listen():
        if message['type'] == 'message':
            try:
                data = json.loads(message['data'])
                print(f"📥 ПОЛУЧЕНО: {data}")
            except Exception as e:
                print(f"❌ Ошибка: {e}")

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Использование:")
        print("  python test_redis_communication.py publish")
        print("  python test_redis_communication.py subscribe")
        sys.exit(1)
    
    if sys.argv[1] == "publish":
        test_publish()
    elif sys.argv[1] == "subscribe":
        test_subscribe()
    else:
        print("❌ Неверная команда. Используйте 'publish' или 'subscribe'") 
"""
🧪 Тест межпроцессной связи FakeRedis
"""

import sys
import os
import time
import json
import threading
from datetime import datetime

# Добавляем путь к проекту
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from fake_redis import get_fake_redis

def test_publish():
    """Тест отправки сообщений"""
    print("🔥 ТЕСТ ОТПРАВИТЕЛЬ: Запуск...")
    
    redis_client = get_fake_redis()
    
    for i in range(5):
        message = {
            "user_id": 6499246016,
            "timestamp": datetime.now().isoformat(),
            "test_number": i + 1
        }
        
        result = redis_client.publish("access:user_removed", json.dumps(message))
        print(f"📤 ОТПРАВЛЕНО #{i+1}: {message}")
        print(f"📊 Результат publish: {result}")
        
        time.sleep(2)

def test_subscribe():
    """Тест получения сообщений"""
    print("🔥 ТЕСТ ПОЛУЧАТЕЛЬ: Запуск...")
    
    redis_client = get_fake_redis()
    pubsub = redis_client.pubsub()
    
    # Подписываемся
    pubsub.subscribe("access:user_removed")
    print("📡 Подписался на access:user_removed")
    
    # Слушаем
    for message in pubsub.listen():
        if message['type'] == 'message':
            try:
                data = json.loads(message['data'])
                print(f"📥 ПОЛУЧЕНО: {data}")
            except Exception as e:
                print(f"❌ Ошибка: {e}")

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Использование:")
        print("  python test_redis_communication.py publish")
        print("  python test_redis_communication.py subscribe")
        sys.exit(1)
    
    if sys.argv[1] == "publish":
        test_publish()
    elif sys.argv[1] == "subscribe":
        test_subscribe()
    else:
        print("❌ Неверная команда. Используйте 'publish' или 'subscribe'") 
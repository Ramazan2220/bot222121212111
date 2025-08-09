import os
import json
import sqlite3
import logging
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)

def init_user_storage(user_id: int, username: str):
    """Инициализация персонального хранилища пользователя"""
    try:
        # Создаем персональную директорию пользователя
        user_dir = f"data/users/{user_id}"
        os.makedirs(user_dir, exist_ok=True)
        
        # Создаем поддиректории для разных типов данных
        subdirs = [
            "accounts",      # Instagram аккаунты пользователя
            "media",         # Загруженные медиафайлы
            "sessions",      # Сессии Instagram
            "analytics",     # Аналитические данные
            "campaigns",     # Кампании и задачи
            "exports",       # Экспортированные данные
            "temp"          # Временные файлы
        ]
        
        for subdir in subdirs:
            os.makedirs(f"{user_dir}/{subdir}", exist_ok=True)
        
        # Создаем конфигурационный файл пользователя
        user_config_file = f"{user_dir}/config.json"
        if not os.path.exists(user_config_file):
            user_config = {
                "user_id": user_id,
                "username": username,
                "created_at": datetime.now().isoformat(),
                "last_activity": datetime.now().isoformat(),
                "settings": {
                    "notifications": True,
                    "auto_backup": True,
                    "analytics_enabled": True,
                    "default_timezone": "UTC"
                },
                "statistics": {
                    "accounts_added": 0,
                    "posts_published": 0,
                    "campaigns_created": 0,
                    "total_sessions": 0
                }
            }
            
            with open(user_config_file, 'w', encoding='utf-8') as f:
                json.dump(user_config, f, ensure_ascii=False, indent=2)
        
        # Создаем персональную базу данных SQLite для пользователя
        create_user_database(user_id)
        
        logger.info(f"✅ Персональное хранилище инициализировано для пользователя {user_id}")
        
    except Exception as e:
        logger.error(f"❌ Ошибка инициализации хранилища для {user_id}: {e}")

def create_user_database(user_id: int):
    """Создание персональной базы данных для пользователя"""
    try:
        db_path = f"data/users/{user_id}/user_data.db"
        
        # Создаем базу данных если её нет
        if not os.path.exists(db_path):
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            # Таблица Instagram аккаунтов пользователя
            cursor.execute('''
                CREATE TABLE user_instagram_accounts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    username TEXT UNIQUE NOT NULL,
                    password TEXT,
                    email TEXT,
                    phone TEXT,
                    is_active BOOLEAN DEFAULT 1,
                    status TEXT DEFAULT 'active',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_activity TIMESTAMP,
                    session_data TEXT,
                    notes TEXT
                )
            ''')
            
            # Таблица задач пользователя
            cursor.execute('''
                CREATE TABLE user_tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    account_id INTEGER,
                    task_type TEXT NOT NULL,
                    status TEXT DEFAULT 'pending',
                    scheduled_time TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    completed_at TIMESTAMP,
                    result TEXT,
                    error_message TEXT,
                    FOREIGN KEY (account_id) REFERENCES user_instagram_accounts (id)
                )
            ''')
            
            # Таблица аналитики пользователя
            cursor.execute('''
                CREATE TABLE user_analytics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    account_id INTEGER,
                    metric_name TEXT NOT NULL,
                    metric_value REAL,
                    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    metadata TEXT,
                    FOREIGN KEY (account_id) REFERENCES user_instagram_accounts (id)
                )
            ''')
            
            # Таблица настроек пользователя
            cursor.execute('''
                CREATE TABLE user_settings (
                    key TEXT PRIMARY KEY,
                    value TEXT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            conn.commit()
            conn.close()
            
            logger.info(f"✅ Персональная база данных создана для пользователя {user_id}")
    
    except Exception as e:
        logger.error(f"❌ Ошибка создания БД для пользователя {user_id}: {e}")

def get_user_data_path(user_id: int, subdir: str = "") -> str:
    """Получение пути к данным пользователя"""
    base_path = f"data/users/{user_id}"
    if subdir:
        return f"{base_path}/{subdir}"
    return base_path

def get_user_database_connection(user_id: int):
    """Получение подключения к персональной базе пользователя"""
    db_path = f"data/users/{user_id}/user_data.db"
    return sqlite3.connect(db_path)

def update_user_activity(user_id: int):
    """Обновление времени последней активности пользователя"""
    try:
        config_file = f"data/users/{user_id}/config.json"
        if os.path.exists(config_file):
            with open(config_file, 'r', encoding='utf-8') as f:
                config = json.load(f)
            
            config['last_activity'] = datetime.now().isoformat()
            
            with open(config_file, 'w', encoding='utf-8') as f:
                json.dump(config, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"Ошибка обновления активности пользователя {user_id}: {e}")

def get_user_config(user_id: int) -> Optional[dict]:
    """Получение конфигурации пользователя"""
    try:
        config_file = f"data/users/{user_id}/config.json"
        if os.path.exists(config_file):
            with open(config_file, 'r', encoding='utf-8') as f:
                return json.load(f)
    except Exception as e:
        logger.error(f"Ошибка чтения конфига пользователя {user_id}: {e}")
    return None

def update_user_statistics(user_id: int, stat_name: str, increment: int = 1):
    """Обновление статистики пользователя"""
    try:
        config = get_user_config(user_id)
        if config:
            if stat_name in config.get('statistics', {}):
                config['statistics'][stat_name] += increment
            else:
                config['statistics'][stat_name] = increment
            
            config_file = f"data/users/{user_id}/config.json"
            with open(config_file, 'w', encoding='utf-8') as f:
                json.dump(config, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"Ошибка обновления статистики пользователя {user_id}: {e}")

def cleanup_user_temp_files(user_id: int):
    """Очистка временных файлов пользователя"""
    try:
        temp_dir = get_user_data_path(user_id, "temp")
        if os.path.exists(temp_dir):
            import shutil
            shutil.rmtree(temp_dir)
            os.makedirs(temp_dir, exist_ok=True)
            logger.info(f"Временные файлы пользователя {user_id} очищены")
    except Exception as e:
        logger.error(f"Ошибка очистки временных файлов пользователя {user_id}: {e}")

def get_user_storage_info(user_id: int) -> dict:
    """Получение информации о хранилище пользователя"""
    try:
        user_path = get_user_data_path(user_id)
        if not os.path.exists(user_path):
            return {"exists": False}
        
        # Подсчет размера директории
        total_size = 0
        file_count = 0
        for dirpath, dirnames, filenames in os.walk(user_path):
            for filename in filenames:
                filepath = os.path.join(dirpath, filename)
                total_size += os.path.getsize(filepath)
                file_count += 1
        
        # Получение конфига
        config = get_user_config(user_id)
        
        return {
            "exists": True,
            "path": user_path,
            "total_size_bytes": total_size,
            "total_size_mb": round(total_size / 1024 / 1024, 2),
            "file_count": file_count,
            "created_at": config.get('created_at') if config else None,
            "last_activity": config.get('last_activity') if config else None,
            "statistics": config.get('statistics') if config else {}
        }
    except Exception as e:
        logger.error(f"Ошибка получения информации о хранилище {user_id}: {e}")
        return {"exists": False, "error": str(e)} 
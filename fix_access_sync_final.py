#!/usr/bin/env python3
"""
ОКОНЧАТЕЛЬНОЕ ИСПРАВЛЕНИЕ СИНХРОНИЗАЦИИ ДОСТУПОВ
Создает shared файловый кеш между админ ботом и основным ботом
"""

import os
import json
import time
from datetime import datetime

# Путь к общему файлу кеша
SHARED_CACHE_FILE = "data/shared_access_cache.json"

def create_shared_cache_system():
    """Создает систему общего кеша доступов"""
    
    print("🔧 СОЗДАНИЕ СИСТЕМЫ ОБЩЕГО КЕША ДОСТУПОВ")
    print("=" * 60)
    
    # 1. Создаем директорию data если её нет
    os.makedirs("data", exist_ok=True)
    print("✅ Директория data создана")
    
    # 2. Патчим AccessManager для использования файлового кеша
    patch_code = '''
    def _load_shared_cache(self):
        """Загружает общий кеш из файла"""
        try:
            if os.path.exists("{cache_file}"):
                with open("{cache_file}", "r", encoding="utf-8") as f:
                    data = json.load(f)
                    # Проверяем время последнего обновления
                    if time.time() - data.get("last_update", 0) < 30:  # 30 секунд
                        return data.get("cache", {{}})
        except Exception as e:
            self.logger.error(f"Ошибка загрузки общего кеша: {{e}}")
        return {{}}
    
    def _save_shared_cache(self):
        """Сохраняет общий кеш в файл"""
        try:
            cache_data = {{
                "cache": self._access_cache,
                "last_update": time.time(),
                "updated_by": "AccessManager"
            }}
            with open("{cache_file}", "w", encoding="utf-8") as f:
                json.dump(cache_data, f, ensure_ascii=False, indent=2, default=str)
        except Exception as e:
            self.logger.error(f"Ошибка сохранения общего кеша: {{e}}")
    
    def _sync_with_shared_cache(self):
        """Синхронизирует с общим кешем"""
        shared_cache = self._load_shared_cache()
        if shared_cache:
            self._access_cache.update(shared_cache)
            self.logger.info(f"Синхронизирован с общим кешем: {{len(shared_cache)}} пользователей")
    '''.format(cache_file=SHARED_CACHE_FILE)
    
    # 3. Читаем текущий AccessManager
    with open("utils/access_manager.py", "r", encoding="utf-8") as f:
        content = f.read()
    
    # 4. Добавляем импорты если их нет
    if "import json" not in content:
        content = content.replace("import logging", "import logging\nimport json\nimport time")
    
    # 5. Добавляем методы в класс AccessManager
    if "_load_shared_cache" not in content:
        # Находим конец класса AccessManager и добавляем методы
        class_end = content.find("\n# Глобальный экземпляр")
        if class_end == -1:
            class_end = content.find("\ndef add_user_access")
        
        if class_end != -1:
            content = content[:class_end] + patch_code + content[class_end:]
    
    # 6. Патчим метод _sync_access_lists
    if "_sync_with_shared_cache()" not in content:
        content = content.replace(
            "self._last_sync = datetime.now()",
            "self._last_sync = datetime.now()\n        self._save_shared_cache()"
        )
    
    # 7. Патчим метод force_sync
    if "_sync_with_shared_cache" not in content:
        content = content.replace(
            "def force_sync(self):",
            "def force_sync(self):\n        \"\"\"Принудительная синхронизация с общим кешем\"\"\"\n        self._sync_with_shared_cache()"
        )
    
    # 8. Сохраняем патченный файл
    with open("utils/access_manager.py", "w", encoding="utf-8") as f:
        f.write(content)
    
    print("✅ AccessManager пропатчен для работы с общим кешем")
    
    # 9. Создаем начальный кеш
    try:
        import sys
        sys.path.insert(0, './utils')
        from access_manager import get_access_manager
        
        manager = get_access_manager()
        manager.force_sync()
        print("✅ Начальный кеш создан")
        
    except Exception as e:
        print(f"⚠️ Ошибка создания начального кеша: {e}")
    
    print("\n🎉 СИСТЕМА ОБЩЕГО КЕША СОЗДАНА!")
    print(f"📁 Файл кеша: {SHARED_CACHE_FILE}")
    print("\n🔄 ПЕРЕЗАПУСТИТЕ ОБА БОТА для применения изменений:")
    print("1. Остановите админ бот (Ctrl+C)")
    print("2. Остановите основной бот (Ctrl+C)")  
    print("3. Запустите админ бот: source test_env/bin/activate && python admin_bot/main.py")
    print("4. Запустите основной бот: source test_env/bin/activate && python main.py")

if __name__ == "__main__":
    create_shared_cache_system() 
"""
ОКОНЧАТЕЛЬНОЕ ИСПРАВЛЕНИЕ СИНХРОНИЗАЦИИ ДОСТУПОВ
Создает shared файловый кеш между админ ботом и основным ботом
"""

import os
import json
import time
from datetime import datetime

# Путь к общему файлу кеша
SHARED_CACHE_FILE = "data/shared_access_cache.json"

def create_shared_cache_system():
    """Создает систему общего кеша доступов"""
    
    print("🔧 СОЗДАНИЕ СИСТЕМЫ ОБЩЕГО КЕША ДОСТУПОВ")
    print("=" * 60)
    
    # 1. Создаем директорию data если её нет
    os.makedirs("data", exist_ok=True)
    print("✅ Директория data создана")
    
    # 2. Патчим AccessManager для использования файлового кеша
    patch_code = '''
    def _load_shared_cache(self):
        """Загружает общий кеш из файла"""
        try:
            if os.path.exists("{cache_file}"):
                with open("{cache_file}", "r", encoding="utf-8") as f:
                    data = json.load(f)
                    # Проверяем время последнего обновления
                    if time.time() - data.get("last_update", 0) < 30:  # 30 секунд
                        return data.get("cache", {{}})
        except Exception as e:
            self.logger.error(f"Ошибка загрузки общего кеша: {{e}}")
        return {{}}
    
    def _save_shared_cache(self):
        """Сохраняет общий кеш в файл"""
        try:
            cache_data = {{
                "cache": self._access_cache,
                "last_update": time.time(),
                "updated_by": "AccessManager"
            }}
            with open("{cache_file}", "w", encoding="utf-8") as f:
                json.dump(cache_data, f, ensure_ascii=False, indent=2, default=str)
        except Exception as e:
            self.logger.error(f"Ошибка сохранения общего кеша: {{e}}")
    
    def _sync_with_shared_cache(self):
        """Синхронизирует с общим кешем"""
        shared_cache = self._load_shared_cache()
        if shared_cache:
            self._access_cache.update(shared_cache)
            self.logger.info(f"Синхронизирован с общим кешем: {{len(shared_cache)}} пользователей")
    '''.format(cache_file=SHARED_CACHE_FILE)
    
    # 3. Читаем текущий AccessManager
    with open("utils/access_manager.py", "r", encoding="utf-8") as f:
        content = f.read()
    
    # 4. Добавляем импорты если их нет
    if "import json" not in content:
        content = content.replace("import logging", "import logging\nimport json\nimport time")
    
    # 5. Добавляем методы в класс AccessManager
    if "_load_shared_cache" not in content:
        # Находим конец класса AccessManager и добавляем методы
        class_end = content.find("\n# Глобальный экземпляр")
        if class_end == -1:
            class_end = content.find("\ndef add_user_access")
        
        if class_end != -1:
            content = content[:class_end] + patch_code + content[class_end:]
    
    # 6. Патчим метод _sync_access_lists
    if "_sync_with_shared_cache()" not in content:
        content = content.replace(
            "self._last_sync = datetime.now()",
            "self._last_sync = datetime.now()\n        self._save_shared_cache()"
        )
    
    # 7. Патчим метод force_sync
    if "_sync_with_shared_cache" not in content:
        content = content.replace(
            "def force_sync(self):",
            "def force_sync(self):\n        \"\"\"Принудительная синхронизация с общим кешем\"\"\"\n        self._sync_with_shared_cache()"
        )
    
    # 8. Сохраняем патченный файл
    with open("utils/access_manager.py", "w", encoding="utf-8") as f:
        f.write(content)
    
    print("✅ AccessManager пропатчен для работы с общим кешем")
    
    # 9. Создаем начальный кеш
    try:
        import sys
        sys.path.insert(0, './utils')
        from access_manager import get_access_manager
        
        manager = get_access_manager()
        manager.force_sync()
        print("✅ Начальный кеш создан")
        
    except Exception as e:
        print(f"⚠️ Ошибка создания начального кеша: {e}")
    
    print("\n🎉 СИСТЕМА ОБЩЕГО КЕША СОЗДАНА!")
    print(f"📁 Файл кеша: {SHARED_CACHE_FILE}")
    print("\n🔄 ПЕРЕЗАПУСТИТЕ ОБА БОТА для применения изменений:")
    print("1. Остановите админ бот (Ctrl+C)")
    print("2. Остановите основной бот (Ctrl+C)")  
    print("3. Запустите админ бот: source test_env/bin/activate && python admin_bot/main.py")
    print("4. Запустите основной бот: source test_env/bin/activate && python main.py")

if __name__ == "__main__":
    create_shared_cache_system() 
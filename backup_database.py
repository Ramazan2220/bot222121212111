#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Автоматическое резервное копирование БД
"""

import os
import shutil
import logging
from datetime import datetime
import schedule
import time
import gzip

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseBackup:
    def __init__(self):
        self.db_path = "data/database.sqlite"
        self.backup_dir = "data/backups"
        self.max_backups = 10  # Максимум 10 резервных копий
        
        # Создаем директорию для бэкапов
        os.makedirs(self.backup_dir, exist_ok=True)
    
    def backup(self):
        """Создать резервную копию БД"""
        try:
            # Проверяем существование БД
            if not os.path.exists(self.db_path):
                logger.error(f"БД не найдена: {self.db_path}")
                return False
            
            # Формируем имя файла бэкапа
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_name = f"database_backup_{timestamp}.sqlite.gz"
            backup_path = os.path.join(self.backup_dir, backup_name)
            
            # Создаем сжатую копию
            with open(self.db_path, 'rb') as f_in:
                with gzip.open(backup_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            
            logger.info(f"✅ Бэкап создан: {backup_path}")
            
            # Очищаем старые бэкапы
            self._cleanup_old_backups()
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка создания бэкапа: {e}")
            return False
    
    def _cleanup_old_backups(self):
        """Удалить старые бэкапы, оставив только последние N"""
        try:
            # Получаем список всех бэкапов
            backups = []
            for filename in os.listdir(self.backup_dir):
                if filename.startswith("database_backup_") and filename.endswith(".sqlite.gz"):
                    filepath = os.path.join(self.backup_dir, filename)
                    backups.append((filepath, os.path.getctime(filepath)))
            
            # Сортируем по дате создания
            backups.sort(key=lambda x: x[1], reverse=True)
            
            # Удаляем старые
            for backup_path, _ in backups[self.max_backups:]:
                os.remove(backup_path)
                logger.info(f"🗑️ Удален старый бэкап: {backup_path}")
                
        except Exception as e:
            logger.error(f"❌ Ошибка при очистке старых бэкапов: {e}")
    
    def restore(self, backup_filename):
        """Восстановить БД из бэкапа"""
        try:
            backup_path = os.path.join(self.backup_dir, backup_filename)
            
            if not os.path.exists(backup_path):
                logger.error(f"Бэкап не найден: {backup_path}")
                return False
            
            # Создаем копию текущей БД
            if os.path.exists(self.db_path):
                shutil.copy2(self.db_path, f"{self.db_path}.before_restore")
            
            # Восстанавливаем из бэкапа
            with gzip.open(backup_path, 'rb') as f_in:
                with open(self.db_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            
            logger.info(f"✅ БД восстановлена из: {backup_path}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка восстановления: {e}")
            return False

def run_scheduled_backups():
    """Запустить автоматические бэкапы каждые 6 часов"""
    backup = DatabaseBackup()
    
    # Создаем первый бэкап сразу
    backup.backup()
    
    # Планируем бэкапы каждые 6 часов
    schedule.every(6).hours.do(backup.backup)
    
    logger.info("🚀 Автоматические бэкапы запущены (каждые 6 часов)")
    
    while True:
        schedule.run_pending()
        time.sleep(60)  # Проверяем каждую минуту

if __name__ == "__main__":
    backup = DatabaseBackup()
    
    print("1. Создать бэкап")
    print("2. Восстановить из бэкапа")
    print("3. Запустить автоматические бэкапы")
    
    choice = input("\nВыберите действие (1-3): ")
    
    if choice == "1":
        backup.backup()
    elif choice == "2":
        # Показываем доступные бэкапы
        backups = [f for f in os.listdir(backup.backup_dir) 
                   if f.startswith("database_backup_") and f.endswith(".sqlite.gz")]
        
        if not backups:
            print("❌ Нет доступных бэкапов")
        else:
            print("\nДоступные бэкапы:")
            for i, b in enumerate(backups, 1):
                print(f"{i}. {b}")
            
            idx = int(input("\nВыберите номер бэкапа: ")) - 1
            if 0 <= idx < len(backups):
                backup.restore(backups[idx])
    elif choice == "3":
        run_scheduled_backups() 
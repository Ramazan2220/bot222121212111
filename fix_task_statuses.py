#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Скрипт для исправления статусов задач в базе данных
"""

import sqlite3
from pathlib import Path

# Путь к базе данных
DB_PATH = Path("data/database.sqlite")

def fix_task_statuses():
    """Исправляет строковые значения статусов на правильные enum значения"""
    
    if not DB_PATH.exists():
        print(f"❌ База данных не найдена: {DB_PATH}")
        return False
    
    try:
        # Подключаемся к базе данных
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Словарь для замены статусов
        status_mapping = {
            'pending': 'PENDING',
            'processing': 'PROCESSING', 
            'completed': 'COMPLETED',
            'failed': 'FAILED',
            'cancelled': 'CANCELLED',
            'scheduled': 'SCHEDULED'
        }
        
        # Исправляем статусы в таблице publish_tasks
        print("📝 Исправляем статусы в таблице publish_tasks...")
        for old_status, new_status in status_mapping.items():
            cursor.execute("""
                UPDATE publish_tasks 
                SET status = ? 
                WHERE LOWER(status) = ?
            """, (new_status, old_status))
            
            if cursor.rowcount > 0:
                print(f"   ✅ Обновлено {cursor.rowcount} записей: {old_status} -> {new_status}")
        
        # Исправляем статусы в таблице tasks (если есть)
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='tasks'")
        if cursor.fetchone():
            print("\n📝 Исправляем статусы в таблице tasks...")
            for old_status, new_status in status_mapping.items():
                cursor.execute("""
                    UPDATE tasks 
                    SET status = ? 
                    WHERE LOWER(status) = ?
                """, (new_status, old_status))
                
                if cursor.rowcount > 0:
                    print(f"   ✅ Обновлено {cursor.rowcount} записей: {old_status} -> {new_status}")
        
        # Сохраняем изменения
        conn.commit()
        
        # Проверяем текущие статусы
        print("\n📊 Текущие статусы в publish_tasks:")
        cursor.execute("SELECT status, COUNT(*) FROM publish_tasks GROUP BY status")
        for status, count in cursor.fetchall():
            print(f"   - {status}: {count}")
        
        conn.close()
        print("\n✅ Статусы успешно исправлены!")
        return True
        
    except Exception as e:
        print(f"❌ Ошибка при исправлении статусов: {e}")
        return False

if __name__ == "__main__":
    fix_task_statuses() 
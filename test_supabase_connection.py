#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
🔍 ТЕСТ ПОДКЛЮЧЕНИЯ К SUPABASE
Проверяем подключение перед миграцией
"""

import logging
import sys

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

try:
    import psycopg2
except ImportError:
    logger.error("❌ psycopg2 не установлен")
    sys.exit(1)

def test_connection():
    """Тест подключения"""
    print("🔍 ТЕСТ ПОДКЛЮЧЕНИЯ К SUPABASE")
    print("=" * 50)
    
    # Получаем URL от пользователя
    url = input("Введите PostgreSQL URL от Supabase: ").strip()
    
    if not url:
        logger.error("❌ URL не может быть пустым")
        return False
        
    # Проверяем формат
    if not url.startswith('postgresql://'):
        logger.error("❌ URL должен начинаться с postgresql://")
        return False
        
    print(f"\n🔗 Тестирую подключение...")
    print(f"📍 Хост: {url.split('@')[1].split(':')[0] if '@' in url else 'неизвестен'}")
    
    try:
        # Пробуем подключиться
        conn = psycopg2.connect(url)
        
        with conn.cursor() as cursor:
            cursor.execute("SELECT version();")
            version = cursor.fetchone()[0]
            
        conn.close()
        
        logger.info("✅ ПОДКЛЮЧЕНИЕ УСПЕШНО!")
        logger.info(f"🐘 PostgreSQL версия: {version}")
        return True
        
    except Exception as e:
        logger.error(f"❌ ОШИБКА ПОДКЛЮЧЕНИЯ: {e}")
        
        # Даем подсказки
        print("\n💡 ВОЗМОЖНЫЕ РЕШЕНИЯ:")
        print("1. Проверьте что Supabase проект полностью создан")
        print("2. Убедитесь что URL скопирован правильно")
        print("3. Проверьте что пароль правильный (без квадратных скобок)")
        print("4. Попробуйте скопировать URL заново из Supabase Dashboard")
        
        return False

if __name__ == "__main__":
    success = test_connection()
    if success:
        print("\n🎉 ПОДКЛЮЧЕНИЕ РАБОТАЕТ!")
        print("Теперь можно запустить миграцию:")
        print("python migrate_to_supabase.py")
    else:
        print("\n💥 ПОДКЛЮЧЕНИЕ НЕ РАБОТАЕТ!")
        print("Исправьте проблемы и попробуйте снова") 
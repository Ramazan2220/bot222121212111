#!/usr/bin/env python3
"""
Быстрая проверка системы доступов
"""

import sys
import os

# Добавляем текущую директорию в путь
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def main():
    print("🔍 БЫСТРАЯ ПРОВЕРКА ДОСТУПОВ")
    print("=" * 50)
    
    test_user_id = 6626270112
    
    try:
        # Импортируем модули напрямую
        sys.path.insert(0, './utils')
        from access_manager import has_access, get_access_manager
        
        print(f"✅ Импорт access_manager успешен")
        
        # Проверяем доступ
        result = has_access(test_user_id)
        print(f"📊 has_access({test_user_id}): {result}")
        
        # Получаем менеджер
        manager = get_access_manager()
        print(f"✅ AccessManager получен: {type(manager).__name__}")
        
        # Принудительная синхронизация
        manager.force_sync()
        print("✅ Принудительная синхронизация выполнена")
        
        # Повторная проверка
        result2 = has_access(test_user_id)
        print(f"📊 has_access после синхронизации: {result2}")
        
        # Детальная информация
        user_info = manager.get_user_info(test_user_id)
        if user_info:
            print("📋 Информация о пользователе:")
            for key, value in user_info.items():
                print(f"   {key}: {value}")
        else:
            print("❌ Информация о пользователе не найдена")
            
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main() 
 
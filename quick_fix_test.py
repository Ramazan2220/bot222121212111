#!/usr/bin/env python3
"""
БЫСТРЫЙ ТЕСТ НОВОЙ СИСТЕМЫ СИНХРОНИЗАЦИИ
Исправленная версия с правильными импортами
"""

import sys
import os
import time

# Добавляем пути для импорта
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)
sys.path.insert(0, os.path.join(current_dir, 'utils'))
sys.path.insert(0, os.path.join(current_dir, 'admin_bot'))

def test_direct_import():
    """Тестирует прямой импорт новой системы"""
    print("🧪 ТЕСТ ПРЯМОГО ИМПОРТА")
    print("=" * 60)
    
    user_id = 6626270112
    
    try:
        # Импортируем напрямую из файла
        from multiprocessing_access_sync import has_access_mp, add_user_mp, remove_user_mp
        
        print("✅ Импорт успешен")
        
        # Проверяем начальное состояние
        print(f"\n1️⃣ Начальное состояние:")
        initial = has_access_mp(user_id)
        print(f"   has_access({user_id}): {initial}")
        
        # Добавляем пользователя
        print(f"\n2️⃣ Добавляем пользователя:")
        user_data = {
            'telegram_id': user_id,
            'is_active': True,
            'subscription_end': '2025-09-01T00:00:00',
            'role': 'trial'
        }
        add_result = add_user_mp(user_id, user_data)
        print(f"   add_user({user_id}): {add_result}")
        
        # Проверяем сразу
        after_add = has_access_mp(user_id)
        print(f"   has_access({user_id}) после добавления: {after_add}")
        
        # Удаляем пользователя
        print(f"\n3️⃣ Удаляем пользователя:")
        remove_result = remove_user_mp(user_id)
        print(f"   remove_user({user_id}): {remove_result}")
        
        # Проверяем сразу
        after_remove = has_access_mp(user_id)
        print(f"   has_access({user_id}) после удаления: {after_remove}")
        
        # Результат
        print(f"\n🏁 РЕЗУЛЬТАТ:")
        if not initial and after_add and not after_remove:
            print(f"   🎉 ВСЁ РАБОТАЕТ ИДЕАЛЬНО!")
            return True
        else:
            print(f"   ❌ Есть проблемы")
            print(f"     Начальное: {initial} (ожидали False)")
            print(f"     После добавления: {after_add} (ожидали True)")
            print(f"     После удаления: {after_remove} (ожидали False)")
            return False
            
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_existing_functions():
    """Тестирует существующие функции из access_manager"""
    print(f"\n🔧 ТЕСТ СУЩЕСТВУЮЩИХ ФУНКЦИЙ")
    print("=" * 60)
    
    user_id = 6626270112
    
    try:
        # Пробуем импортировать из access_manager напрямую
        import access_manager
        
        # Проверяем что функция has_access существует
        if hasattr(access_manager, 'has_access'):
            print("✅ Функция has_access найдена")
            
            # Проверяем новые функции
            if hasattr(access_manager, 'USE_NEW_SYNC'):
                print("✅ Новая система интегрирована!")
                
                # Тестируем
                initial = access_manager.has_access(user_id)
                print(f"   has_access({user_id}): {initial}")
                
                if hasattr(access_manager, 'add_user_access'):
                    add_result = access_manager.add_user_access(user_id)
                    print(f"   add_user_access({user_id}): {add_result}")
                    
                    after_add = access_manager.has_access(user_id)
                    print(f"   has_access({user_id}) после добавления: {after_add}")
                    
                    if hasattr(access_manager, 'delete_user_completely'):
                        remove_result = access_manager.delete_user_completely(user_id)
                        print(f"   delete_user_completely({user_id}): {remove_result}")
                        
                        after_remove = access_manager.has_access(user_id)
                        print(f"   has_access({user_id}) после удаления: {after_remove}")
                        
                        if not initial and after_add and not after_remove:
                            print(f"🎉 ИНТЕГРИРОВАННАЯ СИСТЕМА РАБОТАЕТ!")
                            return True
                        else:
                            print(f"❌ Проблемы с интегрированной системой")
                            return False
                    else:
                        print(f"❌ Функция delete_user_completely не найдена")
                        return False
                else:
                    print(f"❌ Функция add_user_access не найдена")
                    return False
            else:
                print(f"⚠️ Новая система НЕ интегрирована")
                return False
        else:
            print(f"❌ Функция has_access не найдена")
            return False
            
    except Exception as e:
        print(f"❌ Ошибка импорта access_manager: {e}")
        return False

def main():
    print("🚀 БЫСТРЫЙ ТЕСТ СИНХРОНИЗАЦИИ")
    print("=" * 80)
    
    # Тест 1: Прямой импорт новой системы
    direct_test = test_direct_import()
    
    # Тест 2: Интегрированные функции
    integrated_test = test_existing_functions()
    
    print(f"\n🏆 ИТОГОВЫЕ РЕЗУЛЬТАТЫ:")
    print("=" * 80)
    print(f"   Прямой тест новой системы: {'✅' if direct_test else '❌'}")
    print(f"   Интегрированные функции: {'✅' if integrated_test else '❌'}")
    
    if direct_test:
        print(f"\n🎉 НОВАЯ СИСТЕМА MULTIPROCESSING РАБОТАЕТ!")
        if integrated_test:
            print(f"🔥 ИНТЕГРАЦИЯ ТОЖЕ РАБОТАЕТ!")
            print(f"🚀 МОЖНО ПЕРЕЗАПУСКАТЬ БОТЫ!")
        else:
            print(f"⚠️ Интеграция требует доработки")
            print(f"💡 Но новая система готова к использованию")
    else:
        print(f"\n❌ ЕСТЬ ПРОБЛЕМЫ С НОВОЙ СИСТЕМОЙ")
    
    print(f"\n📋 ЧТО ДЕЛАТЬ ДАЛЬШЕ:")
    if direct_test and integrated_test:
        print(f"1️⃣ Перезапустите админ бот: python admin_bot/main.py")
        print(f"2️⃣ Перезапустите основной бот: python main.py")
        print(f"3️⃣ Протестируйте в реальных условиях")
    elif direct_test:
        print(f"1️⃣ Новая система работает, но интеграция неполная")
        print(f"2️⃣ Используйте прямые функции multiprocessing_access_sync")
        print(f"3️⃣ Или доработайте интеграцию")
    else:
        print(f"1️⃣ Проверьте установку multiprocessing")
        print(f"2️⃣ Используйте старую систему из backup_old_sync/")

if __name__ == "__main__":
    main() 
"""
БЫСТРЫЙ ТЕСТ НОВОЙ СИСТЕМЫ СИНХРОНИЗАЦИИ
Исправленная версия с правильными импортами
"""

import sys
import os
import time

# Добавляем пути для импорта
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)
sys.path.insert(0, os.path.join(current_dir, 'utils'))
sys.path.insert(0, os.path.join(current_dir, 'admin_bot'))

def test_direct_import():
    """Тестирует прямой импорт новой системы"""
    print("🧪 ТЕСТ ПРЯМОГО ИМПОРТА")
    print("=" * 60)
    
    user_id = 6626270112
    
    try:
        # Импортируем напрямую из файла
        from multiprocessing_access_sync import has_access_mp, add_user_mp, remove_user_mp
        
        print("✅ Импорт успешен")
        
        # Проверяем начальное состояние
        print(f"\n1️⃣ Начальное состояние:")
        initial = has_access_mp(user_id)
        print(f"   has_access({user_id}): {initial}")
        
        # Добавляем пользователя
        print(f"\n2️⃣ Добавляем пользователя:")
        user_data = {
            'telegram_id': user_id,
            'is_active': True,
            'subscription_end': '2025-09-01T00:00:00',
            'role': 'trial'
        }
        add_result = add_user_mp(user_id, user_data)
        print(f"   add_user({user_id}): {add_result}")
        
        # Проверяем сразу
        after_add = has_access_mp(user_id)
        print(f"   has_access({user_id}) после добавления: {after_add}")
        
        # Удаляем пользователя
        print(f"\n3️⃣ Удаляем пользователя:")
        remove_result = remove_user_mp(user_id)
        print(f"   remove_user({user_id}): {remove_result}")
        
        # Проверяем сразу
        after_remove = has_access_mp(user_id)
        print(f"   has_access({user_id}) после удаления: {after_remove}")
        
        # Результат
        print(f"\n🏁 РЕЗУЛЬТАТ:")
        if not initial and after_add and not after_remove:
            print(f"   🎉 ВСЁ РАБОТАЕТ ИДЕАЛЬНО!")
            return True
        else:
            print(f"   ❌ Есть проблемы")
            print(f"     Начальное: {initial} (ожидали False)")
            print(f"     После добавления: {after_add} (ожидали True)")
            print(f"     После удаления: {after_remove} (ожидали False)")
            return False
            
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_existing_functions():
    """Тестирует существующие функции из access_manager"""
    print(f"\n🔧 ТЕСТ СУЩЕСТВУЮЩИХ ФУНКЦИЙ")
    print("=" * 60)
    
    user_id = 6626270112
    
    try:
        # Пробуем импортировать из access_manager напрямую
        import access_manager
        
        # Проверяем что функция has_access существует
        if hasattr(access_manager, 'has_access'):
            print("✅ Функция has_access найдена")
            
            # Проверяем новые функции
            if hasattr(access_manager, 'USE_NEW_SYNC'):
                print("✅ Новая система интегрирована!")
                
                # Тестируем
                initial = access_manager.has_access(user_id)
                print(f"   has_access({user_id}): {initial}")
                
                if hasattr(access_manager, 'add_user_access'):
                    add_result = access_manager.add_user_access(user_id)
                    print(f"   add_user_access({user_id}): {add_result}")
                    
                    after_add = access_manager.has_access(user_id)
                    print(f"   has_access({user_id}) после добавления: {after_add}")
                    
                    if hasattr(access_manager, 'delete_user_completely'):
                        remove_result = access_manager.delete_user_completely(user_id)
                        print(f"   delete_user_completely({user_id}): {remove_result}")
                        
                        after_remove = access_manager.has_access(user_id)
                        print(f"   has_access({user_id}) после удаления: {after_remove}")
                        
                        if not initial and after_add and not after_remove:
                            print(f"🎉 ИНТЕГРИРОВАННАЯ СИСТЕМА РАБОТАЕТ!")
                            return True
                        else:
                            print(f"❌ Проблемы с интегрированной системой")
                            return False
                    else:
                        print(f"❌ Функция delete_user_completely не найдена")
                        return False
                else:
                    print(f"❌ Функция add_user_access не найдена")
                    return False
            else:
                print(f"⚠️ Новая система НЕ интегрирована")
                return False
        else:
            print(f"❌ Функция has_access не найдена")
            return False
            
    except Exception as e:
        print(f"❌ Ошибка импорта access_manager: {e}")
        return False

def main():
    print("🚀 БЫСТРЫЙ ТЕСТ СИНХРОНИЗАЦИИ")
    print("=" * 80)
    
    # Тест 1: Прямой импорт новой системы
    direct_test = test_direct_import()
    
    # Тест 2: Интегрированные функции
    integrated_test = test_existing_functions()
    
    print(f"\n🏆 ИТОГОВЫЕ РЕЗУЛЬТАТЫ:")
    print("=" * 80)
    print(f"   Прямой тест новой системы: {'✅' if direct_test else '❌'}")
    print(f"   Интегрированные функции: {'✅' if integrated_test else '❌'}")
    
    if direct_test:
        print(f"\n🎉 НОВАЯ СИСТЕМА MULTIPROCESSING РАБОТАЕТ!")
        if integrated_test:
            print(f"🔥 ИНТЕГРАЦИЯ ТОЖЕ РАБОТАЕТ!")
            print(f"🚀 МОЖНО ПЕРЕЗАПУСКАТЬ БОТЫ!")
        else:
            print(f"⚠️ Интеграция требует доработки")
            print(f"💡 Но новая система готова к использованию")
    else:
        print(f"\n❌ ЕСТЬ ПРОБЛЕМЫ С НОВОЙ СИСТЕМОЙ")
    
    print(f"\n📋 ЧТО ДЕЛАТЬ ДАЛЬШЕ:")
    if direct_test and integrated_test:
        print(f"1️⃣ Перезапустите админ бот: python admin_bot/main.py")
        print(f"2️⃣ Перезапустите основной бот: python main.py")
        print(f"3️⃣ Протестируйте в реальных условиях")
    elif direct_test:
        print(f"1️⃣ Новая система работает, но интеграция неполная")
        print(f"2️⃣ Используйте прямые функции multiprocessing_access_sync")
        print(f"3️⃣ Или доработайте интеграцию")
    else:
        print(f"1️⃣ Проверьте установку multiprocessing")
        print(f"2️⃣ Используйте старую систему из backup_old_sync/")

if __name__ == "__main__":
    main() 
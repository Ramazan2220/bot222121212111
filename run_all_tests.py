#!/usr/bin/env python3
"""
Главный скрипт для запуска всех тестов продвинутых систем
Запускает все тестовые файлы для проверки функциональности
"""

import sys
import time
import subprocess
import logging
from pathlib import Path

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def run_test_file(test_file):
    """Запускает отдельный тестовый файл"""
    print(f"\n{'='*60}")
    print(f"🚀 ЗАПУСК ТЕСТА: {test_file}")
    print(f"{'='*60}")
    
    try:
        start_time = time.time()
        
        # Запускаем тестовый файл
        result = subprocess.run(
            [sys.executable, test_file],
            capture_output=True,
            text=True,
            cwd=Path(__file__).parent
        )
        
        duration = time.time() - start_time
        
        # Выводим результат
        if result.returncode == 0:
            print("✅ ТЕСТ ПРОЙДЕН УСПЕШНО")
            if result.stdout:
                print(f"\nВывод:\n{result.stdout}")
        else:
            print("❌ ТЕСТ ЗАВЕРШИЛСЯ С ОШИБКОЙ")
            if result.stdout:
                print(f"\nВывод:\n{result.stdout}")
            if result.stderr:
                print(f"\nОшибки:\n{result.stderr}")
        
        print(f"\n⏱️ Время выполнения: {duration:.1f} секунд")
        
        return result.returncode == 0, duration
        
    except Exception as e:
        print(f"❌ Критическая ошибка при запуске теста {test_file}: {e}")
        return False, 0

def main():
    """Главная функция запуска всех тестов"""
    print("🎯 ЗАПУСК ВСЕХ ТЕСТОВ ПРОДВИНУТЫХ СИСТЕМ")
    print(f"⏰ Время начала: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Список всех тестовых файлов
    test_files = [
        "test_health_monitor.py",
        "test_activity_limiter.py", 
        "test_improved_warmer.py",
        "test_lifecycle_manager.py",
        "test_predictive_monitor.py",
        "test_advanced_verification.py"
    ]
    
    # Проверяем существование файлов
    existing_files = []
    missing_files = []
    
    for test_file in test_files:
        if Path(test_file).exists():
            existing_files.append(test_file)
        else:
            missing_files.append(test_file)
    
    if missing_files:
        print("⚠️ ВНИМАНИЕ: Некоторые тестовые файлы не найдены:")
        for missing_file in missing_files:
            print(f"  ❌ {missing_file}")
        print()
    
    if not existing_files:
        print("❌ КРИТИЧЕСКАЯ ОШИБКА: Тестовые файлы не найдены!")
        return
    
    print(f"📋 Найдено тестовых файлов: {len(existing_files)}")
    print("Список файлов для тестирования:")
    for i, test_file in enumerate(existing_files, 1):
        print(f"  {i}. {test_file}")
    print()
    
    # Результаты тестирования
    results = []
    total_duration = 0
    
    try:
        # Запрашиваем подтверждение
        try:
            print("⚠️ ВНИМАНИЕ: Запуск всех тестов может занять несколько минут")
            print("Некоторые тесты могут запрашивать подтверждение для выполнения действий")
            confirmation = input("\nПродолжить запуск всех тестов? (y/N): ").lower().strip()
            
            if confirmation not in ['y', 'yes', 'да', 'д']:
                print("❌ Тестирование отменено пользователем")
                return
                
        except KeyboardInterrupt:
            print("\n❌ Тестирование прервано")
            return
        
        print()
        
        # Запускаем каждый тест
        for i, test_file in enumerate(existing_files, 1):
            print(f"\n📊 ПРОГРЕСС: {i}/{len(existing_files)}")
            
            success, duration = run_test_file(test_file)
            total_duration += duration
            
            results.append({
                'file': test_file,
                'success': success,
                'duration': duration
            })
            
            # Небольшая пауза между тестами
            if i < len(existing_files):
                print("\n⏳ Пауза 3 секунды перед следующим тестом...")
                time.sleep(3)
        
        # Сводный отчет
        print(f"\n{'='*60}")
        print("📊 СВОДНЫЙ ОТЧЕТ ТЕСТИРОВАНИЯ")
        print(f"{'='*60}")
        
        successful_tests = [r for r in results if r['success']]
        failed_tests = [r for r in results if not r['success']]
        
        success_rate = len(successful_tests) / len(results) * 100 if results else 0
        
        print(f"✅ Успешные тесты: {len(successful_tests)}/{len(results)} ({success_rate:.1f}%)")
        print(f"❌ Неудачные тесты: {len(failed_tests)}/{len(results)}")
        print(f"⏱️ Общее время: {total_duration:.1f} секунд")
        print()
        
        if successful_tests:
            print("✅ УСПЕШНЫЕ ТЕСТЫ:")
            for result in successful_tests:
                print(f"  • {result['file']} ({result['duration']:.1f}с)")
            print()
        
        if failed_tests:
            print("❌ НЕУДАЧНЫЕ ТЕСТЫ:")
            for result in failed_tests:
                print(f"  • {result['file']} ({result['duration']:.1f}с)")
            print()
        
        # Итоговый статус
        if len(successful_tests) == len(results):
            print("🎉 ВСЕ ТЕСТЫ ПРОЙДЕНЫ УСПЕШНО!")
            print("✅ Все продвинутые системы функционируют корректно")
        elif len(successful_tests) > len(failed_tests):
            print("⚠️ БОЛЬШИНСТВО ТЕСТОВ ПРОЙДЕНО")
            print("🔧 Рекомендуется проверить неудачные тесты")
        else:
            print("❌ МНОЖЕСТВЕННЫЕ ОШИБКИ В ТЕСТАХ")
            print("🚨 Требуется диагностика системы")
        
        print(f"\n⏰ Время завершения: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        
    except KeyboardInterrupt:
        print("\n\n⚠️ ТЕСТИРОВАНИЕ ПРЕРВАНО ПОЛЬЗОВАТЕЛЕМ")
        
        if results:
            print("\n📊 Частичные результаты:")
            for result in results:
                status = "✅" if result['success'] else "❌"
                print(f"  {status} {result['file']} ({result['duration']:.1f}с)")
    
    except Exception as e:
        print(f"\n❌ КРИТИЧЕСКАЯ ОШИБКА: {e}")
        logger.error(f"Критическая ошибка в run_all_tests: {e}")

def run_specific_test():
    """Запуск конкретного теста по выбору пользователя"""
    test_files = [
        "test_health_monitor.py",
        "test_activity_limiter.py",
        "test_improved_warmer.py", 
        "test_lifecycle_manager.py",
        "test_predictive_monitor.py",
        "test_advanced_verification.py"
    ]
    
    print("🎯 ВЫБОР КОНКРЕТНОГО ТЕСТА")
    print("=" * 30)
    
    existing_files = [f for f in test_files if Path(f).exists()]
    
    if not existing_files:
        print("❌ Тестовые файлы не найдены!")
        return
    
    print("Доступные тесты:")
    for i, test_file in enumerate(existing_files, 1):
        print(f"  {i}. {test_file}")
    
    try:
        choice = input(f"\nВыберите тест (1-{len(existing_files)}) или 'q' для выхода: ").strip()
        
        if choice.lower() == 'q':
            return
        
        try:
            index = int(choice) - 1
            if 0 <= index < len(existing_files):
                selected_file = existing_files[index]
                print(f"\n🚀 Запуск теста: {selected_file}")
                success, duration = run_test_file(selected_file)
                
                status = "✅ УСПЕШНО" if success else "❌ ОШИБКА"
                print(f"\n📊 РЕЗУЛЬТАТ: {status} (время: {duration:.1f}с)")
            else:
                print("❌ Неверный номер теста")
        except ValueError:
            print("❌ Введите номер теста")
            
    except KeyboardInterrupt:
        print("\n❌ Отменено")

if __name__ == "__main__":
    try:
        print("🤖 СИСТЕМА ТЕСТИРОВАНИЯ ПРОДВИНУТЫХ ФУНКЦИЙ")
        print("=" * 50)
        print("1. Запустить все тесты")
        print("2. Запустить конкретный тест")
        print("q. Выход")
        
        choice = input("\nВыберите опцию (1/2/q): ").strip()
        
        if choice == '1':
            main()
        elif choice == '2':
            run_specific_test()
        elif choice.lower() == 'q':
            print("👋 До свидания!")
        else:
            print("❌ Неверный выбор")
            
    except KeyboardInterrupt:
        print("\n👋 До свидания!")
    except Exception as e:
        print(f"\n❌ Ошибка: {e}")
        logger.error(f"Ошибка в главном меню: {e}") 
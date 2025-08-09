#!/usr/bin/env python3
"""
Автоматический запуск всех тестов продвинутых систем
Версия БЕЗ интерактивных запросов для автоматического тестирования
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
        
        # Запускаем тестовый файл с таймаутом 120 секунд
        result = subprocess.run(
            [sys.executable, test_file],
            capture_output=True,
            text=True,
            cwd=Path(__file__).parent,
            timeout=120  # Максимум 2 минуты на тест
        )
        
        duration = time.time() - start_time
        
        # Выводим результат
        if result.returncode == 0:
            print("✅ ТЕСТ ПРОЙДЕН УСПЕШНО")
            if result.stdout:
                # Показываем только последние строки вывода
                output_lines = result.stdout.strip().split('\n')
                if len(output_lines) > 10:
                    print("📋 Последние строки вывода:")
                    for line in output_lines[-10:]:
                        print(line)
                else:
                    print(f"\nВывод:\n{result.stdout}")
        else:
            print("❌ ТЕСТ ЗАВЕРШИЛСЯ С ОШИБКОЙ")
            if result.stdout:
                print(f"\nВывод:\n{result.stdout}")
            if result.stderr:
                print(f"\nОшибки:\n{result.stderr}")
        
        print(f"\n⏱️ Время выполнения: {duration:.1f} секунд")
        
        return result.returncode == 0, duration
        
    except subprocess.TimeoutExpired:
        print(f"⏰ ТЕСТ ПРЕВЫСИЛ ТАЙМАУТ (120 секунд)")
        return False, 120
    except Exception as e:
        print(f"❌ Критическая ошибка при запуске теста {test_file}: {e}")
        return False, 0

def main():
    """Главная функция запуска всех тестов"""
    print("🎯 АВТОМАТИЧЕСКИЙ ЗАПУСК ВСЕХ ТЕСТОВ ПРОДВИНУТЫХ СИСТЕМ")
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
    
    print("🤖 АВТОМАТИЧЕСКИЙ РЕЖИМ: Запуск без подтверждений")
    print()
    
    # Результаты тестирования
    results = []
    total_duration = 0
    
    try:
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
                print("\n⏳ Пауза 2 секунды перед следующим тестом...")
                time.sleep(2)
        
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
        logger.error(f"Критическая ошибка в run_all_tests_auto: {e}")

if __name__ == "__main__":
    main() 
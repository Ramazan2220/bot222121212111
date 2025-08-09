#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Запуск всех тестов MVP
"""

import sys
import unittest
import logging

# Настраиваем логирование для тестов
logging.basicConfig(
    level=logging.WARNING,  # Только предупреждения и ошибки
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def run_tests():
    """Запустить все тесты"""
    
    print("🧪 ЗАПУСК ТЕСТОВ MVP\n")
    print("=" * 50)
    
    # Создаем test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # Добавляем тесты
    try:
        from tests.test_services import (
            TestRateLimiter,
            TestAntiDetection,
            TestInstagramService,
            TestAsyncTaskProcessor,
            TestAccountAutomation
        )
        
        # Добавляем все тест-классы
        suite.addTests(loader.loadTestsFromTestCase(TestRateLimiter))
        suite.addTests(loader.loadTestsFromTestCase(TestAntiDetection))
        suite.addTests(loader.loadTestsFromTestCase(TestInstagramService))
        suite.addTests(loader.loadTestsFromTestCase(TestAsyncTaskProcessor))
        suite.addTests(loader.loadTestsFromTestCase(TestAccountAutomation))
        
    except ImportError as e:
        print(f"❌ Ошибка импорта тестов: {e}")
        print("\nУбедитесь, что:")
        print("1. Файл tests/test_services.py существует")
        print("2. Все зависимости установлены")
        return False
    
    # Запускаем тесты
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Выводим итоги
    print("\n" + "=" * 50)
    print("📊 РЕЗУЛЬТАТЫ ТЕСТИРОВАНИЯ:")
    print(f"✅ Успешно: {result.testsRun - len(result.failures) - len(result.errors)}")
    print(f"❌ Провалено: {len(result.failures)}")
    print(f"💥 Ошибок: {len(result.errors)}")
    
    if result.wasSuccessful():
        print("\n🎉 ВСЕ ТЕСТЫ ПРОЙДЕНЫ!")
        return True
    else:
        print("\n❌ ЕСТЬ ПРОБЛЕМЫ!")
        if result.failures:
            print("\nПроваленные тесты:")
            for test, traceback in result.failures:
                print(f"- {test}")
        
        if result.errors:
            print("\nТесты с ошибками:")
            for test, traceback in result.errors:
                print(f"- {test}")
        
        return False

def run_specific_test(test_name):
    """Запустить конкретный тест"""
    
    print(f"🧪 Запуск теста: {test_name}\n")
    
    try:
        # Динамически импортируем нужный тест
        module = __import__('tests.test_services', fromlist=[test_name])
        test_class = getattr(module, test_name)
        
        # Создаем и запускаем suite
        loader = unittest.TestLoader()
        suite = loader.loadTestsFromTestCase(test_class)
        
        runner = unittest.TextTestRunner(verbosity=2)
        result = runner.run(suite)
        
        return result.wasSuccessful()
        
    except AttributeError:
        print(f"❌ Тест '{test_name}' не найден!")
        print("\nДоступные тесты:")
        print("- TestRateLimiter")
        print("- TestAntiDetection")
        print("- TestInstagramService")
        print("- TestAsyncTaskProcessor")
        print("- TestAccountAutomation")
        return False

if __name__ == "__main__":
    if len(sys.argv) > 1:
        # Запуск конкретного теста
        success = run_specific_test(sys.argv[1])
    else:
        # Запуск всех тестов
        success = run_tests()
    
    # Возвращаем код выхода
    sys.exit(0 if success else 1) 
#!/usr/bin/env python3
"""
Тест системы Lazy Loading для Instagram клиентов
Проверяет производительность, совместимость и экономию памяти
"""

import os
import sys
import time
import json
import psutil
import threading
from typing import List
from concurrent.futures import ThreadPoolExecutor

# Добавляем корневую директорию в путь
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from instagram.lazy_client_factory import (
    init_lazy_factory, get_lazy_client, get_lazy_factory_stats,
    cleanup_lazy_clients, shutdown_lazy_factory, LazyInstagramClient
)
from database.db_manager import init_db, get_all_accounts


def get_memory_usage_mb():
    """Возвращает использование памяти процессом в MB"""
    process = psutil.Process()
    return process.memory_info().rss / 1024 / 1024


def print_separator(title: str):
    """Печатает красивый разделитель"""
    print("\n" + "=" * 70)
    print(f"🔍 {title}")
    print("=" * 70)


def test_basic_functionality():
    """Тест базовой функциональности"""
    print_separator("ТЕСТ 1: Базовая функциональность")
    
    print("📋 Проверяем создание lazy клиентов...")
    
    # Получаем тестовые аккаунты
    all_accounts = get_all_accounts()
    accounts = all_accounts[:3]  # Берем первые 3
    if not accounts:
        print("❌ Нет аккаунтов в базе данных для тестирования")
        return False
    
    try:
        # Создаем lazy клиенты
        lazy_clients = []
        for account in accounts:
            client = get_lazy_client(account.id)
            lazy_clients.append(client)
            print(f"✅ Lazy клиент создан для аккаунта {account.id} (@{account.username})")
        
        # Проверяем что они действительно lazy
        for client in lazy_clients:
            assert not client.is_active, f"Клиент {client.account_id} не должен быть активным"
            assert client.memory_footprint_mb < 0.01, f"Слишком большой размер lazy клиента"
        
        print(f"✅ Все {len(lazy_clients)} клиентов созданы как lazy")
        
        # Проверяем доступ к свойствам без активации
        for client in lazy_clients:
            account_data = client.account
            print(f"✅ Доступ к данным аккаунта {account_data.username} без активации клиента")
        
        return True
        
    except Exception as e:
        print(f"❌ Ошибка в тесте базовой функциональности: {e}")
        return False


def test_memory_comparison():
    """Тест сравнения использования памяти"""
    print_separator("ТЕСТ 2: Сравнение использования памяти")
    
    all_accounts = get_all_accounts()
    accounts = all_accounts[:10]  # Берем первые 10
    if len(accounts) < 5:
        print("❌ Недостаточно аккаунтов для теста памяти (нужно минимум 5)")
        return False
    
    print(f"📊 Тестируем с {len(accounts)} аккаунтами...")
    
    # Измеряем базовое использование памяти
    baseline_memory = get_memory_usage_mb()
    print(f"🔍 Базовое использование памяти: {baseline_memory:.1f} MB")
    
    try:
        # Создаем lazy клиенты
        print("\n📱 Создаем lazy клиенты...")
        lazy_clients = []
        for account in accounts:
            client = get_lazy_client(account.id)
            lazy_clients.append(client)
        
        lazy_memory = get_memory_usage_mb()
        lazy_overhead = lazy_memory - baseline_memory
        print(f"✅ {len(lazy_clients)} lazy клиентов созданы")
        print(f"💾 Память после создания lazy клиентов: {lazy_memory:.1f} MB (+{lazy_overhead:.1f} MB)")
        
        # Активируем часть клиентов
        print("\n⚡ Активируем 3 клиента...")
        activated_clients = []
        for i in range(min(3, len(lazy_clients))):
            client = lazy_clients[i]
            try:
                # Пытаемся получить user_id (это активирует клиент)
                _ = client.user_id
                activated_clients.append(client)
                print(f"✅ Клиент {client.account_id} активирован")
            except Exception as e:
                print(f"⚠️ Не удалось активировать клиент {client.account_id}: {e}")
        
        active_memory = get_memory_usage_mb()
        active_overhead = active_memory - lazy_memory
        print(f"💾 Память после активации {len(activated_clients)} клиентов: {active_memory:.1f} MB (+{active_overhead:.1f} MB)")
        
        # Подсчитываем экономию
        if len(activated_clients) > 0:
            memory_per_active_client = active_overhead / len(activated_clients)
            estimated_full_memory = baseline_memory + (len(accounts) * memory_per_active_client)
            current_memory = active_memory
            saved_memory = estimated_full_memory - current_memory
            save_percentage = (saved_memory / estimated_full_memory) * 100
            
            print(f"\n📊 РЕЗУЛЬТАТЫ:")
            print(f"• Память на 1 активный клиент: ~{memory_per_active_client:.1f} MB")
            print(f"• Если бы все {len(accounts)} были активны: ~{estimated_full_memory:.1f} MB")
            print(f"• Текущее использование: {current_memory:.1f} MB")
            print(f"• Экономия памяти: {saved_memory:.1f} MB ({save_percentage:.1f}%)")
        
        return True
        
    except Exception as e:
        print(f"❌ Ошибка в тесте памяти: {e}")
        return False


def test_concurrent_access():
    """Тест конкурентного доступа"""
    print_separator("ТЕСТ 3: Конкурентный доступ")
    
    all_accounts = get_all_accounts()
    accounts = all_accounts[:5]  # Берем первые 5
    if len(accounts) < 3:
        print("❌ Недостаточно аккаунтов для теста конкурентности")
        return False
    
    print(f"⚡ Тестируем конкурентный доступ с {len(accounts)} аккаунтами...")
    
    results = []
    errors = []
    
    def worker(account_id: int, worker_id: int):
        """Рабочая функция для тестирования"""
        try:
            print(f"👷 Worker {worker_id}: начинаю работу с аккаунтом {account_id}")
            
            # Получаем клиент
            client = get_lazy_client(account_id)
            
            # Проверяем базовые свойства
            account_data = client.account
            
            # Пытаемся активировать клиент
            start_time = time.time()
            try:
                # Это должно активировать клиент
                _ = client.user_id
                activation_time = time.time() - start_time
                
                results.append({
                    'worker_id': worker_id,
                    'account_id': account_id,
                    'activation_time': activation_time,
                    'success': True
                })
                print(f"✅ Worker {worker_id}: клиент активирован за {activation_time:.2f}с")
                
            except Exception as e:
                results.append({
                    'worker_id': worker_id,
                    'account_id': account_id,
                    'activation_time': 0,
                    'success': False,
                    'error': str(e)
                })
                print(f"⚠️ Worker {worker_id}: ошибка активации - {e}")
            
        except Exception as e:
            errors.append(f"Worker {worker_id}: {e}")
            print(f"❌ Worker {worker_id}: критическая ошибка - {e}")
    
    try:
        # Запускаем несколько потоков
        with ThreadPoolExecutor(max_workers=len(accounts)) as executor:
            futures = []
            for i, account in enumerate(accounts):
                future = executor.submit(worker, account.id, i+1)
                futures.append(future)
            
            # Ждем завершения всех потоков
            for future in futures:
                future.result()
        
        # Анализируем результаты
        successful = [r for r in results if r['success']]
        failed = [r for r in results if not r['success']]
        
        print(f"\n📊 РЕЗУЛЬТАТЫ КОНКУРЕНТНОГО ДОСТУПА:")
        print(f"✅ Успешных активаций: {len(successful)}")
        print(f"❌ Неудачных активаций: {len(failed)}")
        print(f"🚨 Критических ошибок: {len(errors)}")
        
        if successful:
            avg_time = sum(r['activation_time'] for r in successful) / len(successful)
            max_time = max(r['activation_time'] for r in successful)
            min_time = min(r['activation_time'] for r in successful)
            print(f"⏱️ Время активации: мин={min_time:.2f}с, макс={max_time:.2f}с, среднее={avg_time:.2f}с")
        
        return len(errors) == 0 and len(successful) > 0
        
    except Exception as e:
        print(f"❌ Ошибка в тесте конкурентности: {e}")
        return False


def test_statistics_and_cleanup():
    """Тест статистики и очистки"""
    print_separator("ТЕСТ 4: Статистика и очистка")
    
    try:
        # Получаем начальную статистику
        initial_stats = get_lazy_factory_stats()
        print(f"📊 Начальная статистика:")
        print(f"• Всего создано: {initial_stats.total_created}")
        print(f"• Активных сейчас: {initial_stats.currently_active}")
        print(f"• Сэкономлено памяти: {initial_stats.memory_saved_mb:.1f} MB")
        
        # Создаем еще несколько клиентов
        all_accounts = get_all_accounts()
        accounts = all_accounts[:3]  # Берем первые 3
        test_clients = []
        
        print(f"\n🔧 Создаем {len(accounts)} дополнительных клиентов...")
        for account in accounts:
            client = get_lazy_client(account.id)
            test_clients.append(client)
            # Активируем каждый второй
            if len(test_clients) % 2 == 0:
                try:
                    _ = client.user_id
                    print(f"✅ Клиент {account.id} активирован")
                except:
                    print(f"⚠️ Не удалось активировать клиент {account.id}")
        
        # Проверяем обновленную статистику
        updated_stats = get_lazy_factory_stats()
        print(f"\n📊 Обновленная статистика:")
        print(f"• Всего создано: {updated_stats.total_created}")
        print(f"• Активных сейчас: {updated_stats.currently_active}")
        print(f"• Среднее время создания: {updated_stats.avg_creation_time:.3f}с")
        print(f"• Среднее время операции: {updated_stats.avg_operation_time:.3f}с")
        print(f"• Сэкономлено памяти: {updated_stats.memory_saved_mb:.1f} MB")
        
        # Тестируем очистку
        print(f"\n🧹 Тестируем очистку неактивных клиентов...")
        
        # Имитируем старые клиенты установив время доступа в прошлое
        for client in test_clients[:2]:
            if client.is_active:
                client._last_access = time.time() - 7200  # 2 часа назад
        
        cleanup_lazy_clients()
        
        final_stats = get_lazy_factory_stats()
        print(f"📊 Финальная статистика после очистки:")
        print(f"• Всего уничтожено: {final_stats.total_destroyed}")
        print(f"• Активных сейчас: {final_stats.currently_active}")
        
        return True
        
    except Exception as e:
        print(f"❌ Ошибка в тесте статистики: {e}")
        return False


def test_performance_benchmark():
    """Бенчмарк производительности"""
    print_separator("ТЕСТ 5: Бенчмарк производительности")
    
    all_accounts = get_all_accounts()
    accounts = all_accounts[:10]  # Берем первые 10
    if len(accounts) < 5:
        print("❌ Недостаточно аккаунтов для бенчмарка")
        return False
    
    print(f"🚀 Проводим бенчмарк с {len(accounts)} аккаунтами...")
    
    try:
        # Тест 1: Время создания lazy клиентов
        start_time = time.time()
        lazy_clients = []
        for account in accounts:
            client = get_lazy_client(account.id)
            lazy_clients.append(client)
        lazy_creation_time = time.time() - start_time
        
        print(f"⚡ Создание {len(accounts)} lazy клиентов: {lazy_creation_time:.3f}с")
        print(f"⚡ Среднее время на lazy клиент: {lazy_creation_time/len(accounts)*1000:.1f}мс")
        
        # Тест 2: Время активации клиентов
        activation_times = []
        successful_activations = 0
        
        for i, client in enumerate(lazy_clients[:5]):  # Активируем только 5
            try:
                start_time = time.time()
                _ = client.account.username  # Доступ без активации
                no_activation_time = time.time() - start_time
                
                start_time = time.time()
                _ = client.user_id  # Это активирует клиент
                activation_time = time.time() - start_time
                
                activation_times.append(activation_time)
                successful_activations += 1
                
                print(f"✅ Клиент {i+1}: доступ к данным {no_activation_time:.3f}с, активация {activation_time:.3f}с")
                
            except Exception as e:
                print(f"⚠️ Клиент {i+1}: ошибка активации - {e}")
        
        if activation_times:
            avg_activation = sum(activation_times) / len(activation_times)
            max_activation = max(activation_times)
            min_activation = min(activation_times)
            
            print(f"\n📊 РЕЗУЛЬТАТЫ БЕНЧМАРКА:")
            print(f"• Успешных активаций: {successful_activations}/{len(lazy_clients[:5])}")
            print(f"• Среднее время активации: {avg_activation:.3f}с")
            print(f"• Мин/макс время активации: {min_activation:.3f}с / {max_activation:.3f}с")
            print(f"• Производительность: {1200/avg_activation:.0f} операций/час на клиент")
        
        return True
        
    except Exception as e:
        print(f"❌ Ошибка в бенчмарке: {e}")
        return False


def main():
    """Главная функция тестирования"""
    print("🚀 ЗАПУСК КОМПЛЕКСНОГО ТЕСТА LAZY LOADING СИСТЕМЫ")
    print("=" * 70)
    
    try:
        # Инициализация
        print("🔧 Инициализация системы...")
        init_db()
        init_lazy_factory(max_active_clients=50, cleanup_interval=300)  # 5 минут для теста
        print("✅ Система инициализирована")
        
        # Запускаем тесты
        tests = [
            ("Базовая функциональность", test_basic_functionality),
            ("Сравнение памяти", test_memory_comparison),
            ("Конкурентный доступ", test_concurrent_access),
            ("Статистика и очистка", test_statistics_and_cleanup),
            ("Бенчмарк производительности", test_performance_benchmark),
        ]
        
        results = []
        
        for test_name, test_func in tests:
            print(f"\n🧪 Запуск теста: {test_name}")
            try:
                result = test_func()
                results.append((test_name, result))
                status = "✅ ПРОЙДЕН" if result else "❌ ПРОВАЛЕН"
                print(f"🏁 {test_name}: {status}")
            except Exception as e:
                results.append((test_name, False))
                print(f"🏁 {test_name}: ❌ ОШИБКА - {e}")
        
        # Итоговый отчет
        print_separator("ИТОГОВЫЙ ОТЧЕТ")
        
        passed = sum(1 for _, result in results if result)
        total = len(results)
        
        for test_name, result in results:
            status = "✅" if result else "❌"
            print(f"{status} {test_name}")
        
        print(f"\n🏆 РЕЗУЛЬТАТ: {passed}/{total} тестов пройдено")
        
        if passed == total:
            print("🎉 ВСЕ ТЕСТЫ ПРОЙДЕНЫ! Lazy Loading система работает корректно!")
        else:
            print("⚠️ Есть проблемы, требующие внимания")
        
        # Финальная статистика
        final_stats = get_lazy_factory_stats()
        current_memory = get_memory_usage_mb()
        
        print(f"\n📊 ФИНАЛЬНАЯ СТАТИСТИКА:")
        print(f"• Всего создано клиентов: {final_stats.total_created}")
        print(f"• Всего уничтожено: {final_stats.total_destroyed}")
        print(f"• Активных сейчас: {final_stats.currently_active}")
        print(f"• Сэкономлено памяти: {final_stats.memory_saved_mb:.1f} MB")
        print(f"• Текущее использование памяти: {current_memory:.1f} MB")
        print(f"• Среднее время создания: {final_stats.avg_creation_time:.3f}с")
        print(f"• Среднее время операции: {final_stats.avg_operation_time:.3f}с")
        
        return passed == total
        
    except Exception as e:
        print(f"❌ КРИТИЧЕСКАЯ ОШИБКА: {e}")
        return False
    
    finally:
        # Корректно завершаем работу
        print("\n🧹 Завершение работы...")
        try:
            shutdown_lazy_factory()
            print("✅ Lazy factory корректно завершена")
        except Exception as e:
            print(f"⚠️ Ошибка при завершении: {e}")


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 
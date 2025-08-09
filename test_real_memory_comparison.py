#!/usr/bin/env python3
"""
Честный тест сравнения памяти:
Обычные instagrapi.Client VS Lazy Loading клиенты
"""

import os
import sys
import time
import gc
import psutil
import threading
from typing import List

# Добавляем корневую директорию в путь
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from instagrapi import Client as InstagrapiClient
from instagram.lazy_client_factory import init_lazy_factory, get_lazy_client, shutdown_lazy_factory
from database.db_manager import init_db, get_all_accounts
from device_manager import get_or_create_device_settings


def get_memory_usage_mb():
    """Возвращает использование памяти процессом в MB"""
    process = psutil.Process()
    return process.memory_info().rss / 1024 / 1024


def force_garbage_collection():
    """Принудительная сборка мусора"""
    gc.collect()
    gc.collect()
    gc.collect()


def test_regular_clients(num_clients: int = 20):
    """Тест с обычными instagrapi.Client"""
    print(f"\n🔴 ТЕСТ ОБЫЧНЫХ КЛИЕНТОВ ({num_clients} штук)")
    print("=" * 60)
    
    # Получаем аккаунты
    all_accounts = get_all_accounts()
    accounts = all_accounts[:num_clients]
    
    if len(accounts) < num_clients:
        print(f"⚠️ Доступно только {len(accounts)} аккаунтов")
        accounts = all_accounts
        num_clients = len(accounts)
    
    # Измеряем базовую память
    force_garbage_collection()
    baseline_memory = get_memory_usage_mb()
    print(f"📊 Базовая память: {baseline_memory:.1f} MB")
    
    # Создаем обычные клиенты
    print(f"\n📱 Создаем {num_clients} обычных instagrapi.Client...")
    regular_clients = []
    
    start_time = time.time()
    
    for i, account in enumerate(accounts):
        try:
            # Создаем настоящий instagrapi.Client
            device_settings = get_or_create_device_settings(account.id)
            client = InstagrapiClient(settings=device_settings)
            
            # Применяем патчи
            try:
                import instagram.client_patch
                import instagram.deep_patch
            except ImportError:
                pass
            
            regular_clients.append(client)
            
            if (i + 1) % 5 == 0:
                current_memory = get_memory_usage_mb()
                memory_per_client = (current_memory - baseline_memory) / (i + 1)
                print(f"✅ Создано {i+1} клиентов, память: {current_memory:.1f} MB (+{memory_per_client:.1f} MB/клиент)")
        
        except Exception as e:
            print(f"❌ Ошибка создания клиента {i+1}: {e}")
    
    creation_time = time.time() - start_time
    force_garbage_collection()
    final_memory = get_memory_usage_mb()
    
    memory_overhead = final_memory - baseline_memory
    memory_per_client = memory_overhead / len(regular_clients) if regular_clients else 0
    
    print(f"\n📊 РЕЗУЛЬТАТЫ ОБЫЧНЫХ КЛИЕНТОВ:")
    print(f"• Создано клиентов: {len(regular_clients)}")
    print(f"• Время создания: {creation_time:.2f}с")
    print(f"• Среднее время на клиент: {creation_time/len(regular_clients)*1000:.1f}мс")
    print(f"• Финальная память: {final_memory:.1f} MB")
    print(f"• Overhead памяти: {memory_overhead:.1f} MB")
    print(f"• Память на клиент: {memory_per_client:.1f} MB")
    
    # Очищаем
    del regular_clients
    force_garbage_collection()
    
    return {
        'clients_created': len(accounts),
        'total_memory_mb': memory_overhead,
        'memory_per_client_mb': memory_per_client,
        'creation_time_sec': creation_time
    }


def test_lazy_clients(num_clients: int = 20, activate_clients: int = 5):
    """Тест с Lazy Loading клиентами"""
    print(f"\n🟢 ТЕСТ LAZY КЛИЕНТОВ ({num_clients} всего, активируем {activate_clients})")
    print("=" * 60)
    
    # Получаем аккаунты
    all_accounts = get_all_accounts()
    accounts = all_accounts[:num_clients]
    
    if len(accounts) < num_clients:
        print(f"⚠️ Доступно только {len(accounts)} аккаунтов")
        accounts = all_accounts
        num_clients = len(accounts)
    
    # Измеряем базовую память
    force_garbage_collection()
    baseline_memory = get_memory_usage_mb()
    print(f"📊 Базовая память: {baseline_memory:.1f} MB")
    
    # Создаем lazy клиенты
    print(f"\n📱 Создаем {num_clients} lazy клиентов...")
    lazy_clients = []
    
    start_time = time.time()
    
    for i, account in enumerate(accounts):
        try:
            client = get_lazy_client(account.id)
            lazy_clients.append(client)
            
            if (i + 1) % 5 == 0:
                current_memory = get_memory_usage_mb()
                memory_overhead = current_memory - baseline_memory
                print(f"✅ Создано {i+1} lazy клиентов, память: {current_memory:.1f} MB (+{memory_overhead:.3f} MB)")
        
        except Exception as e:
            print(f"❌ Ошибка создания lazy клиента {i+1}: {e}")
    
    lazy_creation_time = time.time() - start_time
    force_garbage_collection()
    lazy_only_memory = get_memory_usage_mb()
    lazy_overhead = lazy_only_memory - baseline_memory
    
    print(f"\n📊 LAZY КЛИЕНТЫ СОЗДАНЫ:")
    print(f"• Создано: {len(lazy_clients)}")
    print(f"• Время создания: {lazy_creation_time:.3f}с")
    print(f"• Память: {lazy_only_memory:.1f} MB (+{lazy_overhead:.3f} MB)")
    
    # Активируем часть клиентов
    print(f"\n⚡ АКТИВИРУЕМ {activate_clients} клиентов...")
    activated_count = 0
    activation_start = time.time()
    
    for i, client in enumerate(lazy_clients[:activate_clients]):
        try:
            # Принудительно активируем клиент через реальную операцию
            print(f"🔄 Активируем клиент {i+1}...")
            
            # Этот вызов должен создать настоящий instagrapi.Client
            real_client = client._ensure_real_client()
            
            # Проверяем что клиент действительно создан
            if client.is_active:
                activated_count += 1
                current_memory = get_memory_usage_mb()
                print(f"✅ Клиент {i+1} активирован, память: {current_memory:.1f} MB")
            else:
                print(f"⚠️ Клиент {i+1} не активировался")
        
        except Exception as e:
            print(f"❌ Ошибка активации клиента {i+1}: {e}")
    
    activation_time = time.time() - activation_start
    force_garbage_collection()
    final_memory = get_memory_usage_mb()
    
    total_overhead = final_memory - baseline_memory
    activation_overhead = final_memory - lazy_only_memory
    memory_per_active_client = activation_overhead / activated_count if activated_count > 0 else 0
    
    print(f"\n📊 РЕЗУЛЬТАТЫ LAZY КЛИЕНТОВ:")
    print(f"• Всего создано: {len(lazy_clients)}")
    print(f"• Активировано: {activated_count}")
    print(f"• Время активации: {activation_time:.2f}с")
    print(f"• Финальная память: {final_memory:.1f} MB")
    print(f"• Общий overhead: {total_overhead:.1f} MB")
    print(f"• Overhead от активации: {activation_overhead:.1f} MB")
    print(f"• Память на активный клиент: {memory_per_active_client:.1f} MB")
    
    return {
        'clients_created': len(lazy_clients),
        'clients_activated': activated_count,
        'total_memory_mb': total_overhead,
        'activation_memory_mb': activation_overhead,
        'memory_per_active_client_mb': memory_per_active_client,
        'lazy_creation_time_sec': lazy_creation_time,
        'activation_time_sec': activation_time
    }


def main():
    """Главная функция сравнительного теста"""
    print("🔥 ЧЕСТНОЕ СРАВНЕНИЕ ПАМЯТИ: ОБЫЧНЫЕ VS LAZY КЛИЕНТЫ")
    print("=" * 70)
    
    try:
        # Инициализация
        print("🔧 Инициализация...")
        init_db()
        init_lazy_factory(max_active_clients=100)
        
        # Параметры теста
        num_clients = 50  # Больше клиентов для четкой разницы
        activate_count = 10  # Активируем только часть
        
        print(f"\n🎯 ПАРАМЕТРЫ ТЕСТА:")
        print(f"• Общее количество клиентов: {num_clients}")
        print(f"• Активируем lazy клиентов: {activate_count}")
        
        # Тест 1: Обычные клиенты
        regular_results = test_regular_clients(num_clients)
        
        # Небольшая пауза для очистки памяти
        print("\n⏸️ Пауза для очистки памяти...")
        time.sleep(2)
        force_garbage_collection()
        time.sleep(1)
        
        # Тест 2: Lazy клиенты
        lazy_results = test_lazy_clients(num_clients, activate_count)
        
        # Сравнение результатов
        print("\n" + "=" * 70)
        print("📊 СРАВНИТЕЛЬНЫЙ АНАЛИЗ")
        print("=" * 70)
        
        print(f"\n🔴 ОБЫЧНЫЕ КЛИЕНТЫ ({regular_results['clients_created']} штук):")
        print(f"• Общая память: {regular_results['total_memory_mb']:.1f} MB")
        print(f"• Память на клиент: {regular_results['memory_per_client_mb']:.1f} MB")
        print(f"• Время создания: {regular_results['creation_time_sec']:.2f}с")
        
        print(f"\n🟢 LAZY КЛИЕНТЫ ({lazy_results['clients_created']} всего, {lazy_results['clients_activated']} активных):")
        print(f"• Общая память: {lazy_results['total_memory_mb']:.1f} MB")
        print(f"• Память от активации: {lazy_results['activation_memory_mb']:.1f} MB")
        print(f"• Память на активный клиент: {lazy_results['memory_per_active_client_mb']:.1f} MB")
        print(f"• Время создания lazy: {lazy_results['lazy_creation_time_sec']:.3f}с")
        print(f"• Время активации: {lazy_results['activation_time_sec']:.2f}с")
        
        # Подсчет экономии
        print(f"\n💰 ЭКОНОМИЯ:")
        
        # Если бы все lazy клиенты были активными
        estimated_full_lazy_memory = (lazy_results['memory_per_active_client_mb'] * 
                                    lazy_results['clients_created'])
        
        memory_saved = regular_results['total_memory_mb'] - lazy_results['total_memory_mb']
        memory_saved_percentage = (memory_saved / regular_results['total_memory_mb']) * 100
        
        potential_memory_saved = regular_results['total_memory_mb'] - estimated_full_lazy_memory
        potential_percentage = (potential_memory_saved / regular_results['total_memory_mb']) * 100
        
        print(f"• Фактическая экономия: {memory_saved:.1f} MB ({memory_saved_percentage:.1f}%)")
        print(f"• Потенциальная экономия: {potential_memory_saved:.1f} MB ({potential_percentage:.1f}%)")
        
        # Скорость создания
        speed_improvement = regular_results['creation_time_sec'] / lazy_results['lazy_creation_time_sec']
        print(f"• Ускорение создания: в {speed_improvement:.0f}x раз")
        
        print(f"\n🎯 ВЫВОДЫ:")
        if memory_saved > 0:
            print(f"✅ Lazy Loading экономит {memory_saved:.1f} MB памяти!")
            print(f"✅ При полной активности экономия составит {potential_memory_saved:.1f} MB")
        else:
            print(f"⚠️ Текущая экономия: {memory_saved:.1f} MB")
            print("💡 Экономия будет заметна при большем количестве клиентов")
        
        if speed_improvement > 1:
            print(f"✅ Создание lazy клиентов в {speed_improvement:.0f}x быстрее!")
        
        return True
        
    except Exception as e:
        print(f"❌ КРИТИЧЕСКАЯ ОШИБКА: {e}")
        return False
    
    finally:
        print("\n🧹 Завершение...")
        try:
            shutdown_lazy_factory()
        except:
            pass


if __name__ == "__main__":
    main() 
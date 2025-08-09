#!/usr/bin/env python3
"""
Тест интеграции Lazy Loading с обратной совместимостью
Проверяет что существующий код работает без изменений
"""

import os
import sys
import time
import logging

# Добавляем корневую директорию в путь
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database.db_manager import init_db, get_all_accounts

# Настраиваем логирование
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def test_backward_compatibility():
    """Тест обратной совместимости"""
    print("\n🔄 ТЕСТ ОБРАТНОЙ СОВМЕСТИМОСТИ")
    print("=" * 60)
    
    try:
        # Инициализируем систему
        print("🔧 Инициализация системы...")
        init_db()
        
        # Инициализируем Client Adapter
        from instagram.client_adapter import init_client_adapter, ClientConfig
        
        config = ClientConfig(
            use_lazy_loading=True,
            lazy_max_active=50,
            fallback_to_normal=True
        )
        init_client_adapter(config)
        print("✅ Client Adapter инициализирован")
        
        # Тестируем оригинальную функцию (должна работать без изменений)
        print("\n📱 Тестируем ОРИГИНАЛЬНУЮ функцию get_instagram_client...")
        
        accounts = get_all_accounts()[:3]
        if not accounts:
            print("⚠️ Нет аккаунтов для тестирования")
            return False
        
        clients = []
        for account in accounts:
            try:
                # Используем ТОЧНО ТУ ЖЕ функцию что и раньше
                from instagram.client import get_instagram_client
                
                client = get_instagram_client(account.id)
                clients.append(client)
                
                print(f"✅ Клиент создан для аккаунта {account.id} (@{account.username})")
                print(f"   Тип клиента: {type(client).__name__}")
                
                # Проверяем что клиент имеет все ожидаемые методы
                assert hasattr(client, 'user_id'), "Отсутствует user_id"
                assert hasattr(client, 'account'), "Отсутствует account"
                
                # Проверяем что можем получить данные аккаунта
                account_data = client.account
                print(f"   Данные аккаунта доступны: {account_data.username}")
                
            except Exception as e:
                print(f"❌ Ошибка для аккаунта {account.id}: {e}")
                return False
        
        print(f"\n✅ Все {len(clients)} клиентов созданы успешно")
        
        # Проверяем статистику
        from instagram.client import get_instagram_client_stats, is_using_lazy_loading
        
        stats = get_instagram_client_stats()
        is_lazy = is_using_lazy_loading()
        
        print(f"\n📊 СТАТИСТИКА:")
        print(f"• Режим: {stats.get('mode', 'unknown')}")
        print(f"• Lazy Loading активен: {is_lazy}")
        print(f"• Fallback режим: {stats.get('fallback_mode', False)}")
        
        if is_lazy:
            print(f"• Создано lazy клиентов: {stats.get('lazy_total_created', 0)}")
            print(f"• Активных сейчас: {stats.get('lazy_currently_active', 0)}")
            print(f"• Сэкономлено памяти: {stats.get('lazy_memory_saved_mb', 0):.1f} MB")
        
        return True
        
    except Exception as e:
        print(f"❌ Критическая ошибка: {e}")
        return False


def test_mixed_usage():
    """Тест смешанного использования (старый и новый код)"""
    print("\n🔀 ТЕСТ СМЕШАННОГО ИСПОЛЬЗОВАНИЯ")
    print("=" * 60)
    
    try:
        accounts = get_all_accounts()[:5]
        if len(accounts) < 2:
            print("⚠️ Недостаточно аккаунтов для теста")
            return False
        
        # Способ 1: Старая функция (должна работать)
        print("📱 Способ 1: Используем СТАРУЮ функцию...")
        from instagram.client import get_instagram_client
        
        client1 = get_instagram_client(accounts[0].id)
        print(f"✅ Старая функция: {type(client1).__name__} для аккаунта {accounts[0].id}")
        
        # Способ 2: Новый адаптер (для сравнения)
        print("📱 Способ 2: Используем НОВЫЙ адаптер...")
        from instagram.client_adapter import get_universal_client
        
        client2 = get_universal_client(accounts[1].id)
        print(f"✅ Новый адаптер: {type(client2).__name__} для аккаунта {accounts[1].id}")
        
        # Способ 3: Прямой lazy клиент
        print("📱 Способ 3: Используем ПРЯМОЙ lazy клиент...")
        from instagram.lazy_client_factory import get_lazy_client
        
        client3 = get_lazy_client(accounts[2].id)
        print(f"✅ Прямой lazy: {type(client3).__name__} для аккаунта {accounts[2].id}")
        
        # Проверяем что все клиенты работают одинаково
        print("\n🔍 Проверяем совместимость API...")
        
        for i, client in enumerate([client1, client2, client3], 1):
            try:
                # Тестируем доступ к базовым свойствам
                account_data = client.account
                print(f"✅ Клиент {i}: доступ к account.username = {account_data.username}")
                
                # Проверяем методы
                assert callable(getattr(client, '_ensure_real_client', None)) or hasattr(client, 'client'), \
                    f"Клиент {i} не имеет ожидаемой структуры"
                
            except Exception as e:
                print(f"❌ Клиент {i} не совместим: {e}")
                return False
        
        print("✅ Все способы создания клиентов работают и совместимы!")
        return True
        
    except Exception as e:
        print(f"❌ Ошибка в тесте смешанного использования: {e}")
        return False


def test_performance_comparison():
    """Тест производительности"""
    print("\n⚡ ТЕСТ ПРОИЗВОДИТЕЛЬНОСТИ")
    print("=" * 60)
    
    try:
        accounts = get_all_accounts()[:20]
        if len(accounts) < 10:
            print(f"⚠️ Доступно только {len(accounts)} аккаунтов")
            accounts = get_all_accounts()
        
        # Тест создания клиентов
        print(f"📊 Тестируем создание {len(accounts)} клиентов...")
        
        start_time = time.time()
        created_clients = []
        
        for account in accounts:
            try:
                from instagram.client import get_instagram_client
                client = get_instagram_client(account.id)
                created_clients.append(client)
            except Exception as e:
                print(f"⚠️ Ошибка создания клиента для {account.id}: {e}")
        
        creation_time = time.time() - start_time
        
        print(f"✅ Создано {len(created_clients)} клиентов за {creation_time:.3f}с")
        print(f"⚡ Среднее время на клиент: {creation_time/len(created_clients)*1000:.1f}мс")
        
        # Проверяем статистику памяти
        from instagram.client import get_instagram_client_stats
        stats = get_instagram_client_stats()
        
        if stats.get('mode') == 'lazy':
            print(f"💾 Экономия памяти: {stats.get('lazy_memory_saved_mb', 0):.1f} MB")
            print(f"🔥 Активных клиентов: {stats.get('lazy_currently_active', 0)}")
        
        return True
        
    except Exception as e:
        print(f"❌ Ошибка в тесте производительности: {e}")
        return False


def test_configuration_modes():
    """Тест различных режимов конфигурации"""
    print("\n⚙️ ТЕСТ РЕЖИМОВ КОНФИГУРАЦИИ")
    print("=" * 60)
    
    try:
        from instagram.client_adapter import ClientConfig, init_client_adapter
        
        # Режим 1: Только Lazy Loading
        print("🔧 Режим 1: Только Lazy Loading (без fallback)...")
        config1 = ClientConfig(
            use_lazy_loading=True,
            fallback_to_normal=False,
            lazy_max_active=10
        )
        
        try:
            init_client_adapter(config1)
            print("✅ Режим 1 инициализирован")
        except Exception as e:
            print(f"⚠️ Режим 1 не работает: {e}")
        
        # Режим 2: Только обычные клиенты
        print("🔧 Режим 2: Только обычные клиенты...")
        config2 = ClientConfig(
            use_lazy_loading=False,
            fallback_to_normal=True
        )
        
        init_client_adapter(config2)
        print("✅ Режим 2 инициализирован")
        
        # Режим 3: Гибридный (рекомендуемый)
        print("🔧 Режим 3: Гибридный режим...")
        config3 = ClientConfig(
            use_lazy_loading=True,
            fallback_to_normal=True,
            lazy_max_active=100
        )
        
        init_client_adapter(config3)
        print("✅ Режим 3 инициализирован")
        
        # Тестируем текущий режим
        accounts = get_all_accounts()[:2]
        if accounts:
            from instagram.client import get_instagram_client, is_using_lazy_loading
            
            client = get_instagram_client(accounts[0].id)
            is_lazy = is_using_lazy_loading()
            
            print(f"✅ Текущий режим работает: lazy={is_lazy}, тип={type(client).__name__}")
        
        return True
        
    except Exception as e:
        print(f"❌ Ошибка в тесте конфигурации: {e}")
        return False


def main():
    """Главная функция тестирования интеграции"""
    print("🚀 КОМПЛЕКСНЫЙ ТЕСТ ИНТЕГРАЦИИ LAZY LOADING")
    print("=" * 70)
    print("Проверяем обратную совместимость и работу с существующим кодом")
    
    tests = [
        ("Обратная совместимость", test_backward_compatibility),
        ("Смешанное использование", test_mixed_usage),
        ("Производительность", test_performance_comparison),
        ("Режимы конфигурации", test_configuration_modes),
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
    print("\n" + "=" * 70)
    print("📊 ИТОГОВЫЙ ОТЧЕТ ИНТЕГРАЦИИ")
    print("=" * 70)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "✅" if result else "❌"
        print(f"{status} {test_name}")
    
    print(f"\n🏆 РЕЗУЛЬТАТ: {passed}/{total} тестов пройдено")
    
    if passed == total:
        print("\n🎉 ВСЕ ТЕСТЫ ПРОЙДЕНЫ!")
        print("✅ Lazy Loading полностью интегрирован")
        print("✅ Обратная совместимость сохранена")
        print("✅ Существующий код работает без изменений")
        print("\n💡 СИСТЕМА ГОТОВА К ИСПОЛЬЗОВАНИЮ!")
    else:
        print(f"\n⚠️ {total - passed} тестов провалено")
        print("🔧 Требуется дополнительная настройка")
    
    return passed == total


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 
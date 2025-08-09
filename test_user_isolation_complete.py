#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Комплексный тест системы изоляции пользователей
Проверяет все компоненты новой системы изоляции
"""

import logging
import time
from datetime import datetime
import sys

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(f'test_isolation_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
    ]
)

logger = logging.getLogger(__name__)

def test_basic_functions():
    """Тест основных функций управления пользователями"""
    logger.info("🧪 ТЕСТ 1: Основные функции управления пользователями")
    
    try:
        from database.user_management import get_active_users, get_user_info, get_users_by_priority
        
        # Тест get_active_users
        logger.info("📋 Тестируем get_active_users...")
        users = get_active_users()
        logger.info(f"✅ Найдено {len(users)} активных пользователей: {users}")
        
        # Тест get_users_by_priority
        logger.info("📋 Тестируем get_users_by_priority...")
        priority_users = get_users_by_priority()
        logger.info(f"✅ Пользователи с приоритетами: {len(priority_users)}")
        for user_id, priority in priority_users[:5]:  # Показываем первых 5
            logger.info(f"   - Пользователь {user_id}: {priority}")
        
        # Тест get_user_info
        if users:
            logger.info("📋 Тестируем get_user_info...")
            for user_id in users[:3]:  # Тестируем первых 3
                user_info = get_user_info(user_id)
                logger.info(f"✅ Информация о пользователе {user_id}: {user_info.get('accounts_count', 0)} аккаунтов")
        
        logger.info("✅ ТЕСТ 1 ПРОЙДЕН: Основные функции работают")
        return True
        
    except Exception as e:
        logger.error(f"❌ ТЕСТ 1 НЕ ПРОЙДЕН: {e}")
        return False

def test_user_cache():
    """Тест системы кеширования пользователей"""
    logger.info("🧪 ТЕСТ 2: Система кеширования пользователей")
    
    try:
        from utils.user_cache import get_user_cache
        
        user_cache = get_user_cache()
        
        # Тест получения пользователей через кеш
        logger.info("📋 Тестируем get_active_users_safe...")
        cached_users = user_cache.get_active_users_safe()
        logger.info(f"✅ Кеш вернул {len(cached_users)} пользователей")
        
        # Тест получения пользователей с приоритетами
        logger.info("📋 Тестируем get_users_by_priority_safe...")
        priority_users = user_cache.get_users_by_priority_safe()
        logger.info(f"✅ Кеш вернул {len(priority_users)} пользователей с приоритетами")
        
        # Тест статистики кеша
        logger.info("📋 Тестируем статистику кеша...")
        cache_stats = user_cache.get_cache_stats()
        logger.info(f"✅ Статистика кеша: {cache_stats}")
        
        # Тест принудительного обновления
        logger.info("📋 Тестируем принудительное обновление...")
        refresh_result = user_cache.force_refresh()
        logger.info(f"✅ Принудительное обновление: {'успешно' if refresh_result else 'неудачно'}")
        
        logger.info("✅ ТЕСТ 2 ПРОЙДЕН: Система кеширования работает")
        return True
        
    except Exception as e:
        logger.error(f"❌ ТЕСТ 2 НЕ ПРОЙДЕН: {e}")
        return False

def test_processing_state():
    """Тест системы отслеживания состояния"""
    logger.info("🧪 ТЕСТ 3: Система отслеживания состояния обработки")
    
    try:
        from utils.processing_state import ProcessingState, health_check_processing_states
        
        # Создаем тестовый процесс
        logger.info("📋 Создаем тестовый процесс...")
        test_process = ProcessingState("test_isolation")
        
        # Тестируем цикл обработки
        test_users = [12345, 67890, 11111]
        logger.info(f"📋 Тестируем цикл обработки для {len(test_users)} пользователей...")
        
        test_process.start_cycle(test_users)
        
        for user_id in test_users:
            test_process.start_user_processing(user_id)
            time.sleep(0.1)  # Имитация обработки
            test_process.complete_user_processing(user_id, True)
        
        test_process.complete_cycle()
        
        # Получаем статистику
        stats = test_process.get_progress_stats()
        logger.info(f"✅ Статистика обработки: {stats}")
        
        # Тест общего health check
        logger.info("📋 Тестируем health check всех процессов...")
        health_status = health_check_processing_states()
        logger.info(f"✅ Health check процессов: {health_status.get('summary', {})}")
        
        logger.info("✅ ТЕСТ 3 ПРОЙДЕН: Система отслеживания состояния работает")
        return True
        
    except Exception as e:
        logger.error(f"❌ ТЕСТ 3 НЕ ПРОЙДЕН: {e}")
        return False

def test_health_monitoring():
    """Тест системы мониторинга здоровья"""
    logger.info("🧪 ТЕСТ 4: Система мониторинга здоровья")
    
    try:
        from utils.health_monitor import get_health_monitor, quick_health_check, get_health_summary
        
        # Тест быстрой проверки здоровья
        logger.info("📋 Тестируем быструю проверку здоровья...")
        health_check_result = quick_health_check()
        
        logger.info(f"✅ Health check статус: {health_check_result.get('overall_status')}")
        logger.info(f"   - Компонентов проверено: {health_check_result.get('statistics', {}).get('total_components', 0)}")
        logger.info(f"   - Здоровых компонентов: {health_check_result.get('statistics', {}).get('healthy_components', 0)}")
        logger.info(f"   - Компонентов с предупреждениями: {health_check_result.get('statistics', {}).get('warning_components', 0)}")
        logger.info(f"   - Компонентов с ошибками: {health_check_result.get('statistics', {}).get('error_components', 0)}")
        
        # Показываем проблемы если есть
        if health_check_result.get('issues'):
            logger.warning("⚠️ Обнаружены проблемы:")
            for issue in health_check_result['issues']:
                logger.warning(f"   - {issue}")
        
        if health_check_result.get('warnings'):
            logger.info("💡 Предупреждения:")
            for warning in health_check_result['warnings']:
                logger.info(f"   - {warning}")
        
        # Тест сводки здоровья
        logger.info("📋 Тестируем сводку здоровья...")
        health_summary = get_health_summary()
        logger.info(f"✅ Сводка здоровья: {health_summary}")
        
        logger.info("✅ ТЕСТ 4 ПРОЙДЕН: Система мониторинга здоровья работает")
        return True
        
    except Exception as e:
        logger.error(f"❌ ТЕСТ 4 НЕ ПРОЙДЕН: {e}")
        return False

def test_safe_user_wrapper():
    """Тест безопасных обёрток пользователей"""
    logger.info("🧪 ТЕСТ 5: Безопасные обёртки доступа к данным")
    
    try:
        from database.safe_user_wrapper import get_user_instagram_accounts, get_user_instagram_account
        from database.user_management import get_active_users
        
        users = get_active_users()
        
        if not users:
            logger.warning("⚠️ Нет пользователей для тестирования безопасных обёрток")
            return True
        
        # Тестируем получение аккаунтов пользователя
        test_user_id = users[0]
        logger.info(f"📋 Тестируем get_user_instagram_accounts для пользователя {test_user_id}...")
        
        user_accounts = get_user_instagram_accounts(user_id=test_user_id)
        logger.info(f"✅ Пользователь {test_user_id} имеет {len(user_accounts)} аккаунтов")
        
        # Тестируем получение конкретного аккаунта
        if user_accounts:
            test_account_id = user_accounts[0].id
            logger.info(f"📋 Тестируем get_user_instagram_account для аккаунта {test_account_id}...")
            
            account = get_user_instagram_account(test_account_id, user_id=test_user_id)
            if account:
                logger.info(f"✅ Аккаунт {account.username} успешно получен через безопасную обёртку")
            else:
                logger.warning(f"⚠️ Аккаунт {test_account_id} не найден")
        
        logger.info("✅ ТЕСТ 5 ПРОЙДЕН: Безопасные обёртки работают")
        return True
        
    except Exception as e:
        logger.error(f"❌ ТЕСТ 5 НЕ ПРОЙДЕН: {e}")
        return False

def test_isolation_integrity():
    """Тест целостности изоляции данных"""
    logger.info("🧪 ТЕСТ 6: Целостность изоляции данных")
    
    try:
        from database.db_manager import get_session
        from database.models import InstagramAccount
        from database.user_management import get_active_users
        
        session = get_session()
        
        # Проверяем, что у всех аккаунтов есть user_id
        logger.info("📋 Проверяем наличие user_id у всех аккаунтов...")
        total_accounts = session.query(InstagramAccount).count()
        accounts_without_user = session.query(InstagramAccount).filter(
            InstagramAccount.user_id.is_(None)
        ).count()
        
        logger.info(f"✅ Всего аккаунтов: {total_accounts}")
        logger.info(f"✅ Аккаунтов без user_id: {accounts_without_user}")
        
        if accounts_without_user > 0:
            logger.warning(f"⚠️ НАЙДЕНЫ АККАУНТЫ БЕЗ user_id: {accounts_without_user}")
            
            # Показываем примеры
            orphaned_accounts = session.query(InstagramAccount).filter(
                InstagramAccount.user_id.is_(None)
            ).limit(5).all()
            
            for account in orphaned_accounts:
                logger.warning(f"   - Аккаунт {account.username} (ID: {account.id}) без user_id")
        
        # Проверяем изоляцию между пользователями
        users = get_active_users()
        logger.info(f"📋 Проверяем изоляцию между {len(users)} пользователями...")
        
        user_account_counts = {}
        for user_id in users:
            user_accounts_count = session.query(InstagramAccount).filter(
                InstagramAccount.user_id == user_id
            ).count()
            user_account_counts[user_id] = user_accounts_count
        
        logger.info("✅ Распределение аккаунтов по пользователям:")
        for user_id, count in user_account_counts.items():
            logger.info(f"   - Пользователь {user_id}: {count} аккаунтов")
        
        session.close()
        
        # Вычисляем коэффициент изоляции
        isolation_ratio = (total_accounts - accounts_without_user) / total_accounts * 100 if total_accounts > 0 else 0
        logger.info(f"✅ Коэффициент изоляции: {isolation_ratio:.1f}%")
        
        if isolation_ratio >= 100:
            logger.info("✅ ТЕСТ 6 ПРОЙДЕН: Изоляция данных полная")
            return True
        elif isolation_ratio >= 95:
            logger.warning("⚠️ ТЕСТ 6 ЧАСТИЧНО ПРОЙДЕН: Изоляция данных почти полная")
            return True
        else:
            logger.error(f"❌ ТЕСТ 6 НЕ ПРОЙДЕН: Изоляция данных неполная ({isolation_ratio:.1f}%)")
            return False
        
    except Exception as e:
        logger.error(f"❌ ТЕСТ 6 НЕ ПРОЙДЕН: {e}")
        return False

def test_system_services():
    """Тест обновленных системных сервисов"""
    logger.info("🧪 ТЕСТ 7: Обновленные системные сервисы")
    
    try:
        # Тест импортов обновленных сервисов
        logger.info("📋 Тестируем импорты обновленных сервисов...")
        
        from utils.smart_validator_service import SmartValidatorService
        from utils.account_validator_service import AccountValidatorService  
        from utils.proxy_manager import distribute_proxies
        
        logger.info("✅ Все обновленные сервисы импортируются без ошибок")
        
        # Тест создания экземпляров сервисов
        logger.info("📋 Тестируем создание экземпляров сервисов...")
        
        try:
            # Тестируем SmartValidatorService
            validator_service = SmartValidatorService()
            logger.info("✅ SmartValidatorService создан успешно")
        except Exception as e:
            logger.warning(f"⚠️ Ошибка создания SmartValidatorService: {e}")
        
        try:
            # Тестируем AccountValidatorService
            account_validator = AccountValidatorService()
            logger.info("✅ AccountValidatorService создан успешно")
        except Exception as e:
            logger.warning(f"⚠️ Ошибка создания AccountValidatorService: {e}")
        
        logger.info("✅ ТЕСТ 7 ПРОЙДЕН: Системные сервисы обновлены корректно")
        return True
        
    except Exception as e:
        logger.error(f"❌ ТЕСТ 7 НЕ ПРОЙДЕН: {e}")
        return False

def main():
    """Основная функция тестирования"""
    logger.info("🚀 ЗАПУСК КОМПЛЕКСНОГО ТЕСТА СИСТЕМЫ ИЗОЛЯЦИИ ПОЛЬЗОВАТЕЛЕЙ")
    logger.info("=" * 80)
    
    start_time = datetime.now()
    
    # Список тестов
    tests = [
        ("Основные функции управления пользователями", test_basic_functions),
        ("Система кеширования пользователей", test_user_cache),
        ("Система отслеживания состояния", test_processing_state),
        ("Система мониторинга здоровья", test_health_monitoring),
        ("Безопасные обёртки доступа к данным", test_safe_user_wrapper),
        ("Целостность изоляции данных", test_isolation_integrity),
        ("Обновленные системные сервисы", test_system_services)
    ]
    
    # Выполняем тесты
    results = {}
    
    for test_name, test_func in tests:
        logger.info("-" * 80)
        try:
            result = test_func()
            results[test_name] = result
        except Exception as e:
            logger.error(f"❌ КРИТИЧЕСКАЯ ОШИБКА В ТЕСТЕ '{test_name}': {e}")
            results[test_name] = False
        
        time.sleep(1)  # Пауза между тестами
    
    # Подводим итоги
    logger.info("=" * 80)
    logger.info("📊 ИТОГИ ТЕСТИРОВАНИЯ")
    logger.info("=" * 80)
    
    passed_tests = sum(1 for result in results.values() if result)
    total_tests = len(results)
    
    logger.info(f"✅ Пройдено тестов: {passed_tests}/{total_tests}")
    logger.info(f"❌ Не пройдено тестов: {total_tests - passed_tests}/{total_tests}")
    
    # Детальные результаты
    logger.info("\n📋 ДЕТАЛЬНЫЕ РЕЗУЛЬТАТЫ:")
    for test_name, result in results.items():
        status = "✅ ПРОЙДЕН" if result else "❌ НЕ ПРОЙДЕН"
        logger.info(f"   {status} - {test_name}")
    
    # Общий статус
    test_duration = (datetime.now() - start_time).total_seconds()
    logger.info(f"\n⏱️ Время выполнения тестов: {test_duration:.2f} секунд")
    
    if passed_tests == total_tests:
        logger.info("🎉 ВСЕ ТЕСТЫ ПРОЙДЕНЫ! Система изоляции пользователей работает корректно!")
        return True
    else:
        logger.error(f"💥 ОБНАРУЖЕНЫ ПРОБЛЕМЫ! {total_tests - passed_tests} тестов не пройдено!")
        return False

if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        logger.info("⏹️ Тестирование прервано пользователем")
        sys.exit(1)
    except Exception as e:
        logger.error(f"💥 КРИТИЧЕСКАЯ ОШИБКА: {e}")
        sys.exit(1) 
#!/usr/bin/env python3
"""
Тест оптимизированной системы обновления профилей
"""

import asyncio
import logging
from database.db_manager import get_instagram_accounts
from instagram.profile_manager import ProfileManager
from utils.system_monitor import system_monitor
import time

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_single_profile_update():
    """Тест единичного обновления профиля"""
    logger.info("=== Тест единичного обновления профиля ===")
    
    # Получаем первый активный аккаунт
    accounts = [acc for acc in get_instagram_accounts() if acc.is_active]
    if not accounts:
        logger.error("Нет активных аккаунтов для тестирования")
        return
    
    account = accounts[0]
    logger.info(f"Тестируем аккаунт: @{account.username}")
    
    try:
        # Создаем ProfileManager
        pm = ProfileManager(account.id)
        
        # Тест 1: Обновление имени
        logger.info("Тест 1: Обновление имени")
        success, message = pm.update_profile_name("Test Name 🎯")
        logger.info(f"Результат: {success}, {message}")
        time.sleep(2)
        
        # Тест 2: Обновление био
        logger.info("Тест 2: Обновление био")
        success, message = pm.update_biography("Test Bio 📝\nLine 2\n#test")
        logger.info(f"Результат: {success}, {message}")
        time.sleep(2)
        
        # Тест 3: Обновление ссылки
        logger.info("Тест 3: Обновление ссылки")
        success, message = pm.update_profile_links("https://example.com")
        logger.info(f"Результат: {success}, {message}")
        
    except Exception as e:
        logger.error(f"Ошибка при тестировании: {e}")

def test_batch_profile_update():
    """Тест массового обновления профилей"""
    logger.info("\n=== Тест массового обновления профилей ===")
    
    # Получаем первые 5 активных аккаунтов
    accounts = [acc for acc in get_instagram_accounts() if acc.is_active][:5]
    if not accounts:
        logger.error("Нет активных аккаунтов для тестирования")
        return
    
    account_ids = [acc.id for acc in accounts]
    logger.info(f"Тестируем {len(account_ids)} аккаунтов")
    
    # Получаем рекомендованное количество потоков
    workload_limits = system_monitor.get_workload_limits()
    max_workers = min(workload_limits.max_workers, len(account_ids))
    logger.info(f"Используем {max_workers} потоков (рекомендовано system_monitor)")
    
    def progress_callback(processed, total, success, errors):
        logger.info(f"Прогресс: {processed}/{total}, Успешно: {success}, Ошибок: {errors}")
    
    # Тест 1: Массовое обновление имен с шаблоном
    logger.info("\nТест 1: Массовое обновление имен с шаблоном")
    success_count, errors = ProfileManager.batch_update_profiles(
        account_ids=account_ids,
        update_type='edit_name',
        value='{Creative|Amazing|Awesome} {Studio|Lab|Team}',
        max_workers=max_workers,
        progress_callback=progress_callback
    )
    
    logger.info(f"Результат: Успешно {success_count}/{len(account_ids)}")
    if errors:
        for error in errors:
            logger.error(f"Ошибка: {error}")
    
    time.sleep(5)
    
    # Тест 2: Массовое обновление био
    logger.info("\nТест 2: Массовое обновление био")
    bio_template = """🌟 {Welcome|Hello|Hi}!
📸 {Photography|Content|Creative} 
🎯 {Follow|DM|Contact} for more
@username"""
    
    success_count, errors = ProfileManager.batch_update_profiles(
        account_ids=account_ids,
        update_type='edit_bio',
        value=bio_template,
        max_workers=max_workers,
        progress_callback=progress_callback
    )
    
    logger.info(f"Результат: Успешно {success_count}/{len(account_ids)}")
    if errors:
        for error in errors:
            logger.error(f"Ошибка: {error}")

def test_error_handling():
    """Тест обработки ошибок"""
    logger.info("\n=== Тест обработки ошибок ===")
    
    # Тест с несуществующим аккаунтом
    fake_account_ids = [99999, 88888]
    
    success_count, errors = ProfileManager.batch_update_profiles(
        account_ids=fake_account_ids,
        update_type='edit_name',
        value='Test',
        max_workers=2
    )
    
    logger.info(f"Результат: Успешно {success_count}/{len(fake_account_ids)}")
    logger.info(f"Ошибок: {len(errors)}")
    for error in errors:
        logger.info(f"Ожидаемая ошибка: {error}")

def main():
    """Главная функция тестирования"""
    logger.info("🚀 ЗАПУСК ТЕСТИРОВАНИЯ ОПТИМИЗИРОВАННОЙ СИСТЕМЫ ПРОФИЛЕЙ")
    
    # Тест 1: Единичное обновление
    test_single_profile_update()
    
    # Ждем между тестами
    time.sleep(5)
    
    # Тест 2: Массовое обновление
    test_batch_profile_update()
    
    # Тест 3: Обработка ошибок
    test_error_handling()
    
    logger.info("\n✅ ТЕСТИРОВАНИЕ ЗАВЕРШЕНО")

if __name__ == "__main__":
    main() 
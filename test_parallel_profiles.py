#!/usr/bin/env python3
"""
Тест параллельного обновления профилей
"""

import time
import logging
from database.db_manager import get_instagram_accounts
from instagram.profile_manager import ProfileManager
from utils.system_monitor import system_monitor
import concurrent.futures
import threading

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_parallel_update():
    """Тестирует параллельное обновление профилей"""
    
    # Получаем первые 5 активных аккаунтов
    all_accounts = get_instagram_accounts()
    accounts = [acc for acc in all_accounts if acc.is_active][:5]
    if not accounts:
        logger.error("Нет активных аккаунтов для тестирования")
        return
    
    logger.info(f"Найдено {len(accounts)} аккаунтов для тестирования")
    
    # Получаем рекомендованное количество потоков
    workload_limits = system_monitor.get_workload_limits()
    max_workers = min(workload_limits.max_workers, len(accounts))
    
    logger.info(f"🖥️ Системные рекомендации:")
    logger.info(f"  - Максимум потоков: {workload_limits.max_workers}")
    logger.info(f"  - Используем потоков: {max_workers}")
    logger.info(f"  - Описание: {workload_limits.description}")
    
    # Тестовое имя для обновления
    test_name = f"Test Update {int(time.time())}"
    
    # Переменные для отслеживания результатов
    success_count = 0
    errors = []
    lock = threading.Lock()
    start_time = time.time()
    
    def update_single_profile(account):
        """Обновляет один профиль"""
        nonlocal success_count
        thread_start = time.time()
        try:
            logger.info(f"[Thread-{threading.current_thread().name}] Начинаю обновление @{account.username}")
            
            profile_manager = ProfileManager(account.id)
            success, message = profile_manager.update_profile_name(test_name)
            
            with lock:
                if success:
                    success_count += 1
                    logger.info(f"✅ @{account.username}: Успешно за {time.time()-thread_start:.1f}с")
                else:
                    errors.append(f"@{account.username}: {message}")
                    logger.error(f"❌ @{account.username}: {message}")
                    
        except Exception as e:
            with lock:
                error_msg = str(e)
                errors.append(f"@{account.username}: {error_msg}")
                logger.error(f"❌ @{account.username}: {error_msg}")
    
    # Запускаем параллельную обработку
    logger.info(f"\n🚀 Запуск параллельной обработки с {max_workers} потоками...\n")
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(update_single_profile, acc): acc for acc in accounts}
        
        # Ждем завершения всех задач
        concurrent.futures.wait(futures)
    
    # Результаты
    total_time = time.time() - start_time
    logger.info(f"\n📊 РЕЗУЛЬТАТЫ:")
    logger.info(f"  - Обработано: {len(accounts)} аккаунтов")
    logger.info(f"  - Успешно: {success_count}")
    logger.info(f"  - Ошибок: {len(errors)}")
    logger.info(f"  - Время: {total_time:.1f}с")
    logger.info(f"  - Среднее время на аккаунт: {total_time/len(accounts):.1f}с")
    
    if errors:
        logger.info("\n❌ Ошибки:")
        for error in errors:
            logger.info(f"  - {error}")
    
    # Сравнение с последовательной обработкой
    sequential_time = len(accounts) * (total_time / len(accounts) * max_workers)
    logger.info(f"\n⚡ Ускорение: {sequential_time/total_time:.1f}x быстрее чем последовательно")

if __name__ == "__main__":
    test_parallel_update() 
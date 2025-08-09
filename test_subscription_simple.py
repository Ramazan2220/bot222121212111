#!/usr/bin/env python3
"""Простой тест системы подписок"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from utils.subscription_service import subscription_service

def test_subscription_logic():
    """Тестирует логику подписки для нового пользователя"""
    # Проверяем конкретного пользователя из скриншота
    real_user_id = 265436026  # ID из скриншота
    test_user_id = 999999999  # Тестовый ID
    
    print("🧪 Тестирование логики подписки...")
    
    # 1. Проверяем реального пользователя из скриншота
    print(f"1. Проверяем РЕАЛЬНОГО пользователя {real_user_id} из скриншота")
    access_info = subscription_service.check_user_access(real_user_id)
    print(f"   Результат: {access_info}")
    print(f"   has_access: {access_info['has_access']}")
    
    stats = subscription_service.get_user_stats(real_user_id)
    print(f"   Статистика: has_access={stats['has_access']}, status={stats['status']}")
    
    # 2. Проверяем нового пользователя
    print(f"\n2. Проверяем нового пользователя {test_user_id} (должен быть не зарегистрирован)")
    access_info = subscription_service.check_user_access(test_user_id)
    print(f"   Результат: {access_info}")
    
    # 3. Создаем пользователя
    print(f"\n3. Создаем пользователя {test_user_id}")
    subscription_service.ensure_user_exists(test_user_id, "test_user")
    
    # 4. Проверяем созданного пользователя
    print(f"\n4. Проверяем созданного пользователя {test_user_id} (должен быть без доступа)")
    access_info = subscription_service.check_user_access(test_user_id)
    print(f"   Результат: {access_info}")
    
    # 5. Получаем статистику
    print(f"\n5. Получаем статистику пользователя {test_user_id}")
    stats = subscription_service.get_user_stats(test_user_id)
    print(f"   Статистика: {stats}")
    
    print(f"\n✅ Тест завершен!")

if __name__ == "__main__":
    test_subscription_logic() 
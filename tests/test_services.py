#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Тесты для новых сервисов MVP
"""

import unittest
import asyncio
import time
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta

# Импортируем тестируемые модули
from services.rate_limiter import RateLimiter, ActionType
from services.anti_detection import AntiDetectionService
from services.instagram_service import InstagramService
from services.account_automation import AccountAutomationService
from services.async_task_processor import AsyncTaskProcessor


class TestRateLimiter(unittest.TestCase):
    """Тесты для RateLimiter"""
    
    def setUp(self):
        self.rate_limiter = RateLimiter()
    
    def test_can_perform_action_new_account(self):
        """Тест лимитов для нового аккаунта"""
        account_id = 999  # Используем несуществующий ID для симуляции нового аккаунта
        
        # Проверяем лимиты
        limits = self.rate_limiter._get_limits(account_id)
        follow_limit = limits["hourly"][ActionType.FOLLOW]
        
        # Имитируем действия с правильными интервалами (вручную добавляем timestamp'ы)
        import time as time_module
        base_time = time_module.time()
        
        # Добавляем действия с интервалом 3 секунды
        for i in range(follow_limit):
            # Добавляем timestamp вручную (имитируем действия в прошлом)
            self.rate_limiter._actions[account_id][ActionType.FOLLOW].append(base_time + i * 3)
        
        # Теперь проверяем, что следующее действие заблокировано по лимиту
        can_do, reason = self.rate_limiter.can_perform_action(account_id, ActionType.FOLLOW)
        self.assertFalse(can_do)
        self.assertIn("лимит", reason)
    
    def test_anti_spam_protection(self):
        """Тест защиты от спама (минимум 2 секунды между действиями)"""
        account_id = 998
        
        # Первое действие должно пройти
        can_do, reason = self.rate_limiter.can_perform_action(account_id, ActionType.LIKE)
        self.assertTrue(can_do)
        self.rate_limiter.record_action(account_id, ActionType.LIKE)
        
        # Сразу второе действие должно быть заблокировано
        can_do, reason = self.rate_limiter.can_perform_action(account_id, ActionType.LIKE)
        self.assertFalse(can_do)
        self.assertIn("быстрые действия", reason)
    
    def test_get_wait_time(self):
        """Тест расчета времени ожидания"""
        account_id = 1
        wait_time = self.rate_limiter.get_wait_time(account_id, ActionType.LIKE)
        
        # Для нового аккаунта должно быть около 30 секунд ±20% (24-36 сек)
        # Но с минимумом 2 секунды
        self.assertGreaterEqual(wait_time, 2)
        self.assertLessEqual(wait_time, 40)
    
    def test_block_action(self):
        """Тест временной блокировки действия"""
        account_id = 1
        
        # Блокируем на 2 секунды
        self.rate_limiter.block_action(account_id, ActionType.POST, 2)
        
        # Сразу после блокировки - нельзя
        can_do, reason = self.rate_limiter.can_perform_action(account_id, ActionType.POST)
        self.assertFalse(can_do)
        self.assertIn("заблокировано", reason)
        
        # Через 3 секунды - можно
        time.sleep(3)
        can_do, reason = self.rate_limiter.can_perform_action(account_id, ActionType.POST)
        self.assertTrue(can_do)


class TestAntiDetection(unittest.TestCase):
    """Тесты для AntiDetection"""
    
    def setUp(self):
        self.anti_detection = AntiDetectionService()
    
    def test_create_human_behavior_pattern(self):
        """Тест создания паттерна поведения"""
        account_id = 1
        pattern = self.anti_detection.create_human_behavior_pattern(account_id)
        
        # Проверяем структуру
        self.assertIn('active_hours', pattern)
        self.assertIn('action_speed', pattern)
        self.assertIn('interests', pattern)
        self.assertIn('scroll_patterns', pattern)
        
        # Проверяем значения
        self.assertGreater(len(pattern['active_hours']), 5)
        self.assertIn(pattern['action_speed'], ['fast', 'medium', 'slow'])
        self.assertGreaterEqual(len(pattern['interests']), 3)
    
    def test_generate_device_fingerprint(self):
        """Тест генерации fingerprint устройства"""
        account_id = 1
        fingerprint = self.anti_detection.generate_device_fingerprint(account_id)
        
        # Проверяем обязательные поля
        required_fields = ['device_id', 'phone_id', 'uuid', 'user_agent', 'app_version']
        for field in required_fields:
            self.assertIn(field, fingerprint)
            self.assertIsNotNone(fingerprint[field])
        
        # Проверяем уникальность
        fingerprint2 = self.anti_detection.generate_device_fingerprint(2)
        self.assertNotEqual(fingerprint['device_id'], fingerprint2['device_id'])
    
    def test_humanize_action_timing(self):
        """Тест человекоподобных задержек"""
        account_id = 1
        
        # Создаем паттерн с известной скоростью
        self.anti_detection.behavior_patterns[account_id] = {
            'action_speed': 'medium'
        }
        
        delays = []
        for _ in range(10):
            delay = self.anti_detection.humanize_action_timing(account_id, 'like')
            delays.append(delay)
        
        # Проверяем диапазон
        self.assertTrue(all(0.3 <= d <= 3.0 for d in delays))
        # Проверяем вариативность
        self.assertGreater(len(set(delays)), 5)
    
    def test_simulate_human_typing(self):
        """Тест симуляции набора текста"""
        text = "Hello World"
        events = self.anti_detection.simulate_human_typing(text)
        
        # Должны быть события для каждого символа (минимум столько же)
        self.assertGreaterEqual(len(events), len(text))
        
        # Последнее событие должно содержать полный текст
        final_text, _ = events[-1]
        self.assertEqual(final_text, text)
        
        # Проверяем, что есть промежуточные состояния
        if len(events) > 1:
            first_text, _ = events[0]
            self.assertLess(len(first_text), len(text))


class TestInstagramService(unittest.TestCase):
    """Тесты для InstagramService"""
    
    @patch('services.instagram_service.get_instagram_account')
    @patch('services.instagram_service.encryption')
    def test_get_decrypted_password(self, mock_encryption, mock_get_account):
        """Тест дешифрования пароля"""
        service = InstagramService()
        
        # Мокаем аккаунт
        mock_account = Mock()
        mock_account.password = 'gAAAAA_encrypted_password'
        mock_get_account.return_value = mock_account
        
        # Мокаем дешифрование
        mock_encryption.decrypt_password.return_value = 'decrypted_password'
        
        # Тестируем
        password = service.get_decrypted_password(1)
        self.assertEqual(password, 'decrypted_password')
        mock_encryption.decrypt_password.assert_called_once_with('gAAAAA_encrypted_password')


class TestAsyncTaskProcessor(unittest.TestCase):
    """Тесты для AsyncTaskProcessor"""
    
    def setUp(self):
        self.processor = AsyncTaskProcessor(max_workers=2)
    
    @patch('services.async_task_processor.get_publish_task')
    @patch('services.async_task_processor.update_publish_task_status')
    @patch('services.async_task_processor.rate_limiter')
    def test_process_tasks_async(self, mock_limiter, mock_update_status, mock_get_task):
        """Тест асинхронной обработки задач"""
        # Мокаем задачи
        mock_task = Mock()
        mock_task.account_id = 1
        mock_task.task_type = 'PHOTO'
        mock_get_task.return_value = mock_task
        
        # Мокаем rate limiter
        mock_limiter.can_perform_action.return_value = (True, None)
        
        # Обрабатываем 3 задачи (используем helper функцию)
        async def run_test():
            return await self.processor.process_tasks_async([1, 2, 3])
        
        results = run_async_test(run_test())
        
        # Проверяем результаты
        self.assertEqual(len(results), 3)
        self.assertEqual(mock_get_task.call_count, 3)
    
    def test_parallel_processing(self):
        """Тест параллельной обработки"""
        from services.async_task_processor import process_tasks_parallel, async_processor
        
        # Мокаем обработку на уровне async_processor
        with patch.object(async_processor, '_process_single_task', return_value=True):
            results = process_tasks_parallel([1, 2, 3])
            self.assertEqual(results, {1: True, 2: True, 3: True})


class TestAccountAutomation(unittest.TestCase):
    """Тесты для AccountAutomationService"""
    
    @patch('services.account_automation.get_instagram_account')
    def test_get_account_status(self, mock_get_account):
        """Тест получения статуса аккаунта"""
        service = AccountAutomationService()
        
        # Мокаем аккаунт
        mock_account = Mock()
        mock_account.username = 'test_user'
        mock_get_account.return_value = mock_account
        
        # Мокаем метрики
        with patch.object(service.health_monitor, 'calculate_comprehensive_health_score', return_value=85):
            with patch.object(service.predictive_monitor, 'calculate_ban_risk_score', return_value=15):
                status = service.get_account_status(1)
        
        # Проверяем результат
        self.assertEqual(status['health_score'], 85)
        self.assertEqual(status['ban_risk_score'], 15)
        # При health=85 и risk=15 должен быть EXCELLENT статус
        self.assertEqual(status['status'], 'EXCELLENT')
        self.assertTrue(status['can_warm'])


def run_async_test(coro):
    """Вспомогательная функция для запуска async тестов"""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


if __name__ == '__main__':
    # Запускаем тесты
    unittest.main() 
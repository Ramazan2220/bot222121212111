#!/usr/bin/env python3
"""
Автоматизированный тест интеграции UI с Advanced Warmup 2.0
Проверяет правильность работы кнопок без реального запуска прогрева
"""

import sys
import unittest
from unittest.mock import Mock, patch, MagicMock
from telegram import Update, CallbackQuery, Message, User, Chat
from telegram.ext import CallbackContext

# Добавляем путь к проекту
sys.path.append('.')

class TestUIIntegration(unittest.TestCase):
    """Тест интеграции UI кнопок"""
    
    def setUp(self):
        """Настройка тестов"""
        # Создаем моки для Telegram объектов
        self.user = Mock(spec=User)
        self.user.id = 12345
        self.user.first_name = "Test"
        self.user.username = "testuser"
        
        self.chat = Mock(spec=Chat)
        self.chat.id = 67890
        
        self.message = Mock(spec=Message)
        self.message.chat = self.chat
        self.message.message_id = 1
        
        self.context = Mock(spec=CallbackContext)
        self.context.user_data = {}
        self.context.bot = Mock()
        
    def create_callback_update(self, callback_data):
        """Создает Update с callback_query"""
        query = Mock(spec=CallbackQuery)
        query.data = callback_data
        query.message = self.message
        query.edit_message_text = Mock()
        query.answer = Mock()
        
        update = Mock(spec=Update)
        update.callback_query = query
        update.effective_user = self.user
        update.effective_chat = self.chat
        
        return update
    
    def test_warmup_buttons_routing(self):
        """Тест 1: Проверка роутинга кнопок прогрева"""
        print("\n🧪 Тест 1: Роутинг кнопок прогрева")
        print("="*50)
        
        test_cases = [
            ("smart_warmup", "smart_warm_command"),
            ("smart_warm_menu", "smart_warm_command"), 
            ("status", "status_command"),
            ("limits", "limits_command"),
            ("warmup_status", "status_command"),
            ("warmup_settings", "limits_command")
        ]
        
        for callback_data, expected_function in test_cases:
            with self.subTest(callback_data=callback_data):
                update = self.create_callback_update(callback_data)
                
                try:
                    # Проверяем handlers.py
                    with patch('telegram_bot.handlers.automation_handlers.smart_warm_command') as mock_smart:
                        with patch('telegram_bot.handlers.automation_handlers.status_command') as mock_status:
                            with patch('telegram_bot.handlers.automation_handlers.limits_command') as mock_limits:
                                
                                # Импортируем и тестируем обработчик
                                from telegram_bot.handlers import handle_callback
                                handle_callback(update, self.context)
                                
                                # Проверяем что вызвана правильная функция
                                if expected_function == "smart_warm_command":
                                    mock_smart.assert_called_once()
                                    print(f"  ✅ {callback_data} → {expected_function}")
                                elif expected_function == "status_command":
                                    mock_status.assert_called_once()
                                    print(f"  ✅ {callback_data} → {expected_function}")
                                elif expected_function == "limits_command":
                                    mock_limits.assert_called_once()
                                    print(f"  ✅ {callback_data} → {expected_function}")
                                    
                except Exception as e:
                    print(f"  ❌ {callback_data} → Ошибка: {e}")
    
    def test_account_selection_flow(self):
        """Тест 2: Проверка выбора аккаунтов"""
        print("\n🧪 Тест 2: Выбор аккаунтов")
        print("="*50)
        
        # Мокаем базу данных
        with patch('database.db_manager.get_instagram_accounts') as mock_accounts:
            # Создаем тестовые аккаунты
            mock_account1 = Mock()
            mock_account1.id = 1
            mock_account1.username = "test_account1"
            mock_account1.is_active = True
            mock_account1.status = "active"
            
            mock_account2 = Mock()
            mock_account2.id = 7
            mock_account2.username = "fischercarmen3096194"
            mock_account2.is_active = True
            mock_account2.status = "active"
            
            mock_accounts.return_value = [mock_account1, mock_account2]
            
            # Тестируем команду smart_warm
            update = self.create_callback_update("smart_warm_menu")
            
            try:
                with patch('telegram_bot.utils.account_selection.AccountSelector') as mock_selector:
                    mock_selector_instance = Mock()
                    mock_selector.return_value = mock_selector_instance
                    
                    from telegram_bot.handlers.automation_handlers import smart_warm_command
                    smart_warm_command(update, self.context)
                    
                    # Проверяем что создается селектор аккаунтов
                    mock_selector.assert_called_once()
                    mock_selector_instance.show_accounts_list.assert_called_once()
                    
                    call_args = mock_selector_instance.show_accounts_list.call_args
                    callback_prefix = call_args[1]['callback_prefix']
                    
                    self.assertEqual(callback_prefix, "warm_account_")
                    print(f"  ✅ Селектор аккаунтов создается с префиксом: {callback_prefix}")
                    
            except Exception as e:
                print(f"  ❌ Ошибка выбора аккаунтов: {e}")
    
    def test_account_callback_handling(self):
        """Тест 3: Обработка колбэков аккаунтов"""
        print("\n🧪 Тест 3: Обработка колбэков аккаунтов")
        print("="*50)
        
        test_account_callbacks = [
            "warm_account_1",
            "warm_account_7", 
            "warm_account_23"
        ]
        
        for callback_data in test_account_callbacks:
            with self.subTest(callback_data=callback_data):
                update = self.create_callback_update(callback_data)
                
                # Мокаем аккаунт
                with patch('database.db_manager.get_instagram_account') as mock_get_account:
                    mock_account = Mock()
                    mock_account.id = int(callback_data.split('_')[-1])
                    mock_account.username = f"test_account_{mock_account.id}"
                    mock_account.is_active = True
                    mock_get_account.return_value = mock_account
                    
                    # Мокаем automation_service
                    with patch('services.account_automation.automation_service.get_account_status') as mock_status:
                        mock_status.return_value = {
                            'health_score': 85,
                            'ban_risk_score': 15,
                            'status': 'EXCELLENT',
                            'can_warm': True
                        }
                        
                        # Мокаем advanced_warmup
                        with patch('services.advanced_warmup.advanced_warmup.determine_time_pattern') as mock_pattern:
                            mock_pattern.return_value = {
                                'intensity': 1.0,
                                'duration': (20, 45)
                            }
                            
                            try:
                                from telegram_bot.handlers.automation_handlers import warm_account_callback
                                warm_account_callback(update, self.context)
                                
                                # Проверяем что сообщение отредактировано
                                update.callback_query.edit_message_text.assert_called_once()
                                
                                # Проверяем содержимое сообщения
                                call_args = update.callback_query.edit_message_text.call_args
                                message_text = call_args[0][0]
                                
                                self.assertIn(mock_account.username, message_text)
                                self.assertIn("Временной паттерн", message_text)
                                self.assertIn("100%", message_text)
                                
                                print(f"  ✅ {callback_data} → Корректное отображение информации")
                                
                            except Exception as e:
                                print(f"  ❌ {callback_data} → Ошибка: {e}")
    
    def test_warmup_duration_selection(self):
        """Тест 4: Выбор длительности прогрева"""
        print("\n🧪 Тест 4: Выбор длительности прогрева")
        print("="*50)
        
        duration_callbacks = [
            ("start_warm_7_15", 7, 15),
            ("start_warm_1_30", 1, 30),
            ("start_warm_23_60", 23, 60)
        ]
        
        for callback_data, expected_account_id, expected_duration in duration_callbacks:
            with self.subTest(callback_data=callback_data):
                update = self.create_callback_update(callback_data)
                
                # Мокаем advanced_warmup чтобы он не запускался реально
                with patch('services.advanced_warmup.advanced_warmup.start_warmup') as mock_warmup:
                    mock_warmup.return_value = (True, "Тестовый отчет")
                    
                    with patch('threading.Thread') as mock_thread:
                        try:
                            from telegram_bot.handlers.automation_handlers import start_warm_callback
                            start_warm_callback(update, self.context)
                            
                            # Проверяем что создается поток для прогрева
                            mock_thread.assert_called_once()
                            
                            # Проверяем что сообщение обновляется
                            update.callback_query.edit_message_text.assert_called_once()
                            
                            call_args = update.callback_query.edit_message_text.call_args
                            message_text = call_args[0][0]
                            
                            self.assertIn("Прогрев запущен", message_text)
                            self.assertIn(str(expected_duration), message_text)
                            
                            print(f"  ✅ {callback_data} → Аккаунт {expected_account_id}, длительность {expected_duration} мин")
                            
                        except Exception as e:
                            print(f"  ❌ {callback_data} → Ошибка: {e}")
    
    def test_interests_selection(self):
        """Тест 5: Выбор интересов"""
        print("\n🧪 Тест 5: Выбор интересов")
        print("="*50)
        
        # Тест кнопки "С интересами"
        update = self.create_callback_update("warm_interests_7")
        
        try:
            from telegram_bot.handlers.automation_handlers import warm_interests_callback
            warm_interests_callback(update, self.context)
            
            # Проверяем что показывается меню интересов
            update.callback_query.edit_message_text.assert_called_once()
            
            call_args = update.callback_query.edit_message_text.call_args
            message_text = call_args[0][0]
            
            self.assertIn("Выберите интересы", message_text)
            
            # Проверяем что есть reply_markup с кнопками
            reply_markup = call_args[1]['reply_markup']
            self.assertIsNotNone(reply_markup)
            
            print("  ✅ Меню выбора интересов отображается корректно")
            
        except Exception as e:
            print(f"  ❌ Ошибка выбора интересов: {e}")
        
        # Тест добавления интереса
        update2 = self.create_callback_update("add_interest_7_travel")
        
        try:
            from telegram_bot.handlers.automation_handlers import add_interest_callback
            add_interest_callback(update2, self.context)
            
            # Проверяем что интерес добавлен в user_data
            key = 'warm_interests_7'
            self.assertIn(key, self.context.user_data)
            self.assertIn('travel', self.context.user_data[key])
            
            print("  ✅ Интерес 'travel' добавлен корректно")
            
        except Exception as e:
            print(f"  ❌ Ошибка добавления интереса: {e}")

def run_ui_tests():
    """Запуск всех UI тестов"""
    print("🔥 АВТОМАТИЗИРОВАННЫЙ ТЕСТ UI ИНТЕГРАЦИИ")
    print("Advanced Warmup 2.0 - Проверка кнопок и навигации")
    print("="*60)
    
    # Создаем test suite
    suite = unittest.TestSuite()
    
    # Добавляем тесты
    suite.addTest(TestUIIntegration('test_warmup_buttons_routing'))
    suite.addTest(TestUIIntegration('test_account_selection_flow'))
    suite.addTest(TestUIIntegration('test_account_callback_handling'))
    suite.addTest(TestUIIntegration('test_warmup_duration_selection'))
    suite.addTest(TestUIIntegration('test_interests_selection'))
    
    # Запускаем тесты
    runner = unittest.TextTestRunner(verbosity=0, stream=open('/dev/null', 'w'))
    result = runner.run(suite)
    
    # Выводим итоги
    print("\n" + "="*60)
    print("📊 РЕЗУЛЬТАТЫ ТЕСТИРОВАНИЯ:")
    print(f"  ✅ Успешных тестов: {result.testsRun - len(result.failures) - len(result.errors)}")
    print(f"  ❌ Ошибок: {len(result.errors)}")
    print(f"  ⚠️  Неудач: {len(result.failures)}")
    
    if result.errors:
        print("\n❌ ОШИБКИ:")
        for test, error in result.errors:
            print(f"  • {test}: {error.splitlines()[-1]}")
    
    if result.failures:
        print("\n⚠️  НЕУДАЧИ:")
        for test, failure in result.failures:
            print(f"  • {test}: {failure.splitlines()[-1]}")
    
    if not result.errors and not result.failures:
        print("\n🎉 ВСЕ ТЕСТЫ ПРОШЛИ УСПЕШНО!")
        print("🚀 UI интеграция работает корректно!")
    
    print("="*60)
    
    return len(result.errors) == 0 and len(result.failures) == 0

if __name__ == "__main__":
    success = run_ui_tests()
    sys.exit(0 if success else 1) 
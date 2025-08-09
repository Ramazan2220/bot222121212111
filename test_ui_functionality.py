#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Тест UI функциональности Telegram бота
Проверяет корректность работы кнопок и выбора аккаунтов
"""

import asyncio
import sys
import traceback
from unittest.mock import MagicMock, AsyncMock
from telegram import Update, CallbackQuery, Message, Chat, User, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import CallbackContext

# Добавляем пути для импорта
sys.path.append('.')

def create_mock_callback_query(data, user_id=123456789):
    """Создает мок CallbackQuery"""
    query = MagicMock()
    query.data = data
    query.from_user.id = user_id
    query.from_user.username = "test_user"
    query.message.chat.id = user_id
    query.message.message_id = 1
    query.answer = AsyncMock()
    query.edit_message_text = AsyncMock()
    query.edit_message_reply_markup = AsyncMock()
    return query

def create_mock_context():
    """Создает мок контекста"""
    context = MagicMock()
    context.user_data = {}
    context.bot_data = {}
    return context

async def test_warmup_buttons():
    """Тестирует кнопки прогрева"""
    print("🔥 Тестирование кнопок прогрева...")
    
    try:
        from telegram_bot.handlers.automation_handlers import (
            smart_warm_command, status_command, limits_command
        )
        
        # Тест главного меню прогрева
        query = create_mock_callback_query("smart_warm_menu")
        context = create_mock_context()
        
        result = await smart_warm_command(query, context)
        print(f"✅ Главное меню прогрева: {result if result else 'OK'}")
        
        # Тест статуса
        query = create_mock_callback_query("status")
        result = await status_command(query, context)
        print(f"✅ Команда статуса: {result if result else 'OK'}")
        
        # Тест лимитов
        query = create_mock_callback_query("limits")
        result = await limits_command(query, context)
        print(f"✅ Команда лимитов: {result if result else 'OK'}")
        
        return True
        
    except Exception as e:
        print(f"❌ Ошибка в тесте кнопок прогрева: {e}")
        traceback.print_exc()
        return False

async def test_account_selection():
    """Тестирует выбор аккаунтов"""
    print("\n👤 Тестирование выбора аккаунтов...")
    
    try:
        from telegram_bot.utils.account_selection import AccountSelector
        from database.db_manager import get_instagram_accounts
        
        # Получаем аккаунты
        accounts = get_instagram_accounts()
        if not accounts:
            print("⚠️ Нет аккаунтов для тестирования")
            return True
            
        print(f"📱 Найдено аккаунтов: {len(accounts)}")
        
        # Создаем селектор
        selector = AccountSelector()
        
        # Тест начала выбора
        query = create_mock_callback_query("test_action")
        context = create_mock_context()
        
        result = await selector.start_selection(
            query, context, 
            action_type="test", 
            title="Тест выбора аккаунтов"
        )
        print(f"✅ Начало выбора: {result}")
        
        # Тест выбора источника "все аккаунты"
        query = create_mock_callback_query("acc_sel_source_all")
        context.user_data['account_selection'] = {
            'action_type': 'test',
            'title': 'Тест'
        }
        
        result = await selector.handle_source_selection(query, context)
        print(f"✅ Выбор всех аккаунтов: {result}")
        
        # Тест выбора конкретного аккаунта
        if accounts:
            acc_id = accounts[0].id
            query = create_mock_callback_query(f"acc_sel_toggle_{acc_id}")
            
            result = await selector.handle_account_toggle(query, context)
            print(f"✅ Переключение аккаунта {acc_id}: {result if result else 'OK'}")
        
        return True
        
    except Exception as e:
        print(f"❌ Ошибка в тесте выбора аккаунтов: {e}")
        traceback.print_exc()
        return False

async def test_profile_handlers():
    """Тестирует обработчики профиля"""
    print("\n👥 Тестирование обработчиков профиля...")
    
    try:
        from telegram_bot.handlers.profile_handlers import (
            profile_setup_menu, start_profile_selection
        )
        
        # Тест меню настройки профиля
        query = create_mock_callback_query("profile_setup")
        context = create_mock_context()
        
        result = await profile_setup_menu(query, context)
        print(f"✅ Меню настройки профиля: {result}")
        
        # Тест выбора источника для профиля
        query = create_mock_callback_query("profile_select_source_all")
        result = await start_profile_selection(query, context)
        print(f"✅ Выбор источника профиля: {result}")
        
        return True
        
    except Exception as e:
        print(f"❌ Ошибка в тесте профиля: {e}")
        traceback.print_exc()
        return False

async def test_callback_routing():
    """Тестирует маршрутизацию callback-запросов"""
    print("\n🔄 Тестирование маршрутизации callback...")
    
    try:
        from telegram_bot.handlers import handle_callback
        
        # Список тестовых callback_data
        test_callbacks = [
            "smart_warm_menu",
            "status", 
            "limits",
            "acc_sel_source_all",
            "acc_sel_source_folder",
            "profile_setup",
            "profile_select_source_all"
        ]
        
        for callback_data in test_callbacks:
            query = create_mock_callback_query(callback_data)
            context = create_mock_context()
            
            try:
                result = await handle_callback(query, context)
                print(f"✅ Callback '{callback_data}': {result if result else 'OK'}")
            except Exception as e:
                print(f"❌ Callback '{callback_data}': {e}")
        
        return True
        
    except Exception as e:
        print(f"❌ Ошибка в тесте маршрутизации: {e}")
        traceback.print_exc()
        return False

async def test_keyboard_generation():
    """Тестирует генерацию клавиатур"""
    print("\n⌨️ Тестирование генерации клавиатур...")
    
    try:
        from telegram_bot.keyboards import (
            get_warmup_menu_keyboard,
            get_warmup_mode_keyboard
        )
        
        # Тест клавиатуры меню прогрева
        keyboard = get_warmup_menu_keyboard()
        print(f"✅ Клавиатура меню прогрева: {len(keyboard.inline_keyboard)} рядов")
        
        # Тест клавиатуры режимов прогрева
        keyboard = get_warmup_mode_keyboard()
        print(f"✅ Клавиатура режимов: {len(keyboard.inline_keyboard)} рядов")
        
        return True
        
    except Exception as e:
        print(f"❌ Ошибка в тесте клавиатур: {e}")
        traceback.print_exc()
        return False

async def main():
    """Главная функция тестирования"""
    print("🧪 ТЕСТИРОВАНИЕ UI ФУНКЦИОНАЛЬНОСТИ TELEGRAM БОТА")
    print("=" * 60)
    
    tests = [
        test_warmup_buttons,
        test_account_selection,
        test_profile_handlers,
        test_callback_routing,
        test_keyboard_generation
    ]
    
    passed = 0
    total = len(tests)
    
    for test in tests:
        try:
            if await test():
                passed += 1
        except Exception as e:
            print(f"❌ Критическая ошибка в тесте: {e}")
    
    print("\n" + "=" * 60)
    print(f"📊 РЕЗУЛЬТАТЫ: {passed}/{total} тестов прошли успешно")
    
    if passed == total:
        print("🎉 ВСЕ ТЕСТЫ ПРОШЛИ УСПЕШНО!")
        return True
    else:
        print("⚠️ Некоторые тесты не прошли")
        return False

if __name__ == "__main__":
    asyncio.run(main()) 
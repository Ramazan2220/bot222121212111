#!/usr/bin/env python3
"""
Тест маршрутов кнопок Telegram бота
Показывает куда ведет каждая кнопка
"""

import logging
from telegram.ext import Updater, CallbackQueryHandler, ConversationHandler
from telegram_bot.bot import setup_bot
from telegram_bot.handlers.profile_handlers import get_profile_handlers

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

def analyze_bot_routes():
    """Анализирует все зарегистрированные маршруты бота"""
    print("\n" + "="*80)
    print("АНАЛИЗ МАРШРУТОВ КНОПОК TELEGRAM БОТА")
    print("="*80 + "\n")
    
    # Получаем обработчики профиля
    profile_handlers = get_profile_handlers()
    
    print("📋 ОБРАБОТЧИКИ ПРОФИЛЯ:")
    print("-" * 60)
    
    for i, handler in enumerate(profile_handlers):
        if isinstance(handler, CallbackQueryHandler):
            pattern = handler.pattern
            callback = handler.callback.__name__ if hasattr(handler.callback, '__name__') else str(handler.callback)
            print(f"{i+1}. Pattern: {pattern}")
            print(f"   → Функция: {callback}")
            print()
    
    # Анализируем ConversationHandler
    conversation_handlers = [h for h in profile_handlers if isinstance(h, ConversationHandler)]
    
    print("\n🔄 CONVERSATION HANDLERS:")
    print("-" * 60)
    
    for i, conv in enumerate(conversation_handlers):
        print(f"\nConversation {i+1}:")
        print(f"  Name: {conv.name if hasattr(conv, 'name') else 'Unnamed'}")
        
        # Entry points
        print("  Entry points:")
        for ep in conv.entry_points:
            if isinstance(ep, CallbackQueryHandler):
                pattern = ep.pattern
                callback = ep.callback.__name__ if hasattr(ep.callback, '__name__') else str(ep.callback)
                print(f"    - Pattern: {pattern} → {callback}")
        
        # States
        if conv.states:
            print("  States:")
            for state, handlers in conv.states.items():
                print(f"    State {state}:")
                for h in handlers:
                    if isinstance(h, CallbackQueryHandler):
                        pattern = h.pattern
                        callback = h.callback.__name__ if hasattr(h.callback, '__name__') else str(h.callback)
                        print(f"      - Pattern: {pattern} → {callback}")

def test_specific_callbacks():
    """Тестирует конкретные callback'ы"""
    print("\n\n🎯 ТЕСТ КОНКРЕТНЫХ CALLBACK'ОВ:")
    print("-" * 60)
    
    test_patterns = [
        "profile_select_source_all",
        "profile_select_source_folder",
        "profile_select_acc_",
        "profile_setup"
    ]
    
    profile_handlers = get_profile_handlers()
    
    for pattern in test_patterns:
        print(f"\n🔍 Ищу обработчики для: '{pattern}'")
        found = False
        
        for handler in profile_handlers:
            if isinstance(handler, CallbackQueryHandler):
                if handler.pattern and pattern in str(handler.pattern.pattern if hasattr(handler.pattern, 'pattern') else handler.pattern):
                    callback = handler.callback.__name__ if hasattr(handler.callback, '__name__') else str(handler.callback)
                    print(f"   ✅ Найден: {handler.pattern} → {callback}")
                    found = True
            
            elif isinstance(handler, ConversationHandler):
                # Проверяем entry points
                for ep in handler.entry_points:
                    if isinstance(ep, CallbackQueryHandler):
                        if ep.pattern and pattern in str(ep.pattern.pattern if hasattr(ep.pattern, 'pattern') else ep.pattern):
                            callback = ep.callback.__name__ if hasattr(ep.callback, '__name__') else str(ep.callback)
                            print(f"   ✅ Найден в entry_points: {ep.pattern} → {callback}")
                            found = True
                
                # Проверяем states
                for state, state_handlers in handler.states.items():
                    for h in state_handlers:
                        if isinstance(h, CallbackQueryHandler):
                            if h.pattern and pattern in str(h.pattern.pattern if hasattr(h.pattern, 'pattern') else h.pattern):
                                callback = h.callback.__name__ if hasattr(h.callback, '__name__') else str(h.callback)
                                print(f"   ✅ Найден в state {state}: {h.pattern} → {callback}")
                                found = True
        
        if not found:
            print(f"   ❌ Обработчик не найден!")

def check_profile_selector_state():
    """Проверяет состояние profile_selector"""
    print("\n\n🔧 ПРОВЕРКА PROFILE_SELECTOR:")
    print("-" * 60)
    
    try:
        from telegram_bot.utils.account_selection import AccountSelector
        # Проверяем состояния селектора
        print("AccountSelector states:")
        print(f"  SELECTING_SOURCE = {AccountSelector.SELECTING_SOURCE if hasattr(AccountSelector, 'SELECTING_SOURCE') else 'Not found'}")
        print(f"  SELECTING_FOLDER = {AccountSelector.SELECTING_FOLDER if hasattr(AccountSelector, 'SELECTING_FOLDER') else 'Not found'}")
        print(f"  SELECTING_ACCOUNTS = {AccountSelector.SELECTING_ACCOUNTS if hasattr(AccountSelector, 'SELECTING_ACCOUNTS') else 'Not found'}")
    except Exception as e:
        print(f"Ошибка при проверке AccountSelector: {e}")

if __name__ == "__main__":
    print("🚀 Запуск анализа маршрутов бота...")
    
    # Анализируем маршруты
    analyze_bot_routes()
    
    # Тестируем конкретные callback'ы
    test_specific_callbacks()
    
    # Проверяем состояние селектора
    check_profile_selector_state()
    
    print("\n\n✅ Анализ завершен!") 
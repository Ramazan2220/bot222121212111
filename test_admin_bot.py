#!/usr/bin/env python3
"""
Скрипт для тестирования админ бота
"""

import os
import sys
import logging

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

logger = logging.getLogger(__name__)

def test_admin_bot():
    """Тестирование админ бота"""
    
    print("🧪 ТЕСТИРОВАНИЕ АДМИН БОТА")
    print("=" * 50)
    
    try:
        # Проверяем токены
        print("🔑 Проверяем токены...")
        
        admin_token = os.getenv('ADMIN_BOT_TOKEN')
        telegram_token = os.getenv('TELEGRAM_BOT_TOKEN', '8092949155:AAEs6GSSqEU4C_3qNkskqVNAdcoAUHZi0fE')
        
        if admin_token:
            print(f"✅ ADMIN_BOT_TOKEN: {admin_token[:20]}... (отдельный токен)")
        else:
            print(f"⚠️ ADMIN_BOT_TOKEN не установлен")
            print(f"🔄 Будет использован основной токен: {telegram_token[:20]}...")
        
        # Проверяем импорты
        print("\n📦 Проверяем импорты...")
        
        from admin_bot.config.settings import ADMIN_BOT_TOKEN, MESSAGES, get_config_info
        from admin_bot.config.admin_list import is_admin, YOUR_TELEGRAM_ID
        from admin_bot.main import AdminBot
        
        print("✅ Импорты успешны")
        
        # Проверяем конфигурацию
        print("\n⚙️ Проверяем конфигурацию...")
        
        config_info = get_config_info()
        
        if config_info['admin_token_set']:
            print("✅ Отдельный токен админ бота установлен")
        else:
            print("⚠️ Используется основной токен (fallback)")
        
        if not ADMIN_BOT_TOKEN:
            print("❌ ADMIN_BOT_TOKEN не установлен")
            return False
        
        if YOUR_TELEGRAM_ID == 123456789:
            print("⚠️ ВНИМАНИЕ: Нужно установить свой Telegram ID в admin_bot/config/admin_list.py")
            print(f"   Замените YOUR_TELEGRAM_ID = 123456789 на ваш реальный ID")
            print(f"   💡 Узнать ID можно у бота @userinfobot")
            
        print("✅ Конфигурация готова")
        
        # Показываем информацию о конфигурации
        print(f"\n📊 Информация о конфигурации:")
        for key, value in config_info.items():
            print(f"   {key}: {value}")
        
        # Проверяем клавиатуры
        print("\n⌨️ Проверяем клавиатуры...")
        
        from admin_bot.keyboards.main_keyboard import get_main_keyboard
        
        # Тестируем клавиатуру для админа
        keyboard = get_main_keyboard(YOUR_TELEGRAM_ID)
        if keyboard:
            print("✅ Главная клавиатура работает")
        else:
            print("❌ Ошибка с главной клавиатурой")
            return False
        
        # Проверяем middleware
        print("\n🔒 Проверяем middleware...")
        
        from admin_bot.middleware.admin_auth import AdminAuthMiddleware
        
        auth_middleware = AdminAuthMiddleware()
        if auth_middleware:
            print("✅ Middleware авторизации готов")
        else:
            print("❌ Ошибка с middleware")
            return False
        
        # Проверяем класс админ бота
        print("\n🤖 Проверяем класс AdminBot...")
        
        admin_bot = AdminBot()
        if admin_bot:
            print("✅ Класс AdminBot создан успешно")
        else:
            print("❌ Ошибка создания AdminBot")
            return False
        
        print("\n🎉 ВСЕ ТЕСТЫ ПРОЙДЕНЫ!")
        
        # Показываем финальные инструкции
        print("\n📋 СЛЕДУЮЩИЕ ШАГИ:")
        
        if not config_info['admin_token_set']:
            print("1️⃣ Создайте отдельного админ бота: python setup_admin_bot.py")
        else:
            print("1️⃣ ✅ Отдельный токен админ бота настроен")
            
        if YOUR_TELEGRAM_ID == 123456789:
            print("2️⃣ Установите свой Telegram ID в admin_bot/config/admin_list.py")
        else:
            print("2️⃣ ✅ Telegram ID настроен")
            
        print("3️⃣ Запустите админ бота: python test_admin_bot.py --run")
        print("4️⃣ Найдите бота в Telegram и отправьте /start")
        
        return True
        
    except ImportError as e:
        print(f"❌ Ошибка импорта: {e}")
        return False
    except Exception as e:
        print(f"❌ Неожиданная ошибка: {e}")
        return False

def show_admin_setup_guide():
    """Показать руководство по настройке админов"""
    
    print("\n" + "="*60)
    print("📖 РУКОВОДСТВО ПО НАСТРОЙКЕ АДМИН БОТА")
    print("="*60)
    
    admin_token = os.getenv('ADMIN_BOT_TOKEN')
    
    if not admin_token:
        print("\n🤖 СОЗДАНИЕ ОТДЕЛЬНОГО АДМИН БОТА:")
        print("   • Используйте скрипт: python setup_admin_bot.py")
        print("   • Или создайте вручную через @BotFather")
    
    print("\n1️⃣ ПОЛУЧЕНИЕ ВАШЕГО TELEGRAM ID:")
    print("   • Напишите боту @userinfobot")
    print("   • Он покажет ваш ID (например: 987654321)")
    
    print("\n2️⃣ НАСТРОЙКА АДМИНОВ:")
    print("   • Откройте файл: admin_bot/config/admin_list.py")
    print("   • Найдите строку: YOUR_TELEGRAM_ID = 123456789")
    print("   • Замените 123456789 на ваш реальный ID")
    
    print("\n3️⃣ ДОБАВЛЕНИЕ ДРУГИХ АДМИНОВ:")
    print("   • SUPER_ADMIN_IDS - супер-админы (полные права)")
    print("   • ADMIN_IDS - обычные админы (управление пользователями)")
    print("   • MODERATOR_IDS - модераторы (только просмотр)")
    
    print("\n4️⃣ ЗАПУСК АДМИН БОТА:")
    if admin_token:
        print("   • Выполните: python admin_bot/main.py")
        print("   • Или: python test_admin_bot.py --run")
    else:
        print("   • Сначала настройте токен: python setup_admin_bot.py")
        print("   • Затем: python test_admin_bot.py --run")
    
    print("\n5️⃣ ИСПОЛЬЗОВАНИЕ:")
    print("   • Найдите бота в Telegram")
    print("   • Отправьте команду /start")
    print("   • Используйте кнопки для навигации")
    
    print("\n" + "="*60)

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--run":
        # Запускаем админ бота
        print("🚀 Запуск админ бота...")
        try:
            from admin_bot.main import AdminBot
            admin_bot = AdminBot()
            admin_bot.run()
        except KeyboardInterrupt:
            print("\n👋 Админ бот остановлен")
        except Exception as e:
            print(f"❌ Ошибка запуска: {e}")
    else:
        # Запускаем тесты
        success = test_admin_bot()
        show_admin_setup_guide()
        
        if success:
            print(f"\n✅ Тестирование завершено успешно!")
            
            # Проверяем, нужна ли настройка токена
            if not os.getenv('ADMIN_BOT_TOKEN'):
                print(f"💡 Для настройки токена: python setup_admin_bot.py")
            else:
                print(f"💡 Для запуска используйте: python {sys.argv[0]} --run")
        else:
            print(f"\n❌ Тестирование не пройдено!")
            sys.exit(1) 
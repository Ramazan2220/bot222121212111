#!/usr/bin/env python3
"""
УПРОЩЕННЫЙ АДМИН БОТ ДЛЯ ТЕСТИРОВАНИЯ СИНХРОНИЗАЦИИ
Без SQLAlchemy - только функции управления доступом
"""

import sys
import os
import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Updater, CommandHandler, CallbackQueryHandler, CallbackContext

# Добавляем пути
sys.path.insert(0, '.')
sys.path.insert(0, './utils')

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Токен бота
ADMIN_BOT_TOKEN = '8092949155:AAEs6GSSqEU4C_3qNkskqVNAdcoAUHZi0fE'

# Список админов
ADMIN_IDS = [6499246016]

def is_admin(user_id):
    """Проверка прав администратора"""
    return user_id in ADMIN_IDS

def start(update: Update, context: CallbackContext):
    """Команда /start"""
    user_id = update.effective_user.id
    
    if not is_admin(user_id):
        update.message.reply_text("❌ Доступ запрещен!")
        return
    
    keyboard = [
        [InlineKeyboardButton("👥 Добавить пользователя", callback_data="add_user")],
        [InlineKeyboardButton("🗑️ Удалить пользователя", callback_data="delete_user")],
        [InlineKeyboardButton("📋 Проверить доступ", callback_data="check_access")],
        [InlineKeyboardButton("🔄 Тест синхронизации", callback_data="test_sync")]
    ]
    
    text = """🤖 **УПРОЩЕННЫЙ АДМИН БОТ**

🎯 **Цель:** Тестирование новой системы синхронизации

✅ **Доступные функции:**
• Добавление пользователей
• Удаление пользователей  
• Проверка доступа
• Тест синхронизации

🚀 **Выберите действие:**"""

    reply_markup = InlineKeyboardMarkup(keyboard)
    update.message.reply_text(text, reply_markup=reply_markup, parse_mode='Markdown')

def button_handler(update: Update, context: CallbackContext):
    """Обработчик кнопок"""
    query = update.callback_query
    user_id = update.effective_user.id
    
    if not is_admin(user_id):
        query.answer("❌ Доступ запрещен!")
        return
    
    data = query.data
    
    if data == "add_user":
        handle_add_user(query, context)
    elif data == "delete_user":
        handle_delete_user(query, context)
    elif data == "check_access":
        handle_check_access(query, context)
    elif data == "test_sync":
        handle_test_sync(query, context)
    elif data.startswith("add_user_"):
        execute_add_user(query, context)
    elif data.startswith("delete_user_"):
        execute_delete_user(query, context)

def handle_add_user(query, context):
    """Добавление пользователя"""
    keyboard = [
        [InlineKeyboardButton("➕ Добавить ID: 6626270112", callback_data="add_user_6626270112")],
        [InlineKeyboardButton("➕ Добавить ID: 999999999", callback_data="add_user_999999999")],
        [InlineKeyboardButton("🔙 Назад", callback_data="back")]
    ]
    
    text = """➕ **ДОБАВЛЕНИЕ ПОЛЬЗОВАТЕЛЯ**

🎯 Выберите тестового пользователя для добавления:"""

    reply_markup = InlineKeyboardMarkup(keyboard)
    query.edit_message_text(text, reply_markup=reply_markup, parse_mode='Markdown')

def handle_delete_user(query, context):
    """Удаление пользователя"""
    keyboard = [
        [InlineKeyboardButton("🗑️ Удалить ID: 6626270112", callback_data="delete_user_6626270112")],
        [InlineKeyboardButton("🗑️ Удалить ID: 999999999", callback_data="delete_user_999999999")],
        [InlineKeyboardButton("🔙 Назад", callback_data="back")]
    ]
    
    text = """🗑️ **УДАЛЕНИЕ ПОЛЬЗОВАТЕЛЯ**

🎯 Выберите пользователя для удаления:"""

    reply_markup = InlineKeyboardMarkup(keyboard)
    query.edit_message_text(text, reply_markup=reply_markup, parse_mode='Markdown')

def execute_add_user(query, context):
    """Выполнение добавления пользователя"""
    user_id = int(query.data.split('_')[-1])
    
    try:
        # Импортируем новую систему
        from utils.access_manager import add_user_access, has_access
        
        # Добавляем пользователя
        logger.info(f"🟢 Админ добавляет пользователя {user_id}")
        result = add_user_access(user_id)
        
        # Проверяем сразу
        access_check = has_access(user_id)
        
        if result and access_check:
            text = f"""✅ **ПОЛЬЗОВАТЕЛЬ ДОБАВЛЕН**

👤 **ID:** `{user_id}`
🔄 **Результат:** {result}
✅ **Проверка доступа:** {access_check}
🕐 **Время:** Мгновенно

🎉 **СИНХРОНИЗАЦИЯ РАБОТАЕТ!**"""
            status = "✅"
        else:
            text = f"""❌ **ОШИБКА ДОБАВЛЕНИЯ**

👤 **ID:** `{user_id}`
🔄 **Результат:** {result}
❌ **Проверка доступа:** {access_check}

⚠️ **ПРОБЛЕМА С СИНХРОНИЗАЦИЕЙ!**"""
            status = "❌"
            
        logger.info(f"{status} Добавление пользователя {user_id}: result={result}, access={access_check}")
        
    except Exception as e:
        text = f"""💥 **КРИТИЧЕСКАЯ ОШИБКА**

👤 **ID:** `{user_id}`
❌ **Ошибка:** `{str(e)}`

🔧 **Требуется диагностика системы!**"""
        logger.error(f"Ошибка добавления пользователя {user_id}: {e}")
    
    keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="back")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    query.edit_message_text(text, reply_markup=reply_markup, parse_mode='Markdown')

def execute_delete_user(query, context):
    """Выполнение удаления пользователя"""
    user_id = int(query.data.split('_')[-1])
    
    try:
        # Импортируем новую систему
        from utils.access_manager import delete_user_completely, has_access
        
        # Удаляем пользователя
        logger.info(f"🔴 Админ удаляет пользователя {user_id}")
        result = delete_user_completely(user_id)
        
        # Проверяем сразу
        access_check = has_access(user_id)
        
        if result and not access_check:
            text = f"""✅ **ПОЛЬЗОВАТЕЛЬ УДАЛЕН**

👤 **ID:** `{user_id}`
🔄 **Результат:** {result}
❌ **Проверка доступа:** {access_check}
🕐 **Время:** Мгновенно

🎉 **СИНХРОНИЗАЦИЯ РАБОТАЕТ!**"""
            status = "✅"
        else:
            text = f"""❌ **ОШИБКА УДАЛЕНИЯ**

👤 **ID:** `{user_id}`
🔄 **Результат:** {result}
⚠️ **Проверка доступа:** {access_check}

⚠️ **ПРОБЛЕМА С СИНХРОНИЗАЦИЕЙ!**"""
            status = "❌"
            
        logger.info(f"{status} Удаление пользователя {user_id}: result={result}, access={access_check}")
        
    except Exception as e:
        text = f"""💥 **КРИТИЧЕСКАЯ ОШИБКА**

👤 **ID:** `{user_id}`
❌ **Ошибка:** `{str(e)}`

🔧 **Требуется диагностика системы!**"""
        logger.error(f"Ошибка удаления пользователя {user_id}: {e}")
    
    keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="back")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    query.edit_message_text(text, reply_markup=reply_markup, parse_mode='Markdown')

def handle_check_access(query, context):
    """Проверка доступа пользователей"""
    try:
        from utils.access_manager import has_access
        
        # Тестовые пользователи
        test_users = [6626270112, 999999999, 6499246016]
        
        text = "📋 **ПРОВЕРКА ДОСТУПА**\n\n"
        
        for user_id in test_users:
            access = has_access(user_id)
            status = "🟢 ЕСТЬ" if access else "🔴 НЕТ"
            text += f"👤 `{user_id}`: {status}\n"
            
        text += f"\n🕐 **Время проверки:** Мгновенно"
        
    except Exception as e:
        text = f"""💥 **ОШИБКА ПРОВЕРКИ**

❌ **Ошибка:** `{str(e)}`"""
        logger.error(f"Ошибка проверки доступа: {e}")
    
    keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="back")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    query.edit_message_text(text, reply_markup=reply_markup, parse_mode='Markdown')

def handle_test_sync(query, context):
    """Тест синхронизации"""
    try:
        from utils.access_manager import add_user_access, delete_user_completely, has_access
        
        test_id = 888777666
        text = "🔄 **ТЕСТ СИНХРОНИЗАЦИИ**\n\n"
        
        # 1. Проверяем начальное состояние
        initial = has_access(test_id)
        text += f"1️⃣ Начальное состояние: {'🟢' if initial else '🔴'}\n"
        
        # 2. Добавляем
        add_result = add_user_access(test_id)
        after_add = has_access(test_id)
        text += f"2️⃣ После добавления: {'🟢' if after_add else '🔴'}\n"
        
        # 3. Удаляем
        delete_result = delete_user_completely(test_id)
        after_delete = has_access(test_id)
        text += f"3️⃣ После удаления: {'🟢' if after_delete else '🔴'}\n"
        
        # Результат
        if not initial and after_add and not after_delete:
            text += f"\n🎉 **ТЕСТ ПРОЙДЕН!**\n✅ Синхронизация работает идеально!"
        else:
            text += f"\n❌ **ТЕСТ ПРОВАЛЕН!**\n⚠️ Проблемы с синхронизацией"
            
    except Exception as e:
        text = f"""💥 **ОШИБКА ТЕСТА**

❌ **Ошибка:** `{str(e)}`"""
        logger.error(f"Ошибка теста синхронизации: {e}")
    
    keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="back")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    query.edit_message_text(text, reply_markup=reply_markup, parse_mode='Markdown')

def main():
    """Запуск бота"""
    try:
        print("🚀 Запуск упрощенного админ бота...")
        print(f"🔑 Токен: {ADMIN_BOT_TOKEN[:10]}...")
        print(f"👥 Админы: {ADMIN_IDS}")
        
        # Тест новой системы
        from utils.access_manager import has_access
        print("✅ Новая система синхронизации загружена")
        
        updater = Updater(token=ADMIN_BOT_TOKEN, use_context=True)
        dp = updater.dispatcher
        
        # Обработчики
        dp.add_handler(CommandHandler("start", start))
        dp.add_handler(CallbackQueryHandler(button_handler))
        
        # Запуск
        updater.start_polling()
        print("🟢 Упрощенный админ бот запущен!")
        print("📱 Используйте /start для начала работы")
        
        updater.idle()
        
    except Exception as e:
        print(f"💥 Критическая ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main() 
"""
УПРОЩЕННЫЙ АДМИН БОТ ДЛЯ ТЕСТИРОВАНИЯ СИНХРОНИЗАЦИИ
Без SQLAlchemy - только функции управления доступом
"""

import sys
import os
import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Updater, CommandHandler, CallbackQueryHandler, CallbackContext

# Добавляем пути
sys.path.insert(0, '.')
sys.path.insert(0, './utils')

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Токен бота
ADMIN_BOT_TOKEN = '8092949155:AAEs6GSSqEU4C_3qNkskqVNAdcoAUHZi0fE'

# Список админов
ADMIN_IDS = [6499246016]

def is_admin(user_id):
    """Проверка прав администратора"""
    return user_id in ADMIN_IDS

def start(update: Update, context: CallbackContext):
    """Команда /start"""
    user_id = update.effective_user.id
    
    if not is_admin(user_id):
        update.message.reply_text("❌ Доступ запрещен!")
        return
    
    keyboard = [
        [InlineKeyboardButton("👥 Добавить пользователя", callback_data="add_user")],
        [InlineKeyboardButton("🗑️ Удалить пользователя", callback_data="delete_user")],
        [InlineKeyboardButton("📋 Проверить доступ", callback_data="check_access")],
        [InlineKeyboardButton("🔄 Тест синхронизации", callback_data="test_sync")]
    ]
    
    text = """🤖 **УПРОЩЕННЫЙ АДМИН БОТ**

🎯 **Цель:** Тестирование новой системы синхронизации

✅ **Доступные функции:**
• Добавление пользователей
• Удаление пользователей  
• Проверка доступа
• Тест синхронизации

🚀 **Выберите действие:**"""

    reply_markup = InlineKeyboardMarkup(keyboard)
    update.message.reply_text(text, reply_markup=reply_markup, parse_mode='Markdown')

def button_handler(update: Update, context: CallbackContext):
    """Обработчик кнопок"""
    query = update.callback_query
    user_id = update.effective_user.id
    
    if not is_admin(user_id):
        query.answer("❌ Доступ запрещен!")
        return
    
    data = query.data
    
    if data == "add_user":
        handle_add_user(query, context)
    elif data == "delete_user":
        handle_delete_user(query, context)
    elif data == "check_access":
        handle_check_access(query, context)
    elif data == "test_sync":
        handle_test_sync(query, context)
    elif data.startswith("add_user_"):
        execute_add_user(query, context)
    elif data.startswith("delete_user_"):
        execute_delete_user(query, context)

def handle_add_user(query, context):
    """Добавление пользователя"""
    keyboard = [
        [InlineKeyboardButton("➕ Добавить ID: 6626270112", callback_data="add_user_6626270112")],
        [InlineKeyboardButton("➕ Добавить ID: 999999999", callback_data="add_user_999999999")],
        [InlineKeyboardButton("🔙 Назад", callback_data="back")]
    ]
    
    text = """➕ **ДОБАВЛЕНИЕ ПОЛЬЗОВАТЕЛЯ**

🎯 Выберите тестового пользователя для добавления:"""

    reply_markup = InlineKeyboardMarkup(keyboard)
    query.edit_message_text(text, reply_markup=reply_markup, parse_mode='Markdown')

def handle_delete_user(query, context):
    """Удаление пользователя"""
    keyboard = [
        [InlineKeyboardButton("🗑️ Удалить ID: 6626270112", callback_data="delete_user_6626270112")],
        [InlineKeyboardButton("🗑️ Удалить ID: 999999999", callback_data="delete_user_999999999")],
        [InlineKeyboardButton("🔙 Назад", callback_data="back")]
    ]
    
    text = """🗑️ **УДАЛЕНИЕ ПОЛЬЗОВАТЕЛЯ**

🎯 Выберите пользователя для удаления:"""

    reply_markup = InlineKeyboardMarkup(keyboard)
    query.edit_message_text(text, reply_markup=reply_markup, parse_mode='Markdown')

def execute_add_user(query, context):
    """Выполнение добавления пользователя"""
    user_id = int(query.data.split('_')[-1])
    
    try:
        # Импортируем новую систему
        from utils.access_manager import add_user_access, has_access
        
        # Добавляем пользователя
        logger.info(f"🟢 Админ добавляет пользователя {user_id}")
        result = add_user_access(user_id)
        
        # Проверяем сразу
        access_check = has_access(user_id)
        
        if result and access_check:
            text = f"""✅ **ПОЛЬЗОВАТЕЛЬ ДОБАВЛЕН**

👤 **ID:** `{user_id}`
🔄 **Результат:** {result}
✅ **Проверка доступа:** {access_check}
🕐 **Время:** Мгновенно

🎉 **СИНХРОНИЗАЦИЯ РАБОТАЕТ!**"""
            status = "✅"
        else:
            text = f"""❌ **ОШИБКА ДОБАВЛЕНИЯ**

👤 **ID:** `{user_id}`
🔄 **Результат:** {result}
❌ **Проверка доступа:** {access_check}

⚠️ **ПРОБЛЕМА С СИНХРОНИЗАЦИЕЙ!**"""
            status = "❌"
            
        logger.info(f"{status} Добавление пользователя {user_id}: result={result}, access={access_check}")
        
    except Exception as e:
        text = f"""💥 **КРИТИЧЕСКАЯ ОШИБКА**

👤 **ID:** `{user_id}`
❌ **Ошибка:** `{str(e)}`

🔧 **Требуется диагностика системы!**"""
        logger.error(f"Ошибка добавления пользователя {user_id}: {e}")
    
    keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="back")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    query.edit_message_text(text, reply_markup=reply_markup, parse_mode='Markdown')

def execute_delete_user(query, context):
    """Выполнение удаления пользователя"""
    user_id = int(query.data.split('_')[-1])
    
    try:
        # Импортируем новую систему
        from utils.access_manager import delete_user_completely, has_access
        
        # Удаляем пользователя
        logger.info(f"🔴 Админ удаляет пользователя {user_id}")
        result = delete_user_completely(user_id)
        
        # Проверяем сразу
        access_check = has_access(user_id)
        
        if result and not access_check:
            text = f"""✅ **ПОЛЬЗОВАТЕЛЬ УДАЛЕН**

👤 **ID:** `{user_id}`
🔄 **Результат:** {result}
❌ **Проверка доступа:** {access_check}
🕐 **Время:** Мгновенно

🎉 **СИНХРОНИЗАЦИЯ РАБОТАЕТ!**"""
            status = "✅"
        else:
            text = f"""❌ **ОШИБКА УДАЛЕНИЯ**

👤 **ID:** `{user_id}`
🔄 **Результат:** {result}
⚠️ **Проверка доступа:** {access_check}

⚠️ **ПРОБЛЕМА С СИНХРОНИЗАЦИЕЙ!**"""
            status = "❌"
            
        logger.info(f"{status} Удаление пользователя {user_id}: result={result}, access={access_check}")
        
    except Exception as e:
        text = f"""💥 **КРИТИЧЕСКАЯ ОШИБКА**

👤 **ID:** `{user_id}`
❌ **Ошибка:** `{str(e)}`

🔧 **Требуется диагностика системы!**"""
        logger.error(f"Ошибка удаления пользователя {user_id}: {e}")
    
    keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="back")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    query.edit_message_text(text, reply_markup=reply_markup, parse_mode='Markdown')

def handle_check_access(query, context):
    """Проверка доступа пользователей"""
    try:
        from utils.access_manager import has_access
        
        # Тестовые пользователи
        test_users = [6626270112, 999999999, 6499246016]
        
        text = "📋 **ПРОВЕРКА ДОСТУПА**\n\n"
        
        for user_id in test_users:
            access = has_access(user_id)
            status = "🟢 ЕСТЬ" if access else "🔴 НЕТ"
            text += f"👤 `{user_id}`: {status}\n"
            
        text += f"\n🕐 **Время проверки:** Мгновенно"
        
    except Exception as e:
        text = f"""💥 **ОШИБКА ПРОВЕРКИ**

❌ **Ошибка:** `{str(e)}`"""
        logger.error(f"Ошибка проверки доступа: {e}")
    
    keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="back")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    query.edit_message_text(text, reply_markup=reply_markup, parse_mode='Markdown')

def handle_test_sync(query, context):
    """Тест синхронизации"""
    try:
        from utils.access_manager import add_user_access, delete_user_completely, has_access
        
        test_id = 888777666
        text = "🔄 **ТЕСТ СИНХРОНИЗАЦИИ**\n\n"
        
        # 1. Проверяем начальное состояние
        initial = has_access(test_id)
        text += f"1️⃣ Начальное состояние: {'🟢' if initial else '🔴'}\n"
        
        # 2. Добавляем
        add_result = add_user_access(test_id)
        after_add = has_access(test_id)
        text += f"2️⃣ После добавления: {'🟢' if after_add else '🔴'}\n"
        
        # 3. Удаляем
        delete_result = delete_user_completely(test_id)
        after_delete = has_access(test_id)
        text += f"3️⃣ После удаления: {'🟢' if after_delete else '🔴'}\n"
        
        # Результат
        if not initial and after_add and not after_delete:
            text += f"\n🎉 **ТЕСТ ПРОЙДЕН!**\n✅ Синхронизация работает идеально!"
        else:
            text += f"\n❌ **ТЕСТ ПРОВАЛЕН!**\n⚠️ Проблемы с синхронизацией"
            
    except Exception as e:
        text = f"""💥 **ОШИБКА ТЕСТА**

❌ **Ошибка:** `{str(e)}`"""
        logger.error(f"Ошибка теста синхронизации: {e}")
    
    keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="back")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    query.edit_message_text(text, reply_markup=reply_markup, parse_mode='Markdown')

def main():
    """Запуск бота"""
    try:
        print("🚀 Запуск упрощенного админ бота...")
        print(f"🔑 Токен: {ADMIN_BOT_TOKEN[:10]}...")
        print(f"👥 Админы: {ADMIN_IDS}")
        
        # Тест новой системы
        from utils.access_manager import has_access
        print("✅ Новая система синхронизации загружена")
        
        updater = Updater(token=ADMIN_BOT_TOKEN, use_context=True)
        dp = updater.dispatcher
        
        # Обработчики
        dp.add_handler(CommandHandler("start", start))
        dp.add_handler(CallbackQueryHandler(button_handler))
        
        # Запуск
        updater.start_polling()
        print("🟢 Упрощенный админ бот запущен!")
        print("📱 Используйте /start для начала работы")
        
        updater.idle()
        
    except Exception as e:
        print(f"💥 Критическая ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main() 
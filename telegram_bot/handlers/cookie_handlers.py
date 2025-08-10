import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import CallbackContext, ConversationHandler
from telegram.filters import Filters
from telegram.ext import MessageHandler, CommandHandler

from config import ADMIN_USER_IDS
from utils.access_manager import has_access
from database.db_manager import add_instagram_account
from telegram.keyboards import get_accounts_menu_keyboard
from instagram.client import Client

logger = logging.getLogger(__name__)

# Состояния для загрузки cookies
COOKIES_FILE, COOKIES_TEXT = range(2)

def handle_cookies_menu(update: Update, context: CallbackContext):
    """Обработчик меню работы с cookies"""
    user_id = update.effective_user.id
    
    if not has_access(user_id):
        update.message.reply_text("❌ У вас нет доступа к этому боту.")
        return
    
    text = """🍪 **УПРАВЛЕНИЕ COOKIES**

📋 **Доступные действия:**

1️⃣ **Загрузить из файла** - загрузка .json файла с cookies
2️⃣ **Ввести текстом** - ввод cookies в текстовом формате  
3️⃣ **Экспорт cookies** - выгрузка существующих cookies

Выберите способ работы с cookies:"""
    
    keyboard = [
        [InlineKeyboardButton("📁 Загрузить файл", callback_data="cookies_upload_file")],
        [InlineKeyboardButton("📝 Ввести текстом", callback_data="cookies_input_text")],
        [InlineKeyboardButton("📤 Экспорт cookies", callback_data="cookies_export")],
        [InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")]
    ]
    
    query = update.callback_query if update.callback_query else None
    if query:
        query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
    else:
        update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

def cookies_upload_file(update: Update, context: CallbackContext):
    """Начало загрузки cookies из файла"""
    query = update.callback_query
    
    text = """📁 **ЗАГРУЗКА COOKIES ИЗ ФАЙЛА**

📋 **Инструкция:**
1. Подготовьте .json файл с cookies
2. Отправьте файл в следующем сообщении
3. Бот автоматически обработает cookies

⚠️ **Формат файла:** JSON с массивом объектов cookies

Отправьте файл с cookies:"""
    
    query.edit_message_text(text, parse_mode='Markdown')
    return COOKIES_FILE

def cookies_input_text(update: Update, context: CallbackContext):
    """Начало ввода cookies текстом"""
    query = update.callback_query
    
    text = """📝 **ВВОД COOKIES ТЕКСТОМ**

📋 **Инструкция:**
1. Скопируйте cookies в JSON формате
2. Вставьте в следующее сообщение
3. Бот обработает данные

⚠️ **Пример формата:**
```json
[
  {"name": "sessionid", "value": "your_session"},
  {"name": "csrftoken", "value": "your_token"}
]
```

Отправьте cookies в текстовом формате:"""
    
    query.edit_message_text(text, parse_mode='Markdown')
    return COOKIES_TEXT

def process_cookies_file(update: Update, context: CallbackContext):
    """Обработка загруженного файла с cookies"""
    try:
        document = update.message.document
        if not document.file_name.endswith('.json'):
            update.message.reply_text("❌ Пожалуйста, загрузите файл в формате .json")
            return ConversationHandler.END
        
        # Загружаем файл
        file = context.bot.get_file(document.file_id)
        file_content = file.download_as_bytearray()
        
        # Парсим JSON
        import json
        cookies_data = json.loads(file_content.decode('utf-8'))
        
        # Обрабатываем cookies
        result = process_cookies_data(update.effective_user.id, cookies_data)
        
        if result['success']:
            update.message.reply_text(
                f"✅ **Cookies успешно загружены!**\n\n"
                f"📊 **Статистика:**\n"
                f"• Обработано: {result['processed']} cookies\n"
                f"• Аккаунтов: {result['accounts']} найдено\n\n"
                f"🎯 Cookies готовы к использованию!",
                parse_mode='Markdown'
            )
        else:
            update.message.reply_text(
                f"❌ **Ошибка обработки cookies:**\n\n{result['error']}",
                parse_mode='Markdown'
            )
        
    except Exception as e:
        logger.error(f"Ошибка обработки файла cookies: {e}")
        update.message.reply_text("❌ Ошибка при обработке файла. Проверьте формат.")
    
    return ConversationHandler.END

def process_cookies_text(update: Update, context: CallbackContext):
    """Обработка cookies введенных текстом"""
    try:
        import json
        cookies_text = update.message.text
        cookies_data = json.loads(cookies_text)
        
        # Обрабатываем cookies
        result = process_cookies_data(update.effective_user.id, cookies_data)
        
        if result['success']:
            update.message.reply_text(
                f"✅ **Cookies успешно обработаны!**\n\n"
                f"📊 **Статистика:**\n"
                f"• Обработано: {result['processed']} cookies\n"
                f"• Аккаунтов: {result['accounts']} найдено\n\n"
                f"🎯 Cookies готовы к использованию!",
                parse_mode='Markdown'
            )
        else:
            update.message.reply_text(
                f"❌ **Ошибка обработки cookies:**\n\n{result['error']}",
                parse_mode='Markdown'
            )
        
    except json.JSONDecodeError:
        update.message.reply_text("❌ Неверный формат JSON. Проверьте синтаксис.")
    except Exception as e:
        logger.error(f"Ошибка обработки текста cookies: {e}")
        update.message.reply_text("❌ Ошибка при обработке данных.")
    
    return ConversationHandler.END

def process_cookies_data(user_id: int, cookies_data: list) -> dict:
    """Обработка данных cookies"""
    try:
        processed_count = 0
        accounts_found = 0
        
        # Здесь логика обработки cookies
        # Пока заглушка
        processed_count = len(cookies_data) if isinstance(cookies_data, list) else 0
        accounts_found = 1 if processed_count > 0 else 0
        
        return {
            'success': True,
            'processed': processed_count,
            'accounts': accounts_found
        }
        
    except Exception as e:
        return {
            'success': False,
            'error': str(e)
        }

def cookies_export(update: Update, context: CallbackContext):
    """Экспорт существующих cookies"""
    query = update.callback_query
    user_id = update.effective_user.id
    
    try:
        # Здесь логика экспорта cookies
        # Пока заглушка
        text = """📤 **ЭКСПОРТ COOKIES**

🔍 **Поиск cookies...**

❌ Cookies не найдены в системе.

💡 **Рекомендации:**
• Сначала загрузите cookies
• Убедитесь что аккаунты добавлены
• Проверьте подключение к Instagram"""
        
        query.edit_message_text(
            text,
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 Назад", callback_data="cookies_menu")
            ]]),
            parse_mode='Markdown'
        )
        
    except Exception as e:
        logger.error(f"Ошибка экспорта cookies: {e}")
        query.edit_message_text("❌ Ошибка при экспорте cookies.")

def cancel_cookies(update: Update, context: CallbackContext):
    """Отмена операции с cookies"""
    update.message.reply_text(
        "❌ Операция отменена.",
        reply_markup=get_accounts_menu_keyboard()
    )
    return ConversationHandler.END

# Создаем ConversationHandler для cookies
cookies_conversation = ConversationHandler(
    entry_points=[],
    states={
        COOKIES_FILE: [MessageHandler(Filters.document, process_cookies_file)],
        COOKIES_TEXT: [MessageHandler(Filters.text & ~Filters.command, process_cookies_text)]
    },
    fallbacks=[CommandHandler('cancel', cancel_cookies)]
)
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Обработчики Telegram бота для настройки прогрева по интересам
"""

import logging
import json
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    CallbackContext, ConversationHandler, CallbackQueryHandler, 
    MessageHandler, Filters
)
from database.db_manager import get_instagram_accounts
from utils.interest_based_warmup import InterestCategory

logger = logging.getLogger(__name__)

# Состояния конversation handler
INTEREST_SETUP, CUSTOM_HASHTAGS, CUSTOM_ACCOUNTS = range(3)

# Словарь для хранения временных настроек пользователей
user_interest_settings = {}


def show_interest_warmup_menu(update: Update, context: CallbackContext) -> None:
    """Показать главное меню прогрева по интересам"""
    query = update.callback_query
    query.answer()
    
    keyboard = [
        [InlineKeyboardButton("🎯 Настроить интересы", callback_data="setup_interests")],
        [InlineKeyboardButton("📊 Шаблоны интересов", callback_data="interest_templates")],
        [InlineKeyboardButton("⚙️ Текущие настройки", callback_data="current_interest_settings")],
        [InlineKeyboardButton("🚀 Тест прогрева по интересам", callback_data="test_interest_warmup")],
        [InlineKeyboardButton("🔙 Назад к прогреву", callback_data="warmup_menu")]
    ]
    
    text = (
        "🎯 ПРОГРЕВ ПО ИНТЕРЕСАМ\n\n"
        "Умная система прогрева, которая:\n"
        "• 🔍 Изучает контент по вашим интересам\n"
        "• 👥 Взаимодействует с целевой аудиторией\n"
        "• 🎯 Формирует алгоритмические предпочтения\n"
        "• 📈 Улучшает органический охват\n\n"
        "Выберите действие:"
    )
    
    query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)


def show_interest_templates(update: Update, context: CallbackContext) -> None:
    """Показать готовые шаблоны интересов"""
    query = update.callback_query
    query.answer()
    
    templates = {
        'fitness': {
            'name': '💪 Фитнес и спорт',
            'description': 'Тренировки, мотивация, здоровый образ жизни',
            'interests': ['fitness', 'health']
        },
        'food': {
            'name': '🍕 Еда и кулинария',
            'description': 'Рецепты, рестораны, гастрономия',
            'interests': ['food']
        },
        'travel': {
            'name': '✈️ Путешествия',
            'description': 'Туризм, природа, приключения',
            'interests': ['travel', 'photography']
        },
        'business': {
            'name': '💼 Бизнес',
            'description': 'Предпринимательство, мотивация, финансы',
            'interests': ['business', 'finance']
        },
        'technology': {
            'name': '💻 Технологии',
            'description': 'IT, гаджеты, инновации',
            'interests': ['technology']
        },
        'lifestyle': {
            'name': '🌟 Лайфстайл',
            'description': 'Мода, красота, развлечения',
            'interests': ['lifestyle', 'fashion', 'beauty']
        }
    }
    
    keyboard = []
    for template_key, template_data in templates.items():
        keyboard.append([InlineKeyboardButton(
            template_data['name'], 
            callback_data=f"select_template_{template_key}"
        )])
    
    keyboard.append([InlineKeyboardButton("🛠 Настроить вручную", callback_data="manual_interests")])
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="interest_warmup_menu")])
    
    text = (
        "📊 ШАБЛОНЫ ИНТЕРЕСОВ\n\n"
        "Выберите готовый шаблон или настройте вручную:\n\n"
    )
    
    for template_data in templates.values():
        text += f"{template_data['name']}\n{template_data['description']}\n\n"
    
    query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)


def select_interest_template(update: Update, context: CallbackContext) -> None:
    """Выбор шаблона интересов"""
    query = update.callback_query
    query.answer()
    
    template_name = query.data.replace('select_template_', '')
    user_id = query.from_user.id
    
    # Шаблоны интересов
    templates = {
        'fitness': {
            'primary_interests': ['fitness'],
            'secondary_interests': ['health', 'motivation'],
            'custom_hashtags': ['gym', 'workout', 'training', 'bodybuilding'],
            'custom_accounts': ['therock', 'nike', 'underarmour']
        },
        'food': {
            'primary_interests': ['food'],
            'secondary_interests': ['lifestyle'],
            'custom_hashtags': ['recipe', 'cooking', 'chef', 'delicious'],
            'custom_accounts': ['gordongram', 'jamieoliver', 'buzzfeedtasty']
        },
        'travel': {
            'primary_interests': ['travel'],
            'secondary_interests': ['photography', 'lifestyle'],
            'custom_hashtags': ['wanderlust', 'adventure', 'explore', 'nature'],
            'custom_accounts': ['natgeo', 'beautifuldestinations', 'lonelyplanet']
        },
        'business': {
            'primary_interests': ['business'],
            'secondary_interests': ['finance', 'motivation'],
            'custom_hashtags': ['entrepreneur', 'startup', 'success', 'leadership'],
            'custom_accounts': ['garyvee', 'forbes', 'entrepreneur']
        },
        'technology': {
            'primary_interests': ['technology'],
            'secondary_interests': ['business'],
            'custom_hashtags': ['innovation', 'ai', 'startup', 'coding'],
            'custom_accounts': ['elonmusk', 'apple', 'techcrunch']
        },
        'lifestyle': {
            'primary_interests': ['lifestyle'],
            'secondary_interests': ['fashion', 'beauty'],
            'custom_hashtags': ['style', 'trend', 'inspiration', 'mood'],
            'custom_accounts': ['voguemagazine', 'harpersbazaar']
        }
    }
    
    if template_name in templates:
        # Сохраняем настройки шаблона
        user_interest_settings[user_id] = templates[template_name].copy()
        
        # Показываем предварительный просмотр
        settings = user_interest_settings[user_id]
        
        text = (
            f"✅ Выбран шаблон: {template_name.upper()}\n\n"
            f"🎯 Основные интересы: {', '.join(settings['primary_interests'])}\n"
            f"🔸 Дополнительные: {', '.join(settings['secondary_interests'])}\n"
            f"🏷 Хештеги: {', '.join(settings['custom_hashtags'][:5])}{'...' if len(settings['custom_hashtags']) > 5 else ''}\n"
            f"👤 Аккаунты: {', '.join(settings['custom_accounts'])}\n\n"
            "Сохранить эти настройки?"
        )
        
        keyboard = [
            [InlineKeyboardButton("✅ Сохранить", callback_data="save_interest_settings")],
            [InlineKeyboardButton("✏️ Редактировать", callback_data="edit_interest_settings")],
            [InlineKeyboardButton("🔙 Выбрать другой", callback_data="interest_templates")]
        ]
        
        query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)


def show_manual_interests_setup(update: Update, context: CallbackContext) -> int:
    """Показать меню ручной настройки интересов"""
    query = update.callback_query
    query.answer()
    
    # Список всех доступных интересов
    all_interests = [category.value for category in InterestCategory]
    
    # Создаем клавиатуру с интересами (по 2 в ряд)
    keyboard = []
    for i in range(0, len(all_interests), 2):
        row = []
        for j in range(i, min(i + 2, len(all_interests))):
            interest = all_interests[j]
            row.append(InlineKeyboardButton(
                f"🔸 {interest.replace('_', ' ').title()}", 
                callback_data=f"toggle_interest_{interest}"
            ))
        keyboard.append(row)
    
    keyboard.append([InlineKeyboardButton("✅ Далее", callback_data="interests_next_step")])
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="interest_templates")])
    
    # Инициализируем настройки пользователя
    user_id = query.from_user.id
    if user_id not in user_interest_settings:
        user_interest_settings[user_id] = {
            'primary_interests': [],
            'secondary_interests': [],
            'custom_hashtags': [],
            'custom_accounts': []
        }
    
    text = (
        "🛠 РУЧНАЯ НАСТРОЙКА ИНТЕРЕСОВ\n\n"
        "Выберите 2-3 основных интереса для прогрева:\n\n"
        "Выбрано: пока ничего\n\n"
        "Нажимайте на интересы для выбора/снятия выбора:"
    )
    
    query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    return INTEREST_SETUP


def toggle_interest_selection(update: Update, context: CallbackContext) -> int:
    """Переключить выбор интереса"""
    query = update.callback_query
    query.answer()
    
    interest = query.data.replace('toggle_interest_', '')
    user_id = query.from_user.id
    
    if user_id not in user_interest_settings:
        user_interest_settings[user_id] = {
            'primary_interests': [],
            'secondary_interests': [],
            'custom_hashtags': [],
            'custom_accounts': []
        }
    
    settings = user_interest_settings[user_id]
    
    # Переключаем интерес
    if interest in settings['primary_interests']:
        settings['primary_interests'].remove(interest)
    elif interest in settings['secondary_interests']:
        settings['secondary_interests'].remove(interest)
    else:
        # Добавляем как основной, если основных меньше 3
        if len(settings['primary_interests']) < 3:
            settings['primary_interests'].append(interest)
        else:
            # Иначе добавляем как дополнительный
            if len(settings['secondary_interests']) < 5:
                settings['secondary_interests'].append(interest)
            else:
                query.answer("Максимум 3 основных и 5 дополнительных интересов!", show_alert=True)
                return INTEREST_SETUP
    
    # Обновляем клавиатуру
    all_interests = [category.value for category in InterestCategory]
    keyboard = []
    
    for i in range(0, len(all_interests), 2):
        row = []
        for j in range(i, min(i + 2, len(all_interests))):
            current_interest = all_interests[j]
            
            # Определяем статус интереса
            if current_interest in settings['primary_interests']:
                prefix = "🟢"  # Основной
            elif current_interest in settings['secondary_interests']:
                prefix = "🟡"  # Дополнительный
            else:
                prefix = "⚪"  # Не выбран
            
            row.append(InlineKeyboardButton(
                f"{prefix} {current_interest.replace('_', ' ').title()}", 
                callback_data=f"toggle_interest_{current_interest}"
            ))
        keyboard.append(row)
    
    keyboard.append([InlineKeyboardButton("✅ Далее", callback_data="interests_next_step")])
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="interest_templates")])
    
    # Обновляем текст
    primary_text = ", ".join(settings['primary_interests']) if settings['primary_interests'] else "нет"
    secondary_text = ", ".join(settings['secondary_interests']) if settings['secondary_interests'] else "нет"
    
    text = (
        "🛠 РУЧНАЯ НАСТРОЙКА ИНТЕРЕСОВ\n\n"
        "🟢 - Основные интересы (макс. 3)\n"
        "🟡 - Дополнительные интересы (макс. 5)\n"
        "⚪ - Не выбрано\n\n"
        f"🎯 Основные: {primary_text}\n"
        f"🔸 Дополнительные: {secondary_text}\n\n"
        "Нажимайте на интересы для выбора/снятия выбора:"
    )
    
    query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    return INTEREST_SETUP


def interests_next_step(update: Update, context: CallbackContext) -> int:
    """Переход к настройке хештегов"""
    query = update.callback_query
    query.answer()
    
    user_id = query.from_user.id
    settings = user_interest_settings.get(user_id, {})
    
    if not settings.get('primary_interests'):
        query.answer("Выберите хотя бы один основной интерес!", show_alert=True)
        return INTEREST_SETUP
    
    text = (
        "🏷 ДОПОЛНИТЕЛЬНЫЕ ХЕШТЕГИ\n\n"
        "Введите дополнительные хештеги для прогрева (необязательно).\n"
        "Каждый хештег с новой строки, без символа #:\n\n"
        "Пример:\n"
        "motivation\n"
        "success\n"
        "goals\n\n"
        "Или отправьте /skip чтобы пропустить:"
    )
    
    keyboard = [
        [InlineKeyboardButton("⏭ Пропустить", callback_data="skip_hashtags")],
        [InlineKeyboardButton("🔙 Назад", callback_data="manual_interests")]
    ]
    
    query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    return CUSTOM_HASHTAGS


def process_custom_hashtags(update: Update, context: CallbackContext) -> int:
    """Обработка пользовательских хештегов"""
    user_id = update.effective_user.id
    text = update.message.text
    
    if text == '/skip':
        return ask_custom_accounts(update, context)
    
    # Парсим хештеги
    hashtags = [tag.strip().replace('#', '') for tag in text.split('\n') if tag.strip()]
    
    if not hashtags:
        update.message.reply_text("❌ Не введено ни одного хештега. Попробуйте еще раз или отправьте /skip")
        return CUSTOM_HASHTAGS
    
    # Сохраняем хештеги
    if user_id not in user_interest_settings:
        user_interest_settings[user_id] = {}
    
    user_interest_settings[user_id]['custom_hashtags'] = hashtags[:20]  # Максимум 20
    
    update.message.reply_text(f"✅ Добавлено {len(hashtags)} хештегов")
    
    return ask_custom_accounts(update, context)


def ask_custom_accounts(update: Update, context: CallbackContext) -> int:
    """Запросить пользовательские аккаунты"""
    text = (
        "👤 ДОПОЛНИТЕЛЬНЫЕ АККАУНТЫ\n\n"
        "Введите дополнительные аккаунты для изучения (необязательно).\n"
        "Каждый аккаунт с новой строки, без символа @:\n\n"
        "Пример:\n"
        "elonmusk\n"
        "garyvee\n"
        "motivation\n\n"
        "Или отправьте /skip чтобы пропустить:"
    )
    
    keyboard = [
        [InlineKeyboardButton("⏭ Пропустить", callback_data="skip_accounts")],
        [InlineKeyboardButton("🔙 Назад", callback_data="back_to_hashtags")]
    ]
    
    if hasattr(update, 'callback_query') and update.callback_query:
        update.callback_query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    else:
        update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    
    return CUSTOM_ACCOUNTS


def process_custom_accounts(update: Update, context: CallbackContext) -> int:
    """Обработка пользовательских аккаунтов"""
    user_id = update.effective_user.id
    text = update.message.text
    
    if text == '/skip':
        return finish_interest_setup(update, context)
    
    # Парсим аккаунты
    accounts = [acc.strip().replace('@', '') for acc in text.split('\n') if acc.strip()]
    
    if not accounts:
        update.message.reply_text("❌ Не введено ни одного аккаунта. Попробуйте еще раз или отправьте /skip")
        return CUSTOM_ACCOUNTS
    
    # Сохраняем аккаунты
    if user_id not in user_interest_settings:
        user_interest_settings[user_id] = {}
    
    user_interest_settings[user_id]['custom_accounts'] = accounts[:10]  # Максимум 10
    
    update.message.reply_text(f"✅ Добавлено {len(accounts)} аккаунтов")
    
    return finish_interest_setup(update, context)


def finish_interest_setup(update: Update, context: CallbackContext) -> int:
    """Завершение настройки интересов"""
    user_id = update.effective_user.id
    settings = user_interest_settings.get(user_id, {})
    
    # Формируем итоговый текст
    text = (
        "✅ НАСТРОЙКА ЗАВЕРШЕНА\n\n"
        f"🎯 Основные интересы: {', '.join(settings.get('primary_interests', []))}\n"
        f"🔸 Дополнительные: {', '.join(settings.get('secondary_interests', []))}\n"
        f"🏷 Хештеги: {len(settings.get('custom_hashtags', []))}\n"
        f"👤 Аккаунты: {len(settings.get('custom_accounts', []))}\n\n"
        "Сохранить эти настройки?"
    )
    
    keyboard = [
        [InlineKeyboardButton("✅ Сохранить", callback_data="save_interest_settings")],
        [InlineKeyboardButton("✏️ Редактировать", callback_data="edit_interest_settings")],
        [InlineKeyboardButton("🔙 Начать заново", callback_data="manual_interests")]
    ]
    
    if hasattr(update, 'callback_query') and update.callback_query:
        update.callback_query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    else:
        update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    
    return ConversationHandler.END


def save_interest_settings(update: Update, context: CallbackContext) -> None:
    """Сохранить настройки интересов"""
    query = update.callback_query
    query.answer()
    
    user_id = query.from_user.id
    settings = user_interest_settings.get(user_id, {})
    
    if not settings:
        query.edit_message_text("❌ Настройки не найдены. Попробуйте заново.", parse_mode=None)
        return
    
    # Сохраняем в файл (можно заменить на базу данных)
    try:
        # Загружаем существующие настройки
        try:
            with open('interest_warmup_settings.json', 'r', encoding='utf-8') as f:
                all_settings = json.load(f)
        except FileNotFoundError:
            all_settings = {}
        
        # Добавляем настройки пользователя
        all_settings[str(user_id)] = settings
        
        # Сохраняем обратно
        with open('interest_warmup_settings.json', 'w', encoding='utf-8') as f:
            json.dump(all_settings, f, ensure_ascii=False, indent=2)
        
        # Очищаем временные настройки
        if user_id in user_interest_settings:
            del user_interest_settings[user_id]
        
        text = (
            "✅ Настройки сохранены!\n\n"
            "Теперь при запуске прогрева будет использоваться "
            "умная система по интересам.\n\n"
            "🎯 Система будет:\n"
            "• Изучать контент по вашим интересам\n"
            "• Взаимодействовать с целевой аудиторией\n"
            "• Формировать алгоритмические предпочтения\n"
            "• Улучшать органический охват"
        )
        
        keyboard = [
            [InlineKeyboardButton("🚀 Тест прогрева", callback_data="test_interest_warmup")],
            [InlineKeyboardButton("🔙 К меню прогрева", callback_data="warmup_menu")]
        ]
        
        query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
        
    except Exception as e:
        logger.error(f"Ошибка сохранения настроек интересов: {e}")
        query.edit_message_text(
            "❌ Ошибка при сохранении настроек. Попробуйте позже.",
            parse_mode=None
        )


def get_interest_conversation_handler():
    """Получить ConversationHandler для настройки интересов"""
    return ConversationHandler(
        entry_points=[
            CallbackQueryHandler(show_manual_interests_setup, pattern='^manual_interests$')
        ],
        states={
            INTEREST_SETUP: [
                CallbackQueryHandler(toggle_interest_selection, pattern='^toggle_interest_'),
                CallbackQueryHandler(interests_next_step, pattern='^interests_next_step$'),
            ],
            CUSTOM_HASHTAGS: [
                MessageHandler(Filters.text & ~Filters.command, process_custom_hashtags),
                CallbackQueryHandler(ask_custom_accounts, pattern='^skip_hashtags$'),
            ],
            CUSTOM_ACCOUNTS: [
                MessageHandler(Filters.text & ~Filters.command, process_custom_accounts),
                CallbackQueryHandler(finish_interest_setup, pattern='^skip_accounts$'),
            ],
        },
        fallbacks=[
            CallbackQueryHandler(show_interest_warmup_menu, pattern='^interest_warmup_menu$'),
        ],
        name="interest_warmup_conversation",
        persistent=False,
    )


# Список всех обработчиков для регистрации в главном боте
INTEREST_WARMUP_HANDLERS = [
    CallbackQueryHandler(show_interest_warmup_menu, pattern='^interest_warmup_menu$'),
    CallbackQueryHandler(show_interest_templates, pattern='^interest_templates$'),
    CallbackQueryHandler(select_interest_template, pattern='^select_template_'),
    CallbackQueryHandler(save_interest_settings, pattern='^save_interest_settings$'),
    get_interest_conversation_handler(),
] 
 
# -*- coding: utf-8 -*-

"""
Обработчики Telegram бота для настройки прогрева по интересам
"""

import logging
import json
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    CallbackContext, ConversationHandler, CallbackQueryHandler, 
    MessageHandler, Filters
)
from database.db_manager import get_instagram_accounts
from utils.interest_based_warmup import InterestCategory

logger = logging.getLogger(__name__)

# Состояния конversation handler
INTEREST_SETUP, CUSTOM_HASHTAGS, CUSTOM_ACCOUNTS = range(3)

# Словарь для хранения временных настроек пользователей
user_interest_settings = {}


def show_interest_warmup_menu(update: Update, context: CallbackContext) -> None:
    """Показать главное меню прогрева по интересам"""
    query = update.callback_query
    query.answer()
    
    keyboard = [
        [InlineKeyboardButton("🎯 Настроить интересы", callback_data="setup_interests")],
        [InlineKeyboardButton("📊 Шаблоны интересов", callback_data="interest_templates")],
        [InlineKeyboardButton("⚙️ Текущие настройки", callback_data="current_interest_settings")],
        [InlineKeyboardButton("🚀 Тест прогрева по интересам", callback_data="test_interest_warmup")],
        [InlineKeyboardButton("🔙 Назад к прогреву", callback_data="warmup_menu")]
    ]
    
    text = (
        "🎯 ПРОГРЕВ ПО ИНТЕРЕСАМ\n\n"
        "Умная система прогрева, которая:\n"
        "• 🔍 Изучает контент по вашим интересам\n"
        "• 👥 Взаимодействует с целевой аудиторией\n"
        "• 🎯 Формирует алгоритмические предпочтения\n"
        "• 📈 Улучшает органический охват\n\n"
        "Выберите действие:"
    )
    
    query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)


def show_interest_templates(update: Update, context: CallbackContext) -> None:
    """Показать готовые шаблоны интересов"""
    query = update.callback_query
    query.answer()
    
    templates = {
        'fitness': {
            'name': '💪 Фитнес и спорт',
            'description': 'Тренировки, мотивация, здоровый образ жизни',
            'interests': ['fitness', 'health']
        },
        'food': {
            'name': '🍕 Еда и кулинария',
            'description': 'Рецепты, рестораны, гастрономия',
            'interests': ['food']
        },
        'travel': {
            'name': '✈️ Путешествия',
            'description': 'Туризм, природа, приключения',
            'interests': ['travel', 'photography']
        },
        'business': {
            'name': '💼 Бизнес',
            'description': 'Предпринимательство, мотивация, финансы',
            'interests': ['business', 'finance']
        },
        'technology': {
            'name': '💻 Технологии',
            'description': 'IT, гаджеты, инновации',
            'interests': ['technology']
        },
        'lifestyle': {
            'name': '🌟 Лайфстайл',
            'description': 'Мода, красота, развлечения',
            'interests': ['lifestyle', 'fashion', 'beauty']
        }
    }
    
    keyboard = []
    for template_key, template_data in templates.items():
        keyboard.append([InlineKeyboardButton(
            template_data['name'], 
            callback_data=f"select_template_{template_key}"
        )])
    
    keyboard.append([InlineKeyboardButton("🛠 Настроить вручную", callback_data="manual_interests")])
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="interest_warmup_menu")])
    
    text = (
        "📊 ШАБЛОНЫ ИНТЕРЕСОВ\n\n"
        "Выберите готовый шаблон или настройте вручную:\n\n"
    )
    
    for template_data in templates.values():
        text += f"{template_data['name']}\n{template_data['description']}\n\n"
    
    query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)


def select_interest_template(update: Update, context: CallbackContext) -> None:
    """Выбор шаблона интересов"""
    query = update.callback_query
    query.answer()
    
    template_name = query.data.replace('select_template_', '')
    user_id = query.from_user.id
    
    # Шаблоны интересов
    templates = {
        'fitness': {
            'primary_interests': ['fitness'],
            'secondary_interests': ['health', 'motivation'],
            'custom_hashtags': ['gym', 'workout', 'training', 'bodybuilding'],
            'custom_accounts': ['therock', 'nike', 'underarmour']
        },
        'food': {
            'primary_interests': ['food'],
            'secondary_interests': ['lifestyle'],
            'custom_hashtags': ['recipe', 'cooking', 'chef', 'delicious'],
            'custom_accounts': ['gordongram', 'jamieoliver', 'buzzfeedtasty']
        },
        'travel': {
            'primary_interests': ['travel'],
            'secondary_interests': ['photography', 'lifestyle'],
            'custom_hashtags': ['wanderlust', 'adventure', 'explore', 'nature'],
            'custom_accounts': ['natgeo', 'beautifuldestinations', 'lonelyplanet']
        },
        'business': {
            'primary_interests': ['business'],
            'secondary_interests': ['finance', 'motivation'],
            'custom_hashtags': ['entrepreneur', 'startup', 'success', 'leadership'],
            'custom_accounts': ['garyvee', 'forbes', 'entrepreneur']
        },
        'technology': {
            'primary_interests': ['technology'],
            'secondary_interests': ['business'],
            'custom_hashtags': ['innovation', 'ai', 'startup', 'coding'],
            'custom_accounts': ['elonmusk', 'apple', 'techcrunch']
        },
        'lifestyle': {
            'primary_interests': ['lifestyle'],
            'secondary_interests': ['fashion', 'beauty'],
            'custom_hashtags': ['style', 'trend', 'inspiration', 'mood'],
            'custom_accounts': ['voguemagazine', 'harpersbazaar']
        }
    }
    
    if template_name in templates:
        # Сохраняем настройки шаблона
        user_interest_settings[user_id] = templates[template_name].copy()
        
        # Показываем предварительный просмотр
        settings = user_interest_settings[user_id]
        
        text = (
            f"✅ Выбран шаблон: {template_name.upper()}\n\n"
            f"🎯 Основные интересы: {', '.join(settings['primary_interests'])}\n"
            f"🔸 Дополнительные: {', '.join(settings['secondary_interests'])}\n"
            f"🏷 Хештеги: {', '.join(settings['custom_hashtags'][:5])}{'...' if len(settings['custom_hashtags']) > 5 else ''}\n"
            f"👤 Аккаунты: {', '.join(settings['custom_accounts'])}\n\n"
            "Сохранить эти настройки?"
        )
        
        keyboard = [
            [InlineKeyboardButton("✅ Сохранить", callback_data="save_interest_settings")],
            [InlineKeyboardButton("✏️ Редактировать", callback_data="edit_interest_settings")],
            [InlineKeyboardButton("🔙 Выбрать другой", callback_data="interest_templates")]
        ]
        
        query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)


def show_manual_interests_setup(update: Update, context: CallbackContext) -> int:
    """Показать меню ручной настройки интересов"""
    query = update.callback_query
    query.answer()
    
    # Список всех доступных интересов
    all_interests = [category.value for category in InterestCategory]
    
    # Создаем клавиатуру с интересами (по 2 в ряд)
    keyboard = []
    for i in range(0, len(all_interests), 2):
        row = []
        for j in range(i, min(i + 2, len(all_interests))):
            interest = all_interests[j]
            row.append(InlineKeyboardButton(
                f"🔸 {interest.replace('_', ' ').title()}", 
                callback_data=f"toggle_interest_{interest}"
            ))
        keyboard.append(row)
    
    keyboard.append([InlineKeyboardButton("✅ Далее", callback_data="interests_next_step")])
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="interest_templates")])
    
    # Инициализируем настройки пользователя
    user_id = query.from_user.id
    if user_id not in user_interest_settings:
        user_interest_settings[user_id] = {
            'primary_interests': [],
            'secondary_interests': [],
            'custom_hashtags': [],
            'custom_accounts': []
        }
    
    text = (
        "🛠 РУЧНАЯ НАСТРОЙКА ИНТЕРЕСОВ\n\n"
        "Выберите 2-3 основных интереса для прогрева:\n\n"
        "Выбрано: пока ничего\n\n"
        "Нажимайте на интересы для выбора/снятия выбора:"
    )
    
    query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    return INTEREST_SETUP


def toggle_interest_selection(update: Update, context: CallbackContext) -> int:
    """Переключить выбор интереса"""
    query = update.callback_query
    query.answer()
    
    interest = query.data.replace('toggle_interest_', '')
    user_id = query.from_user.id
    
    if user_id not in user_interest_settings:
        user_interest_settings[user_id] = {
            'primary_interests': [],
            'secondary_interests': [],
            'custom_hashtags': [],
            'custom_accounts': []
        }
    
    settings = user_interest_settings[user_id]
    
    # Переключаем интерес
    if interest in settings['primary_interests']:
        settings['primary_interests'].remove(interest)
    elif interest in settings['secondary_interests']:
        settings['secondary_interests'].remove(interest)
    else:
        # Добавляем как основной, если основных меньше 3
        if len(settings['primary_interests']) < 3:
            settings['primary_interests'].append(interest)
        else:
            # Иначе добавляем как дополнительный
            if len(settings['secondary_interests']) < 5:
                settings['secondary_interests'].append(interest)
            else:
                query.answer("Максимум 3 основных и 5 дополнительных интересов!", show_alert=True)
                return INTEREST_SETUP
    
    # Обновляем клавиатуру
    all_interests = [category.value for category in InterestCategory]
    keyboard = []
    
    for i in range(0, len(all_interests), 2):
        row = []
        for j in range(i, min(i + 2, len(all_interests))):
            current_interest = all_interests[j]
            
            # Определяем статус интереса
            if current_interest in settings['primary_interests']:
                prefix = "🟢"  # Основной
            elif current_interest in settings['secondary_interests']:
                prefix = "🟡"  # Дополнительный
            else:
                prefix = "⚪"  # Не выбран
            
            row.append(InlineKeyboardButton(
                f"{prefix} {current_interest.replace('_', ' ').title()}", 
                callback_data=f"toggle_interest_{current_interest}"
            ))
        keyboard.append(row)
    
    keyboard.append([InlineKeyboardButton("✅ Далее", callback_data="interests_next_step")])
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="interest_templates")])
    
    # Обновляем текст
    primary_text = ", ".join(settings['primary_interests']) if settings['primary_interests'] else "нет"
    secondary_text = ", ".join(settings['secondary_interests']) if settings['secondary_interests'] else "нет"
    
    text = (
        "🛠 РУЧНАЯ НАСТРОЙКА ИНТЕРЕСОВ\n\n"
        "🟢 - Основные интересы (макс. 3)\n"
        "🟡 - Дополнительные интересы (макс. 5)\n"
        "⚪ - Не выбрано\n\n"
        f"🎯 Основные: {primary_text}\n"
        f"🔸 Дополнительные: {secondary_text}\n\n"
        "Нажимайте на интересы для выбора/снятия выбора:"
    )
    
    query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    return INTEREST_SETUP


def interests_next_step(update: Update, context: CallbackContext) -> int:
    """Переход к настройке хештегов"""
    query = update.callback_query
    query.answer()
    
    user_id = query.from_user.id
    settings = user_interest_settings.get(user_id, {})
    
    if not settings.get('primary_interests'):
        query.answer("Выберите хотя бы один основной интерес!", show_alert=True)
        return INTEREST_SETUP
    
    text = (
        "🏷 ДОПОЛНИТЕЛЬНЫЕ ХЕШТЕГИ\n\n"
        "Введите дополнительные хештеги для прогрева (необязательно).\n"
        "Каждый хештег с новой строки, без символа #:\n\n"
        "Пример:\n"
        "motivation\n"
        "success\n"
        "goals\n\n"
        "Или отправьте /skip чтобы пропустить:"
    )
    
    keyboard = [
        [InlineKeyboardButton("⏭ Пропустить", callback_data="skip_hashtags")],
        [InlineKeyboardButton("🔙 Назад", callback_data="manual_interests")]
    ]
    
    query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    return CUSTOM_HASHTAGS


def process_custom_hashtags(update: Update, context: CallbackContext) -> int:
    """Обработка пользовательских хештегов"""
    user_id = update.effective_user.id
    text = update.message.text
    
    if text == '/skip':
        return ask_custom_accounts(update, context)
    
    # Парсим хештеги
    hashtags = [tag.strip().replace('#', '') for tag in text.split('\n') if tag.strip()]
    
    if not hashtags:
        update.message.reply_text("❌ Не введено ни одного хештега. Попробуйте еще раз или отправьте /skip")
        return CUSTOM_HASHTAGS
    
    # Сохраняем хештеги
    if user_id not in user_interest_settings:
        user_interest_settings[user_id] = {}
    
    user_interest_settings[user_id]['custom_hashtags'] = hashtags[:20]  # Максимум 20
    
    update.message.reply_text(f"✅ Добавлено {len(hashtags)} хештегов")
    
    return ask_custom_accounts(update, context)


def ask_custom_accounts(update: Update, context: CallbackContext) -> int:
    """Запросить пользовательские аккаунты"""
    text = (
        "👤 ДОПОЛНИТЕЛЬНЫЕ АККАУНТЫ\n\n"
        "Введите дополнительные аккаунты для изучения (необязательно).\n"
        "Каждый аккаунт с новой строки, без символа @:\n\n"
        "Пример:\n"
        "elonmusk\n"
        "garyvee\n"
        "motivation\n\n"
        "Или отправьте /skip чтобы пропустить:"
    )
    
    keyboard = [
        [InlineKeyboardButton("⏭ Пропустить", callback_data="skip_accounts")],
        [InlineKeyboardButton("🔙 Назад", callback_data="back_to_hashtags")]
    ]
    
    if hasattr(update, 'callback_query') and update.callback_query:
        update.callback_query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    else:
        update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    
    return CUSTOM_ACCOUNTS


def process_custom_accounts(update: Update, context: CallbackContext) -> int:
    """Обработка пользовательских аккаунтов"""
    user_id = update.effective_user.id
    text = update.message.text
    
    if text == '/skip':
        return finish_interest_setup(update, context)
    
    # Парсим аккаунты
    accounts = [acc.strip().replace('@', '') for acc in text.split('\n') if acc.strip()]
    
    if not accounts:
        update.message.reply_text("❌ Не введено ни одного аккаунта. Попробуйте еще раз или отправьте /skip")
        return CUSTOM_ACCOUNTS
    
    # Сохраняем аккаунты
    if user_id not in user_interest_settings:
        user_interest_settings[user_id] = {}
    
    user_interest_settings[user_id]['custom_accounts'] = accounts[:10]  # Максимум 10
    
    update.message.reply_text(f"✅ Добавлено {len(accounts)} аккаунтов")
    
    return finish_interest_setup(update, context)


def finish_interest_setup(update: Update, context: CallbackContext) -> int:
    """Завершение настройки интересов"""
    user_id = update.effective_user.id
    settings = user_interest_settings.get(user_id, {})
    
    # Формируем итоговый текст
    text = (
        "✅ НАСТРОЙКА ЗАВЕРШЕНА\n\n"
        f"🎯 Основные интересы: {', '.join(settings.get('primary_interests', []))}\n"
        f"🔸 Дополнительные: {', '.join(settings.get('secondary_interests', []))}\n"
        f"🏷 Хештеги: {len(settings.get('custom_hashtags', []))}\n"
        f"👤 Аккаунты: {len(settings.get('custom_accounts', []))}\n\n"
        "Сохранить эти настройки?"
    )
    
    keyboard = [
        [InlineKeyboardButton("✅ Сохранить", callback_data="save_interest_settings")],
        [InlineKeyboardButton("✏️ Редактировать", callback_data="edit_interest_settings")],
        [InlineKeyboardButton("🔙 Начать заново", callback_data="manual_interests")]
    ]
    
    if hasattr(update, 'callback_query') and update.callback_query:
        update.callback_query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    else:
        update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    
    return ConversationHandler.END


def save_interest_settings(update: Update, context: CallbackContext) -> None:
    """Сохранить настройки интересов"""
    query = update.callback_query
    query.answer()
    
    user_id = query.from_user.id
    settings = user_interest_settings.get(user_id, {})
    
    if not settings:
        query.edit_message_text("❌ Настройки не найдены. Попробуйте заново.", parse_mode=None)
        return
    
    # Сохраняем в файл (можно заменить на базу данных)
    try:
        # Загружаем существующие настройки
        try:
            with open('interest_warmup_settings.json', 'r', encoding='utf-8') as f:
                all_settings = json.load(f)
        except FileNotFoundError:
            all_settings = {}
        
        # Добавляем настройки пользователя
        all_settings[str(user_id)] = settings
        
        # Сохраняем обратно
        with open('interest_warmup_settings.json', 'w', encoding='utf-8') as f:
            json.dump(all_settings, f, ensure_ascii=False, indent=2)
        
        # Очищаем временные настройки
        if user_id in user_interest_settings:
            del user_interest_settings[user_id]
        
        text = (
            "✅ Настройки сохранены!\n\n"
            "Теперь при запуске прогрева будет использоваться "
            "умная система по интересам.\n\n"
            "🎯 Система будет:\n"
            "• Изучать контент по вашим интересам\n"
            "• Взаимодействовать с целевой аудиторией\n"
            "• Формировать алгоритмические предпочтения\n"
            "• Улучшать органический охват"
        )
        
        keyboard = [
            [InlineKeyboardButton("🚀 Тест прогрева", callback_data="test_interest_warmup")],
            [InlineKeyboardButton("🔙 К меню прогрева", callback_data="warmup_menu")]
        ]
        
        query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
        
    except Exception as e:
        logger.error(f"Ошибка сохранения настроек интересов: {e}")
        query.edit_message_text(
            "❌ Ошибка при сохранении настроек. Попробуйте позже.",
            parse_mode=None
        )


def get_interest_conversation_handler():
    """Получить ConversationHandler для настройки интересов"""
    return ConversationHandler(
        entry_points=[
            CallbackQueryHandler(show_manual_interests_setup, pattern='^manual_interests$')
        ],
        states={
            INTEREST_SETUP: [
                CallbackQueryHandler(toggle_interest_selection, pattern='^toggle_interest_'),
                CallbackQueryHandler(interests_next_step, pattern='^interests_next_step$'),
            ],
            CUSTOM_HASHTAGS: [
                MessageHandler(Filters.text & ~Filters.command, process_custom_hashtags),
                CallbackQueryHandler(ask_custom_accounts, pattern='^skip_hashtags$'),
            ],
            CUSTOM_ACCOUNTS: [
                MessageHandler(Filters.text & ~Filters.command, process_custom_accounts),
                CallbackQueryHandler(finish_interest_setup, pattern='^skip_accounts$'),
            ],
        },
        fallbacks=[
            CallbackQueryHandler(show_interest_warmup_menu, pattern='^interest_warmup_menu$'),
        ],
        name="interest_warmup_conversation",
        persistent=False,
    )


# Список всех обработчиков для регистрации в главном боте
INTEREST_WARMUP_HANDLERS = [
    CallbackQueryHandler(show_interest_warmup_menu, pattern='^interest_warmup_menu$'),
    CallbackQueryHandler(show_interest_templates, pattern='^interest_templates$'),
    CallbackQueryHandler(select_interest_template, pattern='^select_template_'),
    CallbackQueryHandler(save_interest_settings, pattern='^save_interest_settings$'),
    get_interest_conversation_handler(),
] 
 
 
 
 
# -*- coding: utf-8 -*-

"""
Обработчики Telegram бота для настройки прогрева по интересам
"""

import logging
import json
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    CallbackContext, ConversationHandler, CallbackQueryHandler, 
    MessageHandler, Filters
)
from database.db_manager import get_instagram_accounts
from utils.interest_based_warmup import InterestCategory

logger = logging.getLogger(__name__)

# Состояния конversation handler
INTEREST_SETUP, CUSTOM_HASHTAGS, CUSTOM_ACCOUNTS = range(3)

# Словарь для хранения временных настроек пользователей
user_interest_settings = {}


def show_interest_warmup_menu(update: Update, context: CallbackContext) -> None:
    """Показать главное меню прогрева по интересам"""
    query = update.callback_query
    query.answer()
    
    keyboard = [
        [InlineKeyboardButton("🎯 Настроить интересы", callback_data="setup_interests")],
        [InlineKeyboardButton("📊 Шаблоны интересов", callback_data="interest_templates")],
        [InlineKeyboardButton("⚙️ Текущие настройки", callback_data="current_interest_settings")],
        [InlineKeyboardButton("🚀 Тест прогрева по интересам", callback_data="test_interest_warmup")],
        [InlineKeyboardButton("🔙 Назад к прогреву", callback_data="warmup_menu")]
    ]
    
    text = (
        "🎯 ПРОГРЕВ ПО ИНТЕРЕСАМ\n\n"
        "Умная система прогрева, которая:\n"
        "• 🔍 Изучает контент по вашим интересам\n"
        "• 👥 Взаимодействует с целевой аудиторией\n"
        "• 🎯 Формирует алгоритмические предпочтения\n"
        "• 📈 Улучшает органический охват\n\n"
        "Выберите действие:"
    )
    
    query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)


def show_interest_templates(update: Update, context: CallbackContext) -> None:
    """Показать готовые шаблоны интересов"""
    query = update.callback_query
    query.answer()
    
    templates = {
        'fitness': {
            'name': '💪 Фитнес и спорт',
            'description': 'Тренировки, мотивация, здоровый образ жизни',
            'interests': ['fitness', 'health']
        },
        'food': {
            'name': '🍕 Еда и кулинария',
            'description': 'Рецепты, рестораны, гастрономия',
            'interests': ['food']
        },
        'travel': {
            'name': '✈️ Путешествия',
            'description': 'Туризм, природа, приключения',
            'interests': ['travel', 'photography']
        },
        'business': {
            'name': '💼 Бизнес',
            'description': 'Предпринимательство, мотивация, финансы',
            'interests': ['business', 'finance']
        },
        'technology': {
            'name': '💻 Технологии',
            'description': 'IT, гаджеты, инновации',
            'interests': ['technology']
        },
        'lifestyle': {
            'name': '🌟 Лайфстайл',
            'description': 'Мода, красота, развлечения',
            'interests': ['lifestyle', 'fashion', 'beauty']
        }
    }
    
    keyboard = []
    for template_key, template_data in templates.items():
        keyboard.append([InlineKeyboardButton(
            template_data['name'], 
            callback_data=f"select_template_{template_key}"
        )])
    
    keyboard.append([InlineKeyboardButton("🛠 Настроить вручную", callback_data="manual_interests")])
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="interest_warmup_menu")])
    
    text = (
        "📊 ШАБЛОНЫ ИНТЕРЕСОВ\n\n"
        "Выберите готовый шаблон или настройте вручную:\n\n"
    )
    
    for template_data in templates.values():
        text += f"{template_data['name']}\n{template_data['description']}\n\n"
    
    query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)


def select_interest_template(update: Update, context: CallbackContext) -> None:
    """Выбор шаблона интересов"""
    query = update.callback_query
    query.answer()
    
    template_name = query.data.replace('select_template_', '')
    user_id = query.from_user.id
    
    # Шаблоны интересов
    templates = {
        'fitness': {
            'primary_interests': ['fitness'],
            'secondary_interests': ['health', 'motivation'],
            'custom_hashtags': ['gym', 'workout', 'training', 'bodybuilding'],
            'custom_accounts': ['therock', 'nike', 'underarmour']
        },
        'food': {
            'primary_interests': ['food'],
            'secondary_interests': ['lifestyle'],
            'custom_hashtags': ['recipe', 'cooking', 'chef', 'delicious'],
            'custom_accounts': ['gordongram', 'jamieoliver', 'buzzfeedtasty']
        },
        'travel': {
            'primary_interests': ['travel'],
            'secondary_interests': ['photography', 'lifestyle'],
            'custom_hashtags': ['wanderlust', 'adventure', 'explore', 'nature'],
            'custom_accounts': ['natgeo', 'beautifuldestinations', 'lonelyplanet']
        },
        'business': {
            'primary_interests': ['business'],
            'secondary_interests': ['finance', 'motivation'],
            'custom_hashtags': ['entrepreneur', 'startup', 'success', 'leadership'],
            'custom_accounts': ['garyvee', 'forbes', 'entrepreneur']
        },
        'technology': {
            'primary_interests': ['technology'],
            'secondary_interests': ['business'],
            'custom_hashtags': ['innovation', 'ai', 'startup', 'coding'],
            'custom_accounts': ['elonmusk', 'apple', 'techcrunch']
        },
        'lifestyle': {
            'primary_interests': ['lifestyle'],
            'secondary_interests': ['fashion', 'beauty'],
            'custom_hashtags': ['style', 'trend', 'inspiration', 'mood'],
            'custom_accounts': ['voguemagazine', 'harpersbazaar']
        }
    }
    
    if template_name in templates:
        # Сохраняем настройки шаблона
        user_interest_settings[user_id] = templates[template_name].copy()
        
        # Показываем предварительный просмотр
        settings = user_interest_settings[user_id]
        
        text = (
            f"✅ Выбран шаблон: {template_name.upper()}\n\n"
            f"🎯 Основные интересы: {', '.join(settings['primary_interests'])}\n"
            f"🔸 Дополнительные: {', '.join(settings['secondary_interests'])}\n"
            f"🏷 Хештеги: {', '.join(settings['custom_hashtags'][:5])}{'...' if len(settings['custom_hashtags']) > 5 else ''}\n"
            f"👤 Аккаунты: {', '.join(settings['custom_accounts'])}\n\n"
            "Сохранить эти настройки?"
        )
        
        keyboard = [
            [InlineKeyboardButton("✅ Сохранить", callback_data="save_interest_settings")],
            [InlineKeyboardButton("✏️ Редактировать", callback_data="edit_interest_settings")],
            [InlineKeyboardButton("🔙 Выбрать другой", callback_data="interest_templates")]
        ]
        
        query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)


def show_manual_interests_setup(update: Update, context: CallbackContext) -> int:
    """Показать меню ручной настройки интересов"""
    query = update.callback_query
    query.answer()
    
    # Список всех доступных интересов
    all_interests = [category.value for category in InterestCategory]
    
    # Создаем клавиатуру с интересами (по 2 в ряд)
    keyboard = []
    for i in range(0, len(all_interests), 2):
        row = []
        for j in range(i, min(i + 2, len(all_interests))):
            interest = all_interests[j]
            row.append(InlineKeyboardButton(
                f"🔸 {interest.replace('_', ' ').title()}", 
                callback_data=f"toggle_interest_{interest}"
            ))
        keyboard.append(row)
    
    keyboard.append([InlineKeyboardButton("✅ Далее", callback_data="interests_next_step")])
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="interest_templates")])
    
    # Инициализируем настройки пользователя
    user_id = query.from_user.id
    if user_id not in user_interest_settings:
        user_interest_settings[user_id] = {
            'primary_interests': [],
            'secondary_interests': [],
            'custom_hashtags': [],
            'custom_accounts': []
        }
    
    text = (
        "🛠 РУЧНАЯ НАСТРОЙКА ИНТЕРЕСОВ\n\n"
        "Выберите 2-3 основных интереса для прогрева:\n\n"
        "Выбрано: пока ничего\n\n"
        "Нажимайте на интересы для выбора/снятия выбора:"
    )
    
    query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    return INTEREST_SETUP


def toggle_interest_selection(update: Update, context: CallbackContext) -> int:
    """Переключить выбор интереса"""
    query = update.callback_query
    query.answer()
    
    interest = query.data.replace('toggle_interest_', '')
    user_id = query.from_user.id
    
    if user_id not in user_interest_settings:
        user_interest_settings[user_id] = {
            'primary_interests': [],
            'secondary_interests': [],
            'custom_hashtags': [],
            'custom_accounts': []
        }
    
    settings = user_interest_settings[user_id]
    
    # Переключаем интерес
    if interest in settings['primary_interests']:
        settings['primary_interests'].remove(interest)
    elif interest in settings['secondary_interests']:
        settings['secondary_interests'].remove(interest)
    else:
        # Добавляем как основной, если основных меньше 3
        if len(settings['primary_interests']) < 3:
            settings['primary_interests'].append(interest)
        else:
            # Иначе добавляем как дополнительный
            if len(settings['secondary_interests']) < 5:
                settings['secondary_interests'].append(interest)
            else:
                query.answer("Максимум 3 основных и 5 дополнительных интересов!", show_alert=True)
                return INTEREST_SETUP
    
    # Обновляем клавиатуру
    all_interests = [category.value for category in InterestCategory]
    keyboard = []
    
    for i in range(0, len(all_interests), 2):
        row = []
        for j in range(i, min(i + 2, len(all_interests))):
            current_interest = all_interests[j]
            
            # Определяем статус интереса
            if current_interest in settings['primary_interests']:
                prefix = "🟢"  # Основной
            elif current_interest in settings['secondary_interests']:
                prefix = "🟡"  # Дополнительный
            else:
                prefix = "⚪"  # Не выбран
            
            row.append(InlineKeyboardButton(
                f"{prefix} {current_interest.replace('_', ' ').title()}", 
                callback_data=f"toggle_interest_{current_interest}"
            ))
        keyboard.append(row)
    
    keyboard.append([InlineKeyboardButton("✅ Далее", callback_data="interests_next_step")])
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="interest_templates")])
    
    # Обновляем текст
    primary_text = ", ".join(settings['primary_interests']) if settings['primary_interests'] else "нет"
    secondary_text = ", ".join(settings['secondary_interests']) if settings['secondary_interests'] else "нет"
    
    text = (
        "🛠 РУЧНАЯ НАСТРОЙКА ИНТЕРЕСОВ\n\n"
        "🟢 - Основные интересы (макс. 3)\n"
        "🟡 - Дополнительные интересы (макс. 5)\n"
        "⚪ - Не выбрано\n\n"
        f"🎯 Основные: {primary_text}\n"
        f"🔸 Дополнительные: {secondary_text}\n\n"
        "Нажимайте на интересы для выбора/снятия выбора:"
    )
    
    query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    return INTEREST_SETUP


def interests_next_step(update: Update, context: CallbackContext) -> int:
    """Переход к настройке хештегов"""
    query = update.callback_query
    query.answer()
    
    user_id = query.from_user.id
    settings = user_interest_settings.get(user_id, {})
    
    if not settings.get('primary_interests'):
        query.answer("Выберите хотя бы один основной интерес!", show_alert=True)
        return INTEREST_SETUP
    
    text = (
        "🏷 ДОПОЛНИТЕЛЬНЫЕ ХЕШТЕГИ\n\n"
        "Введите дополнительные хештеги для прогрева (необязательно).\n"
        "Каждый хештег с новой строки, без символа #:\n\n"
        "Пример:\n"
        "motivation\n"
        "success\n"
        "goals\n\n"
        "Или отправьте /skip чтобы пропустить:"
    )
    
    keyboard = [
        [InlineKeyboardButton("⏭ Пропустить", callback_data="skip_hashtags")],
        [InlineKeyboardButton("🔙 Назад", callback_data="manual_interests")]
    ]
    
    query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    return CUSTOM_HASHTAGS


def process_custom_hashtags(update: Update, context: CallbackContext) -> int:
    """Обработка пользовательских хештегов"""
    user_id = update.effective_user.id
    text = update.message.text
    
    if text == '/skip':
        return ask_custom_accounts(update, context)
    
    # Парсим хештеги
    hashtags = [tag.strip().replace('#', '') for tag in text.split('\n') if tag.strip()]
    
    if not hashtags:
        update.message.reply_text("❌ Не введено ни одного хештега. Попробуйте еще раз или отправьте /skip")
        return CUSTOM_HASHTAGS
    
    # Сохраняем хештеги
    if user_id not in user_interest_settings:
        user_interest_settings[user_id] = {}
    
    user_interest_settings[user_id]['custom_hashtags'] = hashtags[:20]  # Максимум 20
    
    update.message.reply_text(f"✅ Добавлено {len(hashtags)} хештегов")
    
    return ask_custom_accounts(update, context)


def ask_custom_accounts(update: Update, context: CallbackContext) -> int:
    """Запросить пользовательские аккаунты"""
    text = (
        "👤 ДОПОЛНИТЕЛЬНЫЕ АККАУНТЫ\n\n"
        "Введите дополнительные аккаунты для изучения (необязательно).\n"
        "Каждый аккаунт с новой строки, без символа @:\n\n"
        "Пример:\n"
        "elonmusk\n"
        "garyvee\n"
        "motivation\n\n"
        "Или отправьте /skip чтобы пропустить:"
    )
    
    keyboard = [
        [InlineKeyboardButton("⏭ Пропустить", callback_data="skip_accounts")],
        [InlineKeyboardButton("🔙 Назад", callback_data="back_to_hashtags")]
    ]
    
    if hasattr(update, 'callback_query') and update.callback_query:
        update.callback_query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    else:
        update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    
    return CUSTOM_ACCOUNTS


def process_custom_accounts(update: Update, context: CallbackContext) -> int:
    """Обработка пользовательских аккаунтов"""
    user_id = update.effective_user.id
    text = update.message.text
    
    if text == '/skip':
        return finish_interest_setup(update, context)
    
    # Парсим аккаунты
    accounts = [acc.strip().replace('@', '') for acc in text.split('\n') if acc.strip()]
    
    if not accounts:
        update.message.reply_text("❌ Не введено ни одного аккаунта. Попробуйте еще раз или отправьте /skip")
        return CUSTOM_ACCOUNTS
    
    # Сохраняем аккаунты
    if user_id not in user_interest_settings:
        user_interest_settings[user_id] = {}
    
    user_interest_settings[user_id]['custom_accounts'] = accounts[:10]  # Максимум 10
    
    update.message.reply_text(f"✅ Добавлено {len(accounts)} аккаунтов")
    
    return finish_interest_setup(update, context)


def finish_interest_setup(update: Update, context: CallbackContext) -> int:
    """Завершение настройки интересов"""
    user_id = update.effective_user.id
    settings = user_interest_settings.get(user_id, {})
    
    # Формируем итоговый текст
    text = (
        "✅ НАСТРОЙКА ЗАВЕРШЕНА\n\n"
        f"🎯 Основные интересы: {', '.join(settings.get('primary_interests', []))}\n"
        f"🔸 Дополнительные: {', '.join(settings.get('secondary_interests', []))}\n"
        f"🏷 Хештеги: {len(settings.get('custom_hashtags', []))}\n"
        f"👤 Аккаунты: {len(settings.get('custom_accounts', []))}\n\n"
        "Сохранить эти настройки?"
    )
    
    keyboard = [
        [InlineKeyboardButton("✅ Сохранить", callback_data="save_interest_settings")],
        [InlineKeyboardButton("✏️ Редактировать", callback_data="edit_interest_settings")],
        [InlineKeyboardButton("🔙 Начать заново", callback_data="manual_interests")]
    ]
    
    if hasattr(update, 'callback_query') and update.callback_query:
        update.callback_query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    else:
        update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    
    return ConversationHandler.END


def save_interest_settings(update: Update, context: CallbackContext) -> None:
    """Сохранить настройки интересов"""
    query = update.callback_query
    query.answer()
    
    user_id = query.from_user.id
    settings = user_interest_settings.get(user_id, {})
    
    if not settings:
        query.edit_message_text("❌ Настройки не найдены. Попробуйте заново.", parse_mode=None)
        return
    
    # Сохраняем в файл (можно заменить на базу данных)
    try:
        # Загружаем существующие настройки
        try:
            with open('interest_warmup_settings.json', 'r', encoding='utf-8') as f:
                all_settings = json.load(f)
        except FileNotFoundError:
            all_settings = {}
        
        # Добавляем настройки пользователя
        all_settings[str(user_id)] = settings
        
        # Сохраняем обратно
        with open('interest_warmup_settings.json', 'w', encoding='utf-8') as f:
            json.dump(all_settings, f, ensure_ascii=False, indent=2)
        
        # Очищаем временные настройки
        if user_id in user_interest_settings:
            del user_interest_settings[user_id]
        
        text = (
            "✅ Настройки сохранены!\n\n"
            "Теперь при запуске прогрева будет использоваться "
            "умная система по интересам.\n\n"
            "🎯 Система будет:\n"
            "• Изучать контент по вашим интересам\n"
            "• Взаимодействовать с целевой аудиторией\n"
            "• Формировать алгоритмические предпочтения\n"
            "• Улучшать органический охват"
        )
        
        keyboard = [
            [InlineKeyboardButton("🚀 Тест прогрева", callback_data="test_interest_warmup")],
            [InlineKeyboardButton("🔙 К меню прогрева", callback_data="warmup_menu")]
        ]
        
        query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
        
    except Exception as e:
        logger.error(f"Ошибка сохранения настроек интересов: {e}")
        query.edit_message_text(
            "❌ Ошибка при сохранении настроек. Попробуйте позже.",
            parse_mode=None
        )


def get_interest_conversation_handler():
    """Получить ConversationHandler для настройки интересов"""
    return ConversationHandler(
        entry_points=[
            CallbackQueryHandler(show_manual_interests_setup, pattern='^manual_interests$')
        ],
        states={
            INTEREST_SETUP: [
                CallbackQueryHandler(toggle_interest_selection, pattern='^toggle_interest_'),
                CallbackQueryHandler(interests_next_step, pattern='^interests_next_step$'),
            ],
            CUSTOM_HASHTAGS: [
                MessageHandler(Filters.text & ~Filters.command, process_custom_hashtags),
                CallbackQueryHandler(ask_custom_accounts, pattern='^skip_hashtags$'),
            ],
            CUSTOM_ACCOUNTS: [
                MessageHandler(Filters.text & ~Filters.command, process_custom_accounts),
                CallbackQueryHandler(finish_interest_setup, pattern='^skip_accounts$'),
            ],
        },
        fallbacks=[
            CallbackQueryHandler(show_interest_warmup_menu, pattern='^interest_warmup_menu$'),
        ],
        name="interest_warmup_conversation",
        persistent=False,
    )


# Список всех обработчиков для регистрации в главном боте
INTEREST_WARMUP_HANDLERS = [
    CallbackQueryHandler(show_interest_warmup_menu, pattern='^interest_warmup_menu$'),
    CallbackQueryHandler(show_interest_templates, pattern='^interest_templates$'),
    CallbackQueryHandler(select_interest_template, pattern='^select_template_'),
    CallbackQueryHandler(save_interest_settings, pattern='^save_interest_settings$'),
    get_interest_conversation_handler(),
] 
 
# -*- coding: utf-8 -*-

"""
Обработчики Telegram бота для настройки прогрева по интересам
"""

import logging
import json
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    CallbackContext, ConversationHandler, CallbackQueryHandler, 
    MessageHandler, Filters
)
from database.db_manager import get_instagram_accounts
from utils.interest_based_warmup import InterestCategory

logger = logging.getLogger(__name__)

# Состояния конversation handler
INTEREST_SETUP, CUSTOM_HASHTAGS, CUSTOM_ACCOUNTS = range(3)

# Словарь для хранения временных настроек пользователей
user_interest_settings = {}


def show_interest_warmup_menu(update: Update, context: CallbackContext) -> None:
    """Показать главное меню прогрева по интересам"""
    query = update.callback_query
    query.answer()
    
    keyboard = [
        [InlineKeyboardButton("🎯 Настроить интересы", callback_data="setup_interests")],
        [InlineKeyboardButton("📊 Шаблоны интересов", callback_data="interest_templates")],
        [InlineKeyboardButton("⚙️ Текущие настройки", callback_data="current_interest_settings")],
        [InlineKeyboardButton("🚀 Тест прогрева по интересам", callback_data="test_interest_warmup")],
        [InlineKeyboardButton("🔙 Назад к прогреву", callback_data="warmup_menu")]
    ]
    
    text = (
        "🎯 ПРОГРЕВ ПО ИНТЕРЕСАМ\n\n"
        "Умная система прогрева, которая:\n"
        "• 🔍 Изучает контент по вашим интересам\n"
        "• 👥 Взаимодействует с целевой аудиторией\n"
        "• 🎯 Формирует алгоритмические предпочтения\n"
        "• 📈 Улучшает органический охват\n\n"
        "Выберите действие:"
    )
    
    query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)


def show_interest_templates(update: Update, context: CallbackContext) -> None:
    """Показать готовые шаблоны интересов"""
    query = update.callback_query
    query.answer()
    
    templates = {
        'fitness': {
            'name': '💪 Фитнес и спорт',
            'description': 'Тренировки, мотивация, здоровый образ жизни',
            'interests': ['fitness', 'health']
        },
        'food': {
            'name': '🍕 Еда и кулинария',
            'description': 'Рецепты, рестораны, гастрономия',
            'interests': ['food']
        },
        'travel': {
            'name': '✈️ Путешествия',
            'description': 'Туризм, природа, приключения',
            'interests': ['travel', 'photography']
        },
        'business': {
            'name': '💼 Бизнес',
            'description': 'Предпринимательство, мотивация, финансы',
            'interests': ['business', 'finance']
        },
        'technology': {
            'name': '💻 Технологии',
            'description': 'IT, гаджеты, инновации',
            'interests': ['technology']
        },
        'lifestyle': {
            'name': '🌟 Лайфстайл',
            'description': 'Мода, красота, развлечения',
            'interests': ['lifestyle', 'fashion', 'beauty']
        }
    }
    
    keyboard = []
    for template_key, template_data in templates.items():
        keyboard.append([InlineKeyboardButton(
            template_data['name'], 
            callback_data=f"select_template_{template_key}"
        )])
    
    keyboard.append([InlineKeyboardButton("🛠 Настроить вручную", callback_data="manual_interests")])
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="interest_warmup_menu")])
    
    text = (
        "📊 ШАБЛОНЫ ИНТЕРЕСОВ\n\n"
        "Выберите готовый шаблон или настройте вручную:\n\n"
    )
    
    for template_data in templates.values():
        text += f"{template_data['name']}\n{template_data['description']}\n\n"
    
    query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)


def select_interest_template(update: Update, context: CallbackContext) -> None:
    """Выбор шаблона интересов"""
    query = update.callback_query
    query.answer()
    
    template_name = query.data.replace('select_template_', '')
    user_id = query.from_user.id
    
    # Шаблоны интересов
    templates = {
        'fitness': {
            'primary_interests': ['fitness'],
            'secondary_interests': ['health', 'motivation'],
            'custom_hashtags': ['gym', 'workout', 'training', 'bodybuilding'],
            'custom_accounts': ['therock', 'nike', 'underarmour']
        },
        'food': {
            'primary_interests': ['food'],
            'secondary_interests': ['lifestyle'],
            'custom_hashtags': ['recipe', 'cooking', 'chef', 'delicious'],
            'custom_accounts': ['gordongram', 'jamieoliver', 'buzzfeedtasty']
        },
        'travel': {
            'primary_interests': ['travel'],
            'secondary_interests': ['photography', 'lifestyle'],
            'custom_hashtags': ['wanderlust', 'adventure', 'explore', 'nature'],
            'custom_accounts': ['natgeo', 'beautifuldestinations', 'lonelyplanet']
        },
        'business': {
            'primary_interests': ['business'],
            'secondary_interests': ['finance', 'motivation'],
            'custom_hashtags': ['entrepreneur', 'startup', 'success', 'leadership'],
            'custom_accounts': ['garyvee', 'forbes', 'entrepreneur']
        },
        'technology': {
            'primary_interests': ['technology'],
            'secondary_interests': ['business'],
            'custom_hashtags': ['innovation', 'ai', 'startup', 'coding'],
            'custom_accounts': ['elonmusk', 'apple', 'techcrunch']
        },
        'lifestyle': {
            'primary_interests': ['lifestyle'],
            'secondary_interests': ['fashion', 'beauty'],
            'custom_hashtags': ['style', 'trend', 'inspiration', 'mood'],
            'custom_accounts': ['voguemagazine', 'harpersbazaar']
        }
    }
    
    if template_name in templates:
        # Сохраняем настройки шаблона
        user_interest_settings[user_id] = templates[template_name].copy()
        
        # Показываем предварительный просмотр
        settings = user_interest_settings[user_id]
        
        text = (
            f"✅ Выбран шаблон: {template_name.upper()}\n\n"
            f"🎯 Основные интересы: {', '.join(settings['primary_interests'])}\n"
            f"🔸 Дополнительные: {', '.join(settings['secondary_interests'])}\n"
            f"🏷 Хештеги: {', '.join(settings['custom_hashtags'][:5])}{'...' if len(settings['custom_hashtags']) > 5 else ''}\n"
            f"👤 Аккаунты: {', '.join(settings['custom_accounts'])}\n\n"
            "Сохранить эти настройки?"
        )
        
        keyboard = [
            [InlineKeyboardButton("✅ Сохранить", callback_data="save_interest_settings")],
            [InlineKeyboardButton("✏️ Редактировать", callback_data="edit_interest_settings")],
            [InlineKeyboardButton("🔙 Выбрать другой", callback_data="interest_templates")]
        ]
        
        query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)


def show_manual_interests_setup(update: Update, context: CallbackContext) -> int:
    """Показать меню ручной настройки интересов"""
    query = update.callback_query
    query.answer()
    
    # Список всех доступных интересов
    all_interests = [category.value for category in InterestCategory]
    
    # Создаем клавиатуру с интересами (по 2 в ряд)
    keyboard = []
    for i in range(0, len(all_interests), 2):
        row = []
        for j in range(i, min(i + 2, len(all_interests))):
            interest = all_interests[j]
            row.append(InlineKeyboardButton(
                f"🔸 {interest.replace('_', ' ').title()}", 
                callback_data=f"toggle_interest_{interest}"
            ))
        keyboard.append(row)
    
    keyboard.append([InlineKeyboardButton("✅ Далее", callback_data="interests_next_step")])
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="interest_templates")])
    
    # Инициализируем настройки пользователя
    user_id = query.from_user.id
    if user_id not in user_interest_settings:
        user_interest_settings[user_id] = {
            'primary_interests': [],
            'secondary_interests': [],
            'custom_hashtags': [],
            'custom_accounts': []
        }
    
    text = (
        "🛠 РУЧНАЯ НАСТРОЙКА ИНТЕРЕСОВ\n\n"
        "Выберите 2-3 основных интереса для прогрева:\n\n"
        "Выбрано: пока ничего\n\n"
        "Нажимайте на интересы для выбора/снятия выбора:"
    )
    
    query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    return INTEREST_SETUP


def toggle_interest_selection(update: Update, context: CallbackContext) -> int:
    """Переключить выбор интереса"""
    query = update.callback_query
    query.answer()
    
    interest = query.data.replace('toggle_interest_', '')
    user_id = query.from_user.id
    
    if user_id not in user_interest_settings:
        user_interest_settings[user_id] = {
            'primary_interests': [],
            'secondary_interests': [],
            'custom_hashtags': [],
            'custom_accounts': []
        }
    
    settings = user_interest_settings[user_id]
    
    # Переключаем интерес
    if interest in settings['primary_interests']:
        settings['primary_interests'].remove(interest)
    elif interest in settings['secondary_interests']:
        settings['secondary_interests'].remove(interest)
    else:
        # Добавляем как основной, если основных меньше 3
        if len(settings['primary_interests']) < 3:
            settings['primary_interests'].append(interest)
        else:
            # Иначе добавляем как дополнительный
            if len(settings['secondary_interests']) < 5:
                settings['secondary_interests'].append(interest)
            else:
                query.answer("Максимум 3 основных и 5 дополнительных интересов!", show_alert=True)
                return INTEREST_SETUP
    
    # Обновляем клавиатуру
    all_interests = [category.value for category in InterestCategory]
    keyboard = []
    
    for i in range(0, len(all_interests), 2):
        row = []
        for j in range(i, min(i + 2, len(all_interests))):
            current_interest = all_interests[j]
            
            # Определяем статус интереса
            if current_interest in settings['primary_interests']:
                prefix = "🟢"  # Основной
            elif current_interest in settings['secondary_interests']:
                prefix = "🟡"  # Дополнительный
            else:
                prefix = "⚪"  # Не выбран
            
            row.append(InlineKeyboardButton(
                f"{prefix} {current_interest.replace('_', ' ').title()}", 
                callback_data=f"toggle_interest_{current_interest}"
            ))
        keyboard.append(row)
    
    keyboard.append([InlineKeyboardButton("✅ Далее", callback_data="interests_next_step")])
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="interest_templates")])
    
    # Обновляем текст
    primary_text = ", ".join(settings['primary_interests']) if settings['primary_interests'] else "нет"
    secondary_text = ", ".join(settings['secondary_interests']) if settings['secondary_interests'] else "нет"
    
    text = (
        "🛠 РУЧНАЯ НАСТРОЙКА ИНТЕРЕСОВ\n\n"
        "🟢 - Основные интересы (макс. 3)\n"
        "🟡 - Дополнительные интересы (макс. 5)\n"
        "⚪ - Не выбрано\n\n"
        f"🎯 Основные: {primary_text}\n"
        f"🔸 Дополнительные: {secondary_text}\n\n"
        "Нажимайте на интересы для выбора/снятия выбора:"
    )
    
    query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    return INTEREST_SETUP


def interests_next_step(update: Update, context: CallbackContext) -> int:
    """Переход к настройке хештегов"""
    query = update.callback_query
    query.answer()
    
    user_id = query.from_user.id
    settings = user_interest_settings.get(user_id, {})
    
    if not settings.get('primary_interests'):
        query.answer("Выберите хотя бы один основной интерес!", show_alert=True)
        return INTEREST_SETUP
    
    text = (
        "🏷 ДОПОЛНИТЕЛЬНЫЕ ХЕШТЕГИ\n\n"
        "Введите дополнительные хештеги для прогрева (необязательно).\n"
        "Каждый хештег с новой строки, без символа #:\n\n"
        "Пример:\n"
        "motivation\n"
        "success\n"
        "goals\n\n"
        "Или отправьте /skip чтобы пропустить:"
    )
    
    keyboard = [
        [InlineKeyboardButton("⏭ Пропустить", callback_data="skip_hashtags")],
        [InlineKeyboardButton("🔙 Назад", callback_data="manual_interests")]
    ]
    
    query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    return CUSTOM_HASHTAGS


def process_custom_hashtags(update: Update, context: CallbackContext) -> int:
    """Обработка пользовательских хештегов"""
    user_id = update.effective_user.id
    text = update.message.text
    
    if text == '/skip':
        return ask_custom_accounts(update, context)
    
    # Парсим хештеги
    hashtags = [tag.strip().replace('#', '') for tag in text.split('\n') if tag.strip()]
    
    if not hashtags:
        update.message.reply_text("❌ Не введено ни одного хештега. Попробуйте еще раз или отправьте /skip")
        return CUSTOM_HASHTAGS
    
    # Сохраняем хештеги
    if user_id not in user_interest_settings:
        user_interest_settings[user_id] = {}
    
    user_interest_settings[user_id]['custom_hashtags'] = hashtags[:20]  # Максимум 20
    
    update.message.reply_text(f"✅ Добавлено {len(hashtags)} хештегов")
    
    return ask_custom_accounts(update, context)


def ask_custom_accounts(update: Update, context: CallbackContext) -> int:
    """Запросить пользовательские аккаунты"""
    text = (
        "👤 ДОПОЛНИТЕЛЬНЫЕ АККАУНТЫ\n\n"
        "Введите дополнительные аккаунты для изучения (необязательно).\n"
        "Каждый аккаунт с новой строки, без символа @:\n\n"
        "Пример:\n"
        "elonmusk\n"
        "garyvee\n"
        "motivation\n\n"
        "Или отправьте /skip чтобы пропустить:"
    )
    
    keyboard = [
        [InlineKeyboardButton("⏭ Пропустить", callback_data="skip_accounts")],
        [InlineKeyboardButton("🔙 Назад", callback_data="back_to_hashtags")]
    ]
    
    if hasattr(update, 'callback_query') and update.callback_query:
        update.callback_query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    else:
        update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    
    return CUSTOM_ACCOUNTS


def process_custom_accounts(update: Update, context: CallbackContext) -> int:
    """Обработка пользовательских аккаунтов"""
    user_id = update.effective_user.id
    text = update.message.text
    
    if text == '/skip':
        return finish_interest_setup(update, context)
    
    # Парсим аккаунты
    accounts = [acc.strip().replace('@', '') for acc in text.split('\n') if acc.strip()]
    
    if not accounts:
        update.message.reply_text("❌ Не введено ни одного аккаунта. Попробуйте еще раз или отправьте /skip")
        return CUSTOM_ACCOUNTS
    
    # Сохраняем аккаунты
    if user_id not in user_interest_settings:
        user_interest_settings[user_id] = {}
    
    user_interest_settings[user_id]['custom_accounts'] = accounts[:10]  # Максимум 10
    
    update.message.reply_text(f"✅ Добавлено {len(accounts)} аккаунтов")
    
    return finish_interest_setup(update, context)


def finish_interest_setup(update: Update, context: CallbackContext) -> int:
    """Завершение настройки интересов"""
    user_id = update.effective_user.id
    settings = user_interest_settings.get(user_id, {})
    
    # Формируем итоговый текст
    text = (
        "✅ НАСТРОЙКА ЗАВЕРШЕНА\n\n"
        f"🎯 Основные интересы: {', '.join(settings.get('primary_interests', []))}\n"
        f"🔸 Дополнительные: {', '.join(settings.get('secondary_interests', []))}\n"
        f"🏷 Хештеги: {len(settings.get('custom_hashtags', []))}\n"
        f"👤 Аккаунты: {len(settings.get('custom_accounts', []))}\n\n"
        "Сохранить эти настройки?"
    )
    
    keyboard = [
        [InlineKeyboardButton("✅ Сохранить", callback_data="save_interest_settings")],
        [InlineKeyboardButton("✏️ Редактировать", callback_data="edit_interest_settings")],
        [InlineKeyboardButton("🔙 Начать заново", callback_data="manual_interests")]
    ]
    
    if hasattr(update, 'callback_query') and update.callback_query:
        update.callback_query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    else:
        update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    
    return ConversationHandler.END


def save_interest_settings(update: Update, context: CallbackContext) -> None:
    """Сохранить настройки интересов"""
    query = update.callback_query
    query.answer()
    
    user_id = query.from_user.id
    settings = user_interest_settings.get(user_id, {})
    
    if not settings:
        query.edit_message_text("❌ Настройки не найдены. Попробуйте заново.", parse_mode=None)
        return
    
    # Сохраняем в файл (можно заменить на базу данных)
    try:
        # Загружаем существующие настройки
        try:
            with open('interest_warmup_settings.json', 'r', encoding='utf-8') as f:
                all_settings = json.load(f)
        except FileNotFoundError:
            all_settings = {}
        
        # Добавляем настройки пользователя
        all_settings[str(user_id)] = settings
        
        # Сохраняем обратно
        with open('interest_warmup_settings.json', 'w', encoding='utf-8') as f:
            json.dump(all_settings, f, ensure_ascii=False, indent=2)
        
        # Очищаем временные настройки
        if user_id in user_interest_settings:
            del user_interest_settings[user_id]
        
        text = (
            "✅ Настройки сохранены!\n\n"
            "Теперь при запуске прогрева будет использоваться "
            "умная система по интересам.\n\n"
            "🎯 Система будет:\n"
            "• Изучать контент по вашим интересам\n"
            "• Взаимодействовать с целевой аудиторией\n"
            "• Формировать алгоритмические предпочтения\n"
            "• Улучшать органический охват"
        )
        
        keyboard = [
            [InlineKeyboardButton("🚀 Тест прогрева", callback_data="test_interest_warmup")],
            [InlineKeyboardButton("🔙 К меню прогрева", callback_data="warmup_menu")]
        ]
        
        query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
        
    except Exception as e:
        logger.error(f"Ошибка сохранения настроек интересов: {e}")
        query.edit_message_text(
            "❌ Ошибка при сохранении настроек. Попробуйте позже.",
            parse_mode=None
        )


def get_interest_conversation_handler():
    """Получить ConversationHandler для настройки интересов"""
    return ConversationHandler(
        entry_points=[
            CallbackQueryHandler(show_manual_interests_setup, pattern='^manual_interests$')
        ],
        states={
            INTEREST_SETUP: [
                CallbackQueryHandler(toggle_interest_selection, pattern='^toggle_interest_'),
                CallbackQueryHandler(interests_next_step, pattern='^interests_next_step$'),
            ],
            CUSTOM_HASHTAGS: [
                MessageHandler(Filters.text & ~Filters.command, process_custom_hashtags),
                CallbackQueryHandler(ask_custom_accounts, pattern='^skip_hashtags$'),
            ],
            CUSTOM_ACCOUNTS: [
                MessageHandler(Filters.text & ~Filters.command, process_custom_accounts),
                CallbackQueryHandler(finish_interest_setup, pattern='^skip_accounts$'),
            ],
        },
        fallbacks=[
            CallbackQueryHandler(show_interest_warmup_menu, pattern='^interest_warmup_menu$'),
        ],
        name="interest_warmup_conversation",
        persistent=False,
    )


# Список всех обработчиков для регистрации в главном боте
INTEREST_WARMUP_HANDLERS = [
    CallbackQueryHandler(show_interest_warmup_menu, pattern='^interest_warmup_menu$'),
    CallbackQueryHandler(show_interest_templates, pattern='^interest_templates$'),
    CallbackQueryHandler(select_interest_template, pattern='^select_template_'),
    CallbackQueryHandler(save_interest_settings, pattern='^save_interest_settings$'),
    get_interest_conversation_handler(),
] 
 
 
 
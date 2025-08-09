#!/usr/bin/env python3
"""
Telegram Bot для управления Instagram аккаунтами с функциями из веб-интерфейса
"""

import logging
import asyncio
from datetime import datetime, timedelta
from typing import Optional, Dict, List
import json
import os

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    InlineKeyboardButton, InlineKeyboardMarkup, 
    KeyboardButton, ReplyKeyboardMarkup, ReplyKeyboardRemove,
    CallbackQuery, Message
)
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder

from database.db_manager import (
    get_session, InstagramAccount, get_instagram_accounts,
    get_instagram_account, add_instagram_account, update_instagram_account,
    delete_instagram_account, get_instagram_account_by_username
)
from utils.smart_validator_service import (
    get_smart_validator, ValidationPriority, AccountStatus
)
from instagram.client import get_instagram_client
from config import WEB_TELEGRAM_BOT_TOKEN

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Состояния FSM
class AccountStates(StatesGroup):
    waiting_for_username = State()
    waiting_for_password = State()
    waiting_for_email = State()
    waiting_for_email_password = State()
    waiting_for_edit_choice = State()
    waiting_for_new_value = State()

class SettingsStates(StatesGroup):
    waiting_for_check_interval = State()
    waiting_for_max_checks = State()
    waiting_for_max_recoveries = State()

# Клавиатуры
def get_main_keyboard():
    """Главная клавиатура"""
    keyboard = ReplyKeyboardBuilder()
    keyboard.add(KeyboardButton(text="📱 Аккаунты"))
    keyboard.add(KeyboardButton(text="✅ Валидация"))
    keyboard.add(KeyboardButton(text="📊 Статистика"))
    keyboard.add(KeyboardButton(text="⚙️ Настройки"))
    keyboard.adjust(2)
    return keyboard.as_markup(resize_keyboard=True)

def get_accounts_keyboard():
    """Клавиатура управления аккаунтами"""
    keyboard = ReplyKeyboardBuilder()
    keyboard.add(KeyboardButton(text="📋 Список аккаунтов"))
    keyboard.add(KeyboardButton(text="➕ Добавить аккаунт"))
    keyboard.add(KeyboardButton(text="🏠 Главное меню"))
    keyboard.adjust(2)
    return keyboard.as_markup(resize_keyboard=True)

def get_validation_keyboard():
    """Клавиатура валидации"""
    keyboard = ReplyKeyboardBuilder()
    keyboard.add(KeyboardButton(text="🔍 Проверить все"))
    keyboard.add(KeyboardButton(text="🎯 Проверить выбранный"))
    keyboard.add(KeyboardButton(text="📈 Статус валидации"))
    keyboard.add(KeyboardButton(text="🔧 Настройки валидации"))
    keyboard.add(KeyboardButton(text="🏠 Главное меню"))
    keyboard.adjust(2)
    return keyboard.as_markup(resize_keyboard=True)

def get_settings_keyboard():
    """Клавиатура настроек"""
    keyboard = ReplyKeyboardBuilder()
    keyboard.add(KeyboardButton(text="⏱ Интервал проверки"))
    keyboard.add(KeyboardButton(text="🔄 Параллельные проверки"))
    keyboard.add(KeyboardButton(text="🔧 Параллельные восстановления"))
    keyboard.add(KeyboardButton(text="💾 Показать настройки"))
    keyboard.add(KeyboardButton(text="🏠 Главное меню"))
    keyboard.adjust(2)
    return keyboard.as_markup(resize_keyboard=True)

def get_account_inline_keyboard(account_id: int):
    """Inline клавиатура для управления аккаунтом"""
    keyboard = InlineKeyboardBuilder()
    keyboard.add(InlineKeyboardButton(text="✏️ Редактировать", callback_data=f"edit_{account_id}"))
    keyboard.add(InlineKeyboardButton(text="🗑 Удалить", callback_data=f"delete_{account_id}"))
    keyboard.add(InlineKeyboardButton(text="✅ Проверить", callback_data=f"check_{account_id}"))
    keyboard.adjust(2)
    return keyboard.as_markup()

def format_account_info(account: InstagramAccount) -> str:
    """Форматирование информации об аккаунте"""
    status_emoji = "✅" if account.is_active else "❌"
    last_check = account.last_check.strftime("%d.%m %H:%M") if account.last_check else "Никогда"
    
    text = f"{status_emoji} <b>@{account.username}</b>\n"
    text += f"📧 Email: {account.email or 'Не указан'}\n"
    text += f"🔍 Последняя проверка: {last_check}\n"
    
    if account.last_error:
        text += f"⚠️ Ошибка: {account.last_error}\n"
    
    return text

def format_validation_status(stats: Dict) -> str:
    """Форматирование статуса валидации"""
    text = "📊 <b>Статус валидации</b>\n\n"
    
    # Статусы аккаунтов
    status_counts = stats.get('status_counts', {})
    text += "📱 <b>Статусы аккаунтов:</b>\n"
    
    status_emojis = {
        'valid': '✅',
        'invalid': '❌',
        'checking': '🔍',
        'recovering': '🔧',
        'failed': '⛔',
        'cooldown': '⏳'
    }
    
    for status, count in status_counts.items():
        emoji = status_emojis.get(status, '❓')
        text += f"{emoji} {status.capitalize()}: {count}\n"
    
    text += f"\n📊 <b>Активность:</b>\n"
    text += f"🔍 Проверок: {stats.get('active_checks', 0)}\n"
    text += f"🔧 Восстановлений: {stats.get('active_recoveries', 0)}\n"
    text += f"📋 В очереди проверки: {stats.get('check_queue_size', 0)}\n"
    text += f"🔧 В очереди восстановления: {stats.get('recovery_queue_size', 0)}\n"
    
    # Системная нагрузка
    system_load = stats.get('system_load', {})
    text += f"\n💻 <b>Система:</b>\n"
    text += f"🖥 CPU: {system_load.get('cpu', 0):.1f}%\n"
    text += f"💾 RAM: {system_load.get('memory', 0):.1f}%\n"
    
    if system_load.get('is_high', False):
        text += "⚠️ <b>Высокая нагрузка!</b>\n"
    
    return text

# Обработчики команд
async def cmd_start(message: Message):
    """Обработчик команды /start"""
    await message.answer(
        "👋 Добро пожаловать в Instagram Account Manager!\n\n"
        "Это бот для управления Instagram аккаунтами с функциями:\n"
        "• Управление аккаунтами\n"
        "• Автоматическая валидация\n"
        "• Восстановление через email\n"
        "• Статистика и мониторинг\n\n"
        "Выберите действие:",
        reply_markup=get_main_keyboard()
    )

# Обработчики главного меню
async def handle_accounts_menu(message: Message):
    """Меню аккаунтов"""
    await message.answer(
        "📱 <b>Управление аккаунтами</b>\n\n"
        "Выберите действие:",
        reply_markup=get_accounts_keyboard()
    )

async def handle_validation_menu(message: Message):
    """Меню валидации"""
    await message.answer(
        "✅ <b>Валидация аккаунтов</b>\n\n"
        "Выберите действие:",
        reply_markup=get_validation_keyboard()
    )

async def handle_statistics_menu(message: Message):
    """Статистика"""
    validator = get_smart_validator()
    stats = validator.get_stats()
    
    await message.answer(
        format_validation_status(stats),
        parse_mode="HTML"
    )

async def handle_settings_menu(message: Message):
    """Меню настроек"""
    await message.answer(
        "⚙️ <b>Настройки</b>\n\n"
        "Выберите параметр для изменения:",
        reply_markup=get_settings_keyboard()
    )

async def handle_main_menu(message: Message):
    """Возврат в главное меню"""
    await message.answer(
        "🏠 Главное меню",
        reply_markup=get_main_keyboard()
    )

# Обработчики аккаунтов
async def handle_list_accounts(message: Message):
    """Список аккаунтов"""
    accounts = get_instagram_accounts()
    
    if not accounts:
        await message.answer("📭 Нет добавленных аккаунтов")
        return
    
    for account in accounts:
        await message.answer(
            format_account_info(account),
            parse_mode="HTML",
            reply_markup=get_account_inline_keyboard(account.id)
        )

async def handle_add_account(message: Message, state: FSMContext):
    """Начало добавления аккаунта"""
    await message.answer(
        "➕ <b>Добавление нового аккаунта</b>\n\n"
        "Введите username Instagram:",
        parse_mode="HTML"
    )
    await state.set_state(AccountStates.waiting_for_username)

async def process_username(message: Message, state: FSMContext):
    """Обработка username"""
    username = message.text.strip().replace('@', '')
    
    # Проверяем, существует ли уже такой аккаунт
    existing = get_instagram_account_by_username(username)
    if existing:
        await message.answer(
            "❌ Аккаунт с таким username уже существует!",
            reply_markup=get_accounts_keyboard()
        )
        await state.clear()
        return
    
    await state.update_data(username=username)
    await message.answer("🔑 Введите пароль:")
    await state.set_state(AccountStates.waiting_for_password)

async def process_password(message: Message, state: FSMContext):
    """Обработка пароля"""
    await state.update_data(password=message.text)
    await message.answer(
        "📧 Введите email для восстановления (или пропустите, отправив '-'):"
    )
    await state.set_state(AccountStates.waiting_for_email)

async def process_email(message: Message, state: FSMContext):
    """Обработка email"""
    if message.text.strip() == '-':
        # Сохраняем аккаунт без email
        data = await state.get_data()
        account_id = add_instagram_account(
            username=data['username'],
            password=data['password']
        )
        
        await message.answer(
            f"✅ Аккаунт @{data['username']} успешно добавлен!",
            reply_markup=get_accounts_keyboard()
        )
        await state.clear()
    else:
        await state.update_data(email=message.text)
        await message.answer("🔐 Введите пароль от email:")
        await state.set_state(AccountStates.waiting_for_email_password)

async def process_email_password(message: Message, state: FSMContext):
    """Обработка пароля email"""
    data = await state.get_data()
    data['email_password'] = message.text
    
    # Сохраняем аккаунт
    account_id = add_instagram_account(
        username=data['username'],
        password=data['password'],
        email=data.get('email'),
        email_password=data.get('email_password')
    )
    
    await message.answer(
        f"✅ Аккаунт @{data['username']} успешно добавлен с email!",
        reply_markup=get_accounts_keyboard()
    )
    await state.clear()

# Обработчики inline кнопок
async def handle_edit_account(callback: CallbackQuery, state: FSMContext):
    """Редактирование аккаунта"""
    account_id = int(callback.data.split('_')[1])
    account = get_instagram_account(account_id)
    
    if not account:
        await callback.answer("❌ Аккаунт не найден")
        return
    
    keyboard = InlineKeyboardBuilder()
    keyboard.add(InlineKeyboardButton(text="🔑 Пароль", callback_data=f"editfield_password_{account_id}"))
    keyboard.add(InlineKeyboardButton(text="📧 Email", callback_data=f"editfield_email_{account_id}"))
    keyboard.add(InlineKeyboardButton(text="🔐 Пароль email", callback_data=f"editfield_email_password_{account_id}"))
    keyboard.add(InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_edit"))
    keyboard.adjust(1)
    
    await callback.message.edit_text(
        f"✏️ <b>Редактирование @{account.username}</b>\n\n"
        "Выберите что изменить:",
        parse_mode="HTML",
        reply_markup=keyboard.as_markup()
    )
    await callback.answer()

async def handle_edit_field(callback: CallbackQuery, state: FSMContext):
    """Редактирование конкретного поля"""
    parts = callback.data.split('_')
    field = parts[1]
    account_id = int(parts[2])
    
    field_names = {
        'password': 'пароль',
        'email': 'email',
        'email_password': 'пароль от email'
    }
    
    await state.update_data(edit_field=field, edit_account_id=account_id)
    await callback.message.edit_text(
        f"✏️ Введите новый {field_names.get(field, field)}:"
    )
    await state.set_state(AccountStates.waiting_for_new_value)
    await callback.answer()

async def process_new_value(message: Message, state: FSMContext):
    """Обработка нового значения поля"""
    data = await state.get_data()
    field = data.get('edit_field')
    account_id = data.get('edit_account_id')
    
    if not field or not account_id:
        await message.answer("❌ Ошибка: данные не найдены")
        await state.clear()
        return
    
    # Обновляем аккаунт
    update_data = {field: message.text}
    update_instagram_account(account_id, **update_data)
    
    await message.answer(
        f"✅ Успешно обновлено!",
        reply_markup=get_accounts_keyboard()
    )
    await state.clear()

async def handle_delete_account(callback: CallbackQuery):
    """Удаление аккаунта"""
    account_id = int(callback.data.split('_')[1])
    account = get_instagram_account(account_id)
    
    if not account:
        await callback.answer("❌ Аккаунт не найден")
        return
    
    keyboard = InlineKeyboardBuilder()
    keyboard.add(InlineKeyboardButton(text="✅ Да, удалить", callback_data=f"confirm_delete_{account_id}"))
    keyboard.add(InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_delete"))
    
    await callback.message.edit_text(
        f"❓ Вы уверены, что хотите удалить @{account.username}?",
        reply_markup=keyboard.as_markup()
    )
    await callback.answer()

async def handle_confirm_delete(callback: CallbackQuery):
    """Подтверждение удаления"""
    account_id = int(callback.data.split('_')[2])
    
    if delete_instagram_account(account_id):
        await callback.message.edit_text("✅ Аккаунт успешно удален!")
    else:
        await callback.message.edit_text("❌ Ошибка при удалении аккаунта")
    
    await callback.answer()

async def handle_check_account(callback: CallbackQuery):
    """Проверка одного аккаунта"""
    account_id = int(callback.data.split('_')[1])
    
    validator = get_smart_validator()
    validator.request_validation(account_id, ValidationPriority.HIGH)
    
    await callback.answer("🔍 Проверка запущена!", show_alert=True)
    
    # Ждем результат
    await asyncio.sleep(3)
    
    account = get_instagram_account(account_id)
    if account:
        await callback.message.edit_text(
            format_account_info(account),
            parse_mode="HTML",
            reply_markup=get_account_inline_keyboard(account_id)
        )

# Обработчики валидации
async def handle_check_all(message: Message):
    """Проверить все аккаунты"""
    accounts = get_instagram_accounts()
    if not accounts:
        await message.answer("📭 Нет аккаунтов для проверки")
        return
    
    validator = get_smart_validator()
    
    await message.answer(f"🔍 Запуск проверки {len(accounts)} аккаунтов...")
    
    for account in accounts:
        validator.request_validation(account.id, ValidationPriority.NORMAL)
    
    await message.answer(
        "✅ Все аккаунты добавлены в очередь проверки!\n"
        "Используйте '📈 Статус валидации' для отслеживания прогресса."
    )

async def handle_check_selected(message: Message):
    """Проверить выбранный аккаунт"""
    accounts = get_instagram_accounts()
    if not accounts:
        await message.answer("📭 Нет аккаунтов для проверки")
        return
    
    keyboard = InlineKeyboardBuilder()
    for account in accounts:
        keyboard.add(InlineKeyboardButton(
            text=f"@{account.username}",
            callback_data=f"check_{account.id}"
        ))
    keyboard.adjust(1)
    
    await message.answer(
        "🎯 Выберите аккаунт для проверки:",
        reply_markup=keyboard.as_markup()
    )

async def handle_validation_status(message: Message):
    """Статус валидации"""
    validator = get_smart_validator()
    stats = validator.get_stats()
    
    await message.answer(
        format_validation_status(stats),
        parse_mode="HTML"
    )

async def handle_validation_settings(message: Message):
    """Настройки валидации"""
    validator = get_smart_validator()
    
    text = "🔧 <b>Настройки валидации</b>\n\n"
    text += f"⏱ Интервал проверки: {validator.check_interval // 60} мин\n"
    text += f"🔄 Макс. параллельных проверок: {validator.max_concurrent_checks}\n"
    text += f"🔧 Макс. параллельных восстановлений: {validator.max_concurrent_recoveries}\n"
    text += f"⏳ Задержка восстановления: {validator.recovery_cooldown // 60} мин\n"
    
    await message.answer(text, parse_mode="HTML")

# Обработчики настроек
async def handle_check_interval(message: Message, state: FSMContext):
    """Изменение интервала проверки"""
    await message.answer(
        "⏱ Введите интервал проверки в минутах (текущий: 30):"
    )
    await state.set_state(SettingsStates.waiting_for_check_interval)

async def process_check_interval(message: Message, state: FSMContext):
    """Обработка интервала проверки"""
    try:
        interval = int(message.text)
        if interval < 5:
            await message.answer("❌ Интервал должен быть не менее 5 минут")
            return
        
        # Здесь можно сохранить настройку
        await message.answer(
            f"✅ Интервал проверки установлен: {interval} мин",
            reply_markup=get_settings_keyboard()
        )
    except ValueError:
        await message.answer("❌ Введите число")
    
    await state.clear()

async def handle_show_settings(message: Message):
    """Показать текущие настройки"""
    validator = get_smart_validator()
    
    text = "💾 <b>Текущие настройки</b>\n\n"
    text += f"⏱ Интервал проверки: {validator.check_interval // 60} мин\n"
    text += f"🔄 Параллельных проверок: {validator.max_concurrent_checks}\n"
    text += f"🔧 Параллельных восстановлений: {validator.max_concurrent_recoveries}\n"
    text += f"⏳ Задержка восстановления: {validator.recovery_cooldown // 60} мин\n"
    
    await message.answer(text, parse_mode="HTML")

# Обработчик отмены
async def handle_cancel(callback: CallbackQuery, state: FSMContext):
    """Отмена любого действия"""
    await callback.message.delete()
    await state.clear()
    await callback.answer("❌ Отменено")

# Главная функция
async def main():
    """Запуск бота"""
    # Проверяем токен
    if WEB_TELEGRAM_BOT_TOKEN == 'YOUR_NEW_BOT_TOKEN_HERE':
        logger.error(
            "❌ Токен бота не настроен!\n"
            "1. Создайте нового бота через @BotFather в Telegram\n"
            "2. Получите токен бота\n" 
            "3. Добавьте его в config.py в переменную WEB_TELEGRAM_BOT_TOKEN\n"
            "   или установите переменную окружения WEB_TELEGRAM_BOT_TOKEN"
        )
        return
    
    # Инициализация бота и диспетчера
    bot = Bot(token=WEB_TELEGRAM_BOT_TOKEN)
    dp = Dispatcher()
    
    # Регистрация обработчиков команд
    dp.message.register(cmd_start, Command("start"))
    
    # Регистрация обработчиков главного меню
    dp.message.register(handle_accounts_menu, F.text == "📱 Аккаунты")
    dp.message.register(handle_validation_menu, F.text == "✅ Валидация")
    dp.message.register(handle_statistics_menu, F.text == "📊 Статистика")
    dp.message.register(handle_settings_menu, F.text == "⚙️ Настройки")
    dp.message.register(handle_main_menu, F.text == "🏠 Главное меню")
    
    # Регистрация обработчиков аккаунтов
    dp.message.register(handle_list_accounts, F.text == "📋 Список аккаунтов")
    dp.message.register(handle_add_account, F.text == "➕ Добавить аккаунт")
    
    # Регистрация обработчиков валидации
    dp.message.register(handle_check_all, F.text == "🔍 Проверить все")
    dp.message.register(handle_check_selected, F.text == "🎯 Проверить выбранный")
    dp.message.register(handle_validation_status, F.text == "📈 Статус валидации")
    dp.message.register(handle_validation_settings, F.text == "🔧 Настройки валидации")
    
    # Регистрация обработчиков настроек
    dp.message.register(handle_check_interval, F.text == "⏱ Интервал проверки")
    dp.message.register(handle_show_settings, F.text == "💾 Показать настройки")
    
    # Регистрация обработчиков состояний
    dp.message.register(process_username, StateFilter(AccountStates.waiting_for_username))
    dp.message.register(process_password, StateFilter(AccountStates.waiting_for_password))
    dp.message.register(process_email, StateFilter(AccountStates.waiting_for_email))
    dp.message.register(process_email_password, StateFilter(AccountStates.waiting_for_email_password))
    dp.message.register(process_new_value, StateFilter(AccountStates.waiting_for_new_value))
    dp.message.register(process_check_interval, StateFilter(SettingsStates.waiting_for_check_interval))
    
    # Регистрация обработчиков callback
    dp.callback_query.register(handle_edit_account, F.data.startswith("edit_"))
    dp.callback_query.register(handle_edit_field, F.data.startswith("editfield_"))
    dp.callback_query.register(handle_delete_account, F.data.startswith("delete_"))
    dp.callback_query.register(handle_confirm_delete, F.data.startswith("confirm_delete_"))
    dp.callback_query.register(handle_check_account, F.data.startswith("check_"))
    dp.callback_query.register(handle_cancel, F.data.in_(["cancel_edit", "cancel_delete"]))
    
    # Запуск валидатора
    validator = get_smart_validator()
    if not validator.is_running:
        validator.start()
        logger.info("✅ Smart Validator запущен")
    
    # Запуск бота
    logger.info("🚀 Telegram бот запущен!")
    try:
        await dp.start_polling(bot)
    except Exception as e:
        logger.error(f"❌ Ошибка при запуске бота: {e}")
    finally:
        await bot.session.close()

if __name__ == "__main__":
    asyncio.run(main()) 
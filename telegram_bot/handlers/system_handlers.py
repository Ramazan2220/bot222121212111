#!/usr/bin/env python3
"""
Обработчики команд для мониторинга системы
"""

import logging
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, ParseMode
from telegram.ext import CommandHandler, CallbackQueryHandler
from config import ADMIN_USER_IDS
from utils.access_manager import has_access
from utils.system_monitor import (
    get_system_status, get_adaptive_limits, get_system_load_percentage,
    set_hardware_profile, system_monitor
)

logger = logging.getLogger(__name__)

def is_admin(user_id):
    """Проверяет, является ли пользователь администратором"""
    return has_access(user_id)

def system_status_handler(update, context):
    """Обработчик команды /system_status - показывает статус системы"""
    if update.callback_query:
        query = update.callback_query
        query.answer()
        
        status = get_system_status()
        load_percentage = get_system_load_percentage()
        
        # Формируем детальный отчет
        report = f"🖥️ *СТАТУС СИСТЕМЫ*\n\n"
        report += f"📊 *Нагрузка системы:* {load_percentage}% ({status['level'].name})\n"
        report += f"🔧 *Профиль железа:* {status['hardware_profile'].upper()}\n\n"
        
        report += f"📈 *Метрики:*\n"
        report += f"• CPU: {status['metrics']['cpu']}\n"
        report += f"• RAM: {status['metrics']['memory']}\n"
        report += f"• Температура: {status['metrics']['temperature']}\n"
        report += f"• Load Average: {status['metrics']['load_avg']}\n\n"
        
        report += f"⚙️ *Текущие лимиты бота:*\n"
        report += f"• Потоков: {status['limits']['max_workers']}\n"
        report += f"• Размер группы: {status['limits']['batch_size']}\n"
        report += f"• Задержка: {status['limits']['delay_between_batches']}\n"
        report += f"• Множитель таймаута: {status['limits']['timeout_multiplier']}\n\n"
        
        report += f"💡 *Описание:* {status['level'].workload.description}"
        
        keyboard = [
            [InlineKeyboardButton("🔄 Обновить", callback_data='system_status'),
             InlineKeyboardButton("🔧 Настройки", callback_data='system_settings')],
            [InlineKeyboardButton("📊 Все уровни", callback_data='system_levels'),
             InlineKeyboardButton("🖥️ Профили", callback_data='system_profiles')],
            [InlineKeyboardButton("🔙 Назад", callback_data='main_menu')]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        query.edit_message_text(
            report,
            reply_markup=reply_markup,
            parse_mode=ParseMode.MARKDOWN
        )
    else:
        # Команда из чата
        status = get_system_status()
        load_percentage = get_system_load_percentage()
        
        report = f"🖥️ *СТАТУС СИСТЕМЫ*\n\n"
        report += f"📊 Нагрузка: {load_percentage}% ({status['level'].name})\n"
        report += f"🔧 Профиль: {status['hardware_profile'].upper()}\n"
        report += f"💻 CPU: {status['metrics']['cpu']} | RAM: {status['metrics']['memory']}\n"
        report += f"⚙️ Потоков: {status['limits']['max_workers']} | Группа: {status['limits']['batch_size']}"
        
        update.message.reply_text(report, parse_mode=ParseMode.MARKDOWN)

def system_levels_handler(update, context):
    """Показывает все уровни нагрузки системы"""
    query = update.callback_query
    query.answer()
    
    report = f"📊 *УРОВНИ НАГРУЗКИ СИСТЕМЫ*\n\n"
    
    current_load = get_system_load_percentage()
    current_level = system_monitor.get_load_level()
    
    for i, level in enumerate(system_monitor.load_levels, 1):
        # Отмечаем текущий уровень
        marker = "🔸" if level.name == current_level.name else "▫️"
        
        report += f"{marker} *{level.name}* ({level.min_load}-{level.max_load}%)\n"
        report += f"   {level.workload.description}\n"
        report += f"   Потоков: {level.workload.max_workers}, "
        report += f"Группа: {level.workload.batch_size}, "
        report += f"Задержка: {level.workload.delay_between_batches}с\n\n"
    
    report += f"📍 *Текущая нагрузка:* {current_load}% ({current_level.name})"
    
    keyboard = [
        [InlineKeyboardButton("🔄 Обновить", callback_data='system_levels')],
        [InlineKeyboardButton("🔙 К статусу", callback_data='system_status')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    query.edit_message_text(
        report,
        reply_markup=reply_markup,
        parse_mode=ParseMode.MARKDOWN
    )

def system_profiles_handler(update, context):
    """Показывает профили железа"""
    query = update.callback_query
    query.answer()
    
    current_profile = system_monitor.hardware_profile
    
    report = f"🖥️ *ПРОФИЛИ ЖЕЛЕЗА*\n\n"
    
    for profile_name, profile_data in system_monitor.hardware_profiles.items():
        # Отмечаем текущий профиль
        marker = "🔸" if profile_name == current_profile else "▫️"
        
        report += f"{marker} *{profile_name.upper()}*\n"
        report += f"   CPU вес: {profile_data['cpu_weight']:.1f} | "
        report += f"RAM вес: {profile_data['memory_weight']:.1f}\n"
        report += f"   Температура вес: {profile_data['temp_weight']:.1f} | "
        report += f"Load вес: {profile_data['load_weight']:.1f}\n"
        report += f"   Критическая температура: {profile_data['temp_critical']}°C\n\n"
    
    report += f"📍 *Текущий профиль:* {current_profile.upper()}"
    
    keyboard = [
        [InlineKeyboardButton("📱 MacBook", callback_data='set_profile_macbook'),
         InlineKeyboardButton("🖥️ Server", callback_data='set_profile_server')],
        [InlineKeyboardButton("☁️ VPS", callback_data='set_profile_vps')],
        [InlineKeyboardButton("🔙 К статусу", callback_data='system_status')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    query.edit_message_text(
        report,
        reply_markup=reply_markup,
        parse_mode=ParseMode.MARKDOWN
    )

def set_hardware_profile_handler(update, context):
    """Устанавливает профиль железа"""
    query = update.callback_query
    query.answer()
    
    # Извлекаем профиль из callback_data
    profile = query.data.replace('set_profile_', '')
    
    # Устанавливаем профиль
    set_hardware_profile(profile)
    
    # Получаем обновленный статус
    status = get_system_status()
    load_percentage = get_system_load_percentage()
    
    report = f"✅ *Профиль железа изменен на:* {profile.upper()}\n\n"
    report += f"📊 *Новая нагрузка:* {load_percentage}% ({status['level'].name})\n"
    report += f"⚙️ *Новые лимиты:*\n"
    report += f"• Потоков: {status['limits']['max_workers']}\n"
    report += f"• Размер группы: {status['limits']['batch_size']}\n"
    report += f"• Задержка: {status['limits']['delay_between_batches']}\n\n"
    report += f"💡 {status['level'].workload.description}"
    
    keyboard = [
        [InlineKeyboardButton("🖥️ Профили", callback_data='system_profiles')],
        [InlineKeyboardButton("🔙 К статусу", callback_data='system_status')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    query.edit_message_text(
        report,
        reply_markup=reply_markup,
        parse_mode=ParseMode.MARKDOWN
    )

def get_system_handlers():
    """Возвращает обработчики для системного мониторинга"""
    return [
        CommandHandler("system_status", system_status_handler),
        CallbackQueryHandler(system_levels_handler, pattern='^system_levels$'),
        CallbackQueryHandler(system_profiles_handler, pattern='^system_profiles$'),
        CallbackQueryHandler(set_hardware_profile_handler, pattern='^set_profile_'),
    ] 
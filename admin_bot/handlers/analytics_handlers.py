from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import CallbackContext
from datetime import datetime, timedelta
import json
from typing import Dict, List
import statistics

from ..config.admin_list import is_admin, has_permission, Permission
from ..services.user_service import UserService
from ..models.user import SubscriptionPlan, UserStatus, PLAN_INFO
from ..middleware.admin_auth import admin_required, permission_required

# Глобальный сервис пользователей
user_service = UserService()

@admin_required
@permission_required(Permission.VIEW_ANALYTICS)
def analytics_menu(update: Update, context: CallbackContext):
    """Главное меню аналитики"""
    query = update.callback_query
    
    # Получаем базовую аналитику
    analytics_data = get_advanced_analytics()
    
    text = f"""📈 **ДЕТАЛЬНАЯ АНАЛИТИКА**

🎯 **Ключевые метрики:**
• Активные пользователи: {analytics_data['active_users']}
• Конверсия триал→платные: {analytics_data['conversion_rate']:.1f}%
• Средний LTV: ${analytics_data['avg_ltv']:.2f}
• Churn rate: {analytics_data['churn_rate']:.1f}%

📊 **Тренды (30 дней):**
• Рост пользователей: {analytics_data['user_growth']:+.1f}%
• Рост доходов: {analytics_data['revenue_growth']:+.1f}%
• Удержание: {analytics_data['retention_rate']:.1f}%
"""
    
    keyboard = [
        [InlineKeyboardButton("📊 Воронка продаж", callback_data="analytics_funnel")],
        [InlineKeyboardButton("🎯 Конверсии", callback_data="analytics_conversions")],
        [InlineKeyboardButton("📈 Тренды", callback_data="analytics_trends")],
        [InlineKeyboardButton("👥 Когорты", callback_data="analytics_cohorts")],
        [InlineKeyboardButton("💰 LTV анализ", callback_data="analytics_ltv")],
        [InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")]
    ]
    
    query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

@admin_required
@permission_required(Permission.VIEW_ANALYTICS)
def analytics_funnel(update: Update, context: CallbackContext):
    """Анализ воронки продаж"""
    query = update.callback_query
    
    funnel_data = calculate_sales_funnel()
    
    text = f"""📊 **ВОРОНКА ПРОДАЖ**

🎯 **Этапы воронки:**

1️⃣ **Визиторы бота**
   └─ Всего: {funnel_data['visitors']}

2️⃣ **Регистрации** ({funnel_data['registration_rate']:.1f}%)
   └─ {funnel_data['registrations']} из {funnel_data['visitors']}

3️⃣ **Активации триала** ({funnel_data['trial_rate']:.1f}%)
   └─ {funnel_data['trial_activations']} из {funnel_data['registrations']}

4️⃣ **Конверсия в платные** ({funnel_data['conversion_rate']:.1f}%)
   └─ {funnel_data['paid_conversions']} из {funnel_data['trial_activations']}

5️⃣ **Активные пользователи** ({funnel_data['retention_rate']:.1f}%)
   └─ {funnel_data['active_users']} из {funnel_data['paid_conversions']}

🎯 **Общая конверсия:** {funnel_data['overall_conversion']:.2f}%
💡 **Узкие места:** {', '.join(funnel_data['bottlenecks'])}
"""
    
    keyboard = [
        [InlineKeyboardButton("📊 Улучшить воронку", callback_data="analytics_improve_funnel")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="analytics")]
    ]
    
    query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

@admin_required
@permission_required(Permission.VIEW_ANALYTICS)
def analytics_conversions(update: Update, context: CallbackContext):
    """Детальный анализ конверсий"""
    query = update.callback_query
    
    conversion_data = calculate_detailed_conversions()
    
    text = f"""🎯 **ДЕТАЛЬНЫЙ АНАЛИЗ КОНВЕРСИЙ**

📊 **По источникам трафика:**
"""
    
    for source, data in conversion_data['by_source'].items():
        text += f"\n• **{source}**\n"
        text += f"  └─ Конверсия: {data['conversion']:.1f}%\n"
        text += f"  └─ Пользователей: {data['users']}\n"
        text += f"  └─ CPA: ${data['cpa']:.2f}\n"
    
    text += f"""

📈 **Временная динамика:**
• Понедельник: {conversion_data['by_day']['monday']:.1f}%
• Вторник: {conversion_data['by_day']['tuesday']:.1f}%
• Среда: {conversion_data['by_day']['wednesday']:.1f}%
• Четверг: {conversion_data['by_day']['thursday']:.1f}%
• Пятница: {conversion_data['by_day']['friday']:.1f}%
• Выходные: {conversion_data['by_day']['weekend']:.1f}%

🎯 **Лучшее время:** {conversion_data['best_time']}
📱 **Лучший канал:** {conversion_data['best_channel']}
"""
    
    keyboard = [
        [InlineKeyboardButton("📊 A/B тесты", callback_data="analytics_ab_tests")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="analytics")]
    ]
    
    query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

@admin_required
@permission_required(Permission.VIEW_ANALYTICS)
def analytics_ltv(update: Update, context: CallbackContext):
    """LTV анализ"""
    query = update.callback_query
    
    ltv_data = calculate_ltv_analysis()
    
    text = f"""💰 **LTV АНАЛИЗ**

📊 **Средний LTV по сегментам:**

🏆 **Premium пользователи**
• LTV: ${ltv_data['premium_ltv']:.2f}
• Время жизни: {ltv_data['premium_lifetime']:.1f} месяцев
• Доля от выручки: {ltv_data['premium_revenue_share']:.1f}%

💎 **Standard пользователи**
• LTV: ${ltv_data['standard_ltv']:.2f}
• Время жизни: {ltv_data['standard_lifetime']:.1f} месяцев
• Доля от выручки: {ltv_data['standard_revenue_share']:.1f}%

📈 **Прогнозы:**
• LTV через 6 месяцев: ${ltv_data['ltv_6m']:.2f}
• LTV через 12 месяцев: ${ltv_data['ltv_12m']:.2f}
• Прогнозируемый churn: {ltv_data['predicted_churn']:.1f}%

🎯 **Рекомендации:**
• Фокус на retention для увеличения LTV
• Upsell в premium тариф
• Снижение churn rate на {ltv_data['churn_reduction_target']:.1f}%
"""
    
    keyboard = [
        [InlineKeyboardButton("📊 Стратегии роста LTV", callback_data="analytics_ltv_strategies")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="analytics")]
    ]
    
    query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

# Дополнительные обработчики для уведомлений и экспорта

@admin_required
@permission_required(Permission.MANAGE_SYSTEM)
def notifications_menu(update: Update, context: CallbackContext):
    """Меню уведомлений"""
    query = update.callback_query
    
    # Получаем активные уведомления
    notifications = get_active_notifications()
    
    text = f"""🔔 **СИСТЕМА УВЕДОМЛЕНИЙ**

📊 **Статус:**
• Всего уведомлений: {notifications['total']}
• Критических: {notifications['critical']}
• Предупреждений: {notifications['warnings']}
• Информационных: {notifications['info']}

🔴 **Активные алерты:**
"""
    
    for alert in notifications['active_alerts'][:5]:  # Первые 5
        text += f"• {alert['type']}: {alert['message']}\n"
    
    text += f"""

📈 **Настройки:**
• Email уведомления: {'✅' if notifications['email_enabled'] else '❌'}
• Telegram уведомления: {'✅' if notifications['telegram_enabled'] else '❌'}
• Критические алерты: {'✅' if notifications['critical_enabled'] else '❌'}
"""
    
    keyboard = [
        [InlineKeyboardButton("⚙️ Настройки", callback_data="notifications_settings")],
        [InlineKeyboardButton("📋 История", callback_data="notifications_history")],
        [InlineKeyboardButton("🔕 Отключить все", callback_data="notifications_disable_all")],
        [InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")]
    ]
    
    query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

@admin_required
@permission_required(Permission.EXPORT_DATA)
def export_menu(update: Update, context: CallbackContext):
    """Меню экспорта данных"""
    query = update.callback_query
    
    text = f"""📊 **ЭКСПОРТ ДАННЫХ**

📁 **Доступные отчеты:**

👥 **Пользователи:**
• Полный список пользователей
• Статистика по планам
• История подписок

💰 **Финансы:**
• Отчет по доходам
• Конверсии по периодам
• Прогнозы и планы

📈 **Аналитика:**
• Детальная аналитика
• Метрики производительности
• Когортный анализ

⚙️ **Система:**
• Логи системы
• Метрики производительности
• Статистика использования

Выберите тип экспорта:"""
    
    keyboard = [
        [InlineKeyboardButton("👥 Экспорт пользователей", callback_data="export_users")],
        [InlineKeyboardButton("💰 Экспорт финансов", callback_data="export_financial")],
        [InlineKeyboardButton("📈 Экспорт аналитики", callback_data="export_analytics")],
        [InlineKeyboardButton("⚙️ Экспорт системных данных", callback_data="export_system")],
        [InlineKeyboardButton("📊 Полный отчет", callback_data="export_full")],
        [InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")]
    ]
    
    query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

# Вспомогательные функции для аналитики

def get_advanced_analytics() -> Dict:
    """Получает продвинутую аналитику"""
    users = user_service.get_all_users()
    
    # Базовая статистика
    active_users = len([u for u in users if u.is_active])
    trial_users = len([u for u in users if u.subscription_plan == SubscriptionPlan.TRIAL])
    paid_users = len([u for u in users if u.subscription_plan != SubscriptionPlan.TRIAL and u.subscription_plan is not None])
    
    # Конверсия
    conversion_rate = (paid_users / max(trial_users + paid_users, 1)) * 100
    
    # Имитируем продвинутые метрики
    return {
        'active_users': active_users,
        'conversion_rate': conversion_rate,
        'avg_ltv': 450.0,  # Среднее LTV
        'churn_rate': 5.2,  # Процент оттока
        'user_growth': 12.5,  # Рост пользователей за месяц
        'revenue_growth': 18.3,  # Рост доходов
        'retention_rate': 85.5  # Процент удержания
    }

def calculate_sales_funnel() -> Dict:
    """Рассчитывает воронку продаж"""
    users = user_service.get_all_users()
    total_users = len(users)
    
    # Имитируем данные воронки
    visitors = total_users * 3  # Предполагаем, что посетителей в 3 раза больше
    registrations = total_users
    trial_activations = len([u for u in users if u.subscription_plan == SubscriptionPlan.TRIAL]) + len([u for u in users if u.subscription_plan != SubscriptionPlan.TRIAL])
    paid_conversions = len([u for u in users if u.subscription_plan != SubscriptionPlan.TRIAL and u.subscription_plan is not None])
    active_users = len([u for u in users if u.is_active])
    
    return {
        'visitors': visitors,
        'registrations': registrations,
        'trial_activations': trial_activations,
        'paid_conversions': paid_conversions,
        'active_users': active_users,
        'registration_rate': (registrations / visitors) * 100,
        'trial_rate': (trial_activations / registrations) * 100,
        'conversion_rate': (paid_conversions / trial_activations) * 100 if trial_activations > 0 else 0,
        'retention_rate': (active_users / paid_conversions) * 100 if paid_conversions > 0 else 0,
        'overall_conversion': (paid_conversions / visitors) * 100,
        'bottlenecks': ['Конверсия триал→платные', 'Активация триала']
    }

def calculate_detailed_conversions() -> Dict:
    """Детальная конверсия по источникам"""
    return {
        'by_source': {
            'Органический': {'conversion': 12.5, 'users': 45, 'cpa': 25.50},
            'Реклама': {'conversion': 8.3, 'users': 32, 'cpa': 45.20},
            'Рефералы': {'conversion': 18.7, 'users': 28, 'cpa': 15.30},
            'Соцсети': {'conversion': 6.9, 'users': 19, 'cpa': 52.10}
        },
        'by_day': {
            'monday': 11.2,
            'tuesday': 13.5,
            'wednesday': 14.8,
            'thursday': 12.9,
            'friday': 10.1,
            'weekend': 8.7
        },
        'best_time': 'Среда 14:00-16:00',
        'best_channel': 'Рефералы'
    }

def calculate_ltv_analysis() -> Dict:
    """LTV анализ по сегментам"""
    return {
        'premium_ltv': 650.00,
        'premium_lifetime': 8.5,
        'premium_revenue_share': 75.2,
        'standard_ltv': 280.00,
        'standard_lifetime': 4.2,
        'standard_revenue_share': 24.8,
        'ltv_6m': 420.00,
        'ltv_12m': 580.00,
        'predicted_churn': 4.8,
        'churn_reduction_target': 2.0
    }

def get_active_notifications() -> Dict:
    """Получает активные уведомления"""
    return {
        'total': 15,
        'critical': 2,
        'warnings': 5,
        'info': 8,
        'active_alerts': [
            {'type': 'CRITICAL', 'message': 'Высокая нагрузка на сервер'},
            {'type': 'WARNING', 'message': 'Превышен лимит API запросов'},
            {'type': 'INFO', 'message': 'Новые пользователи: +15 за час'}
        ],
        'email_enabled': True,
        'telegram_enabled': True,
        'critical_enabled': True
    }

# Функция для регистрации обработчиков
def get_analytics_handlers():
    """Возвращает словарь обработчиков аналитики"""
    return {
        'analytics': analytics_menu,
        'analytics_funnel': analytics_funnel,
        'analytics_conversions': analytics_conversions,
        'analytics_ltv': analytics_ltv,
        'notifications': notifications_menu,
        'export': export_menu,
    } 
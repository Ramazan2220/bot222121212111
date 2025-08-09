from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import CallbackContext
from datetime import datetime, timedelta
import json
from typing import Dict, List
import calendar

from ..config.admin_list import is_admin, has_permission, Permission
from ..services.user_service import UserService
from ..models.user import SubscriptionPlan, UserStatus, PLAN_INFO
from ..middleware.admin_auth import admin_required, permission_required

# Глобальный сервис пользователей
user_service = UserService()

@admin_required
@permission_required(Permission.VIEW_FINANCE)
def financial_menu(update: Update, context: CallbackContext):
    """Главное меню финансовых отчетов"""
    query = update.callback_query
    
    # Получаем базовую статистику
    stats = user_service.get_statistics()
    monthly_revenue = calculate_monthly_revenue()
    yearly_revenue = calculate_yearly_revenue()
    
    text = f"""💰 **ФИНАНСОВЫЕ ОТЧЕТЫ**

📊 **Текущая статистика:**
• Месячный доход: ${monthly_revenue:.2f}
• Годовой прогноз: ${yearly_revenue:.2f}
• Активные подписки: {stats['active_users']}
• Конверсия триала: {calculate_trial_conversion():.1f}%

💳 **Средний чек:** ${stats['estimated_revenue'] / max(stats['total_users'], 1):.2f}
"""
    
    keyboard = [
        [InlineKeyboardButton("📈 Доходы по месяцам", callback_data="financial_monthly")],
        [InlineKeyboardButton("💎 Анализ по планам", callback_data="financial_plans")],
        [InlineKeyboardButton("🔄 Конверсии", callback_data="financial_conversions")],
        [InlineKeyboardButton("📊 Прогнозы", callback_data="financial_forecasts")],
        [InlineKeyboardButton("💳 Детали платежей", callback_data="financial_payments")],
        [InlineKeyboardButton("📤 Экспорт отчета", callback_data="financial_export")],
        [InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")]
    ]
    
    query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

@admin_required
@permission_required(Permission.VIEW_FINANCE)
def financial_monthly(update: Update, context: CallbackContext):
    """Отчет по доходам по месяцам"""
    query = update.callback_query
    
    # Получаем данные за последние 12 месяцев
    monthly_data = get_monthly_revenue_data()
    
    text = "📈 **ДОХОДЫ ПО МЕСЯЦАМ**\n\n"
    
    total_year = 0
    for month_data in monthly_data:
        month_name = calendar.month_name[month_data['month']]
        revenue = month_data['revenue']
        users = month_data['users']
        total_year += revenue
        
        # Эмодзи тренда
        trend = "📈" if month_data.get('growth', 0) > 0 else "📉" if month_data.get('growth', 0) < 0 else "➡️"
        
        text += f"{trend} **{month_name} {month_data['year']}**\n"
        text += f"   💰 Доход: ${revenue:.2f}\n"
        text += f"   👥 Пользователи: {users}\n"
        text += f"   📊 ARPU: ${revenue/max(users, 1):.2f}\n\n"
    
    text += f"💎 **Итого за год:** ${total_year:.2f}"
    
    keyboard = [
        [InlineKeyboardButton("📊 График роста", callback_data="financial_growth_chart")],
        [InlineKeyboardButton("🔍 Детали месяца", callback_data="financial_month_details")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="financial")]
    ]
    
    query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

@admin_required  
@permission_required(Permission.VIEW_FINANCE)
def financial_plans(update: Update, context: CallbackContext):
    """Анализ доходов по тарифным планам"""
    query = update.callback_query
    
    plans_data = analyze_plans_revenue()
    
    text = "💎 **АНАЛИЗ ПО ТАРИФНЫМ ПЛАНАМ**\n\n"
    
    total_revenue = 0
    for plan_key, data in plans_data.items():
        plan_info = PLAN_INFO.get(plan_key)
        if not plan_info:
            continue
            
        plan_name = plan_info['name']
        plan_price = plan_info['price']
        users_count = data['users']
        revenue = data['revenue']
        total_revenue += revenue
        
        percentage = (revenue / max(sum(p['revenue'] for p in plans_data.values()), 1)) * 100
        
        text += f"**{plan_name}**\n"
        text += f"   💰 Цена: ${plan_price}/мес\n"
        text += f"   👥 Пользователей: {users_count}\n"
        text += f"   💵 Доход: ${revenue:.2f} ({percentage:.1f}%)\n"
        text += f"   📈 Потенциал: ${users_count * plan_price:.2f}/мес\n\n"
    
    text += f"💰 **Общий доход:** ${total_revenue:.2f}/мес"
    
    keyboard = [
        [InlineKeyboardButton("📊 Распределение", callback_data="financial_plans_distribution")],
        [InlineKeyboardButton("🎯 Оптимизация", callback_data="financial_plans_optimization")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="financial")]
    ]
    
    query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

@admin_required
@permission_required(Permission.VIEW_FINANCE)  
def financial_conversions(update: Update, context: CallbackContext):
    """Анализ конверсий"""
    query = update.callback_query
    
    conversion_data = calculate_conversion_metrics()
    
    text = "🔄 **АНАЛИЗ КОНВЕРСИЙ**\n\n"
    
    text += f"**Основные метрики:**\n"
    text += f"• Конверсия триал → платная: {conversion_data['trial_to_paid']:.1f}%\n"
    text += f"• Retention 30 дней: {conversion_data['retention_30']:.1f}%\n"
    text += f"• Churn rate: {conversion_data['churn_rate']:.1f}%\n"
    text += f"• LTV пользователя: ${conversion_data['ltv']:.2f}\n\n"
    
    text += f"**По источникам:**\n"
    for source, data in conversion_data['by_source'].items():
        text += f"• {source}: {data['conversion']:.1f}% ({data['users']} польз.)\n"
    
    text += f"\n**Воронка продаж:**\n"
    text += f"1️⃣ Регистрации: {conversion_data['funnel']['registrations']}\n"
    text += f"2️⃣ Активации: {conversion_data['funnel']['activations']} ({conversion_data['funnel']['activation_rate']:.1f}%)\n"
    text += f"3️⃣ Покупки: {conversion_data['funnel']['purchases']} ({conversion_data['funnel']['purchase_rate']:.1f}%)\n"
    
    keyboard = [
        [InlineKeyboardButton("📈 Улучшить конверсию", callback_data="financial_improve_conversion")],
        [InlineKeyboardButton("🎯 A/B тесты", callback_data="financial_ab_tests")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="financial")]
    ]
    
    query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

# Вспомогательные функции

def calculate_monthly_revenue() -> float:
    """Рассчитывает месячный доход"""
    users = user_service.get_all_users()
    monthly_revenue = 0
    
    for user in users:
        if user.is_active and user.subscription_plan:
            plan_info = PLAN_INFO.get(user.subscription_plan)
            if plan_info:
                monthly_revenue += plan_info['price']
    
    return monthly_revenue

def calculate_yearly_revenue() -> float:
    """Рассчитывает годовой прогноз"""
    return calculate_monthly_revenue() * 12

def calculate_trial_conversion() -> float:
    """Рассчитывает конверсию из триала"""
    users = user_service.get_all_users()
    trial_users = [u for u in users if u.subscription_plan == SubscriptionPlan.TRIAL]
    paid_users = [u for u in users if u.subscription_plan != SubscriptionPlan.TRIAL and u.subscription_plan is not None]
    
    total_trial = len(trial_users) + len(paid_users)
    if total_trial == 0:
        return 0
    
    return (len(paid_users) / total_trial) * 100

def get_monthly_revenue_data() -> List[Dict]:
    """Получает данные по доходам по месяцам"""
    # Заглушка - в реальности здесь была бы работа с историческими данными
    current_date = datetime.now()
    monthly_data = []
    
    for i in range(12):
        month_date = current_date - timedelta(days=30 * i)
        
        # Симуляция данных (в реальности из БД)
        base_revenue = calculate_monthly_revenue()
        monthly_revenue = base_revenue * (0.8 + (i * 0.02))  # Симуляция роста
        monthly_users = len(user_service.get_all_users()) - (i * 5)  # Симуляция роста пользователей
        
        monthly_data.append({
            'month': month_date.month,
            'year': month_date.year,
            'revenue': monthly_revenue,
            'users': max(monthly_users, 0),
            'growth': (i * 2) if i > 0 else 0  # Симуляция роста
        })
    
    return monthly_data[::-1]  # Возвращаем в хронологическом порядке

def analyze_plans_revenue() -> Dict:
    """Анализирует доходы по планам"""
    users = user_service.get_all_users()
    plans_data = {}
    
    for plan in SubscriptionPlan:
        if plan == SubscriptionPlan.TRIAL:
            continue
            
        plan_users = [u for u in users if u.subscription_plan == plan and u.is_active]
        plan_info = PLAN_INFO.get(plan)
        
        if plan_info:
            plans_data[plan] = {
                'users': len(plan_users),
                'revenue': len(plan_users) * plan_info['price']
            }
    
    return plans_data

def calculate_conversion_metrics() -> Dict:
    """Рассчитывает метрики конверсии"""
    users = user_service.get_all_users()
    
    # Базовые расчеты (в реальности более сложная логика)
    trial_conversion = calculate_trial_conversion()
    active_users = len([u for u in users if u.is_active])
    total_users = len(users)
    
    return {
        'trial_to_paid': trial_conversion,
        'retention_30': 85.0,  # Заглушка
        'churn_rate': 5.2,     # Заглушка
        'ltv': 600.0,          # Заглушка
        'by_source': {
            'Органический': {'conversion': 12.5, 'users': 45},
            'Реклама': {'conversion': 8.3, 'users': 32},
            'Рефералы': {'conversion': 15.2, 'users': 23}
        },
        'funnel': {
            'registrations': total_users,
            'activations': active_users,
            'activation_rate': (active_users / max(total_users, 1)) * 100,
            'purchases': len([u for u in users if u.subscription_plan != SubscriptionPlan.TRIAL]),
            'purchase_rate': trial_conversion
        }
    }

# Функция для регистрации обработчиков
def get_financial_handlers():
    """Возвращает словарь обработчиков для финансовых отчетов"""
    return {
        'financial': financial_menu,
        'financial_monthly': financial_monthly,
        'financial_plans': financial_plans,
        'financial_conversions': financial_conversions,
    } 
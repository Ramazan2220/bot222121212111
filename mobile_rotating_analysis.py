#!/usr/bin/env python3
"""
Анализ мобильных ротационных прокси для Instagram автоматизации
Сравнение с другими типами прокси
"""

import sys
import os
from datetime import datetime, timedelta
import math

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database.db_manager import get_instagram_accounts

def analyze_mobile_rotating_proxies():
    """Анализирует преимущества и недостатки мобильных ротационных прокси"""
    
    print("📱🔄 АНАЛИЗ МОБИЛЬНЫХ РОТАЦИОННЫХ ПРОКСИ")
    print("=" * 80)
    
    # Получаем количество аккаунтов
    accounts = get_instagram_accounts()
    active_accounts = [acc for acc in accounts if acc.is_active]
    accounts_count = len(active_accounts)
    
    print(f"📊 Активных аккаунтов: {accounts_count}")
    print()
    
    # Сравнительная таблица типов прокси
    proxy_types = {
        'Статичный мобильный': {
            'security': 10,      # Безопасность (1-10)
            'cost_per_ip': 70,   # Стоимость за IP в месяц
            'rotation': 1,       # Ротация (1-10, где 10 = лучшая ротация)
            'instagram_trust': 10, # Доверие Instagram
            'setup_complexity': 3, # Сложность настройки (1-10)
            'scalability': 3,    # Масштабируемость (1-10)
            'limits_per_ip': {
                'likes': 800,
                'follows': 150,
                'posts': 50
            },
            'description': 'Один фиксированный мобильный IP'
        },
        
        'Мобильный ротационный': {
            'security': 9,
            'cost_per_ip': 25,   # Обычно $20-30 за GB трафика
            'rotation': 9,
            'instagram_trust': 9,
            'setup_complexity': 6,
            'scalability': 8,
            'limits_per_ip': {
                'likes': 600,    # Немного ниже из-за ротации
                'follows': 120,
                'posts': 40
            },
            'description': 'Ротация мобильных IP каждые 10-30 минут'
        },
        
        'Residential ротационный': {
            'security': 7,
            'cost_per_ip': 8,    # $5-15 за GB
            'rotation': 10,
            'instagram_trust': 6,
            'setup_complexity': 4,
            'scalability': 10,
            'limits_per_ip': {
                'likes': 400,
                'follows': 80,
                'posts': 30
            },
            'description': 'Ротация домашних IP каждые 5-15 минут'
        },
        
        'Datacenter': {
            'security': 2,
            'cost_per_ip': 3,
            'rotation': 8,
            'instagram_trust': 1,
            'setup_complexity': 2,
            'scalability': 10,
            'limits_per_ip': {
                'likes': 100,
                'follows': 20,
                'posts': 10
            },
            'description': 'Серверные IP - ВЫСОКИЙ РИСК для Instagram'
        }
    }
    
    print("📊 СРАВНИТЕЛЬНАЯ ТАБЛИЦА ТИПОВ ПРОКСИ:")
    print("=" * 80)
    print(f"{'Тип прокси':<25} {'Безоп.':<7} {'Цена':<8} {'Ротация':<8} {'Instagram':<10} {'Сложность':<10}")
    print("-" * 80)
    
    for proxy_type, data in proxy_types.items():
        print(f"{proxy_type:<25} {data['security']:<7} ${data['cost_per_ip']:<7} {data['rotation']:<8} {data['instagram_trust']:<10} {data['setup_complexity']:<10}")
    
    print("\n" + "="*80)
    
    # Детальный анализ мобильных ротационных
    mobile_rotating = proxy_types['Мобильный ротационный']
    
    print("📱🔄 МОБИЛЬНЫЕ РОТАЦИОННЫЕ - ДЕТАЛЬНЫЙ АНАЛИЗ")
    print("=" * 80)
    
    print("✅ ПРЕИМУЩЕСТВА:")
    print("-" * 40)
    print("🛡️  БЕЗОПАСНОСТЬ:")
    print("   • 9/10 - почти как статичные мобильные")
    print("   • Instagram доверяет мобильному трафику")
    print("   • Операторы связи (Verizon, AT&T, T-Mobile)")
    print("   • Реальные устройства и SIM-карты")
    print()
    
    print("🔄 РОТАЦИЯ:")
    print("   • Автоматическая смена IP каждые 10-30 минут")
    print("   • Снижает риск обнаружения паттернов")
    print("   • Разные операторы и регионы")
    print("   • Имитация перемещения пользователя")
    print()
    
    print("💰 СТОИМОСТЬ:")
    print(f"   • ~${mobile_rotating['cost_per_ip']}/месяц за GB трафика")
    print("   • В 2-3 раза дешевле статичных мобильных")
    print("   • В 2-3 раза дороже residential")
    print("   • Оптимальное соотношение цена/качество")
    print()
    
    print("📈 МАСШТАБИРУЕМОСТЬ:")
    print("   • Легко увеличить количество IP")
    print("   • Pay-per-use модель")
    print("   • Подходит для роста проекта")
    print()
    
    print("❌ НЕДОСТАТКИ:")
    print("-" * 40)
    print("⚠️  СЛОЖНОСТЬ НАСТРОЙКИ:")
    print("   • Нужно правильно настроить sticky sessions")
    print("   • Требуется мониторинг ротации")
    print("   • Может потребоваться перелогин при смене IP")
    print()
    
    print("🔄 НЕПРЕДСКАЗУЕМОСТЬ:")
    print("   • Невозможно контролировать время ротации")
    print("   • Может сменить IP в неподходящий момент")
    print("   • Разные провайдеры = разное качество IP")
    print()
    
    print("📊 ЛИМИТЫ:")
    print("   • Немного ниже чем у статичных мобильных")
    print("   • Instagram может быть осторожнее с ротацией")
    print()
    
    # Расчет для ваших аккаунтов
    print("🎯 РАСЧЕТ ДЛЯ ВАШИХ АККАУНТОВ:")
    print("=" * 80)
    
    # Лимиты мобильных ротационных на весь пул IP
    total_daily_limits = mobile_rotating['limits_per_ip']
    
    per_account_limits = {}
    for action, total_limit in total_daily_limits.items():
        per_account = total_limit // accounts_count if accounts_count > 0 else 0
        per_account_limits[action] = per_account
    
    print(f"📊 ЛИМИТЫ НА АККАУНТ ({accounts_count} аккаунтов):")
    print("-" * 50)
    for action, limit in per_account_limits.items():
        print(f"   {action:12}: {limit:3}/день на аккаунт")
    print()
    
    # Оценка риска
    risk_score = 0
    risk_factors = []
    
    if accounts_count > 15:
        risk_score += 2
        risk_factors.append(f"🟡 {accounts_count} аккаунтов - много для одного пула IP")
    elif accounts_count > 10:
        risk_score += 1
        risk_factors.append(f"🟢 {accounts_count} аккаунтов - приемлемо")
    else:
        risk_factors.append(f"✅ {accounts_count} аккаунтов - отлично")
    
    if per_account_limits['likes'] < 30:
        risk_score += 2
        risk_factors.append(f"🔴 Лайки: {per_account_limits['likes']}/день - критично мало")
    elif per_account_limits['likes'] < 50:
        risk_score += 1
        risk_factors.append(f"🟡 Лайки: {per_account_limits['likes']}/день - ограниченно")
    else:
        risk_factors.append(f"✅ Лайки: {per_account_limits['likes']}/день - хорошо")
    
    if per_account_limits['follows'] < 8:
        risk_score += 1
        risk_factors.append(f"🟡 Подписки: {per_account_limits['follows']}/день - ограниченно")
    else:
        risk_factors.append(f"✅ Подписки: {per_account_limits['follows']}/день - достаточно")
    
    print("⚠️  АНАЛИЗ РИСКОВ:")
    print("-" * 40)
    for factor in risk_factors:
        print(f"   {factor}")
    print()
    
    if risk_score <= 1:
        risk_level = "🟢 НИЗКИЙ"
        recommendation = "ОТЛИЧНО - мобильные ротационные идеальны!"
    elif risk_score <= 3:
        risk_level = "🟡 СРЕДНИЙ"
        recommendation = "ХОРОШО - с правильными настройками"
    else:
        risk_level = "🔴 ВЫСОКИЙ"
        recommendation = "РИСКОВАННО - рассмотрите разделение"
    
    print(f"🎯 ОЦЕНКА РИСКА: {risk_level}")
    print(f"💡 РЕКОМЕНДАЦИЯ: {recommendation}")
    print()
    
    # Сравнение стоимости
    print("💰 СРАВНЕНИЕ СТОИМОСТИ (месяц):")
    print("=" * 80)
    
    # Расчет трафика (примерная оценка)
    daily_actions_per_account = 100  # лайки + подписки + просмотры
    monthly_actions = daily_actions_per_account * accounts_count * 30
    estimated_gb = monthly_actions / 1000  # примерно 1000 действий = 1GB
    
    costs = {
        'Один статичный мобильный': 70,
        'Мобильные ротационные': int(estimated_gb * 25),
        'Группировка статичных': math.ceil(accounts_count / 5) * 70,
        'Residential ротационные': int(estimated_gb * 8),
        'Смешанная стратегия': (math.ceil(accounts_count * 0.2) * 70) + (math.ceil(accounts_count * 0.8 / 8) * 15)
    }
    
    print(f"Estimated monthly traffic: ~{estimated_gb:.1f} GB")
    print("-" * 50)
    
    sorted_costs = sorted(costs.items(), key=lambda x: x[1])
    
    for i, (strategy, cost) in enumerate(sorted_costs, 1):
        if i == 1:
            print(f"{i}. {strategy:<30} ${cost:3} 💚 (САМЫЙ ДЕШЕВЫЙ)")
        elif strategy == 'Мобильные ротационные':
            print(f"{i}. {strategy:<30} ${cost:3} 📱 (РЕКОМЕНДУЕМЫЙ)")
        else:
            print(f"{i}. {strategy:<30} ${cost:3}")
    
    print()
    
    # Лучшие провайдеры мобильных ротационных
    print("🏆 ЛУЧШИЕ ПРОВАЙДЕРЫ МОБИЛЬНЫХ РОТАЦИОННЫХ:")
    print("=" * 80)
    
    providers = [
        {
            'name': 'Smartproxy Mobile',
            'price': '$25-35/GB',
            'rotation': '10-30 мин',
            'locations': 'US, UK, DE, CA',
            'operators': 'Verizon, AT&T, T-Mobile',
            'rating': '9/10',
            'pros': 'Высокое качество, стабильность',
            'cons': 'Дороговато'
        },
        {
            'name': 'LunaProxy Mobile',
            'price': '$20-28/GB', 
            'rotation': '15-45 мин',
            'locations': 'US, EU, AS',
            'operators': 'Все крупные операторы',
            'rating': '8.5/10',
            'pros': 'Хорошая цена, много локаций',
            'cons': 'Иногда медленная поддержка'
        },
        {
            'name': 'ProxyEmpire Mobile',
            'price': '$22-30/GB',
            'rotation': '10-60 мин',
            'locations': 'Global',
            'operators': 'Premium carriers only',
            'rating': '8/10',
            'pros': 'Глобальное покрытие',
            'cons': 'Переменчивое качество'
        },
        {
            'name': 'IPRoyal Mobile',
            'price': '$18-25/GB',
            'rotation': '20-40 мин',
            'locations': 'US, EU',
            'operators': 'Major carriers',
            'rating': '7.5/10',
            'pros': 'Бюджетная цена',
            'cons': 'Меньше функций'
        }
    ]
    
    for provider in providers:
        print(f"📱 {provider['name']} - {provider['rating']}")
        print(f"   💰 Цена: {provider['price']}")
        print(f"   🔄 Ротация: {provider['rotation']}")
        print(f"   🌍 Локации: {provider['locations']}")
        print(f"   📡 Операторы: {provider['operators']}")
        print(f"   ✅ Плюсы: {provider['pros']}")
        print(f"   ❌ Минусы: {provider['cons']}")
        print()

def mobile_rotating_recommendations():
    """Рекомендации по настройке мобильных ротационных прокси"""
    
    print("🔧 РЕКОМЕНДАЦИИ ПО НАСТРОЙКЕ МОБИЛЬНЫХ РОТАЦИОННЫХ")
    print("=" * 80)
    
    print("📋 ОБЯЗАТЕЛЬНЫЕ НАСТРОЙКИ:")
    print("-" * 40)
    print("🔄 STICKY SESSIONS:")
    print("   • Время сессии: 15-30 минут")
    print("   • Автоматическое обновление токенов")
    print("   • Graceful reconnection при смене IP")
    print()
    
    print("⏰ ТАЙМИНГИ:")
    print("   • Пауза между действиями: 3-7 секунд")
    print("   • Проверка смены IP каждые 5 минут")
    print("   • Переподключение при необходимости")
    print()
    
    print("🎭 HUMAN-LIKE BEHAVIOR:")
    print("   • Случайные User-Agents для мобильных")
    print("   • Имитация движения по городам")
    print("   • Разные временные зоны активности")
    print()
    
    print("📊 МОНИТОРИНГ:")
    print("   • Отслеживание текущего IP")
    print("   • Логирование всех смен IP")
    print("   • Алерты при проблемах с соединением")
    print("   • Статистика по операторам связи")
    print()
    
    print("💡 ОПТИМИЗАЦИЯ:")
    print("-" * 40)
    print("🎯 СТРАТЕГИЯ ИСПОЛЬЗОВАНИЯ:")
    print("   • Не более 50% аккаунтов одновременно")
    print("   • Распределение по временным зонам")
    print("   • Приоритет VIP аккаунтам")
    print("   • Backup стратегия при сбоях")
    print()
    
    print("🔄 РОТАЦИОННАЯ ЛОГИКА:")
    print("   • Soft rotation - без потери сессий")
    print("   • IP warming для новых адресов")
    print("   • Blacklist для плохих IP")
    print("   • Whitelist для проверенных")

if __name__ == "__main__":
    analyze_mobile_rotating_proxies()
    print()
    mobile_rotating_recommendations() 
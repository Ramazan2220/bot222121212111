#!/usr/bin/env python3
"""
Анализ безопасности использования одного мобильного прокси для всех аккаунтов
"""

import sys
import os
from datetime import datetime, timedelta
import math

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database.db_manager import get_instagram_accounts

def analyze_single_proxy_safety():
    """Анализирует безопасность использования одного прокси"""
    
    print("🔍 АНАЛИЗ БЕЗОПАСНОСТИ ОДНОГО МОБИЛЬНОГО ПРОКСИ")
    print("=" * 70)
    
    # Получаем все аккаунты
    accounts = get_instagram_accounts()
    active_accounts = [acc for acc in accounts if acc.is_active]
    
    print(f"📊 Всего аккаунтов: {len(accounts)}")
    print(f"✅ Активных аккаунтов: {len(active_accounts)}")
    print()
    
    # Instagram лимиты с одного IP (консервативные оценки)
    daily_limits = {
        'likes': 800,           # Лайки в день с одного IP
        'follows': 150,         # Подписки в день с одного IP  
        'unfollows': 150,       # Отписки в день с одного IP
        'comments': 100,        # Комментарии в день с одного IP
        'posts': 50,            # Публикации в день с одного IP
        'stories': 100,         # Истории в день с одного IP
        'dm_sends': 50,         # Личные сообщения в день с одного IP
    }
    
    hourly_limits = {k: v // 16 for k, v in daily_limits.items()}  # 16 активных часов в день
    
    print("📉 ЛИМИТЫ INSTAGRAM С ОДНОГО IP (консервативные оценки):")
    print("-" * 50)
    for action, limit in daily_limits.items():
        print(f"   {action:12}: {limit:3}/день ({hourly_limits[action]:2}/час)")
    print()
    
    # Расчет на аккаунт
    accounts_count = len(active_accounts)
    if accounts_count > 0:
        print(f"📊 ЛИМИТЫ НА АККАУНТ ({accounts_count} аккаунтов):")
        print("-" * 50)
        
        per_account_daily = {}
        per_account_hourly = {}
        
        for action, total_limit in daily_limits.items():
            per_day = total_limit // accounts_count
            per_hour = hourly_limits[action] // accounts_count
            
            per_account_daily[action] = per_day
            per_account_hourly[action] = per_hour
            
            print(f"   {action:12}: {per_day:2}/день ({per_hour:2}/час) на аккаунт")
        print()
        
        # Анализ рисков
        print("⚠️  АНАЛИЗ РИСКОВ:")
        print("-" * 50)
        
        risk_level = 0
        risks = []
        
        # Риск 1: Слишком много аккаунтов
        if accounts_count > 20:
            risk_level += 3
            risks.append(f"🔴 ВЫСОКИЙ РИСК: {accounts_count} аккаунтов на одном IP - слишком много!")
        elif accounts_count > 10:
            risk_level += 2
            risks.append(f"🟡 СРЕДНИЙ РИСК: {accounts_count} аккаунтов на одном IP")
        elif accounts_count > 5:
            risk_level += 1
            risks.append(f"🟢 НИЗКИЙ РИСК: {accounts_count} аккаунтов на одном IP")
        else:
            risks.append(f"✅ БЕЗОПАСНО: {accounts_count} аккаунтов на одном IP")
        
        # Риск 2: Лимиты слишком низкие
        if per_account_daily['likes'] < 20:
            risk_level += 2
            risks.append(f"🔴 Лайки: только {per_account_daily['likes']}/день на аккаунт - очень мало!")
        elif per_account_daily['likes'] < 50:
            risk_level += 1
            risks.append(f"🟡 Лайки: {per_account_daily['likes']}/день на аккаунт - ограниченно")
        
        if per_account_daily['follows'] < 5:
            risk_level += 2
            risks.append(f"🔴 Подписки: только {per_account_daily['follows']}/день на аккаунт - критично мало!")
        elif per_account_daily['follows'] < 20:
            risk_level += 1
            risks.append(f"🟡 Подписки: {per_account_daily['follows']}/день на аккаунт - ограниченно")
        
        # Риск 3: Публикации
        if per_account_daily['posts'] < 1:
            risk_level += 1
            risks.append(f"🟡 Публикации: {per_account_daily['posts']}/день на аккаунт")
        
        for risk in risks:
            print(f"   {risk}")
        print()
        
        # Общая оценка риска
        print("🎯 ОБЩАЯ ОЦЕНКА РИСКА:")
        print("-" * 50)
        
        if risk_level >= 6:
            risk_rating = "🔴 КРИТИЧЕСКИЙ"
            recommendation = "НЕ РЕКОМЕНДУЕТСЯ - используйте несколько IP"
        elif risk_level >= 4:
            risk_rating = "🟡 ВЫСОКИЙ"
            recommendation = "РИСКОВАННО - рассмотрите разделение на группы"
        elif risk_level >= 2:
            risk_rating = "🟠 СРЕДНИЙ"
            recommendation = "ВОЗМОЖНО с осторожностью"
        else:
            risk_rating = "🟢 НИЗКИЙ"
            recommendation = "БЕЗОПАСНО при соблюдении лимитов"
        
        print(f"   Уровень риска: {risk_rating}")
        print(f"   Рекомендация: {recommendation}")
        print()
        
        # Стратегии снижения рисков
        print("🛡️  СТРАТЕГИИ БЕЗОПАСНОГО ИСПОЛЬЗОВАНИЯ:")
        print("-" * 50)
        
        print("1. 📅 ВРЕМЕННОЕ РАСПРЕДЕЛЕНИЕ:")
        print("   - Активность аккаунтов в разное время")
        print("   - Паузы 2-5 минут между действиями")
        print("   - Имитация человеческого поведения")
        print()
        
        print("2. 🔄 РОТАЦИЯ АКТИВНОСТИ:")
        print("   - Не все аккаунты активны одновременно")
        print("   - 30-50% аккаунтов в день максимум")
        print("   - Разные дни недели для разных аккаунтов")
        print()
        
        print("3. 📊 МОНИТОРИНГ ЛИМИТОВ:")
        print("   - Отслеживание общего количества действий")
        print("   - Автоматическая остановка при превышении")
        print("   - Логирование всех действий")
        print()
        
        print("4. 🎭 РАЗНООБРАЗИЕ ДЕЙСТВИЙ:")
        print("   - Не только лайки и подписки")
        print("   - Просмотры, Stories, комментарии")
        print("   - Публикации контента")
        print()
        
        # Альтернативные стратегии
        print("💡 АЛЬТЕРНАТИВНЫЕ СТРАТЕГИИ:")
        print("-" * 50)
        
        # Стратегия 1: Группировка
        groups = math.ceil(accounts_count / 5)  # Группы по 5 аккаунтов
        group_cost = groups * 70  # $70 за мобильный IP
        
        print(f"1. 📱 ГРУППИРОВКА АККАУНТОВ:")
        print(f"   - Разделить на {groups} групп по 5 аккаунтов")
        print(f"   - {groups} мобильных прокси")
        print(f"   - Стоимость: ~${group_cost}/месяц")
        print(f"   - Безопасность: ВЫСОКАЯ")
        print()
        
        # Стратегия 2: Смешанный подход
        vip_accounts = math.ceil(accounts_count * 0.2)  # 20% VIP
        regular_accounts = accounts_count - vip_accounts
        regular_groups = math.ceil(regular_accounts / 8)  # Группы по 8
        
        mixed_cost = (vip_accounts * 70) + (regular_groups * 15)  # Мобильные + Residential
        
        print(f"2. 🔄 СМЕШАННЫЙ ПОДХОД:")
        print(f"   - {vip_accounts} VIP аккаунтов → {vip_accounts} мобильных IP")
        print(f"   - {regular_accounts} обычных → {regular_groups} residential IP")
        print(f"   - Стоимость: ~${mixed_cost}/месяц")
        print(f"   - Безопасность: ОПТИМАЛЬНАЯ")
        print()
        
        # Сравнение с одним IP
        single_cost = 70  # Один мобильный прокси
        print(f"3. 📱 ОДИН МОБИЛЬНЫЙ ПРОКСИ:")
        print(f"   - Все {accounts_count} аккаунтов на одном IP")
        print(f"   - Стоимость: ~${single_cost}/месяц")
        print(f"   - Безопасность: {risk_rating}")
        print()
        
        # Рекомендация по экономии
        savings_group = group_cost - single_cost
        savings_mixed = mixed_cost - single_cost
        
        print("💰 ЭКОНОМИЧЕСКИЙ АНАЛИЗ:")
        print("-" * 50)
        print(f"Экономия с одним IP:")
        print(f"   vs Группировка: ${savings_group}/месяц")
        print(f"   vs Смешанный: ${savings_mixed}/месяц")
        print()
        print("⚖️  Соотношение риск/экономия:")
        if savings_group > 200:
            print("   💡 Экономия существенная - можно попробовать один IP с мерами безопасности")
        else:
            print("   ⚠️  Экономия незначительная - лучше разделить аккаунты")

def create_single_proxy_monitoring():
    """Создает систему мониторинга для одного прокси"""
    
    print("\n" + "="*70)
    print("📊 СИСТЕМА МОНИТОРИНГА ДЛЯ ОДНОГО ПРОКСИ")
    print("="*70)
    
    monitoring_code = '''
# Пример системы мониторинга лимитов для одного IP

class SingleProxyManager:
    def __init__(self):
        self.daily_counters = {
            'likes': 0,
            'follows': 0, 
            'unfollows': 0,
            'comments': 0,
            'posts': 0,
            'stories': 0,
            'dm_sends': 0
        }
        
        self.daily_limits = {
            'likes': 800,
            'follows': 150,
            'unfollows': 150,
            'comments': 100,
            'posts': 50,
            'stories': 100,
            'dm_sends': 50
        }
        
        self.last_reset = datetime.now().date()
    
    def can_perform_action(self, action_type, count=1):
        """Проверяет, можно ли выполнить действие"""
        self._reset_if_new_day()
        
        current = self.daily_counters.get(action_type, 0)
        limit = self.daily_limits.get(action_type, 0)
        
        return (current + count) <= limit
    
    def record_action(self, action_type, count=1):
        """Записывает выполненное действие"""
        self._reset_if_new_day()
        
        if action_type in self.daily_counters:
            self.daily_counters[action_type] += count
            
        return self.daily_counters[action_type]
    
    def _reset_if_new_day(self):
        """Сбрасывает счетчики в новом дне"""
        today = datetime.now().date()
        if today > self.last_reset:
            self.daily_counters = {k: 0 for k in self.daily_counters}
            self.last_reset = today
    
    def get_remaining_limits(self):
        """Возвращает оставшиеся лимиты"""
        self._reset_if_new_day()
        
        remaining = {}
        for action, used in self.daily_counters.items():
            limit = self.daily_limits[action]
            remaining[action] = max(0, limit - used)
            
        return remaining
'''
    
    print("💻 КОД СИСТЕМЫ МОНИТОРИНГА:")
    print(monitoring_code)
    
    print("\n📋 ИСПОЛЬЗОВАНИЕ:")
    print("-" * 30)
    print("proxy_manager = SingleProxyManager()")
    print("if proxy_manager.can_perform_action('likes', 10):")
    print("    # Выполнить лайки")
    print("    proxy_manager.record_action('likes', 10)")
    print("else:")
    print("    # Отложить действие на завтра")

if __name__ == "__main__":
    analyze_single_proxy_safety()
    create_single_proxy_monitoring() 
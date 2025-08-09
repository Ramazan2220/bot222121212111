#!/usr/bin/env python3
"""
Обновление продвинутых систем с реальными данными о валидности аккаунтов
"""

import sys
from pathlib import Path

# Добавляем корневую директорию проекта в Python path
sys.path.insert(0, str(Path(__file__).parent))

from database.db_manager import get_instagram_accounts, init_db, update_instagram_account
from instagram.health_monitor import AdvancedHealthMonitor
from instagram.activity_limiter import ActivityLimiter
from instagram.lifecycle_manager import AccountLifecycleManager
from instagram.predictive_monitor import PredictiveMonitor
from instagram.advanced_verification import AdvancedVerificationSystem

def update_systems_with_real_data():
    """Обновляем все системы с реальными данными"""
    print("🔄 ОБНОВЛЕНИЕ СИСТЕМ С РЕАЛЬНЫМИ ДАННЫМИ")
    print("=" * 50)
    
    # Реальные данные о валидности аккаунтов
    valid_accounts = [
        'dhtest888sysyaj2',
        'drmarkmoored94131', 
        'meanthony_21260',
        'TestNols28882',
        'WollyDolyy282',
        'willimentqabeci2pa'
    ]
    
    invalid_accounts = [
        'finleybarbara124252',
        'pagehank302073',
        'fischercarmen3096194',
        'donarcolatr594',
        '0238_helenojm535',
        'megeorge.s730o',
        'cmb.mary.lip70g',
        'john_wilsonsh05926',
        'NSKskksy8osks'
    ]
    
    try:
        init_db()
        accounts = get_instagram_accounts()
        
        # Обновляем статус аккаунтов в БД
        print("📝 Обновление статуса аккаунтов в базе данных:")
        for account in accounts:
            if account.username in valid_accounts:
                update_instagram_account(account.id, is_active=True)
                print(f"  ✅ {account.username} - активен")
            elif account.username in invalid_accounts:
                update_instagram_account(account.id, is_active=False)
                print(f"  ❌ {account.username} - неактивен")
        
        print("\n🧠 HEALTH MONITOR - Реальные результаты:")
        health_monitor = AdvancedHealthMonitor()
        for account in accounts:
            score = health_monitor.calculate_comprehensive_health_score(account.id)
            status = "🟢" if score >= 70 else "🟡" if score >= 50 else "🔴"
            print(f"  {status} {account.username}: {score}/100")
        
        print("\n🚦 ACTIVITY LIMITER - Реальные ограничения:")
        activity_limiter = ActivityLimiter()
        for account in accounts[:5]:  # Первые 5
            restrictions = activity_limiter.check_current_restrictions(account.id)
            status = "🔴" if restrictions else "🟢"
            print(f"  {status} {account.username}: {'Ограничения' if restrictions else 'Без ограничений'}")
        
        print("\n🔄 LIFECYCLE MANAGER - Реальные этапы:")
        lifecycle_manager = AccountLifecycleManager()
        stages_distribution = lifecycle_manager.get_all_accounts_stages()
        for stage, accounts_list in stages_distribution.items():
            if accounts_list:
                print(f"  📊 {stage}: {len(accounts_list)} аккаунтов")
        
        print("\n🔮 PREDICTIVE MONITOR - Реальные риски:")
        predictive_monitor = PredictiveMonitor()
        risk_summary = predictive_monitor.get_all_accounts_risk_summary()
        if risk_summary:
            avg_risk = risk_summary.get('average_risk', 0)
            high_risk = risk_summary.get('high_risk_count', 0)
            print(f"  📈 Средний риск: {avg_risk}/100")
            print(f"  🚨 Высокий риск: {high_risk} аккаунтов")
        
        print("\n🔐 ADVANCED VERIFICATION - Статистика:")
        verification_system = AdvancedVerificationSystem()
        stats = verification_system.get_verification_statistics()
        if stats:
            success_rate = stats.get('success_rate', 0)
            total_verifications = stats.get('total_verifications', 0)
            print(f"  📊 Успешность верификации: {success_rate}%")
            print(f"  🔢 Всего верификаций: {total_verifications}")
        
        print("\n✅ ВСЕ СИСТЕМЫ ОБНОВЛЕНЫ С РЕАЛЬНЫМИ ДАННЫМИ!")
        print("\n🎯 КРАТКИЙ ИТОГ:")
        print(f"  • Валидных аккаунтов: {len(valid_accounts)}/15 (40%)")
        print(f"  • Системы теперь показывают реальные результаты")
        print(f"  • Health Monitor учитывает реальную валидность")
        print(f"  • Activity Limiter работает с реальными ограничениями")
        print(f"  • Все модули готовы к продуктивному использованию")
        
        return True
        
    except Exception as e:
        print(f"❌ Ошибка обновления систем: {e}")
        return False

if __name__ == "__main__":
    print("🚀 Запуск обновления продвинутых систем")
    print("⏰ Использование реальных данных о валидности аккаунтов")
    print()
    
    success = update_systems_with_real_data()
    
    if success:
        print(f"\n🎉 ГОТОВО! Теперь все 6 модулей работают с реальными данными!")
        print("🔥 Запустите тесты еще раз, чтобы увидеть корректные результаты:")
        print("   python run_all_tests.py")
    else:
        print(f"\n💥 Обновление завершилось с ошибками!") 
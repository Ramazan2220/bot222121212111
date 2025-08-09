#!/usr/bin/env python3
"""
Детальная проверка всех аккаунтов через новые модули
"""

import sys
from pathlib import Path
from datetime import datetime

# Добавляем корневую директорию проекта в Python path
sys.path.insert(0, str(Path(__file__).parent))

from database.db_manager import get_instagram_accounts, init_db
from instagram.health_monitor import AdvancedHealthMonitor
from instagram.lifecycle_manager import AccountLifecycleManager
from instagram.predictive_monitor import PredictiveMonitor
from instagram.advanced_verification import AdvancedVerificationSystem

def check_accounts_detailed():
    """Детальная проверка всех аккаунтов"""
    print('🔍 ДЕТАЛЬНЫЕ РЕЗУЛЬТАТЫ ПО АККАУНТАМ')
    print('='*60)
    
    init_db()
    accounts = get_instagram_accounts()
    
    if not accounts:
        print("❌ Аккаунты не найдены в базе данных")
        return
    
    # Инициализируем модули
    health_monitor = AdvancedHealthMonitor()
    lifecycle_manager = AccountLifecycleManager()
    predictive_monitor = PredictiveMonitor()
    verification_system = AdvancedVerificationSystem()
    
    valid_accounts = []
    problematic_accounts = []
    
    print(f"📋 Всего аккаунтов для проверки: {len(accounts)}")
    print()
    
    for i, account in enumerate(accounts, 1):
        print(f'👤 АККАУНТ {i}: {account.username}')
        print(f'📊 ID: {account.id} | Активен в БД: {account.is_active}')
        
        try:
            # Возраст аккаунта
            age_days = (datetime.now() - account.created_at).days
            print(f'📅 Возраст: {age_days} дней')
            
            # Health Monitor
            health_score = health_monitor.calculate_comprehensive_health_score(account.id)
            print(f'🧠 Health Score: {health_score}/100')
            
            # Lifecycle Manager  
            stage = lifecycle_manager.determine_account_stage(account.id)
            print(f'🔄 Lifecycle Stage: {stage}')
            
            # Predictive Monitor
            risk_score = predictive_monitor.calculate_ban_risk_score(account.id)
            print(f'🔮 Risk Score: {risk_score}/100')
            
            # Verification (пропускаем для простоты)
            verification_status = "⏳ Пропущено"
            print(f'🔐 Верификация: {verification_status}')
            
            # Определяем финальный статус
            if health_score >= 40 and stage == 'ACTIVE' and risk_score <= 40:
                status = '✅ ВАЛИДНЫЙ'
                valid_accounts.append({
                    'username': account.username,
                    'health': health_score,
                    'stage': stage,
                    'risk': risk_score,
                    'age': age_days
                })
            else:
                status = '❌ ПРОБЛЕМНЫЙ'
                problematic_accounts.append({
                    'username': account.username,
                    'health': health_score,
                    'stage': stage,
                    'risk': risk_score,
                    'age': age_days,
                    'reason': get_problem_reason(health_score, stage, risk_score)
                })
            
            print(f'🎯 ИТОГОВЫЙ СТАТУС: {status}')
            print('-' * 50)
            
        except Exception as e:
            print(f'❌ Ошибка при проверке аккаунта {account.username}: {e}')
            print('-' * 50)
    
    # Сводная статистика
    print('\n📊 СВОДНАЯ СТАТИСТИКА')
    print('='*40)
    print(f'✅ Валидных аккаунтов: {len(valid_accounts)}/{len(accounts)} ({len(valid_accounts)/len(accounts)*100:.1f}%)')
    print(f'❌ Проблемных аккаунтов: {len(problematic_accounts)}/{len(accounts)} ({len(problematic_accounts)/len(accounts)*100:.1f}%)')
    
    if valid_accounts:
        print('\n✅ ВАЛИДНЫЕ АККАУНТЫ:')
        for acc in valid_accounts:
            print(f"  • {acc['username']}: Health={acc['health']}, Stage={acc['stage']}, Risk={acc['risk']}")
    
    if problematic_accounts:
        print('\n❌ ПРОБЛЕМНЫЕ АККАУНТЫ:')
        for acc in problematic_accounts:
            print(f"  • {acc['username']}: {acc['reason']}")
            print(f"    Health={acc['health']}, Stage={acc['stage']}, Risk={acc['risk']}")
    
    # Анализ по критериям
    print('\n📈 АНАЛИЗ ПО КРИТЕРИЯМ:')
    health_good = len([a for a in accounts if health_monitor.calculate_comprehensive_health_score(a.id) >= 40])
    stage_active = len([a for a in accounts if lifecycle_manager.determine_account_stage(a.id) == 'ACTIVE'])
    risk_low = len([a for a in accounts if predictive_monitor.calculate_ban_risk_score(a.id) <= 40])
    
    print(f'🧠 Health Score ≥ 40: {health_good}/{len(accounts)} ({health_good/len(accounts)*100:.1f}%)')
    print(f'🔄 Stage = ACTIVE: {stage_active}/{len(accounts)} ({stage_active/len(accounts)*100:.1f}%)')
    print(f'🔮 Risk Score ≤ 40: {risk_low}/{len(accounts)} ({risk_low/len(accounts)*100:.1f}%)')

def get_problem_reason(health_score, stage, risk_score):
    """Определяет причину проблемы с аккаунтом"""
    reasons = []
    
    if health_score < 40:
        reasons.append("Низкое здоровье")
    if stage != 'ACTIVE':
        reasons.append(f"Неактивный этап ({stage})")
    if risk_score > 40:
        reasons.append("Высокий риск")
    
    return ", ".join(reasons) if reasons else "Неизвестная проблема"

if __name__ == "__main__":
    check_accounts_detailed() 
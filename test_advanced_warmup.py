#!/usr/bin/env python3
"""
Тестирование Advanced Warmup 2.0
"""

import asyncio
import logging
from datetime import datetime
from services.advanced_warmup import advanced_warmup

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

async def test_warmup():
    """Тестирование прогрева"""
    print("🔥 Advanced Warmup 2.0 - Тестирование\n")
    
    # 1. Проверка временного паттерна
    print("1️⃣ Проверка временного паттерна:")
    pattern = advanced_warmup.determine_time_pattern()
    if pattern:
        print(f"   ⏰ Текущее время: {datetime.now().strftime('%H:%M')}")
        print(f"   📊 Интенсивность: {pattern['intensity']*100:.0f}%")
        print(f"   ⏱️ Рекомендуемая длительность: {pattern['duration'][0]}-{pattern['duration'][1]} мин")
        if datetime.now().weekday() in [5, 6]:
            print("   🎉 Выходной день (+30% активности)")
    else:
        print("   ❌ Временной паттерн не определен")
    
    # 2. Тест определения стратегии
    print("\n2️⃣ Тест определения стратегии:")
    test_accounts = [
        {"id": 1, "age_days": 5, "expected": "BABY"},
        {"id": 2, "age_days": 15, "expected": "CHILD"},
        {"id": 3, "age_days": 60, "expected": "TEEN"},
        {"id": 4, "age_days": 100, "expected": "ADULT"}
    ]
    
    for acc in test_accounts:
        # Здесь в реальности стратегия определяется по дате создания аккаунта
        # Для теста просто выводим ожидаемую стратегию
        print(f"   Аккаунт {acc['id']} ({acc['age_days']} дней) → {acc['expected']}")
    
    # 3. Демо новых функций
    print("\n3️⃣ Новые функции прогрева:")
    print("   ✅ Сохранение в коллекции")
    print("   ✅ Свайп Reels с зацикливанием")
    print("   ✅ Проверка уведомлений")
    print("   ✅ Поиск локаций")
    print("   ✅ UI взаимодействия (долгое нажатие, случайные лайки)")
    
    # 4. Запрос на тестовый прогрев
    print("\n4️⃣ Хотите запустить тестовый прогрев?")
    print("   ⚠️  ВНИМАНИЕ: Это запустит реальный прогрев на выбранном аккаунте!")
    
    choice = input("\nЗапустить прогрев? (y/n): ")
    if choice.lower() == 'y':
        account_id = int(input("ID аккаунта: "))
        duration = int(input("Длительность (минут): "))
        interests_input = input("Интересы через запятую (или Enter для пропуска): ")
        interests = [i.strip() for i in interests_input.split(",")] if interests_input else []
        
        print(f"\n🚀 Запускаем прогрев...")
        print(f"   📱 Аккаунт ID: {account_id}")
        print(f"   ⏱️ Длительность: {duration} мин")
        if interests:
            print(f"   🎯 Интересы: {', '.join(interests)}")
        
        success, report = await advanced_warmup.start_warmup(
            account_id=account_id,
            duration_minutes=duration,
            interests=interests
        )
        
        print("\n" + "="*50)
        if success:
            print("✅ УСПЕШНО")
        else:
            print("❌ ОШИБКА")
        print("="*50)
        print(report)
    else:
        print("\n❌ Тест отменен")

if __name__ == "__main__":
    asyncio.run(test_warmup()) 
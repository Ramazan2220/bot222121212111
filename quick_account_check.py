import asyncio
from instagrapi import Client
from instagrapi.exceptions import LoginRequired, ChallengeRequired, ClientError
import json
import os
from datetime import datetime
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from database.db_manager import init_db, get_instagram_accounts

def handle_challenge(client, password):
    """Обрабатывает challenge автоматически"""
    try:
        challenge_info = client.challenge_code_handler
        if challenge_info:
            # Если запрашивает пароль - вводим его
            if "password" in str(challenge_info).lower():
                print("   🔐 Challenge запрашивает пароль - вводим автоматически")
                try:
                    client.challenge_resolve(password)
                    return True
                except:
                    print("   ❌ Не удалось пройти challenge с паролем")
                    return False
            # Если запрашивает email код - пропускаем
            elif "email" in str(challenge_info).lower() or "code" in str(challenge_info).lower():
                print("   📧 Challenge запрашивает код с почты - пропускаем")
                return False
    except Exception as e:
        print(f"   ⚠️ Ошибка при обработке challenge: {e}")
    return False

async def test_account(username, password):
    """Test single Instagram account"""
    print(f"\n🔍 Тестирование: {username}")
    print("   📡 Попытка подключения к Instagram...")
    
    cl = Client()
    try:
        cl.login(username, password)
        print(f"✅ Успешный вход для {username}")
        print(f"   👤 ID пользователя: {cl.user_id}")
        print(f"   📊 Статус: Активен")
        return True
    except ChallengeRequired as e:
        print(f"🔐 Challenge для пользователя {username}: {str(e)[:100]}...")
        
        # Проверяем тип challenge
        challenge_str = str(e).lower()
        if "password" in challenge_str:
            print("   🔐 Тип challenge: Пароль - пробуем обработать автоматически")
            try:
                # Пытаемся пройти challenge с паролем
                if handle_challenge(cl, password):
                    print(f"✅ Challenge пройден для {username}")
                    print(f"   👤 ID пользователя: {cl.user_id}")
                    return True
                else:
                    print(f"❌ Не удалось пройти challenge для {username}")
                    return False
            except Exception as ex:
                print(f"❌ Ошибка при прохождении challenge для {username}: {ex}")
                return False
        elif "email" in challenge_str or "code" in challenge_str:
            print("   📧 Тип challenge: Email код - пропускаем (требует ручного ввода)")
            return False
        elif "scraping_warning" in challenge_str:
            print("   ⚠️ Тип challenge: Scraping warning - аккаунт заблокирован от API")
            return False
        else:
            print(f"   ❓ Неизвестный тип challenge - пропускаем")
            return False
    except LoginRequired as e:
        print(f"❌ Ошибка при входе для пользователя {username}: {str(e)}")
        return False
    except ClientError as e:
        print(f"❌ Ошибка при входе для пользователя {username}: {str(e)}")
        return False
    except Exception as e:
        print(f"❌ Неизвестная ошибка для пользователя {username}: {str(e)}")
        return False

async def main():
    # Инициализируем базу данных
    init_db()
    
    # Получаем все аккаунты
    accounts = get_instagram_accounts()
    
    if not accounts:
        print("\n❌ В базе данных нет аккаунтов!")
        print("Сначала добавьте аккаунты через бота.")
        return
    
    total_accounts = len(accounts)
    print(f"\n🔍 Начинаем проверку {total_accounts} аккаунтов...")
    print("🤖 Автоматическая обработка:")
    print("   ✅ Challenge с паролем - обрабатываем автоматически")
    print("   ❌ Challenge с email кодом - пропускаем")
    print("   ❌ Scraping warning - пропускаем")
    
    working_accounts = []
    challenge_accounts = []
    
    for i, account in enumerate(accounts, 1):
        print(f"\n📍 Аккаунт {i}/{total_accounts}")
        username = account.username
        password = account.password
        
        if not username or not password:
            print(f"❌ Пропуск аккаунта {i}: отсутствуют учетные данные")
            continue
            
        result = await test_account(username, password)
        if result:
            working_accounts.append({
                'id': account.id,
                'username': username,
                'password': password
            })
            
            # Создаем резервную копию рабочего аккаунта
            backup_dir = 'working_accounts'
            os.makedirs(backup_dir, exist_ok=True)
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_file = os.path.join(backup_dir, f'working_account_{timestamp}.json')
            
            with open(backup_file, 'w') as f:
                json.dump({
                    'id': account.id,
                    'username': username,
                    'password': password,
                    'tested_at': timestamp
                }, f, indent=2)
            
            print(f"\n💾 Создана резервная копия рабочего аккаунта: {backup_file}")
            
            # Если нашли рабочий аккаунт, спрашиваем о продолжении
            if len(working_accounts) >= 2:  # Если нашли 2 или больше рабочих аккаунтов
                response = input(f"\n🎯 Найдено {len(working_accounts)} рабочих аккаунтов. Продолжить поиск? (y/n): ")
                if response.lower() != 'y':
                    break
        else:
            # Сохраняем информацию о проблемных аккаунтах
            challenge_accounts.append({
                'id': account.id,
                'username': username,
                'status': 'challenge_or_error'
            })
        
        # Небольшая пауза между проверками
        if i < total_accounts:
            print("   ⏳ Пауза 3 секунды...")
            await asyncio.sleep(3)
    
    print(f"\n📊 Итоги проверки:")
    print(f"✅ Рабочих аккаунтов: {len(working_accounts)}")
    print(f"⚠️ Проблемных аккаунтов: {len(challenge_accounts)}")
    print(f"❌ Всего проверено: {total_accounts}")
    
    if working_accounts:
        print("\n📋 Список рабочих аккаунтов:")
        for acc in working_accounts:
            print(f"👤 {acc['username']} (ID: {acc['id']})")
        
        # Обновляем файл рабочих аккаунтов
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        working_file = f"working_accounts_{timestamp}.txt"
        
        with open(working_file, 'w', encoding='utf-8') as f:
            f.write("# Рабочие аккаунты Instagram\n")
            f.write(f"# Проверено: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            for acc in working_accounts:
                f.write(f"{acc['id']}:{acc['username']} (проверен автоматически)\n")
        
        print(f"\n💾 Обновлен файл рабочих аккаунтов: {working_file}")
    
    if challenge_accounts:
        print(f"\n⚠️ Аккаунты с проблемами ({len(challenge_accounts)}):")
        for acc in challenge_accounts:
            print(f"❓ {acc['username']} (ID: {acc['id']}) - требует ручной проверки")

if __name__ == "__main__":
    asyncio.run(main()) 
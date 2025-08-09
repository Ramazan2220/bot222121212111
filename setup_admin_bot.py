#!/usr/bin/env python3
"""
Скрипт для настройки админ бота
"""

import os
import sys
import re

def main():
    print("🔧 НАСТРОЙКА АДМИН БОТА")
    print("=" * 50)
    
    # Проверяем текущее состояние
    admin_token = os.getenv('ADMIN_BOT_TOKEN')
    telegram_token = os.getenv('TELEGRAM_BOT_TOKEN', '8092949155:AAEs6GSSqEU4C_3qNkskqVNAdcoAUHZi0fE')
    
    if admin_token:
        print(f"✅ ADMIN_BOT_TOKEN уже установлен")
        print(f"📝 Токен: {admin_token[:20]}...")
        
        choice = input("\n🤔 Хотите изменить токен? (y/N): ").lower()
        if choice not in ['y', 'yes', 'да']:
            test_admin_bot()
            return
    else:
        print("⚠️ ADMIN_BOT_TOKEN не установлен")
        print(f"🔄 Сейчас используется основной токен: {telegram_token[:20]}...")
    
    print("\n📱 СОЗДАНИЕ НОВОГО АДМИН БОТА:")
    print("1️⃣ Идите к @BotFather в Telegram")
    print("2️⃣ Отправьте команду: /newbot")
    print("3️⃣ Введите имя бота: Instagram Admin Panel")
    print("4️⃣ Введите username: instagram_admin_panel_bot (или другой доступный)")
    print("5️⃣ Скопируйте полученный токен")
    
    print("\n" + "="*50)
    
    # Получаем токен от пользователя
    while True:
        token = input("📋 Введите токен нового админ бота: ").strip()
        
        if not token:
            print("❌ Токен не может быть пустым")
            continue
        
        # Проверяем формат токена
        if not re.match(r'^\d+:[A-Za-z0-9_-]+$', token):
            print("❌ Неверный формат токена. Должен быть: 123456789:ABC-DEF1234...")
            continue
        
        if len(token) < 30:
            print("❌ Токен слишком короткий")
            continue
        
        break
    
    # Сохраняем токен
    save_choice = input(f"\n💾 Как сохранить токен?\n1️⃣ В переменной окружения (рекомендуется)\n2️⃣ В файле config\n3️⃣ Показать команды для ручной настройки\nВыберите (1/2/3): ").strip()
    
    if save_choice == "1":
        save_to_env(token)
    elif save_choice == "2":
        save_to_config(token)
    else:
        show_manual_setup(token)
    
    print("\n🧪 Тестируем админ бота...")
    test_admin_bot()

def save_to_env(token):
    """Сохранить токен в переменной окружения"""
    print(f"\n🔧 Установка переменной окружения...")
    
    # Определяем shell
    shell = os.getenv('SHELL', '/bin/bash')
    
    if 'zsh' in shell:
        config_file = os.path.expanduser('~/.zshrc')
    elif 'bash' in shell:
        config_file = os.path.expanduser('~/.bashrc')
    else:
        config_file = os.path.expanduser('~/.profile')
    
    export_line = f'export ADMIN_BOT_TOKEN="{token}"'
    
    try:
        # Проверяем, есть ли уже запись
        if os.path.exists(config_file):
            with open(config_file, 'r') as f:
                content = f.read()
            
            if 'ADMIN_BOT_TOKEN' in content:
                # Обновляем существующую запись
                updated_content = re.sub(
                    r'export ADMIN_BOT_TOKEN="[^"]*"',
                    export_line,
                    content
                )
                if updated_content == content:
                    # Если не нашли точное совпадение, добавляем в конец
                    updated_content = content + f'\n{export_line}\n'
            else:
                # Добавляем новую запись
                updated_content = content + f'\n# Instagram Admin Bot Token\n{export_line}\n'
        else:
            # Создаем новый файл
            updated_content = f'# Instagram Admin Bot Token\n{export_line}\n'
        
        # Сохраняем файл
        with open(config_file, 'w') as f:
            f.write(updated_content)
        
        print(f"✅ Токен добавлен в {config_file}")
        print(f"🔄 Выполните: source {config_file}")
        print(f"💡 Или перезапустите терминал")
        
        # Устанавливаем для текущей сессии
        os.environ['ADMIN_BOT_TOKEN'] = token
        print("✅ Токен установлен для текущей сессии")
        
    except Exception as e:
        print(f"❌ Ошибка сохранения: {e}")
        show_manual_setup(token)

def save_to_config(token):
    """Сохранить токен в файл конфигурации"""
    config_file = 'admin_bot/config/settings.py'
    
    try:
        with open(config_file, 'r') as f:
            content = f.read()
        
        # Заменяем строку с токеном
        updated_content = re.sub(
            r'ADMIN_BOT_TOKEN = os\.getenv\(\s*\'ADMIN_BOT_TOKEN\',\s*None\s*\)',
            f'ADMIN_BOT_TOKEN = os.getenv(\\n    \'ADMIN_BOT_TOKEN\', \\n    \'{token}\'  # Админ токен\\n)',
            content
        )
        
        with open(config_file, 'w') as f:
            f.write(updated_content)
        
        print(f"✅ Токен сохранен в {config_file}")
        print("⚠️ ВНИМАНИЕ: Токен сохранен в открытом виде в файле!")
        print("🔒 Для безопасности используйте переменные окружения")
        
    except Exception as e:
        print(f"❌ Ошибка сохранения в файл: {e}")
        show_manual_setup(token)

def show_manual_setup(token):
    """Показать инструкции для ручной настройки"""
    print(f"\n📋 РУЧНАЯ НАСТРОЙКА:")
    print(f"1️⃣ Добавьте в ~/.bashrc или ~/.zshrc:")
    print(f'    export ADMIN_BOT_TOKEN="{token}"')
    print(f"2️⃣ Выполните: source ~/.bashrc (или ~/.zshrc)")
    print(f"3️⃣ Или установите для текущей сессии:")
    print(f'    export ADMIN_BOT_TOKEN="{token}"')

def test_admin_bot():
    """Тестируем админ бота"""
    try:
        print("\n🧪 Запуск тестов...")
        import subprocess
        result = subprocess.run([sys.executable, 'test_admin_bot.py'], 
                              capture_output=True, text=True)
        
        if result.returncode == 0:
            print("✅ Тесты пройдены!")
            
            run_choice = input("\n🚀 Запустить админ бота? (Y/n): ").lower()
            if run_choice not in ['n', 'no', 'нет']:
                print("🤖 Запуск админ бота...")
                subprocess.run([sys.executable, 'test_admin_bot.py', '--run'])
        else:
            print("❌ Тесты не пройдены:")
            print(result.stdout)
            print(result.stderr)
    
    except Exception as e:
        print(f"❌ Ошибка тестирования: {e}")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n👋 Настройка прервана пользователем")
    except Exception as e:
        print(f"\n❌ Ошибка: {e}")
        sys.exit(1) 
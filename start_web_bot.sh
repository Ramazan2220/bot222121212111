#!/bin/bash

echo "🚀 Запуск Instagram Web Manager Bot..."

# Проверяем, установлен ли токен
if grep -q "YOUR_NEW_BOT_TOKEN_HERE" config.py; then
    echo "❌ Ошибка: Токен бота не настроен!"
    echo "📝 Откройте config.py и замените YOUR_NEW_BOT_TOKEN_HERE на ваш токен"
    echo ""
    echo "Инструкция:"
    echo "1. Откройте @BotFather в Telegram"
    echo "2. Создайте бота командой /newbot"
    echo "3. Скопируйте токен"
    echo "4. Вставьте в config.py"
    exit 1
fi

# Активируем виртуальное окружение, если есть
if [ -d "venv" ]; then
    source venv/bin/activate
fi

# Запускаем бота
python web_telegram_bot.py 
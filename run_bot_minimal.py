#!/usr/bin/env python3
import logging
from telegram.ext import Updater
from config import TELEGRAM_TOKEN

# Настраиваем логирование
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

def main():
    logger.info("Запуск бота...")
    
    # Создаем Updater
    updater = Updater(TELEGRAM_TOKEN, use_context=True)
    
    # Получаем информацию о боте
    bot_info = updater.bot.get_me()
    logger.info(f"Бот подключен: {bot_info.first_name} (@{bot_info.username})")
    
    # Запускаем бота
    logger.info("Запуск polling...")
    updater.start_polling()
    logger.info("Бот запущен и готов к работе!")
    
    # Ждем завершения
    updater.idle()

if __name__ == '__main__':
    main() 
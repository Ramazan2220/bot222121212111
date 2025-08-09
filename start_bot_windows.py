#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Windows-совместимый запуск Instagram Telegram Bot
Исправляет проблемы с кодировкой в Windows
"""

import os
import sys
import locale
import logging

def setup_windows_encoding():
    """Настройка кодировки для Windows"""
    # Устанавливаем UTF-8 для всех потоков ввода/вывода
    os.environ['PYTHONIOENCODING'] = 'utf-8'
    os.environ['PYTHONLEGACYWINDOWSSTDIO'] = 'utf-8'
    
    # Настройка системной локали
    try:
        locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')
    except locale.Error:
        try:
            locale.setlocale(locale.LC_ALL, 'C.UTF-8')
        except locale.Error:
            pass  # Игнорируем если не можем установить UTF-8
    
    # Настройка логгера для UTF-8
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('bot.log', encoding='utf-8')
        ]
    )
    
    print("[OK] Кодировка настроена для Windows")

def main():
    """Основная функция запуска"""
    print("=" * 50)
    print("INSTAGRAM TELEGRAM BOT - WINDOWS EDITION")
    print("=" * 50)
    
    # Настройка кодировки
    setup_windows_encoding()
    
    # Импорт и запуск основного модуля
    try:
        print("[INFO] Импортируем основной модуль...")
        import main
        print("[INFO] Запускаем бота...")
        main.main()
    except Exception as e:
        print(f"[ERROR] Ошибка запуска: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main() 
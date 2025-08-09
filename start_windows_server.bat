@echo off
chcp 65001 > nul
title Instagram Telegram Bot - Windows Server

echo ===============================================
echo   INSTAGRAM TELEGRAM BOT - WINDOWS SERVER
echo ===============================================
echo.

echo [INFO] Настройка кодировки UTF-8...
set PYTHONIOENCODING=utf-8
set PYTHONLEGACYWINDOWSSTDIO=utf-8

echo [INFO] Запуск бота...
echo.

python start_bot_windows.py

echo.
echo [INFO] Бот остановлен
pause 
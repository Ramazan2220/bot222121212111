@echo off
chcp 65001 > nul
echo [START] Исправляем проблемы для Windows...
echo.

echo [STEP 1] Исправляем кодировку эмодзи...
python fix_windows_encoding.py

echo.
echo [STEP 2] Проверяем исправленный main.py...
echo [LOG] Structured Logger отключен (файл удален)
echo [LOG] Эмодзи заменены на текст

echo.
echo [OK] Исправления применены!
echo [INFO] Теперь можно запускать: python main.py
echo.
pause 
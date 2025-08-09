#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Скрипт для запуска веб-дашборда Instagram Telegram Bot
"""

import os
import sys
import subprocess
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def setup_virtual_environment():
    """Создает и настраивает виртуальное окружение"""
    venv_path = 'web_env'
    
    if not os.path.exists(venv_path):
        logger.info("Создание виртуального окружения...")
        try:
            subprocess.run([sys.executable, '-m', 'venv', venv_path], check=True)
            logger.info("✅ Виртуальное окружение создано")
        except subprocess.CalledProcessError as e:
            logger.error(f"❌ Ошибка создания виртуального окружения: {e}")
            return None
    
    # Определяем путь к Python в виртуальном окружении
    if os.name == 'nt':  # Windows
        python_path = os.path.join(venv_path, 'Scripts', 'python.exe')
        pip_path = os.path.join(venv_path, 'Scripts', 'pip.exe')
    else:  # Unix/Linux/macOS
        python_path = os.path.join(venv_path, 'bin', 'python')
        pip_path = os.path.join(venv_path, 'bin', 'pip')
    
    return python_path, pip_path

def install_dependencies(pip_path):
    """Устанавливает необходимые зависимости"""
    required_packages = ['flask', 'flask-cors']
    
    logger.info("Проверка и установка зависимостей...")
    try:
        # Проверяем, установлены ли пакеты
        result = subprocess.run([pip_path, 'list'], capture_output=True, text=True, check=True)
        installed_packages = result.stdout.lower()
        
        packages_to_install = []
        for package in required_packages:
            if package not in installed_packages:
                packages_to_install.append(package)
        
        if packages_to_install:
            logger.info(f"Установка пакетов: {', '.join(packages_to_install)}")
            subprocess.run([pip_path, 'install'] + packages_to_install, check=True)
            logger.info("✅ Зависимости установлены")
        else:
            logger.info("✅ Все зависимости уже установлены")
        
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ Ошибка установки зависимостей: {e}")
        return False

def check_web_dashboard():
    """Проверяет наличие файлов веб-дашборда"""
    required_files = [
        'web-dashboard/index.html',
        'web-dashboard/js/api.js',
        'web-dashboard/css/mobile.css'
    ]
    
    missing_files = []
    for file_path in required_files:
        if not os.path.exists(file_path):
            missing_files.append(file_path)
    
    if missing_files:
        logger.error(f"Отсутствуют файлы веб-дашборда: {', '.join(missing_files)}")
        return False
    
    return True

def main():
    """Главная функция запуска"""
    logger.info("🚀 Запуск веб-дашборда Instagram Telegram Bot")
    
    # Настраиваем виртуальное окружение
    venv_result = setup_virtual_environment()
    if not venv_result:
        sys.exit(1)
    
    python_path, pip_path = venv_result
    
    # Устанавливаем зависимости
    if not install_dependencies(pip_path):
        sys.exit(1)
    
    # Проверяем файлы веб-дашборда
    if not check_web_dashboard():
        sys.exit(1)
    
    # Проверяем наличие web_api.py
    if not os.path.exists('web_api.py'):
        logger.error("Файл web_api.py не найден!")
        sys.exit(1)
    
    logger.info("✅ Все проверки пройдены")
    logger.info("🌐 Запуск веб-сервера на http://localhost:5000")
    logger.info("📱 Веб-дашборд будет доступен по адресу: http://localhost:5000")
    logger.info("🔧 API будет доступно по адресу: http://localhost:5000/api")
    logger.info("")
    logger.info("Для остановки сервера нажмите Ctrl+C")
    logger.info("=" * 60)
    
    try:
        # Запускаем веб-сервер с использованием Python из виртуального окружения
        subprocess.run([python_path, 'web_api.py'], check=True)
    except KeyboardInterrupt:
        logger.info("\n🛑 Сервер остановлен пользователем")
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ Ошибка при запуске сервера: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"❌ Неожиданная ошибка: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main() 
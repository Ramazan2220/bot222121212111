#!/usr/bin/env python3
"""
ОБЩИЙ MULTIPROCESSING MANAGER
Создает единый Manager для всех процессов (админ бот + основной бот)
"""

import multiprocessing as mp
import time
import os
import signal
import sys
from pathlib import Path

# Путь к файлу с PID менеджера
MANAGER_PID_FILE = Path("data/manager.pid")
MANAGER_PORT = 50000

def start_shared_manager():
    """Запускает общий Manager сервер"""
    print("🚀 Запуск общего multiprocessing Manager...")
    
    # Создаем директорию если нет
    MANAGER_PID_FILE.parent.mkdir(exist_ok=True)
    
    # Проверяем не запущен ли уже
    if MANAGER_PID_FILE.exists():
        try:
            with open(MANAGER_PID_FILE, 'r') as f:
                old_pid = int(f.read().strip())
            os.kill(old_pid, 0)  # Проверяем что процесс живой
            print(f"⚠️ Manager уже запущен (PID: {old_pid})")
            return
        except (OSError, ValueError, ProcessLookupError):
            print("🧹 Очищаю старый PID файл...")
            MANAGER_PID_FILE.unlink()
    
    # Запускаем Manager
    try:
        manager = mp.Manager()
        
        # Общие данные
        shared_users = manager.dict()
        user_added_event = mp.Event()
        user_removed_event = mp.Event()
        
        print(f"✅ Manager запущен на порту {MANAGER_PORT}")
        print(f"📝 PID: {os.getpid()}")
        
        # Сохраняем PID
        with open(MANAGER_PID_FILE, 'w') as f:
            f.write(str(os.getpid()))
        
        # Обработчик сигналов
        def cleanup(signum, frame):
            print(f"\n🛑 Получен сигнал {signum}, останавливаю Manager...")
            if MANAGER_PID_FILE.exists():
                MANAGER_PID_FILE.unlink()
            sys.exit(0)
        
        signal.signal(signal.SIGTERM, cleanup)
        signal.signal(signal.SIGINT, cleanup)
        
        print("🟢 Manager готов к работе!")
        print("📡 Ожидание подключений...")
        
        # Бесконечный цикл
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            cleanup(signal.SIGINT, None)
            
    except Exception as e:
        print(f"💥 Ошибка запуска Manager: {e}")
        if MANAGER_PID_FILE.exists():
            MANAGER_PID_FILE.unlink()
        raise

def stop_shared_manager():
    """Останавливает общий Manager"""
    if not MANAGER_PID_FILE.exists():
        print("🔍 Manager PID файл не найден")
        return False
    
    try:
        with open(MANAGER_PID_FILE, 'r') as f:
            pid = int(f.read().strip())
        
        print(f"🛑 Останавливаю Manager (PID: {pid})...")
        os.kill(pid, signal.SIGTERM)
        
        # Ждем завершения
        for _ in range(10):
            try:
                os.kill(pid, 0)
                time.sleep(0.5)
            except ProcessLookupError:
                break
        
        if MANAGER_PID_FILE.exists():
            MANAGER_PID_FILE.unlink()
        
        print("✅ Manager остановлен")
        return True
        
    except (ValueError, ProcessLookupError, OSError) as e:
        print(f"⚠️ Ошибка остановки Manager: {e}")
        if MANAGER_PID_FILE.exists():
            MANAGER_PID_FILE.unlink()
        return False

def is_manager_running():
    """Проверяет запущен ли Manager"""
    if not MANAGER_PID_FILE.exists():
        return False
    
    try:
        with open(MANAGER_PID_FILE, 'r') as f:
            pid = int(f.read().strip())
        os.kill(pid, 0)
        return True
    except (ValueError, ProcessLookupError, OSError):
        return False

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        command = sys.argv[1]
        if command == "start":
            start_shared_manager()
        elif command == "stop":
            stop_shared_manager()
        elif command == "status":
            if is_manager_running():
                print("🟢 Manager запущен")
            else:
                print("🔴 Manager не запущен")
        else:
            print("Использование: python shared_manager.py [start|stop|status]")
    else:
        print("🎯 Автоматический запуск Manager...")
        start_shared_manager() 
"""
ОБЩИЙ MULTIPROCESSING MANAGER
Создает единый Manager для всех процессов (админ бот + основной бот)
"""

import multiprocessing as mp
import time
import os
import signal
import sys
from pathlib import Path

# Путь к файлу с PID менеджера
MANAGER_PID_FILE = Path("data/manager.pid")
MANAGER_PORT = 50000

def start_shared_manager():
    """Запускает общий Manager сервер"""
    print("🚀 Запуск общего multiprocessing Manager...")
    
    # Создаем директорию если нет
    MANAGER_PID_FILE.parent.mkdir(exist_ok=True)
    
    # Проверяем не запущен ли уже
    if MANAGER_PID_FILE.exists():
        try:
            with open(MANAGER_PID_FILE, 'r') as f:
                old_pid = int(f.read().strip())
            os.kill(old_pid, 0)  # Проверяем что процесс живой
            print(f"⚠️ Manager уже запущен (PID: {old_pid})")
            return
        except (OSError, ValueError, ProcessLookupError):
            print("🧹 Очищаю старый PID файл...")
            MANAGER_PID_FILE.unlink()
    
    # Запускаем Manager
    try:
        manager = mp.Manager()
        
        # Общие данные
        shared_users = manager.dict()
        user_added_event = mp.Event()
        user_removed_event = mp.Event()
        
        print(f"✅ Manager запущен на порту {MANAGER_PORT}")
        print(f"📝 PID: {os.getpid()}")
        
        # Сохраняем PID
        with open(MANAGER_PID_FILE, 'w') as f:
            f.write(str(os.getpid()))
        
        # Обработчик сигналов
        def cleanup(signum, frame):
            print(f"\n🛑 Получен сигнал {signum}, останавливаю Manager...")
            if MANAGER_PID_FILE.exists():
                MANAGER_PID_FILE.unlink()
            sys.exit(0)
        
        signal.signal(signal.SIGTERM, cleanup)
        signal.signal(signal.SIGINT, cleanup)
        
        print("🟢 Manager готов к работе!")
        print("📡 Ожидание подключений...")
        
        # Бесконечный цикл
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            cleanup(signal.SIGINT, None)
            
    except Exception as e:
        print(f"💥 Ошибка запуска Manager: {e}")
        if MANAGER_PID_FILE.exists():
            MANAGER_PID_FILE.unlink()
        raise

def stop_shared_manager():
    """Останавливает общий Manager"""
    if not MANAGER_PID_FILE.exists():
        print("🔍 Manager PID файл не найден")
        return False
    
    try:
        with open(MANAGER_PID_FILE, 'r') as f:
            pid = int(f.read().strip())
        
        print(f"🛑 Останавливаю Manager (PID: {pid})...")
        os.kill(pid, signal.SIGTERM)
        
        # Ждем завершения
        for _ in range(10):
            try:
                os.kill(pid, 0)
                time.sleep(0.5)
            except ProcessLookupError:
                break
        
        if MANAGER_PID_FILE.exists():
            MANAGER_PID_FILE.unlink()
        
        print("✅ Manager остановлен")
        return True
        
    except (ValueError, ProcessLookupError, OSError) as e:
        print(f"⚠️ Ошибка остановки Manager: {e}")
        if MANAGER_PID_FILE.exists():
            MANAGER_PID_FILE.unlink()
        return False

def is_manager_running():
    """Проверяет запущен ли Manager"""
    if not MANAGER_PID_FILE.exists():
        return False
    
    try:
        with open(MANAGER_PID_FILE, 'r') as f:
            pid = int(f.read().strip())
        os.kill(pid, 0)
        return True
    except (ValueError, ProcessLookupError, OSError):
        return False

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        command = sys.argv[1]
        if command == "start":
            start_shared_manager()
        elif command == "stop":
            stop_shared_manager()
        elif command == "status":
            if is_manager_running():
                print("🟢 Manager запущен")
            else:
                print("🔴 Manager не запущен")
        else:
            print("Использование: python shared_manager.py [start|stop|status]")
    else:
        print("🎯 Автоматический запуск Manager...")
        start_shared_manager() 
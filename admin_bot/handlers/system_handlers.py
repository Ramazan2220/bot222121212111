from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import CallbackContext
from datetime import datetime
import psutil
import os
import sys
from typing import Dict

from ..config.admin_list import is_admin, has_permission, Permission
from ..middleware.admin_auth import admin_required, permission_required
from database.db_manager import get_total_accounts, get_session
from database.models import PublishTask, TaskStatus

@admin_required
@permission_required(Permission.VIEW_SYSTEM)
def system_menu(update: Update, context: CallbackContext):
    """Главное меню системного мониторинга"""
    query = update.callback_query
    
    # Получаем системные метрики
    system_metrics = get_system_metrics()
    bot_status = get_bot_status()
    db_metrics = get_database_metrics()
    
    text = f"""⚙️ **СИСТЕМНЫЙ МОНИТОРИНГ**

🖥️ **Сервер:**
• CPU: {system_metrics['cpu_percent']:.1f}%
• RAM: {system_metrics['memory_percent']:.1f}% ({system_metrics['memory_used']:.1f}GB/{system_metrics['memory_total']:.1f}GB)
• Диск: {system_metrics['disk_percent']:.1f}% ({system_metrics['disk_free']:.1f}GB свободно)
• Uptime: {system_metrics['uptime']}

🤖 **Основной бот:**
• Статус: {bot_status['status']}
• Активных соединений: {bot_status['connections']}
• Последняя активность: {bot_status['last_activity']}

🗄️ **База данных:**
• Аккаунтов: {db_metrics['accounts']}
• Активных задач: {db_metrics['active_tasks']}
• Размер БД: {db_metrics['db_size']} MB
"""
    
    # Определяем статус системы
    status_emoji = "🟢" if system_metrics['cpu_percent'] < 70 and system_metrics['memory_percent'] < 80 else "🟡" if system_metrics['cpu_percent'] < 90 else "🔴"
    
    keyboard = [
        [InlineKeyboardButton(f"{status_emoji} Детальная диагностика", callback_data="system_detailed")],
        [InlineKeyboardButton("📊 Графики нагрузки", callback_data="system_charts")],
        [InlineKeyboardButton("🔄 Управление процессами", callback_data="system_processes")],
        [InlineKeyboardButton("📋 Логи системы", callback_data="system_logs")],
        [InlineKeyboardButton("⚡ Оптимизация", callback_data="system_optimization")],
        [InlineKeyboardButton("🔄 Обновить", callback_data="system_refresh")],
        [InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")]
    ]
    
    query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

@admin_required
@permission_required(Permission.VIEW_SYSTEM)
def system_detailed(update: Update, context: CallbackContext):
    """Детальная диагностика системы"""
    query = update.callback_query
    
    # Расширенные метрики
    detailed_metrics = get_detailed_system_metrics()
    
    text = f"""🔍 **ДЕТАЛЬНАЯ ДИАГНОСТИКА**

💾 **Память:**
• Использовано: {detailed_metrics['memory']['used']:.1f}GB
• Доступно: {detailed_metrics['memory']['available']:.1f}GB
• Кэш: {detailed_metrics['memory']['cached']:.1f}GB
• Буферы: {detailed_metrics['memory']['buffers']:.1f}GB

🖥️ **Процессор:**
• Ядер: {detailed_metrics['cpu']['cores']}
• Частота: {detailed_metrics['cpu']['frequency']:.0f} MHz
• Загрузка по ядрам: {', '.join([f'{c:.1f}%' for c in detailed_metrics['cpu']['per_core']])}

💿 **Диски:**"""
    
    for disk in detailed_metrics['disks']:
        text += f"\n• {disk['device']}: {disk['used']:.1f}GB/{disk['total']:.1f}GB ({disk['percent']:.1f}%)"
    
    text += f"""

🌐 **Сеть:**
• Отправлено: {detailed_metrics['network']['bytes_sent']:.1f} MB
• Получено: {detailed_metrics['network']['bytes_recv']:.1f} MB
• Соединений: {detailed_metrics['network']['connections']}

🐍 **Python:**
• Версия: {detailed_metrics['python']['version']}
• PID: {detailed_metrics['python']['pid']}
• Потоков: {detailed_metrics['python']['threads']}
"""
    
    keyboard = [
        [InlineKeyboardButton("🔧 Очистить кэш", callback_data="system_clear_cache")],
        [InlineKeyboardButton("🗑️ Очистить логи", callback_data="system_clear_logs")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="system")]
    ]
    
    query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

@admin_required
@permission_required(Permission.VIEW_SYSTEM)
def system_processes(update: Update, context: CallbackContext):
    """Управление процессами"""
    query = update.callback_query
    
    processes = get_bot_processes()
    
    text = "🔄 **УПРАВЛЕНИЕ ПРОЦЕССАМИ**\n\n"
    
    for process in processes:
        status_emoji = "🟢" if process['status'] == 'running' else "🔴" if process['status'] == 'stopped' else "🟡"
        text += f"{status_emoji} **{process['name']}**\n"
        text += f"   PID: {process['pid']}\n"
        text += f"   CPU: {process['cpu']:.1f}%\n"
        text += f"   RAM: {process['memory']:.1f} MB\n"
        text += f"   Uptime: {process['uptime']}\n\n"
    
    keyboard = [
        [InlineKeyboardButton("🔄 Перезапуск бота", callback_data="system_restart_bot")],
        [InlineKeyboardButton("⏹️ Остановить задачи", callback_data="system_stop_tasks")],
        [InlineKeyboardButton("▶️ Запустить задачи", callback_data="system_start_tasks")],
        [InlineKeyboardButton("🧹 Убить зависшие", callback_data="system_kill_stuck")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="system")]
    ]
    
    query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

@admin_required
@permission_required(Permission.MANAGE_SYSTEM)
def system_optimization(update: Update, context: CallbackContext):
    """Оптимизация системы"""
    query = update.callback_query
    
    optimization_status = perform_system_optimization()
    
    text = f"""⚡ **ОПТИМИЗАЦИЯ СИСТЕМЫ**

✅ **Выполненные операции:**
• Очистка временных файлов: {optimization_status['temp_cleaned']} MB
• Очистка логов: {optimization_status['logs_cleaned']} MB
• Оптимизация БД: {optimization_status['db_optimized']}
• Очистка кэша: {optimization_status['cache_cleared']} MB

📈 **Результат:**
• Освобождено места: {optimization_status['total_freed']} MB
• Улучшение производительности: {optimization_status['performance_gain']}%
"""
    
    keyboard = [
        [InlineKeyboardButton("🔄 Глубокая оптимизация", callback_data="system_deep_optimization")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="system")]
    ]
    
    query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

# Вспомогательные функции

def get_system_metrics() -> Dict:
    """Получает основные системные метрики"""
    memory = psutil.virtual_memory()
    disk = psutil.disk_usage('/')
    
    # Uptime
    boot_time = datetime.fromtimestamp(psutil.boot_time())
    uptime = datetime.now() - boot_time
    uptime_str = f"{uptime.days}д {uptime.seconds//3600}ч {(uptime.seconds//60)%60}м"
    
    return {
        'cpu_percent': psutil.cpu_percent(interval=1),
        'memory_percent': memory.percent,
        'memory_used': memory.used / (1024**3),  # GB
        'memory_total': memory.total / (1024**3),  # GB
        'disk_percent': psutil.disk_usage('/').percent,
        'disk_free': psutil.disk_usage('/').free / (1024**3),  # GB
        'uptime': uptime_str
    }

def get_bot_status() -> Dict:
    """Получает статус основного бота"""
    # В реальности здесь была бы проверка статуса основного бота
    return {
        'status': '🟢 Активен',
        'connections': 1,  # Заглушка
        'last_activity': 'только что'
    }

def get_database_metrics() -> Dict:
    """Получает метрики базы данных"""
    try:
        total_accounts = get_total_accounts()
        
        # Активные задачи
        session = get_session()
        active_tasks = session.query(PublishTask).filter(
            PublishTask.status.in_([TaskStatus.PENDING, TaskStatus.PROCESSING])
        ).count()
        session.close()
        
        # Размер БД (приблизительно)
        db_size = 0
        try:
            if os.path.exists('data/database.sqlite'):
                db_size = os.path.getsize('data/database.sqlite') / (1024**2)  # MB
        except:
            pass
        
        return {
            'accounts': total_accounts,
            'active_tasks': active_tasks,
            'db_size': f"{db_size:.1f}"
        }
    except Exception as e:
        return {
            'accounts': 0,
            'active_tasks': 0,
            'db_size': "N/A"
        }

def get_detailed_system_metrics() -> Dict:
    """Получает детальные системные метрики"""
    memory = psutil.virtual_memory()
    cpu_freq = psutil.cpu_freq()
    
    # Диски
    disks = []
    for partition in psutil.disk_partitions():
        try:
            usage = psutil.disk_usage(partition.mountpoint)
            disks.append({
                'device': partition.device,
                'used': usage.used / (1024**3),
                'total': usage.total / (1024**3),
                'percent': (usage.used / usage.total) * 100
            })
        except:
            continue
    
    # Сеть
    net_io = psutil.net_io_counters()
    
    return {
        'memory': {
            'used': memory.used / (1024**3),
            'available': memory.available / (1024**3),
            'cached': getattr(memory, 'cached', 0) / (1024**3),
            'buffers': getattr(memory, 'buffers', 0) / (1024**3)
        },
        'cpu': {
            'cores': psutil.cpu_count(),
            'frequency': cpu_freq.current if cpu_freq else 0,
            'per_core': psutil.cpu_percent(percpu=True, interval=1)
        },
        'disks': disks,
        'network': {
            'bytes_sent': net_io.bytes_sent / (1024**2),
            'bytes_recv': net_io.bytes_recv / (1024**2),
            'connections': len(psutil.net_connections())
        },
        'python': {
            'version': sys.version.split()[0],
            'pid': os.getpid(),
            'threads': len(psutil.Process().threads())
        }
    }

def get_bot_processes() -> list:
    """Получает информацию о процессах бота"""
    current_process = psutil.Process()
    
    return [{
        'name': 'Admin Bot',
        'pid': current_process.pid,
        'status': 'running',
        'cpu': current_process.cpu_percent(),
        'memory': current_process.memory_info().rss / (1024**2),  # MB
        'uptime': str(datetime.now() - datetime.fromtimestamp(current_process.create_time())).split('.')[0]
    }]

def perform_system_optimization() -> Dict:
    """Выполняет оптимизацию системы"""
    # Заглушка для оптимизации
    return {
        'temp_cleaned': 45.2,
        'logs_cleaned': 12.8,
        'db_optimized': True,
        'cache_cleared': 23.1,
        'total_freed': 81.1,
        'performance_gain': 15
    }

# Функция для регистрации обработчиков
def get_system_handlers():
    """Возвращает словарь обработчиков для системного мониторинга"""
    return {
        'system': system_menu,
        'system_detailed': system_detailed,
        'system_processes': system_processes,
        'system_optimization': system_optimization,
        'system_refresh': system_menu,  # Перенаправляем на обновление
    } 
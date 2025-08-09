"""
🏗️ РЕАЛИСТИЧНЫЙ КАЛЬКУЛЯТОР МИКРОСЕРВИСОВ
Основан на реальных ценах и возможности запуска нескольких сервисов на одном сервере
"""

from typing import Dict, List
from dataclasses import dataclass

@dataclass 
class RealServer:
    """Реальная конфигурация сервера"""
    name: str
    ram_gb: int
    cpu_cores: int
    disk_gb: int
    monthly_cost: float
    
# Реальные серверы от провайдеров
REAL_SERVERS = {
    'small': RealServer('Hetzner CX31', 8, 2, 80, 7.5),
    'medium': RealServer('Hetzner CX41', 16, 4, 160, 15.5), 
    'large': RealServer('Hetzner CX51', 32, 8, 240, 30),
    'xlarge': RealServer('Hetzner CCX33', 64, 8, 320, 60),
    'xxlarge': RealServer('Hetzner CCX53', 128, 16, 640, 120)
}

class RealisticMicroservicesCalculator:
    """
    🎯 РЕАЛИСТИЧНЫЙ КАЛЬКУЛЯТОР МИКРОСЕРВИСОВ
    Учитывает реальные цены и плотность размещения сервисов
    """
    
    def __init__(self):
        pass
    
    def calculate_real_requirements(self, users: int, accounts_per_user: int = 300) -> Dict:
        """Реалистичный расчет требований к ресурсам С УЧЕТОМ ПУЛОВ И ОПТИМИЗАЦИЙ"""
        total_accounts = users * accounts_per_user
        
        # === ПРИМЕНЯЕМ ТЕ ЖЕ ОПТИМИЗАЦИИ ЧТО И В МОНОЛИТЕ ===
        # 1. Только 25% аккаунтов активны одновременно
        active_accounts = int(total_accounts * 0.25)
        sleeping_accounts = int(total_accounts * 0.50)  # 50% в спящем режиме
        inactive_accounts = total_accounts - active_accounts - sleeping_accounts  # 25% полностью неактивных
        
        # 2. Instagram Client Pool - экономия 65% памяти
        client_pool_memory_reduction = 0.65
        
        # 3. Спящий режим - экономия 80% памяти для спящих клиентов
        sleeping_memory_multiplier = 0.2  # 20% от обычной памяти
        
        # === РЕАЛИСТИЧНЫЕ ТРЕБОВАНИЯ С ОПТИМИЗАЦИЯМИ ===
        requirements = {
            'database_proxy': {
                'name': 'Database & Proxy',
                'description': 'PostgreSQL + Redis + Proxy rotation + Connection Pool',
                'ram_gb': 4 + (users * 0.1),  # 4GB базовая + 0.1GB на пользователя
                'cpu_cores': 2 + (users // 50),  # 2 базовых + 1 на каждые 50 пользователей
                'disk_gb': 20 + (users * 2),   # 20GB базовая + 2GB на пользователя
                'daily_requests': active_accounts * 20,  # Только активные аккаунты
                'concurrent_users': min(users, 50)  # Максимум 50 одновременных
            },
            
            'content_publishing': {
                'name': 'Content Publishing',
                'description': 'Публикация + Instagram Client Pool + спящий режим',
                # ПАМЯТЬ: активные клиенты + спящие клиенты с оптимизацией
                'ram_gb': 2 + (
                    (active_accounts * 0.004) * (1 - client_pool_memory_reduction) +  # Активные с пулом
                    (sleeping_accounts * 0.004 * sleeping_memory_multiplier)  # Спящие
                ),
                'cpu_cores': 1 + (active_accounts // 3000),  # CPU только для активных
                'disk_gb': 50 + (users * 5),   # Временное хранение медиа
                'daily_publications': active_accounts * 5,  # Только активные публикуют
                'peak_per_hour': (active_accounts * 5) // 16,
                'optimizations': 'Client Pool + Sleeping Mode'
            },
            
            'scheduled_publishing': {
                'name': 'Scheduled Publishing',
                'description': 'Планировщик + очередь с пулом соединений',
                'ram_gb': 1 + (active_accounts * 0.001),  # Только активные в очереди
                'cpu_cores': 1,  # Планировщик не требует много CPU
                'disk_gb': 10 + (users * 1),   # Хранение расписаний
                'scheduled_jobs': active_accounts * 2,  # Только активные
                'queue_size': active_accounts // 10
            },
            
            'account_warmup': {
                'name': 'Account Warmup',
                'description': 'Прогрев + Activity Optimizer + ротация',
                # Прогрев затрагивает еще меньше аккаунтов - только 20% от активных
                'warmup_accounts': int(active_accounts * 0.2),
                'ram_gb': 2 + (int(active_accounts * 0.2) * 0.003),  # Только аккаунты в прогреве
                'cpu_cores': 1 + (int(active_accounts * 0.2) // 1000),  # CPU для прогреваемых
                'disk_gb': 10 + (users * 2),   # Логи прогрева
                'warmup_requests_per_hour': int(active_accounts * 0.2 * 10),
                'optimizations': 'Activity Optimizer + ротация каждые 45 мин'
            },
            
            'account_processing': {
                'name': 'Account Processing',
                'description': 'IMAP + загрузка + IMAP Pool',
                'ram_gb': 2 + (users * 0.05),  # 50MB на пользователя (не зависит от аккаунтов)
                'cpu_cores': 1 + (users // 100),  # 1 ядро на 100 пользователей
                'disk_gb': 20 + (users * 3),   # Временные данные аккаунтов
                'daily_imap_checks': active_accounts * 2,  # Только активные проверяются
                'monthly_uploads': users,
                'optimizations': 'IMAP Connection Pool'
            },
            
            'profile_design': {
                'name': 'Profile Design',
                'description': 'Генерация профилей (пакетная обработка)',
                # Профильный дизайн - пакетная обработка, не все аккаунты одновременно
                'ram_gb': 3 + (users * 0.02),  # 20MB на пользователя
                'cpu_cores': 2 + (users // 200),  # CPU для генерации контента
                'disk_gb': 30 + (users * 10),  # Шаблоны и медиа
                'monthly_design_jobs': users * 2 * 300,  # 2 раза в месяц × 300 аккаунтов
                'daily_design_jobs': (users * 2 * 300) // 30,  # Распределено по дням
                'optimizations': 'Пакетная обработка + очередь'
            }
        }
        
        # Добавляем информацию об оптимизациях
        requirements['optimization_summary'] = {
            'total_accounts': total_accounts,
            'active_accounts': active_accounts,
            'sleeping_accounts': sleeping_accounts,
            'inactive_accounts': inactive_accounts,
            'client_pool_memory_savings': f'{client_pool_memory_reduction * 100}%',
            'sleeping_mode_savings': f'{(1 - sleeping_memory_multiplier) * 100}%',
            'activity_optimizer': 'Ротация каждые 45 мин'
        }
        
        return requirements
    
    def pack_services_to_servers(self, requirements: Dict) -> Dict:
        """Упаковка сервисов на минимальное количество серверов"""
        # Фильтруем только сервисы (исключаем optimization_summary)
        services = [service for key, service in requirements.items() 
                   if key != 'optimization_summary' and 'ram_gb' in service]
        servers_needed = []
        
        # Сортируем сервисы по требованиям (сначала самые тяжелые)
        services.sort(key=lambda x: x['ram_gb'] * x['cpu_cores'], reverse=True)
        
        for service in services:
            # Ищем подходящий сервер для размещения
            best_server = self._find_best_server_for_service(service)
            
            # Проверяем, можем ли разместить на существующих серверах
            placed = False
            for server_config in servers_needed:
                if self._can_fit_service(server_config, service):
                    server_config['services'].append(service)
                    server_config['used_ram'] += service['ram_gb']
                    server_config['used_cpu'] += service['cpu_cores']
                    server_config['used_disk'] += service['disk_gb']
                    placed = True
                    break
            
            if not placed:
                # Создаем новый сервер
                servers_needed.append({
                    'server_type': best_server,
                    'services': [service],
                    'used_ram': service['ram_gb'],
                    'used_cpu': service['cpu_cores'],
                    'used_disk': service['disk_gb'],
                    'total_ram': best_server.ram_gb,
                    'total_cpu': best_server.cpu_cores,
                    'total_disk': best_server.disk_gb,
                    'monthly_cost': best_server.monthly_cost
                })
        
        return {
            'servers': servers_needed,
            'total_servers': len(servers_needed),
            'total_cost': sum(s['monthly_cost'] for s in servers_needed)
        }
    
    def _find_best_server_for_service(self, service: Dict) -> RealServer:
        """Находит минимальный сервер способный запустить сервис"""
        required_ram = service['ram_gb'] * 1.2  # 20% запас
        required_cpu = service['cpu_cores']
        required_disk = service['disk_gb'] * 1.1  # 10% запас
        
        for server in REAL_SERVERS.values():
            if (server.ram_gb >= required_ram and 
                server.cpu_cores >= required_cpu and 
                server.disk_gb >= required_disk):
                return server
        
        # Если не подходит ни один, возвращаем самый большой
        return REAL_SERVERS['xxlarge']
    
    def _can_fit_service(self, server_config: Dict, service: Dict) -> bool:
        """Проверяет, поместится ли сервис на существующий сервер"""
        new_ram = server_config['used_ram'] + service['ram_gb']
        new_cpu = server_config['used_cpu'] + service['cpu_cores']
        new_disk = server_config['used_disk'] + service['disk_gb']
        
        # Оставляем 20% запас
        return (new_ram <= server_config['total_ram'] * 0.8 and
                new_cpu <= server_config['total_cpu'] * 0.8 and
                new_disk <= server_config['total_disk'] * 0.9)
    
    def calculate_realistic_cost(self, users: int, accounts_per_user: int = 300) -> Dict:
        """Полный реалистичный расчет стоимости микросервисов"""
        requirements = self.calculate_real_requirements(users, accounts_per_user)
        server_layout = self.pack_services_to_servers(requirements)
        
        # Сравнение с монолитом
        from utils.cost_calculator import cost_calculator
        monolith_cost = cost_calculator.calculate_cost_per_user(users, accounts_per_user)
        
        cost_per_user = server_layout['total_cost'] / users
        monolith_cost_per_user = monolith_cost['server_options']['optimal']['cost_per_user']
        
        return {
            'microservices': {
                'requirements': requirements,
                'server_layout': server_layout,
                'total_cost': server_layout['total_cost'],
                'cost_per_user': round(cost_per_user, 2),
                'servers_count': server_layout['total_servers']
            },
            'monolith': {
                'cost_per_user': monolith_cost_per_user
            },
            'comparison': {
                'ratio': round(cost_per_user / monolith_cost_per_user, 2),
                'difference': round(cost_per_user - monolith_cost_per_user, 2),
                'is_microservices_cheaper': cost_per_user < monolith_cost_per_user
            }
        }

# Глобальный экземпляр
realistic_calculator = RealisticMicroservicesCalculator()

def calculate_realistic_microservices(users: int = 100, accounts_per_user: int = 300) -> Dict:
    """Быстрый расчет реалистичной стоимости микросервисов"""
    return realistic_calculator.calculate_realistic_cost(users, accounts_per_user) 
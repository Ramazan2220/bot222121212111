"""
💰 КАЛЬКУЛЯТОР СТОИМОСТИ
Точный расчет затрат на сервер с учетом оптимизаций
"""

import math
from typing import Dict, NamedTuple
from dataclasses import dataclass

@dataclass
class ServerSpecs:
    """Характеристики сервера"""
    ram_gb: int
    cpu_cores: int
    disk_gb: int
    traffic_tb: float
    monthly_cost: float

@dataclass
class OptimizationImpact:
    """Влияние оптимизаций на ресурсы"""
    client_pool_memory_reduction: float = 0.65  # 65% экономии памяти
    activity_optimizer_cpu_reduction: float = 0.40  # 40% экономии CPU
    structured_logging_disk_reduction: float = 0.70  # 70% экономии диска
    sleep_mode_memory_reduction: float = 0.80  # 80% экономии на спящих клиентах

class CostCalculator:
    """
    💰 ТОЧНЫЙ КАЛЬКУЛЯТОР СТОИМОСТИ
    Учитывает реальную активность и все оптимизации
    """
    
    def __init__(self):
        # Базовые характеристики нагрузки на аккаунт
        self.base_memory_per_client = 4.0  # MB
        self.sleeping_memory_per_client = 0.8  # MB (спящий режим)
        self.requests_per_active_account = 10  # запросов/мин
        self.cpu_per_request = 0.002  # секунд CPU на запрос
        
        # Оптимизации
        self.optimizations = OptimizationImpact()
        
    def calculate_realistic_load(self, users: int, accounts_per_user: int = 300) -> Dict:
        """
        Расчет РЕАЛЬНОЙ нагрузки с учетом:
        - Только 25% аккаунтов активны одновременно
        - Спящий режим неактивных клиентов
        - Ротация активности
        """
        total_accounts = users * accounts_per_user
        
        # Реальная активность
        active_accounts = int(total_accounts * 0.25)  # 25% активных
        sleeping_accounts = int(total_accounts * 0.50)  # 50% в спящем режиме
        inactive_accounts = total_accounts - active_accounts - sleeping_accounts  # 25% полностью неактивных
        
        # Память
        memory_active = active_accounts * self.base_memory_per_client
        memory_sleeping = sleeping_accounts * self.sleeping_memory_per_client
        memory_clients_total = memory_active + memory_sleeping
        
        # Применяем оптимизацию Client Pool
        memory_clients_optimized = memory_clients_total * (1 - self.optimizations.client_pool_memory_reduction)
        
        # Системная память
        memory_system = 2000  # 2GB система + БД + прочее
        memory_total = memory_clients_optimized + memory_system
        
        # CPU нагрузка
        requests_per_second = (active_accounts * self.requests_per_active_account) / 60
        cpu_load_base = requests_per_second * self.cpu_per_request
        
        # Применяем оптимизацию Activity Optimizer
        cpu_load_optimized = cpu_load_base * (1 - self.optimizations.activity_optimizer_cpu_reduction)
        
        # Системная нагрузка CPU
        cpu_system = 0.5  # Базовая системная нагрузка
        cpu_total = cpu_load_optimized + cpu_system
        
        # Трафик (только активные аккаунты)
        traffic_per_hour_gb = (active_accounts * self.requests_per_active_account * 60 * 6) / (1024**3)  # ~6KB на запрос
        traffic_per_day_gb = traffic_per_hour_gb * 12  # 12 часов активности
        traffic_per_month_gb = traffic_per_day_gb * 30
        
        # Диск (с учетом оптимизации логирования)
        disk_logs_base = users * 50  # 50GB логов на пользователя в месяц
        disk_logs_optimized = disk_logs_base * (1 - self.optimizations.structured_logging_disk_reduction)
        disk_media = users * 20  # 20GB медиа на пользователя
        disk_total = disk_logs_optimized + disk_media + 30  # +30GB система
        
        return {
            'users': users,
            'total_accounts': total_accounts,
            'active_accounts': active_accounts,
            'sleeping_accounts': sleeping_accounts,
            'inactive_accounts': inactive_accounts,
            'memory': {
                'clients_mb': round(memory_clients_optimized, 1),
                'system_mb': memory_system,
                'total_mb': round(memory_total, 1),
                'total_gb': round(memory_total / 1024, 2)
            },
            'cpu': {
                'load_cores': round(cpu_total, 2),
                'utilization_percent': round((cpu_total / 8) * 100, 1)  # Из 8 ядер
            },
            'traffic': {
                'gb_per_month': round(traffic_per_month_gb, 1),
                'tb_per_month': round(traffic_per_month_gb / 1024, 3)
            },
            'disk': {
                'total_gb': round(disk_total, 1)
            }
        }
    
    def calculate_server_cost(self, load_data: Dict) -> Dict:
        """Расчет стоимости сервера под нагрузку (с учетом реальной нагрузки)"""
        memory_gb = load_data['memory']['total_gb']
        cpu_cores = max(2, math.ceil(load_data['cpu']['load_cores'] * 1.5))  # 50% запас
        disk_gb = load_data['disk']['total_gb']
        traffic_tb = load_data['traffic']['tb_per_month']
        
        # Базовые цены (за ресурс)
        ram_price_per_gb = 3.0    # $3/GB RAM
        cpu_price_per_core = 8.0  # $8/ядро CPU  
        disk_price_per_gb = 0.15  # $0.15/GB SSD
        traffic_price_per_tb = 5.0 # $5/TB трафика
        base_cost = 20  # $20 базовая стоимость
        
        # Расчет стоимости по ресурсам
        def calculate_cost(ram_mult, cpu_mult, disk_mult, traffic_mult):
            actual_ram = max(4, math.ceil(memory_gb * ram_mult))
            actual_cpu = max(2, cpu_cores + cpu_mult)
            actual_disk = max(50, math.ceil(disk_gb * disk_mult))
            actual_traffic = max(1, math.ceil(traffic_tb * traffic_mult))
            
            cost = (base_cost + 
                   actual_ram * ram_price_per_gb +
                   actual_cpu * cpu_price_per_core +
                   actual_disk * disk_price_per_gb +
                   actual_traffic * traffic_price_per_tb)
            
            return ServerSpecs(
                ram_gb=actual_ram,
                cpu_cores=actual_cpu,
                disk_gb=actual_disk,
                traffic_tb=actual_traffic,
                monthly_cost=round(cost, 0)
            )
        
        # Варианты серверов (основаны на реальной нагрузке)
        servers = {
            'budget': calculate_cost(1.2, 1, 1.3, 1.5),      # Минимальные запасы
            'optimal': calculate_cost(1.5, 2, 1.5, 2),       # Разумные запасы  
            'premium': calculate_cost(2.0, 4, 2.0, 3)        # Большие запасы
        }
        
        return servers
    
    def calculate_cost_per_user(self, users: int, accounts_per_user: int = 300) -> Dict:
        """Расчет стоимости на одного пользователя"""
        load_data = self.calculate_realistic_load(users, accounts_per_user)
        servers = self.calculate_server_cost(load_data)
        
        results = {}
        for server_type, specs in servers.items():
            cost_per_user = specs.monthly_cost / users
            profit_margin = ((200 - cost_per_user) / 200) * 100  # При цене $200/месяц
            
            results[server_type] = {
                'server_cost': specs.monthly_cost,
                'cost_per_user': round(cost_per_user, 2),
                'profit_margin_percent': round(profit_margin, 1),
                'specs': specs
            }
            
        return {
            'load_analysis': load_data,
            'server_options': results
        }
    
    def compare_with_without_optimizations(self, users: int) -> Dict:
        """Сравнение с оптимизациями и без"""
        # С оптимизациями
        with_opt = self.calculate_cost_per_user(users)
        
        # Без оптимизаций (временно отключаем)
        original_opts = self.optimizations
        self.optimizations = OptimizationImpact(0, 0, 0, 0)  # Без оптимизаций
        
        without_opt = self.calculate_cost_per_user(users)
        
        # Восстанавливаем оптимизации
        self.optimizations = original_opts
        
        return {
            'with_optimizations': with_opt,
            'without_optimizations': without_opt,
            'savings': {
                'memory_mb': without_opt['load_analysis']['memory']['total_mb'] - with_opt['load_analysis']['memory']['total_mb'],
                'cpu_cores': without_opt['load_analysis']['cpu']['load_cores'] - with_opt['load_analysis']['cpu']['load_cores'],
                'cost_savings_optimal': without_opt['server_options']['optimal']['server_cost'] - with_opt['server_options']['optimal']['server_cost']
            }
        }

# Глобальный калькулятор
cost_calculator = CostCalculator()

def calculate_user_cost(users: int = 10, accounts_per_user: int = 300) -> Dict:
    """Быстрый расчет стоимости пользователя"""
    return cost_calculator.calculate_cost_per_user(users, accounts_per_user)

def get_optimization_savings(users: int = 10) -> Dict:
    """Экономия от оптимизаций"""
    return cost_calculator.compare_with_without_optimizations(users) 
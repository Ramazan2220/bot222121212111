"""
🏭 PRODUCTION-READY КАЛЬКУЛЯТОР
Учитывает реальные паттерны использования, пики нагрузки и отклонения
"""

import math
from typing import Dict, List
from dataclasses import dataclass
from datetime import datetime, timedelta

@dataclass
class LoadPattern:
    """Паттерн нагрузки в зависимости от времени"""
    peak_hours: List[int]  # Часы пиковой нагрузки
    normal_hours: List[int]  # Часы нормальной нагрузки
    low_hours: List[int]  # Часы минимальной нагрузки
    peak_multiplier: float  # Множитель для пиков
    normal_multiplier: float  # Множитель для нормальной нагрузки
    low_multiplier: float  # Множитель для минимальной нагрузки

@dataclass
class SafetyMargins:
    """Запасы безопасности для production"""
    concurrent_users_multiplier: float = 3.0  # Все пользователи могут быть активны одновременно
    memory_safety_margin: float = 1.8  # 80% запас памяти
    cpu_safety_margin: float = 2.0  # 100% запас CPU
    disk_safety_margin: float = 1.5  # 50% запас диска
    network_safety_margin: float = 2.5  # 150% запас на сеть
    system_overhead: float = 1.3  # 30% на системные процессы

@dataclass
class AdditionalLoads:
    """Дополнительные нагрузки которые я не учитывал"""
    monitoring_cpu_percent: float = 5.0  # 5% CPU на мониторинг
    logging_disk_gb_per_user: float = 10.0  # 10GB логов на пользователя в месяц
    backup_disk_multiplier: float = 1.2  # 20% диска на бэкапы
    captcha_solving_ram_gb: float = 2.0  # 2GB RAM на решение капч
    retry_mechanisms_cpu_multiplier: float = 1.4  # 40% больше CPU на retry
    anti_detection_ram_gb: float = 3.0  # 3GB RAM на анти-детект

class ProductionReadyCalculator:
    """
    🏭 PRODUCTION-READY КАЛЬКУЛЯТОР
    Учитывает все реальные факторы production системы
    """
    
    def __init__(self):
        # Реальный паттерн активности (8:00-03:00 следующего дня)
        self.load_pattern = LoadPattern(
            peak_hours=[8, 9, 10, 18, 19, 20, 21, 22],  # Утро и вечер
            normal_hours=[11, 12, 13, 14, 15, 16, 17, 23, 0, 1, 2],  # День и поздний вечер
            low_hours=[3, 4, 5, 6, 7],  # Ночь
            peak_multiplier=2.5,  # В пики нагрузка в 2.5 раза выше
            normal_multiplier=1.0,  # Базовая нагрузка
            low_multiplier=0.3  # Ночью минимальная активность
        )
        
        # Запасы безопасности для production
        self.safety_margins = SafetyMargins()
        
        # Дополнительные нагрузки
        self.additional_loads = AdditionalLoads()
        
    def calculate_realistic_peak_requirements(self, users: int, accounts_per_user: int = 300) -> Dict:
        """
        Расчет требований с учетом ВСЕХ факторов:
        - Временные пики (8:00-03:00)
        - Непредсказуемость (все пользователи одновременно)
        - Дополнительные нагрузки (мониторинг, логи, и т.д.)
        - Запасы безопасности
        """
        total_accounts = users * accounts_per_user
        
        # === 1. УЧИТЫВАЕМ НЕПРЕДСКАЗУЕМОСТЬ ПОЛЬЗОВАТЕЛЕЙ ===
        # В худшем случае все пользователи активны одновременно
        worst_case_active_users = users * self.safety_margins.concurrent_users_multiplier
        worst_case_active_accounts = min(
            int(total_accounts * 0.5),  # Максимум 50% аккаунтов одновременно (физический лимит)
            int(worst_case_active_users * accounts_per_user * 0.3)  # 30% аккаунтов пользователя
        )
        
        # === 2. УЧИТЫВАЕМ ВРЕМЕННЫЕ ПИКИ ===
        # Накладываем пиковый множитель на активность
        peak_active_accounts = int(worst_case_active_accounts * self.load_pattern.peak_multiplier)
        peak_requests_per_hour = peak_active_accounts * 20  # 20 запросов/час на аккаунт в пике
        
        # === 3. БАЗОВЫЕ ТРЕБОВАНИЯ С ПУЛАМИ (из предыдущих расчетов) ===
        base_requirements = self._calculate_base_with_pools(users, peak_active_accounts)
        
        # === 4. ДОБАВЛЯЕМ ДОПОЛНИТЕЛЬНЫЕ НАГРУЗКИ ===
        enhanced_requirements = self._add_additional_loads(base_requirements, users, total_accounts)
        
        # === 5. ПРИМЕНЯЕМ ЗАПАСЫ БЕЗОПАСНОСТИ ===
        production_requirements = self._apply_safety_margins(enhanced_requirements)
        
        # === 6. ДОБАВЛЯЕМ МЕТАДАННЫЕ ===
        production_requirements['load_analysis'] = {
            'total_accounts': total_accounts,
            'worst_case_active_accounts': worst_case_active_accounts,
            'peak_active_accounts': peak_active_accounts,
            'peak_requests_per_hour': peak_requests_per_hour,
            'active_hours_per_day': 19,  # 8:00-03:00
            'peak_hours_per_day': len(self.load_pattern.peak_hours),
            'concurrent_users_assumption': f'{self.safety_margins.concurrent_users_multiplier}x worst case'
        }
        
        return production_requirements
    
    def _calculate_base_with_pools(self, users: int, active_accounts: int) -> Dict:
        """Базовые требования с пулами (из предыдущих расчетов)"""
        # Используем оптимизации из предыдущего калькулятора
        client_pool_memory_reduction = 0.65
        sleeping_memory_multiplier = 0.2
        
        return {
            'database_proxy': {
                'ram_gb': 4 + (users * 0.1),
                'cpu_cores': 2 + (users // 50),
                'disk_gb': 20 + (users * 2),
            },
            'content_publishing': {
                'ram_gb': 2 + (active_accounts * 0.004) * (1 - client_pool_memory_reduction),
                'cpu_cores': 1 + (active_accounts // 3000),
                'disk_gb': 50 + (users * 5),
            },
            'scheduled_publishing': {
                'ram_gb': 1 + (active_accounts * 0.001),
                'cpu_cores': 1,
                'disk_gb': 10 + (users * 1),
            },
            'account_warmup': {
                'ram_gb': 2 + (int(active_accounts * 0.2) * 0.003),
                'cpu_cores': 1 + (int(active_accounts * 0.2) // 1000),
                'disk_gb': 10 + (users * 2),
            },
            'account_processing': {
                'ram_gb': 2 + (users * 0.05),
                'cpu_cores': 1 + (users // 100),
                'disk_gb': 20 + (users * 3),
            },
            'profile_design': {
                'ram_gb': 3 + (users * 0.02),
                'cpu_cores': 2 + (users // 200),
                'disk_gb': 30 + (users * 10),
            }
        }
    
    def _add_additional_loads(self, requirements: Dict, users: int, total_accounts: int) -> Dict:
        """Добавляем дополнительные нагрузки которые я забыл"""
        enhanced = {}
        
        for service_name, service_req in requirements.items():
            enhanced[service_name] = {
                'ram_gb': service_req['ram_gb'],
                'cpu_cores': service_req['cpu_cores'],
                'disk_gb': service_req['disk_gb']
            }
        
        # Добавляем дополнительные нагрузки
        additional_loads = {
            'monitoring_logging': {
                'ram_gb': 4 + (users * 0.1),  # Prometheus, Grafana, ELK stack
                'cpu_cores': 2 + int(users * self.additional_loads.monitoring_cpu_percent / 100),
                'disk_gb': users * self.additional_loads.logging_disk_gb_per_user,
                'description': 'Мониторинг, логирование, алерты'
            },
            'security_captcha': {
                'ram_gb': self.additional_loads.captcha_solving_ram_gb + self.additional_loads.anti_detection_ram_gb,
                'cpu_cores': 1 + (total_accounts // 5000),  # CPU на решение капч
                'disk_gb': 20 + (users * 2),  # Капчи и анти-детект данные
                'description': 'Решение капч, анти-детект, безопасность'
            },
            'backup_maintenance': {
                'ram_gb': 2,  # Минимальная RAM для backup процессов
                'cpu_cores': 1,  # CPU для backup
                'disk_gb': sum(req['disk_gb'] for req in requirements.values()) * (self.additional_loads.backup_disk_multiplier - 1),
                'description': 'Бэкапы, обслуживание, обновления'
            }
        }
        
        # Добавляем новые сервисы
        enhanced.update(additional_loads)
        
        # Увеличиваем CPU на retry механизмы
        for service_name in requirements.keys():
            enhanced[service_name]['cpu_cores'] = math.ceil(
                enhanced[service_name]['cpu_cores'] * self.additional_loads.retry_mechanisms_cpu_multiplier
            )
        
        return enhanced
    
    def _apply_safety_margins(self, requirements: Dict) -> Dict:
        """Применяем запасы безопасности"""
        production_ready = {}
        
        for service_name, service_req in requirements.items():
            production_ready[service_name] = {
                'ram_gb': math.ceil(service_req['ram_gb'] * self.safety_margins.memory_safety_margin),
                'cpu_cores': math.ceil(service_req['cpu_cores'] * self.safety_margins.cpu_safety_margin),
                'disk_gb': math.ceil(service_req['disk_gb'] * self.safety_margins.disk_safety_margin),
            }
            
            if 'description' in service_req:
                production_ready[service_name]['description'] = service_req['description']
        
        # Добавляем системные накладные расходы
        for service_name in requirements.keys():
            if service_name in ['database_proxy', 'content_publishing']:  # Основные сервисы
                production_ready[service_name]['ram_gb'] = math.ceil(
                    production_ready[service_name]['ram_gb'] * self.safety_margins.system_overhead
                )
        
        return production_ready
    
    def calculate_production_servers(self, requirements: Dict) -> Dict:
        """Расчет серверов для production с учетом отказоустойчивости"""
        from utils.realistic_microservices_calculator import REAL_SERVERS
        
        # Группируем сервисы по критичности
        critical_services = ['database_proxy', 'content_publishing']  # Нужно дублирование
        standard_services = [k for k in requirements.keys() 
                           if k not in critical_services and 'description' in requirements[k]]
        
        servers_layout = []
        total_cost = 0
        
        # === КРИТИЧНЫЕ СЕРВИСЫ (с дублированием) ===
        for service_name in critical_services:
            if service_name in requirements:
                service_req = requirements[service_name]
                server = self._find_best_server(service_req)
                
                # Основной сервер
                servers_layout.append({
                    'role': f'{service_name}_primary',
                    'server_type': server,
                    'services': [service_name],
                    'redundancy': 'Primary',
                    'monthly_cost': server.monthly_cost
                })
                
                # Резервный сервер (для отказоустойчивости)
                servers_layout.append({
                    'role': f'{service_name}_backup',
                    'server_type': server,
                    'services': [service_name],
                    'redundancy': 'Backup',
                    'monthly_cost': server.monthly_cost
                })
                
                total_cost += server.monthly_cost * 2  # Основной + резервный
        
        # === СТАНДАРТНЫЕ СЕРВИСЫ (можно группировать) ===
        remaining_services = []
        for service_name in standard_services:
            if service_name in requirements:
                remaining_services.append((service_name, requirements[service_name]))
        
        # Сортируем по размеру (сначала большие)
        remaining_services.sort(key=lambda x: x[1]['ram_gb'] * x[1]['cpu_cores'], reverse=True)
        
        for service_name, service_req in remaining_services:
            # Пытаемся разместить на существующих серверах
            placed = False
            for server_config in servers_layout:
                if (server_config['redundancy'] != 'Backup' and 
                    self._can_fit_service_on_server(server_config, service_req)):
                    server_config['services'].append(service_name)
                    placed = True
                    break
            
            if not placed:
                # Создаем новый сервер
                server = self._find_best_server(service_req)
                servers_layout.append({
                    'role': f'{service_name}_dedicated',
                    'server_type': server,
                    'services': [service_name],
                    'redundancy': 'Single',
                    'monthly_cost': server.monthly_cost
                })
                total_cost += server.monthly_cost
        
        return {
            'servers': servers_layout,
            'total_servers': len(servers_layout),
            'total_cost': total_cost,
            'redundancy_info': {
                'critical_services_duplicated': len(critical_services) * 2,
                'standard_services': len(standard_services),
                'estimated_uptime': '99.9%+'
            }
        }
    
    def _find_best_server(self, service_req: Dict):
        """Находит подходящий сервер для сервиса"""
        from utils.realistic_microservices_calculator import REAL_SERVERS
        
        required_ram = service_req['ram_gb'] * 1.1  # 10% запас
        required_cpu = service_req['cpu_cores']
        required_disk = service_req['disk_gb'] * 1.1
        
        for server in REAL_SERVERS.values():
            if (server.ram_gb >= required_ram and 
                server.cpu_cores >= required_cpu and 
                server.disk_gb >= required_disk):
                return server
        
        return REAL_SERVERS['xxlarge']  # Самый большой если не подходит
    
    def _can_fit_service_on_server(self, server_config: Dict, service_req: Dict) -> bool:
        """Проверяет поместится ли сервис на сервер"""
        # Упрощенная проверка - предполагаем что сервисы небольшие
        return len(server_config['services']) < 2  # Максимум 2 сервиса на сервер
    
    def calculate_full_production_cost(self, users: int, accounts_per_user: int = 300) -> Dict:
        """Полный расчет production стоимости"""
        requirements = self.calculate_realistic_peak_requirements(users, accounts_per_user)
        load_analysis = requirements.pop('load_analysis')
        
        server_layout = self.calculate_production_servers(requirements)
        
        cost_per_user = server_layout['total_cost'] / users
        
        # Сравнение с предыдущими расчетами
        from utils.realistic_microservices_calculator import calculate_realistic_microservices
        optimistic_result = calculate_realistic_microservices(users, accounts_per_user)
        
        return {
            'production_ready': {
                'requirements': requirements,
                'server_layout': server_layout,
                'total_cost': server_layout['total_cost'],
                'cost_per_user': round(cost_per_user, 2),
                'load_analysis': load_analysis
            },
            'optimistic_microservices': {
                'cost_per_user': optimistic_result['microservices']['cost_per_user']
            },
            'reality_check': {
                'cost_increase_ratio': round(cost_per_user / optimistic_result['microservices']['cost_per_user'], 2),
                'additional_cost': round(cost_per_user - optimistic_result['microservices']['cost_per_user'], 2),
                'factors_included': [
                    'Временные пики (8:00-03:00)',
                    'Непредсказуемость пользователей (3x)',
                    'Дополнительные нагрузки (мониторинг, капчи, бэкапы)',
                    'Запасы безопасности (80% память, 100% CPU)',
                    'Отказоустойчивость (дублирование критичных сервисов)',
                    'Системные накладные расходы'
                ]
            }
        }

# Глобальный калькулятор
production_calculator = ProductionReadyCalculator()

def calculate_production_ready_cost(users: int = 100, accounts_per_user: int = 300) -> Dict:
    """Расчет реальной production стоимости"""
    return production_calculator.calculate_full_production_cost(users, accounts_per_user) 
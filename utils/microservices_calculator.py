"""
🏗️ КАЛЬКУЛЯТОР МИКРОСЕРВИСНОЙ АРХИТЕКТУРЫ
Расчет стоимости распределенной системы по серверам
"""

import math
from typing import Dict, List
from dataclasses import dataclass
from datetime import datetime, timedelta

@dataclass
class ServerConfig:
    """Конфигурация сервера"""
    name: str
    description: str
    ram_gb: int
    cpu_cores: int
    disk_gb: int
    traffic_tb: float
    monthly_cost: float
    peak_load_multiplier: float = 1.0  # Мультипликатор для пиковых нагрузок

@dataclass
class UserActivity:
    """Паттерны активности пользователей"""
    accounts_upload_per_month: int = 1
    profile_redesign_per_month: int = 2
    quick_warmup_per_week: int = 20
    advanced_warmup_per_week: int = 7
    publications_per_account_per_day: int = 5

class MicroservicesCalculator:
    """
    🏗️ КАЛЬКУЛЯТОР МИКРОСЕРВИСНОЙ АРХИТЕКТУРЫ
    Распределение нагрузки по специализированным серверам
    """
    
    def __init__(self):
        # Базовые цены за ресурсы
        self.ram_price_per_gb = 2.5    # $2.5/GB RAM (дешевле для специализированных серверов)
        self.cpu_price_per_core = 6.0  # $6/ядро CPU
        self.disk_price_per_gb = 0.12  # $0.12/GB SSD  
        self.traffic_price_per_tb = 4.0 # $4/TB трафика
        self.base_cost_per_server = 15  # $15 базовая стоимость на сервер
        
        # Паттерны использования
        self.activity_patterns = UserActivity()
        
    def calculate_server_loads(self, users: int, accounts_per_user: int = 300) -> Dict:
        """Расчет нагрузки на каждый сервер"""
        total_accounts = users * accounts_per_user
        
        # === СЕРВЕР 1: БАЗА ДАННЫХ И ПРОКСИ ===
        db_load = self._calculate_database_server(users, total_accounts)
        
        # === СЕРВЕР 2: ПУБЛИКАЦИИ КОНТЕНТА ===
        publishing_load = self._calculate_publishing_server(users, total_accounts)
        
        # === СЕРВЕР 3: ЗАПЛАНИРОВАННЫЕ ПУБЛИКАЦИИ ===
        scheduled_load = self._calculate_scheduled_server(users, total_accounts)
        
        # === СЕРВЕР 4: ПРОГРЕВ ===
        warmup_load = self._calculate_warmup_server(users, total_accounts)
        
        # === СЕРВЕР 5: ОБРАБОТКА АККАУНТОВ (IMAP) ===
        account_processing_load = self._calculate_account_processing_server(users, total_accounts)
        
        # === СЕРВЕР 6: ОФОРМЛЕНИЕ АККАУНТОВ ===
        profile_design_load = self._calculate_profile_design_server(users, total_accounts)
        
        return {
            'database_proxy': db_load,
            'publishing': publishing_load,
            'scheduled': scheduled_load,
            'warmup': warmup_load,
            'account_processing': account_processing_load,
            'profile_design': profile_design_load,
            'summary': {
                'users': users,
                'total_accounts': total_accounts,
                'total_servers': 6
            }
        }
    
    def _calculate_database_server(self, users: int, total_accounts: int) -> Dict:
        """🗄️ Сервер базы данных и прокси"""
        # Нагрузка: постоянная работа, все запросы проходят через БД
        daily_requests = total_accounts * 5 * 5  # 5 публикаций × 5 запросов к БД на публикацию
        concurrent_connections = min(200, users * 5)  # 5 одновременных подключений на пользователя
        
        # Прокси управление: все аккаунты используют прокси
        proxy_management_load = total_accounts * 0.1  # Ротация прокси и проверки
        
        # Ресурсы
        ram_needed = 4 + (concurrent_connections * 0.02) + (total_accounts * 0.001)  # Базовая + соединения + данные
        cpu_needed = 2 + (daily_requests / 100000)  # 2 базовых + нагрузка от запросов
        disk_needed = 50 + (total_accounts * 0.5)  # Базовая БД + данные аккаунтов
        traffic_needed = (daily_requests * 2) / (1024**3) * 30  # 2KB на запрос × месяц
        
        return {
            'name': 'Database & Proxy Server',
            'description': 'Основная БД, прокси-менеджмент, кеширование',
            'workload': {
                'daily_requests': int(daily_requests),
                'concurrent_connections': concurrent_connections,
                'proxy_accounts': total_accounts
            },
            'resources': {
                'ram_gb': math.ceil(ram_needed),
                'cpu_cores': math.ceil(cpu_needed),
                'disk_gb': math.ceil(disk_needed),
                'traffic_tb': round(traffic_needed, 3)
            },
            'peak_multiplier': 1.3  # Умеренные пиковые нагрузки при массовых операциях
        }
    
    def _calculate_publishing_server(self, users: int, total_accounts: int) -> Dict:
        """📤 Сервер публикаций контента"""
        # Активность: 5 публикаций на аккаунт в день
        daily_publications = total_accounts * self.activity_patterns.publications_per_account_per_day
        peak_publications_per_hour = daily_publications / 16  # Распределено на 16 активных часов
        
        # Обработка медиа (изображения, видео)
        media_processing_load = daily_publications * 0.3  # 30% требуют обработки
        
        # Ресурсы
        ram_needed = 3 + (peak_publications_per_hour * 0.01)  # Обработка публикаций в памяти
        cpu_needed = 1.5 + (media_processing_load / 1000)  # CPU для обработки медиа
        disk_needed = 100 + (users * 50)  # Временное хранение медиа
        traffic_needed = (daily_publications * 5) / (1024**3) * 30  # 5MB на публикацию
        
        return {
            'name': 'Content Publishing Server',
            'description': 'Публикация постов, историй, reels в реальном времени',
            'workload': {
                'daily_publications': int(daily_publications),
                'peak_hour_publications': int(peak_publications_per_hour),
                'media_processing_jobs': int(media_processing_load)
            },
            'resources': {
                'ram_gb': math.ceil(ram_needed),
                'cpu_cores': math.ceil(cpu_needed),
                'disk_gb': math.ceil(disk_needed),
                'traffic_tb': round(traffic_needed, 3)
            },
            'peak_multiplier': 1.8  # Умеренные пики при массовых публикациях
        }
    
    def _calculate_scheduled_server(self, users: int, total_accounts: int) -> Dict:
        """⏰ Сервер запланированных публикаций"""
        # Планировщик работает постоянно, но легче чем прямые публикации
        daily_scheduled_checks = total_accounts * 24  # Проверка расписания каждый час
        estimated_scheduled_posts = total_accounts * 2  # 2 отложенные публикации на аккаунт в день
        
        # Ресурсы
        ram_needed = 2 + (total_accounts * 0.002)  # Легкая нагрузка
        cpu_needed = 1 + (estimated_scheduled_posts / 5000)  # Планировщик не очень нагружает CPU
        disk_needed = 30 + (estimated_scheduled_posts * 0.1)  # Хранение расписаний
        traffic_needed = (estimated_scheduled_posts * 3) / (1024**3) * 30  # Меньше трафика
        
        return {
            'name': 'Scheduled Publishing Server',
            'description': 'Планировщик публикаций, отложенные посты',
            'workload': {
                'daily_schedule_checks': int(daily_scheduled_checks),
                'scheduled_publications': int(estimated_scheduled_posts)
            },
            'resources': {
                'ram_gb': math.ceil(ram_needed),
                'cpu_cores': math.ceil(cpu_needed),
                'disk_gb': math.ceil(disk_needed),
                'traffic_tb': round(traffic_needed, 3)
            },
            'peak_multiplier': 1.5  # Небольшие пики
        }
    
    def _calculate_warmup_server(self, users: int, total_accounts: int) -> Dict:
        """🔥 Сервер прогрева аккаунтов"""
        # Активность прогрева
        weekly_quick_warmups = users * self.activity_patterns.quick_warmup_per_week
        weekly_advanced_warmups = users * self.activity_patterns.advanced_warmup_per_week
        
        # Предполагаем что прогрев затрагивает 30% аккаунтов одновременно
        active_warmup_accounts = int(total_accounts * 0.3)
        warmup_requests_per_hour = active_warmup_accounts * 15  # 15 запросов/час на аккаунт в прогреве
        
        # Ресурсы
        ram_needed = 2 + (active_warmup_accounts * 0.003)  # Управление сессиями прогрева
        cpu_needed = 1 + (warmup_requests_per_hour / 2000)  # Обработка запросов прогрева
        disk_needed = 40 + (users * 10)  # Логи прогрева и настройки
        traffic_needed = (warmup_requests_per_hour * 24 * 2) / (1024**3) * 30  # 2KB на запрос
        
        return {
            'name': 'Account Warmup Server',
            'description': 'Быстрый и продвинутый прогрев, прогрев по интересам',
            'workload': {
                'weekly_quick_warmups': weekly_quick_warmups,
                'weekly_advanced_warmups': weekly_advanced_warmups,
                'active_warmup_accounts': active_warmup_accounts,
                'warmup_requests_per_hour': warmup_requests_per_hour
            },
            'resources': {
                'ram_gb': math.ceil(ram_needed),
                'cpu_cores': math.ceil(cpu_needed),
                'disk_gb': math.ceil(disk_needed),
                'traffic_tb': round(traffic_needed, 3)
            },
            'peak_multiplier': 1.6  # Умеренные пики при запуске прогрева
        }
    
    def _calculate_account_processing_server(self, users: int, total_accounts: int) -> Dict:
        """📧 Сервер обработки аккаунтов (IMAP, вход)"""
        # Активность: загрузка аккаунтов раз в месяц
        monthly_account_uploads = users * self.activity_patterns.accounts_upload_per_month
        accounts_per_upload = total_accounts / monthly_account_uploads  # В среднем аккаунтов за загрузку
        
        # IMAP проверки и обработка
        daily_imap_checks = total_accounts * 2  # 2 проверки IMAP в день на аккаунт
        email_processing_load = total_accounts * 0.1  # 10% аккаунтов требуют обработки email
        
        # Ресурсы
        ram_needed = 3 + (email_processing_load * 0.005)  # IMAP соединения в памяти
        cpu_needed = 2 + (daily_imap_checks / 10000)  # Обработка email занимает CPU
        disk_needed = 60 + (total_accounts * 0.1)  # Временное хранение данных аккаунтов
        traffic_needed = (daily_imap_checks * 1) / (1024**3) * 30  # 1KB на IMAP проверку
        
        return {
            'name': 'Account Processing Server',
            'description': 'IMAP обработка, вход в аккаунты, загрузка аккаунтов',
            'workload': {
                'monthly_uploads': monthly_account_uploads,
                'daily_imap_checks': int(daily_imap_checks),
                'email_processing_accounts': int(email_processing_load)
            },
            'resources': {
                'ram_gb': math.ceil(ram_needed),
                'cpu_cores': math.ceil(cpu_needed),
                'disk_gb': math.ceil(disk_needed),
                'traffic_tb': round(traffic_needed, 3)
            },
            'peak_multiplier': 2.0  # Умеренные пики при массовой загрузке аккаунтов
        }
    
    def _calculate_profile_design_server(self, users: int, total_accounts: int) -> Dict:
        """🎨 Сервер оформления аккаунтов"""
        # Активность: оформление 2 раза в месяц
        monthly_profile_updates = users * self.activity_patterns.profile_redesign_per_month
        accounts_per_redesign = total_accounts  # Обновляются все аккаунты
        
        # Генерация контента для профилей (исправленная логика)
        # Не все аккаунты обновляются одновременно - распределяем на месяц
        daily_profile_jobs = (monthly_profile_updates * accounts_per_redesign) / 30
        
        # Ресурсы (более реалистичные расчеты)
        ram_needed = 4 + (daily_profile_jobs / 10000)  # Генерация контента требует памяти
        cpu_needed = 2 + (daily_profile_jobs / 5000)   # CPU нагрузка для генерации
        disk_needed = 80 + (users * 30)  # Шаблоны, медиа для профилей
        traffic_needed = (daily_profile_jobs * 30 * 0.5) / (1024**3)  # 0.5MB на профиль
        
        return {
            'name': 'Profile Design Server',
            'description': 'Оформление профилей, генерация аватаров, био',
            'workload': {
                'monthly_profile_updates': monthly_profile_updates,
                'daily_profile_jobs': int(daily_profile_jobs)
            },
            'resources': {
                'ram_gb': math.ceil(ram_needed),
                'cpu_cores': math.ceil(cpu_needed),
                'disk_gb': math.ceil(disk_needed),
                'traffic_tb': round(traffic_needed, 3)
            },
            'peak_multiplier': 1.8  # Умеренные пики при редизайне
        }
    
    def calculate_server_costs(self, server_loads: Dict) -> Dict:
        """Расчет стоимости каждого сервера"""
        servers_cost = {}
        total_cost = 0
        
        for server_key, server_data in server_loads.items():
            if server_key == 'summary':
                continue
                
            resources = server_data['resources']
            peak_mult = server_data['peak_multiplier']
            
            # Применяем пиковый мультипликатор к ресурсам
            peak_ram = math.ceil(resources['ram_gb'] * peak_mult)
            peak_cpu = math.ceil(resources['cpu_cores'] * peak_mult)
            peak_disk = math.ceil(resources['disk_gb'] * 1.2)  # Диск меньше зависит от пиков
            peak_traffic = resources['traffic_tb'] * 1.5  # Трафик увеличивается при пиках
            
            # Расчет стоимости
            cost = (self.base_cost_per_server +
                   peak_ram * self.ram_price_per_gb +
                   peak_cpu * self.cpu_price_per_core +
                   peak_disk * self.disk_price_per_gb +
                   peak_traffic * self.traffic_price_per_tb)
            
            servers_cost[server_key] = {
                'name': server_data['name'],
                'description': server_data['description'],
                'workload': server_data['workload'],
                'base_resources': resources,
                'peak_resources': {
                    'ram_gb': peak_ram,
                    'cpu_cores': peak_cpu,
                    'disk_gb': peak_disk,
                    'traffic_tb': round(peak_traffic, 3)
                },
                'monthly_cost': round(cost, 2),
                'peak_multiplier': peak_mult
            }
            
            total_cost += cost
        
        servers_cost['total_infrastructure'] = {
            'total_monthly_cost': round(total_cost, 2),
            'servers_count': len([k for k in server_loads.keys() if k != 'summary']),
            'users': server_loads['summary']['users'],
            'cost_per_user': round(total_cost / server_loads['summary']['users'], 2)
        }
        
        return servers_cost
    
    def calculate_microservices_efficiency(self, users: int, accounts_per_user: int = 300) -> Dict:
        """Полный расчет эффективности микросервисной архитектуры"""
        server_loads = self.calculate_server_loads(users, accounts_per_user)
        server_costs = self.calculate_server_costs(server_loads)
        
        # Сравнение с монолитной архитектурой
        from utils.cost_calculator import cost_calculator
        monolith_cost = cost_calculator.calculate_cost_per_user(users, accounts_per_user)
        
        microservices_cost_per_user = server_costs['total_infrastructure']['cost_per_user']
        monolith_cost_per_user = monolith_cost['server_options']['optimal']['cost_per_user']
        
        return {
            'microservices': {
                'servers': server_costs,
                'cost_per_user': microservices_cost_per_user
            },
            'monolith': {
                'cost_per_user': monolith_cost_per_user
            },
            'comparison': {
                'microservices_vs_monolith_ratio': round(microservices_cost_per_user / monolith_cost_per_user, 2),
                'cost_difference': round(microservices_cost_per_user - monolith_cost_per_user, 2),
                'is_microservices_cheaper': microservices_cost_per_user < monolith_cost_per_user,
                'scalability_advantage': 'Микросервисы лучше масштабируются и более отказоустойчивы',
                'maintenance_complexity': 'Микросервисы требуют больше DevOps навыков'
            }
        }

# Глобальный калькулятор микросервисов
microservices_calculator = MicroservicesCalculator()

def calculate_microservices_cost(users: int = 100, accounts_per_user: int = 300) -> Dict:
    """Быстрый расчет стоимости микросервисной архитектуры"""
    return microservices_calculator.calculate_microservices_efficiency(users, accounts_per_user) 
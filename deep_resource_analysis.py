#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
🔬 ГЛУБОКИЙ АНАЛИЗ ПОТРЕБЛЕНИЯ РЕСУРСОВ
Реальный расчет CPU, RAM и системных ресурсов для Instagram бота
"""

import sys
import psutil
import threading
import time
from dataclasses import dataclass
from typing import Dict, List, Optional
import json

@dataclass
class ResourceConsumption:
    """Потребление ресурсов для одной операции"""
    cpu_seconds: float  # Секунды активного CPU
    ram_mb: float      # МБ оперативной памяти
    connections: int   # Количество подключений к БД
    disk_io_mb: float  # МБ дисковых операций

class InstagramBotResourceAnalyzer:
    """Анализатор реальных ресурсов Instagram бота"""
    
    def __init__(self):
        self.connection_pool_size = 100  # Connection pool
        self.connection_overhead_mb = 2  # МБ на подключение
        
    def analyze_single_account_operations(self) -> Dict[str, ResourceConsumption]:
        """Анализ операций для одного аккаунта"""
        
        operations = {}
        
        # 1. ПУБЛИКАЦИЯ ПОСТА
        operations['post_publication'] = ResourceConsumption(
            cpu_seconds=self._calculate_post_cpu(),
            ram_mb=self._calculate_post_ram(),
            connections=2,  # DB + Redis
            disk_io_mb=self._calculate_post_disk_io()
        )
        
        # 2. ПРОГРЕВ АККАУНТА (лайк/подписка)
        operations['warmup_action'] = ResourceConsumption(
            cpu_seconds=self._calculate_warmup_cpu(),
            ram_mb=self._calculate_warmup_ram(),
            connections=1,  # Только DB
            disk_io_mb=self._calculate_warmup_disk_io()
        )
        
        # 3. IMAP ПРОВЕРКА
        operations['imap_check'] = ResourceConsumption(
            cpu_seconds=self._calculate_imap_cpu(),
            ram_mb=self._calculate_imap_ram(),
            connections=1,  # DB для обновления
            disk_io_mb=self._calculate_imap_disk_io()
        )
        
        # 4. HEALTH CHECK
        operations['health_check'] = ResourceConsumption(
            cpu_seconds=self._calculate_health_cpu(),
            ram_mb=self._calculate_health_ram(),
            connections=1,  # DB read/write
            disk_io_mb=self._calculate_health_disk_io()
        )
        
        # 5. ОБНОВЛЕНИЕ ПРОФИЛЯ
        operations['profile_update'] = ResourceConsumption(
            cpu_seconds=self._calculate_profile_cpu(),
            ram_mb=self._calculate_profile_ram(),
            connections=1,  # DB update
            disk_io_mb=self._calculate_profile_disk_io()
        )
        
        return operations
        
    def _calculate_post_cpu(self) -> float:
        """CPU для публикации поста"""
        # Детальный расчет:
        return (
            0.1 +   # Создание task в БД
            0.2 +   # Загрузка медиа с диска
            1.5 +   # Обработка изображения/видео (PIL/ffmpeg)
            0.5 +   # Подготовка Instagram API запроса
            0.8 +   # Ожидание ответа Instagram (CPU idle, но процесс занят)
            0.1 +   # Обновление статуса в БД
            0.2     # Логирование и cleanup
        )  # = 3.4 секунды АКТИВНОГО CPU
        
    def _calculate_post_ram(self) -> float:
        """RAM для публикации поста"""
        return (
            15.0 +  # Instagram session object
            25.0 +  # Медиа файл в памяти (сжатый)
            5.0 +   # HTTP запрос/ответ
            3.0 +   # SQLAlchemy objects
            2.0     # Временные переменные
        )  # = 50 МБ на публикацию
        
    def _calculate_post_disk_io(self) -> float:
        """Дисковые операции для публикации"""
        return (
            5.0 +   # Чтение медиа файла
            0.1 +   # Запись в БД (task)
            0.1 +   # Обновление в БД (status)
            0.5     # Логи
        )  # = 5.7 МБ дисковых операций
        
    def _calculate_warmup_cpu(self) -> float:
        """CPU для одного действия прогрева"""
        return (
            0.05 +  # Получение task из очереди
            0.3 +   # Instagram API запрос (лайк/подписка)
            0.5 +   # Ожидание ответа (процесс занят)
            0.05 +  # Обновление счетчиков в БД
            0.1     # Anti-ban задержки
        )  # = 1.0 секунда CPU
        
    def _calculate_warmup_ram(self) -> float:
        """RAM для прогрева"""
        return (
            15.0 +  # Instagram session
            2.0 +   # API request data
            1.0     # SQLAlchemy objects
        )  # = 18 МБ
        
    def _calculate_warmup_disk_io(self) -> float:
        """Диск для прогрева"""
        return (
            0.05 +  # UPDATE в БД
            0.1     # Логи
        )  # = 0.15 МБ
        
    def _calculate_imap_cpu(self) -> float:
        """CPU для IMAP проверки"""
        return (
            0.5 +   # Подключение к IMAP серверу
            0.8 +   # Загрузка и парсинг писем
            0.2 +   # Извлечение кодов/данных
            0.1     # Обновление БД
        )  # = 1.6 секунды
        
    def _calculate_imap_ram(self) -> float:
        """RAM для IMAP"""
        return (
            8.0 +   # IMAP connection
            5.0 +   # Email messages в памяти
            2.0     # Парсинг и обработка
        )  # = 15 МБ
        
    def _calculate_imap_disk_io(self) -> float:
        """Диск для IMAP"""
        return (
            0.05 +  # UPDATE БД
            0.1     # Логи
        )  # = 0.15 МБ
        
    def _calculate_health_cpu(self) -> float:
        """CPU для health check"""
        return (
            0.1 +   # SELECT из БД
            0.4 +   # Instagram API статус запрос
            0.1 +   # Анализ ответа
            0.05    # UPDATE БД
        )  # = 0.65 секунды
        
    def _calculate_health_ram(self) -> float:
        """RAM для health check"""
        return (
            15.0 +  # Instagram session
            3.0 +   # Account data
            1.0     # API response
        )  # = 19 МБ
        
    def _calculate_health_disk_io(self) -> float:
        """Диск для health check"""
        return (
            0.02 +  # SELECT БД
            0.05 +  # UPDATE БД
            0.05    # Логи
        )  # = 0.12 МБ
        
    def _calculate_profile_cpu(self) -> float:
        """CPU для обновления профиля"""
        return (
            0.2 +   # Подготовка данных
            1.0 +   # Instagram API запросы (био, аватар, etc)
            0.5 +   # Обработка аватара
            0.1     # UPDATE БД
        )  # = 1.8 секунды
        
    def _calculate_profile_ram(self) -> float:
        """RAM для профиля"""
        return (
            15.0 +  # Instagram session
            10.0 +  # Avatar image в памяти
            3.0     # Profile data
        )  # = 28 МБ
        
    def _calculate_profile_disk_io(self) -> float:
        """Диск для профиля"""
        return (
            2.0 +   # Загрузка аватара
            0.1 +   # UPDATE БД
            0.1     # Логи
        )  # = 2.2 МБ

    def calculate_daily_load_per_user(self, accounts_per_user: int = 500) -> Dict[str, float]:
        """Расчет ежедневной нагрузки на одного пользователя"""
        
        operations = self.analyze_single_account_operations()
        
        # Количество операций в день на пользователя:
        daily_operations = {
            'post_publication': accounts_per_user * 5,      # 5 постов/день
            'warmup_action': accounts_per_user * 9 / 7,     # 9 в неделю = 1.3/день
            'imap_check': accounts_per_user * 1,            # 1 раз/день
            'health_check': accounts_per_user * 6,          # 6 раз/день
            'profile_update': accounts_per_user * 2 / 30    # 2 раза/месяц
        }
        
        # Суммарная нагрузка:
        total_load = {
            'cpu_seconds_per_day': 0,
            'peak_ram_mb': 0,
            'daily_disk_io_gb': 0,
            'concurrent_connections': 0
        }
        
        for op_name, daily_count in daily_operations.items():
            op_resource = operations[op_name]
            
            total_load['cpu_seconds_per_day'] += daily_count * op_resource.cpu_seconds
            total_load['peak_ram_mb'] += daily_count * op_resource.ram_mb * 0.1  # 10% одновременных
            total_load['daily_disk_io_gb'] += daily_count * op_resource.disk_io_mb / 1024
            total_load['concurrent_connections'] += op_resource.connections * 2  # Пиковый фактор
            
        return total_load

    def calculate_system_load_100_users(self) -> Dict[str, float]:
        """Расчет для 100 пользователей"""
        
        single_user_load = self.calculate_daily_load_per_user()
        
        # Connection pooling эффект - не линейное увеличение
        connection_efficiency = 0.7  # 70% эффективность пула
        
        system_load = {
            'total_cpu_seconds_per_day': single_user_load['cpu_seconds_per_day'] * 100,
            'peak_cpu_utilization': self._calculate_peak_cpu_load(single_user_load),
            'total_ram_gb': (single_user_load['peak_ram_mb'] * 100) / 1024,
            'total_disk_io_gb_per_day': single_user_load['daily_disk_io_gb'] * 100,
            'effective_connections': min(
                single_user_load['concurrent_connections'] * 100 * connection_efficiency,
                self.connection_pool_size * 0.8  # 80% от пула
            )
        }
        
        # Добавляем системные накладные расходы
        system_load.update(self._calculate_system_overhead())
        
        return system_load
        
    def _calculate_peak_cpu_load(self, single_user_load: Dict[str, float]) -> float:
        """Расчет пиковой CPU нагрузки"""
        
        # CPU секунд в день для одного пользователя
        cpu_per_user_per_day = single_user_load['cpu_seconds_per_day']
        
        # Предполагаем, что пиковая нагрузка в 3 раза выше средней
        # и длится 4 часа (14400 секунд)
        peak_duration_seconds = 4 * 60 * 60
        peak_factor = 3.0
        
        # CPU нагрузка в пиковый час для 100 пользователей
        peak_cpu_seconds_per_second = (
            (cpu_per_user_per_day * 100 * peak_factor) / 
            (24 * 60 * 60)  # Секунд в дне
        ) * peak_factor  # Еще раз умножаем на пиковый фактор
        
        return peak_cpu_seconds_per_second
        
    def _calculate_system_overhead(self) -> Dict[str, float]:
        """Системные накладные расходы"""
        
        return {
            'postgresql_ram_gb': 16,      # shared_buffers + effective_cache
            'redis_ram_gb': 4,            # Кэш + очереди
            'system_ram_gb': 6,           # ОС + мониторинг
            'connection_pool_ram_mb': self.connection_pool_size * self.connection_overhead_mb,
            'background_cpu_cores': 4,    # PostgreSQL, Redis, система
        }

    def analyze_connection_pooling_efficiency(self) -> Dict[str, float]:
        """Анализ эффективности connection pooling"""
        
        # Без pooling (наивный подход):
        naive_connections = 500 * 100 * 6  # аккаунты * юзеры * среднее кол-во подключений
        
        # С connection pooling:
        pooled_connections = self.connection_pool_size
        
        # Экономия ресурсов:
        ram_savings_gb = (naive_connections - pooled_connections) * self.connection_overhead_mb / 1024
        
        return {
            'naive_connections': naive_connections,
            'pooled_connections': pooled_connections,
            'connection_efficiency': pooled_connections / naive_connections,
            'ram_savings_gb': ram_savings_gb,
            'cpu_overhead_reduction': 0.6  # 60% меньше CPU на управление подключениями
        }

    def generate_detailed_report(self) -> Dict:
        """Генерация детального отчета"""
        
        single_ops = self.analyze_single_account_operations()
        single_user = self.calculate_daily_load_per_user()
        system_load = self.calculate_system_load_100_users()
        pooling_analysis = self.analyze_connection_pooling_efficiency()
        
        report = {
            'single_operation_analysis': {
                op_name: {
                    'cpu_seconds': op.cpu_seconds,
                    'ram_mb': op.ram_mb,
                    'disk_io_mb': op.disk_io_mb,
                    'connections': op.connections
                }
                for op_name, op in single_ops.items()
            },
            'single_user_daily_load': single_user,
            'system_load_100_users': system_load,
            'connection_pooling_analysis': pooling_analysis,
            'recommendations': self._generate_recommendations(system_load)
        }
        
        return report
        
    def _generate_recommendations(self, system_load: Dict[str, float]) -> Dict[str, str]:
        """Рекомендации по железу"""
        
        required_cpu_cores = max(8, int(system_load['peak_cpu_utilization'] * 1.5))
        required_ram_gb = max(32, int(system_load['total_ram_gb'] * 1.3))
        
        recommendations = {
            'minimum_cpu_cores': f"{required_cpu_cores} cores",
            'minimum_ram_gb': f"{required_ram_gb} GB",
            'recommended_architecture': "monolith" if required_cpu_cores <= 64 else "distributed",
            'database_optimization': "PostgreSQL with connection pooling mandatory",
            'storage_type': "NVMe SSD mandatory for high IOPS",
            'network': "1Gbit/s minimum for Instagram API calls"
        }
        
        # Рекомендация конкретного сервера:
        if required_cpu_cores <= 32 and required_ram_gb <= 64:
            recommendations['server_recommendation'] = "Hetzner AX51-NVMe (€39/month)"
        elif required_cpu_cores <= 64 and required_ram_gb <= 128:
            recommendations['server_recommendation'] = "Hetzner AX161 (€169/month)"
        else:
            recommendations['server_recommendation'] = "Distributed architecture required"
            
        return recommendations

def main():
    """Главная функция для запуска анализа"""
    
    print("🔬 ГЛУБОКИЙ АНАЛИЗ РЕСУРСОВ INSTAGRAM БОТА")
    print("=" * 80)
    
    analyzer = InstagramBotResourceAnalyzer()
    report = analyzer.generate_detailed_report()
    
    # Выводим отчет
    print("\n📊 АНАЛИЗ ОДНОЙ ОПЕРАЦИИ:")
    print("-" * 40)
    for op_name, op_data in report['single_operation_analysis'].items():
        print(f"{op_name}:")
        print(f"  CPU: {op_data['cpu_seconds']:.2f} сек")
        print(f"  RAM: {op_data['ram_mb']:.1f} МБ")
        print(f"  Диск: {op_data['disk_io_mb']:.2f} МБ")
        print(f"  Подключения: {op_data['connections']}")
        print()
    
    print("\n👤 НАГРУЗКА ОДНОГО ПОЛЬЗОВАТЕЛЯ (500 аккаунтов):")
    print("-" * 50)
    user_load = report['single_user_daily_load']
    print(f"CPU в день: {user_load['cpu_seconds_per_day']:.0f} секунд")
    print(f"Пиковая RAM: {user_load['peak_ram_mb']:.0f} МБ")
    print(f"Диск в день: {user_load['daily_disk_io_gb']:.2f} ГБ")
    print(f"Подключения: {user_load['concurrent_connections']}")
    
    print("\n🏭 СИСТЕМНАЯ НАГРУЗКА (100 ПОЛЬЗОВАТЕЛЕЙ):")
    print("-" * 50)
    sys_load = report['system_load_100_users']
    print(f"Пиковая CPU нагрузка: {sys_load['peak_cpu_utilization']:.1f} ядер")
    print(f"Общая RAM: {sys_load['total_ram_gb']:.1f} ГБ")
    print(f"Диск в день: {sys_load['total_disk_io_gb_per_day']:.1f} ГБ")
    print(f"Эффективные подключения: {sys_load['effective_connections']:.0f}")
    
    print("\n🔗 АНАЛИЗ CONNECTION POOLING:")
    print("-" * 40)
    pooling = report['connection_pooling_analysis']
    print(f"Без pooling: {pooling['naive_connections']:,} подключений")
    print(f"С pooling: {pooling['pooled_connections']} подключений")
    print(f"Эффективность: {pooling['connection_efficiency']:.1%}")
    print(f"Экономия RAM: {pooling['ram_savings_gb']:.1f} ГБ")
    
    print("\n💡 РЕКОМЕНДАЦИИ:")
    print("-" * 20)
    recs = report['recommendations']
    for key, value in recs.items():
        print(f"{key.replace('_', ' ').title()}: {value}")
    
    # Сохраняем детальный отчет
    with open('detailed_resource_analysis.json', 'w', encoding='utf-8') as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    
    print(f"\n💾 Детальный отчет сохранен в 'detailed_resource_analysis.json'")
    
    return report

if __name__ == "__main__":
    main() 
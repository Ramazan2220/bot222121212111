#!/usr/bin/env python3
"""
Монитор системных ресурсов для динамической корректировки нагрузки
Гибкая система с процентной шкалой 0-100% и настраиваемыми уровнями
"""

import psutil
import time
import logging
import threading
from typing import Dict, Tuple, List
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class SystemMetrics:
    """Метрики системы"""
    cpu_percent: float
    memory_percent: float
    disk_io_read: float
    disk_io_write: float
    network_io_sent: float
    network_io_recv: float
    temperature: float
    load_average: float

@dataclass
class WorkloadLimits:
    """Лимиты рабочей нагрузки"""
    max_workers: int
    batch_size: int
    delay_between_batches: float
    timeout_multiplier: float
    description: str

@dataclass
class LoadLevel:
    """Уровень нагрузки системы"""
    name: str
    min_load: int  # Минимальный процент нагрузки (0-100)
    max_load: int  # Максимальный процент нагрузки (0-100)
    emoji: str
    color: str
    workload: WorkloadLimits

class SystemResourceMonitor:
    """Монитор системных ресурсов с гибкой процентной системой"""
    
    def __init__(self, hardware_profile: str = "macbook"):
        self.is_monitoring = False
        self.monitor_thread = None
        self.current_metrics = None
        self.metrics_lock = threading.Lock()
        self.hardware_profile = hardware_profile
        
        # Защитные лимиты - система никогда не должна превышать эти значения
        self.safety_limits = {
            "max_cpu_percent": 95,      # Максимальный CPU 95% (было 80%)
            "max_memory_percent": 92,   # Максимальная память 92% (было 85%)
            "max_load_average": 25.0,   # Максимальная нагрузка 25.0 (было 12.0) - для MacBook с многими процессами
            "max_temperature": 80,      # Максимальная температура 80°C
            "emergency_cooldown": 30,   # Время охлаждения 30 секунд (было 60)
        }
        
        # Настройки для разных типов железа
        self.hardware_profiles = {
            "macbook": {
                "cpu_weight": 0.4,      # Высокий вес CPU (MacBook быстро греется)
                "memory_weight": 0.3,   # Средний вес памяти
                "temp_weight": 0.2,     # Важна температура
                "load_weight": 0.1,     # Низкий вес load average
                "temp_critical": 85,    # Критическая температура для MacBook
            },
            "server": {
                "cpu_weight": 0.3,      # Средний вес CPU (сервер выдерживает нагрузку)
                "memory_weight": 0.4,   # Высокий вес памяти (много RAM)
                "temp_weight": 0.1,     # Низкий вес температуры (хорошее охлаждение)
                "load_weight": 0.2,     # Средний вес load average
                "temp_critical": 95,    # Критическая температура для сервера
            },
            "vps": {
                "cpu_weight": 0.35,     # Средне-высокий вес CPU
                "memory_weight": 0.35,  # Средне-высокий вес памяти
                "temp_weight": 0.05,    # Очень низкий вес температуры (виртуализация)
                "load_weight": 0.25,    # Высокий вес load average
                "temp_critical": 90,    # Температура для VPS
            }
        }
        
        # 8 уровней нагрузки системы (0-100%)
        self.load_levels = [
            LoadLevel(
                name="МИНИМАЛЬНАЯ",
                min_load=0, max_load=10,
                emoji="🟢", color="green",
                workload=WorkloadLimits(
                    max_workers=8, batch_size=12, delay_between_batches=2.0,
                    timeout_multiplier=0.6, description="Максимальная активность бота"
                )
            ),
            LoadLevel(
                name="ОЧЕНЬ НИЗКАЯ", 
                min_load=11, max_load=20,
                emoji="🟢", color="green",
                workload=WorkloadLimits(
                    max_workers=6, batch_size=10, delay_between_batches=3.0,
                    timeout_multiplier=0.7, description="Высокая активность бота"
                )
            ),
            LoadLevel(
                name="НИЗКАЯ",
                min_load=21, max_load=35,
                emoji="🟡", color="yellow", 
                workload=WorkloadLimits(
                    max_workers=5, batch_size=8, delay_between_batches=5.0,
                    timeout_multiplier=0.8, description="Повышенная активность бота"
                )
            ),
            LoadLevel(
                name="УМЕРЕННАЯ",
                min_load=36, max_load=50,
                emoji="🟡", color="yellow",
                workload=WorkloadLimits(
                    max_workers=4, batch_size=6, delay_between_batches=8.0,
                    timeout_multiplier=1.0, description="Стандартная активность бота"
                )
            ),
            LoadLevel(
                name="СРЕДНЯЯ",
                min_load=51, max_load=65,
                emoji="🟠", color="orange",
                workload=WorkloadLimits(
                    max_workers=3, batch_size=5, delay_between_batches=12.0,
                    timeout_multiplier=1.2, description="Ограниченная активность бота"
                )
            ),
            LoadLevel(
                name="ПОВЫШЕННАЯ",
                min_load=66, max_load=75,
                emoji="🟠", color="orange",
                workload=WorkloadLimits(
                    max_workers=2, batch_size=4, delay_between_batches=18.0,
                    timeout_multiplier=1.5, description="Сниженная активность бота"
                )
            ),
            LoadLevel(
                name="ВЫСОКАЯ",
                min_load=76, max_load=85,
                emoji="🔴", color="red",
                workload=WorkloadLimits(
                    max_workers=2, batch_size=3, delay_between_batches=25.0,
                    timeout_multiplier=2.0, description="Минимальная активность бота"
                )
            ),
            LoadLevel(
                name="КРИТИЧЕСКАЯ",
                min_load=86, max_load=100,
                emoji="🔴", color="red",
                workload=WorkloadLimits(
                    max_workers=1, batch_size=1, delay_between_batches=60.0,
                    timeout_multiplier=3.0, description="ЭКСТРЕННАЯ ОСТАНОВКА - защита системы"
                )
            )
        ]
        
        # Защитный уровень - активируется при превышении safety_limits
        self.emergency_level = LoadLevel(
            name="ЗАЩИТНЫЙ РЕЖИМ",
            min_load=101, max_load=200,  # Виртуальный диапазон для экстренного режима
            emoji="🚨", color="emergency",
            workload=WorkloadLimits(
                max_workers=1, batch_size=1, delay_between_batches=120.0,
                timeout_multiplier=5.0, description="ЭКСТРЕННАЯ ОСТАНОВКА - система перегружена"
            )
        )
        
                 # Время последнего превышения лимитов для cooldown
        self.last_emergency_time = 0
        
        # Адаптивная защита - текущий уровень снижения (0-100%)
        self.adaptive_reduction_level = 0  # 0 = нет снижения, 100 = максимальное снижение
        self.last_adaptation_time = 0
        self.adaptation_interval = 30  # Интервал адаптации в секундах
        
        # Настройки адаптивной защиты
        self.adaptive_settings = {
            "warning_threshold": 0.85,      # При 85% от лимита начинаем снижение
            "critical_threshold": 0.95,     # При 95% от лимита критическое снижение
            "adaptation_step": 10,          # Шаг адаптации (10%)
            "recovery_step": 5,             # Шаг восстановления (5%)
            "max_reduction": 80,            # Максимальное снижение (80%)
            "stability_time": 60,           # Время стабильности для восстановления
        }
    
    def set_hardware_profile(self, profile: str):
        """Устанавливает профиль железа"""
        if profile in self.hardware_profiles:
            self.hardware_profile = profile
            logger.info(f"🖥️ Установлен профиль железа: {profile}")
        else:
            logger.warning(f"⚠️ Неизвестный профиль железа: {profile}")
    
    def check_safety_limits(self, metrics: SystemMetrics) -> bool:
        """Проверяет, не превышены ли защитные лимиты"""
        if not metrics:
            return False
        
        # Проверяем каждый лимит
        limits_exceeded = []
        
        if metrics.cpu_percent > self.safety_limits["max_cpu_percent"]:
            limits_exceeded.append(f"CPU: {metrics.cpu_percent:.1f}% > {self.safety_limits['max_cpu_percent']}%")
        
        if metrics.memory_percent > self.safety_limits["max_memory_percent"]:
            limits_exceeded.append(f"RAM: {metrics.memory_percent:.1f}% > {self.safety_limits['max_memory_percent']}%")
        
        if metrics.load_average > self.safety_limits["max_load_average"]:
            limits_exceeded.append(f"Load: {metrics.load_average:.2f} > {self.safety_limits['max_load_average']}")
        
        if metrics.temperature > 0 and metrics.temperature > self.safety_limits["max_temperature"]:
            limits_exceeded.append(f"Temp: {metrics.temperature:.1f}°C > {self.safety_limits['max_temperature']}°C")
        
        if limits_exceeded:
            # Обновляем время последнего превышения
            self.last_emergency_time = time.time()
            
            # Логируем критическое превышение
            logger.critical(f"🚨 ПРЕВЫШЕНЫ ЗАЩИТНЫЕ ЛИМИТЫ: {', '.join(limits_exceeded)}")
            logger.critical(f"🚨 АКТИВИРОВАН ЗАЩИТНЫЙ РЕЖИМ - система будет принудительно охлаждена")
            
            return True
        
        return False
    
    def is_in_cooldown(self) -> bool:
        """Проверяет, находится ли система в режиме охлаждения"""
        if self.last_emergency_time == 0:
            return False
        
        cooldown_remaining = self.safety_limits["emergency_cooldown"] - (time.time() - self.last_emergency_time)
        return cooldown_remaining > 0
    
    def calculate_system_stress(self, metrics: SystemMetrics) -> float:
        """Вычисляет общий стресс системы (0.0 - 1.0)"""
        if not metrics:
            return 0.0
        
        # Нормализуем каждую метрику к диапазону 0-1 относительно лимитов
        cpu_stress = min(1.0, metrics.cpu_percent / self.safety_limits["max_cpu_percent"])
        memory_stress = min(1.0, metrics.memory_percent / self.safety_limits["max_memory_percent"])
        load_stress = min(1.0, metrics.load_average / self.safety_limits["max_load_average"])
        
        # Температурный стресс (если доступен)
        temp_stress = 0.0
        if metrics.temperature > 0:
            temp_stress = min(1.0, metrics.temperature / self.safety_limits["max_temperature"])
        
        # Профиль железа определяет веса для разных метрик
        profile_weights = self.hardware_profiles[self.hardware_profile]
        
        # Вычисляем взвешенный стресс
        total_stress = (
            cpu_stress * profile_weights["cpu_weight"] +
            memory_stress * profile_weights["memory_weight"] +
            temp_stress * profile_weights["temp_weight"] +
            load_stress * profile_weights["load_weight"]
        )
        
        return min(1.0, total_stress)
    
    def adapt_protection_level(self, metrics: SystemMetrics) -> int:
        """Адаптивно изменяет уровень защиты на основе стресса системы"""
        current_time = time.time()
        
        # Проверяем, нужно ли адаптироваться
        if current_time - self.last_adaptation_time < self.adaptation_interval:
            return self.adaptive_reduction_level
        
        self.last_adaptation_time = current_time
        
        # Вычисляем текущий стресс
        stress_level = self.calculate_system_stress(metrics)
        
        # Определяем нужное действие
        if stress_level >= self.adaptive_settings["critical_threshold"]:
            # Критический уровень - быстрое снижение
            increase = self.adaptive_settings["adaptation_step"] * 2
            self.adaptive_reduction_level = min(
                self.adaptive_settings["max_reduction"],
                self.adaptive_reduction_level + increase
            )
            logger.warning(f"🔴 КРИТИЧЕСКИЙ СТРЕСС ({stress_level:.1%}) - увеличиваем защиту до {self.adaptive_reduction_level}%")
            
        elif stress_level >= self.adaptive_settings["warning_threshold"]:
            # Предупредительный уровень - постепенное снижение
            increase = self.adaptive_settings["adaptation_step"]
            self.adaptive_reduction_level = min(
                self.adaptive_settings["max_reduction"],
                self.adaptive_reduction_level + increase
            )
            logger.info(f"🟡 ПОВЫШЕННЫЙ СТРЕСС ({stress_level:.1%}) - увеличиваем защиту до {self.adaptive_reduction_level}%")
            
        else:
            # Нормальный уровень - постепенное восстановление
            if self.adaptive_reduction_level > 0:
                decrease = self.adaptive_settings["recovery_step"]
                self.adaptive_reduction_level = max(0, self.adaptive_reduction_level - decrease)
                logger.info(f"🟢 НОРМАЛЬНЫЙ СТРЕСС ({stress_level:.1%}) - снижаем защиту до {self.adaptive_reduction_level}%")
        
        return self.adaptive_reduction_level
    
    def apply_adaptive_reduction(self, base_workload: WorkloadLimits, reduction_percent: int) -> WorkloadLimits:
        """Применяет адаптивное снижение к базовым лимитам"""
        if reduction_percent == 0:
            return base_workload
        
        # Вычисляем коэффициент снижения (0.0 - 1.0)
        reduction_factor = reduction_percent / 100.0
        
        # Применяем снижение к различным параметрам
        reduced_workers = max(1, int(base_workload.max_workers * (1 - reduction_factor * 0.8)))
        reduced_batch = max(1, int(base_workload.batch_size * (1 - reduction_factor * 0.6)))
        increased_delay = base_workload.delay_between_batches * (1 + reduction_factor * 2.0)
        increased_timeout = base_workload.timeout_multiplier * (1 + reduction_factor * 1.5)
        
        return WorkloadLimits(
            max_workers=reduced_workers,
            batch_size=reduced_batch,
            delay_between_batches=increased_delay,
            timeout_multiplier=increased_timeout,
            description=f"{base_workload.description} (адаптивно снижено на {reduction_percent}%)"
        )
    
    def get_system_metrics(self) -> SystemMetrics:
        """Получает текущие метрики системы"""
        try:
            # CPU
            cpu_percent = psutil.cpu_percent(interval=1)
            
            # Память
            memory = psutil.virtual_memory()
            memory_percent = memory.percent
            
            # Диск I/O
            disk_io = psutil.disk_io_counters()
            disk_io_read = disk_io.read_bytes / (1024 * 1024) if disk_io else 0  # MB
            disk_io_write = disk_io.write_bytes / (1024 * 1024) if disk_io else 0  # MB
            
            # Сеть I/O
            net_io = psutil.net_io_counters()
            network_io_sent = net_io.bytes_sent / (1024 * 1024) if net_io else 0  # MB
            network_io_recv = net_io.bytes_recv / (1024 * 1024) if net_io else 0  # MB
            
            # Температура (если доступна)
            temperature = self._get_temperature()
            
            # Средняя нагрузка
            load_average = psutil.getloadavg()[0] if hasattr(psutil, 'getloadavg') else 0
            
            return SystemMetrics(
                cpu_percent=cpu_percent,
                memory_percent=memory_percent,
                disk_io_read=disk_io_read,
                disk_io_write=disk_io_write,
                network_io_sent=network_io_sent,
                network_io_recv=network_io_recv,
                temperature=temperature,
                load_average=load_average
            )
        except Exception as e:
            logger.error(f"Ошибка получения метрик системы: {e}")
            return None
    
    def _get_temperature(self) -> float:
        """Получает температуру процессора (если доступна)"""
        try:
            if hasattr(psutil, "sensors_temperatures"):
                temps = psutil.sensors_temperatures()
                if temps:
                    # Берем первую доступную температуру
                    for name, entries in temps.items():
                        if entries:
                            return entries[0].current
            return 0.0
        except:
            return 0.0
    
    def calculate_system_load_percentage(self) -> int:
        """Вычисляет общий процент нагрузки системы (0-100%)"""
        metrics = self.get_system_metrics()
        if not metrics:
            return 50  # Безопасный fallback
        
        with self.metrics_lock:
            self.current_metrics = metrics
        
        # Получаем настройки для текущего профиля железа
        profile = self.hardware_profiles.get(self.hardware_profile, self.hardware_profiles["macbook"])
        
        # Нормализуем метрики к шкале 0-100
        cpu_load = min(metrics.cpu_percent, 100)
        memory_load = min(metrics.memory_percent, 100)
        
        # Температурная нагрузка (0-100% от критической температуры)
        temp_load = 0
        if metrics.temperature > 0:
            temp_load = min((metrics.temperature / profile["temp_critical"]) * 100, 100)
        
        # Load average нагрузка (приводим к процентам)
        # Для большинства систем load average > 4 считается высокой нагрузкой
        load_avg_load = min((metrics.load_average / 4.0) * 100, 100)
        
        # Взвешенный расчет общей нагрузки
        total_load = (
            cpu_load * profile["cpu_weight"] +
            memory_load * profile["memory_weight"] +
            temp_load * profile["temp_weight"] +
            load_avg_load * profile["load_weight"]
        )
        
        return int(min(max(total_load, 0), 100))
    
    def get_load_level(self) -> LoadLevel:
        """Получает текущий уровень нагрузки с учетом адаптивной защиты"""
        metrics = self.current_metrics
        if not metrics:
            metrics = self.get_system_metrics()
        
        # ПРИОРИТЕТ 1: Проверяем критические лимиты (полная остановка)
        if metrics and self.check_safety_limits(metrics):
            logger.critical("🚨 АКТИВИРОВАН ЗАЩИТНЫЙ РЕЖИМ - превышены критические лимиты!")
            return self.emergency_level
        
        # ПРИОРИТЕТ 2: Проверяем режим охлаждения
        if self.is_in_cooldown():
            cooldown_remaining = self.safety_limits["emergency_cooldown"] - (time.time() - self.last_emergency_time)
            logger.warning(f"❄️ РЕЖИМ ОХЛАЖДЕНИЯ: осталось {cooldown_remaining:.0f}с до восстановления")
            return self.emergency_level
        
        # ПРИОРИТЕТ 3: Адаптивная защита (плавное снижение)
        reduction_level = self.adapt_protection_level(metrics)
        
        # ПРИОРИТЕТ 4: Обычный расчет нагрузки
        load_percentage = self.calculate_system_load_percentage()
        
        # Находим базовый уровень
        base_level = None
        for level in self.load_levels:
            if level.min_load <= load_percentage <= level.max_load:
                base_level = level
                break
        
        if not base_level:
            base_level = self.load_levels[4]  # СРЕДНЯЯ
        
        # Если есть адаптивное снижение, применяем его
        if reduction_level > 0:
            adapted_workload = self.apply_adaptive_reduction(base_level.workload, reduction_level)
            
            # Создаем адаптированный уровень
            adapted_level = LoadLevel(
                name=f"{base_level.name} (⚡{reduction_level}%)",
                min_load=base_level.min_load,
                max_load=base_level.max_load,
                emoji="⚡",
                color="adaptive",
                workload=adapted_workload
            )
            
            return adapted_level
        
        return base_level
    
    def get_workload_limits(self) -> WorkloadLimits:
        """Получает рекомендуемые лимиты нагрузки"""
        level = self.get_load_level()
        return level.workload
    
    def get_system_status(self) -> Dict:
        """Получает подробный статус системы"""
        metrics = self.current_metrics
        if not metrics:
            metrics = self.get_system_metrics()
        
        if not metrics:
            return {"status": "unknown", "message": "Не удалось получить метрики"}
        
        load_percentage = self.calculate_system_load_percentage()
        level = self.get_load_level()
        limits = level.workload
        
        # Проверяем состояние защитных лимитов
        safety_status = "OK"
        safety_warnings = []
        
        if metrics:
            if metrics.cpu_percent > self.safety_limits["max_cpu_percent"] * 0.9:  # 90% от лимита
                safety_warnings.append("CPU приближается к лимиту")
            if metrics.memory_percent > self.safety_limits["max_memory_percent"] * 0.9:
                safety_warnings.append("RAM приближается к лимиту")
            if metrics.load_average > self.safety_limits["max_load_average"] * 0.9:
                safety_warnings.append("Load приближается к лимиту")
            if metrics.temperature > 0 and metrics.temperature > self.safety_limits["max_temperature"] * 0.9:
                safety_warnings.append("Температура приближается к лимиту")
        
        if safety_warnings:
            safety_status = "WARNING"
        
        if level.name == "ЗАЩИТНЫЙ РЕЖИМ":
            safety_status = "EMERGENCY"
        elif self.adaptive_reduction_level > 0:
            safety_status = "ADAPTIVE"
        
        # Вычисляем текущий стресс системы
        stress_level = self.calculate_system_stress(metrics) if metrics else 0.0
        
        return {
            "load_percentage": load_percentage,
            "level": level,
            "hardware_profile": self.hardware_profile,
            "status": level.name.lower().replace(" ", "_"),
            "emoji": level.emoji,
            "message": f"{level.emoji} НАГРУЗКА СИСТЕМЫ: {load_percentage}% ({level.name}) - {limits.description}",
            "safety_status": safety_status,
            "safety_warnings": safety_warnings,
            "is_in_cooldown": self.is_in_cooldown(),
            "cooldown_remaining": max(0, self.safety_limits["emergency_cooldown"] - (time.time() - self.last_emergency_time)) if self.last_emergency_time > 0 else 0,
            "adaptive_protection": {
                "reduction_level": self.adaptive_reduction_level,
                "stress_level": stress_level,
                "is_active": self.adaptive_reduction_level > 0,
                "next_adaptation": max(0, self.adaptation_interval - (time.time() - self.last_adaptation_time)) if self.last_adaptation_time > 0 else 0,
            },
            "metrics": {
                "cpu": f"{metrics.cpu_percent:.1f}%",
                "memory": f"{metrics.memory_percent:.1f}%",
                "temperature": f"{metrics.temperature:.1f}°C" if metrics.temperature > 0 else "N/A",
                "load_avg": f"{metrics.load_average:.2f}",
            },
            "safety_limits": {
                "max_cpu": f"{self.safety_limits['max_cpu_percent']}%",
                "max_memory": f"{self.safety_limits['max_memory_percent']}%",
                "max_load": f"{self.safety_limits['max_load_average']:.1f}",
                "max_temp": f"{self.safety_limits['max_temperature']}°C",
                "cooldown": f"{self.safety_limits['emergency_cooldown']}s",
            },
            "limits": {
                "max_workers": limits.max_workers,
                "batch_size": limits.batch_size,
                "delay_between_batches": f"{limits.delay_between_batches:.1f}s",
                "timeout_multiplier": f"{limits.timeout_multiplier:.1f}x",
            }
        }
    
    def start_monitoring(self, interval: float = 10.0):
        """Запускает мониторинг в отдельном потоке"""
        if self.is_monitoring:
            return
        
        self.is_monitoring = True
        self.monitor_thread = threading.Thread(
            target=self._monitor_loop,
            args=(interval,),
            daemon=True
        )
        self.monitor_thread.start()
        logger.info(f"🖥️ Запущен мониторинг системных ресурсов (профиль: {self.hardware_profile})")
    
    def stop_monitoring(self):
        """Останавливает мониторинг"""
        self.is_monitoring = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=5.0)
        logger.info("🖥️ Мониторинг системных ресурсов остановлен")
    
    def _monitor_loop(self, interval: float):
        """Основной цикл мониторинга"""
        while self.is_monitoring:
            try:
                metrics = self.get_system_metrics()
                if metrics:
                    with self.metrics_lock:
                        self.current_metrics = metrics
                    
                    # Логируем каждые 60 секунд
                    if int(time.time()) % 60 == 0:
                        status = self.get_system_status()
                        logger.info(f"🖥️ {status['message']} | "
                                  f"CPU: {status['metrics']['cpu']} | "
                                  f"RAM: {status['metrics']['memory']} | "
                                  f"Temp: {status['metrics']['temperature']}")
                
                time.sleep(interval)
            except Exception as e:
                logger.error(f"Ошибка в цикле мониторинга: {e}")
                time.sleep(interval)

# Глобальный экземпляр монитора
system_monitor = SystemResourceMonitor()

def set_hardware_profile(profile: str):
    """Устанавливает профиль железа"""
    system_monitor.set_hardware_profile(profile)

def get_adaptive_limits() -> WorkloadLimits:
    """Получает адаптивные лимиты нагрузки"""
    return system_monitor.get_workload_limits()

def get_system_status() -> Dict:
    """Получает статус системы"""
    return system_monitor.get_system_status()

def get_system_load_percentage() -> int:
    """Получает процент нагрузки системы"""
    return system_monitor.calculate_system_load_percentage()

def start_system_monitoring():
    """Запускает мониторинг системы"""
    system_monitor.start_monitoring()

def stop_system_monitoring():
    """Останавливает мониторинг системы"""
    system_monitor.stop_monitoring()

def set_safety_limits(max_cpu=None, max_memory=None, max_load=None, max_temp=None, cooldown=None):
    """Устанавливает защитные лимиты"""
    if max_cpu is not None:
        system_monitor.safety_limits["max_cpu_percent"] = max_cpu
    if max_memory is not None:
        system_monitor.safety_limits["max_memory_percent"] = max_memory
    if max_load is not None:
        system_monitor.safety_limits["max_load_average"] = max_load
    if max_temp is not None:
        system_monitor.safety_limits["max_temperature"] = max_temp
    if cooldown is not None:
        system_monitor.safety_limits["emergency_cooldown"] = cooldown
    
    logger.info(f"🛡️ Обновлены защитные лимиты: CPU≤{system_monitor.safety_limits['max_cpu_percent']}%, "
               f"RAM≤{system_monitor.safety_limits['max_memory_percent']}%, "
               f"Load≤{system_monitor.safety_limits['max_load_average']}, "
               f"Temp≤{system_monitor.safety_limits['max_temperature']}°C")

def get_safety_limits():
    """Получает текущие защитные лимиты"""
    return system_monitor.safety_limits.copy()

def force_cooldown():
    """Принудительно активирует режим охлаждения"""
    system_monitor.last_emergency_time = time.time()
    logger.warning("❄️ Принудительно активирован режим охлаждения")

def reset_cooldown():
    """Сбрасывает режим охлаждения"""
    system_monitor.last_emergency_time = 0
    logger.info("✅ Режим охлаждения сброшен")

def get_adaptive_protection_info():
    """Получает информацию об адаптивной защите"""
    return {
        "reduction_level": system_monitor.adaptive_reduction_level,
        "settings": system_monitor.adaptive_settings.copy(),
        "last_adaptation": system_monitor.last_adaptation_time,
        "next_adaptation": max(0, system_monitor.adaptation_interval - (time.time() - system_monitor.last_adaptation_time)) if system_monitor.last_adaptation_time > 0 else 0,
    }

def set_adaptive_protection_settings(warning_threshold=None, critical_threshold=None, 
                                   adaptation_step=None, recovery_step=None, 
                                   max_reduction=None, adaptation_interval=None):
    """Настраивает параметры адаптивной защиты"""
    if warning_threshold is not None:
        system_monitor.adaptive_settings["warning_threshold"] = warning_threshold
    if critical_threshold is not None:
        system_monitor.adaptive_settings["critical_threshold"] = critical_threshold
    if adaptation_step is not None:
        system_monitor.adaptive_settings["adaptation_step"] = adaptation_step
    if recovery_step is not None:
        system_monitor.adaptive_settings["recovery_step"] = recovery_step
    if max_reduction is not None:
        system_monitor.adaptive_settings["max_reduction"] = max_reduction
    if adaptation_interval is not None:
        system_monitor.adaptation_interval = adaptation_interval
    
    logger.info(f"⚡ Обновлены настройки адаптивной защиты: {system_monitor.adaptive_settings}")

def reset_adaptive_protection():
    """Сбрасывает адаптивную защиту"""
    system_monitor.adaptive_reduction_level = 0
    system_monitor.last_adaptation_time = 0
    logger.info("⚡ Адаптивная защита сброшена")

def force_adaptive_protection(reduction_level: int):
    """Принудительно устанавливает уровень адаптивной защиты"""
    system_monitor.adaptive_reduction_level = max(0, min(100, reduction_level))
    system_monitor.last_adaptation_time = time.time()
    logger.info(f"⚡ Принудительно установлен уровень адаптивной защиты: {reduction_level}%") 
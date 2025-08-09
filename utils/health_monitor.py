#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Система мониторинга здоровья изоляции пользователей
Проверяет состояние всех компонентов системы изоляции
"""

import logging
import json
import time
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from pathlib import Path

from database.user_management import get_active_users, get_user_info, validate_user_exists
from utils.user_cache import get_user_cache
from utils.processing_state import health_check_processing_states
from database.db_manager import get_session
from database.models import InstagramAccount, TelegramUser

logger = logging.getLogger(__name__)

class UserIsolationHealthMonitor:
    """
    Монитор здоровья системы изоляции пользователей
    """
    
    def __init__(self, check_interval: int = 300):  # 5 минут по умолчанию
        self.check_interval = check_interval
        self.is_running = False
        self.monitor_thread = None
        self.health_history = []
        self.max_history_size = 100
        
        # Статистика
        self.stats = {
            "checks_performed": 0,
            "last_check_time": None,
            "errors_detected": 0,
            "warnings_detected": 0
        }
        
        # Блокировка для thread-safety
        self._lock = threading.Lock()
    
    def start_monitoring(self):
        """Запустить мониторинг"""
        if self.is_running:
            logger.warning("🔍 Мониторинг уже запущен")
            return
        
        self.is_running = True
        self.monitor_thread = threading.Thread(target=self._monitoring_loop, daemon=True)
        self.monitor_thread.start()
        
        logger.info(f"🔍 Мониторинг изоляции пользователей запущен (интервал: {self.check_interval}с)")
    
    def stop_monitoring(self):
        """Остановить мониторинг"""
        self.is_running = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=5)
        
        logger.info("🔍 Мониторинг изоляции пользователей остановлен")
    
    def _monitoring_loop(self):
        """Основной цикл мониторинга"""
        while self.is_running:
            try:
                self.perform_health_check()
                time.sleep(self.check_interval)
            except Exception as e:
                logger.error(f"❌ Ошибка в цикле мониторинга: {e}")
                time.sleep(60)  # Увеличиваем интервал при ошибках
    
    def perform_health_check(self) -> Dict[str, Any]:
        """
        Выполнить полную проверку здоровья системы изоляции
        
        Returns:
            Dict: Результаты проверки
        """
        with self._lock:
            check_start_time = datetime.now()
            health_status = {
                "timestamp": check_start_time.isoformat(),
                "overall_status": "UNKNOWN",
                "components": {},
                "issues": [],
                "warnings": [],
                "statistics": {}
            }
            
            try:
                # 1. Проверка основных функций управления пользователями
                health_status["components"]["user_management"] = self._check_user_management()
                
                # 2. Проверка кеширования пользователей
                health_status["components"]["user_cache"] = self._check_user_cache()
                
                # 3. Проверка состояния обработки
                health_status["components"]["processing_states"] = self._check_processing_states()
                
                # 4. Проверка изоляции данных
                health_status["components"]["data_isolation"] = self._check_data_isolation()
                
                # 5. Проверка базы данных
                health_status["components"]["database"] = self._check_database_health()
                
                # 6. Проверка orphaned данных
                health_status["components"]["orphaned_data"] = self._check_orphaned_data()
                
                # Анализ общего статуса
                overall_status, issues, warnings = self._analyze_health_status(health_status["components"])
                health_status["overall_status"] = overall_status
                health_status["issues"] = issues
                health_status["warnings"] = warnings
                
                # Статистика
                check_duration = (datetime.now() - check_start_time).total_seconds()
                health_status["statistics"] = {
                    "check_duration_seconds": check_duration,
                    "total_components": len(health_status["components"]),
                    "healthy_components": len([c for c in health_status["components"].values() if c.get("status") == "HEALTHY"]),
                    "warning_components": len([c for c in health_status["components"].values() if c.get("status") == "WARNING"]),
                    "error_components": len([c for c in health_status["components"].values() if c.get("status") == "ERROR"])
                }
                
                # Обновляем статистику
                self.stats["checks_performed"] += 1
                self.stats["last_check_time"] = check_start_time.isoformat()
                self.stats["errors_detected"] += len(issues)
                self.stats["warnings_detected"] += len(warnings)
                
                # Сохраняем в историю
                self._save_to_history(health_status)
                
                # Логируем результат
                if overall_status == "HEALTHY":
                    logger.info(f"✅ Health check завершен: {overall_status} ({check_duration:.2f}с)")
                elif overall_status == "WARNING":
                    logger.warning(f"⚠️ Health check завершен: {overall_status} ({len(warnings)} предупреждений)")
                else:
                    logger.error(f"❌ Health check завершен: {overall_status} ({len(issues)} проблем)")
                
                return health_status
                
            except Exception as e:
                logger.error(f"❌ Критическая ошибка health check: {e}")
                health_status["overall_status"] = "ERROR"
                health_status["issues"] = [f"Критическая ошибка: {str(e)}"]
                return health_status
    
    def _check_user_management(self) -> Dict[str, Any]:
        """Проверка основных функций управления пользователями"""
        try:
            start_time = time.time()
            
            # Проверяем функцию get_active_users
            users = get_active_users()
            user_count = len(users)
            
            # Проверяем информацию о пользователях
            user_info_errors = 0
            for user_id in users[:5]:  # Проверяем первых 5 для быстроты
                try:
                    get_user_info(user_id)
                except Exception:
                    user_info_errors += 1
            
            duration = time.time() - start_time
            
            status = "HEALTHY"
            issues = []
            if user_count == 0:
                status = "WARNING"
                issues.append("Нет активных пользователей в системе")
            elif user_info_errors > 0:
                status = "WARNING"
                issues.append(f"Ошибки получения информации о {user_info_errors} пользователях")
            
            return {
                "status": status,
                "user_count": user_count,
                "user_info_errors": user_info_errors,
                "response_time": duration,
                "issues": issues
            }
            
        except Exception as e:
            return {
                "status": "ERROR",
                "error": str(e),
                "issues": [f"Ошибка функций управления пользователями: {str(e)}"]
            }
    
    def _check_user_cache(self) -> Dict[str, Any]:
        """Проверка системы кеширования пользователей"""
        try:
            start_time = time.time()
            
            user_cache = get_user_cache()
            
            # Проверяем получение пользователей через кеш
            cached_users = user_cache.get_active_users_safe()
            
            # Получаем статистику кеша
            cache_stats = user_cache.get_cache_stats()
            
            duration = time.time() - start_time
            
            status = "HEALTHY"
            issues = []
            warnings = []
            
            if not cached_users:
                status = "WARNING"
                warnings.append("Кеш пользователей пуст")
            
            if not cache_stats.get("cache_valid", False):
                warnings.append("Кеш пользователей устарел")
            
            if cache_stats.get("stats", {}).get("errors", 0) > 10:
                status = "WARNING"
                warnings.append(f"Много ошибок кеша: {cache_stats['stats']['errors']}")
            
            return {
                "status": status,
                "cached_users_count": len(cached_users),
                "cache_stats": cache_stats,
                "response_time": duration,
                "issues": issues,
                "warnings": warnings
            }
            
        except Exception as e:
            return {
                "status": "ERROR",
                "error": str(e),
                "issues": [f"Ошибка системы кеширования: {str(e)}"]
            }
    
    def _check_processing_states(self) -> Dict[str, Any]:
        """Проверка состояния процессов обработки"""
        try:
            start_time = time.time()
            
            processing_health = health_check_processing_states()
            
            duration = time.time() - start_time
            
            status = "HEALTHY"
            issues = []
            warnings = []
            
            summary = processing_health.get("summary", {})
            
            if summary.get("failed_processes", 0) > 0:
                status = "WARNING"
                warnings.append(f"Процессы с ошибками: {summary['failed_processes']}")
            
            if summary.get("active_processes", 0) > 3:
                warnings.append(f"Много активных процессов: {summary['active_processes']}")
            
            return {
                "status": status,
                "processing_summary": summary,
                "response_time": duration,
                "issues": issues,
                "warnings": warnings
            }
            
        except Exception as e:
            return {
                "status": "ERROR",
                "error": str(e),
                "issues": [f"Ошибка проверки состояния процессов: {str(e)}"]
            }
    
    def _check_data_isolation(self) -> Dict[str, Any]:
        """Проверка изоляции данных между пользователями"""
        try:
            start_time = time.time()
            
            # Проверяем, что у всех аккаунтов есть user_id
            session = get_session()
            
            total_accounts = session.query(InstagramAccount).count()
            accounts_without_user = session.query(InstagramAccount).filter(
                InstagramAccount.user_id.is_(None)
            ).count()
            
            # Проверяем уникальность user_id среди пользователей
            unique_users = session.query(InstagramAccount.user_id).distinct().count()
            
            session.close()
            
            duration = time.time() - start_time
            
            status = "HEALTHY"
            issues = []
            warnings = []
            
            if accounts_without_user > 0:
                status = "ERROR"
                issues.append(f"Найдено {accounts_without_user} аккаунтов без user_id")
            
            isolation_ratio = (total_accounts - accounts_without_user) / total_accounts * 100 if total_accounts > 0 else 0
            
            if isolation_ratio < 100:
                status = "WARNING"
                warnings.append(f"Изоляция данных: {isolation_ratio:.1f}%")
            
            return {
                "status": status,
                "total_accounts": total_accounts,
                "accounts_without_user": accounts_without_user,
                "unique_users": unique_users,
                "isolation_ratio": isolation_ratio,
                "response_time": duration,
                "issues": issues,
                "warnings": warnings
            }
            
        except Exception as e:
            return {
                "status": "ERROR",
                "error": str(e),
                "issues": [f"Ошибка проверки изоляции данных: {str(e)}"]
            }
    
    def _check_database_health(self) -> Dict[str, Any]:
        """Проверка состояния базы данных"""
        try:
            start_time = time.time()
            
            session = get_session()
            
            # Простые проверки
            accounts_count = session.query(InstagramAccount).count()
            users_count = session.query(TelegramUser).count()
            
            # Проверяем связность данных
            users_with_accounts = session.query(InstagramAccount.user_id).distinct().count()
            
            session.close()
            
            duration = time.time() - start_time
            
            status = "HEALTHY"
            issues = []
            warnings = []
            
            if duration > 5:
                status = "WARNING"
                warnings.append(f"Медленный отклик БД: {duration:.2f}с")
            
            if accounts_count == 0:
                warnings.append("Нет аккаунтов в базе данных")
            
            return {
                "status": status,
                "accounts_count": accounts_count,
                "users_count": users_count,
                "users_with_accounts": users_with_accounts,
                "response_time": duration,
                "issues": issues,
                "warnings": warnings
            }
            
        except Exception as e:
            return {
                "status": "ERROR",
                "error": str(e),
                "issues": [f"Ошибка доступа к базе данных: {str(e)}"]
            }
    
    def _check_orphaned_data(self) -> Dict[str, Any]:
        """Проверка orphaned данных"""
        try:
            start_time = time.time()
            
            from database.user_management import cleanup_orphaned_data
            orphaned_count = cleanup_orphaned_data()
            
            duration = time.time() - start_time
            
            status = "HEALTHY"
            issues = []
            warnings = []
            
            if orphaned_count > 0:
                status = "WARNING"
                warnings.append(f"Найдено {orphaned_count} аккаунтов без привязки к пользователю")
            elif orphaned_count == -1:
                status = "ERROR"
                issues.append("Ошибка проверки orphaned данных")
            
            return {
                "status": status,
                "orphaned_accounts": orphaned_count,
                "response_time": duration,
                "issues": issues,
                "warnings": warnings
            }
            
        except Exception as e:
            return {
                "status": "ERROR",
                "error": str(e),
                "issues": [f"Ошибка проверки orphaned данных: {str(e)}"]
            }
    
    def _analyze_health_status(self, components: Dict) -> Tuple[str, List[str], List[str]]:
        """Анализ общего статуса здоровья"""
        all_issues = []
        all_warnings = []
        
        error_components = 0
        warning_components = 0
        healthy_components = 0
        
        for component_name, component_data in components.items():
            component_status = component_data.get("status", "UNKNOWN")
            
            if component_status == "ERROR":
                error_components += 1
                all_issues.extend(component_data.get("issues", []))
            elif component_status == "WARNING":
                warning_components += 1
                all_warnings.extend(component_data.get("warnings", []))
            elif component_status == "HEALTHY":
                healthy_components += 1
        
        # Определяем общий статус
        if error_components > 0:
            overall_status = "ERROR"
        elif warning_components > 0:
            overall_status = "WARNING"
        else:
            overall_status = "HEALTHY"
        
        return overall_status, all_issues, all_warnings
    
    def _save_to_history(self, health_status: Dict):
        """Сохранить результат в историю"""
        # Ограничиваем размер истории
        if len(self.health_history) >= self.max_history_size:
            self.health_history.pop(0)
        
        # Сокращенная версия для истории
        history_entry = {
            "timestamp": health_status["timestamp"],
            "overall_status": health_status["overall_status"],
            "issues_count": len(health_status.get("issues", [])),
            "warnings_count": len(health_status.get("warnings", [])),
            "statistics": health_status.get("statistics", {})
        }
        
        self.health_history.append(history_entry)
    
    def get_health_summary(self) -> Dict[str, Any]:
        """Получить краткую сводку здоровья системы"""
        with self._lock:
            if not self.health_history:
                return {
                    "status": "UNKNOWN",
                    "message": "Мониторинг еще не выполнялся"
                }
            
            latest = self.health_history[-1]
            
            # Анализ тенденций за последние 10 проверок
            recent_history = self.health_history[-10:]
            error_trend = sum(1 for h in recent_history if h["overall_status"] == "ERROR")
            warning_trend = sum(1 for h in recent_history if h["overall_status"] == "WARNING")
            
            return {
                "current_status": latest["overall_status"],
                "last_check": latest["timestamp"],
                "recent_issues": latest["issues_count"],
                "recent_warnings": latest["warnings_count"],
                "trends": {
                    "error_checks": error_trend,
                    "warning_checks": warning_trend,
                    "total_recent_checks": len(recent_history)
                },
                "monitoring_stats": self.stats.copy(),
                "is_monitoring_active": self.is_running
            }

# Глобальный экземпляр монитора
_global_health_monitor = None

def get_health_monitor() -> UserIsolationHealthMonitor:
    """
    Получить глобальный экземпляр монитора здоровья
    
    Returns:
        UserIsolationHealthMonitor: Глобальный монитор здоровья
    """
    global _global_health_monitor
    
    if _global_health_monitor is None:
        _global_health_monitor = UserIsolationHealthMonitor()
    
    return _global_health_monitor

def quick_health_check() -> Dict[str, Any]:
    """
    Быстрая проверка здоровья системы изоляции
    
    Returns:
        Dict: Результаты быстрой проверки
    """
    monitor = get_health_monitor()
    return monitor.perform_health_check()

def get_health_summary() -> Dict[str, Any]:
    """
    Получить краткую сводку здоровья системы
    
    Returns:
        Dict: Краткая сводка
    """
    monitor = get_health_monitor()
    return monitor.get_health_summary() 
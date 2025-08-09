# -*- coding: utf-8 -*-
"""
Интегрированная система автоматизации аккаунтов
Объединяет: прогрев, ML прогнозирование, мониторинг здоровья
"""

import logging
import time
from typing import Dict, Optional, Tuple, List
from datetime import datetime, timedelta

from instagram.health_monitor import AdvancedHealthMonitor
from instagram.predictive_monitor import PredictiveMonitor
from instagram.improved_account_warmer import ImprovedAccountWarmer
from services.rate_limiter import rate_limiter, ActionType
# from services.instagram_service import instagram_service  # ВРЕМЕННО ОТКЛЮЧЕН
from database.db_manager import get_instagram_account, update_instagram_account

logger = logging.getLogger(__name__)

class AccountAutomationService:
    """Централизованный сервис автоматизации аккаунтов"""
    
    def __init__(self):
        self.health_monitor = AdvancedHealthMonitor()
        self.predictive_monitor = PredictiveMonitor()
        self.warmer = ImprovedAccountWarmer()
        
        # Кэш состояний аккаунтов
        self.account_states = {}
    
    def get_account_status(self, account_id: int) -> Dict:
        """Получить полный статус аккаунта"""
        try:
            account = get_instagram_account(account_id)
            if not account:
                return {"error": "Аккаунт не найден"}
            
            # Получаем все метрики
            health_score = self.health_monitor.calculate_comprehensive_health_score(account_id)
            ban_risk = self.predictive_monitor.calculate_ban_risk_score(account_id)
            recommendations = self.health_monitor.get_health_recommendations(account_id)
            risk_details = self.predictive_monitor.get_risk_mitigation_advice(account_id)
            
            # Получаем статистику из rate limiter
            action_stats = rate_limiter.get_action_stats(account_id)
            
            return {
                "account_id": account_id,
                "username": account.username,
                "health_score": health_score,
                "ban_risk_score": ban_risk,
                "status": self._determine_status(health_score, ban_risk),
                "recommendations": recommendations,
                "risk_mitigation": risk_details,
                "action_stats": action_stats,
                "can_warm": self._can_warm_account(health_score, ban_risk),
                "suggested_actions": self._suggest_actions(health_score, ban_risk)
            }
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения статуса аккаунта {account_id}: {e}")
            return {"error": str(e)}
    
    def _determine_status(self, health_score: int, ban_risk: int) -> str:
        """Определить общий статус аккаунта"""
        if ban_risk > 70:
            return "CRITICAL_RISK"
        elif ban_risk > 50:
            return "HIGH_RISK"
        elif health_score < 30:
            return "UNHEALTHY"
        elif health_score < 50:
            return "NEEDS_ATTENTION"
        elif health_score >= 80 and ban_risk < 20:
            return "EXCELLENT"
        else:
            return "GOOD"
    
    def _can_warm_account(self, health_score: int, ban_risk: int) -> bool:
        """Можно ли прогревать аккаунт"""
        return health_score > 20 and ban_risk < 70
    
    def _suggest_actions(self, health_score: int, ban_risk: int) -> List[str]:
        """Предложить действия для аккаунта"""
        actions = []
        
        if ban_risk > 50:
            actions.append("⚠️ Приостановить всю активность на 48 часов")
            actions.append("🔄 Сменить IP/прокси")
            actions.append("📱 Проверить устройство")
        elif ban_risk > 30:
            actions.append("⏱️ Снизить активность на 50%")
            actions.append("🎯 Фокус на просмотре контента")
        
        if health_score < 50:
            actions.append("🔥 Начать прогрев аккаунта")
            actions.append("📸 Добавить фото профиля и био")
            actions.append("👀 Больше просматривать, меньше действий")
        
        if not actions:
            actions.append("✅ Продолжать текущую стратегию")
            
        return actions
    
    def smart_warm_account(self, account_id: int, duration_minutes: int = 30) -> Tuple[bool, str]:
        """Умный прогрев с учетом всех метрик"""
        try:
            # Проверяем состояние аккаунта
            status = self.get_account_status(account_id)
            
            if status.get("error"):
                return False, status["error"]
            
            if not status["can_warm"]:
                return False, f"Прогрев невозможен: статус {status['status']}"
            
            # Проверяем лимиты
            can_view, reason = rate_limiter.can_perform_action(account_id, ActionType.VIEW_FEED)
            if not can_view:
                return False, f"Достигнуты лимиты: {reason}"
            
            # Адаптируем параметры прогрева
            if status["ban_risk_score"] > 30:
                duration_minutes = min(duration_minutes, 15)  # Сокращаем время
                logger.info(f"⚠️ Высокий риск бана ({status['ban_risk_score']}), сокращаю прогрев до {duration_minutes} минут")
            
            # Запускаем прогрев через существующую систему
            success, message = self.warmer.warm_account_improved(account_id, duration_minutes)
            
            if success:
                # Обновляем статистику
                update_instagram_account(account_id, last_warmup=datetime.now())
                logger.info(f"✅ Прогрев аккаунта {account_id} завершен успешно")
            
            return success, message
            
        except Exception as e:
            logger.error(f"❌ Ошибка при умном прогреве: {e}")
            return False, str(e)
    
    def perform_safe_action(self, account_id: int, action_type: ActionType, action_func, *args, **kwargs):
        """Выполнить действие с проверкой безопасности"""
        try:
            # Проверяем состояние аккаунта
            status = self.get_account_status(account_id)
            
            if status["ban_risk_score"] > 60:
                return False, "Слишком высокий риск бана"
            
            # Проверяем лимиты
            can_perform, reason = rate_limiter.can_perform_action(account_id, action_type)
            if not can_perform:
                return False, reason
            
            # Получаем задержку
            wait_time = rate_limiter.get_wait_time(account_id, action_type)
            logger.info(f"⏱️ Ожидание {wait_time} секунд перед действием {action_type.value}")
            time.sleep(wait_time)
            
            # Выполняем действие
            result = action_func(*args, **kwargs)
            
            # Записываем действие
            rate_limiter.record_action(account_id, action_type)
            
            return True, result
            
        except Exception as e:
            logger.error(f"❌ Ошибка при выполнении безопасного действия: {e}")
            return False, str(e)
    
    def get_daily_recommendations(self) -> Dict[int, List[str]]:
        """Получить рекомендации для всех аккаунтов"""
        from database.db_manager import get_instagram_accounts
        
        recommendations = {}
        accounts = get_instagram_accounts()
        
        for account in accounts:
            if account.is_active:
                status = self.get_account_status(account.id)
                if not status.get("error"):
                    recommendations[account.id] = {
                        "username": account.username,
                        "status": status["status"],
                        "health_score": status["health_score"],
                        "ban_risk": status["ban_risk_score"],
                        "actions": status["suggested_actions"]
                    }
        
        return recommendations
    
    def auto_manage_accounts(self):
        """Автоматическое управление всеми аккаунтами"""
        logger.info("🤖 Запуск автоматического управления аккаунтами")
        
        recommendations = self.get_daily_recommendations()
        
        for account_id, data in recommendations.items():
            logger.info(f"\n📱 Аккаунт: {data['username']}")
            logger.info(f"   Статус: {data['status']}")
            logger.info(f"   Здоровье: {data['health_score']}/100")
            logger.info(f"   Риск бана: {data['ban_risk']}/100")
            
            # Автоматические действия
            if data['status'] == "CRITICAL_RISK":
                # Блокируем все действия на 48 часов
                for action_type in ActionType:
                    rate_limiter.block_action(account_id, action_type, 48 * 3600)
                logger.warning(f"🔒 Аккаунт {data['username']} заблокирован на 48 часов")
                
            elif data['status'] in ["NEEDS_ATTENTION", "UNHEALTHY"] and data['ban_risk'] < 50:
                # Запускаем легкий прогрев
                self.smart_warm_account(account_id, duration_minutes=15)

# Глобальный экземпляр
automation_service = AccountAutomationService() 
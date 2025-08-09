import logging
import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional

from database.db_manager import get_instagram_account, update_instagram_account, get_total_accounts
from services.rate_limiter import rate_limiter, ActionType
from instagram.enhanced_warmup_system import EnhancedWarmupSystem
from instagram.ml_health_predictor import MLHealthPredictor
from instagram.client import get_instagram_client

logger = logging.getLogger(__name__)

class EnhancedAccountAutomation:
    """Улучшенная система автоматизации аккаунтов с ML и AI"""
    
    def __init__(self):
        self.warmup_system = EnhancedWarmupSystem()
        self.ml_predictor = MLHealthPredictor()
        self.active_sessions = {}
        
        logger.info("🚀 Инициализирована улучшенная система автоматизации")
    
    async def run_smart_warmup_campaign(self, account_ids: List[int], duration_minutes: int = 30) -> Dict[str, any]:
        """Запускает умную кампанию прогрева для нескольких аккаунтов"""
        results = {
            'total_accounts': len(account_ids),
            'successful': 0,
            'failed': 0,
            'warnings': 0,
            'detailed_results': {},
            'ml_insights': {}
        }
        
        logger.info(f"🎯 Запуск умной кампании прогрева для {len(account_ids)} аккаунтов")
        
        # Группируем аккаунты по приоритету на основе ML анализа
        account_priorities = await self._analyze_accounts_priority(account_ids)
        
        # Обрабатываем аккаунты по приоритету
        for priority_group in ['high', 'medium', 'low']:
            priority_accounts = account_priorities.get(priority_group, [])
            
            if priority_accounts:
                logger.info(f"📊 Обрабатываю группу приоритета '{priority_group}': {len(priority_accounts)} аккаунтов")
                
                # Параллельная обработка в рамках одной группы приоритета
                group_results = await self._process_accounts_batch(priority_accounts, duration_minutes)
                
                # Объединяем результаты
                for account_id, result in group_results.items():
                    results['detailed_results'][account_id] = result
                    if result['success']:
                        results['successful'] += 1
                    else:
                        results['failed'] += 1
                    if result.get('warnings'):
                        results['warnings'] += 1
                
                # Небольшая пауза между группами
                if priority_group != 'low':
                    await asyncio.sleep(60)  # 1 минута
        
        # Генерируем общие ML инсайты
        results['ml_insights'] = self._generate_campaign_insights(results['detailed_results'])
        
        logger.info(f"✅ Кампания завершена: {results['successful']} успешно, {results['failed']} ошибок, {results['warnings']} предупреждений")
        
        return results
    
    async def _analyze_accounts_priority(self, account_ids: List[int]) -> Dict[str, List[int]]:
        """Анализирует приоритет аккаунтов с помощью ML"""
        priorities = {'high': [], 'medium': [], 'low': []}
        
        for account_id in account_ids:
            try:
                # Получаем ML предсказание
                prediction = self.ml_predictor.predict_account_health(account_id)
                
                # Определяем приоритет на основе health score и risk
                if prediction.health_score > 70 and prediction.ban_risk_score < 30:
                    priorities['high'].append(account_id)
                elif prediction.health_score > 50 and prediction.ban_risk_score < 60:
                    priorities['medium'].append(account_id)
                else:
                    priorities['low'].append(account_id)
                    
            except Exception as e:
                logger.warning(f"Ошибка анализа приоритета для аккаунта {account_id}: {e}")
                priorities['medium'].append(account_id)  # Default
        
        logger.info(f"📈 Приоритеты: High({len(priorities['high'])}), Medium({len(priorities['medium'])}), Low({len(priorities['low'])})")
        return priorities
    
    async def _process_accounts_batch(self, account_ids: List[int], duration_minutes: int) -> Dict[int, Dict]:
        """Обрабатывает батч аккаунтов параллельно"""
        tasks = []
        
        for account_id in account_ids:
            task = asyncio.create_task(self._process_single_account(account_id, duration_minutes))
            tasks.append((account_id, task))
        
        results = {}
        for account_id, task in tasks:
            try:
                result = await task
                results[account_id] = result
            except Exception as e:
                logger.error(f"❌ Ошибка обработки аккаунта {account_id}: {e}")
                results[account_id] = {
                    'success': False,
                    'error': str(e),
                    'warnings': []
                }
        
        return results
    
    async def _process_single_account(self, account_id: int, duration_minutes: int) -> Dict:
        """Обрабатывает один аккаунт с ML анализом"""
        result = {
            'success': False,
            'ml_prediction': None,
            'warmup_metrics': None,
            'recommendations': [],
            'warnings': [],
            'error': None
        }
        
        try:
            # Получаем аккаунт
            account = get_instagram_account(account_id)
            if not account:
                result['error'] = "Аккаунт не найден"
                return result
            
            logger.info(f"🔍 Обрабатываю аккаунт {account.username} (ID: {account_id})")
            
            # ML анализ перед прогревом
            client = get_instagram_client(account_id)
            ml_prediction = self.ml_predictor.predict_account_health(account_id, client)
            result['ml_prediction'] = {
                'health_score': ml_prediction.health_score,
                'ban_risk_score': ml_prediction.ban_risk_score,
                'confidence': ml_prediction.confidence,
                'risk_factors': ml_prediction.risk_factors
            }
            
            # Проверяем критические риски
            if ml_prediction.ban_risk_score > 80:
                result['error'] = f"Слишком высокий риск бана ({ml_prediction.ban_risk_score:.1f})"
                result['warnings'].append("account_too_risky")
                return result
            
            # Адаптируем стратегию на основе ML
            adapted_duration = self._adapt_duration_by_ml(duration_minutes, ml_prediction)
            if adapted_duration != duration_minutes:
                result['warnings'].append(f"duration_adapted_from_{duration_minutes}_to_{adapted_duration}")
            
            # Запускаем интеллектуальный прогрев
            success, message, warmup_metrics = self.warmup_system.start_intelligent_warmup(
                account_id, adapted_duration
            )
            
            result['success'] = success
            result['warmup_metrics'] = {
                'actions_performed': warmup_metrics.actions_performed,
                'likes_given': warmup_metrics.likes_given,
                'stories_viewed': warmup_metrics.stories_viewed,
                'profiles_visited': warmup_metrics.profiles_visited,
                'errors_encountered': warmup_metrics.errors_encountered,
                'session_duration': warmup_metrics.session_duration
            }
            
            if not success:
                result['error'] = message
            
            # Генерируем рекомендации
            result['recommendations'] = ml_prediction.recommendations
            
            # Постобработка ML
            await self._post_warmup_ml_analysis(account_id, warmup_metrics, ml_prediction)
            
            logger.info(f"✅ Аккаунт {account_id} обработан: {success}, действий: {warmup_metrics.actions_performed}")
            
        except Exception as e:
            logger.error(f"❌ Ошибка обработки аккаунта {account_id}: {e}")
            result['error'] = str(e)
        
        return result
    
    def _adapt_duration_by_ml(self, original_duration: int, ml_prediction) -> int:
        """Адаптирует продолжительность прогрева на основе ML"""
        health_score = ml_prediction.health_score
        ban_risk = ml_prediction.ban_risk_score
        
        # Высокий риск - сокращаем время
        if ban_risk > 60:
            return max(10, original_duration // 3)
        elif ban_risk > 40:
            return max(15, original_duration // 2)
        
        # Низкое здоровье - увеличиваем время
        if health_score < 40:
            return min(60, original_duration * 1.5)
        elif health_score < 60:
            return min(45, original_duration * 1.2)
        
        return original_duration
    
    async def _post_warmup_ml_analysis(self, account_id: int, warmup_metrics, ml_prediction):
        """Постобработка ML анализа после прогрева"""
        try:
            # Рассчитываем успешность сессии
            success_rate = (warmup_metrics.actions_performed - warmup_metrics.errors_encountered) / max(warmup_metrics.actions_performed, 1)
            
            # Обновляем ML модель на основе результатов
            if success_rate > 0.8:
                outcome = 'healthy'
            elif warmup_metrics.errors_encountered > 5:
                outcome = 'problematic'
            else:
                outcome = 'normal'
            
            # В будущем здесь можно добавить обучение модели
            logger.info(f"📚 Результат прогрева аккаунта {account_id}: {outcome}, успешность: {success_rate:.1%}")
            
        except Exception as e:
            logger.error(f"❌ Ошибка постобработки ML: {e}")
    
    def _generate_campaign_insights(self, detailed_results: Dict[int, Dict]) -> Dict[str, any]:
        """Генерирует инсайты по всей кампании"""
        insights = {
            'average_health_score': 0,
            'average_ban_risk': 0,
            'common_risk_factors': {},
            'performance_by_health_range': {},
            'recommendations_summary': {}
        }
        
        try:
            total_accounts = len(detailed_results)
            if total_accounts == 0:
                return insights
            
            health_scores = []
            ban_risks = []
            all_risk_factors = []
            all_recommendations = []
            
            # Собираем статистику
            for account_id, result in detailed_results.items():
                if result.get('ml_prediction'):
                    ml_pred = result['ml_prediction']
                    health_scores.append(ml_pred['health_score'])
                    ban_risks.append(ml_pred['ban_risk_score'])
                    all_risk_factors.extend(ml_pred['risk_factors'])
                
                if result.get('recommendations'):
                    all_recommendations.extend(result['recommendations'])
            
            # Средние значения
            if health_scores:
                insights['average_health_score'] = sum(health_scores) / len(health_scores)
                insights['average_ban_risk'] = sum(ban_risks) / len(ban_risks)
            
            # Частота факторов риска
            risk_factor_counts = {}
            for factor in all_risk_factors:
                risk_factor_counts[factor] = risk_factor_counts.get(factor, 0) + 1
            insights['common_risk_factors'] = dict(sorted(risk_factor_counts.items(), key=lambda x: x[1], reverse=True)[:5])
            
            # Частота рекомендаций
            recommendation_counts = {}
            for rec in all_recommendations:
                recommendation_counts[rec] = recommendation_counts.get(rec, 0) + 1
            insights['recommendations_summary'] = dict(sorted(recommendation_counts.items(), key=lambda x: x[1], reverse=True)[:5])
            
            # Производительность по диапазонам здоровья
            health_ranges = {'healthy': [], 'medium': [], 'risky': []}
            for account_id, result in detailed_results.items():
                if result.get('ml_prediction'):
                    health = result['ml_prediction']['health_score']
                    success = result.get('success', False)
                    
                    if health > 70:
                        health_ranges['healthy'].append(success)
                    elif health > 40:
                        health_ranges['medium'].append(success)
                    else:
                        health_ranges['risky'].append(success)
            
            for range_name, successes in health_ranges.items():
                if successes:
                    success_rate = sum(successes) / len(successes)
                    insights['performance_by_health_range'][range_name] = {
                        'count': len(successes),
                        'success_rate': success_rate
                    }
            
        except Exception as e:
            logger.error(f"❌ Ошибка генерации инсайтов: {e}")
        
        return insights
    
    async def monitor_accounts_health(self, account_ids: List[int]) -> Dict[str, any]:
        """Мониторинг здоровья аккаунтов с ML"""
        monitoring_results = {
            'timestamp': datetime.now().isoformat(),
            'total_accounts': len(account_ids),
            'health_distribution': {'healthy': 0, 'medium': 0, 'risky': 0, 'critical': 0},
            'alerts': [],
            'recommendations': {},
            'top_risk_factors': {}
        }
        
        logger.info(f"🔍 Мониторинг здоровья {len(account_ids)} аккаунтов")
        
        all_predictions = {}
        all_risk_factors = []
        
        for account_id in account_ids:
            try:
                prediction = self.ml_predictor.predict_account_health(account_id)
                all_predictions[account_id] = prediction
                
                # Классификация по здоровью
                health_score = prediction.health_score
                ban_risk = prediction.ban_risk_score
                
                if health_score > 80 and ban_risk < 20:
                    monitoring_results['health_distribution']['healthy'] += 1
                elif health_score > 60 and ban_risk < 40:
                    monitoring_results['health_distribution']['medium'] += 1
                elif health_score > 40 and ban_risk < 70:
                    monitoring_results['health_distribution']['risky'] += 1
                else:
                    monitoring_results['health_distribution']['critical'] += 1
                
                # Критические алерты
                if ban_risk > 80:
                    monitoring_results['alerts'].append({
                        'account_id': account_id,
                        'type': 'critical_ban_risk',
                        'value': ban_risk,
                        'message': f"Критический риск бана: {ban_risk:.1f}%"
                    })
                
                if health_score < 30:
                    monitoring_results['alerts'].append({
                        'account_id': account_id,
                        'type': 'low_health',
                        'value': health_score,
                        'message': f"Низкое здоровье аккаунта: {health_score:.1f}%"
                    })
                
                # Собираем факторы риска
                all_risk_factors.extend(prediction.risk_factors)
                
                # Рекомендации для проблемных аккаунтов
                if health_score < 60 or ban_risk > 40:
                    monitoring_results['recommendations'][account_id] = prediction.recommendations
                
            except Exception as e:
                logger.error(f"❌ Ошибка мониторинга аккаунта {account_id}: {e}")
                monitoring_results['alerts'].append({
                    'account_id': account_id,
                    'type': 'monitoring_error',
                    'message': f"Ошибка анализа: {str(e)}"
                })
        
        # Топ факторы риска
        risk_factor_counts = {}
        for factor in all_risk_factors:
            risk_factor_counts[factor] = risk_factor_counts.get(factor, 0) + 1
        monitoring_results['top_risk_factors'] = dict(sorted(risk_factor_counts.items(), key=lambda x: x[1], reverse=True)[:5])
        
        logger.info(f"📊 Мониторинг завершен: {len(monitoring_results['alerts'])} алертов, {len(monitoring_results['recommendations'])} рекомендаций")
        
        return monitoring_results
    
    async def optimize_account_strategy(self, account_id: int) -> Dict[str, any]:
        """Оптимизирует стратегию для конкретного аккаунта"""
        try:
            logger.info(f"🎯 Оптимизация стратегии для аккаунта {account_id}")
            
            # ML анализ
            prediction = self.ml_predictor.predict_account_health(account_id)
            
            # Определяем оптимальную стратегию
            strategy = {
                'account_id': account_id,
                'current_health': prediction.health_score,
                'current_risk': prediction.ban_risk_score,
                'confidence': prediction.confidence,
                'strategy_type': '',
                'recommended_actions': [],
                'timing_recommendations': {},
                'risk_mitigation': []
            }
            
            # Выбираем тип стратегии
            if prediction.health_score > 80 and prediction.ban_risk_score < 20:
                strategy['strategy_type'] = 'aggressive_growth'
                strategy['recommended_actions'] = [
                    'Увеличить активность на 20%',
                    'Добавить новые типы контента',
                    'Расширить целевую аудиторию'
                ]
            elif prediction.health_score > 60 and prediction.ban_risk_score < 40:
                strategy['strategy_type'] = 'stable_growth'
                strategy['recommended_actions'] = [
                    'Поддерживать текущий уровень активности',
                    'Фокус на качестве контента',
                    'Постепенное увеличение engagement'
                ]
            elif prediction.health_score > 40:
                strategy['strategy_type'] = 'recovery_mode'
                strategy['recommended_actions'] = [
                    'Снизить активность на 30%',
                    'Фокус на прогреве',
                    'Улучшить паттерны поведения'
                ]
            else:
                strategy['strategy_type'] = 'emergency_recovery'
                strategy['recommended_actions'] = [
                    'Приостановить всю активность на 48 часов',
                    'Сменить прокси',
                    'Провести полный аудит настроек'
                ]
            
            # Рекомендации по таймингу
            if 'high_api_error_rate' in prediction.risk_factors:
                strategy['timing_recommendations']['api_delays'] = 'Увеличить задержки между запросами в 2 раза'
            
            if 'non_human_patterns' in prediction.risk_factors:
                strategy['timing_recommendations']['behavior'] = 'Добавить больше случайности в действия'
            
            # Митигация рисков
            for risk_factor in prediction.risk_factors:
                if risk_factor == 'frequent_challenges':
                    strategy['risk_mitigation'].append('Использовать residential прокси')
                elif risk_factor == 'excessive_activity':
                    strategy['risk_mitigation'].append('Распределить активность на более длинные периоды')
                elif risk_factor == 'new_account':
                    strategy['risk_mitigation'].append('Провести расширенный прогрев на 7-10 дней')
            
            logger.info(f"✅ Стратегия определена: {strategy['strategy_type']} для аккаунта {account_id}")
            
            return strategy
            
        except Exception as e:
            logger.error(f"❌ Ошибка оптимизации стратегии: {e}")
            return {
                'account_id': account_id,
                'error': str(e),
                'strategy_type': 'fallback_conservative'
            }

# Функция для получения экземпляра системы
def get_enhanced_automation():
    """Возвращает экземпляр улучшенной системы автоматизации"""
    return EnhancedAccountAutomation() 
import logging
import time
import random
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
from database.db_manager import get_instagram_account

logger = logging.getLogger(__name__)

class PredictiveMonitor:
    """Предиктивный анализ рисков банов"""
    
    def __init__(self):
        self.risk_cache = {}
        self.cache_timeout = 600  # 10 минут
        self.activity_patterns = {}
        self.risk_factors = {
            'account_age': 0.25,
            'activity_velocity': 0.30,
            'pattern_anomalies': 0.25,
            'restriction_history': 0.20
        }
    
    def calculate_ban_risk_score(self, account_id: int) -> int:
        """Расчет вероятности бана (0-100)"""
        try:
            # Проверяем кэш
            if self._is_cache_valid(account_id):
                return self.risk_cache[account_id]['score']
            
            account = get_instagram_account(account_id)
            if not account:
                return 50  # Средний риск для неизвестного аккаунта
            
            risk_score = 0
            risk_components = {}
            
            # 1. Фактор возраста аккаунта (25%)
            age_risk = self._calculate_age_risk(account_id)
            risk_components['age_risk'] = age_risk
            risk_score += age_risk * self.risk_factors['account_age']
            
            # 2. Скорость активности (30%)
            velocity_risk = self._calculate_velocity_risk(account_id)
            risk_components['velocity_risk'] = velocity_risk
            risk_score += velocity_risk * self.risk_factors['activity_velocity']
            
            # 3. Аномалии в паттернах (25%)
            pattern_risk = self._calculate_pattern_anomaly_risk(account_id)
            risk_components['pattern_risk'] = pattern_risk
            risk_score += pattern_risk * self.risk_factors['pattern_anomalies']
            
            # 4. История ограничений (20%)
            history_risk = self._calculate_restriction_history_risk(account_id)
            risk_components['history_risk'] = history_risk
            risk_score += history_risk * self.risk_factors['restriction_history']
            
            final_score = max(0, min(100, int(risk_score)))
            
            # Кэшируем результат
            self.risk_cache[account_id] = {
                'score': final_score,
                'timestamp': time.time(),
                'components': risk_components,
                'risk_level': self._determine_risk_level(final_score)
            }
            
            logger.info(f"Риск бана для аккаунта {account_id}: {final_score}/100 ({self._determine_risk_level(final_score)})")
            return final_score
            
        except Exception as e:
            logger.error(f"Ошибка расчета риска бана для аккаунта {account_id}: {e}")
            return 50
    
    def analyze_activity_patterns(self, account_id: int) -> Dict[str, any]:
        """Анализ паттернов активности"""
        try:
            account = get_instagram_account(account_id)
            if not account:
                return {}
            
            # Симуляция анализа паттернов активности
            patterns = {
                'temporal_patterns': {
                    'peak_hours': [9, 12, 15, 18, 21],
                    'activity_distribution': 'normal',
                    'weekend_behavior': 'consistent',
                    'night_activity': 'minimal'
                },
                'action_patterns': {
                    'follow_frequency': 'moderate',
                    'like_consistency': 'high',
                    'comment_variety': 'good',
                    'story_engagement': 'regular'
                },
                'behavioral_patterns': {
                    'automation_detected': False,
                    'human_like_delays': True,
                    'pattern_variance': 'healthy',
                    'suspicious_spikes': False
                },
                'anomaly_indicators': {
                    'unusual_timing': False,
                    'velocity_spikes': False,
                    'repetitive_actions': False,
                    'bot_like_behavior': False
                }
            }
            
            # Сохраняем паттерны
            self.activity_patterns[account_id] = {
                'patterns': patterns,
                'analyzed_at': datetime.now(),
                'sample_period': '7_days'
            }
            
            logger.info(f"Анализ паттернов активности для аккаунта {account.username} завершен")
            return patterns
            
        except Exception as e:
            logger.error(f"Ошибка анализа паттернов для аккаунта {account_id}: {e}")
            return {}
    
    def detect_anomalies(self, account_id: int) -> List[Dict[str, any]]:
        """Детекция аномальных действий"""
        try:
            # Анализируем паттерны
            patterns = self.analyze_activity_patterns(account_id)
            
            anomalies = []
            
            # Проверяем различные типы аномалий
            if patterns.get('anomaly_indicators', {}).get('velocity_spikes'):
                anomalies.append({
                    'type': 'velocity_spike',
                    'severity': 'high',
                    'description': 'Обнаружен резкий скачок активности',
                    'recommendation': 'Снизить интенсивность действий',
                    'detected_at': datetime.now()
                })
            
            if patterns.get('anomaly_indicators', {}).get('repetitive_actions'):
                anomalies.append({
                    'type': 'repetitive_pattern',
                    'severity': 'medium',
                    'description': 'Обнаружены повторяющиеся паттерны',
                    'recommendation': 'Увеличить разнообразие действий',
                    'detected_at': datetime.now()
                })
            
            if patterns.get('anomaly_indicators', {}).get('bot_like_behavior'):
                anomalies.append({
                    'type': 'bot_behavior',
                    'severity': 'critical',
                    'description': 'Поведение похоже на бота',
                    'recommendation': 'Немедленно перейти на ручной режим',
                    'detected_at': datetime.now()
                })
            
            if patterns.get('anomaly_indicators', {}).get('unusual_timing'):
                anomalies.append({
                    'type': 'timing_anomaly',
                    'severity': 'low',
                    'description': 'Необычное время активности',
                    'recommendation': 'Скорректировать расписание',
                    'detected_at': datetime.now()
                })
            
            # Если аномалий не обнаружено
            if not anomalies:
                anomalies.append({
                    'type': 'no_anomalies',
                    'severity': 'info',
                    'description': 'Аномалии не обнаружены',
                    'recommendation': 'Продолжить текущую стратегию',
                    'detected_at': datetime.now()
                })
            
            logger.info(f"Обнаружено аномалий для аккаунта {account_id}: {len([a for a in anomalies if a['type'] != 'no_anomalies'])}")
            return anomalies
            
        except Exception as e:
            logger.error(f"Ошибка детекции аномалий для аккаунта {account_id}: {e}")
            return []
    
    def get_risk_mitigation_advice(self, account_id: int) -> List[str]:
        """Советы по снижению рисков"""
        try:
            risk_score = self.calculate_ban_risk_score(account_id)
            
            if account_id not in self.risk_cache:
                return ["Не удалось получить данные о рисках"]
            
            components = self.risk_cache[account_id]['components']
            advice = []
            
            # Советы на основе компонентов риска
            if components.get('age_risk', 0) > 60:
                advice.extend([
                    "Аккаунт слишком новый - увеличьте период прогрева",
                    "Минимизируйте активность в первые недели",
                    "Сосредоточьтесь на создании качественного контента"
                ])
            
            if components.get('velocity_risk', 0) > 60:
                advice.extend([
                    "Снизьте интенсивность действий",
                    "Увеличьте случайные задержки между действиями",
                    "Распределите активность равномерно по времени"
                ])
            
            if components.get('pattern_risk', 0) > 60:
                advice.extend([
                    "Увеличьте разнообразие в паттернах активности",
                    "Избегайте повторяющихся временных интервалов",
                    "Варьируйте типы взаимодействий"
                ])
            
            if components.get('history_risk', 0) > 60:
                advice.extend([
                    "В прошлом были ограничения - будьте особенно осторожны",
                    "Временно снизьте все виды активности",
                    "Рассмотрите смену стратегии"
                ])
            
            # Общие советы на основе уровня риска
            if risk_score >= 80:
                advice.extend([
                    "КРИТИЧЕСКИЙ РИСК: Немедленно остановите автоматизацию",
                    "Переведите аккаунт в ручной режим",
                    "Проанализируйте последние действия"
                ])
            elif risk_score >= 60:
                advice.extend([
                    "ВЫСОКИЙ РИСК: Значительно снизьте активность",
                    "Увеличьте интервалы между действиями в 2-3 раза"
                ])
            elif risk_score >= 40:
                advice.extend([
                    "СРЕДНИЙ РИСК: Слегка снизьте активность",
                    "Добавьте больше случайности в действия"
                ])
            else:
                advice.extend([
                    "НИЗКИЙ РИСК: Продолжайте текущую стратегию",
                    "Поддерживайте текущий уровень активности"
                ])
            
            # Убираем дубликаты и ограничиваем количество
            advice = list(dict.fromkeys(advice))[:10]
            
            logger.info(f"Сгенерировано {len(advice)} рекомендаций для аккаунта {account_id}")
            return advice
            
        except Exception as e:
            logger.error(f"Ошибка получения советов для аккаунта {account_id}: {e}")
            return ["Ошибка при анализе рисков"]
    
    def _calculate_age_risk(self, account_id: int) -> int:
        """Расчет риска на основе возраста аккаунта"""
        try:
            account = get_instagram_account(account_id)
            if not account:
                return 70
            
            account_age = datetime.now() - account.created_at
            age_days = account_age.days
            
            # Чем новее аккаунт, тем выше риск
            if age_days < 1:
                return 90
            elif age_days < 3:
                return 80
            elif age_days < 7:
                return 70
            elif age_days < 14:
                return 50
            elif age_days < 30:
                return 30
            elif age_days < 90:
                return 20
            else:
                return 10
                
        except Exception as e:
            logger.error(f"Ошибка расчета возрастного риска для аккаунта {account_id}: {e}")
            return 50
    
    def _calculate_velocity_risk(self, account_id: int) -> int:
        """Расчет риска на основе скорости активности"""
        # Симуляция - в реальности анализ логов активности
        return random.randint(10, 40)
    
    def _calculate_pattern_anomaly_risk(self, account_id: int) -> int:
        """Расчет риска на основе аномалий в паттернах"""
        # Симуляция - в реальности ML анализ паттернов
        return random.randint(5, 35)
    
    def _calculate_restriction_history_risk(self, account_id: int) -> int:
        """Расчет риска на основе истории ограничений"""
        try:
            account = get_instagram_account(account_id)
            if not account:
                return 50
            
            # Симуляция - проверяем статус аккаунта
            if not account.is_active:
                return 80  # Высокий риск если аккаунт неактивен
            else:
                return random.randint(5, 25)
                
        except Exception as e:
            logger.error(f"Ошибка расчета исторического риска для аккаунта {account_id}: {e}")
            return 30
    
    def _determine_risk_level(self, score: int) -> str:
        """Определение уровня риска"""
        if score >= 80:
            return "КРИТИЧЕСКИЙ"
        elif score >= 60:
            return "ВЫСОКИЙ"
        elif score >= 40:
            return "СРЕДНИЙ"
        elif score >= 20:
            return "НИЗКИЙ"
        else:
            return "МИНИМАЛЬНЫЙ"
    
    def _is_cache_valid(self, account_id: int) -> bool:
        """Проверка валидности кэша"""
        if account_id not in self.risk_cache:
            return False
        
        cache_time = self.risk_cache[account_id]['timestamp']
        return (time.time() - cache_time) < self.cache_timeout
    
    def get_all_accounts_risk_summary(self) -> Dict[str, any]:
        """Сводка рисков по всем аккаунтам"""
        try:
            from database.db_manager import get_instagram_accounts
            accounts = get_instagram_accounts()
            
            risk_distribution = {
                'КРИТИЧЕСКИЙ': [],
                'ВЫСОКИЙ': [],
                'СРЕДНИЙ': [],
                'НИЗКИЙ': [],
                'МИНИМАЛЬНЫЙ': []
            }
            
            total_score = 0
            
            for account in accounts:
                score = self.calculate_ban_risk_score(account.id)
                level = self._determine_risk_level(score)
                total_score += score
                
                risk_distribution[level].append({
                    'id': account.id,
                    'username': account.username,
                    'score': score
                })
            
            summary = {
                'risk_distribution': risk_distribution,
                'average_risk': int(total_score / len(accounts)) if accounts else 0,
                'total_accounts': len(accounts),
                'high_risk_count': len(risk_distribution['КРИТИЧЕСКИЙ']) + len(risk_distribution['ВЫСОКИЙ']),
                'analysis_timestamp': datetime.now()
            }
            
            logger.info(f"Сводка рисков: средний риск {summary['average_risk']}/100, высокий риск: {summary['high_risk_count']} аккаунтов")
            return summary
            
        except Exception as e:
            logger.error(f"Ошибка получения сводки рисков: {e}")
            return {}
    
    def clear_cache(self, account_id: Optional[int] = None):
        """Очистка кэша"""
        if account_id:
            if account_id in self.risk_cache:
                del self.risk_cache[account_id]
            if account_id in self.activity_patterns:
                del self.activity_patterns[account_id]
        else:
            self.risk_cache.clear()
            self.activity_patterns.clear()
        
        logger.info(f"Кэш predictive monitor очищен для {'всех аккаунтов' if not account_id else f'аккаунта {account_id}'}") 
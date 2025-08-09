import logging
import numpy as np
import json
import os
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
import pickle

logger = logging.getLogger(__name__)

@dataclass
class AccountFeatures:
    """Фичи для ML модели"""
    # Базовые характеристики
    account_age_days: float = 0
    follower_count: float = 0
    following_count: float = 0
    media_count: float = 0
    
    # Активность
    posts_last_week: float = 0
    stories_last_week: float = 0
    likes_given_last_week: float = 0
    comments_last_week: float = 0
    
    # Паттерны поведения
    avg_daily_actions: float = 0
    action_variety_score: float = 0
    timing_consistency: float = 0
    human_behavior_score: float = 0
    
    # API метрики
    api_errors_last_week: float = 0
    challenge_requests_last_week: float = 0
    rate_limit_hits_last_week: float = 0
    response_time_avg: float = 0
    
    # Engagement метрики
    engagement_rate: float = 0
    follower_growth_rate: float = 0
    unfollower_rate: float = 0
    
    # Проксі и безопасность
    proxy_changes_last_month: float = 0
    device_changes_last_month: float = 0
    location_changes: float = 0
    
    def to_array(self) -> np.ndarray:
        """Конвертирует в массив для ML модели"""
        return np.array([
            self.account_age_days, self.follower_count, self.following_count, self.media_count,
            self.posts_last_week, self.stories_last_week, self.likes_given_last_week, self.comments_last_week,
            self.avg_daily_actions, self.action_variety_score, self.timing_consistency, self.human_behavior_score,
            self.api_errors_last_week, self.challenge_requests_last_week, self.rate_limit_hits_last_week, self.response_time_avg,
            self.engagement_rate, self.follower_growth_rate, self.unfollower_rate,
            self.proxy_changes_last_month, self.device_changes_last_month, self.location_changes
        ])

@dataclass 
class MLPrediction:
    """Результат ML предсказания"""
    health_score: float
    ban_risk_score: float
    confidence: float
    risk_factors: List[str]
    recommendations: List[str]
    feature_importance: Dict[str, float]

class SimpleMLModel:
    """Простая ML модель на основе весов (без sklearn)"""
    
    def __init__(self):
        # Веса для health score (настроены эмпирически)
        self.health_weights = np.array([
            0.15,  # account_age_days
            0.08,  # follower_count  
            0.05,  # following_count
            0.07,  # media_count
            0.10,  # posts_last_week
            0.08,  # stories_last_week
            0.06,  # likes_given_last_week
            0.04,  # comments_last_week
            0.12,  # avg_daily_actions
            0.10,  # action_variety_score
            0.08,  # timing_consistency
            0.15,  # human_behavior_score
            -0.20, # api_errors_last_week (негативный)
            -0.25, # challenge_requests_last_week (негативный)
            -0.15, # rate_limit_hits_last_week (негативный)
            -0.05, # response_time_avg (негативный)
            0.12,  # engagement_rate
            0.08,  # follower_growth_rate
            -0.10, # unfollower_rate (негативный)
            -0.08, # proxy_changes_last_month (негативный)
            -0.06, # device_changes_last_month (негативный)
            -0.04  # location_changes (негативный)
        ])
        
        # Веса для ban risk (инвертированы относительно health)
        self.ban_risk_weights = -self.health_weights * 0.8
        self.ban_risk_weights[12:16] *= -2  # Удваиваем влияние ошибок API
        
    def predict_health(self, features: AccountFeatures) -> float:
        """Предсказывает health score"""
        feature_array = features.to_array()
        
        # Нормализуем фичи
        normalized_features = self._normalize_features(feature_array)
        
        # Рассчитываем скор
        raw_score = np.dot(normalized_features, self.health_weights)
        
        # Применяем sigmoid для получения score 0-100
        health_score = 100 / (1 + np.exp(-raw_score))
        
        return max(0, min(100, health_score))
    
    def predict_ban_risk(self, features: AccountFeatures) -> float:
        """Предсказывает ban risk"""
        feature_array = features.to_array()
        normalized_features = self._normalize_features(feature_array)
        
        raw_score = np.dot(normalized_features, self.ban_risk_weights)
        ban_risk = 100 / (1 + np.exp(-raw_score))
        
        return max(0, min(100, ban_risk))
    
    def _normalize_features(self, features: np.ndarray) -> np.ndarray:
        """Простая нормализация фич"""
        # Логарифмическая нормализация для больших чисел
        normalized = features.copy()
        
        # Применяем log(1+x) для положительных значений
        for i, val in enumerate(normalized):
            if val > 0:
                normalized[i] = np.log1p(val)
            else:
                normalized[i] = val
        
        # Стандартизация в диапазон [-1, 1]
        max_val = np.max(np.abs(normalized))
        if max_val > 0:
            normalized = normalized / max_val
            
        return normalized

class MLHealthPredictor:
    """ML система для предсказания здоровья аккаунтов"""
    
    def __init__(self, model_path: str = "data/ml_models"):
        self.model_path = model_path
        self.model = SimpleMLModel()
        self.feature_names = [
            'account_age_days', 'follower_count', 'following_count', 'media_count',
            'posts_last_week', 'stories_last_week', 'likes_given_last_week', 'comments_last_week',
            'avg_daily_actions', 'action_variety_score', 'timing_consistency', 'human_behavior_score',
            'api_errors_last_week', 'challenge_requests_last_week', 'rate_limit_hits_last_week', 'response_time_avg',
            'engagement_rate', 'follower_growth_rate', 'unfollower_rate',
            'proxy_changes_last_month', 'device_changes_last_month', 'location_changes'
        ]
        
        # Кэш для предсказаний
        self.predictions_cache = {}
        
        # Создаем директорию для моделей
        os.makedirs(model_path, exist_ok=True)
        
    def predict_account_health(self, account_id: int, client=None) -> MLPrediction:
        """Основная функция предсказания здоровья аккаунта"""
        try:
            # Проверяем кэш
            if account_id in self.predictions_cache:
                cache_time = self.predictions_cache[account_id].get('timestamp', 0)
                if datetime.now().timestamp() - cache_time < 1800:  # 30 минут
                    return self.predictions_cache[account_id]['prediction']
            
            # Извлекаем фичи
            features = self._extract_account_features(account_id, client)
            
            # Делаем предсказание
            health_score = self.model.predict_health(features)
            ban_risk = self.model.predict_ban_risk(features)
            
            # Рассчитываем confidence
            confidence = self._calculate_confidence(features)
            
            # Определяем факторы риска
            risk_factors = self._identify_risk_factors(features)
            
            # Генерируем рекомендации
            recommendations = self._generate_recommendations(features, health_score, ban_risk)
            
            # Рассчитываем важность фич
            feature_importance = self._calculate_feature_importance(features)
            
            prediction = MLPrediction(
                health_score=health_score,
                ban_risk_score=ban_risk,
                confidence=confidence,
                risk_factors=risk_factors,
                recommendations=recommendations,
                feature_importance=feature_importance
            )
            
            # Кэшируем результат
            self.predictions_cache[account_id] = {
                'prediction': prediction,
                'timestamp': datetime.now().timestamp()
            }
            
            logger.info(f"🤖 ML предсказание для аккаунта {account_id}: Health {health_score:.1f}, Risk {ban_risk:.1f}, Confidence {confidence:.1f}")
            
            return prediction
            
        except Exception as e:
            logger.error(f"❌ Ошибка ML предсказания: {e}")
            # Возвращаем дефолтное предсказание
            return MLPrediction(
                health_score=50.0,
                ban_risk_score=50.0,
                confidence=0.3,
                risk_factors=['insufficient_data'],
                recommendations=['collect_more_data'],
                feature_importance={}
            )
    
    def _extract_account_features(self, account_id: int, client=None) -> AccountFeatures:
        """Извлекает фичи аккаунта для ML модели"""
        from database.db_manager import get_instagram_account
        
        features = AccountFeatures()
        
        try:
            # Получаем базовую информацию из БД
            account = get_instagram_account(account_id)
            if account:
                # Возраст аккаунта
                if account.created_at:
                    features.account_age_days = (datetime.now() - account.created_at).days
                
                # Извлекаем сохраненную статистику (если есть)
                if hasattr(account, 'stats') and account.stats:
                    try:
                        stats = json.loads(account.stats) if isinstance(account.stats, str) else account.stats
                        features.follower_count = stats.get('follower_count', 0)
                        features.following_count = stats.get('following_count', 0)
                        features.media_count = stats.get('media_count', 0)
                        features.engagement_rate = stats.get('engagement_rate', 0)
                    except:
                        pass
            
            # Получаем свежие данные через Instagram API
            if client:
                try:
                    user_info = client.user_info(client.user_id)
                    features.follower_count = user_info.follower_count
                    features.following_count = user_info.following_count
                    features.media_count = user_info.media_count
                    
                    # Анализируем активность
                    activity_features = self._analyze_recent_activity(client)
                    features.posts_last_week = activity_features.get('posts', 0)
                    features.engagement_rate = activity_features.get('engagement_rate', 0)
                    
                except Exception as e:
                    logger.warning(f"Не удалось получить данные через API: {e}")
            
            # Анализируем логи активности (заглушка)
            activity_analysis = self._analyze_activity_logs(account_id)
            features.avg_daily_actions = activity_analysis.get('avg_daily_actions', 0)
            features.action_variety_score = activity_analysis.get('variety_score', 0)
            features.timing_consistency = activity_analysis.get('timing_consistency', 0)
            features.human_behavior_score = activity_analysis.get('human_score', 50)
            
            # Анализируем ошибки API (заглушка)
            error_analysis = self._analyze_api_errors(account_id)
            features.api_errors_last_week = error_analysis.get('errors', 0)
            features.challenge_requests_last_week = error_analysis.get('challenges', 0)
            features.rate_limit_hits_last_week = error_analysis.get('rate_limits', 0)
            features.response_time_avg = error_analysis.get('avg_response_time', 1.0)
            
            logger.info(f"📊 Извлечены фичи для аккаунта {account_id}: возраст {features.account_age_days} дней, подписчиков {features.follower_count}")
            
        except Exception as e:
            logger.error(f"❌ Ошибка извлечения фич: {e}")
        
        return features
    
    def _analyze_recent_activity(self, client) -> Dict[str, float]:
        """Анализирует недавнюю активность аккаунта"""
        try:
            # Получаем последние посты
            recent_media = client.user_medias(client.user_id, amount=20)
            
            # Считаем посты за последнюю неделю
            week_ago = datetime.now() - timedelta(days=7)
            posts_last_week = sum(1 for media in recent_media if media.taken_at >= week_ago)
            
            # Рассчитываем engagement rate
            if recent_media:
                total_engagement = sum(media.like_count + media.comment_count for media in recent_media[:10])
                avg_engagement = total_engagement / len(recent_media[:10])
                follower_count = client.user_info(client.user_id).follower_count
                engagement_rate = (avg_engagement / max(follower_count, 1)) * 100
            else:
                engagement_rate = 0
            
            return {
                'posts': posts_last_week,
                'engagement_rate': engagement_rate
            }
            
        except Exception as e:
            logger.error(f"Ошибка анализа активности: {e}")
            return {'posts': 0, 'engagement_rate': 0}
    
    def _analyze_activity_logs(self, account_id: int) -> Dict[str, float]:
        """Анализирует логи активности аккаунта"""
        # Заглушка - в реальности здесь был бы анализ логов
        return {
            'avg_daily_actions': random.uniform(10, 100),
            'variety_score': random.uniform(30, 90),
            'timing_consistency': random.uniform(40, 85),
            'human_score': random.uniform(50, 95)
        }
    
    def _analyze_api_errors(self, account_id: int) -> Dict[str, float]:
        """Анализирует ошибки API за последнее время"""
        # Заглушка - в реальности здесь был бы анализ логов ошибок
        return {
            'errors': random.uniform(0, 10),
            'challenges': random.uniform(0, 3),
            'rate_limits': random.uniform(0, 5),
            'avg_response_time': random.uniform(0.5, 3.0)
        }
    
    def _calculate_confidence(self, features: AccountFeatures) -> float:
        """Рассчитывает уверенность в предсказании"""
        # Простая метрика основанная на количестве доступных данных
        feature_array = features.to_array()
        non_zero_features = np.count_nonzero(feature_array)
        total_features = len(feature_array)
        
        data_completeness = non_zero_features / total_features
        
        # Учитываем возраст аккаунта (больше данных = больше уверенности)
        age_factor = min(features.account_age_days / 30, 1.0)  # Максимум через 30 дней
        
        confidence = (data_completeness * 0.7 + age_factor * 0.3) * 100
        return max(30, min(95, confidence))  # Ограничиваем в разумных пределах
    
    def _identify_risk_factors(self, features: AccountFeatures) -> List[str]:
        """Определяет факторы риска"""
        risk_factors = []
        
        if features.api_errors_last_week > 5:
            risk_factors.append('high_api_error_rate')
        
        if features.challenge_requests_last_week > 2:
            risk_factors.append('frequent_challenges')
        
        if features.rate_limit_hits_last_week > 3:
            risk_factors.append('rate_limiting_issues')
        
        if features.account_age_days < 7:
            risk_factors.append('new_account')
        
        if features.human_behavior_score < 40:
            risk_factors.append('non_human_patterns')
        
        if features.avg_daily_actions > 200:
            risk_factors.append('excessive_activity')
        
        if features.proxy_changes_last_month > 5:
            risk_factors.append('frequent_proxy_changes')
        
        if features.unfollower_rate > 20:
            risk_factors.append('high_unfollow_rate')
        
        return risk_factors
    
    def _generate_recommendations(self, features: AccountFeatures, health_score: float, ban_risk: float) -> List[str]:
        """Генерирует рекомендации для улучшения здоровья аккаунта"""
        recommendations = []
        
        if ban_risk > 70:
            recommendations.append('Немедленно приостановить активность на 24-48 часов')
            recommendations.append('Проверить и сменить прокси')
        elif ban_risk > 40:
            recommendations.append('Снизить интенсивность активности на 50%')
            recommendations.append('Увеличить задержки между действиями')
        
        if health_score < 50:
            recommendations.append('Провести мягкий прогрев в течение 2-3 дней')
            recommendations.append('Сосредоточиться на просмотре контента')
        
        if features.human_behavior_score < 50:
            recommendations.append('Улучшить паттерны поведения - добавить больше вариативности')
            recommendations.append('Использовать более человекоподобные задержки')
        
        if features.api_errors_last_week > 3:
            recommendations.append('Проверить качество прокси и интернет-соединения')
            recommendations.append('Обновить клиент Instagram API')
        
        if features.engagement_rate < 1:
            recommendations.append('Улучшить качество контента для повышения engagement')
            recommendations.append('Быть более активным в комментариях и лайках')
        
        if not recommendations:
            recommendations.append('Продолжать текущую стратегию - показатели хорошие')
        
        return recommendations
    
    def _calculate_feature_importance(self, features: AccountFeatures) -> Dict[str, float]:
        """Рассчитывает важность каждой фичи"""
        feature_array = features.to_array()
        importance = {}
        
        # Простой расчет важности на основе весов модели
        for i, feature_name in enumerate(self.feature_names):
            weight = abs(self.model.health_weights[i])
            value = abs(feature_array[i])
            importance[feature_name] = weight * (1 + np.log1p(value))
        
        # Нормализуем в проценты
        total_importance = sum(importance.values())
        if total_importance > 0:
            importance = {k: (v / total_importance) * 100 for k, v in importance.items()}
        
        # Возвращаем топ-5 наиболее важных фич
        sorted_importance = sorted(importance.items(), key=lambda x: x[1], reverse=True)
        return dict(sorted_importance[:5])
    
    def train_model_with_feedback(self, account_id: int, actual_outcome: str, features: AccountFeatures):
        """Обучает модель на основе обратной связи (простое обновление весов)"""
        try:
            # Простая схема обновления весов на основе фидбека
            if actual_outcome == 'banned':
                # Увеличиваем веса негативных факторов
                self.model.ban_risk_weights *= 1.01
            elif actual_outcome == 'healthy':
                # Увеличиваем веса позитивных факторов
                self.model.health_weights *= 1.01
            
            logger.info(f"📚 Модель обновлена на основе фидбека для аккаунта {account_id}: {actual_outcome}")
            
            # Сохраняем обновленную модель
            self._save_model()
            
        except Exception as e:
            logger.error(f"❌ Ошибка обучения модели: {e}")
    
    def _save_model(self):
        """Сохраняет модель на диск"""
        try:
            model_file = os.path.join(self.model_path, 'health_predictor_model.pkl')
            with open(model_file, 'wb') as f:
                pickle.dump(self.model, f)
            logger.info("💾 ML модель сохранена")
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения модели: {e}")
    
    def _load_model(self):
        """Загружает модель с диска"""
        try:
            model_file = os.path.join(self.model_path, 'health_predictor_model.pkl')
            if os.path.exists(model_file):
                with open(model_file, 'rb') as f:
                    self.model = pickle.load(f)
                logger.info("📂 ML модель загружена")
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки модели: {e}")

# Простой random для совместимости
import random

# Функция для интеграции с существующей системой
def get_ml_health_predictor():
    """Возвращает экземпляр ML предиктора здоровья"""
    return MLHealthPredictor() 
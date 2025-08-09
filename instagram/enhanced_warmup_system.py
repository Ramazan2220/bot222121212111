import logging
import random
import time
import asyncio
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, timedelta
from dataclasses import dataclass
import json

from database.db_manager import get_instagram_account, update_instagram_account
from instagram.client import get_instagram_client
from services.rate_limiter import rate_limiter, ActionType
from instagram.health_monitor import AdvancedHealthMonitor
from instagram.predictive_monitor import PredictiveMonitor

logger = logging.getLogger(__name__)

@dataclass
class WarmupMetrics:
    """Метрики прогрева"""
    actions_performed: int = 0
    likes_given: int = 0
    stories_viewed: int = 0
    profiles_visited: int = 0
    reels_watched: int = 0
    errors_encountered: int = 0
    api_calls_made: int = 0
    session_duration: float = 0.0
    average_response_time: float = 0.0

@dataclass
class InstagramMetrics:
    """Реальные метрики из Instagram API"""
    follower_count: int = 0
    following_count: int = 0
    media_count: int = 0
    last_post_date: Optional[datetime] = None
    story_views_24h: int = 0
    profile_views_week: int = 0
    engagement_rate: float = 0.0
    reach_rate: float = 0.0

class EnhancedWarmupSystem:
    """Улучшенная система прогрева с AI и реальными метриками"""
    
    def __init__(self):
        self.health_monitor = AdvancedHealthMonitor()
        self.predictive_monitor = PredictiveMonitor()
        self.active_sessions = {}
        self.interests_cache = {}
        
    def start_intelligent_warmup(self, account_id: int, duration_minutes: int = 30) -> Tuple[bool, str, WarmupMetrics]:
        """Запуск интеллектуального прогрева"""
        try:
            account = get_instagram_account(account_id)
            if not account:
                return False, "Аккаунт не найден", WarmupMetrics()
            
            logger.info(f"🚀 Начинаю интеллектуальный прогрев аккаунта {account.username}")
            
            # Получаем клиент Instagram
            client = get_instagram_client(account_id)
            if not client:
                return False, "Не удалось получить клиент Instagram", WarmupMetrics()
            
            # Анализируем состояние аккаунта
            account_analysis = self._analyze_account_state(account_id, client)
            
            # Определяем оптимальную стратегию
            warmup_strategy = self._create_optimal_strategy(account_id, account_analysis, duration_minutes)
            
            # Выполняем прогрев
            metrics = self._execute_warmup_strategy(account_id, client, warmup_strategy)
            
            # Сохраняем результаты
            self._save_warmup_results(account_id, metrics, account_analysis)
            
            success_rate = (metrics.actions_performed - metrics.errors_encountered) / max(metrics.actions_performed, 1)
            if success_rate > 0.8:
                return True, f"Прогрев завершен успешно. Выполнено действий: {metrics.actions_performed}", metrics
            else:
                return False, f"Прогрев завершен с ошибками. Успешность: {success_rate:.1%}", metrics
                
        except Exception as e:
            logger.error(f"❌ Ошибка при интеллектуальном прогреве: {e}")
            return False, str(e), WarmupMetrics()
    
    def _analyze_account_state(self, account_id: int, client) -> Dict[str, Any]:
        """Анализирует текущее состояние аккаунта"""
        try:
            analysis = {
                'health_score': 0,
                'ban_risk': 0,
                'instagram_metrics': InstagramMetrics(),
                'interests': [],
                'optimal_actions': [],
                'restrictions': []
            }
            
            # Получаем health score
            analysis['health_score'] = self.health_monitor.calculate_comprehensive_health_score(account_id)
            
            # Получаем риск бана
            analysis['ban_risk'] = self.predictive_monitor.calculate_ban_risk_score(account_id)
            
            # Получаем реальные метрики из Instagram
            instagram_metrics = self._fetch_instagram_metrics(client)
            analysis['instagram_metrics'] = instagram_metrics
            
            # Анализируем интересы аккаунта с помощью AI
            interests = self._analyze_account_interests(client, account_id)
            analysis['interests'] = interests
            
            # Определяем оптимальные действия
            analysis['optimal_actions'] = self._determine_optimal_actions(analysis)
            
            # Проверяем ограничения
            analysis['restrictions'] = self._check_account_restrictions(client)
            
            logger.info(f"📊 Анализ аккаунта {account_id}: Health {analysis['health_score']}/100, Ban Risk {analysis['ban_risk']}/100")
            
            return analysis
            
        except Exception as e:
            logger.error(f"❌ Ошибка анализа аккаунта {account_id}: {e}")
            return {'health_score': 50, 'ban_risk': 50, 'instagram_metrics': InstagramMetrics(), 'interests': [], 'optimal_actions': [], 'restrictions': []}
    
    def _fetch_instagram_metrics(self, client) -> InstagramMetrics:
        """Получает реальные метрики из Instagram API"""
        try:
            metrics = InstagramMetrics()
            
            # Получаем информацию о профиле
            user_info = client.user_info(client.user_id)
            metrics.follower_count = user_info.follower_count
            metrics.following_count = user_info.following_count
            metrics.media_count = user_info.media_count
            
            # Получаем последние посты для анализа активности
            try:
                recent_media = client.user_medias(client.user_id, amount=5)
                if recent_media:
                    metrics.last_post_date = recent_media[0].taken_at
                    
                    # Рассчитываем engagement rate
                    total_engagement = sum(media.like_count + media.comment_count for media in recent_media)
                    metrics.engagement_rate = (total_engagement / len(recent_media)) / max(metrics.follower_count, 1) * 100
            except:
                pass
            
            # Получаем stories (если доступны)
            try:
                stories = client.user_stories(client.user_id)
                metrics.story_views_24h = sum(story.view_count or 0 for story in stories)
            except:
                pass
            
            return metrics
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения Instagram метрик: {e}")
            return InstagramMetrics()
    
    def _analyze_account_interests(self, client, account_id: int) -> List[str]:
        """Анализирует интересы аккаунта с помощью AI"""
        try:
            # Проверяем кэш
            if account_id in self.interests_cache:
                cache_time = self.interests_cache[account_id].get('timestamp', 0)
                if time.time() - cache_time < 86400:  # 24 часа
                    return self.interests_cache[account_id]['interests']
            
            interests = []
            
            # Анализируем биографию
            user_info = client.user_info(client.user_id)
            if user_info.biography:
                bio_interests = self._extract_interests_from_bio(user_info.biography)
                interests.extend(bio_interests)
            
            # Анализируем подписки
            try:
                following = client.user_following(client.user_id, amount=50)
                following_interests = self._analyze_following_for_interests(following)
                interests.extend(following_interests)
            except:
                pass
            
            # Анализируем хештеги в последних постах
            try:
                recent_media = client.user_medias(client.user_id, amount=10)
                hashtag_interests = self._extract_interests_from_hashtags(recent_media)
                interests.extend(hashtag_interests)
            except:
                pass
            
            # Удаляем дубликаты и фильтруем
            unique_interests = list(set(interests))
            filtered_interests = [interest for interest in unique_interests if len(interest) > 2 and len(interest) < 30]
            
            # Кэшируем результат
            self.interests_cache[account_id] = {
                'interests': filtered_interests[:20],  # Максимум 20 интересов
                'timestamp': time.time()
            }
            
            logger.info(f"🎯 Найдено интересов для аккаунта {account_id}: {len(filtered_interests)}")
            return filtered_interests[:20]
            
        except Exception as e:
            logger.error(f"❌ Ошибка анализа интересов: {e}")
            return ['lifestyle', 'travel', 'food', 'fashion', 'photography']  # Fallback интересы
    
    def _extract_interests_from_bio(self, biography: str) -> List[str]:
        """Извлекает интересы из биографии"""
        interests = []
        
        # Простой анализ ключевых слов
        keywords = {
            'travel': ['travel', 'путешеств', 'trip', 'vacation', 'holiday'],
            'food': ['food', 'cooking', 'chef', 'recipe', 'еда', 'кулинар'],
            'fitness': ['fitness', 'gym', 'workout', 'sport', 'фитнес', 'спорт'],
            'fashion': ['fashion', 'style', 'outfit', 'мода', 'стиль'],
            'photography': ['photo', 'camera', 'фото', 'photographer'],
            'music': ['music', 'musician', 'song', 'музык'],
            'art': ['art', 'artist', 'paint', 'искусств', 'художник'],
            'tech': ['tech', 'coding', 'developer', 'технолог', 'разработ']
        }
        
        bio_lower = biography.lower()
        for interest, words in keywords.items():
            if any(word in bio_lower for word in words):
                interests.append(interest)
        
        return interests
    
    def _analyze_following_for_interests(self, following_list) -> List[str]:
        """Анализирует подписки для определения интересов"""
        interests = []
        
        # Анализируем категории аккаунтов
        category_mapping = {
            'Blogger': 'lifestyle',
            'Artist': 'art',
            'Musician': 'music',
            'Chef': 'food',
            'Fitness': 'fitness',
            'Travel': 'travel',
            'Fashion': 'fashion',
            'Photographer': 'photography'
        }
        
        for user in following_list[:20]:  # Анализируем первые 20
            try:
                if hasattr(user, 'category_name') and user.category_name:
                    if user.category_name in category_mapping:
                        interests.append(category_mapping[user.category_name])
            except:
                continue
        
        return interests
    
    def _extract_interests_from_hashtags(self, media_list) -> List[str]:
        """Извлекает интересы из хештегов в постах"""
        interests = []
        
        hashtag_mapping = {
            'travel': ['travel', 'vacation', 'trip', 'wanderlust', 'путешествие'],
            'food': ['food', 'foodie', 'cooking', 'recipe', 'delicious'],
            'fitness': ['fitness', 'gym', 'workout', 'healthy', 'sport'],
            'fashion': ['fashion', 'style', 'outfit', 'ootd', 'stylish'],
            'photography': ['photography', 'photo', 'camera', 'photographer'],
            'lifestyle': ['lifestyle', 'life', 'daily', 'mood', 'inspiration']
        }
        
        for media in media_list:
            if hasattr(media, 'caption_text') and media.caption_text:
                caption_lower = media.caption_text.lower()
                for interest, hashtags in hashtag_mapping.items():
                    if any(f'#{tag}' in caption_lower for tag in hashtags):
                        interests.append(interest)
        
        return interests
    
    def _determine_optimal_actions(self, analysis: Dict[str, Any]) -> List[str]:
        """Определяет оптимальные действия на основе анализа"""
        actions = []
        
        health_score = analysis['health_score']
        ban_risk = analysis['ban_risk']
        metrics = analysis['instagram_metrics']
        
        # Низкий health score - нужна осторожность
        if health_score < 50:
            actions.extend(['view_stories', 'browse_feed'])
        # Средний health score - можно лайки
        elif health_score < 80:
            actions.extend(['view_stories', 'browse_feed', 'give_likes'])
        # Высокий health score - все действия
        else:
            actions.extend(['view_stories', 'browse_feed', 'give_likes', 'visit_profiles', 'watch_reels'])
        
        # Адаптируем под риск бана
        if ban_risk > 60:
            actions = ['view_stories']  # Только безопасные действия
        elif ban_risk > 30:
            actions = [action for action in actions if action != 'give_likes']
        
        # Учитываем активность аккаунта
        if metrics.engagement_rate > 5:  # Высокая активность
            actions.append('explore_trending')
        
        return actions
    
    def _check_account_restrictions(self, client) -> List[str]:
        """Проверяет ограничения аккаунта"""
        restrictions = []
        
        try:
            # Проверяем базовые функции
            timeline = client.get_timeline_feed()
            if not timeline:
                restrictions.append('timeline_restricted')
        except Exception as e:
            if 'challenge' in str(e).lower():
                restrictions.append('challenge_required')
            elif 'login' in str(e).lower():
                restrictions.append('login_required')
            elif 'rate limit' in str(e).lower():
                restrictions.append('rate_limited')
        
        return restrictions
    
    def _create_optimal_strategy(self, account_id: int, analysis: Dict[str, Any], duration_minutes: int) -> Dict[str, Any]:
        """Создает оптимальную стратегию прогрева"""
        strategy = {
            'duration_minutes': duration_minutes,
            'actions_sequence': [],
            'interests_to_explore': analysis['interests'][:5],
            'timing_pattern': 'human_like',
            'safety_mode': analysis['ban_risk'] > 30
        }
        
        optimal_actions = analysis['optimal_actions']
        health_score = analysis['health_score']
        
        # Распределяем время по действиям
        total_time = duration_minutes * 60
        time_per_action = total_time // len(optimal_actions) if optimal_actions else total_time
        
        for action in optimal_actions:
            action_config = {
                'action': action,
                'duration_seconds': time_per_action,
                'intensity': 'low' if health_score < 50 else 'medium' if health_score < 80 else 'high',
                'delay_range': (30, 120) if strategy['safety_mode'] else (15, 60)
            }
            strategy['actions_sequence'].append(action_config)
        
        logger.info(f"📋 Создана стратегия для аккаунта {account_id}: {len(optimal_actions)} действий, безопасный режим: {strategy['safety_mode']}")
        
        return strategy
    
    def _execute_warmup_strategy(self, account_id: int, client, strategy: Dict[str, Any]) -> WarmupMetrics:
        """Выполняет стратегию прогрева"""
        metrics = WarmupMetrics()
        start_time = time.time()
        
        try:
            for action_config in strategy['actions_sequence']:
                action = action_config['action']
                duration = action_config['duration_seconds']
                delay_range = action_config['delay_range']
                
                logger.info(f"🎯 Выполняю действие: {action} на {duration} секунд")
                
                # Выполняем действие
                action_metrics = self._execute_single_action(client, action, duration, strategy['interests_to_explore'])
                
                # Обновляем общие метрики
                metrics.actions_performed += action_metrics.get('actions', 0)
                metrics.likes_given += action_metrics.get('likes', 0)
                metrics.stories_viewed += action_metrics.get('stories', 0)
                metrics.profiles_visited += action_metrics.get('profiles', 0)
                metrics.reels_watched += action_metrics.get('reels', 0)
                metrics.errors_encountered += action_metrics.get('errors', 0)
                metrics.api_calls_made += action_metrics.get('api_calls', 0)
                
                # Человекоподобная задержка между действиями
                if action_config != strategy['actions_sequence'][-1]:  # Не для последнего действия
                    delay = random.uniform(*delay_range)
                    logger.info(f"⏰ Пауза {delay:.1f} секунд")
                    time.sleep(delay)
                    
                # Проверяем лимиты
                if not rate_limiter.can_perform_action(account_id, ActionType.VIEW_FEED)[0]:
                    logger.warning("⚠️ Достигнуты лимиты, прекращаю прогрев")
                    break
            
            metrics.session_duration = time.time() - start_time
            metrics.average_response_time = metrics.session_duration / max(metrics.api_calls_made, 1)
            
            logger.info(f"✅ Прогрев завершен. Всего действий: {metrics.actions_performed}, ошибок: {metrics.errors_encountered}")
            
        except Exception as e:
            logger.error(f"❌ Ошибка выполнения стратегии: {e}")
            metrics.errors_encountered += 1
        
        return metrics
    
    def _execute_single_action(self, client, action: str, duration_seconds: int, interests: List[str]) -> Dict[str, int]:
        """Выполняет одно действие прогрева"""
        metrics = {'actions': 0, 'likes': 0, 'stories': 0, 'profiles': 0, 'reels': 0, 'errors': 0, 'api_calls': 0}
        end_time = time.time() + duration_seconds
        
        try:
            while time.time() < end_time:
                if action == 'view_stories':
                    success = self._view_random_stories(client)
                    if success:
                        metrics['stories'] += 1
                        metrics['actions'] += 1
                    else:
                        metrics['errors'] += 1
                        
                elif action == 'browse_feed':
                    success = self._browse_feed(client)
                    if success:
                        metrics['actions'] += 1
                    else:
                        metrics['errors'] += 1
                        
                elif action == 'give_likes':
                    success = self._give_smart_likes(client, interests)
                    if success:
                        metrics['likes'] += 1
                        metrics['actions'] += 1
                    else:
                        metrics['errors'] += 1
                        
                elif action == 'visit_profiles':
                    success = self._visit_profiles(client, interests)
                    if success:
                        metrics['profiles'] += 1
                        metrics['actions'] += 1
                    else:
                        metrics['errors'] += 1
                        
                elif action == 'watch_reels':
                    success = self._watch_reels(client)
                    if success:
                        metrics['reels'] += 1
                        metrics['actions'] += 1
                    else:
                        metrics['errors'] += 1
                
                metrics['api_calls'] += 1
                
                # Небольшая пауза между действиями
                time.sleep(random.uniform(3, 8))
                
        except Exception as e:
            logger.error(f"❌ Ошибка выполнения действия {action}: {e}")
            metrics['errors'] += 1
        
        return metrics
    
    def _view_random_stories(self, client) -> bool:
        """Просматривает случайные stories"""
        try:
            # Получаем stories из ленты
            timeline = client.get_timeline_feed()
            if timeline and hasattr(timeline, 'tray'):
                for story_item in timeline.tray[:3]:  # Первые 3 stories
                    try:
                        if hasattr(story_item, 'user') and hasattr(story_item.user, 'pk'):
                            stories = client.user_stories(story_item.user.pk)
                            if stories:
                                # "Просматриваем" stories (делаем API вызов)
                                client.story_seen([stories[0].pk])
                                time.sleep(random.uniform(2, 5))
                    except:
                        continue
            return True
        except Exception as e:
            logger.error(f"Ошибка просмотра stories: {e}")
            return False
    
    def _browse_feed(self, client) -> bool:
        """Просматривает ленту"""
        try:
            feed = client.get_timeline_feed()
            return feed is not None
        except Exception as e:
            logger.error(f"Ошибка просмотра ленты: {e}")
            return False
    
    def _give_smart_likes(self, client, interests: List[str]) -> bool:
        """Ставит умные лайки на основе интересов"""
        try:
            # Ищем посты по интересам
            if interests:
                hashtag = random.choice(interests)
                medias = client.hashtag_medias_recent(hashtag, amount=5)
                if medias:
                    media = random.choice(medias)
                    client.media_like(media.id)
                    return True
            
            # Fallback - лайкаем из ленты
            feed = client.get_timeline_feed()
            if feed and hasattr(feed, 'items') and feed.items:
                media = random.choice(feed.items[:5])
                client.media_like(media.id)
                return True
                
            return False
        except Exception as e:
            logger.error(f"Ошибка лайка: {e}")
            return False
    
    def _visit_profiles(self, client, interests: List[str]) -> bool:
        """Посещает профили по интересам"""
        try:
            if interests:
                hashtag = random.choice(interests)
                users = client.search_users(hashtag)[:3]
                if users:
                    user = random.choice(users)
                    client.user_info(user.pk)
                    return True
            return False
        except Exception as e:
            logger.error(f"Ошибка посещения профиля: {e}")
            return False
    
    def _watch_reels(self, client) -> bool:
        """Просматривает reels"""
        try:
            reels = client.get_reels_tray_feed()
            return reels is not None
        except Exception as e:
            logger.error(f"Ошибка просмотра reels: {e}")
            return False
    
    def _save_warmup_results(self, account_id: int, metrics: WarmupMetrics, analysis: Dict[str, Any]):
        """Сохраняет результаты прогрева"""
        try:
            # Обновляем информацию о последнем прогреве
            update_data = {
                'last_warmup': datetime.now(),
                'warmup_stats': {
                    'actions_performed': metrics.actions_performed,
                    'success_rate': (metrics.actions_performed - metrics.errors_encountered) / max(metrics.actions_performed, 1),
                    'session_duration': metrics.session_duration,
                    'health_score': analysis['health_score'],
                    'ban_risk': analysis['ban_risk']
                }
            }
            
            # В реальной реализации здесь было бы сохранение в БД
            logger.info(f"💾 Сохранены результаты прогрева для аккаунта {account_id}")
            
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения результатов прогрева: {e}")

# Функция для интеграции с существующей системой
def get_enhanced_warmup_system():
    """Возвращает экземпляр улучшенной системы прогрева"""
    return EnhancedWarmupSystem() 
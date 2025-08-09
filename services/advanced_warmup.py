import logging
import random
import time
import json
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, timedelta
from dataclasses import dataclass
from enum import Enum

from instagrapi import Client
from instagrapi.exceptions import (
    ClientError, UserNotFound, MediaNotFound,
    LoginRequired, ChallengeRequired
)

from services.anti_detection import anti_detection
from services.rate_limiter import rate_limiter, ActionType
from database.db_manager import get_instagram_account, update_instagram_account

logger = logging.getLogger(__name__)


class WarmupStrategy(Enum):
    """Стратегии прогрева"""
    BABY = "baby"  # Новый аккаунт (0-7 дней)
    CHILD = "child"  # Молодой аккаунт (7-30 дней)  
    TEEN = "teen"  # Подросток (30-90 дней)
    ADULT = "adult"  # Взрослый (90+ дней)
    CUSTOM = "custom"  # Кастомная стратегия


@dataclass(frozen=False)
class WarmupSession:
    """Сессия прогрева"""
    account_id: int
    strategy: WarmupStrategy
    duration_minutes: int
    interests: List[str]
    start_time: datetime
    actions_performed: Dict[str, int]
    errors: List[str]
    time_pattern_used: bool = False  # Добавляем флаг использования временного паттерна
    

class AdvancedWarmupService:
    """
    Продвинутая система прогрева аккаунтов Instagram
    с использованием всех возможностей instagrapi
    """
    
    # Временные паттерны поведения
    TIME_PATTERNS = {
        "morning": {  # 6:00-9:00
            "hours": range(6, 9),
            "duration": (5, 15),
            "actions": ["check_stories", "quick_feed_scroll", "check_notifications"],
            "intensity": 0.3,  # 30% от обычной активности
            "likes_ratio": 0.2,
            "story_views_ratio": 0.6
        },
        "lunch": {  # 12:00-14:00
            "hours": range(12, 14),
            "duration": (10, 25),
            "actions": ["feed_scroll", "reels", "explore", "save_posts"],
            "intensity": 0.6,
            "likes_ratio": 0.4,
            "reels_ratio": 0.7
        },
        "evening": {  # 18:00-22:00
            "hours": range(18, 22),
            "duration": (20, 45),
            "actions": ["all_features"],
            "intensity": 1.0,  # Полная активность
            "likes_ratio": 0.5,
            "comments_ratio": 0.2,
            "save_ratio": 0.3
        },
        "night": {  # 22:00-6:00
            "hours": list(range(22, 24)) + list(range(0, 6)),
            "duration": (5, 10),
            "actions": ["quick_stories", "minimal_activity"],
            "intensity": 0.2,
            "story_views_ratio": 0.8
        }
    }
    
    # Настройки поведения по стратегиям
    STRATEGY_CONFIGS = {
        WarmupStrategy.BABY: {
            "actions_per_hour": (10, 20),
            "likes_ratio": 0.3,
            "follows_ratio": 0.1,
            "comments_ratio": 0.0,
            "story_views_ratio": 0.4,
            "explore_ratio": 0.2,
            "session_duration": (5, 15),  # минуты
            "sessions_per_day": (3, 5),
            "scroll_depth": (3, 7),  # посты
            "read_time": (2, 5)  # секунды на пост
        },
        WarmupStrategy.CHILD: {
            "actions_per_hour": (20, 40),
            "likes_ratio": 0.4,
            "follows_ratio": 0.15,
            "comments_ratio": 0.05,
            "story_views_ratio": 0.3,
            "explore_ratio": 0.1,
            "session_duration": (10, 30),
            "sessions_per_day": (4, 6),
            "scroll_depth": (5, 15),
            "read_time": (3, 8)
        },
        WarmupStrategy.TEEN: {
            "actions_per_hour": (40, 80),
            "likes_ratio": 0.4,
            "follows_ratio": 0.2,
            "comments_ratio": 0.1,
            "story_views_ratio": 0.2,
            "explore_ratio": 0.1,
            "session_duration": (20, 45),
            "sessions_per_day": (5, 8),
            "scroll_depth": (10, 25),
            "read_time": (2, 6)
        },
        WarmupStrategy.ADULT: {
            "actions_per_hour": (80, 150),
            "likes_ratio": 0.35,
            "follows_ratio": 0.25,
            "comments_ratio": 0.15,
            "story_views_ratio": 0.15,
            "explore_ratio": 0.1,
            "session_duration": (30, 60),
            "sessions_per_day": (6, 10),
            "scroll_depth": (20, 50),
            "read_time": (1, 5)
        }
    }
    
    def __init__(self):
        self.rate_limiter = rate_limiter
        self.anti_detection = anti_detection
        self.active_sessions: Dict[int, WarmupSession] = {}
        self.stop_callback = None  # Добавляем callback для проверки остановки
        
    def determine_strategy(self, account_id: int) -> WarmupStrategy:
        """Определить стратегию прогрева на основе возраста аккаунта"""
        try:
            account = get_instagram_account(account_id)
            if not account or not account.created_at:
                return WarmupStrategy.BABY
                
            age_days = (datetime.now() - account.created_at).days
            
            if age_days < 7:
                return WarmupStrategy.BABY
            elif age_days < 30:
                return WarmupStrategy.CHILD
            elif age_days < 90:
                return WarmupStrategy.TEEN
            else:
                return WarmupStrategy.ADULT
                
        except Exception as e:
            logger.error(f"Ошибка определения стратегии: {e}")
            return WarmupStrategy.BABY
    
    def create_instagram_client(self, account_id: int) -> Optional[Client]:
        """Создать клиент Instagram с правильными настройками"""
        try:
            # from services.instagram_service import instagram_service  # ВРЕМЕННО ОТКЛЮЧЕН
            from instagram.client import login_with_session
            from utils.smart_validator_service import validate_before_use, ValidationPriority
            
            # Сначала проверяем и восстанавливаем аккаунт если нужно
            logger.info(f"🔍 Проверка валидности аккаунта ID:{account_id} перед прогревом")
            if not validate_before_use(account_id, ValidationPriority.CRITICAL):
                logger.error(f"❌ Аккаунт ID:{account_id} невалиден или не может быть восстановлен")
                return None
            
            logger.info(f"✅ Аккаунт ID:{account_id} валиден, продолжаем вход")
            
            # Получаем аккаунт
            account = get_instagram_account(account_id)
            if not account:
                return None
                
            # Расшифровываем пароль
            # password = instagram_service.get_decrypted_password(account_id)  # ВРЕМЕННО ОТКЛЮЧЕН
            password = account.password  # Используем пароль напрямую
            
            # Добавляем аккаунт в кэш для автоматической обработки верификации
            from instagram.client_patch import add_account_to_cache
            add_account_to_cache(
                account_id,
                account.username,
                account.email,
                account.email_password
            )
            
            # Используем готовую функцию login_with_session с IMAP поддержкой
            cl = login_with_session(
                username=account.username,
                password=password, 
                account_id=account_id,
                email=account.email,
                email_password=account.email_password
            )
            
            if cl:
                logger.info(f"✅ Успешный вход для @{account.username}")
                
                # ВАЖНО: Сохраняем сессию сразу после успешного входа!
                try:
                    # Используем встроенный метод instagrapi для сохранения сессии
                    cl.dump_settings(f"data/accounts/{account_id}/session.json")
                    logger.info(f"💾 Сессия сохранена для @{account.username}")
                except Exception as e:
                    logger.warning(f"⚠️ Не удалось сохранить сессию: {e}")
                
                return cl
            else:
                logger.error(f"❌ Не удалось войти для @{account.username}")
                return None
                
        except Exception as e:
            logger.error(f"❌ Ошибка создания клиента: {str(e)}")
            return None
    
    def simulate_human_scroll(self, posts: List[Any], config: Dict) -> List[Tuple[Any, float]]:
        """
        Симулировать человеческий скроллинг ленты
        Возвращает список (пост, время_просмотра)
        """
        viewed_posts = []
        scroll_depth = random.randint(*config["scroll_depth"])
        
        for i, post in enumerate(posts[:scroll_depth]):
            # Время просмотра зависит от типа контента
            base_time = random.uniform(*config["read_time"])
            
            # Видео смотрим дольше
            if hasattr(post, 'media_type') and post.media_type == 2:  # Video
                base_time *= random.uniform(2, 4)
            
            # Карусели тоже дольше  
            elif hasattr(post, 'media_type') and post.media_type == 8:  # Carousel
                base_time *= random.uniform(1.5, 2.5)
            
            # Иногда останавливаемся подольше (заинтересовались)
            if random.random() < 0.15:
                base_time *= random.uniform(2, 5)
            
            # Иногда быстро пролистываем
            elif random.random() < 0.3:
                base_time *= random.uniform(0.3, 0.7)
            
            viewed_posts.append((post, base_time))
            
            # Иногда возвращаемся к предыдущему посту
            if i > 0 and random.random() < 0.1:
                prev_post, prev_time = viewed_posts[i-1]
                extra_time = random.uniform(1, 3)
                viewed_posts[i-1] = (prev_post, prev_time + extra_time)
        
        return viewed_posts
    
    def perform_warmup_actions(
        self, 
        cl: Client, 
        session: WarmupSession,
        config: Dict
    ) -> Dict[str, int]:
        """Выполнить действия прогрева"""
        actions = {
            "feed_views": 0,
            "likes": 0,
            "follows": 0,
            "comments": 0,
            "story_views": 0,
            "explore_views": 0,
            "profile_visits": 0,
            "saved_posts": 0,  # Новое
            "reels_views": 0,  # Новое
            "video_loops": 0,  # Новое
            "location_searches": 0,  # Новое
            "notification_checks": 0,  # Новое
            "long_press_previews": 0,  # Новое
            "accidental_likes": 0,  # Новое
            "cancelled_comments": 0,  # Новое
            "ad_clicks": 0  # Новое
        }
        
        try:
            # 1. Просмотр основной ленты
            if random.random() < 0.9:  # 90% сессий начинаются с ленты
                logger.info("📱 Открываем основную ленту...")
                
                # Эмулируем открытие приложения
                time.sleep(random.uniform(0.5, 2))
                
                # Получаем ленту
                timeline = cl.get_timeline_feed()
                if timeline and 'feed_items' in timeline:
                    posts = [item['media_or_ad'] for item in timeline['feed_items'] 
                            if 'media_or_ad' in item]
                    
                    # Симулируем просмотр
                    viewed_posts = self.simulate_human_scroll(posts, config)
                    
                    # Сохраняем для последующего использования
                    self._current_feed_posts = posts
                    
                    for post, view_time in viewed_posts:
                        # Просматриваем
                        logger.info(f"👀 Смотрим пост {view_time:.1f} сек...")
                        time.sleep(view_time)
                        actions["feed_views"] += 1
                        
                        # Решаем, лайкать ли
                        if random.random() < config["likes_ratio"]:
                            if self._can_perform_action(session.account_id, ActionType.LIKE):
                                try:
                                    cl.media_like(post['id'])
                                    actions["likes"] += 1
                                    logger.info("❤️ Поставили лайк")
                                    
                                    # Сохраняем последний лайкнутый пост
                                    self._last_liked_media = post['id']
                                    
                                    # Пауза после лайка
                                    time.sleep(random.uniform(0.5, 1.5))
                                except Exception as e:
                                    logger.warning(f"Не удалось лайкнуть: {e}")
            
            # 2. Просмотр сторис
            if random.random() < config["story_views_ratio"]:
                logger.info("📸 Смотрим истории...")
                self._view_stories(cl, session, config, actions)
            
            # 3. Посещение профилей и подписки
            if random.random() < config["follows_ratio"]:
                logger.info("👤 Изучаем профили...")
                self._explore_profiles(cl, session, config, actions)
            
            # 4. Раздел Explore
            if random.random() < config["explore_ratio"]:
                logger.info("🔍 Заходим в Explore...")
                self._browse_explore(cl, session, config, actions)
            
            # 5. Поиск по интересам
            if session.interests and random.random() < 0.3:
                interest = random.choice(session.interests)
                logger.info(f"🔎 Ищем по интересу: {interest}")
                self._search_by_interest(cl, session, interest, config, actions)
            
            # 6. Reels (ПРИОРИТЕТ)
            if random.random() < config.get("reels_ratio", 0.4):
                self._browse_reels(cl, session)
            
            # 7. Сохранение постов (ПРИОРИТЕТ) 
            if random.random() < config.get("save_ratio", 0.2):
                self._save_interesting_posts(cl, session)
            
            # 8. Проверка уведомлений (ПРИОРИТЕТ)
            if random.random() < 0.4:  # Довольно часто
                self._check_notifications(cl, session, actions)
            
            # 9. Исследование локаций
            if random.random() < 0.15:
                self._explore_locations(cl, session, config, actions)
            
            # 10. UI взаимодействия на протяжении всей сессии
            self._simulate_ui_interactions(cl, session, actions)
            
            # Сохраняем просмотренные посты для последующего использования
            if hasattr(self, '_current_feed_posts'):
                self._last_viewed_posts = self._current_feed_posts
            
        except Exception as e:
            logger.error(f"❌ Ошибка в прогреве: {e}")
            session.errors.append(str(e))
        
        return actions
    
    def _view_stories(self, cl: Client, session: WarmupSession, config: Dict, actions: Dict):
        """Просмотр историй"""
        try:
            # Получаем список историй
            stories_tray = cl.get_timeline_feed().get('tray', [])
            
            if stories_tray:
                # Выбираем случайное количество для просмотра
                stories_to_view = random.randint(3, min(10, len(stories_tray)))
                selected_stories = random.sample(stories_tray, stories_to_view)
                
                for story_item in selected_stories:
                    if 'user' in story_item:
                        username = story_item['user'].get('username', 'unknown')
                        
                        # Время просмотра истории
                        view_time = random.uniform(1.5, 4.5)
                        
                        # Иногда досматриваем до конца
                        if random.random() < 0.3:
                            view_time = random.uniform(4, 7)
                        
                        # Иногда быстро пропускаем
                        elif random.random() < 0.2:
                            view_time = random.uniform(0.5, 1)
                        
                        logger.info(f"📸 Смотрим историю @{username} ({view_time:.1f} сек)")
                        time.sleep(view_time)
                        actions["story_views"] += 1
                        
                        # Иногда отвечаем на историю
                        if random.random() < 0.05:  # 5% шанс
                            reactions = ["🔥", "😍", "👏", "💯", "🙌"]
                            reaction = random.choice(reactions)
                            logger.info(f"💬 Отправляем реакцию: {reaction}")
                            time.sleep(random.uniform(1, 2))
                            
        except Exception as e:
            logger.warning(f"Ошибка при просмотре историй: {e}")
    
    def _explore_profiles(self, cl: Client, session: WarmupSession, config: Dict, actions: Dict):
        """Изучение профилей и подписки"""
        try:
            # Ищем пользователей по интересам
            if session.interests:
                search_query = random.choice(session.interests)
            else:
                search_query = random.choice(["travel", "food", "fitness", "art", "nature"])
            
            users = cl.search_users(search_query)[:20]  # Берем первые 20 результатов
            
            if users:
                # Выбираем несколько для просмотра
                profiles_to_view = random.randint(2, min(5, len(users)))
                selected_users = random.sample(users, profiles_to_view)
                
                for user in selected_users:
                    # Заходим в профиль
                    logger.info(f"👤 Заходим к @{user.username}")
                    
                    # Эмулируем загрузку профиля
                    time.sleep(random.uniform(1, 2.5))
                    
                    # Получаем детали профиля
                    user_info = cl.user_info(user.pk)
                    actions["profile_visits"] += 1
                    
                    # Анализируем профиль
                    browse_time = random.uniform(3, 10)
                    
                    # Если профиль интересный - смотрим дольше
                    if user_info.media_count > 50 and user_info.follower_count > 1000:
                        browse_time *= random.uniform(1.5, 2.5)
                        
                        # Смотрим посты
                        logger.info(f"📷 Смотрим посты @{user.username}")
                        time.sleep(browse_time)
                        
                        # Решаем подписаться ли
                        if random.random() < config["follows_ratio"]:
                            if self._can_perform_action(session.account_id, ActionType.FOLLOW):
                                try:
                                    cl.user_follow(user.pk)
                                    actions["follows"] += 1
                                    logger.info(f"✅ Подписались на @{user.username}")
                                    
                                    # После подписки можем полайкать пару постов
                                    if random.random() < 0.5:
                                        medias = cl.user_medias(user.pk, amount=3)
                                        for media in medias[:random.randint(1, 2)]:
                                            if self._can_perform_action(session.account_id, ActionType.LIKE):
                                                cl.media_like(media.pk)
                                                actions["likes"] += 1
                                                time.sleep(random.uniform(1, 2))
                                                
                                except Exception as e:
                                    logger.warning(f"Не удалось подписаться: {e}")
                    else:
                        # Быстро просматриваем и уходим
                        time.sleep(browse_time)
                        
        except Exception as e:
            logger.warning(f"Ошибка при изучении профилей: {e}")
    
    def _browse_explore(self, cl: Client, session: WarmupSession, config: Dict, actions: Dict):
        """Просмотр раздела Explore"""
        try:
            logger.info("🔍 Открываем Explore...")
            
            # Эмулируем переход в Explore
            time.sleep(random.uniform(0.5, 1.5))
            
            # Получаем популярные посты
            # В instagrapi нет прямого метода для explore, используем поиск по хештегам
            trending_tags = ["instagood", "photooftheday", "beautiful", "love", "nature"]
            tag = random.choice(trending_tags)
            
            medias = cl.hashtag_medias_recent(tag, amount=20)
            
            if medias:
                # Просматриваем посты
                viewed_posts = self.simulate_human_scroll(medias, config)
                
                for media, view_time in viewed_posts:
                    logger.info(f"🔍 Смотрим пост в Explore ({view_time:.1f} сек)")
                    time.sleep(view_time)
                    actions["explore_views"] += 1
                    
                    # Иногда лайкаем
                    if random.random() < config["likes_ratio"] * 0.7:  # Чуть реже чем в основной ленте
                        if self._can_perform_action(session.account_id, ActionType.LIKE):
                            try:
                                cl.media_like(media.pk)
                                actions["likes"] += 1
                                logger.info("❤️ Лайк в Explore")
                            except:
                                pass
                                
        except Exception as e:
            logger.warning(f"Ошибка в Explore: {e}")
    
    def _search_by_interest(self, cl: Client, session: WarmupSession, interest: str, config: Dict, actions: Dict):
        """Поиск по интересам"""
        try:
            # Ищем хештеги
            hashtags = cl.search_hashtags(interest)  # Убрал параметр count
            
            if hashtags:
                hashtag = random.choice(hashtags[:5])  # Берем первые 5
                logger.info(f"#️⃣ Изучаем #{hashtag.name}")
                
                # Получаем посты
                medias = cl.hashtag_medias_recent(hashtag.name, amount=10)
                
                if medias:
                    # Просматриваем несколько
                    for media in medias[:random.randint(3, 7)]:
                        view_time = random.uniform(*config["read_time"])
                        time.sleep(view_time)
                        actions["explore_views"] += 1
                        
                        # Лайкаем если понравилось
                        if random.random() < config["likes_ratio"]:
                            if self._can_perform_action(session.account_id, ActionType.LIKE):
                                try:
                                    cl.media_like(media.pk)
                                    actions["likes"] += 1
                                    logger.info(f"❤️ Лайк по интересу {interest}")
                                except:
                                    pass
                                    
        except Exception as e:
            logger.warning(f"Ошибка поиска по интересам: {e}")
    
    def _can_perform_action(self, account_id: int, action_type: ActionType) -> bool:
        """Проверить можно ли выполнить действие"""
        can_do, reason = rate_limiter.can_perform_action(account_id, action_type)
        if not can_do:
            logger.warning(f"⏳ {reason}")
        return can_do
    
    def start_warmup(
        self, 
        account_id: int, 
        duration_minutes: int = 30,
        interests: List[str] = None,
        strategy: WarmupStrategy = None
    ) -> Tuple[bool, str]:
        """Запустить прогрев аккаунта"""
        try:
            # Определяем стратегию
            if not strategy:
                strategy = self.determine_strategy(account_id)
            
            logger.info(f"🚀 Запускаем прогрев по стратегии: {strategy.value}")
            
            # Создаем сессию
            session = WarmupSession(
                account_id=account_id,
                strategy=strategy,
                duration_minutes=duration_minutes,
                interests=interests or [],
                start_time=datetime.now(),
                actions_performed={},
                errors=[],
                time_pattern_used=False # Изначально не используем паттерн
            )
            
            # Проверяем логин
            cl = self.create_instagram_client(account_id)
            if not cl:
                logger.error(f"❌ Не удалось создать клиент для аккаунта {account_id}")
                return False, "Не удалось войти в аккаунт"
            
            # Проверяем, что аккаунт действительно залогинен
            try:
                # Делаем простой запрос для проверки
                cl.user_info(cl.user_id)
            except Exception as e:
                error_msg = str(e)
                if "login_required" in error_msg or "challenge_required" in error_msg:
                    logger.error(f"❌ Требуется повторный вход для аккаунта {account_id}: {error_msg}")
                    return False, f"Требуется повторный вход: {error_msg}"
                elif "user_not_found" in error_msg or "We can't find an account" in error_msg:
                    logger.error(f"❌ Аккаунт {account_id} не найден в Instagram")
                    return False, "Аккаунт не найден в Instagram"
            
            # Сохраняем сессию
            self.active_sessions[account_id] = session
            
            # Запускаем периодическую проверку на остановку
            import threading
            stop_check_event = threading.Event()
            
            def check_stop():
                while not stop_check_event.is_set():
                    if self.stop_callback and self.stop_callback():
                        logger.info(f"🛑 Получен сигнал остановки прогрева для аккаунта {account_id}")
                        session.is_active = False
                        break
                    time.sleep(1)
            
            stop_thread = threading.Thread(target=check_stop)
            stop_thread.daemon = True
            stop_thread.start()
            
            # Получаем конфиг для стратегии
            config = self.STRATEGY_CONFIGS[strategy]
            
            # Применяем временной паттерн (ПРИОРИТЕТ)
            time_pattern = self.determine_time_pattern()
            if time_pattern:
                logger.info(f"⏰ Применяем временной паттерн: интенсивность {time_pattern['intensity']*100:.0f}%")
                
                # Модифицируем конфиг согласно времени
                config = config.copy()
                for key in ['likes_ratio', 'follows_ratio', 'comments_ratio', 'story_views_ratio']:
                    if key in config:
                        config[key] *= time_pattern['intensity']
                
                # Обновляем длительность сессии
                if 'duration' in time_pattern:
                    duration_minutes = min(
                        duration_minutes,
                        random.randint(*time_pattern['duration'])
                    )
                session.time_pattern_used = True # Указываем, что паттерн использовался
            
            # Выполняем прогрев
            end_time = datetime.now() + timedelta(minutes=duration_minutes)
            total_actions = {}
            
            while datetime.now() < end_time:
                # Проверяем, нужно ли остановить прогрев
                if self.stop_callback and self.stop_callback():
                    logger.info("⏹️ Прогрев остановлен пользователем")
                    break
                    
                # Выполняем действия
                actions = self.perform_warmup_actions(cl, session, config)
                
                # Суммируем действия
                for action, count in actions.items():
                    total_actions[action] = total_actions.get(action, 0) + count
                
                # Пауза между циклами
                pause = random.uniform(30, 120)
                logger.info(f"⏸️ Пауза {pause:.0f} секунд...")
                time.sleep(pause)
                
                # Проверяем не пора ли закончить сессию
                session_duration = (datetime.now() - session.start_time).seconds / 60
                min_duration, max_duration = config["session_duration"]
                
                if session_duration > random.uniform(min_duration, max_duration):
                    logger.info("📱 Закрываем приложение (конец сессии)")
                    break
            
            # Сохраняем результаты
            session.actions_performed = total_actions
            
            # Обновляем статистику аккаунта
            update_instagram_account(
                account_id,
                last_warmup=datetime.now(),
                warmup_stats=json.dumps(total_actions)
            )
            
            # Формируем отчет
            report = f"""
✅ Прогрев завершен!

📊 Основная статистика:
• Просмотров ленты: {total_actions.get('feed_views', 0)}
• Лайков: {total_actions.get('likes', 0)}
• Подписок: {total_actions.get('follows', 0)}
• Просмотров историй: {total_actions.get('story_views', 0)}
• Посещений профилей: {total_actions.get('profile_visits', 0)}
• Просмотров Explore: {total_actions.get('explore_views', 0)}

🎬 Reels и видео:
• Просмотров Reels: {total_actions.get('reels_views', 0)}
• Зацикленных видео: {total_actions.get('video_loops', 0)}

💾 Сохранения и коллекции:
• Сохранено постов: {total_actions.get('saved_posts', 0)}

📍 Локации:
• Поисков мест: {total_actions.get('location_searches', 0)}

🔔 Активность:
• Проверок уведомлений: {total_actions.get('notification_checks', 0)}

🎯 UI взаимодействия:
• Долгих нажатий: {total_actions.get('long_press_previews', 0)}
• Случайных лайков: {total_actions.get('accidental_likes', 0)}
• Отмененных комментариев: {total_actions.get('cancelled_comments', 0)}
• Кликов на рекламу: {total_actions.get('ad_clicks', 0)}

⏱️ Длительность: {(datetime.now() - session.start_time).seconds // 60} минут
🎯 Стратегия: {strategy.value}
⏰ Временной паттерн: {"Применен" if session.time_pattern_used else "Стандартный"}
"""
            
            del self.active_sessions[account_id]
            return True, report
            
        except Exception as e:
            logger.error(f"❌ Критическая ошибка прогрева: {e}")
            if account_id in self.active_sessions:
                del self.active_sessions[account_id]
            return False, str(e)
    
    def determine_time_pattern(self) -> Optional[Dict]:
        """Определить текущий временной паттерн"""
        current_hour = datetime.now().hour
        current_day = datetime.now().weekday()
        
        # Выходные (суббота=5, воскресенье=6)
        is_weekend = current_day in [5, 6]
        
        for pattern_name, pattern in self.TIME_PATTERNS.items():
            if current_hour in pattern["hours"]:
                pattern_copy = pattern.copy()
                
                # Увеличиваем активность в выходные
                if is_weekend:
                    pattern_copy["intensity"] *= 1.3
                    pattern_copy["duration"] = (
                        int(pattern["duration"][0] * 1.5),
                        int(pattern["duration"][1] * 1.5)
                    )
                    
                return pattern_copy
        
        return None
    
    def _save_interesting_posts(self, cl: Client, session: WarmupSession) -> None:
        """Сохранение интересных постов в коллекции"""
        if not self._can_perform_action("save", session):
            return
            
        try:
            # Получаем ленту
            timeline = cl.timeline_feed()
            if not timeline or not hasattr(timeline, 'feed_items'):
                return
                
            posts = [item.media for item in timeline.feed_items if hasattr(item, 'media')][:10]
            
            # Сохраняем 1-3 поста
            posts_to_save = random.randint(1, min(3, len(posts)))
            saved = 0
            
            for post in random.sample(posts, min(posts_to_save, len(posts))):
                try:
                    cl.media_save(post.pk)
                    session.posts_saved += 1
                    saved += 1
                    logger.info(f"💾 Сохранен пост от @{post.user.username}")
                    time.sleep(random.uniform(2, 5))
                except Exception as e:
                    logger.debug(f"Не удалось сохранить пост: {e}")
                    
            if saved > 0:
                logger.info(f"✅ Сохранено {saved} постов")
                
        except Exception as e:
            logger.warning(f"Ошибка при сохранении постов: {e}")
    
    def _browse_reels(self, cl: Client, session: WarmupSession) -> None:
        """Просмотр Reels"""
        if not self._can_perform_action("reels", session):
            return
            
        logger.info("🎬 Открываем Reels...")
        
        try:
            # Получаем Reels через hashtag или timeline
            reels_items = []
            try:
                # Пробуем получить через хештег
                reels_tag = cl.hashtag_medias_recent('reels', amount=20)
                reels_items = [m for m in reels_tag if m.media_type == 2]  # 2 = video/reel
            except Exception as e:
                logger.debug(f"Не удалось получить Reels через хештег: {e}")
                
            # Если не получилось, пробуем timeline
            if not reels_items:
                try:
                    timeline = cl.user_medias(cl.user_id, amount=20)
                    reels_items = [m for m in timeline if m.media_type == 2]
                except Exception as e:
                    logger.debug(f"Не удалось получить Reels из timeline: {e}")
            
            if reels_items:
                reels_to_view = random.randint(5, min(15, len(reels_items)))
                logger.info(f"🎥 Смотрим {reels_to_view} Reels...")
                
                for i, reel in enumerate(reels_items[:reels_to_view]):
                    # Время просмотра в зависимости от длительности
                    duration = getattr(reel, 'video_duration', 15) if hasattr(reel, 'video_duration') else 15
                    view_time = min(duration * 0.8, random.uniform(10, 30))
                    
                    logger.info(f"▶️ Смотрим Reel {i+1}/{reels_to_view} ({view_time:.1f} сек)...")
                    time.sleep(view_time)
                    
                    # Иногда лайкаем
                    if random.random() < 0.3:
                        try:
                            cl.media_like(reel.pk)
                            session.reels_watched += 1
                            logger.info("❤️ Лайкнули Reel")
                        except Exception as e:
                            logger.debug(f"Не удалось лайкнуть: {e}")
                    
                    # Иногда сохраняем
                    if random.random() < 0.1:
                        try:
                            cl.media_save(reel.pk)
                            session.posts_saved += 1
                            logger.info("💾 Сохранили Reel")
                        except Exception as e:
                            logger.debug(f"Не удалось сохранить: {e}")
                            
                    # Зацикливание интересных видео (повторный просмотр)
                    if random.random() < 0.15:
                        logger.info("🔄 Пересматриваем интересное видео...")
                        time.sleep(random.uniform(5, 15))
                        
                    # Пауза между видео
                    time.sleep(random.uniform(2, 5))
                    
                logger.info(f"✅ Просмотрено {reels_to_view} Reels")
                
        except Exception as e:
            logger.warning(f"Ошибка при просмотре Reels: {e}")
    
    def _explore_locations(self, cl: Client, session: WarmupSession, config: Dict, actions: Dict):
        """Искать места поблизости и просматривать посты"""
        try:
            # Симулируем получение геолокации
            # В реальности нужны настоящие координаты
            lat = 55.7558  # Москва для примера
            lng = 37.6173
            
            logger.info("📍 Ищем места поблизости...")
            
            # Поиск мест
            nearby_locations = cl.location_search(lat, lng)
            
            if nearby_locations:
                # Выбираем 1-3 места для изучения
                locations_to_explore = random.sample(
                    nearby_locations, 
                    min(random.randint(1, 3), len(nearby_locations))
                )
                
                for location in locations_to_explore:
                    logger.info(f"📍 Изучаем место: {location.name}")
                    actions["location_searches"] += 1
                    
                    # Получаем посты из места
                    location_medias = cl.location_medias_recent(location.pk, amount=10)
                    
                    # Просматриваем несколько постов
                    for media in location_medias[:random.randint(3, 7)]:
                        view_time = random.uniform(2, 5)
                        time.sleep(view_time)
                        actions["explore_views"] += 1
                        
                        # Иногда лайкаем местный контент
                        if random.random() < 0.15:
                            if self._can_perform_action(session.account_id, ActionType.LIKE):
                                try:
                                    cl.media_like(media.pk)
                                    actions["likes"] += 1
                                except:
                                    pass
                                    
        except Exception as e:
            logger.warning(f"Ошибка при изучении локаций: {e}")
    
    def _check_notifications(self, cl: Client, session: WarmupSession, actions: Dict):
        """Проверять уведомления и активность (ПРИОРИТЕТ)"""
        try:
            logger.info("🔔 Проверяем уведомления...")
            
            # Эмулируем открытие вкладки активности
            time.sleep(random.uniform(1, 2))
            
            # Проверяем последние посты для лайков
            account = get_instagram_account(session.account_id)
            if account and account.username:
                try:
                    # Получаем свой профиль
                    user_id = cl.user_id_from_username(account.username)
                    
                    # Пробуем получить медиа, но с защитой от ошибок
                    try:
                        user_medias = cl.user_medias(user_id, amount=3)
                    except Exception as e:
                        logger.warning(f"Не удалось получить медиа: {e}")
                        user_medias = []
                    
                    for media in user_medias:
                        # Смотрим кто лайкнул (ПРИОРИТЕТ)
                        try:
                            likers = cl.media_likers(media.pk)
                        except Exception as e:
                            logger.warning(f"Не удалось получить лайки: {e}")
                            continue
                        
                        if likers:
                            logger.info(f"👥 {len(likers)} лайков на последнем посте")
                            
                            # Просматриваем несколько профилей лайкнувших
                            for liker in likers[:random.randint(2, 5)]:
                                time.sleep(random.uniform(0.5, 1.5))
                                
                                # Иногда заходим в профиль
                                if random.random() < 0.3:
                                    logger.info(f"👤 Смотрим профиль @{liker.username}")
                                    actions["profile_visits"] += 1
                                    time.sleep(random.uniform(2, 4))
                    
                    # Проверяем новых подписчиков
                    followers = cl.user_followers(user_id, amount=20)
                    logger.info(f"👥 Проверяем {len(followers)} подписчиков")
                    
                    actions["notification_checks"] += 1
                    
                except Exception as e:
                    logger.warning(f"Ошибка при проверке своего профиля: {e}")
                    
        except Exception as e:
            logger.warning(f"Ошибка при проверке уведомлений: {e}")
    
    def _simulate_ui_interactions(self, cl: Client, session: WarmupSession, actions: Dict):
        """Симулировать UI взаимодействия (долгое нажатие, случайные действия)"""
        try:
            # Долгое нажатие для предпросмотра (ПРИОРИТЕТ)
            if random.random() < 0.15:  # 15% постов
                logger.info("👆 Долгое нажатие для предпросмотра")
                press_duration = random.uniform(0.8, 2.5)
                time.sleep(press_duration)
                actions["long_press_previews"] += 1
            
            # Случайно лайкнуть и убрать (ПРИОРИТЕТ)
            if random.random() < 0.05 and hasattr(self, '_last_liked_media'):  # 5% шанс
                logger.info("😅 Упс, случайно лайкнули")
                
                # Быстро убираем лайк
                time.sleep(random.uniform(0.5, 1.5))
                
                try:
                    if self._last_liked_media:
                        cl.media_unlike(self._last_liked_media)
                        logger.info("❌ Убрали случайный лайк")
                        actions["accidental_likes"] += 1
                except:
                    pass
            
            # Начать писать комментарий и отменить (ПРИОРИТЕТ)
            if random.random() < 0.1:  # 10% шанс
                logger.info("💭 Начинаем писать комментарий...")
                
                # Эмулируем набор текста
                typing_time = random.uniform(3, 8)
                time.sleep(typing_time)
                
                # 70% шанс передумать
                if random.random() < 0.7:
                    logger.info("❌ Передумали комментировать")
                    actions["cancelled_comments"] += 1
                else:
                    # Дописываем и отправляем
                    logger.info("💬 Отправили комментарий")
            
            # Иногда кликать на рекламу
            if random.random() < 0.03:  # 3% шанс
                logger.info("📢 Случайно кликнули на рекламу")
                time.sleep(random.uniform(2, 5))
                actions["ad_clicks"] += 1
                
                # Эмулируем возврат
                time.sleep(random.uniform(1, 2))
                logger.info("↩️ Вернулись из рекламы")
                
        except Exception as e:
            logger.warning(f"Ошибка UI взаимодействий: {e}")
    
    def stop_warmup(self, account_id: int) -> Tuple[bool, str]:
        """Остановить прогрев"""
        if account_id in self.active_sessions:
            del self.active_sessions[account_id]
            return True, "Прогрев остановлен"
        return False, "Прогрев не запущен"


# Глобальный экземпляр
advanced_warmup = AdvancedWarmupService() 
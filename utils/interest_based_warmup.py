#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Система прогрева аккаунтов по интересам
Умный таргетированный прогрев для формирования алгоритмических предпочтений
"""

import logging
import random
import time
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)


class InterestCategory(Enum):
    """Категории интересов для прогрева"""
    LIFESTYLE = "lifestyle"
    FITNESS = "fitness"
    FOOD = "food"
    TRAVEL = "travel"
    FASHION = "fashion"
    TECHNOLOGY = "technology"
    BUSINESS = "business"
    ART = "art"
    MUSIC = "music"
    PHOTOGRAPHY = "photography"
    BEAUTY = "beauty"
    MOTIVATION = "motivation"
    EDUCATION = "education"
    ENTERTAINMENT = "entertainment"
    GAMING = "gaming"
    SPORTS = "sports"
    HEALTH = "health"
    FINANCE = "finance"
    AUTOMOTIVE = "automotive"
    REAL_ESTATE = "real_estate"


@dataclass
class InterestProfile:
    """Профиль интересов для аккаунта"""
    primary_interests: List[InterestCategory]  # 2-3 основных интереса
    secondary_interests: List[InterestCategory]  # 3-5 дополнительных
    target_hashtags: List[str]  # Целевые хештеги
    target_accounts: List[str]  # Целевые аккаунты для изучения
    interaction_weights: Dict[str, float]  # Веса для типов взаимодействий
    language: str = "en"  # Язык контента


class InterestBasedWarmup:
    """Система прогрева по интересам"""
    
    # Готовые профили интересов
    INTEREST_PROFILES = {
        InterestCategory.FITNESS: {
            "hashtags": [
                "fitness", "workout", "gym", "bodybuilding", "motivation",
                "fitnessmotivation", "training", "muscle", "strength", "cardio",
                "weightloss", "transformation", "fitspo", "exercise", "sport",
                "healthylifestyle", "nutrition", "protein", "abs", "goals"
            ],
            "accounts": [
                "therock", "mrolympia", "stephencurry30", "kingjames",
                "cristiano", "nike", "underarmour", "gymshark"
            ],
            "interaction_weights": {
                "like": 0.8, "comment": 0.4, "save": 0.6, "follow": 0.3,
                "view_story": 0.7, "watch_reel": 0.9
            }
        },
        
        InterestCategory.FOOD: {
            "hashtags": [
                "food", "foodie", "cooking", "recipe", "delicious",
                "foodphotography", "chef", "restaurant", "foodblogger", "yummy",
                "homemade", "healthy", "vegan", "dessert", "breakfast",
                "dinner", "lunch", "tasty", "foodstagram", "culinary"
            ],
            "accounts": [
                "gordongram", "jamieoliver", "buzzfeedtasty", "foodnetwork",
                "thefoodbabe", "minimalistbaker", "delish", "bonappetitmag"
            ],
            "interaction_weights": {
                "like": 0.9, "comment": 0.5, "save": 0.8, "follow": 0.4,
                "view_story": 0.6, "watch_reel": 0.8
            }
        },
        
        InterestCategory.TRAVEL: {
            "hashtags": [
                "travel", "wanderlust", "adventure", "explore", "vacation",
                "travelgram", "instatravel", "backpacking", "nature", "photography",
                "landscape", "sunset", "beach", "mountains", "city",
                "culture", "trip", "journey", "nomad", "passport"
            ],
            "accounts": [
                "natgeo", "beautifuldestinations", "earthpix", "lonelyplanet",
                "wonderful_places", "backpacker", "hostelworld", "airbnb"
            ],
            "interaction_weights": {
                "like": 0.8, "comment": 0.3, "save": 0.9, "follow": 0.5,
                "view_story": 0.8, "watch_reel": 0.7
            }
        },
        
        InterestCategory.TECHNOLOGY: {
            "hashtags": [
                "technology", "tech", "innovation", "ai", "startup",
                "coding", "programming", "developer", "software", "gadgets",
                "apple", "android", "google", "microsoft", "tesla",
                "blockchain", "cryptocurrency", "iot", "machinelearning", "data"
            ],
            "accounts": [
                "elonmusk", "apple", "google", "microsoft", "tesla",
                "techcrunch", "verge", "wired", "mashable"
            ],
            "interaction_weights": {
                "like": 0.7, "comment": 0.6, "save": 0.7, "follow": 0.6,
                "view_story": 0.5, "watch_reel": 0.6
            }
        },
        
        InterestCategory.BUSINESS: {
            "hashtags": [
                "business", "entrepreneur", "startup", "success", "motivation",
                "leadership", "marketing", "finance", "investment", "money",
                "wealth", "mindset", "goals", "productivity", "networking",
                "innovation", "strategy", "growth", "sales", "ceo"
            ],
            "accounts": [
                "garyvee", "elonmusk", "richardbranson", "oprah", "jeffweiner",
                "forbes", "entrepreneur", "inc", "fastcompany"
            ],
            "interaction_weights": {
                "like": 0.8, "comment": 0.7, "save": 0.8, "follow": 0.7,
                "view_story": 0.6, "watch_reel": 0.5
            }
        }
    }
    
    def __init__(self, account_id: int, client, interest_profile: InterestProfile):
        self.account_id = account_id
        self.client = client
        self.interest_profile = interest_profile
        self.session_stats = {
            'hashtags_explored': [],
            'accounts_visited': [],
            'content_interactions': [],
            'total_actions': 0
        }
    
    def perform_interest_warmup_session(self, duration_minutes: int = 30) -> Dict:
        """Выполнить сессию прогрева по интересам"""
        logger.info(f"🎯 Начинаем прогрев по интересам (длительность: {duration_minutes} мин)")
        
        session_start = time.time()
        session_results = {
            'interests_explored': [],
            'actions_performed': {},
            'session_duration': 0,
            'target_achieved': False
        }
        
        # Планируем сессию
        action_plan = self._plan_interest_session(duration_minutes)
        logger.info(f"📋 План сессии: {action_plan}")
        
        # Выполняем действия по плану
        for action_type, targets in action_plan.items():
            if action_type == 'explore_hashtags':
                result = self._explore_interest_hashtags(targets)
                session_results['actions_performed']['hashtag_exploration'] = result
                
            elif action_type == 'study_accounts':
                result = self._study_target_accounts(targets)
                session_results['actions_performed']['account_study'] = result
                
            elif action_type == 'interact_with_content':
                result = self._interact_with_interest_content(targets)
                session_results['actions_performed']['content_interaction'] = result
                
            elif action_type == 'discover_similar':
                result = self._discover_similar_content()
                session_results['actions_performed']['content_discovery'] = result
        
        session_results['session_duration'] = time.time() - session_start
        session_results['interests_explored'] = list(set(self.session_stats['hashtags_explored']))
        
        logger.info(f"✅ Сессия прогрева по интересам завершена за {session_results['session_duration']:.1f}с")
        return session_results
    
    def _plan_interest_session(self, duration_minutes: int) -> Dict[str, List]:
        """Планирование сессии прогрева по интересам"""
        
        # Выбираем интересы для этой сессии
        primary_interest = random.choice(self.interest_profile.primary_interests)
        secondary_interest = random.choice(self.interest_profile.secondary_interests) if self.interest_profile.secondary_interests else None
        
        # Получаем данные профиля интересов
        primary_data = self.INTEREST_PROFILES.get(primary_interest, {})
        
        # Выбираем хештеги и аккаунты
        target_hashtags = random.sample(
            primary_data.get('hashtags', []), 
            min(3, len(primary_data.get('hashtags', [])))
        )
        
        target_accounts = random.sample(
            primary_data.get('accounts', []), 
            min(2, len(primary_data.get('accounts', [])))
        )
        
        plan = {
            'explore_hashtags': target_hashtags,
            'study_accounts': target_accounts,
            'interact_with_content': [primary_interest.value],
            'discover_similar': ['explore_page']
        }
        
        return plan
    
    def _explore_interest_hashtags(self, hashtags: List[str]) -> int:
        """Исследование хештегов по интересам"""
        explored_count = 0
        
        for hashtag in hashtags:
            try:
                logger.info(f"🔍 Исследуем хештег #{hashtag}")
                
                # Получаем медиа по хештегу
                medias = self.client.hashtag_medias_recent(hashtag, amount=20)
                
                if medias:
                    # Изучаем контент
                    for i, media in enumerate(medias[:5]):
                        # Имитируем просмотр
                        view_time = random.randint(2, 8)
                        time.sleep(view_time)
                        
                        # Иногда лайкаем (в зависимости от интересов)
                        if random.random() < 0.3:  # 30% шанс
                            try:
                                self.client.media_like(media.id)
                                logger.info(f"❤️ Лайкнули пост по #{hashtag}")
                                self.session_stats['content_interactions'].append({
                                    'type': 'like',
                                    'hashtag': hashtag,
                                    'media_id': media.id
                                })
                            except:
                                pass
                        
                        # Иногда сохраняем
                        if random.random() < 0.2:  # 20% шанс
                            try:
                                self.client.media_save(media.id)
                                logger.info(f"💾 Сохранили пост по #{hashtag}")
                                self.session_stats['content_interactions'].append({
                                    'type': 'save',
                                    'hashtag': hashtag,
                                    'media_id': media.id
                                })
                            except:
                                pass
                    
                    explored_count += 1
                    self.session_stats['hashtags_explored'].append(hashtag)
                    
                    # Пауза между хештегами
                    time.sleep(random.randint(10, 30))
                
            except Exception as e:
                logger.warning(f"Ошибка при исследовании #{hashtag}: {e}")
        
        return explored_count
    
    def _study_target_accounts(self, accounts: List[str]) -> int:
        """Изучение целевых аккаунтов"""
        studied_count = 0
        
        for username in accounts:
            try:
                logger.info(f"👤 Изучаем аккаунт @{username}")
                
                # Получаем информацию об аккаунте
                user_info = self.client.user_info_by_username(username)
                
                if user_info:
                    # Просматриваем профиль
                    time.sleep(random.randint(5, 15))
                    
                    # Изучаем последние посты
                    medias = self.client.user_medias(user_info.pk, amount=10)
                    
                    for media in medias[:3]:
                        # Просматриваем пост
                        time.sleep(random.randint(3, 10))
                        
                        # Иногда лайкаем
                        if random.random() < 0.4:  # 40% шанс
                            try:
                                self.client.media_like(media.id)
                                logger.info(f"❤️ Лайкнули пост @{username}")
                            except:
                                pass
                    
                    # Просматриваем Stories если есть
                    try:
                        stories = self.client.user_stories(user_info.pk)
                        if stories:
                            # Просматриваем 1-3 сторис
                            stories_to_watch = min(3, len(stories))
                            for i in range(stories_to_watch):
                                time.sleep(random.randint(8, 20))
                                logger.info(f"👀 Посмотрели сторис @{username}")
                    except:
                        pass
                    
                    studied_count += 1
                    self.session_stats['accounts_visited'].append(username)
                    
                    # Пауза между аккаунтами
                    time.sleep(random.randint(15, 45))
                
            except Exception as e:
                logger.warning(f"Ошибка при изучении @{username}: {e}")
        
        return studied_count
    
    def _interact_with_interest_content(self, interests: List[str]) -> int:
        """Взаимодействие с контентом по интересам"""
        interactions = 0
        
        for interest in interests:
            try:
                # Получаем хештеги для этого интереса
                interest_enum = InterestCategory(interest)
                hashtags = self.INTEREST_PROFILES.get(interest_enum, {}).get('hashtags', [])
                
                if hashtags:
                    # Выбираем случайный хештег
                    hashtag = random.choice(hashtags)
                    
                    # Получаем топ медиа
                    try:
                        medias = self.client.hashtag_medias_top(hashtag, amount=10)
                    except:
                        medias = self.client.hashtag_medias_recent(hashtag, amount=10)
                    
                    if medias:
                        # Взаимодействуем с 2-3 постами
                        for media in medias[:3]:
                            # Просматриваем
                            time.sleep(random.randint(5, 15))
                            
                            # Лайкаем
                            if random.random() < 0.5:
                                try:
                                    self.client.media_like(media.id)
                                    interactions += 1
                                except:
                                    pass
                            
                            # Иногда комментируем
                            if random.random() < 0.1:  # 10% шанс
                                try:
                                    comments = self._get_interest_comments(interest)
                                    comment = random.choice(comments)
                                    self.client.media_comment(media.id, comment)
                                    logger.info(f"💬 Оставили комментарий по интересу {interest}")
                                    interactions += 1
                                except:
                                    pass
                            
                            time.sleep(random.randint(3, 8))
            
            except Exception as e:
                logger.warning(f"Ошибка взаимодействия с контентом {interest}: {e}")
        
        return interactions
    
    def _discover_similar_content(self) -> int:
        """Открытие страницы "Интересное" для изучения рекомендаций"""
        discoveries = 0
        
        try:
            logger.info("🔍 Изучаем рекомендации в разделе 'Интересное'")
            
            # Получаем рекомендации
            explore_data = self.client.get_explore_feed()
            
            if explore_data and 'items' in explore_data:
                # Просматриваем рекомендованный контент
                for item in explore_data['items'][:5]:
                    if 'media' in item:
                        media = item['media']
                        
                        # Имитируем просмотр
                        time.sleep(random.randint(3, 12))
                        
                        # Иногда взаимодействуем
                        if random.random() < 0.3:
                            try:
                                if 'id' in media:
                                    self.client.media_like(media['id'])
                                    discoveries += 1
                                    logger.info("❤️ Лайкнули рекомендованный пост")
                            except:
                                pass
                
                logger.info(f"✅ Изучили {len(explore_data['items'][:5])} рекомендаций")
            
        except Exception as e:
            logger.warning(f"Ошибка при изучении рекомендаций: {e}")
        
        return discoveries
    
    def _get_interest_comments(self, interest: str) -> List[str]:
        """Получить комментарии для конкретного интереса"""
        comment_templates = {
            'fitness': [
                "Great workout! 💪", "Amazing transformation! 🔥", "Keep it up! 💯",
                "Inspiring! 🙌", "Goals! 🎯", "So motivated! ⚡"
            ],
            'food': [
                "Looks delicious! 😋", "Recipe please! 🙏", "Yummy! 🤤",
                "Amazing! 🔥", "Perfect! 👌", "I need this! 😍"
            ],
            'travel': [
                "Beautiful place! 😍", "Adding to my bucket list! ✈️", "Amazing view! 🌅",
                "Wanderlust! 🗺️", "Goals! 🎯", "So beautiful! 💕"
            ],
            'technology': [
                "Awesome tech! 🚀", "Innovation at its best! 💡", "Game changer! 🔥",
                "The future is now! ⚡", "Impressive! 👏", "Love this! 💻"
            ],
            'business': [
                "Great advice! 💡", "So true! 💯", "Exactly! 🎯",
                "Inspiring! 🙌", "Thank you for sharing! 🙏", "Valuable content! 📚"
            ]
        }
        
        return comment_templates.get(interest, [
            "Great content! 👍", "Love this! ❤️", "Amazing! ✨",
            "So good! 🔥", "Perfect! 💯", "Awesome! 🙌"
        ])
    
    @staticmethod
    def create_interest_profile(primary_interests: List[str], 
                              secondary_interests: List[str] = None,
                              custom_hashtags: List[str] = None,
                              custom_accounts: List[str] = None) -> InterestProfile:
        """Создать профиль интересов"""
        
        # Конвертируем строки в enum
        primary_enums = []
        for interest in primary_interests:
            try:
                primary_enums.append(InterestCategory(interest.lower()))
            except ValueError:
                logger.warning(f"Неизвестный интерес: {interest}")
        
        secondary_enums = []
        if secondary_interests:
            for interest in secondary_interests:
                try:
                    secondary_enums.append(InterestCategory(interest.lower()))
                except ValueError:
                    logger.warning(f"Неизвестный дополнительный интерес: {interest}")
        
        # Собираем хештеги и аккаунты
        all_hashtags = custom_hashtags or []
        all_accounts = custom_accounts or []
        
        # Добавляем хештеги из профилей интересов
        for interest in primary_enums + secondary_enums:
            profile_data = InterestBasedWarmup.INTEREST_PROFILES.get(interest, {})
            all_hashtags.extend(profile_data.get('hashtags', []))
            all_accounts.extend(profile_data.get('accounts', []))
        
        # Убираем дубликаты
        all_hashtags = list(set(all_hashtags))
        all_accounts = list(set(all_accounts))
        
        return InterestProfile(
            primary_interests=primary_enums,
            secondary_interests=secondary_enums,
            target_hashtags=all_hashtags,
            target_accounts=all_accounts,
            interaction_weights={
                'like': 0.8, 'comment': 0.3, 'save': 0.6, 'follow': 0.4,
                'view_story': 0.7, 'watch_reel': 0.8
            }
        ) 
 
 
 
 
 
 
# -*- coding: utf-8 -*-

"""
Система прогрева аккаунтов по интересам
Умный таргетированный прогрев для формирования алгоритмических предпочтений
"""

import logging
import random
import time
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)


class InterestCategory(Enum):
    """Категории интересов для прогрева"""
    LIFESTYLE = "lifestyle"
    FITNESS = "fitness"
    FOOD = "food"
    TRAVEL = "travel"
    FASHION = "fashion"
    TECHNOLOGY = "technology"
    BUSINESS = "business"
    ART = "art"
    MUSIC = "music"
    PHOTOGRAPHY = "photography"
    BEAUTY = "beauty"
    MOTIVATION = "motivation"
    EDUCATION = "education"
    ENTERTAINMENT = "entertainment"
    GAMING = "gaming"
    SPORTS = "sports"
    HEALTH = "health"
    FINANCE = "finance"
    AUTOMOTIVE = "automotive"
    REAL_ESTATE = "real_estate"


@dataclass
class InterestProfile:
    """Профиль интересов для аккаунта"""
    primary_interests: List[InterestCategory]  # 2-3 основных интереса
    secondary_interests: List[InterestCategory]  # 3-5 дополнительных
    target_hashtags: List[str]  # Целевые хештеги
    target_accounts: List[str]  # Целевые аккаунты для изучения
    interaction_weights: Dict[str, float]  # Веса для типов взаимодействий
    language: str = "en"  # Язык контента


class InterestBasedWarmup:
    """Система прогрева по интересам"""
    
    # Готовые профили интересов
    INTEREST_PROFILES = {
        InterestCategory.FITNESS: {
            "hashtags": [
                "fitness", "workout", "gym", "bodybuilding", "motivation",
                "fitnessmotivation", "training", "muscle", "strength", "cardio",
                "weightloss", "transformation", "fitspo", "exercise", "sport",
                "healthylifestyle", "nutrition", "protein", "abs", "goals"
            ],
            "accounts": [
                "therock", "mrolympia", "stephencurry30", "kingjames",
                "cristiano", "nike", "underarmour", "gymshark"
            ],
            "interaction_weights": {
                "like": 0.8, "comment": 0.4, "save": 0.6, "follow": 0.3,
                "view_story": 0.7, "watch_reel": 0.9
            }
        },
        
        InterestCategory.FOOD: {
            "hashtags": [
                "food", "foodie", "cooking", "recipe", "delicious",
                "foodphotography", "chef", "restaurant", "foodblogger", "yummy",
                "homemade", "healthy", "vegan", "dessert", "breakfast",
                "dinner", "lunch", "tasty", "foodstagram", "culinary"
            ],
            "accounts": [
                "gordongram", "jamieoliver", "buzzfeedtasty", "foodnetwork",
                "thefoodbabe", "minimalistbaker", "delish", "bonappetitmag"
            ],
            "interaction_weights": {
                "like": 0.9, "comment": 0.5, "save": 0.8, "follow": 0.4,
                "view_story": 0.6, "watch_reel": 0.8
            }
        },
        
        InterestCategory.TRAVEL: {
            "hashtags": [
                "travel", "wanderlust", "adventure", "explore", "vacation",
                "travelgram", "instatravel", "backpacking", "nature", "photography",
                "landscape", "sunset", "beach", "mountains", "city",
                "culture", "trip", "journey", "nomad", "passport"
            ],
            "accounts": [
                "natgeo", "beautifuldestinations", "earthpix", "lonelyplanet",
                "wonderful_places", "backpacker", "hostelworld", "airbnb"
            ],
            "interaction_weights": {
                "like": 0.8, "comment": 0.3, "save": 0.9, "follow": 0.5,
                "view_story": 0.8, "watch_reel": 0.7
            }
        },
        
        InterestCategory.TECHNOLOGY: {
            "hashtags": [
                "technology", "tech", "innovation", "ai", "startup",
                "coding", "programming", "developer", "software", "gadgets",
                "apple", "android", "google", "microsoft", "tesla",
                "blockchain", "cryptocurrency", "iot", "machinelearning", "data"
            ],
            "accounts": [
                "elonmusk", "apple", "google", "microsoft", "tesla",
                "techcrunch", "verge", "wired", "mashable"
            ],
            "interaction_weights": {
                "like": 0.7, "comment": 0.6, "save": 0.7, "follow": 0.6,
                "view_story": 0.5, "watch_reel": 0.6
            }
        },
        
        InterestCategory.BUSINESS: {
            "hashtags": [
                "business", "entrepreneur", "startup", "success", "motivation",
                "leadership", "marketing", "finance", "investment", "money",
                "wealth", "mindset", "goals", "productivity", "networking",
                "innovation", "strategy", "growth", "sales", "ceo"
            ],
            "accounts": [
                "garyvee", "elonmusk", "richardbranson", "oprah", "jeffweiner",
                "forbes", "entrepreneur", "inc", "fastcompany"
            ],
            "interaction_weights": {
                "like": 0.8, "comment": 0.7, "save": 0.8, "follow": 0.7,
                "view_story": 0.6, "watch_reel": 0.5
            }
        }
    }
    
    def __init__(self, account_id: int, client, interest_profile: InterestProfile):
        self.account_id = account_id
        self.client = client
        self.interest_profile = interest_profile
        self.session_stats = {
            'hashtags_explored': [],
            'accounts_visited': [],
            'content_interactions': [],
            'total_actions': 0
        }
    
    def perform_interest_warmup_session(self, duration_minutes: int = 30) -> Dict:
        """Выполнить сессию прогрева по интересам"""
        logger.info(f"🎯 Начинаем прогрев по интересам (длительность: {duration_minutes} мин)")
        
        session_start = time.time()
        session_results = {
            'interests_explored': [],
            'actions_performed': {},
            'session_duration': 0,
            'target_achieved': False
        }
        
        # Планируем сессию
        action_plan = self._plan_interest_session(duration_minutes)
        logger.info(f"📋 План сессии: {action_plan}")
        
        # Выполняем действия по плану
        for action_type, targets in action_plan.items():
            if action_type == 'explore_hashtags':
                result = self._explore_interest_hashtags(targets)
                session_results['actions_performed']['hashtag_exploration'] = result
                
            elif action_type == 'study_accounts':
                result = self._study_target_accounts(targets)
                session_results['actions_performed']['account_study'] = result
                
            elif action_type == 'interact_with_content':
                result = self._interact_with_interest_content(targets)
                session_results['actions_performed']['content_interaction'] = result
                
            elif action_type == 'discover_similar':
                result = self._discover_similar_content()
                session_results['actions_performed']['content_discovery'] = result
        
        session_results['session_duration'] = time.time() - session_start
        session_results['interests_explored'] = list(set(self.session_stats['hashtags_explored']))
        
        logger.info(f"✅ Сессия прогрева по интересам завершена за {session_results['session_duration']:.1f}с")
        return session_results
    
    def _plan_interest_session(self, duration_minutes: int) -> Dict[str, List]:
        """Планирование сессии прогрева по интересам"""
        
        # Выбираем интересы для этой сессии
        primary_interest = random.choice(self.interest_profile.primary_interests)
        secondary_interest = random.choice(self.interest_profile.secondary_interests) if self.interest_profile.secondary_interests else None
        
        # Получаем данные профиля интересов
        primary_data = self.INTEREST_PROFILES.get(primary_interest, {})
        
        # Выбираем хештеги и аккаунты
        target_hashtags = random.sample(
            primary_data.get('hashtags', []), 
            min(3, len(primary_data.get('hashtags', [])))
        )
        
        target_accounts = random.sample(
            primary_data.get('accounts', []), 
            min(2, len(primary_data.get('accounts', [])))
        )
        
        plan = {
            'explore_hashtags': target_hashtags,
            'study_accounts': target_accounts,
            'interact_with_content': [primary_interest.value],
            'discover_similar': ['explore_page']
        }
        
        return plan
    
    def _explore_interest_hashtags(self, hashtags: List[str]) -> int:
        """Исследование хештегов по интересам"""
        explored_count = 0
        
        for hashtag in hashtags:
            try:
                logger.info(f"🔍 Исследуем хештег #{hashtag}")
                
                # Получаем медиа по хештегу
                medias = self.client.hashtag_medias_recent(hashtag, amount=20)
                
                if medias:
                    # Изучаем контент
                    for i, media in enumerate(medias[:5]):
                        # Имитируем просмотр
                        view_time = random.randint(2, 8)
                        time.sleep(view_time)
                        
                        # Иногда лайкаем (в зависимости от интересов)
                        if random.random() < 0.3:  # 30% шанс
                            try:
                                self.client.media_like(media.id)
                                logger.info(f"❤️ Лайкнули пост по #{hashtag}")
                                self.session_stats['content_interactions'].append({
                                    'type': 'like',
                                    'hashtag': hashtag,
                                    'media_id': media.id
                                })
                            except:
                                pass
                        
                        # Иногда сохраняем
                        if random.random() < 0.2:  # 20% шанс
                            try:
                                self.client.media_save(media.id)
                                logger.info(f"💾 Сохранили пост по #{hashtag}")
                                self.session_stats['content_interactions'].append({
                                    'type': 'save',
                                    'hashtag': hashtag,
                                    'media_id': media.id
                                })
                            except:
                                pass
                    
                    explored_count += 1
                    self.session_stats['hashtags_explored'].append(hashtag)
                    
                    # Пауза между хештегами
                    time.sleep(random.randint(10, 30))
                
            except Exception as e:
                logger.warning(f"Ошибка при исследовании #{hashtag}: {e}")
        
        return explored_count
    
    def _study_target_accounts(self, accounts: List[str]) -> int:
        """Изучение целевых аккаунтов"""
        studied_count = 0
        
        for username in accounts:
            try:
                logger.info(f"👤 Изучаем аккаунт @{username}")
                
                # Получаем информацию об аккаунте
                user_info = self.client.user_info_by_username(username)
                
                if user_info:
                    # Просматриваем профиль
                    time.sleep(random.randint(5, 15))
                    
                    # Изучаем последние посты
                    medias = self.client.user_medias(user_info.pk, amount=10)
                    
                    for media in medias[:3]:
                        # Просматриваем пост
                        time.sleep(random.randint(3, 10))
                        
                        # Иногда лайкаем
                        if random.random() < 0.4:  # 40% шанс
                            try:
                                self.client.media_like(media.id)
                                logger.info(f"❤️ Лайкнули пост @{username}")
                            except:
                                pass
                    
                    # Просматриваем Stories если есть
                    try:
                        stories = self.client.user_stories(user_info.pk)
                        if stories:
                            # Просматриваем 1-3 сторис
                            stories_to_watch = min(3, len(stories))
                            for i in range(stories_to_watch):
                                time.sleep(random.randint(8, 20))
                                logger.info(f"👀 Посмотрели сторис @{username}")
                    except:
                        pass
                    
                    studied_count += 1
                    self.session_stats['accounts_visited'].append(username)
                    
                    # Пауза между аккаунтами
                    time.sleep(random.randint(15, 45))
                
            except Exception as e:
                logger.warning(f"Ошибка при изучении @{username}: {e}")
        
        return studied_count
    
    def _interact_with_interest_content(self, interests: List[str]) -> int:
        """Взаимодействие с контентом по интересам"""
        interactions = 0
        
        for interest in interests:
            try:
                # Получаем хештеги для этого интереса
                interest_enum = InterestCategory(interest)
                hashtags = self.INTEREST_PROFILES.get(interest_enum, {}).get('hashtags', [])
                
                if hashtags:
                    # Выбираем случайный хештег
                    hashtag = random.choice(hashtags)
                    
                    # Получаем топ медиа
                    try:
                        medias = self.client.hashtag_medias_top(hashtag, amount=10)
                    except:
                        medias = self.client.hashtag_medias_recent(hashtag, amount=10)
                    
                    if medias:
                        # Взаимодействуем с 2-3 постами
                        for media in medias[:3]:
                            # Просматриваем
                            time.sleep(random.randint(5, 15))
                            
                            # Лайкаем
                            if random.random() < 0.5:
                                try:
                                    self.client.media_like(media.id)
                                    interactions += 1
                                except:
                                    pass
                            
                            # Иногда комментируем
                            if random.random() < 0.1:  # 10% шанс
                                try:
                                    comments = self._get_interest_comments(interest)
                                    comment = random.choice(comments)
                                    self.client.media_comment(media.id, comment)
                                    logger.info(f"💬 Оставили комментарий по интересу {interest}")
                                    interactions += 1
                                except:
                                    pass
                            
                            time.sleep(random.randint(3, 8))
            
            except Exception as e:
                logger.warning(f"Ошибка взаимодействия с контентом {interest}: {e}")
        
        return interactions
    
    def _discover_similar_content(self) -> int:
        """Открытие страницы "Интересное" для изучения рекомендаций"""
        discoveries = 0
        
        try:
            logger.info("🔍 Изучаем рекомендации в разделе 'Интересное'")
            
            # Получаем рекомендации
            explore_data = self.client.get_explore_feed()
            
            if explore_data and 'items' in explore_data:
                # Просматриваем рекомендованный контент
                for item in explore_data['items'][:5]:
                    if 'media' in item:
                        media = item['media']
                        
                        # Имитируем просмотр
                        time.sleep(random.randint(3, 12))
                        
                        # Иногда взаимодействуем
                        if random.random() < 0.3:
                            try:
                                if 'id' in media:
                                    self.client.media_like(media['id'])
                                    discoveries += 1
                                    logger.info("❤️ Лайкнули рекомендованный пост")
                            except:
                                pass
                
                logger.info(f"✅ Изучили {len(explore_data['items'][:5])} рекомендаций")
            
        except Exception as e:
            logger.warning(f"Ошибка при изучении рекомендаций: {e}")
        
        return discoveries
    
    def _get_interest_comments(self, interest: str) -> List[str]:
        """Получить комментарии для конкретного интереса"""
        comment_templates = {
            'fitness': [
                "Great workout! 💪", "Amazing transformation! 🔥", "Keep it up! 💯",
                "Inspiring! 🙌", "Goals! 🎯", "So motivated! ⚡"
            ],
            'food': [
                "Looks delicious! 😋", "Recipe please! 🙏", "Yummy! 🤤",
                "Amazing! 🔥", "Perfect! 👌", "I need this! 😍"
            ],
            'travel': [
                "Beautiful place! 😍", "Adding to my bucket list! ✈️", "Amazing view! 🌅",
                "Wanderlust! 🗺️", "Goals! 🎯", "So beautiful! 💕"
            ],
            'technology': [
                "Awesome tech! 🚀", "Innovation at its best! 💡", "Game changer! 🔥",
                "The future is now! ⚡", "Impressive! 👏", "Love this! 💻"
            ],
            'business': [
                "Great advice! 💡", "So true! 💯", "Exactly! 🎯",
                "Inspiring! 🙌", "Thank you for sharing! 🙏", "Valuable content! 📚"
            ]
        }
        
        return comment_templates.get(interest, [
            "Great content! 👍", "Love this! ❤️", "Amazing! ✨",
            "So good! 🔥", "Perfect! 💯", "Awesome! 🙌"
        ])
    
    @staticmethod
    def create_interest_profile(primary_interests: List[str], 
                              secondary_interests: List[str] = None,
                              custom_hashtags: List[str] = None,
                              custom_accounts: List[str] = None) -> InterestProfile:
        """Создать профиль интересов"""
        
        # Конвертируем строки в enum
        primary_enums = []
        for interest in primary_interests:
            try:
                primary_enums.append(InterestCategory(interest.lower()))
            except ValueError:
                logger.warning(f"Неизвестный интерес: {interest}")
        
        secondary_enums = []
        if secondary_interests:
            for interest in secondary_interests:
                try:
                    secondary_enums.append(InterestCategory(interest.lower()))
                except ValueError:
                    logger.warning(f"Неизвестный дополнительный интерес: {interest}")
        
        # Собираем хештеги и аккаунты
        all_hashtags = custom_hashtags or []
        all_accounts = custom_accounts or []
        
        # Добавляем хештеги из профилей интересов
        for interest in primary_enums + secondary_enums:
            profile_data = InterestBasedWarmup.INTEREST_PROFILES.get(interest, {})
            all_hashtags.extend(profile_data.get('hashtags', []))
            all_accounts.extend(profile_data.get('accounts', []))
        
        # Убираем дубликаты
        all_hashtags = list(set(all_hashtags))
        all_accounts = list(set(all_accounts))
        
        return InterestProfile(
            primary_interests=primary_enums,
            secondary_interests=secondary_enums,
            target_hashtags=all_hashtags,
            target_accounts=all_accounts,
            interaction_weights={
                'like': 0.8, 'comment': 0.3, 'save': 0.6, 'follow': 0.4,
                'view_story': 0.7, 'watch_reel': 0.8
            }
        ) 
 
 
 
 
 
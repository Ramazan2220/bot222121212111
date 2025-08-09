#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SNAPSHOT CACHE SYSTEM
Оптимизация Instagram API через локальное кэширование данных
"""

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any
import json
import sqlite3
from enum import Enum

class DataFreshness(Enum):
    """Уровни свежести данных"""
    REAL_TIME = 0      # Только живые данные (0 мин)
    FRESH = 300        # Свежие данные (5 мин)
    NORMAL = 3600      # Обычные данные (1 час)  
    DAILY = 86400      # Ежедневные данные (24 часа)
    WEEKLY = 604800    # Еженедельные данные (7 дней)

@dataclass
class ProfileSnapshot:
    """Снимок профиля Instagram"""
    account_id: int
    username: str
    full_name: str
    bio: str
    followers_count: int
    following_count: int
    posts_count: int
    is_private: bool
    is_verified: bool
    profile_pic_url: str
    recent_posts: List[Dict[str, Any]]  # Последние 12 постов
    last_updated: datetime
    data_version: str = "1.0"

@dataclass
class SnapshotMeta:
    """Метаданные снимка"""
    created_at: datetime
    freshness_level: DataFreshness
    source: str  # 'api', 'cache', 'fallback'
    errors: List[str]
    api_calls_saved: int

class SnapshotCache:
    """Система snapshot-кэширования"""
    
    def __init__(self, db_path: str = "data/snapshot_cache.db"):
        self.db_path = db_path
        self.init_database()
    
    def init_database(self):
        """Инициализация базы данных"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS profile_snapshots (
                account_id INTEGER PRIMARY KEY,
                username TEXT,
                full_name TEXT,
                bio TEXT,
                followers_count INTEGER,
                following_count INTEGER,
                posts_count INTEGER,
                is_private BOOLEAN,
                is_verified BOOLEAN,
                profile_pic_url TEXT,
                recent_posts TEXT,  -- JSON
                last_updated TIMESTAMP,
                data_version TEXT,
                INDEX(username),
                INDEX(last_updated)
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS cache_stats (
                date DATE PRIMARY KEY,
                api_calls_made INTEGER,
                api_calls_saved INTEGER,
                cache_hit_rate REAL,
                errors_count INTEGER
            )
        """)
        
        conn.commit()
        conn.close()

    def get_profile_data(self, account_id: int, 
                        freshness: DataFreshness = DataFreshness.NORMAL,
                        force_refresh: bool = False) -> Optional[ProfileSnapshot]:
        """
        Получить данные профиля с учетом требований свежести
        
        Args:
            account_id: ID аккаунта
            freshness: Требуемый уровень свежести данных
            force_refresh: Принудительное обновление из API
            
        Returns:
            ProfileSnapshot или None
        """
        if not force_refresh:
            # Сначала пытаемся получить из кэша
            cached = self._get_from_cache(account_id, freshness)
            if cached:
                return cached
        
        # Если кэш не подходит - обновляем из API
        return self._refresh_from_api(account_id)
    
    def _get_from_cache(self, account_id: int, 
                       freshness: DataFreshness) -> Optional[ProfileSnapshot]:
        """Получить данные из кэша"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cutoff_time = datetime.now() - timedelta(seconds=freshness.value)
        
        cursor.execute("""
            SELECT * FROM profile_snapshots 
            WHERE account_id = ? AND last_updated > ?
        """, (account_id, cutoff_time))
        
        row = cursor.fetchone()
        conn.close()
        
        if row:
            return self._row_to_snapshot(row)
        return None
    
    def _refresh_from_api(self, account_id: int) -> Optional[ProfileSnapshot]:
        """Обновить данные из Instagram API"""
        try:
            # Здесь вызов реального Instagram API
            # from instagram.client_adapter import get_instagram_client
            # client = get_instagram_client(account_id)
            # user_info = client.user_info(account_id)
            
            # Временная заглушка
            snapshot = ProfileSnapshot(
                account_id=account_id,
                username=f"user_{account_id}",
                full_name="Test User",
                bio="Test bio",
                followers_count=1000,
                following_count=500,
                posts_count=50,
                is_private=False,
                is_verified=False,
                profile_pic_url="",
                recent_posts=[],
                last_updated=datetime.now()
            )
            
            # Сохраняем в кэш
            self._save_to_cache(snapshot)
            return snapshot
            
        except Exception as e:
            print(f"Ошибка обновления из API: {e}")
            return None
    
    def _save_to_cache(self, snapshot: ProfileSnapshot):
        """Сохранить данные в кэш"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT OR REPLACE INTO profile_snapshots VALUES 
            (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            snapshot.account_id,
            snapshot.username,
            snapshot.full_name,
            snapshot.bio,
            snapshot.followers_count,
            snapshot.following_count,
            snapshot.posts_count,
            snapshot.is_private,
            snapshot.is_verified,
            snapshot.profile_pic_url,
            json.dumps(snapshot.recent_posts),
            snapshot.last_updated,
            snapshot.data_version
        ))
        
        conn.commit()
        conn.close()
    
    def _row_to_snapshot(self, row) -> ProfileSnapshot:
        """Конвертировать строку БД в ProfileSnapshot"""
        return ProfileSnapshot(
            account_id=row[0],
            username=row[1],
            full_name=row[2],
            bio=row[3],
            followers_count=row[4],
            following_count=row[5],
            posts_count=row[6],
            is_private=bool(row[7]),
            is_verified=bool(row[8]),
            profile_pic_url=row[9],
            recent_posts=json.loads(row[10] or '[]'),
            last_updated=datetime.fromisoformat(row[11]),
            data_version=row[12]
        )

# ПРИМЕРЫ ИСПОЛЬЗОВАНИЯ

class SmartCacheManager:
    """Умный менеджер кэша с адаптивными стратегиями"""
    
    def __init__(self):
        self.cache = SnapshotCache()
        self.strategies = {
            'profile_setup': DataFreshness.DAILY,      # Настройка профиля - раз в день
            'warmup': DataFreshness.NORMAL,            # Прогрев - час актуальности  
            'analytics': DataFreshness.WEEKLY,         # Аналитика - неделя
            'publishing': DataFreshness.FRESH,         # Публикация - 5 минут
            'verification': DataFreshness.REAL_TIME    # Проверка - только живые данные
        }
    
    def get_for_task(self, account_id: int, task_type: str) -> Optional[ProfileSnapshot]:
        """Получить данные для конкретной задачи"""
        freshness = self.strategies.get(task_type, DataFreshness.NORMAL)
        return self.cache.get_profile_data(account_id, freshness)

# ИНТЕГРАЦИЯ С СУЩЕСТВУЮЩИМИ МИКРОСЕРВИСАМИ

class ProfileSetupOptimized:
    """Оптимизированная настройка профилей через snapshot"""
    
    def __init__(self):
        self.cache_manager = SmartCacheManager()
    
    def update_bio(self, account_id: int, new_bio: str):
        """Обновление bio с проверкой через кэш"""
        # Получаем текущие данные из кэша (суточной давности достаточно)
        current_data = self.cache_manager.get_for_task(account_id, 'profile_setup')
        
        if current_data and current_data.bio == new_bio:
            print(f"Bio уже актуальное для аккаунта {account_id}, пропускаем API запрос")
            return True
        
        # Только если bio отличается - делаем реальный запрос
        return self._update_bio_via_api(account_id, new_bio)
    
    def _update_bio_via_api(self, account_id: int, new_bio: str):
        """Реальное обновление через API"""
        # Здесь вызов Instagram API
        pass

# СТАТИСТИКА И МОНИТОРИНГ

class CacheAnalytics:
    """Аналитика эффективности кэша"""
    
    def __init__(self, cache: SnapshotCache):
        self.cache = cache
    
    def get_efficiency_report(self) -> Dict[str, Any]:
        """Отчет об эффективности кэширования"""
        conn = sqlite3.connect(self.cache.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                SUM(api_calls_made) as total_made,
                SUM(api_calls_saved) as total_saved,
                AVG(cache_hit_rate) as avg_hit_rate
            FROM cache_stats 
            WHERE date >= date('now', '-7 days')
        """)
        
        stats = cursor.fetchone()
        conn.close()
        
        if stats and stats[0]:
            total_made, total_saved, avg_hit_rate = stats
            return {
                'api_calls_made': total_made,
                'api_calls_saved': total_saved,
                'total_calls_would_be': total_made + total_saved,
                'savings_percentage': (total_saved / (total_made + total_saved)) * 100,
                'average_hit_rate': avg_hit_rate,
                'estimated_time_saved_hours': total_saved * 0.5 / 3600  # ~0.5 сек на запрос
            }
        
        return {'error': 'Недостаточно данных'}

# ПРИМЕР КОНФИГУРАЦИИ СТРАТЕГИЙ

CACHE_STRATEGIES = {
    # Критичные данные - только свежие
    'account_verification': DataFreshness.REAL_TIME,
    'login_check': DataFreshness.REAL_TIME,
    
    # Публикация - нужны относительно свежие данные  
    'post_publishing': DataFreshness.FRESH,
    'story_publishing': DataFreshness.FRESH,
    
    # Прогрев - можно использовать часовые данные
    'warmup_activity': DataFreshness.NORMAL,
    'follow_activity': DataFreshness.NORMAL,
    
    # Настройка профилей - суточные данные подходят
    'bio_update': DataFreshness.DAILY,
    'avatar_update': DataFreshness.DAILY,
    'username_check': DataFreshness.DAILY,
    
    # Аналитика - можно использовать недельные данные
    'growth_analytics': DataFreshness.WEEKLY,
    'audience_analysis': DataFreshness.WEEKLY,
}

if __name__ == "__main__":
    # Тестирование системы
    cache = SnapshotCache()
    manager = SmartCacheManager()
    
    # Пример получения данных для разных задач
    profile_data = manager.get_for_task(123, 'profile_setup')
    verification_data = manager.get_for_task(123, 'verification')
    
    print("Snapshot Cache System готова к работе!") 
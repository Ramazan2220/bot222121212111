"""
Модуль для обработки публикаций в Instagram через Telegram бота

Использует монолитный файл publish_handlers.py для всех типов публикации
"""

import logging
from typing import List

logger = logging.getLogger(__name__)


def get_publish_handlers() -> List:
    """
    Возвращает все обработчики для публикации контента из монолитного файла
    
    Returns:
        List: Список всех обработчиков публикации
    """
    handlers = []
    
    try:
        # Импортируем из монолитного файла publish_handlers.py
        from .. import publish_handlers
        
        # Получаем обработчики публикации
        if hasattr(publish_handlers, 'get_publish_handlers'):
            handlers = publish_handlers.get_publish_handlers()
            logger.info(f"✅ Loaded {len(handlers)} handlers from monolithic publish_handlers.py")
        else:
            logger.warning("❌ Function get_publish_handlers not found in publish_handlers.py")
            
    except ImportError as e:
        logger.error(f"❌ Failed to load publish handlers: {e}")
    except Exception as e:
        logger.error(f"❌ Error loading publish handlers: {e}")
    
    logger.info(f"📊 Total publish handlers loaded: {len(handlers)}")
    
    return handlers


# Экспортируем функции из монолитного файла для обратной совместимости
def start_post_publish(update, context):
    """Запуск публикации поста"""
    try:
        from .. import publish_handlers
        return publish_handlers.start_post_publish(update, context)
    except Exception as e:
        logger.error(f"Error calling start_post_publish: {e}")
        return None


def start_story_publish(update, context):
    """Запуск публикации истории"""
    try:
        from .. import publish_handlers
        return publish_handlers.start_story_publish(update, context)
    except Exception as e:
        logger.error(f"Error calling start_story_publish: {e}")
        return None


def start_reels_publish(update, context):
    """Запуск публикации Reels"""
    try:
        from .. import publish_handlers
        return publish_handlers.start_reels_publish(update, context)
    except Exception as e:
        logger.error(f"Error calling start_reels_publish: {e}")
        return None


def start_igtv_publish(update, context):
    """Запуск публикации IGTV"""
    try:
        from .. import publish_handlers
        return publish_handlers.start_igtv_publish(update, context)
    except Exception as e:
        logger.error(f"Error calling start_igtv_publish: {e}")
        return None


def show_scheduled_posts(update, context):
    """Показ запланированных публикаций"""
    try:
        from .. import publish_handlers
        return publish_handlers.show_scheduled_posts(update, context)
    except Exception as e:
        logger.error(f"Error calling show_scheduled_posts: {e}")
        return None


def show_publication_history(update, context):
    """Показ истории публикаций"""
    try:
        from .. import publish_handlers
        return publish_handlers.show_publication_history(update, context)
    except Exception as e:
        logger.error(f"Error calling show_publication_history: {e}")
        return None


__all__ = [
    'get_publish_handlers',
    'start_post_publish',
    'start_story_publish',
    'start_reels_publish',
    'start_igtv_publish',
    'show_scheduled_posts',
    'show_publication_history'
] 
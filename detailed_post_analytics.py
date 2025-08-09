#!/usr/bin/env python3
"""
Детальная аналитика для каждого отдельного поста, сторис и рилса в Instagram
"""

import sys
import os
from datetime import datetime, timedelta
import json

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database.db_manager import get_instagram_accounts
from instagrapi import Client

def get_detailed_media_info(client, media):
    """Получить детальную информацию о медиа"""
    info = {
        'id': media.id,
        'pk': media.pk,
        'code': media.code,  # Короткий код для URL
        'url': f"https://www.instagram.com/p/{media.code}/",
        'type': media.media_type.name if hasattr(media.media_type, 'name') else str(media.media_type),
        'taken_at': media.taken_at.strftime('%Y-%m-%d %H:%M:%S'),
        'caption': media.caption_text[:100] + '...' if media.caption_text and len(media.caption_text) > 100 else media.caption_text,
        
        # Основная статистика
        'like_count': media.like_count,
        'comment_count': media.comment_count,
        'view_count': getattr(media, 'view_count', 0),
        'play_count': getattr(media, 'play_count', 0),
        
        # Дополнительная информация
        'has_liked': getattr(media, 'has_liked', False),
        'can_viewer_save': getattr(media, 'can_viewer_save', False),
        'can_viewer_reshare': getattr(media, 'can_viewer_reshare', False),
        
        # Геолокация
        'location': media.location.name if media.location else None,
        
        # Упоминания и хештеги
        'user_tags': len(media.usertags) if media.usertags else 0,
        'sponsor_tags': len(media.sponsor_tags) if media.sponsor_tags else 0,
    }
    
    # Для карусели - количество слайдов
    if hasattr(media, 'resources') and media.resources:
        info['carousel_count'] = len(media.resources)
    
    return info

def analyze_individual_posts(client, username, limit=5):
    """Анализ каждого отдельного поста"""
    print(f"📸 ДЕТАЛЬНАЯ АНАЛИТИКА ПОСТОВ ДЛЯ @{username}")
    print("=" * 70)
    
    try:
        # Получаем посты пользователя
        user_id = client.user_id_from_username(username)
        medias = client.user_medias(user_id, amount=limit)
        
        if not medias:
            print("❌ Посты не найдены")
            return
        
        print(f"📊 Найдено {len(medias)} постов для анализа:")
        print()
        
        for i, media in enumerate(medias, 1):
            info = get_detailed_media_info(client, media)
            
            print(f"📸 ПОСТ #{i}")
            print(f"🔗 URL: {info['url']}")
            print(f"📅 Дата: {info['taken_at']}")
            print(f"📝 Тип: {info['type']}")
            
            if info['caption']:
                print(f"💬 Подпись: {info['caption']}")
            
            print(f"❤️  Лайки: {info['like_count']:,}")
            print(f"💬 Комментарии: {info['comment_count']:,}")
            
            if info['view_count'] > 0:
                print(f"👁️  Просмотры: {info['view_count']:,}")
            
            if info['play_count'] > 0:
                print(f"▶️  Воспроизведения: {info['play_count']:,}")
            
            if info['carousel_count']:
                print(f"🎠 Слайдов в карусели: {info['carousel_count']}")
            
            if info['user_tags'] > 0:
                print(f"👥 Отмечено людей: {info['user_tags']}")
            
            if info['location']:
                print(f"📍 Локация: {info['location']}")
            
            # Расчет вовлеченности
            engagement = info['like_count'] + info['comment_count']
            print(f"🔥 Вовлеченность: {engagement:,}")
            
            # Коэффициент взаимодействия на просмотр (для видео)
            if info['view_count'] > 0:
                interaction_rate = (engagement / info['view_count']) * 100
                print(f"📈 Коэффициент взаимодействия: {interaction_rate:.2f}%")
            
            print("-" * 50)
            print()
        
    except Exception as e:
        print(f"❌ Ошибка анализа постов: {e}")

def analyze_individual_stories(client, username):
    """Анализ каждой отдельной истории"""
    print(f"📱 ДЕТАЛЬНАЯ АНАЛИТИКА ИСТОРИЙ ДЛЯ @{username}")
    print("=" * 70)
    
    try:
        user_id = client.user_id_from_username(username)
        stories = client.user_stories(user_id)
        
        if not stories:
            print("❌ Активные истории не найдены")
            return
        
        print(f"📊 Найдено {len(stories)} активных историй:")
        print()
        
        for i, story in enumerate(stories, 1):
            print(f"📱 ИСТОРИЯ #{i}")
            print(f"🆔 ID: {story.id}")
            print(f"📅 Опубликовано: {story.taken_at.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"⏰ Истекает: {story.expires_at.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"📝 Тип: {story.media_type.name if hasattr(story.media_type, 'name') else str(story.media_type)}")
            
            # Статистика просмотров (только для собственных историй)
            if hasattr(story, 'view_count'):
                print(f"👁️  Просмотры: {story.view_count:,}")
            
            # Проверка на наличие ссылок, стикеров и т.д.
            if hasattr(story, 'story_links') and story.story_links:
                print(f"🔗 Ссылки: {len(story.story_links)}")
            
            if hasattr(story, 'story_polls') and story.story_polls:
                print(f"📊 Опросы: {len(story.story_polls)}")
            
            if hasattr(story, 'story_questions') and story.story_questions:
                print(f"❓ Вопросы: {len(story.story_questions)}")
            
            print("-" * 50)
            print()
        
    except Exception as e:
        print(f"❌ Ошибка анализа историй: {e}")

def analyze_individual_reels(client, username, limit=5):
    """Анализ каждого отдельного рилса"""
    print(f"🎥 ДЕТАЛЬНАЯ АНАЛИТИКА РИЛСОВ ДЛЯ @{username}")
    print("=" * 70)
    
    try:
        user_id = client.user_id_from_username(username)
        medias = client.user_medias(user_id, amount=20)  # Берем больше для поиска рилсов
        
        # Фильтруем только рилсы
        reels = [media for media in medias if hasattr(media, 'media_type') and 
                str(media.media_type) in ['8', 'MediaType.CLIPS']]  # Рилсы имеют тип 8
        
        if not reels:
            print("❌ Рилсы не найдены")
            return
        
        reels = reels[:limit]  # Ограничиваем количество
        print(f"📊 Найдено {len(reels)} рилсов для анализа:")
        print()
        
        for i, reel in enumerate(reels, 1):
            info = get_detailed_media_info(client, reel)
            
            print(f"🎥 РИЛС #{i}")
            print(f"🔗 URL: {info['url']}")
            print(f"📅 Дата: {info['taken_at']}")
            
            if info['caption']:
                print(f"💬 Подпись: {info['caption']}")
            
            print(f"❤️  Лайки: {info['like_count']:,}")
            print(f"💬 Комментарии: {info['comment_count']:,}")
            print(f"👁️  Просмотры: {info['view_count']:,}")
            
            # Безопасное отображение воспроизведений
            if info['play_count'] and info['play_count'] > 0:
                print(f"▶️  Воспроизведения: {info['play_count']:,}")
            else:
                print(f"▶️  Воспроизведения: 0")
            
            if info['user_tags'] > 0:
                print(f"👥 Отмечено людей: {info['user_tags']}")
            
            if info['location']:
                print(f"📍 Локация: {info['location']}")
            
            # Расчет метрик для рилсов
            engagement = info['like_count'] + info['comment_count']
            print(f"🔥 Вовлеченность: {engagement:,}")
            
            if info['view_count'] > 0:
                # Безопасный расчет коэффициента досмотра
                if info['play_count'] and info['play_count'] > 0:
                    view_rate = (info['play_count'] / info['view_count']) * 100
                    print(f"📺 Коэффициент досмотра: {view_rate:.2f}%")
                
                interaction_rate = (engagement / info['view_count']) * 100
                print(f"📈 Коэффициент взаимодействия: {interaction_rate:.2f}%")
            
            print("-" * 50)
            print()
        
    except Exception as e:
        print(f"❌ Ошибка анализа рилсов: {e}")

def main():
    """Основная функция для запуска всех видов аналитики"""
    username = 'qmichelle_mepey0347'
    
    # Находим аккаунт
    accounts = get_instagram_accounts()
    target_account = None
    
    for account in accounts:
        if account.username == username:
            target_account = account
            break
    
    if not target_account:
        print(f'❌ Аккаунт @{username} не найден')
        return
    
    print(f'🎯 ДЕТАЛЬНАЯ АНАЛИТИКА ДЛЯ @{username}')
    print('=' * 80)
    
    try:
        # Создаем клиент без прокси
        client = Client()
        
        # Логинимся
        login_result = client.login(target_account.username, target_account.password)
        
        if login_result:
            print('✅ Успешный вход в аккаунт!')
            print()
            
            # Анализируем каждый тип контента отдельно
            analyze_individual_posts(client, username, limit=3)
            print()
            
            analyze_individual_stories(client, username)
            print()
            
            analyze_individual_reels(client, username, limit=3)
            
        else:
            print('❌ Не удалось войти в аккаунт')
    
    except Exception as e:
        print(f'❌ Общая ошибка: {e}')

if __name__ == "__main__":
    main() 
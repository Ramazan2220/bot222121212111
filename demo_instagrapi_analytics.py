#!/usr/bin/env python3
"""
Демонстрация возможностей сбора статистики публикаций с помощью instagrapi
"""

import sys
import os
from datetime import datetime, timedelta

# Добавляем путь для импорта модулей
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database.db_manager import get_instagram_accounts
from instagram.client import get_instagram_client

def demo_post_analytics(client, username):
    """Демонстрация аналитики постов"""
    print("📸 АНАЛИТИКА ПОСТОВ:")
    print("-" * 40)
    
    try:
        # Получаем информацию о профиле
        user_info = client.account_info()
        print(f"👤 Аккаунт: @{user_info.username}")
        print(f"📊 Всего постов: {user_info.media_count}")
        print(f"👥 Подписчики: {user_info.follower_count}")
        print(f"➡️ Подписки: {user_info.following_count}")
        
        # Получаем последние посты
        medias = client.user_medias(user_info.pk, amount=5)
        
        print(f"\n📈 Статистика последних {len(medias)} постов:")
        
        total_likes = 0
        total_comments = 0
        
        for i, media in enumerate(medias, 1):
            print(f"\n{i}. Пост ID: {media.id}")
            print(f"   📅 Дата: {media.taken_at.strftime('%d.%m.%Y %H:%M')}")
            print(f"   ❤️ Лайки: {media.like_count}")
            print(f"   💬 Комментарии: {media.comment_count}")
            print(f"   📝 Подпись: {media.caption_text[:50]}..." if media.caption_text else "   📝 Без подписи")
            
            # Собираем статистику
            total_likes += media.like_count
            total_comments += media.comment_count
            
            # Дополнительная информация
            if hasattr(media, 'view_count') and media.view_count:
                print(f"   👀 Просмотры: {media.view_count}")
                
        print(f"\n📊 ИТОГОВАЯ СТАТИСТИКА:")
        print(f"   Всего лайков: {total_likes}")
        print(f"   Всего комментариев: {total_comments}")
        print(f"   Средние лайки на пост: {total_likes / len(medias):.1f}")
        print(f"   Средние комментарии на пост: {total_comments / len(medias):.1f}")
        
    except Exception as e:
        print(f"❌ Ошибка при получении статистики постов: {e}")

def demo_stories_analytics(client, username):
    """Демонстрация аналитики Stories"""
    print("\n\n📱 АНАЛИТИКА STORIES:")
    print("-" * 40)
    
    try:
        user_info = client.account_info()
        
        # Получаем Stories пользователя
        stories = client.user_stories(user_info.pk)
        
        if stories:
            print(f"📺 Активных Stories: {len(stories)}")
            
            for i, story in enumerate(stories, 1):
                print(f"\n{i}. Story ID: {story.id}")
                print(f"   📅 Дата: {story.taken_at.strftime('%d.%m.%Y %H:%M')}")
                print(f"   ⏰ Истекает: {story.expires_at.strftime('%d.%m.%Y %H:%M')}")
                
                # Получаем детальную информацию о story
                try:
                    story_info = client.story_info(story.id)
                    if hasattr(story_info, 'view_count'):
                        print(f"   👀 Просмотры: {story_info.view_count}")
                    if hasattr(story_info, 'viewer_count'):
                        print(f"   👥 Зрители: {story_info.viewer_count}")
                except:
                    print("   📊 Детальная статистика недоступна")
        else:
            print("📺 Активных Stories нет")
            
        # Получаем архивные Stories (highlights)
        try:
            highlights = client.user_highlights(user_info.pk)
            if highlights:
                print(f"\n✨ Актуальные Stories (Highlights): {len(highlights)}")
                for highlight in highlights[:3]:  # Показываем первые 3
                    print(f"   - {highlight.title}: {highlight.media_count} историй")
        except:
            print("\n✨ Highlights недоступны")
            
    except Exception as e:
        print(f"❌ Ошибка при получении статистики Stories: {e}")

def demo_reels_analytics(client, username):
    """Демонстрация аналитики Reels"""
    print("\n\n🎥 АНАЛИТИКА REELS:")
    print("-" * 40)
    
    try:
        user_info = client.account_info()
        
        # Получаем все медиа и фильтруем Reels
        medias = client.user_medias(user_info.pk, amount=20)
        reels = [media for media in medias if media.media_type == 2 and hasattr(media, 'video_url')]
        
        print(f"🎬 Найдено Reels: {len(reels)}")
        
        if reels:
            total_views = 0
            total_likes = 0
            total_comments = 0
            
            for i, reel in enumerate(reels[:5], 1):  # Показываем первые 5
                print(f"\n{i}. Reels ID: {reel.id}")
                print(f"   📅 Дата: {reel.taken_at.strftime('%d.%m.%Y %H:%M')}")
                print(f"   ❤️ Лайки: {reel.like_count}")
                print(f"   💬 Комментарии: {reel.comment_count}")
                
                # Для Reels доступны дополнительные метрики
                if hasattr(reel, 'view_count') and reel.view_count:
                    print(f"   👀 Просмотры: {reel.view_count}")
                    total_views += reel.view_count
                    
                if hasattr(reel, 'play_count') and reel.play_count:
                    print(f"   ▶️ Воспроизведения: {reel.play_count}")
                    
                total_likes += reel.like_count
                total_comments += reel.comment_count
                
                print(f"   📝 Подпись: {reel.caption_text[:50]}..." if reel.caption_text else "   📝 Без подписи")
                
            print(f"\n📊 ИТОГОВАЯ СТАТИСТИКА REELS:")
            print(f"   Всего просмотров: {total_views}")
            print(f"   Всего лайков: {total_likes}")
            print(f"   Всего комментариев: {total_comments}")
            if len(reels) > 0:
                print(f"   Средние просмотры: {total_views / len(reels[:5]):.1f}")
                print(f"   Средние лайки: {total_likes / len(reels[:5]):.1f}")
        else:
            print("🎬 Reels не найдены")
            
    except Exception as e:
        print(f"❌ Ошибка при получении статистики Reels: {e}")

def demo_engagement_analytics(client, username):
    """Демонстрация аналитики вовлеченности"""
    print("\n\n📈 АНАЛИТИКА ВОВЛЕЧЕННОСТИ:")
    print("-" * 40)
    
    try:
        user_info = client.account_info()
        medias = client.user_medias(user_info.pk, amount=10)
        
        if medias and user_info.follower_count > 0:
            total_engagement = 0
            
            for media in medias:
                # Рассчитываем вовлеченность (лайки + комментарии) / подписчики * 100
                engagement = ((media.like_count + media.comment_count) / user_info.follower_count) * 100
                total_engagement += engagement
                
            avg_engagement = total_engagement / len(medias)
            
            print(f"📊 Средняя вовлеченность: {avg_engagement:.2f}%")
            
            # Классификация уровня вовлеченности
            if avg_engagement >= 6:
                level = "🔥 Отличная"
            elif avg_engagement >= 3:
                level = "✅ Хорошая"
            elif avg_engagement >= 1:
                level = "⚠️ Средняя"
            else:
                level = "❌ Низкая"
                
            print(f"📈 Уровень вовлеченности: {level}")
            
            # Дополнительные метрики
            likes_per_follower = sum(m.like_count for m in medias) / len(medias) / user_info.follower_count * 100
            comments_per_follower = sum(m.comment_count for m in medias) / len(medias) / user_info.follower_count * 100
            
            print(f"❤️ Лайки на подписчика: {likes_per_follower:.3f}%")
            print(f"💬 Комментарии на подписчика: {comments_per_follower:.3f}%")
            
    except Exception as e:
        print(f"❌ Ошибка при расчете вовлеченности: {e}")

def demo_insights_analytics(client, username):
    """Демонстрация бизнес-аналитики (Insights)"""
    print("\n\n📊 БИЗНЕС-АНАЛИТИКА (INSIGHTS):")
    print("-" * 40)
    
    try:
        # Примечание: Insights доступны только для бизнес-аккаунтов
        user_info = client.account_info()
        
        print("⚠️ Insights доступны только для бизнес/creator аккаунтов")
        print("📋 Доступные метрики в instagrapi:")
        print("   - Охват (Reach)")
        print("   - Показы (Impressions)")  
        print("   - Активность профиля")
        print("   - Демография аудитории")
        print("   - Топ публикации")
        print("   - Активность Stories")
        
        # Попытка получить insights (может не работать для обычных аккаунтов)
        try:
            # Получаем insights за последнюю неделю
            end_date = datetime.now()
            start_date = end_date - timedelta(days=7)
            
            # Это работает только для бизнес-аккаунтов
            # insights = client.insights_account(start_date, end_date)
            # print(f"📈 Insights за неделю: {insights}")
            
            print("\n💡 Для получения insights необходимо:")
            print("   1. Переключить аккаунт в бизнес-режим")
            print("   2. Использовать официальный API")
            print("   3. Иметь определенное количество подписчиков")
            
        except Exception as insights_error:
            print(f"📊 Insights недоступны: {insights_error}")
            
    except Exception as e:
        print(f"❌ Ошибка при получении Insights: {e}")

def main():
    """Основная функция демонстрации"""
    print("🎯 ДЕМОНСТРАЦИЯ ВОЗМОЖНОСТЕЙ АНАЛИТИКИ INSTAGRAPI")
    print("=" * 60)
    
    # Получаем первый доступный аккаунт
    accounts = get_instagram_accounts()
    
    if not accounts:
        print("❌ Аккаунты не найдены")
        return
    
    account = accounts[0]  # Берем первый аккаунт
    
    print(f"🧪 Тестирование на аккаунте: @{account.username}")
    print(f"🔧 Создание Instagram клиента...")
    
    try:
        client = get_instagram_client(account.id, skip_recovery=True)
        
        if not client:
            print("❌ Не удалось создать Instagram клиент")
            return
        
        print("✅ Клиент создан успешно!")
        print("\n" + "="*60)
        
        # Запускаем все демонстрации
        demo_post_analytics(client, account.username)
        demo_stories_analytics(client, account.username)
        demo_reels_analytics(client, account.username)
        demo_engagement_analytics(client, account.username)
        demo_insights_analytics(client, account.username)
        
        print("\n" + "="*60)
        print("🎉 ДЕМОНСТРАЦИЯ ЗАВЕРШЕНА!")
        print("\n💡 ВОЗМОЖНОСТИ INSTAGRAPI ДЛЯ АНАЛИТИКИ:")
        print("   ✅ Статистика постов (лайки, комментарии, просмотры)")
        print("   ✅ Аналитика Stories (просмотры, срок действия)")
        print("   ✅ Метрики Reels (воспроизведения, вовлеченность)")
        print("   ✅ Расчет коэффициента вовлеченности")
        print("   ✅ Анализ профиля (подписчики, подписки)")
        print("   ✅ Бизнес-аналитика (для бизнес-аккаунтов)")
        print("   ✅ Исторические данные публикаций")
        print("   ✅ Сравнительный анализ контента")
        
    except Exception as e:
        print(f"❌ Ошибка при работе с аккаунтом: {e}")

if __name__ == "__main__":
    main() 
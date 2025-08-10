"""
Обработчики аналитики для Telegram бота
"""
import os
import logging
from datetime import datetime, timedelta
import tempfile
from typing import List, Dict, Any
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ParseMode
from telegram.ext import CallbackContext, ConversationHandler

from database.db_manager import get_instagram_accounts, get_instagram_account
from telegram_bot.utils.account_selection import create_account_selector

logger = logging.getLogger(__name__)

# Создаем селектор аккаунтов для аналитики
analytics_selector = create_account_selector(
    callback_prefix="analytics_pub",
    title="📊 Аналитика публикаций",
    allow_multiple=True,  # Разрешаем выбор нескольких аккаунтов для сравнительной аналитики
    show_status=True,
    show_folders=True,
    back_callback="analytics_menu"
)

def get_analytics_handlers():
    """Возвращает обработчики для аналитики"""
    from telegram.ext import CallbackQueryHandler
    
    return [
        analytics_selector.get_conversation_handler(),  # Селектор для аналитики
        # Остальные обработчики уже зарегистрированы в основном callback_handler
    ]

# Состояния для аналитики
ANALYTICS_ACCOUNT_SELECT = 1
ANALYTICS_ACTION_SELECT = 2

def start_publications_analytics(update: Update, context: CallbackContext):
    """Начинает процесс аналитики публикаций"""
    query = update.callback_query
    query.answer()
    
    # Callback для обработки выбранных аккаунтов
    def on_account_selected(account_ids: List[int], update_inner: Update, context_inner: CallbackContext):
        if account_ids:
            # Сохраняем выбранные аккаунты
            context_inner.user_data['analytics_account_ids'] = account_ids
            context_inner.user_data['analytics_multiple_accounts'] = len(account_ids) > 1
            
            # Получаем информацию об аккаунтах
            accounts = [get_instagram_account(acc_id) for acc_id in account_ids]
            usernames = [acc.username for acc in accounts if acc]
            context_inner.user_data['analytics_account_usernames'] = usernames
            
            if len(account_ids) == 1:
                # Один аккаунт - сохраняем для совместимости
                context_inner.user_data['analytics_account_id'] = account_ids[0]
                context_inner.user_data['analytics_account_username'] = usernames[0]
            
            # Показываем меню действий аналитики
            show_analytics_actions_menu(update_inner, context_inner)
    
    return analytics_selector.start_selection(update, context, on_account_selected)

def show_analytics_actions_menu(update: Update, context: CallbackContext):
    """Показывает меню действий аналитики для выбранных аккаунтов"""
    query = update.callback_query if hasattr(update, 'callback_query') and update.callback_query else None
    
    # Проверяем, выбрано ли несколько аккаунтов
    multiple_accounts = context.user_data.get('analytics_multiple_accounts', False)
    account_ids = context.user_data.get('analytics_account_ids', [])
    usernames = context.user_data.get('analytics_account_usernames', [])
    
    # Формируем заголовок
    if multiple_accounts:
        text = f"📊 Аналитика для {len(account_ids)} аккаунтов\n"
        text += f"Аккаунты: {', '.join([f'@{u}' for u in usernames[:3]])}"
        if len(usernames) > 3:
            text += f" и ещё {len(usernames) - 3}..."
        text += "\n\nВыберите тип анализа:"
        
        # Меню для множественного анализа
        keyboard = [
            [InlineKeyboardButton("📊 Сравнительная аналитика", callback_data="analytics_comparison")],
            [InlineKeyboardButton("📈 Сводная статистика", callback_data="analytics_summary")],
            [InlineKeyboardButton("🏆 Лучшие посты всех аккаунтов", callback_data="analytics_top_all")],
            [InlineKeyboardButton("📋 Детальный отчет по всем", callback_data="analytics_detailed_all")],
            [InlineKeyboardButton("🔙 Назад к выбору аккаунта", callback_data="publications_stats")]
        ]
    else:
        # Один аккаунт - показываем стандартное меню
        account_username = context.user_data.get('analytics_account_username', 'Unknown')
        text = f"📊 Аналитика для @{account_username}\n\nВыберите тип анализа:"
        
        keyboard = [
            [InlineKeyboardButton("📅 Последние 10 постов", callback_data="analytics_recent_posts")],
            [InlineKeyboardButton("❤️ Топ по лайкам", callback_data="analytics_top_likes")],
            [InlineKeyboardButton("💬 Топ по комментариям", callback_data="analytics_top_comments")],
            [InlineKeyboardButton("📊 Детальный анализ", callback_data="analytics_detailed")],
            [InlineKeyboardButton("📈 Статистика историй", callback_data="analytics_stories")],
            [InlineKeyboardButton("🔙 Назад к выбору аккаунта", callback_data="publications_stats")]
        ]
    
    if query:
        query.edit_message_text(
            text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=None  # Отключаем парсинг entities чтобы избежать ошибок
        )
    else:
        update.message.reply_text(
            text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=None  # Отключаем парсинг entities чтобы избежать ошибок
        )

def handle_analytics_action(update: Update, context: CallbackContext, action: str):
    """Обрабатывает выбранное действие аналитики для одного аккаунта"""
    query = update.callback_query
    query.answer()
    
    # Поддерживаем как старый формат (один аккаунт), так и новый (множественный выбор)
    account_id = context.user_data.get('analytics_account_id')
    account_username = context.user_data.get('analytics_account_username', 'Unknown')
    
    # Если используется новый формат множественного выбора, берем первый аккаунт
    if not account_id:
        account_ids = context.user_data.get('analytics_account_ids', [])
        if account_ids:
            account_id = account_ids[0]
            usernames = context.user_data.get('analytics_account_usernames', [])
            if usernames:
                account_username = usernames[0]
    
    if not account_id:
        query.edit_message_text("❌ Ошибка: аккаунт не выбран")
        return
    
    # Показываем сообщение о начале анализа
    query.edit_message_text(
        f"🔄 Анализирую данные для @{account_username}...\n"
        f"Это может занять несколько минут.",
        parse_mode=None  # Отключаем парсинг entities чтобы избежать ошибок
    )
    
    try:
        # Выполняем анализ в зависимости от действия
        if action == "analytics_recent_posts":
            result_text = analyze_recent_posts(account_id, account_username)
            filename = f"recent_posts_{account_username}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
            
        elif action == "analytics_top_likes":
            result_text = analyze_top_posts_by_likes(account_id, account_username)
            filename = f"top_likes_{account_username}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
            
        elif action == "analytics_top_comments":
            result_text = analyze_top_posts_by_comments(account_id, account_username)
            filename = f"top_comments_{account_username}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
            
        elif action == "analytics_detailed":
            result_text = analyze_detailed_statistics(account_id, account_username)
            filename = f"detailed_analysis_{account_username}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
            
        elif action == "analytics_stories":
            result_text = analyze_stories_statistics(account_id, account_username)
            filename = f"stories_stats_{account_username}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
            
        else:
            query.edit_message_text("❌ Неизвестное действие")
            return
        
        # Создаем временный файл и отправляем
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False, encoding='utf-8') as f:
            f.write(result_text)
            temp_file_path = f.name
        
        # Отправляем файл
        with open(temp_file_path, 'rb') as f:
            query.message.reply_document(
                document=f,
                filename=filename,
                caption=f"📊 Аналитика для @{account_username}",
                parse_mode=None  # Отключаем парсинг entities чтобы избежать ошибок
            )
        
        # Удаляем временный файл
        os.unlink(temp_file_path)
        
        # Возвращаемся к меню действий
        show_analytics_actions_menu(update, context)
        
    except Exception as e:
        logger.error(f"Ошибка при анализе: {e}")
        query.edit_message_text(
            f"❌ Ошибка при анализе данных для @{account_username}:\n"
            f"{str(e)}\n\n"
            f"Возможные причины:\n"
            f"• Аккаунт требует авторизации\n"
            f"• Проблемы с подключением\n"
            f"• Аккаунт заблокирован или приватный",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Назад", callback_data="publications_stats")]
            ]),
            parse_mode=None  # Отключаем парсинг entities чтобы избежать ошибок
        )

def handle_multiple_analytics_action(update: Update, context: CallbackContext, action: str):
    """Обрабатывает действия множественной аналитики"""
    query = update.callback_query
    query.answer()
    
    account_ids = context.user_data.get('analytics_account_ids', [])
    usernames = context.user_data.get('analytics_account_usernames', [])
    
    if not account_ids:
        query.edit_message_text("❌ Ошибка: аккаунты не выбраны")
        return
    
    # Показываем сообщение о начале анализа
    query.edit_message_text(
        f"🔄 Анализирую данные для {len(account_ids)} аккаунтов...\n"
        f"Это может занять несколько минут.",
        parse_mode=None
    )
    
    try:
        # Выполняем анализ в зависимости от действия
        if action == "analytics_comparison":
            result_text = analyze_accounts_comparison(account_ids, usernames)
            filename = f"comparison_{len(account_ids)}_accounts_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
            
        elif action == "analytics_summary":
            result_text = analyze_accounts_summary(account_ids, usernames)
            filename = f"summary_{len(account_ids)}_accounts_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
            
        elif action == "analytics_top_all":
            result_text = analyze_top_posts_all_accounts(account_ids, usernames)
            filename = f"top_posts_all_{len(account_ids)}_accounts_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
            
        elif action == "analytics_detailed_all":
            result_text = analyze_detailed_all_accounts(account_ids, usernames)
            filename = f"detailed_all_{len(account_ids)}_accounts_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
            
        else:
            query.edit_message_text("❌ Неизвестное действие")
            return
        
        # Создаем временный файл и отправляем
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False, encoding='utf-8') as f:
            f.write(result_text)
            temp_file_path = f.name
        
        # Отправляем файл
        with open(temp_file_path, 'rb') as f:
            query.message.reply_document(
                document=f,
                filename=filename,
                caption=f"📊 Аналитика для {len(account_ids)} аккаунтов",
                parse_mode=None
            )
        
        # Удаляем временный файл
        os.unlink(temp_file_path)
        
        # Возвращаемся к меню действий
        show_analytics_actions_menu(update, context)
        
    except Exception as e:
        logger.error(f"Ошибка при множественном анализе: {e}")
        query.edit_message_text(
            f"❌ Ошибка при анализе данных для {len(account_ids)} аккаунтов:\n"
            f"{str(e)}\n\n"
            f"Возможные причины:\n"
            f"• Один или несколько аккаунтов требуют авторизации\n"
            f"• Проблемы с подключением\n"
            f"• Аккаунты заблокированы или приватные",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Назад", callback_data="publications_stats")]
            ]),
            parse_mode=None
        )

def get_authorized_client():
    """Получает авторизованного клиента из активных аккаунтов"""
    try:
        from database.db_manager import get_instagram_accounts
        from instagram.client import get_instagram_client
        import os
        
        # Получаем активные аккаунты
        accounts = get_instagram_accounts()
        active_accounts = [acc for acc in accounts if acc.is_active]
        
        if not active_accounts:
            return None, "❌ Нет активных аккаунтов для получения данных"
        
        # Пытаемся найти аккаунт с рабочей сессией
        for account in active_accounts:
            try:
                session_path = f"data/accounts/{account.id}/session.json"
                if os.path.exists(session_path):
                    # Используем функцию get_instagram_client с skip_recovery для быстрой проверки
                    client = get_instagram_client(account.id, skip_recovery=True)
                    
                    # Проверяем, что клиент работает
                    if client:
                        try:
                            # Быстрая проверка что клиент авторизован
                            client.account_info()
                            logger.info(f"Использую аккаунт {account.username} для аналитики")
                            return client, None
                        except Exception as test_error:
                            logger.warning(f"Клиент для {account.username} не работает: {test_error}")
                            continue
                        
            except Exception as e:
                logger.warning(f"Не удалось использовать аккаунт {account.username}: {e}")
                continue
        
        return None, "❌ Нет рабочих авторизованных аккаунтов"
        
    except Exception as e:
        return None, f"❌ Ошибка при получении клиента: {str(e)}"

def analyze_recent_posts(account_id: int, username: str) -> str:
    """Анализирует последние посты аккаунта"""
    try:
        # Получаем авторизованного клиента
        client, error = get_authorized_client()
        if not client:
            return f"📊 Аналитика последних постов для @{username}\n\n{error}"
        
        try:
            # Получаем информацию о пользователе (публичный доступ)
            user_info = client.user_info_by_username(username)
            user_id = user_info.pk
            
            # Получаем последние 10 постов (публичный доступ)
            medias = client.user_medias(user_id, amount=10)
            
            if not medias:
                return f"📊 Аналитика последних постов для @{username}\n\n❌ Посты не найдены или профиль закрыт"
            
            # Формируем отчет
            report = f"📊 ПОСЛЕДНИЕ {len(medias)} ПОСТОВ - @{username}\n"
            report += f"📅 Дата анализа: {datetime.now().strftime('%d.%m.%Y %H:%M')}\n"
            report += "=" * 50 + "\n\n"
            
            total_likes = 0
            total_comments = 0
            total_views = 0
            
            for i, media in enumerate(medias, 1):
                # Используем базовую информацию без детального анализа чтобы избежать логина
                report += f"📝 ПОСТ #{i}\n"
                report += f"🔗 URL: https://www.instagram.com/p/{media.code}/\n"
                report += f"📅 Дата: {media.taken_at.strftime('%Y-%m-%d %H:%M:%S')}\n"
                # Определяем тип медиа по числовому значению
                media_type_names = {1: 'Фото', 2: 'Видео', 8: 'Карусель'}
                media_type_name = media_type_names.get(media.media_type, f'Тип {media.media_type}')
                report += f"📝 Тип: {media_type_name}\n"
                
                if media.caption_text:
                    caption_preview = media.caption_text[:100] + "..." if len(media.caption_text) > 100 else media.caption_text
                    report += f"💬 Описание: {caption_preview}\n"
                
                report += f"❤️ Лайки: {media.like_count:,}\n"
                report += f"💬 Комментарии: {media.comment_count:,}\n"
                
                if hasattr(media, 'view_count') and media.view_count > 0:
                    report += f"👁️ Просмотры: {media.view_count:,}\n"
                    total_views += media.view_count
                
                if hasattr(media, 'resources') and media.resources:
                    report += f"🎠 Слайдов в карусели: {len(media.resources)}\n"
                
                # Подсчитываем общую статистику
                total_likes += media.like_count
                total_comments += media.comment_count
                
                report += "-" * 30 + "\n"
            
            # Добавляем общую статистику
            report += f"\n📈 ОБЩАЯ СТАТИСТИКА:\n"
            report += f"❤️ Всего лайков: {total_likes:,}\n"
            report += f"💬 Всего комментариев: {total_comments:,}\n"
            if total_views > 0:
                report += f"👁️ Всего просмотров: {total_views:,}\n"
            
            avg_likes = total_likes // len(medias) if medias else 0
            avg_comments = total_comments // len(medias) if medias else 0
            
            report += f"❤️ Средние лайки: {avg_likes:,}\n"
            report += f"💬 Средние комментарии: {avg_comments:,}\n"
            
            # Вычисляем engagement rate
            if user_info.follower_count > 0:
                engagement_rate = ((total_likes + total_comments) / len(medias)) / user_info.follower_count * 100
                report += f"📊 Средний ER: {engagement_rate:.2f}%\n"
            
            return report
            
        except Exception as api_error:
            return f"❌ Ошибка доступа к @{username}: {str(api_error)}\n\nВозможные причины:\n• Профиль приватный\n• Временные ограничения Instagram\n• Аккаунт не существует"
        
    except Exception as e:
        logger.error(f"Ошибка при анализе постов для {username}: {e}")
        return f"❌ Ошибка при анализе постов для @{username}: {str(e)}"

def analyze_top_posts_by_likes(account_id: int, username: str) -> str:
    """Анализирует топ постов по лайкам"""
    try:
        # Получаем авторизованного клиента
        client, error = get_authorized_client()
        if not client:
            return f"❤️ Топ постов по лайкам для @{username}\n\n{error}"
        
        try:
            user_info = client.user_info_by_username(username)
            user_id = user_info.pk
            
            # Получаем больше постов для анализа
            medias = client.user_medias(user_id, amount=30)
            
            if not medias:
                return f"📊 Топ постов по лайкам для @{username}\n\n❌ Посты не найдены"
            
            # Сортируем по лайкам
            medias_sorted = sorted(medias, key=lambda x: x.like_count, reverse=True)[:10]
            
            report = f"❤️ ТОП-{len(medias_sorted)} ПОСТОВ ПО ЛАЙКАМ - @{username}\n"
            report += f"📅 Дата анализа: {datetime.now().strftime('%d.%m.%Y %H:%M')}\n"
            report += f"📊 Проанализировано {len(medias)} постов\n"
            report += "=" * 50 + "\n\n"
            
            for i, media in enumerate(medias_sorted, 1):
                report += f"🏆 МЕСТО #{i}\n"
                report += f"🔗 URL: https://www.instagram.com/p/{media.code}/\n"
                report += f"📅 Дата: {media.taken_at.strftime('%Y-%m-%d %H:%M:%S')}\n"
                report += f"❤️ Лайки: {media.like_count:,}\n"
                report += f"💬 Комментарии: {media.comment_count:,}\n"
                
                if media.caption_text:
                    caption_preview = media.caption_text[:150] + "..." if len(media.caption_text) > 150 else media.caption_text
                    report += f"💬 Описание: {caption_preview}\n"
                
                report += "-" * 30 + "\n"
            
            return report
            
        except Exception as api_error:
            return f"❌ Ошибка доступа к @{username}: {str(api_error)}"
        
    except Exception as e:
        return f"❌ Ошибка при анализе топ постов для @{username}: {str(e)}"

def analyze_top_posts_by_comments(account_id: int, username: str) -> str:
    """Анализирует топ постов по комментариям"""
    try:
        # Получаем авторизованного клиента
        client, error = get_authorized_client()
        if not client:
            return f"💬 Топ постов по комментариям для @{username}\n\n{error}"
        
        try:
            user_info = client.user_info_by_username(username)
            user_id = user_info.pk
            
            medias = client.user_medias(user_id, amount=30)
            
            if not medias:
                return f"📊 Топ постов по комментариям для @{username}\n\n❌ Посты не найдены"
            
            # Сортируем по комментариям
            medias_sorted = sorted(medias, key=lambda x: x.comment_count, reverse=True)[:10]
            
            report = f"💬 ТОП-{len(medias_sorted)} ПОСТОВ ПО КОММЕНТАРИЯМ - @{username}\n"
            report += f"📅 Дата анализа: {datetime.now().strftime('%d.%m.%Y %H:%M')}\n"
            report += f"📊 Проанализировано {len(medias)} постов\n"
            report += "=" * 50 + "\n\n"
            
            for i, media in enumerate(medias_sorted, 1):
                report += f"🏆 МЕСТО #{i}\n"
                report += f"🔗 URL: https://www.instagram.com/p/{media.code}/\n"
                report += f"📅 Дата: {media.taken_at.strftime('%Y-%m-%d %H:%M:%S')}\n"
                report += f"💬 Комментарии: {media.comment_count:,}\n"
                report += f"❤️ Лайки: {media.like_count:,}\n"
                
                if media.caption_text:
                    caption_preview = media.caption_text[:150] + "..." if len(media.caption_text) > 150 else media.caption_text
                    report += f"💬 Описание: {caption_preview}\n"
                
                report += "-" * 30 + "\n"
            
            return report
            
        except Exception as api_error:
            return f"❌ Ошибка доступа к @{username}: {str(api_error)}"
        
    except Exception as e:
        return f"❌ Ошибка при анализе топ постов для @{username}: {str(e)}"

def analyze_detailed_statistics(account_id: int, username: str) -> str:
    """Детальная статистика аккаунта"""
    try:
        # Получаем авторизованного клиента
        client, error = get_authorized_client()
        if not client:
            return f"📊 Детальная статистика для @{username}\n\n{error}"
        
        try:
            user_info = client.user_info_by_username(username)
            user_id = user_info.pk
            
            # Получаем больше постов для детального анализа
            medias = client.user_medias(user_id, amount=50)
            
            if not medias:
                return f"📊 Детальная статистика для @{username}\n\n❌ Посты не найдены"
            
            # Анализируем все посты
            report = f"📊 ДЕТАЛЬНАЯ СТАТИСТИКА - @{username}\n"
            report += f"📅 Дата анализа: {datetime.now().strftime('%d.%m.%Y %H:%M')}\n"
            report += f"📊 Проанализировано постов: {len(medias)}\n"
            report += "=" * 50 + "\n\n"
            
            # Статистика профиля
            report += f"👤 ИНФОРМАЦИЯ О ПРОФИЛЕ:\n"
            report += f"📛 Имя: {user_info.full_name or 'Не указано'}\n"
            report += f"👥 Подписчики: {user_info.follower_count:,}\n"
            report += f"👤 Подписки: {user_info.following_count:,}\n"
            report += f"📝 Постов: {user_info.media_count:,}\n\n"
            
            # Анализ активности по типам контента
            # В instagrapi: 1 = PHOTO, 2 = VIDEO, 8 = ALBUM/CAROUSEL
            photos = sum(1 for m in medias if m.media_type == 1)
            videos = sum(1 for m in medias if m.media_type == 2) 
            carousels = sum(1 for m in medias if m.media_type == 8)
            
            report += f"📊 АНАЛИЗ КОНТЕНТА:\n"
            report += f"📷 Фото: {photos} ({photos/len(medias)*100:.1f}%)\n"
            report += f"🎥 Видео: {videos} ({videos/len(medias)*100:.1f}%)\n"
            report += f"🎠 Карусели: {carousels} ({carousels/len(medias)*100:.1f}%)\n\n"
            
            # Статистика вовлеченности
            total_likes = sum(m.like_count for m in medias)
            total_comments = sum(m.comment_count for m in medias)
            
            avg_likes = total_likes // len(medias)
            avg_comments = total_comments // len(medias)
            
            # Лучший и худший пост
            best_post = max(medias, key=lambda x: x.like_count + x.comment_count * 5)
            worst_post = min(medias, key=lambda x: x.like_count + x.comment_count * 5)
            
            report += f"📈 СТАТИСТИКА ВОВЛЕЧЕННОСТИ:\n"
            report += f"❤️ Всего лайков: {total_likes:,}\n"
            report += f"💬 Всего комментариев: {total_comments:,}\n"
            report += f"❤️ Средние лайки: {avg_likes:,}\n"
            report += f"💬 Средние комментарии: {avg_comments:,}\n"
            
            if user_info.follower_count > 0:
                engagement_rate = ((total_likes + total_comments) / len(medias)) / user_info.follower_count * 100
                report += f"📊 Средний ER: {engagement_rate:.2f}%\n"
            
            report += f"\n🏆 ЛУЧШИЙ ПОСТ: {best_post.like_count:,} лайков, {best_post.comment_count:,} комментариев\n"
            report += f"🔗 URL: https://www.instagram.com/p/{best_post.code}/\n"
            
            report += f"\n📉 ХУДШИЙ ПОСТ: {worst_post.like_count:,} лайков, {worst_post.comment_count:,} комментариев\n"
            report += f"🔗 URL: https://www.instagram.com/p/{worst_post.code}/\n"
            
            return report
            
        except Exception as api_error:
            return f"❌ Ошибка доступа к @{username}: {str(api_error)}"
        
    except Exception as e:
        return f"❌ Ошибка при детальном анализе для @{username}: {str(e)}"

def analyze_stories_statistics(account_id: int, username: str) -> str:
    """Анализ статистики историй"""
    try:
        # Получаем авторизованного клиента
        client, error = get_authorized_client()
        
        report = f"📱 СТАТИСТИКА ИСТОРИЙ - @{username}\n"
        report += f"📅 Дата анализа: {datetime.now().strftime('%d.%m.%Y %H:%M')}\n"
        report += "=" * 50 + "\n\n"
        
        if not client:
            report += f"{error}\n"
            return report
        
        try:
            user_info = client.user_info_by_username(username)
            user_id = user_info.pk
            
            # Пытаемся получить активные истории (публичный доступ ограничен)
            stories = client.user_stories(user_id)
            
            if not stories:
                report += "❌ Активных историй не найдено\n"
                report += "💡 Возможные причины:\n"
                report += "• Нет активных историй (старше 24 часов)\n"
                report += "• Приватный аккаунт\n" 
                report += "• Ограничения публичного доступа\n"
            else:
                report += f"📊 Найдено активных историй: {len(stories)}\n\n"
                
                for i, story in enumerate(stories, 1):
                    report += f"📱 ИСТОРИЯ #{i}\n"
                    report += f"📅 Дата: {story.taken_at.strftime('%d.%m.%Y %H:%M')}\n"
                    # Определяем тип медиа истории
                    story_type_names = {1: 'Фото', 2: 'Видео'}
                    story_type_name = story_type_names.get(story.media_type, f'Тип {story.media_type}')
                    report += f"📝 Тип: {story_type_name}\n"
                    report += f"👁️ Просмотры: недоступно (требуется быть владельцем)\n"
                    report += "-" * 30 + "\n"
            
        except Exception as api_error:
            report += f"❌ Ошибка доступа к историям: {str(api_error)}\n"
            report += "💡 Анализ историй требует особые права доступа\n"
        
        return report
        
    except Exception as e:
        return f"❌ Ошибка при анализе историй для @{username}: {str(e)}"

def start_accounts_analytics(update: Update, context: CallbackContext):
    """Аналитика по аккаунтам"""
    query = update.callback_query
    query.answer()
    
    try:
        accounts = get_instagram_accounts()
        
        report = f"👤 СТАТИСТИКА ПО АККАУНТАМ\n"
        report += f"📅 Дата анализа: {datetime.now().strftime('%d.%m.%Y %H:%M')}\n"
        report += "=" * 50 + "\n\n"
        
        if not accounts:
            report += "❌ Аккаунты не найдены в базе данных"
        else:
            active_accounts = [acc for acc in accounts if acc.is_active]
            inactive_accounts = [acc for acc in accounts if not acc.is_active]
            
            report += f"📊 ОБЩАЯ СТАТИСТИКА:\n"
            report += f"👥 Всего аккаунтов: {len(accounts)}\n"
            report += f"✅ Активных: {len(active_accounts)}\n"
            report += f"❌ Неактивных: {len(inactive_accounts)}\n\n"
            
            if active_accounts:
                report += f"✅ АКТИВНЫЕ АККАУНТЫ ({len(active_accounts)}):\n"
                for i, acc in enumerate(active_accounts, 1):
                    report += f"{i:2d}. @{acc.username}\n"
                    report += f"     📧 Email: {acc.email or 'Не указан'}\n"
                    report += f"     📅 Добавлен: {acc.created_at.strftime('%d.%m.%Y')}\n"
                    
                    # Проверяем наличие сессии
                    session_path = f"data/accounts/{acc.id}/session.json"
                    if os.path.exists(session_path):
                        report += f"     🔐 Сессия: ✅ Сохранена\n"
                    else:
                        report += f"     🔐 Сессия: ❌ Отсутствует\n"
                    
                    report += "\n"
            
            if inactive_accounts:
                report += f"❌ НЕАКТИВНЫЕ АККАУНТЫ ({len(inactive_accounts)}):\n"
                for i, acc in enumerate(inactive_accounts, 1):
                    report += f"{i:2d}. @{acc.username}\n"
                    report += f"     📧 Email: {acc.email or 'Не указан'}\n"
                    report += f"     📅 Добавлен: {acc.created_at.strftime('%d.%m.%Y')}\n"
                    report += "\n"
        
        # Создаем и отправляем файл
        filename = f"accounts_statistics_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False, encoding='utf-8') as f:
            f.write(report)
            temp_file_path = f.name
        
        with open(temp_file_path, 'rb') as f:
            query.message.reply_document(
                document=f,
                filename=filename,
                caption="👤 Статистика по аккаунтам"
            )
        
        os.unlink(temp_file_path)
        
        # Возвращаемся к меню статистики
        from telegram_bot.keyboards import get_statistics_menu_keyboard
        query.edit_message_text(
            "✅ Статистика по аккаунтам отправлена файлом",
            reply_markup=get_statistics_menu_keyboard()
        )
        
    except Exception as e:
        logger.error(f"Ошибка при анализе аккаунтов: {e}")
        query.edit_message_text(
            f"❌ Ошибка при анализе аккаунтов: {str(e)}",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Назад", callback_data="analytics_menu")]
            ])
        )

def analyze_accounts_comparison(account_ids: List[int], usernames: List[str]) -> str:
    """Сравнительная аналитика нескольких аккаунтов"""
    try:
        client, error = get_authorized_client()
        if not client:
            return f"📊 Сравнительная аналитика для {len(account_ids)} аккаунтов\n\n{error}"
        
        report = f"📊 СРАВНИТЕЛЬНАЯ АНАЛИТИКА\n"
        report += f"📅 Дата анализа: {datetime.now().strftime('%d.%m.%Y %H:%M')}\n"
        report += f"👥 Аккаунтов: {len(account_ids)}\n"
        report += "=" * 60 + "\n\n"
        
        accounts_data = []
        
        # Собираем данные по каждому аккаунту
        for i, username in enumerate(usernames):
            try:
                user_info = client.user_info_by_username(username)
                medias = client.user_medias(user_info.pk, amount=20)
                
                if medias:
                    total_likes = sum(m.like_count for m in medias)
                    total_comments = sum(m.comment_count for m in medias)
                    avg_likes = total_likes // len(medias)
                    avg_comments = total_comments // len(medias)
                    
                    # Определяем типы контента
                    photos = sum(1 for m in medias if m.media_type == 1)
                    videos = sum(1 for m in medias if m.media_type == 2)
                    carousels = sum(1 for m in medias if m.media_type == 8)
                    
                    accounts_data.append({
                        'username': username,
                        'followers': user_info.follower_count,
                        'following': user_info.following_count,
                        'posts_count': user_info.media_count,
                        'analyzed_posts': len(medias),
                        'total_likes': total_likes,
                        'total_comments': total_comments,
                        'avg_likes': avg_likes,
                        'avg_comments': avg_comments,
                        'photos': photos,
                        'videos': videos,
                        'carousels': carousels,
                        'engagement_rate': (avg_likes + avg_comments) / user_info.follower_count * 100 if user_info.follower_count > 0 else 0
                    })
                    
            except Exception as e:
                logger.warning(f"Ошибка при анализе {username}: {e}")
                accounts_data.append({
                    'username': username,
                    'error': str(e)
                })
        
        # Формируем сравнительный отчет
        if accounts_data:
            report += "📊 СРАВНЕНИЕ АККАУНТОВ:\n\n"
            
            for i, data in enumerate(accounts_data, 1):
                if 'error' in data:
                    report += f"{i:2d}. @{data['username']} - ❌ Ошибка: {data['error']}\n"
                    continue
                
                report += f"{i:2d}. @{data['username']}\n"
                report += f"    👥 Подписчики: {data['followers']:,}\n"
                report += f"    📝 Постов: {data['posts_count']:,}\n"
                report += f"    ❤️ Средние лайки: {data['avg_likes']:,}\n"
                report += f"    💬 Средние комментарии: {data['avg_comments']:,}\n"
                report += f"    📊 ER: {data['engagement_rate']:.2f}%\n"
                report += f"    📷 Фото: {data['photos']} | 🎥 Видео: {data['videos']} | 🎠 Карусели: {data['carousels']}\n\n"
            
            # Рейтинги
            valid_accounts = [d for d in accounts_data if 'error' not in d]
            if len(valid_accounts) > 1:
                report += "🏆 РЕЙТИНГИ:\n\n"
                
                # Топ по подписчикам
                top_followers = sorted(valid_accounts, key=lambda x: x['followers'], reverse=True)
                report += "👥 Топ по подписчикам:\n"
                for i, acc in enumerate(top_followers[:5], 1):
                    report += f"  {i}. @{acc['username']} - {acc['followers']:,}\n"
                report += "\n"
                
                # Топ по engagement rate
                top_er = sorted(valid_accounts, key=lambda x: x['engagement_rate'], reverse=True)
                report += "📊 Топ по Engagement Rate:\n"
                for i, acc in enumerate(top_er[:5], 1):
                    report += f"  {i}. @{acc['username']} - {acc['engagement_rate']:.2f}%\n"
                report += "\n"
                
                # Топ по средним лайкам
                top_likes = sorted(valid_accounts, key=lambda x: x['avg_likes'], reverse=True)
                report += "❤️ Топ по средним лайкам:\n"
                for i, acc in enumerate(top_likes[:5], 1):
                    report += f"  {i}. @{acc['username']} - {acc['avg_likes']:,}\n"
        
        return report
        
    except Exception as e:
        return f"❌ Ошибка при сравнительной аналитике: {str(e)}"

def analyze_accounts_summary(account_ids: List[int], usernames: List[str]) -> str:
    """Сводная статистика нескольких аккаунтов"""
    try:
        client, error = get_authorized_client()
        if not client:
            return f"📈 Сводная статистика для {len(account_ids)} аккаунтов\n\n{error}"
        
        report = f"📈 СВОДНАЯ СТАТИСТИКА\n"
        report += f"📅 Дата анализа: {datetime.now().strftime('%d.%m.%Y %H:%M')}\n"
        report += f"👥 Аккаунтов: {len(account_ids)}\n"
        report += "=" * 60 + "\n\n"
        
        total_followers = 0
        total_posts = 0
        total_likes = 0
        total_comments = 0
        total_analyzed_posts = 0
        valid_accounts = 0
        
        photos_total = 0
        videos_total = 0
        carousels_total = 0
        
        account_details = []
        
        # Собираем данные
        for username in usernames:
            try:
                user_info = client.user_info_by_username(username)
                medias = client.user_medias(user_info.pk, amount=20)
                
                if medias:
                    account_likes = sum(m.like_count for m in medias)
                    account_comments = sum(m.comment_count for m in medias)
                    
                    photos = sum(1 for m in medias if m.media_type == 1)
                    videos = sum(1 for m in medias if m.media_type == 2)
                    carousels = sum(1 for m in medias if m.media_type == 8)
                    
                    total_followers += user_info.follower_count
                    total_posts += user_info.media_count
                    total_likes += account_likes
                    total_comments += account_comments
                    total_analyzed_posts += len(medias)
                    valid_accounts += 1
                    
                    photos_total += photos
                    videos_total += videos
                    carousels_total += carousels
                    
                    account_details.append({
                        'username': username,
                        'followers': user_info.follower_count,
                        'posts': user_info.media_count,
                        'likes': account_likes,
                        'comments': account_comments
                    })
                    
            except Exception as e:
                logger.warning(f"Ошибка при анализе {username}: {e}")
        
        if valid_accounts > 0:
            # Общая статистика
            report += f"📊 ОБЩИЕ ПОКАЗАТЕЛИ:\n"
            report += f"✅ Успешно проанализировано: {valid_accounts} из {len(usernames)}\n"
            report += f"👥 Общее количество подписчиков: {total_followers:,}\n"
            report += f"📝 Общее количество постов: {total_posts:,}\n"
            report += f"❤️ Общие лайки (последние посты): {total_likes:,}\n"
            report += f"💬 Общие комментарии (последние посты): {total_comments:,}\n"
            report += f"📊 Проанализировано постов: {total_analyzed_posts}\n\n"
            
            # Средние показатели
            avg_followers = total_followers // valid_accounts
            avg_posts = total_posts // valid_accounts
            avg_likes_per_account = total_likes // valid_accounts
            avg_comments_per_account = total_comments // valid_accounts
            
            report += f"📈 СРЕДНИЕ ПОКАЗАТЕЛИ НА АККАУНТ:\n"
            report += f"👥 Средние подписчики: {avg_followers:,}\n"
            report += f"📝 Средние посты: {avg_posts:,}\n"
            report += f"❤️ Средние лайки: {avg_likes_per_account:,}\n"
            report += f"💬 Средние комментарии: {avg_comments_per_account:,}\n\n"
            
            # Анализ контента
            total_content = photos_total + videos_total + carousels_total
            if total_content > 0:
                report += f"📊 АНАЛИЗ КОНТЕНТА:\n"
                report += f"📷 Фото: {photos_total} ({photos_total/total_content*100:.1f}%)\n"
                report += f"🎥 Видео: {videos_total} ({videos_total/total_content*100:.1f}%)\n"
                report += f"🎠 Карусели: {carousels_total} ({carousels_total/total_content*100:.1f}%)\n\n"
            
            # Детали по аккаунтам
            report += f"👥 ДЕТАЛИ ПО АККАУНТАМ:\n"
            for i, acc in enumerate(account_details, 1):
                report += f"{i:2d}. @{acc['username']} - {acc['followers']:,} подписчиков, {acc['likes']:,} лайков\n"
        
        return report
        
    except Exception as e:
        return f"❌ Ошибка при сводной аналитике: {str(e)}"

def analyze_top_posts_all_accounts(account_ids: List[int], usernames: List[str]) -> str:
    """Лучшие посты всех аккаунтов"""
    try:
        client, error = get_authorized_client()
        if not client:
            return f"🏆 Лучшие посты всех аккаунтов\n\n{error}"
        
        report = f"🏆 ЛУЧШИЕ ПОСТЫ ВСЕХ АККАУНТОВ\n"
        report += f"📅 Дата анализа: {datetime.now().strftime('%d.%m.%Y %H:%M')}\n"
        report += f"👥 Аккаунтов: {len(account_ids)}\n"
        report += "=" * 60 + "\n\n"
        
        all_posts = []
        
        # Собираем посты от всех аккаунтов
        for username in usernames:
            try:
                user_info = client.user_info_by_username(username)
                medias = client.user_medias(user_info.pk, amount=20)
                
                for media in medias:
                    all_posts.append({
                        'username': username,
                        'media': media,
                        'score': media.like_count + media.comment_count * 3  # Взвешенный рейтинг
                    })
                    
            except Exception as e:
                logger.warning(f"Ошибка при получении постов {username}: {e}")
        
        if all_posts:
            # Сортируем по рейтингу
            top_posts = sorted(all_posts, key=lambda x: x['score'], reverse=True)[:20]
            
            report += f"🏆 ТОП-{len(top_posts)} ПОСТОВ ПО ВСЕМ АККАУНТАМ:\n\n"
            
            for i, post_data in enumerate(top_posts, 1):
                media = post_data['media']
                username = post_data['username']
                
                media_type_names = {1: 'Фото', 2: 'Видео', 8: 'Карусель'}
                media_type_name = media_type_names.get(media.media_type, f'Тип {media.media_type}')
                
                report += f"🏆 МЕСТО #{i}\n"
                report += f"👤 Аккаунт: @{username}\n"
                report += f"🔗 URL: https://www.instagram.com/p/{media.code}/\n"
                report += f"📅 Дата: {media.taken_at.strftime('%Y-%m-%d %H:%M:%S')}\n"
                report += f"📝 Тип: {media_type_name}\n"
                report += f"❤️ Лайки: {media.like_count:,}\n"
                report += f"💬 Комментарии: {media.comment_count:,}\n"
                report += f"🏆 Рейтинг: {post_data['score']:,}\n"
                
                if media.caption_text:
                    caption_preview = media.caption_text[:100] + "..." if len(media.caption_text) > 100 else media.caption_text
                    report += f"💬 Описание: {caption_preview}\n"
                
                report += "-" * 40 + "\n"
        
        return report
        
    except Exception as e:
        return f"❌ Ошибка при анализе лучших постов: {str(e)}"

def analyze_detailed_all_accounts(account_ids: List[int], usernames: List[str]) -> str:
    """Детальный отчет по всем аккаунтам"""
    try:
        client, error = get_authorized_client()
        if not client:
            return f"📋 Детальный отчет по всем аккаунтам\n\n{error}"
        
        report = f"📋 ДЕТАЛЬНЫЙ ОТЧЕТ ПО ВСЕМ АККАУНТАМ\n"
        report += f"📅 Дата анализа: {datetime.now().strftime('%d.%m.%Y %H:%M')}\n"
        report += f"👥 Аккаунтов: {len(account_ids)}\n"
        report += "=" * 60 + "\n\n"
        
        # Подробный анализ каждого аккаунта
        for i, username in enumerate(usernames, 1):
            try:
                report += f"👤 АККАУНТ #{i}: @{username}\n"
                report += "─" * 50 + "\n"
                
                user_info = client.user_info_by_username(username)
                medias = client.user_medias(user_info.pk, amount=30)
                
                # Информация о профиле
                report += f"📛 Имя: {user_info.full_name or 'Не указано'}\n"
                report += f"👥 Подписчики: {user_info.follower_count:,}\n"
                report += f"👤 Подписки: {user_info.following_count:,}\n"
                report += f"📝 Всего постов: {user_info.media_count:,}\n"
                report += f"📊 Проанализировано: {len(medias) if medias else 0} постов\n\n"
                
                if medias:
                    # Статистика по типам контента
                    photos = sum(1 for m in medias if m.media_type == 1)
                    videos = sum(1 for m in medias if m.media_type == 2)
                    carousels = sum(1 for m in medias if m.media_type == 8)
                    
                    report += f"📊 Типы контента:\n"
                    report += f"  📷 Фото: {photos} ({photos/len(medias)*100:.1f}%)\n"
                    report += f"  🎥 Видео: {videos} ({videos/len(medias)*100:.1f}%)\n"
                    report += f"  🎠 Карусели: {carousels} ({carousels/len(medias)*100:.1f}%)\n\n"
                    
                    # Статистика вовлеченности
                    total_likes = sum(m.like_count for m in medias)
                    total_comments = sum(m.comment_count for m in medias)
                    avg_likes = total_likes // len(medias)
                    avg_comments = total_comments // len(medias)
                    
                    report += f"📈 Вовлеченность:\n"
                    report += f"  ❤️ Всего лайков: {total_likes:,}\n"
                    report += f"  💬 Всего комментариев: {total_comments:,}\n"
                    report += f"  ❤️ Средние лайки: {avg_likes:,}\n"
                    report += f"  💬 Средние комментарии: {avg_comments:,}\n"
                    
                    if user_info.follower_count > 0:
                        engagement_rate = (avg_likes + avg_comments) / user_info.follower_count * 100
                        report += f"  📊 Engagement Rate: {engagement_rate:.2f}%\n"
                    
                    # Лучший пост
                    best_post = max(medias, key=lambda x: x.like_count + x.comment_count * 3)
                    report += f"\n🏆 Лучший пост:\n"
                    report += f"  🔗 https://www.instagram.com/p/{best_post.code}/\n"
                    report += f"  ❤️ {best_post.like_count:,} лайков, 💬 {best_post.comment_count:,} комментариев\n"
                    
                else:
                    report += "❌ Посты не найдены или профиль закрыт\n"
                
                report += "\n" + "=" * 60 + "\n\n"
                
            except Exception as e:
                report += f"❌ Ошибка при анализе @{username}: {str(e)}\n\n"
        
        return report
        
    except Exception as e:
        return f"❌ Ошибка при детальном анализе: {str(e)}"

def start_general_analytics(update: Update, context: CallbackContext):
    """Общая аналитика системы"""
    query = update.callback_query
    query.answer()
    
    try:
        from database.db_manager import get_publish_tasks
        from database.models import TaskStatus, TaskType
        
        # Получаем данные для анализа
        accounts = get_instagram_accounts()
        
        # Получаем задачи за последнюю неделю
        week_ago = datetime.now() - timedelta(days=7)
        
        report = f"📊 ОБЩАЯ СТАТИСТИКА СИСТЕМЫ\n"
        report += f"📅 Дата анализа: {datetime.now().strftime('%d.%m.%Y %H:%M')}\n"
        report += "=" * 50 + "\n\n"
        
        # Статистика аккаунтов
        active_accounts = [acc for acc in accounts if acc.is_active]
        report += f"👥 АККАУНТЫ:\n"
        report += f"📊 Всего: {len(accounts)}\n"
        report += f"✅ Активных: {len(active_accounts)}\n"
        report += f"❌ Неактивных: {len(accounts) - len(active_accounts)}\n\n"
        
        # Статистика папок
        from database.db_manager import get_account_groups
        groups = get_account_groups()
        report += f"📁 ПАПКИ:\n"
        report += f"📊 Всего папок: {len(groups)}\n"
        for group in groups:
            from database.db_manager import get_accounts_in_group
            group_accounts = get_accounts_in_group(group.id)
            report += f"   📁 {group.name}: {len(group_accounts)} аккаунтов\n"
        report += "\n"
        
        # Статистика прокси
        from database.db_manager import get_proxies
        proxies = get_proxies()
        active_proxies = [p for p in proxies if p.is_active]
        report += f"🌐 ПРОКСИ:\n"
        report += f"📊 Всего: {len(proxies)}\n"
        report += f"✅ Активных: {len(active_proxies)}\n"
        report += f"❌ Неактивных: {len(proxies) - len(active_proxies)}\n\n"
        
        # Статистика системы
        import psutil
        cpu_percent = psutil.cpu_percent(interval=1)
        memory = psutil.virtual_memory()
        
        report += f"🖥️ СИСТЕМА:\n"
        report += f"⚡ CPU: {cpu_percent:.1f}%\n"
        report += f"💾 RAM: {memory.percent:.1f}% ({memory.used // 1024**3:.1f}GB / {memory.total // 1024**3:.1f}GB)\n"
        
        # Проверяем место на диске
        disk = psutil.disk_usage('/')
        report += f"💽 Диск: {disk.percent:.1f}% ({disk.used // 1024**3:.1f}GB / {disk.total // 1024**3:.1f}GB)\n\n"
        
        report += f"🔧 РЕКОМЕНДАЦИИ:\n"
        if len(active_accounts) == 0:
            report += "⚠️ Нет активных аккаунтов - добавьте аккаунты\n"
        elif len(active_accounts) < 5:
            report += "💡 Мало аккаунтов для эффективной работы\n"
        
        if len(active_proxies) == 0:
            report += "⚠️ Нет активных прокси - добавьте прокси\n"
        elif len(active_proxies) < len(active_accounts):
            report += "💡 Прокси меньше чем аккаунтов - рекомендуется 1:1\n"
        
        if memory.percent > 80:
            report += "⚠️ Высокое использование RAM\n"
        
        if disk.percent > 90:
            report += "⚠️ Мало места на диске\n"
        
        # Создаем и отправляем файл
        filename = f"general_statistics_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False, encoding='utf-8') as f:
            f.write(report)
            temp_file_path = f.name
        
        with open(temp_file_path, 'rb') as f:
            query.message.reply_document(
                document=f,
                filename=filename,
                caption="📊 Общая статистика системы"
            )
        
        os.unlink(temp_file_path)
        
        # Возвращаемся к меню статистики
        from telegram_bot.keyboards import get_statistics_menu_keyboard
        query.edit_message_text(
            "✅ Общая статистика отправлена файлом",
            reply_markup=get_statistics_menu_keyboard()
        )
        
    except Exception as e:
        logger.error(f"Ошибка при общей аналитике: {e}")
        query.edit_message_text(
            f"❌ Ошибка при общей аналитике: {str(e)}",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Назад", callback_data="analytics_menu")]
            ])
        ) 
def analyze_accounts_comparison(account_ids: List[int], usernames: List[str]) -> str:
    """Сравнительная аналитика нескольких аккаунтов"""
    try:
        client, error = get_authorized_client()
        if not client:
            return f"📊 Сравнительная аналитика для {len(account_ids)} аккаунтов\n\n{error}"
        
        report = f"📊 СРАВНИТЕЛЬНАЯ АНАЛИТИКА\n"
        report += f"📅 Дата анализа: {datetime.now().strftime('%d.%m.%Y %H:%M')}\n"
        report += f"👥 Аккаунтов: {len(account_ids)}\n"
        report += "=" * 60 + "\n\n"
        
        accounts_data = []
        
        # Собираем данные по каждому аккаунту
        for i, username in enumerate(usernames):
            try:
                user_info = client.user_info_by_username(username)
                medias = client.user_medias(user_info.pk, amount=20)
                
                if medias:
                    total_likes = sum(m.like_count for m in medias)
                    total_comments = sum(m.comment_count for m in medias)
                    avg_likes = total_likes // len(medias)
                    avg_comments = total_comments // len(medias)
                    
                    # Определяем типы контента
                    photos = sum(1 for m in medias if m.media_type == 1)
                    videos = sum(1 for m in medias if m.media_type == 2)
                    carousels = sum(1 for m in medias if m.media_type == 8)
                    
                    accounts_data.append({
                        'username': username,
                        'followers': user_info.follower_count,
                        'following': user_info.following_count,
                        'posts_count': user_info.media_count,
                        'analyzed_posts': len(medias),
                        'total_likes': total_likes,
                        'total_comments': total_comments,
                        'avg_likes': avg_likes,
                        'avg_comments': avg_comments,
                        'photos': photos,
                        'videos': videos,
                        'carousels': carousels,
                        'engagement_rate': (avg_likes + avg_comments) / user_info.follower_count * 100 if user_info.follower_count > 0 else 0
                    })
                    
            except Exception as e:
                logger.warning(f"Ошибка при анализе {username}: {e}")
                accounts_data.append({
                    'username': username,
                    'error': str(e)
                })
        
        # Формируем сравнительный отчет
        if accounts_data:
            report += "📊 СРАВНЕНИЕ АККАУНТОВ:\n\n"
            
            for i, data in enumerate(accounts_data, 1):
                if 'error' in data:
                    report += f"{i:2d}. @{data['username']} - ❌ Ошибка: {data['error']}\n"
                    continue
                
                report += f"{i:2d}. @{data['username']}\n"
                report += f"    👥 Подписчики: {data['followers']:,}\n"
                report += f"    📝 Постов: {data['posts_count']:,}\n"
                report += f"    ❤️ Средние лайки: {data['avg_likes']:,}\n"
                report += f"    💬 Средние комментарии: {data['avg_comments']:,}\n"
                report += f"    📊 ER: {data['engagement_rate']:.2f}%\n"
                report += f"    📷 Фото: {data['photos']} | 🎥 Видео: {data['videos']} | 🎠 Карусели: {data['carousels']}\n\n"
            
            # Рейтинги
            valid_accounts = [d for d in accounts_data if 'error' not in d]
            if len(valid_accounts) > 1:
                report += "🏆 РЕЙТИНГИ:\n\n"
                
                # Топ по подписчикам
                top_followers = sorted(valid_accounts, key=lambda x: x['followers'], reverse=True)
                report += "👥 Топ по подписчикам:\n"
                for i, acc in enumerate(top_followers[:5], 1):
                    report += f"  {i}. @{acc['username']} - {acc['followers']:,}\n"
                report += "\n"
                
                # Топ по engagement rate
                top_er = sorted(valid_accounts, key=lambda x: x['engagement_rate'], reverse=True)
                report += "📊 Топ по Engagement Rate:\n"
                for i, acc in enumerate(top_er[:5], 1):
                    report += f"  {i}. @{acc['username']} - {acc['engagement_rate']:.2f}%\n"
                report += "\n"
                
                # Топ по средним лайкам
                top_likes = sorted(valid_accounts, key=lambda x: x['avg_likes'], reverse=True)
                report += "❤️ Топ по средним лайкам:\n"
                for i, acc in enumerate(top_likes[:5], 1):
                    report += f"  {i}. @{acc['username']} - {acc['avg_likes']:,}\n"
        
        return report
        
    except Exception as e:
        return f"❌ Ошибка при сравнительной аналитике: {str(e)}"

def analyze_accounts_summary(account_ids: List[int], usernames: List[str]) -> str:
    """Сводная статистика нескольких аккаунтов"""
    try:
        client, error = get_authorized_client()
        if not client:
            return f"📈 Сводная статистика для {len(account_ids)} аккаунтов\n\n{error}"
        
        report = f"📈 СВОДНАЯ СТАТИСТИКА\n"
        report += f"📅 Дата анализа: {datetime.now().strftime('%d.%m.%Y %H:%M')}\n"
        report += f"👥 Аккаунтов: {len(account_ids)}\n"
        report += "=" * 60 + "\n\n"
        
        total_followers = 0
        total_posts = 0
        total_likes = 0
        total_comments = 0
        total_analyzed_posts = 0
        valid_accounts = 0
        
        photos_total = 0
        videos_total = 0
        carousels_total = 0
        
        account_details = []
        
        # Собираем данные
        for username in usernames:
            try:
                user_info = client.user_info_by_username(username)
                medias = client.user_medias(user_info.pk, amount=20)
                
                if medias:
                    account_likes = sum(m.like_count for m in medias)
                    account_comments = sum(m.comment_count for m in medias)
                    
                    photos = sum(1 for m in medias if m.media_type == 1)
                    videos = sum(1 for m in medias if m.media_type == 2)
                    carousels = sum(1 for m in medias if m.media_type == 8)
                    
                    total_followers += user_info.follower_count
                    total_posts += user_info.media_count
                    total_likes += account_likes
                    total_comments += account_comments
                    total_analyzed_posts += len(medias)
                    valid_accounts += 1
                    
                    photos_total += photos
                    videos_total += videos
                    carousels_total += carousels
                    
                    account_details.append({
                        'username': username,
                        'followers': user_info.follower_count,
                        'posts': user_info.media_count,
                        'likes': account_likes,
                        'comments': account_comments
                    })
                    
            except Exception as e:
                logger.warning(f"Ошибка при анализе {username}: {e}")
        
        if valid_accounts > 0:
            # Общая статистика
            report += f"📊 ОБЩИЕ ПОКАЗАТЕЛИ:\n"
            report += f"✅ Успешно проанализировано: {valid_accounts} из {len(usernames)}\n"
            report += f"👥 Общее количество подписчиков: {total_followers:,}\n"
            report += f"📝 Общее количество постов: {total_posts:,}\n"
            report += f"❤️ Общие лайки (последние посты): {total_likes:,}\n"
            report += f"💬 Общие комментарии (последние посты): {total_comments:,}\n"
            report += f"📊 Проанализировано постов: {total_analyzed_posts}\n\n"
            
            # Средние показатели
            avg_followers = total_followers // valid_accounts
            avg_posts = total_posts // valid_accounts
            avg_likes_per_account = total_likes // valid_accounts
            avg_comments_per_account = total_comments // valid_accounts
            
            report += f"📈 СРЕДНИЕ ПОКАЗАТЕЛИ НА АККАУНТ:\n"
            report += f"👥 Средние подписчики: {avg_followers:,}\n"
            report += f"📝 Средние посты: {avg_posts:,}\n"
            report += f"❤️ Средние лайки: {avg_likes_per_account:,}\n"
            report += f"💬 Средние комментарии: {avg_comments_per_account:,}\n\n"
            
            # Анализ контента
            total_content = photos_total + videos_total + carousels_total
            if total_content > 0:
                report += f"📊 АНАЛИЗ КОНТЕНТА:\n"
                report += f"📷 Фото: {photos_total} ({photos_total/total_content*100:.1f}%)\n"
                report += f"🎥 Видео: {videos_total} ({videos_total/total_content*100:.1f}%)\n"
                report += f"🎠 Карусели: {carousels_total} ({carousels_total/total_content*100:.1f}%)\n\n"
            
            # Детали по аккаунтам
            report += f"👥 ДЕТАЛИ ПО АККАУНТАМ:\n"
            for i, acc in enumerate(account_details, 1):
                report += f"{i:2d}. @{acc['username']} - {acc['followers']:,} подписчиков, {acc['likes']:,} лайков\n"
        
        return report
        
    except Exception as e:
        return f"❌ Ошибка при сводной аналитике: {str(e)}"

def analyze_top_posts_all_accounts(account_ids: List[int], usernames: List[str]) -> str:
    """Лучшие посты всех аккаунтов"""
    try:
        client, error = get_authorized_client()
        if not client:
            return f"🏆 Лучшие посты всех аккаунтов\n\n{error}"
        
        report = f"🏆 ЛУЧШИЕ ПОСТЫ ВСЕХ АККАУНТОВ\n"
        report += f"📅 Дата анализа: {datetime.now().strftime('%d.%m.%Y %H:%M')}\n"
        report += f"👥 Аккаунтов: {len(account_ids)}\n"
        report += "=" * 60 + "\n\n"
        
        all_posts = []
        
        # Собираем посты от всех аккаунтов
        for username in usernames:
            try:
                user_info = client.user_info_by_username(username)
                medias = client.user_medias(user_info.pk, amount=20)
                
                for media in medias:
                    all_posts.append({
                        'username': username,
                        'media': media,
                        'score': media.like_count + media.comment_count * 3  # Взвешенный рейтинг
                    })
                    
            except Exception as e:
                logger.warning(f"Ошибка при получении постов {username}: {e}")
        
        if all_posts:
            # Сортируем по рейтингу
            top_posts = sorted(all_posts, key=lambda x: x['score'], reverse=True)[:20]
            
            report += f"🏆 ТОП-{len(top_posts)} ПОСТОВ ПО ВСЕМ АККАУНТАМ:\n\n"
            
            for i, post_data in enumerate(top_posts, 1):
                media = post_data['media']
                username = post_data['username']
                
                media_type_names = {1: 'Фото', 2: 'Видео', 8: 'Карусель'}
                media_type_name = media_type_names.get(media.media_type, f'Тип {media.media_type}')
                
                report += f"🏆 МЕСТО #{i}\n"
                report += f"👤 Аккаунт: @{username}\n"
                report += f"🔗 URL: https://www.instagram.com/p/{media.code}/\n"
                report += f"📅 Дата: {media.taken_at.strftime('%Y-%m-%d %H:%M:%S')}\n"
                report += f"📝 Тип: {media_type_name}\n"
                report += f"❤️ Лайки: {media.like_count:,}\n"
                report += f"💬 Комментарии: {media.comment_count:,}\n"
                report += f"🏆 Рейтинг: {post_data['score']:,}\n"
                
                if media.caption_text:
                    caption_preview = media.caption_text[:100] + "..." if len(media.caption_text) > 100 else media.caption_text
                    report += f"💬 Описание: {caption_preview}\n"
                
                report += "-" * 40 + "\n"
        
        return report
        
    except Exception as e:
        return f"❌ Ошибка при анализе лучших постов: {str(e)}"

def analyze_detailed_all_accounts(account_ids: List[int], usernames: List[str]) -> str:
    """Детальный отчет по всем аккаунтам"""
    try:
        client, error = get_authorized_client()
        if not client:
            return f"📋 Детальный отчет по всем аккаунтам\n\n{error}"
        
        report = f"📋 ДЕТАЛЬНЫЙ ОТЧЕТ ПО ВСЕМ АККАУНТАМ\n"
        report += f"📅 Дата анализа: {datetime.now().strftime('%d.%m.%Y %H:%M')}\n"
        report += f"👥 Аккаунтов: {len(account_ids)}\n"
        report += "=" * 60 + "\n\n"
        
        # Подробный анализ каждого аккаунта
        for i, username in enumerate(usernames, 1):
            try:
                report += f"👤 АККАУНТ #{i}: @{username}\n"
                report += "─" * 50 + "\n"
                
                user_info = client.user_info_by_username(username)
                medias = client.user_medias(user_info.pk, amount=30)
                
                # Информация о профиле
                report += f"📛 Имя: {user_info.full_name or 'Не указано'}\n"
                report += f"👥 Подписчики: {user_info.follower_count:,}\n"
                report += f"👤 Подписки: {user_info.following_count:,}\n"
                report += f"📝 Всего постов: {user_info.media_count:,}\n"
                report += f"📊 Проанализировано: {len(medias) if medias else 0} постов\n\n"
                
                if medias:
                    # Статистика по типам контента
                    photos = sum(1 for m in medias if m.media_type == 1)
                    videos = sum(1 for m in medias if m.media_type == 2)
                    carousels = sum(1 for m in medias if m.media_type == 8)
                    
                    report += f"📊 Типы контента:\n"
                    report += f"  📷 Фото: {photos} ({photos/len(medias)*100:.1f}%)\n"
                    report += f"  🎥 Видео: {videos} ({videos/len(medias)*100:.1f}%)\n"
                    report += f"  🎠 Карусели: {carousels} ({carousels/len(medias)*100:.1f}%)\n\n"
                    
                    # Статистика вовлеченности
                    total_likes = sum(m.like_count for m in medias)
                    total_comments = sum(m.comment_count for m in medias)
                    avg_likes = total_likes // len(medias)
                    avg_comments = total_comments // len(medias)
                    
                    report += f"📈 Вовлеченность:\n"
                    report += f"  ❤️ Всего лайков: {total_likes:,}\n"
                    report += f"  💬 Всего комментариев: {total_comments:,}\n"
                    report += f"  ❤️ Средние лайки: {avg_likes:,}\n"
                    report += f"  💬 Средние комментарии: {avg_comments:,}\n"
                    
                    if user_info.follower_count > 0:
                        engagement_rate = (avg_likes + avg_comments) / user_info.follower_count * 100
                        report += f"  📊 Engagement Rate: {engagement_rate:.2f}%\n"
                    
                    # Лучший пост
                    best_post = max(medias, key=lambda x: x.like_count + x.comment_count * 3)
                    report += f"\n🏆 Лучший пост:\n"
                    report += f"  🔗 https://www.instagram.com/p/{best_post.code}/\n"
                    report += f"  ❤️ {best_post.like_count:,} лайков, 💬 {best_post.comment_count:,} комментариев\n"
                    
                else:
                    report += "❌ Посты не найдены или профиль закрыт\n"
                
                report += "\n" + "=" * 60 + "\n\n"
                
            except Exception as e:
                report += f"❌ Ошибка при анализе @{username}: {str(e)}\n\n"
        
        return report
        
    except Exception as e:
        return f"❌ Ошибка при детальном анализе: {str(e)}"

def start_general_analytics(update: Update, context: CallbackContext):
    """Общая аналитика системы"""
    query = update.callback_query
    query.answer()
    
    try:
        from database.db_manager import get_publish_tasks
        from database.models import TaskStatus, TaskType
        
        # Получаем данные для анализа
        accounts = get_instagram_accounts()
        
        # Получаем задачи за последнюю неделю
        week_ago = datetime.now() - timedelta(days=7)
        
        report = f"📊 ОБЩАЯ СТАТИСТИКА СИСТЕМЫ\n"
        report += f"📅 Дата анализа: {datetime.now().strftime('%d.%m.%Y %H:%M')}\n"
        report += "=" * 50 + "\n\n"
        
        # Статистика аккаунтов
        active_accounts = [acc for acc in accounts if acc.is_active]
        report += f"👥 АККАУНТЫ:\n"
        report += f"📊 Всего: {len(accounts)}\n"
        report += f"✅ Активных: {len(active_accounts)}\n"
        report += f"❌ Неактивных: {len(accounts) - len(active_accounts)}\n\n"
        
        # Статистика папок
        from database.db_manager import get_account_groups
        groups = get_account_groups()
        report += f"📁 ПАПКИ:\n"
        report += f"📊 Всего папок: {len(groups)}\n"
        for group in groups:
            from database.db_manager import get_accounts_in_group
            group_accounts = get_accounts_in_group(group.id)
            report += f"   📁 {group.name}: {len(group_accounts)} аккаунтов\n"
        report += "\n"
        
        # Статистика прокси
        from database.db_manager import get_proxies
        proxies = get_proxies()
        active_proxies = [p for p in proxies if p.is_active]
        report += f"🌐 ПРОКСИ:\n"
        report += f"📊 Всего: {len(proxies)}\n"
        report += f"✅ Активных: {len(active_proxies)}\n"
        report += f"❌ Неактивных: {len(proxies) - len(active_proxies)}\n\n"
        
        # Статистика системы
        import psutil
        cpu_percent = psutil.cpu_percent(interval=1)
        memory = psutil.virtual_memory()
        
        report += f"🖥️ СИСТЕМА:\n"
        report += f"⚡ CPU: {cpu_percent:.1f}%\n"
        report += f"💾 RAM: {memory.percent:.1f}% ({memory.used // 1024**3:.1f}GB / {memory.total // 1024**3:.1f}GB)\n"
        
        # Проверяем место на диске
        disk = psutil.disk_usage('/')
        report += f"💽 Диск: {disk.percent:.1f}% ({disk.used // 1024**3:.1f}GB / {disk.total // 1024**3:.1f}GB)\n\n"
        
        report += f"🔧 РЕКОМЕНДАЦИИ:\n"
        if len(active_accounts) == 0:
            report += "⚠️ Нет активных аккаунтов - добавьте аккаунты\n"
        elif len(active_accounts) < 5:
            report += "💡 Мало аккаунтов для эффективной работы\n"
        
        if len(active_proxies) == 0:
            report += "⚠️ Нет активных прокси - добавьте прокси\n"
        elif len(active_proxies) < len(active_accounts):
            report += "💡 Прокси меньше чем аккаунтов - рекомендуется 1:1\n"
        
        if memory.percent > 80:
            report += "⚠️ Высокое использование RAM\n"
        
        if disk.percent > 90:
            report += "⚠️ Мало места на диске\n"
        
        # Создаем и отправляем файл
        filename = f"general_statistics_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False, encoding='utf-8') as f:
            f.write(report)
            temp_file_path = f.name
        
        with open(temp_file_path, 'rb') as f:
            query.message.reply_document(
                document=f,
                filename=filename,
                caption="📊 Общая статистика системы"
            )
        
        os.unlink(temp_file_path)
        
        # Возвращаемся к меню статистики
        from telegram_bot.keyboards import get_statistics_menu_keyboard
        query.edit_message_text(
            "✅ Общая статистика отправлена файлом",
            reply_markup=get_statistics_menu_keyboard()
        )
        
    except Exception as e:
        logger.error(f"Ошибка при общей аналитике: {e}")
        query.edit_message_text(
            f"❌ Ошибка при общей аналитике: {str(e)}",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Назад", callback_data="analytics_menu")]
            ])
        ) 
def analyze_accounts_comparison(account_ids: List[int], usernames: List[str]) -> str:
    """Сравнительная аналитика нескольких аккаунтов"""
    try:
        client, error = get_authorized_client()
        if not client:
            return f"📊 Сравнительная аналитика для {len(account_ids)} аккаунтов\n\n{error}"
        
        report = f"📊 СРАВНИТЕЛЬНАЯ АНАЛИТИКА\n"
        report += f"📅 Дата анализа: {datetime.now().strftime('%d.%m.%Y %H:%M')}\n"
        report += f"👥 Аккаунтов: {len(account_ids)}\n"
        report += "=" * 60 + "\n\n"
        
        accounts_data = []
        
        # Собираем данные по каждому аккаунту
        for i, username in enumerate(usernames):
            try:
                user_info = client.user_info_by_username(username)
                medias = client.user_medias(user_info.pk, amount=20)
                
                if medias:
                    total_likes = sum(m.like_count for m in medias)
                    total_comments = sum(m.comment_count for m in medias)
                    avg_likes = total_likes // len(medias)
                    avg_comments = total_comments // len(medias)
                    
                    # Определяем типы контента
                    photos = sum(1 for m in medias if m.media_type == 1)
                    videos = sum(1 for m in medias if m.media_type == 2)
                    carousels = sum(1 for m in medias if m.media_type == 8)
                    
                    accounts_data.append({
                        'username': username,
                        'followers': user_info.follower_count,
                        'following': user_info.following_count,
                        'posts_count': user_info.media_count,
                        'analyzed_posts': len(medias),
                        'total_likes': total_likes,
                        'total_comments': total_comments,
                        'avg_likes': avg_likes,
                        'avg_comments': avg_comments,
                        'photos': photos,
                        'videos': videos,
                        'carousels': carousels,
                        'engagement_rate': (avg_likes + avg_comments) / user_info.follower_count * 100 if user_info.follower_count > 0 else 0
                    })
                    
            except Exception as e:
                logger.warning(f"Ошибка при анализе {username}: {e}")
                accounts_data.append({
                    'username': username,
                    'error': str(e)
                })
        
        # Формируем сравнительный отчет
        if accounts_data:
            report += "📊 СРАВНЕНИЕ АККАУНТОВ:\n\n"
            
            for i, data in enumerate(accounts_data, 1):
                if 'error' in data:
                    report += f"{i:2d}. @{data['username']} - ❌ Ошибка: {data['error']}\n"
                    continue
                
                report += f"{i:2d}. @{data['username']}\n"
                report += f"    👥 Подписчики: {data['followers']:,}\n"
                report += f"    📝 Постов: {data['posts_count']:,}\n"
                report += f"    ❤️ Средние лайки: {data['avg_likes']:,}\n"
                report += f"    💬 Средние комментарии: {data['avg_comments']:,}\n"
                report += f"    📊 ER: {data['engagement_rate']:.2f}%\n"
                report += f"    📷 Фото: {data['photos']} | 🎥 Видео: {data['videos']} | 🎠 Карусели: {data['carousels']}\n\n"
            
            # Рейтинги
            valid_accounts = [d for d in accounts_data if 'error' not in d]
            if len(valid_accounts) > 1:
                report += "🏆 РЕЙТИНГИ:\n\n"
                
                # Топ по подписчикам
                top_followers = sorted(valid_accounts, key=lambda x: x['followers'], reverse=True)
                report += "👥 Топ по подписчикам:\n"
                for i, acc in enumerate(top_followers[:5], 1):
                    report += f"  {i}. @{acc['username']} - {acc['followers']:,}\n"
                report += "\n"
                
                # Топ по engagement rate
                top_er = sorted(valid_accounts, key=lambda x: x['engagement_rate'], reverse=True)
                report += "📊 Топ по Engagement Rate:\n"
                for i, acc in enumerate(top_er[:5], 1):
                    report += f"  {i}. @{acc['username']} - {acc['engagement_rate']:.2f}%\n"
                report += "\n"
                
                # Топ по средним лайкам
                top_likes = sorted(valid_accounts, key=lambda x: x['avg_likes'], reverse=True)
                report += "❤️ Топ по средним лайкам:\n"
                for i, acc in enumerate(top_likes[:5], 1):
                    report += f"  {i}. @{acc['username']} - {acc['avg_likes']:,}\n"
        
        return report
        
    except Exception as e:
        return f"❌ Ошибка при сравнительной аналитике: {str(e)}"

def analyze_accounts_summary(account_ids: List[int], usernames: List[str]) -> str:
    """Сводная статистика нескольких аккаунтов"""
    try:
        client, error = get_authorized_client()
        if not client:
            return f"📈 Сводная статистика для {len(account_ids)} аккаунтов\n\n{error}"
        
        report = f"📈 СВОДНАЯ СТАТИСТИКА\n"
        report += f"📅 Дата анализа: {datetime.now().strftime('%d.%m.%Y %H:%M')}\n"
        report += f"👥 Аккаунтов: {len(account_ids)}\n"
        report += "=" * 60 + "\n\n"
        
        total_followers = 0
        total_posts = 0
        total_likes = 0
        total_comments = 0
        total_analyzed_posts = 0
        valid_accounts = 0
        
        photos_total = 0
        videos_total = 0
        carousels_total = 0
        
        account_details = []
        
        # Собираем данные
        for username in usernames:
            try:
                user_info = client.user_info_by_username(username)
                medias = client.user_medias(user_info.pk, amount=20)
                
                if medias:
                    account_likes = sum(m.like_count for m in medias)
                    account_comments = sum(m.comment_count for m in medias)
                    
                    photos = sum(1 for m in medias if m.media_type == 1)
                    videos = sum(1 for m in medias if m.media_type == 2)
                    carousels = sum(1 for m in medias if m.media_type == 8)
                    
                    total_followers += user_info.follower_count
                    total_posts += user_info.media_count
                    total_likes += account_likes
                    total_comments += account_comments
                    total_analyzed_posts += len(medias)
                    valid_accounts += 1
                    
                    photos_total += photos
                    videos_total += videos
                    carousels_total += carousels
                    
                    account_details.append({
                        'username': username,
                        'followers': user_info.follower_count,
                        'posts': user_info.media_count,
                        'likes': account_likes,
                        'comments': account_comments
                    })
                    
            except Exception as e:
                logger.warning(f"Ошибка при анализе {username}: {e}")
        
        if valid_accounts > 0:
            # Общая статистика
            report += f"📊 ОБЩИЕ ПОКАЗАТЕЛИ:\n"
            report += f"✅ Успешно проанализировано: {valid_accounts} из {len(usernames)}\n"
            report += f"👥 Общее количество подписчиков: {total_followers:,}\n"
            report += f"📝 Общее количество постов: {total_posts:,}\n"
            report += f"❤️ Общие лайки (последние посты): {total_likes:,}\n"
            report += f"💬 Общие комментарии (последние посты): {total_comments:,}\n"
            report += f"📊 Проанализировано постов: {total_analyzed_posts}\n\n"
            
            # Средние показатели
            avg_followers = total_followers // valid_accounts
            avg_posts = total_posts // valid_accounts
            avg_likes_per_account = total_likes // valid_accounts
            avg_comments_per_account = total_comments // valid_accounts
            
            report += f"📈 СРЕДНИЕ ПОКАЗАТЕЛИ НА АККАУНТ:\n"
            report += f"👥 Средние подписчики: {avg_followers:,}\n"
            report += f"📝 Средние посты: {avg_posts:,}\n"
            report += f"❤️ Средние лайки: {avg_likes_per_account:,}\n"
            report += f"💬 Средние комментарии: {avg_comments_per_account:,}\n\n"
            
            # Анализ контента
            total_content = photos_total + videos_total + carousels_total
            if total_content > 0:
                report += f"📊 АНАЛИЗ КОНТЕНТА:\n"
                report += f"📷 Фото: {photos_total} ({photos_total/total_content*100:.1f}%)\n"
                report += f"🎥 Видео: {videos_total} ({videos_total/total_content*100:.1f}%)\n"
                report += f"🎠 Карусели: {carousels_total} ({carousels_total/total_content*100:.1f}%)\n\n"
            
            # Детали по аккаунтам
            report += f"👥 ДЕТАЛИ ПО АККАУНТАМ:\n"
            for i, acc in enumerate(account_details, 1):
                report += f"{i:2d}. @{acc['username']} - {acc['followers']:,} подписчиков, {acc['likes']:,} лайков\n"
        
        return report
        
    except Exception as e:
        return f"❌ Ошибка при сводной аналитике: {str(e)}"

def analyze_top_posts_all_accounts(account_ids: List[int], usernames: List[str]) -> str:
    """Лучшие посты всех аккаунтов"""
    try:
        client, error = get_authorized_client()
        if not client:
            return f"🏆 Лучшие посты всех аккаунтов\n\n{error}"
        
        report = f"🏆 ЛУЧШИЕ ПОСТЫ ВСЕХ АККАУНТОВ\n"
        report += f"📅 Дата анализа: {datetime.now().strftime('%d.%m.%Y %H:%M')}\n"
        report += f"👥 Аккаунтов: {len(account_ids)}\n"
        report += "=" * 60 + "\n\n"
        
        all_posts = []
        
        # Собираем посты от всех аккаунтов
        for username in usernames:
            try:
                user_info = client.user_info_by_username(username)
                medias = client.user_medias(user_info.pk, amount=20)
                
                for media in medias:
                    all_posts.append({
                        'username': username,
                        'media': media,
                        'score': media.like_count + media.comment_count * 3  # Взвешенный рейтинг
                    })
                    
            except Exception as e:
                logger.warning(f"Ошибка при получении постов {username}: {e}")
        
        if all_posts:
            # Сортируем по рейтингу
            top_posts = sorted(all_posts, key=lambda x: x['score'], reverse=True)[:20]
            
            report += f"🏆 ТОП-{len(top_posts)} ПОСТОВ ПО ВСЕМ АККАУНТАМ:\n\n"
            
            for i, post_data in enumerate(top_posts, 1):
                media = post_data['media']
                username = post_data['username']
                
                media_type_names = {1: 'Фото', 2: 'Видео', 8: 'Карусель'}
                media_type_name = media_type_names.get(media.media_type, f'Тип {media.media_type}')
                
                report += f"🏆 МЕСТО #{i}\n"
                report += f"👤 Аккаунт: @{username}\n"
                report += f"🔗 URL: https://www.instagram.com/p/{media.code}/\n"
                report += f"📅 Дата: {media.taken_at.strftime('%Y-%m-%d %H:%M:%S')}\n"
                report += f"📝 Тип: {media_type_name}\n"
                report += f"❤️ Лайки: {media.like_count:,}\n"
                report += f"💬 Комментарии: {media.comment_count:,}\n"
                report += f"🏆 Рейтинг: {post_data['score']:,}\n"
                
                if media.caption_text:
                    caption_preview = media.caption_text[:100] + "..." if len(media.caption_text) > 100 else media.caption_text
                    report += f"💬 Описание: {caption_preview}\n"
                
                report += "-" * 40 + "\n"
        
        return report
        
    except Exception as e:
        return f"❌ Ошибка при анализе лучших постов: {str(e)}"

def analyze_detailed_all_accounts(account_ids: List[int], usernames: List[str]) -> str:
    """Детальный отчет по всем аккаунтам"""
    try:
        client, error = get_authorized_client()
        if not client:
            return f"📋 Детальный отчет по всем аккаунтам\n\n{error}"
        
        report = f"📋 ДЕТАЛЬНЫЙ ОТЧЕТ ПО ВСЕМ АККАУНТАМ\n"
        report += f"📅 Дата анализа: {datetime.now().strftime('%d.%m.%Y %H:%M')}\n"
        report += f"👥 Аккаунтов: {len(account_ids)}\n"
        report += "=" * 60 + "\n\n"
        
        # Подробный анализ каждого аккаунта
        for i, username in enumerate(usernames, 1):
            try:
                report += f"👤 АККАУНТ #{i}: @{username}\n"
                report += "─" * 50 + "\n"
                
                user_info = client.user_info_by_username(username)
                medias = client.user_medias(user_info.pk, amount=30)
                
                # Информация о профиле
                report += f"📛 Имя: {user_info.full_name or 'Не указано'}\n"
                report += f"👥 Подписчики: {user_info.follower_count:,}\n"
                report += f"👤 Подписки: {user_info.following_count:,}\n"
                report += f"📝 Всего постов: {user_info.media_count:,}\n"
                report += f"📊 Проанализировано: {len(medias) if medias else 0} постов\n\n"
                
                if medias:
                    # Статистика по типам контента
                    photos = sum(1 for m in medias if m.media_type == 1)
                    videos = sum(1 for m in medias if m.media_type == 2)
                    carousels = sum(1 for m in medias if m.media_type == 8)
                    
                    report += f"📊 Типы контента:\n"
                    report += f"  📷 Фото: {photos} ({photos/len(medias)*100:.1f}%)\n"
                    report += f"  🎥 Видео: {videos} ({videos/len(medias)*100:.1f}%)\n"
                    report += f"  🎠 Карусели: {carousels} ({carousels/len(medias)*100:.1f}%)\n\n"
                    
                    # Статистика вовлеченности
                    total_likes = sum(m.like_count for m in medias)
                    total_comments = sum(m.comment_count for m in medias)
                    avg_likes = total_likes // len(medias)
                    avg_comments = total_comments // len(medias)
                    
                    report += f"📈 Вовлеченность:\n"
                    report += f"  ❤️ Всего лайков: {total_likes:,}\n"
                    report += f"  💬 Всего комментариев: {total_comments:,}\n"
                    report += f"  ❤️ Средние лайки: {avg_likes:,}\n"
                    report += f"  💬 Средние комментарии: {avg_comments:,}\n"
                    
                    if user_info.follower_count > 0:
                        engagement_rate = (avg_likes + avg_comments) / user_info.follower_count * 100
                        report += f"  📊 Engagement Rate: {engagement_rate:.2f}%\n"
                    
                    # Лучший пост
                    best_post = max(medias, key=lambda x: x.like_count + x.comment_count * 3)
                    report += f"\n🏆 Лучший пост:\n"
                    report += f"  🔗 https://www.instagram.com/p/{best_post.code}/\n"
                    report += f"  ❤️ {best_post.like_count:,} лайков, 💬 {best_post.comment_count:,} комментариев\n"
                    
                else:
                    report += "❌ Посты не найдены или профиль закрыт\n"
                
                report += "\n" + "=" * 60 + "\n\n"
                
            except Exception as e:
                report += f"❌ Ошибка при анализе @{username}: {str(e)}\n\n"
        
        return report
        
    except Exception as e:
        return f"❌ Ошибка при детальном анализе: {str(e)}"

def start_general_analytics(update: Update, context: CallbackContext):
    """Общая аналитика системы"""
    query = update.callback_query
    query.answer()
    
    try:
        from database.db_manager import get_publish_tasks
        from database.models import TaskStatus, TaskType
        
        # Получаем данные для анализа
        accounts = get_instagram_accounts()
        
        # Получаем задачи за последнюю неделю
        week_ago = datetime.now() - timedelta(days=7)
        
        report = f"📊 ОБЩАЯ СТАТИСТИКА СИСТЕМЫ\n"
        report += f"📅 Дата анализа: {datetime.now().strftime('%d.%m.%Y %H:%M')}\n"
        report += "=" * 50 + "\n\n"
        
        # Статистика аккаунтов
        active_accounts = [acc for acc in accounts if acc.is_active]
        report += f"👥 АККАУНТЫ:\n"
        report += f"📊 Всего: {len(accounts)}\n"
        report += f"✅ Активных: {len(active_accounts)}\n"
        report += f"❌ Неактивных: {len(accounts) - len(active_accounts)}\n\n"
        
        # Статистика папок
        from database.db_manager import get_account_groups
        groups = get_account_groups()
        report += f"📁 ПАПКИ:\n"
        report += f"📊 Всего папок: {len(groups)}\n"
        for group in groups:
            from database.db_manager import get_accounts_in_group
            group_accounts = get_accounts_in_group(group.id)
            report += f"   📁 {group.name}: {len(group_accounts)} аккаунтов\n"
        report += "\n"
        
        # Статистика прокси
        from database.db_manager import get_proxies
        proxies = get_proxies()
        active_proxies = [p for p in proxies if p.is_active]
        report += f"🌐 ПРОКСИ:\n"
        report += f"📊 Всего: {len(proxies)}\n"
        report += f"✅ Активных: {len(active_proxies)}\n"
        report += f"❌ Неактивных: {len(proxies) - len(active_proxies)}\n\n"
        
        # Статистика системы
        import psutil
        cpu_percent = psutil.cpu_percent(interval=1)
        memory = psutil.virtual_memory()
        
        report += f"🖥️ СИСТЕМА:\n"
        report += f"⚡ CPU: {cpu_percent:.1f}%\n"
        report += f"💾 RAM: {memory.percent:.1f}% ({memory.used // 1024**3:.1f}GB / {memory.total // 1024**3:.1f}GB)\n"
        
        # Проверяем место на диске
        disk = psutil.disk_usage('/')
        report += f"💽 Диск: {disk.percent:.1f}% ({disk.used // 1024**3:.1f}GB / {disk.total // 1024**3:.1f}GB)\n\n"
        
        report += f"🔧 РЕКОМЕНДАЦИИ:\n"
        if len(active_accounts) == 0:
            report += "⚠️ Нет активных аккаунтов - добавьте аккаунты\n"
        elif len(active_accounts) < 5:
            report += "💡 Мало аккаунтов для эффективной работы\n"
        
        if len(active_proxies) == 0:
            report += "⚠️ Нет активных прокси - добавьте прокси\n"
        elif len(active_proxies) < len(active_accounts):
            report += "💡 Прокси меньше чем аккаунтов - рекомендуется 1:1\n"
        
        if memory.percent > 80:
            report += "⚠️ Высокое использование RAM\n"
        
        if disk.percent > 90:
            report += "⚠️ Мало места на диске\n"
        
        # Создаем и отправляем файл
        filename = f"general_statistics_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False, encoding='utf-8') as f:
            f.write(report)
            temp_file_path = f.name
        
        with open(temp_file_path, 'rb') as f:
            query.message.reply_document(
                document=f,
                filename=filename,
                caption="📊 Общая статистика системы"
            )
        
        os.unlink(temp_file_path)
        
        # Возвращаемся к меню статистики
        from telegram_bot.keyboards import get_statistics_menu_keyboard
        query.edit_message_text(
            "✅ Общая статистика отправлена файлом",
            reply_markup=get_statistics_menu_keyboard()
        )
        
    except Exception as e:
        logger.error(f"Ошибка при общей аналитике: {e}")
        query.edit_message_text(
            f"❌ Ошибка при общей аналитике: {str(e)}",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Назад", callback_data="analytics_menu")]
            ])
        ) 
def analyze_accounts_comparison(account_ids: List[int], usernames: List[str]) -> str:
    """Сравнительная аналитика нескольких аккаунтов"""
    try:
        client, error = get_authorized_client()
        if not client:
            return f"📊 Сравнительная аналитика для {len(account_ids)} аккаунтов\n\n{error}"
        
        report = f"📊 СРАВНИТЕЛЬНАЯ АНАЛИТИКА\n"
        report += f"📅 Дата анализа: {datetime.now().strftime('%d.%m.%Y %H:%M')}\n"
        report += f"👥 Аккаунтов: {len(account_ids)}\n"
        report += "=" * 60 + "\n\n"
        
        accounts_data = []
        
        # Собираем данные по каждому аккаунту
        for i, username in enumerate(usernames):
            try:
                user_info = client.user_info_by_username(username)
                medias = client.user_medias(user_info.pk, amount=20)
                
                if medias:
                    total_likes = sum(m.like_count for m in medias)
                    total_comments = sum(m.comment_count for m in medias)
                    avg_likes = total_likes // len(medias)
                    avg_comments = total_comments // len(medias)
                    
                    # Определяем типы контента
                    photos = sum(1 for m in medias if m.media_type == 1)
                    videos = sum(1 for m in medias if m.media_type == 2)
                    carousels = sum(1 for m in medias if m.media_type == 8)
                    
                    accounts_data.append({
                        'username': username,
                        'followers': user_info.follower_count,
                        'following': user_info.following_count,
                        'posts_count': user_info.media_count,
                        'analyzed_posts': len(medias),
                        'total_likes': total_likes,
                        'total_comments': total_comments,
                        'avg_likes': avg_likes,
                        'avg_comments': avg_comments,
                        'photos': photos,
                        'videos': videos,
                        'carousels': carousels,
                        'engagement_rate': (avg_likes + avg_comments) / user_info.follower_count * 100 if user_info.follower_count > 0 else 0
                    })
                    
            except Exception as e:
                logger.warning(f"Ошибка при анализе {username}: {e}")
                accounts_data.append({
                    'username': username,
                    'error': str(e)
                })
        
        # Формируем сравнительный отчет
        if accounts_data:
            report += "📊 СРАВНЕНИЕ АККАУНТОВ:\n\n"
            
            for i, data in enumerate(accounts_data, 1):
                if 'error' in data:
                    report += f"{i:2d}. @{data['username']} - ❌ Ошибка: {data['error']}\n"
                    continue
                
                report += f"{i:2d}. @{data['username']}\n"
                report += f"    👥 Подписчики: {data['followers']:,}\n"
                report += f"    📝 Постов: {data['posts_count']:,}\n"
                report += f"    ❤️ Средние лайки: {data['avg_likes']:,}\n"
                report += f"    💬 Средние комментарии: {data['avg_comments']:,}\n"
                report += f"    📊 ER: {data['engagement_rate']:.2f}%\n"
                report += f"    📷 Фото: {data['photos']} | 🎥 Видео: {data['videos']} | 🎠 Карусели: {data['carousels']}\n\n"
            
            # Рейтинги
            valid_accounts = [d for d in accounts_data if 'error' not in d]
            if len(valid_accounts) > 1:
                report += "🏆 РЕЙТИНГИ:\n\n"
                
                # Топ по подписчикам
                top_followers = sorted(valid_accounts, key=lambda x: x['followers'], reverse=True)
                report += "👥 Топ по подписчикам:\n"
                for i, acc in enumerate(top_followers[:5], 1):
                    report += f"  {i}. @{acc['username']} - {acc['followers']:,}\n"
                report += "\n"
                
                # Топ по engagement rate
                top_er = sorted(valid_accounts, key=lambda x: x['engagement_rate'], reverse=True)
                report += "📊 Топ по Engagement Rate:\n"
                for i, acc in enumerate(top_er[:5], 1):
                    report += f"  {i}. @{acc['username']} - {acc['engagement_rate']:.2f}%\n"
                report += "\n"
                
                # Топ по средним лайкам
                top_likes = sorted(valid_accounts, key=lambda x: x['avg_likes'], reverse=True)
                report += "❤️ Топ по средним лайкам:\n"
                for i, acc in enumerate(top_likes[:5], 1):
                    report += f"  {i}. @{acc['username']} - {acc['avg_likes']:,}\n"
        
        return report
        
    except Exception as e:
        return f"❌ Ошибка при сравнительной аналитике: {str(e)}"

def analyze_accounts_summary(account_ids: List[int], usernames: List[str]) -> str:
    """Сводная статистика нескольких аккаунтов"""
    try:
        client, error = get_authorized_client()
        if not client:
            return f"📈 Сводная статистика для {len(account_ids)} аккаунтов\n\n{error}"
        
        report = f"📈 СВОДНАЯ СТАТИСТИКА\n"
        report += f"📅 Дата анализа: {datetime.now().strftime('%d.%m.%Y %H:%M')}\n"
        report += f"👥 Аккаунтов: {len(account_ids)}\n"
        report += "=" * 60 + "\n\n"
        
        total_followers = 0
        total_posts = 0
        total_likes = 0
        total_comments = 0
        total_analyzed_posts = 0
        valid_accounts = 0
        
        photos_total = 0
        videos_total = 0
        carousels_total = 0
        
        account_details = []
        
        # Собираем данные
        for username in usernames:
            try:
                user_info = client.user_info_by_username(username)
                medias = client.user_medias(user_info.pk, amount=20)
                
                if medias:
                    account_likes = sum(m.like_count for m in medias)
                    account_comments = sum(m.comment_count for m in medias)
                    
                    photos = sum(1 for m in medias if m.media_type == 1)
                    videos = sum(1 for m in medias if m.media_type == 2)
                    carousels = sum(1 for m in medias if m.media_type == 8)
                    
                    total_followers += user_info.follower_count
                    total_posts += user_info.media_count
                    total_likes += account_likes
                    total_comments += account_comments
                    total_analyzed_posts += len(medias)
                    valid_accounts += 1
                    
                    photos_total += photos
                    videos_total += videos
                    carousels_total += carousels
                    
                    account_details.append({
                        'username': username,
                        'followers': user_info.follower_count,
                        'posts': user_info.media_count,
                        'likes': account_likes,
                        'comments': account_comments
                    })
                    
            except Exception as e:
                logger.warning(f"Ошибка при анализе {username}: {e}")
        
        if valid_accounts > 0:
            # Общая статистика
            report += f"📊 ОБЩИЕ ПОКАЗАТЕЛИ:\n"
            report += f"✅ Успешно проанализировано: {valid_accounts} из {len(usernames)}\n"
            report += f"👥 Общее количество подписчиков: {total_followers:,}\n"
            report += f"📝 Общее количество постов: {total_posts:,}\n"
            report += f"❤️ Общие лайки (последние посты): {total_likes:,}\n"
            report += f"💬 Общие комментарии (последние посты): {total_comments:,}\n"
            report += f"📊 Проанализировано постов: {total_analyzed_posts}\n\n"
            
            # Средние показатели
            avg_followers = total_followers // valid_accounts
            avg_posts = total_posts // valid_accounts
            avg_likes_per_account = total_likes // valid_accounts
            avg_comments_per_account = total_comments // valid_accounts
            
            report += f"📈 СРЕДНИЕ ПОКАЗАТЕЛИ НА АККАУНТ:\n"
            report += f"👥 Средние подписчики: {avg_followers:,}\n"
            report += f"📝 Средние посты: {avg_posts:,}\n"
            report += f"❤️ Средние лайки: {avg_likes_per_account:,}\n"
            report += f"💬 Средние комментарии: {avg_comments_per_account:,}\n\n"
            
            # Анализ контента
            total_content = photos_total + videos_total + carousels_total
            if total_content > 0:
                report += f"📊 АНАЛИЗ КОНТЕНТА:\n"
                report += f"📷 Фото: {photos_total} ({photos_total/total_content*100:.1f}%)\n"
                report += f"🎥 Видео: {videos_total} ({videos_total/total_content*100:.1f}%)\n"
                report += f"🎠 Карусели: {carousels_total} ({carousels_total/total_content*100:.1f}%)\n\n"
            
            # Детали по аккаунтам
            report += f"👥 ДЕТАЛИ ПО АККАУНТАМ:\n"
            for i, acc in enumerate(account_details, 1):
                report += f"{i:2d}. @{acc['username']} - {acc['followers']:,} подписчиков, {acc['likes']:,} лайков\n"
        
        return report
        
    except Exception as e:
        return f"❌ Ошибка при сводной аналитике: {str(e)}"

def analyze_top_posts_all_accounts(account_ids: List[int], usernames: List[str]) -> str:
    """Лучшие посты всех аккаунтов"""
    try:
        client, error = get_authorized_client()
        if not client:
            return f"🏆 Лучшие посты всех аккаунтов\n\n{error}"
        
        report = f"🏆 ЛУЧШИЕ ПОСТЫ ВСЕХ АККАУНТОВ\n"
        report += f"📅 Дата анализа: {datetime.now().strftime('%d.%m.%Y %H:%M')}\n"
        report += f"👥 Аккаунтов: {len(account_ids)}\n"
        report += "=" * 60 + "\n\n"
        
        all_posts = []
        
        # Собираем посты от всех аккаунтов
        for username in usernames:
            try:
                user_info = client.user_info_by_username(username)
                medias = client.user_medias(user_info.pk, amount=20)
                
                for media in medias:
                    all_posts.append({
                        'username': username,
                        'media': media,
                        'score': media.like_count + media.comment_count * 3  # Взвешенный рейтинг
                    })
                    
            except Exception as e:
                logger.warning(f"Ошибка при получении постов {username}: {e}")
        
        if all_posts:
            # Сортируем по рейтингу
            top_posts = sorted(all_posts, key=lambda x: x['score'], reverse=True)[:20]
            
            report += f"🏆 ТОП-{len(top_posts)} ПОСТОВ ПО ВСЕМ АККАУНТАМ:\n\n"
            
            for i, post_data in enumerate(top_posts, 1):
                media = post_data['media']
                username = post_data['username']
                
                media_type_names = {1: 'Фото', 2: 'Видео', 8: 'Карусель'}
                media_type_name = media_type_names.get(media.media_type, f'Тип {media.media_type}')
                
                report += f"🏆 МЕСТО #{i}\n"
                report += f"👤 Аккаунт: @{username}\n"
                report += f"🔗 URL: https://www.instagram.com/p/{media.code}/\n"
                report += f"📅 Дата: {media.taken_at.strftime('%Y-%m-%d %H:%M:%S')}\n"
                report += f"📝 Тип: {media_type_name}\n"
                report += f"❤️ Лайки: {media.like_count:,}\n"
                report += f"💬 Комментарии: {media.comment_count:,}\n"
                report += f"🏆 Рейтинг: {post_data['score']:,}\n"
                
                if media.caption_text:
                    caption_preview = media.caption_text[:100] + "..." if len(media.caption_text) > 100 else media.caption_text
                    report += f"💬 Описание: {caption_preview}\n"
                
                report += "-" * 40 + "\n"
        
        return report
        
    except Exception as e:
        return f"❌ Ошибка при анализе лучших постов: {str(e)}"

def analyze_detailed_all_accounts(account_ids: List[int], usernames: List[str]) -> str:
    """Детальный отчет по всем аккаунтам"""
    try:
        client, error = get_authorized_client()
        if not client:
            return f"📋 Детальный отчет по всем аккаунтам\n\n{error}"
        
        report = f"📋 ДЕТАЛЬНЫЙ ОТЧЕТ ПО ВСЕМ АККАУНТАМ\n"
        report += f"📅 Дата анализа: {datetime.now().strftime('%d.%m.%Y %H:%M')}\n"
        report += f"👥 Аккаунтов: {len(account_ids)}\n"
        report += "=" * 60 + "\n\n"
        
        # Подробный анализ каждого аккаунта
        for i, username in enumerate(usernames, 1):
            try:
                report += f"👤 АККАУНТ #{i}: @{username}\n"
                report += "─" * 50 + "\n"
                
                user_info = client.user_info_by_username(username)
                medias = client.user_medias(user_info.pk, amount=30)
                
                # Информация о профиле
                report += f"📛 Имя: {user_info.full_name or 'Не указано'}\n"
                report += f"👥 Подписчики: {user_info.follower_count:,}\n"
                report += f"👤 Подписки: {user_info.following_count:,}\n"
                report += f"📝 Всего постов: {user_info.media_count:,}\n"
                report += f"📊 Проанализировано: {len(medias) if medias else 0} постов\n\n"
                
                if medias:
                    # Статистика по типам контента
                    photos = sum(1 for m in medias if m.media_type == 1)
                    videos = sum(1 for m in medias if m.media_type == 2)
                    carousels = sum(1 for m in medias if m.media_type == 8)
                    
                    report += f"📊 Типы контента:\n"
                    report += f"  📷 Фото: {photos} ({photos/len(medias)*100:.1f}%)\n"
                    report += f"  🎥 Видео: {videos} ({videos/len(medias)*100:.1f}%)\n"
                    report += f"  🎠 Карусели: {carousels} ({carousels/len(medias)*100:.1f}%)\n\n"
                    
                    # Статистика вовлеченности
                    total_likes = sum(m.like_count for m in medias)
                    total_comments = sum(m.comment_count for m in medias)
                    avg_likes = total_likes // len(medias)
                    avg_comments = total_comments // len(medias)
                    
                    report += f"📈 Вовлеченность:\n"
                    report += f"  ❤️ Всего лайков: {total_likes:,}\n"
                    report += f"  💬 Всего комментариев: {total_comments:,}\n"
                    report += f"  ❤️ Средние лайки: {avg_likes:,}\n"
                    report += f"  💬 Средние комментарии: {avg_comments:,}\n"
                    
                    if user_info.follower_count > 0:
                        engagement_rate = (avg_likes + avg_comments) / user_info.follower_count * 100
                        report += f"  📊 Engagement Rate: {engagement_rate:.2f}%\n"
                    
                    # Лучший пост
                    best_post = max(medias, key=lambda x: x.like_count + x.comment_count * 3)
                    report += f"\n🏆 Лучший пост:\n"
                    report += f"  🔗 https://www.instagram.com/p/{best_post.code}/\n"
                    report += f"  ❤️ {best_post.like_count:,} лайков, 💬 {best_post.comment_count:,} комментариев\n"
                    
                else:
                    report += "❌ Посты не найдены или профиль закрыт\n"
                
                report += "\n" + "=" * 60 + "\n\n"
                
            except Exception as e:
                report += f"❌ Ошибка при анализе @{username}: {str(e)}\n\n"
        
        return report
        
    except Exception as e:
        return f"❌ Ошибка при детальном анализе: {str(e)}"

def start_general_analytics(update: Update, context: CallbackContext):
    """Общая аналитика системы"""
    query = update.callback_query
    query.answer()
    
    try:
        from database.db_manager import get_publish_tasks
        from database.models import TaskStatus, TaskType
        
        # Получаем данные для анализа
        accounts = get_instagram_accounts()
        
        # Получаем задачи за последнюю неделю
        week_ago = datetime.now() - timedelta(days=7)
        
        report = f"📊 ОБЩАЯ СТАТИСТИКА СИСТЕМЫ\n"
        report += f"📅 Дата анализа: {datetime.now().strftime('%d.%m.%Y %H:%M')}\n"
        report += "=" * 50 + "\n\n"
        
        # Статистика аккаунтов
        active_accounts = [acc for acc in accounts if acc.is_active]
        report += f"👥 АККАУНТЫ:\n"
        report += f"📊 Всего: {len(accounts)}\n"
        report += f"✅ Активных: {len(active_accounts)}\n"
        report += f"❌ Неактивных: {len(accounts) - len(active_accounts)}\n\n"
        
        # Статистика папок
        from database.db_manager import get_account_groups
        groups = get_account_groups()
        report += f"📁 ПАПКИ:\n"
        report += f"📊 Всего папок: {len(groups)}\n"
        for group in groups:
            from database.db_manager import get_accounts_in_group
            group_accounts = get_accounts_in_group(group.id)
            report += f"   📁 {group.name}: {len(group_accounts)} аккаунтов\n"
        report += "\n"
        
        # Статистика прокси
        from database.db_manager import get_proxies
        proxies = get_proxies()
        active_proxies = [p for p in proxies if p.is_active]
        report += f"🌐 ПРОКСИ:\n"
        report += f"📊 Всего: {len(proxies)}\n"
        report += f"✅ Активных: {len(active_proxies)}\n"
        report += f"❌ Неактивных: {len(proxies) - len(active_proxies)}\n\n"
        
        # Статистика системы
        import psutil
        cpu_percent = psutil.cpu_percent(interval=1)
        memory = psutil.virtual_memory()
        
        report += f"🖥️ СИСТЕМА:\n"
        report += f"⚡ CPU: {cpu_percent:.1f}%\n"
        report += f"💾 RAM: {memory.percent:.1f}% ({memory.used // 1024**3:.1f}GB / {memory.total // 1024**3:.1f}GB)\n"
        
        # Проверяем место на диске
        disk = psutil.disk_usage('/')
        report += f"💽 Диск: {disk.percent:.1f}% ({disk.used // 1024**3:.1f}GB / {disk.total // 1024**3:.1f}GB)\n\n"
        
        report += f"🔧 РЕКОМЕНДАЦИИ:\n"
        if len(active_accounts) == 0:
            report += "⚠️ Нет активных аккаунтов - добавьте аккаунты\n"
        elif len(active_accounts) < 5:
            report += "💡 Мало аккаунтов для эффективной работы\n"
        
        if len(active_proxies) == 0:
            report += "⚠️ Нет активных прокси - добавьте прокси\n"
        elif len(active_proxies) < len(active_accounts):
            report += "💡 Прокси меньше чем аккаунтов - рекомендуется 1:1\n"
        
        if memory.percent > 80:
            report += "⚠️ Высокое использование RAM\n"
        
        if disk.percent > 90:
            report += "⚠️ Мало места на диске\n"
        
        # Создаем и отправляем файл
        filename = f"general_statistics_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False, encoding='utf-8') as f:
            f.write(report)
            temp_file_path = f.name
        
        with open(temp_file_path, 'rb') as f:
            query.message.reply_document(
                document=f,
                filename=filename,
                caption="📊 Общая статистика системы"
            )
        
        os.unlink(temp_file_path)
        
        # Возвращаемся к меню статистики
        from telegram_bot.keyboards import get_statistics_menu_keyboard
        query.edit_message_text(
            "✅ Общая статистика отправлена файлом",
            reply_markup=get_statistics_menu_keyboard()
        )
        
    except Exception as e:
        logger.error(f"Ошибка при общей аналитике: {e}")
        query.edit_message_text(
            f"❌ Ошибка при общей аналитике: {str(e)}",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Назад", callback_data="analytics_menu")]
            ])
        ) 
def analyze_accounts_comparison(account_ids: List[int], usernames: List[str]) -> str:
    """Сравнительная аналитика нескольких аккаунтов"""
    try:
        client, error = get_authorized_client()
        if not client:
            return f"📊 Сравнительная аналитика для {len(account_ids)} аккаунтов\n\n{error}"
        
        report = f"📊 СРАВНИТЕЛЬНАЯ АНАЛИТИКА\n"
        report += f"📅 Дата анализа: {datetime.now().strftime('%d.%m.%Y %H:%M')}\n"
        report += f"👥 Аккаунтов: {len(account_ids)}\n"
        report += "=" * 60 + "\n\n"
        
        accounts_data = []
        
        # Собираем данные по каждому аккаунту
        for i, username in enumerate(usernames):
            try:
                user_info = client.user_info_by_username(username)
                medias = client.user_medias(user_info.pk, amount=20)
                
                if medias:
                    total_likes = sum(m.like_count for m in medias)
                    total_comments = sum(m.comment_count for m in medias)
                    avg_likes = total_likes // len(medias)
                    avg_comments = total_comments // len(medias)
                    
                    # Определяем типы контента
                    photos = sum(1 for m in medias if m.media_type == 1)
                    videos = sum(1 for m in medias if m.media_type == 2)
                    carousels = sum(1 for m in medias if m.media_type == 8)
                    
                    accounts_data.append({
                        'username': username,
                        'followers': user_info.follower_count,
                        'following': user_info.following_count,
                        'posts_count': user_info.media_count,
                        'analyzed_posts': len(medias),
                        'total_likes': total_likes,
                        'total_comments': total_comments,
                        'avg_likes': avg_likes,
                        'avg_comments': avg_comments,
                        'photos': photos,
                        'videos': videos,
                        'carousels': carousels,
                        'engagement_rate': (avg_likes + avg_comments) / user_info.follower_count * 100 if user_info.follower_count > 0 else 0
                    })
                    
            except Exception as e:
                logger.warning(f"Ошибка при анализе {username}: {e}")
                accounts_data.append({
                    'username': username,
                    'error': str(e)
                })
        
        # Формируем сравнительный отчет
        if accounts_data:
            report += "📊 СРАВНЕНИЕ АККАУНТОВ:\n\n"
            
            for i, data in enumerate(accounts_data, 1):
                if 'error' in data:
                    report += f"{i:2d}. @{data['username']} - ❌ Ошибка: {data['error']}\n"
                    continue
                
                report += f"{i:2d}. @{data['username']}\n"
                report += f"    👥 Подписчики: {data['followers']:,}\n"
                report += f"    📝 Постов: {data['posts_count']:,}\n"
                report += f"    ❤️ Средние лайки: {data['avg_likes']:,}\n"
                report += f"    💬 Средние комментарии: {data['avg_comments']:,}\n"
                report += f"    📊 ER: {data['engagement_rate']:.2f}%\n"
                report += f"    📷 Фото: {data['photos']} | 🎥 Видео: {data['videos']} | 🎠 Карусели: {data['carousels']}\n\n"
            
            # Рейтинги
            valid_accounts = [d for d in accounts_data if 'error' not in d]
            if len(valid_accounts) > 1:
                report += "🏆 РЕЙТИНГИ:\n\n"
                
                # Топ по подписчикам
                top_followers = sorted(valid_accounts, key=lambda x: x['followers'], reverse=True)
                report += "👥 Топ по подписчикам:\n"
                for i, acc in enumerate(top_followers[:5], 1):
                    report += f"  {i}. @{acc['username']} - {acc['followers']:,}\n"
                report += "\n"
                
                # Топ по engagement rate
                top_er = sorted(valid_accounts, key=lambda x: x['engagement_rate'], reverse=True)
                report += "📊 Топ по Engagement Rate:\n"
                for i, acc in enumerate(top_er[:5], 1):
                    report += f"  {i}. @{acc['username']} - {acc['engagement_rate']:.2f}%\n"
                report += "\n"
                
                # Топ по средним лайкам
                top_likes = sorted(valid_accounts, key=lambda x: x['avg_likes'], reverse=True)
                report += "❤️ Топ по средним лайкам:\n"
                for i, acc in enumerate(top_likes[:5], 1):
                    report += f"  {i}. @{acc['username']} - {acc['avg_likes']:,}\n"
        
        return report
        
    except Exception as e:
        return f"❌ Ошибка при сравнительной аналитике: {str(e)}"

def analyze_accounts_summary(account_ids: List[int], usernames: List[str]) -> str:
    """Сводная статистика нескольких аккаунтов"""
    try:
        client, error = get_authorized_client()
        if not client:
            return f"📈 Сводная статистика для {len(account_ids)} аккаунтов\n\n{error}"
        
        report = f"📈 СВОДНАЯ СТАТИСТИКА\n"
        report += f"📅 Дата анализа: {datetime.now().strftime('%d.%m.%Y %H:%M')}\n"
        report += f"👥 Аккаунтов: {len(account_ids)}\n"
        report += "=" * 60 + "\n\n"
        
        total_followers = 0
        total_posts = 0
        total_likes = 0
        total_comments = 0
        total_analyzed_posts = 0
        valid_accounts = 0
        
        photos_total = 0
        videos_total = 0
        carousels_total = 0
        
        account_details = []
        
        # Собираем данные
        for username in usernames:
            try:
                user_info = client.user_info_by_username(username)
                medias = client.user_medias(user_info.pk, amount=20)
                
                if medias:
                    account_likes = sum(m.like_count for m in medias)
                    account_comments = sum(m.comment_count for m in medias)
                    
                    photos = sum(1 for m in medias if m.media_type == 1)
                    videos = sum(1 for m in medias if m.media_type == 2)
                    carousels = sum(1 for m in medias if m.media_type == 8)
                    
                    total_followers += user_info.follower_count
                    total_posts += user_info.media_count
                    total_likes += account_likes
                    total_comments += account_comments
                    total_analyzed_posts += len(medias)
                    valid_accounts += 1
                    
                    photos_total += photos
                    videos_total += videos
                    carousels_total += carousels
                    
                    account_details.append({
                        'username': username,
                        'followers': user_info.follower_count,
                        'posts': user_info.media_count,
                        'likes': account_likes,
                        'comments': account_comments
                    })
                    
            except Exception as e:
                logger.warning(f"Ошибка при анализе {username}: {e}")
        
        if valid_accounts > 0:
            # Общая статистика
            report += f"📊 ОБЩИЕ ПОКАЗАТЕЛИ:\n"
            report += f"✅ Успешно проанализировано: {valid_accounts} из {len(usernames)}\n"
            report += f"👥 Общее количество подписчиков: {total_followers:,}\n"
            report += f"📝 Общее количество постов: {total_posts:,}\n"
            report += f"❤️ Общие лайки (последние посты): {total_likes:,}\n"
            report += f"💬 Общие комментарии (последние посты): {total_comments:,}\n"
            report += f"📊 Проанализировано постов: {total_analyzed_posts}\n\n"
            
            # Средние показатели
            avg_followers = total_followers // valid_accounts
            avg_posts = total_posts // valid_accounts
            avg_likes_per_account = total_likes // valid_accounts
            avg_comments_per_account = total_comments // valid_accounts
            
            report += f"📈 СРЕДНИЕ ПОКАЗАТЕЛИ НА АККАУНТ:\n"
            report += f"👥 Средние подписчики: {avg_followers:,}\n"
            report += f"📝 Средние посты: {avg_posts:,}\n"
            report += f"❤️ Средние лайки: {avg_likes_per_account:,}\n"
            report += f"💬 Средние комментарии: {avg_comments_per_account:,}\n\n"
            
            # Анализ контента
            total_content = photos_total + videos_total + carousels_total
            if total_content > 0:
                report += f"📊 АНАЛИЗ КОНТЕНТА:\n"
                report += f"📷 Фото: {photos_total} ({photos_total/total_content*100:.1f}%)\n"
                report += f"🎥 Видео: {videos_total} ({videos_total/total_content*100:.1f}%)\n"
                report += f"🎠 Карусели: {carousels_total} ({carousels_total/total_content*100:.1f}%)\n\n"
            
            # Детали по аккаунтам
            report += f"👥 ДЕТАЛИ ПО АККАУНТАМ:\n"
            for i, acc in enumerate(account_details, 1):
                report += f"{i:2d}. @{acc['username']} - {acc['followers']:,} подписчиков, {acc['likes']:,} лайков\n"
        
        return report
        
    except Exception as e:
        return f"❌ Ошибка при сводной аналитике: {str(e)}"

def analyze_top_posts_all_accounts(account_ids: List[int], usernames: List[str]) -> str:
    """Лучшие посты всех аккаунтов"""
    try:
        client, error = get_authorized_client()
        if not client:
            return f"🏆 Лучшие посты всех аккаунтов\n\n{error}"
        
        report = f"🏆 ЛУЧШИЕ ПОСТЫ ВСЕХ АККАУНТОВ\n"
        report += f"📅 Дата анализа: {datetime.now().strftime('%d.%m.%Y %H:%M')}\n"
        report += f"👥 Аккаунтов: {len(account_ids)}\n"
        report += "=" * 60 + "\n\n"
        
        all_posts = []
        
        # Собираем посты от всех аккаунтов
        for username in usernames:
            try:
                user_info = client.user_info_by_username(username)
                medias = client.user_medias(user_info.pk, amount=20)
                
                for media in medias:
                    all_posts.append({
                        'username': username,
                        'media': media,
                        'score': media.like_count + media.comment_count * 3  # Взвешенный рейтинг
                    })
                    
            except Exception as e:
                logger.warning(f"Ошибка при получении постов {username}: {e}")
        
        if all_posts:
            # Сортируем по рейтингу
            top_posts = sorted(all_posts, key=lambda x: x['score'], reverse=True)[:20]
            
            report += f"🏆 ТОП-{len(top_posts)} ПОСТОВ ПО ВСЕМ АККАУНТАМ:\n\n"
            
            for i, post_data in enumerate(top_posts, 1):
                media = post_data['media']
                username = post_data['username']
                
                media_type_names = {1: 'Фото', 2: 'Видео', 8: 'Карусель'}
                media_type_name = media_type_names.get(media.media_type, f'Тип {media.media_type}')
                
                report += f"🏆 МЕСТО #{i}\n"
                report += f"👤 Аккаунт: @{username}\n"
                report += f"🔗 URL: https://www.instagram.com/p/{media.code}/\n"
                report += f"📅 Дата: {media.taken_at.strftime('%Y-%m-%d %H:%M:%S')}\n"
                report += f"📝 Тип: {media_type_name}\n"
                report += f"❤️ Лайки: {media.like_count:,}\n"
                report += f"💬 Комментарии: {media.comment_count:,}\n"
                report += f"🏆 Рейтинг: {post_data['score']:,}\n"
                
                if media.caption_text:
                    caption_preview = media.caption_text[:100] + "..." if len(media.caption_text) > 100 else media.caption_text
                    report += f"💬 Описание: {caption_preview}\n"
                
                report += "-" * 40 + "\n"
        
        return report
        
    except Exception as e:
        return f"❌ Ошибка при анализе лучших постов: {str(e)}"

def analyze_detailed_all_accounts(account_ids: List[int], usernames: List[str]) -> str:
    """Детальный отчет по всем аккаунтам"""
    try:
        client, error = get_authorized_client()
        if not client:
            return f"📋 Детальный отчет по всем аккаунтам\n\n{error}"
        
        report = f"📋 ДЕТАЛЬНЫЙ ОТЧЕТ ПО ВСЕМ АККАУНТАМ\n"
        report += f"📅 Дата анализа: {datetime.now().strftime('%d.%m.%Y %H:%M')}\n"
        report += f"👥 Аккаунтов: {len(account_ids)}\n"
        report += "=" * 60 + "\n\n"
        
        # Подробный анализ каждого аккаунта
        for i, username in enumerate(usernames, 1):
            try:
                report += f"👤 АККАУНТ #{i}: @{username}\n"
                report += "─" * 50 + "\n"
                
                user_info = client.user_info_by_username(username)
                medias = client.user_medias(user_info.pk, amount=30)
                
                # Информация о профиле
                report += f"📛 Имя: {user_info.full_name or 'Не указано'}\n"
                report += f"👥 Подписчики: {user_info.follower_count:,}\n"
                report += f"👤 Подписки: {user_info.following_count:,}\n"
                report += f"📝 Всего постов: {user_info.media_count:,}\n"
                report += f"📊 Проанализировано: {len(medias) if medias else 0} постов\n\n"
                
                if medias:
                    # Статистика по типам контента
                    photos = sum(1 for m in medias if m.media_type == 1)
                    videos = sum(1 for m in medias if m.media_type == 2)
                    carousels = sum(1 for m in medias if m.media_type == 8)
                    
                    report += f"📊 Типы контента:\n"
                    report += f"  📷 Фото: {photos} ({photos/len(medias)*100:.1f}%)\n"
                    report += f"  🎥 Видео: {videos} ({videos/len(medias)*100:.1f}%)\n"
                    report += f"  🎠 Карусели: {carousels} ({carousels/len(medias)*100:.1f}%)\n\n"
                    
                    # Статистика вовлеченности
                    total_likes = sum(m.like_count for m in medias)
                    total_comments = sum(m.comment_count for m in medias)
                    avg_likes = total_likes // len(medias)
                    avg_comments = total_comments // len(medias)
                    
                    report += f"📈 Вовлеченность:\n"
                    report += f"  ❤️ Всего лайков: {total_likes:,}\n"
                    report += f"  💬 Всего комментариев: {total_comments:,}\n"
                    report += f"  ❤️ Средние лайки: {avg_likes:,}\n"
                    report += f"  💬 Средние комментарии: {avg_comments:,}\n"
                    
                    if user_info.follower_count > 0:
                        engagement_rate = (avg_likes + avg_comments) / user_info.follower_count * 100
                        report += f"  📊 Engagement Rate: {engagement_rate:.2f}%\n"
                    
                    # Лучший пост
                    best_post = max(medias, key=lambda x: x.like_count + x.comment_count * 3)
                    report += f"\n🏆 Лучший пост:\n"
                    report += f"  🔗 https://www.instagram.com/p/{best_post.code}/\n"
                    report += f"  ❤️ {best_post.like_count:,} лайков, 💬 {best_post.comment_count:,} комментариев\n"
                    
                else:
                    report += "❌ Посты не найдены или профиль закрыт\n"
                
                report += "\n" + "=" * 60 + "\n\n"
                
            except Exception as e:
                report += f"❌ Ошибка при анализе @{username}: {str(e)}\n\n"
        
        return report
        
    except Exception as e:
        return f"❌ Ошибка при детальном анализе: {str(e)}"

def start_general_analytics(update: Update, context: CallbackContext):
    """Общая аналитика системы"""
    query = update.callback_query
    query.answer()
    
    try:
        from database.db_manager import get_publish_tasks
        from database.models import TaskStatus, TaskType
        
        # Получаем данные для анализа
        accounts = get_instagram_accounts()
        
        # Получаем задачи за последнюю неделю
        week_ago = datetime.now() - timedelta(days=7)
        
        report = f"📊 ОБЩАЯ СТАТИСТИКА СИСТЕМЫ\n"
        report += f"📅 Дата анализа: {datetime.now().strftime('%d.%m.%Y %H:%M')}\n"
        report += "=" * 50 + "\n\n"
        
        # Статистика аккаунтов
        active_accounts = [acc for acc in accounts if acc.is_active]
        report += f"👥 АККАУНТЫ:\n"
        report += f"📊 Всего: {len(accounts)}\n"
        report += f"✅ Активных: {len(active_accounts)}\n"
        report += f"❌ Неактивных: {len(accounts) - len(active_accounts)}\n\n"
        
        # Статистика папок
        from database.db_manager import get_account_groups
        groups = get_account_groups()
        report += f"📁 ПАПКИ:\n"
        report += f"📊 Всего папок: {len(groups)}\n"
        for group in groups:
            from database.db_manager import get_accounts_in_group
            group_accounts = get_accounts_in_group(group.id)
            report += f"   📁 {group.name}: {len(group_accounts)} аккаунтов\n"
        report += "\n"
        
        # Статистика прокси
        from database.db_manager import get_proxies
        proxies = get_proxies()
        active_proxies = [p for p in proxies if p.is_active]
        report += f"🌐 ПРОКСИ:\n"
        report += f"📊 Всего: {len(proxies)}\n"
        report += f"✅ Активных: {len(active_proxies)}\n"
        report += f"❌ Неактивных: {len(proxies) - len(active_proxies)}\n\n"
        
        # Статистика системы
        import psutil
        cpu_percent = psutil.cpu_percent(interval=1)
        memory = psutil.virtual_memory()
        
        report += f"🖥️ СИСТЕМА:\n"
        report += f"⚡ CPU: {cpu_percent:.1f}%\n"
        report += f"💾 RAM: {memory.percent:.1f}% ({memory.used // 1024**3:.1f}GB / {memory.total // 1024**3:.1f}GB)\n"
        
        # Проверяем место на диске
        disk = psutil.disk_usage('/')
        report += f"💽 Диск: {disk.percent:.1f}% ({disk.used // 1024**3:.1f}GB / {disk.total // 1024**3:.1f}GB)\n\n"
        
        report += f"🔧 РЕКОМЕНДАЦИИ:\n"
        if len(active_accounts) == 0:
            report += "⚠️ Нет активных аккаунтов - добавьте аккаунты\n"
        elif len(active_accounts) < 5:
            report += "💡 Мало аккаунтов для эффективной работы\n"
        
        if len(active_proxies) == 0:
            report += "⚠️ Нет активных прокси - добавьте прокси\n"
        elif len(active_proxies) < len(active_accounts):
            report += "💡 Прокси меньше чем аккаунтов - рекомендуется 1:1\n"
        
        if memory.percent > 80:
            report += "⚠️ Высокое использование RAM\n"
        
        if disk.percent > 90:
            report += "⚠️ Мало места на диске\n"
        
        # Создаем и отправляем файл
        filename = f"general_statistics_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False, encoding='utf-8') as f:
            f.write(report)
            temp_file_path = f.name
        
        with open(temp_file_path, 'rb') as f:
            query.message.reply_document(
                document=f,
                filename=filename,
                caption="📊 Общая статистика системы"
            )
        
        os.unlink(temp_file_path)
        
        # Возвращаемся к меню статистики
        from telegram_bot.keyboards import get_statistics_menu_keyboard
        query.edit_message_text(
            "✅ Общая статистика отправлена файлом",
            reply_markup=get_statistics_menu_keyboard()
        )
        
    except Exception as e:
        logger.error(f"Ошибка при общей аналитике: {e}")
        query.edit_message_text(
            f"❌ Ошибка при общей аналитике: {str(e)}",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Назад", callback_data="analytics_menu")]
            ])
        ) 
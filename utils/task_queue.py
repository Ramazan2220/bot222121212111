import threading
import logging
import queue
import time
from datetime import datetime
import concurrent.futures
import traceback
import random
import json
import os
from typing import List

from database.db_manager import update_publish_task_status, get_publish_task, update_task_status
from database.models import TaskStatus, TaskType
from instagram.post_manager import PostManager
from instagram.reels_manager import ReelsManager
from instagram.story_manager import StoryManager
from instagram.client_patch import add_account_to_cache
from utils.content_uniquifier import uniquify_for_publication
from utils.system_monitor import get_adaptive_limits  # Добавляем импорт крутой системы мониторинга
from utils.user_logger import log_user_info, log_user_error  # per-user logs

logger = logging.getLogger(__name__)

# Создаем очередь задач
task_queue = queue.Queue()

# Словарь для хранения результатов выполнения задач
task_results = {}

# Пул потоков для параллельного выполнения задач
# Теперь количество потоков будет динамически адаптироваться под нагрузку
MAX_WORKERS = 50  # Максимальное количество потоков (при минимальной нагрузке)
executor = concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS)

# Глобальная переменная для хранения активных пакетов задач
active_task_batches = {}

def get_task_adaptive_limits():
    """Получает адаптивные лимиты на основе крутой системной нагрузки"""
    try:
        # Получаем текущие лимиты нагрузки из крутой системы мониторинга
        limits = get_adaptive_limits()
        
        # Адаптируем количество потоков под текущую нагрузку
        adaptive_workers = min(limits.max_workers, MAX_WORKERS)
        
        # Получаем задержку между задачами
        delay_between_tasks = limits.delay_between_batches
        
        logger.debug(f"🔧 Адаптивные лимиты от крутой системы: потоков={adaptive_workers}, задержка={delay_between_tasks}с, описание='{limits.description}'")
        
        return adaptive_workers, delay_between_tasks, limits
        
    except Exception as e:
        logger.warning(f"⚠️ Ошибка получения лимитов нагрузки: {e}, используем значения по умолчанию")
        # Создаем простой объект с базовыми лимитами
        class BasicLimits:
            max_workers = MAX_WORKERS
            delay_between_batches = 5.0
            description = "Базовые лимиты (ошибка получения данных)"
        return MAX_WORKERS, 5.0, BasicLimits()

def check_system_overload():
    """Проверяет, не перегружена ли система через крутую систему мониторинга"""
    try:
        limits = get_adaptive_limits()
        
        # Если система в защитном режиме или критической нагрузке
        # Проверяем только реально критические состояния
        is_emergency = (
            "ЗАЩИТНЫЙ РЕЖИМ" in limits.description or 
            "ЭКСТРЕННАЯ ОСТАНОВКА" in limits.description
        )
        
        if is_emergency:
            logger.warning(f"🚨 Система перегружена! Уровень: '{limits.description}' | Потоков: {limits.max_workers} | Задержка: {limits.delay_between_batches}с")
            return True
            
        return False
        
    except Exception as e:
        logger.error(f"❌ Ошибка проверки перегрузки системы: {e}")
        return False

def process_task(task_id, chat_id, bot):
    """Обрабатывает задачу публикации"""
    try:
        # Проверяем перегрузку системы перед началом задачи
        if check_system_overload():
            logger.warning(f"⏸️ Задача #{task_id} отложена из-за критической перегрузки системы")
            time.sleep(30)  # Ждем 30 секунд при перегрузке (было 60)
            
            # Повторно проверяем после ожидания
            if check_system_overload():
                logger.error(f"🚨 Система все еще критически перегружена, отменяем задачу #{task_id}")
                update_publish_task_status(task_id, TaskStatus.FAILED, error_message="Критическая перегрузка системы")
                return False
        
        # Получаем задачу из БД
        task_data = get_publish_task(task_id)
        if not task_data:
            logger.error(f"Задача #{task_id} не найдена")
            return False

        user_id = task_data.get('user_id')
        if user_id:
            log_user_info(user_id, f"🚀 Старт задачи публикации #{task_id} для @{task_data.get('account_username')}")
        
        # Если chat_id не передан, используем user_id из задачи
        if not chat_id and task_data.get('user_id'):
            chat_id = task_data['user_id']
            logger.info(f"📧 Используем user_id из задачи для уведомлений: {chat_id}")
        
        logger.info(f"🚀 Начинаю обработку задачи #{task_id} для аккаунта {task_data['account_username']}")
        if user_id:
            log_user_info(user_id, f"🔍 Проверяем валидность аккаунта @{task_data['account_username']} перед публикацией")
        
        # Проверяем валидность аккаунта перед использованием
        from utils.smart_validator_service import validate_before_use, ValidationPriority
        
        logger.info(f"🔍 Проверка валидности аккаунта @{task_data['account_username']} перед публикацией")
        
        if not validate_before_use(task_data['account_id'], ValidationPriority.CRITICAL):
            logger.warning(f"❌ Аккаунт @{task_data['account_username']} невалиден или не готов")
            update_publish_task_status(task_id, TaskStatus.FAILED, error_message="Аккаунт невалиден или требует восстановления")
            if user_id:
                log_user_error(user_id, f"❌ Публикация не выполнена: аккаунт @{task_data['account_username']} невалиден")
            
            # Отправляем уведомление об ошибке
            if bot and chat_id:
                try:
                    bot.send_message(
                        chat_id,
                        f"❌ Не удалось выполнить публикацию!\n"
                        f"Аккаунт: @{task_data['account_username']}\n"
                        f"Причина: Аккаунт невалиден или требует восстановления"
                    )
                except:
                    pass
            
            return False
        
        logger.info(f"✅ Аккаунт @{task_data['account_username']} валиден, продолжаем публикацию")
        if user_id:
            log_user_info(user_id, f"✅ Аккаунт @{task_data['account_username']} валиден, начинаем публикацию")
        
        # Добавляем аккаунт в кэш для автоматической обработки верификации
        add_account_to_cache(
            task_data['account_id'],
            task_data['account_username'],
            task_data['account_email'],
            task_data['account_email_password']
        )
        
        # Обновляем статус задачи
        update_publish_task_status(task_id, TaskStatus.PROCESSING)

        # Получаем адаптивную задержку на основе крутой системы мониторинга
        _, adaptive_delay, system_limits = get_task_adaptive_limits()
        
        # Добавляем адаптивную задержку между действиями
        base_delay = random.uniform(2, 5)  # Базовая задержка 2-5 секунд
        total_delay = base_delay + (adaptive_delay * 0.1)  # Добавляем 10% от системной задержки
        
        logger.info(f"⏳ Адаптивная задержка {total_delay:.2f} секунд (базовая: {base_delay:.2f}с, системная: {adaptive_delay:.1f}с) | Уровень: {system_limits.description}")
        if user_id:
            log_user_info(user_id, f"⏳ Имитируем задержку перед публикацией: {total_delay:.1f}с")
        time.sleep(total_delay)

        # Определяем тип задачи и выполняем соответствующее действие
        task_type = task_data['task_type']
        media_path = task_data['media_path']
        caption = task_data['caption'] or ""
        hashtags = task_data['hashtags'] or ""
        options = task_data['options'] or {}
        
        # Инициализируем переменные результата
        success = False
        media_id = None

        # Если options это строка, пробуем распарсить JSON
        if isinstance(options, str):
            try:
                logger.info(f"🔍 Парсим options JSON: {options[:200]}...")
                options = json.loads(options) if options else {}
                logger.info(f"✅ Успешно распарсили options: {list(options.keys())}")
            except Exception as parse_error:
                logger.error(f"❌ Ошибка парсинга options JSON: {parse_error}")
                options = {}

        # Объединяем caption и hashtags
        full_caption = caption
        if hashtags:
            full_caption = f"{caption}\n\n{hashtags}" if caption else hashtags

        # Проверяем, нужна ли уникализация
        uniquify_content = options.get('uniquify_content', False)
        
        # Проверяем расширение файла для определения типа контента
        file_extension = os.path.splitext(media_path)[1].lower()
        is_video = file_extension in ['.mp4', '.mov', '.avi', '.mkv', '.webm']
        
        # Определяем тип контента для уникализации
        if task_type == TaskType.CAROUSEL:
            content_type = 'carousel'
        elif task_type == TaskType.REEL or is_video:
            content_type = 'reel'
        elif task_type == TaskType.STORY:
            content_type = 'story'
        else:
            content_type = 'photo'
        
        # Применяем уникализацию если включена
        if uniquify_content:
            logger.info(f"🎨 Применяется уникализация контента для типа: {content_type}")
            
            # Для карусели нужно сначала распарсить пути
            if task_type == TaskType.CAROUSEL:
                try:
                    media_paths = json.loads(media_path)
                    if not isinstance(media_paths, list):
                        media_paths = [media_path]
                except:
                    media_paths = [media_path]
                
                # Уникализируем карусель
                unique_paths, unique_caption = uniquify_for_publication(media_paths, content_type, full_caption)
                media_path = json.dumps(unique_paths)  # Обратно в JSON
                full_caption = unique_caption
            else:
                # Уникализируем одиночный файл
                unique_path, unique_caption = uniquify_for_publication(media_path, content_type, full_caption)
                media_path = unique_path
                full_caption = unique_caption
                
            logger.info(f"✅ Контент успешно уникализирован")
        
        # Логируем информацию для отладки
        logger.info(f"📋 Тип задачи из БД: {task_type}")
        logger.info(f"📄 Расширение файла: {file_extension}")
        logger.info(f"🎬 Это видео: {is_video}")
        
        # Выбираем правильный менеджер в зависимости от типа задачи
        # Проверяем, это Reels или обычное видео
        is_reels = (
            task_type in ['reel', 'reels'] or 
            task_type == TaskType.REEL or 
            (task_type == TaskType.VIDEO and is_video and any(key in options for key in ['hide_from_feed', 'usertags', 'music_track']))
        )
        
        if is_reels:
            # Для видео используем ReelsManager
            logger.info(f"📹 Используем ReelsManager для публикации видео/рилс")
            manager = ReelsManager(task_data['account_id'])
            
            # Подготавливаем данные для публикации
            reels_data = {
                'hashtags': options.get('hashtags', []),
                'usertags': options.get('usertags', []),
                'location': options.get('location'),
                'cover_time': options.get('cover_time', 0),
                'uniquify_content': options.get('uniquify_content', False)
            }
            
            # Проверяем распределенные теги
            distributed_usertags = options.get('distributed_usertags', [])
            if distributed_usertags:
                # Ищем теги для текущего аккаунта
                account_tags = None
                for item in distributed_usertags:
                    if item['account_id'] == task_data['account_id']:
                        account_tags = item['tags']
                        break
                
                if account_tags:
                    reels_data['usertags'] = account_tags
                    logger.info(f"📊 Используются распределенные теги для аккаунта {task_data['account_id']}: {len(account_tags)} тегов")
            
            # Обрабатываем обложку
            thumbnail_path = options.get('thumbnail_path')
            if thumbnail_path:
                reels_data['thumbnail_path'] = thumbnail_path
                logger.info(f"🖼️ Используется загруженная обложка: {thumbnail_path}")
            
            logger.info(f"📋 Дополнительные данные Reels: {reels_data}")
            
            # Добавляем пользовательские теги
            if reels_data['usertags']:
                logger.info(f"👥 Добавляю теги пользователей в Reels: {len(reels_data['usertags'])} шт.")
            
            # Добавляем локацию
            if reels_data['location']:
                logger.info(f"📍 Добавляю локацию в Reels: {reels_data['location']}")
            
            # Добавляем обложку
            if reels_data.get('thumbnail_path'):
                logger.info(f"🖼️ Используется загруженная обложка")
            elif reels_data['cover_time'] > 0:
                logger.info(f"🖼️ Установка обложки на {reels_data['cover_time']} секунд")
            
            # Публикуем Reels
            success, result = manager.publish_reel(
                video_path=media_path,
                caption=full_caption,
                thumbnail_path=reels_data.get('thumbnail_path'),
                usertags=reels_data['usertags'],
                location=reels_data['location'],
                hashtags=reels_data['hashtags'],
                cover_time=reels_data['cover_time']
            )
            
            # ReelsManager возвращает кортеж (success, media_id_or_error)
            if success:
                media_id = result
            else:
                # Если success=False, то result содержит ошибку
                media_id = None
                # Проверяем, не является ли это серьезной ошибкой
                if isinstance(result, str) and result.startswith("ERROR"):
                    success = False
                    logger.error(f"❌ Серьезная ошибка при публикации Reels: {result}")
                else:
                    success = False
                    logger.error(f"❌ Ошибка при публикации Reels: {result}")
        elif task_type == TaskType.STORY:
            # Для историй используем StoryManager
            logger.info(f"📱 Используем StoryManager для публикации истории")
            manager = StoryManager(task_data['account_id'])
            
            # Извлекаем параметры для историй из options (правильное поле!)
            story_params = {}
            story_options = options  # options уже парсится выше
            
            logger.info(f"📋 Дополнительные данные Stories: {story_options}")
            
            # Обработка упоминаний
            if 'mentions' in story_options and story_options['mentions']:
                story_params['mentions'] = story_options['mentions']
                logger.info(f"👥 Добавляю упоминания в историю: {len(story_options['mentions'])} пользователей")
            
            # Обработка ссылки - КРИТИЧЕСКИ ВАЖНО
            if 'link' in story_options and story_options['link']:
                story_params['link'] = story_options['link']
                logger.info(f"🔗 Добавляю ссылку в историю: {story_options['link']}")
            elif 'story_link' in story_options and story_options['story_link']:
                story_params['link'] = story_options['story_link']
                logger.info(f"🔗 Добавляю ссылку в историю: {story_options['story_link']}")
            

            
            # Обработка текста поверх фото/видео
            if 'story_text' in story_options and story_options['story_text']:
                story_params['story_text'] = story_options['story_text']
                story_params['story_text_color'] = story_options.get('story_text_color', '#ffffff')
                story_params['story_text_position'] = story_options.get('story_text_position', {})
                logger.info(f"💬 Добавляю текст в историю: {story_options['story_text']}")
            
            # Проверяем, это альбом историй или одиночная
            try:
                # Пробуем распарсить как JSON массив
                media_paths = json.loads(media_path)
                if isinstance(media_paths, list) and len(media_paths) > 1:
                    # Альбом историй
                    result = manager.publish_story_album(
                        media_paths, 
                        caption=full_caption,
                        mentions=story_params.get('mentions'),
                        link=story_params.get('link'),
                        story_text=story_params.get('story_text'),
                        story_text_color=story_params.get('story_text_color'),
                        story_text_position=story_params.get('story_text_position')
                    )
                else:
                    # Одиночная история
                    if isinstance(media_paths, list):
                        media_path = media_paths[0]
                    result = manager.publish_story(
                        media_path, 
                        caption=full_caption,
                        mentions=story_params.get('mentions'),
                        link=story_params.get('link'),
                        story_text=story_params.get('story_text'),
                        story_text_color=story_params.get('story_text_color'),
                        story_text_position=story_params.get('story_text_position')
                    )
            except:
                # Если не JSON, то это одиночный файл
                result = manager.publish_story(
                    media_path, 
                    caption=full_caption,
                    mentions=story_params.get('mentions'),
                    link=story_params.get('link'),
                    story_text=story_params.get('story_text'),
                    story_text_color=story_params.get('story_text_color'),
                    story_text_position=story_params.get('story_text_position')
                )
            
            # StoryManager возвращает кортеж (success, media_id)
            if isinstance(result, tuple):
                success, media_id = result
            else:
                success = result
                media_id = None
        else:
            # Для остальных типов используем PostManager
            logger.info(f"📸 Используем PostManager для публикации {task_type}")
            manager = PostManager(task_data['account_id'])
            
            # Определяем тип публикации
            if task_type == 'photo' or task_type == TaskType.PHOTO:
                result = manager.publish_photo(
                    media_path,
                    caption=full_caption
                )
            elif task_type == 'carousel' or task_type == TaskType.CAROUSEL:
                # Для карусели media_path содержит JSON список путей
                try:
                    media_paths = json.loads(media_path)
                    if not isinstance(media_paths, list):
                        media_paths = [media_path]
                except:
                    # Если не удалось распарсить JSON, используем как обычный путь
                    media_paths = options.get('media_paths', [media_path])
                
                result = manager.publish_carousel(
                    media_paths,
                    caption=full_caption
                )
            elif task_type == 'mosaic' or task_type == TaskType.MOSAIC:
                result = manager.publish_mosaic(
                    media_path,
                    caption=full_caption,
                    crop_to_square=options.get('crop_to_square', False)
                )
            else:
                # По умолчанию публикуем как фото
                result = manager.publish_photo(
                    media_path,
                    caption=full_caption
                )
            
            # PostManager возвращает кортеж (success, media_id)
            if isinstance(result, tuple):
                success, media_id = result
            else:
                success = result
                media_id = None

        # Обновляем статус задачи в зависимости от результата
        if success:
            # media_id уже должен быть извлечен из кортежа выше
            # Преобразуем в строку только если это число
            if media_id is not None:
                media_id = str(media_id)
            
            update_publish_task_status(task_id, TaskStatus.COMPLETED, media_id=media_id)
            logger.info(f"✅ Задача #{task_id} успешно выполнена")
            
            # Отправляем уведомление в Telegram если есть бот
            if bot and chat_id:
                try:
                    # Генерируем правильную ссылку в зависимости от типа контента
                    if media_id:
                        if task_type == TaskType.VIDEO or task_type == 'reel':
                            media_url = f"https://www.instagram.com/reel/{media_id}/"
                        elif task_type == TaskType.STORY:
                            media_url = f"https://www.instagram.com/stories/highlight/{media_id}/"
                        else:
                            # Для постов, каруселей и других типов
                            media_url = f"https://www.instagram.com/p/{media_id}/"
                    else:
                        media_url = "Ссылка недоступна"
                    
                    # Определяем тип контента для уведомления
                    content_type = "Reels" if task_type == TaskType.VIDEO or task_type == 'reel' else str(task_type).title()
                    
                    bot.send_message(
                        chat_id,
                        f"✅ Публикация успешно завершена!\n"
                        f"Аккаунт: @{task_data['account_username']}\n"
                        f"Тип: {content_type}\n"
                        f"Ссылка: {media_url}"
                    )
                except Exception as e:
                    logger.error(f"Ошибка при отправке уведомления: {e}")
        else:
            error_msg = f"Не удалось опубликовать контент"
            update_publish_task_status(task_id, TaskStatus.FAILED, error_message=error_msg)
            logger.error(f"❌ Задача #{task_id} завершилась с ошибкой")
            
            # Отправляем уведомление об ошибке
            if bot and chat_id:
                try:
                    bot.send_message(
                        chat_id,
                        f"❌ Ошибка публикации!\n"
                        f"Аккаунт: @{task_data['account_username']}\n"
                        f"Ошибка: {error_msg}"
                    )
                except Exception as e:
                    logger.error(f"Ошибка при отправке уведомления: {e}")

        # Проверяем, нужно ли отправить итоговый отчет
        check_and_send_batch_report(task_id, chat_id, bot)

        return success

    except Exception as e:
        logger.error(f"❌ Ошибка обработки задачи #{task_id}: {e}\n{traceback.format_exc()}")
        try:
            update_publish_task_status(task_id, TaskStatus.FAILED, error_message=str(e))
        finally:
            pass
        # Перс-лог ошибки
        try:
            task_data = get_publish_task(task_id) or {}
            uid = task_data.get('user_id')
            if uid:
                log_user_error(uid, f"❌ Ошибка выполнения задачи #{task_id}: {e}")
        except Exception:
            pass
        return False

def register_task_batch(task_ids: List[int], chat_id: int, bot):
    """Регистрирует пакет задач для отправки итогового отчета"""
    if not task_ids:
        return
        
    batch_id = f"{chat_id}_{int(time.time())}"
    active_task_batches[batch_id] = {
        'task_ids': task_ids,
        'chat_id': chat_id,
        'bot': bot,
        'completed_tasks': set(),
        'failed_tasks': set(),
        'created_at': time.time()
    }
    
    logger.info(f"📦 Зарегистрирован пакет задач {batch_id}: {len(task_ids)} задач")

def check_and_send_batch_report(task_id: int, chat_id: int, bot):
    """Проверяет завершение пакета задач и отправляет итоговый отчет"""
    if not chat_id or not bot:
        return
        
    # Ищем пакет, содержащий эту задачу
    batch_to_update = None
    for batch_id, batch_data in active_task_batches.items():
        if task_id in batch_data['task_ids'] and batch_data['chat_id'] == chat_id:
            batch_to_update = batch_id
            break
    
    if not batch_to_update:
        return
        
    batch_data = active_task_batches[batch_to_update]
    
    # Получаем статус задачи
    task_data = get_publish_task(task_id)
    if not task_data:
        return
        
    # Обновляем статус задачи в пакете
    if task_data['status'] == TaskStatus.COMPLETED:
        batch_data['completed_tasks'].add(task_id)
    elif task_data['status'] == TaskStatus.FAILED:
        batch_data['failed_tasks'].add(task_id)
    
    # Проверяем, завершены ли все задачи
    total_tasks = len(batch_data['task_ids'])
    completed_count = len(batch_data['completed_tasks'])
    failed_count = len(batch_data['failed_tasks'])
    finished_count = completed_count + failed_count
    
    if finished_count >= total_tasks:
        # Все задачи завершены, отправляем итоговый отчет
        try:
            from telegram import InlineKeyboardButton, InlineKeyboardMarkup
            
            report_message = f"📊 **Итоговый отчет о публикации**\n\n"
            report_message += f"📋 Всего задач: {total_tasks}\n"
            report_message += f"✅ Успешно: {completed_count}\n"
            report_message += f"❌ Ошибок: {failed_count}\n\n"
            
            if completed_count > 0:
                report_message += f"🎉 Успешно опубликовано в {completed_count} аккаунтах!\n"
            
            if failed_count > 0:
                report_message += f"⚠️ Ошибки в {failed_count} аккаунтах\n"
            
            # Добавляем процент успешности
            success_rate = (completed_count / total_tasks) * 100
            report_message += f"📈 Успешность: {success_rate:.1f}%\n\n"
            
            # Добавляем время выполнения
            execution_time = time.time() - batch_data['created_at']
            report_message += f"⏱️ Время выполнения: {execution_time:.1f} секунд"
            
            # Создаем кнопки для навигации
            keyboard = [
                [InlineKeyboardButton("📊 История публикаций", callback_data="publication_history")],
                [InlineKeyboardButton("🔙 Главное меню", callback_data="main_menu")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            bot.send_message(
                chat_id,
                report_message,
                reply_markup=reply_markup
            )
            
            logger.info(f"📊 Отправлен итоговый отчет для пакета {batch_to_update}: {completed_count}/{total_tasks} успешно")
            
        except Exception as e:
            logger.error(f"Ошибка при отправке итогового отчета: {e}")
        
        # Удаляем завершенный пакет
        del active_task_batches[batch_to_update]

def task_worker():
    """Функция-обработчик очереди задач с адаптивным управлением нагрузкой"""
    logger.info("🚀 Запущен адаптивный обработчик очереди задач")

    # Словарь для отслеживания выполняющихся задач
    futures = {}
    last_load_check = 0
    current_max_workers = MAX_WORKERS

    while True:
        try:
            # Проверяем нагрузку системы каждые 30 секунд
            current_time = time.time()
            if current_time - last_load_check > 30:
                try:
                    # Получаем адаптивные лимиты от крутой системы мониторинга
                    adaptive_workers, system_delay, system_limits = get_task_adaptive_limits()
                    
                    # Обновляем максимальное количество потоков если изменилось
                    if adaptive_workers != current_max_workers:
                        logger.info(f"🔧 Адаптация нагрузки: потоков {current_max_workers} → {adaptive_workers} | Уровень: {system_limits.description}")
                        current_max_workers = adaptive_workers
                        
                        # Если нужно уменьшить количество потоков, ждем завершения лишних
                        if len(futures) > current_max_workers:
                            logger.info(f"⏳ Ожидание завершения {len(futures) - current_max_workers} задач для снижения нагрузки")
                    
                    last_load_check = current_time
                    
                except Exception as e:
                    logger.warning(f"⚠️ Ошибка проверки нагрузки: {e}")
                    last_load_check = current_time

            # Проверяем завершенные задачи
            done_futures = []
            for future, task_info in list(futures.items()):
                if future.done():
                    try:
                        # Получаем результат, чтобы обработать возможные исключения
                        future.result()
                    except Exception as e:
                        logger.error(f"❌ Ошибка в задаче {task_info}: {e}")
                        logger.error(traceback.format_exc())
                    done_futures.append(future)

            # Удаляем завершенные задачи из словаря
            for future in done_futures:
                del futures[future]

            # Проверяем, не перегружена ли система критически
            if check_system_overload():
                logger.warning("🚨 Система критически перегружена! Приостанавливаем обработку новых задач")
                time.sleep(30)  # Ждем 30 секунд при критической перегрузке (было 60)
                continue

            # Получаем новую задачу из очереди, если есть место в пуле
            if len(futures) < current_max_workers:
                try:
                    # Неблокирующее получение задачи с таймаутом
                    task = task_queue.get(block=True, timeout=1.0)

                    if task is None:
                        # Сигнал для завершения
                        break

                    task_id, chat_id, bot = task

                    # Запускаем задачу в пуле потоков
                    future = executor.submit(process_task, task_id, chat_id, bot)
                    futures[future] = (task_id, chat_id)

                    # Отмечаем задачу как взятую из очереди
                    task_queue.task_done()
                    
                    logger.debug(f"📋 Запущена задача #{task_id} ({len(futures)}/{current_max_workers} потоков)")

                except queue.Empty:
                    # Если очередь пуста, просто продолжаем цикл
                    pass
            else:
                # Если все рабочие потоки заняты, ждем немного
                time.sleep(0.5)

        except Exception as e:
            logger.error(f"❌ Критическая ошибка в обработчике очереди: {e}")
            logger.error(traceback.format_exc())
            time.sleep(1)  # Пауза перед следующей итерацией

# Запускаем обработчик в отдельном потоке
worker_thread = None

def start_task_queue():
    """Запускает обработчик очереди задач"""
    global worker_thread

    if worker_thread is None or not worker_thread.is_alive():
        worker_thread = threading.Thread(target=task_worker, daemon=True)
        worker_thread.start()
        logger.info("Запущен поток обработки очереди задач")
    else:
        logger.info("Поток обработки очереди задач уже запущен")

def stop_task_queue():
    """Останавливает обработчик очереди задач"""
    global worker_thread, executor

    if worker_thread and worker_thread.is_alive():
        task_queue.put(None)  # Сигнал для завершения
        worker_thread.join(timeout=5.0)

        # Завершаем пул потоков
        executor.shutdown(wait=False)

        # Создаем новый пул потоков для следующего запуска
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS)

        logger.info("Поток обработки очереди задач остановлен")

def add_task_to_queue(task_id, chat_id=None, bot=None, delay_seconds=0):
    """Добавляет задачу в очередь на выполнение
    
    Args:
        task_id: ID задачи в базе данных
        chat_id: ID чата для отправки уведомлений (опционально)
        bot: Объект бота для отправки уведомлений (опционально)
        delay_seconds: Задержка перед выполнением в секундах
    """
    try:
        # Проверяем, что задача существует
        task = get_publish_task(task_id)
        if not task:
            logger.error(f"Задача #{task_id} не найдена")
            return False

        # Обновляем статус задачи
        update_publish_task_status(task_id, TaskStatus.PROCESSING)

        if delay_seconds > 0:
            # Если нужна задержка, добавляем задачу с таймером
            def delayed_add():
                time.sleep(delay_seconds)
                task_queue.put((task_id, chat_id, bot))
                logger.info(f"Задача #{task_id} добавлена в очередь после задержки {delay_seconds}с")
            
            # Запускаем в отдельном потоке
            delay_thread = threading.Thread(target=delayed_add, daemon=True)
            delay_thread.start()
            logger.info(f"Задача #{task_id} запланирована с задержкой {delay_seconds} секунд")
        else:
            # Добавляем задачу в очередь немедленно
            task_queue.put((task_id, chat_id, bot))
            logger.info(f"Задача #{task_id} добавлена в очередь")

        return True
    except Exception as e:
        logger.error(f"Ошибка при добавлении задачи #{task_id} в очередь: {e}")
        logger.error(traceback.format_exc())
        return False

def get_task_status(task_id):
    """Возвращает статус выполнения задачи"""
    try:
        if task_id in task_results:
            return task_results[task_id]

        # Если задача не найдена в результатах, проверяем БД
        task = get_publish_task(task_id)
        if task:
            return {
                'success': task.status == TaskStatus.COMPLETED,
                'result': task.error_message or "В процессе выполнения",
                'completed_at': task.updated_at
            }

        return None
    except Exception as e:
        logger.error(f"Ошибка при получении статуса задачи #{task_id}: {e}")
        return {'success': False, 'result': f"Ошибка: {str(e)}"}

def get_queue_stats():
    """Возвращает статистику очереди задач с учетом крутой системы мониторинга"""
    try:
        # Получаем адаптивные лимиты от крутой системы мониторинга
        adaptive_workers, system_delay, system_limits = get_task_adaptive_limits()
        
        return {
            'queue_size': task_queue.qsize(),
            'max_workers': MAX_WORKERS,
            'current_max_workers': adaptive_workers,
            'system_delay': system_delay,
            'load_level': system_limits.description,
            'is_overloaded': check_system_overload(),
            'timeout_multiplier': system_limits.timeout_multiplier if hasattr(system_limits, 'timeout_multiplier') else 1.0,
            'batch_size': system_limits.batch_size if hasattr(system_limits, 'batch_size') else 1
        }
    except Exception as e:
        logger.error(f"Ошибка при получении статистики очереди: {e}")
        return {
            'queue_size': task_queue.qsize(),
            'max_workers': MAX_WORKERS,
            'current_max_workers': MAX_WORKERS,
            'system_delay': 5.0,
            'load_level': "Ошибка получения данных",
            'is_overloaded': False,
            'timeout_multiplier': 1.0,
            'batch_size': 1
        }
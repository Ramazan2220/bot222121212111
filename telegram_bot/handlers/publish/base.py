"""
Базовые классы для обработчиков публикации
"""

import os
import tempfile
import logging
from typing import List, Dict, Optional, Any
from abc import ABC, abstractmethod
from datetime import datetime

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, ParseMode, Update
from telegram.ext import ConversationHandler, CallbackContext

from database.db_manager import get_instagram_account, create_publish_task
from database.models import TaskType, TaskStatus
from utils.task_queue import add_task_to_queue
from telegram_bot.utils.account_selection import AccountSelector
from utils.content_uniquifier import ContentUniquifier
from utils.user_logger import log_user_info, log_user_error

logger = logging.getLogger(__name__)


class BasePublishHandler(ABC):
    """Базовый класс для всех типов публикаций"""
    
    def __init__(self, publish_type: str, task_type: TaskType):
        self.publish_type = publish_type
        self.task_type = task_type
        self.content_uniquifier = ContentUniquifier()
        
    @abstractmethod
    def get_media_prompt(self) -> str:
        """Возвращает текст подсказки для загрузки медиа"""
        pass
    
    @abstractmethod
    def get_media_types(self) -> List[str]:
        """Возвращает список поддерживаемых типов медиа"""
        pass
    
    @abstractmethod
    def validate_media(self, media_path: str, media_type: str) -> tuple[bool, str]:
        """Валидирует загруженное медиа. Возвращает (успех, сообщение об ошибке)"""
        pass
    
    def cleanup_user_data(self, context: CallbackContext):
        """Очищает временные данные пользователя"""
        keys_to_remove = [
            'publish_type', 'publish_media_type', 'publish_account_ids', 
            'publish_account_usernames', 'publish_account_id', 'publish_account_username',
            'publish_media_path', 'publish_media_paths', 'publish_caption', 'publish_hashtags',
            'publish_to_all_accounts', 'is_scheduled_post', 'schedule_publish_type',
            'waiting_for_schedule_time', 'selected_accounts'
        ]
        
        # Добавляем специфичные для типа ключи
        keys_to_remove.extend(self.get_specific_keys_to_cleanup())
        
        for key in keys_to_remove:
            context.user_data.pop(key, None)
    
    def get_specific_keys_to_cleanup(self) -> List[str]:
        """Возвращает дополнительные ключи для очистки (переопределяется в наследниках)"""
        return []
    
    def create_account_selector(self, back_callback: str = "menu_publications") -> AccountSelector:
        """Создает селектор аккаунтов для выбора"""
        title_map = {
            'post': "📸 Публикация поста",
            'story': "📱 Публикация истории", 
            'reels': "🎥 Публикация Reels",
            'igtv': "🎬 Публикация IGTV"
        }
        
        return AccountSelector(
            callback_prefix=f"{self.publish_type}_select",
            title=title_map.get(self.publish_type, "📤 Публикация"),
            allow_multiple=True,
            show_status=True,
            show_folders=True,
            back_callback=back_callback
        )
    
    def download_media(self, update: Update, context: CallbackContext) -> Optional[List[str]]:
        """Скачивает медиа файлы и возвращает пути к ним"""
        media_files = []
        media_type = None
        
        if update.message.photo:
            media_files = [update.message.photo[-1]]
            media_type = 'PHOTO'
        elif update.message.video:
            media_files = [update.message.video]
            media_type = 'VIDEO'
        elif update.message.document:
            media_file = update.message.document
            if media_file.mime_type.startswith('video/'):
                media_files = [media_file]
                media_type = 'VIDEO'
            elif media_file.mime_type.startswith('image/'):
                media_files = [media_file]
                media_type = 'PHOTO'
        
        if not media_files:
            return None
        
        # Проверяем поддерживаемые типы
        if media_type not in self.get_media_types():
            update.message.reply_text(
                f"❌ Этот тип контента поддерживает только: {', '.join(self.get_media_types())}"
            )
            return None
        
        # Скачиваем файлы
        media_paths = []
        for media_file in media_files:
            file_id = media_file.file_id
            media = context.bot.get_file(file_id)
            
            # Определяем расширение
            file_extension = '.mp4' if media_type == 'VIDEO' else '.jpg'
            
            # Создаем временный файл
            with tempfile.NamedTemporaryFile(delete=False, suffix=file_extension) as temp_file:
                media_path = temp_file.name
            
            # Скачиваем
            media.download(media_path)
            
            # Валидируем
            is_valid, error_msg = self.validate_media(media_path, media_type)
            if not is_valid:
                os.unlink(media_path)
                update.message.reply_text(f"❌ {error_msg}")
                # Удаляем уже скачанные файлы
                for path in media_paths:
                    if os.path.exists(path):
                        os.unlink(path)
                return None
            
            media_paths.append(media_path)
        
        # Сохраняем в контексте
        context.user_data['publish_media_type'] = media_type
        if len(media_paths) == 1:
            context.user_data['publish_media_path'] = media_paths[0]
        else:
            context.user_data['publish_media_paths'] = media_paths
        
        return media_paths
    
    def prepare_content_for_accounts(self, context: CallbackContext) -> List[Dict[str, Any]]:
        """Подготавливает контент для каждого аккаунта с уникализацией"""
        account_ids = context.user_data.get('publish_account_ids', [])
        media_path = context.user_data.get('publish_media_path')
        media_paths = context.user_data.get('publish_media_paths', [])
        caption = context.user_data.get('publish_caption', '')
        hashtags = context.user_data.get('publish_hashtags', [])
        
        # Если один файл
        if media_path and not media_paths:
            media_paths = [media_path]
        
        prepared_content = []
        
        for i, account_id in enumerate(account_ids):
            account = get_instagram_account(account_id)
            if not account:
                continue
            
            # Уникализируем контент для каждого аккаунта
            unique_content = {
                'account_id': account_id,
                'account_username': account.username,
                'caption': caption,
                'hashtags': hashtags,
                'media_paths': []
            }
            
            # Уникализируем медиа файлы
            for media_path in media_paths:
                if len(account_ids) > 1:
                    # Для множественной публикации уникализируем
                    unique_path, _ = self.content_uniquifier.uniquify_content(
                        media_path, 
                        self.publish_type,
                        ""
                    )
                    # Если uniquify_content вернул список, берем первый элемент
                    if isinstance(unique_path, list):
                        unique_path = unique_path[0]
                else:
                    # Для одного аккаунта не уникализируем
                    unique_path = media_path
                
                unique_content['media_paths'].append(unique_path)
            
            # Уникализируем текст
            if len(account_ids) > 1 and caption:
                unique_content['caption'] = self.content_uniquifier.uniquify_text(
                    caption,
                    account_index=i,
                    total_accounts=len(account_ids)
                )
            
            # Уникализируем хештеги
            if len(account_ids) > 1 and hashtags:
                # Для множественной публикации просто перемешиваем хештеги
                import random
                unique_hashtags = hashtags.copy()
                random.shuffle(unique_hashtags)
                unique_content['hashtags'] = unique_hashtags
            else:
                unique_content['hashtags'] = hashtags
            
            prepared_content.append(unique_content)
        
        return prepared_content
    
    def create_publish_tasks(self, context: CallbackContext, scheduled_time: Optional[datetime] = None) -> List[int]:
        """Создает задачи публикации для всех выбранных аккаунтов"""
        prepared_content = self.prepare_content_for_accounts(context)
        task_ids = []
        user_id = context.user_data.get('user_id') or (getattr(getattr(context, 'update', None), 'effective_user', None) and context.update.effective_user.id)
        
        for content in prepared_content:
            try:
                # Создаем задачу в БД
                task = create_publish_task(
                    account_id=content['account_id'],
                    task_type=self.task_type,
                    media_paths=content['media_paths'],
                    caption=content['caption'],
                    hashtags=content['hashtags'],
                    scheduled_time=scheduled_time,
                    additional_data=self.get_additional_task_data(context)
                )
                
                if task:
                    task_ids.append(task.id)
                    if user_id:
                        log_user_info(user_id, f"🗂️ Создана задача #{task.id} для аккаунта ID={content['account_id']} ({self.task_type.name})")
                    
                    # Если не запланированная, добавляем в очередь
                    if not scheduled_time:
                        add_task_to_queue(task.id)
                        if user_id:
                            log_user_info(user_id, f"🚀 Задача #{task.id} добавлена в очередь")
            except Exception as e:
                logger.error(f"Ошибка создания задачи публикации: {e}")
                if user_id:
                    log_user_error(user_id, f"❌ Ошибка создания задачи публикации: {e}")
        
        return task_ids
    
    def get_additional_task_data(self, context: CallbackContext) -> Dict[str, Any]:
        """Возвращает дополнительные данные для задачи (переопределяется в наследниках)"""
        return {}
    
    def format_confirmation_message(self, context: CallbackContext) -> str:
        """Форматирует сообщение подтверждения публикации"""
        account_ids = context.user_data.get('publish_account_ids', [])
        caption = context.user_data.get('publish_caption', '')
        hashtags = context.user_data.get('publish_hashtags', [])
        media_type = context.user_data.get('publish_media_type', 'PHOTO')
        
        # Информация об аккаунтах
        if len(account_ids) == 1:
            account = get_instagram_account(account_ids[0])
            account_info = f"👤 Аккаунт: @{account.username}"
        else:
            account_info = f"👥 Аккаунты: {len(account_ids)} шт."
        
        # Типы публикаций
        type_names = {
            'post': 'Пост',
            'story': 'История', 
            'reels': 'Reels',
            'igtv': 'IGTV'
        }
        
        message = f"*Подтверждение публикации*\n\n"
        message += f"{account_info}\n"
        message += f"📄 Тип: {type_names.get(self.publish_type, 'Публикация')}\n"
        message += f"📱 Медиа: {media_type}\n"
        
        if caption:
            message += f"✏️ Подпись: {caption[:50]}{'...' if len(caption) > 50 else ''}\n"
        
        if hashtags:
            message += f"#️⃣ Хештеги: {' '.join(hashtags[:5])}{'...' if len(hashtags) > 5 else ''}\n"
        
        # Добавляем специфичную информацию
        message += self.format_specific_info(context)
        
        return message
    
    def format_specific_info(self, context: CallbackContext) -> str:
        """Форматирует специфичную для типа информацию (переопределяется в наследниках)"""
        return "" 
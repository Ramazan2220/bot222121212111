import logging
import os
import json
import traceback
import time
from pathlib import Path
import concurrent.futures
from typing import Optional, List, Dict, Tuple, Union
from datetime import datetime

from instagram.client import InstagramClient
from database.db_manager import update_task_status, get_instagram_accounts, update_instagram_account
from config import MAX_WORKERS
from database.models import TaskStatus
from instagram.email_utils import get_verification_code_from_email, mark_account_problematic

logger = logging.getLogger(__name__)

class StoryManager:
    def __init__(self, account_id):
        self.instagram = InstagramClient(account_id)

    def _ensure_login_with_recovery(self, max_attempts=3):
        """Обеспечивает вход в аккаунт с IMAP восстановлением и обновлением статуса"""
        account = self.instagram.account
        
        for attempt in range(max_attempts):
            try:
                logger.info(f"🔐 Попытка входа {attempt + 1}/{max_attempts} для аккаунта {account.username}")
                
                # Пытаемся войти обычным способом
                if self.instagram.check_login():
                    logger.info(f"✅ Успешный вход для аккаунта {account.username}")
                    # Обновляем статус аккаунта как активный
                    update_instagram_account(
                        account.id,
                        is_active=True,
                        status="active",
                        last_error=None,
                        last_check=datetime.now()
                    )
                    logger.info(f"✅ Статус аккаунта {account.username} обновлен как активный")
                    return True
                else:
                    # Если check_login вернул False, это означает проблему входа
                    logger.warning(f"⚠️ check_login вернул False для {account.username}, пытаемся IMAP восстановление...")
                    
                    # Проверяем наличие email для восстановления
                    if not account.email or not account.email_password:
                        logger.warning(f"❌ У аккаунта {account.username} нет данных email для восстановления")
                        mark_account_problematic(
                            account.email or account.username,
                            "no_email_data",
                            "Нет данных email для восстановления"
                        )
                        return False
                    
                    # Пытаемся получить код верификации из email
                    try:
                        logger.info(f"📧 Получаем код верификации из email для @{account.username}")
                        verification_code = get_verification_code_from_email(
                            account.email, 
                            account.email_password, 
                            max_attempts=3, 
                            delay_between_attempts=15
                        )
                        
                        if verification_code:
                            logger.info(f"✅ Получен код верификации {verification_code} для @{account.username}")
                            
                            # Пытаемся войти с кодом верификации
                            login_success = self.instagram.login_with_challenge_code(verification_code)
                            if login_success:
                                logger.info(f"✅ Успешное IMAP восстановление для @{account.username}")
                                
                                # Проверяем что восстановление прошло успешно
                                try:
                                    self.instagram.client.get_timeline_feed()
                                    # Обновляем статус в БД как активный
                                    update_instagram_account(
                                        account.id,
                                        is_active=True,
                                        status="active",
                                        last_error=None,
                                        last_check=datetime.now()
                                    )
                                    logger.info(f"✅ IMAP восстановление подтверждено для @{account.username}")
                                    return True
                                except Exception as verify_error:
                                    logger.warning(f"❌ IMAP восстановление @{account.username} не подтвердилось: {verify_error}")
                                    mark_account_problematic(
                                        account.email or account.username,
                                        "recovery_verify_failed",
                                        f"Восстановление не подтвердилось: {verify_error}"
                                    )
                                    return False
                            else:
                                logger.warning(f"❌ Не удалось войти с кодом верификации для @{account.username}")
                                mark_account_problematic(
                                    account.email or account.username,
                                    "recovery_login_failed",
                                    "Не удалось войти с кодом верификации"
                                )
                                return False
                        else:
                            logger.warning(f"❌ Не удалось получить код верификации для @{account.username}")
                            mark_account_problematic(
                                account.email or account.username,
                                "email_code_failed",
                                "Не удалось получить код из email"
                            )
                            return False
                            
                    except Exception as recovery_error:
                        logger.error(f"❌ Ошибка IMAP восстановления для {account.username}: {recovery_error}")
                        mark_account_problematic(
                            account.email or account.username,
                            "imap_recovery_error",
                            f"Ошибка IMAP восстановления: {str(recovery_error)}"
                        )
                        return False
                    
            except Exception as e:
                error_msg = str(e).lower()
                logger.warning(f"⚠️ Ошибка входа для {account.username}: {e}")
                
                # Проверяем, требуется ли восстановление
                if any(keyword in error_msg for keyword in ['challenge_required', 'verification_required', 'confirm_email', 'checkpoint', 'login_required']):
                    logger.info(f"🔄 Обнаружена ошибка верификации ({e}), пытаемся восстановить через IMAP для @{account.username}...")
                    
                    # Проверяем наличие email для восстановления
                    if not account.email or not account.email_password:
                        logger.warning(f"❌ У аккаунта {account.username} нет данных email для восстановления")
                        mark_account_problematic(
                            account.email or account.username,
                            "no_email_data",
                            "Нет данных email для восстановления"
                        )
                        return False
                    
                    # Пытаемся получить код верификации из email
                    try:
                        logger.info(f"📧 Получаем код верификации из email для @{account.username}")
                        verification_code = get_verification_code_from_email(
                            account.email, 
                            account.email_password, 
                            max_attempts=3, 
                            delay_between_attempts=15
                        )
                        
                        if verification_code:
                            logger.info(f"✅ Получен код верификации {verification_code} для @{account.username}")
                            
                            # Пытаемся войти с кодом верификации
                            login_success = self.instagram.login_with_challenge_code(verification_code)
                            if login_success:
                                logger.info(f"✅ Успешное IMAP восстановление для @{account.username}")
                                
                                # Проверяем что восстановление прошло успешно
                                try:
                                    self.instagram.client.get_timeline_feed()
                                    # Обновляем статус в БД как активный
                                    update_instagram_account(
                                        account.id,
                                        is_active=True,
                                        status="active",
                                        last_error=None,
                                        last_check=datetime.now()
                                    )
                                    logger.info(f"✅ IMAP восстановление подтверждено для @{account.username}")
                                    return True
                                except Exception as verify_error:
                                    logger.warning(f"❌ IMAP восстановление @{account.username} не подтвердилось: {verify_error}")
                                    mark_account_problematic(
                                        account.email or account.username,
                                        "recovery_verify_failed",
                                        f"Восстановление не подтвердилось: {verify_error}"
                                    )
                                    return False
                            else:
                                logger.warning(f"❌ Не удалось войти с кодом верификации для @{account.username}")
                                mark_account_problematic(
                                    account.email or account.username,
                                    "recovery_login_failed",
                                    "Не удалось войти с кодом верификации"
                                )
                                return False
                        else:
                            logger.warning(f"❌ Не удалось получить код верификации для @{account.username}")
                            mark_account_problematic(
                                account.email or account.username,
                                "email_code_failed",
                                "Не удалось получить код из email"
                            )
                            return False
                            
                    except Exception as recovery_error:
                        logger.error(f"❌ Ошибка IMAP восстановления для {account.username}: {recovery_error}")
                        mark_account_problematic(
                            account.email or account.username,
                            "imap_recovery_error",
                            f"Ошибка IMAP восстановления: {str(recovery_error)}"
                        )
                        return False
                
                # Если это последняя попытка и восстановление не удалось
                if attempt == max_attempts - 1:
                    logger.error(f"❌ Все попытки входа исчерпаны для {account.username}")
                    # Отмечаем аккаунт как проблемный
                    mark_account_problematic(
                        account.email or account.username,
                        "login_failed",
                        f"Не удалось войти после {max_attempts} попыток: {str(e)}"
                    )
                    return False
                
                # Ждем перед следующей попыткой
                time.sleep(5)
        
        return False

    def publish_story(self, media_path: str, caption: Optional[str] = None, 
                     mentions: Optional[List[Dict]] = None,
                     hashtags: Optional[List[str]] = None,
                     location: Optional[Dict] = None,
                     link: Optional[str] = None,
                     story_text: Optional[str] = None,
                     story_text_color: Optional[str] = None,
                     story_text_size: Optional[str] = None,
                     story_text_position: Optional[Dict] = None,
                     location_name: Optional[str] = None) -> Tuple[bool, Union[str, int]]:
        """
        Публикация фото или видео в Stories
        
        Args:
            media_path: Путь к файлу (фото или видео)
            caption: Текст истории
            mentions: Список упоминаний пользователей [{"username": "user", "x": 0.5, "y": 0.5}]
            hashtags: Список хештегов ["tag1", "tag2"]
            location: Геолокация {"pk": "123", "name": "Place", "lat": 0.0, "lng": 0.0}
            link: Ссылка (swipe up)
            story_text: Текст поверх фото/видео
            story_text_color: Цвет текста (#ffffff)
            story_text_size: Размер текста (small, medium, large)
            story_text_position: Позиция текста {"x": 0.5, "y": 0.5, "width": 0.8, "height": 0.1}
            location_name: Название локации для поиска
            
        Returns:
            Tuple[bool, Union[str, int]]: (успех, media_id или сообщение об ошибке)
        """
        try:
            # Проверяем вход с восстановлением
            if not self._ensure_login_with_recovery():
                logger.error(f"❌ Не удалось войти в аккаунт для публикации Story")
                return False, "ERROR - Не удалось войти в аккаунт для публикации Story"

            # Проверяем существование файла
            if not os.path.exists(media_path):
                logger.error(f"❌ Файл не найден: {media_path}")
                return False, f"ERROR - Файл не найден: {media_path}"

            # Определяем тип файла
            file_ext = os.path.splitext(media_path)[1].lower()
            is_video = file_ext in ['.mp4', '.mov', '.avi', '.mkv', '.webm']

            # Подготавливаем параметры для публикации
            kwargs = {}
            
            # Начинаем с базовой подписи
            final_caption = caption or ""
                
            # Конвертируем параметры в формат instagrapi
            if mentions:
                from instagrapi.types import StoryMention, UserShort
                story_mentions = []
                for mention in mentions:
                    try:
                        if isinstance(mention, dict):
                            username = mention.get('username', '').replace('@', '')
                            if username:
                                try:
                                    user_info = self.instagram.client.user_info_by_username(username)
                                    user_short = UserShort(
                                        pk=user_info.pk,
                                        username=user_info.username
                                    )
                                    story_mentions.append(StoryMention(
                                        user=user_short,
                                        x=mention.get('x', 0.5),
                                        y=mention.get('y', 0.5),
                                        width=mention.get('width', 0.5),
                                        height=mention.get('height', 0.1)
                                    ))
                                    logger.info(f"✅ Добавлено упоминание @{username} (ID: {user_info.pk})")
                                except Exception as user_error:
                                    logger.warning(f"❌ Не удалось найти пользователя @{username}: {user_error}")
                        elif isinstance(mention, str):
                            username = mention.replace('@', '')
                            if username:
                                try:
                                    user_info = self.instagram.client.user_info_by_username(username)
                                    user_short = UserShort(
                                        pk=user_info.pk,
                                        username=user_info.username
                                    )
                                    story_mentions.append(StoryMention(
                                        user=user_short,
                                        x=0.5, y=0.5, width=0.5, height=0.1
                                    ))
                                    logger.info(f"✅ Добавлено упоминание @{username} (ID: {user_info.pk})")
                                except Exception as user_error:
                                    logger.warning(f"❌ Не удалось найти пользователя @{username}: {user_error}")
                    except Exception as mention_error:
                        logger.warning(f"❌ Ошибка при обработке упоминания {mention}: {mention_error}")
                
                if story_mentions:
                    kwargs['mentions'] = story_mentions
                    logger.info(f"✅ Добавлено {len(story_mentions)} упоминаний в Story")
                
            if location:
                from instagrapi.types import StoryLocation, Location
                loc = Location(
                    pk=location.get('pk'),
                    name=location.get('name'),
                    lat=location.get('lat'),
                    lng=location.get('lng')
                )
                kwargs['locations'] = [StoryLocation(
                    location=loc,
                    x=0.5,
                    y=0.5,
                    width=0.5,
                    height=0.1
                )]
                
            # ВАЖНО: Обработка ссылки
            if link:
                from instagrapi.types import StoryLink
                logger.info(f"🔗 Добавляю ссылку в историю: {link}")
                kwargs['links'] = [StoryLink(webUri=link)]

            # НОВОЕ: Обработка текста поверх фото/видео
            if story_text:
                logger.info(f"💬 Добавляю текст поверх истории: {story_text}")
                
                # Добавляем текст в подпись Story
                if final_caption:
                    final_caption = f"{final_caption}\n\n{story_text}"
                else:
                    final_caption = story_text
                
                logger.info(f"✅ Текст добавлен в подпись Story: {story_text}")
            
            # Устанавливаем финальную подпись
            if final_caption:
                kwargs['caption'] = final_caption

            # Публикуем в зависимости от типа
            if is_video:
                media = self.instagram.client.video_upload_to_story(
                    Path(media_path),
                    **kwargs
                )
            else:
                media = self.instagram.client.photo_upload_to_story(
                    Path(media_path),
                    **kwargs
                )

            logger.info(f"Story успешно опубликована: {media.pk}")
            return True, media.pk
            
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Ошибка при публикации Story: {error_msg}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            
            # Проверяем, нужно ли повторить попытку с восстановлением
            if any(keyword in error_msg.lower() for keyword in ['challenge_required', 'login_required', 'verification_required']):
                logger.info(f"🔄 Обнаружена ошибка входа, пытаемся войти повторно с восстановлением...")
                
                # Принудительно пытаемся IMAP восстановление даже если check_login возвращает True
                account = self.instagram.account
                
                # Проверяем наличие email для восстановления
                if account.email and account.email_password:
                    logger.info(f"📧 Принудительное IMAP восстановление для @{account.username} из-за ошибки публикации Story...")
                    
                    try:
                        # Получаем код верификации из email
                        verification_code = get_verification_code_from_email(
                            account.email, 
                            account.email_password, 
                            max_attempts=3, 
                            delay_between_attempts=15
                        )
                        
                        if verification_code:
                            logger.info(f"✅ Получен код верификации {verification_code} для @{account.username}")
                            
                            # Пытаемся войти с кодом верификации
                            login_success = self.instagram.login_with_challenge_code(verification_code)
                            if login_success:
                                logger.info(f"✅ Принудительное IMAP восстановление успешно для @{account.username}")
                                
                                # Проверяем что восстановление прошло успешно
                                try:
                                    self.instagram.client.get_timeline_feed()
                                    logger.info(f"✅ Принудительное IMAP восстановление подтверждено для @{account.username}")
                                    
                                    # Повторяем публикацию Story
                                    logger.info(f"🔄 Повторная попытка публикации Story после IMAP восстановления...")
                                    
                                    try:
                                        # Публикуем в зависимости от типа
                                        if is_video:
                                            media = self.instagram.client.video_upload_to_story(
                                                Path(media_path),
                                                **kwargs
                                            )
                                        else:
                                            media = self.instagram.client.photo_upload_to_story(
                                                Path(media_path),
                                                **kwargs
                                            )
                                        
                                        if media:
                                            logger.info(f"Story успешно опубликована после IMAP восстановления: {media.pk}")
                                            return True, media.pk
                                        else:
                                            logger.error("Не удалось опубликовать Story после IMAP восстановления")
                                            mark_account_problematic(
                                                self.instagram.account.email,
                                                "story_failed_after_imap",
                                                "Не удалось опубликовать Story после IMAP восстановления"
                                            )
                                            return False, "ERROR - Не удалось опубликовать Story после IMAP восстановления"
                                            
                                    except Exception as pub_retry_error:
                                        logger.error(f"Ошибка при повторной публикации Story после IMAP: {pub_retry_error}")
                                        mark_account_problematic(
                                            self.instagram.account.email,
                                            "story_error_after_imap",
                                            f"Ошибка при повторной публикации Story после IMAP: {str(pub_retry_error)}"
                                        )
                                        return False, f"ERROR - {pub_retry_error}"
                                        
                                except Exception as verify_error:
                                    logger.warning(f"❌ Принудительное IMAP восстановление Story не подтвердилось: {verify_error}")
                                    mark_account_problematic(
                                        account.email,
                                        "forced_story_recovery_verify_failed",
                                        f"Принудительное восстановление Story не подтвердилось: {verify_error}"
                                    )
                            else:
                                logger.warning(f"❌ Принудительное IMAP восстановление Story не удалось для @{account.username}")
                                mark_account_problematic(
                                    account.email,
                                    "forced_story_recovery_login_failed",
                                    "Принудительное IMAP восстановление Story не удалось"
                                )
                        else:
                            logger.warning(f"❌ Не удалось получить код для принудительного восстановления Story @{account.username}")
                            mark_account_problematic(
                                account.email,
                                "forced_story_email_code_failed",
                                "Не удалось получить код для принудительного восстановления Story"
                            )
                            
                    except Exception as forced_recovery_error:
                        logger.error(f"❌ Ошибка принудительного IMAP восстановления Story: {forced_recovery_error}")
                        mark_account_problematic(
                            account.email,
                            "forced_story_imap_recovery_error",
                            f"Ошибка принудительного IMAP восстановления Story: {str(forced_recovery_error)}"
                        )
                
                # Если принудительное восстановление не помогло, пробуем обычный метод
                if self._ensure_login_with_recovery():
                    logger.info(f"🔄 Повторная попытка публикации Story после входа...")
                    
                    try:
                        # Публикуем в зависимости от типа
                        if is_video:
                            media = self.instagram.client.video_upload_to_story(
                                Path(media_path),
                                **kwargs
                            )
                        else:
                            media = self.instagram.client.photo_upload_to_story(
                                Path(media_path),
                                **kwargs
                            )
                        
                        if media:
                            logger.info(f"Story успешно опубликована после повторной попытки: {media.pk}")
                            return True, media.pk
                        else:
                            logger.error("Не удалось опубликовать Story при повторной попытке")
                            # Помечаем аккаунт как проблемный
                            mark_account_problematic(
                                self.instagram.account.email,
                                "story_failed_retry",
                                "Не удалось опубликовать Story при повторной попытке"
                            )
                            return False, "ERROR - Не удалось опубликовать Story при повторной попытке"
                            
                    except Exception as retry_error:
                        logger.error(f"Ошибка при повторной попытке публикации Story: {retry_error}")
                        # Помечаем аккаунт как проблемный
                        mark_account_problematic(
                            self.instagram.account.email,
                            "story_error_retry",
                            f"Ошибка при повторной попытке Story: {str(retry_error)}"
                        )
                        return False, f"ERROR - {retry_error}"
                else:
                    logger.error(f"❌ Не удалось войти повторно для публикации Story")
                    # Помечаем аккаунт как проблемный
                    mark_account_problematic(
                        self.instagram.account.email,
                        "story_login_failed_retry",
                        "Не удалось войти повторно для публикации Story"
                    )
                    return False, "ERROR - Не удалось войти повторно"
            
            # Для всех остальных ошибок тоже помечаем аккаунт как проблемный
            mark_account_problematic(
                self.instagram.account.email,
                "story_error",
                f"Ошибка публикации Story: {error_msg}"
            )
            
            return False, f"ERROR - {error_msg}"

    def publish_story_album(self, media_paths: List[str], caption: Optional[str] = None, **kwargs) -> Tuple[bool, Union[str, List[int]]]:
        """
        Публикация нескольких фото/видео в Stories последовательно
        
        Args:
            media_paths: Список путей к файлам
            caption: Текст для всех историй
            
        Returns:
            Tuple[bool, Union[str, List[int]]]: (успех, список media_id или сообщение об ошибке)
        """
        try:
            media_ids = []
            
            for media_path in media_paths:
                success, result = self.publish_story(media_path, caption, **kwargs)
                if success:
                    media_ids.append(result)
                else:
                    logger.warning(f"Не удалось опубликовать {media_path}: {result}")
                    
            if media_ids:
                logger.info(f"Опубликовано {len(media_ids)} из {len(media_paths)} Stories")
                return True, media_ids
            else:
                return False, "Не удалось опубликовать ни одной истории"
                
        except Exception as e:
            logger.error(f"Ошибка при публикации альбома Stories: {e}")
            return False, str(e)

    def execute_story_task(self, task):
        """Выполнение задачи по публикации Story"""
        try:
            # Обновляем статус задачи
            update_task_status(task.id, TaskStatus.PROCESSING)

            # Получаем дополнительные параметры
            story_options = {}
            if hasattr(task, 'story_options') and task.story_options:
                try:
                    story_options = json.loads(task.story_options)
                except Exception as e:
                    logger.warning(f"Не удалось разобрать story_options для задачи {task.id}: {e}")
            elif hasattr(task, 'additional_data') and task.additional_data:
                try:
                    additional_data = json.loads(task.additional_data)
                    story_options = additional_data
                except Exception as e:
                    logger.warning(f"Не удалось разобрать additional_data для задачи {task.id}: {e}")

            # Проверяем, это альбом или одиночная история
            if isinstance(task.media_path, list) or (isinstance(task.media_path, str) and task.media_path.startswith('[')):
                # Альбом Stories
                if isinstance(task.media_path, str):
                    media_paths = json.loads(task.media_path)
                else:
                    media_paths = task.media_path
                    
                success, result = self.publish_story_album(media_paths, task.caption)
            else:
                # Одиночная Story
                success, result = self.publish_story(
                    task.media_path,
                    task.caption,
                    mentions=story_options.get('mentions'),
                    hashtags=story_options.get('hashtags'),
                    location=story_options.get('location'),
                    link=story_options.get('link')
                )

            # Обновляем статус задачи
            if success:
                update_task_status(task.id, TaskStatus.COMPLETED, media_id=str(result))
                logger.info(f"Задача {task.id} по публикации Story выполнена успешно")
                return True, result
            else:
                update_task_status(task.id, TaskStatus.FAILED, error_message=result)
                logger.error(f"Задача {task.id} по публикации Story не выполнена: {result}")
                return False, result
                
        except Exception as e:
            logger.error(f"Ошибка при выполнении задачи {task.id} по публикации Story: {e}")
            update_task_status(task.id, TaskStatus.FAILED, error_message=str(e))
            return False, str(e)

def publish_stories_in_parallel(media_path: Union[str, List[str]], caption: str, 
                               account_ids: List[int], **kwargs) -> Dict:
    """Публикация Stories в несколько аккаунтов параллельно"""
    results = {}

    def publish_to_account(account_id):
        manager = StoryManager(account_id)
        if isinstance(media_path, list):
            success, result = manager.publish_story_album(media_path, caption)
        else:
            success, result = manager.publish_story(media_path, caption, **kwargs)
        return account_id, success, result

    # Используем ThreadPoolExecutor для параллельной публикации
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(publish_to_account, account_id) for account_id in account_ids]

        for future in concurrent.futures.as_completed(futures):
            try:
                account_id, success, result = future.result()
                results[account_id] = {'success': success, 'result': result}
            except Exception as e:
                logger.error(f"Ошибка при параллельной публикации Story: {e}")

    return results 
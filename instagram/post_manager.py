import logging
import os
import traceback
import time
from pathlib import Path
from datetime import datetime

from instagram.client import InstagramClient
from database.db_manager import update_task_status, update_instagram_account
from utils.image_splitter import split_image_for_mosaic
from database.models import TaskStatus
from instagram.email_utils_optimized import get_verification_code_from_email
from instagram.email_utils import mark_account_problematic

logger = logging.getLogger(__name__)

class PostManager:
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

    def _ensure_login(self, max_attempts=3):
        """Обеспечивает вход в аккаунт с повторными попытками (старая версия для совместимости)"""
        return self._ensure_login_with_recovery(max_attempts)

    def publish_photo(self, image_path, caption="", hashtags="", hide_from_feed=False):
        """
        Публикует фото в Instagram
        
        Args:
            image_path (str): Путь к изображению
            caption (str): Подпись к посту
            hashtags (str): Хештеги
            hide_from_feed (bool): Скрыть из основной ленты
        
        Returns:
            tuple: (success, media_id)
        """
        try:
            # Проверяем вход с восстановлением
            if not self._ensure_login_with_recovery():
                logger.error(f"❌ Не удалось войти в аккаунт для публикации фото")
                return False, "ERROR - Не удалось войти в аккаунт для публикации фото"

            # Проверяем существование файла
            if not os.path.exists(image_path):
                logger.error(f"❌ Файл не найден: {image_path}")
                return False, f"ERROR - Файл не найден: {image_path}"
            
            # Объединяем подпись и хештеги
            full_caption = caption
            if hashtags:
                full_caption = f"{caption}\n\n{hashtags}" if caption else hashtags

            # Публикуем фото
            media = self.instagram.client.photo_upload(
                image_path,
                caption=full_caption
            )
            
            if media:
                logger.info(f"Фото успешно опубликовано: {media.id}")
                return True, media.id
            else:
                logger.error("Не удалось опубликовать фото")
                # Помечаем аккаунт как проблемный при неудачной публикации
                mark_account_problematic(
                    self.instagram.account.email,
                    "publication_failed",
                    "Не удалось опубликовать фото"
                )
                return False, "ERROR - Не удалось опубликовать фото"
                
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Ошибка при публикации фото: {error_msg}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            
            # Проверяем, нужно ли повторить попытку с восстановлением
            if any(keyword in error_msg.lower() for keyword in ['challenge_required', 'login_required', 'verification_required']):
                logger.info(f"🔄 Обнаружена ошибка входа, пытаемся войти повторно с восстановлением...")
                
                # Принудительно пытаемся IMAP восстановление даже если check_login возвращает True
                account = self.instagram.account
                
                # Проверяем наличие email для восстановления
                if account.email and account.email_password:
                    logger.info(f"📧 Принудительное IMAP восстановление для @{account.username} из-за ошибки публикации...")
                    
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
                                    
                                    # Повторяем публикацию
                                    logger.info(f"🔄 Повторная попытка публикации фото после IMAP восстановления...")
                                    
                                    try:
                                        media = self.instagram.client.photo_upload(
                                            image_path,
                                            caption=full_caption
                                        )
                                        
                                        if media:
                                            logger.info(f"Фото успешно опубликовано после IMAP восстановления: {media.id}")
                                            return True, media.id
                                        else:
                                            logger.error("Не удалось опубликовать фото после IMAP восстановления")
                                            mark_account_problematic(
                                                self.instagram.account.email,
                                                "publication_failed_after_imap",
                                                "Не удалось опубликовать фото после IMAP восстановления"
                                            )
                                            return False, "ERROR - Не удалось опубликовать фото после IMAP восстановления"
                                            
                                    except Exception as pub_retry_error:
                                        logger.error(f"Ошибка при повторной публикации после IMAP: {pub_retry_error}")
                                        mark_account_problematic(
                                            self.instagram.account.email,
                                            "publication_error_after_imap",
                                            f"Ошибка при повторной публикации после IMAP: {str(pub_retry_error)}"
                                        )
                                        return False, f"ERROR - {pub_retry_error}"
                                        
                                except Exception as verify_error:
                                    logger.warning(f"❌ Принудительное IMAP восстановление не подтвердилось: {verify_error}")
                                    mark_account_problematic(
                                        account.email,
                                        "forced_recovery_verify_failed",
                                        f"Принудительное восстановление не подтвердилось: {verify_error}"
                                    )
                            else:
                                logger.warning(f"❌ Принудительное IMAP восстановление не удалось для @{account.username}")
                                mark_account_problematic(
                                    account.email,
                                    "forced_recovery_login_failed",
                                    "Принудительное IMAP восстановление не удалось"
                                )
                        else:
                            logger.warning(f"❌ Не удалось получить код для принудительного восстановления @{account.username}")
                            mark_account_problematic(
                                account.email,
                                "forced_email_code_failed",
                                "Не удалось получить код для принудительного восстановления"
                            )
                            
                    except Exception as forced_recovery_error:
                        logger.error(f"❌ Ошибка принудительного IMAP восстановления: {forced_recovery_error}")
                        mark_account_problematic(
                            account.email,
                            "forced_imap_recovery_error",
                            f"Ошибка принудительного IMAP восстановления: {str(forced_recovery_error)}"
                        )
                
                # Если принудительное восстановление не помогло, пробуем обычный метод
                if self._ensure_login_with_recovery():
                    logger.info(f"🔄 Повторная попытка публикации фото после входа...")
                    
                    try:
                        media = self.instagram.client.photo_upload(
                            image_path,
                            caption=full_caption
                        )
                        
                        if media:
                            logger.info(f"Фото успешно опубликовано после повторной попытки: {media.id}")
                            return True, media.id
                        else:
                            logger.error("Не удалось опубликовать фото при повторной попытке")
                            # Помечаем аккаунт как проблемный
                            mark_account_problematic(
                                self.instagram.account.email,
                                "publication_failed_retry",
                                "Не удалось опубликовать фото при повторной попытке"
                            )
                            return False, "ERROR - Не удалось опубликовать фото при повторной попытке"
                            
                    except Exception as retry_error:
                        logger.error(f"Ошибка при повторной попытке публикации фото: {retry_error}")
                        # Помечаем аккаунт как проблемный
                        mark_account_problematic(
                            self.instagram.account.email,
                            "publication_error_retry",
                            f"Ошибка при повторной попытке: {str(retry_error)}"
                        )
                        return False, f"ERROR - {retry_error}"
                else:
                    logger.error(f"❌ Не удалось войти повторно для публикации фото")
                    # Помечаем аккаунт как проблемный
                    mark_account_problematic(
                        self.instagram.account.email,
                        "login_failed_retry",
                        "Не удалось войти повторно для публикации"
                    )
                    return False, "ERROR - Не удалось войти повторно"
            
            # Для всех остальных ошибок тоже помечаем аккаунт как проблемный
            mark_account_problematic(
                self.instagram.account.email,
                "publication_error",
                f"Ошибка публикации: {error_msg}"
            )
            
            return False, f"ERROR - {error_msg}"

    def publish_carousel(self, media_paths, caption="", hashtags="", hide_from_feed=False):
        """
        Публикует карусель (альбом) из нескольких изображений
        
        Args:
            media_paths (list): Список путей к изображениям
            caption (str): Подпись к посту
            hashtags (str): Хештеги
            hide_from_feed (bool): Скрыть из основной ленты
        
        Returns:
            tuple: (success, media_id)
        """
        try:
            logger.info(f"🎠 Начинаю публикацию карусели из {len(media_paths)} файлов")
            logger.info(f"📁 Пути к файлам: {media_paths}")
            
            # Проверяем вход с восстановлением
            if not self._ensure_login_with_recovery():
                logger.error(f"❌ Не удалось войти в аккаунт для публикации карусели")
                return False, "ERROR - Не удалось войти в аккаунт для публикации карусели"
            
            # Проверяем существование всех файлов
            for path in media_paths:
                if not os.path.exists(path):
                    logger.error(f"❌ Файл не найден: {path}")
                    return False, f"ERROR - Файл не найден: {path}"
                logger.info(f"✅ Файл найден: {path}")
            
            # Объединяем подпись и хештеги
            full_caption = caption
            if hashtags:
                full_caption = f"{caption}\n\n{hashtags}" if caption else hashtags
            
            logger.info(f"📤 Отправляю {len(media_paths)} файлов в Instagram API")
            
            # Публикуем карусель
            media = self.instagram.client.album_upload(
                media_paths,
                caption=full_caption
            )
            
            if media:
                logger.info(f"Карусель успешно опубликована: {media.id}")
                return True, media.id
            else:
                logger.error("Не удалось опубликовать карусель")
                # Помечаем аккаунт как проблемный при неудачной публикации
                mark_account_problematic(
                    self.instagram.account.email,
                    "carousel_publication_failed",
                    "Не удалось опубликовать карусель"
                )
                return False, "ERROR - Не удалось опубликовать карусель"
                
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Ошибка при публикации карусели: {error_msg}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            
            # Проверяем, нужно ли повторить попытку с восстановлением
            if any(keyword in error_msg.lower() for keyword in ['challenge_required', 'login_required', 'verification_required']):
                logger.info(f"🔄 Обнаружена ошибка входа, пытаемся войти повторно с восстановлением...")
                
                # Принудительно пытаемся IMAP восстановление даже если check_login возвращает True
                account = self.instagram.account
                
                # Проверяем наличие email для восстановления
                if account.email and account.email_password:
                    logger.info(f"📧 Принудительное IMAP восстановление для @{account.username} из-за ошибки публикации карусели...")
                    
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
                                    
                                    # Повторяем публикацию карусели
                                    logger.info(f"🔄 Повторная попытка публикации карусели после IMAP восстановления...")
                                    
                                    # Проверяем файлы еще раз
                                    for path in media_paths:
                                        if not os.path.exists(path):
                                            logger.error(f"❌ Файл не найден для повторной попытки: {path}")
                                            return False, f"ERROR - Файл не найден: {path}"
                                        logger.info(f"✅ Файл найден для повторной попытки: {path}")
                                    
                                    try:
                                        logger.info(f"📤 Повторная отправка {len(media_paths)} файлов в Instagram API после IMAP")
                                        media = self.instagram.client.album_upload(
                                            media_paths,
                                            caption=full_caption
                                        )
                                        
                                        if media:
                                            logger.info(f"Карусель успешно опубликована после IMAP восстановления: {media.id}")
                                            return True, media.id
                                        else:
                                            logger.error("Не удалось опубликовать карусель после IMAP восстановления")
                                            mark_account_problematic(
                                                self.instagram.account.email,
                                                "carousel_failed_after_imap",
                                                "Не удалось опубликовать карусель после IMAP восстановления"
                                            )
                                            return False, "ERROR - Не удалось опубликовать карусель после IMAP восстановления"
                                            
                                    except Exception as pub_retry_error:
                                        logger.error(f"Ошибка при повторной публикации карусели после IMAP: {pub_retry_error}")
                                        mark_account_problematic(
                                            self.instagram.account.email,
                                            "carousel_error_after_imap",
                                            f"Ошибка при повторной публикации карусели после IMAP: {str(pub_retry_error)}"
                                        )
                                        return False, f"ERROR - {pub_retry_error}"
                                        
                                except Exception as verify_error:
                                    logger.warning(f"❌ Принудительное IMAP восстановление карусели не подтвердилось: {verify_error}")
                                    mark_account_problematic(
                                        account.email,
                                        "forced_carousel_recovery_verify_failed",
                                        f"Принудительное восстановление карусели не подтвердилось: {verify_error}"
                                    )
                            else:
                                logger.warning(f"❌ Принудительное IMAP восстановление карусели не удалось для @{account.username}")
                                mark_account_problematic(
                                    account.email,
                                    "forced_carousel_recovery_login_failed",
                                    "Принудительное IMAP восстановление карусели не удалось"
                                )
                        else:
                            logger.warning(f"❌ Не удалось получить код для принудительного восстановления карусели @{account.username}")
                            mark_account_problematic(
                                account.email,
                                "forced_carousel_email_code_failed",
                                "Не удалось получить код для принудительного восстановления карусели"
                            )
                            
                    except Exception as forced_recovery_error:
                        logger.error(f"❌ Ошибка принудительного IMAP восстановления карусели: {forced_recovery_error}")
                        mark_account_problematic(
                            account.email,
                            "forced_carousel_imap_recovery_error",
                            f"Ошибка принудительного IMAP восстановления карусели: {str(forced_recovery_error)}"
                        )
                
                # Если принудительное восстановление не помогло, пробуем обычный метод
                if self._ensure_login_with_recovery():
                    logger.info(f"🔄 Повторная попытка публикации карусели после входа...")
                    
                    # Проверяем файлы еще раз
                    for path in media_paths:
                        if not os.path.exists(path):
                            logger.error(f"❌ Файл не найден для повторной попытки: {path}")
                            return False, f"ERROR - Файл не найден: {path}"
                        logger.info(f"✅ Файл найден для повторной попытки: {path}")
                    
                    try:
                        logger.info(f"📤 Повторная отправка {len(media_paths)} файлов в Instagram API")
                        media = self.instagram.client.album_upload(
                            media_paths,
                            caption=full_caption
                        )
                        
                        if media:
                            logger.info(f"Карусель успешно опубликована после повторной попытки: {media.id}")
                            return True, media.id
                        else:
                            logger.error("Не удалось опубликовать карусель при повторной попытке")
                            # Помечаем аккаунт как проблемный
                            mark_account_problematic(
                                self.instagram.account.email,
                                "carousel_failed_retry",
                                "Не удалось опубликовать карусель при повторной попытке"
                            )
                            return False, "ERROR - Не удалось опубликовать карусель при повторной попытке"
                            
                    except Exception as retry_error:
                        logger.error(f"Ошибка при повторной попытке публикации карусели: {retry_error}")
                        # Помечаем аккаунт как проблемный
                        mark_account_problematic(
                            self.instagram.account.email,
                            "carousel_error_retry",
                            f"Ошибка при повторной попытке карусели: {str(retry_error)}"
                        )
                        return False, f"ERROR - {retry_error}"
                else:
                    logger.error(f"❌ Не удалось войти повторно для публикации карусели")
                    # Помечаем аккаунт как проблемный
                    mark_account_problematic(
                        self.instagram.account.email,
                        "carousel_login_failed_retry",
                        "Не удалось войти повторно для публикации карусели"
                    )
                    return False, "ERROR - Не удалось войти повторно"
            
            # Для всех остальных ошибок карусели тоже помечаем аккаунт как проблемный
            mark_account_problematic(
                self.instagram.account.email,
                "carousel_error",
                f"Ошибка публикации карусели: {error_msg}"
            )
            
            return False, f"ERROR - {error_msg}"

    def publish_mosaic(self, image_path, caption=None):
        """Публикация мозаики из 6 частей"""
        try:
            # Проверяем статус входа с повторными попытками
            if not self._ensure_login():
                logger.error(f"Не удалось войти в аккаунт для публикации мозаики")
                return False, "Ошибка входа в аккаунт"

            # Проверяем существование файла
            if not os.path.exists(image_path):
                logger.error(f"Файл {image_path} не найден")
                return False, f"Файл не найден: {image_path}"

            # Разделяем изображение на 6 частей
            split_images = split_image_for_mosaic(image_path)
            if not split_images:
                logger.error(f"Не удалось разделить изображение на части")
                return False, "Не удалось разделить изображение на части"

            # Публикуем части в обратном порядке (чтобы в профиле они отображались правильно)
            for i, img_path in enumerate(reversed(split_images)):
                # Для первой публикации используем указанное описание, для остальных - пустое
                part_caption = caption if i == 0 else ""

                success, result = self.publish_photo(img_path, part_caption)
                if not success:
                    logger.error(f"Ошибка при публикации части {i+1} мозаики: {result}")
                    return False, f"Ошибка при публикации части {i+1} мозаики: {result}"

                # Небольшая пауза между публикациями
                time.sleep(5)

            logger.info(f"Мозаика успешно опубликована")
            return True, None
        except Exception as e:
            logger.error(f"Ошибка при публикации мозаики: {e}")
            return False, str(e)

    def execute_post_task(self, task):
        """Выполнение задачи по публикации поста"""
        try:
            # Обновляем статус задачи
            update_task_status(task.id, TaskStatus.PROCESSING)

            # Определяем тип задачи и выполняем соответствующее действие
            if task.task_type == 'post':
                success, result = self.publish_photo(task.media_path, task.caption)
            elif task.task_type == 'mosaic':
                success, result = self.publish_mosaic(task.media_path, task.caption)
            else:
                logger.error(f"Неизвестный тип задачи: {task.task_type}")
                update_task_status(task.id, TaskStatus.FAILED, error_message=f"Неизвестный тип задачи: {task.task_type}")
                return False, f"Неизвестный тип задачи: {task.task_type}"

            if success:
                update_task_status(task.id, TaskStatus.COMPLETED)
                logger.info(f"Задача {task.id} по публикации {task.task_type} выполнена успешно")
                return True, None
            else:
                update_task_status(task.id, TaskStatus.FAILED, error_message=result)
                logger.error(f"Задача {task.id} по публикации {task.task_type} не выполнена: {result}")
                return False, result
        except Exception as e:
            update_task_status(task.id, TaskStatus.FAILED, error_message=str(e))
            logger.error(f"Ошибка при выполнении задачи {task.id} по публикации {task.task_type}: {e}")
            return False, str(e)
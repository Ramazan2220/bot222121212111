import logging
import os
import json
from pathlib import Path
import concurrent.futures
from typing import List, Dict, Optional, Tuple

from instagram.client import InstagramClient
from database.db_manager import update_task_status, get_instagram_accounts
from config import MAX_WORKERS
from instagram.clip_upload_patch import *  # Импортируем патч
from database.models import TaskStatus
from utils.content_uniquifier import ContentUniquifier
from instagrapi.types import Usertag, Location

logger = logging.getLogger(__name__)

class ReelsManager:
    def __init__(self, account_id):
        self.instagram = InstagramClient(account_id)
        self.account_id = account_id
        self.uniquifier = ContentUniquifier()

    def publish_reel(self, video_path, caption=None, thumbnail_path=None, 
                    usertags=None, location=None, hashtags=None, cover_time=0):
        """
        Публикация видео в Reels с расширенными возможностями
        
        Args:
            video_path: Путь к видео файлу
            caption: Подпись к Reels
            thumbnail_path: Путь к превью (обложке) - может быть загруженным фото
            usertags: Список пользовательских тегов
            location: Геолокация
            hashtags: Список хештегов
            cover_time: Время в секундах для выбора кадра обложки (игнорируется если есть thumbnail_path)
        """
        try:
            # Проверяем статус входа с восстановлением
            if not self._ensure_login_with_recovery():
                return False, "Не удалось войти в аккаунт"

            # Проверяем существование файла
            if not os.path.exists(video_path):
                logger.error(f"Файл {video_path} не найден")
                return False, f"Файл не найден: {video_path}"

            # Подготавливаем подпись с хештегами
            full_caption = self._prepare_caption(caption, hashtags)
            
            # Подготавливаем пользовательские теги
            processed_usertags = self._prepare_usertags(usertags)
            
            # Подготавливаем локацию
            processed_location = self._prepare_location(location)
            
            # Определяем обложку
            final_thumbnail_path = None
            generated_thumbnail = None
            
            if thumbnail_path and os.path.exists(thumbnail_path):
                # Используем загруженную обложку
                final_thumbnail_path = thumbnail_path
                logger.info(f"Используется загруженная обложка: {thumbnail_path}")
            elif cover_time > 0:
                # Генерируем обложку из видео
                generated_thumbnail = self._generate_thumbnail(video_path, cover_time)
                if generated_thumbnail:
                    final_thumbnail_path = generated_thumbnail
                    logger.info(f"Сгенерирована обложка на {cover_time} секунд: {generated_thumbnail}")
            
            # Публикуем Reels
            media = self.instagram.client.clip_upload(
                Path(video_path),
                caption=full_caption,
                thumbnail=Path(final_thumbnail_path) if final_thumbnail_path else None
            )
            
            # Добавляем пользовательские теги и локацию после публикации
            if media:
                # Добавляем пользовательские теги
                if processed_usertags:
                    try:
                        self.instagram.client.media_edit(media.pk, full_caption, usertags=processed_usertags)
                        logger.info(f"Добавлены пользовательские теги к Reels: {len(processed_usertags)} шт.")
                    except Exception as e:
                        logger.warning(f"Не удалось добавить пользовательские теги: {e}")
                
                # Добавляем локацию
                if processed_location:
                    try:
                        self.instagram.client.media_edit(media.pk, full_caption, location=processed_location)
                        logger.info(f"Добавлена локация к Reels: {processed_location.name}")
                    except Exception as e:
                        logger.warning(f"Не удалось добавить локацию: {e}")

            # Очищаем временную обложку
            if generated_thumbnail and os.path.exists(generated_thumbnail):
                try:
                    os.remove(generated_thumbnail)
                except:
                    pass

            logger.info(f"Reels успешно опубликован: {media.pk}")
            return True, media.pk
            
        except Exception as e:
            error_msg = str(e) if e else "Unknown error"
            logger.error(f"Ошибка при публикации Reels: {error_msg}")
            # Логируем серьезные ошибки входа для дальнейшего анализа
            if error_msg and ("login" in error_msg.lower() or "challenge" in error_msg.lower()):
                logger.error(f"Критическая ошибка входа в аккаунт {self.account_id}: {e}")
            
            # Проверяем, нужно ли повторить попытку с восстановлением
            if error_msg and any(keyword in error_msg.lower() for keyword in ['challenge_required', 'login_required', 'verification_required']):
                logger.info(f"🔄 Обнаружена ошибка входа, пытаемся войти повторно с восстановлением...")
                
                # Принудительно пытаемся IMAP восстановление даже если check_login возвращает True
                account = self.instagram.account
                
                # Проверяем наличие email для восстановления
                if account.email and account.email_password:
                    logger.info(f"📧 Принудительное IMAP восстановление для @{account.username} из-за ошибки публикации Reels...")
                    
                    try:
                        from instagram.email_utils_optimized import get_verification_code_from_email
                        from instagram.email_utils import mark_account_problematic
                        from database.db_manager import update_instagram_account
                        from datetime import datetime
                        
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
                                    
                                    # Повторяем публикацию Reels
                                    logger.info(f"🔄 Повторная попытка публикации Reels после IMAP восстановления...")
                                    
                                    try:
                                        # Повторяем публикацию
                                        media_retry = self.instagram.client.clip_upload(
                                            Path(video_path),
                                            caption=full_caption,
                                            thumbnail=Path(thumbnail_path) if thumbnail_path and os.path.exists(thumbnail_path) else None
                                        )
                                        
                                        if media_retry:
                                            # Добавляем пользовательские теги и локацию после публикации
                                            if processed_usertags:
                                                try:
                                                    self.instagram.client.media_edit(media_retry.pk, full_caption, usertags=processed_usertags)
                                                    logger.info(f"Добавлены пользовательские теги к Reels после IMAP: {len(processed_usertags)} шт.")
                                                except Exception as tag_error:
                                                    logger.warning(f"Не удалось добавить пользовательские теги после IMAP: {tag_error}")
                                            
                                            if processed_location:
                                                try:
                                                    self.instagram.client.media_edit(media_retry.pk, full_caption, location=processed_location)
                                                    logger.info(f"Добавлена локация к Reels после IMAP: {processed_location.name}")
                                                except Exception as loc_error:
                                                    logger.warning(f"Не удалось добавить локацию после IMAP: {loc_error}")
                                            
                                            logger.info(f"Reels успешно опубликован после IMAP восстановления: {media_retry.pk}")
                                            return True, media_retry.pk
                                        else:
                                            logger.error("Не удалось опубликовать Reels после IMAP восстановления")
                                            mark_account_problematic(
                                                self.instagram.account.email,
                                                "reels_failed_after_imap",
                                                "Не удалось опубликовать Reels после IMAP восстановления"
                                            )
                                            return False, "ERROR - Не удалось опубликовать Reels после IMAP восстановления"
                                            
                                    except Exception as pub_retry_error:
                                        logger.error(f"Ошибка при повторной публикации Reels после IMAP: {pub_retry_error}")
                                        mark_account_problematic(
                                            self.instagram.account.email,
                                            "reels_error_after_imap",
                                            f"Ошибка при повторной публикации Reels после IMAP: {str(pub_retry_error)}"
                                        )
                                        return False, f"ERROR - {pub_retry_error}"
                                        
                                except Exception as verify_error:
                                    logger.warning(f"❌ Принудительное IMAP восстановление Reels не подтвердилось: {verify_error}")
                                    mark_account_problematic(
                                        account.email,
                                        "forced_reels_recovery_verify_failed",
                                        f"Принудительное восстановление Reels не подтвердилось: {verify_error}"
                                    )
                            else:
                                logger.warning(f"❌ Принудительное IMAP восстановление Reels не удалось для @{account.username}")
                                mark_account_problematic(
                                    account.email,
                                    "forced_reels_recovery_login_failed",
                                    "Принудительное IMAP восстановление Reels не удалось"
                                )
                        else:
                            logger.warning(f"❌ Не удалось получить код для принудительного восстановления Reels @{account.username}")
                            mark_account_problematic(
                                account.email,
                                "forced_reels_email_code_failed",
                                "Не удалось получить код для принудительного восстановления Reels"
                            )
                            
                    except Exception as forced_recovery_error:
                        logger.error(f"❌ Ошибка принудительного IMAP восстановления Reels: {forced_recovery_error}")
                        mark_account_problematic(
                            account.email,
                            "forced_reels_imap_recovery_error",
                            f"Ошибка принудительного IMAP восстановления Reels: {str(forced_recovery_error)}"
                        )
                
                # Если принудительное восстановление не помогло, пробуем обычный метод
                if self._ensure_login_with_recovery():
                    logger.info(f"🔄 Повторная попытка публикации Reels после входа...")
                    
                    try:
                        # Повторяем публикацию
                        media_retry = self.instagram.client.clip_upload(
                            Path(video_path),
                            caption=full_caption,
                            thumbnail=Path(thumbnail_path) if thumbnail_path and os.path.exists(thumbnail_path) else None
                        )
                        
                        if media_retry:
                            # Добавляем пользовательские теги и локацию после публикации
                            if processed_usertags:
                                try:
                                    self.instagram.client.media_edit(media_retry.pk, full_caption, usertags=processed_usertags)
                                    logger.info(f"Добавлены пользовательские теги к Reels после повторного входа: {len(processed_usertags)} шт.")
                                except Exception as tag_error:
                                    logger.warning(f"Не удалось добавить пользовательские теги после повторного входа: {tag_error}")
                            
                            if processed_location:
                                try:
                                    self.instagram.client.media_edit(media_retry.pk, full_caption, location=processed_location)
                                    logger.info(f"Добавлена локация к Reels после повторного входа: {processed_location.name}")
                                except Exception as loc_error:
                                    logger.warning(f"Не удалось добавить локацию после повторного входа: {loc_error}")
                            
                            logger.info(f"Reels успешно опубликован после повторного входа: {media_retry.pk}")
                            return True, media_retry.pk
                        else:
                            logger.error("Не удалось опубликовать Reels после повторного входа")
                            return False, "ERROR - Не удалось опубликовать Reels после повторного входа"
                            
                    except Exception as retry_error:
                        logger.error(f"Ошибка при повторной публикации Reels: {retry_error}")
                        return False, f"ERROR - {retry_error}"
            
            return False, str(e)

    def get_location_by_name(self, name: str) -> Optional[Location]:
        """Поиск локации по названию или координатам"""
        try:
            if not self._ensure_login_with_recovery():
                return None
            
            logger.info(f"🔍 Поиск локации: {name}")
            
            # Проверяем, если введены координаты в формате "lat,lng"
            if ',' in name and len(name.split(',')) == 2:
                try:
                    lat_str, lng_str = name.split(',')
                    lat = float(lat_str.strip())
                    lng = float(lng_str.strip())
                    logger.info(f"📍 Используются координаты: {lat}, {lng}")
                except ValueError:
                    logger.warning(f"❌ Неверный формат координат: {name}")
                    return None
            else:
                # Расширенный словарь координат популярных городов и мест
                city_coords = {
                    # Украина
                    'kiev': (50.4501, 30.5234), 'киев': (50.4501, 30.5234), 'kyiv': (50.4501, 30.5234),
                    'odessa': (46.4825, 30.7233), 'одесса': (46.4825, 30.7233),
                    'kharkiv': (49.9935, 36.2304), 'харьков': (49.9935, 36.2304),
                    'lviv': (49.8397, 24.0297), 'львов': (49.8397, 24.0297),
                    'dnipro': (48.4647, 35.0462), 'днепр': (48.4647, 35.0462),
                    'ukraine': (50.4501, 30.5234), 'украина': (50.4501, 30.5234),
                    
                    # Россия
                    'moscow': (55.7558, 37.6176), 'москва': (55.7558, 37.6176),
                    'saint petersburg': (59.9311, 30.3609), 'санкт-петербург': (59.9311, 30.3609), 'spb': (59.9311, 30.3609),
                    'novosibirsk': (55.0084, 82.9357), 'новосибирск': (55.0084, 82.9357),
                    'yekaterinburg': (56.8431, 60.6454), 'екатеринбург': (56.8431, 60.6454),
                    'kazan': (55.8304, 49.0661), 'казань': (55.8304, 49.0661),
                    'sochi': (43.6028, 39.7342), 'сочи': (43.6028, 39.7342),
                    'russia': (55.7558, 37.6176), 'россия': (55.7558, 37.6176),
                    
                    # Европа
                    'london': (51.5074, -0.1278), 'лондон': (51.5074, -0.1278),
                    'paris': (48.8566, 2.3522), 'париж': (48.8566, 2.3522),
                    'berlin': (52.5200, 13.4050), 'берлин': (52.5200, 13.4050),
                    'rome': (41.9028, 12.4964), 'рим': (41.9028, 12.4964),
                    'madrid': (40.4168, -3.7038), 'мадрид': (40.4168, -3.7038),
                    'amsterdam': (52.3676, 4.9041), 'амстердам': (52.3676, 4.9041),
                    'vienna': (48.2082, 16.3738), 'вена': (48.2082, 16.3738),
                    'prague': (50.0755, 14.4378), 'прага': (50.0755, 14.4378),
                    'warsaw': (52.2297, 21.0122), 'варшава': (52.2297, 21.0122),
                    'stockholm': (59.3293, 18.0686), 'стокгольм': (59.3293, 18.0686),
                    
                    # Америка
                    'new york': (40.7128, -74.0060), 'нью-йорк': (40.7128, -74.0060), 'nyc': (40.7128, -74.0060),
                    'los angeles': (34.0522, -118.2437), 'лос-анджелес': (34.0522, -118.2437), 'la': (34.0522, -118.2437),
                    'chicago': (41.8781, -87.6298), 'чикаго': (41.8781, -87.6298),
                    'miami': (25.7617, -80.1918), 'майами': (25.7617, -80.1918),
                    'toronto': (43.6532, -79.3832), 'торонто': (43.6532, -79.3832),
                    'vancouver': (49.2827, -123.1207), 'ванкувер': (49.2827, -123.1207),
                    'mexico city': (19.4326, -99.1332), 'мехико': (19.4326, -99.1332),
                    
                    # Азия
                    'tokyo': (35.6762, 139.6503), 'токио': (35.6762, 139.6503),
                    'seoul': (37.5665, 126.9780), 'сеул': (37.5665, 126.9780),
                    'beijing': (39.9042, 116.4074), 'пекин': (39.9042, 116.4074),
                    'shanghai': (31.2304, 121.4737), 'шанхай': (31.2304, 121.4737),
                    'singapore': (1.3521, 103.8198), 'сингапур': (1.3521, 103.8198),
                    'dubai': (25.2048, 55.2708), 'дубай': (25.2048, 55.2708),
                    'mumbai': (19.0760, 72.8777), 'мумбаи': (19.0760, 72.8777),
                    'bangkok': (13.7563, 100.5018), 'бангкок': (13.7563, 100.5018),
                    
                    # Австралия и Океания
                    'sydney': (-33.8688, 151.2093), 'сидней': (-33.8688, 151.2093),
                    'melbourne': (-37.8136, 144.9631), 'мельбурн': (-37.8136, 144.9631),
                    
                    # Африка
                    'cairo': (30.0444, 31.2357), 'каир': (30.0444, 31.2357),
                    'cape town': (-33.9249, 18.4241), 'кейптаун': (-33.9249, 18.4241),
                    
                    # Популярные места
                    'times square': (40.7580, -73.9855),
                    'red square': (55.7539, 37.6208), 'красная площадь': (55.7539, 37.6208),
                    'eiffel tower': (48.8584, 2.2945), 'эйфелева башня': (48.8584, 2.2945),
                    'big ben': (51.4994, -0.1245), 'биг бен': (51.4994, -0.1245),
                    'colosseum': (41.8902, 12.4922), 'колизей': (41.8902, 12.4922),
                    'central park': (40.7829, -73.9654), 'центральный парк': (40.7829, -73.9654),
                    'hollywood': (34.0928, -118.3287), 'голливуд': (34.0928, -118.3287),
                    'las vegas': (36.1699, -115.1398), 'лас-вегас': (36.1699, -115.1398),
                    'machu picchu': (-13.1631, -72.5450), 'мачу-пикчу': (-13.1631, -72.5450)
                }
                
                # Используем координаты центра мира для широкого поиска
                lat, lng = 50.0, 30.0  # Примерные координаты для поиска
                
                name_lower = name.lower()
                for city, coords in city_coords.items():
                    if city in name_lower:
                        lat, lng = coords
                        break
            
            # Ищем локации в радиусе от координат
            locations = self.instagram.client.location_search(lat, lng)
            
            if locations:
                # Ищем наиболее подходящую локацию
                for location in locations:
                    # Проверяем совпадение по названию
                    if any(word.lower() in location.name.lower() for word in name.split() if len(word) > 2):
                        logger.info(f"✅ Найдена локация: {location.name} (ID: {location.pk})")
                        return location
                
                # Если точного совпадения нет, берем первую
                location = locations[0]
                logger.info(f"✅ Найдена локация (первая): {location.name} (ID: {location.pk})")
                return location
            else:
                logger.warning(f"❌ Локация '{name}' не найдена")
                return None
                
        except Exception as e:
            logger.error(f"Ошибка при поиске локации '{name}': {e}")
            return None

    def _ensure_login_with_recovery(self, max_attempts=3):
        """Обеспечивает вход в аккаунт с IMAP восстановлением и обновлением статуса"""
        from instagram.email_utils_optimized import get_verification_code_from_email
        from instagram.email_utils import mark_account_problematic
        from database.db_manager import update_instagram_account
        from datetime import datetime
        import time
        
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

    def _check_login_with_recovery(self):
        """Проверка входа с автоматическим восстановлением при необходимости"""
        return self._ensure_login_with_recovery()

    def _prepare_caption(self, caption: str, hashtags: List[str] = None) -> str:
        """Подготовка подписи с хештегами"""
        if not caption:
            caption = ""
        
        if hashtags:
            # Добавляем хештеги в конец подписи
            hashtag_str = " ".join([f"#{tag.lstrip('#')}" for tag in hashtags])
            if caption:
                caption = f"{caption}\n\n{hashtag_str}"
            else:
                caption = hashtag_str
        
        return caption

    def _prepare_usertags(self, usertags: List[Dict] = None) -> List[Usertag]:
        """Подготовка пользовательских тегов"""
        if not usertags:
            return []
        
        processed_tags = []
        try:
            for tag in usertags:
                username = tag.get('username', '').lstrip('@')
                if username:
                    # Получаем информацию о пользователе
                    user_info = self.instagram.client.user_info_by_username(username)
                    if user_info:
                        # Конвертируем User в UserShort или используем только нужные поля
                        from instagrapi.types import UserShort
                        user_short = UserShort(
                            pk=user_info.pk,
                            username=user_info.username,
                            full_name=user_info.full_name,
                            profile_pic_url=user_info.profile_pic_url,
                            profile_pic_url_hd=user_info.profile_pic_url_hd,
                            is_verified=user_info.is_verified,
                            is_private=user_info.is_private
                        )
                        
                        usertag = Usertag(
                            user=user_short,
                            x=tag.get('x', 0.5),  # Позиция по X (0.0-1.0)
                            y=tag.get('y', 0.5)   # Позиция по Y (0.0-1.0)
                        )
                        processed_tags.append(usertag)
                        logger.info(f"Добавлен тег пользователя: @{username}")
                    else:
                        logger.warning(f"Не удалось найти пользователя: @{username}")
        except Exception as e:
            logger.error(f"Ошибка при подготовке пользовательских тегов: {e}")
        
        return processed_tags

    def _prepare_location(self, location_name: str = None) -> Optional[Location]:
        """Подготовка локации"""
        if not location_name:
            return None
        
        try:
            location = self.get_location_by_name(location_name)
            if location:
                logger.info(f"Добавлена локация: {location_name}")
            else:
                logger.warning(f"Локация не найдена: {location_name}")
            return location
        except Exception as e:
            logger.error(f"Ошибка при подготовке локации: {e}")
            return None

    def _generate_thumbnail(self, video_path: str, cover_time: float) -> Optional[str]:
        """Генерация обложки из видео"""
        try:
            import cv2
            import tempfile
            
            # Открываем видео
            cap = cv2.VideoCapture(video_path)
            if not cap.isOpened():
                logger.error(f"Не удалось открыть видео: {video_path}")
                return None
            
            # Получаем FPS видео
            fps = cap.get(cv2.CAP_PROP_FPS)
            if fps <= 0:
                fps = 30  # Fallback FPS
            
            # Вычисляем номер кадра
            frame_number = int(cover_time * fps)
            
            # Устанавливаем позицию
            cap.set(cv2.CAP_PROP_POS_FRAMES, frame_number)
            
            # Читаем кадр
            ret, frame = cap.read()
            cap.release()
            
            if not ret:
                logger.error(f"Не удалось извлечь кадр на {cover_time} секунде")
                return None
            
            # Сохраняем кадр как временный файл
            with tempfile.NamedTemporaryFile(delete=False, suffix='.jpg') as temp_file:
                thumbnail_path = temp_file.name
            
            cv2.imwrite(thumbnail_path, frame)
            logger.info(f"Обложка создана: {thumbnail_path} (время: {cover_time}с)")
            return thumbnail_path
            
        except ImportError:
            logger.warning("OpenCV не установлен, обложка не будет создана")
            return None
        except Exception as e:
            logger.error(f"Ошибка при создании обложки: {e}")
            return None

    def execute_reel_task(self, task):
        """Выполнение задачи по публикации Reels"""
        try:
            # Обновляем статус задачи
            update_task_status(task.id, TaskStatus.PROCESSING)

            # Получаем параметры из задачи
            options = self._parse_task_options(task)
            
            # Уникализируем контент если нужно
            video_path = task.media_path
            caption = task.caption or ""
            
            if options.get('uniquify_content', False):
                video_path, caption = self.uniquifier.uniquify_content(
                    video_path, 'reel', caption
                )

            # Публикуем Reels
            success, result = self.publish_reel(
                video_path=video_path,
                caption=caption,
                thumbnail_path=options.get('thumbnail_path'),
                usertags=options.get('usertags', []),
                location=options.get('location'),
                hashtags=options.get('hashtags', []),
                cover_time=options.get('cover_time', 0)
            )

            # Обновляем статус задачи
            if success:
                update_task_status(task.id, TaskStatus.COMPLETED, media_id=result)
                logger.info(f"Задача {task.id} по публикации Reels выполнена успешно")
                return True, result
            else:
                update_task_status(task.id, TaskStatus.FAILED, error_message=result)
                logger.error(f"Задача {task.id} по публикации Reels не выполнена: {result}")
                return False, result
                
        except Exception as e:
            logger.error(f"Ошибка при выполнении задачи {task.id} по публикации Reels: {e}")
            update_task_status(task.id, TaskStatus.FAILED, error_message=str(e))
            return False, str(e)

    def _parse_task_options(self, task) -> Dict:
        """Парсинг опций задачи"""
        options = {}
        
        # Пробуем получить из options
        if hasattr(task, 'options') and task.options:
            try:
                options.update(json.loads(task.options))
            except Exception as e:
                logger.warning(f"Не удалось разобрать options для задачи {task.id}: {e}")
        
        # Пробуем получить из additional_data
        if hasattr(task, 'additional_data') and task.additional_data:
            try:
                options.update(json.loads(task.additional_data))
            except Exception as e:
                logger.warning(f"Не удалось разобрать additional_data для задачи {task.id}: {e}")
        
        return options

def publish_reels_in_parallel(video_path, caption, account_ids, 
                             usertags=None, location=None, 
                             hashtags=None, cover_time=0, uniquify_content=True):
    """
    Публикация Reels в несколько аккаунтов параллельно с уникализацией
    """
    results = {}
    uniquifier = ContentUniquifier()

    def publish_to_account(account_id):
        manager = ReelsManager(account_id)
        
        # Уникализируем контент для каждого аккаунта
        unique_video_path = video_path
        unique_caption = caption
        
        if uniquify_content and len(account_ids) > 1:
            unique_video_path, unique_caption = uniquifier.uniquify_content(
                video_path, 'reel', caption
            )
        
        success, result = manager.publish_reel(
            video_path=unique_video_path,
            caption=unique_caption,
            usertags=usertags,
            location=location,
            hashtags=hashtags,
            cover_time=cover_time
        )
        
        return account_id, success, result

    # Используем ThreadPoolExecutor для параллельной публикации
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(publish_to_account, account_id) for account_id in account_ids]

        for future in concurrent.futures.as_completed(futures):
            try:
                account_id, success, result = future.result()
                results[account_id] = {'success': success, 'result': result}
            except Exception as e:
                logger.error(f"Ошибка при параллельной публикации: {e}")

    return results


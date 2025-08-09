import os
import logging
import random
import time
import re
from typing import Dict, Any, Tuple, Optional, List
import concurrent.futures
import threading

from pathlib import Path

from database.db_manager import get_instagram_account
from instagram.client import get_instagram_client

logger = logging.getLogger(__name__)

class ProfileManager:
    """Менеджер для управления профилем Instagram"""
    
    def __init__(self, account_id):
        self.account_id = account_id
        self.account = get_instagram_account(account_id)
        self.client = get_instagram_client(account_id)

        if self.client is None:
            logger.error(f"Не удалось инициализировать клиент для аккаунта {account_id}")
            # Пробуем выполнить вход еще раз
            if self.account:
                logger.info(f"Пробуем повторно войти в аккаунт {self.account.username}")
                from instagram.client import test_instagram_login_with_proxy
                success = test_instagram_login_with_proxy(
                    account_id,
                    self.account.username,
                    self.account.password,
                    getattr(self.account, 'email', None),
                    getattr(self.account, 'email_password', None)
                )
                if success:
                    self.client = get_instagram_client(account_id)
                    logger.info(f"Повторный вход в аккаунт {self.account.username} успешен")
                else:
                    logger.error(f"Повторный вход в аккаунт {self.account.username} не удался")

            # Если клиент все еще None, вызываем исключение
            if self.client is None:
                raise Exception(f"Клиент Instagram не инициализирован для аккаунта {account_id}")

    def get_profile_info(self):
        """Получает информацию о профиле"""
        try:
            # Добавляем небольшую задержку для имитации человеческого поведения
            time.sleep(random.uniform(1, 3))
            profile_info = self.client.account_info()
            return profile_info
        except Exception as e:
            logger.error(f"Ошибка при получении информации о профиле: {e}")
            return {}

    def get_profile_links(self):
        """Получает ссылки профиля"""
        try:
            # Добавляем небольшую задержку для имитации человеческого поведения
            time.sleep(random.uniform(1, 2))
            profile_info = self.client.account_info()
            return profile_info.external_url  # Исправлено: используем external_url вместо get('external_links')
        except Exception as e:
            logger.error(f"Ошибка при получении ссылок профиля: {e}")
            return ""

    def update_profile_name(self, full_name):
        """Обновляет имя профиля"""
        try:
            # Добавляем задержку для имитации человеческого поведения
            time.sleep(random.uniform(2, 4))
            result = self.client.account_edit(full_name=full_name)
            
            # Если успешно обновлено в Instagram, обновляем в базе данных
            if result:
                from database.db_manager import update_instagram_account
                success, message = update_instagram_account(self.account_id, full_name=full_name)

                if not success:
                    logger.warning(f"Имя профиля обновлено в Instagram, но не обновлено в базе данных: {message}")
                else:
                    logger.info(f"Имя профиля успешно обновлено в Instagram и в базе данных")
            
            return True, "Имя профиля успешно обновлено"
        except Exception as e:
            logger.error(f"Ошибка при обновлении имени профиля: {e}")
            return False, str(e)

    def update_username(self, username):
        """Обновляет имя пользователя"""
        try:
            # Добавляем задержку для имитации человеческого поведения
            time.sleep(random.uniform(2, 4))

            # Обновляем имя пользователя в Instagram
            result = self.client.account_edit(username=username)

            # Если успешно обновлено в Instagram, обновляем в базе данных
            if result:
                from database.db_manager import update_instagram_account
                success, message = update_instagram_account(self.account_id, username=username)

                if not success:
                    logger.warning(f"Имя пользователя обновлено в Instagram, но не обновлено в базе данных: {message}")
                else:
                    logger.info(f"Имя пользователя успешно обновлено в Instagram и в базе данных")

                # Обновляем имя пользователя в объекте аккаунта
                self.account.username = username

            return True, "Имя пользователя успешно обновлено"
        except Exception as e:
            logger.error(f"Ошибка при обновлении имени пользователя: {e}")
            return False, str(e)

    def update_biography(self, biography):
        """Обновляет описание профиля"""
        try:
            # Добавляем задержку для имитации человеческого поведения
            time.sleep(random.uniform(2, 4))
            result = self.client.account_edit(biography=biography)
            
            # Если успешно обновлено в Instagram, обновляем в базе данных
            if result:
                from database.db_manager import update_instagram_account
                success, message = update_instagram_account(self.account_id, biography=biography)

                if not success:
                    logger.warning(f"Биография обновлена в Instagram, но не обновлена в базе данных: {message}")
                else:
                    logger.info(f"Биография успешно обновлена в Instagram и в базе данных")
            
            return True, "Описание профиля успешно обновлено"
        except Exception as e:
            logger.error(f"Ошибка при обновлении описания профиля: {e}")
            return False, str(e)

    def update_profile_links(self, link):
        """Обновляет ссылку профиля используя правильный метод set_external_url"""
        try:
            # Получаем URL из параметра
            url = link
            if isinstance(link, list) and link:
                url = link[0].get('url', '')
            elif isinstance(link, str):
                url = link

            if not url:
                logger.warning("Некорректный формат ссылки или ссылка отсутствует.")
                return False, "Некорректная ссылка"

            logger.info(f"Добавляем ссылку в профиль: {url}")
            
            # Добавляем задержку для имитации человеческого поведения
            time.sleep(random.uniform(2, 5))
            
            # Используем ПРАВИЛЬНЫЙ метод set_external_url
            try:
                result = self.client.set_external_url(url)
                logger.info(f"✅ Запрос set_external_url выполнен: {result.get('status', 'unknown')}")
                
                # Проверяем результат
                time.sleep(3)
                updated_info = self.client.account_info()
                
                # Проверяем в bio_links (новый метод добавляет ссылки туда)
                if hasattr(updated_info, 'bio_links') and updated_info.bio_links:
                    for bio_link in updated_info.bio_links:
                        if bio_link.get('url') == url:
                            logger.info(f"✅ Ссылка успешно добавлена в bio_links: {url}")
                            return True, f"Ссылка добавлена в профиль: {url}"
                
                # Проверяем external_url (на случай если заменилась основная ссылка)
                if updated_info.external_url == url:
                    logger.info(f"✅ Ссылка установлена как основная external_url: {url}")
                    return True, f"Ссылка установлена как основная: {url}"
                
                # Если результат успешный, но ссылка не видна сразу
                if result.get('status') == 'ok':
                    logger.info(f"✅ Запрос успешен, ссылка должна появиться в профиле: {url}")
                    return True, f"Ссылка добавлена (может потребоваться время для отображения): {url}"
                
                # Если ничего не сработало - пробуем fallback
                logger.warning("Прямое добавление не дало видимого результата, пробуем добавить в биографию...")
                return self.add_link_via_bio(url)
                
            except Exception as e:
                logger.error(f"Ошибка метода set_external_url: {e}")
                
                # Fallback к старому методу account_edit
                logger.info("Пробуем через account_edit...")
                return self._legacy_update_links(url)

        except Exception as e:
            logger.error(f"Ошибка при обновлении ссылки профиля: {e}")
            # Последний fallback - добавление в био
            logger.info("Используем резервный метод добавления в биографию...")
            return self.add_link_via_bio(link)
    
    def _legacy_update_links(self, url):
        """Старая логика обновления ссылок (резервный метод)"""
        try:
            # Добавляем задержку
            time.sleep(random.uniform(3, 6))

            # Получаем текущую информацию о профиле
            current_info = self.client.account_info()

            # Пробуем обновить ссылку
            result = self.client.account_edit(external_url=url)
            logger.info(f"Результат обновления ссылки (legacy): {result}")

            # Проверяем результат
            time.sleep(2)
            updated_info = self.client.account_info()
            if updated_info.external_url == url:
                return True, "Ссылка профиля успешно обновлена"
            else:
                logger.info("Прямое обновление не сработало, пробуем добавить в био...")
                return self.add_link_via_bio(url)
                
        except Exception as e:
            logger.error(f"Ошибка в legacy методе: {e}")
            return self.add_link_via_bio(url)

    def check_account_eligibility(self):
        """Проверяет, может ли аккаунт добавлять внешние ссылки"""
        try:
            info = self.client.account_info()
            
            # Проверяем тип аккаунта
            is_business = getattr(info, 'is_business', False)
            is_verified = getattr(info, 'is_verified', False)
            follower_count = getattr(info, 'follower_count', 0)
            
            # Instagram обычно разрешает ссылки:
            # - Бизнес аккаунтам
            # - Верифицированным аккаунтам  
            # - Аккаунтам с 10k+ подписчиков (иногда)
            can_add_links = (is_business or is_verified or follower_count >= 10000)
            
            logger.info(f"Проверка возможности добавления ссылок: "
                       f"Бизнес={is_business}, Верифицирован={is_verified}, "
                       f"Подписчиков={follower_count}, Может добавлять={can_add_links}")
            
            return can_add_links
            
        except Exception as e:
            logger.warning(f"Ошибка проверки типа аккаунта: {e}")
            # В случае ошибки предполагаем, что можно попробовать
            return True

    def add_link_via_bio(self, link):
        """Добавляет ссылку через описание профиля как альтернативный метод"""
        try:
            current_info = self.client.account_info()
            current_bio = current_info.biography or ""
            
            # Проверяем, есть ли уже ссылка в био
            if link in current_bio:
                return True, "Ссылка уже присутствует в описании профиля"
            
            # Добавляем ссылку в конец био
            if current_bio:
                new_bio = f"{current_bio}\n\n🔗 {link}"
            else:
                new_bio = f"🔗 {link}"
                
            # Проверяем длину (Instagram ограничивает био до 150 символов)
            if len(new_bio) > 150:
                # Обрезаем старое био, чтобы поместилась ссылка
                max_bio_length = 150 - len(f"\n\n🔗 {link}")
                trimmed_bio = current_bio[:max_bio_length].strip()
                new_bio = f"{trimmed_bio}\n\n🔗 {link}"
            
            result = self.client.account_edit(biography=new_bio)
            logger.info(f"Результат добавления ссылки в био: {result}")
            
            return True, "Ссылка добавлена в описание профиля"
            
        except Exception as e:
            logger.error(f"Ошибка добавления ссылки в био: {e}")
            return False, str(e)

    def convert_to_business_account(self):
        """Попытка конвертировать аккаунт в бизнес-аккаунт"""
        try:
            # Этот метод может отсутствовать в некоторых версиях instagrapi
            if hasattr(self.client, 'account_convert_to_business'):
                result = self.client.account_convert_to_business()
                logger.info(f"Результат конвертации в бизнес-аккаунт: {result}")
                return True, "Аккаунт успешно конвертирован в бизнес-аккаунт"
            else:
                return False, "Конвертация в бизнес-аккаунт не поддерживается этой версией instagrapi"
                
        except Exception as e:
            logger.error(f"Ошибка конвертации в бизнес-аккаунт: {e}")
            return False, str(e)

    def update_profile_picture(self, photo_path):
        """Обновляет фото профиля"""
        try:
            # Добавляем задержку для имитации человеческого поведения
            time.sleep(random.uniform(3, 6))
            result = self.client.account_change_picture(photo_path)
            return True, "Фото профиля успешно обновлено"
        except Exception as e:
            logger.error(f"Ошибка при обновлении фото профиля: {e}")
            return False, str(e)

    def remove_profile_picture(self):
        """Удаляет фото профиля"""
        try:
            logger.info(f"Удаление фото профиля для аккаунта {self.account.username}")
            
            # В instagrapi нет прямого метода удаления фото профиля
            # Вместо этого загружаем пустое/дефолтное изображение
            # Создаем однопиксельное прозрачное изображение
            from PIL import Image
            import tempfile
            
            # Создаем минимальное изображение
            img = Image.new('RGB', (320, 320), color='white')
            
            with tempfile.NamedTemporaryFile(suffix='.jpg', delete=False) as tmp:
                img.save(tmp.name, 'JPEG')
                tmp_path = tmp.name
            
            try:
                # Загружаем как новое фото профиля
                result = self.client.account_change_picture(tmp_path)
                logger.info(f"Результат удаления фото профиля: {result}")
                
                # Удаляем временный файл
                os.unlink(tmp_path)
                
                return True, "Фото профиля успешно удалено"
            except Exception as e:
                # Удаляем временный файл в случае ошибки
                if os.path.exists(tmp_path):
                    os.unlink(tmp_path)
                raise e
                
        except Exception as e:
            logger.error(f"Ошибка при удалении фото профиля: {e}")
            return False, str(e)
    
    def delete_all_posts(self):
        """Удаляет все посты аккаунта"""
        try:
            logger.info(f"Удаление всех постов для аккаунта {self.account.username}")
            
            # Получаем user_id
            try:
                user_id = self.client.user_id_from_username(self.account.username)
            except Exception as e:
                logger.error(f"Ошибка получения user_id: {e}")
                return False, f"Не удалось получить ID пользователя: {str(e)}"
            
            # Пытаемся получить медиа разными способами
            medias = []
            
            # Способ 1: Через user_medias (может вызвать ошибку парсинга)
            try:
                medias = self.client.user_medias(user_id, amount=50)
                logger.info(f"Получено {len(medias)} медиа через user_medias")
            except Exception as e:
                logger.warning(f"Ошибка получения медиа через user_medias: {e}")
                
                # Способ 2: Через user_medias_v1 (старый API)
                try:
                    medias = self.client.user_medias_v1(user_id, amount=50)
                    logger.info(f"Получено {len(medias)} медиа через user_medias_v1")
                except Exception as e:
                    logger.warning(f"Ошибка получения медиа через user_medias_v1: {e}")
                    
                    # Способ 3: Через feed
                    try:
                        feed = self.client.user_feed(user_id)
                        medias = feed.get('items', [])
                        logger.info(f"Получено {len(medias)} медиа через user_feed")
                    except Exception as e:
                        logger.error(f"Все методы получения медиа не сработали: {e}")
                        return False, "Не удалось получить список постов"
            
            if not medias:
                return True, "Нет постов для удаления"
            
            deleted_count = 0
            errors_count = 0
            
            for media in medias:
                try:
                    # Получаем ID медиа
                    media_id = None
                    if hasattr(media, 'id'):
                        media_id = media.id
                    elif hasattr(media, 'pk'):
                        media_id = media.pk
                    elif isinstance(media, dict):
                        media_id = media.get('id') or media.get('pk')
                    
                    if not media_id:
                        logger.warning(f"Не удалось получить ID для медиа: {media}")
                        errors_count += 1
                        continue
                    
                    # Удаляем медиа
                    self.client.media_delete(media_id)
                    deleted_count += 1
                    logger.info(f"Удален пост {media_id}")
                    
                    # Задержка между удалениями
                    time.sleep(random.uniform(2, 4))
                    
                except Exception as e:
                    errors_count += 1
                    logger.error(f"Ошибка удаления поста: {e}")
            
            message = f"Удалено постов: {deleted_count}"
            if errors_count > 0:
                message += f", ошибок: {errors_count}"
                
            return deleted_count > 0, message
            
        except Exception as e:
            logger.error(f"Ошибка при удалении постов: {e}")
            return False, str(e)
    
    def clear_biography(self):
        """Очищает описание профиля"""
        try:
            logger.info(f"Очистка описания профиля для аккаунта {self.account.username}")
            
            # Устанавливаем пустое описание
            result = self.client.account_edit(biography="")
            logger.info(f"Результат очистки описания: {result}")
            
            # Обновляем в базе данных
            from database.db_manager import update_instagram_account
            update_instagram_account(self.account_id, biography="")
            
            return True, "Описание профиля успешно очищено"
            
        except Exception as e:
            logger.error(f"Ошибка при очистке описания: {e}")
            return False, str(e)

    def upload_photo(self, photo_path, caption="", pin=False):
        """Загружает фото в профиль"""
        try:
            # Добавляем задержку для имитации человеческого поведения
            time.sleep(random.uniform(4, 8))
            result = self.client.photo_upload(photo_path, caption)

            # Если нужно закрепить пост
            if pin and result.get('pk'):
                # Добавляем небольшую задержку перед закреплением
                time.sleep(random.uniform(2, 4))
                self.client.highlight_create("Закрепленные", [result.get('pk')])

            return True, "Фото успешно загружено"
        except Exception as e:
            logger.error(f"Ошибка при загрузке фото: {e}")
            return False, str(e)

    def upload_video(self, video_path, caption="", pin=False):
        """Загружает видео в профиль"""
        try:
            # Добавляем задержку для имитации человеческого поведения
            time.sleep(random.uniform(5, 10))
            result = self.client.video_upload(video_path, caption)

            # Если нужно закрепить пост
            if pin and result.get('pk'):
                # Добавляем небольшую задержку перед закреплением
                time.sleep(random.uniform(2, 4))
                self.client.highlight_create("Закрепленные", [result.get('pk')])

            return True, "Видео успешно загружено"
        except Exception as e:
            logger.error(f"Ошибка при загрузке видео: {e}")
            return False, str(e)

    def execute_profile_task(self, task):
        """Выполняет задачу по обновлению профиля"""
        try:
            # Добавляем задержку перед началом выполнения задачи
            time.sleep(random.uniform(2, 5))

            # Получаем опции задачи
            options = task.options or {}

            # Обновляем имя пользователя, если оно указано
            if options.get('username'):
                success, message = self.update_username(options.get('username'))
                if not success:
                    logger.error(f"Ошибка при обновлении имени пользователя: {message}")
                # Добавляем задержку между действиями
                time.sleep(random.uniform(2, 4))

            # Обновляем полное имя, если оно указано
            if options.get('full_name'):
                success, message = self.update_profile_name(options.get('full_name'))
                if not success:
                    logger.error(f"Ошибка при обновлении полного имени: {message}")
                # Добавляем задержку между действиями
                time.sleep(random.uniform(2, 4))

                # Обновляем в базе данных
                from database.db_manager import update_instagram_account
                update_instagram_account(self.account_id, full_name=options.get('full_name'))

            # Обновляем описание профиля, если оно указано
            if task.caption or options.get('biography'):
                bio = task.caption or options.get('biography')
                success, message = self.update_biography(bio)
                if not success:
                    logger.error(f"Ошибка при обновлении описания профиля: {message}")
                # Добавляем задержку между действиями
                time.sleep(random.uniform(2, 4))

                # Обновляем в базе данных
                from database.db_manager import update_instagram_account
                update_instagram_account(self.account_id, biography=bio)

            # Обновляем ссылку профиля, если она указана
            if options.get('external_url'):
                success, message = self.update_profile_links(options.get('external_url'))
                if not success:
                    logger.error(f"Ошибка при обновлении ссылки профиля: {message}")
                # Добавляем задержку между действиями
                time.sleep(random.uniform(2, 4))

            # Обновляем фото профиля, если оно указано
            if task.media_path and os.path.exists(task.media_path):
                success, message = self.update_profile_picture(task.media_path)
                if not success:
                    logger.error(f"Ошибка при обновлении фото профиля: {message}")

            # После всех изменений, получаем обновленную информацию о профиле
            profile_info = self.get_profile_info()

            # Обновляем информацию в базе данных
            if profile_info:
                from database.db_manager import update_instagram_account
                update_data = {
                    'username': profile_info.username,
                    'full_name': profile_info.full_name,
                    'biography': profile_info.biography
                }
                success, message = update_instagram_account(self.account_id, **update_data)
                if not success:
                    logger.error(f"Ошибка при обновлении информации аккаунта в базе данных: {message}")

            return True, "Профиль успешно обновлен"
        except Exception as e:
            logger.error(f"Ошибка при выполнении задачи обновления профиля: {e}")
            return False, str(e)
        except Exception as e:
            logger.error(f"Ошибка при удалении фото профиля: {e}")
            return False, str(e)

    @staticmethod
    def batch_update_profiles(account_ids: List[int], update_type: str, value: str = None, 
                            max_workers: int = 4, progress_callback=None) -> Tuple[int, List[Dict[str, Any]]]:
        """
        Параллельное обновление профилей
        
        Args:
            account_ids: Список ID аккаунтов для обновления
            update_type: Тип обновления ('name', 'username', 'bio', 'link')
            value: Значение для обновления (может содержать шаблоны)
            max_workers: Максимальное количество потоков
            progress_callback: Функция обратного вызова для обновления прогресса
            
        Returns:
            Tuple[успешно_обновлено, список_ошибок]
        """
        success_count = 0
        errors = []
        processed_count = 0
        lock = threading.Lock()
        
        def update_single_profile(account_id: int) -> None:
            """Обновляет один профиль"""
            nonlocal success_count, processed_count
            
            try:
                # Получаем аккаунт
                from database.db_manager import get_instagram_account
                account = get_instagram_account(account_id)
                
                if not account:
                    with lock:
                        errors.append({'account_id': account_id, 'error': 'Аккаунт не найден'})
                        processed_count += 1
                    return
                    
                # Проверяем статус аккаунта
                if account.status != 'active':
                    with lock:
                        errors.append({
                            'account_id': account_id, 
                            'username': account.username,
                            'error': f'Аккаунт неактивен (статус: {account.status})'
                        })
                        processed_count += 1
                    return
                
                # Создаем ProfileManager
                try:
                    profile_manager = ProfileManager(account_id)
                except Exception as e:
                    with lock:
                        errors.append({
                            'account_id': account_id,
                            'username': account.username,
                            'error': f'Не удалось войти в аккаунт: {str(e)}'
                        })
                        processed_count += 1
                    return
                
                # Обрабатываем значение с шаблонами
                processed_value = ProfileManager._process_template_value(value, account)
                
                # Выполняем обновление в зависимости от типа
                if update_type == 'name' or update_type == 'edit_name':
                    success, message = profile_manager.update_profile_name(processed_value)
                elif update_type == 'username' or update_type == 'edit_username':
                    success, message = profile_manager.update_username(processed_value)
                elif update_type == 'bio' or update_type == 'edit_bio':
                    success, message = profile_manager.update_biography(processed_value)
                elif update_type == 'link' or update_type == 'add_link':
                    success, message = profile_manager.update_profile_links(processed_value)
                else:
                    raise ValueError(f"Неизвестный тип обновления: {update_type}")
                
                # Обновляем счетчики
                with lock:
                    if success:
                        success_count += 1
                        logger.info(f"✅ @{account.username}: {message}")
                    else:
                        errors.append({
                            'account_id': account_id,
                            'username': account.username,
                            'error': message
                        })
                        logger.error(f"❌ @{account.username}: {message}")
                    
                    processed_count += 1
                    
                    # Вызываем callback прогресса
                    if progress_callback:
                        progress_callback(processed_count, len(account_ids), success_count, len(errors))
                        
            except Exception as e:
                with lock:
                    errors.append({
                        'account_id': account_id,
                        'username': getattr(account, 'username', f'ID {account_id}'),
                        'error': str(e)
                    })
                    processed_count += 1
                    logger.error(f"Ошибка при обновлении аккаунта {account_id}: {e}")
        
        # Запускаем параллельную обработку
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(update_single_profile, acc_id) for acc_id in account_ids]
            
            # Ждем завершения всех задач
            concurrent.futures.wait(futures)
        
        return success_count, errors
    
    @staticmethod
    def _process_template_value(template: str, account) -> str:
        """Обрабатывает шаблоны в значении"""
        import re
        import random
        
        # Функция для обработки шаблонов вида {вариант1|вариант2|вариант3}
        def replace_template(match):
            options = match.group(1).split('|')
            return random.choice(options)
        
        # Заменяем шаблоны
        processed = re.sub(r'\{([^}]+)\}', replace_template, template)
        
        # Заменяем специальные переменные
        processed = processed.replace('@username', f'@{account.username}')
        processed = processed.replace('@full_name', account.full_name or account.username)
        
        return processed
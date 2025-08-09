import logging
import random
import time
import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Set
from sqlalchemy.orm import Session
from instagrapi import Client
from instagrapi.types import User, UserShort

from database.models import FollowTask, FollowHistory, FollowTaskStatus, FollowSourceType
from database.db_manager import get_session
from instagram.client import get_instagram_client

logger = logging.getLogger(__name__)


class FollowManager:
    """Менеджер для управления автоподписками"""
    
    def __init__(self, task_id: int):
        self.task_id = task_id
        self.session = get_session()
        self.task = self.session.query(FollowTask).filter_by(id=task_id).first()
        if not self.task:
            raise ValueError(f"Задача с ID {task_id} не найдена")
        
        self.client = None
        self.instagram_client = None
        self._executor = ThreadPoolExecutor(max_workers=1)
        
    def __enter__(self):
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            self.session.close()
    
    def initialize_client(self) -> bool:
        """Инициализация Instagram клиента"""
        try:
            # Проверяем валидность аккаунта перед использованием
            from utils.smart_validator_service import validate_before_use, ValidationPriority
            
            logger.info(f"🔍 Проверка валидности аккаунта {self.task.account_id} перед запуском автоподписок")
            
            if not validate_before_use(self.task.account_id, ValidationPriority.HIGH):
                logger.error(f"❌ Аккаунт {self.task.account_id} невалиден или не готов")
                return False
            
            logger.info(f"✅ Аккаунт {self.task.account_id} валиден")
            
            client = get_instagram_client(self.task.account_id)
            if not client:
                logger.error(f"Не удалось получить клиент для аккаунта {self.task.account_id}")
                return False
            
            self.client = client
            self.instagram_client = self.client
            return True
            
        except Exception as e:
            logger.error(f"Ошибка инициализации клиента: {e}")
            return False
    
    def get_unique_targets(self, all_targets: List[UserShort]) -> List[UserShort]:
        """Получить уникальные цели для подписки (которые еще не обработаны этим аккаунтом)"""
        try:
            # Получаем историю подписок для этого аккаунта
            followed_user_ids = self.session.query(FollowHistory.target_user_id).filter_by(
                account_id=self.task.account_id
            ).all()
            
            followed_set = {str(user_id[0]) for user_id in followed_user_ids}
            
            # Также добавляем уже обработанных в этой задаче
            processed_set = set(self.task.processed_users or [])
            
            # Фильтруем только уникальные цели
            unique_targets = []
            for target in all_targets:
                target_id = str(target.pk)
                if target_id not in followed_set and target_id not in processed_set:
                    unique_targets.append(target)
            
            logger.info(f"📊 Найдено {len(unique_targets)} уникальных целей из {len(all_targets)} общих")
            return unique_targets
            
        except Exception as e:
            logger.error(f"Ошибка при получении уникальных целей: {e}")
            return all_targets
    
    def apply_filters(self, users: List[UserShort]) -> List[UserShort]:
        """Применить фильтры к списку пользователей"""
        if not self.task.filters:
            return users
        
        filtered_users = []
        filters = self.task.filters
        
        for user in users:
            try:
                # Получаем полную информацию о пользователе для фильтрации
                if hasattr(user, 'is_private') and hasattr(user, 'follower_count'):
                    # У нас уже есть информация
                    user_info = user
                else:
                    # Нужно получить полную информацию
                    user_info = self.instagram_client.user_info(user.pk)
                
                # Применяем фильтры
                if filters.get('skip_private', False) and user_info.is_private:
                    self.task.skipped_count += 1
                    continue
                
                if filters.get('skip_no_avatar', False) and not user_info.profile_pic_url:
                    self.task.skipped_count += 1
                    continue
                
                if filters.get('only_business', False) and not getattr(user_info, 'is_business', False):
                    self.task.skipped_count += 1
                    continue
                
                # Фильтры по количеству подписчиков
                min_followers = filters.get('min_followers', 0)
                max_followers = filters.get('max_followers', float('inf'))
                
                if user_info.follower_count < min_followers or user_info.follower_count > max_followers:
                    self.task.skipped_count += 1
                    continue
                
                filtered_users.append(user)
                
            except Exception as e:
                logger.warning(f"Не удалось проверить пользователя {user.username}: {e}")
                continue
        
        logger.info(f"🔍 После фильтрации осталось {len(filtered_users)} из {len(users)} пользователей")
        return filtered_users
    
    def get_source_users(self) -> List[UserShort]:
        """Получить список пользователей из источника"""
        try:
            source_type = self.task.source_type
            source_value = self.task.source_value.strip().replace('@', '').replace('#', '')
            
            users = []
            
            if source_type == FollowSourceType.FOLLOWERS:
                # Получаем подписчиков аккаунта
                user_id = self.instagram_client.user_id_from_username(source_value)
                users = self.instagram_client.user_followers(user_id, amount=1000)
                
            elif source_type == FollowSourceType.FOLLOWING:
                # Получаем подписки аккаунта
                user_id = self.instagram_client.user_id_from_username(source_value)
                users = self.instagram_client.user_following(user_id, amount=1000)
                
            elif source_type == FollowSourceType.HASHTAG:
                # Получаем пользователей из постов по хештегу
                medias = self.instagram_client.hashtag_medias_recent(source_value, amount=50)
                user_ids_seen = set()
                for media in medias:
                    if media.user.pk not in user_ids_seen:
                        users.append(media.user)
                        user_ids_seen.add(media.user.pk)
                
            elif source_type == FollowSourceType.LOCATION:
                # Получаем пользователей из постов по локации
                # Сначала ищем локацию
                locations = self.instagram_client.fbsearch_places(source_value)
                if locations:
                    location_pk = locations[0].pk
                    medias = self.instagram_client.location_medias_recent(location_pk, amount=50)
                    user_ids_seen = set()
                    for media in medias:
                        if media.user.pk not in user_ids_seen:
                            users.append(media.user)
                            user_ids_seen.add(media.user.pk)
                
            elif source_type == FollowSourceType.LIKERS:
                # Получаем лайкнувших пост
                # Извлекаем media_id из URL
                media_pk = self.instagram_client.media_pk_from_url(source_value)
                users = self.instagram_client.media_likers(media_pk)
                
            elif source_type == FollowSourceType.COMMENTERS:
                # Получаем комментаторов поста
                media_pk = self.instagram_client.media_pk_from_url(source_value)
                comments = self.instagram_client.media_comments(media_pk)
                user_ids_seen = set()
                for comment in comments:
                    if comment.user.pk not in user_ids_seen:
                        users.append(comment.user)
                        user_ids_seen.add(comment.user.pk)
            
            logger.info(f"📥 Получено {len(users)} пользователей из источника {source_type.value}: {source_value}")
            return users
            
        except Exception as e:
            logger.error(f"Ошибка при получении пользователей из источника: {e}")
            return []
    
    def follow_user(self, user: UserShort) -> bool:
        """Подписаться на пользователя"""
        try:
            # Получаем полную информацию о пользователе через username
            user_info = self.instagram_client.user_info_by_username(user.username)
            
            # Проверяем, не подписаны ли уже
            if user_info.friendship_status.following:
                logger.info(f"ℹ️ Уже подписаны на @{user.username}")
                return False
            
            # Подписываемся
            result = self.instagram_client.user_follow(user.pk)
            
            if result:
                logger.info(f"✅ Подписались на @{user.username}")
                
                # Записываем в историю
                history = FollowHistory(
                    account_id=self.task.account_id,
                    target_user_id=str(user.pk),
                    target_username=user.username,
                    task_id=self.task.id
                )
                self.session.add(history)
                
                # Обновляем статистику задачи
                self.task.followed_count += 1
                self.task.last_action_at = datetime.now()
                
                # Добавляем в список обработанных
                if not self.task.processed_users:
                    self.task.processed_users = []
                self.task.processed_users.append(str(user.pk))
                
                self.session.commit()
                return True
            else:
                logger.warning(f"⚠️ Не удалось подписаться на @{user.username}")
                self.task.failed_count += 1
                return False
                
        except Exception as e:
            logger.error(f"❌ Ошибка при подписке на @{user.username}: {e}")
            self.task.failed_count += 1
            self.session.commit()
            return False
    
    def calculate_delay(self) -> int:
        """Рассчитать задержку между подписками"""
        # Базовая задержка исходя из скорости
        base_delay = 3600 / self.task.follows_per_hour
        
        # Добавляем случайность ±30%
        min_delay = int(base_delay * 0.7)
        max_delay = int(base_delay * 1.3)
        
        return random.randint(min_delay, max_delay)
    
    def should_continue(self) -> bool:
        """Проверить, нужно ли продолжать выполнение задачи"""
        # Проверяем статус
        self.session.refresh(self.task)
        if self.task.status != FollowTaskStatus.RUNNING:
            return False
        
        # Проверяем лимит
        if self.task.followed_count >= self.task.follow_limit:
            logger.info(f"🎯 Достигнут лимит подписок: {self.task.followed_count}/{self.task.follow_limit}")
            self.task.status = FollowTaskStatus.COMPLETED
            self.task.completed_at = datetime.now()
            self.session.commit()
            return False
        
        return True
    
    def run(self):
        """Основной цикл выполнения задачи"""
        try:
            logger.info(f"🚀 Запуск задачи автоподписки #{self.task.id}: {self.task.name}")
            
            # Инициализируем клиент
            if not self.initialize_client():
                self.task.status = FollowTaskStatus.FAILED
                self.task.error = "Не удалось инициализировать Instagram клиент"
                self.session.commit()
                return
            
            # Обновляем статус
            self.task.status = FollowTaskStatus.RUNNING
            self.task.started_at = datetime.now()
            self.session.commit()
            
            # Проверяем, является ли это пакетной подпиской
            if self.task.source_value == 'batch_follow' and self.task.filters:
                target_accounts = self.task.filters.get('target_accounts', [])
                if target_accounts:
                    logger.info(f"📋 Выполняем пакетную подписку на {len(target_accounts)} аккаунтов")
                    self.run_batch_follow(target_accounts)
                    return
            
            # Получаем пользователей из источника
            all_users = self.get_source_users()
            if not all_users:
                self.task.status = FollowTaskStatus.FAILED
                self.task.error = "Не удалось получить пользователей из источника"
                self.session.commit()
                return
            
            # Получаем уникальные цели
            unique_users = self.get_unique_targets(all_users)
            
            # Применяем фильтры
            filtered_users = self.apply_filters(unique_users)
            
            if not filtered_users:
                self.task.status = FollowTaskStatus.COMPLETED
                self.task.completed_at = datetime.now()
                self.task.error = "Нет подходящих пользователей после применения фильтров"
                self.session.commit()
                return
            
            # Перемешиваем список для случайности
            random.shuffle(filtered_users)
            
            # Выполняем подписки
            for user in filtered_users:
                if not self.should_continue():
                    break
                
                # Подписываемся
                success = self.follow_user(user)
                
                # Задержка между подписками
                if success:
                    delay = self.calculate_delay()
                    logger.info(f"⏱️ Ожидание {delay} секунд до следующей подписки...")
                    time.sleep(delay)
                else:
                    # Короткая задержка при ошибке
                    time.sleep(random.randint(5, 15))
            
            # Завершаем задачу
            if self.task.status == FollowTaskStatus.RUNNING:
                self.task.status = FollowTaskStatus.COMPLETED
                self.task.completed_at = datetime.now()
                self.session.commit()
            
            logger.info(f"✅ Задача завершена. Подписок: {self.task.followed_count}, Пропущено: {self.task.skipped_count}, Ошибок: {self.task.failed_count}")
            
        except Exception as e:
            logger.error(f"❌ Критическая ошибка в задаче: {e}")
            self.task.status = FollowTaskStatus.FAILED
            self.task.error = str(e)
            self.session.commit()
        finally:
            # Закрываем клиент
            if self.client:
                self.client.close()
    
    def run_batch_follow(self, target_accounts: List[str]):
        """Выполнить пакетную подписку на список аккаунтов"""
        try:
            # Применяем фильтр уникальности если нужно
            unique_follows = self.task.filters.get('unique_follows', True)
            
            for target_username in target_accounts:
                if not self.should_continue():
                    break
                
                try:
                    # Убираем @ если есть
                    username = target_username.strip().replace('@', '')
                    
                    # Получаем информацию о пользователе
                    user_info = self.instagram_client.user_info_by_username(username)
                    if not user_info:
                        logger.warning(f"❌ Пользователь @{username} не найден")
                        self.task.failed_count += 1
                        self.session.commit()
                        continue
                    
                    # Проверяем уникальность если нужно
                    if unique_follows:
                        existing = self.session.query(FollowHistory).filter_by(
                            account_id=self.task.account_id,
                            target_user_id=str(user_info.pk)
                        ).first()
                        
                        if existing:
                            logger.info(f"ℹ️ Уже подписаны на @{username} ранее")
                            self.task.skipped_count += 1
                            self.session.commit()
                            continue
                    
                    # Подписываемся
                    user_short = UserShort(
                        pk=user_info.pk,
                        username=user_info.username,
                        full_name=user_info.full_name,
                        profile_pic_url=user_info.profile_pic_url,
                        is_private=user_info.is_private,
                        is_verified=user_info.is_verified
                    )
                    
                    success = self.follow_user(user_short)
                    
                    # Задержка между подписками
                    if success:
                        delay_min = self.task.filters.get('delay_min', 30)
                        delay_max = self.task.filters.get('delay_max', 90)
                        delay = random.randint(delay_min, delay_max)
                        logger.info(f"⏱️ Ожидание {delay} секунд до следующей подписки...")
                        time.sleep(delay)
                    else:
                        # Короткая задержка при ошибке
                        time.sleep(random.randint(5, 15))
                        
                except Exception as e:
                    logger.error(f"❌ Ошибка при подписке на @{target_username}: {e}")
                    self.task.failed_count += 1
                    self.session.commit()
                    continue
            
            # Завершаем задачу
            if self.task.status == FollowTaskStatus.RUNNING:
                self.task.status = FollowTaskStatus.COMPLETED
                self.task.completed_at = datetime.now()
                self.session.commit()
            
            logger.info(f"✅ Пакетная подписка завершена. Подписок: {self.task.followed_count}, Пропущено: {self.task.skipped_count}, Ошибок: {self.task.failed_count}")
            
        except Exception as e:
            logger.error(f"❌ Критическая ошибка в пакетной подписке: {e}")
            self.task.status = FollowTaskStatus.FAILED
            self.task.error = str(e)
            self.session.commit()

    async def execute_follow_task(self, task: FollowTask):
        """Выполнить задачу автоподписки асинхронно"""
        try:
            logger.info(f"🚀 Начинаем асинхронное выполнение задачи #{task.id}: {task.name}")
            
            # Инициализируем клиент
            if not self.initialize_client():
                self.task.status = FollowTaskStatus.FAILED
                self.task.error = "Не удалось инициализировать Instagram клиент"
                self.session.commit()
                return
            
            # Обновляем статус
            self.task.status = FollowTaskStatus.RUNNING
            self.task.started_at = datetime.now()
            self.session.commit()
            
            # Проверяем, является ли это пакетной подпиской
            if self.task.source_value == 'batch_follow' and self.task.filters:
                target_accounts = self.task.filters.get('target_accounts', [])
                if target_accounts:
                    logger.info(f"📋 Выполняем асинхронную пакетную подписку на {len(target_accounts)} аккаунтов")
                    await self.async_run_batch_follow(target_accounts)
                    return
            
            # Получаем пользователей из источника
            all_users = await self.async_get_source_users()
            if not all_users:
                self.task.status = FollowTaskStatus.FAILED
                self.task.error = "Не удалось получить пользователей из источника"
                self.session.commit()
                return
            
            # Получаем уникальные цели
            unique_users = await self.async_get_unique_targets(all_users)
            
            # Применяем фильтры
            filtered_users = await self.async_apply_filters(unique_users)
            
            if not filtered_users:
                self.task.status = FollowTaskStatus.COMPLETED
                self.task.completed_at = datetime.now()
                self.task.error = "Нет подходящих пользователей после применения фильтров"
                self.session.commit()
                return
            
            # Перемешиваем список для случайности
            random.shuffle(filtered_users)
            
            # Выполняем подписки
            for user in filtered_users:
                if not await self.async_should_continue():
                    break
                
                # Подписываемся
                success = await self.async_follow_user(user)
                
                # Задержка между подписками
                if success:
                    delay = self.calculate_delay()
                    logger.info(f"⏱️ Ожидание {delay} секунд до следующей подписки...")
                    await asyncio.sleep(delay)
                else:
                    # Короткая задержка при ошибке
                    await asyncio.sleep(random.randint(5, 15))
            
            # Завершаем задачу
            if self.task.status == FollowTaskStatus.RUNNING:
                self.task.status = FollowTaskStatus.COMPLETED
                self.task.completed_at = datetime.now()
                self.session.commit()
            
            logger.info(f"✅ Задача завершена. Подписок: {self.task.followed_count}, Пропущено: {self.task.skipped_count}, Ошибок: {self.task.failed_count}")
            
        except Exception as e:
            logger.error(f"❌ Критическая ошибка в задаче: {e}")
            self.task.status = FollowTaskStatus.FAILED
            self.task.error = str(e)
            self.session.commit()
        finally:
            # Закрываем клиент
            if self.client:
                self.client.close()
    
    async def _run_in_executor(self, func, *args):
        """Выполнить функцию в отдельном потоке"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self._executor, func, *args)
    
    async def check_if_already_following(self, account_id: int, user_id: str) -> bool:
        """Проверить, подписан ли уже на пользователя"""
        try:
            # Проверяем в истории подписок
            existing = self.session.query(FollowHistory).filter_by(
                account_id=account_id,
                target_user_id=str(user_id)
            ).first()
            
            return existing is not None
        except Exception as e:
            logger.error(f"Ошибка при проверке подписки: {e}")
            return False
    
    async def save_follow_history(self, account_id: int, user_id: str, username: str, task_id: int):
        """Сохранить запись о подписке в историю"""
        try:
            history = FollowHistory(
                account_id=account_id,
                target_user_id=str(user_id),
                target_username=username,
                task_id=task_id,
                followed_at=datetime.now()
            )
            self.session.add(history)
            self.session.commit()
        except Exception as e:
            logger.error(f"Ошибка при сохранении истории: {e}")
    
    async def async_get_source_users(self) -> List[UserShort]:
        """Асинхронно получить список пользователей из источника"""
        try:
            source_type = self.task.source_type
            source_value = self.task.source_value.strip().replace('@', '').replace('#', '')
            
            users = []
            
            if source_type == FollowSourceType.FOLLOWERS:
                # Получаем подписчиков аккаунта
                user_id = await self._run_in_executor(
                    self.instagram_client.user_id_from_username, source_value
                )
                users = await self._run_in_executor(
                    self.instagram_client.user_followers, user_id, amount=1000
                )
                
            elif source_type == FollowSourceType.FOLLOWING:
                # Получаем подписки аккаунта
                user_id = await self._run_in_executor(
                    self.instagram_client.user_id_from_username, source_value
                )
                users = await self._run_in_executor(
                    self.instagram_client.user_following, user_id, amount=1000
                )
                
            elif source_type == FollowSourceType.HASHTAG:
                # Получаем пользователей из постов по хештегу
                medias = await self._run_in_executor(
                    self.instagram_client.hashtag_medias_recent, source_value, amount=50
                )
                user_ids_seen = set()
                for media in medias:
                    if media.user.pk not in user_ids_seen:
                        users.append(media.user)
                        user_ids_seen.add(media.user.pk)
                
            elif source_type == FollowSourceType.LOCATION:
                # Получаем пользователей из постов по локации
                locations = await self._run_in_executor(
                    self.instagram_client.fbsearch_places, source_value
                )
                if locations:
                    location_pk = locations[0].pk
                    medias = await self._run_in_executor(
                        self.instagram_client.location_medias_recent, location_pk, amount=50
                    )
                    user_ids_seen = set()
                    for media in medias:
                        if media.user.pk not in user_ids_seen:
                            users.append(media.user)
                            user_ids_seen.add(media.user.pk)
                
            elif source_type == FollowSourceType.LIKERS:
                # Получаем лайкнувших пост
                media_pk = await self._run_in_executor(
                    self.instagram_client.media_pk_from_url, source_value
                )
                users = await self._run_in_executor(
                    self.instagram_client.media_likers, media_pk
                )
                
            elif source_type == FollowSourceType.COMMENTERS:
                # Получаем комментаторов поста
                media_pk = await self._run_in_executor(
                    self.instagram_client.media_pk_from_url, source_value
                )
                comments = await self._run_in_executor(
                    self.instagram_client.media_comments, media_pk
                )
                user_ids_seen = set()
                for comment in comments:
                    if comment.user.pk not in user_ids_seen:
                        users.append(comment.user)
                        user_ids_seen.add(comment.user.pk)
            
            logger.info(f"📥 Получено {len(users)} пользователей из источника {source_type.value}: {source_value}")
            return users
            
        except Exception as e:
            logger.error(f"Ошибка при получении пользователей из источника: {e}")
            return []
    
    async def async_get_unique_targets(self, all_targets: List[UserShort]) -> List[UserShort]:
        """Асинхронно получить уникальные цели для подписки"""
        return await self._run_in_executor(self.get_unique_targets, all_targets)
    
    async def async_apply_filters(self, users: List[UserShort]) -> List[UserShort]:
        """Асинхронно применить фильтры к списку пользователей"""
        return await self._run_in_executor(self.apply_filters, users)
    
    async def async_should_continue(self) -> bool:
        """Асинхронно проверить, нужно ли продолжать выполнение задачи"""
        return await self._run_in_executor(self.should_continue)
    
    async def async_follow_user(self, user: UserShort) -> bool:
        """Асинхронно подписаться на пользователя"""
        try:
            # Получаем полную информацию о пользователе через username
            user_info = await self._run_in_executor(
                self.instagram_client.user_info_by_username, user.username
            )
            
            # Проверяем, не подписаны ли уже
            if user_info.friendship_status.following:
                logger.info(f"ℹ️ Уже подписаны на @{user.username}")
                return False
            
            # Подписываемся
            result = await self._run_in_executor(
                self.instagram_client.user_follow, user.pk
            )
            
            if result:
                logger.info(f"✅ Подписались на @{user.username}")
                
                # Записываем в историю
                history = FollowHistory(
                    account_id=self.task.account_id,
                    target_user_id=str(user.pk),
                    target_username=user.username,
                    task_id=self.task.id
                )
                self.session.add(history)
                
                # Обновляем статистику задачи
                self.task.followed_count += 1
                self.task.last_action_at = datetime.now()
                
                # Добавляем в список обработанных
                if not self.task.processed_users:
                    self.task.processed_users = []
                self.task.processed_users.append(str(user.pk))
                
                self.session.commit()
                return True
            else:
                logger.warning(f"⚠️ Не удалось подписаться на @{user.username}")
                self.task.failed_count += 1
                return False
                
        except Exception as e:
            logger.error(f"❌ Ошибка при подписке на @{user.username}: {e}")
            self.task.failed_count += 1
            self.session.commit()
            return False
    
    async def async_run_batch_follow(self, target_accounts: List[str]):
        """Асинхронно выполнить пакетную подписку на список аккаунтов"""
        try:
            # Применяем фильтр уникальности если нужно
            unique_follows = self.task.filters.get('unique_follows', True)
            
            for target_username in target_accounts:
                if not await self.async_should_continue():
                    break
                
                try:
                    # Убираем @ если есть
                    username = target_username.strip().replace('@', '')
                    
                    # Получаем информацию о пользователе
                    user_info = await self._run_in_executor(
                        self.instagram_client.user_info_by_username, username
                    )
                    if not user_info:
                        logger.warning(f"❌ Пользователь @{username} не найден")
                        self.task.failed_count += 1
                        self.session.commit()
                        continue
                    
                    # Проверяем уникальность если нужно
                    if unique_follows:
                        existing = await self._run_in_executor(
                            lambda: self.session.query(FollowHistory).filter_by(
                                account_id=self.task.account_id,
                                target_user_id=str(user_info.pk)
                            ).first()
                        )
                        
                        if existing:
                            logger.info(f"ℹ️ Уже подписаны на @{username} ранее")
                            self.task.skipped_count += 1
                            self.session.commit()
                            continue
                    
                    # Подписываемся
                    user_short = UserShort(
                        pk=user_info.pk,
                        username=user_info.username,
                        full_name=user_info.full_name,
                        profile_pic_url=user_info.profile_pic_url,
                        is_private=user_info.is_private,
                        is_verified=user_info.is_verified
                    )
                    
                    success = await self.async_follow_user(user_short)
                    
                    # Задержка между подписками
                    if success:
                        delay_min = self.task.filters.get('delay_min', 30)
                        delay_max = self.task.filters.get('delay_max', 90)
                        delay = random.randint(delay_min, delay_max)
                        logger.info(f"⏱️ Ожидание {delay} секунд до следующей подписки...")
                        await asyncio.sleep(delay)
                    else:
                        # Короткая задержка при ошибке
                        await asyncio.sleep(random.randint(5, 15))
                        
                except Exception as e:
                    logger.error(f"❌ Ошибка при подписке на @{target_username}: {e}")
                    self.task.failed_count += 1
                    self.session.commit()
                    continue
            
            # Завершаем задачу
            if self.task.status == FollowTaskStatus.RUNNING:
                self.task.status = FollowTaskStatus.COMPLETED
                self.task.completed_at = datetime.now()
                self.session.commit()
            
            logger.info(f"✅ Пакетная подписка завершена. Подписок: {self.task.followed_count}, Пропущено: {self.task.skipped_count}, Ошибок: {self.task.failed_count}")
            
        except Exception as e:
            logger.error(f"❌ Критическая ошибка в пакетной подписке: {e}")
            self.task.status = FollowTaskStatus.FAILED
            self.task.error = str(e)
            self.session.commit() 
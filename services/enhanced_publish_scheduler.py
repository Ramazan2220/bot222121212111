import logging
import asyncio
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, asdict
from enum import Enum
import uuid

from database.db_manager import get_session, get_instagram_account
from database.models import PublishTask, TaskStatus, ContentType
from services.enhanced_account_automation import get_enhanced_automation
from instagram.ml_health_predictor import get_ml_health_predictor

logger = logging.getLogger(__name__)

class ScheduleStrategy(Enum):
    IMMEDIATE = "immediate"
    OPTIMAL_TIME = "optimal_time"  
    CUSTOM_TIME = "custom_time"
    BATCH_PROCESSING = "batch_processing"
    SMART_DISTRIBUTION = "smart_distribution"

@dataclass
class ContentItem:
    """Элемент контента для публикации"""
    id: str
    content_type: ContentType
    media_path: str
    caption: str
    hashtags: List[str] = None
    priority: int = 5  # 1-10, где 10 - наивысший
    target_accounts: List[int] = None
    optimal_time: datetime = None
    created_at: datetime = None
    
    def __post_init__(self):
        if self.hashtags is None:
            self.hashtags = []
        if self.target_accounts is None:
            self.target_accounts = []
        if self.created_at is None:
            self.created_at = datetime.now()
        if self.id is None:
            self.id = str(uuid.uuid4())

@dataclass 
class PublishCampaign:
    """Кампания публикаций"""
    id: str
    name: str
    content_items: List[ContentItem]
    schedule_strategy: ScheduleStrategy
    start_time: datetime
    end_time: datetime = None
    account_groups: List[str] = None
    auto_retry: bool = True
    max_retries: int = 3
    created_by: int = None
    status: str = "draft"
    
    def __post_init__(self):
        if self.account_groups is None:
            self.account_groups = []
        if self.id is None:
            self.id = str(uuid.uuid4())

class EnhancedPublishScheduler:
    """Улучшенная система планирования публикаций"""
    
    def __init__(self):
        self.ml_predictor = get_ml_health_predictor()
        self.automation = get_enhanced_automation()
        self.active_campaigns = {}
        self.content_calendar = {}
        
        logger.info("🚀 Инициализирована улучшенная система планирования публикаций")
    
    async def create_publish_campaign(self, campaign: PublishCampaign) -> Tuple[bool, str]:
        """Создает кампанию публикаций"""
        try:
            logger.info(f"📅 Создание кампании '{campaign.name}' с {len(campaign.content_items)} элементами контента")
            
            # Валидация кампании
            validation_result = self._validate_campaign(campaign)
            if not validation_result[0]:
                return False, validation_result[1]
            
            # Оптимизируем время публикации если нужно
            if campaign.schedule_strategy == ScheduleStrategy.OPTIMAL_TIME:
                await self._optimize_campaign_timing(campaign)
            elif campaign.schedule_strategy == ScheduleStrategy.SMART_DISTRIBUTION:
                await self._smart_distribute_content(campaign)
            
            # Создаем задачи в базе данных
            task_ids = await self._create_publish_tasks(campaign)
            
            if task_ids:
                # Сохраняем кампанию
                self.active_campaigns[campaign.id] = campaign
                campaign.status = "active"
                
                # Добавляем в контент-календарь
                self._add_to_content_calendar(campaign)
                
                logger.info(f"✅ Кампания '{campaign.name}' создана: {len(task_ids)} задач")
                return True, f"Создано {len(task_ids)} задач для публикации"
            else:
                return False, "Не удалось создать задачи публикации"
                
        except Exception as e:
            logger.error(f"❌ Ошибка создания кампании: {e}")
            return False, str(e)
    
    def _validate_campaign(self, campaign: PublishCampaign) -> Tuple[bool, str]:
        """Валидирует кампанию"""
        if not campaign.name:
            return False, "Не указано название кампании"
        
        if not campaign.content_items:
            return False, "Нет элементов контента для публикации"
        
        if campaign.start_time <= datetime.now():
            return False, "Время начала должно быть в будущем"
        
        if campaign.end_time and campaign.end_time <= campaign.start_time:
            return False, "Время окончания должно быть после времени начала"
        
        # Проверяем контент
        for item in campaign.content_items:
            if not item.media_path:
                return False, f"Не указан путь к медиа для элемента {item.id}"
            
            if not item.target_accounts:
                return False, f"Не указаны целевые аккаунты для элемента {item.id}"
        
        return True, "Валидация прошла успешно"
    
    async def _optimize_campaign_timing(self, campaign: PublishCampaign):
        """Оптимизирует время публикации на основе ML анализа"""
        try:
            logger.info(f"🎯 Оптимизация времени для кампании '{campaign.name}'")
            
            # Анализируем все целевые аккаунты
            all_accounts = set()
            for item in campaign.content_items:
                all_accounts.update(item.target_accounts)
            
            account_schedules = {}
            
            # Получаем оптимальное время для каждого аккаунта
            for account_id in all_accounts:
                optimal_times = await self._get_optimal_posting_times(account_id)
                account_schedules[account_id] = optimal_times
            
            # Распределяем контент по оптимальному времени
            for item in campaign.content_items:
                best_time = self._find_best_time_for_accounts(
                    item.target_accounts, 
                    account_schedules,
                    campaign.start_time,
                    campaign.end_time
                )
                item.optimal_time = best_time
                
            logger.info(f"✅ Оптимизация времени завершена для {len(campaign.content_items)} элементов")
            
        except Exception as e:
            logger.error(f"❌ Ошибка оптимизации времени: {e}")
    
    async def _get_optimal_posting_times(self, account_id: int) -> List[datetime]:
        """Получает оптимальное время для публикации для аккаунта"""
        try:
            # ML анализ для определения лучшего времени
            prediction = self.ml_predictor.predict_account_health(account_id)
            
            optimal_times = []
            base_time = datetime.now().replace(hour=9, minute=0, second=0, microsecond=0)
            
            # Генерируем оптимальные слоты на основе health score
            if prediction.health_score > 80:
                # Здоровые аккаунты - можем публиковать чаще
                for day in range(7):
                    day_base = base_time + timedelta(days=day)
                    optimal_times.extend([
                        day_base.replace(hour=9),   # Утро
                        day_base.replace(hour=13),  # Обед
                        day_base.replace(hour=18),  # Вечер
                        day_base.replace(hour=21)   # Ночь
                    ])
            elif prediction.health_score > 60:
                # Средние аккаунты - умеренно
                for day in range(7):
                    day_base = base_time + timedelta(days=day)
                    optimal_times.extend([
                        day_base.replace(hour=10),  # Утро
                        day_base.replace(hour=15),  # День
                        day_base.replace(hour=19)   # Вечер
                    ])
            else:
                # Слабые аккаунты - осторожно
                for day in range(7):
                    day_base = base_time + timedelta(days=day)
                    optimal_times.append(day_base.replace(hour=14))  # Только день
            
            # Фильтруем время в будущем
            now = datetime.now()
            optimal_times = [t for t in optimal_times if t > now]
            
            return optimal_times[:20]  # Возвращаем первые 20 слотов
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения оптимального времени для аккаунта {account_id}: {e}")
            return []
    
    def _find_best_time_for_accounts(self, account_ids: List[int], account_schedules: Dict, start_time: datetime, end_time: datetime = None) -> datetime:
        """Находит лучшее время для публикации на всех аккаунтах"""
        try:
            if end_time is None:
                end_time = start_time + timedelta(days=7)
            
            # Собираем все возможные времена
            all_times = []
            for account_id in account_ids:
                if account_id in account_schedules:
                    account_times = account_schedules[account_id]
                    # Фильтруем по временному диапазону
                    filtered_times = [t for t in account_times if start_time <= t <= end_time]
                    all_times.extend(filtered_times)
            
            if not all_times:
                # Fallback - используем стартовое время
                return start_time
            
            # Находим время, которое подходит большинству аккаунтов
            time_counts = {}
            for time in all_times:
                # Округляем до часа для группировки
                hour_time = time.replace(minute=0, second=0, microsecond=0)
                time_counts[hour_time] = time_counts.get(hour_time, 0) + 1
            
            # Возвращаем время с наибольшим количеством совпадений
            best_time = max(time_counts.items(), key=lambda x: x[1])[0]
            return best_time
            
        except Exception as e:
            logger.error(f"❌ Ошибка поиска лучшего времени: {e}")
            return start_time
    
    async def _smart_distribute_content(self, campaign: PublishCampaign):
        """Умно распределяет контент по времени"""
        try:
            logger.info(f"🧠 Умное распределение {len(campaign.content_items)} элементов контента")
            
            # Сортируем контент по приоритету
            campaign.content_items.sort(key=lambda x: x.priority, reverse=True)
            
            current_time = campaign.start_time
            time_interval = timedelta(hours=2)  # Интервал между публикациями
            
            if campaign.end_time:
                total_duration = campaign.end_time - campaign.start_time
                time_interval = total_duration / len(campaign.content_items)
            
            for i, item in enumerate(campaign.content_items):
                # Добавляем некоторую случайность (±30 минут)
                random_offset = timedelta(minutes=random.randint(-30, 30))
                item.optimal_time = current_time + random_offset
                
                current_time += time_interval
                
                logger.debug(f"📅 Элемент {item.id}: {item.optimal_time}")
            
            logger.info(f"✅ Распределение завершено")
            
        except Exception as e:
            logger.error(f"❌ Ошибка умного распределения: {e}")
    
    async def _create_publish_tasks(self, campaign: PublishCampaign) -> List[str]:
        """Создает задачи публикации в базе данных"""
        try:
            session = get_session()
            task_ids = []
            
            for item in campaign.content_items:
                publish_time = item.optimal_time or campaign.start_time
                
                for account_id in item.target_accounts:
                    # Проверяем здоровье аккаунта перед созданием задачи
                    prediction = self.ml_predictor.predict_account_health(account_id)
                    
                    if prediction.ban_risk_score > 80:
                        logger.warning(f"⚠️ Пропуск аккаунта {account_id} из-за высокого риска бана ({prediction.ban_risk_score:.1f}%)")
                        continue
                    
                    task = PublishTask(
                        account_id=account_id,
                        content_type=item.content_type,
                        media_path=item.media_path,
                        caption=item.caption,
                        hashtags=json.dumps(item.hashtags) if item.hashtags else None,
                        scheduled_time=publish_time,
                        status=TaskStatus.PENDING,
                        created_by=campaign.created_by,
                        campaign_id=campaign.id,
                        priority=item.priority,
                        max_retries=campaign.max_retries if campaign.auto_retry else 0
                    )
                    
                    session.add(task)
                    session.flush()
                    task_ids.append(str(task.id))
            
            session.commit()
            session.close()
            
            logger.info(f"💾 Создано {len(task_ids)} задач публикации")
            return task_ids
            
        except Exception as e:
            logger.error(f"❌ Ошибка создания задач публикации: {e}")
            session.rollback()
            session.close()
            return []
    
    def _add_to_content_calendar(self, campaign: PublishCampaign):
        """Добавляет кампанию в контент-календарь"""
        try:
            for item in campaign.content_items:
                publish_time = item.optimal_time or campaign.start_time
                date_key = publish_time.date().isoformat()
                
                if date_key not in self.content_calendar:
                    self.content_calendar[date_key] = []
                
                calendar_entry = {
                    'campaign_id': campaign.id,
                    'campaign_name': campaign.name,
                    'content_id': item.id,
                    'content_type': item.content_type.value,
                    'scheduled_time': publish_time.isoformat(),
                    'accounts_count': len(item.target_accounts),
                    'priority': item.priority
                }
                
                self.content_calendar[date_key].append(calendar_entry)
            
            logger.info(f"📅 Кампания '{campaign.name}' добавлена в контент-календарь")
            
        except Exception as e:
            logger.error(f"❌ Ошибка добавления в календарь: {e}")
    
    async def batch_schedule_posts(self, content_batch: List[ContentItem], account_ids: List[int], start_time: datetime, interval_hours: int = 2) -> Tuple[bool, str]:
        """Пакетное планирование постов"""
        try:
            logger.info(f"📦 Пакетное планирование: {len(content_batch)} постов для {len(account_ids)} аккаунтов")
            
            # Создаем кампанию для пакета
            campaign = PublishCampaign(
                id=str(uuid.uuid4()),
                name=f"Batch Campaign {datetime.now().strftime('%Y%m%d_%H%M%S')}",
                content_items=content_batch,
                schedule_strategy=ScheduleStrategy.BATCH_PROCESSING,
                start_time=start_time
            )
            
            # Назначаем аккаунты всем элементам контента
            for item in campaign.content_items:
                if not item.target_accounts:
                    item.target_accounts = account_ids.copy()
            
            # Распределяем время с интервалами
            current_time = start_time
            for item in campaign.content_items:
                item.optimal_time = current_time
                current_time += timedelta(hours=interval_hours)
            
            # Создаем кампанию
            success, message = await self.create_publish_campaign(campaign)
            
            if success:
                return True, f"Пакет из {len(content_batch)} постов запланирован успешно"
            else:
                return False, f"Ошибка планирования пакета: {message}"
                
        except Exception as e:
            logger.error(f"❌ Ошибка пакетного планирования: {e}")
            return False, str(e)
    
    def get_content_calendar(self, start_date: datetime = None, end_date: datetime = None) -> Dict[str, List]:
        """Получает контент-календарь за период"""
        try:
            if start_date is None:
                start_date = datetime.now().date()
            if end_date is None:
                end_date = start_date + timedelta(days=7)
            
            filtered_calendar = {}
            
            for date_str, entries in self.content_calendar.items():
                date_obj = datetime.fromisoformat(date_str).date()
                if start_date <= date_obj <= end_date:
                    filtered_calendar[date_str] = entries
            
            return filtered_calendar
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения календаря: {e}")
            return {}
    
    async def reschedule_failed_tasks(self) -> Dict[str, int]:
        """Перепланирует неудачные задачи"""
        try:
            session = get_session()
            
            # Получаем неудачные задачи
            failed_tasks = session.query(PublishTask).filter(
                PublishTask.status == TaskStatus.FAILED,
                PublishTask.retry_count < PublishTask.max_retries
            ).all()
            
            results = {'rescheduled': 0, 'skipped': 0}
            
            for task in failed_tasks:
                # ML анализ аккаунта перед повторной попыткой
                prediction = self.ml_predictor.predict_account_health(task.account_id)
                
                if prediction.ban_risk_score < 60:  # Безопасно повторить
                    # Планируем через 1-3 часа
                    new_time = datetime.now() + timedelta(hours=random.randint(1, 3))
                    task.scheduled_time = new_time
                    task.status = TaskStatus.PENDING
                    task.retry_count += 1
                    results['rescheduled'] += 1
                    
                    logger.info(f"🔄 Задача {task.id} перепланирована на {new_time}")
                else:
                    results['skipped'] += 1
                    logger.warning(f"⚠️ Задача {task.id} пропущена из-за риска аккаунта")
            
            session.commit()
            session.close()
            
            logger.info(f"✅ Перепланировано: {results['rescheduled']}, пропущено: {results['skipped']}")
            return results
            
        except Exception as e:
            logger.error(f"❌ Ошибка перепланирования: {e}")
            session.rollback()
            session.close()
            return {'rescheduled': 0, 'skipped': 0, 'error': str(e)}
    
    def get_campaign_status(self, campaign_id: str) -> Dict[str, any]:
        """Получает статус кампании"""
        try:
            session = get_session()
            
            # Получаем задачи кампании
            tasks = session.query(PublishTask).filter(
                PublishTask.campaign_id == campaign_id
            ).all()
            
            session.close()
            
            if not tasks:
                return {'error': 'Кампания не найдена'}
            
            status = {
                'campaign_id': campaign_id,
                'total_tasks': len(tasks),
                'pending': sum(1 for t in tasks if t.status == TaskStatus.PENDING),
                'processing': sum(1 for t in tasks if t.status == TaskStatus.PROCESSING),
                'completed': sum(1 for t in tasks if t.status == TaskStatus.COMPLETED),
                'failed': sum(1 for t in tasks if t.status == TaskStatus.FAILED),
                'progress_percentage': 0
            }
            
            if status['total_tasks'] > 0:
                status['progress_percentage'] = (status['completed'] / status['total_tasks']) * 100
            
            return status
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения статуса кампании: {e}")
            return {'error': str(e)}

# Простой random для совместимости 
import random

# Функция для получения экземпляра планировщика
def get_enhanced_publish_scheduler():
    """Возвращает экземпляр улучшенного планировщика публикаций"""
    return EnhancedPublishScheduler() 
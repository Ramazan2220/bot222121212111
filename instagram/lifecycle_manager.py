import logging
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
from database.db_manager import get_instagram_account

logger = logging.getLogger(__name__)

class AccountLifecycleManager:
    """Управление жизненным циклом аккаунтов"""
    
    STAGES = {
        'NEW': 'Новый аккаунт',
        'WARMING': 'Прогревающийся',
        'ACTIVE': 'Активный',
        'MATURE': 'Зрелый',
        'RESTRICTED': 'Ограниченный'
    }
    
    def __init__(self):
        self.stage_cache = {}
    
    def determine_account_stage(self, account_id: int) -> str:
        """Определение этапа развития аккаунта"""
        try:
            account = get_instagram_account(account_id)
            if not account:
                return 'UNKNOWN'
            
            # Вычисляем возраст аккаунта
            account_age = datetime.now() - account.created_at
            age_days = account_age.days
            
            # Определяем этап на основе возраста и активности
            if not account.is_active:
                stage = 'RESTRICTED'
            elif age_days < 3:
                stage = 'NEW'
            elif age_days < 14:
                stage = 'WARMING'
            elif age_days < 90:
                stage = 'ACTIVE'
            else:
                stage = 'MATURE'
            
            # Кэшируем результат
            self.stage_cache[account_id] = {
                'stage': stage,
                'determined_at': datetime.now(),
                'age_days': age_days
            }
            
            logger.info(f"Определен этап аккаунта {account.username}: {stage} (возраст: {age_days} дней)")
            return stage
            
        except Exception as e:
            logger.error(f"Ошибка определения этапа для аккаунта {account_id}: {e}")
            return 'UNKNOWN'
    
    def get_stage_recommendations(self, stage: str) -> Dict[str, any]:
        """Получение рекомендаций действий для этапа"""
        try:
            recommendations = {
                'NEW': {
                    'description': 'Новый аккаунт требует осторожного начала',
                    'daily_actions': {
                        'follows': '5-10',
                        'likes': '10-20',
                        'comments': '0-2',
                        'stories_views': '5-15'
                    },
                    'recommended_actions': [
                        'Заполнить профиль полностью',
                        'Загрузить аватар',
                        'Опубликовать 1-2 поста',
                        'Минимальная активность'
                    ],
                    'avoid_actions': [
                        'Массовые подписки',
                        'Частые лайки',
                        'Автоматизация'
                    ],
                    'duration': '3-7 дней'
                },
                'WARMING': {
                    'description': 'Постепенное увеличение активности',
                    'daily_actions': {
                        'follows': '10-25',
                        'likes': '20-50',
                        'comments': '2-8',
                        'stories_views': '15-40'
                    },
                    'recommended_actions': [
                        'Регулярные посты (через день)',
                        'Взаимодействие с подписчиками',
                        'Просмотр ленты',
                        'Stories активность'
                    ],
                    'avoid_actions': [
                        'Резкие скачки активности',
                        'Одинаковые интервалы действий'
                    ],
                    'duration': '7-14 дней'
                },
                'ACTIVE': {
                    'description': 'Полноценная активность с ограничениями',
                    'daily_actions': {
                        'follows': '25-75',
                        'likes': '50-150',
                        'comments': '8-25',
                        'stories_views': '40-100'
                    },
                    'recommended_actions': [
                        'Регулярные публикации',
                        'Активное взаимодействие',
                        'Использование Stories',
                        'Reels публикации'
                    ],
                    'avoid_actions': [
                        'Превышение лимитов',
                        'Спам-активность'
                    ],
                    'duration': '14-90 дней'
                },
                'MATURE': {
                    'description': 'Зрелый аккаунт с максимальными возможностями',
                    'daily_actions': {
                        'follows': '50-200',
                        'likes': '100-500',
                        'comments': '15-50',
                        'stories_views': '100-300'
                    },
                    'recommended_actions': [
                        'Полная автоматизация',
                        'Массовые кампании',
                        'Активная монетизация',
                        'Использование всех функций'
                    ],
                    'avoid_actions': [
                        'Резкие изменения поведения'
                    ],
                    'duration': 'Постоянно'
                },
                'RESTRICTED': {
                    'description': 'Аккаунт с ограничениями - восстановление',
                    'daily_actions': {
                        'follows': '0-5',
                        'likes': '5-15',
                        'comments': '0-2',
                        'stories_views': '5-20'
                    },
                    'recommended_actions': [
                        'Минимальная активность',
                        'Ожидание снятия ограничений',
                        'Качественный контент',
                        'Ручные действия'
                    ],
                    'avoid_actions': [
                        'Любая автоматизация',
                        'Массовые действия',
                        'Частая активность'
                    ],
                    'duration': 'До снятия ограничений'
                }
            }
            
            return recommendations.get(stage, {
                'description': 'Неизвестный этап',
                'daily_actions': {},
                'recommended_actions': [],
                'avoid_actions': [],
                'duration': 'Неопределено'
            })
            
        except Exception as e:
            logger.error(f"Ошибка получения рекомендаций для этапа {stage}: {e}")
            return {}
    
    def plan_stage_transition(self, account_id: int) -> Dict[str, any]:
        """Планирование перехода между этапами"""
        try:
            current_stage = self.determine_account_stage(account_id)
            account = get_instagram_account(account_id)
            
            if not account:
                return {}
            
            account_age = datetime.now() - account.created_at
            age_days = account_age.days
            
            transition_plan = {
                'current_stage': current_stage,
                'current_age_days': age_days,
                'next_stage': None,
                'transition_date': None,
                'days_until_transition': None,
                'preparation_actions': []
            }
            
            # Определяем следующий этап и время перехода
            if current_stage == 'NEW':
                transition_plan.update({
                    'next_stage': 'WARMING',
                    'transition_date': account.created_at + timedelta(days=7),
                    'days_until_transition': max(0, 7 - age_days),
                    'preparation_actions': [
                        'Заполнить все поля профиля',
                        'Загрузить качественный аватар',
                        'Опубликовать первые посты',
                        'Настроить приватность'
                    ]
                })
            elif current_stage == 'WARMING':
                transition_plan.update({
                    'next_stage': 'ACTIVE',
                    'transition_date': account.created_at + timedelta(days=14),
                    'days_until_transition': max(0, 14 - age_days),
                    'preparation_actions': [
                        'Увеличить частоту публикаций',
                        'Начать Stories активность',
                        'Расширить сеть подписок',
                        'Улучшить взаимодействие'
                    ]
                })
            elif current_stage == 'ACTIVE':
                transition_plan.update({
                    'next_stage': 'MATURE',
                    'transition_date': account.created_at + timedelta(days=90),
                    'days_until_transition': max(0, 90 - age_days),
                    'preparation_actions': [
                        'Стабилизировать активность',
                        'Нарастить аудиторию',
                        'Оптимизировать контент-стратегию',
                        'Подготовить к автоматизации'
                    ]
                })
            elif current_stage == 'MATURE':
                transition_plan.update({
                    'next_stage': 'MATURE',
                    'transition_date': None,
                    'days_until_transition': 0,
                    'preparation_actions': [
                        'Поддерживать активность',
                        'Мониторить метрики',
                        'Оптимизировать процессы'
                    ]
                })
            elif current_stage == 'RESTRICTED':
                transition_plan.update({
                    'next_stage': 'WARMING',
                    'transition_date': None,  # Зависит от снятия ограничений
                    'days_until_transition': None,
                    'preparation_actions': [
                        'Минимизировать активность',
                        'Дождаться снятия ограничений',
                        'Анализировать причины блокировки',
                        'Подготовить стратегию восстановления'
                    ]
                })
            
            logger.info(f"План перехода для аккаунта {account.username}: {current_stage} -> {transition_plan['next_stage']}")
            return transition_plan
            
        except Exception as e:
            logger.error(f"Ошибка планирования перехода для аккаунта {account_id}: {e}")
            return {}
    
    def execute_stage_actions(self, account_id: int) -> Tuple[bool, str]:
        """Выполнение действий для текущего этапа"""
        try:
            current_stage = self.determine_account_stage(account_id)
            recommendations = self.get_stage_recommendations(current_stage)
            
            if not recommendations:
                return False, "Не удалось получить рекомендации"
            
            account = get_instagram_account(account_id)
            if not account:
                return False, "Аккаунт не найден"
            
            # В реальной реализации здесь были бы вызовы конкретных действий
            # на основе рекомендаций для этапа
            
            actions_log = []
            
            # Симуляция выполнения рекомендованных действий
            for action in recommendations.get('recommended_actions', []):
                try:
                    # Здесь был бы вызов соответствующей функции
                    success = self._simulate_action_execution(action, account_id)
                    if success:
                        actions_log.append(f"✅ {action}")
                    else:
                        actions_log.append(f"❌ {action}")
                except Exception as e:
                    actions_log.append(f"❌ {action}: {e}")
            
            result_message = f"Выполнены действия для этапа {current_stage}:\n" + "\n".join(actions_log)
            
            logger.info(f"Выполнение действий для аккаунта {account.username} на этапе {current_stage} завершено")
            return True, result_message
            
        except Exception as e:
            error_msg = f"Ошибка выполнения действий для аккаунта {account_id}: {e}"
            logger.error(error_msg)
            return False, error_msg
    
    def get_all_accounts_stages(self) -> Dict[str, List[Dict]]:
        """Получение этапов всех аккаунтов"""
        try:
            from database.db_manager import get_instagram_accounts
            accounts = get_instagram_accounts()
            
            stages_distribution = {stage: [] for stage in self.STAGES.keys()}
            
            for account in accounts:
                stage = self.determine_account_stage(account.id)
                account_age = datetime.now() - account.created_at
                
                stages_distribution[stage].append({
                    'id': account.id,
                    'username': account.username,
                    'age_days': account_age.days,
                    'is_active': account.is_active
                })
            
            logger.info(f"Распределение по этапам: {[(stage, len(accounts)) for stage, accounts in stages_distribution.items()]}")
            return stages_distribution
            
        except Exception as e:
            logger.error(f"Ошибка получения этапов всех аккаунтов: {e}")
            return {}
    
    def _simulate_action_execution(self, action: str, account_id: int) -> bool:
        """Симуляция выполнения действия (для демонстрации)"""
        # В реальной реализации здесь были бы вызовы реальных функций
        import time
        time.sleep(0.1)  # Имитация работы
        return True  # Симуляция успешного выполнения
    
    def clear_cache(self, account_id: Optional[int] = None):
        """Очистка кэша этапов"""
        if account_id and account_id in self.stage_cache:
            del self.stage_cache[account_id]
        else:
            self.stage_cache.clear()
        
        logger.info(f"Кэш lifecycle manager очищен для {'всех аккаунтов' if not account_id else f'аккаунта {account_id}'}") 
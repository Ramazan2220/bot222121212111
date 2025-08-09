import logging
import random
import time
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
from database.db_manager import get_instagram_account
from instagram.client import get_instagram_client

logger = logging.getLogger(__name__)

class ImprovedAccountWarmer:
    """Улучшенная система прогрева аккаунтов"""
    
    def __init__(self):
        self.warming_sessions = {}
    
    def warm_account_improved(self, account_id: int, duration_minutes: int = 30) -> Tuple[bool, str]:
        """Основная функция прогрева с адаптивными настройками"""
        try:
            account = get_instagram_account(account_id)
            if not account:
                return False, "Аккаунт не найден"
            
            logger.info(f"Начинаю улучшенный прогрев аккаунта {account.username} на {duration_minutes} минут")
            
            # Получаем клиент Instagram
            client = get_instagram_client(account_id)
            if not client:
                return False, "Не удалось получить клиент Instagram"
            
            # Определяем стратегию прогрева на основе возраста аккаунта
            strategy = self._determine_warming_strategy(account_id)
            
            # Инициализируем сессию прогрева
            session_id = f"{account_id}_{int(time.time())}"
            self.warming_sessions[session_id] = {
                'account_id': account_id,
                'start_time': time.time(),
                'duration': duration_minutes * 60,
                'strategy': strategy,
                'actions_performed': [],
                'errors': []
            }
            
            total_actions = 0
            successful_actions = 0
            
            end_time = time.time() + (duration_minutes * 60)
            
            while time.time() < end_time:
                try:
                    # Выбираем случайное действие на основе стратегии
                    action = self._select_next_action(strategy)
                    
                    # Выполняем действие
                    success, action_result = self._execute_warming_action(client, action, account_id)
                    
                    total_actions += 1
                    if success:
                        successful_actions += 1
                    
                    # Логируем действие
                    self.warming_sessions[session_id]['actions_performed'].append({
                        'action': action,
                        'success': success,
                        'result': action_result,
                        'timestamp': time.time()
                    })
                    
                    # Адаптивная пауза между действиями
                    delay = self._calculate_adaptive_delay(action, account_id, success)
                    logger.info(f"Выполнено действие '{action}' для {account.username}. Пауза: {delay} сек")
                    time.sleep(delay)
                    
                except Exception as e:
                    error_msg = f"Ошибка при выполнении действия: {e}"
                    logger.error(error_msg)
                    self.warming_sessions[session_id]['errors'].append(error_msg)
                    time.sleep(30)  # Пауза при ошибке
            
            # Завершаем сессию
            success_rate = (successful_actions / total_actions * 100) if total_actions > 0 else 0
            result_message = f"Выполнено {successful_actions}/{total_actions} действий (успешность: {success_rate:.1f}%)"
            
            logger.info(f"Прогрев аккаунта {account.username} завершен: {result_message}")
            
            # Очищаем сессию
            del self.warming_sessions[session_id]
            
            return True, result_message
            
        except Exception as e:
            error_msg = f"Ошибка прогрева аккаунта {account_id}: {e}"
            logger.error(error_msg)
            return False, error_msg
    
    def adaptive_feed_browsing(self, client, duration: int = 300) -> Tuple[bool, str]:
        """Адаптивный просмотр ленты"""
        try:
            actions_count = 0
            start_time = time.time()
            
            while time.time() - start_time < duration:
                try:
                    # Получаем ленту
                    feed = client.feed_timeline()
                    if not feed or 'items' not in feed:
                        break
                    
                    # Прокручиваем ленту
                    for item in feed['items'][:random.randint(3, 8)]:
                        if time.time() - start_time >= duration:
                            break
                        
                        # Случайно лайкаем посты (10% вероятность)
                        if random.random() < 0.1:
                            try:
                                client.media_like(item['id'])
                                actions_count += 1
                                time.sleep(random.randint(2, 5))
                            except:
                                pass
                        
                        time.sleep(random.randint(1, 3))
                    
                    time.sleep(random.randint(10, 30))
                    
                except Exception as e:
                    logger.warning(f"Ошибка при просмотре ленты: {e}")
                    time.sleep(15)
            
            return True, f"Просмотр ленты завершен, выполнено {actions_count} лайков"
            
        except Exception as e:
            return False, f"Ошибка просмотра ленты: {e}"
    
    def smart_reels_interaction(self, client, duration: int = 200) -> Tuple[bool, str]:
        """Умное взаимодействие с reels"""
        try:
            actions_count = 0
            start_time = time.time()
            
            while time.time() - start_time < duration:
                try:
                    # Получаем reels
                    reels = client.feed_reels_tray()
                    if not reels or 'items' not in reels:
                        break
                    
                    # Взаимодействуем с reels
                    for reel in reels['items'][:random.randint(2, 5)]:
                        if time.time() - start_time >= duration:
                            break
                        
                        # Случайно лайкаем (15% вероятность)
                        if random.random() < 0.15:
                            try:
                                client.media_like(reel['id'])
                                actions_count += 1
                                time.sleep(random.randint(2, 4))
                            except:
                                pass
                        
                        time.sleep(random.randint(3, 8))
                    
                    time.sleep(random.randint(15, 45))
                    
                except Exception as e:
                    logger.warning(f"Ошибка при взаимодействии с reels: {e}")
                    time.sleep(20)
            
            return True, f"Взаимодействие с reels завершено, выполнено {actions_count} лайков"
            
        except Exception as e:
            return False, f"Ошибка взаимодействия с reels: {e}"
    
    def intelligent_story_viewing(self, client, duration: int = 150) -> Tuple[bool, str]:
        """Интеллектуальный просмотр stories"""
        try:
            actions_count = 0
            start_time = time.time()
            
            while time.time() - start_time < duration:
                try:
                    # Получаем stories
                    stories = client.feed_reels_tray()
                    if not stories or 'items' not in stories:
                        break
                    
                    # Просматриваем stories
                    for story in stories['items'][:random.randint(2, 6)]:
                        if time.time() - start_time >= duration:
                            break
                        
                        try:
                            # Просматриваем story
                            client.story_seen([story['id']])
                            actions_count += 1
                            time.sleep(random.randint(3, 8))
                        except:
                            pass
                    
                    time.sleep(random.randint(20, 60))
                    
                except Exception as e:
                    logger.warning(f"Ошибка при просмотре stories: {e}")
                    time.sleep(25)
            
            return True, f"Просмотр stories завершен, просмотрено {actions_count} историй"
            
        except Exception as e:
            return False, f"Ошибка просмотра stories: {e}"
    
    def _determine_warming_strategy(self, account_id: int) -> Dict[str, float]:
        """Определение стратегии прогрева на основе возраста аккаунта"""
        try:
            account = get_instagram_account(account_id)
            if not account:
                return self._get_default_strategy()
            
            account_age = datetime.now() - account.created_at
            age_days = account_age.days
            
            if age_days < 7:  # Новый аккаунт
                return {
                    'feed_browsing': 0.6,
                    'story_viewing': 0.25,
                    'reels_interaction': 0.10,
                    'profile_visits': 0.05
                }
            elif age_days < 30:  # Молодой аккаунт
                return {
                    'feed_browsing': 0.5,
                    'story_viewing': 0.25,
                    'reels_interaction': 0.15,
                    'profile_visits': 0.10
                }
            else:  # Зрелый аккаунт
                return {
                    'feed_browsing': 0.4,
                    'story_viewing': 0.25,
                    'reels_interaction': 0.20,
                    'profile_visits': 0.15
                }
                
        except Exception as e:
            logger.error(f"Ошибка определения стратегии для аккаунта {account_id}: {e}")
            return self._get_default_strategy()
    
    def _get_default_strategy(self) -> Dict[str, float]:
        """Стратегия по умолчанию"""
        return {
            'feed_browsing': 0.5,
            'story_viewing': 0.25,
            'reels_interaction': 0.15,
            'profile_visits': 0.10
        }
    
    def _select_next_action(self, strategy: Dict[str, float]) -> str:
        """Выбор следующего действия на основе стратегии"""
        actions = list(strategy.keys())
        weights = list(strategy.values())
        return random.choices(actions, weights=weights)[0]
    
    def _execute_warming_action(self, client, action: str, account_id: int) -> Tuple[bool, str]:
        """Выполнение действия прогрева"""
        try:
            if action == 'feed_browsing':
                return self.adaptive_feed_browsing(client, duration=random.randint(60, 180))
            elif action == 'story_viewing':
                return self.intelligent_story_viewing(client, duration=random.randint(30, 90))
            elif action == 'reels_interaction':
                return self.smart_reels_interaction(client, duration=random.randint(45, 120))
            elif action == 'profile_visits':
                return self._visit_random_profiles(client, count=random.randint(2, 5))
            else:
                return False, f"Неизвестное действие: {action}"
                
        except Exception as e:
            return False, f"Ошибка выполнения действия {action}: {e}"
    
    def _visit_random_profiles(self, client, count: int = 3) -> Tuple[bool, str]:
        """Посещение случайных профилей"""
        try:
            visited = 0
            
            # Получаем рекомендованных пользователей
            try:
                suggestions = client.discover_explore()
                if suggestions and 'items' in suggestions:
                    users = [item['user'] for item in suggestions['items'][:count*2] if 'user' in item]
                else:
                    return False, "Не удалось получить рекомендации"
            except:
                return False, "Ошибка получения рекомендаций"
            
            for user in users[:count]:
                try:
                    # Посещаем профиль
                    client.user_info(user['id'])
                    visited += 1
                    time.sleep(random.randint(5, 15))
                except:
                    pass
            
            return True, f"Посещено {visited} профилей"
            
        except Exception as e:
            return False, f"Ошибка посещения профилей: {e}"
    
    def _calculate_adaptive_delay(self, action: str, account_id: int, success: bool) -> int:
        """Расчет адаптивной задержки"""
        try:
            # Базовые задержки
            base_delays = {
                'feed_browsing': 30,
                'story_viewing': 20,
                'reels_interaction': 25,
                'profile_visits': 15
            }
            
            base_delay = base_delays.get(action, 20)
            
            # Корректировка на основе возраста аккаунта
            account = get_instagram_account(account_id)
            if account:
                account_age = datetime.now() - account.created_at
                age_days = account_age.days
                
                if age_days < 7:        # Новый аккаунт - больше пауз
                    multiplier = 2.0
                elif age_days < 30:     # Молодой аккаунт
                    multiplier = 1.5
                else:                   # Зрелый аккаунт
                    multiplier = 1.0
            else:
                multiplier = 1.5
            
            # Корректировка на основе успешности
            if not success:
                multiplier *= 1.5  # Увеличиваем паузы при ошибках
            
            delay = int(base_delay * multiplier)
            
            # Добавляем случайность ±30%
            variance = int(delay * 0.3)
            delay = random.randint(delay - variance, delay + variance)
            
            return max(5, delay)  # Минимум 5 секунд
            
        except Exception as e:
            logger.error(f"Ошибка расчета задержки: {e}")
            return 30


# Функция для совместимости с handlers.py
def warm_account_improved(account_id: int, duration_minutes: int = 30) -> Tuple[bool, str]:
    """Функция-обертка для использования в handlers.py"""
    warmer = ImprovedAccountWarmer()
    return warmer.warm_account_improved(account_id, duration_minutes) 
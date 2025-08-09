# Умная система валидации аккаунтов

## Обзор

Умная система валидации - это интеллектуальный сервис для проверки и восстановления Instagram аккаунтов с управлением нагрузкой на систему.

## Ключевые особенности

### 1. **Управление нагрузкой**
- Ограничение одновременных проверок (по умолчанию 3)
- Ограничение одновременных восстановлений (по умолчанию 1)
- Мониторинг CPU и RAM
- Автоматическая приостановка при высокой нагрузке

### 2. **Система приоритетов**
- **CRITICAL** - для активных задач (немедленная проверка)
- **HIGH** - для важных операций
- **NORMAL** - обычная проверка
- **LOW** - фоновая проверка

### 3. **Интеллектуальная очередь**
- Приоритетная обработка задач
- Автоматическое откладывание низкоприоритетных задач при нагрузке
- Cooldown для проблемных аккаунтов

### 4. **Предотвращение перегрузки**
- Максимум 2-3 одновременных восстановления через IMAP
- Задержка между попытками восстановления
- Отказ от восстановления после 3 неудачных попыток

## Использование

### Инициализация
```python
from utils.smart_validator_service import get_smart_validator

validator = get_smart_validator()
validator.start()
```

### Проверка перед использованием
```python
from utils.smart_validator_service import validate_before_use, ValidationPriority

# Для критических задач
if validate_before_use(account_id, ValidationPriority.CRITICAL):
    # Аккаунт готов к использованию
    pass
else:
    # Аккаунт невалиден или не готов
    pass
```

### Запрос проверки
```python
# Добавить аккаунт в очередь проверки
validator.request_validation(account_id, ValidationPriority.NORMAL)

# Проверить статус
status = validator.get_account_status(account_id)
is_valid = validator.is_account_valid(account_id)
```

### Получение статистики
```python
stats = validator.get_stats()
# Содержит:
# - total_accounts
# - status_counts (по каждому статусу)
# - active_checks
# - active_recoveries
# - check_queue_size
# - recovery_queue_size
# - system_load

load = validator.get_system_load()
# Содержит:
# - cpu_usage
# - memory_usage
# - is_high_load
```

## Интеграция с задачами

### Публикации
```python
# В task_queue.py
if not validate_before_use(task.account_id, ValidationPriority.CRITICAL):
    # Пропускаем задачу
    task.status = TaskStatus.FAILED
    task.error_message = "Аккаунт невалиден"
    return
```

### Автоподписки
```python
# В follow_manager.py
if not validate_before_use(self.task.account_id, ValidationPriority.HIGH):
    logger.error("Аккаунт невалиден")
    return False
```

## Настройки

При создании валидатора:
```python
SmartValidatorService(
    check_interval_minutes=30,      # Интервал между проверками
    max_concurrent_checks=3,        # Макс. одновременных проверок
    max_concurrent_recoveries=1,    # Макс. одновременных восстановлений
    recovery_cooldown_minutes=60    # Задержка между попытками восстановления
)
```

## Статусы аккаунтов

- **VALID** - аккаунт работает
- **INVALID** - требует восстановления
- **CHECKING** - проверяется
- **RECOVERING** - восстанавливается
- **FAILED** - не удалось восстановить
- **COOLDOWN** - временно заблокирован после неудачи

## Защита от перегрузки

1. **Быстрая проверка** - только легкий запрос timeline
2. **Ограничение восстановлений** - не более 1-2 одновременно
3. **Приоритеты** - критические задачи первыми
4. **Мониторинг нагрузки** - автоматическая пауза при высокой нагрузке
5. **Cooldown** - задержка 60 минут после неудачного восстановления

## Решение проблем

### Закольцованный запрос пароля
Система автоматически:
- Ограничивает попытки запроса пароля (максимум 3)
- Прерывает цикл при превышении лимита
- Пытается получить пароль из БД
- Сбрасывает счетчики при успешном входе

### Высокая нагрузка
При высокой нагрузке система:
- Откладывает низкоприоритетные задачи
- Приостанавливает восстановления
- Увеличивает задержки между операциями
- Логирует состояние системы

## API веб-интерфейса

### Запуск проверки всех аккаунтов
```
POST /api/accounts/validate
```

### Проверка одного аккаунта
```
POST /api/accounts/<id>/check
```

### Получение статуса
```
GET /api/accounts/validate/status
```

### Обновление настроек
```
PUT /api/accounts/validate/settings
{
    "check_interval_minutes": 30
}
```

## Мониторинг

Система предоставляет детальную информацию:
- Количество аккаунтов в каждом статусе
- Размер очередей
- Активные операции
- Нагрузка CPU/RAM
- История проверок 
# РУКОВОДСТВО ПО МИГРАЦИИ НА НОВУЮ СИСТЕМУ СИНХРОНИЗАЦИИ

## Что изменилось

Старая система использовала:
- Файловый кеш (data/shared_access_cache.json)
- Обновление каждые 30 секунд
- Возможные задержки синхронизации

Новая система использует:
- multiprocessing.Manager для shared state
- Event-driven архитектуру
- МГНОВЕННУЮ синхронизацию

## Как использовать

Все функции остались теми же:
- has_access(user_id) - проверка доступа
- add_user_access(user_id, user_data) - добавление пользователя
- remove_user_access(user_id) - удаление пользователя
- delete_user_completely(user_id) - полное удаление

## Преимущества

✅ Мгновенная синхронизация между процессами
✅ Нет файловых операций
✅ Event-driven архитектура
✅ Автоматический fallback на старую систему
✅ Thread-safe операции

## Тестирование

Запустите тест:
```
python test_new_sync_systems.py
```

## Мониторинг

Проверить статистику:
```python
from multiprocessing_access_sync import get_mp_sync
sync = get_mp_sync()
print(sync.get_stats())
```

## Rollback

Если что-то не работает, восстановите файлы из backup_old_sync/

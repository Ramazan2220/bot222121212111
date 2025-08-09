import os
import logging
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import joinedload
from typing import Tuple, Union
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.pool import QueuePool
from sqlalchemy import text

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DATABASE_URL
from database.models import Base, InstagramAccount, Proxy, PublishTask, TaskStatus, AccountGroup

logger = logging.getLogger(__name__)

# Импорт Database Connection Pool
from database.connection_pool import init_db_pool, get_session_direct, get_db_stats, dispose_db_pool

# Создаем директорию для базы данных, если она не существует
os.makedirs(os.path.dirname(DATABASE_URL.replace("sqlite:///", "")), exist_ok=True)

# Создаем движок SQLAlchemy
engine = create_engine(DATABASE_URL)

# Создаем фабрику сессий
Session = sessionmaker(bind=engine)

# Флаг для отслеживания инициализации пула
_pool_initialized = False

def init_db():
    """Инициализирует базу данных"""
    global _pool_initialized
    
    # Создаем таблицы
    Base.metadata.create_all(engine)
    logger.info("База данных инициализирована")
    
    # Инициализируем Connection Pool
    if not _pool_initialized:
        try:
            init_db_pool(
                database_url=DATABASE_URL,
                pool_size=50,        # 🔥 Увеличено для экстремальных тестов
                max_overflow=100,    # 🔥 Увеличено для экстремальных тестов
                pool_timeout=60,     # 🔥 Увеличен таймаут
                pool_recycle=3600
            )
            _pool_initialized = True
            logger.info("✅ Database Connection Pool инициализирован в db_manager")
        except Exception as e:
            logger.warning(f"⚠️ Не удалось инициализировать Database Connection Pool: {e}")
            logger.info("🔄 Используется стандартный механизм сессий")

def get_session():
    """Возвращает новую сессию базы данных с автоматической изоляцией пользователей"""
    global _pool_initialized
    
    # Получаем базовую сессию
    if _pool_initialized:
        try:
            session = get_session_direct()
        except Exception as e:
            logger.warning(f"⚠️ Ошибка получения сессии из пула: {e}")
            logger.info("🔄 Переключаемся на стандартную сессию")
            session = Session()
    else:
        session = Session()
    
    # 🔒 ПРИМЕНЯЕМ АВТОМАТИЧЕСКУЮ ИЗОЛЯЦИЮ ПОЛЬЗОВАТЕЛЕЙ
    try:
        from database.smart_query_interceptor import session_query_interceptor, SmartQueryInterceptor
        current_user_id = SmartQueryInterceptor.get_current_user()
        
        if current_user_id is not None:
            session = session_query_interceptor(session)
            logger.debug(f"🔒 Сессия настроена для изоляции пользователя {current_user_id}")
    except Exception as e:
        logger.debug(f"🔒 Изоляция не применена: {e}")
    
    return session

def add_instagram_account(username, password, email=None, email_password=None):
    """Добавляет новый аккаунт Instagram в базу данных"""
    try:
        session = get_session()

        # Проверяем, существует ли уже аккаунт с таким именем пользователя
        existing_account = session.query(InstagramAccount).filter_by(username=username).first()
        if existing_account:
            session.close()
            return False, "Аккаунт с таким именем пользователя уже существует"

        # Создаем новый аккаунт
        account = InstagramAccount(
            username=username,
            password=password,
            email=email,
            email_password=email_password,
            is_active=True
        )

        session.add(account)
        session.commit()
        account_id = account.id
        session.close()

        return True, account_id
    except Exception as e:
        logger.error(f"Ошибка при добавлении аккаунта: {e}")
        return False, str(e)

def add_instagram_account_without_login(username, password, email, email_password):
    """
    Добавляет аккаунт Instagram в базу данных без проверки входа

    Args:
        username (str): Имя пользователя Instagram
        password (str): Пароль от аккаунта Instagram
        email (str): Email, привязанный к аккаунту
        email_password (str): Пароль от email

    Returns:
        InstagramAccount: Объект аккаунта, если успешно добавлен, иначе None
    """
    logger.info(f"Добавление аккаунта {username} в базу данных без проверки входа")

    session = get_session()

    try:
        # Проверяем, существует ли уже аккаунт с таким именем
        existing_account = session.query(InstagramAccount).filter_by(username=username).first()

        if existing_account:
            logger.warning(f"Аккаунт {username} уже существует в базе данных")
            session.close()
            return existing_account

        # Создаем новый аккаунт
        account = InstagramAccount(
            username=username,
            password=password,
            email=email,
            email_password=email_password,
            is_active=False,  # Устанавливаем неактивным, пока не проверим вход
            created_at=datetime.now()
        )

        session.add(account)
        session.commit()

        logger.info(f"Аккаунт {username} успешно добавлен в базу данных с ID {account.id}")

        return account
    except Exception as e:
        session.rollback()
        logger.error(f"Ошибка при добавлении аккаунта {username} в базу данных: {str(e)}")
        return None
    finally:
        session.close()
        

def get_instagram_account(account_id, user_id: int = None):
    """
    Получает аккаунт Instagram по ID с предзагрузкой связанных данных и изоляцией по пользователям
    
    Args:
        account_id: ID аккаунта Instagram
        user_id (int): ID пользователя Telegram для фильтрации аккаунтов
    
    Returns:
        InstagramAccount: Аккаунт для конкретного пользователя или None
    """
    try:
        from sqlalchemy.orm import joinedload
        session = get_session()
        
        # Используем eager loading для предзагрузки связанных данных
        query = session.query(InstagramAccount)\
                       .options(joinedload(InstagramAccount.groups))\
                       .options(joinedload(InstagramAccount.proxy))
        
        # КРИТИЧЕСКИ ВАЖНО: Фильтруем по user_id для изоляции данных
        if user_id is not None:
            account = query.filter_by(id=account_id, user_id=user_id).first()
            logger.debug(f"🔒 Поиск аккаунта {account_id} для пользователя {user_id}")
        else:
            # Только для системных операций (миграция, admin)
            account = query.filter_by(id=account_id).first()
            logger.warning(f"⚠️ СИСТЕМНЫЙ ЗАПРОС: Поиск аккаунта {account_id} БЕЗ фильтрации")
        
        session.close()
        return account
    except Exception as e:
        logger.error(f"Ошибка при получении аккаунта {account_id} для пользователя {user_id}: {e}")
        return None

def get_instagram_accounts(user_id: int = None):
    """
    Получает список аккаунтов Instagram из базы данных с изоляцией по пользователям
    
    Args:
        user_id (int): ID пользователя Telegram для фильтрации аккаунтов
    
    Returns:
        List[InstagramAccount]: Список аккаунтов для конкретного пользователя
    """
    try:
        session = get_session()
        
        # КРИТИЧЕСКИ ВАЖНО: Фильтруем по user_id для изоляции данных
        if user_id is not None:
            accounts = session.query(InstagramAccount).filter_by(user_id=user_id).all()
            logger.info(f"🔒 Получено {len(accounts)} аккаунтов для пользователя {user_id}")
        else:
            # Только для системных операций (миграция, admin)
            accounts = session.query(InstagramAccount).all()
            logger.warning(f"⚠️ СИСТЕМНЫЙ ЗАПРОС: Получено {len(accounts)} аккаунтов БЕЗ фильтрации")
        
        session.close()
        return accounts
        
    except Exception as e:
        logger.error(f"Ошибка при получении аккаунтов для пользователя {user_id}: {e}")
        return []

def get_total_accounts():
    """Возвращает общее количество аккаунтов"""
    try:
        session = get_session()
        count = session.query(InstagramAccount).count()
        session.close()
        return count
    except Exception as e:
        logger.error(f"Ошибка при подсчете аккаунтов: {e}")
        return 0

def get_user_active_accounts(user_id=None):
    """Получает список активных аккаунтов пользователя"""
    try:
        from sqlalchemy.orm import joinedload
        session = get_session()
        
        # Получаем только активные аккаунты
        query = session.query(InstagramAccount)\
                       .options(joinedload(InstagramAccount.groups))\
                       .options(joinedload(InstagramAccount.proxy))\
                       .filter(InstagramAccount.is_active == True)
        
        # Если передан user_id, можно добавить фильтрацию по пользователю
        # Пока возвращаем все активные аккаунты
        accounts = query.all()
        session.close()
        return accounts
    except Exception as e:
        logger.error(f"Ошибка при получении активных аккаунтов: {e}")
        return []

def save_media_file(file_path, media_data):
    """Сохраняет медиа файл (заглушка)"""
    try:
        # Простая заглушка - можно расширить позже
        import os
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, 'wb') as f:
            f.write(media_data)
        return True
    except Exception as e:
        logger.error(f"Ошибка при сохранении медиа файла: {e}")
        return False

def get_user_published_posts(user_id=None, limit=50):
    """Получает список опубликованных постов пользователя"""
    try:
        session = get_session()
        
        # Получаем выполненные задачи публикации
        query = session.query(PublishTask)\
                       .filter(PublishTask.status == TaskStatus.COMPLETED)\
                       .order_by(PublishTask.created_at.desc())\
                       .limit(limit)
        
        tasks = query.all()
        session.close()
        return tasks
    except Exception as e:
        logger.error(f"Ошибка при получении опубликованных постов: {e}")
        return []

def update_instagram_account(account_id, **kwargs):
    """Обновляет данные аккаунта Instagram"""
    try:
        session = get_session()
        account = session.query(InstagramAccount).filter_by(id=account_id).first()

        if not account:
            session.close()
            return False, "Аккаунт не найден"

        # Обновляем поля аккаунта
        for key, value in kwargs.items():
            if hasattr(account, key):
                setattr(account, key, value)

        session.commit()
        session.close()

        return True, "Аккаунт успешно обновлен"
    except Exception as e:
        logger.error(f"Ошибка при обновлении аккаунта: {e}")
        return False, str(e)

def delete_instagram_account(account_id):
    """Удаляет аккаунт Instagram"""
    try:
        session = get_session()
        account = session.query(InstagramAccount).filter_by(id=account_id).first()

        if not account:
            session.close()
            return False, "Аккаунт не найден"

        session.delete(account)
        session.commit()
        session.close()

        return True, None
    except Exception as e:
        logger.error(f"Ошибка при удалении аккаунта: {e}")
        return False, str(e)

def add_proxy(protocol, host, port, username=None, password=None):
    """
    Добавляет новый прокси в базу данных
    """
    try:
        session = get_session()

        # Проверяем, существует ли уже прокси с такими параметрами
        # ИЗМЕНЕНИЕ: Добавляем username в условие проверки
        query = session.query(Proxy).filter_by(
            protocol=protocol,
            host=host,
            port=port
        )

        # Если указан username, добавляем его в условие
        if username:
            query = query.filter_by(username=username)

        existing_proxy = query.first()

        if existing_proxy:
            session.close()
            return False, "Прокси с такими параметрами уже существует"

        # Создаем новый прокси
        proxy = Proxy(
            protocol=protocol,
            host=host,
            port=port,
            username=username,
            password=password,
            is_active=True
        )

        session.add(proxy)
        session.commit()
        proxy_id = proxy.id
        session.close()

        return True, proxy_id
    except Exception as e:
        logger.error(f"Ошибка при добавлении прокси: {e}")
        return False, str(e)

def get_proxy(proxy_id):
    """
    Получает прокси по ID

    Args:
        proxy_id (int): ID прокси

    Returns:
        Proxy: Объект прокси или None, если не найден
    """
    try:
        session = get_session()
        proxy = session.query(Proxy).filter_by(id=proxy_id).first()
        session.close()
        return proxy
    except Exception as e:
        logger.error(f"Ошибка при получении прокси: {e}")
        return None

def get_proxies(active_only=False):
    """
    Получает список всех прокси

    Args:
        active_only (bool): Если True, возвращает только активные прокси

    Returns:
        list: Список объектов Proxy
    """
    try:
        session = get_session()
        query = session.query(Proxy)

        if active_only:
            query = query.filter_by(is_active=True)

        proxies = query.all()
        session.close()
        return proxies
    except Exception as e:
        logger.error(f"Ошибка при получении списка прокси: {e}")
        return []

def update_proxy(proxy_id, **kwargs):
    """
    Обновляет данные прокси

    Args:
        proxy_id (int): ID прокси
        **kwargs: Параметры для обновления

    Returns:
        tuple: (success, message)
    """
    try:
        session = get_session()
        proxy = session.query(Proxy).filter_by(id=proxy_id).first()

        if not proxy:
            session.close()
            return False, "Прокси не найден"

        # Обновляем поля прокси
        for key, value in kwargs.items():
            if hasattr(proxy, key):
                setattr(proxy, key, value)

        session.commit()
        session.close()

        return True, None
    except Exception as e:
        logger.error(f"Ошибка при обновлении прокси: {e}")
        return False, str(e)

def delete_proxy(proxy_id):
    """
    Удаляет прокси

    Args:
        proxy_id (int): ID прокси

    Returns:
        tuple: (success, message)
    """
    try:
        session = get_session()
        proxy = session.query(Proxy).filter_by(id=proxy_id).first()

        if not proxy:
            session.close()
            return False, "Прокси не найден"

        # Удаляем связи с аккаунтами
        accounts = session.query(InstagramAccount).filter_by(proxy_id=proxy_id).all()
        for account in accounts:
            account.proxy_id = None

        session.delete(proxy)
        session.commit()
        session.close()

        return True, None
    except Exception as e:
        logger.error(f"Ошибка при удалении прокси: {e}")
        return False, str(e)

def assign_proxy_to_account(account_id, proxy_id):
    """
    Назначает прокси аккаунту

    Args:
        account_id (int): ID аккаунта
        proxy_id (int): ID прокси

    Returns:
        tuple: (success, message)
    """
    try:
        session = get_session()
        account = session.query(InstagramAccount).filter_by(id=account_id).first()
        proxy = session.query(Proxy).filter_by(id=proxy_id).first()

        if not account:
            session.close()
            return False, "Аккаунт не найден"

        if not proxy:
            session.close()
            return False, "Прокси не найден"

        account.proxy_id = proxy_id
        session.commit()
        session.close()

        return True, None
    except Exception as e:
        logger.error(f"Ошибка при назначении прокси аккаунту: {e}")
        return False, str(e)

def get_proxy(proxy_id):
    """Получает прокси по ID"""
    try:
        session = get_session()
        proxy = session.query(Proxy).filter_by(id=proxy_id).first()
        session.close()
        return proxy
    except Exception as e:
        logger.error(f"Ошибка при получении прокси: {e}")
        return None

def get_proxies():
    """Получает список всех прокси"""
    try:
        session = get_session()
        proxies = session.query(Proxy).all()
        session.close()
        return proxies
    except Exception as e:
        logger.error(f"Ошибка при получении списка прокси: {e}")
        return []

def update_proxy(proxy_id, **kwargs):
    """Обновляет данные прокси"""
    try:
        session = get_session()
        proxy = session.query(Proxy).filter_by(id=proxy_id).first()

        if not proxy:
            session.close()
            return False, "Прокси не найден"

        # Обновляем поля прокси
        for key, value in kwargs.items():
            if hasattr(proxy, key):
                setattr(proxy, key, value)

        session.commit()
        session.close()

        return True, None
    except Exception as e:
        logger.error(f"Ошибка при обновлении прокси: {e}")
        return False, str(e)

def delete_proxy(proxy_id):
    """Удаляет прокси"""
    try:
        session = get_session()
        proxy = session.query(Proxy).filter_by(id=proxy_id).first()

        if not proxy:
            session.close()
            return False, "Прокси не найден"

        session.delete(proxy)
        session.commit()
        session.close()

        return True, None
    except Exception as e:
        logger.error(f"Ошибка при удалении прокси: {e}")
        return False, str(e)

def assign_proxy_to_account(account_id, proxy_id):
    """Назначает прокси аккаунту"""
    try:
        session = get_session()
        account = session.query(InstagramAccount).filter_by(id=account_id).first()
        proxy = session.query(Proxy).filter_by(id=proxy_id).first()

        if not account:
            session.close()
            return False, "Аккаунт не найден"

        if not proxy:
            session.close()
            return False, "Прокси не найден"

        account.proxy_id = proxy_id
        session.commit()
        session.close()

        return True, None
    except Exception as e:
        logger.error(f"Ошибка при назначении прокси аккаунту: {e}")
        return False, str(e)

def create_publish_task(account_id, task_type, media_path, caption="", scheduled_time=None, additional_data=None, user_id=None):
    """Создает новую задачу на публикацию"""
    try:
        session = get_session()

        # Определяем статус задачи
        if scheduled_time:
            status = TaskStatus.SCHEDULED
        else:
            status = TaskStatus.PENDING

        task = PublishTask(
            account_id=account_id,
            task_type=task_type,
            media_path=media_path,
            caption=caption,
            status=status,
            scheduled_time=scheduled_time,
            options=additional_data,  # Используем поле options для хранения дополнительных данных
            user_id=user_id  # Добавляем user_id
        )

        session.add(task)
        session.commit()
        task_id = task.id
        session.close()

        logger.info(f"✅ Создана задача #{task_id} со статусом {status.value}" + 
                   (f" на {scheduled_time}" if scheduled_time else ""))

        return True, task_id
    except Exception as e:
        logger.error(f"Ошибка при создании задачи: {e}")
        return False, str(e)

def update_publish_task_status(task_id, status, error_message=None, media_id=None):
    """Обновляет статус задачи на публикацию"""
    try:
        session = get_session()
        task = session.query(PublishTask).filter_by(id=task_id).first()

        if not task:
            session.close()
            return False, "Задача не найдена"

        task.status = status
        task.error_message = error_message
        task.media_id = media_id  # Теперь у нас есть это поле!

        # Если задача завершена успешно и есть media_id, сохраняем его также в options для обратной совместимости
        if status == TaskStatus.COMPLETED and media_id:
            task.completed_at = datetime.now()
            
            # Обновляем options, чтобы включить media_id (для обратной совместимости)
            try:
                import json
                options = json.loads(task.options) if task.options and isinstance(task.options, str) else task.options or {}
                options['media_id'] = media_id
                task.options = json.dumps(options)
            except Exception as e:
                logger.warning(f"Не удалось обновить options с media_id: {e}")
        elif status == TaskStatus.COMPLETED:
            task.completed_at = datetime.now()

        session.commit()
        session.close()

        return True, None
    except Exception as e:
        logger.error(f"Ошибка при обновлении статуса задачи: {e}")
        return False, str(e)

def update_task_status(task_id, status, error_message=None, media_id=None):
    """
    Обновляет статус задачи публикации
    Эта функция является алиасом для update_publish_task_status
    """
    return update_publish_task_status(task_id, status, error_message, media_id)

def get_publish_task(task_id):
    """Получает задачу на публикацию по ID"""
    try:
        session = get_session()
        # Используем joinedload для загрузки связанного аккаунта
        task = session.query(PublishTask).options(
            joinedload(PublishTask.account)
        ).filter_by(id=task_id).first()
        
        # Важно: не закрываем сессию сразу, чтобы объект оставался привязанным
        if task:
            # Делаем копию данных, которые нам нужны
            task_data = {
                'id': task.id,
                'account_id': task.account_id,
                'account_username': task.account.username if task.account else None,
                'account_email': task.account.email if task.account else None,
                'account_email_password': task.account.email_password if task.account else None,
                'task_type': task.task_type,
                'status': task.status,
                'media_path': task.media_path,
                'caption': task.caption,
                'hashtags': task.hashtags,
                'options': task.options,
                'user_id': task.user_id,  # Добавляем user_id
                'account': task.account  # Сохраняем ссылку на объект аккаунта
            }
            session.close()
            return task_data
        else:
            session.close()
            return None
    except Exception as e:
        logger.error(f"Ошибка при получении задачи: {e}")
        return None

def get_publish_tasks(account_id=None, status=None):
    """Получает список задач на публикацию"""
    try:
        session = get_session()
        query = session.query(PublishTask)

        if account_id:
            query = query.filter_by(account_id=account_id)

        if status:
            query = query.filter_by(status=status)

        tasks = query.all()
        session.close()
        return tasks
    except Exception as e:
        logger.error(f"Ошибка при получении списка задач: {e}")
        return []

def get_pending_tasks():
    """Получает список задач, ожидающих выполнения"""
    try:
        session = get_session()
        tasks = session.query(PublishTask).filter_by(status=TaskStatus.PENDING).all()
        session.close()
        return tasks
    except Exception as e:
        logger.error(f"Ошибка при получении списка ожидающих задач: {e}")
        return []

def get_scheduled_tasks():
    """Получает список запланированных задач, готовых к выполнению"""
    try:
        session = get_session()
        # Получаем задачи со статусом SCHEDULED или PENDING, у которых есть scheduled_time
        tasks = session.query(PublishTask).filter(
            PublishTask.scheduled_time.isnot(None),
            PublishTask.status.in_([TaskStatus.SCHEDULED, TaskStatus.PENDING])
        ).options(
            joinedload(PublishTask.account)
        ).all()
        
        logger.debug(f"📋 Найдено {len(tasks)} запланированных задач")
        
        # Не закрываем сессию сразу, чтобы объекты оставались привязанными
        result = []
        for task in tasks:
            result.append(task)
        
        session.close()
        return result
        
    except Exception as e:
        logger.error(f"❌ Ошибка при получении списка запланированных задач: {e}")
        return []

def delete_publish_task(task_id):
    """Удаляет задачу на публикацию"""
    try:
        session = get_session()
        task = session.query(PublishTask).filter_by(id=task_id).first()

        if not task:
            session.close()
            return False, "Задача не найдена"

        session.delete(task)
        session.commit()
        session.close()

        return True, None
    except Exception as e:
        logger.error(f"Ошибка при удалении задачи: {e}")
        return False, str(e)

def bulk_add_instagram_accounts(accounts_data):
    """
    Массовое добавление аккаунтов Instagram

    Args:
        accounts_data (list): Список словарей с данными аккаунтов
            [
                {
                    "username": "user1",
                    "password": "pass1",
                    "proxy_id": None,  # опционально
                    "description": ""  # опционально
                },
                ...
            ]

    Returns:
        tuple: (успешно_добавленные, ошибки)
    """
    session = get_session()
    success = []
    errors = []

    for data in accounts_data:
        try:
            # Проверяем, существует ли уже аккаунт
            existing = session.query(InstagramAccount).filter_by(username=data["username"]).first()
            if existing:
                errors.append((data["username"], "Аккаунт уже существует"))
                continue

            # Создаем новый аккаунт
            account = InstagramAccount(
                username=data["username"],
                password=data["password"],
                is_active=True,
                proxy_id=data.get("proxy_id"),
                email=data.get("email"),
                email_password=data.get("email_password")
            )

            session.add(account)
            session.commit()
            success.append(data["username"])

        except Exception as e:
            session.rollback()
            errors.append((data["username"], str(e)))

    session.close()
    return success, errors

def update_account_session_data(account_id, session_data):
    """Обновляет данные сессии аккаунта Instagram"""
    try:
        session = get_session()
        account = session.query(InstagramAccount).filter_by(id=account_id).first()

        if not account:
            session.close()
            return False, "Аккаунт не найден"

        account.session_data = session_data
        account.last_login = datetime.now()
        
        session.commit()
        session.close()

        return True, None
    except Exception as e:
        logger.error(f"Ошибка при обновлении данных сессии аккаунта: {e}")
        return False, str(e)

def get_proxy_for_account(account_id):
    """
    Получает прокси, назначенный для аккаунта

    Args:
        account_id (int): ID аккаунта Instagram

    Returns:
        Proxy: Объект прокси или None, если прокси не назначен
    """
    session = get_session()
    try:
        account = session.query(InstagramAccount).filter_by(id=account_id).first()
        if account and account.proxy_id:
            proxy = session.query(Proxy).filter_by(id=account.proxy_id).first()
            return proxy
        return None
    except Exception as e:
        logger.error(f"Ошибка при получении прокси для аккаунта {account_id}: {str(e)}")
        return None
    finally:
        session.close()

def activate_instagram_account(account_id):
    """
    Активирует аккаунт Instagram

    Args:
        account_id (int): ID аккаунта Instagram

    Returns:
        bool: True, если аккаунт успешно активирован, иначе False
    """
    session = get_session()
    try:
        account = session.query(InstagramAccount).filter_by(id=account_id).first()
        if account:
            account.is_active = True
            account.updated_at = datetime.now()
            session.commit()
            logger.info(f"Аккаунт {account.username} (ID: {account_id}) активирован")
            return True
        return False
    except Exception as e:
        session.rollback()
        logger.error(f"Ошибка при активации аккаунта {account_id}: {str(e)}")
        return False
    finally:
        session.close()

def get_active_accounts():
    """Получает список активных аккаунтов Instagram"""
    try:
        session = get_session()
        accounts = session.query(InstagramAccount).filter_by(is_active=True).all()
        session.close()
        return accounts
    except Exception as e:
        logger.error(f"Ошибка при получении списка активных аккаунтов: {e}")
        return []

def get_accounts_with_email():
    """Получает список аккаунтов с указанной электронной почтой"""
    try:
        session = get_session()
        accounts = session.query(InstagramAccount).filter(
            InstagramAccount.email != None,
            InstagramAccount.email != ""
        ).all()
        session.close()
        return accounts
    except Exception as e:
        logger.error(f"Ошибка при получении списка аккаунтов с email: {e}")
        return []

def get_all_accounts():
    """Получает список всех аккаунтов Instagram из базы данных"""
    try:
        session = get_session()
        accounts = session.query(InstagramAccount).all()
        session.close()
        return accounts
    except Exception as e:
        logger.error(f"Ошибка при получении списка всех аккаунтов: {e}")
        return []

def update_account_session_data(account_id, session_data, last_login=None):
    """Обновляет данные сессии аккаунта Instagram"""
    try:
        session = get_session()
        account = session.query(InstagramAccount).filter_by(id=account_id).first()

        if not account:
            session.close()
            return False, "Аккаунт не найден"

        account.session_data = session_data
        if last_login:
            account.last_login = last_login
        else:
            account.last_login = datetime.now()

        session.commit()
        session.close()

        return True, None
    except Exception as e:
        logger.error(f"Ошибка при обновлении данных сессии аккаунта: {e}")
        return False, str(e)

def get_instagram_account_by_username(username):
    """Получает аккаунт Instagram по username с предзагрузкой связанных данных"""
    try:
        from sqlalchemy.orm import joinedload
        session = get_session()
        
        # Используем eager loading для предзагрузки связанных данных
        account = session.query(InstagramAccount)\
                         .options(joinedload(InstagramAccount.groups))\
                         .options(joinedload(InstagramAccount.proxy))\
                         .filter_by(username=username)\
                         .first()
        session.close()
        return account
    except Exception as e:
        logger.error(f"Ошибка при получении аккаунта по username: {e}")
        return None

def generate_and_save_device_id(account_id):
    """Генерирует и сохраняет уникальный device_id для аккаунта"""
    import uuid
    import random
    import string
    
    try:
        # Генерируем device_id в формате, похожем на реальный Instagram device_id
        # Формат: android-<hex_string>
        random_hex = ''.join(random.choices(string.hexdigits.lower(), k=16))
        device_id = f"android-{random_hex}"
        
        # Сохраняем в базе данных
        session = get_session()
        account = session.query(InstagramAccount).filter_by(id=account_id).first()
        
        if not account:
            session.close()
            return False, "Аккаунт не найден"
        
        # Если device_id уже существует, возвращаем его
        if account.device_id:
            session.close()
            return True, account.device_id
        
        account.device_id = device_id
        session.commit()
        session.close()
        
        logger.info(f"Device ID сгенерирован для аккаунта {account.username}: {device_id}")
        return True, device_id
        
    except Exception as e:
        logger.error(f"Ошибка при генерации device_id для аккаунта {account_id}: {e}")
        return False, str(e)

def get_or_create_device_id(account_id):
    """Получает существующий device_id или создает новый для аккаунта"""
    try:
        session = get_session()
        account = session.query(InstagramAccount).filter_by(id=account_id).first()
        
        if not account:
            session.close()
            return None
        
        if account.device_id:
            session.close()
            return account.device_id
        
        session.close()
        
        # Если device_id нет, генерируем новый
        success, device_id = generate_and_save_device_id(account_id)
        if success:
            return device_id
        else:
            return None
            
    except Exception as e:
        logger.error(f"Ошибка при получении/создании device_id для аккаунта {account_id}: {e}")
        return None

def ensure_account_device_consistency(account_id):
    """Обеспечивает консистентность настроек устройства и прокси для аккаунта"""
    try:
        session = get_session()
        account = session.query(InstagramAccount).filter_by(id=account_id).first()
        
        if not account:
            session.close()
            return False, "Аккаунт не найден"
        
        changes_made = False
        
        # Проверяем и создаем device_id если необходимо
        if not account.device_id:
            success, device_id = generate_and_save_device_id(account_id)
            if success:
                changes_made = True
                logger.info(f"Device ID создан для аккаунта {account.username}")
        
        # Проверяем назначение прокси если не назначен
        if not account.proxy_id:
            from utils.proxy_manager import assign_proxy_to_account
            proxy_success, proxy_message = assign_proxy_to_account(account_id)
            if proxy_success:
                changes_made = True
                logger.info(f"Прокси назначен аккаунту {account.username}")
        
        session.close()
        
        if changes_made:
            return True, "Настройки аккаунта обновлены"
        else:
            return True, "Все настройки аккаунта актуальны"
            
    except Exception as e:
        logger.error(f"Ошибка при проверке консистентности аккаунта {account_id}: {e}")
        return False, str(e)

# ===============================================
# Функции для работы с группами аккаунтов
# ===============================================

def create_account_group(name: str, description: str = None, icon: str = '📁') -> Tuple[bool, Union[int, str]]:
    """
    Создает новую группу аккаунтов
    
    Args:
        name: Название группы
        description: Описание группы
        icon: Эмодзи иконка
        
    Returns:
        (success, group_id или сообщение об ошибке)
    """
    session = Session()
    try:
        # Проверяем, не существует ли уже группа с таким именем
        existing = session.query(AccountGroup).filter_by(name=name).first()
        if existing:
            return False, "Группа с таким именем уже существует"
        
        group = AccountGroup(
            name=name,
            description=description,
            icon=icon
        )
        
        session.add(group)
        session.commit()
        
        logger.info(f"✅ Создана группа аккаунтов: {name} (ID: {group.id})")
        return True, group.id
        
    except Exception as e:
        session.rollback()
        logger.error(f"❌ Ошибка при создании группы: {e}")
        return False, str(e)
    finally:
        session.close()

def get_account_groups():
    """Получает список всех групп аккаунтов"""
    session = Session()
    try:
        groups = session.query(AccountGroup).order_by(AccountGroup.name).all()
        return groups
    except Exception as e:
        logger.error(f"Ошибка при получении списка групп: {e}")
        return []
    finally:
        session.close()

def get_account_group(group_id: int):
    """Получает группу по ID"""
    session = Session()
    try:
        group = session.query(AccountGroup).filter_by(id=group_id).first()
        return group
    except Exception as e:
        logger.error(f"Ошибка при получении группы {group_id}: {e}")
        return None
    finally:
        session.close()

def update_account_group(group_id: int, name: str = None, description: str = None, icon: str = None) -> Tuple[bool, str]:
    """Обновляет информацию о группе"""
    session = Session()
    try:
        group = session.query(AccountGroup).filter_by(id=group_id).first()
        if not group:
            return False, "Группа не найдена"
        
        if name:
            group.name = name
        if description is not None:
            group.description = description
        if icon:
            group.icon = icon
        
        session.commit()
        logger.info(f"✅ Группа {group_id} обновлена")
        return True, "Группа успешно обновлена"
        
    except Exception as e:
        session.rollback()
        logger.error(f"❌ Ошибка при обновлении группы: {e}")
        return False, str(e)
    finally:
        session.close()

def delete_account_group(group_id: int) -> Tuple[bool, str]:
    """Удаляет группу аккаунтов"""
    session = Session()
    try:
        group = session.query(AccountGroup).filter_by(id=group_id).first()
        if not group:
            return False, "Группа не найдена"
        
        session.delete(group)
        session.commit()
        
        logger.info(f"✅ Группа {group.name} удалена")
        return True, "Группа успешно удалена"
        
    except Exception as e:
        session.rollback()
        logger.error(f"❌ Ошибка при удалении группы: {e}")
        return False, str(e)
    finally:
        session.close()

def add_account_to_group(account_id: int, group_id: int) -> Tuple[bool, str]:
    """Добавляет аккаунт в группу"""
    session = Session()
    try:
        account = session.query(InstagramAccount).filter_by(id=account_id).first()
        if not account:
            return False, "Аккаунт не найден"
        
        group = session.query(AccountGroup).filter_by(id=group_id).first()
        if not group:
            return False, "Группа не найдена"
        
        if group not in account.groups:
            account.groups.append(group)
            session.commit()
            logger.info(f"✅ Аккаунт {account.username} добавлен в группу {group.name}")
            return True, "Аккаунт добавлен в группу"
        else:
            return False, "Аккаунт уже находится в этой группе"
        
    except Exception as e:
        session.rollback()
        logger.error(f"❌ Ошибка при добавлении аккаунта в группу: {e}")
        return False, str(e)
    finally:
        session.close()

def remove_account_from_group(account_id: int, group_id: int) -> Tuple[bool, str]:
    """Удаляет аккаунт из группы"""
    session = Session()
    try:
        account = session.query(InstagramAccount).filter_by(id=account_id).first()
        if not account:
            return False, "Аккаунт не найден"
        
        group = session.query(AccountGroup).filter_by(id=group_id).first()
        if not group:
            return False, "Группа не найдена"
        
        if group in account.groups:
            account.groups.remove(group)
            session.commit()
            logger.info(f"✅ Аккаунт {account.username} удален из группы {group.name}")
            return True, "Аккаунт удален из группы"
        else:
            return False, "Аккаунт не находится в этой группе"
        
    except Exception as e:
        session.rollback()
        logger.error(f"❌ Ошибка при удалении аккаунта из группы: {e}")
        return False, str(e)
    finally:
        session.close()

def get_accounts_in_group(group_id: int):
    """Получает список аккаунтов в группе"""
    session = Session()
    try:
        group = session.query(AccountGroup).filter_by(id=group_id).first()
        if not group:
            return []
        
        accounts = group.accounts
        return accounts
        
    except Exception as e:
        logger.error(f"Ошибка при получении аккаунтов группы: {e}")
        return []
    finally:
        session.close()

def get_accounts_without_group():
    """Получает список аккаунтов, не входящих ни в одну группу"""
    session = Session()
    try:
        # Получаем все аккаунты без групп
        accounts = session.query(InstagramAccount).filter(
            ~InstagramAccount.groups.any()
        ).all()
        return accounts
        
    except Exception as e:
        logger.error(f"Ошибка при получении аккаунтов без группы: {e}")
        return []
    finally:
        session.close()

def add_instagram_account_for_user(user_id: int, username, password, email=None, email_password=None):
    """
    🔒 НОВАЯ ФУНКЦИЯ: Добавляет Instagram аккаунт с изоляцией по пользователям
    
    Args:
        user_id (int): ID пользователя Telegram
        username (str): Имя пользователя Instagram
        password (str): Пароль Instagram
        email (str): Email аккаунта
        email_password (str): Пароль от email
    
    Returns:
        InstagramAccount: Созданный аккаунт или None при ошибке
    """
    try:
        session = get_session()
        
        # Проверяем существование аккаунта у ЭТОГО пользователя
        existing_account = session.query(InstagramAccount).filter_by(
            username=username, 
            user_id=user_id
        ).first()
        
        if existing_account:
            logger.warning(f"⚠️ Аккаунт {username} уже существует у пользователя {user_id}")
            session.close()
            return existing_account

        # КРИТИЧЕСКИ ВАЖНО: Создаём аккаунт с user_id для изоляции
        account = InstagramAccount(
            username=username,
            password=password,
            email=email,
            email_password=email_password,
            user_id=user_id,  # 🔒 ИЗОЛЯЦИЯ ПО ПОЛЬЗОВАТЕЛЯМ
            is_active=False,
            created_at=datetime.now()
        )

        session.add(account)
        session.commit()

        logger.info(f"✅ Аккаунт {username} добавлен для пользователя {user_id} с ID {account.id}")
        
        session.close()
        return account

    except Exception as e:
        logger.error(f"❌ Ошибка при добавлении аккаунта {username} для пользователя {user_id}: {str(e)}")
        if session:
            session.rollback()
            session.close()
        return None

def reset_sequences_for_tests():
    """Reset PostgreSQL sequences for primary keys to max(id)+1 to avoid duplicate key errors in tests.
    Safe to call on PostgreSQL; no-op on others.
    """
    try:
        session = get_session()
        def reset_one(table: str, pk: str = 'id'):
            try:
                session.execute(text(
                    f"SELECT setval(pg_get_serial_sequence('{table}', '{pk}'), COALESCE((SELECT MAX({pk}) FROM {table}), 0) + 1, false)"
                ))
            except Exception as e:
                logger.debug(f"Skip resetting sequence for {table}: {e}")
        for t in ['instagram_accounts', 'publish_tasks', 'warmup_tasks', 'telegram_users']:
            reset_one(t, 'id')
        session.commit()
        session.close()
        logger.info("🔧 PK sequences reset to MAX(id)+1 for tests")
    except Exception as e:
        logger.warning(f"Could not reset sequences: {e}")

def backfill_warmup_user_ids(batch_size: int = 1000) -> int:
    """Backfill WarmupTask.user_id from settings->user_id or account.user_id.
    Returns number of rows updated.
    """
    session = get_session()
    updated = 0
    try:
        from database.models import WarmupTask, InstagramAccount
        import json
        # Fetch ids where user_id is NULL
        ids = [row[0] for row in session.query(WarmupTask.id).filter(WarmupTask.user_id == None).limit(batch_size).all()]
        for tid in ids:
            task = session.query(WarmupTask).get(tid)
            uid = None
            try:
                if task.settings:
                    s = task.settings if isinstance(task.settings, dict) else json.loads(task.settings)
                    uid = s.get('user_id')
            except Exception:
                uid = None
            if not uid:
                acc = session.query(InstagramAccount).get(task.account_id)
                uid = getattr(acc, 'user_id', None)
            if uid:
                task.user_id = int(uid)
                updated += 1
        session.commit()
        return updated
    except Exception as e:
        session.rollback()
        logger.warning(f"Warmup backfill failed: {e}")
        return updated
    finally:
        session.close()

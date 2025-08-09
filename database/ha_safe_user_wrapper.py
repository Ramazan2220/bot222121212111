# 🛡️ HIGH AVAILABILITY SAFE USER WRAPPERS
"""
Безопасные обертки для работы с пользовательскими данными
с поддержкой High Availability БД
"""

import logging
from typing import List, Optional, Dict, Any
from sqlalchemy import and_
from sqlalchemy.orm import Session
from database.models import InstagramAccount, PublishTask, WarmupTask, TelegramUser
from database.ha_db_manager import ha_db_manager

logger = logging.getLogger(__name__)

# 📖 READ OPERATIONS (используют Slave DB)

def get_user_instagram_accounts(user_id: int, only_active: bool = True) -> List[InstagramAccount]:
    """
    Получение Instagram аккаунтов пользователя (READ операция)
    Читает со Slave DB для разгрузки Master
    """
    try:
        with ha_db_manager.get_read_session() as session:
            query = session.query(InstagramAccount).filter(
                InstagramAccount.user_id == user_id
            )
            
            if only_active:
                query = query.filter(InstagramAccount.is_active == True)
            
            accounts = query.all()
            logger.info(f"📖 Получено {len(accounts)} аккаунтов для пользователя {user_id}")
            return accounts
            
    except Exception as e:
        logger.error(f"❌ Ошибка получения аккаунтов пользователя {user_id}: {e}")
        return []

def get_user_instagram_account(user_id: int, account_id: int) -> Optional[InstagramAccount]:
    """
    Получение конкретного Instagram аккаунта пользователя (READ операция)
    """
    try:
        with ha_db_manager.get_read_session() as session:
            account = session.query(InstagramAccount).filter(
                and_(
                    InstagramAccount.id == account_id,
                    InstagramAccount.user_id == user_id
                )
            ).first()
            
            if account:
                logger.info(f"📖 Найден аккаунт {account.username} для пользователя {user_id}")
            else:
                logger.warning(f"⚠️ Аккаунт {account_id} не найден для пользователя {user_id}")
                
            return account
            
    except Exception as e:
        logger.error(f"❌ Ошибка получения аккаунта {account_id} пользователя {user_id}: {e}")
        return None

def get_user_publish_tasks(user_id: int, limit: int = 100, status: str = None) -> List[PublishTask]:
    """
    Получение задач публикации пользователя (READ операция)
    """
    try:
        with ha_db_manager.get_read_session() as session:
            query = session.query(PublishTask).filter(
                PublishTask.user_id == user_id
            )
            
            if status:
                query = query.filter(PublishTask.status == status)
            
            tasks = query.order_by(PublishTask.created_at.desc()).limit(limit).all()
            logger.info(f"📖 Получено {len(tasks)} задач публикации для пользователя {user_id}")
            return tasks
            
    except Exception as e:
        logger.error(f"❌ Ошибка получения задач пользователя {user_id}: {e}")
        return []

def get_user_warmup_tasks(user_id: int, limit: int = 100) -> List[WarmupTask]:
    """
    Получение задач прогрева пользователя (READ операция)
    """
    try:
        with ha_db_manager.get_read_session() as session:
            # Получаем аккаунты пользователя
            account_ids = session.query(InstagramAccount.id).filter(
                InstagramAccount.user_id == user_id
            ).subquery()
            
            # Получаем задачи прогрева для этих аккаунтов
            tasks = session.query(WarmupTask).filter(
                WarmupTask.account_id.in_(account_ids)
            ).order_by(WarmupTask.created_at.desc()).limit(limit).all()
            
            logger.info(f"📖 Получено {len(tasks)} задач прогрева для пользователя {user_id}")
            return tasks
            
    except Exception as e:
        logger.error(f"❌ Ошибка получения задач прогрева пользователя {user_id}: {e}")
        return []

def get_user_statistics(user_id: int) -> Dict[str, Any]:
    """
    Получение статистики пользователя (READ операция)
    """
    try:
        with ha_db_manager.get_read_session() as session:
            # Подсчет аккаунтов
            total_accounts = session.query(InstagramAccount).filter(
                InstagramAccount.user_id == user_id
            ).count()
            
            active_accounts = session.query(InstagramAccount).filter(
                and_(
                    InstagramAccount.user_id == user_id,
                    InstagramAccount.is_active == True
                )
            ).count()
            
            # Подсчет задач
            pending_tasks = session.query(PublishTask).filter(
                and_(
                    PublishTask.user_id == user_id,
                    PublishTask.status == 'PENDING'
                )
            ).count()
            
            completed_tasks = session.query(PublishTask).filter(
                and_(
                    PublishTask.user_id == user_id,
                    PublishTask.status == 'COMPLETED'
                )
            ).count()
            
            stats = {
                "user_id": user_id,
                "accounts": {
                    "total": total_accounts,
                    "active": active_accounts,
                    "inactive": total_accounts - active_accounts
                },
                "tasks": {
                    "pending": pending_tasks,
                    "completed": completed_tasks
                }
            }
            
            logger.info(f"📊 Статистика пользователя {user_id}: {stats}")
            return stats
            
    except Exception as e:
        logger.error(f"❌ Ошибка получения статистики пользователя {user_id}: {e}")
        return {"error": str(e)}

# ✍️ WRITE OPERATIONS (используют Master DB)

def create_user_instagram_account(user_id: int, **kwargs) -> Optional[InstagramAccount]:
    """
    Создание Instagram аккаунта для пользователя (WRITE операция)
    Пишет в Master DB
    """
    try:
        with ha_db_manager.get_write_session() as session:
            # Проверяем, что аккаунт с таким username не существует у пользователя
            existing = session.query(InstagramAccount).filter(
                and_(
                    InstagramAccount.user_id == user_id,
                    InstagramAccount.username == kwargs.get('username')
                )
            ).first()
            
            if existing:
                logger.warning(f"⚠️ Аккаунт {kwargs.get('username')} уже существует у пользователя {user_id}")
                return existing
            
            # Создаем новый аккаунт
            account = InstagramAccount(user_id=user_id, **kwargs)
            session.add(account)
            session.commit()
            
            logger.info(f"✅ Создан аккаунт {account.username} для пользователя {user_id}")
            return account
            
    except Exception as e:
        logger.error(f"❌ Ошибка создания аккаунта для пользователя {user_id}: {e}")
        return None

def update_user_instagram_account(user_id: int, account_id: int, **kwargs) -> bool:
    """
    Обновление Instagram аккаунта пользователя (WRITE операция)
    """
    try:
        with ha_db_manager.get_write_session() as session:
            account = session.query(InstagramAccount).filter(
                and_(
                    InstagramAccount.id == account_id,
                    InstagramAccount.user_id == user_id
                )
            ).first()
            
            if not account:
                logger.warning(f"⚠️ Аккаунт {account_id} не найден для пользователя {user_id}")
                return False
            
            # Обновляем поля
            for key, value in kwargs.items():
                if hasattr(account, key):
                    setattr(account, key, value)
            
            session.commit()
            logger.info(f"✅ Обновлен аккаунт {account.username} для пользователя {user_id}")
            return True
            
    except Exception as e:
        logger.error(f"❌ Ошибка обновления аккаунта {account_id} пользователя {user_id}: {e}")
        return False

def delete_user_instagram_account(user_id: int, account_id: int) -> bool:
    """
    Удаление Instagram аккаунта пользователя (WRITE операция)
    """
    try:
        with ha_db_manager.get_write_session() as session:
            account = session.query(InstagramAccount).filter(
                and_(
                    InstagramAccount.id == account_id,
                    InstagramAccount.user_id == user_id
                )
            ).first()
            
            if not account:
                logger.warning(f"⚠️ Аккаунт {account_id} не найден для пользователя {user_id}")
                return False
            
            username = account.username
            session.delete(account)
            session.commit()
            
            logger.info(f"✅ Удален аккаунт {username} для пользователя {user_id}")
            return True
            
    except Exception as e:
        logger.error(f"❌ Ошибка удаления аккаунта {account_id} пользователя {user_id}: {e}")
        return False

def create_user_publish_task(user_id: int, account_id: int, **kwargs) -> Optional[PublishTask]:
    """
    Создание задачи публикации для пользователя (WRITE операция)
    """
    try:
        with ha_db_manager.get_write_session() as session:
            # Проверяем, что аккаунт принадлежит пользователю
            account = session.query(InstagramAccount).filter(
                and_(
                    InstagramAccount.id == account_id,
                    InstagramAccount.user_id == user_id
                )
            ).first()
            
            if not account:
                logger.error(f"❌ Аккаунт {account_id} не принадлежит пользователю {user_id}")
                return None
            
            # Создаем задачу
            task = PublishTask(
                user_id=user_id,
                account_id=account_id,
                **kwargs
            )
            session.add(task)
            session.commit()
            
            logger.info(f"✅ Создана задача публикации для пользователя {user_id}, аккаунт {account.username}")
            return task
            
    except Exception as e:
        logger.error(f"❌ Ошибка создания задачи публикации для пользователя {user_id}: {e}")
        return None

def update_publish_task_status(user_id: int, task_id: int, status: str, **kwargs) -> bool:
    """
    Обновление статуса задачи публикации (WRITE операция)
    """
    try:
        with ha_db_manager.get_write_session() as session:
            task = session.query(PublishTask).filter(
                and_(
                    PublishTask.id == task_id,
                    PublishTask.user_id == user_id
                )
            ).first()
            
            if not task:
                logger.warning(f"⚠️ Задача {task_id} не найдена для пользователя {user_id}")
                return False
            
            task.status = status
            for key, value in kwargs.items():
                if hasattr(task, key):
                    setattr(task, key, value)
            
            session.commit()
            logger.info(f"✅ Обновлен статус задачи {task_id} для пользователя {user_id}: {status}")
            return True
            
    except Exception as e:
        logger.error(f"❌ Ошибка обновления задачи {task_id} пользователя {user_id}: {e}")
        return False

# 🔍 VALIDATION & SECURITY

def validate_user_access_to_account(user_id: int, account_id: int) -> bool:
    """
    Проверка доступа пользователя к аккаунту
    """
    try:
        with ha_db_manager.get_read_session() as session:
            account = session.query(InstagramAccount).filter(
                and_(
                    InstagramAccount.id == account_id,
                    InstagramAccount.user_id == user_id
                )
            ).first()
            
            return account is not None
            
    except Exception as e:
        logger.error(f"❌ Ошибка проверки доступа пользователя {user_id} к аккаунту {account_id}: {e}")
        return False

def validate_user_access_to_task(user_id: int, task_id: int) -> bool:
    """
    Проверка доступа пользователя к задаче
    """
    try:
        with ha_db_manager.get_read_session() as session:
            task = session.query(PublishTask).filter(
                and_(
                    PublishTask.id == task_id,
                    PublishTask.user_id == user_id
                )
            ).first()
            
            return task is not None
            
    except Exception as e:
        logger.error(f"❌ Ошибка проверки доступа пользователя {user_id} к задаче {task_id}: {e}")
        return False

# 📊 MONITORING & HEALTH

def get_ha_database_status() -> Dict[str, Any]:
    """
    Получение статуса High Availability БД
    """
    try:
        replication_status = ha_db_manager.get_replication_status()
        connection_stats = ha_db_manager.get_connection_stats()
        
        return {
            "replication": replication_status,
            "connections": connection_stats,
            "healthy_slaves": len(ha_db_manager.get_healthy_slaves()),
            "total_slaves": len(ha_db_manager.slave_engines)
        }
        
    except Exception as e:
        logger.error(f"❌ Ошибка получения статуса HA БД: {e}")
        return {"error": str(e)}

def test_ha_database_connectivity() -> Dict[str, Any]:
    """
    Тестирование подключений к HA БД
    """
    results = {
        "master_write": False,
        "master_read": False,
        "slaves_read": [],
        "errors": []
    }
    
    # Тест записи в Master
    try:
        with ha_db_manager.get_write_session() as session:
            session.execute("SELECT 1")
            results["master_write"] = True
    except Exception as e:
        results["errors"].append(f"Master write error: {e}")
    
    # Тест чтения с Master
    try:
        with ha_db_manager.get_read_session() as session:
            session.execute("SELECT 1")
            results["master_read"] = True
    except Exception as e:
        results["errors"].append(f"Read error: {e}")
    
    # Тест каждого Slave
    for i, (url, engine) in enumerate(ha_db_manager.slave_engines.items()):
        try:
            with engine.connect() as conn:
                conn.execute("SELECT 1")
                results["slaves_read"].append(f"Slave {i+1}: OK")
        except Exception as e:
            results["slaves_read"].append(f"Slave {i+1}: ERROR - {e}")
    
    return results 
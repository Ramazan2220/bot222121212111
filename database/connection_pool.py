# -*- coding: utf-8 -*-
"""
Database Connection Pool - Пул переиспользуемых соединений с базой данных

Обеспечивает:
- Переиспользование соединений для снижения накладных расходов
- Автоматическое управление жизненным циклом соединений
- Обратную совместимость с существующим кодом
- Метрики и мониторинг производительности
- Thread-safe операции
"""

import time
import logging
import threading
from typing import Optional, Dict, Any
from dataclasses import dataclass
from contextlib import contextmanager
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import StaticPool

from config import ENGINE_POOL_PRE_PING, ENGINE_POOL_USE_LIFO

logger = logging.getLogger(__name__)

@dataclass
class ConnectionStats:
    """Статистика соединений с БД"""
    sessions_created: int = 0
    sessions_reused: int = 0
    active_sessions: int = 0
    total_sessions: int = 0
    avg_session_time: float = 0.0
    peak_sessions: int = 0
    connection_errors: int = 0
    last_activity: float = 0.0

class DatabaseConnectionPool:
    """Пул соединений с базой данных с метриками и автоочисткой"""
    
    def __init__(self, database_url: str, 
                 pool_size: int = 20,
                 max_overflow: int = 30,
                 pool_timeout: int = 30,
                 pool_recycle: int = 3600):
        """
        Инициализация пула соединений
        
        Args:
            database_url: URL подключения к БД
            pool_size: Базовый размер пула
            max_overflow: Максимальное количество дополнительных соединений
            pool_timeout: Таймаут получения соединения (сек)
            pool_recycle: Время жизни соединения (сек)
        """
        self.database_url = database_url
        self.pool_size = pool_size
        self.max_overflow = max_overflow
        self.pool_timeout = pool_timeout
        self.pool_recycle = pool_recycle
        
        # Создаем engine с настройками пула
        if 'sqlite' in database_url.lower():
            # Для SQLite используем StaticPool
            self.engine = create_engine(
                database_url,
                poolclass=StaticPool,
                connect_args={'check_same_thread': False},
                echo=False
            )
        else:
            # Для других БД используем обычный пул
            self.engine = create_engine(
                database_url,
                pool_size=pool_size,
                max_overflow=max_overflow,
                pool_timeout=pool_timeout,
                pool_recycle=pool_recycle,
                pool_pre_ping=ENGINE_POOL_PRE_PING,
                pool_use_lifo=ENGINE_POOL_USE_LIFO,
                echo=False
            )
        
        # Создаем sessionmaker
        self.SessionLocal = sessionmaker(
            bind=self.engine,
            autocommit=False,
            autoflush=False
        )
        
        # Статистика
        self.stats = ConnectionStats()
        self._lock = threading.RLock()
        self._session_times: Dict[int, float] = {}
        
        # Настраиваем события для мониторинга
        self._setup_events()
        
        logger.info(f"🗄️ Database Connection Pool инициализирован: pool_size={pool_size}, max_overflow={max_overflow}")
    
    def _setup_events(self):
        """Настройка событий для мониторинга"""
        
        @event.listens_for(self.engine, "connect")
        def on_connect(dbapi_conn, connection_record):
            with self._lock:
                self.stats.sessions_created += 1
                logger.debug("📊 Новое соединение с БД создано")
        
        @event.listens_for(self.engine, "checkout")
        def on_checkout(dbapi_conn, connection_record, connection_proxy):
            with self._lock:
                self.stats.active_sessions += 1
                self.stats.peak_sessions = max(self.stats.peak_sessions, self.stats.active_sessions)
                self.stats.last_activity = time.time()
        
        @event.listens_for(self.engine, "checkin")
        def on_checkin(dbapi_conn, connection_record):
            with self._lock:
                self.stats.active_sessions = max(0, self.stats.active_sessions - 1)
    
    @contextmanager
    def get_session(self):
        """
        Контекстный менеджер для получения сессии БД
        
        Использование:
            with db_pool.get_session() as session:
                # работа с БД
                pass
        """
        start_time = time.time()
        session = self.SessionLocal()
        session_id = id(session)
        
        try:
            with self._lock:
                self.stats.sessions_created += 1
                self.stats.total_sessions += 1
                self.stats.last_activity = time.time()
                self._session_times[session_id] = start_time
            
            logger.debug(f"📊 Создана сессия БД: {session_id}")
            yield session
            
        except Exception as e:
            logger.error(f"❌ Ошибка в сессии БД {session_id}: {e}")
            with self._lock:
                self.stats.connection_errors += 1
            session.rollback()
            raise
        finally:
            try:
                session.close()
                
                # Обновляем статистику времени
                if session_id in self._session_times:
                    session_time = time.time() - self._session_times[session_id]
                    with self._lock:
                        # Обновляем среднее время сессии
                        if self.stats.avg_session_time == 0:
                            self.stats.avg_session_time = session_time
                        else:
                            self.stats.avg_session_time = (
                                self.stats.avg_session_time * 0.9 + session_time * 0.1
                            )
                        del self._session_times[session_id]
                
                logger.debug(f"📊 Сессия БД {session_id} закрыта")
                
            except Exception as e:
                logger.warning(f"⚠️ Ошибка при закрытии сессии {session_id}: {e}")
    
    def get_session_direct(self) -> Session:
        """
        Получить сессию БД напрямую (для обратной совместимости)
        
        ВАЖНО: Необходимо вызвать session.close() вручную!
        
        Returns:
            Session объект SQLAlchemy
        """
        start_time = time.time()
        session = self.SessionLocal()
        
        with self._lock:
            self.stats.sessions_created += 1
            self.stats.total_sessions += 1
            self.stats.last_activity = time.time()
        
        logger.debug(f"📊 Создана прямая сессия БД: {id(session)}")
        return session
    
    def get_stats(self) -> dict:
        """Получить статистику пула"""
        with self._lock:
            pool_status = {}
            
            # Получаем информацию о пуле если возможно
            try:
                if hasattr(self.engine.pool, 'size'):
                    pool_status.update({
                        'pool_size': self.engine.pool.size(),
                        'checked_in': self.engine.pool.checkedin(),
                        'checked_out': self.engine.pool.checkedout(),
                        'overflow': getattr(self.engine.pool, 'overflow', 0),
                        'invalid': getattr(self.engine.pool, 'invalid', 0)
                    })
            except Exception as e:
                logger.debug(f"Не удалось получить статус пула: {e}")
            
            return {
                'connection_stats': {
                    'sessions_created': self.stats.sessions_created,
                    'sessions_reused': self.stats.sessions_reused,
                    'active_sessions': self.stats.active_sessions,
                    'total_sessions': self.stats.total_sessions,
                    'avg_session_time': round(self.stats.avg_session_time, 4),
                    'peak_sessions': self.stats.peak_sessions,
                    'connection_errors': self.stats.connection_errors,
                    'last_activity': self.stats.last_activity
                },
                'pool_status': pool_status,
                'config': {
                    'pool_size': self.pool_size,
                    'max_overflow': self.max_overflow,
                    'pool_timeout': self.pool_timeout,
                    'pool_recycle': self.pool_recycle
                }
            }
    
    def health_check(self) -> bool:
        """Проверка здоровья пула соединений"""
        try:
            from sqlalchemy import text
            with self.get_session() as session:
                session.execute(text("SELECT 1"))
                return True
        except Exception as e:
            logger.error(f"❌ Проверка здоровья БД не прошла: {e}")
            return False
    
    def reset_stats(self):
        """Сброс статистики"""
        with self._lock:
            self.stats = ConnectionStats()
            self._session_times.clear()
            logger.info("📊 Статистика Database Connection Pool сброшена")
    
    def dispose(self):
        """Закрытие всех соединений"""
        try:
            self.engine.dispose()
            logger.info("🛑 Database Connection Pool: все соединения закрыты")
        except Exception as e:
            logger.error(f"❌ Ошибка при закрытии пула: {e}")

# Глобальный экземпляр пула
_db_pool: Optional[DatabaseConnectionPool] = None

def init_db_pool(database_url: str,
                pool_size: int = 20,
                max_overflow: int = 30,
                pool_timeout: int = 30,
                pool_recycle: int = 3600):
    """Инициализировать глобальный пул соединений БД"""
    global _db_pool
    _db_pool = DatabaseConnectionPool(
        database_url=database_url,
        pool_size=pool_size,
        max_overflow=max_overflow,
        pool_timeout=pool_timeout,
        pool_recycle=pool_recycle
    )
    logger.info("🗄️ Глобальный Database Connection Pool инициализирован")

@contextmanager
def get_db_session():
    """
    Контекстный менеджер для получения сессии из глобального пула
    
    Использование:
        with get_db_session() as session:
            # работа с БД
            pass
    """
    if _db_pool is None:
        raise RuntimeError("Database Connection Pool не инициализирован. Вызовите init_db_pool()")
    
    with _db_pool.get_session() as session:
        yield session

def get_session_direct() -> Session:
    """
    Получить сессию БД напрямую из глобального пула (для обратной совместимости)
    
    ВАЖНО: Необходимо вызвать session.close() вручную!
    
    Returns:
        Session объект SQLAlchemy
    """
    if _db_pool is None:
        raise RuntimeError("Database Connection Pool не инициализирован. Вызовите init_db_pool()")
    
    return _db_pool.get_session_direct()

def get_db_stats() -> dict:
    """Получить статистику глобального пула БД"""
    if _db_pool:
        return _db_pool.get_stats()
    return {'error': 'Pool not initialized'}

def db_health_check() -> bool:
    """Проверка здоровья глобального пула БД"""
    if _db_pool:
        return _db_pool.health_check()
    return False

def reset_db_stats():
    """Сброс статистики глобального пула БД"""
    if _db_pool:
        _db_pool.reset_stats()

def dispose_db_pool():
    """Закрытие глобального пула БД"""
    global _db_pool
    if _db_pool:
        _db_pool.dispose()
        _db_pool = None

logger.info("📦 Database Connection Pool модуль загружен") 
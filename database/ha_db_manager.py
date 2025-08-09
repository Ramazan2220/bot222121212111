# 🛡️ HIGH AVAILABILITY DATABASE MANAGER
import os
import random
import logging
import time
from contextlib import contextmanager
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import QueuePool
from sqlalchemy.exc import OperationalError, DisconnectionError
import psycopg2

logger = logging.getLogger(__name__)

class HADatabaseManager:
    """High Availability Database Manager с автоматическим failover"""
    
    def __init__(self):
        # URLs для подключения
        self.master_url = os.getenv('DATABASE_MASTER_URL')
        self.slave_urls = [
            url for url in [
                os.getenv('DATABASE_SLAVE1_URL'),
                os.getenv('DATABASE_SLAVE2_URL'),
                os.getenv('DATABASE_SLAVE3_URL'),
            ] if url
        ]
        
        # Инициализация engines
        self._init_engines()
        
        # Состояние серверов
        self.master_healthy = True
        self.slave_health = {url: True for url in self.slave_urls}
        
        # Последняя проверка здоровья
        self.last_health_check = 0
        self.health_check_interval = 30  # секунд
    
    def _init_engines(self):
        """Инициализация подключений к БД"""
        
        # Master engine (для записи)
        if self.master_url:
            self.master_engine = create_engine(
                self.master_url,
                poolclass=QueuePool,
                pool_size=50,
                max_overflow=100,
                pool_timeout=60,
                pool_pre_ping=True,  # Проверка соединений
                pool_recycle=3600,   # Пересоздание через час
                echo=False
            )
        else:
            self.master_engine = None
            logger.error("❌ Master database URL не настроен!")
        
        # Slave engines (для чтения)
        self.slave_engines = {}
        for i, url in enumerate(self.slave_urls):
            try:
                engine = create_engine(
                    url,
                    poolclass=QueuePool,
                    pool_size=20,
                    max_overflow=40,
                    pool_timeout=30,
                    pool_pre_ping=True,
                    pool_recycle=3600,
                    echo=False
                )
                self.slave_engines[url] = engine
                logger.info(f"✅ Slave {i+1} подключен: {url[:50]}...")
            except Exception as e:
                logger.error(f"❌ Ошибка подключения к Slave {i+1}: {e}")
    
    def _check_health(self):
        """Проверка здоровья всех БД"""
        current_time = time.time()
        if current_time - self.last_health_check < self.health_check_interval:
            return
        
        self.last_health_check = current_time
        
        # Проверка Master
        if self.master_engine:
            try:
                with self.master_engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                    if not self.master_healthy:
                        logger.info("✅ Master DB восстановлен!")
                    self.master_healthy = True
            except Exception as e:
                if self.master_healthy:
                    logger.error(f"❌ Master DB недоступен: {e}")
                self.master_healthy = False
        
        # Проверка Slaves
        for url, engine in self.slave_engines.items():
            try:
                with engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                    if not self.slave_health[url]:
                        logger.info(f"✅ Slave DB восстановлен: {url[:50]}...")
                    self.slave_health[url] = True
            except Exception as e:
                if self.slave_health[url]:
                    logger.error(f"❌ Slave DB недоступен: {e}")
                self.slave_health[url] = False
    
    def get_healthy_slaves(self):
        """Получить список здоровых Slave engines"""
        self._check_health()
        return [
            engine for url, engine in self.slave_engines.items()
            if self.slave_health[url]
        ]
    
    @contextmanager
    def get_write_session(self):
        """Сессия для записи (всегда Master с fallback)"""
        self._check_health()
        
        # Пробуем Master
        if self.master_healthy and self.master_engine:
            try:
                Session = sessionmaker(bind=self.master_engine)
                session = Session()
                yield session
                return
            except (OperationalError, DisconnectionError) as e:
                logger.error(f"❌ Ошибка записи в Master: {e}")
                self.master_healthy = False
        
        # Если Master недоступен - пробуем промотировать Slave
        healthy_slaves = self.get_healthy_slaves()
        if healthy_slaves:
            logger.warning("⚠️ Master недоступен! Пытаемся записать в Slave...")
            try:
                # Берем первый здоровый Slave
                slave_engine = healthy_slaves[0]
                Session = sessionmaker(bind=slave_engine)
                session = Session()
                yield session
                return
            except Exception as e:
                logger.error(f"❌ Ошибка записи в Slave: {e}")
        
        # Если ничего не работает
        raise Exception("❌ Все базы данных недоступны для записи!")
    
    @contextmanager 
    def get_read_session(self):
        """Сессия для чтения (Slaves с fallback на Master)"""
        self._check_health()
        
        # Пробуем читать со Slaves
        healthy_slaves = self.get_healthy_slaves()
        if healthy_slaves:
            try:
                # Round-robin по здоровым Slaves
                slave_engine = random.choice(healthy_slaves)
                Session = sessionmaker(bind=slave_engine)
                session = Session()
                yield session
                return
            except (OperationalError, DisconnectionError) as e:
                logger.warning(f"⚠️ Ошибка чтения со Slave: {e}")
        
        # Fallback на Master
        if self.master_healthy and self.master_engine:
            try:
                logger.info("📖 Читаем с Master (Slaves недоступны)")
                Session = sessionmaker(bind=self.master_engine)
                session = Session()
                yield session
                return
            except Exception as e:
                logger.error(f"❌ Ошибка чтения с Master: {e}")
        
        # Если ничего не работает
        raise Exception("❌ Все базы данных недоступны для чтения!")
    
    def get_replication_status(self):
        """Получить статус репликации"""
        if not self.master_healthy or not self.master_engine:
            return {"error": "Master недоступен"}
        
        try:
            with self.master_engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT 
                        client_addr,
                        application_name,
                        state,
                        pg_wal_lsn_diff(pg_current_wal_lsn(), sent_lsn) as send_lag_bytes,
                        pg_wal_lsn_diff(sent_lsn, flush_lsn) as flush_lag_bytes,
                        pg_wal_lsn_diff(flush_lsn, replay_lsn) as replay_lag_bytes
                    FROM pg_stat_replication
                """))
                
                replicas = []
                for row in result:
                    replicas.append({
                        "client_addr": row[0],
                        "application_name": row[1],
                        "state": row[2],
                        "send_lag_mb": (row[3] or 0) / 1024 / 1024,
                        "flush_lag_mb": (row[4] or 0) / 1024 / 1024,
                        "replay_lag_mb": (row[5] or 0) / 1024 / 1024
                    })
                
                return {
                    "master_healthy": self.master_healthy,
                    "slaves_count": len(self.slave_engines),
                    "healthy_slaves": len(self.get_healthy_slaves()),
                    "replicas": replicas
                }
        
        except Exception as e:
            logger.error(f"❌ Ошибка получения статуса репликации: {e}")
            return {"error": str(e)}
    
    def force_failover(self, target_slave_url=None):
        """Принудительное переключение на Slave"""
        logger.warning("🔄 Запущен принудительный failover...")
        
        healthy_slaves = self.get_healthy_slaves()
        if not healthy_slaves:
            raise Exception("❌ Нет здоровых Slaves для failover!")
        
        # Выбираем целевой Slave
        if target_slave_url and target_slave_url in self.slave_engines:
            target_engine = self.slave_engines[target_slave_url]
        else:
            target_engine = healthy_slaves[0]
            target_slave_url = next(
                url for url, engine in self.slave_engines.items() 
                if engine == target_engine
            )
        
        try:
            # Пытаемся промотировать Slave
            # (В реальности это должно быть автоматизировано через patroni/repmgr)
            logger.info(f"🔄 Промотируем Slave: {target_slave_url[:50]}...")
            
            # Временно делаем этот Slave новым Master
            self.master_engine = target_engine
            self.master_healthy = True
            
            # Удаляем его из списка Slaves
            del self.slave_engines[target_slave_url]
            del self.slave_health[target_slave_url]
            
            logger.info("✅ Failover выполнен успешно!")
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка failover: {e}")
            return False
    
    def get_connection_stats(self):
        """Статистика подключений"""
        stats = {
            "master": {"pool_size": 0, "checked_out": 0, "checked_in": 0},
            "slaves": []
        }
        
        try:
            if self.master_engine:
                pool = self.master_engine.pool
                stats["master"] = {
                    "pool_size": pool.size(),
                    "checked_out": pool.checkedout(),
                    "checked_in": pool.checkedin(),
                    "healthy": self.master_healthy
                }
            
            for i, (url, engine) in enumerate(self.slave_engines.items()):
                pool = engine.pool
                stats["slaves"].append({
                    "slave_id": i + 1,
                    "url": url[:50] + "...",
                    "pool_size": pool.size(),
                    "checked_out": pool.checkedout(),
                    "checked_in": pool.checkedin(),
                    "healthy": self.slave_health[url]
                })
        
        except Exception as e:
            logger.error(f"❌ Ошибка получения статистики: {e}")
            stats["error"] = str(e)
        
        return stats

# Глобальный экземпляр
ha_db_manager = HADatabaseManager()

# Совместимость со старым кодом
def get_session():
    """Backward compatibility - читаем со Slaves"""
    return ha_db_manager.get_read_session()

def init_db():
    """Инициализация БД"""
    from database.models import Base
    
    # Создаем таблицы на Master
    if ha_db_manager.master_engine:
        Base.metadata.create_all(ha_db_manager.master_engine)
        logger.info("✅ Таблицы созданы на Master DB")
    else:
        logger.error("❌ Master DB недоступен для создания таблиц!") 
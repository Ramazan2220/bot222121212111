import logging
import imaplib
import threading
import time
from typing import Optional, Dict, Set, Tuple, List
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from queue import Queue, Empty
from contextlib import contextmanager

logger = logging.getLogger(__name__)

@dataclass
class IMAPConnection:
    """Представляет одно IMAP соединение"""
    mail: imaplib.IMAP4_SSL
    email: str
    created_at: datetime = field(default_factory=datetime.now)
    last_used: datetime = field(default_factory=datetime.now)
    is_busy: bool = False
    use_count: int = 0

class IMAPConnectionPool:
    """Пул IMAP соединений с автоматическим управлением жизненным циклом"""
    
    def __init__(self, 
                 max_connections_per_email: int = 2,
                 max_idle_time_minutes: int = 60,
                 max_connection_lifetime_hours: int = 12,
                 cleanup_interval_minutes: int = 5):
        
        self.max_connections_per_email = max_connections_per_email
        self.max_idle_time = timedelta(minutes=max_idle_time_minutes)
        self.max_lifetime = timedelta(hours=max_connection_lifetime_hours)
        self.cleanup_interval = timedelta(minutes=cleanup_interval_minutes)
        
        # Структуры данных
        self._pools: Dict[str, Queue] = {}  # email -> Queue[IMAPConnection]
        self._active_connections: Dict[str, List[IMAPConnection]] = {}  # Изменено с Set на List
        self._connection_count: Dict[str, int] = {}
        
        # Блокировки
        self._pool_lock = threading.RLock()
        self._cleanup_lock = threading.Lock()
        
        # Фоновая очистка
        self._cleanup_thread = None
        self._shutdown = False
        # Отложенный запуск cleanup thread - только когда нужен
        self._cleanup_thread_started = False
        
        # Статистика
        self.stats = {
            'connections_created': 0,
            'connections_reused': 0,
            'connections_expired': 0,
            'cleanup_runs': 0
        }
    
    def _start_cleanup_thread(self):
        """Запускает фоновый поток очистки"""
        def cleanup_worker():
            while not self._shutdown:
                try:
                    time.sleep(self.cleanup_interval.total_seconds())
                    if not self._shutdown:
                        self._cleanup_expired_connections()
                except Exception as e:
                    logger.error(f"Ошибка в cleanup_worker: {e}")
        
        self._cleanup_thread = threading.Thread(target=cleanup_worker, daemon=True)
        self._cleanup_thread.start()
        logger.info(f"🔄 Запущена автоочистка пула IMAP (интервал: {self.cleanup_interval})")
    
    def _get_imap_server(self, email: str) -> str:
        """Определяет IMAP сервер по email"""
        domain = email.split('@')[1].lower()
        
        # Мappинг доменов на IMAP серверы
        imap_servers = {
            'firstmail.ltd': 'imap.firstmail.ltd',
            'wildbmail.com': 'imap.firstmail.ltd',  # Предполагаем тот же сервер
            'gmail.com': 'imap.gmail.com',
            'outlook.com': 'imap-mail.outlook.com',
            'hotmail.com': 'imap-mail.outlook.com',
            'yahoo.com': 'imap.mail.yahoo.com',
        }
        
        return imap_servers.get(domain, 'imap.firstmail.ltd')  # Дефолт
    
    def _create_connection(self, email: str, password: str) -> Optional[IMAPConnection]:
        """Создает новое IMAP соединение"""
        try:
            imap_server = self._get_imap_server(email)
            logger.debug(f"🔌 Создание IMAP соединения: {email} -> {imap_server}")
            
            mail = imaplib.IMAP4_SSL(imap_server, 993)
            mail.login(email, password)
            
            connection = IMAPConnection(
                mail=mail,
                email=email,
                created_at=datetime.now(),
                last_used=datetime.now()
            )
            
            with self._pool_lock:
                self.stats['connections_created'] += 1
            
            logger.info(f"✅ IMAP соединение создано для {email}")
            return connection
            
        except Exception as e:
            logger.error(f"❌ Не удалось создать IMAP соединение для {email}: {e}")
            return None
    
    @contextmanager
    def get_connection(self, email: str, password: str):
        """Контекстный менеджер для получения IMAP соединения из пула"""
        # Ленивый запуск cleanup thread при первом использовании
        if not self._cleanup_thread_started:
            self._start_cleanup_thread()
            self._cleanup_thread_started = True
            
        connection = None
        
        try:
            # Пытаемся получить из пула
            connection = self._get_from_pool(email, password)
            
            if connection:
                connection.is_busy = True
                connection.last_used = datetime.now()
                connection.use_count += 1
                
                with self._pool_lock:
                    self.stats['connections_reused'] += 1
                
                logger.debug(f"♻️ Переиспользовано соединение для {email} (использований: {connection.use_count})")
            
            yield connection
            
        finally:
            # Возвращаем в пул
            if connection:
                connection.is_busy = False
                connection.last_used = datetime.now()
                self._return_to_pool(connection)
    
    def _get_from_pool(self, email: str, password: str) -> Optional[IMAPConnection]:
        """Получает соединение из пула или создает новое"""
        with self._pool_lock:
            # Инициализируем структуры для email если нужно
            if email not in self._pools:
                self._pools[email] = Queue()
                self._active_connections[email] = []
                self._connection_count[email] = 0
            
            # Пытаемся получить из очереди
            try:
                connection = self._pools[email].get_nowait()
                
                # Проверяем что соединение еще живо
                if self._is_connection_valid(connection):
                    return connection
                else:
                    # Соединение протухло, закрываем
                    self._close_connection(connection)
                    self._connection_count[email] -= 1
            
            except Empty:
                pass
            
            # Создаем новое соединение если есть место
            if self._connection_count[email] < self.max_connections_per_email:
                connection = self._create_connection(email, password)
                if connection:
                    self._connection_count[email] += 1
                    self._active_connections[email].append(connection)
                    return connection
            
            return None
    
    def _return_to_pool(self, connection: IMAPConnection):
        """Возвращает соединение в пул"""
        if not connection:
            return
            
        with self._pool_lock:
            email = connection.email
            
            # Проверяем что соединение еще валидно
            if self._is_connection_valid(connection):
                self._pools[email].put(connection)
                logger.debug(f"🔄 Соединение возвращено в пул: {email}")
            else:
                # Закрываем протухшее соединение
                self._close_connection(connection)
                if email in self._active_connections:
                    self._active_connections[email].remove(connection)
                self._connection_count[email] -= 1
                logger.debug(f"❌ Протухшее соединение закрыто: {email}")
    
    def _is_connection_valid(self, connection: IMAPConnection) -> bool:
        """Проверяет валидность соединения"""
        if not connection or not connection.mail:
            return False
        
        now = datetime.now()
        
        # Проверяем возраст
        if now - connection.created_at > self.max_lifetime:
            return False
        
        # Проверяем idle время
        if now - connection.last_used > self.max_idle_time:
            return False
        
        # Проверяем что соединение активно
        try:
            connection.mail.noop()  # Ping IMAP сервера
            return True
        except:
            return False
    
    def _close_connection(self, connection: IMAPConnection):
        """Безопасно закрывает IMAP соединение"""
        try:
            if connection.mail:
                connection.mail.logout()
                logger.debug(f"🔌 IMAP соединение закрыто для {connection.email}")
        except:
            pass  # Игнорируем ошибки при закрытии
    
    def _cleanup_expired_connections(self):
        """Очищает протухшие соединения"""
        if not self._cleanup_lock.acquire(blocking=False):
            return  # Другая очистка уже идет
        
        try:
            with self._pool_lock:
                self.stats['cleanup_runs'] += 1
                expired_count = 0
                
                for email in list(self._pools.keys()):
                    # Очищаем очередь от протухших соединений
                    new_queue = Queue()
                    
                    while not self._pools[email].empty():
                        try:
                            connection = self._pools[email].get_nowait()
                            if self._is_connection_valid(connection):
                                new_queue.put(connection)
                            else:
                                self._close_connection(connection)
                                self._active_connections[email].remove(connection)
                                self._connection_count[email] -= 1
                                expired_count += 1
                        except Empty:
                            break
                    
                    self._pools[email] = new_queue
                
                if expired_count > 0:
                    self.stats['connections_expired'] += expired_count
                    logger.info(f"🧹 Очистка пула: удалено {expired_count} протухших соединений")
        
        finally:
            self._cleanup_lock.release()
    
    def get_stats(self) -> dict:
        """Возвращает статистику пула"""
        with self._pool_lock:
            total_active = sum(self._connection_count.values())
            return {
                **self.stats,
                'active_connections_total': total_active,
                'pools_count': len(self._pools),
                'active_by_email': dict(self._connection_count)
            }
    
    def shutdown(self):
        """Завершает работу пула и закрывает все соединения"""
        logger.info("🛑 Завершение работы IMAP пула...")
        
        self._shutdown = True
        
        # Ждем завершения cleanup thread
        if self._cleanup_thread:
            self._cleanup_thread.join(timeout=5)
        
        # Закрываем все соединения
        with self._pool_lock:
            for email in list(self._pools.keys()):
                while not self._pools[email].empty():
                    try:
                        connection = self._pools[email].get_nowait()
                        self._close_connection(connection)
                    except Empty:
                        break
                
                for connection in self._active_connections[email]:
                    self._close_connection(connection)
        
        logger.info("✅ IMAP пул завершен")

# Глобальный экземпляр пула
imap_pool = IMAPConnectionPool()

# Функция для получения соединения (совместимость с существующим кодом)
def get_imap_connection(email: str, password: str):
    """Получает IMAP соединение из пула"""
    return imap_pool.get_connection(email, password) 

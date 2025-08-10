from sqlalchemy import Column, Integer, BigInteger, String, Boolean, DateTime, ForeignKey, Text, Float, JSON, Enum, Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from datetime import datetime
import enum

# Для совместимости с PostgreSQL
from sqlalchemy.dialects import postgresql

Base = declarative_base()

# Таблица связи между аккаунтами и прокси
account_proxy = Table(
    'account_proxy',
    Base.metadata,
    Column('account_id', Integer, ForeignKey('instagram_accounts.id'), primary_key=True),
    Column('proxy_id', Integer, ForeignKey('proxies.id'), primary_key=True)
)

# Таблица связи между аккаунтами и группами
account_groups = Table(
    'account_groups',
    Base.metadata,
    Column('account_id', Integer, ForeignKey('instagram_accounts.id'), primary_key=True),
    Column('group_id', Integer, ForeignKey('account_groups_table.id'), primary_key=True)
)

# Базовые Python Enum классы
class TaskStatus(enum.Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    SCHEDULED = "scheduled"

class TaskType(enum.Enum):
    VIDEO = "video"
    PHOTO = "photo"
    CAROUSEL = "carousel"
    STORY = "story"
    REEL = "reel"
    REELS = "reels"  # Добавляем для совместимости
    IGTV = "igtv"

class ContentType(enum.Enum):
    PHOTO = "photo"
    VIDEO = "video"
    CAROUSEL = "carousel"
    STORY = "story"

# SQLAlchemy ENUM типы для PostgreSQL совместимости
TaskStatusEnum = Enum(TaskStatus, name='task_status')
TaskTypeEnum = Enum(TaskType, name='task_type')
ContentTypeEnum = Enum(ContentType, name='content_type')

class AccountGroup(Base):
    __tablename__ = 'account_groups_table'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    description = Column(Text, nullable=True)
    icon = Column(String(10), default='📁')  # Эмодзи иконка для группы
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    
    # Отношения
    accounts = relationship("InstagramAccount", secondary=account_groups, back_populates="groups")

class Proxy(Base):
    __tablename__ = 'proxies'

    id = Column(Integer, primary_key=True)
    host = Column(String(255), nullable=False)
    port = Column(Integer, nullable=False)
    username = Column(String(255), nullable=True)
    password = Column(String(255), nullable=True)
    protocol = Column(String(10), default='http')
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    # Отношения
    accounts = relationship("InstagramAccount", secondary=account_proxy, back_populates="proxies")

class InstagramAccount(Base):
    __tablename__ = 'instagram_accounts'

    id = Column(Integer, primary_key=True)
    username = Column(String(255), nullable=False, unique=True)
    password = Column(String(255), nullable=False)
    email = Column(String(255))
    email_password = Column(String(255))
    phone = Column(String(255))  # Телефон
    website = Column(String(255))  # Веб-сайт
    full_name = Column(String(255))  # Добавляем поле для полного имени
    biography = Column(Text)  # Добавляем поле для описания профиля
    device_id = Column(String(255), nullable=True)  # Уникальный ID устройства для Instagram
    
    # ✅ КРИТИЧЕСКИ ВАЖНО: Изоляция данных по пользователям
    user_id = Column(BigInteger, nullable=False, index=True)  # ID пользователя Telegram для изоляции
    
    is_active = Column(Boolean, default=True)
    status = Column(String(50), default='active')  # Статус аккаунта: active, inactive, banned, etc.
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    session_data = Column(Text)
    last_error = Column(Text, nullable=True)  # Последняя ошибка при проверке
    last_check = Column(DateTime, nullable=True)

    # Отношения
    proxies = relationship("Proxy", secondary=account_proxy, back_populates="accounts")
    tasks = relationship("PublishTask", back_populates="account")
    proxy_id = Column(Integer, ForeignKey('proxies.id'), nullable=True)
    proxy = relationship("Proxy", foreign_keys=[proxy_id])
    groups = relationship("AccountGroup", secondary=account_groups, back_populates="accounts")

class PublishTask(Base):
    __tablename__ = 'publish_tasks'

    id = Column(Integer, primary_key=True)
    account_id = Column(Integer, ForeignKey('instagram_accounts.id'), nullable=False)
    user_id = Column(Integer, nullable=True)  # ID пользователя Telegram который создал задачу
    task_type = Column(Enum(TaskType), nullable=False)
    status = Column(Enum(TaskStatus), default=TaskStatus.PENDING)
    media_path = Column(String(255), nullable=True)  # Путь к медиафайлу
    caption = Column(Text, nullable=True)  # Текст публикации
    hashtags = Column(Text, nullable=True)  # Хэштеги
    scheduled_time = Column(DateTime, nullable=True)  # Время запланированной публикации
    completed_time = Column(DateTime, nullable=True)  # Время выполнения задачи
    error_message = Column(Text, nullable=True)  # Сообщение об ошибке, если задача не выполнена
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    # Дополнительные поля для разных типов публикаций
    location = Column(String(255), nullable=True)  # Местоположение
    first_comment = Column(Text, nullable=True)  # Первый комментарий
    options = Column(JSON, nullable=True)  # Дополнительные опции в формате JSON

    # Для карусели
    media_paths = Column(JSON, nullable=True)  # Список путей к медиафайлам для карусели

    # Для историй
    story_options = Column(JSON, nullable=True)  # Опции для историй (стикеры, опросы и т.д.)

    # Для рилс
    audio_path = Column(String(255), nullable=True)  # Путь к аудиофайлу для рилс
    audio_start_time = Column(Float, nullable=True)  # Время начала аудио
    
    # ID опубликованного поста в Instagram
    media_id = Column(String(255), nullable=True)  # ID медиа в Instagram после публикации

    # Отношения
    account = relationship("InstagramAccount", back_populates="tasks")

class TelegramUser(Base):
    __tablename__ = 'telegram_users'

    id = Column(Integer, primary_key=True)
    telegram_id = Column(BigInteger, unique=True, nullable=False)
    username = Column(String(255), nullable=True)
    first_name = Column(String(255), nullable=True)
    last_name = Column(String(255), nullable=True)
    is_admin = Column(Boolean, default=False)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    last_activity = Column(DateTime, nullable=True)

class Setting(Base):
    __tablename__ = 'settings'

    id = Column(Integer, primary_key=True)
    key = Column(String(255), unique=True, nullable=False)
    value = Column(Text, nullable=True)
    description = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

class Log(Base):
    __tablename__ = 'logs'

    id = Column(Integer, primary_key=True)
    level = Column(String(20), nullable=False)  # INFO, WARNING, ERROR, DEBUG
    message = Column(Text, nullable=False)
    source = Column(String(255), nullable=True)  # Источник лога (модуль, функция)
    created_at = Column(DateTime, default=datetime.now)

    # Дополнительные данные
    data = Column(JSON, nullable=True)  # Дополнительные данные в формате JSON

class WarmupStatus(enum.Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

class WarmupTask(Base):
    __tablename__ = 'warmup_tasks'

    id = Column(Integer, primary_key=True)
    account_id = Column(Integer, ForeignKey('instagram_accounts.id'), nullable=False)
    status = Column(Enum(WarmupStatus), default=WarmupStatus.PENDING)
    settings = Column(JSON, nullable=False)  # Настройки прогрева
    progress = Column(JSON, nullable=True)  # Прогресс выполнения
    error = Column(Text, nullable=True)  # Сообщение об ошибке
    created_at = Column(DateTime, default=datetime.now)
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    # Новый столбец для изоляции: может быть NULL до миграции и бэкфилла
    user_id = Column(BigInteger, nullable=True, index=True)
    
    # Отношения
    account = relationship("InstagramAccount")

class FollowTaskStatus(enum.Enum):
    PENDING = "pending"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    STOPPED = "stopped"

class FollowSourceType(enum.Enum):
    FOLLOWERS = "followers"      # Подписчики аккаунта
    FOLLOWING = "following"       # Подписки аккаунта
    HASHTAG = "hashtag"          # По хештегу
    LOCATION = "location"        # По геолокации
    LIKERS = "likers"           # Лайкнувшие пост
    COMMENTERS = "commenters"    # Комментаторы поста

class FollowTask(Base):
    __tablename__ = 'follow_tasks'

    id = Column(Integer, primary_key=True)
    account_id = Column(Integer, ForeignKey('instagram_accounts.id'), nullable=False)
    name = Column(String(255), nullable=False)  # Название задачи
    source_type = Column(Enum(FollowSourceType), nullable=False)
    source_value = Column(String(255), nullable=False)  # username, hashtag, location name или URL поста
    status = Column(Enum(FollowTaskStatus), default=FollowTaskStatus.PENDING)
    
    # Настройки скорости
    follows_per_hour = Column(Integer, default=20)
    follow_limit = Column(Integer, default=500)  # Лимит подписок для задачи
    
    # Фильтры
    filters = Column(JSON, nullable=True)  # skip_private, skip_no_avatar, only_business, min_followers, max_followers
    
    # Статистика
    followed_count = Column(Integer, default=0)  # Количество выполненных подписок
    skipped_count = Column(Integer, default=0)   # Количество пропущенных аккаунтов
    failed_count = Column(Integer, default=0)    # Количество неудачных попыток
    
    # Список обработанных пользователей (чтобы не подписываться дважды)
    processed_users = Column(JSON, default=list)  # Список user_id уже обработанных
    
    # Время
    created_at = Column(DateTime, default=datetime.now)
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    last_action_at = Column(DateTime, nullable=True)  # Время последней подписки
    
    # Ошибки
    error = Column(Text, nullable=True)
    
    # Отношения
    account = relationship("InstagramAccount")

# Таблица для хранения уникальных подписок для каждого аккаунта
class FollowHistory(Base):
    __tablename__ = 'follow_history'
    
    id = Column(Integer, primary_key=True)
    account_id = Column(Integer, ForeignKey('instagram_accounts.id'), nullable=False)
    target_user_id = Column(String(255), nullable=False)  # ID пользователя в Instagram
    target_username = Column(String(255), nullable=True)  # Username для истории
    followed_at = Column(DateTime, default=datetime.now)
    unfollowed_at = Column(DateTime, nullable=True)  # Если отписались
    task_id = Column(Integer, ForeignKey('follow_tasks.id'), nullable=True)
    
    # Отношения
    account = relationship("InstagramAccount")
    task = relationship("FollowTask")

#!/usr/bin/env python3
"""
Скрипт для восстановления файла database/models.py
"""

import os
import shutil

def create_models_file():
    """Создает файл database/models.py с правильным содержимым"""
    
    models_content = '''from sqlalchemy import Column, Integer, String, Boolean, DateTime, ForeignKey, Text, Float, JSON, Enum, Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from datetime import datetime
import enum

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
    REELS = "reels"
    IGTV = "igtv"

class AccountGroup(Base):
    __tablename__ = 'account_groups_table'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    description = Column(Text, nullable=True)
    icon = Column(String(10), default='📁')
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    
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

    accounts = relationship("InstagramAccount", secondary=account_proxy, back_populates="proxies")

class InstagramAccount(Base):
    __tablename__ = 'instagram_accounts'

    id = Column(Integer, primary_key=True)
    username = Column(String(255), nullable=False, unique=True)
    password = Column(String(255), nullable=False)
    email = Column(String(255))
    email_password = Column(String(255))
    phone = Column(String(255))
    website = Column(String(255))
    full_name = Column(String(255))
    biography = Column(Text)
    device_id = Column(String(255), nullable=True)
    is_active = Column(Boolean, default=True)
    status = Column(String(50), default='active')
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    session_data = Column(Text)
    last_error = Column(Text, nullable=True)
    last_check = Column(DateTime, nullable=True)

    proxies = relationship("Proxy", secondary=account_proxy, back_populates="accounts")
    tasks = relationship("PublishTask", back_populates="account")
    proxy_id = Column(Integer, ForeignKey('proxies.id'), nullable=True)
    proxy = relationship("Proxy", foreign_keys=[proxy_id])
    groups = relationship("AccountGroup", secondary=account_groups, back_populates="accounts")

class PublishTask(Base):
    __tablename__ = 'publish_tasks'

    id = Column(Integer, primary_key=True)
    account_id = Column(Integer, ForeignKey('instagram_accounts.id'), nullable=False)
    user_id = Column(Integer, nullable=True)
    task_type = Column(Enum(TaskType), nullable=False)
    status = Column(Enum(TaskStatus), default=TaskStatus.PENDING)
    media_path = Column(String(255), nullable=True)
    caption = Column(Text, nullable=True)
    hashtags = Column(Text, nullable=True)
    scheduled_time = Column(DateTime, nullable=True)
    completed_time = Column(DateTime, nullable=True)
    error_message = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    location = Column(String(255), nullable=True)
    first_comment = Column(Text, nullable=True)
    options = Column(JSON, nullable=True)
    media_paths = Column(JSON, nullable=True)
    story_options = Column(JSON, nullable=True)
    audio_path = Column(String(255), nullable=True)
    audio_start_time = Column(Float, nullable=True)
    media_id = Column(String(255), nullable=True)

    account = relationship("InstagramAccount", back_populates="tasks")

class TelegramUser(Base):
    __tablename__ = 'telegram_users'

    id = Column(Integer, primary_key=True)
    telegram_id = Column(Integer, unique=True, nullable=False)
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
    level = Column(String(20), nullable=False)
    message = Column(Text, nullable=False)
    source = Column(String(255), nullable=True)
    created_at = Column(DateTime, default=datetime.now)
    data = Column(JSON, nullable=True)

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
    settings = Column(JSON, nullable=False)
    progress = Column(JSON, nullable=True)
    error = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.now)
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    
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
    FOLLOWERS = "followers"
    FOLLOWING = "following"
    HASHTAG = "hashtag"
    LOCATION = "location"
    LIKERS = "likers"
    COMMENTERS = "commenters"

class FollowTask(Base):
    __tablename__ = 'follow_tasks'

    id = Column(Integer, primary_key=True)
    account_id = Column(Integer, ForeignKey('instagram_accounts.id'), nullable=False)
    name = Column(String(255), nullable=False)
    source_type = Column(Enum(FollowSourceType), nullable=False)
    source_value = Column(String(255), nullable=False)
    status = Column(Enum(FollowTaskStatus), default=FollowTaskStatus.PENDING)
    
    follows_per_hour = Column(Integer, default=20)
    follow_limit = Column(Integer, default=500)
    filters = Column(JSON, nullable=True)
    followed_count = Column(Integer, default=0)
    skipped_count = Column(Integer, default=0)
    failed_count = Column(Integer, default=0)
    processed_users = Column(JSON, default=list)
    
    created_at = Column(DateTime, default=datetime.now)
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    last_action_at = Column(DateTime, nullable=True)
    error = Column(Text, nullable=True)
    
    account = relationship("InstagramAccount")

class FollowHistory(Base):
    __tablename__ = 'follow_history'
    
    id = Column(Integer, primary_key=True)
    account_id = Column(Integer, ForeignKey('instagram_accounts.id'), nullable=False)
    target_user_id = Column(String(255), nullable=False)
    target_username = Column(String(255), nullable=True)
    followed_at = Column(DateTime, default=datetime.now)
    unfollowed_at = Column(DateTime, nullable=True)
    task_id = Column(Integer, ForeignKey('follow_tasks.id'), nullable=True)
    
    account = relationship("InstagramAccount")
    task = relationship("FollowTask")
'''

    # Создаем папку database если не существует
    os.makedirs('database', exist_ok=True)
    
    # Записываем файл models.py
    with open('database/models.py', 'w', encoding='utf-8') as f:
        f.write(models_content)
    
    print("✅ Файл database/models.py успешно создан!")

if __name__ == "__main__":
    create_models_file() 
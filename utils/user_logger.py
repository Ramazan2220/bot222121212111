import logging
import os
from pathlib import Path
from logging.handlers import TimedRotatingFileHandler
from typing import Dict

from config import DATA_DIR

_USER_LOGGERS: Dict[int, logging.Logger] = {}
_LOGS_ROOT = Path(DATA_DIR) / 'users'


class _UserIdFilter(logging.Filter):
    def __init__(self, user_id: int):
        super().__init__()
        self.user_id = user_id

    def filter(self, record: logging.LogRecord) -> bool:
        if not hasattr(record, 'user_id'):
            record.user_id = self.user_id
        return True


def _ensure_user_log_dir(user_id: int) -> Path:
    user_dir = _LOGS_ROOT / str(user_id) / 'logs'
    os.makedirs(user_dir, exist_ok=True)
    return user_dir


def get_user_logger(user_id: int) -> logging.Logger:
    """Return a per-user logger that writes to data/users/<id>/logs/user.log.

    - Rotates daily at midnight, keeps 7 days.
    - Format: time level user_id message
    """
    if user_id in _USER_LOGGERS:
        return _USER_LOGGERS[user_id]

    user_dir = _ensure_user_log_dir(user_id)
    log_file = user_dir / 'user.log'

    logger = logging.getLogger(f'user.{user_id}')
    logger.setLevel(logging.INFO)
    logger.propagate = True  # also go to root handlers if configured

    # Avoid duplicate handlers if called twice
    if not any(isinstance(h, TimedRotatingFileHandler) and getattr(h, '_user_log', False) for h in logger.handlers):
        handler = TimedRotatingFileHandler(str(log_file), when='midnight', backupCount=7, encoding='utf-8')
        handler._user_log = True  # marker
        fmt = logging.Formatter('%(asctime)s %(levelname)s user=%(user_id)s %(message)s')
        handler.setFormatter(fmt)
        handler.addFilter(_UserIdFilter(user_id))
        logger.addHandler(handler)

    _USER_LOGGERS[user_id] = logger
    return logger


def log_user_info(user_id: int, message: str):
    get_user_logger(user_id).info(message, extra={'user_id': user_id})


def log_user_warning(user_id: int, message: str):
    get_user_logger(user_id).warning(message, extra={'user_id': user_id})


def log_user_error(user_id: int, message: str):
    get_user_logger(user_id).error(message, extra={'user_id': user_id})


def attach_user_context(logger: logging.Logger, user_id: int) -> logging.LoggerAdapter:
    """Wrap an existing logger to inject user_id into records."""
    return logging.LoggerAdapter(logger, {'user_id': user_id}) 
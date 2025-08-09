#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import logging
from typing import Optional, Callable, Any
from config import (
    REDIS_USE_FAKE, REDIS_URL, REDIS_HOST, REDIS_PORT, REDIS_DB, REDIS_PASSWORD,
    REDIS_DEFAULT_TTL_SECONDS
)

logger = logging.getLogger(__name__)

try:
    import redis as real_redis
except Exception:
    real_redis = None

# Init client (singleton)
_client = None


def _get_client():
    global _client
    if _client is not None:
        return _client

    if REDIS_USE_FAKE or real_redis is None:
        from fake_redis import get_fake_redis
        _client = get_fake_redis()
        logger.info("🟡 RedisCache: FakeRedis in use")
    else:
        try:
            if REDIS_URL:
                _client = real_redis.Redis.from_url(REDIS_URL)
            else:
                _client = real_redis.Redis(
                    host=REDIS_HOST,
                    port=REDIS_PORT,
                    db=REDIS_DB,
                    password=REDIS_PASSWORD,
                    decode_responses=False
                )
            # ping
            _client.ping()
            logger.info("🟢 RedisCache: Connected to Redis")
        except Exception as e:
            from fake_redis import get_fake_redis
            _client = get_fake_redis()
            logger.warning(f"🟡 RedisCache: fallback to FakeRedis due to: {e}")
    return _client


def cache_json(key: str, producer: Callable[[], Any], ttl_seconds: Optional[int] = None) -> Any:
    """Get JSON from Redis or produce and set with TTL.
    - key: redis key
    - producer: function returning JSON-serializable object
    - ttl_seconds: TTL or default
    """
    client = _get_client()
    ttl = ttl_seconds or REDIS_DEFAULT_TTL_SECONDS
    try:
        raw = client.get(key) if hasattr(client, 'get') else None
        if raw:
            try:
                if isinstance(raw, (bytes, bytearray)):
                    raw = raw.decode('utf-8')
                return json.loads(raw)
            except Exception:
                pass
        # miss
        value = producer()
        try:
            payload = json.dumps(value)
            if hasattr(client, 'setex'):
                client.setex(key, ttl, payload)
            else:
                client.set(key, payload)
        except Exception:
            pass
        return value
    except Exception as e:
        logger.warning(f"RedisCache get '{key}' failed: {e}")
        return producer()


def delete(key: str) -> None:
    try:
        client = _get_client()
        if hasattr(client, 'delete'):
            client.delete(key)
    except Exception:
        pass 
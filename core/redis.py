import json
import logging
from typing import Any, Optional

import redis

from core.config import settings

logger = logging.getLogger(__name__)

_redis_client: Optional[redis.Redis] = None


def get_redis() -> Optional[redis.Redis]:
    global _redis_client
    if _redis_client is None:
        try:
            _redis_client = redis.from_url(settings.redis_url, decode_responses=True)
            _redis_client.ping()
        except Exception as e:
            logger.error(f"Redis connection failed: {e}")
            _redis_client = None
    return _redis_client


def cache_get(key: str) -> Optional[Any]:
    client = get_redis()
    if not client:
        return None
    try:
        raw = client.get(key)
        if raw:
            return json.loads(raw)
    except Exception as e:
        logger.warning(f"Redis GET error for {key}: {e}")
    return None


def cache_set(key: str, value: Any, ttl: int = 300) -> bool:
    client = get_redis()
    if not client:
        return False
    try:
        client.setex(key, ttl, json.dumps(value, default=str))
        return True
    except Exception as e:
        logger.warning(f"Redis SET error for {key}: {e}")
        return False


def cache_delete(key: str) -> bool:
    client = get_redis()
    if not client:
        return False
    try:
        client.delete(key)
        return True
    except Exception as e:
        logger.warning(f"Redis DELETE error for {key}: {e}")
        return False


def cache_invalidate_storms():
    """Invalidate all storm-related cache keys."""
    client = get_redis()
    if not client:
        return
    try:
        for pattern in ["storms:*", "map:*"]:
            keys = client.keys(pattern)
            if keys:
                client.delete(*keys)
    except Exception as e:
        logger.warning(f"Cache invalidation error: {e}")


def cache_invalidate_lightning_tiles():
    """Invalidate PNG tile cache after a new GFS run is ingested."""
    client = get_redis()
    if not client:
        return
    try:
        keys = client.keys("lightning:risk:tile:png:*")
        if keys:
            client.delete(*keys)
            logger.info(f"Invalidated {len(keys)} lightning risk tile cache entries")
    except Exception as e:
        logger.warning(f"Lightning tile cache invalidation error: {e}")

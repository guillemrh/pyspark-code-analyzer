# backend/app/cache.py
import json
from typing import Any
import redis
from app.config import settings

# create Redis client (use connection URL)
redis_client = redis.Redis.from_url(settings.redis_url, decode_responses=True)


def make_cache_key_for_code(code: str) -> str:
    # deterministic key for the same input
    # consider hashing for long payloads
    import hashlib

    h = hashlib.blake2b(code.encode("utf-8"), digest_size=16).hexdigest()
    return f"explain:code:{h}"


def set_result(key: str, value: Any, ttl: int = 3600):
    redis_client.set(key, json.dumps(value), ex=ttl)


def get_result(key: str):
    raw = redis_client.get(key)
    if not raw:
        return None
    return json.loads(raw)


def get_or_set_job(key: str, initial_value: Any, ttl: int = 3600) -> tuple[Any, bool]:
    """
    Atomically get existing value or set initial value.
    Returns (value, was_created) tuple.
    Uses Redis SETNX for atomicity.
    """
    serialized = json.dumps(initial_value)
    # SETNX returns True if key was set (didn't exist), False if it existed
    was_created = redis_client.setnx(key, serialized)

    if was_created:
        # Set TTL on newly created key
        redis_client.expire(key, ttl)
        return initial_value, True
    else:
        # Key existed, get current value
        raw = redis_client.get(key)
        return json.loads(raw) if raw else None, False

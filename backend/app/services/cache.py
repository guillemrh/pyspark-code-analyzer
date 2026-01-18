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

    h = hashlib.blake2b(code.encode("utf-8"), digest_size=8).hexdigest()
    return f"explain:code:{h}"


def set_result(key: str, value: Any, ttl: int = 3600):
    redis_client.set(key, json.dumps(value), ex=ttl)


def get_result(key: str):
    raw = redis_client.get(key)
    if not raw:
        return None
    return json.loads(raw)

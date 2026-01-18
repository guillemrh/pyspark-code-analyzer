from fastapi import Request, HTTPException
from .services.cache import redis_client
from .config import RATE_LIMIT, RATE_LIMIT_WINDOW


def rate_limit(request: Request):
    ip = request.client.host
    key = f"rate:{ip}"

    count = redis_client.incr(key)
    if count == 1:
        redis_client.expire(key, RATE_LIMIT_WINDOW)

    if count > RATE_LIMIT:
        raise HTTPException(
            status_code=429, detail="Rate limit exceeded. Try again later."
        )

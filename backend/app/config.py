# backend/app/config.py
import os
import sys
from pydantic import field_validator
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    app_env: str = "local"  # local | docker | prod
    app_version: str = "dev"

    gemini_api_key: str
    backend_port: int = 8050  # default port for Docker network
    timeout_seconds: int = 15
    redis_url: str = "redis://redis:6379/0"
    gemini_model: str
    gemini_fallback_model: str | None = None

    @field_validator("gemini_api_key")
    @classmethod
    def api_key_not_empty(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("GEMINI_API_KEY must be set and non-empty")
        return v

    @field_validator("redis_url")
    @classmethod
    def redis_url_valid(cls, v: str) -> str:
        if not v.startswith(("redis://", "rediss://")):
            raise ValueError("REDIS_URL must start with redis:// or rediss://")
        return v

    class Config:
        env_file = ".env" if os.getenv("APP_ENV", "local") == "local" else None


# Force validation at module load time
try:
    settings = Settings()
except Exception as e:
    print(f"FATAL: Configuration error: {e}", file=sys.stderr)
    sys.exit(1)

# --- Application behavior (policy) ---
CACHE_TTL = 3600
RATE_LIMIT = 5
RATE_LIMIT_WINDOW = 60
MAX_CODE_SIZE = 100_000

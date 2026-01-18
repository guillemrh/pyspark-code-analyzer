# backend/app/config.py
import os
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

    class Config:
        env_file = ".env" if os.getenv("APP_ENV", "local") == "local" else None


settings = Settings()

# --- Application behavior (policy) ---
CACHE_TTL = 3600
RATE_LIMIT = 5
RATE_LIMIT_WINDOW = 60

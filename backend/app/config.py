# backend/app/config.py
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    gemini_api_key: str
    backend_port: int = 8050  # default port for Docker network
    timeout_seconds: int = 15
    redis_url: str = "redis://redis:6379/0"
    gemini_model: str = "gemini-2.5-flash"

    class Config:
        env_file = ".env"

settings = Settings()

# --- Application behavior (policy) ---
CACHE_TTL = 3600
RATE_LIMIT = 5
RATE_LIMIT_WINDOW = 60
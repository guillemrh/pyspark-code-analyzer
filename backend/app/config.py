# backend/app/config.py
from pydantic import BaseSettings

class Settings(BaseSettings):
    gemini_api_key: str
    backend_port: int = 8005  # default port for Docker network
    timeout_seconds: int = 15

    class Config:
        env_file = ".env"

settings = Settings()
from pathlib import Path

from loguru import logger
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    DB_URL: str  # Database URL for SQLAlchemy
    CELERY_BACKEND: str  # Celery backend URL
    CELERY_BROKER: str  # Celery broker URL
    REDIS_HOST: str  # Redis host
    REDIS_PORT: int  # Redis port
    REDIS_DB: int  # Redis database number
    LOCAL_DOWNLOAD_PATH: str  # Local path for downloaded files
    MINIO_ENDPOINT: str  # MinIO endpoint URL
    MINIO_ACCESS_KEY: str  # MinIO access key
    MINIO_SECRET_KEY: str  # MinIO secret key
    FORMAT_LOG: str = (
        "{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}"  # Log format for Loguru
    )
    LOG_ROTATION: str = "10 MB"  # Log rotation size for Loguru

    model_config = SettingsConfigDict(
        env_file=Path(__file__).parent.parent / ".env", env_file_encoding="utf-8"
    )


settings = Settings()

logger.add(
    sink="app.log",
    format=settings.FORMAT_LOG,
    level="INFO",
    rotation=settings.LOG_ROTATION,
)

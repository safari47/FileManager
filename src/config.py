from loguru import logger
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    DB_URL: str  # Database URL for SQLAlchemy
    REDIS_URL: str  # Redis URL for caching
    FORMAT_LOG: str = (
        "{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}"  # Log format for Loguru
    )
    LOG_ROTATION: str = "10 MB"  # Log rotation size for Loguru


settings = Settings()

logger.add(
    sink="app.log",
    format=settings.FORMAT_LOG,
    level="INFO",
    rotation=settings.LOG_ROTATION,
)
